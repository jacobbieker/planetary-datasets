"""Lazy, memory-bounded datasets assembled from one-field-per-file sources.

Some archives - DWD's ICON output is the motivating one - publish a forecast run as tens
of thousands of small files, one per parameter, level and lead time. Decoded, a single
ICON-Global run with model levels is hundreds of GB, so it can never be loaded and then
written: it has to be streamed into the store one zarr chunk at a time.

:func:`lazy_field_dataset` builds an :class:`xarray.Dataset` whose variables are dask
arrays in which **every dask block is exactly one zarr chunk** of shape
``{init_time: 1, step: S, <level>: 1, values: n_cells}``. Computing a block allocates one
buffer, has an injected :data:`Reader` decode each of its source fields straight into
the buffer's rows, and leaves NaN wherever a source is missing. Nothing here knows about
GRIB or ICON; the caller supplies where each field lives (``uri_for``) and how to decode
it (the reader).

Writing such a dataset with :func:`planetary_datasets.common.store.write_to_icechunk`
streams it block by block. How many blocks are resident at once is set by the dask
scheduler, which :func:`bounded_compute` pins to a fixed-size thread pool for the
duration of the write; :func:`estimate_peak_bytes` turns that into a memory estimate.

Typical use::

    ds = lazy_field_dataset(variables, coords, reader, fetch_workers=8, token=run_id)
    require_memory(estimate_peak_bytes(ds, 4) / BYTES_PER_GB, "icon run")
    with bounded_compute(4):
        write_to_icechunk(repo, ds, append_dim="init_time")
"""

from __future__ import annotations

import contextlib
import math
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator, Mapping, Sequence

import dask
import dask.array as da
import numcodecs.blosc
import numpy as np
import xarray as xr
from dask.base import tokenize
from dask.highlevelgraph import HighLevelGraph
from loguru import logger

__all__ = [
    "FieldVariable",
    "Reader",
    "WRITE_OVERHEAD_FACTOR",
    "bounded_compute",
    "choose_step_chunk",
    "estimate_peak_bytes",
    "lazy_field_dataset",
    "missing_report",
    "read_failures",
]

#: The trailing dimension every field variable ends with: one decoded field, flattened.
VALUES_DIM = "values"

#: Decodes the field at ``uri`` into ``out_row`` (a 1-D view into a block buffer),
#: writing every element, and returns True; or returns False when the source is
#: permanently missing. Transient failures - a timeout, a 503 - must raise, so the write
#: fails instead of storing NaN. Any other return value is a bug in the reader and raises
#: TypeError rather than being taken for a miss.
Reader = Callable[[str, np.ndarray], bool]

#: Peak resident bytes one in-flight block costs, as a multiple of its decoded size.
#:
#: Writing a block holds, at worst: the block buffer itself (1x); zarr's
#: ``write_empty_chunks=False`` test, a boolean ``isnan`` mask (0.25x for float32); the
#: blosc output buffer, allocated at the full input size plus header before it is
#: trimmed (1x); and the compressed bytes copied once more on their way into icechunk's
#: store (up to 1x for incompressible data, far less for real fields). That is 3.25x for
#: data blosc cannot compress at all; 3.5 leaves headroom for allocator slack.
#:
#: Measured with zarr 3.4, icechunk 2.2, numcodecs 0.17 and dask 2025.12 on macOS arm64,
#: as peak RSS (``ru_maxrss``) growth over the pre-write RSS while writing 141.6 MB
#: blocks (12 steps of a 2,949,120-cell float32 field) of uniform random data, which
#: blosc cannot compress and so is the worst case. Per block in flight that came to
#: 3.23x with one worker, 2.70x with two, 2.39x with four and 2.20x with eight; the
#: fixed costs amortise as workers are added, so the single-worker figure bounds them all.
WRITE_OVERHEAD_FACTOR = 3.5

#: Bytes per MiB, the unit :func:`choose_step_chunk` sizes chunks in.
_MIB = 1024**2

# dask's config is process-global, so two overlapping bounded_compute blocks would each
# restore the other's settings - or a pool already shut down. Held while one is active.
_BOUNDED = threading.Lock()


@dataclass(frozen=True)
class FieldVariable:
    """One data variable built from single-field source files.

    Attributes:
        name: Variable name in the dataset.
        dims: Dimension names. The last is always ``"values"``, the flattened field.
        shape: Length along each dimension.
        chunks: Chunk length along each dimension. The ``values`` chunk is the full field,
            so a block is a whole number of fields.
        uri_for: Maps an index over every dimension except ``values`` to the source URI
            holding that field, or None when the field does not exist. Called for every
            field when its block is computed, and by :func:`missing_report`; it must be
            cheap and must not do I/O.
        dtype: Floating point dtype of the decoded data. Missing fields are NaN.
        attrs: Attributes written with the variable.
    """

    name: str
    dims: tuple[str, ...]
    shape: tuple[int, ...]
    chunks: tuple[int, ...]
    uri_for: Callable[[tuple[int, ...]], str | None]
    dtype: np.dtype = np.float32
    attrs: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "dims", tuple(self.dims))
        object.__setattr__(self, "shape", tuple(int(n) for n in self.shape))
        object.__setattr__(self, "chunks", tuple(int(n) for n in self.chunks))
        object.__setattr__(self, "dtype", np.dtype(self.dtype))
        if not self.dims or self.dims[-1] != VALUES_DIM:
            raise ValueError(f"{self.name}: dims must end with {VALUES_DIM!r}, got {self.dims}")
        if not len(self.dims) == len(self.shape) == len(self.chunks):
            raise ValueError(
                f"{self.name}: dims {self.dims}, shape {self.shape} and chunks {self.chunks} "
                "must have the same length"
            )
        if any(n < 1 for n in self.shape) or any(c < 1 for c in self.chunks):
            raise ValueError(f"{self.name}: shape and chunks must be positive")
        if self.chunks[-1] != self.shape[-1]:
            raise ValueError(
                f"{self.name}: the {VALUES_DIM!r} chunk must be the whole field "
                f"({self.shape[-1]}), got {self.chunks[-1]}"
            )
        if self.dtype.kind != "f":
            raise ValueError(
                f"{self.name}: dtype must be floating point to hold NaN, got {self.dtype}"
            )

    @property
    def numblocks(self) -> tuple[int, ...]:
        """Number of blocks along each dimension."""
        return tuple(math.ceil(n / c) for n, c in zip(self.shape, self.chunks))


class _BlockLoader:
    """Computes the blocks of one :class:`FieldVariable`; dask calls it once per block.

    Holds a running count of fields the reader reported missing, for
    :func:`read_failures`. It is cumulative over every compute of the dataset, retries
    included.
    """

    def __init__(self, variable: FieldVariable, reader: Reader, fetch_workers: int) -> None:
        self.variable = variable
        self.reader = reader
        self.fetch_workers = fetch_workers
        self._lock = threading.Lock()
        self.reader_missing = 0

    def __call__(self, *block_index: int) -> np.ndarray:
        """Read and return the block at ``block_index``, NaN where a field is missing."""
        var = self.variable
        started = time.perf_counter()
        starts = tuple(b * c for b, c in zip(block_index, var.chunks))
        shape = tuple(min(c, n - s) for c, n, s in zip(var.chunks, var.shape, starts))
        # Not pre-filled: every row is either written whole by the reader or set to NaN
        # here, and skipping the fill saves a pass over ~150 MB per block.
        buffer = np.empty(shape, dtype=var.dtype)

        jobs: list[tuple[str, tuple[int, ...]]] = []
        absent = 0
        for local in np.ndindex(*shape[:-1]):
            uri = var.uri_for(tuple(s + i for s, i in zip(starts, local)))
            if uri is None:
                buffer[local] = np.nan
                absent += 1
            else:
                jobs.append((uri, local))

        not_found = self._read_rows(buffer, jobs)
        with self._lock:
            self.reader_missing += not_found
        logger.debug(
            f"{var.name} block {block_index}: {len(jobs) - not_found} field(s) read, "
            f"{absent} without a source, {not_found} missing at the source, "
            f"{time.perf_counter() - started:.2f}s"
        )
        return buffer

    def _read_one(self, uri: str, row: np.ndarray) -> bool:
        """Read one field into ``row``; on a permanent miss, reset the row to NaN."""
        try:
            found = self.reader(uri, row)
        except Exception as exc:
            exc.add_note(f"while reading {uri} for {self.variable.name}")
            raise
        if found is False or found is np.False_:
            # The row is uninitialised memory, or part-written if the reader discovered
            # the miss late; either way it must not be stored as data.
            row[...] = np.nan
            return False
        if found is True or found is np.True_:
            return True
        raise TypeError(
            f"reader returned {found!r} for {uri}; it must return True (read) or False "
            "(permanently missing)"
        )

    def _read_rows(self, buffer: np.ndarray, jobs: list[tuple[str, tuple[int, ...]]]) -> int:
        """Read every ``(uri, row index)`` job into ``buffer``; return how many were missing."""
        if not jobs:
            return 0
        workers = min(self.fetch_workers, len(jobs))
        if workers == 1:
            return sum(not self._read_one(uri, buffer[local]) for uri, local in jobs)

        executor = ThreadPoolExecutor(workers, thread_name_prefix=f"fetch-{self.variable.name}")
        try:
            futures = [executor.submit(self._read_one, uri, buffer[local]) for uri, local in jobs]
            # result() re-raises a reader's exception; the finally then drops the reads
            # that have not started rather than finishing a block that is going to fail.
            return sum(not future.result() for future in as_completed(futures))
        finally:
            executor.shutdown(wait=True, cancel_futures=True)


def _lazy_array(
    variable: FieldVariable, loader: _BlockLoader, token: str | None, coords_token: str
) -> da.Array:
    """A dask array over ``variable`` with exactly one task per block.

    The graph is built directly rather than through ``map_blocks``: each entry is one
    small tuple naming the shared loader and the block's indices, with no per-block
    ``block_info`` dictionaries. Forty-five ICON-Global-shaped variables (113 steps in
    12-step chunks, 90 levels; 40,500 blocks) build in 0.7 s and 13 MB.

    The task name is a hash of the variable's layout, the coordinates and ``token``.
    Without a token, the URIs of the variable's first and last fields stand in for the
    source, so datasets with the same coordinates read from different places still get
    different names.
    """
    if token is None:
        last = tuple(n - 1 for n in variable.shape[:-1])
        token = tokenize(variable.uri_for((0,) * len(last)), variable.uri_for(last))
    identity = tokenize(
        variable.name,
        variable.dims,
        variable.shape,
        variable.chunks,
        variable.dtype.str,
        coords_token,
        token,
    )
    name = f"lazyfield-{variable.name}-{identity}"
    dsk = {(name, *index): (loader, *index) for index in np.ndindex(*variable.numblocks)}
    graph = HighLevelGraph.from_collections(name, dsk, dependencies=())
    chunks = da.core.normalize_chunks(variable.chunks, variable.shape, dtype=variable.dtype)
    meta = np.empty((0,) * len(variable.shape), dtype=variable.dtype)
    return da.Array(graph, name, chunks, meta=meta)


def _loader_of(data: Any) -> _BlockLoader | None:
    """The loader behind a dask array built by :func:`_lazy_array`, or None.

    Read from the array's own graph rather than a registry, so it is always the loader
    of this array: a subset or transformed array has a different name and finds nothing.
    """
    if not isinstance(data, da.Array):
        return None
    layer = data.dask.layers.get(data.name)
    if layer is None:
        return None
    task = layer.get((data.name, *(0,) * data.ndim))
    func = task[0] if isinstance(task, tuple) and task else getattr(task, "func", None)
    return func if isinstance(func, _BlockLoader) else None


def lazy_field_dataset(
    variables: Sequence[FieldVariable],
    coords: Mapping[str, Any],
    reader: Reader,
    *,
    fetch_workers: int = 8,
    attrs: Mapping[str, Any] | None = None,
    token: str | None = None,
) -> xr.Dataset:
    """Build a lazy dataset where every dask block is one zarr chunk of source fields.

    Nothing is read here. Each block, when computed, allocates one buffer, reads its
    fields into it with up to ``fetch_workers`` concurrent ``reader`` calls (each writing
    its own row, so no field is copied), and leaves NaN for fields whose ``uri_for`` is
    None or whose reader returned False.

    The dask chunks are the variables' ``chunks``, and :func:`write_to_icechunk` derives
    the zarr chunks of a new store from them, so the store is chunked exactly as
    declared. Appends must use the same chunking.

    Args:
        variables: The variables to build. Names must be unique.
        coords: Coordinates, as accepted by :class:`xarray.Dataset`. Every dimension used
            by a variable needs a coordinate of matching length if it is to be appended
            along or aligned on.
        reader: Decodes one field into a row; see :data:`Reader`.
        fetch_workers: Concurrent reader calls per block.
        attrs: Dataset attributes.
        token: Identifies the source data (a run's base URL and init time, say). Dask
            task names are keyed on it and on ``coords``, so it must differ between
            datasets that read different data with the same coordinates: two with the
            same token, coordinates and variable layout would be merged as one if
            combined in a single graph. Defaults, per variable, to the URIs of its first
            and last fields.

    Returns:
        The lazy dataset.

    Raises:
        ValueError: On duplicate variable names or a non-positive ``fetch_workers``.
    """
    if fetch_workers < 1:
        raise ValueError(f"fetch_workers must be at least 1, got {fetch_workers}")
    names = [v.name for v in variables]
    if len(set(names)) != len(names):
        raise ValueError(f"duplicate variable names: {names}")
    coords_token = tokenize(dict(coords))

    data_vars = {}
    for variable in variables:
        loader = _BlockLoader(variable, reader, fetch_workers)
        array = _lazy_array(variable, loader, token, coords_token)
        data_vars[variable.name] = xr.Variable(variable.dims, array, attrs=dict(variable.attrs))
    return xr.Dataset(data_vars, coords=coords, attrs=dict(attrs or {}))


def choose_step_chunk(n_values: int, itemsize: int, n_steps: int, target_mb: float = 150.0) -> int:
    """How many lead times to put in one chunk so a chunk is about ``target_mb``.

    The largest step count whose chunk fits in ``target_mb`` MiB (but at least one), then
    evened out so the last chunk along ``step`` is not a sliver: 113 steps at a 12-step
    budget gives ten chunks of 12, 12, ..., 5 rather than a final chunk of one.

    Args:
        n_values: Cells in one field.
        itemsize: Bytes per cell.
        n_steps: Lead times in a run.
        target_mb: Chunk budget in MiB.

    Returns:
        Steps per chunk, in ``[1, n_steps]``.

    Raises:
        ValueError: On non-positive arguments.
    """
    if n_values < 1 or itemsize < 1 or n_steps < 1 or target_mb <= 0:
        raise ValueError("n_values, itemsize, n_steps and target_mb must be positive")
    field_bytes = n_values * itemsize
    fit = max(1, min(n_steps, int(target_mb * _MIB // field_bytes)))
    return math.ceil(n_steps / math.ceil(n_steps / fit))


@contextlib.contextmanager
def bounded_compute(num_workers: int) -> Iterator[None]:
    """Run dask computations in the block on at most ``num_workers`` threads.

    ``icechunk.xarray.to_icechunk`` (icechunk 2.x) writes dask variables with
    ``dask.array.store(compute=False)`` and then a single ``dask.compute`` of the merged
    changesets, passing no scheduler of its own. That compute resolves its scheduler from
    dask's config, which this sets to the threaded scheduler backed by a dedicated
    ``num_workers`` pool. A config scheduler outranks a default distributed client, and a
    config ``pool`` outranks ``num_workers``, so both are set to keep an outer setting from
    widening the write. The local scheduler never runs more tasks than the pool has
    workers, so at most ``num_workers`` blocks are being read or encoded at once.

    Blosc's internal thread count is capped at ``num_workers`` as well. zarr calls blosc
    from worker threads, where numcodecs already compresses single-threaded, but a
    compress from the main thread would otherwise fan out to every core.

    Both settings are process-wide: any other dask work in the process runs on the same
    small pool while the block is active. For the same reason only one block may be
    active at a time; overlapping ones would restore each other's settings, so a second
    one raises instead.

    Args:
        num_workers: Maximum concurrent dask tasks.

    Raises:
        ValueError: If ``num_workers`` is less than one.
        RuntimeError: If another ``bounded_compute`` is already active in the process.
    """
    if num_workers < 1:
        raise ValueError(f"num_workers must be at least 1, got {num_workers}")
    if not _BOUNDED.acquire(blocking=False):
        raise RuntimeError(
            "bounded_compute is already active in this process; its dask and blosc "
            "settings are process-wide and cannot be nested or overlapped"
        )
    try:
        previous_blosc = numcodecs.blosc.get_nthreads()
        pool = ThreadPoolExecutor(num_workers, thread_name_prefix="bounded-compute")
        numcodecs.blosc.set_nthreads(min(previous_blosc, num_workers))
        try:
            with dask.config.set(scheduler="threads", pool=pool, num_workers=num_workers):
                yield
        finally:
            numcodecs.blosc.set_nthreads(previous_blosc)
            pool.shutdown(wait=True)
    finally:
        _BOUNDED.release()


def _block_nbytes(var: xr.Variable | xr.DataArray) -> int:
    """Bytes of the largest block of a dask variable, or the whole of an eager one."""
    chunksizes = var.chunksizes
    if not chunksizes:
        return int(var.nbytes)
    return math.prod(max(sizes) for sizes in chunksizes.values()) * var.dtype.itemsize


def estimate_peak_bytes(ds: xr.Dataset, num_workers: int, *, reader_bytes: int = 0) -> int:
    """Conservative peak memory for writing ``ds`` under :func:`bounded_compute`.

    ``num_workers`` blocks in flight, each costing the largest block times
    :data:`WRITE_OVERHEAD_FACTOR`, plus everything that is resident regardless: eager
    coordinates (a 2.9M-cell latitude/longitude pair is 47 MB) and any eager data
    variables. The interpreter's own baseline (a few hundred MB with xarray, dask, zarr
    and icechunk imported) is not included.

    Finished blocks do not pile up behind their store tasks: writing 5,000 blocks across
    ten variables under ``bounded_compute(4)``, no more than four block buffers were ever
    alive at once, so ``num_workers`` is the right multiplier.

    Args:
        ds: Dataset to be written, typically from :func:`lazy_field_dataset`.
        num_workers: The ``num_workers`` the write runs under.
        reader_bytes: Extra memory one in-flight reader call holds - compressed source
            bytes plus any decode scratch. Multiplied by each lazy variable's
            ``fetch_workers`` and by ``num_workers``.

    Returns:
        Estimated peak bytes.
    """
    largest = 0
    fetch_workers = 1
    resident = 0
    for var in ds.variables.values():
        if var.chunks is None:
            resident += int(var.nbytes)
            continue
        largest = max(largest, _block_nbytes(var))
        loader = _loader_of(var.data)
        if loader is not None:
            fetch_workers = max(fetch_workers, loader.fetch_workers)
    per_worker = largest * WRITE_OVERHEAD_FACTOR + reader_bytes * fetch_workers
    return int(max(1, num_workers) * per_worker) + resident


def _loaders(ds: xr.Dataset) -> Iterator[tuple[str, _BlockLoader]]:
    """Each data variable of ``ds`` still backed directly by a block loader."""
    for name, var in ds.data_vars.items():
        loader = _loader_of(var.data)
        if loader is not None:
            yield str(name), loader


def missing_report(ds: xr.Dataset) -> dict[str, int]:
    """Fields with no source URI, per variable, from ``uri_for`` alone; nothing is read.

    Only variables taken unmodified from :func:`lazy_field_dataset` are reported: a
    variable that has been subset or transformed no longer maps onto its loader's index
    space.

    Args:
        ds: A dataset from :func:`lazy_field_dataset`.

    Returns:
        Variable name to the number of fields whose ``uri_for`` returned None.
    """
    report = {}
    for name, loader in _loaders(ds):
        var = loader.variable
        report[name] = sum(var.uri_for(index) is None for index in np.ndindex(*var.shape[:-1]))
    return report


def read_failures(ds: xr.Dataset) -> dict[str, int]:
    """Fields a reader reported missing so far, per variable.

    Counts are cumulative over every compute of the dataset, so a write retried after a
    commit race counts its misses twice.

    Args:
        ds: A dataset from :func:`lazy_field_dataset`.

    Returns:
        Variable name to the number of reader calls that returned False.
    """
    return {name: loader.reader_missing for name, loader in _loaders(ds)}
