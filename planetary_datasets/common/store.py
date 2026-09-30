"""Writing datasets into icechunk stores.

Replaces the ``write_to_icechunk`` function that appeared in twenty files and the
encoding-dict builder that appeared in seventy-one. The append-or-create decision, the
"already written, skip" guard and the compression settings all live here.
"""

from __future__ import annotations

import contextlib
import itertools
import random
import time
import warnings
from typing import Iterable, Sequence

import icechunk
import numpy as np
import pandas as pd
import xarray as xr
import zarr
import zarr.codecs
from icechunk.xarray import to_icechunk
from loguru import logger

from planetary_datasets.common.dataset import coords_match

# Coordinates that must line up with the existing store or an append will corrupt it.
ALIGNMENT_COORDS = ("latitude", "longitude", "level", "isobaricInhPa", "height")

# Errors that mean "this store has nothing in it yet" when the repository is also empty.
STORE_READ_ERRORS = (ValueError, KeyError, FileNotFoundError, icechunk.IcechunkError)

#: Raised when a commit races another writer. ``RebaseFailedError`` is the one that
#: actually surfaces when two writers touched the same chunk; catching only
#: ``ConflictError`` leaves that case unhandled.
RACE_ERRORS = (icechunk.ConflictError, icechunk.RebaseFailedError)

#: How many times a racing commit is retried before giving up.
COMMIT_ATTEMPTS = 5

#: Start of the FutureWarning xarray raises when it decodes a timedelta variable - a
#: forecast ``step`` axis, here - from its ``units`` attribute alone.
_TIMEDELTA_DECODE_WARNING = "In a future version, xarray will not decode the variable"


class StoreReadError(RuntimeError):
    """A store that is known to hold data could not be read.

    Distinct from an empty store. Treating this as empty would silently discard the
    archive, so it is raised rather than swallowed.
    """


@contextlib.contextmanager
def quiet_timedelta_decoding():
    """Silence xarray's timedelta-decoding FutureWarning for the duration of a write.

    Appending makes xarray read the store's own coordinates back to validate the write,
    and a ``step`` axis written before xarray started tagging timedelta variables with a
    ``dtype`` attribute - which is every forecast store in this project - warns once per
    append. The flag that settles it, ``decode_timedelta``, cannot be passed through
    icechunk's writer, so it is silenced here instead; every read this project makes
    passes ``decode_timedelta=True`` explicitly, so the future default does not change
    what we get back.

    Use it around bespoke ``to_icechunk`` calls that append to a store with a ``step``.
    """
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore", message=_TIMEDELTA_DECODE_WARNING, category=FutureWarning
        )
        yield


def has_committed_data(repo: icechunk.Repository, branch: str = "main") -> bool:
    """True when the repository has at least one commit beyond initialisation.

    ``Repository.open_or_create`` leaves a single "initialized" snapshot behind, so a
    count above one means real data has been written. This is the authoritative
    emptiness check: a failed read must never be mistaken for an empty store, or the
    create-fresh path would overwrite an existing archive.
    """
    # Stop at the second snapshot: this runs before every write, and counting a long
    # store's whole history each time grows with every commit made.
    return next(itertools.islice(repo.ancestry(branch=branch), 1, None), None) is not None


def build_encoding(
    ds: xr.Dataset,
    append_dim: str = "time",
    clevel: int = 9,
) -> dict:
    """Build the zstd/bitshuffle encoding dict used for initial writes."""
    encoding: dict = {}
    for var in ds.data_vars:
        encoding[var] = {
            "compressors": zarr.codecs.BloscCodec(cname="zstd", clevel=clevel, shuffle="bitshuffle")
        }
    # Only a datetime append dimension gets CF time encoding. Applying it to, say, a
    # string station id fails in the encoder with "ufunc 'rint' not supported".
    if append_dim in ds.coords and np.issubdtype(ds.coords[append_dim].dtype, np.datetime64):
        encoding[append_dim] = {
            "units": "seconds since 1970-01-01",
            "calendar": "standard",
            "dtype": "int64",
        }
    return encoding


#: Mantissa bits a float32 carries. Keeping all of them is a no-op.
FLOAT32_MANTISSA_BITS = 23

#: Attribute recording how many mantissa bits a variable was rounded to, so a reader can
#: see that the trailing digits are not meaningful rather than inferring it.
KEEPBITS_ATTR = "bitround_keepbits"


def bitround(values: np.ndarray, keepbits: int) -> np.ndarray:
    """Round a float array's mantissa to ``keepbits`` bits, nearest-even.

    Zeroing the low mantissa bits of a float leaves long runs of identical bits, which is
    exactly what the bitshuffle filter in :func:`build_encoding` is there to exploit: the
    compressor then has whole planes of zeros to collapse. It is the single biggest lever
    on the size of these stores — an ocean salinity field is 6.7x smaller under zstd alone
    and 23x smaller with ``keepbits=12`` — and unlike dropping to float16 it is a bounded,
    declared loss rather than a change of type.

    The error is relative: rounding to ``k`` bits leaves a value accurate to roughly one
    part in ``2**k``, so it costs the same fraction of a large value as of a small one.
    Non-finite values pass through untouched.
    """
    array = np.asarray(values)
    if keepbits >= FLOAT32_MANTISSA_BITS or array.dtype.kind != "f":
        return array
    if keepbits < 1:
        raise ValueError(f"keepbits must be >= 1, got {keepbits}")

    original = array.dtype
    # float16 has ten mantissa bits; rounding it to twelve would be a no-op that still
    # paid a round trip through float32, and rounding it lower is better done by not
    # having cast to float16 in the first place.
    if original == np.float16:
        return array

    as32 = array.astype(np.float32)
    bits = as32.view(np.uint32)
    shift = np.uint32(FLOAT32_MANTISSA_BITS - keepbits)
    mask = np.uint32((0xFFFFFFFF >> shift) << shift)
    half = np.uint32(1 << (int(shift) - 1))
    # Add half an interval, minus one, plus the lowest kept bit: the "minus one, plus the
    # tie bit" is what makes an exact tie round to even rather than always up, so a field
    # of ties does not acquire a systematic positive bias.
    ones = (bits >> shift) & np.uint32(1)
    rounded = ((bits + half - np.uint32(1) + ones) & mask).view(np.float32)
    return rounded.astype(original, copy=False)


#: Keepbits below this are not worth storing: at six bits a value is good to one part in
#: 64, which is visible in almost any geophysical field.
MIN_USEFUL_KEEPBITS = 6


def keepbits_for_tolerance(
    magnitude: float,
    absolute_tolerance: float | None = None,
    relative_tolerance: float | None = None,
) -> int:
    """The fewest mantissa bits that hold a quantity to a stated tolerance.

    This is the arithmetic behind a MARS-style per-parameter table: rather than one
    keepbits for a whole dataset, each variable gets the precision its *units* justify.

    Bitrounding is relative — keeping ``k`` bits leaves a value good to about ``2**-(k+1)``
    of itself — so an absolute tolerance has to be converted using the magnitude the
    variable actually reaches::

        k = ceil(log2(magnitude / absolute_tolerance)) - 1

    That is why the same tolerance costs a different number of bits for different
    variables, and why pressure is so expensive: 10 Pa out of 100000 Pa is one part in
    10000, or fourteen bits, while 0.01 K out of 320 K is one part in 32000, fifteen bits.

    Args:
        magnitude: The largest absolute value the variable reaches.
        absolute_tolerance: Largest acceptable error in the variable's own units.
        relative_tolerance: Largest acceptable error as a fraction of the value. Used when
            no absolute tolerance is given.

    Returns:
        Bits to keep, clamped to ``[MIN_USEFUL_KEEPBITS, FLOAT32_MANTISSA_BITS]``.
    """
    if absolute_tolerance is not None:
        if absolute_tolerance <= 0:
            raise ValueError("absolute_tolerance must be positive")
        magnitude = abs(float(magnitude))
        if not np.isfinite(magnitude) or magnitude == 0.0:
            return MIN_USEFUL_KEEPBITS
        ratio = magnitude / absolute_tolerance
    elif relative_tolerance is not None:
        if not 0 < relative_tolerance < 1:
            raise ValueError("relative_tolerance must be in (0, 1)")
        ratio = 1.0 / relative_tolerance
    else:
        raise ValueError("give either absolute_tolerance or relative_tolerance")

    bits = int(np.ceil(np.log2(ratio))) - 1
    return int(np.clip(bits, MIN_USEFUL_KEEPBITS, FLOAT32_MANTISSA_BITS))


def measured_keepbits(
    values, absolute_tolerance: float | None = None, relative_tolerance: float | None = None
) -> int:
    """:func:`keepbits_for_tolerance` with the magnitude taken from the data itself."""
    array = np.asarray(values)
    finite = array[np.isfinite(array)]
    magnitude = float(np.max(np.abs(finite))) if finite.size else 0.0
    return keepbits_for_tolerance(
        magnitude,
        absolute_tolerance=absolute_tolerance,
        relative_tolerance=relative_tolerance,
    )


def bitround_dataset(ds: xr.Dataset, keepbits) -> xr.Dataset:
    """Apply :func:`bitround` to floating point data variables, per variable.

    Args:
        ds: The dataset to round.
        keepbits: Either one number for every variable, or a callable taking a variable
            name and returning the bits to keep for it — or None to store that one
            exactly. Per-variable because how much precision is negligible is a property
            of the quantity, not of the dataset: a wind component crosses zero and is
            differenced to get direction, so it wants every bit it has, while a
            temperature field does not.

    Coordinates are never rounded: they are small, and a rounded latitude would fail the
    alignment check on the next append.
    """
    resolve = keepbits if callable(keepbits) else (lambda _name: keepbits)

    out = ds.copy()
    for name in ds.data_vars:
        bits = resolve(str(name))
        if bits is None or bits >= FLOAT32_MANTISSA_BITS:
            continue
        if ds[name].dtype.kind != "f" or ds[name].dtype == np.float16:
            continue
        rounded = xr.apply_ufunc(
            bitround,
            ds[name],
            kwargs={"keepbits": bits},
            dask="parallelized",
            keep_attrs=True,
            output_dtypes=[ds[name].dtype],
        )
        rounded.attrs = {**ds[name].attrs, KEEPBITS_ATTR: bits}
        out[name] = rounded
    return out


def existing_times(repo: icechunk.Repository, append_dim: str = "time") -> np.ndarray:
    """Return the values already present along ``append_dim``.

    An empty array is returned when the store does not exist yet or has no such
    coordinate, which is the normal state before the first write.
    """
    try:
        ds = xr.open_zarr(
            repo.readonly_session("main").store, consolidated=False, decode_timedelta=True
        )
    except STORE_READ_ERRORS as exc:
        if has_committed_data(repo):
            raise StoreReadError(
                f"store holds committed data but could not be read ({type(exc).__name__}: {exc}). "
                "Refusing to report it as empty."
            ) from exc
        logger.debug(f"store is empty ({type(exc).__name__})")
        return np.array([])
    if append_dim not in ds.coords:
        return np.array([])
    return ds.coords[append_dim].values


def latest_time(repo: icechunk.Repository, append_dim: str = "time") -> pd.Timestamp | None:
    """The last value stored along ``append_dim``, or None for an empty store.

    :func:`write_to_icechunk` only appends after this, so it is also the answer to
    "would the store still accept a step at t?": only if ``t`` is later.
    """
    present = existing_times(repo, append_dim)
    return pd.Timestamp(present.max()) if present.size else None


def has_timestep(repo: icechunk.Repository, timestep, append_dim: str = "time") -> bool:
    """True when ``timestep`` is already present in the store."""
    times = existing_times(repo, append_dim=append_dim)
    if times.size == 0:
        return False
    value = getattr(timestep, "to_numpy", lambda: timestep)()
    return bool(np.isin(value, times).any())


def missing_timesteps(
    repo: icechunk.Repository,
    desired: Sequence,
    append_dim: str = "time",
) -> list:
    """Return the subset of ``desired`` that is not yet in the store."""
    times = existing_times(repo, append_dim=append_dim)
    if times.size == 0:
        return list(desired)
    return [t for t in desired if not np.isin(getattr(t, "to_numpy", lambda: t)(), times).any()]


def missing_periods(
    repo: icechunk.Repository,
    desired: Sequence,
    unit: str = "D",
    append_dim: str = "time",
) -> list:
    """Return the timestamps in ``desired`` whose *period* is not represented in the store.

    :func:`missing_timesteps` asks whether an exact value is stored, which only works when
    the partition timestamp is itself one of the stored values. It is not for a swath
    archive keyed by granule time, or for a forecast archive whose initialisation dates
    fall inside the partition rather than on its first instant: there the question is
    "does the store already hold anything from this day/month?".

    Args:
        repo: Target repository.
        desired: Partition timestamps to test.
        unit: numpy datetime64 unit naming the period, e.g. ``D`` for a day or ``M`` for a
            month.
        append_dim: Dimension holding the stored timestamps.
    """
    times = existing_times(repo, append_dim=append_dim)
    if times.size == 0:
        return list(desired)
    # Truncated inside numpy: a Python set of Timestamps costs gigabytes once a swath
    # store holds tens of millions of scan times, and this runs before every partition.
    stored = np.unique(times.astype(f"datetime64[{unit}]"))
    wanted = np.array([np.datetime64(t, unit) for t in desired], dtype=f"datetime64[{unit}]")
    return [t for t, present in zip(desired, np.isin(wanted, stored)) if not present]


def _backoff(attempt: int) -> None:
    """Sleep a jittered exponential interval before retrying a raced commit."""
    time.sleep(min(2**attempt, 30) * (0.5 + random.random()))


def _axis_arrays(group: zarr.Group, append_dim: str) -> list[tuple[str, zarr.Array, int]]:
    """Every array in ``group`` that spans ``append_dim``, with the axis it spans it on.

    Arrays without the dimension - a static 2-D ``latitude``, the model level
    coefficients - are left alone: growing the append axis does not move them.
    """
    found = []
    for path, array in group.arrays():
        dims = tuple(array.metadata.dimension_names or ())
        if append_dim in dims:
            found.append((path, array, dims.index(append_dim)))
    return found


def unreorderable_reason(
    repo: icechunk.Repository, append_dim: str = "time"
) -> str | None:
    """Why this store's append axis cannot be reordered, or None when it can be.

    Reordering remaps chunks through :meth:`icechunk.Session.reindex_array`, which moves
    whole chunks. An array holding several steps per chunk along ``append_dim`` would need
    its chunks split to put one step somewhere else, which a manifest rewrite cannot do.
    Every store this project writes a timestep at a time is chunked one step per chunk, so
    this is a guard against the exception, not the common case.
    """
    session = repo.readonly_session("main")
    try:
        group = zarr.open_group(session.store, mode="r", zarr_format=3)
    except STORE_READ_ERRORS as exc:
        return f"store could not be opened ({type(exc).__name__}: {exc})"
    for path, array, axis in _axis_arrays(group, append_dim):
        if path == append_dim:
            continue
        if array.chunks[axis] != 1:
            return (
                f"{path} is chunked {array.chunks[axis]} steps per chunk along "
                f"{append_dim}; reordering needs one step per chunk"
            )
    return None


def _encode_axis(array: zarr.Array, values: np.ndarray) -> np.ndarray:
    """Encode append-axis values the way the stored coordinate array holds them.

    A datetime axis is written CF-encoded (``seconds since 1970-01-01`` by
    :func:`build_encoding`), so the raw integers have to be rebuilt from the array's own
    ``units``/``calendar``. Anything else - a station id, an integer member number - is
    stored as-is.
    """
    if not np.issubdtype(np.asarray(values).dtype, np.datetime64):
        return np.asarray(values).astype(array.dtype)
    encoded, _, _ = xr.coding.times.encode_cf_datetime(
        values,
        units=array.attrs["units"],
        calendar=array.attrs.get("calendar", "standard"),
    )
    return encoded.astype(array.dtype)


def _reorder_axis(
    session: icechunk.Session,
    append_dim: str,
    old: pd.Index,
    new: pd.Index,
) -> None:
    """Move the append axis from ``old`` to ``new``, taking existing chunks with it.

    ``new`` must contain every value in ``old``. Arrays are resized along ``append_dim``
    and their chunks remapped with :meth:`icechunk.Session.reindex_array`: a manifest
    rewrite, so no chunk is read or copied however much data the store holds. Positions no
    existing chunk maps onto are left empty and read back as fill values.

    Nothing is committed here, and nothing else may be writing to the store while it runs;
    see :func:`sort_append_axis`, its only caller.
    """
    forward_map = new.get_indexer(old)
    backward_map = {int(dst): src for src, dst in enumerate(forward_map)}
    # A pure append leaves every existing chunk exactly where it is.
    moved = not np.array_equal(forward_map, np.arange(len(old)))

    group = zarr.open_group(session.store, mode="r+", zarr_format=3)
    for path, array, axis in _axis_arrays(group, append_dim):
        shape = list(array.shape)
        shape[axis] = len(new)
        if path == append_dim:
            array.resize(tuple(shape))
            array[:] = _encode_axis(array, new.values)
            continue
        array.resize(tuple(shape))
        if not moved:
            continue

        def forward(coord, axis=axis):
            coord = list(coord)
            coord[axis] = int(forward_map[coord[axis]])
            return coord

        def backward(coord, axis=axis):
            coord = list(coord)
            source = backward_map.get(coord[axis])
            if source is None:
                return None
            coord[axis] = source
            return coord

        session.reindex_array(f"/{path}", forward, backward)


def axis_is_sorted(repo: icechunk.Repository, append_dim: str = "time") -> bool:
    """True when the store's append axis is in order, so it needs no reordering."""
    return pd.Index(existing_times(repo, append_dim=append_dim)).is_monotonic_increasing


def sort_append_axis(
    repo: icechunk.Repository,
    append_dim: str = "time",
    attempts: int = COMMIT_ATTEMPTS,
) -> bool:
    """Sort a store's append axis in place, moving existing chunks to match.

    Out-of-order writes leave ``append_dim`` unsorted: the values are all correct and
    paired with the right data, but they do not ascend, so ``.sel(time=slice(...))`` on
    the store is unreliable. This puts them back in order by resizing nothing and moving
    nothing: the chunks are remapped onto their sorted indices through
    :meth:`icechunk.Session.reindex_array`, which rewrites the manifest only. No chunk is
    read or copied, however large the store is.

    **This must not run while anything else is writing to the store.** It moves chunks, so
    a writer that looked up a position before the move would write it to the wrong index.
    Run it as its own step, with the store's ingest quiet; ``dags/factory.py`` builds the
    reorder asset into the same Dagster pool as the ingest it belongs to, which is what
    keeps the two from overlapping.

    Returns True when the axis was changed, False when it was already sorted.
    """
    for attempt in range(attempts):
        session = repo.writable_session("main")
        old = pd.Index(
            xr.open_zarr(session.store, consolidated=False, decode_timedelta=True)
            .coords[append_dim]
            .values
        )
        if old.is_monotonic_increasing:
            logger.info(f"{append_dim} is already sorted over {len(old)} step(s), nothing to do")
            return False
        if not old.is_unique:
            duplicated = old[old.duplicated()].unique()
            raise ValueError(
                f"{append_dim} holds {len(duplicated)} duplicated value(s), e.g. "
                f"{duplicated[0]}; sorting would leave two chunks claiming one position. "
                "Resolve the duplicates before reordering."
            )
        blocked = unreorderable_reason(repo, append_dim)
        if blocked is not None:
            raise ValueError(f"cannot reorder {append_dim}: {blocked}")

        _reorder_axis(session, append_dim, old, old.sort_values())
        try:
            session.commit(f"Sort {append_dim} over {len(old)} step(s)")
        except RACE_ERRORS:
            # Something else is writing. Retrying is only safe because the sort is
            # recomputed from the new snapshot; it is still a sign the store was not quiet.
            if attempt == attempts - 1:
                raise
            logger.warning(
                f"reorder raced a writer; the store is not quiet (attempt {attempt + 1})"
            )
            _backoff(attempt)
            continue
        logger.info(f"sorted {append_dim} over {len(old)} step(s)")
        return True
    raise RuntimeError("unreachable")


def write_to_icechunk(
    repo: icechunk.Repository,
    ds: xr.Dataset,
    append_dim: str = "time",
    message: str | None = None,
    check_vars: bool = True,
    alignment_coords: Iterable[str] = ALIGNMENT_COORDS,
    require_monotonic: bool = False,
    attempts: int = COMMIT_ATTEMPTS,
) -> bool:
    """Write ``ds`` to ``repo``, creating the store or adding to it along ``append_dim``.

    Returns True if data was committed, False if the write was skipped. Skipping is normal
    and not an error: it means the timestep is already stored, or the dataset does not line
    up with what is there.

    Steps may arrive in any order. A step that sorts before the end of the store is still
    appended onto the end — this only ever appends — which leaves ``append_dim`` unsorted
    until someone reorders it. That is the trade deliberately taken: a backfill can run its
    partitions in whatever order Dagster schedules them and lose nothing, where refusing
    out-of-order steps lost every one of them permanently.

    An unsorted axis is *correct but not ordered*: every value is paired with the right
    data, so ``.sel(time=t)`` and ``missing_timesteps`` are exact, but ``.sel`` over a
    *slice* is unreliable until the store is put back in order. Sorting is
    :func:`sort_append_axis`, run on its own with nothing else writing — never from here,
    because it moves chunks out from under any concurrent writer. In Dagster it is the
    separate ``<name>-reorder`` asset, which a human materialises.

    Args:
        repo: Target repository.
        ds: Dataset to write. Must have ``append_dim`` as a coordinate.
        append_dim: Dimension to add along.
        message: Commit message. A default naming the timestep is used when omitted.
        check_vars: Refuse to write when the variable set differs from the store's.
        alignment_coords: Coordinates that must match the store exactly before writing.
        require_monotonic: Drop incoming steps that fall at or before the last stored one
            instead of appending them out of order. The old behaviour, and lossy: the
            dropped steps are not written anywhere. For stores that must stay sorted at
            every instant and would rather keep a visible gap.
        attempts: How many times to retry a commit that races another writer.
    """
    if append_dim not in ds.coords:
        raise ValueError(f"dataset has no {append_dim!r} coordinate to append along")

    for attempt in range(attempts):
        try:
            return _write_once(
                repo,
                ds,
                append_dim=append_dim,
                message=message,
                check_vars=check_vars,
                alignment_coords=alignment_coords,
                require_monotonic=require_monotonic,
            )
        except RACE_ERRORS as exc:
            # Another writer committed under us. Everything the decision rested on - what
            # is already stored, where each step belongs on the axis - was read from that
            # stale snapshot, so the whole write is redone against the new one rather than
            # the commit simply being retried.
            if attempt == attempts - 1:
                logger.error(f"write raced another writer {attempts} times, giving up: {exc}")
                raise
            logger.warning(f"write raced another writer ({type(exc).__name__}), retrying")
            _backoff(attempt)
    raise RuntimeError("unreachable")


def _write_once(
    repo: icechunk.Repository,
    ds: xr.Dataset,
    append_dim: str,
    message: str | None,
    check_vars: bool,
    alignment_coords: Iterable[str],
    require_monotonic: bool,
) -> bool:
    """One attempt at :func:`write_to_icechunk`; raises on a raced commit."""
    session = repo.writable_session("main")
    try:
        existing = xr.open_zarr(session.store, consolidated=False, decode_timedelta=True)
        first_write = append_dim not in existing.coords
    except STORE_READ_ERRORS as exc:
        # A read failure against a store that already holds data must not fall through to
        # the create-fresh path: that would overwrite the whole archive with one timestep.
        if has_committed_data(repo):
            raise StoreReadError(
                f"store holds committed data but could not be read ({type(exc).__name__}: {exc}). "
                "Refusing to overwrite it with a fresh write."
            ) from exc
        logger.debug(f"creating new store ({type(exc).__name__})")
        existing = None
        first_write = True

    # atleast_1d so a scalar append coordinate does not raise IndexError here.
    incoming = np.atleast_1d(ds[append_dim].values)

    if first_write:
        with quiet_timedelta_decoding():
            to_icechunk(ds, session, encoding=build_encoding(ds, append_dim=append_dim))
        session.commit(message or f"Initial write of {append_dim} {incoming[0]}")
        logger.info(f"created store with {ds.sizes.get(append_dim, 1)} step(s)")
        return True

    present = existing.coords[append_dim].values
    already = np.isin(incoming, present)
    if already.all():
        logger.debug(f"{incoming[0]} already in store, skipping write")
        return False

    if already.any():
        # Partial overlap: writing the whole batch would duplicate the steps that are
        # already stored. Write only the new ones.
        keep = np.flatnonzero(~already)
        logger.info(
            f"{int(already.sum())} of {incoming.size} steps already stored, "
            f"writing the remaining {keep.size}"
        )
        ds = ds.isel({append_dim: keep})
        incoming = incoming[~already]

    if check_vars and set(ds.data_vars) != set(existing.data_vars):
        only_new = set(ds.data_vars) - set(existing.data_vars)
        only_old = set(existing.data_vars) - set(ds.data_vars)
        logger.error(
            f"variable mismatch, skipping write. only in new: {sorted(only_new)}; "
            f"only in store: {sorted(only_old)}"
        )
        return False

    ok, bad = coords_match(existing, ds, tuple(alignment_coords))
    if not ok:
        logger.error(f"coordinate {bad!r} does not match the store, skipping write")
        return False

    # Steps that sort before the end of the store. A provider that revisits a partial hour
    # (MRMS and the UK radar composite both do, deliberately, while the upstream archive is
    # still filling in) and any backfill whose partitions do not run in time order produce
    # these. They are appended onto the end like anything else, leaving the axis unsorted
    # until the store's reorder asset is run.
    if present.size:
        stale = incoming < present.max()
        if stale.any() and require_monotonic:
            logger.error(
                f"{int(stale.sum())} of {incoming.size} step(s) are before the last stored "
                f"{append_dim} ({present.max()}) and require_monotonic is set; dropping "
                f"them rather than writing an unsorted {append_dim}. They are not written "
                f"anywhere. First dropped: {incoming[stale][0]}"
            )
            keep = np.flatnonzero(~stale)
            if keep.size == 0:
                return False
            ds = ds.isel({append_dim: keep})
            incoming = incoming[~stale]
        elif stale.any():
            logger.info(
                f"{int(stale.sum())} of {incoming.size} step(s) sort before the stored "
                f"{present.max()}; appending them out of order. {append_dim} will need "
                f"sorting (sort_append_axis). First: {incoming[stale][0]}"
            )

    # "a-" appends only the variables that have append_dim. Everything else - static
    # 2-D latitude/longitude, say - was written with the store and was just checked
    # against it; plain "a" would rewrite it on every append.
    with quiet_timedelta_decoding():
        to_icechunk(ds, session, append_dim=append_dim, mode="a-")
    session.commit(
        message or f"Append {append_dim} {incoming[0]}",
        rebase_with=icechunk.ConflictDetector(),
    )
    logger.info(f"appended {append_dim} {incoming[0]}")
    return True
