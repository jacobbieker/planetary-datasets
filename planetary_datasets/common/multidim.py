"""Append partitions to a store whose variables grow along several dimensions at once.

Observation products come in groups of different lengths: Aeolus L2B has one row per
Mie wind result, another per Rayleigh wind result, another per measurement, and so on.
Keeping each group in its own store would split one product over many, so they share a
store, each group along its own dimension, and a partition grows every one of them in a
single commit. Dimensions listed as fixed (a product's 24 range bins, say) never grow and
must match what the store already has.

The store is one flat group that ``xarray.open_zarr`` opens directly. A ledger of the
partitions written is kept in its attributes, committed with the rows, so a partition is
either wholly in the store and listed or not there at all, and the store's arrays are
never read back to decide what to skip.

Dtypes are decided by :mod:`planetary_datasets.common.lossless` when the store is
created, and every later partition is checked against them before it is written.
"""

from __future__ import annotations

import time
from typing import Iterable, Mapping

import icechunk
import numpy as np
import xarray as xr
import zarr
from icechunk.xarray import to_icechunk
from loguru import logger

from planetary_datasets.common.lossless import cast_exactly, narrowest_dtypes
from planetary_datasets.common.store import (
    STORE_READ_ERRORS,
    StoreReadError,
    build_encoding,
    has_committed_data,
    quiet_timedelta_decoding,
)

#: Store attribute listing the partitions written.
LEDGER_ATTR = "partitions_written"

#: Target uncompressed bytes per chunk along a growing dimension.
CHUNK_BYTES = 4 * 2**20
#: zstd level. Still lossless; level 9 compressed an EarthCARE frame 5% smaller than this
#: but took 15 times as long (151 s against 10 s), which no backfill can afford.
CLEVEL = 5
#: Attempts at a partition whose commit conflicts with another writer's.
COMMIT_ATTEMPTS = 5


def ledger(repo: icechunk.Repository) -> set[str]:
    """Partitions a store holds, from its ledger; empty for a store not created yet."""
    try:
        group = zarr.open_group(repo.readonly_session("main").store, mode="r")
    except (*STORE_READ_ERRORS, zarr.errors.GroupNotFoundError):
        return set()
    return set(group.attrs.get(LEDGER_ATTR, []))


def json_safe_attrs(attrs: Mapping) -> dict:
    """``attrs`` with numpy values made JSON-serialisable; anything else is dropped."""
    out = {}
    for key, value in attrs.items():
        if isinstance(value, np.ndarray):
            value = value.tolist()
        elif isinstance(value, np.generic):
            value = value.item()
        if isinstance(value, (str, int, float, bool, list)) or value is None:
            out[key] = value
    return out


def _chunk_rows(variable: xr.Variable) -> int:
    inner = int(np.prod(variable.shape[1:])) if variable.ndim > 1 else 1
    return max(1, CHUNK_BYTES // max(1, inner * variable.dtype.itemsize))


def _encoding(ds: xr.Dataset, fixed: set[str]) -> dict:
    encoding = build_encoding(ds, append_dim="", clevel=CLEVEL)
    for name, variable in ds.variables.items():
        if variable.dtype.kind in "OUS":
            encoding.pop(name, None)
            continue
        entry = encoding.setdefault(name, {})
        if "_FillValue" in variable.attrs:
            entry["_FillValue"] = np.asarray(variable.attrs.pop("_FillValue")).astype(
                variable.dtype
            )[()]
        else:
            # Otherwise xarray declares a default (NaN, or 0 for integers) that the source
            # never had, and a reader would mask real values equal to it.
            entry["_FillValue"] = None
        if variable.ndim and variable.dims[0] not in fixed:
            entry["chunks"] = (_chunk_rows(variable), *variable.shape[1:])
        # The source's fill value and dtype would otherwise override the chosen dtype.
        variable.encoding = {}
    return encoding


def _growing_variables(ds: xr.Dataset, dim: str) -> list[str]:
    return [name for name in ds.variables if ds[name].ndim and ds[name].dims[0] == dim]


def _array_dims(array: zarr.Array) -> tuple[str, ...]:
    return tuple(array.metadata.dimension_names or ())


def _append(session: icechunk.Session, ds: xr.Dataset, fixed: set[str]) -> None:
    group = zarr.open_group(session.store, mode="a")
    # The store's variables on this partition's dimensions; other products' are untouched.
    stored = {
        name
        for name in group.array_keys()
        if set(_array_dims(group[name])) and set(_array_dims(group[name])) <= set(ds.dims)
    }
    if set(ds.variables) != stored:
        raise ValueError(
            f"partition variables differ from the store's: "
            f"new {sorted(set(ds.variables) - stored)}, absent {sorted(stored - set(ds.variables))}"
        )
    for dim in fixed:
        for name in ds.variables:
            if dim in ds[name].dims:
                axis = ds[name].dims.index(dim)
                if group[name].shape[axis] != ds.sizes[dim]:
                    raise ValueError(f"{name}: fixed dimension {dim} changed size")
    for dim in ds.dims:
        if dim in fixed or not ds.sizes[dim]:
            continue
        names = _growing_variables(ds, dim)
        start = group[names[0]].shape[0]
        stop = start + ds.sizes[dim]
        for name in names:
            array = group[name]
            if array.shape[0] != start:
                raise ValueError(f"{name}: {array.shape[0]} rows along {dim}, expected {start}")
            array.resize((stop, *array.shape[1:]))
        strings = [name for name in names if ds[name].dtype.kind in "OUS"]
        for name in strings:
            group[name][start:stop] = np.asarray(ds[name].values, dtype=object)
        rest = [name for name in names if name not in strings]
        if rest:
            part = ds[rest].drop_indexes([n for n in rest if n in ds.indexes], errors="ignore")
            # The attributes are the store's already; a region write must not restate them.
            for variable in part.variables.values():
                variable.attrs = {}
            to_icechunk(part, session, region={dim: slice(start, stop)})


def append_partition(
    repo: icechunk.Repository,
    ds: xr.Dataset,
    partition: str | None,
    fixed_dims: Iterable[str] = (),
    label: str = "store",
) -> bool:
    """Write one partition's rows along every dimension and add it to the ledger.

    Several writers may share a store if they write different variables (one per
    product, say): a commit is rebased over the others', and a partition whose changes
    really do overlap another writer's is written again from the new tip.

    Returns False, writing nothing, when the ledger already lists ``partition``. With
    ``partition=None`` the ledger is neither checked nor updated: the caller keeps track of
    what it has written some other way.

    Raises:
        LossyCastError: A value does not fit the store's dtype exactly.
        StoreReadError: The store holds data but could not be read.
    """
    for attempt in range(1, COMMIT_ATTEMPTS + 1):
        try:
            return _append_once(repo, ds, partition, set(fixed_dims), label)
        except (icechunk.ConflictError, icechunk.RebaseFailedError) as exc:
            if attempt == COMMIT_ATTEMPTS:
                raise
            logger.warning(f"{label}: commit conflicted ({exc}); writing it again")
            time.sleep(attempt)
    raise AssertionError("unreachable")


def _append_once(
    repo: icechunk.Repository,
    ds: xr.Dataset,
    partition: str | None,
    fixed: set[str],
    label: str,
) -> bool:
    ds = ds.drop_indexes(list(ds.indexes))
    clash = [d for d in ds.dims if d in ds.variables and d not in ds.coords]
    if clash:
        raise ValueError(f"variables named like dimensions: {clash}")
    session = repo.writable_session("main")
    try:
        group = zarr.open_group(session.store, mode="r")
        exists = bool(list(group.array_keys()))
        attrs = dict(group.attrs) if exists else {}
    except (*STORE_READ_ERRORS, zarr.errors.GroupNotFoundError) as exc:
        if has_committed_data(repo):
            raise StoreReadError(f"{label}: store holds data but could not be read: {exc}") from exc
        exists, attrs = False, {}
    written = list(attrs.get(LEDGER_ATTR, []))
    if partition is not None and partition in written:
        return False

    with quiet_timedelta_decoding():
        present = set(group.array_keys()) & set(ds.variables) if exists else set()
        if present:
            dtypes = {name: group[name].dtype for name in present}
            ds = cast_exactly(ds, {k: v for k, v in dtypes.items() if v.kind in "fiu"})
            _append(session, ds, fixed)
        else:
            # A new store, or variables new to it (another product sharing the store):
            # their dtypes are decided now, from this partition.
            ds = cast_exactly(ds, narrowest_dtypes(ds))
            for variable in ds.variables.values():
                variable.attrs = json_safe_attrs(variable.attrs)
            ds.attrs = json_safe_attrs(ds.attrs) if not exists else {}
            mode = "a" if exists else "w-"
            to_icechunk(ds, session, mode=mode, encoding=_encoding(ds, fixed))
    group = zarr.open_group(session.store, mode="a")
    # Adding variables with xarray replaces the group's attributes; put them back. Left
    # alone when unchanged, so writers of different variables never touch shared metadata.
    if partition is not None:
        attrs[LEDGER_ATTR] = sorted(set(written) | {partition})
    if dict(group.attrs) != attrs:
        group.attrs.update(attrs)
    session.commit(
        f"{label}: {partition or ', '.join(f'{k}={v}' for k, v in ds.sizes.items())}",
        rebase_with=icechunk.ConflictDetector(),
    )
    logger.info(f"{label}: wrote {partition or ''} ({dict(ds.sizes)})")
    return True


def mark_written(repo: icechunk.Repository, partition: str, label: str = "store") -> None:
    """Add ``partition`` to the ledger of an existing store without writing rows."""
    session = repo.writable_session("main")
    group = zarr.open_group(session.store, mode="a")
    written = set(group.attrs.get(LEDGER_ATTR, []))
    if partition in written:
        return
    group.attrs[LEDGER_ATTR] = sorted(written | {partition})
    session.commit(f"{label}: {partition} complete")


__all__ = [
    "CHUNK_BYTES",
    "LEDGER_ATTR",
    "append_partition",
    "json_safe_attrs",
    "ledger",
    "mark_written",
]
