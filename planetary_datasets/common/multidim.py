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

from planetary_datasets.common.lossless import (
    cast_exactly,
    fits,
    narrowest_dtype,
    narrowest_dtypes,
)
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


def _open(store, group: str | None, mode: str) -> zarr.Group:
    """The store's root group, or ``group`` within it."""
    return zarr.open_group(store, path=group or "", mode=mode)


def ledger(repo: icechunk.Repository, group: str | None = None) -> set[str]:
    """Partitions a store (or ``group``) holds, from its ledger; empty if not created yet."""
    try:
        group = _open(repo.readonly_session("main").store, group, "r")
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
        if variable.dtype.kind == "M":
            # Units fixed at creation that hold any later datetime exactly; xarray would
            # otherwise pick the coarsest that fits the first partition (whole days, say).
            entry.update(units=TIME_UNITS, calendar="proleptic_gregorian", dtype="int64")
            entry["_FillValue"] = NAT  # NaT, declared so it decodes back to NaT
        elif variable.dtype.kind == "m":
            entry.update(units="nanoseconds", dtype="int64")
            entry["_FillValue"] = NAT
        if variable.ndim and variable.dims[0] not in fixed:
            entry["chunks"] = (_chunk_rows(variable), *variable.shape[1:])
        # The source's fill value and dtype would otherwise override the chosen dtype.
        variable.encoding = {}
    return encoding


def _growing_variables(ds: xr.Dataset, dim: str) -> list[str]:
    return [name for name in ds.variables if ds[name].ndim and ds[name].dims[0] == dim]


def _array_dims(array: zarr.Array) -> tuple[str, ...]:
    return tuple(array.metadata.dimension_names or ())


def pad_value(attrs: Mapping, dtype: np.dtype, array_fill=None):
    """What padding is filled with: the variable's fill value, else the array's, else
    NaN for floats, "" for strings and 0 for anything else."""
    if "_FillValue" in attrs and attrs["_FillValue"] is not None:
        return attrs["_FillValue"]
    if array_fill is not None:
        return array_fill
    if dtype.kind == "f":
        return np.nan
    if dtype.kind in "OUS":
        return ""
    return 0


def pad_to(ds: xr.Dataset, sizes: Mapping[str, int]) -> xr.Dataset:
    """``ds`` with every variable padded at the end of each dimension in ``sizes``."""
    variables = {}
    changed = False
    for name, variable in ds.variables.items():
        widths = [(0, max(0, sizes.get(d, n) - n)) for d, n in zip(variable.dims, variable.shape)]
        if any(w[1] for w in widths):
            values = variable.values
            fill = pad_value(variable.attrs, values.dtype)
            if values.dtype.kind in "OUS":
                values = values.astype(object)
            variable = xr.Variable(
                variable.dims, np.pad(values, widths, constant_values=fill), variable.attrs
            )
            changed = True
        variables[name] = variable
    # Built in one go: padding variable by variable leaves a dimension at two sizes.
    return xr.Dataset(variables, attrs=ds.attrs) if changed else ds


def _stored_size(group: zarr.Group, dim: str) -> int | None:
    for name in group.array_keys():
        dims = _array_dims(group[name])
        if dim in dims:
            return group[name].shape[dims.index(dim)]
    return None


def _grow_dim(group: zarr.Group, dim: str, size: int) -> None:
    """Widen every stored array on ``dim`` to ``size``; the new positions read as fill."""
    for name in group.array_keys():
        array = group[name]
        dims = _array_dims(array)
        if dim in dims and array.shape[dims.index(dim)] < size:
            shape = list(array.shape)
            shape[dims.index(dim)] = size
            array.resize(tuple(shape))


def _create_array(group: zarr.Group, name: str, variable: xr.Variable, rows: int) -> None:
    """A variable new to the store, ``rows`` long already (those rows read as fill)."""
    from xarray.backends.zarr import FillValueCoder

    attrs = json_safe_attrs({k: v for k, v in variable.attrs.items() if k != "_FillValue"})
    fill = variable.attrs.get("_FillValue")
    strings = variable.dtype.kind in "OUS"
    if fill is not None and not strings:
        fill = np.asarray(fill).astype(variable.dtype)[()]
        attrs["_FillValue"] = (
            FillValueCoder.encode(fill, variable.dtype)
            if variable.dtype.kind == "f"
            else fill.item()
        )
    if variable.dtype.kind in "Mm":
        raise NotImplementedError(f"{name}: cannot add a datetime variable to an existing store")
    group.create_array(
        name,
        shape=(rows, *variable.shape[1:]),
        chunks=(_chunk_rows(variable), *variable.shape[1:]),
        dtype=str if strings else variable.dtype,
        compressors=(zarr.codecs.ZstdCodec(level=CLEVEL),)
        if strings
        else (zarr.codecs.BloscCodec(cname="zstd", clevel=CLEVEL, shuffle="bitshuffle"),),
        # Without a fill value of its own, a float's earlier rows read as NaN, not 0.
        fill_value=None
        if strings
        else (np.nan if fill is None and variable.dtype.kind == "f" else fill),
        dimension_names=variable.dims,
        attributes=attrs,
    )


#: Units every datetime array is created with: int64 nanoseconds hold any datetime64[ns].
TIME_UNITS = "nanoseconds since 1970-01-01"
_UNIT_NS = {
    "nanoseconds": 1,
    "microseconds": 10**3,
    "milliseconds": 10**6,
    "seconds": 10**9,
    "minutes": 60 * 10**9,
    "hours": 3600 * 10**9,
    "days": 86400 * 10**9,
}
NAT = np.iinfo("int64").min


def _time_fill(array: zarr.Array) -> int:
    from xarray.backends.zarr import FillValueCoder

    fill = array.attrs.get("_FillValue")
    if isinstance(fill, str):
        fill = FillValueCoder.decode(fill, array.dtype)
    return int(fill) if fill is not None else NAT


def _rescale_to_ns(array: zarr.Array) -> None:
    """Rewrite a stored time array in exact int64 nanoseconds (since 1970 for datetimes)."""
    import pandas as pd

    unit, _, since = str(array.attrs["units"]).partition(" since ")
    factor = _UNIT_NS[unit.strip()]
    fill = _time_fill(array)
    values = array[:].astype("int64")
    valid = values != fill
    offset = 0
    if since:
        ref = pd.Timestamp(since.strip())
        ref = ref.tz_convert(None) if ref.tzinfo else ref
        offset = int((ref - pd.Timestamp("1970-01-01")).value)
    rescaled = np.where(valid, values * factor + offset, NAT)
    if array.dtype != np.dtype("int64"):
        raise ValueError(f"{array.name}: cannot rescale a {array.dtype} time array in place")
    array[:] = rescaled
    attrs = dict(array.attrs)
    attrs["units"] = TIME_UNITS if since else "nanoseconds"
    attrs["_FillValue"] = NAT
    array.attrs.update(attrs)


def encode_times(array: zarr.Array, values: np.ndarray) -> np.ndarray:
    """``values`` (datetime64/timedelta64) as the stored ``array`` encodes them, exactly.

    An array whose units cannot hold the new values exactly (days, when a time has
    seconds) is first rewritten in nanoseconds; nothing is ever rounded.
    """
    import pandas as pd

    if "units" not in array.attrs:
        raise ValueError(f"{array.name}: stored time array has no units")
    unit, _, since = str(array.attrs["units"]).partition(" since ")
    unit = unit.strip()
    if unit not in _UNIT_NS:
        raise ValueError(f"{array.name}: unsupported time units {array.attrs['units']!r}")
    kind = "M" if since else "m"
    ns = np.asarray(values).astype(f"{kind}8[ns]").view("int64")
    missing = ns == NAT
    offset = 0
    if since:
        ref = pd.Timestamp(since.strip())
        ref = ref.tz_convert(None) if ref.tzinfo else ref
        offset = int((ref - pd.Timestamp("1970-01-01")).value)
    factor = _UNIT_NS[unit]
    shifted = ns - offset
    if factor != 1 and np.any((shifted % factor != 0) & ~missing):
        _rescale_to_ns(array)
        return encode_times(array, values)
    out = np.where(missing, _time_fill(array), shifted // factor)
    return out.astype(array.dtype)


def _append(
    session: icechunk.Session, ds: xr.Dataset, fixed: set[str], path: str | None = None
) -> None:
    """Append ``ds`` along each of its growing dimensions.

    The partition and the store are reconciled first: a fixed dimension larger in the
    partition widens the store's arrays, and one smaller in the partition is padded. A
    variable the store lacks is created (earlier rows read as fill); one the partition
    lacks is extended with fill. Products' processing baselines differ in exactly these
    ways (EarthCARE's height grid went from 241 to 242 levels).
    """
    group = _open(session.store, path, "a")
    targets = {}
    for dim in fixed:
        stored = _stored_size(group, dim)
        targets[dim] = max(ds.sizes[dim], stored or 0)
        if stored is not None and targets[dim] > stored:
            _grow_dim(group, dim, targets[dim])
    ds = pad_to(ds, targets)
    for dim in ds.dims:
        if dim in fixed or not ds.sizes[dim]:
            continue
        existing = [n for n in group.array_keys() if _array_dims(group[n])[:1] == (dim,)]
        start = group[existing[0]].shape[0] if existing else 0
        for name in existing:
            if group[name].shape[0] != start:
                raise ValueError(
                    f"{name}: {group[name].shape[0]} rows along {dim}, expected {start}"
                )
        stop = start + ds.sizes[dim]
        names = _growing_variables(ds, dim)
        for name in names:
            if name not in group:
                variable = ds[name].variable
                if variable.dtype.kind == "f":
                    narrowed = narrowest_dtype(variable.values, variable.attrs.get("_FillValue"))
                    variable = cast_exactly(xr.Dataset({name: variable}), {name: narrowed})[
                        name
                    ].variable
                _create_array(group, name, variable, start)
                existing.append(name)
        for name in existing:
            array = group[name]
            array.resize((stop, *array.shape[1:]))
        # The values are already in the store's encoding (read raw, cast exactly), so they
        # go to zarr as they are. A region write through xarray would re-encode them with
        # the store's attributes: dividing by a stored scale_factor, and putting the fill
        # value in place of any NaN.
        times = [name for name in names if ds[name].dtype.kind in "Mm"]
        for name in names:
            if name in times:
                continue
            values = ds[name].values
            if values.dtype.kind in "OUS":
                values = np.asarray(values, dtype=object)
            elif group[name].dtype != values.dtype:
                values = cast_exactly(xr.Dataset({name: ds[name]}), {name: group[name].dtype})[
                    name
                ].values
            group[name][start:stop] = values
        for name in times:
            group[name][start:stop] = encode_times(group[name], ds[name].values)


#: Suffix of an array's wider copy while :func:`widen` replaces it.
WIDENING = ".__widening"


def widen(
    repo: icechunk.Repository,
    name: str,
    dtype: np.dtype,
    label: str = "store",
    group: str | None = None,
) -> None:
    """Rewrite array ``name`` in the wider ``dtype``, values unchanged.

    A float's dtype is narrowed from the first partition written; a later one can hold
    values that need more. The array is copied at the wider dtype, the original removed
    and the copy moved into its place, which icechunk only allows in a session of its
    own; a widening interrupted between the two commits is finished by the next call.
    """
    from xarray.backends.zarr import FillValueCoder

    copy = name + WIDENING
    path = group
    prefix = f"/{path}/" if path else "/"
    group = _open(repo.readonly_session("main").store, path, "r")
    if name in group:
        session = repo.writable_session("main")
        group = _open(session.store, path, "a")
        old = group[name]
        attrs = dict(old.attrs)
        if "_FillValue" in attrs and old.dtype.kind == "f":
            value = FillValueCoder.decode(attrs["_FillValue"], old.dtype)
            attrs["_FillValue"] = FillValueCoder.encode(value, dtype)
        fill = old.fill_value
        new = group.create_array(
            copy,
            shape=old.shape,
            chunks=old.chunks,
            dtype=dtype,
            serializer=old.serializer,
            compressors=old.compressors,
            filters=old.filters,
            fill_value=None if fill is None else np.asarray(fill).astype(dtype)[()],
            dimension_names=old.metadata.dimension_names,
            attributes=attrs,
        )
        step = max(1, old.chunks[0]) * 64
        for start in range(0, old.shape[0], step):
            new[start : start + step] = old[start : start + step].astype(dtype)
        del group[name]
        session.commit(
            f"{label}: widen {name} to {dtype} (copy)", rebase_with=icechunk.ConflictDetector()
        )
    if copy in _open(repo.readonly_session("main").store, path, "r"):
        session = repo.rearrange_session("main")
        session.move(f"{prefix}{copy}", f"{prefix}{name}")
        session.commit(f"{label}: widen {name} to {dtype}", rebase_with=icechunk.ConflictDetector())
    logger.info(f"{label}: widened {name} to {dtype}")


def append_partition(
    repo: icechunk.Repository,
    ds: xr.Dataset,
    partition: str | None,
    fixed_dims: Iterable[str] = (),
    label: str = "store",
    group: str | None = None,
) -> bool:
    """Write one partition's rows along every dimension and add it to the ledger.

    Several writers may share a store if they write different variables (one per
    product, say): a commit is rebased over the others', and a partition whose changes
    really do overlap another writer's is written again from the new tip.

    Returns False, writing nothing, when the ledger already lists ``partition``. With
    ``partition=None`` the ledger is neither checked nor updated: the caller keeps track of
    what it has written some other way. With ``group``, the rows, and the ledger, go to that
    group of the store; writers of different groups rebase over each other's commits.

    Raises:
        LossyCastError: A value does not fit the store's dtype exactly.
        StoreReadError: The store holds data but could not be read.
    """
    for attempt in range(1, COMMIT_ATTEMPTS + 1):
        try:
            return _append_once(repo, ds, partition, set(fixed_dims), label, group)
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
    path: str | None = None,
) -> bool:
    ds = ds.drop_indexes(list(ds.indexes))
    clash = [d for d in ds.dims if d in ds.variables and d not in ds.coords]
    if clash:
        raise ValueError(f"variables named like dimensions: {clash}")
    session = repo.writable_session("main")
    try:
        group = _open(session.store, path, "r")
        exists = bool(list(group.array_keys()))
        attrs = dict(group.attrs) if exists else {}
    except (*STORE_READ_ERRORS, zarr.errors.GroupNotFoundError) as exc:
        # A group not created yet is expected in a store that holds others.
        missing_group = path and isinstance(exc, zarr.errors.GroupNotFoundError)
        if not missing_group and has_committed_data(repo):
            raise StoreReadError(f"{label}: store holds data but could not be read: {exc}") from exc
        exists, attrs = False, {}
    written = list(attrs.get(LEDGER_ATTR, []))
    if partition is not None and partition in written:
        return False

    with quiet_timedelta_decoding():
        present = set(group.array_keys()) & set(ds.variables) if exists else set()
        if present:
            dtypes = {name: group[name].dtype for name in present}
            narrow = {
                name: dtype
                for name, dtype in dtypes.items()
                if dtype.kind == "f"
                and ds[name].dtype.kind == "f"
                and not fits(ds[name].values, dtype)
            }
            if narrow:
                # Narrowed from earlier partitions, but this one needs more precision.
                for name in narrow:
                    widen(repo, name, np.dtype(ds[name].dtype), label, path)
                return _append_once(repo, ds, partition, fixed, label, path)
            # Times are encoded into their stored units by encode_times, not cast: a cast of
            # datetime64 to the stored int64 is raw nanoseconds whatever the units say.
            ds = cast_exactly(
                ds,
                {
                    k: v
                    for k, v in dtypes.items()
                    if v.kind in "fiu" and ds[k].dtype.kind not in "Mm"
                },
            )
            _append(session, ds, fixed, path)
        else:
            # A new store, or variables new to it (another product sharing the store):
            # their dtypes are decided now, from this partition.
            ds = cast_exactly(ds, narrowest_dtypes(ds))
            for variable in ds.variables.values():
                variable.attrs = json_safe_attrs(variable.attrs)
            ds.attrs = json_safe_attrs(ds.attrs) if not exists else {}
            mode = "a" if exists else "w-"
            # xarray encodes datetime64[s] (or [ms], [us]) under nanosecond units as NaT, so
            # every time column goes to it at nanosecond resolution.
            ds = ds.assign(
                {
                    name: ds[name].astype(f"{ds[name].dtype.kind}8[ns]")
                    for name in ds.variables
                    if ds[name].dtype.kind in "Mm"
                    and ds[name].dtype != f"{ds[name].dtype.kind}8[ns]"
                }
            )
            to_icechunk(ds, session, mode=mode, encoding=_encoding(ds, fixed), group=path)
    group = _open(session.store, path, "a")
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


def mark_written(
    repo: icechunk.Repository, partition: str, label: str = "store", group: str | None = None
) -> None:
    """Add ``partition`` to the ledger of an existing store (or ``group``) without rows."""
    for attempt in range(1, COMMIT_ATTEMPTS + 1):
        session = repo.writable_session("main")
        node = _open(session.store, group, "a")
        written = set(node.attrs.get(LEDGER_ATTR, []))
        if partition in written:
            return
        node.attrs[LEDGER_ATTR] = sorted(written | {partition})
        try:
            session.commit(
                f"{label}: {partition} complete", rebase_with=icechunk.ConflictDetector()
            )
            return
        except (icechunk.ConflictError, icechunk.RebaseFailedError):
            if attempt == COMMIT_ATTEMPTS:
                raise
            time.sleep(attempt)


def concat_frames(
    frames: list[xr.Dataset], prefix: str, first_index: int, frame: str = "frame"
) -> xr.Dataset:
    """Files of one product end to end along ``<prefix>_along_track``.

    Each file's variables are expected on ``<prefix>_along_track`` (first) or on
    ``<prefix>_<frame>``, one row per file. Every track row gets ``<prefix>_<frame>_index``,
    its file's position in the store counting from ``first_index``.
    """
    along, per = f"{prefix}_along_track", f"{prefix}_{frame}"
    secondary = sorted({d for ds in frames for d in ds.dims} - {along, per})
    largest = {d: max(ds.sizes.get(d, 0) for ds in frames) for d in secondary}
    with_index = []
    for offset, ds in enumerate(frames):
        index = np.full(ds.sizes[along], first_index + offset, dtype="uint32")
        sizes = {
            f"{prefix}_size_{d.removeprefix(prefix + '_')}": (
                (per,),
                np.array([ds.sizes.get(d, 0)], "uint32"),
            )
            for d in secondary
        }
        ds = ds.assign({f"{prefix}_{frame}_index": ((along,), index), **sizes})
        with_index.append(pad_to(ds, largest))
    names = list(dict.fromkeys(n for d in with_index for n in d.variables))
    track = [n for n in names if next(d[n] for d in with_index if n in d).dims[:1] == (along,)]
    rest = [n for n in names if n not in track]
    # A variable some files lack is given fill there, so the files concatenate.
    for i, d in enumerate(with_index):
        missing = {}
        for n in names:
            if n in d:
                continue
            like = next(o[n] for o in with_index if n in o)
            rows = d.sizes[along] if n in track else 1
            shape = (rows, *like.shape[1:])
            fill = pad_value(like.attrs, like.dtype)
            missing[n] = xr.Variable(
                like.dims,
                np.full(shape, fill, dtype=object if like.dtype.kind in "OUS" else like.dtype),
                like.attrs,
            )
        if missing:
            with_index[i] = d.assign(missing)
    return xr.merge(
        [
            xr.concat([d[track] for d in with_index], dim=along, data_vars="all"),
            xr.concat([d[rest] for d in with_index], dim=per, data_vars="all"),
        ],
        compat="no_conflicts",
        # Variable attributes carry the fill values, units and scale factors xarray
        # decodes with; "drop" here would drop them along with the dataset's.
        combine_attrs="override",
    )


__all__ = [
    "widen",
    "pad_to",
    "pad_value",
    "CHUNK_BYTES",
    "concat_frames",
    "LEDGER_ATTR",
    "append_partition",
    "json_safe_attrs",
    "ledger",
    "mark_written",
]
