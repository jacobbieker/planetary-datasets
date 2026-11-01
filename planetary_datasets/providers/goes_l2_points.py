r"""GOES-R point products materialised into Icechunk: derived motion winds and GLM lightning.

The ABI derived motion winds (``ABI-L2-DMW*``, ``ABI-L2-DMWV*``) and the GLM lightning
events (``GLM-L2-LCFA``) are lists of observations, a different length in every file, so
they cannot be appended as virtual references like the gridded L2 products
(:mod:`planetary_datasets.providers.virtualized.goes_l2`). They are copied instead, one
store per satellite with a group per product::

    bkr/geo/goes19_l2_points.icechunk
        /dmw_f_c14        derived motion winds, full disk, from band 14
        /dmwv_m1_c08      the clear-sky water vapour winds, mesoscale sector 1, band 8
        /glm_lcfa         GLM events, groups and flashes

Open the whole store with ``xr.open_datatree(store, engine="zarr", consolidated=False)``
or one product with ``xr.open_zarr(store, group="glm_lcfa", consolidated=False)``.

Each group is ragged: the observations of every file are appended along one dimension
(``wind``; for GLM ``event``, ``group`` and ``flash``), each row carrying the index of its
file (``wind_file_index``, ...) into the ``file`` dimension, which holds the file name,
coverage times and the file's scalar variables. Values are copied as published (packed
integers with their ``scale_factor``/``add_offset``, fill values declared), so the store
decodes as the files do; floats narrow only where every value survives exactly. ``lat``,
``lon`` and ``DQF`` are named ``latitude``, ``longitude`` and ``data_quality_flag``.

GLM times are offsets from each file's own start (``event_time_offset``'s units are
"seconds since <that file's start>"), so they are stored with units of seconds and the
start each counts from is ``time_offset_reference`` along ``file``: an event's time is
``time_offset_reference[event_file_index] + event_time_offset``.

Each partition (a day of winds, six hours of lightning) is one commit, recorded in its
group's ledger; the last week goes first, newest first, then the backlog oldest first.

    python -m planetary_datasets.providers.goes_l2_points --satellite 19 --follow

Files are parsed in worker processes (Python's default forkserver start), which import
the calling program's main module: call :func:`run_prefix` from behind an
``if __name__ == "__main__":`` guard, as ``main`` here is.
"""

from __future__ import annotations

import argparse
import datetime as dt
import io
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from typing import Sequence

import icechunk
import numpy as np
import pandas as pd
import xarray as xr
import zarr
from loguru import logger

from planetary_datasets.common import multidim
from planetary_datasets.config import Config, get_config
from planetary_datasets.providers.virtualized.goes_l2 import (
    SATELLITES,
    SETTLE,
    _children,
    order_days,
    source_store,
)

#: Source prefixes, by the product family a lane can be told to run.
FAMILIES = {
    "DMW": ("ABI-L2-DMWF", "ABI-L2-DMWC", "ABI-L2-DMWM"),
    "DMWV": ("ABI-L2-DMWVF", "ABI-L2-DMWVC", "ABI-L2-DMWVM"),
    "GLM": ("GLM-L2-LCFA",),
}
#: Hours per partition: a day of winds is small; a day of lightning is tens of millions
#: of events, so it goes six hours at a time.
BLOCK_HOURS = {"DMW": 24, "DMWV": 24, "GLM": 6}
#: The source's per-file list dimensions, and what they are called here.
RAGGED = {
    "nMeasures": "wind",
    "number_of_events": "event",
    "number_of_groups": "group",
    "number_of_flashes": "flash",
}
FILE_DIM = "file"
RENAMES = {"lat": "latitude", "lon": "longitude", "DQF": "data_quality_flag"}
#: GOES-R's own epoch: times counted from it are absolute, from anything else per file.
J2000 = pd.Timestamp("2000-01-01 12:00:00")
#: Attributes that must agree for packed values to be appended to what is stored.
PACKING = ("scale_factor", "add_offset", "_Unsigned", "units")
#: Global attributes kept per file.
FILE_ATTRS = ("time_coverage_start", "time_coverage_end", "orbital_slot", "production_site")
DOWNLOAD_WORKERS = 16
RECENT_DAYS = 7
ROOT_ATTRS = {
    "title": "GOES-R ABI derived motion winds and GLM lightning, materialised from NOAA Open Data",
    "source": "s3://noaa-goes{NN} (mirror: gs://gcp-public-data-goes-{NN})",
    "layout": (
        "One group per product: dmw[v]_<sector>_c<band>, glm_lcfa. Rows of every file are "
        "appended along wind (or event/group/flash), each with <dim>_file_index into the "
        "file dimension. Open with xr.open_datatree(store, engine='zarr', "
        "consolidated=False) or xr.open_zarr(store, group=..., consolidated=False)."
    ),
}

_DMW_NAME = re.compile(r"OR_ABI-L2-(DMWV?)(F|C|M1|M2)-M\d+C(\d\d)_G\d\d_s")


class PackingChanged(ValueError):
    """A file's packing (scale, offset, units) differs from what its group stores."""


def _same(a, b) -> bool:
    """Packing attributes agree; numbers compared as the float32 they are published as."""
    if a is None or b is None:
        return a is None and b is None
    try:
        return bool(np.all(np.float32(a) == np.float32(b)))
    except (TypeError, ValueError):
        return str(a) == str(b)


def store_prefix(satellite: int) -> str:
    return f"bkr/geo/goes{satellite}_l2_points.icechunk"


def group_of(key: str) -> str | None:
    """The store group a source file belongs to, or None if it is not a point product."""
    name = key.rsplit("/", 1)[-1]
    if not name.endswith(".nc"):
        return None
    if name.startswith("OR_GLM-L2-LCFA_"):
        return "glm_lcfa"
    match = _DMW_NAME.match(name)
    if match is None:
        return None
    code, sector, band = match.groups()
    return f"{code.lower()}_{sector.lower()}_c{band}"


def family_of(group: str) -> str:
    return "GLM" if group == "glm_lcfa" else group.split("_", 1)[0].upper()


def partitions_of(day: dt.date, family: str) -> list[str]:
    """The ledger names of a day's partitions: the day, or its six-hour blocks."""
    hours = BLOCK_HOURS[family]
    if hours >= 24:
        return [day.isoformat()]
    return [f"{day.isoformat()}T{h:02d}" for h in range(0, 24, hours)]


def partition_of(key: str, day: dt.date, family: str) -> str:
    """The partition a file of ``day`` falls in, from its scan start."""
    hours = BLOCK_HOURS[family]
    if hours >= 24:
        return day.isoformat()
    stamp = next(p for p in key.rsplit("/", 1)[-1].split("_") if p.startswith("s"))
    hour = int(stamp[8:10])
    return f"{day.isoformat()}T{hour - hour % hours:02d}"


# ----------------------------------------------------------------------------- reading


def _reference(units: str) -> pd.Timestamp | None:
    if " since " not in units:
        return None
    return pd.Timestamp(units.split(" since ", 1)[1].strip().rstrip("Z"))


#: HDF5 bookkeeping attributes h5netcdf shows on variables.
_HDF_ATTRS = (
    "DIMENSION_LIST",
    "REFERENCE_LIST",
    "CLASS",
    "NAME",
    "_Netcdf4Dimid",
    "_Netcdf4Coordinates",
    "_nc3_strict",
)


def _plain(value):
    """An HDF5 attribute as a plain value: decoded bytes, a scalar for a one-element array."""
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    if isinstance(value, np.ndarray) and value.size == 1 and value.dtype.kind != "S":
        return value.reshape(()).item() if value.dtype.kind in "iuf" else value
    if isinstance(value, np.ndarray) and value.dtype.kind == "S":
        return [v.decode("utf-8", "replace") for v in value.ravel()]
    return value


def read_file(body: bytes, key: str) -> dict:
    """One file as ``{name: (dims, values, attrs)}``, ragged dimensions renamed.

    Values are the file's raw (packed) values. A list variable timed from the file's own
    start gets units of seconds, and that start is returned as ``time_offset_reference``.
    """
    import h5netcdf

    # h5netcdf directly, not through xarray: a tenth of the time per file, which decides
    # how long 27 million lightning files take.
    with h5netcdf.File(io.BytesIO(body), "r") as f:
        variables = {
            name: (
                tuple(RAGGED.get(d, d) for d in var.dimensions),
                np.asarray(var[...]),
                {k: _plain(v) for k, v in var.attrs.items() if k not in _HDF_ATTRS},
            )
            for name, var in f.variables.items()
        }
        global_attrs = {k: _plain(v) for k, v in f.attrs.items()}
    out: dict[str, tuple] = {}
    reference = None
    for name, (dims, values, attrs) in variables.items():
        if values.dtype.kind == "S":
            values = values.astype(object)
        since = _reference(str(attrs.get("units", "")))
        if since is not None and since != J2000:
            if reference is not None and since != reference:
                raise ValueError(f"{key}: time offsets count from {reference} and {since}")
            reference = since
            attrs["units"] = "s"
            attrs["comment"] = "seconds from the start of this file: time_offset_reference"
        if not (dims and dims[0] in RAGGED.values()):
            dims = (FILE_DIM, *dims)
            values = values[np.newaxis]
        out[RENAMES.get(name, name)] = (dims, values, attrs)
    out["file_name"] = ((FILE_DIM,), np.array([key.rsplit("/", 1)[-1]], dtype=object), {})
    for attr in FILE_ATTRS:
        value = global_attrs.get(attr, "")
        out[attr] = ((FILE_DIM,), np.array([str(value)], dtype=object), {})
    if reference is not None:
        out["time_offset_reference"] = (
            (FILE_DIM,),
            np.array([reference.to_datetime64()], dtype="datetime64[ns]"),
            {"long_name": "start of the file; list times are offsets from it"},
        )
    return out


def _rows(parsed: dict, dim: str) -> int:
    for dims, values, _ in parsed.values():
        if dims and dims[0] == dim:
            return values.shape[0]
    return 1 if dim == FILE_DIM else 0


def combine(files: Sequence[dict], first_file: int) -> xr.Dataset:
    """Files read by :func:`read_file` as one dataset, rows end to end.

    Each list row gets ``<dim>_file_index``, numbered on from ``first_file``. A variable
    some files lack is filled for theirs; fixed dimensions are padded to the widest.

    Raises:
        PackingChanged: The files' packing of one variable differs.
    """
    names: dict[str, tuple] = {}
    for parsed in files:
        for name, (dims, values, attrs) in parsed.items():
            names.setdefault(name, (dims, attrs, values.dtype))
    for parsed in files:
        for name, (_, _, attrs) in parsed.items():
            first = names[name][1]
            if not all(_same(attrs.get(k), first.get(k)) for k in PACKING):
                raise PackingChanged(f"{name}: packing differs between files of one partition")
    ragged = sorted({d[0] for d, _, _ in names.values() if d and d[0] in RAGGED.values()})
    variables = {}
    for name, (dims, attrs, dtype) in names.items():
        lead = dims[0]
        trailing = [
            max(p[name][1].shape[i] for p in files if name in p) for i in range(1, len(dims))
        ]
        fill = multidim.pad_value(attrs, np.dtype(dtype))
        parts = []
        for parsed in files:
            if name in parsed:
                values = parsed[name][1]
                widths = [(0, 0)] + [(0, t - s) for t, s in zip(trailing, values.shape[1:])]
                if any(w[1] for w in widths):
                    values = np.pad(
                        values.astype(object) if values.dtype.kind in "OUS" else values,
                        widths,
                        constant_values=fill,
                    )
            else:
                values = np.full((_rows(parsed, lead), *trailing), fill, dtype=dtype)
            parts.append(values)
        variables[name] = (dims, np.concatenate(parts), attrs)
    for dim in ragged:
        index = np.concatenate(
            [np.full(_rows(p, dim), first_file + i, dtype="int32") for i, p in enumerate(files)]
        )
        variables[f"{dim}_file_index"] = (
            (dim,),
            index,
            {"long_name": f"index into {FILE_DIM} of this {dim}'s file"},
        )
    return xr.Dataset({n: xr.Variable(d, v, a) for n, (d, v, a) in variables.items()})


# ----------------------------------------------------------------------------- store


def ensure_root(repo: icechunk.Repository, satellite: int) -> None:
    """Create the root group once, before any product group, so group writers never meet."""
    attrs = {k: v.replace("{NN}", str(satellite)) for k, v in ROOT_ATTRS.items()}
    for attempt in range(1, 6):
        try:
            root = zarr.open_group(repo.readonly_session("main").store, mode="r")
            if dict(root.attrs) == attrs:
                return
        except (zarr.errors.GroupNotFoundError, FileNotFoundError):
            pass
        session = repo.writable_session("main")
        zarr.open_group(session.store, mode="a").attrs.update(attrs)
        try:
            session.commit("goes l2 points: root group", rebase_with=icechunk.ConflictDetector())
            return
        except (icechunk.ConflictError, icechunk.RebaseFailedError):
            if attempt == 5:
                raise
            time.sleep(attempt)


def stored_files(repo: icechunk.Repository, group: str) -> int:
    """How many files a group holds; 0 before its first write."""
    try:
        node = zarr.open_group(repo.readonly_session("main").store, path=group, mode="r")
        return int(node["file_name"].shape[0]) if "file_name" in node else 0
    except (zarr.errors.GroupNotFoundError, FileNotFoundError, KeyError):
        return 0


def stored_packing(repo: icechunk.Repository, group: str) -> dict[str, dict]:
    try:
        node = zarr.open_group(repo.readonly_session("main").store, path=group, mode="r")
    except (zarr.errors.GroupNotFoundError, FileNotFoundError):
        return {}
    return {name: {k: a.attrs.get(k) for k in PACKING} for name, a in node.arrays()}


def check_packing(ds: xr.Dataset, stored: dict[str, dict], group: str) -> None:
    """Packed values only append where they are packed as the group stores them."""
    for name, var in ds.variables.items():
        # Times and strings are encoded by the writer, not carried packed.
        if name not in stored or var.dtype.kind in "MmOUST":
            continue
        for key in PACKING:
            new, old = var.attrs.get(key), stored[name].get(key)
            if not _same(new, old):
                raise PackingChanged(f"{group}/{name}: {key} {old!r} stored, {new!r} incoming")


#: Processes that parse files: HDF5 reading holds the GIL, so threads do not help (180
#: ms a lightning file on 8 threads, 20 ms on 8 processes).
PARSE_WORKERS = 8
_PARSER = None
_PARSER_LOCK = threading.Lock()


def _parser():
    global _PARSER
    with _PARSER_LOCK:
        if _PARSER is None:
            from concurrent.futures import ProcessPoolExecutor

            _PARSER = ProcessPoolExecutor(PARSE_WORKERS)
        return _PARSER


def _discard_parser(pool) -> None:
    """Drop ``pool`` once one of its workers has died (the OOM killer): a broken pool
    fails every later call, so without this a lane wrote nothing until restarted. Only
    the pool that broke is dropped, so threads sharing it make one replacement."""
    global _PARSER
    with _PARSER_LOCK:
        if _PARSER is pool:
            _PARSER = None
    pool.shutdown(wait=False, cancel_futures=True)


def _parse(bodies: Sequence[tuple[str, bytes]]) -> list:
    """:func:`_read_safely` over ``bodies``, in worker processes for more than a few.

    A pool that breaks part way is replaced and the partition parsed again, once.
    """
    if len(bodies) <= 8:
        return [_read_safely(item) for item in bodies]
    for attempt in (1, 2):
        pool = _parser()
        try:
            return list(pool.map(_read_safely, bodies, chunksize=8))
        except BrokenProcessPool:
            _discard_parser(pool)
            if attempt == 2:
                raise
            logger.warning("goes points: a parser process died; parsing again on a new pool")
    raise AssertionError("unreachable")


def _read_safely(item: tuple[str, bytes]) -> dict | str:
    """:func:`read_file`, or why it failed (an unreadable file is left out, not fatal)."""
    key, body = item
    try:
        return read_file(body, key)
    except Exception as exc:  # noqa: BLE001 - reported by the caller
        return f"{type(exc).__name__}: {exc}"


def write_partition(
    repo: icechunk.Repository, group: str, partition: str, bodies: Sequence[tuple[str, bytes]]
) -> int:
    """Copy one partition's files into ``group``; one commit. Returns files written."""
    parsed = []
    for key, result in zip((k for k, _ in bodies), _parse(bodies)):
        if isinstance(result, str):
            logger.warning(f"{group}: {key}: {result}")
        else:
            parsed.append(result)
    if not parsed:
        multidim.mark_written(repo, partition, label=group, group=group)
        return 0
    ds = combine(parsed, stored_files(repo, group))
    check_packing(ds, stored_packing(repo, group), group)
    fixed = [d for d in ds.dims if d != FILE_DIM and d not in RAGGED.values()]
    multidim.append_partition(repo, ds, partition, fixed_dims=fixed, label=group, group=group)
    return len(parsed)


# ----------------------------------------------------------------------------- lane


def available_days(store, prefix: str) -> list[dt.date]:
    days = []
    for year in _children(store, f"{prefix}/"):
        if not year.isdigit() or int(year) < 2017:
            continue
        for doy in _children(store, f"{prefix}/{year}/"):
            if doy.isdigit():
                days.append(dt.date(int(year), 1, 1) + dt.timedelta(int(doy) - 1))
    return days


def day_keys(store, prefix: str, day: dt.date) -> list[str]:
    import obstore

    keys = []
    for batch in obstore.list(store, prefix=f"{prefix}/{day:%Y}/{day.timetuple().tm_yday:03d}/"):
        keys += [o["path"] for o in batch]
    return sorted(keys)


def fetch(store, keys: Sequence[str]) -> list[tuple[str, bytes]]:
    import obstore

    def get(key):
        return key, bytes(obstore.get(store, key).bytes())

    with ThreadPoolExecutor(DOWNLOAD_WORKERS) as pool:
        return list(pool.map(get, keys))


def run_prefix(
    satellite: int,
    prefix: str,
    config: Config,
    start: dt.date | None = None,
    end: dt.date | None = None,
    now: dt.datetime | None = None,
    store=None,
) -> int:
    """Write every settled partition of one source prefix not yet stored. Returns files."""
    now = now or dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    last = (now - SETTLE).date() - dt.timedelta(days=1)
    store = store or source_store(SATELLITES[satellite])
    family = "GLM" if prefix.startswith("GLM") else prefix[len("ABI-L2-") :].rstrip("FCM")
    repo = config.icechunk_repo(store_prefix(satellite))
    ensure_root(repo, satellite)
    days = [d for d in available_days(store, prefix) if d <= last]
    days = [d for d in days if (start is None or d >= start) and (end is None or d <= end)]
    total = 0
    ledgers: dict[str, set[str]] = {}
    for day in order_days(days, last, RECENT_DAYS):
        keys = [k for k in day_keys(store, prefix, day) if group_of(k)]
        groups: dict[tuple[str, str], list[str]] = {}
        for key in keys:
            groups.setdefault((group_of(key), partition_of(key, day, family)), []).append(key)
        for (group, partition), members in sorted(groups.items()):
            if group not in ledgers:
                ledgers[group] = multidim.ledger(repo, group=group)
            if partition in ledgers[group]:
                continue
            try:
                total += write_partition(repo, group, partition, fetch(store, members))
                ledgers[group].add(partition)
                logger.info(f"goes{satellite} {group} {partition}: {len(members)} file(s)")
            except Exception as exc:  # noqa: BLE001 - one bad partition must not stop the rest
                logger.exception(
                    f"goes{satellite} {group} {partition}: {type(exc).__name__}: {exc}"
                )
    return total


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--satellite", type=int, required=True, choices=sorted(SATELLITES))
    parser.add_argument("--products", default=",".join(FAMILIES), help="DMW,DMWV,GLM")
    parser.add_argument("--start", type=dt.date.fromisoformat)
    parser.add_argument("--end", type=dt.date.fromisoformat)
    parser.add_argument("--workers", type=int, default=3, help="source prefixes at once")
    parser.add_argument("--follow", action="store_true", help="keep passing, hourly")
    args = parser.parse_args(argv)
    config = get_config()
    store = source_store(SATELLITES[args.satellite])
    top = set(_children(store, ""))
    prefixes = [p for f in args.products.split(",") for p in FAMILIES[f] if p in top]
    while True:
        logger.info(f"goes{args.satellite} points: {prefixes}")
        with ThreadPoolExecutor(args.workers) as pool:
            written = sum(
                pool.map(
                    lambda p: run_prefix(args.satellite, p, config, args.start, args.end),
                    prefixes,
                )
            )
        logger.info(f"goes{args.satellite} points: pass done, {written:,} file(s) copied")
        if not args.follow:
            return 0
        for _ in range(6):  # an hour, saying so every ten minutes for the box's stall guard
            time.sleep(600)
            logger.info(f"goes{args.satellite} points: waiting for the next pass")


if __name__ == "__main__":
    sys.exit(main())
