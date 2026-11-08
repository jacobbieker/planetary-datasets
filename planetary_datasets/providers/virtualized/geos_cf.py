r"""Virtual Icechunk stores of the NASA GEOS-CF archive on the NCCS data portal.

GEOS-CF (Composition Forecast) is GMAO's global atmospheric composition model: meteorology,
GEOS-Chem chemistry, aerosols and emissions at 0.25 degrees, run as an analysis (replay)
and as daily five-day forecasts. Its NetCDF4 files are referenced, not copied: each store
holds virtual chunk references into ``https://portal.nccs.nasa.gov/datashare/gmao/geos-cf``
plus the small coordinates, one store per collection and stream, next to the materialised
15-minute subsets (``bkr/geos/geos_15min.icechunk``, ``bkr/geos/geos_v2_15min.icechunk``)::

    bkr/geos/geos_v2_ana_chm_inst_1hr_v72.icechunk      v2 analysis, 72 model levels
    bkr/geos/geos_v2_ana_met_tavg_1hr_slv.icechunk      v2 analysis, single level
    bkr/geos/geos_v2_fcst_htf_inst_15mn_slv.icechunk    v2 forecasts, 09Z runs
    bkr/geos/geos_v2_hindcast_aqc_tavg_1hr_slv.icechunk v2 hindcast, 2023-01 to 2025-08
    bkr/geos/geos_v1_ana_xgc_tavg_1hr_x1.icechunk       v1 analysis, 2018 to 2026-01
    bkr/geos/geos_v1_fcst_aqc_tavg_1hr_v1.icechunk      v1 forecasts, 12Z runs

The streams (:data:`STREAMS`): v2 analysis and forecasts are live and followed; the v2
hindcast and all of v1 are finished archives, referenced once. v1's 3-D collections (model
and pressure levels, about 200 GB a day) are left out: their small HDF5 chunks would take
minutes of range requests per file, or a download of the whole archive, to reference.

Layout, ARCO-ERA5 style:

* analysis stores are ``(time, [model_level | pressure_level], latitude, longitude)``,
  appended along ``time``, one commit per day;
* forecast stores are ``(init_time, step, [level], latitude, longitude)`` with a
  ``valid_time`` coordinate, appended along ``init_time``, one commit per run. A step the
  portal never published reads as missing (NaN), so every run has the full step axis;
* single-level collections drop GEOS's length-one ``lev`` and keep its value as a scalar
  ``model_level`` coordinate (the lowest model layer, 72);
* variables are named by :mod:`.geos_cf_names` (``NO2`` is
  ``nitrogen_dioxide_mole_fraction``), each keeping its GEOS name as ``source_name``.

Times come from the file names, which state them exactly (``..._20261101_0030z``); a
time-averaged field is stamped at the middle of its hour, as GEOS-CF stamps it. Each store's
root attributes carry a ledger of the days or runs written, so a run resumes where it
stopped, and a layout signature: a collection whose variables, grid or chunking change
continues in a generation named for the day it changed (``..._v72_01092026.icechunk``).

    python -m planetary_datasets.providers.virtualized.geos_cf --streams v2_ana,v2_fcst --follow
    python -m planetary_datasets.providers.virtualized.geos_cf --streams v2_hindcast,v1_ana,v1_fcst
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import hashlib
import html
import json
import pathlib
import re
import sys
import tempfile
import time
import urllib.parse
import warnings
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable, Sequence

import numpy as np
import pandas as pd
from loguru import logger

from planetary_datasets.config import Config, get_config
from planetary_datasets.providers.virtualized.geos_cf_names import variable_names

HOST = "https://portal.nccs.nasa.gov"
PORTAL = f"{HOST}/datashare/gmao/geos-cf"
#: The virtual chunk container every reference points into.
CONTAINER = f"{HOST}/"
STORE_ROOT = "bkr/geos"
#: Root attribute listing the days (analysis) or runs (forecasts) a store holds.
LEDGER_ATTR = "geos_cf_written"
#: Root attribute fixing a store's layout; a different layout goes to a new generation.
SIGNATURE_ATTR = "geos_cf_layout"
#: A day or run younger than this is written only once complete; older, with what exists.
SETTLE = dt.timedelta(days=3)
#: Forecast length of every GEOS-CF run.
FORECAST_LENGTH = pd.Timedelta(hours=120)
#: Level codes of the single-level collections; the rest are on model or pressure levels.
SINGLE_LEVEL = frozenset({"slv", "x1", "v1"})
#: Small variables read into the store rather than referenced.
LOADABLE = ("time", "lat", "lon", "lev")

TIME_ENCODING = {
    "units": "seconds since 1970-01-01",
    "calendar": "proleptic_gregorian",
    "dtype": "int64",
}
STEP_ENCODING = {"units": "minutes", "dtype": "int64"}

_FILE = re.compile(
    r"^(?:GEOS-CF\.v01\.(?:rpl|fcst)|GEOS\.cf\.(?:ana|fcst)|CF2_hindcast)\."
    r"(?P<collection>[a-z]+_[a-z]+_[0-9a-z]+_[A-Za-z0-9_]+?)\."
    r"(?P<first>\d{8}_\d{2}(?:\d{2})?)z(?:\+(?P<valid>\d{8}_\d{4})z)?"
    r"(?:\.R(?P<revision>\d+))?\.nc4$"
)
_HREF = re.compile(r'href="([^"?#]+)"')


@dataclasses.dataclass(frozen=True)
class Stream:
    """One run of GEOS-CF on the portal: a version's analysis, forecasts or hindcast.

    Attributes:
        name: Store-name part, e.g. ``v2_ana``.
        path: Portal path under :data:`PORTAL`, e.g. ``v2/ana``.
        forecast: Files are forecast steps of daily runs, not analysis times.
        run_dir: Subdirectory a day's runs sit in (``H12`` for v1 forecasts).
        single_level_only: Leave out the 3-D collections.
        download: Read each file's metadata from a local copy rather than over HTTP. v1's
            small chunks make an HTTP read thousands of requests long.
    """

    name: str
    path: str
    forecast: bool = False
    run_dir: str = ""
    single_level_only: bool = False
    download: bool = False


STREAMS: dict[str, Stream] = {
    "v1_ana": Stream("v1_ana", "v1/ana", single_level_only=True, download=True),
    "v1_fcst": Stream(
        "v1_fcst",
        "v1/forecast",
        forecast=True,
        run_dir="H12",
        single_level_only=True,
        download=True,
    ),
    "v2_ana": Stream("v2_ana", "v2/ana"),
    "v2_fcst": Stream("v2_fcst", "v2/fcst", forecast=True),
    "v2_hindcast": Stream("v2_hindcast", "v2_hindcast/ana"),
}


@dataclasses.dataclass(frozen=True)
class Collection:
    """A GEOS-CF collection, e.g. ``chm_inst_1hr_glo_L1440x721_v72``, within a stream."""

    stream: Stream
    name: str

    @property
    def parts(self) -> list[str]:
        """The name's underscore-separated parts."""
        return self.name.split("_")

    @property
    def averaged(self) -> bool:
        """Time-averaged (``tavg``), stamped mid-interval, rather than instantaneous."""
        return self.parts[1] == "tavg"

    @property
    def levels(self) -> str:
        """The level code: ``slv``, ``x1``, ``v1`` (single), ``v72``, ``v36``, ``p23``..."""
        return self.parts[-1]

    @property
    def single_level(self) -> bool:
        """One level only (``slv``, ``x1``, ``v1``)."""
        return self.levels in SINGLE_LEVEL

    @property
    def level_dim(self) -> str | None:
        """ERA5's name for the vertical dimension, or None for a single-level collection."""
        if self.single_level:
            return None
        return "pressure_level" if self.levels.startswith("p") else "model_level"

    @property
    def interval(self) -> pd.Timedelta:
        """Time between files: 15 minutes, 1 hour or 3 hours."""
        code = self.parts[2]
        return {
            "15mn": pd.Timedelta(minutes=15),
            "1hr": pd.Timedelta(hours=1),
            "3hr": pd.Timedelta(hours=3),
        }[code]

    @property
    def per_day(self) -> int:
        """Files in a complete analysis day."""
        return int(pd.Timedelta(days=1) / self.interval)

    @property
    def steps(self) -> pd.TimedeltaIndex:
        """A run's lead times: mid-interval for averages, interval ends for instants."""
        n = int(FORECAST_LENGTH / self.interval)
        first = self.interval / 2 if self.averaged else self.interval
        return pd.timedelta_range(first, periods=n, freq=self.interval)

    @property
    def short(self) -> str:
        """``chm_inst_1hr_v72``; the coarse v1 ``sat`` grid is marked as such."""
        p = self.parts
        grid = "_0p5deg" if "721x361" in self.name else ""
        return f"{p[0]}_{p[1]}_{p[2]}_{self.levels}{grid}"

    @property
    def store_prefix(self) -> str:
        """Where the collection's store lives, next to the 15-minute subsets."""
        return f"{STORE_ROOT}/geos_{self.stream.name}_{self.short}.icechunk"

    @property
    def append_dim(self) -> str:
        """``init_time`` for forecasts, ``time`` for analyses."""
        return "init_time" if self.stream.forecast else "time"

    @property
    def label(self) -> str:
        """Name used in logs and commit messages."""
        return f"geos_{self.stream.name}_{self.short}"


@dataclasses.dataclass(frozen=True)
class SourceFile:
    """One file on the portal."""

    url: str
    collection: str
    valid: pd.Timestamp
    init: pd.Timestamp | None
    revision: int


def parse_name(name: str, directory: str) -> SourceFile | None:
    """A portal file name as a :class:`SourceFile`, or None for anything else (constants)."""
    m = _FILE.match(urllib.parse.unquote(name))
    if not m:
        return None
    first = m["first"]
    fmt = "%Y%m%d_%H%M" if len(first) == 13 else "%Y%m%d_%H"
    first_time = pd.Timestamp(dt.datetime.strptime(first, fmt))
    if m["valid"]:
        init, valid = first_time, pd.Timestamp(dt.datetime.strptime(m["valid"], "%Y%m%d_%H%M"))
    else:
        init, valid = None, first_time
    revision = int(m["revision"]) if m["revision"] is not None else -1
    url = directory.rstrip("/") + "/" + urllib.parse.unquote(name)
    return SourceFile(url, m["collection"], valid, init, revision)


def latest_revisions(files: Iterable[SourceFile]) -> list[SourceFile]:
    """One file per collection and time: the highest revision (``R1`` over ``R0`` over none)."""
    best: dict[tuple, SourceFile] = {}
    for f in files:
        key = (f.collection, f.init, f.valid)
        if key not in best or f.revision > best[key].revision:
            best[key] = f
    return sorted(best.values(), key=lambda f: (f.collection, f.init or f.valid, f.valid))


# ----------------------------------------------------------------------------- portal


class Portal:
    """Directory listings on the NCCS portal, or on a local ``file://`` mirror in tests.

    Attributes:
        root: URL of the GEOS-CF tree.
        container: URL prefix of the virtual chunk container the references point into.
    """

    def __init__(self, root: str = PORTAL, retries: int = 4):
        """List ``root`` (a URL), retrying each listing ``retries`` times."""
        self.root = root.rstrip("/")
        self.retries = retries
        parts = urllib.parse.urlsplit(self.root)
        self.container = (
            f"{self.root}/" if parts.scheme == "file" else f"{parts.scheme}://{parts.netloc}/"
        )

    def list(self, path: str) -> list[str]:
        """Entry names in the directory ``path`` (relative to the root); ``[]`` if absent.

        Subdirectories end in ``/``, as the portal's index pages write them.
        """
        import requests

        url = f"{self.root}/{path.strip('/')}/"
        if url.startswith("file://"):
            local = pathlib.Path(url.removeprefix("file://"))
            if not local.is_dir():
                return []
            return sorted(e.name + ("/" if e.is_dir() else "") for e in local.iterdir())
        for attempt in range(self.retries):
            try:
                response = requests.get(url, timeout=120)
                if response.status_code == 404:
                    return []
                response.raise_for_status()
                break
            except requests.RequestException as exc:
                if attempt == self.retries - 1:
                    raise
                logger.debug(f"listing {url}: {exc}; retrying")
                time.sleep(2**attempt * 5)
        names = []
        for href in _HREF.findall(response.text):
            href = html.unescape(href)
            if href.startswith(("/", "..", "mailto:")) or "://" in href:
                continue
            names.append(href)
        return names

    def directory(self, path: str) -> str:
        """URL of the directory ``path`` (relative to the root)."""
        return f"{self.root}/{path.strip('/')}"


def day_dirs(portal: Portal, stream: Stream) -> list[dt.date]:
    """Every day the stream has a directory for."""
    days = []
    for year in portal.list(stream.path):
        if not re.fullmatch(r"Y\d{4}/", year):
            continue
        for month in portal.list(f"{stream.path}/{year}"):
            if not re.fullmatch(r"M\d{2}/", month):
                continue
            for day in portal.list(f"{stream.path}/{year}{month}"):
                if re.fullmatch(r"D\d{2}/", day):
                    days.append(dt.date(int(year[1:5]), int(month[1:3]), int(day[1:3])))
    return sorted(days)


def day_files(portal: Portal, stream: Stream, day: dt.date) -> list[SourceFile]:
    """The files of one day directory (its run subdirectory, for v1 forecasts)."""
    path = f"{stream.path}/Y{day:%Y}/M{day:%m}/D{day:%d}"
    if stream.run_dir:
        path = f"{path}/{stream.run_dir}"
    directory = portal.directory(path)
    parsed = (parse_name(name, directory) for name in portal.list(path))
    return latest_revisions(f for f in parsed if f is not None)


# ----------------------------------------------------------------------------- virtual


class Opener:
    """Opens portal files as virtual datasets, over HTTP or from a downloaded copy."""

    def __init__(self, stream: Stream, portal: Portal, scratch: pathlib.Path | None = None):
        """Read ``portal``'s files for ``stream``; downloads, if any, go under ``scratch``."""
        import virtualizarr as vz
        from obspec_utils.registry import ObjectStoreRegistry
        from obstore.store import HTTPStore, LocalStore

        self.stream = stream
        self.parser = vz.parsers.HDFParser()
        root = urllib.parse.urlsplit(portal.root)
        self.local = ObjectStoreRegistry({"file://": LocalStore("/")})
        if root.scheme == "file":
            self.registry = self.local
        else:
            base = f"{root.scheme}://{root.netloc}"
            self.registry = ObjectStoreRegistry({base: HTTPStore.from_url(base)})
        self.scratch = scratch

    def open(self, url: str):
        """The file at ``url`` as a virtual dataset whose references point at ``url``."""
        import virtualizarr as vz

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            if not self.stream.download or url.startswith("file://"):
                ds = vz.open_virtual_dataset(
                    url,
                    registry=self.registry,
                    parser=self.parser,
                    loadable_variables=list(LOADABLE),
                )
                return _load_coordinates(ds)
            from planetary_datasets.common import download_one

            with tempfile.TemporaryDirectory(dir=self.scratch, prefix="geos-cf-") as tmp:
                path = download_one(url, pathlib.Path(tmp) / url.rsplit("/", 1)[-1], retries=4)
                if path is None:
                    raise OSError(f"could not download {url}")
                ds = vz.open_virtual_dataset(
                    f"file://{pathlib.Path(path).resolve()}",
                    registry=self.local,
                    parser=self.parser,
                    loadable_variables=list(LOADABLE),
                )
                # Read before the copy is deleted; the references then point at the portal.
                return _load_coordinates(ds).vz.rename_paths(lambda _old: url)


def _load_coordinates(ds):
    """``ds`` with every non-virtual variable read into memory."""
    for var in ds.variables.values():
        if not _is_virtual(var):
            var.load()
    return ds


def _is_virtual(var) -> bool:
    from virtualizarr.manifests import ManifestArray

    return isinstance(var.data, ManifestArray)


def shape(ds, collection: Collection, f: SourceFile, names: dict[str, str]):
    """One opened file in the store's layout: renamed, single level squeezed, time stamped."""
    ds = ds.drop_vars([v for v in ("time",) if v in ds.variables])
    if "time" in ds.dims:
        ds = ds.isel(time=0)
    if "lev" in ds.dims and collection.single_level:
        ds = ds.isel(lev=0)
    renames = {"lat": "latitude", "lon": "longitude"}
    if "lev" in ds.variables:
        renames["lev"] = collection.level_dim or "model_level"
    ds = ds.rename({k: v for k, v in renames.items() if k in ds.variables or k in ds.dims})
    for code, name in names.items():
        if code in ds.data_vars:
            ds[code].attrs["source_name"] = code
    ds = ds.rename_vars({c: n for c, n in names.items() if c in ds.data_vars})
    if collection.stream.forecast:
        step = pd.Timedelta(f.valid - f.init)
        ds = ds.expand_dims(step=[step.to_timedelta64()])
    else:
        ds = ds.expand_dims(time=[f.valid.to_datetime64()])
    return ds


def empty_like(ds, step: pd.Timedelta):
    """``ds`` (one step) as a step the portal never published: every reference dropped."""
    out = ds.assign_coords(step=[step.to_timedelta64()])
    for name, var in out.data_vars.items():
        if _is_virtual(var):
            out[name] = var.copy(data=var.data.with_fill_value_only(var.data.metadata.fill_value))
        elif np.issubdtype(var.dtype, np.floating):
            out[name] = var.copy(data=np.full(var.shape, np.nan, var.dtype))
    return out


def _concat(parts, dim: str):
    import xarray as xr

    return xr.concat(
        parts,
        dim=dim,
        data_vars="minimal",
        coords="minimal",
        compat="override",
        join="override",
        combine_attrs="drop_conflicts",
    )


def layout_signature(vds, collection: Collection) -> str:
    """What must match to append: variables, their chunks, codecs, dtype and the grid."""
    parts = []
    for name in sorted(vds.data_vars):
        var = vds[name]
        if _is_virtual(var):
            meta = var.data.metadata
            parts.append(
                (name, var.dims, tuple(meta.chunks), str(meta.codecs), str(meta.data_type))
            )
        else:
            parts.append((name, var.dims, str(var.dtype)))
    for axis in ("latitude", "longitude", collection.level_dim or "model_level"):
        if axis in vds.variables:
            values = np.asarray(vds[axis].values)
            parts.append((axis, hashlib.sha256(values.tobytes()).hexdigest()))
    return hashlib.sha256(json.dumps(parts, default=str).encode()).hexdigest()[:16]


#: The opener of a worker process, set by :func:`_init_worker`.
_WORKER_OPENER: Opener | None = None


def _init_worker(stream_name: str, portal_root: str, scratch: str | None) -> None:
    """Build the worker process's opener (run once per worker by the pool)."""
    global _WORKER_OPENER
    _WORKER_OPENER = Opener(
        STREAMS[stream_name], Portal(portal_root), pathlib.Path(scratch) if scratch else None
    )


def _open_url(url: str, opener: Opener | None = None):
    """``(url, dataset, None)``, or ``(url, None, error)`` if the file would not open."""
    try:
        return url, (opener or _WORKER_OPENER).open(url), None
    except Exception as exc:  # noqa: BLE001 - reported by the caller, per settling rule
        return url, None, f"{type(exc).__name__}: {exc}"


def open_pool(stream: Stream, portal: Portal, scratch: pathlib.Path | None, workers: int):
    """Worker processes that open files, or None to open them in this process.

    Processes, not threads: h5py holds one global lock through every call, so threads
    parsing HDF5 metadata run one at a time, and a v2 day took most of an hour.
    """
    if workers <= 0:
        return None
    import multiprocessing
    from concurrent.futures import ProcessPoolExecutor

    return ProcessPoolExecutor(
        workers,
        mp_context=multiprocessing.get_context("spawn"),
        initializer=_init_worker,
        initargs=(stream.name, portal.root, str(scratch) if scratch else None),
    )


def open_unit(
    opener: Opener,
    collection: Collection,
    files: Sequence[SourceFile],
    settled: bool,
    pool=None,
):
    """The files of one day (analysis) or run (forecast) combined in the store's layout.

    Files are opened in ``pool`` (see :func:`open_pool`), or by ``opener`` in this process
    without one. Returns ``(dataset, files left out)``; dataset is None if nothing could be
    opened. A file that will not open fails the unit while it is recent (the next pass
    retries it) and is left out once the unit has settled.
    """
    urls = [f.url for f in files]
    if pool is None:
        results = [_open_url(url, opener) for url in urls]
    else:
        results = list(pool.map(_open_url, urls))
    errors = {url: error for url, _, error in results if error}
    by_url = {url: ds for url, ds, _ in results if ds is not None}
    opened = [(f, by_url[f.url]) for f in files if f.url in by_url]
    if errors and not settled:
        first = next(iter(errors.items()))
        raise RuntimeError(
            f"{collection.label}: {len(errors)} file(s) would not open, e.g. {first}"
        )
    for url, error in errors.items():
        logger.warning(f"{collection.label}: {url}: {error}; left out")
    if not opened:
        return None, len(errors)
    codes: dict[str, dict] = {}
    for _, ds in opened:
        for n, v in ds.data_vars.items():
            codes.setdefault(n, dict(v.attrs))
    names = variable_names(codes)
    shaped = [(f, shape(ds, collection, f, names)) for f, ds in opened]
    if not collection.stream.forecast:
        shaped.sort(key=lambda item: item[0].valid)
        vds = _concat([ds for _, ds in shaped], "time")
        vds["time"].encoding.update(TIME_ENCODING)
    else:
        by_step = {pd.Timedelta(f.valid - f.init): ds for f, ds in shaped}
        template = shaped[0][1]
        steps = collection.steps
        unexpected = sorted(set(by_step) - set(steps))
        if unexpected:
            raise ValueError(f"{collection.label}: steps {unexpected} outside the run's grid")
        parts = [by_step.get(step, None) for step in steps]
        missing = sum(p is None for p in parts)
        parts = [
            p if p is not None else empty_like(template, step) for p, step in zip(parts, steps)
        ]
        vds = _concat(parts, "step")
        init = shaped[0][0].init
        vds = vds.expand_dims(init_time=[init.to_datetime64()])
        valid = np.datetime64(init.to_datetime64()) + vds["step"].values
        vds = vds.assign_coords(valid_time=(("init_time", "step"), valid[np.newaxis, :]))
        vds["init_time"].encoding.update(TIME_ENCODING)
        vds["valid_time"].encoding.update(TIME_ENCODING)
        vds["step"].encoding.update(STEP_ENCODING)
        if missing:
            logger.info(f"{collection.label} {init}: {missing} step(s) not published; left missing")
    vds.attrs["geos_cf_variable_source_names"] = json.dumps({n: c for c, n in names.items()})
    return vds, len(errors)


# ----------------------------------------------------------------------------- stores


def open_store(
    prefix: str,
    collection: Collection,
    config: Config,
    create: bool = True,
    container: str = CONTAINER,
):
    """Open (or create) one store, its references read through ``container``."""
    from planetary_datasets.providers.virtualized.virtual_repo import open_virtual_repo

    split = 1 if collection.stream.forecast else collection.per_day
    return open_virtual_repo(
        prefix,
        virtual_buckets=[container],
        split_size=split,
        split_dim=collection.append_dim,
        config=config,
        create=create,
    )


def _root_attrs(repo) -> dict:
    """The store's root attributes; empty only for a store with no root group yet.

    Any other failure propagates: read as "empty", a ledger that failed to load would
    write its days again, and a signature that failed to load would pick a generation.
    """
    import zarr
    from zarr.errors import GroupNotFoundError

    try:
        return dict(zarr.open_group(repo.readonly_session("main").store, mode="r").attrs)
    except GroupNotFoundError:
        return {}


def stored_attrs(
    prefix: str, collection: Collection, config: Config, container: str = CONTAINER
) -> dict:
    """Root attributes of the store at ``prefix``; empty when there is none (never creates)."""
    import icechunk

    try:
        repo = open_store(prefix, collection, config, create=False, container=container)
    except (FileNotFoundError, icechunk.RepositoryNotFoundError):
        return {}
    return _root_attrs(repo)


def generations(config: Config, base: str) -> list[str]:
    """The base store and its dated generations that exist, base first.

    Listed, never opened: opening a name to see whether it exists would create it.
    """
    import fsspec

    parent, _, name = base.rpartition("/")
    stem = name.removesuffix(".icechunk")
    pattern = re.compile(rf"^{re.escape(stem)}_(?P<date>\d{{8}})(?:_\d+)?\.icechunk$")
    root = config.store_path(parent)
    try:
        if config.use_local_store:
            filesystem, path = fsspec.core.url_to_fs(root)
        else:
            filesystem, path = fsspec.core.url_to_fs(root, **config.fsspec_storage_options())
        filesystem.invalidate_cache(path)
        entries = [str(e).rstrip("/").rpartition("/")[2] for e in filesystem.ls(path, detail=False)]
    except FileNotFoundError:
        entries = []
    dated = sorted(
        (dt.datetime.strptime(m["date"], "%d%m%Y"), e) for e in entries if (m := pattern.match(e))
    )
    return [base] + [f"{parent}/{e}" for _, e in dated]


def written(collection: Collection, config: Config, container: str = CONTAINER) -> set[str]:
    """Days or runs in any generation's ledger."""
    done: set[str] = set()
    for prefix in generations(config, collection.store_prefix):
        done |= set(stored_attrs(prefix, collection, config, container).get(LEDGER_ATTR, []))
    return done


def store_for(
    collection: Collection,
    signature: str,
    when: dt.date,
    config: Config,
    container: str = CONTAINER,
) -> str:
    """The generation whose layout is ``signature``: the first empty or matching, else new."""
    from planetary_datasets.common.generations import prefix_for_date

    for prefix in generations(config, collection.store_prefix):
        attrs = stored_attrs(prefix, collection, config, container)
        if attrs.get(SIGNATURE_ATTR) in (None, signature):
            return prefix
    for disambiguator in range(10):
        prefix = prefix_for_date(collection.store_prefix, when, disambiguator)
        if stored_attrs(prefix, collection, config, container).get(SIGNATURE_ATTR) in (
            None,
            signature,
        ):
            logger.info(f"{collection.label}: layout changed on {when}; new generation {prefix}")
            return prefix
    raise RuntimeError(f"{collection.label}: no generation free for {when}")


def write_unit(
    collection: Collection,
    key: str,
    when: dt.date,
    vds,
    left_out: int,
    config: Config,
    container: str = CONTAINER,
) -> int:
    """Append one day or run to the matching generation, in one commit. Returns files written."""
    import icechunk
    import zarr

    signature = layout_signature(vds, collection)
    prefix = store_for(collection, signature, when, config, container)
    attrs = stored_attrs(prefix, collection, config, container)
    repo = open_store(prefix, collection, config, container=container)
    session = repo.writable_session("main")
    if attrs.get(SIGNATURE_ATTR):
        vds.vz.to_icechunk(session.store, append_dim=collection.append_dim)
    else:
        vds.vz.to_icechunk(session.store)
    root = zarr.open_group(session.store, mode="a")
    ledger = set(root.attrs.get(LEDGER_ATTR, [])) | {key}
    root.attrs.update(
        {
            SIGNATURE_ATTR: signature,
            LEDGER_ATTR: sorted(ledger),
            "source": f"{PORTAL}/{collection.stream.path}/",
            "collection": collection.name,
            "model": "NASA GMAO GEOS-CF",
        }
    )
    n = int(vds.sizes["step" if collection.stream.forecast else "time"])
    session.commit(
        f"{collection.label}: {key} ({n} file(s), {left_out} left out)",
        rebase_with=icechunk.ConflictDetector(),
    )
    logger.info(f"{collection.label} {key}: {n} file(s) -> {prefix} ({left_out} left out)")
    return n


# ----------------------------------------------------------------------------- passes


def units(collection: Collection, files: Sequence[SourceFile]) -> dict[str, list[SourceFile]]:
    """A collection's files by day (analysis) or run (forecast), keyed as in the ledger."""
    out: dict[str, list[SourceFile]] = {}
    for f in files:
        if f.collection != collection.name:
            continue
        key = f.init.isoformat() if collection.stream.forecast else f.valid.date().isoformat()
        out.setdefault(key, []).append(f)
    return out


def expected(collection: Collection) -> int:
    """Files in a complete day (analysis) or run (forecast)."""
    return len(collection.steps) if collection.stream.forecast else collection.per_day


def run_stream(
    stream: Stream,
    config: Config,
    portal: Portal,
    start: dt.date | None = None,
    end: dt.date | None = None,
    only: Sequence[str] | None = None,
    workers: int = 4,
    open_workers: int = 8,
    now: dt.datetime | None = None,
) -> int:
    """Write every settled, unwritten day or run of one stream. Returns files written."""
    now = now or dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    days = [
        d
        for d in day_dirs(portal, stream)
        if (start is None or d >= start) and (end is None or d <= end)
    ]
    if not days:
        logger.info(f"geos_{stream.name}: no days on the portal")
        return 0
    scratch = pathlib.Path(config.scratch_dir)
    opener = Opener(stream, portal, scratch=scratch)
    pool = open_pool(stream, portal, scratch, open_workers)
    total = 0
    try:
        ledgers: dict[str, set[str]] = {}
        #: Collections with a unit waiting (still published, or failed while recent).
        deferred: set[str] = set()
        total = 0

        def collection_of(name: str) -> Collection | None:
            c = Collection(stream, name)
            if stream.single_level_only and not c.single_level:
                return None
            if only and c.short not in only:
                return None
            return c

        for day in days:
            stamp = day.isoformat()
            if ledgers and all(
                any(k.startswith(stamp) for k in ledger) for ledger in ledgers.values()
            ):
                continue  # every collection seen so far has this day; nothing to list
            try:
                files = day_files(portal, stream, day)
            except Exception as exc:  # noqa: BLE001 - one unlistable day must not stop the stream
                logger.warning(f"geos_{stream.name} {day}: listing failed: {exc}")
                continue
            names = sorted({f.collection for f in files})
            collections = [c for c in map(collection_of, names) if c is not None]
            for c in collections:
                if c.name not in ledgers:
                    ledgers[c.name] = written(c, config, portal.container)

            def write_collection(c: Collection) -> int:
                if c.name in deferred:
                    return 0
                count = 0
                for key, unit_files in sorted(units(c, files).items()):
                    if key in ledgers[c.name]:
                        continue
                    when = dt.datetime.fromisoformat(key)
                    settled = now - when >= SETTLE + (
                        FORECAST_LENGTH.to_pytimedelta()
                        if stream.forecast
                        else dt.timedelta(days=1)
                    )
                    if len(unit_files) < expected(c) and not settled:
                        # Still being published. Later units wait behind it, so the store's
                        # times stay in order.
                        deferred.add(c.name)
                        break
                    try:
                        vds, left_out = open_unit(opener, c, unit_files, settled, pool)
                        if vds is None:
                            continue
                        count += write_unit(
                            c, key, when.date(), vds, left_out, config, portal.container
                        )
                        ledgers[c.name].add(key)
                    except Exception as exc:  # noqa: BLE001 - one bad unit must not stop the rest
                        logger.exception(f"{c.label} {key}: {type(exc).__name__}: {exc}")
                        if not settled:
                            # Retried next pass; later units wait behind it, as above.
                            deferred.add(c.name)
                            break
                return count

            with ThreadPoolExecutor(max(1, workers)) as threads:
                total += sum(threads.map(write_collection, collections))
    finally:
        if pool is not None:
            pool.shutdown()
    return total


def main(argv: Sequence[str] | None = None) -> int:
    """Command line: reference the chosen streams once, or keep following them."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--streams", default="v2_ana,v2_fcst", help=f"comma-separated: {', '.join(STREAMS)}"
    )
    parser.add_argument("--collections", help="comma-separated short names, e.g. chm_inst_1hr_v72")
    parser.add_argument("--start", type=dt.date.fromisoformat)
    parser.add_argument("--end", type=dt.date.fromisoformat)
    parser.add_argument("--workers", type=int, default=4, help="collections written at once")
    parser.add_argument(
        "--open-workers", type=int, default=8, help="processes opening files (0: none)"
    )
    parser.add_argument("--follow", action="store_true", help="keep passing")
    parser.add_argument(
        "--pause", type=int, default=3600, help="seconds between passes when following"
    )
    args = parser.parse_args(argv)
    config = get_config()
    portal = Portal()
    streams = [STREAMS[s] for s in args.streams.split(",")]
    only = args.collections.split(",") if args.collections else None
    while True:
        for stream in streams:
            try:
                written_files = run_stream(
                    stream,
                    config,
                    portal,
                    args.start,
                    args.end,
                    only,
                    args.workers,
                    args.open_workers,
                )
            except Exception as exc:  # noqa: BLE001 - following: retry at the next pass
                if not args.follow:
                    raise
                logger.exception(f"geos_{stream.name}: pass failed: {type(exc).__name__}: {exc}")
                continue
            logger.info(f"geos_{stream.name}: pass done, {written_files:,} file(s) referenced")
        if not args.follow:
            return 0
        waited = 0
        while waited < args.pause:  # saying so every ten minutes, for the box's stall guard
            time.sleep(min(600, args.pause - waited))
            waited += 600
            logger.info("geos_cf: waiting for the next pass")


if __name__ == "__main__":
    sys.exit(main())
