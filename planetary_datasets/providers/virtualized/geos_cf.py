r"""Virtual Icechunk stores of the NASA GEOS-CF archive on the NCCS data portal.

GEOS-CF (Composition Forecast) is GMAO's global atmospheric composition model: meteorology,
GEOS-Chem chemistry, aerosols and emissions at 0.25 degrees, run as an analysis (replay)
and as daily five-day forecasts. Its NetCDF4 files are referenced, not copied: each store
holds virtual chunk references into ``https://portal.nccs.nasa.gov/datashare/gmao/geos-cf``
plus the small coordinates, one store per stream, next to the materialised 15-minute subsets
(``bkr/geos/geos_15min.icechunk``, ``bkr/geos/geos_v2_15min.icechunk``)::

    bkr/geos/geos_cf_v2_ana.icechunk    v2 hindcast 2023-01 to 2025-08, then live analysis
    bkr/geos/geos_cf_v2_fcst.icechunk   v2 forecasts, 09Z runs
    bkr/geos/geos_cf_v1_ana.icechunk    v1 analysis, 2018 to 2026-01
    bkr/geos/geos_cf_v1_fcst.icechunk   v1 forecasts, 12Z runs

Each collection is a zarr group of its stream's store, named for its short name
(``met_tavg_1hr_slv``, ``chm_inst_1hr_v72``): collections cannot share a time axis (15
minutes, hourly or three-hourly; instants on the hour, averages on the half hour) or a
vertical one (72 model levels, 23 or 28 pressure levels), so ``xr.open_zarr(store,
group="met_tavg_1hr_slv")`` opens one of them with every one of its variables.

The streams (:data:`STREAMS`): v2 analysis and forecasts are live and followed; all of v1
is a finished archive, referenced once. The v2 analysis store begins with the v2 hindcast
(``v2_hindcast/ana``, a replay stamped by valid time) and continues with the live analysis
(``v2/ana``): a collection's group takes the hindcast up to the day before the first day
the live analysis has that collection, and the live analysis from that day on, so its time
axis neither repeats nor goes back where the two overlap. A collection the hindcast never
had (``ghg_inst_3hr_v72``) begins with the live analysis. v1's 3-D collections (model and
pressure levels, about 200 GB a day) are left out: their small HDF5 chunks would take
minutes of range requests per file, or a download of the whole archive, to reference.

Layout, ARCO-ERA5 style:

* analysis groups are ``(time, [model_level | pressure_level], latitude, longitude)``,
  appended along ``time``, one commit per collection and day;
* forecast groups are ``(init_time, step, [level], latitude, longitude)`` with a
  ``valid_time`` coordinate, appended along ``init_time``, one commit per collection and
  run. A step the portal never published reads as missing (NaN), so every run has the full
  step axis;
* single-level collections drop GEOS's length-one ``lev`` and keep its value as a scalar
  ``model_level`` coordinate (the lowest model layer, 72);
* variables are named by :mod:`.geos_cf_names` (``NO2`` is
  ``nitrogen_dioxide_mole_fraction``), each keeping its GEOS name as ``source_name``.

Times come from the file names, which state them exactly (``..._20261101_0030z``); a
time-averaged field is stamped at the middle of its hour, as GEOS-CF stamps it. Everything
that tracks a collection is an attribute of its group, never of the store's root, so the
collections of a stream are written at once without their commits conflicting: a ledger of
the days or runs written, so a run resumes where it stopped, and a layout signature: a
collection whose variables, grid or chunking change continues in a sibling group named for
the day it changed (``chm_inst_1hr_v72_01092026``); the store and the other groups carry on.

    python -m planetary_datasets.providers.virtualized.geos_cf --streams v2_ana,v2_fcst --follow
    python -m planetary_datasets.providers.virtualized.geos_cf --streams v1_ana,v1_fcst
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
import threading
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
#: Group attribute listing the days (analysis) or runs (forecasts) a group holds.
LEDGER_ATTR = "geos_cf_written"
#: Group attribute listing, per day or run, the files left out of it (for a later repair).
LEFT_OUT_ATTR = "geos_cf_left_out"
#: Group attribute: the first day written from the live analysis rather than the hindcast.
LIVE_FROM_ATTR = "geos_cf_live_from"
#: Further attempts at a file that would not open, and the seconds before the first.
OPEN_RETRIES = 2
OPEN_RETRY_PAUSE = 15.0
#: Group attribute fixing a group's layout; a different layout goes to a new generation.
SIGNATURE_ATTR = "geos_cf_layout"
#: Manifest rows per split along ``time`` for an analysis group, by its file interval: one
#: split a day. Matched against the array's path, so a dated generation group gets the same.
ANALYSIS_SPLITS = {"15mn": 96, "1hr": 24, "3hr": 8}
#: Held around each commit. The collections of a stream are written by threads into one
#: store, and the local filesystem storage (tests, scratch runs) cannot commit two at once
#: safely: the branch update is not conditional there, so one commit could silently replace
#: the other. Serialised, each commit sees the one before and rebases over it (the groups
#: differ, so there is nothing to conflict); only the short commit waits, not the writes.
_COMMIT_LOCK = threading.Lock()
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
    """One run of GEOS-CF on the portal, a version's analysis or forecasts, and its store.

    Attributes:
        name: Store-name part, e.g. ``v2_ana``.
        path: Portal path under :data:`PORTAL`, e.g. ``v2/ana``.
        forecast: Files are forecast steps of daily runs, not analysis times.
        run_dir: Subdirectory a day's runs sit in (``H12`` for v1 forecasts).
        single_level_only: Leave out the 3-D collections.
        download: Read each file's metadata from a local copy rather than over HTTP. v1's
            small chunks make an HTTP read thousands of requests long.
        hindcast: Portal path of a replay that comes before ``path`` in the same store
            (``v2_hindcast/ana``), or empty. See :func:`choose_units` for where one ends.
    """

    name: str
    path: str
    forecast: bool = False
    run_dir: str = ""
    single_level_only: bool = False
    download: bool = False
    hindcast: str = ""

    @property
    def store_prefix(self) -> str:
        """Where the stream's store lives, next to the 15-minute subsets."""
        return f"{STORE_ROOT}/geos_cf_{self.name}.icechunk"

    @property
    def paths(self) -> list[str]:
        """Portal paths the stream reads, oldest first: the hindcast, if any, then its own."""
        return [p for p in (self.hindcast, self.path) if p]


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
    "v2_ana": Stream("v2_ana", "v2/ana", hindcast="v2_hindcast/ana"),
    "v2_fcst": Stream("v2_fcst", "v2/fcst", forecast=True),
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
        """The stream's store, which holds this collection as a group."""
        return self.stream.store_prefix

    @property
    def group(self) -> str:
        """The collection's group in its stream's store (the first generation's)."""
        return self.short

    @property
    def append_dim(self) -> str:
        """``init_time`` for forecasts, ``time`` for analyses."""
        return "init_time" if self.stream.forecast else "time"

    @property
    def label(self) -> str:
        """Name used in logs and commit messages."""
        return f"geos_cf_{self.stream.name}/{self.short}"


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


def day_dirs(portal: Portal, stream: Stream, source: str | None = None) -> list[dt.date]:
    """Every day the stream (or its portal path ``source``) has a directory for."""
    source = source or stream.path
    days = []
    for year in portal.list(source):
        if not re.fullmatch(r"Y\d{4}/", year):
            continue
        for month in portal.list(f"{source}/{year}"):
            if not re.fullmatch(r"M\d{2}/", month):
                continue
            for day in portal.list(f"{source}/{year}{month}"):
                if re.fullmatch(r"D\d{2}/", day):
                    days.append(dt.date(int(year[1:5]), int(month[1:3]), int(day[1:3])))
    return sorted(days)


def day_files(
    portal: Portal, stream: Stream, day: dt.date, source: str | None = None
) -> list[SourceFile]:
    """The files of one day directory (its run subdirectory, for v1 forecasts).

    ``source`` is the portal path to read, the stream's own by default.
    """
    path = f"{source or stream.path}/Y{day:%Y}/M{day:%m}/D{day:%d}"
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
    without one. Returns ``(datasets, URLs left out)``: one dataset per layout the unit's
    files come in (usually one; empty if nothing could be opened). A file that will not
    open fails the unit while it is recent (the next pass retries it) and is left out once
    the unit has settled. A settled unit none of whose files opens is taken for a portal
    outage, not damage: :func:`run_stream` holds its collection back behind it until a
    later pass opens it, keeping the times in order.
    """

    def attempt(urls: list[str]):
        if pool is None:
            return [_open_url(url, opener) for url in urls]
        return list(pool.map(_open_url, urls))

    results = attempt([f.url for f in files])
    by_url = {url: ds for url, ds, _ in results if ds is not None}
    errors = {url: error for url, _, error in results if error}
    # A failure can be the portal's, not the file's: try again before leaving a file out,
    # since a unit once written is not visited again.
    for retry in range(OPEN_RETRIES):
        if not errors:
            break
        time.sleep(OPEN_RETRY_PAUSE * 2**retry)
        for url, ds, error in attempt(list(errors)):
            if ds is not None:
                by_url[url] = ds
                errors.pop(url)
            else:
                errors[url] = error
    opened = [(f, by_url[f.url]) for f in files if f.url in by_url]
    if errors and not settled:
        first = next(iter(errors.items()))
        raise RuntimeError(
            f"{collection.label}: {len(errors)} file(s) would not open, e.g. {first}"
        )
    for url, error in errors.items():
        logger.warning(f"{collection.label}: {url}: {error}; left out")
    if not opened:
        return [], sorted(errors)
    codes: dict[str, dict] = {}
    for _, ds in opened:
        for n, v in ds.data_vars.items():
            codes.setdefault(n, dict(v.attrs))
    names = variable_names(codes)
    shaped = [(f, shape(ds, collection, f, names)) for f, ds in opened]
    # Virtual references cannot mix chunk shapes within one array, so the files of a unit
    # that are laid out differently (re-chunked on a day mid-archive) cannot be combined.
    by_layout: dict[str, list] = {}
    for f, ds in sorted(shaped, key=lambda item: item[0].valid):
        by_layout.setdefault(layout_signature(ds, collection), []).append((f, ds))
    batches = list(by_layout.values())
    if len(batches) > 1:
        sizes = [len(b) for b in batches]
        logger.warning(
            f"{collection.label}: {len(shaped)} file(s) in {len(batches)} layouts {sizes}; "
            + (
                "each layout is written to its own group"
                if not collection.stream.forecast
                else "keeping the largest, leaving the others out"
            )
        )
        if collection.stream.forecast:
            keep = max(range(len(batches)), key=lambda i: (sizes[i], -i))
            for i, batch in enumerate(batches):
                if i != keep:
                    errors.update(
                        {f.url: "layout differs from the run's other files" for f, _ in batch}
                    )
            batches = [batches[keep]]
    attrs = json.dumps({n: c for c, n in names.items()})
    return [_combine(collection, batch, attrs) for batch in batches], sorted(errors)


def _combine(collection: Collection, shaped: list, source_names: str):
    """One layout's files as a single dataset: a day's ``time`` axis, or a run's ``step`` one."""
    if not collection.stream.forecast:
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
    vds.attrs["geos_cf_variable_source_names"] = source_names
    return vds


# ----------------------------------------------------------------------------- stores
def open_store(
    stream: Stream,
    config: Config,
    create: bool = True,
    container: str = CONTAINER,
):
    """Open (or create) a stream's store, its references read through ``container``.

    Manifests split one run a split for forecasts, and one day a split for analyses: each
    collection's group by its own interval, matched on the array path (``.._1hr_..``), since
    a store-wide size would give a 15-minute group four times the rows of an hourly one.
    Creating the store also creates its root group, once, so that no collection's commit
    touches the root and the collections' commits never conflict.
    """
    import icechunk
    import zarr
    from zarr.errors import GroupNotFoundError

    from planetary_datasets.providers.virtualized.virtual_repo import open_virtual_repo

    if stream.forecast:
        split, path_splits = 1, None
    else:
        split = ANALYSIS_SPLITS["1hr"]
        path_splits = {rf".*_{code}_.*": {"time": n} for code, n in ANALYSIS_SPLITS.items()}
    repo = open_virtual_repo(
        stream.store_prefix,
        virtual_buckets=[container],
        split_size=split,
        split_dim="init_time" if stream.forecast else "time",
        config=config,
        create=create,
        path_splits=path_splits,
    )
    if not create:
        return repo
    try:
        zarr.open_group(repo.readonly_session("main").store, mode="r")
    except GroupNotFoundError:
        session = repo.writable_session("main")
        root = zarr.open_group(session.store, mode="a")
        attrs = {
            "model": "NASA GMAO GEOS-CF",
            "geos_cf_stream": stream.name,
            "source": f"{PORTAL}/{stream.path}/",
        }
        if stream.hindcast:
            attrs["hindcast_source"] = f"{PORTAL}/{stream.hindcast}/"
        root.attrs.update(attrs)
        session.commit(
            f"geos_cf_{stream.name}: create store", rebase_with=icechunk.ConflictDetector()
        )
    return repo


def group_attrs(repo, group: str) -> dict:
    """Attributes of one group of the store; empty only for a group not written yet.

    Any other failure propagates: read as "empty", a ledger that failed to load would
    write its days again, and a signature that failed to load would pick a generation.
    """
    import zarr
    from zarr.errors import GroupNotFoundError

    try:
        node = zarr.open_group(repo.readonly_session("main").store, path=group, mode="r")
    except (GroupNotFoundError, FileNotFoundError):
        return {}
    return dict(node.attrs)


def generation_name(collection: Collection, when: dt.date, disambiguator: int = 0) -> str:
    """The group a collection continues in once its layout changes on ``when``."""
    from planetary_datasets.common.generations import GENERATION_DATE_FORMAT

    name = f"{collection.group}_{when.strftime(GENERATION_DATE_FORMAT)}"
    return f"{name}_{disambiguator}" if disambiguator else name


def generations(repo, collection: Collection) -> list[str]:
    """The collection's base group and its dated generation groups that exist, base first."""
    import zarr
    from zarr.errors import GroupNotFoundError

    from planetary_datasets.common.generations import GENERATION_DATE_FORMAT

    try:
        root = zarr.open_group(repo.readonly_session("main").store, mode="r")
        names = list(root.group_keys())
    except GroupNotFoundError:
        names = []
    pattern = re.compile(rf"^{re.escape(collection.group)}_(?P<date>\d{{8}})(?:_(?P<n>\d+))?$")
    dated = sorted(
        (dt.datetime.strptime(m["date"], GENERATION_DATE_FORMAT), int(m["n"] or 0), name)
        for name in names
        if (m := pattern.match(name))
    )
    return [collection.group] + [name for *_, name in dated]


def written(repo, collection: Collection) -> set[str]:
    """Days or runs in any of the collection's generation groups' ledgers."""
    done: set[str] = set()
    for group in generations(repo, collection):
        done |= set(group_attrs(repo, group).get(LEDGER_ATTR, []))
    return done


def live_since(repo, collection: Collection) -> dt.date | None:
    """The first day the collection was written from the live analysis, if it has been."""
    days = [
        dt.date.fromisoformat(a[LIVE_FROM_ATTR])
        for group in generations(repo, collection)
        if LIVE_FROM_ATTR in (a := group_attrs(repo, group))
    ]
    return min(days, default=None)


def group_for(
    repo,
    collection: Collection,
    signature: str,
    when: dt.date,
    taken: Sequence[str] = (),
) -> str:
    """The generation group whose layout is ``signature``: the first empty or matching, else new.

    Groups in ``taken`` (already chosen for another layout, not yet committed) are skipped.
    """
    for group in generations(repo, collection):
        if group not in taken and group_attrs(repo, group).get(SIGNATURE_ATTR) in (
            None,
            signature,
        ):
            return group
    for disambiguator in range(10 + len(taken)):
        group = generation_name(collection, when, disambiguator)
        if group not in taken and group_attrs(repo, group).get(SIGNATURE_ATTR) in (
            None,
            signature,
        ):
            logger.info(f"{collection.label}: layout changed on {when}; new group {group}")
            return group
    raise RuntimeError(f"{collection.label}: no generation free for {when}")


def write_unit(
    collection: Collection,
    key: str,
    when: dt.date,
    parts: Sequence,
    left_out: Sequence[str],
    repo,
    live: bool = False,
) -> int:
    """Append one day or run to the collection's matching group(s), in one commit.

    ``parts`` holds one dataset per layout of the unit's files (see :func:`open_unit`); each
    goes to the generation group of its layout, and all of them in the same commit, so the
    unit is written whole or not at all. Only those groups are written, their attributes
    included, so the commits of a stream's collections, made at once, rebase over each
    other. ``live`` marks a unit read from the live analysis of a stream with a hindcast;
    the first such day is recorded on the group (:data:`LIVE_FROM_ATTR`). Returns the files
    written.
    """
    import icechunk
    import zarr

    stream = collection.stream
    session = repo.writable_session("main")
    taken: list[str] = []
    total = 0
    for i, vds in enumerate(parts):
        signature = layout_signature(vds, collection)
        group = group_for(repo, collection, signature, when, taken)
        taken.append(group)
        attrs = group_attrs(repo, group)
        if attrs.get(SIGNATURE_ATTR):
            vds.vz.to_icechunk(session.store, group=group, append_dim=collection.append_dim)
        else:
            vds.vz.to_icechunk(session.store, group=group)
        node = zarr.open_group(session.store, path=group, mode="a")
        ledger = set(node.attrs.get(LEDGER_ATTR, [])) | {key}
        left_out_by_unit = dict(node.attrs.get(LEFT_OUT_ATTR, {}))
        if left_out and i == 0:
            left_out_by_unit[key] = list(left_out)
        update = {
            SIGNATURE_ATTR: signature,
            LEDGER_ATTR: sorted(ledger),
            LEFT_OUT_ATTR: left_out_by_unit,
            "source": f"{PORTAL}/{stream.path}/",
            "collection": collection.name,
            "model": "NASA GMAO GEOS-CF",
        }
        if stream.hindcast:
            update["hindcast_source"] = f"{PORTAL}/{stream.hindcast}/"
            if live and LIVE_FROM_ATTR not in node.attrs:
                update[LIVE_FROM_ATTR] = when.isoformat()
        node.attrs.update(update)
        total += int(vds.sizes["step" if stream.forecast else "time"])
    with _COMMIT_LOCK:
        session.commit(
            f"{collection.label}: {key} ({total} file(s), {len(left_out)} left out)",
            rebase_with=icechunk.ConflictDetector(),
        )
    logger.info(
        f"{collection.label} {key}: {total} file(s) -> {stream.store_prefix}/{', '.join(taken)} "
        f"({len(left_out)} left out)"
    )
    return total


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


def choose_files(
    stream: Stream,
    day: dt.date,
    by_source: dict[str, list[SourceFile]],
    live_from: dict[str, dt.date],
) -> tuple[list[SourceFile], set[str]]:
    """One day's files, each collection's from one source; and the collections read live.

    A stream with no hindcast takes its own files. With one, a collection is read from the
    hindcast up to the day before the first day the live analysis has it, and from the live
    analysis from that day on, whether or not the hindcast goes further: the two never
    meet in a group, so its time axis neither repeats nor goes back. ``live_from`` holds
    that first live day per collection name (from the groups, then from the days seen);
    days must come oldest first, and it is updated here.
    """
    live = by_source.get(stream.path, [])
    live_names = {f.collection for f in live}
    for name in live_names:
        if name not in live_from or day < live_from[name]:
            live_from[name] = day
    chosen = list(live)
    if stream.hindcast:
        chosen += [
            f
            for f in by_source.get(stream.hindcast, [])
            if f.collection not in live_names
            and (f.collection not in live_from or day < live_from[f.collection])
        ]
    return chosen, live_names if stream.hindcast else set()


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
    """Write every settled, unwritten day or run of one stream. Returns files written.

    Days go oldest first, across the hindcast and the live analysis of a stream with both,
    so a group gets every hindcast day before its first live one. A day that cannot be
    listed ends the pass, for the same reason: the next pass starts again from it.
    """
    now = now or dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    listed = {
        source: {
            d
            for d in day_dirs(portal, stream, source)
            if (start is None or d >= start) and (end is None or d <= end)
        }
        for source in stream.paths
    }
    days = sorted(set().union(*listed.values()))
    if not days:
        logger.info(f"geos_cf_{stream.name}: no days on the portal")
        return 0
    repo = open_store(stream, config, container=portal.container)
    scratch = pathlib.Path(config.scratch_dir)
    opener = Opener(stream, portal, scratch=scratch)
    pool = open_pool(stream, portal, scratch, open_workers)
    total = 0
    try:
        ledgers: dict[str, set[str]] = {}
        #: First day each collection is read from the live analysis (hindcast streams).
        live_from: dict[str, dt.date] = {}
        #: Collections with a unit waiting (still published, or failed while recent).
        deferred: set[str] = set()

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
                by_source = {
                    source: day_files(portal, stream, day, source)
                    for source in stream.paths
                    if day in listed[source]
                }
            except Exception as exc:  # noqa: BLE001 - retried next pass, from this day
                logger.warning(
                    f"geos_cf_{stream.name} {day}: listing failed: {exc}; pass ends here"
                )
                break
            names = sorted({f.collection for files in by_source.values() for f in files})
            collections = [c for c in map(collection_of, names) if c is not None]
            for c in collections:
                if c.name not in ledgers:
                    ledgers[c.name] = written(repo, c)
                    if (first := live_since(repo, c)) is not None:
                        live_from[c.name] = first
            files, read_live = choose_files(stream, day, by_source, live_from)

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
                        # Still being published. Later units wait behind it, so the group's
                        # times stay in order.
                        deferred.add(c.name)
                        break
                    try:
                        parts, left_out = open_unit(opener, c, unit_files, settled, pool)
                        if not parts:
                            # Nothing opened, even after retries: an outage. Later units
                            # wait behind it rather than be appended ahead of it.
                            logger.warning(f"{c.label} {key}: no file opened; retrying next pass")
                            deferred.add(c.name)
                            break
                        count += write_unit(
                            c, key, when.date(), parts, left_out, repo, c.name in read_live
                        )
                        ledgers[c.name].add(key)
                    except Exception as exc:  # noqa: BLE001 - one bad unit must not stop the rest
                        logger.exception(f"{c.label} {key}: {type(exc).__name__}: {exc}")
                        # Recent units are retried next pass, and so are I/O and storage
                        # failures of any unit: later units wait behind it, as above. A
                        # deterministic failure (ValueError, RuntimeError) of a settled unit
                        # would block the collection for good, so later units go on.
                        if not settled or not isinstance(exc, (ValueError, RuntimeError)):
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
    unknown = [s for s in args.streams.split(",") if s not in STREAMS]
    if unknown:
        # v2_hindcast is no stream of its own: it is the start of v2_ana's store.
        parser.error(f"unknown stream(s) {', '.join(unknown)}; choose from {', '.join(STREAMS)}")
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
                logger.exception(f"geos_cf_{stream.name}: pass failed: {type(exc).__name__}: {exc}")
                continue
            logger.info(f"geos_cf_{stream.name}: pass done, {written_files:,} file(s) referenced")
        if not args.follow:
            return 0
        waited = 0
        while waited < args.pause:  # saying so every ten minutes, for the box's stall guard
            time.sleep(min(600, args.pause - waited))
            waited += 600
            logger.info("geos_cf: waiting for the next pass")


if __name__ == "__main__":
    sys.exit(main())
