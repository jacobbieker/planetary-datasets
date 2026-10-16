"""NCEP ADP upper-air observations from NCAR GDEX ``d337000``, as ragged point stores.

``d337000`` is NCEP's PREPBUFR archive: every conventional observation that went into the
GDAS analysis, one file per 6-hourly cycle holding the reports from three hours either
side of it. Two kinds are kept here, each in a store of its own:

* ``bkr/obs/ncep_amdar.icechunk`` -- aircraft: PREPBUFR message types ``AIRCAR`` (ACARS /
  MDCRS, report types 133/233) and ``AIRCFT`` (AMDAR 131/231, AIREP and PIREP 130/230,
  TAMDAR 134/234, Canadian AMDAR 135/235);
* ``bkr/obs/ncep_adpupa.icechunk`` -- radiosondes, pibals and dropsondes: message type
  ``ADPUPA`` (report types 120/220/221/132...).

They come out of the same decode of the same file, so splitting them costs nothing, and
they are different products: a sounding is a profile from a fixed station, an aircraft
report a single point on a moving platform.

Layout. One row per observation point along a ``time`` dimension whose values repeat. A
PREPBUFR report is split into a mass report (pressure, temperature, humidity, height) and
a wind report sharing its position and time; the two are merged back into one row here,
keyed on message type, station, time, position and pressure, so a row carries everything
observed at that point. ``report_type`` is the mass report's type (1xx), or the wind
report's (2xx) for a point that only had winds. Values are the top of PREPBUFR's event
stack, i.e. after NCEP's quality control, with sensible rather than virtual temperature;
each variable keeps its quality mark (``*_qm``: 0-3 usable, 9 not checked, 13+ rejected
or purged). Nothing is dropped on quality: filter on the marks. Radiosonde levels carry
the launch position, as MET's decoder does not output balloon drift.

Sources and coverage. The archive has three generations:

* ``prepnr/<year>/prepbufr.gdas.<YYYYMMDDHH>.wo40.nr``, 1997-05-01 to 2008-06-30 18z:
  everything, aircraft included.
* ``prepnr/<year>/prepbufr.gdas.<YYYYMMDDHH>.nr``, from 2008-07-01: NCEP's
  "non-restricted" files, with the restricted aircraft data (AMDAR, ACARS) removed. Until
  the 48-hour files begin they are the only source, so 2008-07-01 to 2015-09-04 06z holds
  radiosondes in full but only AIREP/PIREP (and for a while TAMDAR) aircraft reports.
* ``prep48h/<year>/prepbufr.gdas.<YYYYMMDD>.t<HH>z.nr.48h``, from 2015-09-04 12z: the
  same files released 48 hours later with the restricted aircraft data back in. GDEX
  publishes them roughly 60-70 hours after the cycle.

A cycle whose 48-hour file is still absent :data:`PREP48H_FALLBACK_AGE` after the cycle is
taken from the non-restricted file instead, and listed under :data:`FALLBACK_ATTR`.

Decoding. PREPBUFR uses NCEP's embedded local BUFR tables, which ecCodes cannot read, so
it is converted with MET's ``pb2nc``: the local binary when ``PB2NC_BINARY`` or ``PATH``
has one, otherwise the MET Docker image (``MET_IMAGE``). Decoding dominates the cost of a
partition; :attr:`NCEPUpperAirProvider.prefetch_workers` decodes the next cycles
concurrently while the current one is written.

Bookkeeping. As in :mod:`~planetary_datasets.providers.observations.osmc_ragged`, each
store records the cycles it holds in its group attributes (:data:`LEDGER_ATTR`), updated
in the same commit as the rows, so "is this cycle stored?" is one attribute read. The
ledger is a list of ``[first, last]`` runs of consecutive cycles rather than one entry per
cycle, so it stays a few lines long across the whole archive. The radiosonde store is
written first and the aircraft store last, so the aircraft ledger is the record of which
cycles are complete; a cycle interrupted between the two resumes without duplicating the
radiosonde rows. Cycles may be written in any order.
"""

from __future__ import annotations

import os
import pathlib
import shutil
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Dict, Iterable, List, Sequence

import icechunk
import numpy as np
import pandas as pd
import requests
import xarray as xr
import zarr
from icechunk.xarray import to_icechunk
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common.download import download_one
from planetary_datasets.common.staged import StagedFilesMixin
from planetary_datasets.common.store import (
    STORE_READ_ERRORS,
    StoreReadError,
    build_encoding,
    has_committed_data,
    quiet_timedelta_decoding,
)
from planetary_datasets.config import Config
from planetary_datasets.providers._timestamps import to_naive_utc
from planetary_datasets.providers.observations.amdar import (
    CONTAINER_TMP_DIR,
    pb2nc_available,
    render_pb2nc_config,
    run_pb2nc,
    run_pb2nc_docker,
)

GDEX_BASE_URL = "https://data.gdex.ucar.edu/d337000"
PREP48H_URL = GDEX_BASE_URL + "/prep48h/{cycle:%Y}/prepbufr.gdas.{cycle:%Y%m%d}.t{cycle:%H}z.nr.48h"
PREPNR_URL = GDEX_BASE_URL + "/prepnr/{cycle:%Y}/prepbufr.gdas.{cycle:%Y%m%d%H}{suffix}"

#: First cycle in the archive.
ARCHIVE_START = pd.Timestamp("1997-05-01T00")
#: First ``.nr`` file. Before it the files are ``.wo40.nr`` and still carry every aircraft.
NR_START = pd.Timestamp("2008-07-01T00")
#: First 48-hour file, which restores the aircraft data the ``.nr`` files strip.
PREP48H_START = pd.Timestamp("2015-09-04T12")
#: How long a missing 48-hour file is waited for before the ``.nr`` file is used instead.
PREP48H_FALLBACK_AGE = pd.Timedelta(30, "D")
#: Hours after a cycle by which GDEX has published its 48-hour file.
PUBLICATION_LAG_HOURS = 72

CYCLE_FREQ = "6h"
CYCLE = pd.Timedelta(6, "h")
#: A GDAS cycle holds reports from three hours either side of its nominal time.
HALF_WINDOW = pd.Timedelta(3, "h")

AMDAR_STORE = "bkr/obs/ncep_amdar.icechunk"
ADPUPA_STORE = "bkr/obs/ncep_adpupa.icechunk"
AIRCRAFT_MESSAGE_TYPES = ("AIRCAR", "AIRCFT")
SOUNDING_MESSAGE_TYPES = ("ADPUPA",)
#: Stores in the order they are written. The last one's ledger marks a cycle complete.
STORES: Dict[str, tuple[str, ...]] = {
    ADPUPA_STORE: SOUNDING_MESSAGE_TYPES,
    AMDAR_STORE: AIRCRAFT_MESSAGE_TYPES,
}

LEDGER_ATTR = "ingested_cycles"
FALLBACK_ATTR = "nr_fallback_cycles"
#: Rows per chunk along ``time``: about two cycles of aircraft reports.
CHUNK_ROWS = 250_000

#: BUFR mnemonics pb2nc is asked for, and the MET names it writes them under.
OBS_BUFR_VAR = ("POB", "QOB", "TOB", "ZOB", "UOB", "VOB")
#: MET variable name -> (value column, quality-mark column).
MET_VARIABLES: Dict[str, tuple[str, str]] = {
    "PRES": ("pressure", "pressure_qm"),
    "SPFH": ("specific_humidity", "humidity_qm"),
    "TMP": ("temperature", "temperature_qm"),
    "HGT": ("height", "height_qm"),
    "UGRD": ("u_wind", "wind_qm"),
    "VGRD": ("v_wind", "wind_qm"),
}

STRING_VARS = ("station_id", "message_type")
FLOAT_VARS = (
    "latitude",
    "longitude",
    "elevation",
    "pressure",
    "height",
    "temperature",
    "specific_humidity",
    "u_wind",
    "v_wind",
    "report_type",
    "instrument_type",
    "pressure_qm",
    "height_qm",
    "temperature_qm",
    "humidity_qm",
    "wind_qm",
)
#: Every variable a partition writes, so appends always match the store.
VARIABLES = STRING_VARS + FLOAT_VARS
UNITS: Dict[str, str] = {
    "latitude": "degrees_north",
    "longitude": "degrees_east",
    "elevation": "m",
    "pressure": "Pa",
    "height": "m",
    "temperature": "K",
    "specific_humidity": "kg kg-1",
    "u_wind": "m s-1",
    "v_wind": "m s-1",
}
DESCRIPTIONS: Dict[str, str] = {
    "station_id": "PREPBUFR station id (SID): WMO station, or aircraft / flight id",
    "message_type": "PREPBUFR message type: AIRCAR, AIRCFT or ADPUPA",
    "elevation": "header elevation (ELV): station height, or aircraft altitude",
    "report_type": "PREPBUFR report type (TYP); the mass type (1xx) when merged with winds",
    "instrument_type": "PREPBUFR instrument type (ITP)",
    "height": "observed height (ZOB), else the height MET assigns the level",
}

STORE_ATTRS = {
    "title": "NCEP ADP global upper-air observations (PREPBUFR)",
    "source": "NSF NCAR GDEX d337000, https://doi.org/10.5065/Z83F-N512",
    "decoder": "MET pb2nc, event stack TOP, every quality mark kept",
    "aircraft_coverage": (
        "AMDAR/ACARS are absent from 2008-07-01T00 to 2015-09-04T06: the only files for "
        "that period are NCEP's non-restricted ones, which strip them"
    ),
}


class PrepbufrUnavailable(RuntimeError):
    """The archive could not be reached for a cycle (rather than not having it)."""


# ---------------------------------------------------------------------------- cycles


def as_cycle(it) -> pd.Timestamp:
    """A partition timestamp as a naive-UTC cycle time, which must be on a 6 h boundary."""
    cycle = to_naive_utc(it)
    if cycle != cycle.floor(CYCLE_FREQ):
        raise ValueError(f"{cycle} is not a GDAS cycle time (00, 06, 12 or 18 UTC)")
    return cycle


def cycle_label(it) -> str:
    """The ledger spelling of a cycle, ``YYYY-MM-DDTHH``."""
    return f"{as_cycle(it):%Y-%m-%dT%H}"


def _cycle_number(it) -> int:
    return int((as_cycle(it) - pd.Timestamp(0)) // CYCLE)


def _number_label(number: int) -> str:
    return cycle_label(pd.Timestamp(0) + number * CYCLE)


def ledger_runs(raw) -> List[tuple[int, int]]:
    """Parse a stored ledger into sorted ``(first, last)`` cycle numbers."""
    runs = sorted((_cycle_number(first), _cycle_number(last)) for first, last in raw or [])
    return runs


def ledger_contains(runs: Sequence[tuple[int, int]], it) -> bool:
    """True when the cycle falls inside one of the runs."""
    number = _cycle_number(it)
    return any(first <= number <= last for first, last in runs)


def ledger_add(raw, it) -> List[List[str]]:
    """The stored ledger with one more cycle, merging runs it joins."""
    number = _cycle_number(it)
    runs = ledger_runs(raw) + [(number, number)]
    runs.sort()
    merged: List[List[int]] = []
    for first, last in runs:
        if merged and first <= merged[-1][1] + 1:
            merged[-1][1] = max(merged[-1][1], last)
        else:
            merged.append([first, last])
    return [[_number_label(first), _number_label(last)] for first, last in merged]


# ---------------------------------------------------------------------------- sources


def prep48h_url(cycle) -> str:
    """The 48-hour-delayed file, with the restricted aircraft data, for a cycle."""
    return PREP48H_URL.format(cycle=as_cycle(cycle))


def prepnr_url(cycle, suffix: str | None = None) -> str:
    """The non-restricted file for a cycle; ``.wo40.nr`` before :data:`NR_START`."""
    cycle = as_cycle(cycle)
    if suffix is None:
        suffix = ".wo40.nr" if cycle < NR_START else ".nr"
    return PREPNR_URL.format(cycle=cycle, suffix=suffix)


def source_urls(cycle, now: pd.Timestamp | None = None) -> List[str]:
    """Files that can supply a cycle, best first.

    The 48-hour file is preferred whenever the archive has them. The non-restricted file
    is only offered in its place once the 48-hour one is :data:`PREP48H_FALLBACK_AGE`
    overdue, because a cycle taken from it is recorded as done and never revisited.
    """
    cycle = as_cycle(cycle)
    if cycle < ARCHIVE_START:
        return []
    if cycle < NR_START:
        # The suffix change is dated from the archive listing; try both either side of it.
        return [prepnr_url(cycle, ".wo40.nr"), prepnr_url(cycle, ".nr")]
    if cycle < PREP48H_START:
        return [prepnr_url(cycle, ".nr")]
    now = to_naive_utc(now if now is not None else pd.Timestamp.now(tz="UTC"))
    urls = [prep48h_url(cycle)]
    if now - cycle > PREP48H_FALLBACK_AGE:
        urls.append(prepnr_url(cycle, ".nr"))
    return urls


def is_restricted_fallback(name: str, cycle) -> bool:
    """True when ``name`` is a non-restricted file standing in for a 48-hour one."""
    return as_cycle(cycle) >= PREP48H_START and ".48h" not in name


def url_is_absent(url: str, timeout: float = 60.0) -> bool:
    """True when the archive answers 404 for ``url``; False when it has it or is unsure."""
    try:
        return requests.head(url, allow_redirects=True, timeout=timeout).status_code == 404
    except requests.RequestException as exc:
        logger.debug(f"could not probe {url}: {exc}")
        return False


# ---------------------------------------------------------------------------- decoding


def pb2nc_config_text(tmp_dir: str | os.PathLike) -> str:
    """The pb2nc configuration for this archive: aircraft and soundings, all marks kept."""
    return render_pb2nc_config(
        tmp_dir,
        obs_window=(-21600, 21600),
        message_types=AIRCRAFT_MESSAGE_TYPES + SOUNDING_MESSAGE_TYPES,
        obs_bufr_var=OBS_BUFR_VAR,
        quality_mark_thresh=15,
    )


def _strings(values: np.ndarray) -> np.ndarray:
    """A MET string table (fixed-width bytes) as Python strings."""
    return np.array(
        [
            (v.decode("utf-8", "ignore") if isinstance(v, bytes) else str(v)).strip("\x00 ")
            for v in np.asarray(values).ravel()
        ],
        dtype=object,
    )


def _indices(ds: xr.Dataset, name: str) -> np.ndarray:
    """A MET index array, which netCDF hands back as float when it has a fill value."""
    return np.asarray(ds[name].values).astype(np.int64)


def _one_per_level(row: np.ndarray, candidates: np.ndarray, mark: np.ndarray) -> np.ndarray:
    """Of the observations ``candidates`` of one variable, one per level, marked first.

    pb2nc writes each value of the older (``wo40``) files twice: once with its quality
    mark and once with ``NA``, and for radiosonde temperatures the two differ. The copy
    with a mark is the top of the event stack, which is the one wanted.
    """
    if candidates.size == 0:
        return candidates
    marked_last = candidates[np.argsort(~np.isnan(mark[candidates]), kind="stable")]
    # np.unique keeps the first occurrence; reversing makes that the last, i.e. marked.
    reversed_ = marked_last[::-1]
    _, first = np.unique(row[reversed_], return_index=True)
    return reversed_[first]


def pb2nc_levels(ds: xr.Dataset) -> pd.DataFrame:
    """Flatten a MET point-observation file into one row per report level.

    MET writes one value per (report, level, variable); this pivots the variables into
    columns, each with its quality mark. A report's levels are told apart by their
    pressure, which pb2nc writes for every PREPBUFR level.

    Raises:
        ValueError: ``ds`` is not a MET point-observation file.
    """
    required = ("hdr_typ", "hdr_sid", "hdr_vld", "hdr_lat", "hdr_lon", "hdr_elv", "obs_hid")
    missing = [name for name in required if name not in ds.variables]
    if missing:
        raise ValueError(f"not a MET point-observation file: missing {missing}")
    columns = {name: [] for name in ("time",) + VARIABLES}
    hid = _indices(ds, "obs_hid")
    if hid.size == 0:
        return pd.DataFrame({name: [] for name in columns})

    vid = _indices(ds, "obs_vid")
    level = np.asarray(ds["obs_lvl"].values, dtype="float64")
    obs_height = np.asarray(ds["obs_hgt"].values, dtype="float64")
    value = np.asarray(ds["obs_val"].values, dtype="float64")
    marks = pd.to_numeric(pd.Series(_strings(ds["obs_qty_table"].values)), errors="coerce")
    mark = marks.to_numpy(dtype="float64")[_indices(ds, "obs_qty")]
    var_names = _strings(ds["obs_var"].values)

    row, levels = pd.factorize(pd.MultiIndex.from_arrays([hid, level]))
    n = len(levels)
    header = np.asarray(levels.get_level_values(0), dtype=np.int64)
    out: Dict[str, np.ndarray] = {
        name: np.full(n, np.nan) for name in {c for pair in MET_VARIABLES.values() for c in pair}
    }
    for index, name in enumerate(var_names):
        if name not in MET_VARIABLES:
            continue
        column, mark_column = MET_VARIABLES[name]
        chosen = _one_per_level(row, np.flatnonzero(vid == index), mark)
        out[column][row[chosen]] = value[chosen]
        out[mark_column][row[chosen]] = mark[chosen]

    # Height MET assigns the level, for levels with no observed height.
    assigned = np.full(n, np.nan)
    known = ~np.isnan(obs_height)
    assigned[row[known]] = obs_height[known]
    out["height"] = np.where(np.isnan(out["height"]), assigned, out["height"])
    level_hpa = np.asarray(levels.get_level_values(1), dtype="float64")
    out["pressure"] = np.where(np.isnan(out["pressure"]), level_hpa * 100.0, out["pressure"])

    def header_values(name: str, default=np.nan) -> np.ndarray:
        if name not in ds.variables:
            return np.full(n, default)
        return np.asarray(ds[name].values, dtype="float64")[header]

    times = pd.to_datetime(
        pd.Series(_strings(ds["hdr_vld_table"].values)), format="%Y%m%d_%H%M%S", errors="coerce"
    ).to_numpy()
    longitude = header_values("hdr_lon")
    frame = pd.DataFrame(
        {
            "time": times[_indices(ds, "hdr_vld")[header]],
            "station_id": _strings(ds["hdr_sid_table"].values)[_indices(ds, "hdr_sid")[header]],
            "message_type": _strings(ds["hdr_typ_table"].values)[_indices(ds, "hdr_typ")[header]],
            "latitude": header_values("hdr_lat"),
            "longitude": (longitude + 180.0) % 360.0 - 180.0,
            "elevation": header_values("hdr_elv"),
            "report_type": header_values("hdr_prpt_typ"),
            "instrument_type": header_values("hdr_inst_typ"),
            **out,
        }
    )
    return frame[["time", *VARIABLES]]


def merge_mass_and_wind(frame: pd.DataFrame) -> pd.DataFrame:
    """Merge the mass and wind halves of each report into one row per observation point.

    PREPBUFR files every report twice, temperature/humidity under a 1xx report type and
    winds under the matching 2xx, with the same station, time, position and pressure.
    Rows agreeing on those are combined, taking each value from the first row that has
    it, mass report first; ``report_type`` keeps the mass type.
    """
    if frame.empty:
        return frame
    keys = ["message_type", "station_id", "time", "latitude", "longitude", "pressure"]
    ordered = frame.sort_values("report_type", kind="stable", na_position="last")
    merged = ordered.groupby(keys, sort=False, dropna=False).first().reset_index()
    return merged[["time", *VARIABLES]]


def read_pb2nc(path: str | os.PathLike) -> pd.DataFrame:
    """One converted cycle as a table of merged observation points.

    MET's ``-9999`` fill values decode to NaN; its index arrays never hold one.
    """
    with xr.open_dataset(path, decode_times=False) as handle:
        ds = handle.load()
    return merge_mass_and_wind(pb2nc_levels(ds))


def table_to_dataset(table: pd.DataFrame, attrs: dict | None = None) -> xr.Dataset:
    """A table of observation points as the stores' dataset, sorted by time and station."""
    times = pd.to_datetime(table["time"]).to_numpy(dtype="datetime64[ns]")
    keep = ~np.isnat(times)
    table = table.loc[keep]
    times = pd.DatetimeIndex(times[keep]).floor("s").to_numpy()
    stations = table["station_id"].astype(str).to_numpy()
    order = np.lexsort((stations, times))
    data = {}
    for name in STRING_VARS:
        values = table[name].astype(str).to_numpy(dtype=object)[order]
        data[name] = ("time", values)
    for name in FLOAT_VARS:
        data[name] = ("time", table[name].to_numpy(dtype="float32")[order])
    ds = xr.Dataset(data, coords={"time": times[order]}, attrs=dict(attrs or {}))
    for name, units in UNITS.items():
        ds[name].attrs["units"] = units
    for name, text in DESCRIPTIONS.items():
        ds[name].attrs["description"] = text
    return ds


def empty_dataset(attrs: dict | None = None) -> xr.Dataset:
    """The stores' dataset with no rows."""
    return table_to_dataset(pd.DataFrame({name: [] for name in ("time",) + VARIABLES}), attrs)


# ---------------------------------------------------------------------------- writing


def read_ledger(repo: icechunk.Repository) -> List[tuple[int, int]]:
    """Cycles a store holds, as runs; empty for a store not created yet."""
    try:
        group = zarr.open_group(repo.readonly_session("main").store, mode="r")
    except (*STORE_READ_ERRORS, zarr.errors.GroupNotFoundError):
        return []
    return ledger_runs(group.attrs.get(LEDGER_ATTR, []))


def append_with_ledger(
    repo: icechunk.Repository,
    ds: xr.Dataset,
    cycle,
    label: str,
    fallback: bool = False,
    chunk_rows: int = CHUNK_ROWS,
) -> bool:
    """Append one cycle's rows and add the cycle to the store's ledger, in one commit.

    Returns False, writing nothing, when the ledger already lists the cycle. A cycle with
    no rows is still recorded, so it is not asked for again; if it is the store's first,
    only the group and its ledger are created.
    """
    session = repo.writable_session("main")
    try:
        group = zarr.open_group(session.store, mode="r")
        exists = "time" in group
        raw = list(group.attrs.get(LEDGER_ATTR, []))
        fallbacks = list(group.attrs.get(FALLBACK_ATTR, []))
    except (*STORE_READ_ERRORS, zarr.errors.GroupNotFoundError) as exc:
        if has_committed_data(repo):
            raise StoreReadError(f"{label}: store holds data but could not be read: {exc}") from exc
        exists, raw, fallbacks = False, [], []
    if ledger_contains(ledger_runs(raw), cycle):
        return False

    rows = ds.sizes.get("time", 0)
    with quiet_timedelta_decoding():
        if rows and not exists:
            encoding = build_encoding(ds, append_dim="time")
            for name in [*ds.data_vars, "time"]:
                encoding[name]["chunks"] = (chunk_rows,)
            to_icechunk(ds, session, mode="a", encoding=encoding)
        elif rows:
            to_icechunk(ds, session, append_dim="time")
    group = zarr.open_group(session.store, mode="a")
    for key, value in STORE_ATTRS.items():
        group.attrs[key] = value
    group.attrs[LEDGER_ATTR] = ledger_add(raw, cycle)
    if fallback:
        group.attrs[FALLBACK_ATTR] = sorted(set(fallbacks) | {cycle_label(cycle)})
    session.commit(f"{label}: {rows} observation(s) for cycle {cycle_label(cycle)}")
    return True


# ---------------------------------------------------------------------------- provider


class NCEPUpperAirProvider(StagedFilesMixin, BaseProvider):
    """One GDAS cycle of aircraft and radiosonde observations from NCAR GDEX d337000.

    The provider's own store is the aircraft one, :data:`AMDAR_STORE`;
    :meth:`write_to_icechunk` writes the radiosonde store first and the aircraft store
    last. Partitions are cycle times, every 6 hours from :data:`ARCHIVE_START`, and cover
    the half-open window of three hours either side.

    Args:
        config: Configuration override, mainly for tests.
        prefetch_workers: Cycles decoded at once. Above 1, fetching a cycle also starts
            decoding the ones after it, so a backfill walking forward finds them ready.
            Defaults to ``NCEP_PREPBUFR_WORKERS``, else 1.
        container_memory_gb: Memory cap for each MET container, when pb2nc runs in Docker.
    """

    name = "ncep_amdar"
    append_dim = "time"
    store_prefix = AMDAR_STORE
    partition_freq = CYCLE_FREQ
    staged_glob = "*.nc"
    #: Request timeout for one PREPBUFR file, in seconds.
    download_timeout = 1800
    #: Seconds pb2nc may take on one file; a modern 48-hour file takes about 4 minutes
    #: under emulation.
    decode_timeout = 3600

    def __init__(
        self,
        config: Config | None = None,
        prefetch_workers: int | None = None,
        container_memory_gb: float = 4,
    ):
        super().__init__(config=config)
        if prefetch_workers is None:
            prefetch_workers = int(os.environ.get("NCEP_PREPBUFR_WORKERS") or 1)
        self.prefetch_workers = max(1, int(prefetch_workers))
        self.container_memory_gb = container_memory_gb
        self._repos: Dict[str, icechunk.Repository] = {}
        self._pool: ThreadPoolExecutor | None = None
        self._pending: Dict[pd.Timestamp, Future] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ stores

    def repo_for(self, prefix: str) -> icechunk.Repository:
        """The repository of one of :data:`STORES`; the aircraft one is this provider's."""
        if prefix == self.store_prefix:
            return self.get_icechunk_repo()
        if prefix not in self._repos:
            self._repos[prefix] = self.config.icechunk_repo(prefix)
        return self._repos[prefix]

    def missing_timesteps(self, desired: Iterable) -> List[pd.Timestamp]:
        """Cycles not in the aircraft store's ledger, which is written last for each."""
        runs = read_ledger(self.get_icechunk_repo())
        return [as_cycle(it) for it in desired if not ledger_contains(runs, it)]

    def describe_store(self) -> str:
        """Both stores, radiosondes first, for a log line."""
        return ", ".join(self.config.store_path(prefix) for prefix in STORES)

    # ------------------------------------------------------------------ staging

    @property
    def bufr_dir(self) -> pathlib.Path:
        """Root of the staged conversions, shared with the AMDAR Dagster assets."""
        configured = os.environ.get("AMDAR_BUFR_DIR")
        if configured:
            return pathlib.Path(configured).expanduser()
        return self.config.data_dir / "amdar"

    @property
    def archive_root(self) -> pathlib.Path:
        """Host directory the MET image stages into."""
        return self.bufr_dir

    def staged_dir(self, it) -> pathlib.Path:
        """Where a staged conversion of the cycle is left, by Dagster or by prefetching."""
        return self.bufr_dir / "cycles" / f"{as_cycle(it):%Y%m%d%H}"

    # ------------------------------------------------------------------ fetching

    def convert(self, raw: pathlib.Path, output: pathlib.Path) -> pathlib.Path:
        """Decode one PREPBUFR file with the local pb2nc, else the MET image."""
        work = output.parent
        work.mkdir(parents=True, exist_ok=True)
        config_path = work / f"{raw.name}.pb2nc.cfg"
        try:
            if pb2nc_available():
                config_path.write_text(pb2nc_config_text(work))
                return run_pb2nc(raw, output, config_path, timeout=self.decode_timeout)
            config_path.write_text(pb2nc_config_text(CONTAINER_TMP_DIR))
            return run_pb2nc_docker(
                raw,
                output,
                config_path,
                memory_gb=self.container_memory_gb,
                timeout=self.decode_timeout,
            )
        finally:
            config_path.unlink(missing_ok=True)

    def download(self, cycle: pd.Timestamp, dest_dir: pathlib.Path) -> pathlib.Path | None:
        """Download the best available file for a cycle; None when the archive has none.

        Raises:
            PrepbufrUnavailable: A file could not be downloaded but is not known absent.
        """
        for url in source_urls(cycle):
            dest = dest_dir / url.rsplit("/", 1)[-1]
            if download_one(url, dest, timeout=self.download_timeout) is not None:
                if ".48h" not in dest.name and cycle >= PREP48H_START:
                    logger.warning(f"{self.name}: no 48-hour file for {cycle}, using {dest.name}")
                return dest
            if not url_is_absent(url):
                raise PrepbufrUnavailable(f"could not download {url}")
        logger.info(f"{self.name}: GDEX has no PREPBUFR for cycle {cycle}")
        return None

    def stage(self, it) -> List[pathlib.Path]:
        """Download and decode one cycle into :meth:`staged_dir`, returning its NetCDF."""
        cycle = as_cycle(it)
        staged = self.staged_files(cycle) if self.staged_dir(cycle).is_dir() else []
        if staged:
            return staged
        target = self.staged_dir(cycle)
        target.mkdir(parents=True, exist_ok=True)
        raw = self.download(cycle, target)
        if raw is None:
            shutil.rmtree(target, ignore_errors=True)
            return []
        try:
            return [self.convert(raw, target / f"{raw.name}.nc")]
        finally:
            raw.unlink(missing_ok=True)

    def _submit(self, cycle: pd.Timestamp) -> Future:
        with self._lock:
            if cycle not in self._pending:
                if self._pool is None:
                    self._pool = ThreadPoolExecutor(
                        max_workers=self.prefetch_workers, thread_name_prefix="pb2nc"
                    )
                self._pending[cycle] = self._pool.submit(self.stage, cycle)
            return self._pending[cycle]

    def fetch(self, it, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """The cycle's decoded NetCDF: staged already, or downloaded and decoded now.

        With :attr:`prefetch_workers` above 1 the following cycles are queued for decoding
        too. An empty list means GDEX has no file for the cycle.
        """
        cycle = as_cycle(it)
        if self.prefetch_workers > 1:
            future = self._submit(cycle)
            published = to_naive_utc(pd.Timestamp.now(tz="UTC")) - pd.Timedelta(
                PUBLICATION_LAG_HOURS, "h"
            )
            stored = read_ledger(self.get_icechunk_repo())
            for step in range(1, self.prefetch_workers):
                ahead = cycle + step * CYCLE
                if ahead <= published and not ledger_contains(stored, ahead):
                    self._submit(ahead)
            try:
                paths = future.result()
            finally:
                with self._lock:
                    self._pending.pop(cycle, None)
        else:
            paths = self.stage(cycle)
        return [str(p) for p in paths]

    # ------------------------------------------------------------------ processing

    def process(
        self,
        input_files: List[str],
        it,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Read the decoded cycle into merged observation points, clipped to its window."""
        cycle = as_cycle(it)
        tables = [read_pb2nc(path) for path in input_files]
        tables = [t for t in tables if not t.empty]
        names = [pathlib.Path(p).name for p in input_files]
        attrs = {
            "cycle": cycle_label(cycle),
            "source_files": ",".join(names),
            "fallback": int(any(is_restricted_fallback(n, cycle) for n in names)),
        }
        if not tables:
            return empty_dataset(attrs)
        table = pd.concat(tables, ignore_index=True)
        times = pd.to_datetime(table["time"])
        inside = (times >= cycle - HALF_WINDOW) & (times < cycle + HALF_WINDOW)
        if not inside.all():
            logger.warning(
                f"{self.name}: dropping {int((~inside).sum())} of {len(table)} observation(s) "
                f"outside cycle {cycle_label(cycle)}'s window"
            )
        ds = table_to_dataset(table.loc[inside], attrs)
        counts = pd.Series(ds["message_type"].values).value_counts().to_dict()
        logger.info(f"{self.name}: cycle {cycle_label(cycle)}: {counts}")
        return ds

    def write_to_icechunk(self, repo: icechunk.Repository, processed: xr.Dataset) -> bool:
        """Write the radiosonde store, then the aircraft store, each with its ledger.

        Returns whether the aircraft store took the cycle. A staged conversion is removed
        once both stores hold it.
        """
        cycle = as_cycle(pd.Timestamp(processed.attrs["cycle"]))
        fallback = bool(processed.attrs.get("fallback", 0))
        message_types = processed["message_type"].values
        written = False
        for prefix, kinds in STORES.items():
            subset = processed.isel(time=np.flatnonzero(np.isin(message_types, kinds)))
            target = repo if prefix == self.store_prefix else self.repo_for(prefix)
            label = prefix.rsplit("/", 1)[-1].removesuffix(".icechunk")
            written = append_with_ledger(target, subset, cycle, label, fallback=fallback)
        self.discard_staged(cycle, settled=True)
        return written

    def close(self) -> None:
        """Stop the prefetch pool, abandoning queued decodes."""
        if self._pool is not None:
            self._pool.shutdown(wait=False, cancel_futures=True)
            self._pool = None
