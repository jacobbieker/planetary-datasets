"""NOAA OSMC / AOML marine observations as ragged point stores, 2012 to the present.

One row per observation, along a ``time`` dimension whose values repeat: a day holds a few
hundred thousand reports from tens of thousands of platforms, a different number every day,
so a dense ``(platform, time)`` grid does not fit them. The stores are:

* ``bkr/aoml/v2/osmc_all.icechunk`` — every observation;
* ``bkr/aoml/v2/osmc_<type>.icechunk`` — one per platform type, e.g. ``osmc_drifting_buoys``.
  The ``(GENERIC)`` spelling of a type shares a store with the plain one, and each row keeps
  its original ``platform_type``. A type first seen upstream gets a store when it appears.

Sources. NOAA's ERDDAP (``OSMC_flattened``, which AOML's ``OSMC_RealTime`` now redirects to)
starts on 2016-01-01. Earlier years only survive in the yearly stores published under
``bkr/aoml/aoml_<year>.icechunk``, built when more of the record was online; those are flat,
time-sorted and carry the same variables bar ``country``. Days before :data:`ERDDAP_START`
are sliced out of them, later days are fetched from ERDDAP, one request a day.

Bookkeeping. Every store records the days it holds in its group attributes
(:data:`LEDGER_ATTR`), updated in the same commit as the rows. "Is this day stored?" is then
one attribute read rather than a scan of billions of timestamps, and a day interrupted
half-way through its stores is resumed without duplicating the stores it already reached:
each type store is skipped when it already lists the day, and ``osmc_all`` is written last,
so its ledger is the record of which days are complete.
"""

from __future__ import annotations

import pathlib
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List
from urllib.parse import quote

import icechunk
import numpy as np
import pandas as pd
import requests
import xarray as xr
import zarr
from icechunk.xarray import to_icechunk
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common import naming
from planetary_datasets.common.store import (
    STORE_READ_ERRORS,
    StoreReadError,
    build_encoding,
    has_committed_data,
    quiet_timedelta_decoding,
)
from planetary_datasets.providers.observations._points import _decode_block, naive_utc
from planetary_datasets.providers.observations.osmc import (
    NO_RESULTS_MARKER,
    OSMC_VARIABLES,
    rows_to_time_dim,
)

ERDDAP_FLATTENED = "https://osmc.noaa.gov/erddap/tabledap/OSMC_flattened.nc"
#: First day ERDDAP serves. Everything before comes from the yearly stores.
ERDDAP_START = pd.Timestamp("2016-01-01")
ARCHIVE_START = pd.Timestamp("2012-01-01")
YEARLY_STORE = "bkr/aoml/aoml_{year}.icechunk"
STORE_BASE = "bkr/aoml/v2"
ALL_STORE = f"{STORE_BASE}/osmc_all.icechunk"
LEDGER_ATTR = "ingested_days"

STRING_VARS = ("platform_code", "platform_type", "country")
#: Kept at full precision: a float32 latitude is good to about a metre, but platform ids
#: run past float32's 24-bit integer range.
FLOAT64_VARS = ("platform_id", "latitude", "longitude")
#: Units as ERDDAP publishes them (and as the yearly stores carry them).
UNITS: Dict[str, str] = {
    "latitude": "degrees_north",
    "longitude": "degrees_east",
    "observation_depth": "m",
    "sst": "Deg C",
    "atmp": "Deg C",
    "dewpoint": "Deg C",
    "ztmp": "Deg C",
    "precip": "mm",
    "rainfall_rate": "m s-1",
    "slp": "hPa",
    "windspd": "m/s",
    "winddir": "Deg true",
    "wind_to_direction": "degree",
    "wvht": "m",
    "waterlevel": "m",
    "waterlevel_met_res": "m",
    "waterlevel_wrt_lcd": "m",
    "water_col_ht": "m",
    "clouds": "oktas",
    "uo": "m s-1",
    "vo": "m s-1",
    "wo": "m s-1",
    "sss": "1",
    "rlds": "W m-2",
    "rsds": "W m-2",
    "sea_water_elec_conductivity": "S m-1",
    "sea_water_pressure": "dbar",
}

#: Rows per chunk along ``time``.
CHUNK_ROWS = 250_000


def type_slug(platform_type) -> str:
    """Store key for a ``platform_type``: the ``(GENERIC)`` spelling joins the plain one."""
    label = re.sub(r"\s*\(GENERIC\)\s*$", "", str(platform_type or "").strip(), flags=re.I)
    slug = re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")
    return slug or "unknown"


def store_prefix_for(slug: str) -> str:
    """Store holding one platform type."""
    return f"{STORE_BASE}/osmc_{slug}.icechunk"


def day_url(day: pd.Timestamp) -> str:
    """ERDDAP request for one UTC day. Its time bounds are inclusive at both ends."""
    start = naive_utc(day).strftime("%Y-%m-%dT%H:%M:%SZ")
    end = (naive_utc(day) + pd.Timedelta(1, "D") - pd.Timedelta(1, "s")).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    return (
        f"{ERDDAP_FLATTENED}?{quote(','.join(OSMC_VARIABLES), safe='()')}"
        f"&time%3E={quote(start)}&time%3C={quote(end)}"
    )


def normalise(ds: xr.Dataset) -> xr.Dataset:
    """Put either source into one schema: every OSMC variable, fixed dtypes, time-sorted.

    Strings are stored variable-length, so no width has to be guessed up front, with
    missing values as empty strings; ``country`` is absent before 2016 and comes out empty.
    """
    if "time" not in ds.dims:
        ds = rows_to_time_dim(ds)
    ds = ds.reset_coords([c for c in ds.coords if c != "time"]).drop_encoding()
    n = ds.sizes["time"]
    out = {}
    for name in OSMC_VARIABLES:
        if name == "time":
            continue
        if name in STRING_VARS:
            values = ds[name].values if name in ds else np.full(n, "", dtype=object)
            values = np.array(
                [
                    "" if v is None or (isinstance(v, float) and np.isnan(v)) else str(v)
                    for v in values
                ],
                dtype=object,
            )
        else:
            dtype = "float64" if name in FLOAT64_VARS else "float32"
            values = ds[name].values.astype(dtype) if name in ds else np.full(n, np.nan, dtype)
        out[name] = ("time", values)
    times = np.asarray(ds["time"].values, dtype="datetime64[ns]")
    result = xr.Dataset(out, coords={"time": times})
    for name, units in UNITS.items():
        result[name].attrs["units"] = units
    order = np.argsort(times, kind="stable")
    return result.isel(time=order)


def _yearly_times(store_path: str, repo: icechunk.Repository) -> np.ndarray:
    """A yearly store's whole time axis, ~1 GB for a year."""
    group = zarr.open_group(repo.readonly_session("main").store, mode="r")
    times = _decode_block(np.asarray(group["time"][:]), dict(group["time"].attrs))
    if times.size and not bool((np.diff(times) >= np.timedelta64(0, "ns")).all()):
        raise ValueError(f"{store_path}: time is not sorted, so days cannot be sliced out")
    return times


#: Chunks of ``time`` per manifest piece: CHUNK_ROWS (250,000) x 100 = 25 million rows.
MANIFEST_SPLIT_CHUNKS = 100


def repository_config() -> icechunk.RepositoryConfig:
    """Repository settings for the OSMC stores: manifests split along ``time``.

    Unsplit, every array's manifest lists all of its chunks and a commit rewrites it whole;
    split, an append rewrites only the last piece, so a commit's work and memory stay
    bounded however long the store grows.
    """
    splitting = icechunk.config.ManifestSplittingConfig.from_dict(
        {
            icechunk.config.ManifestSplitCondition.AnyArray(): {
                icechunk.config.ManifestSplitDimCondition.DimensionName(
                    "time"
                ): MANIFEST_SPLIT_CHUNKS
            }
        }
    )
    return icechunk.RepositoryConfig(manifest=icechunk.config.ManifestConfig(splitting=splitting))


def _append_rows(session: icechunk.Session, ds: xr.Dataset) -> None:
    """Append ``ds`` along ``time`` without reading what the store already holds.

    ``to_icechunk(..., append_dim="time")`` opens the store through xarray, which builds a
    pandas index from the whole ``time`` coordinate; ``osmc_all`` passed 580 million rows
    and that read alone took over five minutes, growing with every day appended. Here
    each array along ``time`` is grown in zarr and the new rows are written as a region.
    String columns are assigned in zarr directly: xarray reads the whole existing column
    before writing a region of a variable-length string array (1.35 GB per 20 million
    rows), which took the lane past 100 GB on the full stores.
    """
    group = zarr.open_group(session.store, mode="a")
    start = group["time"].shape[0]
    stop = start + ds.sizes["time"]
    along = [name for name in ds.variables if "time" in ds[name].dims]
    for name in along:
        array = group[name]
        array.resize((stop, *array.shape[1:]))
    # Values go to zarr as they are: a region write through xarray would re-encode them
    # with the stored scale_factor/add_offset (the ERA5 unit conversions) and divide them.
    # Only datetimes need xarray's encoding.
    times = [name for name in along if ds[name].dtype.kind in "Mm"]
    for name in along:
        if name in times:
            continue
        values = ds[name].values
        group[name][start:stop] = (
            np.asarray(values, dtype=object) if values.dtype.kind in "OUS" else values
        )
    if times:
        # Without its index, time is written like any other variable; as an index
        # coordinate a region write leaves it unwritten.
        rest = ds[times].drop_indexes("time")
        to_icechunk(rest, session, region={"time": slice(start, stop)})


def _append_with_ledger(
    repo: icechunk.Repository, ds: xr.Dataset, day: str, label: str, prefix: str = ALL_STORE
) -> bool:
    """Append one day's rows and add the day to the store's ledger, in one commit.

    Skips (False) when the ledger already lists the day. A day with no rows for an existing
    store still records the day, so it is not asked for again.
    """
    session = repo.writable_session("main")
    try:
        group = zarr.open_group(session.store, mode="r")
        exists = "time" in group
        ledger = list(group.attrs.get(LEDGER_ATTR, [])) if exists else []
    except (*STORE_READ_ERRORS, zarr.errors.GroupNotFoundError) as exc:
        if has_committed_data(repo):
            raise StoreReadError(f"{label}: store holds data but could not be read: {exc}") from exc
        exists, ledger = False, []
    if day in ledger:
        return False
    stored = set(group.array_keys()) if exists else set()
    ds = naming.prepare_for_store(ds, prefix, stored, raw=True)
    rows = ds.sizes.get("time", 0)
    if not exists and rows == 0:
        return False

    with quiet_timedelta_decoding():
        if not exists:
            encoding = build_encoding(ds, append_dim="time")
            for name in ds.data_vars:
                encoding[name]["chunks"] = (CHUNK_ROWS,)
            encoding["time"]["chunks"] = (CHUNK_ROWS,)
            to_icechunk(ds, session, encoding=encoding)
        elif rows:
            _append_rows(session, ds)
    group = zarr.open_group(session.store, mode="a")
    group.attrs[LEDGER_ATTR] = sorted(set(ledger) | {day})
    session.commit(f"{label}: {rows} observation(s) for {day}")
    return True


def ledger(repo: icechunk.Repository) -> set[str]:
    """Days a store holds, from its ledger; empty for a store not created yet."""
    try:
        group = zarr.open_group(repo.readonly_session("main").store, mode="r")
    except (*STORE_READ_ERRORS, zarr.errors.GroupNotFoundError):
        return set()
    return set(group.attrs.get(LEDGER_ATTR, []))


class OSMCRaggedProvider(BaseProvider):
    """One UTC day of OSMC observations, written to ``osmc_all`` and every type store.

    The provider's own store is ``osmc_all``; :meth:`write_to_icechunk` fans each day out
    to the type stores first and writes ``osmc_all`` last.

    Attributes:
        write_workers: Type stores written at once. They are separate stores, so this does
            not put two writers on one.
    """

    name = "osmc_ragged"
    append_dim = "time"
    store_prefix = ALL_STORE
    #: Each type store is named in ``_append_with_ledger``, writing raw values.
    names_in_writer = True
    request_timeout = 1800
    write_workers = 8

    def __init__(self, config=None):
        super().__init__(config=config)
        self._type_repos: Dict[str, icechunk.Repository] = {}
        #: One yearly store's time axis, kept while a backfill walks through that year.
        self._yearly: tuple[str, np.ndarray] | None = None

    def type_repo(self, slug: str) -> icechunk.Repository:
        if slug not in self._type_repos:
            self._type_repos[slug] = self.config.icechunk_repo(
                store_prefix_for(slug), config=repository_config()
            )
        return self._type_repos[slug]

    def repository_config(self) -> icechunk.RepositoryConfig:
        """Manifests split along ``time``; see :func:`repository_config`."""
        return repository_config()

    def missing_timesteps(self, desired) -> List[pd.Timestamp]:
        """Days not yet in ``osmc_all``'s ledger, which is written last for every day."""
        done = ledger(self.get_icechunk_repo())
        return [it for it in desired if naive_utc(it).strftime("%Y-%m-%d") not in done]

    # ------------------------------------------------------------------ sources

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """The day's ERDDAP file, or a reference to the yearly store for days before 2016.

        ERDDAP answers a day with no reports with 404 and a body saying so; that is an
        empty day. Anything else that is not a 200 raises, so the day is retried.
        """
        day = naive_utc(it).normalize()
        if day < ERDDAP_START:
            return [f"yearly:{YEARLY_STORE.format(year=day.year)}"]
        dest_dir = pathlib.Path(temp_dir) if temp_dir is not None else self.config.scratch_dir
        dest = dest_dir / f"osmc_{day:%Y%m%d}.nc"
        dest.parent.mkdir(parents=True, exist_ok=True)
        response = requests.get(day_url(day), timeout=self.request_timeout, stream=True)
        try:
            if response.status_code == 404 and NO_RESULTS_MARKER in response.text.lower():
                logger.info(f"{self.name}: ERDDAP has no observations for {day:%Y-%m-%d}")
                return []
            response.raise_for_status()
            part = dest.with_name(dest.name + ".part")
            with open(part, "wb") as handle:
                for chunk in response.iter_content(chunk_size=8 << 20):
                    handle.write(chunk)
            if part.stat().st_size == 0:
                part.unlink()
                raise RuntimeError(f"ERDDAP returned an empty body for {day:%Y-%m-%d}")
            part.replace(dest)
        finally:
            response.close()
        return [str(dest)]

    def read_yearly_day(self, prefix: str, day: pd.Timestamp) -> xr.Dataset:
        """The rows of one day, sliced from a time-sorted yearly store."""
        repo = self.config.icechunk_repo(prefix)
        if self._yearly is None or self._yearly[0] != prefix:
            self._yearly = (prefix, _yearly_times(self.config.store_path(prefix), repo))
        times = self._yearly[1]
        lo, hi = (
            int(i)
            for i in np.searchsorted(
                times, [np.datetime64(day, "ns"), np.datetime64(day + pd.Timedelta(1, "D"), "ns")]
            )
        )
        # Sliced straight from zarr: xr.open_zarr would rebuild the year's whole time index
        # (~100M entries) on every call. The arrays hold no scale or offset packing, and
        # their missing values are stored as NaN, so the raw values are the decoded ones.
        group = zarr.open_group(repo.readonly_session("main").store, mode="r")
        columns = {
            name: ("time", np.asarray(group[name][lo:hi]))
            for name in group.array_keys()
            if name != "time" and name in OSMC_VARIABLES
        }
        return xr.Dataset(columns, coords={"time": times[lo:hi]})

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Read the day from its source into the shared schema, clipped to the day."""
        day = naive_utc(it).normalize()
        source = input_files[0]
        if source.startswith("yearly:"):
            raw = self.read_yearly_day(source.removeprefix("yearly:"), day)
        else:
            with xr.open_dataset(source) as handle:
                raw = handle.load()
        ds = normalise(raw)
        times = ds["time"].values
        keep = (times >= np.datetime64(day, "ns")) & (
            times < np.datetime64(day + pd.Timedelta(1, "D"), "ns")
        )
        ds = ds.isel(time=np.flatnonzero(keep))
        logger.info(f"{self.name}: {ds.sizes['time']} observation(s) for {day:%Y-%m-%d}")
        return ds

    # ------------------------------------------------------------------ writing

    def write_to_icechunk(self, repo: icechunk.Repository, processed: xr.Dataset) -> bool:
        """Write each type store, then ``osmc_all``, each with its ledger, for one day."""
        times = processed["time"].values
        if times.size == 0:
            return False
        day = pd.Timestamp(times[0]).strftime("%Y-%m-%d")
        slugs = np.array([type_slug(t) for t in processed["platform_type"].values], dtype=object)

        def write_type(slug: str) -> tuple[str, bool]:
            subset = processed.isel(time=np.flatnonzero(slugs == slug))
            return slug, _append_with_ledger(
                self.type_repo(slug), subset, day, f"osmc_{slug}", store_prefix_for(slug)
            )

        with ThreadPoolExecutor(max_workers=self.write_workers) as pool:
            results = dict(pool.map(write_type, sorted(set(slugs))))
        logger.info(
            f"{self.name}: {day} -> {sum(results.values())} type store(s) written, "
            f"{len(results) - sum(results.values())} already held it"
        )
        return _append_with_ledger(repo, processed, day, "osmc_all", ALL_STORE)
