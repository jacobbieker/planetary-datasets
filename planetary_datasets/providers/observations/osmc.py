"""NOAA OSMC in-situ marine observations from the GTS (drifters, floats, ships, gauges).

The Observing System Monitoring Center republishes the marine reports that reach the
Global Telecommunication System through NOAA/AOML's ERDDAP server:
https://erddap.aoml.noaa.gov/gdp/erddap/tabledap/OSMC_RealTime.html

Access is open, so no credential is read here and no new environment variable is
introduced. Store locations follow the usual ``ICECHUNK_*`` settings; downloads use
``PLANETARY_DATASETS_DATA_DIR`` / the partition's temporary directory.

``OSMC_RealTime`` serves a rolling window rather than the full record, so a backfill of
older months returns nothing for them (see :data:`NO_RESULTS_MARKER`). Keeping the
partitions running from 2012 is still right: the published ``bkr/aoml/*`` stores were
built when more history was online, and this keeps appending to them as new months
arrive without renumbering anything.

ERDDAP hands back a netCDF whose single dimension is ``row``: one row per observation,
with ``time``, ``latitude``, ``longitude`` and the platform identifiers repeated per row.
:func:`rows_to_time_dim` turns that into the layout the store uses — ``time`` as the
dimension coordinate, everything else a data variable. Keeping latitude, longitude and
the platform identifiers as *variables* rather than coordinates matters: they vary per
observation, and promoting them to coordinates makes every downstream ``sel`` and every
append alignment check treat them as an index they are not.
"""

from __future__ import annotations

import pathlib
from typing import Dict, List, Sequence, Tuple
from urllib.parse import quote

import numpy as np
import pandas as pd
import requests
import xarray as xr
from loguru import logger

from planetary_datasets.providers.observations._points import (
    PointObservationProvider,
    clip_to_window,
    naive_utc,
    widen_string_vars,
)

ERDDAP_BASE = "https://erddap.aoml.noaa.gov/gdp/erddap/tabledap/OSMC_RealTime.nc"

#: ERDDAP answers a query that matched nothing with HTTP 404 and this in the body. It is
#: not an error: it means the month genuinely holds no reports of that platform type.
#: ``OSMC_RealTime`` keeps a rolling window, so every month older than that window
#: answers this way and the partition is correctly recorded as empty.
NO_RESULTS_MARKER = "your query produced no matching results"

#: The full variable list requested from ERDDAP. Ordering is significant to ERDDAP: the
#: first entries become the response's leading columns.
OSMC_VARIABLES: Tuple[str, ...] = (
    "platform_id",
    "platform_code",
    "platform_type",
    "country",
    "time",
    "latitude",
    "longitude",
    "observation_depth",
    "sst",
    "atmp",
    "precip",
    "sss",
    "ztmp",
    "zsal",
    "slp",
    "windspd",
    "winddir",
    "wvht",
    "waterlevel",
    "clouds",
    "dewpoint",
    "uo",
    "vo",
    "wo",
    "rainfall_rate",
    "hur",
    "sea_water_elec_conductivity",
    "sea_water_pressure",
    "rlds",
    "rsds",
    "waterlevel_met_res",
    "waterlevel_wrt_lcd",
    "water_col_ht",
    "wind_to_direction",
)

#: Store key -> the ``platform_type`` value ERDDAP filters on. ``all`` applies no filter.
PLATFORM_TYPES: Dict[str, str | None] = {
    "drifters": "DRIFTING BUOYS (GENERIC)",
    "ships": "SHIPS (GENERIC)",
    "moored": "MOORED BUOYS (GENERIC)",
    "shore": "SHORE AND BOTTOM STATIONS (GENERIC)",
    "tide": "TIDE GAUGE STATIONS (GENERIC)",
    "glider": "PROFILING FLOATS AND GLIDERS (GENERIC)",
    "weather": "WEATHER BUOYS",
    "station": "C-MAN WEATHER STATIONS",
    "volunteer_ships": "VOLUNTEER OBSERVING SHIPS (GENERIC)",
    "vosclim": "VOSCLIM",
    "all": None,
}

#: Every ``platform_type`` value seen in the archive, as surveyed for the AOML split.
ALL_PLATFORM_TYPES: Tuple[str, ...] = (
    "C-MAN WEATHER STATIONS",
    "DRIFTING BUOYS",
    "DRIFTING BUOYS (GENERIC)",
    "GLIDERS",
    "GLOSS",
    "ICE BUOYS",
    "MOORED BUOYS",
    "MOORED BUOYS (GENERIC)",
    "PROFILING FLOATS AND GLIDERS",
    "PROFILING FLOATS AND GLIDERS (GENERIC)",
    "RESEARCH",
    "SHIPS",
    "SHIPS (GENERIC)",
    "SHORE AND BOTTOM STATIONS (GENERIC)",
    "TAGGED ANIMAL",
    "TIDE GAUGE STATIONS (GENERIC)",
    "TROPICAL MOORED BUOYS",
    "TSUNAMI WARNING STATIONS",
    "UNCREWED SURFACE VEHICLE",
    "UNKNOWN",
    "VOLUNTEER OBSERVING SHIPS",
    "VOLUNTEER OBSERVING SHIPS (GENERIC)",
    "VOSCLIM",
    "WEATHER AND OCEAN OBS",
    "WEATHER BUOYS",
    "WEATHER OBS",
)


def store_prefix_for(dataset: str) -> str:
    """Store prefix for an OSMC platform group, matching the published AOML layout."""
    if dataset not in PLATFORM_TYPES:
        raise ValueError(
            f"unknown OSMC dataset {dataset!r}; expected one of {sorted(PLATFORM_TYPES)}"
        )
    return f"bkr/aoml/aoml_{dataset}.icechunk"


def platform_categories(types: Sequence[str] = ALL_PLATFORM_TYPES) -> Dict[str, List[str]]:
    """Group raw ``platform_type`` values into the coarse categories used downstream.

    Reproduces the grouping from the AOML splitter, which sorts by substring rather than
    an explicit table so that new upstream platform types land in a sensible bucket
    instead of disappearing.
    """
    return {
        "buoys": [t for t in types if "BUOY" in t],
        "ships": [
            t
            for t in types
            if "SHIP" in t or "UNCREWED SURFACE VEHICLE" in t or "VOSCLIM" in t
        ],
        "stations": [t for t in types if "STATION" in t],
        "obs": [t for t in types if "OBS" in t],
        "gliders": [t for t in types if "GLIDER" in t or "FLOAT" in t],
    }


def _stamp(value) -> str:
    return pd.Timestamp(value).strftime("%Y-%m-%dT%H:%M:%SZ")


def osmc_url(
    start,
    end,
    platform_type: str | None = None,
    variables: Sequence[str] = OSMC_VARIABLES,
) -> str:
    """Build the ERDDAP tabledap request for a time range and optional platform type.

    ``>=`` and ``<=`` are written with the comparison character percent-encoded and the
    ``=`` left literal, which is the form ERDDAP's own URL builder emits.
    """
    url = f"{ERDDAP_BASE}?{quote(','.join(variables), safe='()')}"
    if platform_type:
        url += "&platform_type=" + quote(f'"{platform_type}"', safe="()")
    url += "&time%3E=" + quote(_stamp(start), safe="()")
    url += "&time%3C=" + quote(_stamp(end), safe="()")
    return url


def rows_to_time_dim(ds: xr.Dataset, row_dim: str | None = None) -> xr.Dataset:
    """Swap ERDDAP's ``row`` dimension for ``time``, leaving everything else a variable.

    This is the reshape that ``aoml_owner.py`` sketched, made robust to the dimension
    being named something other than ``row`` and to ``time`` already being a coordinate.
    """
    if "time" not in ds.variables:
        raise ValueError("dataset has no 'time' variable to index by")

    if row_dim is None:
        dims = list(ds["time"].dims)
        if len(dims) != 1:
            raise ValueError(f"expected 'time' to be 1-D, got dims {dims}")
        row_dim = dims[0]

    if row_dim != "time":
        ds = ds.set_coords("time").swap_dims({row_dim: "time"})
        # ERDDAP usually leaves 'row' as a bare dimension, but drop it if it is also a
        # variable so the reshaped dataset carries no stale index.
        ds = ds.drop_vars(row_dim, errors="ignore")
    elif "time" not in ds.coords:
        ds = ds.set_coords("time")
    # Anything ERDDAP marked as a coordinate other than time is a per-observation value,
    # not an index; demote it so appends and selections behave.
    extra_coords = [name for name in ds.coords if name != "time"]
    if extra_coords:
        ds = ds.reset_coords(extra_coords)
    return ds


def reorganise_by_platform_id(ds: xr.Dataset, chunk_time: int = 1000) -> xr.Dataset:
    """Re-index a flat OSMC dataset as ``(platform_id, time)``.

    Carried over from the AOML reorg script. Every platform is reindexed onto the union
    of all observation times before concatenating, so the result is a dense rectangle
    with NaN where a platform did not report.

    This materialises the whole cross product and is only appropriate for a subset small
    enough to fit in memory — a few hundred platforms over a month, not the full archive.
    """
    if "platform_id" not in ds.variables:
        raise ValueError("dataset has no 'platform_id' variable")

    platform_ids = np.unique(np.asarray(ds["platform_id"].values))
    logger.info(f"reorganising {len(platform_ids)} platform(s) by id")

    per_platform: List[xr.Dataset] = []
    all_times: List[np.ndarray] = []
    for platform_id in platform_ids:
        selected = np.flatnonzero(np.asarray(ds["platform_id"].values) == platform_id)
        if selected.size == 0:
            continue
        platform_ds = ds.isel(time=selected).drop_duplicates("time")
        platform_type = np.asarray(platform_ds["platform_type"].values).ravel()
        platform_ds = platform_ds.drop_vars(
            [v for v in ("platform_id", "platform_type") if v in platform_ds.variables]
        )
        platform_ds = platform_ds.expand_dims({"platform_id": [platform_id]})
        if platform_type.size:
            # Attach the type *along* platform_id. A bare list would make it a new
            # standalone dimension, which concat then unions across platforms, leaving
            # a platform_type axis with no connection to the platform it describes.
            platform_ds = platform_ds.assign_coords(
                platform_type=("platform_id", [platform_type[0]])
            )
        all_times.append(np.asarray(platform_ds["time"].values))
        per_platform.append(platform_ds)

    if not per_platform:
        raise ValueError("no platforms with observations to reorganise")

    times = np.sort(np.unique(np.concatenate(all_times)))
    per_platform = [p.reindex({"time": times}) for p in per_platform]
    combined = xr.concat(per_platform, dim="platform_id", join="outer")
    return combined.chunk({"time": chunk_time, "platform_id": -1})


class OSMCProvider(PointObservationProvider):
    """One month of NOAA OSMC GTS observations for a single platform group.

    Args:
        dataset: A key of :data:`PLATFORM_TYPES`, e.g. ``drifters`` or ``all``.
        config: Optional configuration override.
    """

    append_dim = "time"
    partition_freq = "MS"
    #: Rows per chunk. Matches the AOML stores already published under this prefix.
    chunk_rows = 100_000
    #: Seconds to wait for ERDDAP. A busy month can take several minutes to assemble.
    request_timeout = 1800

    def __init__(self, dataset: str = "drifters", config=None):
        self.store_prefix = store_prefix_for(dataset)
        super().__init__(config=config)
        self.dataset = dataset
        self.name = f"osmc_{dataset}"

    @property
    def platform_type(self) -> str | None:
        return PLATFORM_TYPES[self.dataset]

    def url_for(self, it) -> str:
        """The ERDDAP request covering the month starting at ``it``."""
        start, end = self.partition_bounds(it)
        # ERDDAP's time constraint is inclusive at both ends, so ask for one second less
        # than the next partition's start rather than duplicating a boundary observation.
        return osmc_url(start, end - np.timedelta64(1, "s"), platform_type=self.platform_type)

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Download one month of observations.

        ``requests`` is used directly rather than :func:`~planetary_datasets.common
        .download.download_one` because the status code has to be read: ERDDAP answers a
        query that matched nothing with **404 and a body saying so**, which is a genuine
        empty partition, while any other failure is transient and must raise so the
        partition is retried instead of being recorded as permanently done.
        """
        url = self.url_for(it)
        month = naive_utc(it).strftime("%Y-%m")
        dest_dir = pathlib.Path(temp_dir) if temp_dir is not None else self.local_dir("osmc")
        dest = pathlib.Path(dest_dir) / f"osmc_{self.dataset}_{month}.nc"
        dest.parent.mkdir(parents=True, exist_ok=True)

        response = requests.get(url, timeout=self.request_timeout, stream=True)
        try:
            if response.status_code == 404 and NO_RESULTS_MARKER in response.text.lower():
                logger.info(f"{self.name}: no observations for {month}")
                return []
            response.raise_for_status()
            # Land on a sidecar and rename, so an interrupted transfer never leaves a
            # truncated netCDF that a later run would happily try to open.
            part = dest.with_name(dest.name + ".part")
            written = 0
            with open(part, "wb") as handle:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    handle.write(chunk)
                    written += len(chunk)
            if written == 0:
                part.unlink(missing_ok=True)
                raise RuntimeError(f"OSMC returned an empty body for {self.dataset} {month}")
            part.replace(dest)
        finally:
            response.close()
        return [str(dest)]

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Reshape the ERDDAP response into the flat, time-indexed point layout.

        Each source file is read inside a ``with`` and loaded eagerly: leaving the
        result lazily bound to a handle inside the partition's temporary directory
        leaks a file descriptor per partition across a multi-year backfill.
        """
        parts = []
        for path in input_files:
            with xr.open_dataset(path) as raw:
                parts.append(rows_to_time_dim(raw).load())
        ds = parts[0] if len(parts) == 1 else xr.concat(parts, dim="time")
        ds = ds.sortby("time").drop_encoding()
        ds = widen_string_vars(ds)

        start, end = self.partition_bounds(it)
        ds = clip_to_window(ds, start, end)
        rows = ds.sizes.get("time", 0)
        logger.info(f"{self.name}: {rows} observation(s) for {naive_utc(it):%Y-%m}")
        return ds.chunk({"time": min(self.chunk_rows, max(1, rows))})
