"""Meteostat bulk hourly station observations.

Meteostat aggregates national weather services into one hourly archive, published as
gzipped CSV, one file per station per year:

    https://data.meteostat.net/hourly/{year}/{station}.csv.gz

The roster comes from ``https://bulk.meteostat.net/v2/stations/lite.json.gz``, which also
carries each station's hourly inventory window; stations whose inventory does not overlap
the partition are not requested.

Replaces ``dags/assets/observation/meteostat.py``, a module-level loop that ran on import
and wrote into ``/Users/jacob/Development/meteostat_data``. Behaviour changes: nothing
runs on import, the year file is cached under ``PLANETARY_DATASETS_DATA_DIR`` so the
twelve monthly partitions of a year share one download, and the result is written to
Icechunk instead of being left as loose CSV.

Environment: ``PLANETARY_DATASETS_DATA_DIR`` (roster and year-file cache). No credentials.
"""

from __future__ import annotations

import gzip
import io
import json
import pathlib

import fsspec
import pandas as pd
from loguru import logger

from planetary_datasets.providers.observations.base import (
    Station,
    StationObservationProvider,
    partial_path,
)

STATIONS_URL = "https://bulk.meteostat.net/v2/stations/lite.json.gz"
HOURLY_URL = "https://data.meteostat.net/hourly/{year}/{station_id}.csv.gz"

#: Numeric columns of the bulk hourly CSV. Each has a matching ``*_source`` column naming
#: the provider, which is text and is dropped.
VARIABLES = (
    "temp",
    "rhum",
    "prcp",
    "snwd",
    "wdir",
    "wspd",
    "wpgt",
    "pres",
    "tsun",
    "cldc",
    "coco",
)


def _station_entry(entry: dict) -> tuple[Station, pd.Timestamp | None, pd.Timestamp | None]:
    """Turn one roster entry into a station plus its hourly inventory window."""
    location = entry.get("location") or {}
    station = Station(
        id=str(entry["id"]),
        latitude=location.get("latitude"),
        longitude=location.get("longitude"),
        elevation=location.get("elevation"),
        name=(entry.get("name") or {}).get("en"),
    )
    hourly = ((entry.get("inventory") or {}).get("hourly")) or {}
    start = pd.to_datetime(hourly.get("start"), errors="coerce")
    end = pd.to_datetime(hourly.get("end"), errors="coerce")
    return station, (None if pd.isna(start) else start), (None if pd.isna(end) else end)


def load_roster(url: str = STATIONS_URL) -> list[dict]:
    """Download and decode the Meteostat lite station roster."""
    with fsspec.open(url, "rb", compression="infer") as handle:
        return json.load(handle)


class MeteostatHourlyProvider(StationObservationProvider):
    """Hourly Meteostat observations, one partition per calendar month."""

    name = "meteostat_hourly"
    store_prefix = "bkr/obs/meteostat_hourly.icechunk"
    source_url = HOURLY_URL

    partition_freq = "MS"
    sample_freq = "1h"
    align_how = "exact"
    variables = VARIABLES
    max_workers = 8

    def __init__(self, config=None, stations=None):
        super().__init__(config=config, stations=stations)
        self._inventory: dict[str, tuple[pd.Timestamp | None, pd.Timestamp | None]] = {}

    @property
    def cache_dir(self) -> pathlib.Path:
        return self.config.data_dir / "meteostat"

    @property
    def roster_cache(self) -> pathlib.Path:
        return self.cache_dir / "stations.json"

    def all_stations(self) -> list[Station]:
        """The roster of stations with an hourly inventory, cached on disk."""
        cache = self.roster_cache
        if cache.is_file():
            try:
                raw = json.loads(cache.read_text())
            except json.JSONDecodeError as exc:
                logger.warning(f"{self.name}: ignoring unreadable roster cache {cache} ({exc})")
                raw = None
        else:
            raw = None

        if raw is None:
            raw = load_roster()
            cache.parent.mkdir(parents=True, exist_ok=True)
            cache.write_text(json.dumps(raw))
            logger.info(f"{self.name}: cached {len(raw)} roster entries to {cache}")

        stations = []
        for entry in raw:
            try:
                station, start, end = _station_entry(entry)
            except (KeyError, TypeError):
                continue
            if start is None and end is None:
                # No hourly inventory at all: the station only has daily or monthly data.
                continue
            stations.append(station)
            self._inventory[station.id] = (start, end)
        return sorted(stations, key=lambda s: s.id)

    def covers(self, station: Station, it: pd.Timestamp) -> bool:
        """True when the station's hourly inventory overlaps the partition."""
        start, end = self._inventory.get(station.id, (None, None))
        times = self.partition_times(it)
        if start is not None and start > times[-1]:
            return False
        if end is not None and end < times[0]:
            return False
        return True

    def station_filename(self, station: Station, it: pd.Timestamp) -> str:
        return f"{station.safe_id}-{pd.Timestamp(it).year}.csv.gz"

    def fetch_station(
        self,
        station: Station,
        it: pd.Timestamp,
        temp_dir: pathlib.Path,
    ) -> pathlib.Path | None:
        # station_list() populates the inventory, and fetch() always calls it first.
        if self._inventory and not self.covers(station, it):
            return None

        year = pd.Timestamp(it).year
        destination = self.cache_dir / str(year) / self.station_filename(station, it)
        if destination.is_file() and destination.stat().st_size > 0:
            return destination

        url = HOURLY_URL.format(year=year, station_id=station.id)
        destination.parent.mkdir(parents=True, exist_ok=True)
        part = partial_path(destination)
        try:
            with fsspec.open(url, "rb") as src, open(part, "wb") as out:
                out.write(src.read())
        except FileNotFoundError:
            part.unlink(missing_ok=True)
            return None
        part.replace(destination)
        return destination

    def read_station(
        self,
        path: pathlib.Path,
        station: Station,
        it: pd.Timestamp,
    ) -> pd.DataFrame | None:
        with gzip.open(path, "rb") as handle:
            raw = handle.read()
        if not raw.strip():
            return None
        df = pd.read_csv(io.BytesIO(raw))
        needed = {"year", "month", "day", "hour"}
        if df.empty or not needed.issubset(df.columns):
            return None
        index = pd.to_datetime(df[["year", "month", "day", "hour"]], errors="coerce")
        df = df.set_index(pd.DatetimeIndex(index))
        df = df[df.index.notna()]

        times = self.partition_times(it)
        window = df.loc[
            (df.index >= times[0]) & (df.index < times[-1] + pd.Timedelta(self.sample_freq))
        ]
        if window.empty:
            return None
        keep = [v for v in self.variables if v in window.columns]
        return window[keep]
