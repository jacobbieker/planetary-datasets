"""NOAA Integrated Surface Database (ISD) hourly observations.

ISD is the global archive of hourly and synoptic surface reports. NOAA publishes it on
the open ``noaa-isd-pds`` S3 bucket as one gzipped fixed-width file per station per year,
alongside ``isd-history.csv`` giving the station roster.

Replaces the ``isdy.py`` scratch script at the repository root, its ``pb/`` copy and the
fully commented-out ``dags/assets/observation/isd.py``. Those used the ``isd`` library's
``Batch`` helper, which no longer exists in the packaged version; the per-line
:class:`isd.Record` parser is used instead.

Partitions are monthly. The year file a month needs is cached under
``PLANETARY_DATASETS_DATA_DIR/isd/<year>/`` so the twelve partitions of a year each
download it at most once between them. The full roster is around 30,000 stations and a
year of it is tens of gigabytes; pass ``stations=[...]`` for a targeted backfill.

Environment: ``PLANETARY_DATASETS_DATA_DIR`` (year-file cache). No credentials; the
bucket is anonymous.
"""

from __future__ import annotations

import gzip
import io
import pathlib
import threading

import fsspec
import numpy as np
import pandas as pd
from loguru import logger

from planetary_datasets.common.time import freq_to_timedelta
from planetary_datasets.providers.observations.base import (
    Station,
    StationObservationProvider,
    partial_path,
)

#: Public, anonymous source bucket. Not an output location.
ISD_BUCKET = "noaa-isd-pds"
ISD_HISTORY = f"s3://{ISD_BUCKET}/isd-history.csv"

#: Numeric fields kept from :class:`isd.Record`. The quality-control codes and the raw
#: additional-data strings are dropped: they are not numeric and would dominate the store.
VARIABLES = (
    "wind_direction",
    "wind_speed",
    "ceiling",
    "visibility",
    "air_temperature",
    "dew_point_temperature",
    "sea_level_pressure",
)

#: The fixed-width columns use out-of-range sentinels for "missing". The ``isd`` package
#: returns them as-is, so they are masked here; leaving them in would put 999.9 degree
#: temperatures into the store.
MISSING_SENTINELS = {
    "wind_direction": 999,
    "wind_speed": 999.9,
    "ceiling": 99999,
    "visibility": 999999,
    "air_temperature": 999.9,
    "dew_point_temperature": 999.9,
    "sea_level_pressure": 9999.9,
}
ELEVATION_MISSING = 9999.0


def _s3():
    import s3fs

    return s3fs.S3FileSystem(anon=True)


def parse_isd_text(text: str) -> pd.DataFrame:
    """Parse the lines of an ISD station-year file into a time-indexed dataframe.

    Unparseable lines are skipped: the archive contains occasional truncated records and
    dropping one report is better than losing the station's year.
    """
    from isd import IsdError, Record

    times: list[pd.Timestamp] = []
    rows: list[list[float]] = []
    skipped = 0
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            record = Record.parse(line)
        except (IsdError, ValueError, IndexError):
            skipped += 1
            continue
        try:
            stamp = pd.Timestamp(
                year=record.year,
                month=record.month,
                day=record.day,
                hour=record.hour,
                minute=record.minute,
            )
        except ValueError:
            skipped += 1
            continue
        times.append(stamp)
        rows.append([getattr(record, var) for var in VARIABLES])

    if skipped:
        logger.debug(f"skipped {skipped} unparseable ISD line(s)")
    if not rows:
        return pd.DataFrame(columns=list(VARIABLES))

    df = pd.DataFrame(rows, columns=list(VARIABLES), index=pd.DatetimeIndex(times))
    for var, sentinel in MISSING_SENTINELS.items():
        df[var] = pd.to_numeric(df[var], errors="coerce").mask(lambda s: s == sentinel)
    return df.sort_index()


def read_isd_history() -> list[Station]:
    """Read the ISD station roster from ``isd-history.csv``.

    The roster spans the whole archive rather than one year, so the station axis stays
    identical from partition to partition.
    """
    with fsspec.open(ISD_HISTORY, "rb", anon=True) as handle:
        raw = handle.read()

    df = pd.read_csv(io.BytesIO(raw), dtype={"USAF": str, "WBAN": str})
    df = df.dropna(subset=["USAF", "WBAN"])
    elevation = pd.to_numeric(df.get("ELEV(M)"), errors="coerce").replace(
        ELEVATION_MISSING, np.nan
    )
    stations = [
        Station(
            id=f"{usaf}-{wban}",
            latitude=lat if pd.notna(lat) else None,
            longitude=lon if pd.notna(lon) else None,
            elevation=elev if pd.notna(elev) else None,
            name=str(name) if pd.notna(name) else None,
        )
        for usaf, wban, lat, lon, elev, name in zip(
            df["USAF"],
            df["WBAN"],
            pd.to_numeric(df.get("LAT"), errors="coerce"),
            pd.to_numeric(df.get("LON"), errors="coerce"),
            elevation,
            df.get("STATION NAME", pd.Series([None] * len(df))),
        )
    ]
    # Duplicate USAF-WBAN pairs exist where a station moved; keep the first.
    seen: dict[str, Station] = {}
    for station in stations:
        seen.setdefault(station.id, station)
    return [seen[sid] for sid in sorted(seen)]


class ISDProvider(StationObservationProvider):
    """Hourly ISD surface observations, one partition per calendar month."""

    name = "isd"
    store_prefix = "bkr/obs/isd.icechunk"
    source_url = f"s3://{ISD_BUCKET}/data/"

    partition_freq = "MS"
    sample_freq = "1h"
    # ISD reports are irregular: routine synoptic reports plus specials. The last report
    # in each hour is kept, which matches how the hourly summaries are usually derived.
    align_how = "last"
    variables = VARIABLES
    max_workers = 8

    def __init__(self, config=None, stations=None, bucket: str = ISD_BUCKET):
        """Build the provider.

        Args:
            config: Optional configuration override.
            stations: Restrict to these stations, as ids or :class:`Station` objects.
            bucket: Source bucket. Only useful for pointing at a mirror.
        """
        super().__init__(config=config, stations=stations)
        self.bucket = bucket
        self._year_keys: dict[int, set[str]] = {}
        self._year_keys_lock = threading.Lock()

    @property
    def cache_dir(self) -> pathlib.Path:
        """Root of the station-year file cache."""
        return self.config.data_dir / "isd"

    def all_stations(self) -> list[Station]:
        return read_isd_history()

    def year_key(self, station: Station, year: int) -> str:
        """S3 key of one station's file for a year."""
        return f"{self.bucket}/data/{year}/{station.id}-{year}.gz"

    def year_keys(self, year: int) -> set[str]:
        """Every key published for ``year``, listed once and cached.

        One prefix listing replaces a ``exists`` call per station, which on the full
        roster is thirty thousand round trips per partition.
        """
        with self._year_keys_lock:
            if year not in self._year_keys:
                self._year_keys[year] = set(_s3().ls(f"{self.bucket}/data/{year}/"))
                logger.debug(f"{self.name}: {len(self._year_keys[year])} file(s) listed for {year}")
            return self._year_keys[year]

    def station_filename(self, station: Station, it: pd.Timestamp) -> str:
        """Local name of a station's year file. Shared by the year's twelve partitions."""
        return f"{station.safe_id}-{pd.Timestamp(it).year}.gz"

    def fetch_station(
        self,
        station: Station,
        it: pd.Timestamp,
        temp_dir: pathlib.Path,
    ) -> pathlib.Path | None:
        """Download the station's year file, reusing the on-disk cache across months."""
        year = pd.Timestamp(it).year
        destination = self.cache_dir / str(year) / self.station_filename(station, it)
        if destination.is_file() and destination.stat().st_size > 0:
            return destination

        key = self.year_key(station, year)
        if key not in self.year_keys(year):
            # Most stations are absent from most years; that is not a failure.
            return None

        destination.parent.mkdir(parents=True, exist_ok=True)
        part = partial_path(destination)
        with _s3().open(key, "rb") as src, open(part, "wb") as out:
            out.write(src.read())
        part.replace(destination)
        return destination

    def read_station(
        self,
        path: pathlib.Path,
        station: Station,
        it: pd.Timestamp,
    ) -> pd.DataFrame | None:
        """Parse the year file and cut it down to the partition's month."""
        with gzip.open(path, "rb") as handle:
            text = handle.read().decode("utf-8", errors="replace")
        df = parse_isd_text(text)
        if df.empty:
            return None
        times = self.partition_times(it)
        # The file holds a whole year; the partition is one month of it.
        window = df.loc[
            (df.index >= times[0]) & (df.index < times[-1] + freq_to_timedelta(self.sample_freq))
        ]
        return window if not window.empty else None
