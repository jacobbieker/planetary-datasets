"""NOAA SOLRAD surface radiation network.

SOLRAD measures global, direct and diffuse irradiance plus UVB at a handful of US sites,
published as one fixed-width file per site per day under

    https://gml.noaa.gov/aftp/data/radiation/solrad/

``pvlib.iotools.get_solrad`` handles the fixed-width parsing and the format change at
2015-01-01, when the sampling went from three-minute to one-minute averages.

Replaces ``dags/assets/observation/solrad.py``, which was a commented-out loop writing
one CSV per station. Behaviour changes: partitions are daily rather than "everything
since 2015 in one call", and the result is an Icechunk store.

Site coordinates are deliberately not duplicated here. They are in the header of every
source file and in the NOAA site table; hardcoding a second copy is how those drift.

Environment: none. The FTP-over-HTTP archive is anonymous.
"""

from __future__ import annotations

import pathlib

import pandas as pd
from loguru import logger

from planetary_datasets.providers.observations.base import (
    Station,
    StationObservationProvider,
    download_or_none,
)

ARCHIVE_URL = "https://gml.noaa.gov/aftp/data/radiation/solrad/"
FILE_URL = ARCHIVE_URL + "{site}/{year}/{site}{yy}{doy:03d}.dat"

#: The SOLRAD sites, as three-letter abbreviations. Not every site has data for every
#: year; absent days simply come back empty.
SITES = ("abq", "bis", "hnx", "msn", "ort", "sea", "slc", "ste", "tlh")

SITE_NAMES = {
    "abq": "Albuquerque, NM",
    "bis": "Bismarck, ND",
    "hnx": "Hanford, CA",
    "msn": "Madison, WI",
    "ort": "Oak Ridge, TN",
    "sea": "Seattle, WA",
    "slc": "Salt Lake City, UT",
    "ste": "Sterling, VA",
    "tlh": "Tallahassee, FL",
}

#: Measured quantities kept. The ``*_flag`` columns are quality codes and the date parts
#: are already in the index.
VARIABLES = (
    "solar_zenith",
    "ghi",
    "dni",
    "dhi",
    "uvb",
    "uvb_temp",
    "std_dw_psp",
    "std_direct",
    "std_diffuse",
    "std_uvb",
)


class SolradProvider(StationObservationProvider):
    """One-minute SOLRAD irradiance, one partition per UTC day."""

    name = "solrad"
    store_prefix = "bkr/obs/solrad.icechunk"
    source_url = ARCHIVE_URL

    partition_freq = "D"
    sample_freq = "1min"
    # Pre-2015 files are three-minute averages, so two thirds of the grid is NaN for
    # those days. Snapping would invent readings that were never taken.
    align_how = "exact"
    variables = VARIABLES
    max_workers = 4

    def all_stations(self) -> list[Station]:
        return [Station(id=site, name=SITE_NAMES.get(site)) for site in SITES]

    def station_filename(self, station: Station, it: pd.Timestamp) -> str:
        # Kept identical to the upstream name: pvlib's reader picks the Madison column
        # layout by looking for "msn" in the filename.
        day = pd.Timestamp(it)
        return f"{station.safe_id}{day.year % 100:02d}{day.dayofyear:03d}.dat"

    def fetch_station(
        self,
        station: Station,
        it: pd.Timestamp,
        temp_dir: pathlib.Path,
    ) -> pathlib.Path | None:
        # Downloaded here rather than through pvlib.iotools.get_solrad so a 404 (the site
        # did not report) is told apart from a server error (retry the partition).
        day = pd.Timestamp(it)
        url = FILE_URL.format(
            site=station.id, year=day.year, yy=f"{day.year % 100:02d}", doy=day.dayofyear
        )
        return download_or_none(url, temp_dir / self.station_filename(station, it))

    def read_station(
        self,
        path: pathlib.Path,
        station: Station,
        it: pd.Timestamp,
    ) -> pd.DataFrame | None:
        import pvlib

        try:
            df, _meta = pvlib.iotools.read_solrad(str(path))
        except Exception as exc:  # noqa: BLE001 - a truncated file must not sink the day
            logger.warning(f"{self.name}: could not parse {path.name} ({exc})")
            return None
        keep = [v for v in self.variables if v in df.columns]
        return df[keep] if keep else None
