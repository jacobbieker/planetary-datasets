"""NOAA SURFRAD surface radiation budget network.

SURFRAD measures the full shortwave and longwave radiation budget plus basic meteorology
at seven US sites, published as one file per site per day:

    https://gml.noaa.gov/aftp/data/radiation/surfrad/{site}/{year}/{site}{yy}{doy}.dat

``pvlib.iotools.read_surfrad`` accepts that URL directly and does the parsing.

Replaces ``dags/assets/observation/surfrad.py``, which was a commented-out triple loop
writing one CSV per site-day. Behaviour changes: partitions are daily, the result is an
Icechunk store, and the retired sites are kept in the roster so the station axis does
not change when a site stops reporting.

The companion ``.aod`` files the original script had commented out are not read here:
they are a different format that ``read_surfrad`` does not parse.

Site coordinates are deliberately not duplicated here; they are in the header of every
source file.

Environment: none. The archive is anonymous.
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

ARCHIVE_URL = "https://gml.noaa.gov/aftp/data/radiation/surfrad/"
FILE_URL = ARCHIVE_URL + "{site}/{year}/{site}{yy}{doy:03d}.dat"

#: All SURFRAD sites, including the three that have been retired (``red``, ``rut``,
#: ``slv``). Retired sites stay on the axis so old and new partitions line up.
SITES = ("bon", "dra", "fpk", "gwn", "psu", "red", "rut", "slv", "sxf", "tbl", "was")

SITE_NAMES = {
    "bon": "Bondville, IL",
    "dra": "Desert Rock, NV",
    "fpk": "Fort Peck, MT",
    "gwn": "Goodwin Creek, MS",
    "psu": "Penn State, PA",
    "red": "Red Lake, AZ",
    "rut": "Rutland, VT",
    "slv": "Alamosa, CO",
    "sxf": "Sioux Falls, SD",
    "tbl": "Table Mountain, CO",
    "was": "Wasco, OR",
}

#: Measured quantities kept, dropping the ``*_flag`` quality codes and the date parts.
#: These are pvlib's mapped names, not the raw SURFRAD column names: ``read_surfrad``
#: renames by default, so ``dw_solar`` arrives as ``ghi``, ``diffuse`` as ``dhi`` and so
#: on.
VARIABLES = (
    "solar_zenith",
    "ghi",
    "uw_solar",
    "dni",
    "dhi",
    "dw_ir",
    "dw_casetemp",
    "dw_dometemp",
    "uw_ir",
    "uw_casetemp",
    "uw_dometemp",
    "uvb",
    "par",
    "netsolar",
    "netir",
    "totalnet",
    "temp_air",
    "relative_humidity",
    "wind_speed",
    "wind_direction",
    "pressure",
)


def file_url(site: str, day: pd.Timestamp) -> str:
    """URL of one site-day file."""
    day = pd.Timestamp(day)
    return FILE_URL.format(site=site, year=day.year, yy=f"{day.year % 100:02d}", doy=day.dayofyear)


class SurfradProvider(StationObservationProvider):
    """One-minute SURFRAD radiation budget, one partition per UTC day."""

    name = "surfrad"
    store_prefix = "bkr/obs/surfrad.icechunk"
    source_url = ARCHIVE_URL

    partition_freq = "D"
    sample_freq = "1min"
    align_how = "exact"
    variables = VARIABLES
    max_workers = 4

    def all_stations(self) -> list[Station]:
        return [Station(id=site, name=SITE_NAMES.get(site)) for site in SITES]

    def station_filename(self, station: Station, it: pd.Timestamp) -> str:
        day = pd.Timestamp(it)
        return f"{station.safe_id}{day.year % 100:02d}{day.dayofyear:03d}.dat"

    def fetch_station(
        self,
        station: Station,
        it: pd.Timestamp,
        temp_dir: pathlib.Path,
    ) -> pathlib.Path | None:
        # Downloaded here rather than letting read_surfrad fetch the URL, so a 404 (a
        # retired site, or a day the instrument was down) is told apart from a server
        # error that should fail the partition and be retried.
        return download_or_none(
            file_url(station.id, it), temp_dir / self.station_filename(station, it)
        )

    def read_station(
        self,
        path: pathlib.Path,
        station: Station,
        it: pd.Timestamp,
    ) -> pd.DataFrame | None:
        import pvlib

        try:
            df, _meta = pvlib.iotools.read_surfrad(str(path))
        except Exception as exc:  # noqa: BLE001 - a truncated file must not sink the day
            logger.warning(f"{self.name}: could not parse {path.name} ({exc})")
            return None
        keep = [v for v in self.variables if v in df.columns]
        return df[keep] if keep else None
