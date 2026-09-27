"""NREL Measurement and Instrumentation Data Center (MIDC).

MIDC is NREL's collection of research-grade irradiance and meteorology stations. Raw
data is served per site and date range by

    https://midcdmz.nrel.gov/apps/data_api.pl

``dags/assets/observation/midc.py`` was an empty file, so there is no prior behaviour to
preserve; this fills the stub using the same pvlib-based shape as the SOLRAD and SURFRAD
providers.

The request is made here rather than through ``pvlib.iotools.read_midc_raw_data_from_nrel``
because that function has the API host misspelled as ``midcdmz.nlr.gov``; pvlib's parser
is still used, via ``read_midc(..., raw_data=True)``. Fetching directly also lets a 404
be told apart from a server error.

Every site reports a different instrument set under a different column name, so the
variable axis is the union of what ``MIDC_VARIABLE_MAP`` can rename. Columns a site does
not have stay NaN. Note that two sites measure DNI with different instruments, which
pvlib maps to ``dni_chp1`` and ``dni_nip`` rather than to ``dni``.

Environment: none. The raw-data API is anonymous.
"""

from __future__ import annotations

import io
import pathlib

import pandas as pd
from loguru import logger

from planetary_datasets.providers.observations.base import (
    Station,
    StationObservationProvider,
    download_or_none,
)

API_URL = "https://midcdmz.nrel.gov/apps/data_api.pl"

SITE_NAMES = {
    "BMS": "Baseline Measurement System, Golden, CO",
    "UOSMRL": "University of Oregon, Eugene, OR",
    "HSU": "Humboldt State University, Arcata, CA",
    "UTPASRL": "UT Pan American, Edinburg, TX",
    "UAT": "University of Arizona, Tucson, AZ",
    "STAC": "South Table Mountain, Golden, CO",
    "UNLV": "University of Nevada, Las Vegas, NV",
    "ORNL": "Oak Ridge National Laboratory, TN",
    "NELHA": "Natural Energy Laboratory of Hawaii Authority",
    "ULL": "University of Louisiana at Lafayette, LA",
    "NWTC": "National Wind Technology Center, Boulder, CO",
}


def midc_sites() -> list[str]:
    """Sites pvlib can map, in a stable order."""
    from pvlib.iotools.midc import MIDC_VARIABLE_MAP

    return sorted(MIDC_VARIABLE_MAP)


def midc_variables() -> tuple[str, ...]:
    """Union of the pvlib names every site's variable map can produce."""
    from pvlib.iotools.midc import MIDC_VARIABLE_MAP

    return tuple(sorted({name for site in MIDC_VARIABLE_MAP.values() for name in site.values()}))


class MIDCProvider(StationObservationProvider):
    """MIDC raw observations, one partition per UTC day."""

    name = "midc"
    store_prefix = "bkr/obs/midc.icechunk"
    source_url = API_URL

    partition_freq = "D"
    sample_freq = "1min"
    align_how = "exact"
    max_workers = 4
    request_timeout: float = 120.0

    def __init__(self, config=None, stations=None):
        super().__init__(config=config, stations=stations)
        # Resolved from pvlib rather than hardcoded, so the two lists cannot drift.
        self.variables = midc_variables()

    def all_stations(self) -> list[Station]:
        return [Station(id=site, name=SITE_NAMES.get(site)) for site in midc_sites()]

    def station_filename(self, station: Station, it: pd.Timestamp) -> str:
        return f"{station.safe_id}.csv"

    def station_url(self, station: Station, it: pd.Timestamp) -> str:
        day = pd.Timestamp(it)
        return f"{API_URL}?site={station.id}&begin={day:%Y%m%d}&end={day:%Y%m%d}"

    def fetch_station(
        self,
        station: Station,
        it: pd.Timestamp,
        temp_dir: pathlib.Path,
    ) -> pathlib.Path | None:
        return download_or_none(
            self.station_url(station, it),
            temp_dir / self.station_filename(station, it),
            timeout=self.request_timeout,
        )

    def read_station(
        self,
        path: pathlib.Path,
        station: Station,
        it: pd.Timestamp,
    ) -> pd.DataFrame | None:
        from pvlib.iotools import read_midc
        from pvlib.iotools.midc import MIDC_VARIABLE_MAP

        text = path.read_text()
        if not text.strip():
            return None
        try:
            # The header and the data rows do not always have the same number of
            # columns, so the column count is taken from the header, as pvlib does.
            width = len(pd.read_csv(io.StringIO(text), nrows=0).columns)
            df = read_midc(
                io.StringIO(text),
                variable_map=MIDC_VARIABLE_MAP.get(station.id, {}),
                raw_data=True,
                usecols=range(width),
            )
        except Exception as exc:  # noqa: BLE001 - one unreadable site must not sink the day
            logger.warning(f"{self.name}: could not parse {path.name} ({exc})")
            return None
        if df.empty:
            return None
        # MIDC timestamps are localised to the site; the store's axis is UTC.
        if df.index.tz is not None:
            df.index = df.index.tz_convert("UTC")
        keep = [v for v in self.variables if v in df.columns]
        return df[keep] if keep else None
