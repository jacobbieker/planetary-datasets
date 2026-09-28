"""AERONET aerosol optical depth from the NASA version 3 web service.

AERONET is NASA's global sun-photometer network. The v3 web service returns the whole
network for a date range in a single request, so this provider does not fetch per
station:

    https://aeronet.gsfc.nasa.gov/cgi-bin/print_web_data_v3

Replaces ``dags/assets/observation/aeronet.py``, whose body was entirely commented out
because it depended on ``monetio``. The web service is called directly instead, which
drops the dependency and removes the per-hour ``pd.date_range`` the old code built and
never used.

Site coordinates come from ``aeronet_locations_v3.txt``, which is the full roster and is
therefore stable between partitions. Observations are irregular — a photometer reports
whenever the sun is unobstructed — so they are averaged onto an hourly grid.

Environment: ``PLANETARY_DATASETS_DATA_DIR`` (site roster cache). No credentials.
"""

from __future__ import annotations

import io
import json
import pathlib

import numpy as np
import pandas as pd
import requests
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common.store import write_to_icechunk as _write_to_icechunk
from planetary_datasets.providers.observations.base import (
    OBSERVATION_ALIGNMENT_COORDS,
    NoStationDataError,
    Station,
    frames_to_dataset,
    partition_time_index,
)

WEB_SERVICE = "https://aeronet.gsfc.nasa.gov/cgi-bin/print_web_data_v3"
LOCATIONS_URL = "https://aeronet.gsfc.nasa.gov/aeronet_locations_v3.txt"

#: Quality levels the service exposes, as the flag it expects in the query string.
QUALITY_FLAGS = {"AOD10": "AOD10", "AOD15": "AOD15", "AOD20": "AOD20"}

#: Row header line index in the service's response. The first five lines are a preamble
#: naming the product, the version and the principal investigator.
HEADER_ROW = 5

#: AERONET writes -999 for every absent measurement.
MISSING = -999.0

#: Channels kept. The service returns every wavelength any instrument in the network has,
#: most of which are -999 for most sites; these are the ones with broad coverage.
VARIABLES = (
    "AOD_1020nm",
    "AOD_870nm",
    "AOD_675nm",
    "AOD_500nm",
    "AOD_440nm",
    "AOD_380nm",
    "AOD_340nm",
    "Precipitable_Water(cm)",
)


def read_locations(url: str = LOCATIONS_URL, timeout: float = 60.0) -> list[Station]:
    """Read the AERONET site roster."""
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    # Line 0 is a generation banner, line 1 is the header.
    df = pd.read_csv(io.StringIO(response.text), skiprows=1)
    stations = {}
    for row in df.itertuples(index=False):
        values = dict(zip(df.columns, row))
        site = str(values["Site_Name"]).strip()
        stations[site] = Station(
            id=site,
            latitude=float(values["Latitude(decimal_degrees)"]),
            longitude=float(values["Longitude(decimal_degrees)"]),
            elevation=float(values["Elevation(meters)"]),
        )
    return [stations[name] for name in sorted(stations)]


def parse_web_data(text: str) -> pd.DataFrame:
    """Parse a v3 web-service response into a dataframe indexed by UTC time.

    Returns an empty frame when the response is the preamble alone, which is what the
    service sends for a date with no observations.
    """
    lines = text.splitlines()
    if len(lines) <= HEADER_ROW + 1:
        return pd.DataFrame()

    df = pd.read_csv(io.StringIO("\n".join(lines[HEADER_ROW:])))
    if df.empty or "AERONET_Site" not in df.columns:
        return pd.DataFrame()

    stamps = pd.to_datetime(
        df["Date(dd:mm:yyyy)"] + " " + df["Time(hh:mm:ss)"],
        format="%d:%m:%Y %H:%M:%S",
        errors="coerce",
    )
    df = df.set_index(pd.DatetimeIndex(stamps))
    df = df[df.index.notna()]
    # np.nan rather than pd.NA: pd.NA turns a float column into object dtype, which then
    # cannot be resampled onto the hourly grid.
    return df.replace(MISSING, np.nan)


class AeronetProvider(BaseProvider):
    """AERONET direct-sun AOD for the whole network, one partition per UTC day."""

    name = "aeronet"
    store_prefix = "bkr/obs/aeronet_aod.icechunk"
    append_dim = "time"

    partition_freq = "D"
    sample_freq = "1h"
    variables = VARIABLES
    request_timeout: float = 600.0

    def __init__(self, config=None, quality: str = "AOD15", sites=None):
        """Build the provider.

        Args:
            config: Optional configuration override.
            quality: ``AOD10`` (unscreened), ``AOD15`` (cloud screened) or ``AOD20``
                (quality assured). Level 2.0 lags by up to a year.
            sites: Restrict the station axis to these site names.
        """
        super().__init__(config=config)
        if quality not in QUALITY_FLAGS:
            raise ValueError(f"quality must be one of {sorted(QUALITY_FLAGS)}, got {quality!r}")
        self.quality = quality
        self._site_override = list(sites) if sites is not None else None
        self._stations: list[Station] | None = None

    @property
    def roster_cache(self) -> pathlib.Path:
        return self.config.data_dir / "aeronet" / "sites.json"

    def station_list(self) -> list[Station]:
        """The canonical site axis, resolved once and cached on disk between runs."""
        if self._stations is not None:
            return self._stations

        cache = self.roster_cache
        roster: list[Station] | None = None
        if cache.is_file():
            try:
                roster = [Station(**entry) for entry in json.loads(cache.read_text())]
            except (json.JSONDecodeError, TypeError, ValueError) as exc:
                logger.warning(f"{self.name}: ignoring unreadable roster cache {cache} ({exc})")
            # An empty cache is a run that died mid-write. Trusting it would build a
            # station-less store that every later partition then has to match.
            if not roster:
                logger.warning(f"{self.name}: roster cache {cache} is empty, refetching")
                roster = None
        if roster is None:
            roster = read_locations()
            if not roster:
                raise ValueError(f"{self.name}: {LOCATIONS_URL} returned no sites")
            cache.parent.mkdir(parents=True, exist_ok=True)
            cache.write_text(json.dumps([s.__dict__ for s in roster]))

        if self._site_override is not None:
            known = {s.id: s for s in roster}
            missing = sorted(set(self._site_override) - set(known))
            if missing:
                raise ValueError(f"{self.name}: unknown site(s) {missing[:5]}")
            roster = [known[name] for name in self._site_override]

        self._stations = roster
        return roster

    def partition_times(self, it: pd.Timestamp) -> pd.DatetimeIndex:
        return partition_time_index(it, self.partition_freq, self.sample_freq)

    def request_url(self, it: pd.Timestamp) -> str:
        """Query for one UTC day of all-points data across the whole network."""
        day = pd.Timestamp(it)
        return (
            f"{WEB_SERVICE}?year={day.year}&month={day.month}&day={day.day}"
            f"&year2={day.year}&month2={day.month}&day2={day.day}"
            f"&{QUALITY_FLAGS[self.quality]}=1&AVG=10&if_no_html=1"
        )

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> list[str]:
        directory = pathlib.Path(temp_dir) if temp_dir is not None else self.config.scratch_dir
        directory.mkdir(parents=True, exist_ok=True)

        response = requests.get(self.request_url(it), timeout=self.request_timeout)
        response.raise_for_status()
        text = response.text
        if len(text.splitlines()) <= HEADER_ROW + 1:
            logger.info(f"{self.name}: no observations for {it}")
            return []

        path = directory / f"aeronet_{pd.Timestamp(it):%Y%m%d}.csv"
        path.write_text(text)
        return [str(path)]

    def process(self, input_files, it, temp_dir=None, **kwargs):
        df = parse_web_data(pathlib.Path(input_files[0]).read_text())
        if df.empty:
            raise NoStationDataError(f"{self.name}: response for {it} held no observations")

        stations = self.station_list()
        known = {s.id for s in stations}
        frames = {
            str(site): group[[c for c in self.variables if c in group.columns]]
            for site, group in df.groupby("AERONET_Site")
            if str(site) in known
        }
        if not frames:
            raise NoStationDataError(
                f"{self.name}: none of the {df['AERONET_Site'].nunique()} reporting sites for "
                f"{it} are in the roster; the site list is probably stale"
            )

        return frames_to_dataset(
            frames,
            stations,
            self.partition_times(it),
            variables=self.variables,
            sample_freq=self.sample_freq,
            # Sun photometers report every few minutes when the sky is clear; averaging
            # onto the hour keeps the store dense without discarding most of the day.
            how="mean",
            attrs={"network": self.name, "source": WEB_SERVICE, "quality": self.quality},
        )

    def write_to_icechunk(self, repo, processed):
        return _write_to_icechunk(
            repo,
            processed,
            append_dim=self.append_dim,
            message=f"{self.name}: {processed[self.append_dim].values[0]}",
            alignment_coords=OBSERVATION_ALIGNMENT_COORDS,
        )
