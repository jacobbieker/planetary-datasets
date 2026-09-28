"""ASOS one-minute observations from the Iowa Environmental Mesonet.

ASOS is the US Automated Surface Observing System. The IEM mirrors the one-minute
sub-hourly feed and serves it from a CGI endpoint that takes one station and one time
window per request:

    https://mesonet.agron.iastate.edu/cgi-bin/request/asos1min.py

The station roster is assembled from the IEM's per-state ``*_ASOS`` network GeoJSON. That
is 50 extra requests, so the roster is cached under ``PLANETARY_DATASETS_DATA_DIR`` and
only refreshed when the cache is missing.

Replaces the standalone ``pb/asos2.py`` / ``dags/assets/observation/asos.py`` script,
which wrote text files to hardcoded paths under ``/Users/jacob`` and ``/run/media``.
Behaviour changes: the output is an Icechunk store rather than one text file per
station-year, the retry loop is bounded per partition instead of per process, and the
destination comes from :func:`~planetary_datasets.config.get_config`.

Environment: ``PLANETARY_DATASETS_DATA_DIR`` (station roster cache). No credentials.
"""

from __future__ import annotations

import json
import pathlib
import time
from typing import Sequence

import pandas as pd
import requests
from loguru import logger

from planetary_datasets.providers.observations.base import Station, StationObservationProvider

SERVICE = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos1min.py"
NETWORK_GEOJSON = "https://mesonet.agron.iastate.edu/geojson/network/{network}.geojson"

STATES = (
    "AK AL AR AZ CA CO CT DE FL GA HI IA ID IL IN KS KY LA MA MD ME MI MN "
    "MO MS MT NC ND NE NH NJ NM NV NY OH OK OR PA RI SC SD TN TX UT VA VT "
    "WA WI WV WY"
).split()

#: The one-minute variables the IEM exposes. ``*_nd`` are the visibility "night/day"
#: flags and ``ptype`` is a code, but all of them are numeric and stored as such.
VARIABLES = (
    "tmpf",
    "dwpf",
    "sknt",
    "drct",
    "gust_drct",
    "gust_sknt",
    "vis1_coeff",
    "vis1_nd",
    "vis2_coeff",
    "vis2_nd",
    "vis3_coeff",
    "vis3_nd",
    "ptype",
    "precip",
    "pres1",
    "pres2",
    "pres3",
)

# The IEM answers 200 with a body starting "ERROR" when it is shedding load.
ERROR_PREFIX = "ERROR"


def fetch_network_stations(networks: Sequence[str], timeout: float = 60.0) -> list[Station]:
    """Read station metadata from the IEM network GeoJSON endpoints."""
    stations: dict[str, Station] = {}
    for network in networks:
        url = NETWORK_GEOJSON.format(network=network)
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        for site in response.json().get("features", []):
            props = site.get("properties", {})
            sid = props.get("sid")
            if not sid:
                continue
            lon, lat = (site.get("geometry", {}).get("coordinates") or [None, None])[:2]
            stations[sid] = Station(
                id=sid,
                latitude=lat,
                longitude=lon,
                elevation=props.get("elevation"),
                name=props.get("sname"),
            )
    # Sorted so the station axis does not depend on dict or network iteration order.
    return [stations[sid] for sid in sorted(stations)]


class ASOSOneMinuteProvider(StationObservationProvider):
    """One-minute ASOS observations, one partition per UTC day."""

    name = "asos_1min"
    store_prefix = "bkr/obs/asos_1min.icechunk"
    source_url = SERVICE

    partition_freq = "D"
    sample_freq = "1min"
    align_how = "exact"
    variables = VARIABLES
    # The IEM asks that automated clients stay light; six concurrent requests is well
    # inside what the service tolerates for a day of one-minute data.
    max_workers = 6

    #: Attempts per station request before the partition is failed.
    max_attempts: int = 6
    #: Base seconds for the exponential backoff between attempts.
    backoff: float = 5.0
    request_timeout: float = 300.0

    def __init__(self, config=None, stations=None, networks: Sequence[str] | None = None):
        """Build the provider.

        Args:
            config: Optional configuration override.
            stations: Restrict to these stations, as ids or :class:`Station` objects.
            networks: IEM network names to build the roster from. Defaults to the 50
                state ``*_ASOS`` networks.
        """
        super().__init__(config=config, stations=stations)
        self.networks = list(networks) if networks is not None else [f"{s}_ASOS" for s in STATES]

    @property
    def roster_cache(self) -> pathlib.Path:
        """Where the station roster is cached between runs."""
        return self.config.data_dir / "asos" / "stations.json"

    def all_stations(self) -> list[Station]:
        cache = self.roster_cache
        if cache.is_file():
            try:
                raw = json.loads(cache.read_text())
                return [Station(**entry) for entry in raw]
            except (json.JSONDecodeError, TypeError, ValueError) as exc:
                logger.warning(f"{self.name}: ignoring unreadable roster cache {cache} ({exc})")

        stations = fetch_network_stations(self.networks)
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_text(json.dumps([s.__dict__ for s in stations], indent=1))
        logger.info(f"{self.name}: cached {len(stations)} stations to {cache}")
        return stations

    def station_url(self, station: Station, it: pd.Timestamp) -> str:
        """Build the CGI request for one station and one UTC day."""
        start = pd.Timestamp(it)
        end = start + pd.Timedelta(days=1)
        params = "&".join(f"vars={v}" for v in self.variables)
        return (
            f"{SERVICE}?station={station.id}&{params}"
            f"&sts={start:%Y-%m-%dT%H:%MZ}&ets={end:%Y-%m-%dT%H:%MZ}"
            "&sample=1min&what=download&delim=comma&gis=yes&tz=UTC"
        )

    def _get_text(self, url: str) -> str:
        """GET with bounded exponential backoff, treating an ``ERROR`` body as a failure."""
        last: str = "no attempt made"
        for attempt in range(1, self.max_attempts + 1):
            try:
                response = requests.get(url, timeout=self.request_timeout)
                response.raise_for_status()
                text = response.text
                if not text.startswith(ERROR_PREFIX):
                    return text
                last = text.splitlines()[0] if text else ERROR_PREFIX
            except requests.RequestException as exc:
                last = str(exc)
            if attempt < self.max_attempts:
                time.sleep(self.backoff * attempt)
        # Raising rather than returning "" lets the base class tell a dead service apart
        # from a station the archive does not cover.
        raise RuntimeError(f"{self.max_attempts} attempts failed for {url}: {last}")

    def station_filename(self, station: Station, it: pd.Timestamp) -> str:
        return f"{station.safe_id}.csv"

    def fetch_station(
        self,
        station: Station,
        it: pd.Timestamp,
        temp_dir: pathlib.Path,
    ) -> pathlib.Path | None:
        text = self._get_text(self.station_url(station, it))
        # A station with no data still gets the CSV header back, so count the rows.
        if len(text.splitlines()) < 2:
            return None
        path = temp_dir / self.station_filename(station, it)
        path.write_text(text)
        return path

    def read_station(
        self,
        path: pathlib.Path,
        station: Station,
        it: pd.Timestamp,
    ) -> pd.DataFrame | None:
        df = pd.read_csv(path)
        if df.empty or "valid(UTC)" not in df.columns:
            return None
        df = df.set_index(pd.to_datetime(df["valid(UTC)"], utc=True).dt.tz_localize(None))
        keep = [v for v in self.variables if v in df.columns]
        return df[keep]
