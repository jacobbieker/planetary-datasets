"""NASA Earthdata: CMR granule search and temporary S3 credentials for Earthdata Cloud.

Earthdata Cloud buckets (``ob-cumulus-prod-public`` for OB.DAAC, among others) are in
us-west-2 and cannot be read anonymously: a DAAC hands out S3 keys valid for an hour in
exchange for an Earthdata login (``EARTHDATA_USERNAME`` / ``EARTHDATA_PASSWORD``). They
are fetched here as needed and never written anywhere, stores included.

Source: https://cmr.earthdata.nasa.gov/search/site/docs/search/api.html
"""

from __future__ import annotations

import threading
import time
from typing import Any, Iterator

import pandas as pd
import requests
from loguru import logger

from planetary_datasets.config import Config, MissingCredential, get_config

CMR_GRANULES = "https://cmr.earthdata.nasa.gov/search/granules.json"
#: S3 credential endpoints per DAAC.
S3_CREDENTIALS = {"obdaac": "https://obdaac-tea.earthdatacloud.nasa.gov/s3credentials"}
#: Refresh S3 credentials this long before they expire.
REFRESH_MARGIN_S = 600
PAGE_SIZE = 2000


class EarthdataS3:
    """Temporary S3 credentials for one DAAC, refreshed before they lapse. Thread-safe."""

    def __init__(self, daac: str = "obdaac", config: Config | None = None):
        """Use the configured Earthdata login against ``daac``'s credential endpoint."""
        self.url = S3_CREDENTIALS[daac]
        self.config = config or get_config()
        self._lock = threading.Lock()
        self._credentials: dict[str, str] | None = None
        self._expires = 0.0

    def credentials(self) -> dict[str, str]:
        """``accessKeyId``, ``secretAccessKey`` and ``sessionToken``, valid for a while yet."""
        with self._lock:
            if self._credentials is None or time.time() > self._expires - REFRESH_MARGIN_S:
                self._credentials, self._expires = self._fetch()
            return self._credentials

    def filesystem(self):
        """An ``s3fs`` filesystem on the current credentials."""
        import s3fs

        c = self.credentials()
        return s3fs.S3FileSystem(
            key=c["accessKeyId"], secret=c["secretAccessKey"], token=c["sessionToken"]
        )

    def _fetch(self) -> tuple[dict[str, str], float]:
        login, password = self.config.credentials.require(
            "earthdata_username", "earthdata_password"
        )
        session = requests.Session()
        session.auth = (login, password)
        response = session.get(self.url, timeout=60)
        if response.status_code in (401, 403):
            raise MissingCredential(f"Earthdata refused the configured login at {self.url}")
        response.raise_for_status()
        body = response.json()
        expires = pd.Timestamp(body["expiration"]).timestamp()
        logger.debug(f"earthdata: S3 credentials until {body['expiration']}")
        keys = ("accessKeyId", "secretAccessKey", "sessionToken")
        return {k: body[k] for k in keys}, expires


def granules(
    short_name: str,
    version: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    session: requests.Session | None = None,
) -> Iterator[dict[str, Any]]:
    """CMR granule entries of a collection starting in ``[start, end)``, every page."""
    http = session or requests.Session()
    params = {
        "short_name": short_name,
        "version": version,
        "temporal": f"{start:%Y-%m-%dT%H:%M:%SZ},{end - pd.Timedelta(1, 's'):%Y-%m-%dT%H:%M:%SZ}",
        "page_size": PAGE_SIZE,
        "sort_key": "start_date",
    }
    headers: dict[str, str] = {}
    while True:
        response = _get_with_retries(http, params, headers)
        entries = response.json()["feed"]["entry"]
        for entry in entries:
            if start <= pd.Timestamp(entry["time_start"]).tz_convert(None) < end:
                yield entry
        after = response.headers.get("CMR-Search-After")
        if not after or len(entries) < PAGE_SIZE:
            return
        headers = {"CMR-Search-After": after}


def _get_with_retries(
    http: requests.Session, params: dict, headers: dict, attempts: int = 5
) -> requests.Response:
    """GET a CMR page, riding out its occasional 5xx and dropped connections."""
    for attempt in range(1, attempts + 1):
        try:
            response = http.get(CMR_GRANULES, params=params, headers=headers, timeout=120)
            if response.status_code < 500:
                response.raise_for_status()
                return response
            error: Exception = requests.HTTPError(f"CMR {response.status_code}")
        except (requests.ConnectionError, requests.Timeout) as exc:
            error = exc
        if attempt == attempts:
            raise error
        logger.debug(f"earthdata: CMR attempt {attempt} failed ({error}); retrying")
        time.sleep(5 * 2 ** (attempt - 1))
    raise AssertionError("unreachable")


def s3_url(entry: dict[str, Any]) -> str:
    """The granule's ``s3://`` data link."""
    for link in entry.get("links", []):
        href = link.get("href", "")
        if href.startswith("s3://") and href.endswith((".nc", ".h5", ".hdf")):
            return href
    raise ValueError(f"{entry.get('producer_granule_id')}: no s3:// data link")


__all__ = ["EarthdataS3", "granules", "s3_url"]
