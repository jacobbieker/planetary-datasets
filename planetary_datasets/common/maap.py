"""ESA MAAP: catalogue search and authenticated downloads.

ESA distributes EarthCARE and Aeolus through MAAP. Searching its STAC catalogue is
anonymous; downloading a file needs a bearer token. The user generates a 90-day *offline*
token at https://portal.maap.eo.esa.int/ini/services/auth/token/90dToken.php
(``MAAP_OFFLINE_TOKEN``), which is exchanged here for access tokens that last ten hours,
using the public ``offline-token`` client ESA documents for exactly this.

Source: https://catalog.maap.eo.esa.int/doc/examples/ESAMAAP_ecdataaccess.html
"""

from __future__ import annotations

import threading
import time
from typing import Any, Iterator

import requests
from loguru import logger

from planetary_datasets.config import Config, MissingCredential, get_config

CATALOGUE_URL = "https://catalog.maap.eo.esa.int/catalogue"
TOKEN_URL = "https://iam.maap.eo.esa.int/realms/esa-maap/protocol/openid-connect/token"
#: ESA's public client for exchanging offline tokens; valid for every user.
CLIENT_ID = "offline-token"
CLIENT_SECRET = "p1eL7uonXs6MDxtGbgKdPVRAmnGxHpVE"
TOKEN_PAGE = "https://portal.maap.eo.esa.int/ini/services/auth/token/90dToken.php"
#: Refresh an access token this long before it expires.
REFRESH_MARGIN_S = 600
#: Items per catalogue page.
PAGE_SIZE = 500


class MaapAuthError(MissingCredential):
    """The offline token is missing, expired or revoked."""


class MaapAuth:
    """Hands out a current access token, exchanging the offline token as needed.

    Thread-safe: download workers share one instance.
    """

    def __init__(self, config: Config | None = None):
        """Read the offline token from the configuration."""
        self.config = config or get_config()
        self._lock = threading.Lock()
        self._token: str | None = None
        self._expires = 0.0

    def access_token(self) -> str:
        """A token valid for at least :data:`REFRESH_MARGIN_S` more seconds."""
        with self._lock:
            if self._token is None or time.time() > self._expires - REFRESH_MARGIN_S:
                self._token, self._expires = self._exchange()
            return self._token

    def headers(self) -> dict[str, str]:
        """``Authorization`` header for a download."""
        return {"Authorization": f"Bearer {self.access_token()}"}

    def _exchange(self) -> tuple[str, float]:
        (offline,) = self.config.credentials.require("maap_offline_token")
        response = requests.post(
            TOKEN_URL,
            data={
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "grant_type": "refresh_token",
                "refresh_token": offline,
                "scope": "offline_access openid",
            },
            timeout=60,
        )
        if response.status_code in (400, 401):
            raise MaapAuthError(
                f"MAAP refused MAAP_OFFLINE_TOKEN ({response.status_code}: "
                f"{response.text[:200]}); it lasts 90 days, make a new one at {TOKEN_PAGE}"
            )
        response.raise_for_status()
        body = response.json()
        logger.debug(f"maap: access token for {body['expires_in']} s")
        return body["access_token"], time.time() + float(body["expires_in"])


def search(
    collection: str,
    start: str,
    end: str,
    filters: dict[str, str] | None = None,
    session: requests.Session | None = None,
) -> Iterator[dict[str, Any]]:
    """STAC items in ``collection`` sensed in ``[start, end]``, following every page.

    Args:
        collection: Catalogue collection id, e.g. ``EarthCAREL2Validated_MAAP``.
        start: ISO start time.
        end: ISO end time.
        filters: CQL2 text equalities, e.g. ``{"productType": "CPR_CLD_2A"}``.
        session: Reuse a ``requests`` session.
    """
    http = session or requests
    params: dict[str, Any] = {"datetime": f"{start}/{end}", "limit": PAGE_SIZE}
    if filters:
        params["filter"] = " AND ".join(f"{k}='{v}'" for k, v in filters.items())
    url: str | None = f"{CATALOGUE_URL}/collections/{collection}/items"
    while url:
        response = http.get(
            url, params=params, headers={"Accept": "application/geo+json"}, timeout=300
        )
        response.raise_for_status()
        page = response.json()
        yield from page.get("features", [])
        url = next(
            (link["href"] for link in page.get("links", []) if link.get("rel") == "next"), None
        )
        # The next link carries the query.
        params = {}


__all__ = ["CATALOGUE_URL", "MaapAuth", "MaapAuthError", "search"]
