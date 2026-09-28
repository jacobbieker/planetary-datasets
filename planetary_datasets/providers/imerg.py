"""IMERG: NASA GPM half-hourly global precipitation.

IMERG (Integrated Multi-satellitE Retrievals for GPM) merges every available passive
microwave and infrared satellite estimate into a global half-hourly precipitation field on
a 0.1 degree grid. NASA publishes it in three runs that trade latency for accuracy:

===========  ================  =========  =========================================
Product      GES DISC short    Latency    Notes
===========  ================  =========  =========================================
``early``    GPM_3IMERGHHE.07  ~4 hours   Forward-propagated only, for nowcasting.
``late``     GPM_3IMERGHHL.07  ~14 hours  Forward and backward propagation.
``final``    GPM_3IMERGHH.07   ~3.5 mths  Gauge-calibrated research-grade product.
===========  ================  =========  =========================================

One partition is one UTC day, which is 48 half-hourly granules. Data is fetched from the
GES DISC HTTPS archive, which requires an Earthdata Login; set ``EARTHDATA_USERNAME`` and
``EARTHDATA_PASSWORD``.

This replaces the standalone ``imerg_early.py`` / ``imerg_late.py`` / ``imerg_final.py``
scripts. Those relied on a recursive ``wget`` into a hardcoded external drive, a
pre-allocated "dummy" Zarr covering 2000-2026 that was filled in by region, and a final
``aws s3 sync --profile=sc`` to Source Cooperative. Here the day is appended straight into
the configured icechunk store, so the store *is* the published artefact and there is no
second copy to keep in sync. Note that the scripts named "late" in fact pointed at the
Final Run collection (``GPM_3IMERGHH.07``); ``final`` below keeps their store location.

The stores move with that change, from ``s3://<bucket>/bkr/imerg/imerg_*.zarr`` to
``.../imerg_*.icechunk``. The old Zarr archives are left in place and are not updated; a
consumer reading them has to be repointed.

Usage::

    from planetary_datasets.providers.imerg import IMERGProvider

    IMERGProvider("late").run_partition(pd.Timestamp("2026-01-01"))
"""

from __future__ import annotations

import contextlib
import pathlib
import re
import time
from dataclasses import dataclass
from typing import Iterable, List, Sequence
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common.dataset import make_lat_lon_coords_consistent
from planetary_datasets.common.download import cleanup_files
from planetary_datasets.common.store import existing_times
from planetary_datasets.config import Config
from planetary_datasets.memory import memory_guard, require_dataset_fits

#: GES DISC HTTPS archive root for the GPM Level 3 collections.
GESDISC_ROOT = "https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3"

#: Hosts that keep the Authorization header across an Earthdata Login redirect.
EARTHDATA_AUTH_SUFFIX = ".nasa.gov"

#: Variables carried over from the original scripts, in store order.
DEFAULT_VARIABLES = (
    "precipitation",
    "randomError",
    "probabilityLiquidPrecipitation",
    "precipitationQualityIndex",
)

#: Auxiliary bounds dimensions in the HDF5 ``/Grid`` group that are not worth storing.
BOUNDS_DIMS = ("latv", "lonv", "nv")

#: Granules per UTC day.
GRANULES_PER_DAY = 48

_HDF5_HREF = re.compile(r'href="([^"?#]+\.HDF5)"', re.IGNORECASE)


class IncompleteDay(RuntimeError):
    """Raised when a UTC day has fewer granules than it should."""


@dataclass(frozen=True)
class IMERGProduct:
    """One of the three IMERG runs."""

    code: str
    collection: str
    store_prefix: str
    latency: str
    description: str


PRODUCTS: dict[str, IMERGProduct] = {
    "early": IMERGProduct(
        code="early",
        collection="GPM_3IMERGHHE.07",
        store_prefix="bkr/imerg/imerg_early.icechunk",
        latency="~4 hours",
        description="GPM IMERG Early Run, half-hourly 0.1 degree global precipitation",
    ),
    "late": IMERGProduct(
        code="late",
        collection="GPM_3IMERGHHL.07",
        store_prefix="bkr/imerg/imerg_late.icechunk",
        latency="~14 hours",
        description="GPM IMERG Late Run, half-hourly 0.1 degree global precipitation",
    ),
    "final": IMERGProduct(
        code="final",
        collection="GPM_3IMERGHH.07",
        store_prefix="bkr/imerg/imerg_final.icechunk",
        latency="~3.5 months",
        description="GPM IMERG Final Run, gauge-calibrated half-hourly global precipitation",
    ),
}


def get_product(product: str | IMERGProduct) -> IMERGProduct:
    """Look up a product by code, accepting an :class:`IMERGProduct` unchanged."""
    if isinstance(product, IMERGProduct):
        return product
    try:
        return PRODUCTS[product.lower()]
    except KeyError:
        raise ValueError(
            f"unknown IMERG product {product!r}, expected one of {sorted(PRODUCTS)}"
        ) from None


class EarthdataSession(requests.Session):
    """A session that survives the Earthdata Login redirect dance.

    GES DISC answers an unauthenticated data request with a redirect to
    ``urs.earthdata.nasa.gov`` and then redirects back. ``requests`` drops the
    ``Authorization`` header whenever a redirect changes host, which makes the round trip
    fail with a 401; this keeps the header for NASA hosts and strips it everywhere else so
    the credentials are never sent to a third party.
    """

    def rebuild_auth(self, prepared_request, response) -> None:
        headers = prepared_request.headers
        if "Authorization" not in headers:
            return
        original = urlparse(response.request.url)
        redirect = urlparse(prepared_request.url)
        if (original.hostname, original.scheme) == (redirect.hostname, redirect.scheme):
            return
        # An https -> http downgrade would put the password on the wire in the clear, so
        # it is stripped even when the host is NASA's.
        same_family = (redirect.hostname or "").endswith(EARTHDATA_AUTH_SUFFIX)
        downgraded = original.scheme == "https" and redirect.scheme != "https"
        if not same_family or downgraded:
            del headers["Authorization"]


class IMERGProvider(BaseProvider):
    """Download and store one IMERG run, one UTC day at a time.

    Args:
        product: ``"early"``, ``"late"`` or ``"final"``.
        config: Configuration override, mainly for tests.
        variables: Subset of :data:`DEFAULT_VARIABLES` to keep.
    """

    append_dim = "time"

    def __init__(
        self,
        product: str | IMERGProduct = "final",
        config: Config | None = None,
        variables: Sequence[str] | None = None,
    ):
        super().__init__(config)
        self.product = get_product(product)
        self.name = f"imerg-{self.product.code}"
        self.store_prefix = self.product.store_prefix
        self.variables = tuple(variables) if variables is not None else DEFAULT_VARIABLES
        self._session: requests.Session | None = None

    # ------------------------------------------------------------------ locations

    @property
    def staging_dir(self) -> pathlib.Path:
        """Where granules are downloaded when no temporary directory is supplied."""
        return self.config.scratch_dir / "imerg" / self.product.code

    def staging_dir_for(self, it: pd.Timestamp) -> pathlib.Path:
        """Staging directory for one day.

        Derived from the timestamp alone so the Dagster download and write assets agree on
        the location without passing paths between them.
        """
        return self.staging_dir / f"{pd.Timestamp(it).normalize():%Y%m%d}"

    def staged_files(self, it: pd.Timestamp) -> List[str]:
        """Granules already downloaded for ``it``, in filename order."""
        return sorted(str(p) for p in self.staging_dir_for(it).glob("*.HDF5"))

    def listing_url(self, day: pd.Timestamp) -> str:
        """URL of the GES DISC directory index holding one day of granules."""
        return (
            f"{GESDISC_ROOT}/{self.product.collection}/"
            f"{day.strftime('%Y')}/{day.dayofyear:03d}/"
        )

    # ------------------------------------------------------------------ http

    def session(self) -> requests.Session:
        """An authenticated Earthdata session, created on first use and reused.

        Raises :class:`~planetary_datasets.config.MissingCredential` when the Earthdata
        credentials are not configured, rather than making an anonymous request that comes
        back as an unhelpful HTML login page.
        """
        if self._session is None:
            username, password = self.config.credentials.require(
                "earthdata_username", "earthdata_password"
            )
            session = EarthdataSession()
            session.auth = (username, password)
            self._session = session
        return self._session

    def list_remote_granules(self, day: pd.Timestamp, timeout: float = 60.0) -> List[str]:
        """Return the URLs of every HDF5 granule GES DISC holds for ``day``.

        An empty list is returned when the day is not in the archive, which is the normal
        answer for dates before the collection starts or after its latency window.
        """
        url = self.listing_url(day)
        response = self.session().get(url, timeout=timeout)
        if response.status_code == 404:
            logger.info(f"{self.name}: no archive directory for {day.date()} ({url})")
            return []
        response.raise_for_status()

        # urljoin, not string concatenation: the index mixes relative filenames with
        # absolute ``/data/...`` paths, and appending the latter to the directory URL
        # produces a 404 for every granule.
        granules = sorted({urljoin(url, href) for href in _HDF5_HREF.findall(response.text)})
        if granules and len(granules) != GRANULES_PER_DAY:
            logger.warning(
                f"{self.name}: {len(granules)} granules listed for {day.date()}, "
                f"expected {GRANULES_PER_DAY}"
            )
        return granules

    def download_granule(
        self,
        url: str,
        dest: pathlib.Path,
        retries: int = 3,
        timeout: float = 300.0,
        chunk_size: int = 1024 * 1024,
        backoff: float = 2.0,
    ) -> pathlib.Path | None:
        """Download one granule, atomically and with retries.

        ``planetary_datasets.common.download.download_one`` cannot be used here because
        these URLs need the Earthdata Login redirect handling above. The write-to-``.part``
        then rename behaviour and the exponential backoff are the same, so an interrupted
        run never leaves a truncated file that the skip-if-present check would accept, and
        a throttled run gets a chance to wait the limit out.
        """
        if dest.is_file() and dest.stat().st_size > 0:
            logger.debug(f"{self.name}: {dest.name} already downloaded")
            return dest

        dest.parent.mkdir(parents=True, exist_ok=True)
        part = dest.with_name(dest.name + ".part")
        for attempt in range(1, retries + 1):
            try:
                with self.session().get(url, stream=True, timeout=timeout) as response:
                    response.raise_for_status()
                    with open(part, "wb") as out:
                        for chunk in response.iter_content(chunk_size=chunk_size):
                            out.write(chunk)
                part.replace(dest)
                return dest
            except Exception as exc:  # noqa: BLE001 - any transport error is worth retrying
                part.unlink(missing_ok=True)
                if attempt == retries:
                    logger.warning(f"{self.name}: failed to download {url}: {exc}")
                    return None
                sleep = backoff * (2 ** (attempt - 1))
                logger.debug(
                    f"{self.name}: attempt {attempt}/{retries} for {url} failed ({exc}), "
                    f"retrying in {sleep:.1f}s"
                )
                time.sleep(sleep)
        return None

    # ------------------------------------------------------------------ provider API

    def fetch(
        self,
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> List[str]:
        """Download every granule for the UTC day starting at ``it``."""
        day = pd.Timestamp(it).normalize()
        if temp_dir is None:
            dest_dir = self.staging_dir_for(day)
        else:
            dest_dir = pathlib.Path(temp_dir) / f"{day:%Y%m%d}"

        granules = self.list_remote_granules(day)
        if not granules:
            return []

        paths = [self.download_granule(url, dest_dir / url.rsplit("/", 1)[-1]) for url in granules]
        downloaded = [str(p) for p in paths if p is not None]
        if len(downloaded) != len(granules):
            logger.warning(
                f"{self.name}: downloaded {len(downloaded)}/{len(granules)} granules "
                f"for {day.date()}"
            )
        return downloaded

    def open_granule(self, filename: str | pathlib.Path) -> xr.Dataset:
        """Open one IMERG HDF5 granule as a tidy, chunked dataset.

        The ``/Grid`` group carries cell-bounds dimensions that no consumer of the store
        needs, and names its axes ``lat``/``lon`` where every other dataset here uses
        ``latitude``/``longitude``.
        """
        data = xr.open_dataset(filename, group="/Grid")
        drop = [dim for dim in BOUNDS_DIMS if dim in data.dims]
        if drop:
            data = data.drop_dims(drop)
        data = data.rename({"lat": "latitude", "lon": "longitude"})

        missing = [v for v in self.variables if v not in data.data_vars]
        if missing:
            raise ValueError(
                f"{self.name}: {pathlib.Path(filename).name} is missing "
                f"{sorted(missing)}; found {sorted(data.data_vars)}"
            )
        data = data[list(self.variables)]

        # Chunk before casting: ``astype`` on a backend-lazy variable would load it, so a
        # day's worth of granules would all be resident before the memory guard around
        # ``process`` ever got to size them. On a dask-backed variable it stays lazy.
        data = data.chunk({"time": 1, "latitude": -1, "longitude": -1})

        # Cast the data variables only. float16 is what the original stores use and halves
        # their size; the coordinates must stay at full precision or the 0.1 degree grid
        # would not round-trip.
        for var in data.data_vars:
            data[var] = data[var].astype("float16")
        return data

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Concatenate a day of granules into one time-sorted dataset."""
        if not input_files:
            raise ValueError(f"{self.name}: no input files to process for {it}")

        granules = [self.open_granule(f) for f in sorted(input_files)]
        data = granules[0] if len(granules) == 1 else xr.concat(granules, dim="time")
        data = data.sortby("time")
        data = make_lat_lon_coords_consistent(data)
        data = data.chunk({"time": 1, "latitude": -1, "longitude": -1})
        data.attrs.update(
            {
                "title": self.product.description,
                "source": f"{GESDISC_ROOT}/{self.product.collection}",
                "imerg_product": self.product.code,
                "imerg_collection": self.product.collection,
                "latency": self.product.latency,
            }
        )
        return data

    def write_staged(
        self,
        it: pd.Timestamp,
        cleanup: bool = True,
        allow_partial: bool = False,
    ) -> bool:
        """Process whatever is staged for ``it`` and append it to the store.

        The counterpart to :meth:`fetch` with no ``temp_dir``: the Dagster download asset
        stages a day, this writes it. Any granules still missing are fetched first, so the
        two can also be run as one step. Returns True when data was written.

        A short day is refused rather than written. Committing 30 of 48 granules would put
        the day's first timestep in the store, and the skip-if-present check would then
        call the day done forever, leaving a silent permanent gap.

        Args:
            it: Partition timestamp, the start of the UTC day.
            cleanup: Remove the staged granules once they are safely committed. Unlike the
                original asset, which deleted each file as soon as its region write
                returned, nothing is removed unless the commit succeeded.
            allow_partial: Write a day even if fewer than 48 granules are available. Only
                for days the archive genuinely never completed.

        Raises:
            IncompleteDay: When the day is short and ``allow_partial`` is False.
        """
        day = pd.Timestamp(it).normalize()
        if not self.missing_timesteps(pd.DatetimeIndex([day])):
            logger.debug(f"{self.name}: {day.date()} already in {self.store_path}, skipping")
            return False

        files = self.staged_files(day)
        if len(files) < GRANULES_PER_DAY:
            # fetch skips granules that are already on disk, so this fills the gaps left
            # by a run that timed out or by individual downloads that gave up.
            files = self.fetch(day) or files
        if not files:
            logger.info(f"{self.name}: no granules staged for {day.date()}, skipping")
            return False
        if len(files) < GRANULES_PER_DAY and not allow_partial:
            raise IncompleteDay(
                f"{self.name}: only {len(files)}/{GRANULES_PER_DAY} granules for "
                f"{day.date()}; refusing to write a partial day. Retry later, or pass "
                "allow_partial=True if the archive never completed this day."
            )

        with memory_guard(what=f"{self.name} {day.date()}"):
            processed = self.process(files, day)
            require_dataset_fits(processed, what=f"{self.name} {day.date()}")
            written = self.write_to_icechunk(self.get_icechunk_repo(), processed)

        # Only discard the inputs once they are committed. write_to_icechunk returns False
        # both for "already there" and for a rejected write, and re-downloading 1.4 GB to
        # diagnose the latter is not a good trade.
        if cleanup and written:
            self.cleanup_staged(day)
        return written

    def run_partition(self, it: pd.Timestamp) -> bool:
        """Fetch, process and write one UTC day.

        Overrides the base implementation to go through :meth:`write_staged`, so a run
        from the CLI gets the same short-day guard and the same resumable staging
        directory as the Dagster chain instead of a throwaway temporary directory.
        """
        return self.write_staged(it)

    def cleanup_staged(self, it: pd.Timestamp) -> int:
        """Delete the staged granules for a day, and the sidecar XML files beside them."""
        day_dir = self.staging_dir_for(it)
        if not day_dir.is_dir():
            return 0
        removed = cleanup_files(*day_dir.iterdir())
        with contextlib.suppress(OSError):
            day_dir.rmdir()
        logger.debug(f"{self.name}: removed {removed} staged file(s) from {day_dir}")
        return removed

    # ------------------------------------------------------------------ publish

    def publish(self) -> dict:
        """Report where the store is published.

        The provider appends directly to the location :class:`Config` resolves, which is
        the public bucket unless ``ICECHUNK_LOCAL_PATH`` redirects it. That replaces the
        ``aws s3 sync --profile=sc`` step the original assets ran: there is no second copy
        to push, so publishing is a matter of confirming the target and its extent.
        """
        times = existing_times(self.get_icechunk_repo(), append_dim=self.append_dim)
        info = {
            "store_path": self.store_path,
            "published": not self.config.use_local_store,
            "timesteps": int(times.size),
            "first_time": str(times[0]) if times.size else None,
            "last_time": str(times[-1]) if times.size else None,
        }
        logger.info(
            f"{self.name}: {info['timesteps']} timestep(s) at {info['store_path']}"
            + ("" if info["published"] else " (local store, not published)")
        )
        return info


def day_range(start: str | pd.Timestamp, end: str | pd.Timestamp) -> pd.DatetimeIndex:
    """Daily partition timestamps between ``start`` and ``end`` inclusive."""
    return pd.date_range(pd.Timestamp(start).normalize(), pd.Timestamp(end).normalize(), freq="1D")


def backfill(
    product: str | IMERGProduct,
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
    config: Config | None = None,
    variables: Sequence[str] | None = None,
) -> int:
    """Run every missing day in a range. Returns the number of days written.

    This is the supported replacement for the ad hoc ``multiprocessing.Pool`` driver that
    used to run at import time in ``imerg_late_run.py``. Days are processed one at a time:
    each one holds ~2.5 GB of half-hourly fields in flight, so running 'one per core' was
    what made that script unusable on anything but the largest host.
    """
    provider = IMERGProvider(product, config=config, variables=variables)
    return provider.run_range(day_range(start, end))


def backfill_all(
    products: Iterable[str] = ("early", "late", "final"),
    start: str | pd.Timestamp = "2000-06-01",
    end: str | pd.Timestamp = "2026-12-31",
    config: Config | None = None,
) -> dict[str, int]:
    """Backfill several products in turn. Returns days written per product."""
    return {str(p): backfill(p, start, end, config=config) for p in products}
