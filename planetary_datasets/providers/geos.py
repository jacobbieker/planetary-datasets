"""NASA GEOS-CF 15-minute global analysis.

GMAO publishes the GEOS Composition Forecast (GEOS-CF) single-level analysis as one NetCDF4
file per 15-minute instant on the NCCS datashare portal. Two collections are archived here:

* **v1** — ``GEOS-CF.v01.rpl.htf_inst_15mn_g1440x721_x1``, written to
  ``bkr/geos/geos_15min.icechunk``.
* **v2** — ``GEOS.cf.ana.htf_inst_15mn_glo_L1440x721_slv``, written to
  ``bkr/geos/geos_v2_15min.icechunk``. v2 files are republished under revision suffixes
  (``.R0``, ``.R1``) as well as unsuffixed, so several candidate URLs are tried per instant.

One partition is one 15-minute instant, which is also one source file. Backfilling a whole
day is ``provider.run_day(day)``.
"""

from __future__ import annotations

import os
import pathlib
from dataclasses import dataclass
from typing import List, Sequence

import icechunk
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common import download_one

PORTAL = "https://portal.nccs.nasa.gov/datashare/gmao/geos-cf"

STORE_PREFIXES: dict[int, str] = {
    1: "bkr/geos/geos_15min.icechunk",
    2: "bkr/geos/geos_v2_15min.icechunk",
}

#: v2 files appear under these revision suffixes. They are tried in order and the first that
#: downloads wins, which reproduces what the original scripts settled on: they fetched all
#: three and wrote them in sorted filename order, so ``R0`` landed first and the later
#: revisions were skipped as duplicate timesteps.
V2_REVISIONS: tuple[str, ...] = (".R0", ".R1", "")

#: Sea-level pressure needs the full float32 range; every other v1 variable is stored as
#: float16, which is how the v1 store was originally created.
FULL_PRECISION_VARS: tuple[str, ...] = ("SLP",)


@dataclass(frozen=True)
class DayResult:
    """Outcome of running one day's worth of instants.

    Attributes:
        attempted: Instants that were missing from the store when the day started.
        written: Instants successfully appended.
        failed: Instants that raised. Instants the portal has not published are neither
            written nor failed: they are simply not there yet.
    """

    attempted: int = 0
    written: int = 0
    failed: int = 0

    @property
    def unavailable(self) -> int:
        """Instants that were missing and neither written nor failed."""
        return self.attempted - self.written - self.failed


def build_urls(it: pd.Timestamp, version: int) -> list[str]:
    """Candidate source URLs for a single 15-minute instant, best first."""
    if version not in STORE_PREFIXES:
        raise ValueError(f"unknown GEOS-CF version {version!r}, expected 1 or 2")

    day = f"Y{it.year:04d}/M{it.month:02d}/D{it.day:02d}"
    stamp = f"{it.strftime('%Y%m%d')}_{it.hour:02d}{it.minute:02d}z"

    if version == 1:
        return [f"{PORTAL}/v1/ana/{day}/GEOS-CF.v01.rpl.htf_inst_15mn_g1440x721_x1.{stamp}.nc4"]

    base = f"{PORTAL}/v2/ana/{day}/GEOS.cf.ana.htf_inst_15mn_glo_L1440x721_slv.{stamp}"
    return [f"{base}{revision}.nc4" for revision in V2_REVISIONS]


def preprocess_geos(ds: xr.Dataset, to_float16: bool = False) -> xr.Dataset:
    """Rename the spatial coordinates and drop the degenerate vertical dimension.

    GEOS-CF single-level files carry a length-one ``lev`` dimension that would otherwise be
    written into the store for no benefit.

    Args:
        ds: Dataset as read from a GEOS-CF NetCDF4 file.
        to_float16: Downcast every variable except :data:`FULL_PRECISION_VARS`.
    """
    pairs = (("lon", "longitude"), ("lat", "latitude"))
    renames = {old: new for old, new in pairs if old in ds.variables}
    if renames:
        ds = ds.rename(renames)
    if "lev" in ds.dims:
        ds = ds.isel(lev=0)
    if "lev" in ds.variables:
        ds = ds.drop_vars("lev")
    if to_float16:
        for var in ds.data_vars:
            if var not in FULL_PRECISION_VARS:
                ds[var] = ds[var].astype("float16")
    return ds


class GEOSProvider(BaseProvider):
    """NASA GEOS-CF 15-minute analysis, one partition per instant.

    Args:
        version: 1 or 2, selecting the GEOS-CF collection and its store.
        to_float16: Downcast all but :data:`FULL_PRECISION_VARS`. Defaults to True for v1,
            matching how that store was created, and False for v2.
        retries: Download attempts per candidate URL before moving to the next revision.
    """

    name = "geos"
    append_dim = "time"
    store_prefix = STORE_PREFIXES[1]

    def __init__(
        self,
        version: int = 1,
        to_float16: bool | None = None,
        retries: int = 3,
        config=None,
    ):
        """Configure the provider for one GEOS-CF collection."""
        super().__init__(config=config)
        if version not in STORE_PREFIXES:
            raise ValueError(f"unknown GEOS-CF version {version!r}, expected 1 or 2")
        self.version = version
        self.store_prefix = STORE_PREFIXES[version]
        self.name = f"geos_v{version}"
        self.to_float16 = (version == 1) if to_float16 is None else to_float16
        self.retries = retries
        self._repo: icechunk.Repository | None = None

    @staticmethod
    def day_timestamps(day: pd.Timestamp) -> pd.DatetimeIndex:
        """The 96 instants belonging to ``day``, for backfilling a whole day."""
        start = pd.Timestamp(day).normalize()
        return pd.date_range(start, start + pd.Timedelta(1, "D"), freq="15min", inclusive="left")

    def get_icechunk_repo(self) -> icechunk.Repository:
        """Open the store, first disabling the AWS checksum headers source.coop rejects.

        The original scripts set this at import time, which changed the behaviour of every
        S3 client in the process. Doing it here scopes it to the moment a GEOS store is
        actually opened. The assignment is unconditional: an inherited
        ``AWS_REQUEST_CHECKSUM_CALCULATION=WHEN_SUPPORTED`` would make every commit to
        source.coop fail with an opaque S3 error.

        The handle is cached because a day partition opens the store once per instant.
        """
        os.environ["AWS_REQUEST_CHECKSUM_CALCULATION"] = "WHEN_REQUIRED"
        if self._repo is None:
            self._repo = super().get_icechunk_repo()
        return self._repo

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Download the first available revision of the file for ``it``.

        With several candidate revisions, all of them are probed with a single attempt
        first. A missing revision answers 404 immediately and permanently, so retrying it
        with backoff before trying the next candidate would spend most of a backfill asleep.
        Only if every candidate fails the cheap pass is the full retry budget spent, which
        is what distinguishes "not published" from a transient network failure.
        """
        default_dir = self.config.scratch_dir / self.name
        dest_dir = pathlib.Path(temp_dir) if temp_dir is not None else default_dir
        dest_dir.mkdir(parents=True, exist_ok=True)

        urls = build_urls(it, self.version)
        # With a single candidate there is nothing to probe, so go straight to full retries.
        passes = (1, self.retries) if len(urls) > 1 else (self.retries,)

        for retries in passes:
            for url in urls:
                path = download_one(url, dest_dir / url.rsplit("/", 1)[-1], retries=retries)
                if path is not None:
                    return [str(path)]

        logger.warning(f"{self.name}: no GEOS-CF file published for {it}")
        return []

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Open the downloaded file and shape it for the store."""
        # Read eagerly and close the file: the temporary directory it lives in is removed as
        # soon as run_partition returns, and a lazy dataset would break on the write.
        with xr.open_dataset(input_files[0]) as raw:
            ds = preprocess_geos(raw, to_float16=self.to_float16).load()

        if "time" not in ds.dims:
            ds = ds.expand_dims("time")
        ds = ds.sortby("time")

        stored = pd.Timestamp(ds["time"].values[0])
        if stored != pd.Timestamp(it):
            # Trust the partition key rather than the file: an off-by-one here would write a
            # timestep that run_partition has already decided is missing, corrupting the axis.
            raise ValueError(f"{self.name}: file for {it} reports time {stored}")

        return ds.chunk({"time": 1, "latitude": -1, "longitude": -1})

    def run_day(self, day: pd.Timestamp) -> DayResult:
        """Run every missing instant in ``day``.

        Unlike :meth:`~planetary_datasets.base.BaseProvider.run_range`, which reports only a
        count, this separates "the portal has not published these instants" from "every
        instant raised". A caller that cannot tell them apart would record a day as done
        after an outage wrote nothing, and never come back to it.
        """
        attempted = self.missing_timesteps(self.day_timestamps(day))
        written = 0
        failed = 0
        for it in attempted:
            try:
                if self.run_partition(it):
                    written += 1
            except Exception as exc:  # noqa: BLE001 - one bad instant must not stop the day
                failed += 1
                logger.exception(f"{self.name}: instant {it} failed: {exc}")
        return DayResult(attempted=len(attempted), written=written, failed=failed)

    def run_days(self, days: Sequence[pd.Timestamp]) -> DayResult:
        """Run every missing instant across several days, summing the results."""
        total = DayResult()
        for day in days:
            result = self.run_day(day)
            total = DayResult(
                attempted=total.attempted + result.attempted,
                written=total.written + result.written,
                failed=total.failed + result.failed,
            )
        return total
