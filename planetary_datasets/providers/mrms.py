"""NOAA Multi-Radar Multi-Sensor (MRMS) radar mosaics.

MRMS publishes gridded radar products for five domains — CONUS plus Alaska, the Caribbean,
Guam and Hawaii — as gzipped GRIB2, one file per product per timestep. Instantaneous
products such as ``PrecipRate`` and ``PrecipFlag`` arrive every two minutes; the hourly QPE
accumulations arrive on the hour.

Everything here is driven by one engine, :class:`MRMSProvider`. The historical scripts
differed only in three things, which are now parameters:

* which **products** to keep (``PrecipFlag`` + ``PrecipRate``, rate only, or a QPE field),
* which **domain** to read (``CONUS`` or one of the regional grids),
* where the store lives (``store_prefix``).

Inputs are resolved in this order for each timestep:

1. A local mirror of the archive under ``<data_dir>/MRMS`` — either the AWS-style layout
   (``aws/<REGION>/<YYYYMMDD>/``) or the Iowa State ``mtarchive`` tree.
2. The public, anonymous ``noaa-mrms-pds`` bucket (2020-10-14 onwards).
3. The Iowa State mtarchive HTTPS mirror, which is the only source for CONUS before the
   AWS archive starts.
"""

from __future__ import annotations

import contextlib
import gzip
import pathlib
import shutil
import tempfile
from dataclasses import dataclass
from functools import lru_cache
from glob import glob
from typing import Dict, List, Sequence

import icechunk
import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common.download import cleanup_files, download_one
from planetary_datasets.common.store import existing_times

AWS_BUCKET = "noaa-mrms-pds"
AWS_HTTPS_ROOT = f"https://{AWS_BUCKET}.s3.amazonaws.com"
IASTATE_ROOT = "https://mtarchive.geol.iastate.edu"
IASTATE_MIRROR_DIR = "mtarchive.geol.iastate.edu"

#: MRMS domains, named as they appear in the public bucket and in the store names.
REGIONS: tuple[str, ...] = ("CONUS", "ALASKA", "CARIB", "GUAM", "HAWAII")

#: Domains other than CONUS. These are the ones written to per-region stores.
NON_CONUS_REGIONS: tuple[str, ...] = tuple(r for r in REGIONS if r != "CONUS")

#: The AWS archive only goes back this far; earlier CONUS data comes from Iowa State.
AWS_ARCHIVE_START = pd.Timestamp("2020-10-14")


@dataclass(frozen=True)
class MRMSProduct:
    """One MRMS product: its archive name, the variable it becomes, and its stored dtype."""

    product: str
    variable: str
    dtype: str
    #: Native cadence of the product, as a pandas frequency string.
    freq: str = "2min"


PRODUCTS: Dict[str, MRMSProduct] = {
    p.product: p
    for p in (
        MRMSProduct("PrecipFlag", "precipitation_flag", "int8"),
        MRMSProduct("PrecipRate", "precipitation_rate", "float16"),
        MRMSProduct("RadarOnly_QPE_01H", "radar_only_qpe_01h", "float32", freq="1h"),
        MRMSProduct("GaugeCorr_QPE_01H", "gauge_corrected_qpe_01h", "float32", freq="1h"),
        MRMSProduct(
            "MultiSensor_QPE_01H_Pass1", "multisensor_qpe_01h_pass1", "float32", freq="1h"
        ),
        MRMSProduct(
            "MultiSensor_QPE_01H_Pass2", "multisensor_qpe_01h_pass2", "float32", freq="1h"
        ),
    )
}

#: The pairing every historical MRMS script wrote: instantaneous rate plus its type flag.
DEFAULT_PRODUCTS: tuple[str, ...] = ("PrecipFlag", "PrecipRate")


def naive_utc(timestamp) -> pd.Timestamp:
    """Normalise a timestamp to timezone-naive UTC.

    Dagster hands a partition start over as a timezone-aware UTC datetime, while archive
    filenames and the stored ``time`` coordinate are naive. Comparing the two directly
    either raises or, worse, silently never matches, so every timestamp entering a provider
    goes through here first.
    """
    ts = pd.Timestamp(timestamp)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts


def timestamp_from_filename(filename: str | pathlib.Path) -> pd.Timestamp:
    """Extract the observation time from an MRMS filename.

    Handles both ``MRMS_PrecipRate_00.00_20260920-000000.grib2.gz`` and the older
    ``MRMS_PrecipRate_20160122-194600.grib2.gz`` naming.
    """
    basename = pathlib.Path(filename).name
    if "_00.00_" in basename:
        stamp = basename.split("_00.00_")[-1].split(".")[0]
    else:
        stamp = basename.split("_")[-1].split(".")[0]
    return pd.to_datetime(stamp, format="%Y%m%d-%H%M%S")


def product_from_filename(filename: str | pathlib.Path, products: Sequence[str]) -> str | None:
    """Return which of ``products`` a filename belongs to, or None if it matches none.

    The longest name is tried first so ``MultiSensor_QPE_01H_Pass1`` is not mistaken for a
    prefix of a shorter product name.
    """
    basename = pathlib.Path(filename).name
    for product in sorted(products, key=len, reverse=True):
        if product in basename:
            return product
    return None


def load_mrms_grib(
    filepath: str | pathlib.Path,
    variable: str,
    dtype: str,
    temp_dir: str | pathlib.Path | None = None,
) -> xr.Dataset:
    """Decompress one gzipped MRMS GRIB2 file and return it as a single-timestep dataset.

    The GRIB carries the field as ``unknown``; it is renamed to ``variable`` and cast to
    ``dtype``. Only data variables are cast — casting the whole dataset would also touch
    the latitude and longitude coordinates.
    """
    filepath = pathlib.Path(filepath)
    timestamp = timestamp_from_filename(filepath)

    with contextlib.ExitStack() as stack:
        if temp_dir is None:
            # Never unzip next to the source: the local archive mirror may be read-only,
            # and cfgrib drops an index sidecar beside whatever it opens.
            work = pathlib.Path(stack.enter_context(tempfile.TemporaryDirectory(prefix="mrms-")))
        else:
            work = pathlib.Path(temp_dir)
            work.mkdir(parents=True, exist_ok=True)

        unzipped = work / f"{filepath.name.removesuffix('.grib2.gz')}.{variable}.grib2"
        try:
            with gzip.open(filepath, "rb") as src, open(unzipped, "wb") as out:
                shutil.copyfileobj(src, out)
            ds = xr.load_dataset(unzipped, engine="cfgrib", decode_timedelta=False)
        finally:
            cleanup_files(unzipped)

    if "unknown" in ds.data_vars:
        ds = ds.rename({"unknown": variable})
    elif len(ds.data_vars) == 1:
        ds = ds.rename({next(iter(ds.data_vars)): variable})
    else:
        raise ValueError(f"cannot tell which variable in {filepath.name} is {variable!r}")

    ds = ds.expand_dims(dim="time")
    ds["time"] = [timestamp]
    ds = ds.drop_vars(["step", "heightAboveSea", "valid_time"], errors="ignore")
    ds[variable] = ds[variable].astype(dtype)
    return ds.chunk({"latitude": -1, "longitude": -1})


def _list_aws_day_uncached(region: str, product: str, day: str) -> frozenset[str]:
    """Filenames available on the public bucket for one region, product and ``YYYYMMDD``."""
    import s3fs

    fs = s3fs.S3FileSystem(anon=True)
    prefix = f"{AWS_BUCKET}/{region}/{product}_00.00/{day}"
    try:
        keys = fs.ls(prefix, detail=False)
    except FileNotFoundError:
        logger.debug(f"mrms: no AWS archive at {prefix}")
        return frozenset()
    return frozenset(k.split("/")[-1] for k in keys)


_list_aws_day_cached = lru_cache(maxsize=256)(_list_aws_day_uncached)


def list_aws_day(region: str, product: str, day: str) -> frozenset[str]:
    """Filenames on the public bucket for one region, product and ``YYYYMMDD``.

    Listing a day once is far cheaper than probing each of up to 720 timesteps, and it
    stops gaps in the archive from turning into retry storms. A day that is still being
    written to is deliberately not cached: a long-running backfill that reaches today
    would otherwise keep seeing the listing it took on its first hour.
    """
    today = naive_utc(pd.Timestamp.now("UTC")).strftime("%Y%m%d")
    if day >= today:
        return _list_aws_day_uncached(region, product, day)
    return _list_aws_day_cached(region, product, day)


class MRMSProvider(BaseProvider):
    """MRMS radar mosaics for one domain and one set of products.

    One partition is one hour, written as a single append. Timesteps missing any of the
    requested products are dropped rather than written half-empty, which is what keeps the
    ``PrecipFlag``/``PrecipRate`` pair aligned in the store.

    Args:
        region: One of :data:`REGIONS`.
        products: Archive product names to merge into each timestep. Defaults to
            :data:`DEFAULT_PRODUCTS`.
        store_prefix: Override the derived store location.
        freq: Cadence to sample within the partition. Defaults to the coarsest native
            cadence of the requested products.
        archive_dir: Local mirror of the archive. Defaults to ``<data_dir>/MRMS``.
        config: Optional configuration override.
    """

    name = "mrms"
    append_dim = "time"
    store_prefix = "bkr/mrms/mrms.icechunk"

    #: One hour of two-minute fields at full CONUS resolution is a few GB decompressed;
    #: the guard keeps a wide partition from taking the host down.
    guard_memory = True

    partition_span = pd.Timedelta(hours=1)

    #: How long an hour may still gain files before its gaps are treated as permanent.
    settle_after = pd.Timedelta(days=2)

    def __init__(
        self,
        region: str = "CONUS",
        products: Sequence[str] | None = None,
        store_prefix: str | None = None,
        freq: str | None = None,
        archive_dir: str | pathlib.Path | None = None,
        config=None,
    ):
        super().__init__(config=config)
        if region not in REGIONS:
            raise ValueError(f"unknown MRMS region {region!r}, expected one of {REGIONS}")

        self.region = region
        self.products = tuple(products) if products else DEFAULT_PRODUCTS
        unknown = [p for p in self.products if p not in PRODUCTS]
        if unknown:
            raise ValueError(
                f"unknown MRMS product(s) {unknown}, expected one of {sorted(PRODUCTS)}"
            )

        self.freq = freq or self._coarsest_freq()
        self.store_prefix = store_prefix or self._default_store_prefix()
        self.name = f"mrms-{region.lower()}"
        self._archive_dir = pathlib.Path(archive_dir) if archive_dir is not None else None

    def _coarsest_freq(self) -> str:
        """The slowest native cadence among the requested products.

        Merging a two-minute field with an hourly one only lines up on the hour, so the
        slowest product sets the pace.
        """
        return max(
            (PRODUCTS[p].freq for p in self.products),
            key=lambda f: pd.Timedelta(f),
        )

    def _default_store_prefix(self) -> str:
        """Where this combination of domain and products is published.

        The three names the archive already uses are kept verbatim so appends land in the
        existing stores; anything else gets a name derived from its variables.
        """
        if self.region == "CONUS":
            if self.products == DEFAULT_PRODUCTS:
                return "bkr/mrms/mrms.icechunk"
            if self.products == ("PrecipRate",):
                return "bkr/mrms/mrms_preciprate.icechunk"
        if self.products == DEFAULT_PRODUCTS:
            return f"bkr/mrms/mrms_{self.region}.icechunk"
        slug = "_".join(PRODUCTS[p].variable for p in self.products)
        if self.region == "CONUS":
            return f"bkr/mrms/mrms_{slug}.icechunk"
        return f"bkr/mrms/mrms_{self.region}_{slug}.icechunk"

    @property
    def archive_dir(self) -> pathlib.Path:
        """Local mirror of the MRMS archive, if one has been downloaded."""
        if self._archive_dir is not None:
            return self._archive_dir
        return self.config.data_dir / "MRMS"

    def partition_timestamps(self, it: pd.Timestamp) -> pd.DatetimeIndex:
        """The timesteps that make up the partition starting at ``it``."""
        it = naive_utc(it)
        step = pd.Timedelta(self.freq)
        return pd.date_range(it, it + self.partition_span - step, freq=self.freq)

    def run_partition(self, it: pd.Timestamp, check_present: bool = True) -> bool:
        """Run one hour, accepting a timezone-aware partition start from Dagster.

        ``check_present`` has to be forwarded, not dropped: :meth:`BaseProvider.run_range`
        passes it explicitly after filtering the timestamps itself, and an override that
        does not accept it turns every partition of a backfill into a ``TypeError``.
        """
        return super().run_partition(naive_utc(it), check_present=check_present)

    def missing_timesteps(self, desired: pd.DatetimeIndex) -> List[pd.Timestamp]:
        """Which partitions in ``desired`` still have work to do.

        A partition is one hour of many timesteps, so "already stored" cannot be decided
        from its first timestep alone: an hour written while the archive was still filling
        in would otherwise be sealed half-empty forever. A recent hour is therefore
        re-attempted until every timestep is present, while an hour older than
        :attr:`settle_after` counts as done once any of it is stored — by then the gaps are
        real gaps, not publishing lag, and re-downloading them every pass is pure waste.
        """
        present = existing_times(self.get_icechunk_repo(), append_dim=self.append_dim)
        if present.size == 0:
            return [naive_utc(it) for it in desired]

        now = naive_utc(pd.Timestamp.now("UTC"))
        missing: List[pd.Timestamp] = []
        for raw in desired:
            it = naive_utc(raw)
            stored = np.isin(self.partition_timestamps(it).values, present)
            if stored.all():
                continue
            if stored.any() and (now - it) > self.settle_after:
                logger.debug(f"{self.name}: {it} is partial but settled, leaving it alone")
                continue
            missing.append(it)
        return missing

    def local_file(self, product: str, timestamp: pd.Timestamp) -> str | None:
        """Find ``product`` at ``timestamp`` in the local archive mirror, if present."""
        timestamp = naive_utc(timestamp)
        stamp = timestamp.strftime("%Y%m%d-%H%M%S")
        root = self.archive_dir
        patterns = [
            str(
                root
                / "aws"
                / self.region
                / timestamp.strftime("%Y%m%d")
                / f"MRMS_{product}_*{stamp}.grib2.gz"
            ),
            str(
                root
                / IASTATE_MIRROR_DIR
                / timestamp.strftime("%Y")
                / timestamp.strftime("%m")
                / timestamp.strftime("%d")
                / "mrms"
                / "ncep"
                / product
                / f"*{stamp}.grib2.gz"
            ),
        ]
        for pattern in patterns:
            matches = sorted(glob(pattern))
            if matches:
                return matches[0]
        return None

    def remote_url(self, product: str, timestamp: pd.Timestamp) -> str | None:
        """URL for ``product`` at ``timestamp``, or None if the archive has no such file."""
        timestamp = naive_utc(timestamp)
        day = timestamp.strftime("%Y%m%d")
        stamp = timestamp.strftime("%Y%m%d-%H%M%S")
        filename = f"MRMS_{product}_00.00_{stamp}.grib2.gz"

        if timestamp >= AWS_ARCHIVE_START:
            if filename in list_aws_day(self.region, product, day):
                return f"{AWS_HTTPS_ROOT}/{self.region}/{product}_00.00/{day}/{filename}"
            return None

        if self.region != "CONUS":
            # Only CONUS is mirrored at Iowa State, and only CONUS predates the AWS archive.
            return None
        return (
            f"{IASTATE_ROOT}/{timestamp:%Y}/{timestamp:%m}/{timestamp:%d}"
            f"/mrms/ncep/{product}/{filename}"
        )

    def fetch(
        self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs
    ) -> List[str]:
        """Collect the files for the hour starting at ``it``.

        Only timesteps that have every requested product are returned. An hour in which
        one product never published is then an empty fetch, which the base class treats as
        "nothing to do" rather than an error — the right outcome for a routine archive gap.
        """
        dest_root = pathlib.Path(temp_dir) if temp_dir is not None else self.config.scratch_dir
        files: List[str] = []

        for timestamp in self.partition_timestamps(it):
            step_files: List[str] = []
            for product in self.products:
                local = self.local_file(product, timestamp)
                if local is not None:
                    step_files.append(local)
                    continue
                url = self.remote_url(product, timestamp)
                if url is None:
                    continue
                dest = dest_root / product / url.split("/")[-1]
                downloaded = download_one(url, dest, retries=2, backoff=0.5)
                if downloaded is not None:
                    step_files.append(str(downloaded))

            if len(step_files) == len(self.products):
                files.extend(step_files)
            elif step_files:
                logger.debug(
                    f"{self.name}: {timestamp} has {len(step_files)}/{len(self.products)} "
                    "product(s), skipping timestep"
                )

        logger.debug(f"{self.name}: {len(files)} file(s) for {naive_utc(it)}")
        return files

    def group_by_timestamp(self, input_files: Sequence[str]) -> Dict[pd.Timestamp, Dict[str, str]]:
        """Index the fetched files by timestamp and product."""
        grouped: Dict[pd.Timestamp, Dict[str, str]] = {}
        for path in input_files:
            product = product_from_filename(path, self.products)
            if product is None:
                continue
            grouped.setdefault(timestamp_from_filename(path), {})[product] = path
        return grouped

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Merge the products at each timestep and concatenate the hour along ``time``."""
        grouped = self.group_by_timestamp(input_files)
        wanted = set(self.products)

        steps: List[xr.Dataset] = []
        for timestamp in sorted(grouped):
            available = grouped[timestamp]
            if set(available) != wanted:
                logger.debug(
                    f"{self.name}: {timestamp} has {sorted(available)}, "
                    f"needs {sorted(wanted)}, skipping timestep"
                )
                continue
            merged = xr.merge(
                [
                    load_mrms_grib(
                        available[product],
                        PRODUCTS[product].variable,
                        PRODUCTS[product].dtype,
                        temp_dir=temp_dir,
                    )
                    for product in self.products
                ]
            )
            steps.append(merged)

        if not steps:
            raise FileNotFoundError(f"no complete MRMS timesteps for {it} in {self.region}")

        combined = xr.concat(steps, dim="time")
        return combined.chunk({"time": 1, "latitude": -1, "longitude": -1})

    def write_to_icechunk(self, repo: icechunk.Repository, processed: xr.Dataset) -> bool:
        """Append only the timesteps the store does not already hold.

        A partition covers thirty two-minute steps, and the archive is not gap-free: the
        first step of an hour is often missing, so the base class's "is the partition start
        already stored?" check cannot stand in for a per-timestep check. Filtering here
        keeps a re-run from appending duplicate times.

        Revisiting a settled-but-partial hour can still surface a step that sorts *before*
        what is already stored; the base writer's monotonicity guard drops those, since
        icechunk cannot insert and an unsorted ``time`` would break slicing store-wide.
        """
        present = existing_times(repo, append_dim=self.append_dim)
        if present.size:
            keep = ~np.isin(processed[self.append_dim].values, present)
            if not keep.any():
                logger.debug(f"{self.name}: all {keep.size} timestep(s) already stored")
                return False
            if not keep.all():
                logger.info(
                    f"{self.name}: dropping {int((~keep).sum())} already-stored timestep(s)"
                )
                processed = processed.isel({self.append_dim: keep})
        return super().write_to_icechunk(repo, processed)


class MRMSPrecipRateProvider(MRMSProvider):
    """CONUS ``PrecipRate`` on its own, for consumers that do not need the type flag."""

    def __init__(self, **kwargs):
        kwargs.setdefault("products", ("PrecipRate",))
        super().__init__(**kwargs)
        self.name = "mrms-preciprate"


def region_providers(**kwargs) -> List[MRMSProvider]:
    """One provider per non-CONUS domain, each writing ``bkr/mrms/mrms_<REGION>.icechunk``.

    ``kwargs`` are passed to every provider; ``region`` is set per domain and so cannot be
    supplied. Construct :class:`MRMSProvider` directly to target a single domain.
    """
    if "region" in kwargs:
        raise TypeError(
            "region_providers() builds one provider per region; "
            "use MRMSProvider(region=...) for a single domain"
        )
    return [MRMSProvider(region=region, **kwargs) for region in NON_CONUS_REGIONS]


def default_providers(**kwargs) -> List[MRMSProvider]:
    """The full set the archive is built from: CONUS, rate-only, and the four regions.

    ``kwargs`` are passed to every provider; ``region``, ``products`` and ``store_prefix``
    are fixed per store and so cannot be supplied.
    """
    fixed = [k for k in ("region", "products", "store_prefix") if k in kwargs]
    if fixed:
        raise TypeError(
            f"default_providers() sets {', '.join(fixed)} per store; "
            "construct MRMSProvider directly to override"
        )
    return [
        MRMSProvider(**kwargs),
        MRMSPrecipRateProvider(**kwargs),
        *region_providers(**kwargs),
    ]
