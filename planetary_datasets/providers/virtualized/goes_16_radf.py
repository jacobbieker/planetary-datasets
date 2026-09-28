"""Virtual ingest of GOES-16 ABI-L1b-RadF (per-channel radiance) into Icechunk.

GOES-16 is still operational (launched 2016, became operational 2017) and covers
the CONUS and full-disk from geostationary orbit at 75.2 W.

Each file contains:
  - Rad(y, x)  -- scaled integer radiance (int16)
  - DQF(y, x)  -- data quality flag (int8)
  - plus scalar/1-D metadata (t, x, y, goes_imager_projection, etc.)

Channel resolutions (full-disk):
  - 0.5 km  C02:              21696 x 21696
  - 1.0 km  C01, C03, C05:    10848 x 10848
  - 2.0 km  C04, C06-C16:      5424 x 5424

GOES-16 has two codec eras for its Rad and DQF arrays:
  - Pre-Shuffle  (before ~2023-04-19): BytesCodec + Zlib
  - Post-Shuffle (from  ~2023-04-19):  BytesCodec + Shuffle + Zlib

Reprocessed data (ABI-L1b-RadF-Reproc) is also available, covering
2018-01-04 through 2024-12-28. All Reproc files use post-Shuffle codecs.
Pass reproc=True to use the reprocessed archive instead.

Usage:
    from planetary_datasets.providers.virtualized.goes_16_radf import (
        ingest_all_days,
        smoke_test,
    )

    # Quick test -- channel 13 (2km IR)
    vds = smoke_test(channel=13, n_files=3)

    # Use reprocessed data
    vds = smoke_test(channel=13, n_files=3, reproc=True)

    # Full ingest for a single channel
    import icechunk as ic
    storage = ic.s3_storage(...)
    repo = ic.Repository.open_or_create(storage)
    ingest_all_days(repo, channel=13, branch="main")

    # Ingest reprocessed data
    ingest_all_days(repo, channel=13, branch="main", reproc=True)
"""

from __future__ import annotations

import datetime
from collections.abc import Iterable, Iterator
from typing import Any, TYPE_CHECKING

import numpy as np
import xarray as xr
from obspec_utils.registry import ObjectStoreRegistry

from . import goes_radf_common as common

if TYPE_CHECKING:
    import icechunk


# =============================================================================
# GOES-16 constants
# =============================================================================
SATELLITE = "goes16"
#: Public archive bucket, resolved through the shared config so a mirror
#: can be substituted with GOES16_SOURCE_BUCKET. Read once at import,
#: as get_config() itself is.
BUCKET = common.source_bucket(SATELLITE)
SATELLITE_NAME = "GOES-16"
PRODUCT = "ABI-L1b-RadF/"
PRODUCT_REPROC = "ABI-L1b-RadF-Reproc/"
PRODUCT_KEYS = ["ABI-L1b-RadF-Reproc", "ABI-L1b-RadF"]

#: Default location of the virtualized store, relative to the configured
#: bucket. Resolve with get_config().store_path(); the per-channel and
#: per-era suffixes are added by common.suffixed_prefix().
STORE_PREFIX = common.default_store_prefix(SATELLITE)

MIN_YEAR = 2017
ARCHIVE_START_DATE = datetime.date(2017, 2, 28)

REPROC_START_DATE = datetime.date(2018, 1, 4)
REPROC_END_DATE = datetime.date(2024, 12, 28)

EPOCH_THRESHOLD = np.datetime64("2017-01-01", "ns")

# Re-export common constants used by callers
CHANNEL_GRID_SIZE = common.CHANNEL_GRID_SIZE
RADF_CHUNK_SIZE = common.RADF_CHUNK_SIZE
DATETIME_NS = common.DATETIME_NS
MAX_CONSECUTIVE_FAILED_DAYS = common.MAX_CONSECUTIVE_FAILED_DAYS
_KEEP_DATA_VARS = common._KEEP_DATA_VARS
RadFValidationError = common.RadFValidationError
parse_scan_start_to_datetime = common.parse_scan_start_to_datetime


def _store():
    """Create an anonymous obstore handle for the GOES-16 bucket."""
    return common.make_store(BUCKET)


def _ch(channel: int) -> str:
    return common._ch(channel)


def _grid_size(channel: int) -> int:
    return common._grid_size(channel)


# =============================================================================
# Loadable variables
# =============================================================================
DEFAULT_LOADABLE_VARIABLES: tuple[str, ...] = (
    "t", "x", "y",
    "x_image", "y_image",
    "x_image_bounds", "y_image_bounds", "time_bounds",
    "goes_imager_projection",
    "nominal_satellite_height",
    "nominal_satellite_subpoint_lat",
    "nominal_satellite_subpoint_lon",
    "geospatial_lat_lon_extent",
    "earth_sun_distance_anomaly_in_AU",
    "band_id",
    "band_wavelength",
    "esun",
    "kappa0",
    "planck_fk1", "planck_fk2", "planck_bc1", "planck_bc2",
    "valid_pixel_count",
    "missing_pixel_count",
    "saturated_pixel_count",
    "undersaturated_pixel_count",
    "min_radiance_value_of_valid_pixels",
    "max_radiance_value_of_valid_pixels",
    "mean_radiance_value_of_valid_pixels",
    "std_dev_radiance_value_of_valid_pixels",
    "percent_uncorrectable_L0_errors",
    "percent_uncorrectable_GRB_errors",
    "algorithm_dynamic_input_data_container",
    "processing_parm_version_container",
    "algorithm_product_version_container",
    "focal_plane_temperature_threshold_exceeded_count",
    "maximum_focal_plane_temperature",
    "focal_plane_temperature_threshold_increasing",
    "focal_plane_temperature_threshold_decreasing",
    "yaw_flip_flag",
    "star_id",
    "t_star_look",
    "band_wavelength_star_look",
    "time_bounds_swaths",
    "time_bounds_rows",
    "reprocessing_version",
)

# =============================================================================
# Expected codec pipelines
# =============================================================================
_EXPECTED_CODECS_PRE: dict[str, list[dict[str, Any]]] = {
    "Rad": [
        {"class": "BytesCodec", "endian": "little"},
        {"class": "Zlib", "codec_name": "numcodecs.zlib", "codec_config": {"level": 1}},
    ],
    "DQF": [
        {"class": "BytesCodec", "endian": None},
        {"class": "Zlib", "codec_name": "numcodecs.zlib", "codec_config": {"level": 1}},
    ],
}
_EXPECTED_CODECS_POST: dict[str, list[dict[str, Any]]] = {
    "Rad": [
        {"class": "BytesCodec", "endian": "little"},
        {"class": "Shuffle", "codec_name": "numcodecs.shuffle", "codec_config": {"elementsize": 2}},
        {"class": "Zlib", "codec_name": "numcodecs.zlib", "codec_config": {"level": 1}},
    ],
    "DQF": [
        {"class": "BytesCodec", "endian": None},
        {"class": "Shuffle", "codec_name": "numcodecs.shuffle", "codec_config": {"elementsize": 1}},
        {"class": "Zlib", "codec_name": "numcodecs.zlib", "codec_config": {"level": 1}},
    ],
}


def _check_codecs(ds: xr.Dataset) -> xr.Dataset:
    return common.check_codecs_dual_era(ds, _EXPECTED_CODECS_PRE, _EXPECTED_CODECS_POST)


preprocess = common.make_preprocess(_check_codecs, EPOCH_THRESHOLD)


# =============================================================================
# Archive listing (thin wrappers)
# =============================================================================
def list_years() -> list[int]:
    return common.list_years(_store(), PRODUCT, MIN_YEAR)


def list_days_in_year(year: int) -> list[int]:
    return common.list_days_in_year(_store(), PRODUCT, year)


def iter_days(
    *,
    start: str | None = None,
    end: str | None = None,
) -> Iterator[tuple[int, int]]:
    return common.iter_days(
        _store(), PRODUCT, MIN_YEAR, PRODUCT_KEYS, start=start, end=end,
    )


def list_day_files(
    year: int,
    doy: int,
    channel: int,
    *,
    start: str | None = None,
    end: str | None = None,
    reproc: bool = False,
) -> list[str]:
    return common.list_day_files(
        _store(), BUCKET, PRODUCT, year, doy, channel,
        start=start, end=end,
        product_reproc=PRODUCT_REPROC, reproc=reproc,
    )


def iter_archive(
    channel: int,
    *,
    start: str | None = None,
    end: str | None = None,
    reproc: bool = False,
) -> Iterator[str]:
    return common.iter_archive(
        _store(), BUCKET, PRODUCT, MIN_YEAR, PRODUCT_KEYS, channel,
        start=start, end=end,
        product_reproc=PRODUCT_REPROC, reproc=reproc,
    )


def iter_archive_by_day(
    channel: int,
    *,
    start: str | None = None,
    end: str | None = None,
    reproc: bool = False,
) -> Iterator[tuple[tuple[int, int], list[str]]]:
    return common.iter_archive_by_day(
        _store(), BUCKET, PRODUCT, MIN_YEAR, PRODUCT_KEYS, channel,
        start=start, end=end,
        product_reproc=PRODUCT_REPROC, reproc=reproc,
    )


def days_to_ingest(
    channel: int,
    *,
    start: str | None = None,
    end: str | None = None,
    reproc: bool = False,
) -> list[tuple[tuple[int, int], list[str]]]:
    return common.days_to_ingest(
        _store(), BUCKET, PRODUCT, MIN_YEAR, PRODUCT_KEYS, channel,
        start=start, end=end,
        product_reproc=PRODUCT_REPROC, reproc=reproc,
    )


# =============================================================================
# Smoke test
# =============================================================================
def smoke_test(
    channel: int = 13,
    n_files: int = 3,
    *,
    start: str | None = None,
    reproc: bool = False,
) -> xr.Dataset:
    return common.smoke_test(
        _store(), BUCKET, PRODUCT, MIN_YEAR, PRODUCT_KEYS, SATELLITE_NAME,
        preprocess, list(DEFAULT_LOADABLE_VARIABLES),
        channel=channel, n_files=n_files, start=start,
        product_reproc=PRODUCT_REPROC, reproc=reproc,
    )


# =============================================================================
# Ingest
# =============================================================================
def ingest_all_days(
    repo: "icechunk.Repository",
    channel: int,
    *,
    branch: str = "main",
    group: str | None = None,
    registry: ObjectStoreRegistry | None = None,
    loop_start_day: int = 0,
    loop_end_day: int | None = None,
    batch_size: int = 1,
    start: str | None = None,
    end: str | None = None,
    loadable_variables: Iterable[str] = DEFAULT_LOADABLE_VARIABLES,
    resume: bool = True,
    zarr_async_concurrency: int | None = None,
    reproc: bool = False,
    log_dir: str | None = None,
    repo_factory: Any = None,
) -> None:
    """Ingest every selected day for a single channel into repo."""
    product_prefix = "ABI-L1b-RadF-Reproc" if reproc else "ABI-L1b-RadF"
    if group is None:
        group = f"{product_prefix}/{_ch(channel)}"

    all_days = days_to_ingest(channel, start=start, end=end, reproc=reproc)
    product_label = "ABI-L1b-RadF-Reproc" if reproc else "ABI-L1b-RadF"

    common.ingest_all_days(
        repo, channel,
        satellite_name=SATELLITE_NAME,
        satellite=SATELLITE,
        product_label=product_label,
        archive_start_date=ARCHIVE_START_DATE,
        all_days=all_days,
        preprocess_fn=preprocess,
        branch=branch,
        group=group,
        registry=registry,
        bucket=BUCKET,
        loop_start_day=loop_start_day,
        loop_end_day=loop_end_day,
        batch_size=batch_size,
        loadable_variables=loadable_variables,
        resume=resume,
        zarr_async_concurrency=zarr_async_concurrency,
        log_dir=log_dir,
        repo_factory=repo_factory,
        epoch_threshold=EPOCH_THRESHOLD,
    )


def ingest_all_channels(
    repo: "icechunk.Repository",
    *,
    channels: Iterable[int] | None = None,
    branch: str = "main",
    base_group: str | None = None,
    reproc: bool = False,
    **kwargs,
) -> None:
    """Ingest multiple channels sequentially into separate groups."""
    if base_group is None:
        base_group = "ABI-L1b-RadF-Reproc" if reproc else "ABI-L1b-RadF"
    common.ingest_all_channels(
        repo,
        ingest_fn=lambda repo, ch, **kw: ingest_all_days(
            repo, ch, reproc=reproc, **kw
        ),
        channels=channels,
        branch=branch,
        base_group=base_group,
        **kwargs,
    )


# =============================================================================
# CLI
# =============================================================================
if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(
        description="Ingest GOES-16 ABI-L1b-RadF (per-channel radiance) into "
                    "Icechunk via VirtualiZarr.",
    )
    p.add_argument("--smoke-test", action="store_true",
                    help="Run a quick smoke test and exit.")
    p.add_argument("--channel", type=int, default=13,
                    help="ABI channel number 1-16 (default: 13).")
    p.add_argument("--n-files", type=int, default=3,
                    help="Number of files for smoke test (default: 3).")
    p.add_argument("--reproc", action="store_true",
                    help="Use ABI-L1b-RadF-Reproc reprocessed data "
                         "(covers 2018-01-04 to 2024-12-28, post-Shuffle codecs).")
    args = p.parse_args()

    if args.smoke_test:
        vds = smoke_test(channel=args.channel, n_files=args.n_files, reproc=args.reproc)
        print(vds)
    else:
        print("Use --smoke-test to verify the pipeline, or import and call "
              "ingest_all_days() / ingest_all_channels() programmatically.")
