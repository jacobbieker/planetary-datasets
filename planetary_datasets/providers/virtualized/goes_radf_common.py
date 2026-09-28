"""Common utilities for GOES ABI-L1b-RadF virtual ingest scripts.

Shared by goes_16_radf.py, goes_17_radf.py, goes_18_radf.py, and goes_19_radf.py.
All four satellites use the same ABI instrument and file schema — only the bucket,
operational dates, codec eras, and a handful of loadable variables differ.
"""

from __future__ import annotations

import contextlib
import ctypes
import sys
import dataclasses
import functools
import datetime
import gc
import os
import pathlib
import time
import traceback
import warnings
from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING, Any, Callable

import numpy as np
import obstore as obs
import pandas as pd
import virtualizarr as vz
import xarray as xr
from obspec_utils.registry import ObjectStoreRegistry
from virtualizarr.manifests import ManifestArray

from planetary_datasets.config import get_config
from planetary_datasets.memory import configure_malloc_arenas, process_tree_rss_gb

if TYPE_CHECKING:
    import icechunk


# =============================================================================
# Constants
# =============================================================================
CHANNEL_GRID_SIZE: dict[int, int] = {
    1: 10848,
    2: 21696,
    3: 10848,
    4: 5424,
    5: 10848,
    **{ch: 5424 for ch in range(6, 17)},
}

RADF_CHUNK_SIZE = 226

DATETIME_NS = np.dtype("datetime64[ns]")

MAX_CONSECUTIVE_FAILED_DAYS = 14

#: Reopen the icechunk Repository every N batches.
#:
#: Repeatedly appending into a growing store leaks memory in proportion to the
#: *manifest split size*, so this only pays off alongside a tight split. Over
#: 28 synthetic GOES-sized batches (82,944 chunk entries each, no network),
#: growth in the second half was:
#:
#:     no splitting, no reopen   +5.21GB   +185 MB/batch
#:     720 (5d) split, no reopen +1.41GB    +28 MB/batch
#:     144 (1d) split, no reopen +0.59GB    +13 MB/batch
#:     144 (1d) split + reopen   +0.40GB     +3 MB/batch
#:
#: Splitting bounds the manifest; reopening sheds what the Repository object
#: still holds. The reopen costs ~0s, so it runs every batch. See the matching
#: split config in each ingest CLI's _open_repo.
#:
#: These two alone were NOT enough. In production they cut the leak only from
#: ~6.75 to ~4.2 GB/h — the synthetic above overstated them because it
#: fabricates manifests instead of doing the real networked HDF open, so it
#: never fragments the heap. The dominant cause was glibc retaining freed
#: arenas: a per-stage trace showed `gc.collect()` reclaiming -1MB while RSS
#: climbed +40..330MB per batch. Adding malloc_trim() to the cleanup stage and
#: MALLOC_ARENA_MAX=2 in the image took the same workload from climbing to
#: dead flat (0.20GB across 7 consecutive batches, ~262MB returned each time).
#: Keep all three: they address different layers.
REOPEN_REPO_EVERY = 1

_MAX_T_VS_FILENAME_DRIFT = np.timedelta64(1, "h")

# Keyword arguments for combining a set of RadF files along `t`. Shared by the
# ingest, the smoke test, and the backwards era probe so that the probe tests
# exactly the combination the ingest will later perform.
_COMBINE_KWARGS: dict[str, Any] = {
    "combine": "nested",
    "concat_dim": "t",
    "combine_attrs": "drop_conflicts",
    "coords": "minimal",
    "compat": "override",
}

# The xr.concat equivalent of _COMBINE_KWARGS, used to test whether two
# already-opened virtual datasets can live in one store.
_CONCAT_KWARGS: dict[str, Any] = {
    "combine_attrs": "drop_conflicts",
    "coords": "minimal",
    "compat": "override",
}

# Data variables to keep after preprocess. Any variables not in this set are
# dropped to ensure all files produce identical schemas for xr.concat.
# This prevents concat failures from intermittent variables (star calibration,
# reproc-only metadata, etc.) that only appear in some files.
_KEEP_DATA_VARS = frozenset({
    # Virtual arrays
    "Rad", "DQF",
    "x", "y",
    # Created by add_time_dimension
    "x_coord", "y_coord",
    # Standard ABI L1b metadata (present in every RadF file)
    "x_image", "y_image", "x_image_bounds", "y_image_bounds",
    "time_bounds", "goes_imager_projection",
    "nominal_satellite_height",
    "nominal_satellite_subpoint_lat", "nominal_satellite_subpoint_lon",
    "geospatial_lat_lon_extent", "earth_sun_distance_anomaly_in_AU",
    "band_id", "band_wavelength", "esun", "kappa0",
    "planck_fk1", "planck_fk2", "planck_bc1", "planck_bc2",
    "valid_pixel_count", "missing_pixel_count",
    "saturated_pixel_count", "undersaturated_pixel_count",
    "min_radiance_value_of_valid_pixels", "max_radiance_value_of_valid_pixels",
    "mean_radiance_value_of_valid_pixels", "std_dev_radiance_value_of_valid_pixels",
    "yaw_flip_flag",
})


# =============================================================================
# Configuration
# =============================================================================
#: Public archive buckets the virtual ingests read from, keyed by satellite id.
#:
#: These are NOAA/NASA Open Data buckets, read anonymously. They are not
#: credentials and not deployment-specific, but a site running against a mirror
#: can point each one somewhere else with ``<SATELLITE>_SOURCE_BUCKET`` in the
#: environment or in ``.env`` — see :func:`source_bucket`.
DEFAULT_SOURCE_BUCKETS: dict[str, str] = {
    "goes16": "s3://noaa-goes16",
    "goes17": "s3://noaa-goes17",
    "goes18": "s3://noaa-goes18",
    "goes19": "s3://noaa-goes19",
}

#: Region the public archive buckets live in. Overridable per process with
#: ``GOES_SOURCE_REGION`` for mirrors hosted elsewhere.
DEFAULT_SOURCE_REGION = "us-east-1"

#: Where the virtualized stores live under the configured bucket.
DEFAULT_STORE_ROOT = "bkr/geo/virtualized"


def source_bucket(satellite: str, default: str | None = None) -> str:
    """Archive bucket URL for ``satellite``.

    Resolution order: ``<SATELLITE>_SOURCE_BUCKET`` in the environment (``.env``
    included, since :func:`~planetary_datasets.config.get_config` loads it),
    then ``default``, then :data:`DEFAULT_SOURCE_BUCKETS`.
    """
    get_config()  # ensures .env has been loaded into the environment
    override = os.environ.get(f"{satellite.upper()}_SOURCE_BUCKET")
    if override:
        return override.strip().rstrip("/")
    resolved = default or DEFAULT_SOURCE_BUCKETS.get(satellite)
    if not resolved:
        raise ValueError(
            f"No source bucket known for {satellite!r}. Pass one, or set "
            f"{satellite.upper()}_SOURCE_BUCKET."
        )
    return resolved.rstrip("/")


def source_region(default: str = DEFAULT_SOURCE_REGION) -> str:
    """Region for the public archive buckets."""
    get_config()
    return os.environ.get("GOES_SOURCE_REGION", "").strip() or default


def default_store_prefix(
    satellite: str,
    *,
    product: str = "radf",
    root: str | None = None,
) -> str:
    """Store prefix for a satellite's virtualized product, e.g.
    ``bkr/geo/virtualized/goes16_radf.icechunk``.

    Resolve it to an ``s3://`` URI or a local directory with
    ``get_config().store_path(...)``; ``ICECHUNK_LOCAL_PATH`` redirects it.
    """
    get_config()
    base = root or os.environ.get("GOES_STORE_ROOT", "").strip() or DEFAULT_STORE_ROOT
    return f"{base.strip('/')}/{satellite}_{product}.icechunk"


def suffixed_prefix(prefix: str, *, channel: int | None = None, era: str | None = None) -> str:
    """Append the per-channel and per-era suffixes to a store prefix.

    ``bkr/geo/virtualized/goes16_radf.icechunk`` with channel 13 and era
    ``2023-04-19`` becomes
    ``bkr/geo/virtualized/goes16_radf_C13_2023-04-19.icechunk``. Each channel
    and each codec era gets its own store, so the suffix is part of the
    store's identity and a rerun with the same suffix resumes it.
    """
    suffix = f"_C{channel:02d}" if channel is not None else ""
    if era:
        suffix += f"_{era}"
    if not suffix:
        return prefix
    if prefix.endswith(".icechunk"):
        return prefix[: -len(".icechunk")] + suffix + ".icechunk"
    return prefix + suffix


def default_log_dir() -> pathlib.Path:
    """Directory for the per-channel ingest event logs.

    Under ``data_dir`` rather than ``scratch_dir``: these record which days were
    skipped and why, and are the only account of a multi-week backfill.
    """
    return get_config().data_dir / "logs" / "virtualized"


def scratch_dir() -> pathlib.Path:
    """Scratch directory for this process, from the shared config."""
    return get_config().scratch_dir


def virtual_chunk_buckets(extra: Iterable[str] = ()) -> list[str]:
    """Bucket names (no scheme) that virtual chunk references may point into.

    Resolves each satellite through :func:`source_bucket`, so a mirror
    configured with ``<SATELLITE>_SOURCE_BUCKET`` gets a chunk container too.
    Without that the manifests would reference the mirror while only the NOAA
    prefixes were authorized, and every chunk read would fail. The defaults
    stay in the list as well: an existing store's manifests still point at
    them.
    """
    names: list[str] = []
    candidates = [
        *(source_bucket(sat) for sat in DEFAULT_SOURCE_BUCKETS),
        *DEFAULT_SOURCE_BUCKETS.values(),
        *extra,
    ]
    for bucket in candidates:
        name = bucket.split("://", 1)[-1].strip("/")
        if name and name not in names:
            names.append(name)
    return names


def open_virtual_repository(
    storage,
    *,
    virtual_buckets: Iterable[str] | None = None,
    manifest_split_size: int = 144,
    region: str | None = None,
) -> "icechunk.Repository":
    """Open or create the Icechunk repository a virtual ingest writes into.

    ``manifest_split_size`` defaults to one manifest per day (GOES ABI RadF is
    144 full-disk scans a day), matching the commit batch. Appending into a
    growing array holds memory in proportion to the split size, so this is a
    memory setting as much as a layout one: over 28 synthetic GOES-sized
    batches the second-half slope was +185 MB/batch unsplit, +28 at a 5-day
    split and +13 at one day — and +3 once :data:`REOPEN_REPO_EVERY` reopens
    the Repository each batch. The old 5-day split is what let a long backfill
    climb into the watchdog budget.
    """
    import icechunk

    split_config = icechunk.config.ManifestSplittingConfig.from_dict(
        {
            icechunk.config.ManifestSplitCondition.AnyArray(): {
                icechunk.config.ManifestSplitDimCondition.DimensionName(
                    "t"
                ): manifest_split_size
            }
        }
    )
    config = icechunk.RepositoryConfig(
        manifest=icechunk.config.ManifestConfig(splitting=split_config)
    )

    buckets = list(virtual_buckets) if virtual_buckets is not None else virtual_chunk_buckets()
    s3_store = icechunk.s3_store(region=region or source_region(), anonymous=True)
    for bucket in buckets:
        config.set_virtual_chunk_container(
            icechunk.VirtualChunkContainer(
                url_prefix=f"s3://{bucket}/",
                store=s3_store,
            ),
        )
    # Registering a container is not enough to read through it — without
    # credentials, any read of a virtual chunk fails.
    virtual_credentials = icechunk.containers_credentials(
        {f"s3://{bucket}/": icechunk.s3_anonymous_credentials() for bucket in buckets}
    )

    repo = icechunk.Repository.open_or_create(
        storage, config, authorize_virtual_chunk_access=virtual_credentials
    )
    repo.save_config()
    return repo


def configure_ingest_process() -> None:
    """Apply the process-level memory settings the long backfills need.

    Bounds glibc arena growth for any child process started afterwards, which
    is the setting the production image bakes in as ``MALLOC_ARENA_MAX=2``.
    Call this before spawning the per-channel pool so a run outside the image
    gets the same behaviour.
    """
    configure_malloc_arenas(2)


# =============================================================================
# Channel helpers
# =============================================================================
def _ch(channel: int) -> str:
    """Format channel number as C01..C16."""
    return f"C{channel:02d}"


def _grid_size(channel: int) -> int:
    """Return the y=x pixel count for a given channel."""
    return CHANNEL_GRID_SIZE[channel]


# =============================================================================
# Filename / URL helpers
# =============================================================================
def _is_aborted_scan(path: str) -> bool:
    """True if the scan-start and scan-end tokens are identical (aborted scan)."""
    fname = path.rsplit("/", 1)[-1]
    try:
        s = fname.split("_s")[1].split("_")[0]
        e = fname.split("_e")[1].split("_")[0]
    except IndexError:
        return False
    return s == e


def _scan_start_token(url: str) -> str:
    """Extract the sYYYYDDDHHMMSST token from a GOES filename."""
    fname = url.rsplit("/", 1)[-1]
    return fname.split("_s")[1].split("_")[0]


def _channel_from_filename(path: str) -> int:
    """Extract the channel number from a RadF filename."""
    fname = path.rsplit("/", 1)[-1]
    after_radf = fname.split("RadF-")[1]
    mode_chan = after_radf.split("_")[0]
    return int(mode_chan[-2:])


def parse_scan_start_to_datetime(url_or_filename: str) -> np.datetime64:
    """Parse the sYYYYDDDHHMMSST filename token into a datetime64[ns]."""
    token = _scan_start_token(url_or_filename)
    if len(token) != 14:
        raise ValueError(f"Unexpected scan-start token length: {token!r}")
    year = int(token[0:4])
    doy = int(token[4:7])
    hour = int(token[7:9])
    minute = int(token[9:11])
    sec = int(token[11:13])
    tenths = int(token[13:14])
    date = datetime.date(year, 1, 1) + datetime.timedelta(days=doy - 1)
    return np.datetime64(
        datetime.datetime(
            date.year, date.month, date.day, hour, minute, sec, tenths * 100_000
        ),
        "ns",
    )


def parse_url_to_day(
    url: str,
    product_keys: list[str],
) -> tuple[int, int]:
    """Extract (year, doy) from a GOES L1b RadF file URL."""
    parts = url.split("/")
    for product_key in product_keys:
        if product_key in parts:
            idx = parts.index(product_key)
            if len(parts) >= idx + 3:
                return int(parts[idx + 1]), int(parts[idx + 2])
    raise ValueError(f"URL doesn't contain any of {product_keys}: {url}")


# =============================================================================
# Codec validation
# =============================================================================
def _codec_matches(codec: Any, expected: dict[str, Any]) -> bool:
    if type(codec).__name__ != expected["class"]:
        return False
    for key, want in expected.items():
        if key == "class":
            continue
        got = getattr(codec, key, None)
        if hasattr(got, "value"):
            got = got.value
        if key == "codec_config":
            got_d = dict(got) if got is not None else {}
            for k, v in want.items():
                if got_d.get(k) != v:
                    return False
            continue
        if got != want:
            return False
    return True


def _pipeline_matches(
    actual: list[Any],
    expected: list[dict[str, Any]] | None,
) -> bool:
    """Return True if actual codecs match an expected pipeline spec.

    ``expected`` is None when the era's table does not list this variable at
    all, which counts as "does not match" rather than an error: the dual-era
    check calls this once per era and only needs one of them to match.
    """
    if expected is None or len(actual) != len(expected):
        return False
    return all(_codec_matches(act, exp) for act, exp in zip(actual, expected))


def check_codecs_dual_era(
    ds: xr.Dataset,
    expected_pre: dict[str, list[dict[str, Any]]],
    expected_post: dict[str, list[dict[str, Any]]],
) -> xr.Dataset:
    """Validate codecs when two eras (pre-Shuffle and post-Shuffle) are accepted.

    Unexpectedly virtual variables are dropped rather than rejected.
    """
    errors: list[str] = []
    to_drop: list[str] = []
    for name, da in {**ds.data_vars, **ds.coords}.items():
        if not isinstance(da.data, ManifestArray):
            continue
        pre = expected_pre.get(name)
        post = expected_post.get(name)
        if pre is None and post is None:
            to_drop.append(name)
            continue
        actual = list(da.data.metadata.codecs)
        if not (_pipeline_matches(actual, pre) or _pipeline_matches(actual, post)):
            errors.append(
                f"{name}: codec pipeline does not match either the pre-Shuffle "
                f"or post-Shuffle expected pipeline. Got: {actual!r}"
            )
    if errors:
        raise ValueError("Codec validation failed:\n  " + "\n  ".join(errors))
    if to_drop:
        ds = ds.drop_vars(to_drop)
    return ds


def check_codecs_single_era(
    ds: xr.Dataset,
    expected: dict[str, list[dict[str, Any]]],
) -> xr.Dataset:
    """Validate codecs when only a single codec era is expected.

    Unexpectedly virtual variables are dropped rather than rejected.
    """
    errors: list[str] = []
    to_drop: list[str] = []
    for name, da in {**ds.data_vars, **ds.coords}.items():
        if not isinstance(da.data, ManifestArray):
            continue
        exp = expected.get(name)
        if exp is None:
            to_drop.append(name)
            continue
        actual = list(da.data.metadata.codecs)
        if len(actual) != len(exp):
            errors.append(
                f"{name}: expected {len(exp)} codecs, got {len(actual)}: {actual!r}"
            )
            continue
        for i, (act, e) in enumerate(zip(actual, exp)):
            if not _codec_matches(act, e):
                errors.append(
                    f"{name}: codec[{i}] mismatch — expected {e}, got {act!r}"
                )
    if errors:
        raise ValueError("Codec validation failed:\n  " + "\n  ".join(errors))
    if to_drop:
        ds = ds.drop_vars(to_drop)
    return ds


# =============================================================================
# Preprocessing pipeline
# =============================================================================
class RadFValidationError(Exception):
    """Pipeline failure annotated with the source filename."""

    def __init__(self, source: str, original: Exception):
        self.source = source
        self.original = original
        super().__init__(f"\nPipeline failed for {source}:\n\n{original}")


class CodecChangeDetected(Exception):
    """Raised when consecutive codec validation failures indicate a codec era change."""

    def __init__(self, codec_change_date: datetime.date, first_error: str):
        self.codec_change_date = codec_change_date
        self.first_error = first_error
        super().__init__(
            f"Codec change detected on {codec_change_date}: {first_error}"
        )


def _is_codec_error(exc: Exception) -> bool:
    """Return True if the exception is due to codec validation failure."""
    if isinstance(exc, RadFValidationError):
        exc = exc.original
    return isinstance(exc, ValueError) and "codec" in str(exc).lower()


def _source_of(ds: xr.Dataset) -> str:
    return (
        ds.encoding.get("source")
        or ds.attrs.get("dataset_name")
        or "<unknown>"
    )


def _parse_scan_start_from_source(source: str) -> np.datetime64:
    """Parse the sYYYYDDDHHMMSST filename token into datetime64[ns]."""
    fname = source.rsplit("/", 1)[-1]
    token = fname.split("_s")[1].split("_")[0]
    if len(token) != 14:
        raise ValueError(f"Unexpected scan-start token length: {token!r}")
    year = int(token[0:4])
    doy = int(token[4:7])
    hour = int(token[7:9])
    minute = int(token[9:11])
    sec = int(token[11:13])
    tenths = int(token[13:14])
    date = datetime.date(year, 1, 1) + datetime.timedelta(days=doy - 1)
    return np.datetime64(
        datetime.datetime(
            date.year, date.month, date.day, hour, minute, sec, tenths * 100_000
        ),
        "ns",
    )


def validate_raw_t(
    ds: xr.Dataset,
    epoch_threshold: np.datetime64,
) -> None:
    """Validate t-coordinate sanity: above epoch threshold and close to scan start."""
    t = ds["t"].values
    if t < epoch_threshold:
        raise ValueError(
            f"`t` coordinate is {t!s} (before {epoch_threshold!s}) — likely an "
            f"aborted/placeholder scan with a sentinel timestamp."
        )

    scan_start: np.datetime64 | None = None
    for candidate in (
        (ds.encoding.get("source") or "").rsplit("/", 1)[-1],
        ds.attrs.get("dataset_name") or "",
    ):
        if not candidate:
            continue
        try:
            scan_start = _parse_scan_start_from_source(candidate)
            break
        except (ValueError, IndexError):
            continue
    if scan_start is not None:
        drift = abs(t - scan_start)
        if drift > _MAX_T_VS_FILENAME_DRIFT:
            raise ValueError(
                f"`t` coordinate ({t!s}) drifts from filename scan-start "
                f"({scan_start!s}) by {drift} — file appears mis-stamped."
            )


def add_time_dimension(ds: xr.Dataset) -> xr.Dataset:
    """expand_dims('t') on every data variable using the scalar t coord.

    Also promotes the x and y coordinate arrays to data variables with a t
    dimension (as x_coord and y_coord), since the satellite's orbital position
    can drift over time causing the fixed-grid projection to shift.
    """
    if "t" not in ds.coords:
        raise ValueError("Expected a scalar 't' coordinate in the dataset.")
    t_value = ds["t"].values
    t_attrs = dict(ds["t"].attrs)
    t_encoding = dict(ds["t"].encoding)

    out = ds.copy()
    if "x" in out.coords:
        out["x_coord"] = xr.DataArray(
            out["x"].values, dims=("x",), attrs=dict(out["x"].attrs)
        )
        out["x_coord"].encoding["_FillValue"] = None
    if "y" in out.coords:
        out["y_coord"] = xr.DataArray(
            out["y"].values, dims=("y",), attrs=dict(out["y"].attrs)
        )
        out["y_coord"].encoding["_FillValue"] = None

    out = out.drop_vars("t")
    new_data_vars: dict[str, xr.DataArray] = {}
    for name, da in out.data_vars.items():
        expanded = da.expand_dims({"t": [t_value]}, axis=0)
        expanded.encoding = dict(da.encoding)
        new_data_vars[name] = expanded

    new_ds = xr.Dataset(new_data_vars, coords=out.coords)
    new_ds["t"].attrs.update(t_attrs)
    new_ds["t"].encoding.update(t_encoding)
    return new_ds


def finalize_encoding(ds: xr.Dataset) -> xr.Dataset:
    """Encoding fixups for the cleaned dataset."""
    out = ds

    # Suppress xarray's auto-added _FillValue=NaN on float variables
    for var in (*out.variables.values(),):
        if "_FillValue" in var.encoding:
            continue
        encoded_dtype = np.dtype(var.encoding.get("dtype", var.dtype))
        if np.issubdtype(encoded_dtype, np.floating):
            var.encoding["_FillValue"] = None

    # Cast scale_factor / add_offset back to float32
    for var in (*out.variables.values(),):
        for key in ("scale_factor", "add_offset"):
            if key in var.attrs:
                var.attrs[key] = np.float32(var.attrs[key])
            if key in var.encoding:
                var.encoding[key] = np.float32(var.encoding[key])

    return out


def make_preprocess(
    check_codecs_fn: Callable[[xr.Dataset], xr.Dataset],
    epoch_threshold: np.datetime64,
    keep_data_vars: frozenset[str] = _KEEP_DATA_VARS,
) -> Callable[[xr.Dataset], xr.Dataset]:
    """Factory: returns a preprocess function for use with open_virtual_mfdataset."""

    def preprocess(ds: xr.Dataset) -> xr.Dataset:
        try:
            ds = check_codecs_fn(ds)
            validate_raw_t(ds, epoch_threshold)
            cleaned = finalize_encoding(ds)
            cleaned = add_time_dimension(cleaned)
            to_drop = [v for v in cleaned.data_vars if v not in keep_data_vars]
            if to_drop:
                cleaned = cleaned.drop_vars(to_drop)
            return cleaned
        except Exception as e:
            raise RadFValidationError(_source_of(ds), e) from e

    return preprocess


def check_codecs_permissive(
    ds: xr.Dataset,
    expected_virtual_vars: frozenset[str] = frozenset({"Rad", "DQF"}),
) -> xr.Dataset:
    """Accept any codec pipeline for expected virtual variables, drop unexpected ones."""
    to_drop: list[str] = []
    for name, da in {**ds.data_vars, **ds.coords}.items():
        if not isinstance(da.data, ManifestArray):
            continue
        if name not in expected_virtual_vars:
            to_drop.append(name)
    if to_drop:
        ds = ds.drop_vars(to_drop)
    return ds


def make_preprocess_no_codec_check(
    epoch_threshold: np.datetime64,
    keep_data_vars: frozenset[str] = _KEEP_DATA_VARS,
) -> Callable[[xr.Dataset], xr.Dataset]:
    """Create a preprocess function that accepts any codec pipeline.

    Used after a codec change is detected — the virtual references still
    record the correct per-chunk codec, so we just need to skip validation.
    """
    return make_preprocess(
        lambda ds: check_codecs_permissive(ds),
        epoch_threshold,
        keep_data_vars,
    )


# =============================================================================
# Archive listing helpers
# =============================================================================
def make_store(bucket: str, region: str | None = None):
    """Create an anonymous obstore handle for a public archive bucket.

    ``region`` defaults to :func:`source_region`, which is where the NOAA Open
    Data buckets live unless a mirror has been configured.
    """
    return obs.store.from_url(
        bucket, region=region or source_region(), skip_signature=True
    )


def common_prefix_children(store, prefix: str) -> list[str]:
    result = obs.list_with_delimiter(store, prefix=prefix)
    return sorted(
        p[len(prefix):].rstrip("/") for p in result["common_prefixes"]
    )


def list_years(store, product: str, min_year: int) -> list[int]:
    """Every year directory under the product prefix, filtered to >= min_year."""
    out: list[int] = []
    for c in common_prefix_children(store, product):
        c = c.strip("/")
        if len(c) == 4 and c.isdigit() and int(c) >= min_year:
            out.append(int(c))
    return sorted(out)


def list_days_in_year(store, product: str, year: int) -> list[int]:
    """Every DOY directory present in a given year."""
    out: list[int] = []
    for c in common_prefix_children(store, f"{product}{year}/"):
        c = c.strip("/")
        if len(c) == 3 and c.isdigit():
            out.append(int(c))
    return sorted(out)


def iter_days(
    store,
    product: str,
    min_year: int,
    product_keys: list[str],
    *,
    start: str | None = None,
    end: str | None = None,
) -> Iterator[tuple[int, int]]:
    """Yield (year, doy) for every day in the archive, in chronological order."""
    start_day = parse_url_to_day(start, product_keys) if start else None
    end_day = parse_url_to_day(end, product_keys) if end else None

    for year in list_years(store, product, min_year):
        if start_day and year < start_day[0]:
            continue
        if end_day and year > end_day[0]:
            return
        for doy in list_days_in_year(store, product, year):
            current = (year, doy)
            if start_day and current < start_day:
                continue
            if end_day and current > end_day:
                return
            yield current


def list_day_files(
    store,
    bucket: str,
    product: str,
    year: int,
    doy: int,
    channel: int,
    *,
    start: str | None = None,
    end: str | None = None,
    product_reproc: str | None = None,
    reproc: bool = False,
) -> list[str]:
    """Every .nc file URL for a specific channel in a single day.

    Filters out aborted scans and non-matching channels. Returns URLs
    in chronological scan-time order.
    """
    effective_product = product_reproc if (reproc and product_reproc) else product
    day_prefix = f"{effective_product}{year}/{doy:03d}/"
    urls: list[str] = []
    for page in obs.list(store, prefix=day_prefix):
        for obj in page:
            path = obj["path"]
            if not path.endswith(".nc"):
                continue
            if _is_aborted_scan(path):
                continue
            fname = path.rsplit("/", 1)[-1]
            if "RadF-M" not in fname:
                continue
            if _channel_from_filename(path) != channel:
                continue
            urls.append(f"{bucket}/{path}")
    urls.sort(key=_scan_start_token)

    # Deduplicate by scan-start token
    seen: set[str] = set()
    deduped: list[str] = []
    for u in urls:
        tok = _scan_start_token(u)
        if tok not in seen:
            seen.add(tok)
            deduped.append(u)
    urls = deduped

    if start is not None:
        urls = [u for u in urls if _scan_start_token(u) >= _scan_start_token(start)]
    if end is not None:
        urls = [u for u in urls if _scan_start_token(u) <= _scan_start_token(end)]
    return urls


def iter_archive(
    store,
    bucket: str,
    product: str,
    min_year: int,
    product_keys: list[str],
    channel: int,
    *,
    start: str | None = None,
    end: str | None = None,
    product_reproc: str | None = None,
    reproc: bool = False,
) -> Iterator[str]:
    """Flat iterator over every file URL for a channel across the archive."""
    for year, doy in iter_days(
        store, product, min_year, product_keys, start=start, end=end
    ):
        yield from list_day_files(
            store, bucket, product, year, doy, channel,
            product_reproc=product_reproc, reproc=reproc,
        )


def iter_archive_by_day(
    store,
    bucket: str,
    product: str,
    min_year: int,
    product_keys: list[str],
    channel: int,
    *,
    start: str | None = None,
    end: str | None = None,
    product_reproc: str | None = None,
    reproc: bool = False,
) -> Iterator[tuple[tuple[int, int], list[str]]]:
    """Per-day iterator: yield ((year, doy), [urls]) for a channel."""
    for year, doy in iter_days(
        store, product, min_year, product_keys, start=start, end=end
    ):
        files = list_day_files(
            store, bucket, product, year, doy, channel,
            product_reproc=product_reproc, reproc=reproc,
        )
        yield (year, doy), files


def days_to_ingest(
    store,
    bucket: str,
    product: str,
    min_year: int,
    product_keys: list[str],
    channel: int,
    *,
    start: str | None = None,
    end: str | None = None,
    product_reproc: str | None = None,
    reproc: bool = False,
) -> list[tuple[tuple[int, int], list[str]]]:
    """Eagerly enumerate the days/URLs for a channel."""
    return list(iter_archive_by_day(
        store, bucket, product, min_year, product_keys, channel,
        start=start, end=end, product_reproc=product_reproc, reproc=reproc,
    ))


# =============================================================================
# Batch quality check
# =============================================================================
def validate_batch_data_vars(
    vds: xr.Dataset,
    keep_data_vars: frozenset[str] = _KEEP_DATA_VARS,
) -> tuple[frozenset[str], frozenset[str]]:
    """Check that vds data variables match the expected set.

    Returns (missing, extra) frozensets.
    """
    actual = frozenset(vds.data_vars)
    present = actual | frozenset(vds.coords) | frozenset(vds.dims)
    missing = keep_data_vars - present
    extra = actual - keep_data_vars
    return missing, extra


# =============================================================================
# Ingest helpers
# =============================================================================
def malloc_trim() -> bool:
    """Ask glibc to return free heap to the OS. True if it released anything.

    A batch allocates and frees hundreds of MB of chunk-manifest data through
    both Python and icechunk's Rust allocator. glibc keeps those freed spans
    in its arenas rather than handing them back, so RSS climbs across batches
    even though `gc.collect()` finds nothing to free — which is exactly what
    the per-stage trace showed (cleanup reclaiming -1MB while RSS rose). This
    is a no-op off glibc (musl, macOS), so callers must not rely on it.
    """
    try:
        return bool(ctypes.CDLL("libc.so.6").malloc_trim(0))
    except (OSError, AttributeError):
        return False


def rss_gb() -> float:
    """Resident set size of this process and its children, in GB.

    Delegates to :func:`planetary_datasets.memory.process_tree_rss_gb`. It
    must report *current* usage, not a peak: an earlier version read
    ``ru_maxrss``, which never goes down and so cannot show whether memory was
    released — that produced a bogus "5x improvement" measurement once.
    """
    return process_tree_rss_gb()


@contextlib.contextmanager
def timer(name: str):
    """Print elapsed time, and the RSS the block did not give back.

    Retention per stage is the measurement that matters for the long
    backfills: a stage that holds a few hundred MB every batch is what walks
    a multi-day run into the memory budget.
    """
    t0 = time.perf_counter()
    r0 = rss_gb()
    yield
    elapsed = time.perf_counter() - t0
    r1 = rss_gb()
    print(
        f"  {name}: {elapsed:.1f}s"
        f" [rss {r1:.2f}GB, {(r1 - r0) * 1024:+.0f}MB]",
        flush=True,
    )


def _date_from_doy(year: int, doy: int) -> datetime.date:
    return datetime.date(year, 1, 1) + datetime.timedelta(days=doy - 1)


def _archive_day_index(
    year: int, doy: int, archive_start_date: datetime.date
) -> int:
    return (_date_from_doy(year, doy) - archive_start_date).days


def _schema_exists(
    repo: "icechunk.Repository",
    branch: str,
    group: str | None = None,
) -> bool:
    """Return True if a zarr group is already committed at group on branch."""
    import zarr

    try:
        session = repo.readonly_session(branch=branch)
        zarr.open_group(
            store=session.store, path=group or "", zarr_format=3, mode="r"
        )
        return True
    except FileNotFoundError:
        return False


def _last_committed_day(
    repo: "icechunk.Repository",
    branch: str,
    group: str | None = None,
) -> tuple[tuple[int, int], np.datetime64] | None:
    """Return ((year, doy), last_t) of the latest already-committed day,
    or None if the group doesn't exist or has no t data yet.
    """
    try:
        session = repo.readonly_session(branch=branch)
        existing = xr.open_zarr(
            session.store, group=group or None, chunks=None, zarr_format=3,
        )
    except (FileNotFoundError, KeyError):
        return None
    if "t" not in existing.coords or existing.sizes.get("t", 0) == 0:
        return None

    # Check the *raw* index, before any dedup or sort: resuming from max(t)
    # when the store is out of order would silently skip every day between
    # the true last append and that maximum. Sorting first would make the
    # check unfailable.
    t_idx = existing.xindexes["t"].to_pandas_index()
    if not (t_idx.is_monotonic_increasing and t_idx.is_unique):
        raise ValueError(
            "Cannot auto-resume: existing dataset's `t` index is not strictly "
            f"increasing (monotonic={t_idx.is_monotonic_increasing}, "
            f"unique={t_idx.is_unique})."
        )

    last_t_np = existing["t"].values[-1]
    last_t_ts = pd.Timestamp(last_t_np)
    return (last_t_ts.year, int(last_t_ts.dayofyear)), np.datetime64(
        last_t_np, "ns"
    )


def log_event(
    log_dir: str,
    satellite: str,
    channel_label: str,
    date_str: str,
    event_type: str,
    reason: str,
) -> None:
    """Append a skip/error entry to the per-channel log file.

    `channel_label` is the already-formatted channel name — "C13" for GOES ABI,
    a band name like "ir087" for instruments whose channels are not numbered.
    """
    from planetary_datasets.common.paths import safe_component, safe_join

    # satellite and channel_label come from CLI arguments, so reduce each to a single path
    # segment rather than letting a stray separator put the log outside log_dir.
    name = f"{safe_component(satellite)}_{safe_component(channel_label)}_ingest.log"
    pathlib.Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_path = safe_join(log_dir, name)
    timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    with open(log_path, "a") as f:
        f.write(f"{timestamp} | {date_str} | {event_type} | {reason}\n")


# =============================================================================
# Main ingest loop
# =============================================================================
def ingest_all_days(
    repo: "icechunk.Repository",
    channel: int | str,
    *,
    satellite_name: str,
    satellite: str,
    product_label: str,
    archive_start_date: datetime.date,
    all_days: list[tuple[tuple[int, int], list[str]]],
    preprocess_fn: Callable[[xr.Dataset], xr.Dataset],
    branch: str = "main",
    group: str | None = None,
    registry: ObjectStoreRegistry | None = None,
    bucket: str | None = None,
    loop_start_day: int = 0,
    loop_end_day: int | None = None,
    batch_size: int = 1,
    loadable_variables: Iterable[str] = (),
    resume: bool = True,
    zarr_async_concurrency: int | None = None,
    log_dir: str | None = None,
    keep_data_vars: frozenset[str] = _KEEP_DATA_VARS,
    repo_factory: Callable[[str], "icechunk.Repository"] | None = None,
    epoch_threshold: np.datetime64 | None = None,
    channel_label: str | None = None,
    grid_size: int | None = None,
    batch_repair_fn: Callable[[list[str]], list[str]] | None = None,
    open_batch_fn: Callable[..., xr.Dataset] | None = None,
    day_urls_fn: Callable[[tuple[int, int]], list[str]] | None = None,
    repo_reopen_fn: Callable[[], "icechunk.Repository"] | None = None,
    reopen_every: int = REOPEN_REPO_EVERY,
    scan_start_fn: Callable[[str], np.datetime64] = parse_scan_start_to_datetime,
) -> None:
    """Ingest every selected day for a single channel into repo, in batches
    of batch_size days per commit.

    This is the common implementation used by all GOES RadF satellite modules,
    and by any other geostationary imager whose files virtualize the same way.

    `channel` is an ABI channel number by default. Instruments whose channels
    are named rather than numbered (GK-2A AMI bands, say) pass `channel_label`
    and `grid_size` instead, which also skips the ABI 1-16 range check.
    """
    if channel_label is None:
        if not isinstance(channel, int) or channel < 1 or channel > 16:
            raise ValueError(f"Channel must be 1-16, got {channel!r}")
        channel_label = _ch(channel)
    if grid_size is None:
        grid_size = _grid_size(channel)

    if registry is None:
        if bucket is None:
            raise ValueError("Must provide registry or bucket")
        store = make_store(bucket)
        registry = ObjectStoreRegistry({bucket: store})

    if zarr_async_concurrency is not None:
        import zarr

        zarr.config.set({"async.concurrency": zarr_async_concurrency})
        print(
            f"zarr async concurrency set to {zarr_async_concurrency}",
            flush=True,
        )

    parser = vz.parsers.HDFParser()
    loadable_variables = list(loadable_variables)

    grid = grid_size
    print(
        f"Channel {channel_label}: {grid}x{grid} grid "
        f"({grid * 0.000014 * 35786:.1f} km resolution)",
        flush=True,
    )

    end_idx = loop_end_day if loop_end_day is not None else len(all_days)
    selected = all_days[loop_start_day:end_idx]
    print(
        f"Ingesting {len(selected)} day(s) "
        f"[{loop_start_day}:{end_idx}] of {len(all_days)} total available.",
        flush=True,
    )

    is_first_write = not _schema_exists(repo, branch, group)
    if is_first_write:
        print(
            f"No existing schema on '{branch}' at group {group!r} — the first "
            "iteration will create it (no append_dim).",
            flush=True,
        )

    skip_through: tuple[int, int] | None = None
    last_committed_t: np.datetime64 | None = None
    if resume and not is_first_write:
        info = _last_committed_day(repo, branch, group)
        if info is not None:
            skip_through, last_committed_t = info
            print(
                f"Auto-resuming: last committed day is "
                f"{skip_through[0]}-{skip_through[1]:03d} "
                f"(last t = {last_committed_t}). Skipping days at or before.",
                flush=True,
            )

    n_batches = (len(selected) + batch_size - 1) // batch_size
    boundary_checked = False
    files_ingested = 0
    batches_failed = 0
    consecutive_failed_days = 0
    all_consecutive_are_codec = True
    codec_change_date: datetime.date | None = None
    # First batch of the current run of failures, so that a codec change can
    # rewind to it: the days that *proved* the era boundary belong to the new
    # era, and without a rewind they are in neither store.
    first_failed_batch_ind: int | None = None
    # Batches already rewound to, so a genuinely broken day cannot bounce the
    # loop between "new era" and "still failing" forever.
    rewound_to: set[int] = set()

    # An index-driven loop rather than `for ... in range(...)`: the increment
    # happens at the top, so every `continue` below still advances, and the
    # codec-change branch can step the index backwards to retry.
    batch_ind = -1
    while True:
        batch_ind += 1
        if batch_ind >= n_batches:
            break
        batch_start = batch_ind * batch_size
        batch = selected[batch_start : batch_start + batch_size]

        if skip_through is not None:
            batch = [item for item in batch if item[0] > skip_through]
        if not batch:
            continue

        # Days may arrive without their file list when the era probe used a
        # cheap sampling lister; resolve them now, only for days being ingested.
        if day_urls_fn is not None:
            batch = [
                (day, urls if urls else day_urls_fn(day)) for day, urls in batch
            ]
        urls_for_this_batch = [u for _, urls in batch for u in urls]

        (first_year, first_doy), _ = batch[0]
        (last_year, last_doy), _ = batch[-1]
        first_date = _date_from_doy(first_year, first_doy)
        last_date = _date_from_doy(last_year, last_doy)
        first_archive_day = _archive_day_index(
            first_year, first_doy, archive_start_date
        )
        last_archive_day = _archive_day_index(
            last_year, last_doy, archive_start_date
        )

        date_range = (
            first_date.isoformat()
            if first_date == last_date
            else f"{first_date.isoformat()} to {last_date.isoformat()}"
        )

        if not urls_for_this_batch:
            print(
                f"\n  SKIPPING batch {batch_ind} "
                f"({date_range}) — no files found for {channel_label}",
                flush=True,
            )
            if log_dir is not None:
                log_event(
                    log_dir, satellite, channel_label, date_range,
                    "SKIPPED", f"no files found for {channel_label}",
                )
            continue

        try:
            if not boundary_checked and last_committed_t is not None:
                # This guard only runs on resume, so a parser that cannot read
                # the mission's filenames breaks *every* batch after a resume
                # while fresh stores look fine — which is how it silently
                # truncated 11 of 15 GK-2A bands. It is a sanity check, not
                # part of the write, so a parse failure must not cost the day.
                try:
                    first_file_scan_start = scan_start_fn(urls_for_this_batch[0])
                except Exception as exc:
                    print(
                        f"  Skipping the cross-batch ordering check — could not "
                        f"parse a scan start from "
                        f"{urls_for_this_batch[0].rsplit('/', 1)[-1]!r} "
                        f"({type(exc).__name__}: {exc}). Pass scan_start_fn for "
                        f"this mission to re-enable it.",
                        flush=True,
                    )
                    boundary_checked = True
                else:
                    if first_file_scan_start <= last_committed_t:
                        raise ValueError(
                            f"Cross-batch ordering violation: existing data's "
                            f"last `t` is {last_committed_t}, but the first "
                            f"uncommitted file's scan-start parses to "
                            f"{first_file_scan_start}."
                        )
                    boundary_checked = True

            print(f"\n====== Batch {batch_ind} ======")
            print(
                f"Ingesting {len(batch)} day(s) "
                f"({first_date.isoformat()} through {last_date.isoformat()}, "
                f"archive days {first_archive_day}-{last_archive_day})"
            )
            print(f"Total files in this batch: {len(urls_for_this_batch)}")

            def _open_batch(urls: list[str]) -> xr.Dataset:
                # Instruments whose timestep spans many files (Himawari ISatSS
                # tiles each hold one chunk of a 10x10 mosaic) supply their own
                # builder; one URL is not one dataset for them.
                if open_batch_fn is not None:
                    return open_batch_fn(
                        urls,
                        registry=registry,
                        parser=parser,
                        preprocess_fn=preprocess_fn,
                        loadable_variables=loadable_variables,
                    )
                return vz.open_virtual_mfdataset(
                    urls,
                    registry=registry,
                    parser=parser,
                    preprocess=preprocess_fn,
                    loadable_variables=loadable_variables,
                    parallel="dask",
                    **_COMBINE_KWARGS,
                )

            with timer("Creating the virtual dataset"):
                try:
                    vds = _open_batch(urls_for_this_batch)
                except NotImplementedError as e:
                    # A handful of files carry a different codec pipeline from
                    # the rest of their day — a single uncompressed file is
                    # enough to make the whole day unconcatenatable. Where the
                    # caller knows how to spot them, drop the outliers and
                    # retry rather than losing every timestep for that day.
                    if batch_repair_fn is None or "codec" not in str(e).lower():
                        raise
                    kept = batch_repair_fn(urls_for_this_batch)
                    dropped = len(urls_for_this_batch) - len(kept)
                    if not kept or dropped <= 0:
                        raise
                    msg = (
                        f"mixed codecs in batch; dropped {dropped} outlier "
                        f"file(s) of {len(urls_for_this_batch)} and retried"
                    )
                    print(f"    {msg}", flush=True)
                    if log_dir is not None:
                        log_event(
                            log_dir, satellite, channel_label, date_range,
                            "CODEC_OUTLIERS_DROPPED", msg,
                        )
                    urls_for_this_batch = kept
                    vds = _open_batch(kept)

            with timer("Validating batch data variables"):
                missing, extra = validate_batch_data_vars(vds, keep_data_vars)
                if extra:
                    print(
                        f"    Dropping {len(extra)} extra data var(s): "
                        f"{sorted(extra)}"
                    )
                    vds = vds.drop_vars(list(extra))
                if missing:
                    reason = (
                        f"Missing {len(missing)} required data var(s): "
                        f"{sorted(missing)}"
                    )
                    raise ValueError(reason)

            with timer("Checking t is strictly increasing"):
                t_idx = vds.indexes["t"]
                if not (t_idx.is_monotonic_increasing and t_idx.is_unique):
                    raise ValueError(
                        f"Batch {first_date.isoformat()}..{last_date.isoformat()}: "
                        f"`t` is not strictly increasing across the "
                        f"{len(urls_for_this_batch)} files "
                        f"(monotonic={t_idx.is_monotonic_increasing}, "
                        f"unique={t_idx.is_unique})."
                    )

            with timer("Opening an icechunk session"):
                session = repo.writable_session(branch)

            with timer("Writing to icechunk"):
                from xarray.coding.variables import SerializationWarning

                with warnings.catch_warnings():
                    warnings.filterwarnings(
                        "ignore",
                        category=SerializationWarning,
                        message=r".*floating point data as an integer dtype without any _FillValue.*",
                    )
                    if is_first_write:
                        vds.vz.to_icechunk(session.store, group=group)
                        is_first_write = False
                    else:
                        vds.vz.to_icechunk(
                            session.store, group=group, append_dim="t"
                        )

            with timer("Committing to icechunk"):
                msg = (
                    f"Ingested {satellite_name} {product_label} "
                    f"{channel_label} files for "
                    f"{first_date.isoformat()} through "
                    f"{last_date.isoformat()}. "
                    f"Archive days {first_archive_day}-{last_archive_day}."
                )
                session.commit(msg)
                print(msg)

            with timer("Cleaning up memory"):
                vds.close()
                del vds
                del session
                gc.collect()
                malloc_trim()

            # Repeated append+commit grows RSS in proportion to the manifest
            # split size. With a one-day split this sheds most of what is left,
            # taking the slope from +13 to +3 MB/batch for ~0s. See
            # REOPEN_REPO_EVERY for the measurements.
            if (
                repo_reopen_fn is not None
                and reopen_every > 0
                and (batch_ind + 1) % reopen_every == 0
            ):
                with timer(f"Reopening the repository (every {reopen_every} batches)"):
                    del repo
                    gc.collect()
                    repo = repo_reopen_fn()

            files_ingested += len(urls_for_this_batch)
            consecutive_failed_days = 0
            all_consecutive_are_codec = True
            codec_change_date = None
            first_failed_batch_ind = None

        except Exception as e:
            print(
                f"\n  SKIPPING batch {batch_ind} "
                f"({date_range}) — "
                f"{type(e).__name__}: {e}",
                flush=True,
            )
            # Skips are normal (missing days, codec boundaries), so the default
            # is one line. But a bare "IndexError: list index out of range" is
            # undiagnosable after the fact, and these batches are expensive to
            # reproduce, so allow opting into the traceback.
            if os.environ.get("INGEST_DEBUG_TRACEBACKS"):
                traceback.print_exc()
            batches_failed += 1
            consecutive_failed_days += len(batch)
            if first_failed_batch_ind is None:
                first_failed_batch_ind = batch_ind

            if _is_codec_error(e):
                if codec_change_date is None:
                    codec_change_date = first_date
            else:
                all_consecutive_are_codec = False

            if log_dir is not None:
                log_event(
                    log_dir, satellite, channel_label, date_range,
                    "ERROR", f"{type(e).__name__}: {e}",
                )
            if consecutive_failed_days >= MAX_CONSECUTIVE_FAILED_DAYS:
                # If all consecutive failures are codec-related, handle
                # by creating a new Icechunk store for the new codec era.
                retry_from = first_failed_batch_ind
                if (
                    all_consecutive_are_codec
                    and codec_change_date is not None
                    and repo_factory is not None
                    and epoch_threshold is not None
                    and retry_from is not None
                    and retry_from not in rewound_to
                ):
                    date_str = codec_change_date.isoformat()
                    print(
                        f"\nCodec change detected on {date_str} for "
                        f"{channel_label}. Creating new Icechunk store "
                        f"with date suffix '{date_str}' and retrying from "
                        f"batch {retry_from}.",
                        flush=True,
                    )
                    if log_dir is not None:
                        log_event(
                            log_dir, satellite, channel_label, date_str,
                            "CODEC_CHANGE",
                            f"Creating new store for codec era "
                            f"starting {date_str}, retrying from batch "
                            f"{retry_from}",
                        )
                    repo = repo_factory(date_str)
                    preprocess_fn = make_preprocess_no_codec_check(
                        epoch_threshold, keep_data_vars
                    )
                    # The era store may already exist from an earlier run:
                    # writing without append_dim would recreate its arrays and
                    # drop everything already committed. Re-derive both the
                    # first-write flag and the resume point against the *new*
                    # repo — the old store's resume point would otherwise keep
                    # filtering out days this store does not have.
                    is_first_write = not _schema_exists(repo, branch, group)
                    skip_through = None
                    last_committed_t = None
                    boundary_checked = False
                    if resume and not is_first_write:
                        info = _last_committed_day(repo, branch, group)
                        if info is not None:
                            skip_through, last_committed_t = info
                            print(
                                f"Auto-resuming the new era store: last "
                                f"committed day is {skip_through[0]}-"
                                f"{skip_through[1]:03d} (last t = "
                                f"{last_committed_t}).",
                                flush=True,
                            )
                    consecutive_failed_days = 0
                    all_consecutive_are_codec = True
                    codec_change_date = None
                    first_failed_batch_ind = None
                    # Retry the days that proved the boundary: they belong to
                    # the new era. -1 because the loop increments at the top.
                    rewound_to.add(retry_from)
                    batch_ind = retry_from - 1
                    continue

                msg = (
                    f"Stopping {channel_label}: "
                    f"{consecutive_failed_days} consecutive "
                    f"day(s) skipped/failed "
                    f"(threshold: {MAX_CONSECUTIVE_FAILED_DAYS})."
                )
                print(f"\n{msg}", flush=True)
                if log_dir is not None:
                    log_event(
                        log_dir, satellite, channel_label,
                        first_date.isoformat(),
                        "STOPPED", msg,
                    )
                return
            # Re-derive in case schema was written but commit failed
            is_first_write = not _schema_exists(repo, branch, group)

    if batches_failed:
        print(
            f"\nWARNING: {batches_failed} batch(es) failed and were skipped.",
            flush=True,
        )


# =============================================================================
# Backwards era discovery
# =============================================================================
# Files sampled per day when probing whether a day still combines with the era
# being built. Two files also exercises the intra-day concat.
PROBE_FILES_PER_DAY = 2

# Consecutive probe failures required before declaring an era boundary. One
# corrupt day should not end an era, whereas a codec/schema change affects
# every day past the boundary.
PROBE_MISMATCH_TOLERANCE = 3


def open_virtual_batch(
    urls: list[str],
    *,
    registry: ObjectStoreRegistry,
    parser: Any,
    preprocess_fn: Callable[[xr.Dataset], xr.Dataset],
    loadable_variables: Iterable[str],
) -> xr.Dataset:
    """Open and combine a set of RadF files exactly as the ingest does."""
    return vz.open_virtual_mfdataset(
        urls,
        registry=registry,
        parser=parser,
        preprocess=preprocess_fn,
        loadable_variables=list(loadable_variables),
        **_COMBINE_KWARGS,
    )


def _probe_urls(urls: list[str], n: int) -> list[str]:
    """Pick up to n URLs spread evenly across a day's files."""
    if n < 1:
        raise ValueError(f"probe_files_per_day must be >= 1, got {n}")
    if len(urls) <= n:
        return list(urls)
    if n == 1:
        return [urls[0]]
    step = (len(urls) - 1) / (n - 1)
    return [urls[round(i * step)] for i in range(n)]


def _virtual_grid_shapes(ds: xr.Dataset) -> dict[str, tuple[int, ...]]:
    """Shape excluding `t` of every virtual data variable, keyed by name."""
    return {
        name: tuple(size for dim, size in zip(da.dims, da.shape) if dim != "t")
        for name, da in ds.data_vars.items()
        if isinstance(da.data, ManifestArray)
    }


def combine_failure_reason(
    reference: xr.Dataset,
    candidate: xr.Dataset,
) -> str | None:
    """Return None if candidate concatenates onto reference, else the reason.

    The concat here is the same one virtualizarr performs during ingest, so it
    raises on exactly the differences that matter: dtype, codec pipeline, chunk
    shape, and non-concat-axis shape. A reason means the two days cannot share
    an Icechunk store.
    """
    ref_vars = frozenset(reference.data_vars)
    cand_vars = frozenset(candidate.data_vars)
    if ref_vars != cand_vars:
        missing = sorted(ref_vars - cand_vars)
        extra = sorted(cand_vars - ref_vars)
        return f"data variables differ (missing={missing}, extra={extra})"

    ref_grids = _virtual_grid_shapes(reference)
    if _virtual_grid_shapes(candidate) != ref_grids:
        return (
            f"virtual grid shapes differ: "
            f"{_virtual_grid_shapes(candidate)} != {ref_grids}"
        )

    try:
        combined = xr.concat([candidate, reference], dim="t", **_CONCAT_KWARGS)
    except Exception as e:
        return f"{type(e).__name__}: {e}"

    combined_grids = _virtual_grid_shapes(combined)
    if combined_grids != ref_grids:
        return (
            f"concatenation reshaped the grid ({combined_grids} != {ref_grids}) "
            f"— the x/y coordinates were aligned rather than matched"
        )
    return None


def iter_eras_backwards(
    store,
    bucket: str,
    product: str,
    min_year: int,
    product_keys: list[str],
    channel: int | str,
    *,
    registry: ObjectStoreRegistry,
    loadable_variables: Iterable[str],
    epoch_threshold: np.datetime64,
    end_date: datetime.date,
    start_date: datetime.date | None = None,
    keep_data_vars: frozenset[str] = _KEEP_DATA_VARS,
    probe_files_per_day: int = PROBE_FILES_PER_DAY,
    mismatch_tolerance: int = PROBE_MISMATCH_TOLERANCE,
    product_reproc: str | None = None,
    reproc_for_date: Callable[[datetime.date], bool] | None = None,
    satellite: str | None = None,
    log_dir: str | None = None,
    channel_label: str | None = None,
    list_days_fn: Callable[..., Iterable[tuple[int, int]]] | None = None,
    list_day_files_fn: Callable[..., list[str]] | None = None,
    preprocess_fn: Callable[[xr.Dataset], xr.Dataset] | None = None,
    open_batch_fn: Callable[..., xr.Dataset] | None = None,
    probe_select_fn: Callable[[list[str], int], list[str]] | None = None,
    probe_open_fn: Callable[..., xr.Dataset] | None = None,
    probe_list_fn: Callable[..., list[str]] | None = None,
) -> Iterator[tuple[datetime.date, list[tuple[tuple[int, int], list[str]]]]]:
    """Walk the archive backwards from end_date, yielding one era at a time.

    An era is a maximal run of consecutive days that all combine with the
    newest day in the run (its anchor). Each era is yielded as
    ``(era_end_date, all_days)`` with all_days in chronological order, ready to
    hand straight to ingest_all_days — the file listing done while probing is
    reused, so no day is listed twice.

    Eras come out newest-first: the first ends at end_date, the next ends on
    the day the first one could not reach, and so on back to start_date.

    Defaults target the GOES `product/YYYY/DOY/` archive layout. An instrument
    laid out differently injects `list_days_fn` / `list_day_files_fn` (and a
    matching `preprocess_fn`); both still speak the `(year, doy)` day key, so
    the era bookkeeping is unchanged.
    """
    if channel_label is None:
        channel_label = _ch(channel)
    if list_days_fn is None:
        list_days_fn = iter_days
    if list_day_files_fn is None:
        list_day_files_fn = list_day_files
    days = [
        day
        for day in list_days_fn(store, product, min_year, product_keys)
        if (_date_from_doy(*day) <= end_date
            and (start_date is None or _date_from_doy(*day) >= start_date))
    ]
    days.reverse()
    print(
        f"{channel_label}: probing {len(days)} archive day(s) backwards from "
        f"{end_date.isoformat()}.",
        flush=True,
    )

    parser = vz.parsers.HDFParser()
    if preprocess_fn is None:
        preprocess_fn = make_preprocess_no_codec_check(epoch_threshold, keep_data_vars)
    loadable_variables = list(loadable_variables)

    def probe(
        day: tuple[int, int],
    ) -> tuple[list[str], xr.Dataset | None, str | None]:
        """Open a sample of a day's files. Returns (urls, vds, open_error)."""
        year, doy = day
        reproc = (
            reproc_for_date(_date_from_doy(year, doy))
            if reproc_for_date is not None
            else False
        )
        # A probe needs only a representative file, so an instrument whose
        # day prefix is huge (Himawari's holds ~200k objects across every band
        # and slot) can supply a far cheaper listing for this step. The full
        # listing is then deferred to ingest time, for days actually ingested.
        lister = probe_list_fn or list_day_files_fn
        urls = lister(
            store, bucket, product, year, doy, channel,
            product_reproc=product_reproc, reproc=reproc,
        )
        if not urls:
            return [], None, "no files found"
        select = probe_select_fn or _probe_urls
        # Combinability is a property of the array metadata, so the probe can
        # use a far cheaper opener than the ingest. Himawari uses this to test
        # one tile per day instead of two 88-tile scenes.
        opener = probe_open_fn or open_batch_fn or open_virtual_batch
        try:
            vds = opener(
                select(urls, probe_files_per_day),
                registry=registry,
                parser=parser,
                preprocess_fn=preprocess_fn,
                loadable_variables=loadable_variables,
            )
        except Exception as e:
            return urls, None, f"{type(e).__name__}: {e}"
        return urls, vds, None

    def note(date: datetime.date, event: str, reason: str) -> None:
        if log_dir is not None and satellite is not None:
            log_event(log_dir, satellite, channel_label, date.isoformat(), event, reason)

    # Probes are cached only for days awaiting an era-boundary decision, since
    # those are the ones re-examined after the walk rewinds to a boundary.
    probes: dict[tuple[int, int], tuple[list[str], xr.Dataset | None, str | None]] = {}
    era_days: list[tuple[tuple[int, int], list[str]]] = []  # newest-first
    era_end: datetime.date | None = None
    reference: xr.Dataset | None = None
    pending: list[int] = []  # indices into `days`, newest-first

    def drop(day: tuple[int, int], vds: xr.Dataset | None) -> None:
        probes.pop(day, None)
        if vds is not None:
            vds.close()

    def absorb_pending() -> None:
        """Move tolerated mismatch days into the current era.

        They sit inside the era's date range, so the era owns them;
        ingest_all_days skips whichever of them genuinely fail.
        """
        for idx in pending:
            pday = days[idx]
            purls, pvds, _ = probes.pop(pday)
            if pvds is not None:
                pvds.close()
            era_days.append((pday, [] if probe_list_fn is not None else purls))
        pending.clear()

    i = 0
    probed_count = 0
    probe_rss0 = rss_gb()
    while i < len(days):
        day = days[i]
        date = _date_from_doy(*day)
        urls, vds, open_error = probes[day] if day in probes else probe(day)

        # The probe walks back a day at a time and can run for hundreds of
        # days before committing anything, so it needs its own memory trace:
        # a leak here is invisible in the per-batch ingest timers.
        probed_count += 1
        if probed_count % 50 == 0:
            probe_rss = rss_gb()
            print(
                f"  [probe] {probed_count} day(s) examined, "
                f"rss {probe_rss:.2f}GB ({(probe_rss - probe_rss0) * 1024:+.0f}MB "
                f"since the walk began, {len(probes)} cached)",
                flush=True,
            )

        if not urls:
            drop(day, vds)
            note(date, "SKIPPED", f"no files found for {channel_label}")
            i += 1
            continue

        if open_error is not None:
            reason = open_error
        elif reference is None:
            reason = None
        else:
            reason = combine_failure_reason(reference, vds)

        if reason is None:
            if reference is None:
                reference = vds
                era_end = date
                print(f"  {date.isoformat()}: era anchor.", flush=True)
            else:
                drop(day, vds)
            probes.pop(day, None)
            absorb_pending()
            # With a probe-only lister the urls here are a sample, not the
            # day's full file set; leave them empty so the ingest re-lists.
            era_days.append((day, [] if probe_list_fn is not None else urls))
            i += 1
            continue

        print(f"  {date.isoformat()}: does not combine — {reason}", flush=True)
        note(date, "PROBE_MISMATCH", reason)

        if reference is None:
            # No era is open yet, so this day cannot anchor one. Drop it and
            # keep walking back rather than counting it towards a boundary.
            drop(day, vds)
            i += 1
            continue

        probes[day] = (urls, vds, open_error)
        pending.append(i)
        if len(pending) < mismatch_tolerance:
            i += 1
            continue

        # `mismatch_tolerance` days in a row failed to combine: era boundary.
        boundary = pending[0]
        era_start = _date_from_doy(*era_days[-1][0])
        print(
            f"  Era boundary at {_date_from_doy(*days[boundary]).isoformat()}: "
            f"{channel_label} combines from {era_start.isoformat()} "
            f"through {era_end.isoformat()} ({len(era_days)} day(s)).",
            flush=True,
        )
        yield era_end, list(reversed(era_days))
        reference.close()
        era_days, era_end, reference = [], None, None
        pending.clear()
        # The newest unreachable day anchors the next, older era.
        i = boundary

    absorb_pending()
    if era_days:
        era_start = _date_from_doy(*era_days[-1][0])
        print(
            f"  Reached the start of the archive: {channel_label} combines from "
            f"{era_start.isoformat()} through {era_end.isoformat()} "
            f"({len(era_days)} day(s)).",
            flush=True,
        )
        yield era_end, list(reversed(era_days))
    if reference is not None:
        reference.close()


def ingest_backwards(
    channel: int,
    *,
    repo_factory: Callable[[str], "icechunk.Repository"],
    satellite_name: str,
    satellite: str,
    product_label: str,
    archive_start_date: datetime.date,
    store,
    bucket: str,
    product: str,
    min_year: int,
    product_keys: list[str],
    loadable_variables: Iterable[str],
    epoch_threshold: np.datetime64,
    end_date: datetime.date | None = None,
    start_date: datetime.date | None = None,
    first_store_suffix: str | None = None,
    branch: str = "main",
    group: str | None = "",
    batch_size: int = 1,
    zarr_async_concurrency: int | None = None,
    log_dir: str | None = None,
    keep_data_vars: frozenset[str] = _KEEP_DATA_VARS,
    registry: ObjectStoreRegistry | None = None,
    probe_files_per_day: int = PROBE_FILES_PER_DAY,
    mismatch_tolerance: int = PROBE_MISMATCH_TOLERANCE,
    product_reproc: str | None = None,
    reproc_for_date: Callable[[datetime.date], bool] | None = None,
    reproc_label: str | None = None,
    max_eras: int | None = None,
    channel_label: str | None = None,
    grid_size: int | None = None,
    list_days_fn: Callable[..., Iterable[tuple[int, int]]] | None = None,
    list_day_files_fn: Callable[..., list[str]] | None = None,
    era_preprocess_fn: Callable[[xr.Dataset], xr.Dataset] | None = None,
    batch_repair_fn: Callable[[list[str]], list[str]] | None = None,
    open_batch_fn: Callable[..., xr.Dataset] | None = None,
    probe_select_fn: Callable[[list[str], int], list[str]] | None = None,
    probe_open_fn: Callable[..., xr.Dataset] | None = None,
    probe_list_fn: Callable[..., list[str]] | None = None,
    day_urls_fn: Callable[[tuple[int, int]], list[str]] | None = None,
    scan_start_fn: Callable[[str], np.datetime64] = parse_scan_start_to_datetime,
) -> list[str]:
    """Ingest a channel backwards from end_date, one store per combinable era.

    The newest era is discovered and ingested first, into a store suffixed with
    first_store_suffix (today's date by default); older eras land in stores
    suffixed with the date each era ends. Within an era the days are ingested
    forwards, oldest first, because Icechunk can only append along `t`.

    Codec validation is left to the backwards probe rather than the
    satellite's hardcoded codec whitelist: an era is codec-homogeneous by
    construction, so the per-file check has nothing left to catch and would
    only reject codec pipelines that are new but self-consistent.

    Returns the store suffix of every era ingested, newest first.
    """
    if end_date is None:
        end_date = datetime.date.today()
    if registry is None:
        registry = ObjectStoreRegistry({bucket: store})
    if channel_label is None:
        channel_label = _ch(channel)

    preprocess_fn = era_preprocess_fn or make_preprocess_no_codec_check(
        epoch_threshold, keep_data_vars
    )
    suffixes: list[str] = []

    eras = iter_eras_backwards(
        store, bucket, product, min_year, product_keys, channel,
        registry=registry,
        loadable_variables=loadable_variables,
        epoch_threshold=epoch_threshold,
        end_date=end_date,
        start_date=start_date,
        keep_data_vars=keep_data_vars,
        probe_files_per_day=probe_files_per_day,
        mismatch_tolerance=mismatch_tolerance,
        product_reproc=product_reproc,
        reproc_for_date=reproc_for_date,
        satellite=satellite,
        log_dir=log_dir,
        channel_label=channel_label,
        list_days_fn=list_days_fn,
        list_day_files_fn=list_day_files_fn,
        preprocess_fn=era_preprocess_fn,
        open_batch_fn=open_batch_fn,
        probe_select_fn=probe_select_fn,
        probe_open_fn=probe_open_fn,
        probe_list_fn=probe_list_fn,
    )

    for era_index, (era_end, era_days) in enumerate(eras):
        suffix = (
            first_store_suffix
            if era_index == 0 and first_store_suffix
            else era_end.isoformat()
        )
        era_start = _date_from_doy(*era_days[0][0])
        label = product_label
        if (
            reproc_label is not None
            and reproc_for_date is not None
            and reproc_for_date(era_end)
        ):
            label = reproc_label

        print(
            f"\n########## {satellite_name} {channel_label} era {era_index}: "
            f"{era_start.isoformat()} to {era_end.isoformat()} "
            f"({len(era_days)} day(s)) -> store suffix {suffix!r} ##########\n",
            flush=True,
        )
        if log_dir is not None:
            log_event(
                log_dir, satellite, channel_label, era_end.isoformat(), "ERA",
                f"{era_start.isoformat()}..{era_end.isoformat()} "
                f"({len(era_days)} days) -> store suffix {suffix}",
            )

        ingest_all_days(
            repo_factory(suffix), channel,
            satellite_name=satellite_name,
            satellite=satellite,
            product_label=label,
            archive_start_date=archive_start_date,
            all_days=era_days,
            preprocess_fn=preprocess_fn,
            branch=branch,
            group=group,
            registry=registry,
            bucket=bucket,
            batch_size=batch_size,
            loadable_variables=loadable_variables,
            zarr_async_concurrency=zarr_async_concurrency,
            log_dir=log_dir,
            keep_data_vars=keep_data_vars,
            epoch_threshold=epoch_threshold,
            channel_label=channel_label,
            grid_size=grid_size,
            batch_repair_fn=batch_repair_fn,
            open_batch_fn=open_batch_fn,
            day_urls_fn=day_urls_fn,
            scan_start_fn=scan_start_fn,
            repo_reopen_fn=lambda s=suffix: repo_factory(s),
        )
        suffixes.append(suffix)

        if max_eras is not None and len(suffixes) >= max_eras:
            print(
                f"\nStopping after {max_eras} era(s) as requested.",
                flush=True,
            )
            break

    if not suffixes:
        print(
            f"{channel_label}: no combinable days found at or before "
            f"{end_date.isoformat()}.",
            flush=True,
        )
    return suffixes


def ingest_all_channels(
    repo: "icechunk.Repository",
    *,
    ingest_fn: Callable,
    channels: Iterable[int] | None = None,
    branch: str = "main",
    base_group: str = "ABI-L1b-RadF",
    **kwargs,
) -> None:
    """Ingest multiple channels sequentially into separate groups."""
    channel_list = list(channels) if channels is not None else list(range(1, 17))

    for channel in channel_list:
        group = f"{base_group}/{_ch(channel)}"
        print(
            f"\n############# Channel {_ch(channel)} → {group!r} #############\n",
            flush=True,
        )
        ingest_fn(
            repo,
            channel,
            branch=branch,
            group=group,
            **kwargs,
        )


def smoke_test(
    store,
    bucket: str,
    product: str,
    min_year: int,
    product_keys: list[str],
    satellite_name: str,
    preprocess_fn: Callable[[xr.Dataset], xr.Dataset],
    loadable_variables: Iterable[str],
    channel: int = 13,
    n_files: int = 3,
    *,
    start: str | None = None,
    product_reproc: str | None = None,
    reproc: bool = False,
) -> xr.Dataset:
    """Open the first n_files for a channel via open_virtual_mfdataset + preprocess."""
    label = "Reproc " if reproc else ""
    urls = []
    for i, u in enumerate(iter_archive(
        store, bucket, product, min_year, product_keys, channel,
        start=start, product_reproc=product_reproc, reproc=reproc,
    )):
        urls.append(u)
        if i + 1 >= n_files:
            break
    if not urls:
        raise ValueError(
            f"No {label}files found for channel {_ch(channel)} "
            f"in {satellite_name} archive."
        )
    print(f"Opening {len(urls)} {_ch(channel)} file(s):")
    for u in urls:
        print(f"  {u}")

    registry = ObjectStoreRegistry({bucket: store})

    vmds = vz.open_virtual_mfdataset(
        urls,
        registry=registry,
        parser=vz.parsers.HDFParser(),
        loadable_variables=list(loadable_variables),
        preprocess=preprocess_fn,
        **_COMBINE_KWARGS,
    )
    return vmds


# =============================================================================
# Per-satellite archives
# =============================================================================

#: Variables the ABI L1b files carry on every satellite.
_BASE_LOADABLE_VARIABLES: tuple[str, ...] = (
    "t", "x", "y", "x_image", "y_image", "x_image_bounds", "y_image_bounds",
    "time_bounds", "goes_imager_projection", "nominal_satellite_height",
    "nominal_satellite_subpoint_lat", "nominal_satellite_subpoint_lon",
    "geospatial_lat_lon_extent", "earth_sun_distance_anomaly_in_AU", "band_id",
    "band_wavelength", "esun", "kappa0", "planck_fk1", "planck_fk2", "planck_bc1",
    "planck_bc2", "valid_pixel_count", "missing_pixel_count", "saturated_pixel_count",
    "undersaturated_pixel_count", "min_radiance_value_of_valid_pixels",
    "max_radiance_value_of_valid_pixels", "mean_radiance_value_of_valid_pixels",
    "std_dev_radiance_value_of_valid_pixels", "percent_uncorrectable_L0_errors",
    "percent_uncorrectable_GRB_errors", "algorithm_dynamic_input_data_container",
    "processing_parm_version_container", "algorithm_product_version_container",
    "focal_plane_temperature_threshold_exceeded_count", "maximum_focal_plane_temperature",
    "focal_plane_temperature_threshold_increasing",
    "focal_plane_temperature_threshold_decreasing", "yaw_flip_flag", "star_id",
    "t_star_look", "band_wavelength_star_look", "time_bounds_swaths", "time_bounds_rows",
    "reprocessing_version",
)

_ZLIB = {"class": "Zlib", "codec_name": "numcodecs.zlib", "codec_config": {"level": 1}}


def _shuffle(elementsize: int) -> dict[str, Any]:
    return {
        "class": "Shuffle",
        "codec_name": "numcodecs.shuffle",
        "codec_config": {"elementsize": elementsize},
    }


#: Before ABI switched the Shuffle filter on.
CODECS_PRE_SHUFFLE: dict[str, list[dict[str, Any]]] = {
    "Rad": [{"class": "BytesCodec", "endian": "little"}, _ZLIB],
    "DQF": [{"class": "BytesCodec", "endian": None}, _ZLIB],
}

#: After the switch, and the only era for satellites launched since.
CODECS_POST_SHUFFLE: dict[str, list[dict[str, Any]]] = {
    "Rad": [{"class": "BytesCodec", "endian": "little"}, _shuffle(2), _ZLIB],
    "DQF": [{"class": "BytesCodec", "endian": None}, _shuffle(1), _ZLIB],
}


@dataclasses.dataclass(frozen=True)
class GoesArchive:
    """One GOES satellite's ABI L1b full-disk archive.

    The per-satellite modules are generated from these; everything else lives in the
    functions above and takes the configuration as arguments.
    """

    satellite: str
    satellite_name: str
    min_year: int
    archive_start_date: datetime.date
    loadable_variables: tuple[str, ...] = _BASE_LOADABLE_VARIABLES
    reproc_start_date: datetime.date | None = None
    reproc_end_date: datetime.date | None = None
    product: str = "ABI-L1b-RadF/"
    #: None for a satellite NOAA never reprocessed, which also drops the reproc key below.
    product_reproc: str | None = "ABI-L1b-RadF-Reproc/"
    #: One entry for a satellite with a single codec era, two for one that predates the
    #: Shuffle switch and so has to accept both.
    codec_eras: tuple[dict[str, list[dict[str, Any]]], ...] = (
        CODECS_PRE_SHUFFLE,
        CODECS_POST_SHUFFLE,
    )

    @property
    def product_keys(self) -> list[str]:
        return ["ABI-L1b-RadF-Reproc", "ABI-L1b-RadF"] if self.product_reproc else ["ABI-L1b-RadF"]

    @property
    def bucket(self) -> str:
        return source_bucket(self.satellite)

    @property
    def store_prefix(self) -> str:
        return default_store_prefix(self.satellite)

    @property
    def epoch_threshold(self) -> np.datetime64:
        """Scan times before the satellite existed are rejected as corrupt."""
        return np.datetime64(f"{self.min_year}-01-01", "ns")

    def store(self):
        return make_store(self.bucket)

    def check_codecs(self, ds: xr.Dataset) -> xr.Dataset:
        if len(self.codec_eras) == 1:
            return check_codecs_single_era(ds, self.codec_eras[0])
        return check_codecs_dual_era(ds, *self.codec_eras)

    @functools.cached_property
    def preprocess(self):
        return make_preprocess(self.check_codecs, self.epoch_threshold)

    def _product(self, reproc: bool) -> str:
        if reproc and not self.product_reproc:
            raise ValueError(f"{self.satellite_name} has no reprocessed archive")
        return self.product_reproc if reproc else self.product

    def list_years(self) -> list[int]:
        return list_years(self.store(), self.product, self.min_year)

    def list_days_in_year(self, year: int) -> list[int]:
        return list_days_in_year(self.store(), self.product, year)

    def iter_days(self, *, start=None, end=None):
        return iter_days(
            self.store(), self.product, self.min_year, self.product_keys,
            start=start, end=end,
        )

    def list_day_files(self, year, doy, channel, *, start=None, end=None, reproc=False):
        return list_day_files(
            self.store(), self.bucket, self.product, year, doy, channel,
            start=start, end=end, product_reproc=self.product_reproc, reproc=reproc,
        )

    def iter_archive(self, channel, *, start=None, end=None, reproc=False):
        return iter_archive(
            self.store(), self.bucket, self.product, self.min_year,
            self.product_keys, channel, start=start, end=end,
            product_reproc=self.product_reproc, reproc=reproc,
        )

    def iter_archive_by_day(self, channel, *, start=None, end=None, reproc=False):
        return iter_archive_by_day(
            self.store(), self.bucket, self.product, self.min_year,
            self.product_keys, channel, start=start, end=end,
            product_reproc=self.product_reproc, reproc=reproc,
        )

    def days_to_ingest(self, channel, *, start=None, end=None, reproc=False):
        return days_to_ingest(
            self.store(), self.bucket, self.product, self.min_year,
            self.product_keys, channel, start=start, end=end,
            product_reproc=self.product_reproc, reproc=reproc,
        )

    def smoke_test(self, channel=13, n_files=3, *, start=None, reproc=False):
        return smoke_test(
            self.store(), self.bucket, self.product, self.min_year,
            self.product_keys, self.satellite_name, self.preprocess,
            list(self.loadable_variables), channel=channel, n_files=n_files,
            start=start, product_reproc=self.product_reproc, reproc=reproc,
        )

    def ingest_all_days(self, repo, channel, *, group=None, reproc=False,
                        start=None, end=None, loadable_variables=None, **kwargs):
        label = self._product(reproc).rstrip("/")
        if group is None:
            group = f"{label}/{_ch(channel)}"
        return ingest_all_days(
            repo, channel,
            satellite_name=self.satellite_name, satellite=self.satellite,
            product_label=label, archive_start_date=self.archive_start_date,
            all_days=self.days_to_ingest(channel, start=start, end=end, reproc=reproc),
            preprocess_fn=self.preprocess, group=group, bucket=self.bucket,
            loadable_variables=loadable_variables or self.loadable_variables,
            epoch_threshold=self.epoch_threshold, **kwargs,
        )

    def ingest_all_channels(self, repo, *, channels=None, base_group=None,
                            reproc=False, **kwargs):
        if base_group is None:
            base_group = self._product(reproc).rstrip("/")
        return ingest_all_channels(
            repo,
            ingest_fn=lambda r, ch, **kw: self.ingest_all_days(r, ch, reproc=reproc, **kw),
            channels=channels, base_group=base_group, **kwargs,
        )


SATELLITES: dict[str, GoesArchive] = {
    "goes16": GoesArchive(
        satellite="goes16", satellite_name="GOES-16", min_year=2017,
        archive_start_date=datetime.date(2017, 2, 28),
        reproc_start_date=datetime.date(2018, 1, 4),
        reproc_end_date=datetime.date(2024, 12, 28),
    ),
    "goes17": GoesArchive(
        satellite="goes17", satellite_name="GOES-17", min_year=2018,
        archive_start_date=datetime.date(2018, 2, 12),
        reproc_start_date=datetime.date(2018, 8, 28),
        reproc_end_date=datetime.date(2023, 1, 1),
    ),
    "goes18": GoesArchive(
        satellite="goes18", satellite_name="GOES-18", min_year=2022,
        archive_start_date=datetime.date(2022, 7, 28),
        loadable_variables=tuple(
            v for v in _BASE_LOADABLE_VARIABLES
            if v not in {
                "algorithm_dynamic_input_data_container", "algorithm_product_version_container",
                "band_wavelength_star_look", "processing_parm_version_container",
                "reprocessing_version", "star_id", "t_star_look", "time_bounds_rows",
                "time_bounds_swaths",
            }
        ) + ("channel_integration_time", "channel_gain_field"),
        product_reproc=None,
    ),
    "goes19": GoesArchive(
        satellite="goes19", satellite_name="GOES-19", min_year=2024,
        archive_start_date=datetime.date(2024, 10, 10),
        loadable_variables=tuple(
            v for v in _BASE_LOADABLE_VARIABLES
            if v not in {"reprocessing_version", "time_bounds_rows", "time_bounds_swaths"}
        ) + (
            "channel_integration_time", "channel_gain_field", "a_h_NRTH", "b_h_NRTH",
            "number_of_harmonization_coefficients", "num_star_looks",
        ),
        product_reproc=None,
        # Launched after the Shuffle switch, so there is only one era to accept.
        codec_eras=(CODECS_POST_SHUFFLE,),
    ),
}


def bind(satellite: str) -> dict[str, Any]:
    """Module namespace for one satellite, used by the ``goes_*_radf`` shims."""
    archive = SATELLITES[satellite]
    return {
        # Re-exported so the per-satellite module namespace matches what it was before
        # these became shims, including for tests that patch through it.
        "common": sys.modules[__name__],
        "CHANNEL_GRID_SIZE": CHANNEL_GRID_SIZE,
        "RADF_CHUNK_SIZE": RADF_CHUNK_SIZE,
        "DATETIME_NS": DATETIME_NS,
        "MAX_CONSECUTIVE_FAILED_DAYS": MAX_CONSECUTIVE_FAILED_DAYS,
        "_KEEP_DATA_VARS": _KEEP_DATA_VARS,
        "RadFValidationError": RadFValidationError,
        "parse_scan_start_to_datetime": parse_scan_start_to_datetime,
        "ARCHIVE": archive,
        "SATELLITE": archive.satellite,
        "SATELLITE_NAME": archive.satellite_name,
        "BUCKET": archive.bucket,
        "STORE_PREFIX": archive.store_prefix,
        "PRODUCT": archive.product,
        "PRODUCT_REPROC": archive.product_reproc,
        "PRODUCT_KEYS": archive.product_keys,
        "MIN_YEAR": archive.min_year,
        "ARCHIVE_START_DATE": archive.archive_start_date,
        "REPROC_START_DATE": archive.reproc_start_date,
        "REPROC_END_DATE": archive.reproc_end_date,
        "EPOCH_THRESHOLD": archive.epoch_threshold,
        "DEFAULT_LOADABLE_VARIABLES": archive.loadable_variables,
        "preprocess": archive.preprocess,
        "list_years": archive.list_years,
        "list_days_in_year": archive.list_days_in_year,
        "iter_days": archive.iter_days,
        "list_day_files": archive.list_day_files,
        "iter_archive": archive.iter_archive,
        "iter_archive_by_day": archive.iter_archive_by_day,
        "days_to_ingest": archive.days_to_ingest,
        "smoke_test": archive.smoke_test,
        "ingest_all_days": archive.ingest_all_days,
        "ingest_all_channels": archive.ingest_all_channels,
        "_store": archive.store,
        "_ch": _ch,
        "_grid_size": _grid_size,
    }


def satellite_cli(satellite: str, argv: list[str] | None = None) -> None:
    """Command line for one satellite's module, shared by all of them."""
    import argparse

    archive = SATELLITES[satellite]
    parser = argparse.ArgumentParser(
        description=f"Ingest {archive.satellite_name} ABI-L1b-RadF into Icechunk via VirtualiZarr."
    )
    parser.add_argument("--smoke-test", action="store_true", help="Run a quick smoke test and exit.")
    parser.add_argument("--channel", type=int, default=13, help="ABI channel 1-16 (default: 13).")
    parser.add_argument("--n-files", type=int, default=3, help="Files for the smoke test (default: 3).")
    if archive.product_reproc:
        parser.add_argument("--reproc", action="store_true", help="Use the reprocessed archive.")
    args = parser.parse_args(argv)

    if args.smoke_test:
        print(archive.smoke_test(
            channel=args.channel, n_files=args.n_files, reproc=getattr(args, "reproc", False)
        ))
    else:
        parser.print_help()
