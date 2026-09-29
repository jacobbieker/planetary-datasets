"""Virtual ingest of GK-2A AMI L1B full-disk radiance into Icechunk.

GEO-KOMPSAT-2A carries the AMI imager at 128.2E. The NOAA open-data mirror
holds the full-disk (FD) L1B product as netCDF4, one file per band per
10-minute slot:

    AMI/L1B/FD/YYYYMM/DD/HH/gk2a_ami_le1b_<band>_fd<RRR>ge_<YYYYMMDDHHMM>.nc

Each file contains:
  - image_pixel_values(dim_image_y, dim_image_x) -- scaled counts (uint16)
  - a block of dim_1 calibration/navigation scalars
  - variable-length star-look and landmark diagnostics, which are dropped

Band resolutions (the resolution is encoded in the filename, so no channel
-> grid table is needed as it is for GOES ABI):
  - 0.5 km  vi006                       22000 x 22000
  - 1.0 km  vi004, vi005, vi008         11000 x 11000
  - 2.0 km  the 12 IR/NR/SW/WV bands     5500 x 5500

Two structural differences from the GOES ABI ingest drive this module:

1. There is no time coordinate anywhere in the file. `t` is reconstructed
   from the `observation_start_time` global attribute (seconds since J2000,
   2000-01-01 12:00 UTC) and cross-checked against the filename slot.
2. There are no x/y coordinate arrays. The fixed-grid navigation parameters
   are instead carried forward as per-timestep variables, so the projection
   stays reconstructable even as the spacecraft's INR drifts.

Usage:
    from planetary_datasets.providers.virtualized.gk2a_ami_fd import (
        ingest_all_days, smoke_test,
    )

    vds = smoke_test(band="ir087", n_files=3)
"""

from __future__ import annotations

import datetime
from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING, Any

import numpy as np
import obstore as obs
import xarray as xr
from loguru import logger
from obspec_utils.registry import ObjectStoreRegistry

from planetary_datasets.config import Config
from planetary_datasets.providers.virtualized import virtual_repo

from . import goes_radf_common as common

if TYPE_CHECKING:
    import icechunk


# =============================================================================
# GK-2A constants
# =============================================================================
#: NOAA's public Open Data mirror of the KMA archive. This is the identity of
#: the dataset rather than a deployment choice, so it is a constant here; the
#: *output* store location is resolved through the shared config.
BUCKET = "s3://noaa-gk2a-pds"
SATELLITE = "gk2a"
SATELLITE_NAME = "GK-2A"
PRODUCT = "AMI/L1B/FD/"
PRODUCT_LABEL = "AMI-L1B-FD"
# Unused by this module's listing (which is date-based, not DOY-based) but
# kept for signature compatibility with the shared era machinery.
PRODUCT_KEYS = ["FD"]

MIN_YEAR = 2023
# First day present under AMI/L1B/FD/. 2023-02-16 is an isolated early day;
# continuous coverage starts 2023-02-23.
ARCHIVE_START_DATE = datetime.date(2023, 2, 16)

EPOCH_THRESHOLD = np.datetime64("2023-01-01", "ns")

#: `observation_start_time` / `observation_end_time` are seconds since J2000.
GK2A_EPOCH = np.datetime64("2000-01-01T12:00:00", "ns")

#: How far the reconstructed `t` may sit from the filename's nominal slot.
#: A full-disk scan takes about nine minutes, and `t` is its start.
_MAX_T_VS_SLOT_DRIFT = np.timedelta64(20, "m")

#: Files at or above this fraction of the raw array size hold an uncompressed
#: image and cannot share a Zarr array with the Zlib-compressed majority.
#: Measured separation is wide — compressed files run 56-59% of raw.
_UNCOMPRESSED_SIZE_FRACTION = 0.90

BAND_RESOLUTION: dict[str, str] = {
    "vi006": "005",
    "vi004": "010", "vi005": "010", "vi008": "010",
    **{b: "020" for b in (
        "ir087", "ir096", "ir105", "ir112", "ir123", "ir133",
        "nr013", "nr016", "sw038", "wv063", "wv069", "wv073",
    )},
}

_RESOLUTION_GRID: dict[str, int] = {"005": 22000, "010": 11000, "020": 5500}

BANDS: tuple[str, ...] = tuple(BAND_RESOLUTION)

#: The half-kilometre band, excluded from bulk runs the way GOES C02 is.
HIGH_RES_BAND = "vi006"

#: Logical store name, before the band and era discriminators are appended.
#: Resolved against the configured bucket, or ``ICECHUNK_LOCAL_PATH``. Sits
#: under ``bkr/geo/virtualized`` with the other virtualized geostationary
#: stores, rather than loose in ``bkr/geo`` beside the materialised ones.
DEFAULT_STORE_BASE = "bkr/geo/virtualized/gk2a_ami_fd"

#: AMI full disk runs a ten-minute cadence, so one day is 144 slots. Used as
#: the manifest split size so a split matches a day-sized commit batch.
SLOTS_PER_DAY = 144

# Variables kept after preprocess. Everything else is dropped so that every
# file yields an identical schema for xr.concat. The star-look, landmark and
# INR-performance blocks are excluded specifically because their dimensions
# are sized per file (dim_matched_lmks, dim_vis_stars, dim_ir_stars,
# dim_inr_perform) and dim_boa_swaths is length zero.
_GK2A_KEEP_DATA_VARS = frozenset({
    # Payload.
    "image_pixel_values",
    # dim_1 calibration / ephemeris block, fixed length in every file.
    "gsics_coeff_intercept", "gsics_coeff_intercept_standard_error",
    "gsics_coeff_slope", "gsics_coeff_slope_standard_error",
    "gsics_coeff_quadratic", "gsics_coeff_quadratic_standard_error",
    "gsics_coeff_valid_range_lower_limit",
    "gsics_coeff_valid_range_upper_limit",
    "gsics_coeff_start_time_of_validity_period",
    "gsics_coeff_end_time_of_validity_period",
    "sc_position", "sun_position", "moon_position",
    "nav_average_residual_ew", "nav_average_residual_ns",
    "nav_measurement_type",
    "number_of_inr_performance", "number_of_ir_stars",
    "number_of_matched_lmk", "number_of_vis_stars",
    # Synthesised by add_time_and_navigation.
    "t_end",
    "gk2a_imager_projection",
    "nav_cfac", "nav_coff", "nav_lfac", "nav_loff",
    "nav_sub_longitude", "nav_satellite_height",
    "nav_earth_equatorial_radius", "nav_earth_polar_radius",
})

#: Dimensions whose length varies per file, or is zero.
_DROP_DIMS = (
    "dim_boa_swaths", "dim_matched_lmks", "dim_inr_perform",
    "dim_vis_stars", "dim_ir_stars",
)

DEFAULT_LOADABLE_VARIABLES: tuple[str, ...] = tuple(
    sorted(_GK2A_KEEP_DATA_VARS - {"image_pixel_values"})
)

# Re-exported for callers.
RadFValidationError = common.RadFValidationError
MAX_CONSECUTIVE_FAILED_DAYS = common.MAX_CONSECUTIVE_FAILED_DAYS


def _store():
    """Create an anonymous obstore handle for the GK-2A bucket."""
    return common.make_store(BUCKET)


def _band_label(band: str) -> str:
    return band.lower()


def grid_size(band: str) -> int:
    """Return the y=x pixel count for a band."""
    return _RESOLUTION_GRID[BAND_RESOLUTION[_band_label(band)]]


# =============================================================================
# Expected codec pipeline (single era observed 2023 -> 2026)
# =============================================================================
_EXPECTED_CODECS: dict[str, list[dict[str, Any]]] = {
    "image_pixel_values": [
        {"class": "BytesCodec", "endian": "little"},
        {"class": "Zlib", "codec_name": "numcodecs.zlib", "codec_config": {"level": 1}},
    ],
}


def _check_codecs(ds: xr.Dataset) -> xr.Dataset:
    return common.check_codecs_single_era(ds, _EXPECTED_CODECS)


# =============================================================================
# Filename helpers
# =============================================================================
def _filename(url: str) -> str:
    return url.rsplit("/", 1)[-1]


def band_from_filename(url: str) -> str:
    """Extract the band token from a GK-2A L1B filename."""
    return _filename(url).split("_")[3]


def parse_slot_to_datetime(url: str) -> np.datetime64:
    """Parse the trailing YYYYMMDDHHMM filename token into a datetime64[ns]."""
    token = _filename(url).rsplit("_", 1)[-1].split(".")[0]
    if len(token) != 12 or not token.isdigit():
        raise ValueError(f"Unexpected GK-2A slot token: {token!r}")
    return np.datetime64(
        datetime.datetime(
            int(token[0:4]), int(token[4:6]), int(token[6:8]),
            int(token[8:10]), int(token[10:12]),
        ),
        "ns",
    )


def _seconds_to_datetime64(seconds: float) -> np.datetime64:
    """Convert a J2000-relative second count to datetime64[ns]."""
    return GK2A_EPOCH + np.timedelta64(int(round(float(seconds) * 1e9)), "ns")


# =============================================================================
# Preprocess
# =============================================================================
def validate_observation_time(ds: xr.Dataset, t: np.datetime64) -> None:
    """Check the reconstructed `t` is sane and close to the filename slot."""
    if t < EPOCH_THRESHOLD:
        raise ValueError(
            f"`t` reconstructed as {t!s} (before {EPOCH_THRESHOLD!s}) — "
            f"observation_start_time looks like a sentinel."
        )
    source = (
        ds.encoding.get("source")
        or ds.attrs.get("file_name")
        or ""
    )
    if not source:
        return
    try:
        slot = parse_slot_to_datetime(source)
    except (ValueError, IndexError):
        return
    drift = abs(t - slot)
    if drift > _MAX_T_VS_SLOT_DRIFT:
        raise ValueError(
            f"`t` ({t!s}) drifts from the filename slot ({slot!s}) by {drift} "
            f"— file appears mis-stamped."
        )


def add_time_and_navigation(ds: xr.Dataset) -> xr.Dataset:
    """Give every variable a `t` dimension and carry navigation forward.

    GK-2A files have no time coordinate and no x/y coordinate arrays, so `t`
    is reconstructed from `observation_start_time` and the fixed-grid
    navigation parameters are promoted from global attributes to per-timestep
    variables. Keeping them per-timestep rather than as static attributes
    means INR drift stays visible in the store.
    """
    attrs = ds.attrs
    if "observation_start_time" not in attrs:
        raise ValueError("Expected an 'observation_start_time' global attribute.")

    t = _seconds_to_datetime64(attrs["observation_start_time"])
    validate_observation_time(ds, t)
    t_end = _seconds_to_datetime64(
        attrs.get("observation_end_time", attrs["observation_start_time"])
    )

    out = ds
    present = [d for d in _DROP_DIMS if d in out.dims]
    if present:
        out = out.drop_dims(present)
    rename = {
        old: new
        for old, new in (("dim_image_y", "y"), ("dim_image_x", "x"))
        if old in out.dims
    }
    if rename:
        out = out.rename(rename)

    new_data_vars: dict[str, xr.DataArray] = {}
    for name, da in out.data_vars.items():
        expanded = da.expand_dims({"t": [t]}, axis=0)
        expanded.encoding = dict(da.encoding)
        new_data_vars[name] = expanded

    def scalar(value: float | int, **var_attrs: Any) -> xr.DataArray:
        arr = xr.DataArray(np.asarray([value]), dims=("t",), attrs=var_attrs)
        arr.encoding["_FillValue"] = None
        return arr

    new_data_vars["t_end"] = xr.DataArray(
        np.asarray([t_end], dtype="datetime64[ns]"),
        dims=("t",),
        attrs={"long_name": "observation end time"},
    )

    # Fixed-grid navigation, enough to rebuild the geostationary projection.
    for var_name, attr_name in (
        ("nav_cfac", "cfac"),
        ("nav_coff", "coff"),
        ("nav_lfac", "lfac"),
        ("nav_loff", "loff"),
        ("nav_sub_longitude", "sub_longitude"),
        ("nav_satellite_height", "nominal_satellite_height"),
        ("nav_earth_equatorial_radius", "earth_equatorial_radius"),
        ("nav_earth_polar_radius", "earth_polar_radius"),
    ):
        if attr_name in attrs:
            new_data_vars[var_name] = scalar(
                float(attrs[attr_name]), source_attribute=attr_name
            )

    # CF grid mapping, mirroring the role of goes_imager_projection.
    a = float(attrs.get("earth_equatorial_radius", np.nan))
    b = float(attrs.get("earth_polar_radius", np.nan))
    h = float(attrs.get("nominal_satellite_height", np.nan)) - a
    lon_0 = float(attrs.get("sub_longitude", np.nan)) * 180.0 / np.pi
    proj = xr.DataArray(np.zeros(1, dtype="int8"), dims=("t",), attrs={
        "grid_mapping_name": "geostationary",
        "perspective_point_height": h,
        "semi_major_axis": a,
        "semi_minor_axis": b,
        "longitude_of_projection_origin": lon_0,
        "latitude_of_projection_origin": 0.0,
        "sweep_angle_axis": "y",
    })
    proj.encoding["_FillValue"] = None
    new_data_vars["gk2a_imager_projection"] = proj

    new_ds = xr.Dataset(new_data_vars, coords={"t": [t]})
    new_ds["t"].attrs.update({
        "long_name": "observation start time",
        "standard_name": "time",
    })
    new_ds.attrs.update(attrs)
    return new_ds


def _preprocess(ds: xr.Dataset, check_codecs: bool = True) -> xr.Dataset:
    try:
        if check_codecs:
            ds = _check_codecs(ds)
        else:
            ds = common.check_codecs_permissive(
                ds, expected_virtual_vars=frozenset({"image_pixel_values"})
            )
        cleaned = common.finalize_encoding(ds)
        cleaned = add_time_and_navigation(cleaned)
        to_drop = [v for v in cleaned.data_vars if v not in _GK2A_KEEP_DATA_VARS]
        if to_drop:
            cleaned = cleaned.drop_vars(to_drop)
        return cleaned
    except Exception as e:
        raise common.RadFValidationError(common._source_of(ds), e) from e


def preprocess(ds: xr.Dataset) -> xr.Dataset:
    """Strict preprocess: validates the expected codec pipeline."""
    return _preprocess(ds, check_codecs=True)


def preprocess_no_codec_check(ds: xr.Dataset) -> xr.Dataset:
    """Permissive preprocess used once an era has been probe-validated."""
    return _preprocess(ds, check_codecs=False)


# =============================================================================
# Archive listing (AMI/L1B/FD/YYYYMM/DD/HH/)
# =============================================================================
def list_months(store=None) -> list[str]:
    """Every YYYYMM directory under the FD product prefix."""
    store = store if store is not None else _store()
    return [
        c for c in common.common_prefix_children(store, PRODUCT)
        if len(c) == 6 and c.isdigit() and int(c[:4]) >= MIN_YEAR
    ]


def list_days_in_month(month: str, store=None) -> list[int]:
    """Every DD directory present in a YYYYMM month."""
    store = store if store is not None else _store()
    return sorted(
        int(c) for c in common.common_prefix_children(store, f"{PRODUCT}{month}/")
        if len(c) == 2 and c.isdigit()
    )


def iter_days(
    store=None,
    product: str = PRODUCT,
    min_year: int = MIN_YEAR,
    product_keys: list[str] | None = None,
) -> Iterator[tuple[int, int]]:
    """Yield (year, doy) for every day in the archive, chronologically.

    Signature matches `goes_radf_common.iter_days` so it can be injected as
    `list_days_fn` into the shared backwards era walk.
    """
    store = store if store is not None else _store()
    for month in list_months(store):
        year, mon = int(month[:4]), int(month[4:6])
        for dd in list_days_in_month(month, store):
            try:
                date = datetime.date(year, mon, dd)
            except ValueError:
                continue
            yield (date.year, date.timetuple().tm_yday)


def list_day_files(
    store=None,
    bucket: str = BUCKET,
    product: str = PRODUCT,
    year: int | None = None,
    doy: int | None = None,
    channel: str = "",
    *,
    product_reproc: str | None = None,
    reproc: bool = False,
) -> list[str]:
    """Every file URL for one band on one day, in slot order.

    One paginated listing of the whole day prefix covers all 24 hour
    directories, rather than 24 separate listings.

    Signature matches `goes_radf_common.list_day_files` so it can be injected
    as `list_day_files_fn`.
    """
    store = store if store is not None else _store()
    band = _band_label(channel)
    date = common._date_from_doy(year, doy)
    day_prefix = f"{product}{date.year}{date.month:02d}/{date.day:02d}/"
    stem = f"gk2a_ami_le1b_{band}_fd"

    # A couple of files a day are written with the image array uncompressed
    # while the rest use Zlib. Zarr cannot hold both codec pipelines in one
    # array, so a single such file would cost the whole day. They are exactly
    # the uncompressed size, so the object listing identifies them for free —
    # no need to open anything.
    uncompressed_bytes = grid_size(band) ** 2 * 2
    size_cutoff = int(uncompressed_bytes * _UNCOMPRESSED_SIZE_FRACTION)

    urls: list[str] = []
    oversized: list[str] = []
    for page in obs.list(store, prefix=day_prefix):
        for o in page:
            path = o["path"]
            if not path.endswith(".nc"):
                continue
            if not _filename(path).startswith(stem):
                continue
            if o["size"] >= size_cutoff:
                oversized.append(_filename(path))
                continue
            urls.append(f"{bucket}/{path}")
    if oversized:
        logger.info(
            f"{date.isoformat()} {band}: skipping {len(oversized)} "
            f"uncompressed file(s) (>= {size_cutoff / 1e6:.0f}MB): "
            f"{oversized[:3]}{'...' if len(oversized) > 3 else ''}"
        )

    # Deduplicate by slot, then order by slot.
    seen: set[str] = set()
    deduped: list[str] = []
    for u in sorted(urls, key=lambda u: _filename(u).rsplit("_", 1)[-1]):
        tok = _filename(u).rsplit("_", 1)[-1]
        if tok not in seen:
            seen.add(tok)
            deduped.append(u)
    return deduped


def days_to_ingest(
    band: str,
    *,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
) -> list[tuple[tuple[int, int], list[str]]]:
    """Eagerly enumerate (day, urls) for a band over a date range."""
    store = _store()
    out: list[tuple[tuple[int, int], list[str]]] = []
    for year, doy in iter_days(store):
        date = common._date_from_doy(year, doy)
        if start_date and date < start_date:
            continue
        if end_date and date > end_date:
            break
        out.append(
            ((year, doy), list_day_files(store, BUCKET, PRODUCT, year, doy, band))
        )
    return out


# =============================================================================
# Mixed-codec repair
# =============================================================================
def _image_codecs(url: str, registry: ObjectStoreRegistry, parser: Any):
    """Codec class names for one file's image array, or None if unreadable."""
    import virtualizarr as vz

    try:
        vds = vz.open_virtual_dataset(url, registry=registry, parser=parser)
        return tuple(
            type(c).__name__
            for c in vds["image_pixel_values"].data.metadata.codecs
        )
    except Exception:
        return None


def drop_codec_outliers(
    urls: list[str],
    registry: ObjectStoreRegistry | None = None,
    max_workers: int = 16,
) -> list[str]:
    """Keep only the files whose image codec pipeline is the day's majority.

    The archive carries the occasional file whose `image_pixel_values` is
    stored uncompressed while every other file that day uses Zlib. Zarr cannot
    hold both in one array, so a single such file would otherwise cost the
    whole day. Unreadable files are dropped too.

    Used as `batch_repair_fn`, so this only runs for batches that have already
    failed to combine.
    """
    import collections
    from concurrent.futures import ThreadPoolExecutor

    import virtualizarr as vz

    if registry is None:
        registry = ObjectStoreRegistry({BUCKET: _store()})
    parser = vz.parsers.HDFParser()

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        codecs = list(
            pool.map(lambda u: _image_codecs(u, registry, parser), urls)
        )

    counts = collections.Counter(c for c in codecs if c is not None)
    if not counts:
        return []
    majority, _ = counts.most_common(1)[0]
    return [u for u, c in zip(urls, codecs) if c == majority]


# =============================================================================
# Smoke test
# =============================================================================
def smoke_test(band: str = "ir087", n_files: int = 3) -> xr.Dataset:
    """Open the first n_files for a band through the real preprocess."""
    store = _store()
    urls: list[str] = []
    for year, doy in iter_days(store):
        urls = list_day_files(store, BUCKET, PRODUCT, year, doy, band)[:n_files]
        if urls:
            break
    if not urls:
        raise ValueError(f"No files found for GK-2A band {band!r}.")
    logger.info(f"opening {len(urls)} {band} file(s): {urls}")
    return common.open_virtual_batch(
        urls,
        registry=ObjectStoreRegistry({BUCKET: store}),
        parser=__import__("virtualizarr").parsers.HDFParser(),
        preprocess_fn=preprocess,
        loadable_variables=DEFAULT_LOADABLE_VARIABLES,
    )


# =============================================================================
# Store location
# =============================================================================
def store_prefix_for(
    band: str,
    era: str | None = None,
    base: str = DEFAULT_STORE_BASE,
) -> str:
    """Store prefix for one band, optionally for one era of the backwards walk."""
    return virtual_repo.store_prefix(base, _band_label(band), era)


def open_repo(
    band: str,
    era: str | None = None,
    *,
    base: str = DEFAULT_STORE_BASE,
    config: Config | None = None,
) -> "icechunk.Repository":
    """Open or create the store for one GK-2A band, resolved through the config."""
    return virtual_repo.open_virtual_repo(
        store_prefix_for(band, era, base),
        virtual_buckets=BUCKET,
        split_size=SLOTS_PER_DAY,
        config=config,
    )


# =============================================================================
# Ingest
# =============================================================================
def ingest_day(
    date: datetime.date,
    band: str,
    *,
    repo: icechunk.Repository | None = None,
    base: str = DEFAULT_STORE_BASE,
    config: Config | None = None,
    group: str | None = None,
    **kwargs: Any,
) -> int:
    """Ingest a single day for one band. Returns the timesteps now stored for it.

    This is the entry point the Dagster daily partition calls. Enumerating the
    day directly avoids walking the whole archive listing just to reach one day.

    The result is read back from the store rather than counted from the listing,
    because the shared engine logs and swallows a failed batch: without the read
    a partition that committed nothing would still report success.

    ``group`` is resolved once here and used for the write and for both guards.
    They have to agree: the ingest writes into ``AMI-L1B-FD/<band>``, and guards
    reading the store's root find no ``t`` coordinate there, so a successful
    ingest raises ``NothingCommitted`` and a re-run sees nothing to skip.

    Raises:
        OutOfOrderPartition: when the store already holds a newer day.
        NothingCommitted: when the ingest ran but committed nothing.
    """
    band = _band_label(band)
    if repo is None:
        repo = open_repo(band, base=base, config=config)
    if group is None:
        group = f"{PRODUCT_LABEL}/{band}"

    what = f"GK-2A {band}"
    already = virtual_repo.guard_append_order(repo, date, what, group=group)
    if already:
        logger.info(f"{what}: {date.isoformat()} already holds {already} step(s), skipping")
        return already

    urls = list_day_files(
        _store(), BUCKET, PRODUCT, date.year, date.timetuple().tm_yday, band
    )
    if not urls:
        logger.warning(f"{what}: no files for {date.isoformat()}")
        return 0

    ingest_all_days(
        repo,
        band,
        all_days=[((date.year, date.timetuple().tm_yday), urls)],
        group=group,
        **kwargs,
    )
    return virtual_repo.require_committed(repo, date, what, group=group)


def ingest_all_days(
    repo: "icechunk.Repository",
    band: str,
    *,
    branch: str = "main",
    group: str | None = None,
    registry: ObjectStoreRegistry | None = None,
    batch_size: int = 1,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
    loadable_variables: Iterable[str] = DEFAULT_LOADABLE_VARIABLES,
    resume: bool = True,
    zarr_async_concurrency: int | None = None,
    log_dir: str | None = None,
    all_days: list[tuple[tuple[int, int], list[str]]] | None = None,
    **kwargs: Any,
) -> None:
    """Ingest every selected day for a single band into repo."""
    band = _band_label(band)
    if group is None:
        group = f"{PRODUCT_LABEL}/{band}"
    if all_days is None:
        all_days = days_to_ingest(band, start_date=start_date, end_date=end_date)

    common.ingest_all_days(
        repo, band,
        satellite_name=SATELLITE_NAME,
        satellite=SATELLITE,
        product_label=PRODUCT_LABEL,
        archive_start_date=ARCHIVE_START_DATE,
        all_days=all_days,
        preprocess_fn=preprocess,
        branch=branch,
        group=group,
        registry=registry,
        bucket=BUCKET,
        batch_size=batch_size,
        loadable_variables=loadable_variables,
        resume=resume,
        zarr_async_concurrency=zarr_async_concurrency,
        log_dir=log_dir,
        keep_data_vars=_GK2A_KEEP_DATA_VARS,
        epoch_threshold=EPOCH_THRESHOLD,
        channel_label=band,
        grid_size=grid_size(band),
        batch_repair_fn=drop_codec_outliers,
        # GK-2A filenames carry no GOES `_s<token>`; the default parser raises
        # on them, which only bites on resume and silently truncates eras.
        scan_start_fn=parse_slot_to_datetime,
        **kwargs,
    )


def ingest_backwards(
    band: str,
    *,
    repo_factory: Any,
    end_date: datetime.date | None = None,
    start_date: datetime.date | None = None,
    first_store_suffix: str | None = None,
    branch: str = "main",
    group: str | None = "",
    batch_size: int = 1,
    zarr_async_concurrency: int | None = None,
    log_dir: str | None = None,
    max_eras: int | None = None,
    registry: ObjectStoreRegistry | None = None,
) -> list[str]:
    """Walk this band backwards from end_date, one store per combinable era."""
    band = _band_label(band)
    store = _store()
    return common.ingest_backwards(
        band,
        repo_factory=repo_factory,
        satellite_name=SATELLITE_NAME,
        satellite=SATELLITE,
        product_label=PRODUCT_LABEL,
        archive_start_date=ARCHIVE_START_DATE,
        store=store,
        bucket=BUCKET,
        product=PRODUCT,
        min_year=MIN_YEAR,
        product_keys=PRODUCT_KEYS,
        loadable_variables=DEFAULT_LOADABLE_VARIABLES,
        epoch_threshold=EPOCH_THRESHOLD,
        end_date=end_date,
        start_date=start_date or ARCHIVE_START_DATE,
        first_store_suffix=first_store_suffix,
        branch=branch,
        group=group,
        batch_size=batch_size,
        zarr_async_concurrency=zarr_async_concurrency,
        log_dir=log_dir,
        keep_data_vars=_GK2A_KEEP_DATA_VARS,
        registry=registry,
        max_eras=max_eras,
        channel_label=band,
        grid_size=grid_size(band),
        list_days_fn=iter_days,
        list_day_files_fn=list_day_files,
        era_preprocess_fn=preprocess_no_codec_check,
        batch_repair_fn=drop_codec_outliers,
        scan_start_fn=parse_slot_to_datetime,
    )
