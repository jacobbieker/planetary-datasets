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

#: On-disk encoding for `t` and `t_end`: exact at nanosecond resolution.
_TIME_ENCODING = {"units": "nanoseconds since 2000-01-01T12:00:00", "dtype": "int64"}

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
    # Fix the time units. Left to xarray, a store started from a single scan
    # gets "days since <t>", and every later append is then serialised in
    # finer units under those unchanged attrs, corrupting `t`. Only a store's
    # first write sets these; an append reuses whatever the store already has.
    for name in ("t", "t_end"):
        new_ds[name].encoding.update(_TIME_ENCODING)
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
    return _list_band_files(store, [day_prefix], band, bucket, date.isoformat())


def _list_band_files(
    store,
    prefixes: Iterable[str],
    band: str,
    bucket: str,
    label: str,
) -> list[str]:
    """One band's file URLs under each listed prefix, deduplicated and in slot order."""
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
    for prefix in prefixes:
        for page in obs.list(store, prefix=prefix):
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
            f"{label} {band}: skipping {len(oversized)} "
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


def _hour_prefixes(
    start: datetime.datetime, end: datetime.datetime, product: str = PRODUCT
) -> list[str]:
    """Every ``YYYYMM/DD/HH/`` directory from ``start``'s hour through ``end``'s.

    Walking hours rather than days is what lets a window cross midnight (or a
    month end) without listing either whole day.
    """
    hour = start.replace(minute=0, second=0, microsecond=0)
    out: list[str] = []
    while hour <= end:
        out.append(f"{product}{hour:%Y%m}/{hour:%d}/{hour:%H}/")
        hour += datetime.timedelta(hours=1)
    return out


def list_recent_files(
    band: str,
    since: datetime.datetime,
    until: datetime.datetime,
    store=None,
    bucket: str = BUCKET,
) -> list[str]:
    """One band's file URLs whose slot lies in ``(since, until]``, in slot order.

    Both bounds are naive UTC. Only the hour directories the window touches
    are listed, so a 30-minute append lists one or two prefixes, not a day.
    """
    if since >= until:
        return []
    store = store if store is not None else _store()
    band = _band_label(band)
    urls = _list_band_files(
        store,
        _hour_prefixes(since, until),
        band,
        bucket,
        f"{since:%Y-%m-%dT%H:%M}..{until:%Y-%m-%dT%H:%M}",
    )
    lo, hi = np.datetime64(since, "ns"), np.datetime64(until, "ns")
    return [u for u in urls if lo < parse_slot_to_datetime(u) <= hi]


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
    create: bool = True,
) -> "icechunk.Repository":
    """Open or create the store for one GK-2A band, resolved through the config.

    ``create=False`` opens an existing store only, raising when there is none.
    """
    return virtual_repo.open_virtual_repo(
        store_prefix_for(band, era, base),
        virtual_buckets=BUCKET,
        split_size=SLOTS_PER_DAY,
        config=config,
        create=create,
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


# =============================================================================
# Live append
# =============================================================================
#: Default window an append looks back over. Long enough to ride out a missed
#: run or two of a 30-minute schedule, short enough to list in a few requests.
DEFAULT_APPEND_LOOKBACK_MINUTES = 180

#: Attempts per band when a commit loses a race with another writer.
APPEND_MAX_ATTEMPTS = 3


def _utcnow() -> datetime.datetime:
    """The current time as naive UTC, matching the archive's slot tokens."""
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)


def last_committed_time(
    repo: "icechunk.Repository",
    *,
    branch: str = "main",
    group: str | None = "",
) -> np.datetime64 | None:
    """The newest `t` committed to ``group``, or None for an empty store.

    Refuses a store whose `t` is not strictly increasing, as the resume does:
    appending after max(t) would otherwise hide whatever is out of order.
    """
    if not common._schema_exists(repo, branch, group):
        return None
    info = common._last_committed_day(repo, branch, group)
    return None if info is None else info[1]


def check_time_units(
    repo: "icechunk.Repository", *, branch: str = "main", group: str | None = ""
) -> None:
    """Refuse a store whose `t` units cannot hold a scan start exactly.

    An append reuses the store's units. `t` carries nanoseconds, so anything
    coarser has xarray serialise the new values in other units under the old
    attrs, which silently corrupts `t` — the fate of a store started from a
    single scan before the preprocess pinned its units.
    """
    import zarr

    arr = zarr.open_array(
        store=repo.readonly_session(branch=branch).store,
        path=f"{group}/t" if group else "t",
        mode="r",
    )
    units = str(arr.attrs.get("units", ""))
    if not units.startswith("nanoseconds since"):
        raise ValueError(
            f"the store's `t` is encoded as {units!r}; appending would corrupt it. "
            "Only stores with nanosecond `t` units can be appended to."
        )


class StaleSession(RuntimeError):
    """The branch moved between reading the store's last `t` and writing."""


class _ConflictWatch:
    """Wraps a repository so a lost commit race is noticed, and so is a won one.

    The shared engine logs and swallows a failed commit, so this is how the
    append tells a conflict, which is worth retrying, from anything else.

    It also refuses a session that does not start from ``expected_snapshot``,
    the snapshot the store's last `t` was read from. Another writer committing
    in between would otherwise go unnoticed: the session would start from
    their snapshot, the commit would not conflict, and the same scans would be
    appended twice.
    """

    def __init__(self, repo: "icechunk.Repository", expected_snapshot: str):
        self._repo = repo
        self._expected_snapshot = expected_snapshot
        self.conflicted = False
        #: The snapshot id of a successful commit, if there was one.
        self.committed: str | None = None

    def __getattr__(self, name: str) -> Any:
        return getattr(self._repo, name)

    def writable_session(self, *args: Any, **kwargs: Any) -> "_SessionWatch":
        session = self._repo.writable_session(*args, **kwargs)
        if session.snapshot_id != self._expected_snapshot:
            self.conflicted = True
            raise StaleSession(
                f"branch moved from {self._expected_snapshot} to "
                f"{session.snapshot_id} since the store's last `t` was read"
            )
        return _SessionWatch(session, self)


class _SessionWatch:
    def __init__(self, session: Any, owner: _ConflictWatch):
        self._session = session
        self._owner = owner

    def __getattr__(self, name: str) -> Any:
        return getattr(self._session, name)

    def commit(self, *args: Any, **kwargs: Any) -> Any:
        import icechunk

        try:
            out = self._session.commit(*args, **kwargs)
        except (icechunk.ConflictError, icechunk.RebaseFailedError):
            self._owner.conflicted = True
            raise
        self._owner.committed = out
        return out


def _times_in_snapshot(
    repo: "icechunk.Repository", snapshot_id: str, group: str | None
) -> np.ndarray:
    """The `t` values stored in ``group`` as of one snapshot."""
    ds = xr.open_zarr(
        repo.readonly_session(snapshot_id=snapshot_id).store,
        group=group or None,
        consolidated=False,
        decode_timedelta=True,
    )
    return ds["t"].values


class NothingNewer(RuntimeError):
    """Every candidate scan's `t` turned out to be at or before the store's last."""


class _NewerThanOpener:
    """A batch opener that keeps only scans whose `t` is strictly after ``last``.

    The listing is filtered by slot (``slot > last``), which is right for the
    usual few-seconds offset of `t` from its slot. But the preprocess accepts
    `t` up to ``_MAX_T_VS_SLOT_DRIFT`` either side of the slot, so the slot
    filter can let through a scan the store already holds. When the combined
    batch does not start after ``last``, each file is opened on its own and
    those that do not advance `t` are dropped, so a mis-stamped file neither
    duplicates a timestep nor fails the scans queued behind it.
    """

    def __init__(self, last: np.datetime64 | None):
        self.last = last
        self.nothing_newer = False

    def __call__(self, urls: list[str], **kwargs: Any) -> xr.Dataset:
        vds = common.open_virtual_batch(urls, **kwargs)
        if self.last is None or _first_t(vds) > self.last:
            return vds
        keep = [u for u in urls if _first_t(common.open_virtual_batch([u], **kwargs)) > self.last]
        logger.warning(
            f"{len(urls) - len(keep)} file(s) do not advance `t` past {self.last}; "
            f"dropped: {[_filename(u) for u in urls if u not in keep]}"
        )
        if not keep:
            self.nothing_newer = True
            raise NothingNewer(f"no scan's `t` is after the store's last ({self.last})")
        return common.open_virtual_batch(keep, **kwargs)


def _first_t(vds: xr.Dataset) -> np.datetime64:
    return np.datetime64(vds.indexes["t"].min(), "ns")


def _group_by_day(urls: list[str]) -> list[tuple[tuple[int, int], list[str]]]:
    """Slot-ordered URLs grouped into the engine's ((year, doy), urls) days."""
    days: dict[tuple[int, int], list[str]] = {}
    for u in urls:
        date = parse_slot_to_datetime(u).astype("datetime64[D]").item()
        days.setdefault((date.year, date.timetuple().tm_yday), []).append(u)
    return sorted(days.items())


def _read_new_errors(log_path: Any, offset: int) -> str | None:
    """The newest ERROR/STOPPED reason the engine logged past ``offset``."""
    try:
        with open(log_path) as f:
            f.seek(offset)
            lines = f.read().splitlines()
    except OSError:
        return None
    reasons = []
    for line in lines:
        fields = line.split(" | ", 3)
        if len(fields) == 4 and fields[2] in ("ERROR", "STOPPED"):
            reasons.append(fields[3])
    return reasons[-1] if reasons else None


def append_latest(
    band: str,
    *,
    lookback_minutes: int = DEFAULT_APPEND_LOOKBACK_MINUTES,
    now: datetime.datetime | None = None,
    repo: icechunk.Repository | None = None,
    base: str = DEFAULT_STORE_BASE,
    config: Config | None = None,
    group: str | None = "",
    branch: str = "main",
    create: bool = False,
    max_attempts: int = APPEND_MAX_ATTEMPTS,
    log_dir: str | None = None,
    store=None,
    **kwargs: Any,
) -> dict[str, Any]:
    """Append every scan newer than the live store's last `t`, within the lookback.

    The sub-day counterpart to :func:`ingest_day`. A whole day is no use here:
    ``guard_append_order`` skips any day the store already has data for. This
    instead lists only the hour directories between the store's newest `t`
    (bounded by ``lookback_minutes`` before ``now``) and ``now``, crossing day
    boundaries, and appends the strictly newer scans in one commit. The strict
    codec preprocess, the uncompressed-file filter and the codec-outlier repair
    all still apply, so a codec change fails the band rather than mixing two
    eras in one store.

    A commit that loses a race with another writer is retried from a fresh
    session, re-reading the store's last `t` first.

    ``group`` defaults to the root, which is where the CLI and the Dagster
    assets write the live stores. ``create`` allows a missing store to be
    started; by default a missing store is an error, since seeding the live
    store with a few hours of data would block the backfill behind it.

    Returns:
        ``{"appended": n, "last": iso-or-None}``, where ``last`` is the
        store's newest `t` after the append.

    Raises:
        NothingCommitted: when there were newer scans but none could be
            committed, carrying the engine's logged reason where there is one.
    """
    import tempfile

    from planetary_datasets.common.paths import safe_component, safe_join

    band = _band_label(band)
    if band not in BAND_RESOLUTION:
        raise ValueError(f"Unknown GK-2A band {band!r}; expected one of {BANDS}.")
    if lookback_minutes <= 0:
        raise ValueError(f"lookback_minutes must be positive, got {lookback_minutes}")
    now = now if now is not None else _utcnow()
    floor = now - datetime.timedelta(minutes=lookback_minutes)
    what = f"GK-2A {band}"
    store = store if store is not None else _store()

    # The engine reports a failed batch only through its event log, so one is
    # always kept, in a scratch directory if the caller did not ask for one.
    tmp = tempfile.TemporaryDirectory() if log_dir is None else None
    log_dir = tmp.name if tmp is not None else log_dir
    log_path = safe_join(
        log_dir, f"{safe_component(SATELLITE)}_{safe_component(band)}_ingest.log"
    )

    try:
        for attempt in range(1, max(1, max_attempts) + 1):
            live = repo if repo is not None else open_repo(
                band, base=base, config=config, create=create
            )
            # Read before `last`: if the branch moves in between, the session
            # check sees a mismatch and retries, rather than the reverse.
            base_snapshot = live.lookup_branch(branch)
            last = last_committed_time(live, branch=branch, group=group)
            last_iso = None if last is None else str(last.astype("datetime64[s]"))
            since = floor
            extra: dict[str, Any] = {}
            if last is not None:
                last_dt = last.astype("datetime64[us]").item()
                since = max(floor, last_dt)
                if last_dt < floor:
                    # The lookback bounds the work, but the store only appends
                    # along `t`: scans between its last `t` and the window are
                    # skipped for good once newer ones land. Say so loudly.
                    extra["gap"] = {"from": last_iso, "to": f"{floor:%Y-%m-%dT%H:%M:%S}"}
                    logger.warning(
                        f"{what}: the store's last `t` ({last_iso}) is older than the "
                        f"{lookback_minutes}-minute lookback; scans from then until "
                        f"{floor:%Y-%m-%dT%H:%M} will not be appended"
                    )
            urls = list_recent_files(band, since, now, store=store)
            if last is not None:
                # Slot after `t`, so the scan already stored (whose `t` sits
                # just after its own slot) is left out; the opener catches a
                # file whose `t` drifted the other way.
                urls = [u for u in urls if parse_slot_to_datetime(u) > last]
            if not urls:
                logger.info(
                    f"{what}: nothing newer than {last_iso} "
                    f"since {floor:%Y-%m-%dT%H:%M}"
                )
                return {"appended": 0, "last": last_iso, **extra}

            if last is not None:
                check_time_units(live, branch=branch, group=group)
            all_days = _group_by_day(urls)
            logger.info(
                f"{what}: appending {len(urls)} scan(s) after {last_iso} "
                f"({_filename(urls[0])} .. {_filename(urls[-1])})"
                + (f", attempt {attempt}/{max_attempts}" if attempt > 1 else "")
            )
            try:
                offset = log_path.stat().st_size
            except OSError:
                offset = 0

            watched = _ConflictWatch(live, base_snapshot)
            opener = _NewerThanOpener(last)
            ingest_all_days(
                watched,
                band,
                all_days=all_days,
                branch=branch,
                group=group,
                # One commit for the whole window, even across midnight.
                batch_size=len(all_days),
                # The engine's resume skips whole days at or before the store's
                # last one, which is every sub-day append. The filtering is
                # done above instead, and re-checked by the opener.
                resume=False,
                open_batch_fn=opener,
                log_dir=log_dir,
                **kwargs,
            )

            if watched.committed is not None:
                # Counted in the snapshot this append committed, not at the
                # branch tip, which may already hold another writer's scans.
                times = _times_in_snapshot(live, watched.committed, group)
                appended = int(times.size if last is None else (times > last).sum())
                newest = str(times.max().astype("datetime64[s]")) if times.size else last_iso
                logger.info(f"{what}: appended {appended} scan(s), last {newest}")
                return {"appended": appended, "last": newest, **extra}
            if opener.nothing_newer:
                logger.info(f"{what}: no candidate's `t` is after {last_iso}")
                return {"appended": 0, "last": last_iso, **extra}
            if watched.conflicted and attempt < max_attempts:
                logger.warning(
                    f"{what}: commit conflicted with another writer; retrying "
                    f"from a fresh session ({attempt}/{max_attempts})"
                )
                continue
            reason = _read_new_errors(log_path, offset)
            raise virtual_repo.NothingCommitted(
                f"{what}: {len(urls)} newer scan(s) found but none committed"
                + (" (commit conflicted on every attempt)" if watched.conflicted else "")
                + (f": {reason}" if reason else "")
            )
        raise AssertionError("unreachable")  # pragma: no cover
    finally:
        if tmp is not None:
            tmp.cleanup()
