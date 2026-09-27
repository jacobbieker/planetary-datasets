"""Virtual ingest of GOES-17 ABI-L2-MCMIPF into Icechunk.

Adapted from the GOES-16 ingest notebook by the VirtualiZarr project:
  https://github.com/zarr-developers/VirtualiZarr/blob/main/examples/V2/goes-16-ingest.ipynb

Key differences from GOES-16:
  - Bucket: s3://noaa-goes17 (anonymous, us-east-1)
  - Satellite ID in filenames: G17 (not G16)
  - Operational period: 2018-02-12 through 2023-01-10 (decommissioned)
  - GOES-17 had a known ABI loop heat pipe anomaly affecting IR channels;
    the same product schema applies, but some channels may have degraded
    data quality during hot seasons.
  - Only two eras (no post-2023-04-19 codec change, since GOES-17 was
    decommissioned before that transition):
      pre-operational:  2018-02-12 through ~2018-06-xx (placeholder calibration)
      operational:      ~2018-06-xx through 2023-01-10

Usage:
    from planetary_datasets.providers.virtualized.goes_17 import (
        ingest_all_days,
        smoke_test,
    )

    # Quick test
    vds = smoke_test(n_files=3)

    # Full ingest
    import icechunk as ic
    storage = ic.s3_storage(...)
    repo = ic.Repository.open_or_create(storage)
    ingest_all_days(repo, branch="main", group="ABI-L2-MCMIPF")
"""

from __future__ import annotations

import contextlib
import datetime
import gc
import time
import warnings
from collections.abc import Iterable, Iterator
from typing import Any, Literal, TYPE_CHECKING

import numpy as np
import obstore as obs
import pandas as pd
import xarray as xr
import virtualizarr as vz
from obspec_utils.registry import ObjectStoreRegistry
from virtualizarr.manifests import ChunkManifest, ManifestArray
from zarr.codecs import BytesCodec, Zlib
from zarr.core.chunk_key_encodings import DefaultChunkKeyEncoding
from zarr.core.dtype import Int8, Int16
from zarr.core.metadata.v3 import ArrayV3Metadata

from planetary_datasets.providers.virtualized.goes_radf_common import source_bucket

try:  # zarr >= 3.2
    from zarr.core.metadata.v3 import RegularChunkGridMetadata as _ChunkGrid
except ImportError:  # zarr <= 3.1
    from zarr.core.chunk_grids import RegularChunkGrid as _ChunkGrid

if TYPE_CHECKING:
    import icechunk


# =============================================================================
# GOES-17 constants
# =============================================================================
SATELLITE = "goes17"
#: Public archive bucket, resolved through the shared config so a mirror can be
#: substituted with GOES17_SOURCE_BUCKET.
BUCKET = source_bucket(SATELLITE)
PRODUCT_SHORT = "mcmipf"
PRODUCT = "ABI-L2-MCMIPF/"

# GOES-17 data starts in 2018; anything earlier is a sentinel/test directory.
MIN_YEAR = 2018

# GOES-17 was decommissioned 2023-01-10. All data uses the pre-Shuffle codec
# pipeline (BytesCodec + Zlib, no Shuffle filter) since the codec change on
# GOES-16 happened 2023-04-19, after GOES-17 was already offline.
# Therefore GOES-17 has a single codec era ("pre").
#
# GOES-17 went through similar calibration transitions as GOES-16.
# The operational calibration values are the same ABI instrument calibration
# (the ABI instruments are identical across GOES-R series satellites).
# For simplicity we treat GOES-17 as a single operational era starting from
# the earliest available data, since the brief pre-op period is very short.
ARCHIVE_START_DATE = datetime.date(2018, 2, 12)
ARCHIVE_END_DATE = datetime.date(2023, 1, 10)


def _store():
    """Create an anonymous obstore handle for the GOES-17 bucket."""
    return obs.store.from_url(BUCKET, region="us-east-1", skip_signature=True)


# =============================================================================
# Grid / schema constants (same ABI instrument as GOES-16)
# =============================================================================
X_SIZE = 5424
Y_SIZE = 5424
N_BANDS = 16
ALL_CHANNELS = range(1, 17)
REFLECTIVE_CHANNELS = range(1, 7)   # C01-C06
EMISSIVE_CHANNELS = range(7, 17)    # C07-C16

DATETIME_NS = np.dtype("datetime64[ns]")

MAX_CONSECUTIVE_FAILED_DAYS = 14


def _ch(i: int) -> str:
    return f"C{i:02d}"


# Operational calibration — same ABI instrument across GOES-R series.
OPERATIONAL_CALIBRATION: dict[str, dict[str, np.float32]] = {
    "CMI_C01": {"scale_factor": np.float32(0.00031746001332066953), "add_offset": np.float32(0.0)},
    "CMI_C02": {"scale_factor": np.float32(0.00031746001332066953), "add_offset": np.float32(0.0)},
    "CMI_C03": {"scale_factor": np.float32(0.00031746001332066953), "add_offset": np.float32(0.0)},
    "CMI_C04": {"scale_factor": np.float32(0.00031746001332066953), "add_offset": np.float32(0.0)},
    "CMI_C05": {"scale_factor": np.float32(0.00031746001332066953), "add_offset": np.float32(0.0)},
    "CMI_C06": {"scale_factor": np.float32(0.00031746001332066953), "add_offset": np.float32(0.0)},
    "CMI_C07": {"scale_factor": np.float32(0.013096179813146591),   "add_offset": np.float32(197.30999755859375)},
    "CMI_C08": {"scale_factor": np.float32(0.042249858379364014),   "add_offset": np.float32(138.0500030517578)},
    "CMI_C09": {"scale_factor": np.float32(0.042339108884334564),   "add_offset": np.float32(137.6999969482422)},
    "CMI_C10": {"scale_factor": np.float32(0.04988918825984001),    "add_offset": np.float32(126.91000366210938)},
    "CMI_C11": {"scale_factor": np.float32(0.05216431990265846),    "add_offset": np.float32(127.69000244140625)},
    "CMI_C12": {"scale_factor": np.float32(0.047270338982343674),   "add_offset": np.float32(117.48999786376953)},
    "CMI_C13": {"scale_factor": np.float32(0.06145332008600235),    "add_offset": np.float32(89.62000274658203)},
    "CMI_C14": {"scale_factor": np.float32(0.059850748628377914),   "add_offset": np.float32(96.19000244140625)},
    "CMI_C15": {"scale_factor": np.float32(0.05956082046031952),    "add_offset": np.float32(97.37999725341797)},
    "CMI_C16": {"scale_factor": np.float32(0.055081531405448914),   "add_offset": np.float32(92.69999694824219)},
}


# Recommended loadable_variables — every low-dimensional variable.
DEFAULT_LOADABLE_VARIABLES: tuple[str, ...] = (
    "x", "y", "t", "band",
    "x_image", "y_image",
    "x_image_bounds", "y_image_bounds", "time_bounds",
    "goes_imager_projection",
    "nominal_satellite_height",
    "nominal_satellite_subpoint_lat",
    "nominal_satellite_subpoint_lon",
    "geospatial_lat_lon_extent",
    "percent_uncorrectable_GRB_errors",
    "percent_uncorrectable_L0_errors",
    "algorithm_product_version_container",
    "dynamic_algorithm_input_data_container",
    *(f"band_id_{_ch(i)}"             for i in ALL_CHANNELS),
    *(f"band_wavelength_{_ch(i)}"     for i in ALL_CHANNELS),
    *(f"outlier_pixel_count_{_ch(i)}" for i in ALL_CHANNELS),
    *(f"{s}_reflectance_factor_{_ch(i)}"
      for s in ("min", "max", "mean", "std_dev")
      for i in REFLECTIVE_CHANNELS),
    *(f"{s}_brightness_temperature_{_ch(i)}"
      for s in ("min", "max", "mean", "std_dev")
      for i in EMISSIVE_CHANNELS),
)

_PER_CHANNEL_2D = ("CMI_", "DQF_")
_PER_CHANNEL_BAND_LOADED = ("band_id_", "band_wavelength_")
_PER_CHANNEL_SCALAR_LOADED = (
    "outlier_pixel_count_",
    "min_reflectance_factor_", "max_reflectance_factor_",
    "mean_reflectance_factor_", "std_dev_reflectance_factor_",
    "min_brightness_temperature_", "max_brightness_temperature_",
    "mean_brightness_temperature_", "std_dev_brightness_temperature_",
)

_EXPECTED_SHARED_VARS: tuple[tuple[str, Any, tuple[str, ...]], ...] = (
    ("algorithm_product_version_container",    np.int32,    ()),
    ("dynamic_algorithm_input_data_container", np.int32,    ()),
    ("geospatial_lat_lon_extent",              np.float32,  ()),
    ("goes_imager_projection",                 np.int32,    ()),
    ("nominal_satellite_height",               np.float32,  ()),
    ("nominal_satellite_subpoint_lat",         np.float32,  ()),
    ("nominal_satellite_subpoint_lon",         np.float32,  ()),
    ("percent_uncorrectable_GRB_errors",       np.float32,  ()),
    ("percent_uncorrectable_L0_errors",        np.float32,  ()),
    ("time_bounds",    DATETIME_NS, ("number_of_time_bounds",)),
    ("x_image_bounds", np.float32,  ("number_of_image_bounds",)),
    ("y_image_bounds", np.float32,  ("number_of_image_bounds",)),
)

_SHARED_DIM_SIZES: dict[str, int] = {
    "number_of_time_bounds": 2,
    "number_of_image_bounds": 2,
}

_CANONICAL_VAR_ATTRS: dict[str, dict[str, Any]] = {
    "x_image_bounds": {"units": "rad"},
    "y_image_bounds": {"units": "rad"},
}

_CANONICAL_VAR_ATTR_OVERRIDES: dict[str, dict[str, Any]] = {
    f"DQF_C{i:02d}": {"valid_range": np.array([0, 4], dtype=np.int8)}
    for i in ALL_CHANNELS
}

EXPECTED_FILL_VALUES: dict[str, Any] = {
    "CMI_": np.int16(-1),
    "DQF_": np.int8(-1),
}


# =============================================================================
# Zarr metadata for fill-only ManifestArrays
# =============================================================================
# GOES-17 was decommissioned before the Shuffle codec change, so all files use
# the pre-Shuffle codec pipeline (BytesCodec + Zlib, no Shuffle).
_MCMIPF_2D_CHUNKS = (226, 226)
_MCMIPF_2D_GRID = (Y_SIZE // _MCMIPF_2D_CHUNKS[0], X_SIZE // _MCMIPF_2D_CHUNKS[1])


def _make_2d_metadata(
    *,
    data_type: Any,
    fill_value: Any,
    codecs: tuple,
) -> ArrayV3Metadata:
    return ArrayV3Metadata(
        shape=(Y_SIZE, X_SIZE),
        data_type=data_type,
        chunk_grid=_ChunkGrid(chunk_shape=_MCMIPF_2D_CHUNKS),
        chunk_key_encoding=DefaultChunkKeyEncoding(separator="/"),
        fill_value=fill_value,
        codecs=codecs,
        attributes={},
        dimension_names=None,
        storage_transformers=(),
    )


CMI_METADATA: ArrayV3Metadata = _make_2d_metadata(
    data_type=Int16(endianness="little"),
    fill_value=np.int16(-1),
    codecs=(BytesCodec(endian="little"), Zlib(level=1)),
)

DQF_METADATA: ArrayV3Metadata = _make_2d_metadata(
    data_type=Int8(),
    fill_value=np.int8(-1),
    codecs=(BytesCodec(endian=None), Zlib(level=1)),
)

_PER_CHANNEL_2D_METADATA: dict[str, ArrayV3Metadata] = {
    "CMI_": CMI_METADATA,
    "DQF_": DQF_METADATA,
}


def _make_fill_manifest_array(metadata: ArrayV3Metadata) -> ManifestArray:
    """Construct a fill-only ManifestArray from canonical metadata."""
    paths = np.full(_MCMIPF_2D_GRID, "", dtype=np.dtypes.StringDType())
    offsets = np.zeros(_MCMIPF_2D_GRID, dtype=np.uint64)
    lengths = np.zeros(_MCMIPF_2D_GRID, dtype=np.uint64)
    manifest = ChunkManifest.from_arrays(paths=paths, offsets=offsets, lengths=lengths)
    return ManifestArray(metadata=metadata, chunkmanifest=manifest)


# Expected codec pipeline — GOES-17 only has the pre-Shuffle variant.
_EXPECTED_CODECS_BY_PREFIX: dict[str, list[dict[str, Any]]] = {
    "CMI_C": [
        {"class": "BytesCodec", "endian": "little"},
        {"class": "Zlib", "codec_name": "numcodecs.zlib",
         "codec_config": {"level": 1}},
    ],
    "DQF_C": [
        {"class": "BytesCodec", "endian": None},
        {"class": "Zlib", "codec_name": "numcodecs.zlib",
         "codec_config": {"level": 1}},
    ],
}


# =============================================================================
# Archive listing helpers
# =============================================================================
def _parse_url_to_day(url: str) -> tuple[int, int]:
    """Extract (year, doy) from a GOES file URL or key."""
    parts = url.split("/")
    try:
        idx = parts.index("ABI-L2-MCMIPF")
    except ValueError as e:
        raise ValueError(f"URL doesn't contain ABI-L2-MCMIPF: {url}") from e
    if len(parts) < idx + 3:
        raise ValueError(f"URL doesn't have year/doy: {url}")
    return int(parts[idx + 1]), int(parts[idx + 2])


def _common_prefix_children(prefix: str) -> list[str]:
    store = _store()
    result = obs.list_with_delimiter(store, prefix=prefix)
    return sorted(
        p[len(prefix):].rstrip("/") for p in result["common_prefixes"]
    )


def list_years() -> list[int]:
    """Every year directory present under the product prefix, filtered to >= MIN_YEAR."""
    out: list[int] = []
    for c in _common_prefix_children(PRODUCT):
        c = c.strip("/")
        if len(c) == 4 and c.isdigit() and int(c) >= MIN_YEAR:
            out.append(int(c))
    return sorted(out)


def list_days_in_year(year: int) -> list[int]:
    """Every DOY directory present in a given year."""
    out: list[int] = []
    for c in _common_prefix_children(f"{PRODUCT}{year}/"):
        c = c.strip("/")
        if len(c) == 3 and c.isdigit():
            out.append(int(c))
    return sorted(out)


def iter_days(
    *,
    start: str | None = None,
    end: str | None = None,
) -> Iterator[tuple[int, int]]:
    """Yield (year, doy) for every day in the archive, in chronological order."""
    start_day = _parse_url_to_day(start) if start else None
    end_day = _parse_url_to_day(end) if end else None

    for year in list_years():
        if start_day and year < start_day[0]:
            continue
        if end_day and year > end_day[0]:
            return
        for doy in list_days_in_year(year):
            current = (year, doy)
            if start_day and current < start_day:
                continue
            if end_day and current > end_day:
                return
            yield current


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
        datetime.datetime(date.year, date.month, date.day, hour, minute, sec, tenths * 100_000),
        "ns",
    )


def list_day_files(
    year: int,
    doy: int,
    *,
    start: str | None = None,
    end: str | None = None,
) -> list[str]:
    """Every .nc file URL in a single day, in chronological scan-time order.

    Filters out aborted scans (identical scan-start and scan-end tokens).
    """
    store = _store()
    day_prefix = f"{PRODUCT}{year}/{doy:03d}/"
    urls: list[str] = []
    for page in obs.list(store, prefix=day_prefix):
        for obj in page:
            path = obj["path"]
            if path.endswith(".nc") and not _is_aborted_scan(path):
                urls.append(f"{BUCKET}/{path}")
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
    *,
    start: str | None = None,
    end: str | None = None,
) -> Iterator[str]:
    """Flat iterator over every file URL across the archive."""
    for year, doy in iter_days(start=start, end=end):
        yield from list_day_files(year, doy, start=start, end=end)


def iter_archive_by_day(
    *,
    start: str | None = None,
    end: str | None = None,
) -> Iterator[tuple[tuple[int, int], list[str]]]:
    """Per-day iterator: yield ((year, doy), [urls]) for each day."""
    for year, doy in iter_days(start=start, end=end):
        yield (year, doy), list_day_files(year, doy, start=start, end=end)


# =============================================================================
# Calibration validation
# =============================================================================
def check_calibration(ds: xr.Dataset) -> None:
    """Validate that every present CMI_C{NN}'s scale_factor / add_offset
    matches the operational calibration table.

    GOES-17 used the same ABI instrument as GOES-16, so the operational
    calibration values are identical. We skip calibration-era routing since
    the pre-operational period is very short and we treat all data uniformly.
    """
    mismatches: list[tuple[str, str, Any, Any]] = []
    for i in range(1, 17):
        name = f"CMI_C{i:02d}"
        if name not in ds.variables:
            continue
        attrs = ds[name].attrs
        expected = OPERATIONAL_CALIBRATION[name]
        for key in ("scale_factor", "add_offset"):
            actual = attrs.get(key)
            expected_val = expected[key]
            if actual is None:
                mismatches.append((name, key, "MISSING", expected_val))
                continue
            if not np.isclose(actual, expected_val):
                mismatches.append((name, key, actual, expected_val))

    if mismatches:
        lines = [
            f"CMI calibration mismatch ({len(mismatches)} issue(s)):",
        ]
        for name, key, actual, expected_val in mismatches:
            lines.append(f"  {name}.attrs[{key!r}] = {actual!r}, expected {expected_val!r}")
        raise ValueError("\n".join(lines))


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


def _check_codecs(ds: xr.Dataset) -> None:
    """Raise ValueError listing every codec mismatch / unexpected virtual var."""
    errors: list[str] = []
    for name, da in {**ds.data_vars, **ds.coords}.items():
        if not isinstance(da.data, ManifestArray):
            continue
        expected = None
        for prefix, exp in _EXPECTED_CODECS_BY_PREFIX.items():
            if name.startswith(prefix):
                expected = exp
                break
        if expected is None:
            errors.append(
                f"{name}: unexpectedly virtual — only CMI_C* and DQF_C* are "
                f"supposed to remain as ManifestArray"
            )
            continue
        actual = list(da.data.metadata.codecs)
        if len(actual) != len(expected):
            errors.append(
                f"{name}: expected {len(expected)} codecs, got {len(actual)}: {actual!r}"
            )
            continue
        for i, (act, exp) in enumerate(zip(actual, expected)):
            if not _codec_matches(act, exp):
                errors.append(
                    f"{name}: codec[{i}] mismatch — expected {exp}, got {act!r}"
                )
    if errors:
        raise ValueError("Codec validation failed:\n  " + "\n  ".join(errors))


# =============================================================================
# Preprocessing pipeline (Dataset -> Dataset)
# =============================================================================
class MCMIPFValidationError(Exception):
    """Pipeline failure annotated with the source filename."""

    def __init__(self, source: str, original: Exception):
        self.source = source
        self.original = original
        super().__init__(f"\nPipeline failed for {source}:\n\n{original}")


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
        datetime.datetime(date.year, date.month, date.day, hour, minute, sec, tenths * 100_000),
        "ns",
    )


_MAX_T_VS_FILENAME_DRIFT = np.timedelta64(1, "h")


def _first_present(ds: xr.Dataset, prefix: str) -> str | None:
    for i in ALL_CHANNELS:
        name = f"{prefix}{_ch(i)}"
        if name in ds.variables:
            return name
    return None


def _fill_for_dtype(dtype: np.dtype) -> Any:
    if np.issubdtype(dtype, np.floating):
        return np.nan
    return 0


def validate_raw(ds: xr.Dataset) -> xr.Dataset:
    """Validate the raw file structure, codecs, calibration, and t-value sanity.
    Returns ds unchanged.
    """
    _check_codecs(ds)
    check_calibration(ds)

    t = ds["t"].values
    epoch_threshold = np.datetime64("2018-01-01", "ns")
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
    return ds


def fill_missing_channels(ds: xr.Dataset) -> xr.Dataset:
    """Back-fill every missing per-channel variable."""
    out = ds

    for prefix in _PER_CHANNEL_2D:
        metadata = _PER_CHANNEL_2D_METADATA[prefix]
        for i in ALL_CHANNELS:
            name = f"{prefix}{_ch(i)}"
            if name in out.variables:
                continue
            out = out.assign({
                name: xr.DataArray(
                    _make_fill_manifest_array(metadata),
                    dims=("y", "x"),
                    attrs={},
                )
            })

    for prefix in _PER_CHANNEL_BAND_LOADED:
        template_name = _first_present(out, prefix)
        if template_name is None:
            continue
        template_da = out[template_name]
        fill = _fill_for_dtype(template_da.dtype)
        for i in ALL_CHANNELS:
            name = f"{prefix}{_ch(i)}"
            if name in out.variables:
                continue
            out = out.assign({name: xr.full_like(template_da, fill)})

    for prefix in _PER_CHANNEL_SCALAR_LOADED:
        template_name = _first_present(out, prefix)
        if template_name is None:
            continue
        template_da = out[template_name]
        fill = _fill_for_dtype(template_da.dtype)
        for i in ALL_CHANNELS:
            name = f"{prefix}{_ch(i)}"
            if name in out.variables:
                continue
            out = out.assign({name: xr.full_like(template_da, fill)})

    return out


def fill_missing_scalar_metadata(ds: xr.Dataset) -> xr.Dataset:
    """Synthesize placeholders for any expected scalar/1-D metadata variable
    that's missing from this source file.
    """
    out = ds
    for name, dtype, dims in _EXPECTED_SHARED_VARS:
        if name in out.variables:
            continue
        shape = tuple(
            out.sizes[d] if d in out.sizes else _SHARED_DIM_SIZES[d]
            for d in dims
        )
        np_dtype = np.dtype(dtype)
        if np.issubdtype(np_dtype, np.floating):
            fill_val: Any = np.nan
        elif np.issubdtype(np_dtype, np.datetime64):
            fill_val = np.datetime64("NaT")
        else:
            fill_val = 0
        arr = np.full(shape, fill_val, dtype=np_dtype)
        out = out.assign({name: xr.DataArray(arr, dims=dims)})
    return out


def finalize_per_channel(ds: xr.Dataset) -> xr.Dataset:
    """Per-channel encoding fixups."""
    out = ds

    for i in ALL_CHANNELS:
        name = f"outlier_pixel_count_{_ch(i)}"
        if name in out.variables:
            out[name].encoding["dtype"] = np.int32
            out[name].encoding["_FillValue"] = np.int32(-1)

    for var in (*out.variables.values(),):
        if "_FillValue" in var.encoding:
            continue
        encoded_dtype = np.dtype(var.encoding.get("dtype", var.dtype))
        if np.issubdtype(encoded_dtype, np.floating):
            var.encoding["_FillValue"] = None

    for var in (*out.variables.values(),):
        for key in ("scale_factor", "add_offset"):
            if key in var.attrs:
                var.attrs[key] = np.float32(var.attrs[key])
            if key in var.encoding:
                var.encoding[key] = np.float32(var.encoding[key])

    for var_name, canonical_attrs in _CANONICAL_VAR_ATTRS.items():
        if var_name not in out.variables:
            continue
        for attr_name, attr_value in canonical_attrs.items():
            out[var_name].attrs.setdefault(attr_name, attr_value)

    for var_name, override_attrs in _CANONICAL_VAR_ATTR_OVERRIDES.items():
        if var_name not in out.variables:
            continue
        for attr_name, attr_value in override_attrs.items():
            out[var_name].attrs[attr_name] = attr_value

    return out


def consolidate_band_coords(ds: xr.Dataset) -> xr.Dataset:
    """Collapse the 16 per-channel band_id_C{NN} / band_wavelength_C{NN}
    coords into a single band dim coord + wavelength(band) non-dim coord.
    """
    out = ds

    band_ids: list[Any] = []
    wavelengths: list[Any] = []
    sample_band_id_attrs: dict[str, Any] = {}
    sample_wavelength_attrs: dict[str, Any] = {}
    for i in ALL_CHANNELS:
        bid_name = f"band_id_{_ch(i)}"
        bwl_name = f"band_wavelength_{_ch(i)}"
        if bid_name not in out.variables:
            raise ValueError(f"consolidate_band_coords: missing {bid_name!r}")
        if bwl_name not in out.variables:
            raise ValueError(f"consolidate_band_coords: missing {bwl_name!r}")
        band_ids.append(out[bid_name].values.flat[0])
        wavelengths.append(out[bwl_name].values.flat[0])
        if i == 1:
            sample_band_id_attrs = dict(out[bid_name].attrs)
            sample_wavelength_attrs = dict(out[bwl_name].attrs)

    drop_names = [f"band_id_{_ch(i)}" for i in ALL_CHANNELS] + [
        f"band_wavelength_{_ch(i)}" for i in ALL_CHANNELS
    ]
    out = out.drop_vars(drop_names)

    band_arr = np.array(band_ids, dtype=np.int8)
    wavelength_arr = np.array(wavelengths, dtype=np.float32)
    band_da = xr.DataArray(
        band_arr, dims="band", name="band", attrs=sample_band_id_attrs,
    )
    wavelength_da = xr.DataArray(
        wavelength_arr, dims="band", name="wavelength", attrs=sample_wavelength_attrs,
    )
    out = out.assign_coords(band=band_da, wavelength=wavelength_da)
    out["wavelength"].encoding["_FillValue"] = None
    return out


def add_time_dimension(ds: xr.Dataset) -> xr.Dataset:
    """expand_dims('t') on every data variable using the scalar t coord."""
    if "t" not in ds.coords:
        raise ValueError("Expected a scalar 't' coordinate in the dataset.")
    t_value = ds["t"].values
    t_attrs = dict(ds["t"].attrs)
    t_encoding = dict(ds["t"].encoding)

    out = ds.drop_vars("t")
    new_data_vars: dict[str, xr.DataArray] = {}
    for name, da in out.data_vars.items():
        expanded = da.expand_dims({"t": [t_value]}, axis=0)
        expanded.encoding = dict(da.encoding)
        new_data_vars[name] = expanded

    new_ds = xr.Dataset(new_data_vars, coords=out.coords)
    new_ds["t"].attrs.update(t_attrs)
    new_ds["t"].encoding.update(t_encoding)
    return new_ds


def preprocess(ds: xr.Dataset) -> xr.Dataset:
    """Run the full preprocessing pipeline on a single virtual dataset.

    Suitable as the preprocess argument of virtualizarr.open_virtual_mfdataset.
    """
    try:
        validate_raw(ds)
        cleaned = fill_missing_channels(ds)
        cleaned = fill_missing_scalar_metadata(cleaned)
        cleaned = finalize_per_channel(cleaned)
        cleaned = consolidate_band_coords(cleaned)
        cleaned = add_time_dimension(cleaned)
        return cleaned
    except Exception as e:
        raise MCMIPFValidationError(_source_of(ds), e) from e


# =============================================================================
# Smoke test
# =============================================================================
def smoke_test(n_files: int = 3, *, start: str | None = None) -> xr.Dataset:
    """Open the first n_files via open_virtual_mfdataset + preprocess.

    Returns the combined virtual dataset.
    """
    urls = list(zip(range(n_files), iter_archive(start=start)))
    urls = [u for _, u in urls]
    if not urls:
        raise ValueError("No files found in GOES-17 archive.")
    print(f"Opening {len(urls)} file(s):")
    for u in urls:
        print(f"  {u}")

    store = _store()
    registry = ObjectStoreRegistry({BUCKET: store})

    vmds = vz.open_virtual_mfdataset(
        urls,
        registry=registry,
        parser=vz.parsers.HDFParser(),
        loadable_variables=list(DEFAULT_LOADABLE_VARIABLES),
        preprocess=preprocess,
        concat_dim="t",
        combine="nested",
        combine_attrs="drop_conflicts",
        coords="minimal",
        compat="override",
    )
    return vmds


# =============================================================================
# Ingest helpers
# =============================================================================
@contextlib.contextmanager
def timer(name: str):
    """Print elapsed time for a labelled block."""
    t0 = time.perf_counter()
    yield
    print(f"  {name}: {time.perf_counter() - t0:.1f}s", flush=True)


def _date_from_doy(year: int, doy: int) -> datetime.date:
    return datetime.date(year, 1, 1) + datetime.timedelta(days=doy - 1)


def _archive_day_index(year: int, doy: int) -> int:
    return (_date_from_doy(year, doy) - ARCHIVE_START_DATE).days


def _schema_exists(
    repo: "icechunk.Repository",
    branch: str,
    group: str | None = None,
) -> bool:
    """Return True if a zarr group is already committed at group on branch."""
    import zarr

    try:
        session = repo.readonly_session(branch=branch)
        zarr.open_group(store=session.store, path=group or "", zarr_format=3, mode="r")
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

    t_idx = existing.xindexes["t"].to_pandas_index()
    if not (t_idx.is_monotonic_increasing and t_idx.is_unique):
        raise ValueError(
            "Cannot auto-resume: existing dataset's `t` index is not strictly "
            f"increasing (monotonic={t_idx.is_monotonic_increasing}, "
            f"unique={t_idx.is_unique})."
        )

    last_t_np = existing["t"].values[-1]
    last_t_ts = pd.Timestamp(last_t_np)
    return (last_t_ts.year, int(last_t_ts.dayofyear)), np.datetime64(last_t_np, "ns")


def days_to_ingest(
    *,
    start: str | None = None,
    end: str | None = None,
) -> list[tuple[tuple[int, int], list[str]]]:
    """Eagerly enumerate the days/URLs we'll iterate."""
    return list(iter_archive_by_day(start=start, end=end))


def _log_event(
    log_dir: str,
    date_str: str,
    event_type: str,
    reason: str,
) -> None:
    """Append a skip/error entry to the log file."""
    from pathlib import Path

    log_path = Path(log_dir) / f"{SATELLITE}_{PRODUCT_SHORT}_ingest.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    with open(log_path, "a") as f:
        f.write(f"{timestamp} | {date_str} | {event_type} | {reason}\n")


def ingest_all_days(
    repo: "icechunk.Repository",
    *,
    branch: str = "main",
    group: str | None = "ABI-L2-MCMIPF",
    registry: ObjectStoreRegistry | None = None,
    loop_start_day: int = 0,
    loop_end_day: int | None = None,
    batch_size: int = 1,
    start: str | None = None,
    end: str | None = None,
    loadable_variables: Iterable[str] = DEFAULT_LOADABLE_VARIABLES,
    resume: bool = True,
    zarr_async_concurrency: int | None = None,
    log_dir: str | None = None,
) -> None:
    """Ingest every selected day into repo, in batches of batch_size days
    per commit.

    GOES-17 has a single codec era (pre-Shuffle) so there is no era-splitting
    logic — all data goes into a single group.

    log_dir: Directory for log files. If set, skipped/errored days are logged.
    """
    if registry is None:
        store = _store()
        registry = ObjectStoreRegistry({BUCKET: store})

    if zarr_async_concurrency is not None:
        import zarr
        zarr.config.set({"async.concurrency": zarr_async_concurrency})
        print(
            f"zarr async concurrency set to {zarr_async_concurrency}",
            flush=True,
        )

    parser = vz.parsers.HDFParser()
    loadable_variables = list(loadable_variables)

    all_days = days_to_ingest(start=start, end=end)
    end_idx = loop_end_day if loop_end_day is not None else len(all_days)
    selected = all_days[loop_start_day:end_idx]
    print(
        f"Ingesting {len(selected)} day(s) "
        f"[{loop_start_day}:{end_idx}] of {len(all_days)} total available.",
        flush=True,
    )

    is_first_write = not _schema_exists(repo, branch, group)
    if is_first_write:
        where = f"group {group!r}" if group else "root group"
        print(
            f"No existing schema on '{branch}' at {where} — the first iteration "
            "will create it (no append_dim).",
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
                f"(last t = {last_committed_t}). Skipping any selected days "
                f"at or before that.",
                flush=True,
            )

    n_batches = (len(selected) + batch_size - 1) // batch_size
    boundary_checked = False
    files_ingested = 0
    consecutive_failed_days = 0
    batches_failed = 0
    for batch_ind in range(n_batches):
        batch_start = batch_ind * batch_size
        batch = selected[batch_start : batch_start + batch_size]

        if skip_through is not None:
            batch = [item for item in batch if item[0] > skip_through]
        if not batch:
            continue

        urls_for_this_batch = [u for _, urls in batch for u in urls]

        (first_year, first_doy), _ = batch[0]
        (last_year, last_doy), _ = batch[-1]
        first_date = _date_from_doy(first_year, first_doy)
        last_date = _date_from_doy(last_year, last_doy)
        first_archive_day = _archive_day_index(first_year, first_doy)
        last_archive_day = _archive_day_index(last_year, last_doy)

        if not urls_for_this_batch:
            date_range = (
                first_date.isoformat()
                if first_date == last_date
                else f"{first_date.isoformat()} to {last_date.isoformat()}"
            )
            print(
                f"\n  SKIPPING batch {batch_ind} "
                f"({date_range}) — no files found",
                flush=True,
            )
            if log_dir is not None:
                _log_event(log_dir, date_range, "SKIPPED", "no files found")
            continue

        try:
            if not boundary_checked and last_committed_t is not None:
                first_file_scan_start = parse_scan_start_to_datetime(urls_for_this_batch[0])
                if first_file_scan_start <= last_committed_t:
                    raise ValueError(
                        f"Cross-batch ordering violation: existing data's last `t` "
                        f"is {last_committed_t}, but the first uncommitted file's "
                        f"scan-start parses to {first_file_scan_start}."
                    )
                boundary_checked = True

            print(f"\n====== Batch {batch_ind} ======")
            print(
                f"Ingesting {len(batch)} day(s) "
                f"({first_date.isoformat()} through {last_date.isoformat()}, "
                f"archive days {first_archive_day}-{last_archive_day})"
            )
            print(f"Total files in this batch: {len(urls_for_this_batch)}")

            with timer("Creating the virtual dataset"):
                vds = vz.open_virtual_mfdataset(
                    urls_for_this_batch,
                    registry=registry,
                    parser=parser,
                    preprocess=preprocess,
                    combine="nested",
                    concat_dim="t",
                    combine_attrs="drop_conflicts",
                    coords="minimal",
                    compat="override",
                    loadable_variables=loadable_variables,
                    parallel="dask",
                )

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
                        vds.vz.to_icechunk(session.store, group=group, append_dim="t")

            with timer("Committing to icechunk"):
                msg = (
                    f"Ingested GOES-17 ABI-L2-MCMIPF files for "
                    f"{first_date.isoformat()} through {last_date.isoformat()}. "
                    f"Archive days {first_archive_day}-{last_archive_day}."
                )
                session.commit(msg)
                print(msg)

            with timer("Cleaning up memory"):
                vds.close()
                del vds
                del session
                gc.collect()

            files_ingested += len(urls_for_this_batch)
            consecutive_failed_days = 0

        except Exception as e:
            print(
                f"\n  SKIPPING batch {batch_ind} "
                f"({first_date.isoformat()} to {last_date.isoformat()}) — "
                f"{type(e).__name__}: {e}",
                flush=True,
            )
            batches_failed += 1
            consecutive_failed_days += len(batch)
            if log_dir is not None:
                date_range = (
                    first_date.isoformat()
                    if first_date == last_date
                    else f"{first_date.isoformat()} to {last_date.isoformat()}"
                )
                _log_event(
                    log_dir, date_range,
                    "ERROR", f"{type(e).__name__}: {e}",
                )
            if consecutive_failed_days >= MAX_CONSECUTIVE_FAILED_DAYS:
                msg = (
                    f"Stopping: {consecutive_failed_days} consecutive "
                    f"day(s) skipped/failed (threshold: {MAX_CONSECUTIVE_FAILED_DAYS})."
                )
                print(f"\n{msg}", flush=True)
                if log_dir is not None:
                    _log_event(
                        log_dir, first_date.isoformat(),
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
# CLI
# =============================================================================
if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(
        description="Ingest GOES-17 ABI-L2-MCMIPF into Icechunk via VirtualiZarr.",
    )
    p.add_argument("--smoke-test", action="store_true",
                    help="Run a quick smoke test on 3 files and exit.")
    p.add_argument("--n-files", type=int, default=3,
                    help="Number of files for smoke test (default: 3).")
    args = p.parse_args()

    if args.smoke_test:
        vds = smoke_test(n_files=args.n_files)
        print(vds)
    else:
        print("Use --smoke-test to verify the pipeline, or import and call "
              "ingest_all_days() programmatically.")
