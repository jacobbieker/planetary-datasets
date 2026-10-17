"""JPSS VIIRS moderate-resolution (M-band) SDRs, copied out of the NOAA open data buckets.

**Why these, and why copied.** NOAA keeps the VIIRS M-band SDRs (``VIIRS-M1-SDR`` …
``VIIRS-M16-SDR``) and their terrain-corrected geolocation (``VIIRS-MOD-GEO-TC``) on AWS
for a rolling 90 days only; the I-band and Day/Night Band SDRs are kept indefinitely
(see :mod:`planetary_datasets.providers.polar._jpss`). The M-bands are the only VIIRS
imagery whose calibrated swath data disappears, so they are materialised into Icechunk
rather than referenced virtually — a virtual reference would dangle once the source
file expires. The rolling imagery EDRs (``VIIRS-I*-IMG-EDR``, ``VIIRS-NCC-EDR``) are
resamplings of the I-band and DNB SDRs that stay in the archive, so they are not copied.

**Layout.** One store per spacecraft, ``bkr/polar/jpss_viirs_mband_<sat>.icechunk``, so
NOAA-20, NOAA-21 and Suomi-NPP backfill in parallel against the retention deadline. One
granule (48 scans, ~85 s) is one step along ``time`` — the middle of its sensing
interval — with the swath on ``(y, x) = (768, 3200)`` and per-scan times on ``scan``.
Every 2-D field is chunked one granule per chunk.

**Precision.** Each quantity is kept the way the source carries it:

* Reflectance (M1–M11), brightness temperature (M12, M14–M16) and the radiances that
  IDPS packs (M1, M2, M6, M8–M12, M14–M16) stay ``uint16`` with the source's scale and
  offset, so they are exact. The packing is identical on all three spacecraft and is
  checked for every granule; a file packed differently raises :class:`PackingChanged`
  rather than being re-quantised.
* The radiances IDPS stores as float32 (dual-gain M3, M4, M5, M7 and M13) and M13's
  brightness temperature are float32 rounded to 16 mantissa bits (one part in ~1e5, finer
  than the packed bands' quantisation). Latitude and longitude are exact float32; the
  sun/satellite angles and terrain height keep 16 mantissa bits.
* Every fill code (``65528``–``65535`` packed, ``<= -999`` float) becomes NaN; the
  per-pixel quality flags (``QF1_VIIRSMBANDSDR``) are kept raw as ``uint8``.

Reflective bands M1–M6 and M9 are not processed at night; those granules are all-NaN
for those bands and compress to almost nothing.

Granules are read lazily, one HDF5 dataset per dask task, so an hour of a spacecraft
(~42 granules, ~4.5 GB of HDF5 by day) never has to be resident at once.
"""

from __future__ import annotations

import pathlib
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Dict, List, Mapping, Tuple

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.common import download_many
from planetary_datasets.providers.polar._granule import mid_time
from planetary_datasets.providers.polar._jpss import (
    BUCKETS,
    GRANULE_TIME_ENCODING,
    PLATFORM_NAMES,
    IncompleteDownload,
    JpssGranuleProvider,
    granule_end,
    granule_key,
    granule_start,
)

#: Rows and columns of one M-band granule: 48 scans of 16 detectors, 3200 samples.
GRANULE_SHAPE: Tuple[int, int] = (768, 3200)
#: Scans in one granule.
SCANS = 48

#: Smallest uint16 code IDPS reserves as a fill (N/A, missing, bow-tie trim, …).
PACKED_FILL_MIN = 65528
#: The single fill value packed variables are stored with.
PACKED_FILL = 65535
#: Float fills are -999.x; nothing physical in these products is anywhere near it.
FLOAT_FILL_MAX = -990.0

#: Scale and offset IDPS packs every M-band reflectance with.
REFLECTANCE_PACKING = (np.float32(2.4417415e-05), np.float32(0.0))

#: Product prefix in the buckets and HDF5 group name of the geolocation.
GEO_PRODUCT = "VIIRS-MOD-GEO-TC"
#: Filename prefix of the geolocation granules.
GEO_FILE_PREFIX = "GMTCO"

#: Mantissa bits kept for float fields that are not packed in the source.
FLOAT_KEEPBITS = 16


class PackingChanged(RuntimeError):
    """A granule is packed differently from the store's fixed packing."""


@dataclass(frozen=True)
class Field:
    """One stored quantity of one band.

    Attributes:
        dataset: HDF5 dataset under ``All_Data/VIIRS-Mxx-SDR_All``.
        packing: ``(scale, offset)`` of the ``uint16`` source, or None when the source is
            float32.
        units: Units of the decoded value.
        long_name: Description, without the band.
    """

    dataset: str
    packing: Tuple[np.float32, np.float32] | None
    units: str
    long_name: str


def _reflective(radiance: Tuple[float, float] | None) -> Dict[str, Field]:
    packing = None if radiance is None else (np.float32(radiance[0]), np.float32(radiance[1]))
    return {
        "reflectance": Field(
            "Reflectance", REFLECTANCE_PACKING, "1", "Top of atmosphere reflectance"
        ),
        "radiance": Field("Radiance", packing, "W m-2 sr-1 um-1", "Spectral radiance"),
    }


def _emissive(bt: Tuple[float, float] | None, radiance: Tuple[float, float] | None):
    def pack(values):
        return None if values is None else (np.float32(values[0]), np.float32(values[1]))

    return {
        "brightness_temperature": Field(
            "BrightnessTemperature", pack(bt), "K", "Brightness temperature"
        ),
        "radiance": Field("Radiance", pack(radiance), "W m-2 sr-1 um-1", "Spectral radiance"),
    }


#: Stored quantities per band, with the packing IDPS uses for each (read from operational
#: files of NOAA-20, NOAA-21 and Suomi-NPP, July and October 2026 — identical on all three).
BANDS: Dict[str, Dict[str, Field]] = {
    "M01": _reflective((0.011265738, -0.21)),
    "M02": _reflective((0.012584126, -0.2)),
    "M03": _reflective(None),
    "M04": _reflective(None),
    "M05": _reflective(None),
    "M06": _reflective((0.0010696354, -0.09)),
    "M07": _reflective(None),
    "M08": _reflective((0.0030219604, -0.14)),
    "M09": _reflective((0.0014133105, -0.09)),
    "M10": _reflective((0.0013045005, -0.04)),
    "M11": _reflective((0.0005826606, -0.02)),
    "M12": _emissive((0.002518046, 203.0), (6.732538e-05, 0.0)),
    "M13": _emissive(None, None),
    "M14": _emissive((0.0037389167, 120.0), (0.00039955133, -0.03)),
    "M15": _emissive((0.004120439, 111.0), (0.000390085, -0.02)),
    "M16": _emissive((0.004257787, 103.0), (0.0003386543, -0.02)),
}

#: Nominal centre wavelength of each band, micrometres.
WAVELENGTHS_UM: Dict[str, float] = {
    "M01": 0.412,
    "M02": 0.445,
    "M03": 0.488,
    "M04": 0.555,
    "M05": 0.672,
    "M06": 0.746,
    "M07": 0.865,
    "M08": 1.240,
    "M09": 1.378,
    "M10": 1.610,
    "M11": 2.250,
    "M12": 3.700,
    "M13": 4.050,
    "M14": 8.550,
    "M15": 10.763,
    "M16": 12.013,
}

#: Geolocation fields: stored name -> (HDF5 dataset, units, long name).
GEO_FIELDS: Dict[str, Tuple[str, str, str]] = {
    "latitude": ("Latitude", "degrees_north", "Latitude"),
    "longitude": ("Longitude", "degrees_east", "Longitude"),
    "solar_zenith_angle": ("SolarZenithAngle", "degree", "Solar zenith angle"),
    "solar_azimuth_angle": ("SolarAzimuthAngle", "degree", "Solar azimuth angle"),
    "satellite_zenith_angle": ("SatelliteZenithAngle", "degree", "Satellite zenith angle"),
    "satellite_azimuth_angle": ("SatelliteAzimuthAngle", "degree", "Satellite azimuth angle"),
    "terrain_height": ("Height", "m", "Terrain height above the ellipsoid"),
}

#: Spacecraft code used in IDPS filenames.
IDPS_SPACECRAFT: Dict[str, str] = {"n20": "j01", "n21": "j02", "snpp": "npp"}

#: Variables kept to full precision.
EXACT_FLOATS = ("latitude", "longitude", "scan_start_offset")


def band_product(band: str) -> str:
    """Bucket prefix and HDF5 product name of a band, e.g. ``VIIRS-M5-SDR`` for M05."""
    return f"VIIRS-M{int(band[1:])}-SDR"


def band_file_prefix(band: str) -> str:
    """Filename prefix of a band's granules, e.g. ``SVM05``."""
    return f"SV{band}"


def variable_name(band: str, quantity: str) -> str:
    """Name of a stored variable, e.g. ``M05_reflectance``."""
    return f"{band}_{quantity}"


def packed_variables() -> Dict[str, Tuple[np.float32, np.float32]]:
    """Every stored variable kept packed, with its ``(scale, offset)``."""
    return {
        variable_name(band, quantity): field.packing
        for band, fields in BANDS.items()
        for quantity, field in fields.items()
        if field.packing is not None
    }


# ------------------------------------------------------------------ readers


def _decode_packed(raw: np.ndarray, factors: np.ndarray, expected, where: str) -> np.ndarray:
    """Decode IDPS ``uint16`` counts to float32, with every fill code as NaN.

    A granule with nothing valid (a reflective band at night) carries fill factors too;
    it decodes to all-NaN without being checked.
    """
    valid = raw < PACKED_FILL_MIN
    if not valid.any():
        return np.full(raw.shape, np.nan, dtype=np.float32)
    if float(factors[0]) <= FLOAT_FILL_MAX:
        raise PackingChanged(f"{where} has valid counts but fill scale factors {factors}")
    scale, offset = np.float32(factors[0]), np.float32(factors[1])
    if not (np.isclose(scale, expected[0], rtol=1e-6) and np.isclose(offset, expected[1])):
        raise PackingChanged(
            f"{where} is packed as scale={scale} offset={offset}, the store expects "
            f"scale={expected[0]} offset={expected[1]}"
        )
    decoded = raw.astype(np.float64) * float(expected[0]) + float(expected[1])
    return np.where(valid, decoded, np.nan).astype(np.float32)


def _decode_float(raw: np.ndarray) -> np.ndarray:
    """Float32 source values with their -999.x fills as NaN."""
    raw = raw.astype(np.float32, copy=False)
    return np.where(raw > FLOAT_FILL_MAX, raw, np.float32(np.nan)).astype(np.float32)


def _check_shape(array: np.ndarray, where: str) -> None:
    if array.shape != GRANULE_SHAPE:
        raise ValueError(f"{where} has shape {array.shape}, expected {GRANULE_SHAPE}")


def read_band_field(path: str, band: str, quantity: str) -> np.ndarray:
    """Read one band quantity of one granule as a ``(1, y, x)`` float32 array."""
    import h5py

    field = BANDS[band][quantity]
    group = f"All_Data/{band_product(band)}_All"
    where = f"{pathlib.Path(path).name}:{field.dataset}"
    with h5py.File(path, "r") as h5:
        raw = h5[f"{group}/{field.dataset}"][()]
        _check_shape(raw, where)
        if field.packing is None:
            out = _decode_float(raw)
        else:
            factors = h5[f"{group}/{field.dataset}Factors"][()]
            out = _decode_packed(raw, factors, field.packing, where)
    return out[np.newaxis]


def read_band_flags(path: str, band: str) -> np.ndarray:
    """Read one band's per-pixel quality flags as a ``(1, y, x)`` uint8 array."""
    import h5py

    with h5py.File(path, "r") as h5:
        raw = h5[f"All_Data/{band_product(band)}_All/QF1_VIIRSMBANDSDR"][()]
    _check_shape(raw, f"{pathlib.Path(path).name}:QF1_VIIRSMBANDSDR")
    return raw.astype(np.uint8)[np.newaxis]


def read_geo_field(path: str, name: str) -> np.ndarray:
    """Read one geolocation field of one granule as a ``(1, y, x)`` float32 array."""
    import h5py

    dataset = GEO_FIELDS[name][0]
    with h5py.File(path, "r") as h5:
        raw = h5[f"All_Data/{GEO_PRODUCT}_All/{dataset}"][()]
    _check_shape(raw, f"{pathlib.Path(path).name}:{dataset}")
    return _decode_float(raw)[np.newaxis]


def read_scan_offsets(path: str, reference: pd.Timestamp) -> np.ndarray:
    """Start of each scan in seconds after ``reference``, NaN for missing scans.

    The ``StartTime`` dataset is IDPS Epoch Time — microseconds since 1958 on an atomic
    scale, so it runs ahead of UTC by the accumulated leap seconds. Measuring each scan
    from the granule's own beginning, whose UTC time is in the attributes, sidesteps
    the leap-second table.

    Offsets rather than datetimes because a datetime variable holding NaT does not
    survive a store round trip with the pinned numpy/xarray (decoding overflows).
    """
    import h5py

    with h5py.File(path, "r") as h5:
        iet = h5[f"All_Data/{GEO_PRODUCT}_All/StartTime"][()].astype(np.int64)
        attrs = h5[f"Data_Products/{GEO_PRODUCT}/{GEO_PRODUCT}_Gran_0"].attrs
        begin_iet = int(np.ravel(attrs["N_Beginning_Time_IET"])[0])
        day = np.ravel(attrs["Beginning_Date"])[0].decode()
        tod = np.ravel(attrs["Beginning_Time"])[0].decode().rstrip("Z")
    begin = pd.Timestamp(f"{day[:4]}-{day[4:6]}-{day[6:8]}T{tod[:2]}:{tod[2:4]}:{tod[4:]}")
    shift = (begin - pd.Timestamp(reference)) / pd.Timedelta(1, "s")
    offsets = np.full(SCANS, np.nan, dtype=np.float64)
    valid = iet[:SCANS] > 0
    offsets[: valid.size][valid] = (iet[:SCANS][valid] - begin_iet) / 1e6 + shift
    return offsets


def _lazy(func, *args, shape: Tuple[int, ...], dtype):
    """Wrap ``func(*args)`` as a single-chunk dask array of the given shape and dtype."""
    import dask
    import dask.array as da

    return da.from_delayed(dask.delayed(func, pure=True)(*args), shape=shape, dtype=dtype)


# ------------------------------------------------------------------ provider


class JpssViirsMbandProvider(JpssGranuleProvider):
    """VIIRS M-band SDRs and geolocation for one JPSS spacecraft.

    Attributes:
        satellite: ``n20``, ``n21`` or ``snpp``.
        download_workers: Concurrent downloads; an hour is ~714 files.
    """

    append_dim = "time"
    window = pd.Timedelta(1, "h")
    keepbits = FLOAT_KEEPBITS
    keepbits_by_variable = {
        **{name: None for name in packed_variables()},
        **{name: None for name in EXACT_FLOATS},
    }
    encoding_overrides = {
        **{
            name: {
                "dtype": "uint16",
                "scale_factor": scale,
                "add_offset": offset,
                "_FillValue": np.uint16(PACKED_FILL),
            }
            for name, (scale, offset) in packed_variables().items()
        },
    }

    def __init__(
        self,
        satellite: str,
        config=None,
        granule_limit: int | None = None,
        download_workers: int = 16,
    ):
        """Build a provider for one spacecraft.

        Args:
            satellite: One of :data:`~planetary_datasets.providers.polar._jpss.BUCKETS`.
            config: Override the process-wide configuration.
            granule_limit: Stop after this many granules per partition. For smoke tests
                against the live archive, not for production backfills.
            download_workers: Concurrent downloads.
        """
        super().__init__(config=config)
        if satellite not in BUCKETS:
            raise ValueError(f"unknown JPSS satellite {satellite!r}; expected {sorted(BUCKETS)}")
        self.satellite = satellite
        self.bucket = BUCKETS[satellite]
        self.name = f"jpss_viirs_mband_{satellite}"
        self.store_prefix = f"bkr/polar/jpss_viirs_mband_{satellite}.icechunk"
        self.granule_limit = granule_limit
        self.download_workers = download_workers
        overrides = dict(type(self).encoding_overrides)
        for name in self.swath_variables():
            overrides[name] = {**overrides.get(name, {}), "chunks": (1, *GRANULE_SHAPE)}
        for name in ("start_time", "end_time"):
            overrides[name] = dict(GRANULE_TIME_ENCODING)
        self.encoding_overrides = overrides

    @staticmethod
    def swath_variables() -> List[str]:
        """Every stored ``(time, y, x)`` variable."""
        names = [
            variable_name(band, quantity) for band, fields in BANDS.items() for quantity in fields
        ]
        names += [variable_name(band, "quality_flags") for band in BANDS]
        return names + list(GEO_FIELDS)

    @staticmethod
    def products() -> Dict[str, str]:
        """Bucket prefix -> filename prefix, for every file a granule needs."""
        out = {band_product(band): band_file_prefix(band) for band in BANDS}
        out[GEO_PRODUCT] = GEO_FILE_PREFIX
        return out

    # ------------------------------------------------------------------ fetch

    def granule_sets(self, it: pd.Timestamp) -> List[Dict[str, str]]:
        """``{product: key}`` for every complete granule this partition owns.

        A granule is only taken when its geolocation and all sixteen bands are listed; a
        partial one is logged and left out rather than stored with holes.
        """
        start, end = self.partition_window(it)
        days = self._days_to_list(start, end)
        fs = self._s3()

        def listing(product: str) -> Tuple[str, Dict[Tuple[str, ...], str]]:
            keys: Dict[Tuple[str, ...], str] = {}
            for day in days:
                listed = self._list_day(fs, self.bucket, product, day)
                keys.update(self._index_keys(listed, window=(start, end), owned=True))
            return product, keys

        with ThreadPoolExecutor(max_workers=8) as pool:
            indexed = dict(pool.map(listing, self.products()))

        granules: Dict[Tuple[str, ...], Dict[str, str]] = {}
        for product, keys in indexed.items():
            for key, path in keys.items():
                granules.setdefault(key, {})[product] = path

        complete = []
        for key in sorted(granules, key=lambda k: granule_start(next(iter(granules[k].values())))):
            files = granules[key]
            missing = sorted(set(self.products()) - set(files))
            if missing:
                logger.warning(f"{self.name}: granule {'_'.join(key)} lacks {missing}, skipping")
                continue
            complete.append(files)
        if self.granule_limit is not None:
            complete = complete[: self.granule_limit]
        return complete

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Download every file of every complete granule in the partition.

        Raises:
            IncompleteDownload: when any file failed after retries, so the partition is
                retried rather than written with holes.
        """
        granules = self.granule_sets(it)
        if not granules:
            return []
        dest = pathlib.Path(temp_dir) if temp_dir is not None else self.config.data_dir / self.name
        urls = [
            # ``fs.ls`` returns "bucket/key"; strip the bucket back off for the URL.
            self._https_url(self.bucket, path.split("/", 1)[1])
            for files in granules
            for path in files.values()
        ]
        paths = download_many(urls, dest, workers=self.download_workers)
        if len(paths) != len(urls):
            raise IncompleteDownload(
                f"{self.name}: only {len(paths)}/{len(urls)} files downloaded for {it}"
            )
        logger.info(f"{self.name}: fetched {len(paths)} files ({len(granules)} granules) for {it}")
        return [str(p) for p in paths]

    # ------------------------------------------------------------------ process

    def granule_dataset(self, files: Mapping[str, str]) -> xr.Dataset:
        """One granule as a lazy single-step dataset.

        Args:
            files: Filename prefix (``SVM01`` … ``SVM16``, ``GMTCO``) -> local path.
        """
        geo = files[GEO_FILE_PREFIX]
        name = pathlib.Path(geo).name
        start, end = granule_start(name), granule_end(name)
        shape = (1, *GRANULE_SHAPE)
        dims = ("time", "y", "x")

        data_vars: Dict[str, xr.DataArray] = {}
        for band, fields in BANDS.items():
            path = files[band_file_prefix(band)]
            for quantity, field in fields.items():
                data = _lazy(read_band_field, path, band, quantity, shape=shape, dtype=np.float32)
                attrs = {
                    "units": field.units,
                    "long_name": f"{field.long_name}, VIIRS band {band}",
                    "central_wavelength_um": WAVELENGTHS_UM[band],
                }
                data_vars[variable_name(band, quantity)] = xr.DataArray(
                    data, dims=dims, attrs=attrs
                )
            flags = _lazy(read_band_flags, path, band, shape=shape, dtype=np.uint8)
            data_vars[variable_name(band, "quality_flags")] = xr.DataArray(
                flags,
                dims=dims,
                attrs={"long_name": f"QF1_VIIRSMBANDSDR quality flags, VIIRS band {band}"},
            )
        for var, (_, units, long_name) in GEO_FIELDS.items():
            data = _lazy(read_geo_field, geo, var, shape=shape, dtype=np.float32)
            data_vars[var] = xr.DataArray(
                data, dims=dims, attrs={"units": units, "long_name": long_name}
            )

        data_vars["scan_start_offset"] = xr.DataArray(
            read_scan_offsets(geo, start)[np.newaxis],
            dims=("time", "scan"),
            attrs={"units": "s", "long_name": "Start of each scan, seconds after start_time"},
        )
        data_vars["start_time"] = xr.DataArray(
            np.array([start], dtype="datetime64[ns]"), dims="time"
        )
        data_vars["end_time"] = xr.DataArray(np.array([end], dtype="datetime64[ns]"), dims="time")
        data_vars["orbit_number"] = xr.DataArray(
            np.array([int(granule_key(name)[4][1:])], dtype=np.int32), dims="time"
        )
        data_vars["platform_name"] = xr.DataArray(
            np.array([PLATFORM_NAMES[self.satellite]]), dims="time"
        )
        ds = xr.Dataset(data_vars, coords={"time": [mid_time(start, end)]})
        ds.attrs = {
            "platform": PLATFORM_NAMES[self.satellite],
            "instrument": "VIIRS",
            "source": f"s3://{self.bucket}/VIIRS-M*-SDR, {GEO_PRODUCT}",
        }
        return ds

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Group the downloaded files by granule and stack the complete ones on time."""
        prefixes = set(self.products().values())
        granules: Dict[Tuple[str, ...], Dict[str, str]] = {}
        for path in input_files:
            name = pathlib.Path(path).name
            prefix = name.split("_", 1)[0]
            if prefix not in prefixes:
                continue
            try:
                granules.setdefault(granule_key(name), {})[prefix] = path
            except ValueError as exc:
                logger.warning(f"{self.name}: ignoring {name} ({exc})")

        datasets = []
        for key in sorted(granules):
            files = granules[key]
            missing = sorted(prefixes - set(files))
            if missing:
                logger.warning(f"{self.name}: granule {'_'.join(key)} lacks {missing}, skipping")
                continue
            try:
                datasets.append(self.granule_dataset(files))
            except (OSError, KeyError, ValueError) as exc:
                logger.warning(f"{self.name}: granule {'_'.join(key)} unreadable: {exc}")

        return self.restrict_to_window(self.concat_granules(datasets), it)
