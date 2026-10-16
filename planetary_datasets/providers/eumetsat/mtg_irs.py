"""MTG IRS Level 1 spectral soundings from the EUMETSAT Data Store.

The Infrared Sounder on MTG-S1 is a Fourier-transform spectrometer that stares at the
disc one *dwell* at a time: a 160x160 block of ~4 km pixels, each with a full spectrum of
877 long-wave (LWIR) and 1076 mid-wave (MWIR) samples. The disc is split into four Local
Area Coverages, LAC1 to LAC4 from south to north; LAC4 (Europe) is scanned every 30
minutes and the others in rotation. The Data Store splits the archive by data policy:

* ``EO:EUM:DAT:0741`` — LAC1-3, and LAC4 other than the hourly scans
* ``EO:EUM:DAT:1199`` — LAC4, the scans ending on the full hour

Each collection gets a store of its own, beside the FCI, SEVIRI and RSS stores under
``bkr/geo``. A Data Store product is one repeat cycle, ~4.7 GB as ~72 ``CHK-BODY``
netCDF files of one dwell each, plus a trailer and PNG quicklooks that are not needed.

**Layout.** One dwell is one step along ``time`` (the dwell's central time), so every
LAC shares one store and one shape: ``(time, y, x)`` for per-pixel fields and
``(time, y, x, wavenumber_lwir|wavenumber_mwir)`` for the spectra. ``dwell_number``
(the dwell's fixed position on the disc) and ``lac`` say where a step is; select on
them to rebuild a scan.

**Precision.** Radiances are stored as the int16 counts the source carries, with the
source's scale and offset, so the store is exact and no larger than the source. The packing
is fixed when the store is created; a file packed differently raises
:class:`PackingChanged` rather than being re-quantised into the old packing.

**Source quirks handled here.** ``data/time`` declares its units as
``seconds since 01:01:2000``, which xarray decodes into 2053, so it is decoded by hand.
The enum and boolean variables use ``_FillValue = 0`` — the value of ``EV`` and ``false``
— so a CF-decoding read turns every valid zero into NaN; they are read raw.

Detector-level diagnostics (``dc_image_radiance``, ``detector_sample_quality``, 480x480 per
dwell and band) are not stored.
"""

from __future__ import annotations

import datetime as dt
import os
import pathlib
import re
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, List

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.providers.polar._eumdac import EumdacProvider

#: First IRS L1 product in the Data Store.
ARCHIVE_START = dt.datetime(2026, 7, 31, 10, 0)

#: Spectral bands, as named by the product's groups.
BANDS = ("lwir", "mwir")

#: The packing of ``effective_radiance`` in IRSL1FS v7B, per band: (scale, offset).
RADIANCE_PACKING: dict[str, tuple[float, float]] = {
    "lwir": (np.float32(8e-08).item(), np.float32(0.00085).item()),
    "mwir": (np.float32(6e-09).item(), np.float32(8e-05).item()),
}
RADIANCE_FILL = -32767

#: Per-pixel geometry, cloud and geolocation, decoded to float32.
PIXEL_VARIABLES = (
    "latitude",
    "longitude",
    "satellite_zenith_angle",
    "satellite_azimuth_angle",
    "solar_zenith_angle",
    "solar_azimuth_angle",
    "cloud_fraction",
    "cloud_signal",
)

#: Per-pixel boolean warnings, per band, kept as 0/1.
BAND_WARNINGS = (
    "instrument_quality_warning",
    "radiometric_quality_warning",
    "spectral_quality_warning",
    "rad_rts_noise_warning",
    "phase_average_warning",
    "phase_std_warning",
)

DWELL_TYPES = ("EV", "BB", "DS1", "DS2", "SA-EV", "SA-DS2")
STROKE_DIRECTIONS = ("Forward", "Backward")

#: Epoch of every time in the product, whatever its ``units`` attribute claims.
EPOCH = np.datetime64("2000-01-01T00:00:00", "ns")

#: Sensing start and end in an entry's filename, e.g. ``_20261013005222_20261013005231_N_``.
_SENSING = re.compile(r"_(\d{14})_(\d{14})_[A-Z]_")

#: Chunking in the store. A radiance chunk is a quarter of a dwell with its whole spectrum,
#: ~11 MB (LWIR) and ~14 MB (MWIR) of int16 before compression.
RADIANCE_CHUNK = 80


class CollectionUnavailable(RuntimeError):
    """The Data Store could not search a collection."""


class PackingChanged(RuntimeError):
    """A file's radiance packing differs from the one the store is created with."""


def is_body_chunk(entry: str) -> bool:
    """True for the per-dwell netCDF files; the trailer and quicklooks are skipped."""
    return "-CHK-BODY-" in entry and entry.endswith(".nc")


def entry_sensing(entry: str) -> tuple[pd.Timestamp, pd.Timestamp] | None:
    """Sensing start and end encoded in an entry's filename, or None if absent."""
    found = _SENSING.search(pathlib.Path(entry).name)
    if found is None:
        return None
    return (
        pd.Timestamp(dt.datetime.strptime(found.group(1), "%Y%m%d%H%M%S")),
        pd.Timestamp(dt.datetime.strptime(found.group(2), "%Y%m%d%H%M%S")),
    )


def entry_overlaps(entry: str, start: pd.Timestamp, end: pd.Timestamp) -> bool:
    """True when a dwell's sensing overlaps ``[start, end)``.

    Overlap rather than containment, so a dwell sensed across a boundary is downloaded by
    both partitions; :meth:`~planetary_datasets.providers.polar.GranuleProvider.
    restrict_to_window` then keeps it in whichever one its central time falls in.
    """
    sensing = entry_sensing(entry)
    if sensing is None:
        return False
    return sensing[0] < end and sensing[1] >= start


def seconds_since_2000(seconds: Any) -> np.datetime64:
    """Decode a product time to whole seconds, which is what the store's ``time`` holds."""
    return (
        (EPOCH + np.timedelta64(int(round(float(seconds) * 1e3)), "ms"))
        .astype("datetime64[s]")
        .astype("datetime64[ns]")
    )


def _raw(path: str, group: str, names: tuple[str, ...], dtype: str = "uint8") -> xr.Dataset:
    """Read flags and enums undecoded, without the ``_FillValue`` that would mask zeros."""
    # decode_coords=False: the flags name the *raw* latitude/longitude as coordinates.
    with xr.open_dataset(
        path,
        engine="h5netcdf",
        group=group,
        mask_and_scale=False,
        decode_times=False,
        decode_coords=False,
    ) as ds:
        out = ds[list(names)].load().drop_encoding()
    for name in names:
        out[name] = out[name].astype(dtype)
        out[name].attrs.pop("_FillValue", None)
        # Undecoded, this names latitude/longitude, which the store holds as data
        # variables; written out, it would make them read back as coordinates.
        out[name].attrs.pop("coordinates", None)
    return out


def _flag_attrs(meanings: tuple[str, ...]) -> dict[str, Any]:
    return {
        "flag_values": list(range(len(meanings))),
        "flag_meanings": " ".join(m.replace("-", "_") for m in meanings),
    }


def read_dwell(path: str) -> xr.Dataset:
    """Read one ``CHK-BODY`` file into a single ``time`` step.

    The spectra stay lazy (dask, chunked as on disk) so a partition of a few hundred dwells
    is never resident at once; everything else is small and read eagerly.

    Raises:
        PackingChanged: when a band's radiance is not packed as :data:`RADIANCE_PACKING`.
    """
    with xr.open_dataset(path, engine="h5netcdf", group="data", decode_times=False) as data:
        # latitude/longitude arrive as coordinates; they vary per dwell, so are data here.
        pixels = data[list(PIXEL_VARIABLES)].reset_coords().load()
        centre = seconds_since_2000(data["time"].values)
        dwell_number = int(data["dwell_number"].values)
    pixels = pixels.drop_encoding().astype("float32")

    flags = _raw(path, "data", ("dwell_type", "stroke_direction", "dust_warning"))
    with xr.open_dataset(
        path, engine="h5netcdf", group="state/instrument", decode_times=False
    ) as instrument:
        lac = int(instrument["repeat_cycle_lac_id"].values)
        cycle_start = seconds_since_2000(instrument["repeat_cycle_start_time"].values)

    parts = [pixels, flags[["dust_warning"]]]
    for band in BANDS:
        measured = xr.open_dataset(
            path, engine="h5netcdf", group=f"data/{band}/measured", chunks={}
        )
        radiance = measured["effective_radiance"]
        scale, offset = RADIANCE_PACKING[band]
        found = (radiance.encoding.get("scale_factor"), radiance.encoding.get("add_offset"))
        if found != (np.float32(scale), np.float32(offset)):
            raise PackingChanged(
                f"{pathlib.Path(path).name}: {band} radiance is packed as "
                f"scale={found[0]} offset={found[1]}, the store expects "
                f"scale={scale} offset={offset}"
            )
        with xr.open_dataset(path, engine="h5netcdf", group=f"data/{band}") as spectral:
            wavenumber = spectral["wavenumber"].values.astype("float32")
        radiance = radiance.drop_encoding().rename({"wavenumber": f"wavenumber_{band}"})
        radiance.attrs = {
            "long_name": f"Effective radiance, {band.upper()}",
            "standard_name": "effective_radiance_in_wavenumber",
            "units": "W m-2 sr-1 (m-1)-1",
        }
        parts.append(radiance.to_dataset(name=f"{band}_effective_radiance"))
        parts.append(
            _raw(path, f"data/{band}/measured", ("spatial_sample_quality",), dtype="int16").rename(
                {"spatial_sample_quality": f"{band}_spatial_sample_quality"}
            )
        )
        warnings = _raw(path, f"data/{band}/quality_band", BAND_WARNINGS)
        parts.append(warnings.rename({name: f"{band}_{name}" for name in BAND_WARNINGS}))
        parts.append(
            xr.Dataset(
                coords={f"wavenumber_{band}": (f"wavenumber_{band}", wavenumber, {"units": "m-1"})}
            )
        )

    ds = xr.merge(parts, combine_attrs="drop_conflicts", join="exact")
    ds = ds.rename({"dwell_row": "y", "dwell_column": "x"})
    ds = ds.expand_dims("time").assign_coords(time=[centre])
    ds["dwell_number"] = xr.DataArray(np.array([dwell_number], dtype="uint16"), dims="time")
    ds["dwell_type"] = xr.DataArray(
        flags["dwell_type"].values.reshape(1), dims="time", attrs=_flag_attrs(DWELL_TYPES)
    )
    ds["stroke_direction"] = xr.DataArray(
        flags["stroke_direction"].values.reshape(1),
        dims="time",
        attrs=_flag_attrs(STROKE_DIRECTIONS),
    )
    ds["lac"] = xr.DataArray(
        np.array([lac], dtype="uint8"),
        dims="time",
        attrs={"long_name": "Local Area Coverage, 1-4 south to north"},
    )
    ds["repeat_cycle_start_time"] = xr.DataArray(np.array([cycle_start]), dims="time")
    return ds


class MtgIrsProvider(EumdacProvider):
    """Base class for the MTG IRS L1 spectral sounding collections.

    Attributes:
        download_workers: Dwell files downloaded at once. An hour is ~290 files of ~72 MB.
    """

    append_dim = "time"
    window = pd.Timedelta(1, "h")
    download_workers: int = 8
    alignment_coords = ("wavenumber_lwir", "wavenumber_mwir")
    encoding_overrides = {
        f"{band}_effective_radiance": {
            "dtype": "int16",
            "scale_factor": scale,
            "add_offset": offset,
            "_FillValue": RADIANCE_FILL,
        }
        for band, (scale, offset) in RADIANCE_PACKING.items()
    }

    def align_to_store(self, repo, ds: xr.Dataset) -> xr.Dataset:
        """Nothing to align: ``time`` is chunked one dwell per chunk, so a stored chunk is
        never partial. The base version reads ``ds.chunks``, which raises here because the
        spectra are chunked differently from the per-pixel fields along ``y``/``x``."""
        return ds

    def search(self, it: pd.Timestamp) -> List[Any]:
        """Products overlapping the partition; raises if the collection cannot be searched."""
        from eumdac.collection import CollectionError

        try:
            return super().search(it)
        except CollectionError as exc:
            raise CollectionUnavailable(
                f"{self.name}: the Data Store cannot search {self.collection_id}: {exc}"
            ) from exc

    def _download_entry(self, product: Any, entry: str, dest: pathlib.Path) -> pathlib.Path:
        """Download one entry atomically, retrying with backoff. Raises once out of tries.

        A dwell that cannot be fetched fails the partition rather than being skipped: the
        partition counts as done once any dwell from it is stored, so a skipped dwell would
        be a hole no later run fills.
        """
        target = dest / pathlib.Path(entry).name
        if target.is_file() and target.stat().st_size > 0:
            return target
        part = target.with_name(target.name + ".part")
        for attempt in range(1, self.download_retries + 1):
            try:
                with product.open(entry=entry) as src, open(part, "wb") as out:
                    shutil.copyfileobj(src, out, length=8 << 20)
                os.replace(part, target)
                return target
            except Exception as exc:  # noqa: BLE001 - the Data Store fails transiently
                part.unlink(missing_ok=True)
                if attempt == self.download_retries:
                    raise
                logger.warning(
                    f"{self.name}: {target.name} failed "
                    f"(attempt {attempt}/{self.download_retries}): {exc}"
                )
                time.sleep(2**attempt)
        raise RuntimeError("unreachable")

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Download the dwells sensed during the partition, and nothing else of a product."""
        start, end = self.partition_window(it)
        dest = self.workdir(temp_dir)
        jobs = []
        for product in self.search(it):
            jobs += [
                (product, entry)
                for entry in product.entries
                if is_body_chunk(entry) and entry_overlaps(entry, start, end)
            ]
        if not jobs:
            return []
        logger.info(f"{self.name}: downloading {len(jobs)} dwell(s) for {start}")
        with ThreadPoolExecutor(max_workers=self.download_workers) as pool:
            paths = list(pool.map(lambda job: self._download_entry(*job, dest), jobs))
        return sorted(str(p) for p in paths)

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Stack the partition's dwells along ``time``, lazily, chunked for the store."""
        dwells = [read_dwell(path) for path in input_files]
        ds = self.restrict_to_window(self.concat_granules(dwells), it)
        if ds.sizes.get("time", 0) == 0:
            return ds
        # A dwell sensed across a boundary is fetched by both partitions, and a product
        # can be listed twice; neither may become two steps.
        ds = ds.isel(time=~ds.indexes["time"].duplicated())
        ds.attrs = {
            "title": "MTG IRS Level 1 spectral soundings",
            "platform": "MTG-S1",
            "instrument": "IRS",
            "source": f"EUMETSAT Data Store {self.collection_id}",
        }
        # Spectra in quarter dwells; the per-pixel fields are ~100 kB a dwell, so whole.
        ds = ds.chunk({"time": 1, "y": -1, "x": -1})
        for band in BANDS:
            name = f"{band}_effective_radiance"
            ds[name] = ds[name].chunk(
                {"y": RADIANCE_CHUNK, "x": RADIANCE_CHUNK, f"wavenumber_{band}": -1}
            )
        return ds


class MtgIrsL1Provider(MtgIrsProvider):
    """MTG IRS L1 spectral soundings: LAC1-3 and the non-hourly LAC4 scans."""

    name = "mtg_irs_l1"
    store_prefix = "bkr/geo/mtg_irs_l1.icechunk"
    collection_id = "EO:EUM:DAT:0741"


class MtgIrsL1Lac4HourlyProvider(MtgIrsProvider):
    """MTG IRS L1 spectral soundings: the LAC4 (Europe) scans ending on the full hour."""

    name = "mtg_irs_l1_lac4_hourly"
    store_prefix = "bkr/geo/mtg_irs_l1_lac4_hourly.icechunk"
    collection_id = "EO:EUM:DAT:1199"


PROVIDERS: dict[str, type[MtgIrsProvider]] = {
    cls.name: cls for cls in (MtgIrsL1Provider, MtgIrsL1Lac4HourlyProvider)
}
