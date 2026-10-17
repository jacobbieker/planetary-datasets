"""ATMS-based level 2 granules (snowfall rate, MiRS) from the NOAA JPSS open data buckets.

The ATMS SDR, TDR and geolocation are kept on AWS indefinitely and are already archived
by :class:`~planetary_datasets.providers.polar.jpss_atms.JpssAtmsProvider`. The NDE level
2 products retrieved from them are not: since 2026-09-14 they are kept for a rolling 90
days (see :mod:`planetary_datasets.providers.polar._jpss`), so they are copied into
Icechunk here, one store per product and spacecraft:

=========================  ====================  ====================================
Store (``bkr/polar/…``)    Bucket prefix         Contents
=========================  ====================  ====================================
``jpss_atms_sfr_<sat>``    ``ATMS-SFR``          Snowfall rate, ice water path, ...
``jpss_mirs_img_<sat>``    ``NPR_MIRS_IMG``      MiRS surface/column retrievals, BTs
``jpss_mirs_snd_<sat>``    ``NPR_MIRS_SND``      MiRS temperature/moisture profiles
=========================  ====================  ====================================

Each granule is one ATMS granule — 12 scans of 96 fields of view, ~32 s — and becomes one
step along ``time`` (the middle of its sensing interval), with the scan on ``y`` and the
field of view on ``x``.

**Why one store per spacecraft.** NOAA-20 and NOAA-21 granules share their boundaries to
within 0.1 s, so interleaving the constellation on one ``time`` axis — at the whole-second
resolution the granule stores use — gives two steps with the same timestamp for most
granules. Separate stores keep ``time`` unique, and let each spacecraft backfill in
parallel against the retention deadline.

**Precision.** Variables are stored with the packing the source netCDF uses: MiRS keeps
most fields as ``int16`` with a scale factor, and so does the store; float32 fields stay
float32. Static per-channel or per-level variables (``Freq``, ``Polo``, ``Plevel``,
``Player``) become coordinates. The integer ``ScanTime_*`` components carry units such as
``days``; that attribute is renamed ``time_component_units`` so a reader does not decode
a day-of-month into a duration.

ATMS ``ATMS_BUFR`` (the same brightness temperatures as the SDR, in BUFR) and
``ATMS-SCIENCE-RDR`` (raw packets) are also rolling but are not archived: the first
duplicates the permanent SDR, the second needs the IDPS processing chain to be of use.
"""

from __future__ import annotations

import pathlib
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.common import download_many
from planetary_datasets.common.store import ALIGNMENT_COORDS
from planetary_datasets.providers.polar._granule import mid_time, serialize_dataset_attrs
from planetary_datasets.providers.polar._jpss import (
    BUCKETS,
    GRANULE_TIME_ENCODING,
    PLATFORM_NAMES,
    IncompleteDownload,
    JpssGranuleProvider,
    nde_granule_end,
    nde_granule_key,
    nde_granule_start,
)

#: Nominal ATMS granule shape. Short granules are padded up to this so they concatenate.
GRANULE_SHAPE = {"y": 12, "x": 96}

#: Source dimension names and what they become.
DIM_RENAMES = {"Scanline": "y", "Field_of_view": "x"}

#: Static variables that become coordinates when present.
STATIC_COORDS = ("Freq", "Polo", "Plevel", "Player")

#: Global attributes that do not change between granules and are worth keeping.
KEPT_ATTRS = ("title", "institution", "project", "processing_level", "Conventions")

#: Units xarray would decode an integer into a timedelta for.
DURATION_UNITS = ("days", "hours", "minutes", "seconds")


def source_packing(var: xr.Variable) -> Dict[str, Any] | None:
    """How a decoded variable was packed in its source file, or None if it was not.

    An integer variable with a scale factor or a fill value is decoded by xarray to
    floats; storing those floats would double or quadruple it and gain nothing.
    Returning the source's packing lets it be written back the same way.
    """
    dtype = var.encoding.get("dtype")
    if dtype is None or np.dtype(dtype).kind not in "iu":
        return None
    if not any(key in var.encoding for key in ("scale_factor", "add_offset", "_FillValue")):
        return None
    packing: Dict[str, Any] = {"dtype": np.dtype(dtype).name}
    for key in ("scale_factor", "add_offset"):
        if key in var.encoding:
            packing[key] = var.encoding[key]
    if "_FillValue" in var.encoding:
        packing["_FillValue"] = np.dtype(dtype).type(var.encoding["_FillValue"])
    return packing


class JpssNdeL2Provider(JpssGranuleProvider):
    """An NDE level 2 product on the ATMS granule grid, for one spacecraft.

    Subclasses set :attr:`product`, :attr:`base_name` and :attr:`time_chunk`.

    Attributes:
        product: Bucket prefix, e.g. ``ATMS-SFR``.
        base_name: Provider name without the spacecraft, e.g. ``jpss_atms_sfr``.
        time_chunk: Granules per chunk along ``time``. One granule is a few KB per field,
            so chunks span several — but well under the ~112 granules of an hourly
            partition, because every append rewrites the partly filled last chunk and the
            superseded copy stays in the repository until it is garbage collected.
    """

    product: str
    base_name: str
    time_chunk: int = 32
    append_dim = "time"
    window = pd.Timedelta(1, "h")
    alignment_coords = (*ALIGNMENT_COORDS, *STATIC_COORDS)

    key_of = staticmethod(nde_granule_key)
    start_of = staticmethod(nde_granule_start)
    end_of = staticmethod(nde_granule_end)

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
        self.name = f"{self.base_name}_{satellite}"
        self.store_prefix = f"bkr/polar/{self.name}.icechunk"
        self.granule_limit = granule_limit
        self.download_workers = download_workers
        self._packing: Dict[str, Dict[str, Any]] = {}

    # ------------------------------------------------------------------ fetch

    def granule_keys(self, it: pd.Timestamp) -> List[str]:
        """``bucket/key`` of every granule this partition owns, oldest first."""
        fs = self._s3()
        start, end = self.partition_window(it)
        indexed: Dict[tuple, str] = {}
        for day in self._days_to_list(start, end):
            listed = self._list_day(fs, self.bucket, self.product, day)
            indexed.update(self._index_keys(listed, window=(start, end), owned=True))
        found = sorted(indexed.values(), key=self.start_of)
        if self.granule_limit is not None:
            found = found[: self.granule_limit]
        return found

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Download every granule the partition owns.

        Raises:
            IncompleteDownload: when any file failed after retries.
        """
        keys = self.granule_keys(it)
        if not keys:
            return []
        dest = pathlib.Path(temp_dir) if temp_dir is not None else self.config.data_dir / self.name
        # ``fs.ls`` returns "bucket/key"; strip the bucket back off for the URL.
        urls = [self._https_url(self.bucket, path.split("/", 1)[1]) for path in keys]
        paths = download_many(urls, dest, workers=self.download_workers)
        if len(paths) != len(urls):
            raise IncompleteDownload(
                f"{self.name}: only {len(paths)}/{len(urls)} files downloaded for {it}"
            )
        logger.info(f"{self.name}: fetched {len(paths)} granules for {it}")
        return [str(p) for p in paths]

    # ------------------------------------------------------------------ process

    def process_granule(self, path: str) -> xr.Dataset | None:
        """Read one granule into a single-step dataset, or None if it is oversized."""
        name = pathlib.Path(path).name
        with xr.open_dataset(path, decode_times=False, decode_timedelta=False) as src:
            ds = src.load()

        ds = ds.drop_vars([v for v in ds.data_vars if ds[v].ndim == 0])
        static = [v for v in STATIC_COORDS if v in ds.data_vars]
        ds = ds.set_coords(static)
        moved = [c for c in ("Latitude", "Longitude") if c in ds.coords]
        ds = ds.reset_coords(moved).rename(
            {c: c.lower() for c in ("Latitude", "Longitude") if c in ds.variables}
        )
        ds = ds.rename_dims({k: v for k, v in DIM_RENAMES.items() if k in ds.dims})

        for var in ds.data_vars:
            packing = source_packing(ds[var].variable)
            if packing is not None:
                self._packing.setdefault(str(var), packing)
            if ds[var].attrs.get("units") in DURATION_UNITS:
                ds[var].attrs["time_component_units"] = ds[var].attrs.pop("units")

        ds = self.fit_to_shape(ds, GRANULE_SHAPE)
        if ds is None:
            return None

        start, end = self.start_of(name), self.end_of(name)
        # expand_dims only touches data variables, so the static coordinates stay static.
        ds = ds.expand_dims("time").assign_coords(time=[mid_time(start, end)])
        ds["start_time"] = xr.DataArray(np.array([start], dtype="datetime64[ns]"), dims="time")
        ds["end_time"] = xr.DataArray(np.array([end], dtype="datetime64[ns]"), dims="time")
        ds["platform_name"] = xr.DataArray(np.array([PLATFORM_NAMES[self.satellite]]), dims="time")

        attrs = {key: ds.attrs[key] for key in KEPT_ATTRS if key in ds.attrs}
        attrs["platform"] = PLATFORM_NAMES[self.satellite]
        attrs["source"] = f"s3://{self.bucket}/{self.product}"
        ds.attrs = attrs
        for var in ds.variables:
            ds[var].encoding = {}
        return serialize_dataset_attrs(ds)

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Read every granule and concatenate them on time."""
        datasets = []
        for path in sorted(input_files, key=self.start_of):
            try:
                granule = self.process_granule(path)
            except (OSError, KeyError, ValueError) as exc:
                logger.warning(f"{self.name}: granule {path} failed: {exc}")
                continue
            if granule is not None:
                datasets.append(granule)
        return self.restrict_to_window(self.concat_granules(datasets), it)

    def chunking(self, ds: xr.Dataset) -> Dict[str, Dict[str, Any]]:
        """Chunks of :attr:`time_chunk` granules for every time-dependent variable."""
        out = {}
        for name, var in ds.data_vars.items():
            if self.append_dim in var.dims:
                chunks = tuple(
                    self.time_chunk if dim == self.append_dim else var.sizes[dim]
                    for dim in var.dims
                )
                out[str(name)] = {"chunks": chunks}
        return out

    def write_to_icechunk(self, repo, processed: xr.Dataset) -> bool:
        """Write with the source's packing and :meth:`chunking` when creating the store.

        Both only take effect on the first write; appends reuse the stored encoding.
        """
        overrides = self.chunking(processed)
        for name, packing in self._packing.items():
            if name in processed.data_vars:
                overrides[name] = {**overrides.get(name, {}), **packing}
        for name in ("start_time", "end_time"):
            overrides[name] = {**overrides.get(name, {}), **GRANULE_TIME_ENCODING}
        self.encoding_overrides = overrides
        return super().write_to_icechunk(repo, processed)


class JpssAtmsSfrProvider(JpssNdeL2Provider):
    """ATMS snowfall rate (SFR v2) for one JPSS spacecraft."""

    base_name = "jpss_atms_sfr"
    product = "ATMS-SFR"
    time_chunk = 64


class JpssMirsImgProvider(JpssNdeL2Provider):
    """MiRS ATMS surface and column retrievals (``NPR_MIRS_IMG``) for one spacecraft."""

    base_name = "jpss_mirs_img"
    product = "NPR_MIRS_IMG"
    time_chunk = 32


class JpssMirsSndProvider(JpssNdeL2Provider):
    """MiRS ATMS temperature, moisture and hydrometeor profiles (``NPR_MIRS_SND``)."""

    base_name = "jpss_mirs_snd"
    product = "NPR_MIRS_SND"
    time_chunk = 8
