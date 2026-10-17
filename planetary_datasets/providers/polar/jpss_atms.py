"""JPSS ATMS microwave sounder granules from the NOAA open data buckets.

ATMS SDR brightness temperatures live in ``noaa-nesdis-<sat>-pds`` alongside a matching
geolocation product, one pair of HDF5 files per 32-second granule. NOAA-21, NOAA-20 and
Suomi-NPP are read together and interleaved on ``time``, so one store holds the whole
constellation.

Replaces ``dags/assets/icechunky/jpss.py`` and ``jpss_combine.py``, which globbed a USB
disk and wrote to a store path baked into the file.
"""

from __future__ import annotations

import pathlib
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.common import download_many
from planetary_datasets.providers.polar._granule import mid_time, serialize_dataset_attrs
from planetary_datasets.providers.polar._jpss import (  # noqa: F401 - re-exported
    JpssGranuleProvider,
    granule_end,
    granule_key,
    granule_overlaps,
    granule_start,
)

#: The 22 ATMS channels plus the geolocation and geometry fields satpy exposes.
ATMS_VARIABLES = [str(channel) for channel in range(1, 23)] + [
    "lat",
    "lon",
    "sat_azi",
    "sat_zen",
    "sol_azi",
    "sol_zen",
    "surf_alt",
]

#: Nominal granule shape. Short granules are padded up to this so they concatenate.
GRANULE_SHAPE = {"y": 12, "x": 96}

#: Public NOAA open-data bucket per spacecraft, and the date each archive starts.
SATELLITES: Dict[str, Dict[str, str]] = {
    "n21": {"bucket": "noaa-nesdis-n21-pds", "start": "2023-02-27"},
    "n20": {"bucket": "noaa-nesdis-n20-pds", "start": "2022-11-07"},
    "snpp": {"bucket": "noaa-nesdis-snpp-pds", "start": "2022-11-07"},
}

SDR_PREFIX = "ATMS-SDR"
GEO_PREFIX = "ATMS-SDR-GEO"

#: Variables that keep full precision; everything else is stored as float16.
FULL_PRECISION = ("latitude", "longitude", "start_time", "end_time", "platform_name")


class JpssAtmsProvider(JpssGranuleProvider):
    """ATMS SDR brightness temperatures for the JPSS constellation."""

    name = "jpss_atms"
    append_dim = "time"
    store_prefix = "bkr/polar/jpss_atms.icechunk"
    window = pd.Timedelta(1, "h")

    def __init__(
        self,
        config=None,
        satellites: tuple[str, ...] = ("n21", "n20", "snpp"),
        granule_limit: int | None = None,
        download_workers: int = 8,
    ):
        """Build a provider for some or all of the JPSS constellation.

        Args:
            config: Override the process-wide configuration.
            satellites: Subset of :data:`SATELLITES` to ingest.
            granule_limit: Stop after this many granule pairs per partition. For smoke
                tests against the live archive, not for production backfills.
            download_workers: Concurrent downloads.
        """
        super().__init__(config=config)
        unknown = set(satellites) - set(SATELLITES)
        if unknown:
            raise ValueError(f"unknown JPSS satellites: {sorted(unknown)}")
        self.satellites = satellites
        self.granule_limit = granule_limit
        self.download_workers = download_workers

    # ------------------------------------------------------------------ fetch

    def granule_pairs(self, it: pd.Timestamp) -> List[Tuple[str, str, str]]:
        """``(bucket, sdr_key, geo_key)`` for every granule overlapping the partition."""
        fs = self._s3()
        start, end = self.partition_window(it)
        days = self._days_to_list(start, end)

        pairs: List[Tuple[str, str, str]] = []
        for satellite in self.satellites:
            spec = SATELLITES[satellite]
            if end <= pd.Timestamp(spec["start"]):
                continue
            sdr_keys: Dict[Tuple[str, ...], str] = {}
            geo_keys: Dict[Tuple[str, ...], str] = {}
            for day in days:
                listed = self._list_day(fs, spec["bucket"], SDR_PREFIX, day)
                sdr_keys.update(self._index_keys(listed, window=(start, end)))
                listed = self._list_day(fs, spec["bucket"], GEO_PREFIX, day)
                geo_keys.update(self._index_keys(listed))

            for key in sorted(sdr_keys):
                geo = geo_keys.get(key)
                if geo is None:
                    logger.warning(f"{self.name}: no geolocation for {sdr_keys[key]}, skipping")
                    continue
                pairs.append((spec["bucket"], sdr_keys[key], geo))

        pairs.sort(key=lambda pair: granule_start(pair[1]))
        if self.granule_limit is not None:
            pairs = pairs[: self.granule_limit]
        return pairs

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Download the SDR and geolocation file of every granule in the partition."""
        pairs = self.granule_pairs(it)
        if not pairs:
            return []

        dest = pathlib.Path(temp_dir) if temp_dir is not None else self.config.data_dir / self.name
        urls = []
        for bucket, sdr, geo in pairs:
            # ``fs.ls`` returns "bucket/key"; strip the bucket back off for the URL.
            urls.append(self._https_url(bucket, sdr.split("/", 1)[1]))
            urls.append(self._https_url(bucket, geo.split("/", 1)[1]))

        paths = download_many(urls, dest, workers=self.download_workers)
        logger.info(f"{self.name}: fetched {len(paths)}/{len(urls)} files for {it}")
        return [str(p) for p in paths]

    # ------------------------------------------------------------------ process

    def process_granule(self, sdr_file: str, geo_file: str) -> xr.Dataset | None:
        """Read one ATMS granule and its geolocation into a single-timestep dataset.

        Returns None for a granule larger than :data:`GRANULE_SHAPE`, which cannot be
        concatenated with the rest and would otherwise lose the whole partition.
        """
        from satpy import Scene

        scene = Scene(reader="atms_sdr_hdf5", filenames=[sdr_file, geo_file])
        scene.load(ATMS_VARIABLES)

        longitude, latitude = scene[ATMS_VARIABLES[0]].attrs["area"].get_lonlats()
        ds = scene.to_xarray_dataset()
        ds["longitude"] = (("y", "x"), longitude)
        ds["latitude"] = (("y", "x"), latitude)

        attrs = dict(ds.attrs)
        ds = self.fit_to_shape(ds, GRANULE_SHAPE)
        if ds is None:
            return None
        ds.attrs = attrs

        start = pd.Timestamp(ds.attrs["start_time"])
        end = pd.Timestamp(ds.attrs["end_time"])
        ds = ds.expand_dims("time").assign_coords(time=[mid_time(start, end)])
        ds["start_time"] = xr.DataArray([start], dims=["time"]).astype("datetime64[ns]")
        ds["end_time"] = xr.DataArray([end], dims=["time"]).astype("datetime64[ns]")
        ds["platform_name"] = xr.DataArray([str(ds.attrs["platform_name"])], dims=["time"])

        ds = ds.load()
        for var in ds.data_vars:
            if var in ("latitude", "longitude"):
                ds[var] = ds[var].astype(np.float32)
            elif var not in FULL_PRECISION:
                ds[var] = ds[var].astype(np.float16)

        ds = serialize_dataset_attrs(ds, drop=("start_time", "end_time", "platform_name"))
        return ds.drop_vars("crs", errors="ignore")

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Pair the downloaded files back up by granule and concatenate them on time."""
        sdr = {}
        geo = {}
        for path in input_files:
            name = pathlib.Path(path).name
            if name.startswith("SATMS"):
                sdr[granule_key(name)] = path
            elif name.startswith("GATMO"):
                geo[granule_key(name)] = path

        datasets = []
        for key in sorted(sdr, key=lambda k: granule_start(sdr[k])):
            if key not in geo:
                logger.warning(f"{self.name}: geolocation missing for {sdr[key]}, skipping granule")
                continue
            try:
                granule = self.process_granule(sdr[key], geo[key])
            except Exception as exc:  # noqa: BLE001 - one bad granule must not lose the hour
                logger.warning(f"{self.name}: granule {sdr[key]} failed: {exc}")
                continue
            if granule is not None:
                datasets.append(granule)

        return self.restrict_to_window(self.concat_granules(datasets), it)
