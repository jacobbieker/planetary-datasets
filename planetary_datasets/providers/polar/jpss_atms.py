"""JPSS ATMS microwave sounder granules from the NOAA open data buckets.

ATMS SDR brightness temperatures live in ``noaa-nesdis-<sat>-pds`` alongside a matching
geolocation product, one pair of HDF5 files per 32-second granule. NOAA-21, NOAA-20 and
Suomi-NPP are read together and written to the ``atms`` group of
``bkr/polar/jpss.icechunk``, one row per granule along ``granule`` (see
:class:`~planetary_datasets.providers.polar._jpss.JpssProductProvider`), with the swath on
``(y, x) = (12, 96)``.

The SDRs are kept on AWS indefinitely, unlike the products beside them in the store.

Replaces ``dags/assets/icechunky/jpss.py`` and ``jpss_combine.py``, which globbed a USB
disk and wrote to a store path baked into the file. The earlier ``jpss_atms.icechunk``
store, which interleaved the constellation on a whole-second ``time`` axis, is no longer
written.
"""

from __future__ import annotations

import pathlib
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.common import download_many
from planetary_datasets.providers.polar._granule import serialize_dataset_attrs
from planetary_datasets.providers.polar._jpss import (  # noqa: F401 - re-exported
    BUCKETS,
    JpssProductProvider,
    granule_end,
    granule_key,
    granule_overlaps,
    granule_start,
    idps_satellite,
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
    "n21": {"bucket": BUCKETS["n21"], "start": "2023-02-27"},
    "n20": {"bucket": BUCKETS["n20"], "start": "2022-11-07"},
    "snpp": {"bucket": BUCKETS["snpp"], "start": "2022-11-07"},
}

SDR_PREFIX = "ATMS-SDR"
GEO_PREFIX = "ATMS-SDR-GEO"

#: Variables that keep full precision; everything else is stored as float16.
FULL_PRECISION = ("latitude", "longitude", "time", "start_time", "end_time", "platform_name")


class JpssAtmsProvider(JpssProductProvider):
    """ATMS SDR brightness temperatures for the JPSS constellation."""

    name = "jpss_atms"
    group = "atms"
    source_product = f"{SDR_PREFIX}, {GEO_PREFIX}"
    satellite_starts = {sat: spec["start"] for sat, spec in SATELLITES.items()}
    #: ~2 KB per field per granule as float16; an hour of three spacecraft is ~340.
    granule_chunk = 64

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
        super().__init__(
            config=config,
            satellites=satellites,
            granule_limit=granule_limit,
            download_workers=download_workers,
        )

    # ------------------------------------------------------------------ fetch

    def granule_pairs(self, it: pd.Timestamp) -> List[Tuple[str, str, str]]:
        """``(bucket, sdr_key, geo_key)`` for every unstored granule overlapping the hour."""
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

        wanted = set(self.drop_stored([sdr for _, sdr, _ in pairs]))
        pairs = [pair for pair in pairs if pair[1] in wanted]
        pairs.sort(key=lambda pair: granule_start(pair[1]))
        if self.granule_limit is not None:
            pairs = pairs[: self.granule_limit]
        return pairs

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Download the SDR and geolocation file of every unstored granule in the hour."""
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
        """Read one ATMS granule and its geolocation into a one-granule dataset.

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

        name = pathlib.Path(sdr_file).name
        ds = self.on_granule_axis(
            ds,
            idps_satellite(name),
            granule_start(name),
            pd.Timestamp(attrs["start_time"]),
            pd.Timestamp(attrs["end_time"]),
        )

        ds = ds.load()
        for var in ds.data_vars:
            if var in ("latitude", "longitude"):
                ds[var] = ds[var].astype(np.float32)
            elif var not in FULL_PRECISION:
                ds[var] = ds[var].astype(np.float16)

        # Anything naming one spacecraft or one granule would describe only the first
        # granule of the group; those live on the granule axis instead.
        kept = {
            key: value
            for key, value in attrs.items()
            if key not in ("start_time", "end_time") and "platform" not in key
        }
        ds.attrs = {**kept, **self.group_attrs("ATMS")}
        ds = serialize_dataset_attrs(ds)
        return ds.drop_vars("crs", errors="ignore")

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Pair the downloaded files back up by granule and stack them on ``granule``."""
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
