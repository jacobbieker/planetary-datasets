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
from planetary_datasets.providers.polar._granule import (
    GranuleProvider,
    mid_time,
    serialize_dataset_attrs,
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


def granule_key(filename: str) -> Tuple[str, ...]:
    """Identity of a granule, shared by its SDR and geolocation file.

    ``SATMS_j02_d20250601_t0000244_e0000560_b13247_c2025…_oeac_ops.h5`` and the matching
    ``GATMO_…`` differ only in the product prefix and creation stamp, so the spacecraft,
    day, start, end and orbit fields identify the pair. The spacecraft is part of the key
    because all three are ingested into one store and are paired up together.
    """
    parts = pathlib.Path(filename).name.split("_")
    if len(parts) < 6:
        raise ValueError(f"not an ATMS granule filename: {filename}")
    return tuple(parts[1:6])


def _time_of_day(day: str, field: str) -> pd.Timestamp:
    """Decode a ``HHMMSSd`` filename field — hours, minutes, seconds and tenths."""
    tod = field[1:]
    stamp = pd.Timestamp(f"{day}T{tod[0:2]}:{tod[2:4]}:{tod[4:6]}")
    return stamp + pd.Timedelta(milliseconds=int(tod[6]) * 100)


def granule_start(filename: str) -> pd.Timestamp:
    """Sensing start time encoded in an ATMS granule filename."""
    parts = pathlib.Path(filename).name.split("_")
    return _time_of_day(parts[2][1:], parts[3])


def granule_end(filename: str) -> pd.Timestamp:
    """Sensing end time encoded in an ATMS granule filename.

    The day is only in the filename once, so a granule running over midnight has an end
    field earlier than its start field; that rolls over to the following day.
    """
    parts = pathlib.Path(filename).name.split("_")
    day = parts[2][1:]
    start = _time_of_day(day, parts[3])
    end = _time_of_day(day, parts[4])
    return end + pd.Timedelta("1D") if end < start else end


def granule_overlaps(filename: str, start: pd.Timestamp, end: pd.Timestamp) -> bool:
    """True when a granule's sensing interval intersects ``[start, end)``.

    Selection has to be by overlap, not by start time: the timestamp a granule is stored
    under is the middle of its sensing interval, so a granule starting just before the
    hour is owned by the *next* partition. Selecting on start alone would have neither
    partition keep it — this one discards it as out of window, and the next one never
    looks at it.
    """
    return granule_start(filename) < end and granule_end(filename) > start


class JpssAtmsProvider(GranuleProvider):
    """ATMS SDR brightness temperatures for the JPSS constellation."""

    name = "jpss_atms"
    append_dim = "time"
    store_prefix = "bkr/polar/jpss_atms.icechunk"
    window = pd.Timedelta("1h")

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

    @staticmethod
    def _https_url(bucket: str, key: str) -> str:
        return f"https://{bucket}.s3.amazonaws.com/{key}"

    def _list_day(self, fs, bucket: str, prefix: str, day: pd.Timestamp) -> List[str]:
        path = f"{bucket}/{prefix}/{day.year:04d}/{day.month:02d}/{day.day:02d}/"
        try:
            return [str(key) for key in fs.ls(path, detail=False)]
        except FileNotFoundError:
            logger.debug(f"{self.name}: no listing for {path}")
            return []

    def _index_keys(
        self,
        keys: List[str],
        window: Tuple[pd.Timestamp, pd.Timestamp] | None = None,
    ) -> Dict[Tuple[str, ...], str]:
        """Index listed object keys by granule, skipping anything unparseable.

        A day prefix occasionally holds an object that is not a granule; one of them must
        not take the partition down with it.
        """
        indexed: Dict[Tuple[str, ...], str] = {}
        for key in keys:
            try:
                if window is not None and not granule_overlaps(key, *window):
                    continue
                indexed[granule_key(key)] = key
            except (ValueError, IndexError) as exc:
                logger.warning(f"{self.name}: ignoring unparseable key {key} ({exc})")
        return indexed

    def _days_to_list(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DatetimeIndex:
        """Day prefixes that can hold a granule overlapping ``[start, end)``.

        A granule is filed under the day it *started*, so a window beginning exactly at
        midnight also has to look at the day before. Any later window cannot, which keeps
        the usual case to a single listing.
        """
        first = start.normalize()
        if start == first:
            first -= pd.Timedelta("1D")
        return pd.date_range(first, end.normalize(), freq="1D")

    def granule_pairs(self, it: pd.Timestamp) -> List[Tuple[str, str, str]]:
        """``(bucket, sdr_key, geo_key)`` for every granule overlapping the partition."""
        import s3fs

        fs = s3fs.S3FileSystem(anon=True)
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
