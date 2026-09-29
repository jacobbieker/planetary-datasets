"""Publish partitions staged by the earth2studio observation downloader.

The processing half of the earth2studio observation pipeline. The download half,
:mod:`planetary_datasets.providers.earth2studio_download`, runs in the
``docker/earth2studio`` image and stages each partition under ``E2S_OBS_ARCHIVE_DIR``
(default ``<data_dir>/earth2studio``), finishing with a JSON manifest. The providers here
publish what it staged and then delete it:

:class:`ParquetObsProvider`
    ``table`` datasets. The staged Parquet file becomes one partition of a Parquet dataset
    at ``bkr/earth2studio/<name>.parquet``; see
    :class:`~planetary_datasets.common.parquet.ParquetSink`.
:class:`GridObsProvider`
    ``grid`` and ``granules`` datasets, appended to ``bkr/earth2studio/<name>.icechunk``
    one staged file at a time, so a full-disk hour never has to be in memory at once.

:func:`provider_for` builds the right one for a dataset name.
"""

from __future__ import annotations

import json
import os
import pathlib
from typing import Any, List

import icechunk
import pandas as pd
import pyarrow.parquet as pq
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common.parquet import ParquetSink
from planetary_datasets.common.store import ALIGNMENT_COORDS, existing_times, has_committed_data
from planetary_datasets.config import Config, get_config
from planetary_datasets.memory import memory_guard, require_dataset_fits
from planetary_datasets.providers.earth2studio_download import (
    DATASETS,
    Dataset,
    manifest_path,
    partition_window,
    staged_dir,
)
from planetary_datasets.providers.radar import to_naive_utc

ARCHIVE_ENV = "E2S_OBS_ARCHIVE_DIR"
ARCHIVE_SUBDIR = "earth2studio"
STORE_ROOT = "bkr/earth2studio"

#: Spatial chunk edge for gridded stores. A GOES full-disk frame is 5424 square, and one
#: chunk per frame would make every read fetch 118 MB per band.
SPATIAL_CHUNK = 1024


class StagedPartitionError(RuntimeError):
    """A staged partition does not hold what its manifest says."""


class _StagedMixin:
    """Locating, reading and discarding one dataset's staged partitions."""

    dataset: Dataset

    @property
    def archive_root(self) -> pathlib.Path:
        """The staging root the downloader image is given, shared by every dataset."""
        raw = (os.environ.get(ARCHIVE_ENV) or "").strip()
        if raw:
            return pathlib.Path(raw).expanduser()
        return self.config.data_dir / ARCHIVE_SUBDIR

    def window(self, it) -> tuple[pd.Timestamp, pd.Timestamp]:
        """``[start, end)`` of the partition starting at ``it``."""
        start, end = partition_window(self.dataset, to_naive_utc(it).to_pydatetime())
        return pd.Timestamp(start), pd.Timestamp(end)

    def manifest(self, it) -> dict[str, Any] | None:
        """The staged partition's manifest, or None when it is not (fully) staged."""
        start, _ = self.window(it)
        path = manifest_path(self.archive_root, self.dataset, start.to_pydatetime())
        if not path.is_file():
            return None
        return json.loads(path.read_text())

    def staged_files(self, it) -> List[pathlib.Path]:
        """The files a staged partition's manifest lists, in order."""
        manifest = self.manifest(it)
        if manifest is None:
            return []
        start, _ = self.window(it)
        directory = staged_dir(self.archive_root, self.dataset, start.to_pydatetime())
        files = [directory / name for name in manifest["files"]]
        absent = [f.name for f in files if not f.is_file()]
        if absent:
            raise StagedPartitionError(f"{self.dataset.name}: manifest lists missing {absent}")
        return files

    def _remove_staged(self, it) -> List[pathlib.Path]:
        start, _ = self.window(it)
        manifest = manifest_path(self.archive_root, self.dataset, start.to_pydatetime())
        removed = []
        if manifest.is_file():
            for path in self.staged_files(it):
                path.unlink(missing_ok=True)
                removed.append(path)
            manifest.unlink()
            removed.append(manifest)
        return removed


class ParquetObsProvider(_StagedMixin):
    """Publishes a ``table`` dataset's staged partitions to a Parquet dataset.

    Not a :class:`~planetary_datasets.base.BaseProvider`: that class is built around an
    icechunk store, and a Parquet partition is written whole in one put.
    """

    def __init__(self, name: str, config: Config | None = None):
        """Build the publisher for ``table`` dataset ``name``."""
        self.dataset = DATASETS[name]
        if self.dataset.publish != "parquet":
            raise ValueError(f"{name} is published to {self.dataset.publish}, not parquet")
        self.name = name
        self.store_prefix = f"{STORE_ROOT}/{name}.parquet"
        self._config = config
        self.sink = ParquetSink(self.store_prefix, config)

    @property
    def config(self) -> Config:
        """Configuration for this provider, defaulting to the process-wide config."""
        return self._config if self._config is not None else get_config()

    @property
    def store_path(self) -> str:
        """Where the dataset is published."""
        return self.sink.root

    def partition_stored(self, it) -> bool:
        """True when the partition starting at ``it`` has been published."""
        return self.sink.exists(self.window(it)[0])

    def appendable(self, it) -> bool:
        """A Parquet dataset accepts partitions in any order."""
        return True

    def run_partition(self, it) -> bool:
        """Publish one staged partition. Returns True if it was written."""
        start, end = self.window(it)
        if self.partition_stored(start):
            logger.debug(f"{self.name}: {start} already published")
            return False
        files = self.staged_files(start)
        if not files:
            logger.debug(f"{self.name}: nothing staged for {start}")
            return False
        (path,) = files
        table = pq.read_table(path)
        if table.num_rows:
            times = pd.DatetimeIndex(table.column("time").to_pandas())
            if times.min() < start or times.max() >= end:
                raise StagedPartitionError(
                    f"{self.name}: {path.name} has rows outside [{start}, {end})"
                )
        self.sink.write(start, table)
        return True

    def discard_staged(self, it) -> List[pathlib.Path]:
        """Delete the staged partition once it has been published."""
        if not self.partition_stored(it):
            return []
        return self._remove_staged(it)


class GridObsProvider(_StagedMixin, BaseProvider):
    """Appends a ``grid`` or ``granules`` dataset's staged partitions to icechunk."""

    append_dim = "time"
    alignment_coords = (*ALIGNMENT_COORDS, "x", "y", "tile")

    def __init__(self, name: str, config: Config | None = None):
        """Build the publisher for ``grid`` or ``granules`` dataset ``name``."""
        super().__init__(config)
        self.dataset = DATASETS[name]
        if self.dataset.publish != "icechunk":
            raise ValueError(f"{name} is published to {self.dataset.publish}, not icechunk")
        self.name = name
        self.store_prefix = f"{STORE_ROOT}/{name}.icechunk"

    def _stored_times(self) -> pd.DatetimeIndex:
        return pd.DatetimeIndex(existing_times(self.get_icechunk_repo(), self.append_dim))

    def partition_stored(self, it) -> bool:
        """True when any of the partition's frames is in the store.

        Frames are committed in time order, so one present means the partition was
        written; a partial one cannot be completed anyway (see :meth:`appendable`).
        """
        start, end = self.window(it)
        stored = self._stored_times()
        return bool(((stored >= start) & (stored < end)).any())

    def appendable(self, it) -> bool:
        """True when the store would accept this partition: it starts after the end."""
        stored = self._stored_times()
        return stored.empty or self.window(it)[0] > stored.max()

    def fetch(self, it, temp_dir=None, **kwargs) -> List[str]:
        """The staged files of this partition; empty when none is staged."""
        return [str(p) for p in self.staged_files(it)]

    def process(self, input_files, it, temp_dir=None, **kwargs) -> xr.Dataset:
        """Read staged files and check they belong to this partition."""
        start, end = self.window(it)
        parts = []
        for path in input_files:
            with xr.open_dataset(path, engine="h5netcdf") as staged:
                parts.append(staged.load().drop_encoding())
        ds = xr.concat(parts, dim="time", join="exact") if len(parts) > 1 else parts[0]
        # Geolocation requested as a variable (Sentinel-3) is staged as latitude/longitude.
        missing = set(self.dataset.variables) - {"s3sy_lat", "s3sy_lon"} - set(ds.data_vars)
        if missing:
            raise StagedPartitionError(f"{self.name}: staged partition lacks {sorted(missing)}")
        times = pd.DatetimeIndex(ds["time"].values)
        if (times < start).any() or (times >= end).any():
            raise StagedPartitionError(f"{self.name}: staged frames fall outside [{start}, {end})")
        return ds.sortby("time")

    def prepare_for_write(self, processed: xr.Dataset) -> xr.Dataset:
        """Chunk one frame per chunk in time and ``SPATIAL_CHUNK`` in space."""
        chunks = {}
        for dim, size in processed.sizes.items():
            if dim == self.append_dim or dim == "tile":
                chunks[dim] = 1
            else:
                chunks[dim] = min(size, SPATIAL_CHUNK)
        return processed.chunk(chunks)

    def write_to_icechunk(self, repo: icechunk.Repository, processed: xr.Dataset) -> bool:
        """Write, leaving out static 2-D geolocation once the store has it.

        Full-disk latitude/longitude are hundreds of MB and never change; xarray would
        otherwise rewrite them on every append. The 1-D ``x``/``y`` axes still guard the
        grid, because :func:`~planetary_datasets.common.dataset.coords_match` compares
        every alignment coordinate present on both sides.
        """
        if has_committed_data(repo):
            static = [
                name
                for name in processed.coords
                if processed[name].ndim > 1 and self.append_dim not in processed[name].dims
            ]
            processed = processed.drop_vars(static)
        return super().write_to_icechunk(repo, processed)

    def run_partition(self, it, check_present: bool = True) -> bool:
        """Append the partition one staged file at a time. True if anything was written."""
        start, _ = self.window(it)
        if check_present and self.partition_stored(start):
            logger.debug(f"{self.name}: {start} already in {self.store_path}")
            return False
        files = self.fetch(start)
        if not files:
            logger.debug(f"{self.name}: nothing staged for {start}")
            return False
        repo = self.get_icechunk_repo()
        written = False
        for path in files:
            with memory_guard(what=f"{self.name} {start} {pathlib.Path(path).name}"):
                ds = self.process([path], start)
                require_dataset_fits(ds, what=f"{self.name} {start}")
            written = self.write_to_icechunk(repo, ds) or written
        return written

    def discard_staged(self, it) -> List[pathlib.Path]:
        """Delete the staged partition once it is stored or can no longer be."""
        if not (self.partition_stored(it) or not self.appendable(it)):
            return []
        return self._remove_staged(it)


def provider_for(name: str, config: Config | None = None):
    """The publishing provider for dataset ``name``."""
    dataset = DATASETS[name]
    if dataset.publish == "parquet":
        return ParquetObsProvider(name, config)
    return GridObsProvider(name, config)


__all__ = [
    "ARCHIVE_ENV",
    "GridObsProvider",
    "ParquetObsProvider",
    "STORE_ROOT",
    "StagedPartitionError",
    "provider_for",
]
