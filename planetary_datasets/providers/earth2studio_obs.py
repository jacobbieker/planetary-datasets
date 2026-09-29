"""Publish partitions staged by the earth2studio observation downloader.

The processing half of the earth2studio observation pipeline. The download half,
:mod:`planetary_datasets.providers.earth2studio_download`, runs in the
``docker/earth2studio`` image and stages each partition under ``E2S_OBS_ARCHIVE_DIR``
(default ``<data_dir>/earth2studio``), finishing with a JSON manifest. The providers here
publish what it staged and then delete it:

:class:`ParquetObsProvider`
    ``table`` datasets. The staged Parquet file becomes one partition of a Parquet dataset
    at ``bkr/obs/<name>.parquet``; see
    :class:`~planetary_datasets.common.parquet.ParquetSink`.
:class:`GridObsProvider`
    ``grid`` and ``granules`` datasets, appended to ``bkr/obs/<name>.icechunk``
    one staged file at a time, so a full-disk hour never has to be in memory at once.

:func:`provider_for` builds the right one for a dataset name. Both satisfy
:class:`dags.staged.StagedPublisher`.
"""

from __future__ import annotations

import json
import os
import pathlib
from typing import Any, List

import pandas as pd
import pyarrow.parquet as pq
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common.parquet import ParquetSink
from planetary_datasets.common.store import ALIGNMENT_COORDS, existing_times
from planetary_datasets.config import Config, get_config
from planetary_datasets.memory import memory_guard, require_dataset_fits
from planetary_datasets.providers._timestamps import to_naive_utc
from planetary_datasets.providers.earth2studio_download import (
    DATASETS,
    Dataset,
    manifest_path,
    partition_window,
    staged_dir,
)

ARCHIVE_ENV = "E2S_OBS_ARCHIVE_DIR"
ARCHIVE_SUBDIR = "earth2studio"
#: Both kinds of store go with the project's other observation datasets.
STORE_ROOT = "bkr/obs"

#: Spatial chunk edge for gridded stores. A GOES full-disk frame is 5424 square, and one
#: chunk per frame would make every read fetch 118 MB per band.
SPATIAL_CHUNK = 1024


class StagedPartitionError(RuntimeError):
    """A staged partition does not hold what its manifest says."""


class _StagedMixin:
    """What both publishers share: identity, the staging directory, and its cleanup.

    Subclasses provide ``appendable``: whether the store would take the partition now.
    """

    dataset: Dataset

    @property
    def name(self) -> str:
        """Store and asset name."""
        return self.dataset.name

    @property
    def store_prefix(self) -> str:
        """Location of the store relative to the configured bucket."""
        return f"{STORE_ROOT}/{self.dataset.name}.{self.dataset.publish}"

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

    def _manifest_path(self, it) -> pathlib.Path:
        return manifest_path(self.archive_root, self.dataset, self.window(it)[0].to_pydatetime())

    def manifest(self, it) -> dict[str, Any] | None:
        """The staged partition's manifest, or None when it is not (fully) staged."""
        path = self._manifest_path(it)
        return json.loads(path.read_text()) if path.is_file() else None

    def has_staged(self, it) -> bool:
        """True when the partition is fully staged: its manifest is written last."""
        return self._manifest_path(it).is_file()

    def staged_files(self, it) -> List[pathlib.Path]:
        """The files a staged partition's manifest lists, in order."""
        manifest = self.manifest(it)
        if manifest is None:
            return []
        directory = staged_dir(self.archive_root, self.dataset, self.window(it)[0].to_pydatetime())
        files = [directory / name for name in manifest["files"]]
        absent = [f.name for f in files if not f.is_file()]
        if absent:
            raise StagedPartitionError(f"{self.name}: manifest lists missing {absent}")
        return files

    def discard_staged(self, it, settled: bool = False) -> List[pathlib.Path]:
        """Delete the staged partition once the store can no longer take it.

        ``settled=True`` says the caller already knows, having just published it or found
        the store closed to it, so the store is not read again.
        """
        if not settled and self.appendable(it):
            return []
        manifest = self._manifest_path(it)
        if not manifest.is_file():
            return []
        removed = self.staged_files(it)
        for path in removed:
            path.unlink(missing_ok=True)
        manifest.unlink()
        return [*removed, manifest]


class ParquetObsProvider(_StagedMixin):
    """Publishes a ``table`` dataset's staged partitions to a Parquet dataset.

    Not a :class:`~planetary_datasets.base.BaseProvider`: that class is built around an
    icechunk store, and a Parquet partition is written whole in one put.
    """

    def __init__(self, name: str, config: Config | None = None):
        """Build the publisher for ``table`` dataset ``name``."""
        self.dataset = DATASETS[name]
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
        """Partitions may arrive in any order; only one already published is refused."""
        return not self.partition_stored(it)

    def run_partition(self, it, check_present: bool = True) -> bool:
        """Publish one staged partition as-is. Returns True if it was written.

        The staged file is already the final Parquet, so it is checked from its footer
        statistics and uploaded unchanged rather than decoded and encoded again.
        """
        start, end = self.window(it)
        if check_present and self.partition_stored(start):
            logger.debug(f"{self.name}: {start} already published")
            return False
        files = self.staged_files(start)
        if not files:
            logger.debug(f"{self.name}: nothing staged for {start}")
            return False
        (path,) = files
        low, high = time_bounds(path)
        if low is not None and (low < start or high >= end):
            raise StagedPartitionError(
                f"{self.name}: {path.name} has rows outside [{start}, {end})"
            )
        self.sink.put(start, path)
        return True


def time_bounds(path: pathlib.Path) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    """The earliest and latest ``time`` in a Parquet file, from its row-group statistics."""
    metadata = pq.ParquetFile(path).metadata
    column = metadata.schema.to_arrow_schema().get_field_index("time")
    lows, highs = [], []
    for i in range(metadata.num_row_groups):
        stats = metadata.row_group(i).column(column).statistics
        if stats is not None and stats.has_min_max:
            lows.append(pd.Timestamp(stats.min))
            highs.append(pd.Timestamp(stats.max))
    return (min(lows), max(highs)) if lows else (None, None)


class GridObsProvider(_StagedMixin, BaseProvider):
    """Appends a ``grid`` or ``granules`` dataset's staged partitions to icechunk.

    Whether the store would take a partition is
    :meth:`~planetary_datasets.base.BaseProvider.appendable`: only after its last step.
    """

    append_dim = "time"
    alignment_coords = (*ALIGNMENT_COORDS, "x", "y", "tile")

    def __init__(self, name: str, config: Config | None = None):
        """Build the publisher for ``grid`` or ``granules`` dataset ``name``."""
        super().__init__(config)
        self.dataset = DATASETS[name]

    def partition_stored(self, it) -> bool:
        """True when any of the partition's frames is in the store.

        Frames are committed in time order, so one present means the partition was
        written; a partial one cannot be completed anyway.
        """
        start, end = self.window(it)
        stored = pd.DatetimeIndex(existing_times(self.get_icechunk_repo(), self.append_dim))
        return bool(((stored >= start) & (stored < end)).any())

    def fetch(self, it, temp_dir=None, **kwargs) -> List[str]:
        """The staged files of this partition; empty when none is staged."""
        return [str(p) for p in self.staged_files(it)]

    def process(self, input_files, it, temp_dir=None, **kwargs) -> xr.Dataset:
        """Read one staged file and check it belongs to this partition."""
        (path,) = input_files
        start, end = self.window(it)
        with xr.open_dataset(path, engine="h5netcdf") as staged:
            ds = staged.load().drop_encoding()
        missing = self.dataset.staged_variables - set(ds.data_vars)
        if missing:
            raise StagedPartitionError(f"{self.name}: staged partition lacks {sorted(missing)}")
        times = pd.DatetimeIndex(ds["time"].values)
        if (times < start).any() or (times >= end).any():
            raise StagedPartitionError(f"{self.name}: staged frames fall outside [{start}, {end})")
        # sortby copies everything even when already in order, which it normally is.
        return ds if times.is_monotonic_increasing else ds.sortby("time")

    def prepare_for_write(self, processed: xr.Dataset) -> xr.Dataset:
        """Chunk one frame per chunk in time and ``SPATIAL_CHUNK`` in space."""
        chunks = {
            dim: 1 if dim in (self.append_dim, "tile") else min(size, SPATIAL_CHUNK)
            for dim, size in processed.sizes.items()
        }
        return processed.chunk(chunks)

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


PUBLISHERS = {"parquet": ParquetObsProvider, "icechunk": GridObsProvider}


def provider_for(name: str, config: Config | None = None):
    """The publishing provider for dataset ``name``."""
    return PUBLISHERS[DATASETS[name].publish](name, config)


__all__ = [
    "ARCHIVE_ENV",
    "GridObsProvider",
    "ParquetObsProvider",
    "STORE_ROOT",
    "StagedPartitionError",
    "provider_for",
    "time_bounds",
]
