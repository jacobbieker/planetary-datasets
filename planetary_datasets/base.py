"""Base provider interface.

A provider knows how to fetch the inputs for one partition of a dataset and turn them into
an :class:`xarray.Dataset`. Everything else — opening the store, skipping work that is
already done, choosing compression, appending safely, watching memory — is handled here so
each provider stays small.

Subclasses set :attr:`name`, :attr:`append_dim` and :attr:`store_prefix`, and implement
:meth:`fetch` and :meth:`process`. Dagster assets call :meth:`run_partition`.
"""

from __future__ import annotations

import contextlib
import pathlib
import tempfile
from abc import ABC, abstractmethod
from typing import Iterator, List

import icechunk
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.common.store import (
    missing_timesteps as _missing_timesteps,
)
from planetary_datasets.common.store import (
    write_to_icechunk as _write_to_icechunk,
)
from planetary_datasets.config import Config, get_config
from planetary_datasets.memory import memory_guard, require_dataset_fits


class BaseProvider(ABC):
    """Abstract base provider used by the Dagster assets and the CLI.

    Attributes:
        name: Short identifier, used in logs and as the Dagster asset name.
        append_dim: Dimension new data is appended along, usually ``time`` or ``init_time``.
        store_prefix: Location of the store relative to the configured bucket, e.g.
            ``bkr/dmi/hawaii_nams.icechunk``. Resolved to S3 or a local directory by
            :class:`~planetary_datasets.config.Config`.
    """

    name: str
    append_dim: str = "time"
    store_prefix: str

    #: Guard the process step with a memory ceiling. Set False for providers that manage
    #: their own memory, such as the virtualized ingests.
    guard_memory: bool = True

    def __init__(self, config: Config | None = None):
        self._config = config
        self._repo: icechunk.Repository | None = None

    @property
    def config(self) -> Config:
        """Configuration for this provider, defaulting to the process-wide config."""
        return self._config if self._config is not None else get_config()

    @property
    def store_path(self) -> str:
        """Fully resolved store location, for logging and diagnostics."""
        return self.config.store_path(self.store_prefix)

    @abstractmethod
    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Return input URIs or local paths for the given partition timestamp.

        Return an empty list when nothing is available for the partition; that is treated
        as "nothing to do", not as a failure.
        """

    @abstractmethod
    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Turn the fetched inputs into a dataset ready to write."""

    def get_icechunk_repo(self) -> icechunk.Repository:
        """Open or create the repository this provider writes to.

        The handle is cached: a backfill of one day can be ~96 partitions, and reopening
        the store for each one is pure overhead.
        """
        if self._repo is None:
            self._repo = self.config.icechunk_repo(self.store_prefix)
        return self._repo

    def missing_timesteps(self, desired: pd.DatetimeIndex) -> List[pd.Timestamp]:
        """Return the timesteps in ``desired`` that are not yet stored."""
        return _missing_timesteps(self.get_icechunk_repo(), list(desired), append_dim=self.append_dim)

    def write_to_icechunk(self, repo: icechunk.Repository, processed: xr.Dataset) -> bool:
        """Write a processed dataset. Providers may override for special handling."""
        return _write_to_icechunk(
            repo,
            processed,
            append_dim=self.append_dim,
            message=f"{self.name}: {processed[self.append_dim].values[0]}",
        )

    @staticmethod
    @contextlib.contextmanager
    def local_tempdir() -> Iterator[pathlib.Path]:
        """Yield a temporary directory that is removed on exit.

        This is a context manager rather than a plain function: returning the path of a
        bare ``TemporaryDirectory`` lets it be finalised as soon as the reference goes out
        of scope, deleting the directory while the caller still expects it to exist.
        """
        with tempfile.TemporaryDirectory(prefix="planetary-datasets-") as td:
            yield pathlib.Path(td)

    def run_partition(self, it: pd.Timestamp, check_present: bool = True) -> bool:
        """Fetch, process and write one partition.

        Returns True if data was written, False if there was nothing to do. This is the
        method Dagster assets call.

        Args:
            it: Partition timestamp.
            check_present: Skip the "already stored?" query. :meth:`run_range` sets this
                False because it has already filtered the timestamps.
        """
        repo = self.get_icechunk_repo()

        if check_present and not self.missing_timesteps(pd.DatetimeIndex([it])):
            logger.debug(f"{self.name}: {it} already in {self.store_path}, skipping")
            return False

        with self.local_tempdir() as temp_dir:
            input_files = self.fetch(it, temp_dir=temp_dir)
            if not input_files:
                logger.debug(f"{self.name}: no input files for {it}, skipping")
                return False

            logger.info(f"{self.name}: processing {len(input_files)} file(s) for {it}")
            if self.guard_memory:
                # Only the processing is guarded. memory_guard raises when the block
                # exits, so keeping the commit outside it means a breach prevents the
                # write rather than leaving a committed store behind a failed run.
                with memory_guard(what=f"{self.name} {it}"):
                    processed = self.process(input_files, it, temp_dir=temp_dir)
                    require_dataset_fits(processed, what=f"{self.name} {it}")
            else:
                processed = self.process(input_files, it, temp_dir=temp_dir)

            return self.write_to_icechunk(repo, processed)

    def run_range(self, timestamps: pd.DatetimeIndex) -> int:
        """Run every missing partition in ``timestamps``. Returns the number written.

        A failure on one partition is logged and does not stop the rest, matching how the
        original scripts behaved over archive gaps.
        """
        written = 0
        # Filter once here rather than re-reading the time coordinate per partition.
        for it in self.missing_timesteps(timestamps):
            try:
                if self.run_partition(it, check_present=False):
                    written += 1
            except Exception as exc:  # noqa: BLE001 - one bad partition must not stop a backfill
                logger.exception(f"{self.name}: partition {it} failed: {exc}")
        return written
