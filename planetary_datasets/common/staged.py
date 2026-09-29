"""Provider side of the partitions a Docker image stages as files (see ``dags/staged.py``)."""

from __future__ import annotations

import pathlib
import shutil
from typing import List

import pandas as pd

from planetary_datasets.providers._timestamps import to_naive_utc


class StagedFilesMixin:
    """Implements :class:`dags.staged.StagedPublisher` for a directory of files per partition."""

    staged_glob = "*.nc"

    def staged_dir(self, it: pd.Timestamp) -> pathlib.Path:
        """Directory the image stages the partition starting at ``it`` into."""
        raise NotImplementedError

    def staged_files(self, it: pd.Timestamp) -> List[pathlib.Path]:
        """The partition's staged files."""
        found = self.staged_dir(to_naive_utc(it)).rglob(self.staged_glob)
        return sorted(p for p in found if p.is_file())

    def has_staged(self, it: pd.Timestamp) -> bool:
        """True when anything is staged for the partition."""
        return bool(self.staged_files(it))

    def partition_stored(self, it: pd.Timestamp) -> bool:
        """True when the partition is in the store."""
        return not self.missing_timesteps(pd.DatetimeIndex([to_naive_utc(it)]))

    def discard_staged(self, it: pd.Timestamp, settled: bool = False) -> List[pathlib.Path]:
        """Delete the staging directory once the store no longer accepts the partition."""
        if not settled and self.appendable(it):
            return []
        removed = self.staged_files(it)
        shutil.rmtree(self.staged_dir(to_naive_utc(it)), ignore_errors=True)
        return removed
