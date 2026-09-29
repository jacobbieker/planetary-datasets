"""Partitioned Parquet datasets, for observations that are rows rather than a grid.

Point observations — station reports, satellite footprints, lightning flashes — have no
fixed grid to append to an icechunk store along. They are kept as a Parquet dataset
instead: one file per partition, laid out Hive-style so any Parquet reader can prune by
date::

    <prefix>/date=2026-09-29/part-202609290600.parquet

A partition is written as a single object and only ever replaced whole, so a rerun is
idempotent and "is this partition done?" is one existence check. An empty partition is
still written (with the schema and no rows), so a quiet hour is not fetched again.

Location follows :class:`~planetary_datasets.config.Config` exactly as the icechunk stores
do: a local directory when ``ICECHUNK_LOCAL_PATH`` is set, otherwise the configured
bucket, prefix, endpoint and credentials. Read a dataset back with, for example::

    pd.read_parquet("s3://us-west-2.opendata.source.coop/bkr/obs/ghcn_daily.parquet")
"""

from __future__ import annotations

import io
import os
import pathlib
from typing import List

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from planetary_datasets.config import Config, get_config

#: Hive partition key of the directory level.
PARTITION_KEY = "date"


class ParquetSink:
    """A partitioned Parquet dataset at a store prefix.

    Args:
        prefix: Location of the dataset relative to the configured bucket, e.g.
            ``bkr/obs/earth2studio/ghcn_daily.parquet``.
        config: Configuration override, defaulting to the process-wide config.
    """

    def __init__(self, prefix: str, config: Config | None = None):
        """Create a sink; nothing is touched until the first read or write."""
        self.prefix = prefix
        self._config = config
        self._fs = None

    @property
    def config(self) -> Config:
        """Configuration for this sink, defaulting to the process-wide config."""
        return self._config if self._config is not None else get_config()

    @property
    def root(self) -> str:
        """The dataset's root, as a local path or an ``s3://`` URI."""
        return self.config.store_path(self.prefix)

    @property
    def fs(self):
        """The fsspec filesystem the dataset lives on."""
        if self._fs is None:
            self._fs = self._filesystem()
        return self._fs

    def _filesystem(self):
        import fsspec

        cfg = self.config
        if cfg.use_local_store:
            return fsspec.filesystem("file")
        creds = cfg.credentials
        kwargs: dict = {"client_kwargs": {"region_name": cfg.region}}
        if cfg.endpoint_url:
            kwargs["endpoint_url"] = cfg.endpoint_url
        if creds.aws_profile:
            kwargs["profile"] = creds.aws_profile
        elif creds.aws_access_key_id and creds.aws_secret_access_key:
            kwargs["key"] = creds.aws_access_key_id
            kwargs["secret"] = creds.aws_secret_access_key
        return fsspec.filesystem("s3", **kwargs)

    def key(self, it: pd.Timestamp) -> str:
        """Path of the partition starting at ``it``, relative to :attr:`root`."""
        return f"{PARTITION_KEY}={it:%Y-%m-%d}/part-{it:%Y%m%d%H%M}.parquet"

    def path(self, it: pd.Timestamp) -> str:
        """Full path of the partition starting at ``it``."""
        return f"{self.root.rstrip('/')}/{self.key(it)}"

    def exists(self, it: pd.Timestamp) -> bool:
        """True when the partition starting at ``it`` has been written."""
        return bool(self.fs.exists(self.path(it)))

    def write(self, it: pd.Timestamp, table: pa.Table | pd.DataFrame) -> str:
        """Write (or replace) the partition starting at ``it``. Returns its path.

        The whole file is built in memory and put as one object, so a reader never sees a
        partial partition. Locally the same guarantee comes from writing to a temporary
        name and renaming.
        """
        if isinstance(table, pd.DataFrame):
            table = pa.Table.from_pandas(table, preserve_index=False)
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="zstd")
        payload = buffer.getvalue()

        path = self.path(it)
        if self.config.use_local_store:
            target = pathlib.Path(path)
            target.parent.mkdir(parents=True, exist_ok=True)
            partial = target.with_name(target.name + ".part")
            partial.write_bytes(payload)
            os.replace(partial, target)
        else:
            self.fs.pipe_file(path, payload)
        logger.info(f"wrote {table.num_rows} row(s) to {path}")
        return path

    def partitions(self) -> List[str]:
        """Every partition file in the dataset, sorted."""
        try:
            found = self.fs.glob(f"{self.root.rstrip('/')}/{PARTITION_KEY}=*/part-*.parquet")
        except FileNotFoundError:
            return []
        return sorted(str(p) for p in found)


__all__ = ["PARTITION_KEY", "ParquetSink"]
