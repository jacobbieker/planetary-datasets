from __future__ import annotations

import os
import pathlib
import shutil
import tempfile
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import List

import icechunk
import pandas as pd
import xarray as xr
from icechunk.xarray import to_icechunk
from loguru import logger
import zarr.codecs

"""Base provider interface."""


class BaseProvider(ABC):
    """Abstract base provider that defines the provider interface used by Dagster assets.

    Implementations must provide methods to fetch input locations, determine missing
    timesteps, access an Icechunk repository, and process inputs into a virtualized
    xarray-like object exposing `.virtualize`.
    """

    name: str
    append_dim: str
    icechunk_path: str

    @abstractmethod
    def fetch(self, it: pd.Timestamp, **kwargs) -> List[str]:
        """Return list of input URIs (s3:// or local) for the given partition timestamp.

        Should return an empty list if no inputs are available for the partition.
        """

    def missing_timesteps(self, desired_timestamps: pd.DatetimeIndex) -> List[pd.Timestamp]:
        """Return a list of missing timesteps between start and end for the target store."""
        try:
            times = xr.open_zarr(self.get_icechunk_repo().readonly_session("main").store, consolidated=False)[self.append_dim].values
        except:
            times = []
        # Filter times to be between start and end
        missing_times = [time for time in desired_timestamps if time not in times]
        return missing_times

    def get_icechunk_repo(self,):
        """Return an open or newly created icechunk.Repository for writing results."""
        if "s3://" in self.icechunk_path:
            bucket = str(self.icechunk_path).split("s3://")[1].split("/")[0]
            prefix = "/".join(str(self.icechunk_path).split("s3://")[1].split("/")[1:])
            storage = icechunk.s3_storage(bucket=bucket,
                                          prefix=prefix,
                                          access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
                                          secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
                                          region="us-west-2", )
        else:
            storage = icechunk.local_filesystem_storage(self.icechunk_path)
        repo = icechunk.Repository.open_or_create(storage)
        return repo

    @abstractmethod
    def process(self, input_files: List[str], it: pd.Timestamp, temp_dir: str | None = None, **kwargs) -> xr.Dataset:
        """Process input files and return a processed object exposing for writing.

        Implementations are free to return an xarray.Dataset or a virtualized wrapper from
        kerchunk/virtualizarr as long as `to_icechunk(session.store, ...)` works.
        """

    def write_to_icechunk(self, repo: icechunk.Repository, processed: xr.Dataset):
        """Default writer using icechunk.Session.

        Providers may override this if they need specialized handling.
        """
        session = repo.writable_session("main")
        # Check if store already has data
        try:
            existing = xr.open_zarr(session.store, consolidated=False)
            has_data = len(existing.dims) > 0
            data_vars = xr.open_zarr(self.get_icechunk_repo().readonly_session("main").store,
                                     consolidated=False).data_vars.keys()

        except Exception:
            has_data = False

        if has_data:
            if data_vars is not None and set(processed.data_vars.keys()) != set(data_vars):
                logger.debug(f"Data variables do not match, skipping...")
                logger.error(set(processed.data_vars.keys()) - set(data_vars))
                logger.debug(data_vars)
                return
            # Check to ensure level/isobaricinhPA/lat/lon are in the same order as existing data
            for coord in ["latitude", "longitude", "level", "isobaricInhPa"]:
                if coord in existing.dims and coord in processed.dims:
                    if not all(existing[coord].values == processed[coord].values):
                        logger.error(f"{coord} values do not match, skipping...")
                        return
            logger.debug(f"Appending {processed[self.append_dim].values} to icechunk")
            to_icechunk(processed, session, append_dim=self.append_dim)
        else:
            encoding = {}
            for dv in processed.data_vars:
                encoding[dv] = {
                    "compressors": zarr.codecs.BloscCodec(cname='zstd', clevel=9,
                                                          shuffle=zarr.codecs.BloscShuffle.bitshuffle)}
            encoding[self.append_dim] = {"units": "seconds since 1970-01-01", "calendar": "standard", "dtype": "int64"}
            # If time is not in the repo, then append it
            # if data.time.values[0] not in xr.open_zarr(repo.readonly_session("main").store, consolidated=False).time.values:
            to_icechunk(processed, session, encoding=encoding)
        session.commit(f"add {processed[self.append_dim].values} data to store", rebase_with=icechunk.ConflictDetector())

    @staticmethod
    def local_tempdir():
        """Context manager yielding a pathlib.Path to a temporary directory."""
        td = tempfile.TemporaryDirectory()
        return pathlib.Path(td.name)

    def run_partition(self, it: pd.Timestamp):
        """High level orchestration: fetch -> process -> write.

        This is the method Dagster assets should call. It is safe if no inputs are found
        (it will simply return None).
        """
        repo = self.get_icechunk_repo()
        missing_times = self.missing_timesteps(pd.DatetimeIndex([it]))
        if len(missing_times) == 0:
            logger.debug(f"Timestep {it} already exists in {self.name} icechunk at {self.icechunk_path}, skipping.")
            return
        # Create a local temporary directory and pass it to fetch/process when
        # providers support it. We prefer keyword invocation and fall back to the
        # previous call signature if a provider does not accept the tmpdir arg.
        temp_dir = self.local_tempdir()
        print(temp_dir)
        input_files = self.fetch(it, temp_dir=temp_dir)

        if not input_files:
            logger.debug(f"No input files found for {self.name} at {it}, skipping.")
            return

        processed = self.process(input_files, it)

        # write processed result to icechunk repo
        self.write_to_icechunk(repo, processed)

        # Best-effort remove tempdir (TemporaryDirectory context already cleans up,
        # but calling rmtree here is safe with ignore_errors=True)
        shutil.rmtree(temp_dir, ignore_errors=True)
