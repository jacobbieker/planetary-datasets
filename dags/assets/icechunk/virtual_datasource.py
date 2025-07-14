"""
This is a way to have a virtual dataset that works with the data loading, but doesn't download and open the data files

until absolutely necessary. A xarray dataset of the available data allows for selecting timesteps and such correctly,
then at the end of making an example, the actual data is fetched from AWS/remote storage and loaded then.
"""

import xarray as xr
import pandas as pd
import numpy as np
from datetime import datetime
import os
import pathlib
from pathlib import Path
import asyncio
from assimilation.data.load.virtual.utils import datasource_cache_root
import s3fs
import nest_asyncio
import shutil


class VirtualDataset:
    def __init__(self, *args, **kwargs):
        # Set up S3 filesystem
        try:
            loop = asyncio.get_running_loop()
            loop.run_until_complete(self._async_init())
        except RuntimeError:
            self.fs = None

    @property
    def available_times(self) -> pd.DatetimeIndex:
        """Returns the available times in the dataset."""
        raise NotImplementedError(
            "This method should be implemented in subclasses to return available times."
        )

    async def _async_init(self) -> None:
        """Async initialization of S3 filesystem"""
        self.fs = s3fs.S3FileSystem(anon=True, client_kwargs={}, asynchronous=True)

    def __call__(
        self,
        time: datetime | list[datetime],
        variable: str | list[str] | None = None,
    ) -> xr.DataArray:
        """Function to get data

        Parameters
        ----------
        time : datetime | list[datetime]
            Timestamps to return data for (UTC).
        variable : str | list[str]
            String, list of strings or array of strings that refer to variables to
            return.

        Returns
        -------
        xr.DataArray
            Data array containing the requested GOES data
        """
        nest_asyncio.apply()  # Patch asyncio to work in notebooks
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            # If no event loop exists, create one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        if self.fs is None:
            loop.run_until_complete(self._async_init())

        xr_array = loop.run_until_complete(
            asyncio.wait_for(self.fetch(time, variable), timeout=self._async_timeout)
        )

        # Delete cache if needed
        if not self._cache:
            shutil.rmtree(self.cache, ignore_errors=True)

        return xr_array.sortby("time")

    def _validate_time(self, times: list[datetime]) -> None:
        """Verify if date time is valid for GOES

        Parameters
        ----------
        times : list[datetime]
            List of date times to fetch data
        """
        for time in times:
            # Check if satellite was operational at this time
            if self._satellite not in self.HISTORY_RANGE:
                raise ValueError(f"Unknown satellite {self._satellite}")

            start_date, end_date = self.HISTORY_RANGE[self._satellite]
            if time < start_date:
                raise ValueError(
                    f"Requested date time {time} is before {self._satellite} became operational ({start_date})"
                )
            if end_date and time > end_date:
                raise ValueError(
                    f"Requested date time {time} is after {self._satellite} was retired ({end_date})"
                )

    async def _fetch_remote_file(self, path: str) -> str:
        """Fetches remote file into cache"""
        if self.fs is None:
            raise ValueError("File system is not initialized")

        # Keep the end of the path
        filename = str(Path(path).name)
        cache_path = os.path.join(self.cache, filename)

        if not pathlib.Path(cache_path).is_file():
            if self.fs.async_impl:
                data = await self.fs._cat_file(path)
            else:
                data = await asyncio.to_thread(self.fs.read_block, path)
            with open(cache_path, "wb") as file:
                await asyncio.to_thread(file.write, data)

        return cache_path

    @property
    def cache(self) -> str:
        """Return appropriate cache location."""
        cache_location = os.path.join(datasource_cache_root(), self._satellite)
        if not self._cache:
            cache_location = os.path.join(cache_location, f"tmp_{self._satellite}")
        return cache_location
