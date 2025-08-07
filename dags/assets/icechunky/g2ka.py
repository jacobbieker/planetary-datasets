# SPDX-FileCopyrightText: Copyright (c) 2024-2025 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import functools

import pandas as pd
from satpy import Scene
import os
import pathlib
import shutil
from pathlib import Path
from datetime import datetime, timezone

import nest_asyncio
import numpy as np
import s3fs
import xarray as xr
import logging

logger = logging.getLogger(__name__)
from tqdm.asyncio import tqdm
from typing import Any
import yaml
import pyresample
import datetime as dt
from dags.assets.icechunky.virtual_datasource import VirtualDataset


def _serialize(d: dict[str, Any]) -> dict[str, Any]:
    sd: dict[str, Any] = {}
    for key, value in d.items():
        if isinstance(value, dt.datetime):
            sd[key] = value.isoformat()
        elif isinstance(value, bool | np.bool_):
            sd[key] = str(value)
        elif isinstance(value, pyresample.geometry.AreaDefinition):
            sd[key] = yaml.load(value.dump(), Loader=yaml.SafeLoader)  # type:ignore
        elif isinstance(value, dict):
            sd[key] = _serialize(value)
        else:
            sd[key] = str(value)
    return sd


class GK2A(VirtualDataset):
    """GK2A data source.

    This data source provides access to GK2A-16 and GK2A-18 satellite data from AWS S3.
    The data is exclusively ABI (Advanced Baseline Imager) data for now.

    Parameters
    ----------
    satellite : str, optional
        Which GK2A satellite to use ('GK2A16' or 'GK2A18'), by default 'GK2A16'
    scan_mode : str, optional
        For ABI: Scan mode ('F' for Full Disk, 'C' for Continental US)
        Mesoscale data is currently not supported due to the changing scan position.
    max_workers : int, optional
        Maximum number of workers for parallel downloads, by default 24
    cache : bool, optional
        Whether to cache downloaded files, by default True
    verbose : bool, optional
        Whether to print progress information, by default True
    async_timeout : int, optional
        Timeout for async operations in seconds, by default 600

    Notes
    -----
    ABI Data:
    - 16 spectral bands (C01-C16):
        - C01, C02 (Visible)
        - C03, C04, C05, C06 (Near IR)
        - C07-C16 (IR)
    - Scan modes:
        - Full Disk (F): Entire Earth view
        - Continental US (C): Continental US (20°N-50°N, 125°W-65°W)
    """

    GK2A_CHANNELS = [
        "IR087",
        "IR096",
        "IR105",
        "IR112",
        "IR123",
        "IR133",
        "NR013",
        "NR016",
        "SW038",
        "VI004",
        "VI005",
        "VI006",
        "VI008",
        "WV063",
        "WV069",
        "WV073",
    ]
    HISTORY_RANGE = {
        "gk2a": (datetime(2023, 2, 23), None),
    }
    BASE_URL = "s3://noaa-{satellite}-pds/AMI/L1B/FD/{year:04d}{month:02d}/{day:02d}/{hour:02d}/"

    def __init__(
        self,
        satellite: str = "gk2a",
        variables: list[str] = GK2A_CHANNELS,
        max_workers: int = 24,
        cache: bool = True,
        verbose: bool = True,
        async_timeout: int = 6000,
        subsample: bool | int = False,
    ):
        super().__init__()
        self._satellite = satellite.lower()
        self._max_workers = max_workers
        self._cache = cache
        self._verbose = verbose
        self._async_timeout = async_timeout
        self.channels = variables
        self.subsample = subsample

        high_res_channels = ["VI006"]
        medium_res_channels = ["VI008", "VI005", "VI004"]
        low_res_channels = [
            "IR087",
            "IR096",
            "IR105",
            "IR112",
            "IR123",
            "IR133",
            "NR013",
            "NR016",
            "SW038",
            "WV063",
            "WV069",
            "WV073",
        ]

        used_high_res = []
        used_medium_res = []
        used_low_res = []
        for channel in self.channels:
            if channel in high_res_channels:
                used_high_res.append(channel)
            elif channel in medium_res_channels:
                used_medium_res.append(channel)
            elif channel in low_res_channels:
                used_low_res.append(channel)

        # Assert only one resolution is used
        if len(used_high_res) >= 1:
            assert len(used_medium_res) == 0 and len(used_low_res) == 0, (
                "If high resolution channels are used, medium and low resolution channels cannot be used!"
            )
        elif len(used_medium_res) >= 1:
            assert len(used_high_res) == 0 and len(used_low_res) == 0, (
                "If medium resolution channels are used, high and low resolution channels cannot be used!"
            )
        elif len(used_low_res) >= 1:
            assert len(used_high_res) == 0 and len(used_medium_res) == 0, (
                "If low resolution channels are used, high and medium resolution channels cannot be used!"
            )

    def available_times(self) -> pd.DatetimeIndex:
        """Return available times for the GK2A data source.

        Returns
        -------
        pd.DatetimeIndex
            Available times for the GK2A data source.
        """
        start_date, end_date = self.HISTORY_RANGE[self._satellite]
        if end_date is None:
            end_date = datetime.now()

        # Generate a date range with 10-minute intervals
        return pd.date_range(start=start_date, end=end_date, freq="10min")

    async def fetch(
        self,
        time: datetime | list[datetime],
        variable: str | list[str] | None,
    ) -> xr.Dataset:
        """Async function to get data

        Parameters
        ----------
        time : datetime | list[datetime] | TimeArray
            Timestamps to return data for
        variable : str | list[str] | VariableArray
            Variables to return using standardized names

        Returns
        -------
        xr.DataArray
            GK2A data array
        """
        if self.fs is None:
            raise ValueError(
                "File store is not initialized! If you are calling this \
            function directly make sure the data source is initialized inside the async \
            loop!"
            )

        # Create cache dir if doesn't exist
        pathlib.Path(self.cache).mkdir(parents=True, exist_ok=True)

        # Make sure input time is valid
        self._validate_time(time)

        # https://filesystem-spec.readthedocs.io/en/latest/async.html#using-from-async
        if isinstance(self.fs, s3fs.S3FileSystem):
            session = await self.fs.set_session()
        else:
            session = None

        # Create download tasks
        async_tasks = [t for t in time]
        func_map = map(functools.partial(self.fetch_array, variable=variable), async_tasks)

        datasets = await tqdm.gather(
            *func_map, desc="Fetching GK2A data", disable=(not self._verbose)
        )

        datasets = [d for d in datasets if d is not None]  # Filter out None results
        if not datasets:
            logger.warning("No data fetched for the given time and variable.")
            return None

        # Concatenate datasets along time dimension
        datasets = xr.concat(datasets, dim="time")

        # Close aiohttp client if s3fs
        if session:
            await session.close()

        # Delete cache if needed
        if not self._cache:
            shutil.rmtree(self.cache)

        # Close aiohttp client if s3fs
        # https://github.com/fsspec/s3fs/issues/943
        # https://github.com/zarr-developers/zarr-python/issues/2901
        if isinstance(self.fs, s3fs.S3FileSystem):
            await self.fs.set_session()  # Make sure the session was actually initalized
            s3fs.S3FileSystem.close_session(asyncio.get_event_loop(), self.fs.s3)

        return datasets

    async def fetch_array(
        self,
        time: datetime,
        variable: str | list[str] | None = None,
    ) -> tuple[xr.Dataset, xr.Dataset, xr.Dataset]:
        """Fetch GK2A data array

        Parameters
        ----------
        time : datetime
            Time to get data for

        Returns
        -------
        np.ndarray
            GK2A data array
        """

        variable_to_load = self.channels if variable is None else variable

        # Filenames are all lowercase for the variables
        file_variable_names = [variable.lower() for variable in self.GK2A_CHANNELS]

        func_map = map(functools.partial(self._get_s3_path, time=time), file_variable_names)
        gk2a_uris = await asyncio.gather(*func_map)
        gk2a_uris = [uri for uri in gk2a_uris if uri is not None]  # Filter out None results
        logger.debug(f"Fetching GK2A file: {gk2a_uris}")
        func_map = map(self._fetch_remote_file, gk2a_uris)
        # Get the S3 path for the GK2A data file
        gk2a_files = await asyncio.gather(*func_map)
        # G2KA seems to have broken files sometimes, so if fails to load here, return None
        try:
            scn = Scene(
                reader="ami_l1b",
                filenames=gk2a_files,
                reader_kwargs={"storage_options": {"anon": True}},
            )
            scn.load(
                self.GK2A_CHANNELS
            )  # Nice 2km resolution data, could go to L1b and get native resolution, although mostly larger
            # Add latitude/longitude to coordinates
        except:
            logger.error(
                f"Failed to load GK2A data for time {time} and variables {variable_to_load}."
            )
            return None
        dataset = scn.to_xarray_dataset(datasets=variable_to_load).astype(np.float16)
        orbit_params = scn.to_xarray_dataset(datasets=["VI006"]).attrs["orbital_parameters"]
        import pandas as pd

        start_time = pd.Timestamp(dataset.attrs["start_time"])
        end_time = pd.Timestamp(dataset.attrs["end_time"])
        # Get the middle time of two times
        mid_time = start_time + (end_time - start_time) / 2
        dataset["time"] = mid_time
        dataset = dataset.assign_coords({"time": dataset["time"]})
        # Expand coords for data to have time dimension
        dataset = dataset.expand_dims("time")
        dataset["start_time"] = xr.DataArray([start_time], coords={"time": dataset["time"]})
        dataset["end_time"] = xr.DataArray([end_time], coords={"time": dataset["time"]})
        dataset["platform_name"] = xr.DataArray(
            [dataset.attrs["platform_name"]], coords={"time": dataset["time"]}
        )
        dataset["orbital_parameters"] = xr.DataArray(
            [orbit_params],
            dims=("time",),
        ).astype(f"U16")
        dataset["area"] = xr.DataArray(
            [str(dataset.attrs["area"])],
            dims=("time",),
        ).astype(f"U512")
        # Now reduce to float16 for everything other than latitude/longitude
        for var in dataset.data_vars:
            if var not in [
                "latitude",
                "longitude",
                "start_time",
                "end_time",
                "platform_name",
                "area",
                "orbital_parameters",
            ]:
                dataset[var] = dataset[var].astype(np.float16)
            if var in ["latitude", "longitude"]:
                dataset[var] = dataset[var].astype(np.float32)
        # Drop a few attributes
        dataset.attrs.pop("end_time")
        dataset.attrs.pop("start_time")
        dataset.attrs.pop("platform_name")
        dataset.attrs = _serialize(dataset.attrs)
        for var in dataset.data_vars:
            dataset[var].attrs = _serialize(dataset[var].attrs)
        dataset = dataset.drop_vars("crs")
        dataset = dataset.chunk({"time": 1, "y": -1, "x": -1})
        # Add x and y coords per time as well, so that the exact location can be reconstructed
        dataset["x_geostationary_coordinates"] = xr.DataArray(
            [dataset.x.values],
            dims=("time", "x"),
            coords={"time": dataset.time, "x": dataset.x},
        )
        dataset["y_geostationary_coordinates"] = xr.DataArray(
            [dataset.y.values],
            dims=("time", "y"),
            coords={"time": dataset.time, "y": dataset.y},
        )
        return dataset

    async def _get_s3_path(self, variable: str, time: datetime) -> str:
        """Get the S3 path for the GK2A data file"""
        if self.fs is None:
            raise ValueError("File system is not initialized")

        # Get needed date components
        year = time.year
        day = time.day
        hour = time.hour

        base_url = self.BASE_URL.format(
            satellite=self._satellite,
            year=year,
            month=time.month,
            day=day,
            hour=hour,
        )
        try:
            # List files in the directory to find the most recent one
            files = await self.fs._ls(base_url)
        except Exception as e:
            logger.error(f"Failed to list files in {base_url}: {e}")
            return None

        files = [f for f in files if variable in f]
        if len(files) == 0:
            return None

        # Get time stamps from file names
        def get_time(file_name):
            start_str = file_name.split("/")[-1].split("_")[-1].split(".")[0]
            return datetime.strptime(start_str, "%Y%m%d%H%M")

        time_stamps = [get_time(f) for f in files]

        # Get the index of the file that is the closest to the requested time
        # NOTE: Some of the M1 and M2 files seem to have ~10 min gaps here and there.
        # This fixes this issue by just taking the closest file. Still, some caution
        # is advised. Currently we only support F and C scan modes and those do not
        # have any gaps. Keeping this here for future reference though.
        if isinstance(time, pd.Timestamp):
            time = time.to_pydatetime()
        file_index = np.argmin(np.abs(np.array(time_stamps) - time))

        # Get the file name
        file_name = files[file_index]

        return file_name


if __name__ == "__main__":
    high_res_channels = ["VI006"]
    medium_res_channels = ["VI008", "VI005", "VI004"]
    low_res_channels = [
        "IR087",
        "IR096",
        "IR105",
        "IR112",
        "IR123",
        "IR133",
        "NR013",
        "NR016",
        "SW038",
        "WV063",
        "WV069",
        "WV073",
    ]
    import icechunk
    import zarr
    from icechunk.xarray import to_icechunk

    # Fetch data for a specific time and variable
    # times = pd.date_range("2025-04-26T12:00:00Z", periods=10, freq="10m").to_list()
    date_range = pd.date_range("2023-02-23T00:00:00", "2025-06-30T23:59:59", freq="10min")[::-1]
    # Check date range once for the times
    times = {}
    date_ranges = {}
    names = ["g2ka_500m", "g2ka_1km", "g2ka_2km"]
    first_write = True
    """
    for name in names:
        storage = icechunk.local_filesystem_storage(f"/data/geo/{name}.icechunk")
        storage = icechunk.s3_storage(
            bucket="bkr",
            prefix=f"geo/{name}.icechunk",
            endpoint_url="https://data.source.coop",
            access_key_id="SC11A9JDAZLVTF959664D1NI",
            secret_access_key="P0qxms7SFORhGJOqBPjQoygRVIdrt0M542l9grr08XF9Kwk5XJzj9lZQXxS3YKsT",
            allow_http=True,
            region="us-west-2",
            force_path_style=True,
        )

        repo = icechunk.Repository.open_or_create(storage)
        session = repo.readonly_session("main")
        try:
            ds = xr.open_zarr(session.store, consolidated=False)
            print(ds)
            times[name] = ds.time.values
            # Check number of unique times
            print(f"Number of unique times in the store: {len(np.unique(times[name]))}")
            print(times[name])
            # Check to when the last one is in there
            for d in date_range:
                if d > ds.time.values[-1]:
                    date_ranges[name] = date_range[date_range <= ds.time.values[-1]]
                    break
        except Exception:
            first_write = False
    """
    for date_idx in range(len(date_range)):
        names = ["g2ka_500m", "g2ka_1km", "g2ka_2km"]
        for idx, channel_set in enumerate(
            [high_res_channels, medium_res_channels, low_res_channels]
        ):
            #if names[idx] == "g2ka_1km":
            storage = icechunk.local_filesystem_storage(f"/data/geo/{names[idx]}.icechunk")
            #else:
            #    storage = icechunk.s3_storage(
            #        bucket="bkr",
            #        prefix=f"geo/{names[idx]}.icechunk",
            #        endpoint_url="https://data.source.coop",
            #        access_key_id="SC11A9JDAZLVTF959664D1NI",
            #        secret_access_key="P0qxms7SFORhGJOqBPjQoygRVIdrt0M542l9grr08XF9Kwk5XJzj9lZQXxS3YKsT",
            #        allow_http=True,
            #        region="us-west-2",
            #        force_path_style=True,
            #    )
            repo = icechunk.Repository.open_or_create(storage)
            date = (
                date_ranges[names[idx]][date_idx]
                if names[idx] in date_ranges
                else date_range[date_idx]
            )
            himwari = GK2A(
                max_workers=24,
                cache=True,
                verbose=True,
                variables=channel_set,
            )
            # Save the dataset to a Zarr file
            encoding = {
                "time": {
                    "units": "milliseconds since 1970-01-01",
                    "calendar": "standard",
                    "dtype": "int64",
                }
            }
            print(date)
            ds = himwari([date.to_pydatetime()])
            if ds is None:
                continue
            variables = []
            for var in ds.data_vars:
                if var not in ["orbital_parameters", "start_time", "end_time", "area"]:
                    variables.append(var)
            encoding.update(
                {
                    v: {
                        "compressors": zarr.codecs.BloscCodec(
                            cname="zstd", clevel=9, shuffle=zarr.codecs.BloscShuffle.bitshuffle
                        )
                    }
                    for v in variables
                }
            )
            try:
                if date_idx == 0:
                    print(ds)
                    try:
                        session = repo.writable_session("main")
                        to_icechunk(
                            ds.chunk({"time": 1, "x": -1, "y": -1}), session, encoding=encoding
                        )
                        print(session.commit(f"add {date} data to store"))
                        first_write = False
                    except FileExistsError:
                        session = repo.writable_session("main")
                        to_icechunk(
                            ds.chunk({"time": 1, "x": -1, "y": -1}), session, append_dim="time"
                        )
                        print(session.commit(f"add {date} data to store"))
                else:
                    session = repo.writable_session("main")
                    to_icechunk(ds.chunk({"time": 1, "x": -1, "y": -1}), session, append_dim="time")
                    print(session.commit(f"add {date} data to store"))
                if names[idx] == "g2ka_2km":
                    # Clear out the cache
                    import shutil

                    shutil.rmtree(himwari.cache, ignore_errors=True)
            except Exception as e:
                print(f"Failed to write data for {date}: {e}")
                continue
