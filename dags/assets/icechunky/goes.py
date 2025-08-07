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
import numpy as np
import s3fs
import xarray as xr
from tqdm.asyncio import tqdm
import logging

logger = logging.getLogger(__name__)
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


class GOES(VirtualDataset):
    """GOES (Geostationary Operational Environmental Satellite) data source.

    This data source provides access to GOES-16 and GOES-18 satellite data from AWS S3.
    The data is exclusively ABI (Advanced Baseline Imager) data for now.

    Parameters
    ----------
    satellite : str, optional
        Which GOES satellite to use ('goes16' or 'goes18'), by default 'goes16'
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

    SCAN_TIME_FREQUENCY = {
        "F": 600,
        "C": 300,
    }  # Scan time frequency in seconds
    SOURCE = {
        "l1": "l1",
        "l2": "l2",
    }
    VALID_SCAN_MODES = {
        "goes16": ["F", "C"],
        "goes17": ["F", "C"],
        "goes18": ["F", "C"],
        "goes19": ["F", "C"],
        "goes-east": ["F", "C"],
        "goes-west": ["F", "C"],
    }
    GOES_CHANNELS = [
        "C01",
        "C02",
        "C03",
        "C04",
        "C05",
        "C06",
        "C07",
        "C08",
        "C09",
        "C10",
        "C11",
        "C12",
        "C13",
        "C14",
        "C15",
        "C16",
    ]
    HISTORY_RANGE = {
        "goes16": (
            datetime(2017, 12, 18),
            datetime(2025, 4, 7),
        ),  # GOES-16 operational from Dec 18, 2017
        "goes17": (
            datetime(2019, 2, 12),
            datetime(2023, 1, 4),
        ),  # GOES-17 operational from Feb 12, 2019
        "goes18": (datetime(2023, 1, 4), None),  # GOES-18 operational from Jan 4, 2023
        "goes19": (datetime(2025, 4, 7), None),  # GOES-19 operational from Apr 7, 2025
        "goes-east": (datetime(2017, 12, 18), None),  # GOES-16/19 are the east satellites
        "goes-west": (datetime(2019, 2, 12), None),  # GOES-17/18 are the west satellites
    }
    BASE_URL = (
        "s3://noaa-{satellite}/ABI-L2-MCMIP{scan_mode}/{year:04d}/{day_of_year:03d}/{hour:02d}/"
    )
    BASE_L1_URL = (
        "s3://noaa-{satellite}/ABI-L1b-Rad{scan_mode}/{year:04d}/{day_of_year:03d}/{hour:02d}/"
    )

    def __init__(
        self,
        satellite: str = "goes16",
        scan_mode: str = "F",
        source: str = "l2",
        variables: list[str] = GOES_CHANNELS,
        max_workers: int = 24,
        cache: bool = True,
        verbose: bool = True,
        async_timeout: int = 6000,
        subsample: bool | int = False,
    ):
        super().__init__()
        self._satellite = satellite.lower()
        self._scan_mode = scan_mode.upper()
        self._max_workers = max_workers
        self._cache = cache
        self._verbose = verbose
        self._async_timeout = async_timeout
        self.channels = variables
        self.source = source.lower()
        self.subsample = subsample
        if self.source == "l1":
            high_res_channels = ["C02"]
            medium_res_channels = ["C01", "C03", "C05"]
            low_res_channels = [
                "C04",
                "C06",
                "C07",
                "C08",
                "C09",
                "C10",
                "C11",
                "C12",
                "C13",
                "C14",
                "C15",
                "C16",
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

        # Validate satellite and scan mode
        if self._satellite not in self.VALID_SCAN_MODES:
            raise ValueError(f"Invalid satellite {self._satellite}")
        if self._scan_mode not in self.VALID_SCAN_MODES[self._satellite]:
            if self._scan_mode == "M1" or self._scan_mode == "M2":
                raise ValueError(
                    f"Mesoscale data ({self._scan_mode}) is currently not supported by this data source due to the changing scan position."
                )
            else:
                raise ValueError(f"Invalid scan mode {self._scan_mode} for {self._satellite}")

    def available_times(self) -> pd.DatetimeIndex:
        """Returns the available times for the GOES data source.

        This function returns a Pandas DatetimeIndex of the available times for the GOES data source.
        The times are generated based on the scan frequency and the operational history of the satellite.

        Returns
        -------
        pd.DatetimeIndex
            DatetimeIndex of available times for the GOES data source.
        """
        start_date, end_date = self.HISTORY_RANGE[self._satellite]
        if end_date is None:
            end_date = datetime.now()

        # Generate time range based on scan frequency
        time_range = pd.date_range(
            start=start_date,
            end=end_date,
            freq=pd.Timedelta(seconds=self.SCAN_TIME_FREQUENCY[self._scan_mode]),
        )
        return time_range

    async def fetch(
        self,
        time: datetime | list[datetime],
        variable: str | list[str] | None,
    ) -> xr.DataArray:
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
            GOES data array
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
            *func_map, desc="Fetching GOES data", disable=(not self._verbose)
        )

        datasets = [ds for ds in datasets if ds is not None]  # Filter out None results

        if not datasets:
            logger.error(
                f"No GOES data found for {time} with variables {variable}. "
                "Check if the time is within the operational range of the satellite."
            )
            return None

        dataset = xr.concat(datasets, dim="time")

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

        return dataset

    async def fetch_array(
        self,
        time: datetime,
        variable: str | list[str] | None = None,
    ) -> np.ndarray:
        """Fetch GOES data array

        Parameters
        ----------
        time : datetime
            Time to get data for

        Returns
        -------
        np.ndarray
            GOES data array
        """

        variable_to_load = self.GOES_CHANNELS # if variable is None else variable
        # If l2, different from l1b data loading
        if self.source == "l1":
            func_map = map(functools.partial(self._get_s3_path, time=time), variable_to_load)
            goes_uris = await asyncio.gather(*func_map)
            goes_uris = [uri for uri in goes_uris if uri is not None]
            if not goes_uris:
                logger.error(
                    f"No GOES L1b data found for {time} with variables {variable_to_load}. "
                    "Check if the time is within the operational range of the satellite."
                )
                return None
            func_map = map(self._fetch_remote_file, goes_uris)
            # Get the S3 path for the GOES data file
            logger.debug(f"Fetching GOES file: {goes_uris}")
            goes_files = await asyncio.gather(*func_map)
            scn = Scene(
                reader="abi_l1b",
                filenames=goes_files,
                reader_kwargs={"storage_options": {"anon": True}},
            )
        else:
            # Get the S3 path for the GOES data file
            goes_uri = await self._get_s3_path(
                variable=f"OR_ABI-L2-MCMIP{self._scan_mode}", time=time
            )
            if goes_uri is None:
                logger.error(
                    f"No GOES L2 data found for {time} with variables {variable_to_load}. "
                    "Check if the time is within the operational range of the satellite."
                )
                return None
            logger.debug(f"Fetching GOES file: {goes_uri}")
            # Download the file to cache
            goes_file = await self._fetch_remote_file(goes_uri)
            scn = Scene(
                reader="abi_l2_nc",
                filenames=[goes_file],
                reader_kwargs={"storage_options": {"anon": True}},
            )
        scn.load(
            self.GOES_CHANNELS
        )  # Nice 2km resolution data, could go to L1b and get native resolution, although mostly larger
        dataset = scn.to_xarray_dataset(datasets=variable).load().astype(np.float16)
        orbit_params = scn.to_xarray_dataset(datasets=high_res_channels).attrs["orbital_parameters"]
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

    async def _get_s3_path(self, variable: str, time: datetime | pd.Timestamp) -> str:
        """Get the S3 path for the GOES data file"""
        if self.fs is None:
            raise ValueError("File system is not initialized")

        if self._satellite == "goes-east":
            satellite = "goes16" if time < datetime(2025, 4, 7) else "goes19"
        elif self._satellite == "goes-west":
            satellite = "goes17" if time < datetime(2023, 1, 4) else "goes18"
        else:
            satellite = self._satellite

        # Get needed date components
        year = time.year
        day_of_year = time.timetuple().tm_yday
        hour = time.hour
        if self.source == "l1":
            base_url = self.BASE_L1_URL.format(
                satellite=satellite,
                scan_mode=self._scan_mode[0:1],
                year=year,
                day_of_year=day_of_year,
                hour=hour,
            )
        else:
            base_url = self.BASE_URL.format(
                satellite=satellite,
                scan_mode=self._scan_mode[0:1],
                year=year,
                day_of_year=day_of_year,
                hour=hour,
            )

        # List files in the directory to find the most recent one
        try:
            # List files in the directory to find the most recent one
            files = await self.fs._ls(base_url)
        except Exception as e:
            logger.error(f"Failed to list files in {base_url}: {e}")
            return None

        matching_files = [f for f in files if variable in f]
        if len(matching_files) == 0:
            return None

        # Get time stamps from file names
        def get_time(file_name):
            start_str = file_name.split("/")[-1].split("_")[-3][1:-1]
            return datetime.strptime(start_str, "%Y%j%H%M%S")

        time_stamps = [get_time(f) for f in matching_files]

        # Get the index of the file that is the closest to the requested time
        # NOTE: Some of the M1 and M2 files seem to have ~10 min gaps here and there.
        # This fixes this issue by just taking the closest file. Still, some caution
        # is advised. Currently we only support F and C scan modes and those do not
        # have any gaps. Keeping this here for future reference though.
        # Convert Timestamp to np.datetime64 for comparison
        if isinstance(time, pd.Timestamp):
            time = time.to_pydatetime()
        file_index = np.argmin(np.abs(np.array(time_stamps) - time))

        # Get the file name
        file_name = matching_files[file_index]

        return file_name


if __name__ == "__main__":
    import icechunk
    import zarr
    from icechunk.xarray import to_icechunk

    # Example usage
    high_res_channels = ["C02"]
    medium_res_channels = ["C01", "C03", "C05"]
    low_res_channels = [
        "C04",
        "C06",
        "C07",
        "C08",
        "C09",
        "C10",
        "C11",
        "C12",
        "C13",
        "C14",
        "C15",
        "C16",
    ]

    # Fetch data for a specific time and variable
    # times = pd.date_range("2025-04-26T12:00:00Z", periods=10, freq="10m").to_list()
    date_range = pd.date_range("2015-07-07T00:00:00", "2025-06-30T23:59:59", freq="10min")[::-1]
    # Check date range once for the times
    times = {}
    date_ranges = {}
    names = [
        "goes_west_500m",
        "goes_west_1km",
        "goes_west_2km",
        "goes_east_500m",
        "goes_east_1km",
        "goes_east_2km",
    ]
    """"
    for name in names:
        storage = icechunk.local_filesystem_storage(f"/data/goes/{name}.icechunk")
        repo = icechunk.Repository.open_or_create(storage)
        #storage = icechunk.local_filesystem_storage(f"{name}.icechunk")
        #storage = icechunk.s3_storage(
        #    bucket="bkr",
        #    prefix=f"geo/{name}.icechunk",
        #    secret_access_key="P0qxms7SFORhGJOqBPjQoygRVIdrt0M542l9grr08XF9Kwk5XJzj9lZQXxS3YKsT",
        #    allow_http=True,
        #    region="us-west-2",
        #    force_path_style=True,
        #)
        #repo = icechunk.Repository.open(storage)
        session = repo.readonly_session("main")
        ds = xr.open_zarr(session.store, consolidated=False)
        print(ds)
        times[name] = ds.time.values
        # Check number of unique times
        print(f"Number of unique times in the store: {len(np.unique(times))}")
        # Check to when the last one is in there
        for d in date_range:
            if d > ds.time.values[-1]:
                date_ranges[name] = date_range[date_range <= ds.time.values[-1]]
                break
    """
    for date_idx in range(len(date_range)):
        for idx, channel_set in enumerate(
            [
                high_res_channels,
                medium_res_channels,
                low_res_channels,
                high_res_channels,
                medium_res_channels,
                low_res_channels,
            ]
        ):
            sat = "goes-west" if "goes_west" in names[idx] else "goes-east"
            storage = icechunk.local_filesystem_storage(f"/data/geo/{names[idx]}.icechunk")
            # storage = icechunk.local_filesystem_storage(f"{names[idx]}.icechunk")
            #storage = icechunk.s3_storage(
            #    bucket="bkr",
            #    prefix=f"geo/{names[idx]}.icechunk",
            #    secret_access_key="P0qxms7SFORhGJOqBPjQoygRVIdrt0M542l9grr08XF9Kwk5XJzj9lZQXxS3YKsT",
            #    allow_http=True,
            #    region="us-west-2",
            #    force_path_style=True,
            #)
            repo = icechunk.Repository.open_or_create(storage)
            date = (
                date_ranges[names[idx]][date_idx]
                if names[idx] in date_ranges
                else date_range[date_idx]
            )
            himwari = GOES(
                satellite=sat,
                max_workers=24,
                cache=True,
                verbose=True,
                variables=channel_set,
                source="l1",
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
                print(f"No data found for {date} with variables {channel_set}")
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

            if date_idx == 0:
                print(ds)
                try:
                    session = repo.writable_session("main")
                    to_icechunk(ds.chunk({"time": 1, "x": -1, "y": -1}), session, encoding=encoding)
                    print(session.commit(f"add {date} data to store"))
                except FileExistsError:
                    session = repo.writable_session("main")
                    to_icechunk(ds.chunk({"time": 1, "x": -1, "y": -1}), session, append_dim="time")
                    print(session.commit(f"add {date} data to store"))
            else:
                session = repo.writable_session("main")
                to_icechunk(ds.chunk({"time": 1, "x": -1, "y": -1}), session, append_dim="time")
                print(session.commit(f"add {date} data to store"))
            if names[idx] == "goes_west_2km" or names[idx] == "goes_east_2km":
                # Clear out the cache
                import shutil

                shutil.rmtree(himwari.cache, ignore_errors=True)
