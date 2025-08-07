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

import numpy as np
import s3fs
import xarray as xr
from tqdm.asyncio import tqdm

HISTORY_RANGE = {
    "n21": (datetime(2023, 2, 27), None),
    "n20": (datetime(2022, 11, 7), None),
    "snpp": (datetime(2022, 11, 7), None),
    "jpss": (
        datetime(2022, 11, 7),
        None,
    ),  # Load all of the JPSS data together, output would have extra dimension of "satellite"
}
EXPECTED_RESOLUTIONS = {
    "n21": {"y": 12, "x": 96},
    "n20": {"y": 12, "x": 96},
    "snpp": {"y": 12, "x": 96},
    "jpss": {"y": 36, "x": 96},
}
BASE_URL = "s3://noaa-nesdis-{satellite}-pds/ATMS-SDR/{year:04d}/{month:02d}/{day:02d}/"
BASE_GEO_URL = "s3://noaa-nesdis-{satellite}-pds/ATMS-SDR-GEO/{year:04d}/{month:02d}/{day:02d}/"
ATMS_VARIABLES = [
    "1",
    "10",
    "11",
    "12",
    "13",
    "14",
    "15",
    "16",
    "17",
    "18",
    "19",
    "2",
    "20",
    "21",
    "22",
    "3",
    "4",
    "5",
    "6",
    "7",
    "8",
    "9",
    "lat",
    "lon",
    "sat_azi",
    "sat_zen",
    "sol_azi",
    "sol_zen",
    "surf_alt",
]

from typing import Any
import yaml
import pyresample
import datetime as dt


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


def process_atms(filename, geo_filename) -> xr.Dataset:
    # Download the file to cache
    scn = Scene(
        reader="atms_sdr_hdf5",
        filenames=[filename, geo_filename],
        reader_kwargs={"storage_options": {"anon": True}},
    )
    scn.load(ATMS_VARIABLES)
    # Add latitude/longitude to coordinates
    longitude, latitude = scn[ATMS_VARIABLES[0]].attrs["area"].get_lonlats()
    dataset = scn.to_xarray_dataset()
    # Add time dimension to the latitude/longitude so that is
    dataset["longitude"] = (("y", "x"), longitude)
    dataset["latitude"] = (("y", "x"), latitude)
    # Pad here any that we need in y and x
    if dataset.y.size < EXPECTED_RESOLUTIONS["n21"]["y"]:
        pad_y = EXPECTED_RESOLUTIONS["n21"]["y"] - dataset.y.size
        dataset = dataset.pad({"y": (0, pad_y)}, mode="constant", constant_values=np.nan)
    if dataset.x.size < EXPECTED_RESOLUTIONS["n21"]["x"]:
        pad_x = EXPECTED_RESOLUTIONS["n21"]["x"] - dataset.x.size
        dataset = dataset.pad({"x": (0, pad_x)}, mode="constant", constant_values=np.nan)
    # Add time coordinate as mid point of the start_time and end_time
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
    dataset = dataset.load()
    # Now reduce to float16 for everything other than latitude/longitude
    for var in dataset.data_vars:
        if var not in ["latitude", "longitude", "start_time", "end_time", "platform_name"]:
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
    return dataset


def wrap_process_atms(filenames):
    """
    Wraps the process_atms function to handle a list of filenames.
    """
    filename, geo_filename = filenames
    return process_atms(filename, geo_filename)


if __name__ == "__main__":
    import icechunk
    from icechunk.xarray import to_icechunk
    import glob
    import zarr
    import multiprocessing as mp
    import tqdm

    start_uri = "/run/media/jacob/Tester/"
    n_21 = start_uri + "N21/"
    n_20 = start_uri + "N20/"
    snpp = start_uri + "SNPP/"
    date_range = pd.date_range("2022-11-08", "2025-06-30", freq="1D")
    storage = icechunk.local_filesystem_storage("jpss_atms.icechunk")
    repo = icechunk.Repository.open_or_create(storage)
    pool = mp.Pool(mp.cpu_count())
    times = []
    first_write = True
    for time_idx, date in tqdm.tqdm(enumerate(date_range), total=len(date_range)):
        day = date.to_pydatetime()
        satellite_dses = []
        for satellite in ["n21", "n20", "snpp"]:
            if day >= HISTORY_RANGE[satellite][0]:
                files = sorted(
                    list(
                        glob.glob(
                            f"{start_uri}/{satellite.upper()}/ATMS-SDR/{day.year:04d}/{day.month:02d}/{day.day:02d}/*"
                        )
                    )
                )
                geo_files = sorted(
                    list(
                        glob.glob(
                            f"{start_uri}/{satellite.upper()}/ATMS-SDR-GEO/{day.year:04d}/{day.month:02d}/{day.day:02d}/*"
                        )
                    )
                )
                try:
                    assert len(files) == len(geo_files), (
                        f"Expected files and geo files to be the same, for {satellite} {date} got {len(files)}, {len(geo_files)}"
                    )

                    dses = pool.map(wrap_process_atms, zip(files, geo_files))
                    ds = xr.concat(dses, "time")
                    satellite_dses.append(ds)
                except AssertionError as e:
                    print(f"Assertion error for {satellite} on {date}: {e}")
                    continue
        if len(satellite_dses) == 0:
            print("No data found for", date)
            continue
        elif len(satellite_dses) == 1:
            satellite_ds = satellite_dses[0]
        else:
            satellite_ds = xr.concat(satellite_dses, "time").sortby("time")
        times += satellite_ds.time.values.tolist()
        encoding = {
            "time": {
                "units": "nanoseconds since 2020-01-01",
                "calendar": "standard",
                "dtype": "int64",
            }
        }
        variables = []
        for var in satellite_ds.data_vars:
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
        print(satellite_ds)
        try:
            if first_write:
                session = repo.writable_session("main")
                to_icechunk(
                    satellite_ds.chunk({"time": 1, "x": -1, "y": -1}), session, encoding=encoding
                )
                print(session.commit(f"add {date} data to store"))
                first_write = False
            else:
                session = repo.writable_session("main")
                to_icechunk(
                    satellite_ds.chunk({"time": 1, "x": -1, "y": -1}), session, append_dim="time"
                )
                print(session.commit(f"add {date} data to store"))
        except Exception as e:
            print(f"Failed to write data for {date}: {e}")
            continue
