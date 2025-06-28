import glob

import xarray as xr
import os
import datetime as dt
import datetime as dt
import os
from subprocess import Popen
from typing import TYPE_CHECKING
import icechunk
from icechunk.xarray import to_icechunk

import aiohttp.client_exceptions
import dagster as dg
import dask.array
import fsspec
import numpy as np
import pandas as pd
import xarray as xr
import zarr
from pathlib import Path

def open_and_process_eeps_file(filename: str | Path) -> xr.Dataset:
    data = xr.open_dataset(filename)
    # Get time start and time end from attributes
    data["time_start"] = pd.Timestamp(data.attrs["time_coverage_start"])
    data["time_end"] = pd.Timestamp(data.attrs["time_coverage_end"])
    # Create lat/lon from geospatial lat min, lat max, lon min, lon_max, and resolution
    lat_min = data.attrs["geospatial_lat_min"]
    lat_max = data.attrs["geospatial_lat_max"]
    lon_min = data.attrs["geospatial_lon_min"]
    lon_max = data.attrs["geospatial_lon_max"]
    lat_resolution = data.attrs["geospatial_lat_resolution"]
    lon_resolution = data.attrs["geospatial_lon_resolution"]
    latitudes = np.arange(lat_min, lat_max + lat_resolution, lat_resolution)
    longitudes = np.arange(lon_min, lon_max + lon_resolution, lon_resolution)
    # Reverse latitudes to be high to low
    latitudes = latitudes[::-1]
    data = data.rename({"Rows": "latitude", "Columns": "longitude"})
    data = data.assign_coords(latitude=("latitude", latitudes), longitude=("longitude", longitudes))
    data["DQF"] = data["DQF"].astype(np.uint8)
    data["RRQPE"] = data["RRQPE"].astype(np.float16)
    # Set time coordinate to be time_start
    data["time"] = [pd.Timestamp(data.attrs["time_coverage_start"])]
    data = data.assign_coords({"time": data["time"]})
    # Make time a datetime64
    data["time"] = data["time"].astype("datetime64[ns]")
    # Do same for time start and end
    data["time_start"] = data["time_start"].astype("datetime64[ns]")
    data["time_end"] = data["time_end"].astype("datetime64[ns]")
    data["number_of_inputs"] = data["MonitoringMetaData"].attrs.get("Number_of_Input_Files", 0)
    data["missing_inputs"] = data["MonitoringMetaData"].attrs.get("Missing_Inputs", "")
    data = data.drop_vars(["quality_information", "MonitoringMetaData"])
    for var in data.data_vars:
        data[var] = data[var].expand_dims("time")
    return data
storage = icechunk.local_filesystem_storage("eeps.icechunk")
if Path("eeps.icechunk").exists():
    print("Found existing icechunk repository, using it.")
    repo = icechunk.Repository.open(storage)
else:
    print("No existing icechunk repository, creating it it.")
    repo = icechunk.Repository.open(storage)

current_ds = xr.open_zarr(repo.writable_session("main").store, consolidated=False)
current_times = current_ds.time.values
num_inputs = current_ds.number_of_inputs.values
files = sorted(list(Path("/Users/jacob/Development/planetary-datasets/EEPS/RainRate-Blend-INST/").rglob("*.nc")))
for i, file in enumerate(files):
    try:
        data = open_and_process_eeps_file(file)
        if data.time.values[0] in current_times:
            print(f"Skipping {file} as it already exists in the repository.")
            continue
    except Exception as e:
        print(f"Failed to open {file}: {e}")
        continue
    data.load()
    if os.path.exists("eeps.icechunk") and i > 0:
        session = repo.writable_session("main")
        to_icechunk(data.chunk({"time": 1, "latitude": -1, "longitude": -1}), session, append_dim='time')
        print(session.commit(f"add {data.time.values} data to store"))
    else:
        variables = list(data.data_vars)
        encoding = {
            v: {"compressors": zarr.codecs.BloscCodec(cname='zstd', clevel=9,
                                                      shuffle=zarr.codecs.BloscShuffle.bitshuffle)}
            for v in variables}
        encoding["time"] = {"units": "nanoseconds since 1970-01-01"}
        session = repo.writable_session("main")
        to_icechunk(data.chunk({"time": 1, "latitude": -1, "longitude": -1}), session, encoding=encoding)
        print(session.commit(f"add {data.time.values} data to store"))