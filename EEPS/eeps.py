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
from itertools import repeat
import functools

import aiohttp.client_exceptions
import dagster as dg
import dask.array
import fsspec
import numpy as np
import pandas as pd
import xarray as xr
import zarr
import time
from pathlib import Path
import multiprocessing as mp
import icechunk as ic
import warnings
warnings.simplefilter("ignore")

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

def get_timestamp_from_filename(filename: str | Path) -> pd.Timestamp:
    start_time = str(filename).split("blend_s")[-1].split("_e")[0]
    return pd.Timestamp(f"{start_time[:4]}-{start_time[4:6]}-{start_time[6:8]}T{start_time[8:10]}:{start_time[10:12]}:00")

def write_single_timestamp(file):
    storage = icechunk.local_filesystem_storage("eeps.icechunk")
    repo = ic.Repository.open(storage)
    data = open_and_process_eeps_file(file)
    data.load()
    session = repo.writable_session("main")
    # Read the timestamps
    timestamps = xr.open_zarr(session.store).time.values
    if data["time"].values[0] not in timestamps:
        # Append instead of write to region
        to_icechunk(data.chunk({"time": 1, "latitude": -1, "longitude": -1}), session, append_dim="time")
    else:
        to_icechunk(data.chunk({"time": 1, "latitude": -1, "longitude": -1}), session, region="auto")
    session.commit(f"add {data.time.values} data to store", rebase_with=icechunk.ConflictDetector())
    # keep trying until it succeeds
    """
    while True:
        try:

            break
        except ic.ConflictError:
            count += 1
            print(f"Conflict error for {file}, retrying... Count: {count}")
            pass
    """

if __name__ == "__main__":
    files = sorted(list(Path("/Users/jacob/Development/planetary-datasets/EEPS/RainRate-Blend-INST/").rglob("*GLB-5*.nc")))
    timestamps = [get_timestamp_from_filename(f) for f in files]
    # Open up the first one, and use that to write Zarr
    data = open_and_process_eeps_file(files[0])
    print(data)
    variables = list(data.data_vars)
    encoding = {
        "time": {
            "units": "seconds since 1970-01-01",
            "calendar": "standard",
            "dtype": "int64",
        }
    }
    encoding.update({
        v: {"compressors": zarr.codecs.BloscCodec(cname='zstd', clevel=9, shuffle=zarr.codecs.BloscShuffle.bitshuffle)}
        for v in variables})
    print(encoding)
    # Create empty dask arrays of the same size as the data
    dummies_4d_var = dask.array.zeros(
        (len(timestamps), data.latitude.shape[0], data.longitude.shape[0]), chunks=(1, -1, -1),
        dtype=np.float16)
    dummies_int_var = dask.array.zeros(
        (len(timestamps), len(data.latitude), len(data.longitude)), chunks=(1, -1, -1),
        dtype=np.uint8)
    dummies_inputs_var = dask.array.zeros(
        (len(timestamps),), chunks=(1,), dtype=np.int32)
    dummies_timestamp_var = dask.array.zeros(
        (len(timestamps),), chunks=(1,), dtype="datetime64[ns]")
    dummies_string_var = dask.array.empty(
        (len(timestamps),), chunks=(1,), dtype=data["missing_inputs"].dtype)
    default_dataarray = xr.DataArray(dummies_4d_var, coords={"time": timestamps,
                                                             "latitude": data.latitude.values,
                                                             "longitude": data.longitude.values},
                                     dims=["time", "latitude", "longitude"], attrs=data["RRQPE"].attrs)
    dqf_dataarray = xr.DataArray(dummies_int_var, coords={"time": timestamps,
                                                             "latitude": data.latitude.values,
                                                             "longitude": data.longitude.values},
                                     dims=["time", "latitude", "longitude"], attrs=data["DQF"].attrs)
    time_array = xr.DataArray(dummies_timestamp_var, coords={"time": timestamps}, dims=["time"])
    inputs_dataarray = xr.DataArray(dummies_inputs_var, coords={"time": timestamps}, dims=["time"])
    string_dataarray = xr.DataArray(dummies_string_var, coords={"time": timestamps}, dims=["time"])
    dummy_dataset = xr.Dataset({"RRQPE": default_dataarray, "DQF": dqf_dataarray, "time_start": time_array, "time_end": time_array, "number_of_inputs": inputs_dataarray, "missing_inputs": string_dataarray},
                               coords={"time": timestamps,
                                       "latitude": data.latitude.values,
                                       "longitude": data.longitude.values},
                               attrs=data.attrs)
    print(dummy_dataset)

    mp.set_start_method('forkserver')
    storage = icechunk.local_filesystem_storage("eeps.icechunk")
    if Path("eeps.icechunk").exists():
        print("Found existing icechunk repository, using it.")
        repo = icechunk.Repository.open(storage)
    else:
        print("No existing icechunk repository, creating it it.")
        repo = icechunk.Repository.create(storage)

        session = repo.writable_session("main")
        dummy_dataset.chunk({"time": 1, "latitude": -1, "longitude": -1}).to_zarr(session.store, compute=False, encoding=encoding)
        session.commit("Wrote updated metadata")

    # Make the metadata
    current_ds = xr.open_zarr(repo.writable_session("main").store, consolidated=False)
    print(current_ds)
    current_times = current_ds.time.values
    num_inputs = current_ds.number_of_inputs.values
    pool = mp.Pool(1)
    import tqdm
    for _ in tqdm.tqdm(pool.imap_unordered(write_single_timestamp, files[5870+1590+5600+1000+5470+16:]), total=len(files[5870+1590+5600+1000+5470+16:])):
        pass
