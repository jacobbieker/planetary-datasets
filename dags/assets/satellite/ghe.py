import zarr
import xarray as xr
import numpy as np
import pandas as pd
import datetime as dt
import dask.array
import icechunk
from icechunk.xarray import to_icechunk
from pathlib import Path
import tqdm


ARCHIVE_FOLDER = "/run/media/jacob/Tester/GHE/"
BASE_URL = "s3://noaa-ghe-pds/"
ZARR_PATH = "/run/media/jacob/Tester/ghe.icechunk"

storage = icechunk.local_filesystem_storage(ZARR_PATH)
repo = icechunk.Repository.open(storage)
files = sorted(list(Path(f"{ARCHIVE_FOLDER}rain_rate/").rglob("*.nc.gz")))
nav = xr.open_dataset(f"{ARCHIVE_FOLDER}NPR.GEO.GHE.v1.Navigation.netcdf").load()

current_ds = xr.open_zarr(repo.writable_session("main").store, consolidated=False)
current_times = current_ds.time.values


def get_ghe_element(filename: str) -> xr.Dataset:
    """Gather the global mosaic of geostationary satellites hydrology estimation from NOAA on AWS

    Args:
        filename: Path to the file to open

    Returns:
        xarray object with global mosaic
    """
    # Get time from the name, where the time is in the format NPR.GEO.GHE.v1.SYYYYMMDDHHMM.nc.gz
    time_str = str(filename).split("/")[-1].split(".")[4][1:]  # Extract the time part
    time = dt.datetime.strptime(time_str, "%Y%m%d%H%M")
    ds = xr.open_dataset(filename).load()
    # Add in coordinates from navigation one
    ds = ds.assign_coords({"latitude": nav.latitude, "longitude": nav.longitude})
    # Add in timestamp
    time = pd.Timestamp(time)
    ds = ds.assign_coords({"time": time})
    ds = ds.rename({"rain": "rainfall_estimate"})
    for var in ds.data_vars:
        ds[var] = ds[var].expand_dims("time")
    # Convert to float16 to save some space
    ds = ds.astype(np.float16)
    return ds

import os

for i, file in tqdm.tqdm(enumerate(files), total=len(files)):
    try:
        data = get_ghe_element(file)
        if data.time.values[0] in current_times:
            print(f"Skipping {file} as it already exists in the repository.")
            continue
    except Exception as e:
        print(f"Failed to open {file}: {e}")
        continue
    data.load()
    if os.path.exists(ZARR_PATH):
        session = repo.writable_session("main")
        to_icechunk(data.chunk({"time": 1, "lines": -1, "elems": -1}), session, append_dim='time')
        print(session.commit(f"add {data.time.values} data to store"))
    else:
        variables = list(data.data_vars)
        encoding = {
            v: {"compressors": zarr.codecs.BloscCodec(cname='zstd', clevel=9,
                                                      shuffle=zarr.codecs.BloscShuffle.bitshuffle)}
            for v in variables}
        encoding["time"] = {"units": "nanoseconds since 1970-01-01"}
        session = repo.writable_session("main")
        to_icechunk(data.chunk({"time": 1, "lines": -1, "elems": -1}), session, encoding=encoding)
        print(session.commit(f"add {data.time.values} data to store"))

exit()

def get_ghe_mosaic(time: dt.datetime) -> xr.Dataset:
    """Gather the global mosaic of geostationary satellites hydrology estimation from NOAA on AWS

    Args:
        time: datetime object

    Returns:
        xarray object with global mosaic
    """
    # Do it for each of the 4 files for the hour interval
    dses = []
    for minute_offset in range(0, 60, 15):
        time = time.replace(minute=minute_offset)
        ds = xr.open_dataset(f"{ARCHIVE_FOLDER}rain_rate/{time.year}/{time.month:02}/{time.day:02}/NPR.GEO.GHE.v1.S{time.strftime('%Y%m%d%H%M')}.nc.gz").load()
        # Add in coordinates from navigation one
        nav = xr.open_dataset(f"{ARCHIVE_FOLDER}NPR.GEO.GHE.v1.Navigation.netcdf").load()
        ds = ds.assign_coords({"latitude": nav.latitude, "longitude": nav.longitude})
        # Add in timestamp
        ds = ds.assign_coords({"time": pd.Timestamp(time)})
        ds = ds.rename({"rain": "rainfall_estimate"})
        # Convert to float16 to save some space
        ds = ds.astype(np.float16)
        dses.append(ds)
    return xr.concat(dses, dim="time").sortby("time")

data = get_ghe_mosaic(dt.datetime(2019, 6, 24, 20, 0, 0))
encoding = {"rainfall_estimate": {
    "compressors": zarr.codecs.BloscCodec(cname='zstd', clevel=9, shuffle=zarr.codecs.BloscShuffle.bitshuffle)}}
encoding["time"] = {"units": "nanoseconds since 1970-01-01"}
# Get the number of partitions to create
date_range = pd.date_range(start="2019-06-24T20:00:00", end="2025-03-05T19:15:00", freq="15min")
dummies = dask.array.zeros((len(date_range), data.lines.shape[0], data.elems.shape[0]), chunks=(1, -1, -1),
                           dtype=np.float16)
default_dataarray = xr.DataArray(dummies,
                                 coords={"time": date_range, "latitude": (["lines", "elems"], data.latitude.values),
                                         "longitude": (["lines", "elems"], data.longitude.values)},
                                 dims=["time", "lines", "elems"])
dummy_dataset = xr.Dataset({v: default_dataarray for v in ["rainfall_estimate"]},
                           coords={"time": date_range, "latitude": (["lines", "elems"], data.latitude.values),
                                   "longitude": (["lines", "elems"], data.longitude.values)})
print(dummy_dataset)
dummy_dataset.to_zarr(ZARR_PATH, mode="w", compute=False, zarr_format=3, encoding=encoding)

for date in pd.date_range(start="2019-06-24T20:00:00", end="2025-03-05T20:00:00", freq="1H"):
    dataset: xr.Dataset = get_ghe_mosaic(date)
    print(dataset)
    dataset.chunk({"time": 1, "lines": -1, "elems": -1}).to_zarr(ZARR_PATH,
                                                                 region={
                                                                     "time": "auto",
                                                                     "latitude": "auto",
                                                                     "longitude": "auto"}, )
    print(f"Added data for {date} to {ZARR_PATH}")