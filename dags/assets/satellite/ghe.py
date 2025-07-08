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
import icechunk as ic

ARCHIVE_FOLDER = "/run/media/jacob/Tester/GHE/"
BASE_URL = "s3://noaa-ghe-pds/"
ZARR_PATH = "/run/media/jacob/Tester/ghe2.icechunk"
files = sorted(list(Path(f"{ARCHIVE_FOLDER}rain_rate/").rglob("*.nc.gz")))
nav = xr.open_dataset(f"{ARCHIVE_FOLDER}NPR.GEO.GHE.v1.Navigation.netcdf").load()

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

def write_single_timestamp(file):
    storage = icechunk.local_filesystem_storage("eeps.icechunk")
    repo = ic.Repository.open(storage)
    data = get_ghe_element(file)
    data.load()
    while True:
        try:
            session = repo.writable_session("main")
            # Read the timestamps
            to_icechunk(data.chunk({"time": 1, "latitude": -1, "longitude": -1}), session, region="auto")
            session.commit(f"add {data.time.values} data to store", rebase_with=icechunk.ConflictDetector())
            # Remove source file once it is written
            os.remove(file)
            break
        except Exception as e:
            print(f"Failed to write {file}: {e}")
            continue

if __name__ == "__main__":
    import os
    import multiprocessing as mp
    mp.set_start_method('forkserver')
    storage = icechunk.local_filesystem_storage(ZARR_PATH)
    repo = icechunk.Repository.open_or_create(storage)

    session = repo.writable_session("main")
    data = get_ghe_element(files[-1])
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
    to_icechunk(dummy_dataset.chunk({"time": 1, "latitude": -1, "longitude": -1}), session, encoding=encoding)

    pool = mp.Pool(mp.cpu_count())
    for _ in tqdm.tqdm(pool.imap_unordered(write_single_timestamp, files), total=len(files)):
        pass
