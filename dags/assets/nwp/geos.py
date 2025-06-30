import icechunk
import os
import fsspec
import pandas as pd
import xarray as xr
import tempfile
import zarr
from icechunk.xarray import to_icechunk

def get_geos_day(day: pd.Timestamp, archive_folder: str | None = None) -> list[str]:
    """
    Download GEOS 15 minutely data for a specific date, and return the filepaths

    Pattern for the 15 minutely data is:
    https://portal.nccs.nasa.gov/datashare/gmao/geos-cf/v1/ana/Y2025/M06/D28/GEOS-CF.v01.rpl.htf_inst_15mn_g1440x721_x1.20250628_1130z.nc4

    Args:
        day (pd.Timestamp): The date for which to download the data
        archive_folder (str | None): Optional folder to save the downloaded files. If None, uses a temporary directory.

    Returns:
        list[str]: List of filepaths to the downloaded data files
    """

    if archive_folder is None:
        archive_folder = tempfile.mkdtemp(prefix=f"geos_data_{day.strftime('%Y%m%d')}")
    else:
        if not os.path.isdir(archive_folder):
            raise ValueError(f"Provided archive_folder {archive_folder} is not a directory.")
        if not os.path.exists(archive_folder):
            os.makedirs(archive_folder)

    urls = []
    for hour in range(0, 24):
        for minute in [0, 15, 30, 45]:
            url = f"https://portal.nccs.nasa.gov/datashare/gmao/geos-cf/v1/ana/Y{day.year:04d}/M{day.month:02d}/D{day.day:02d}/GEOS-CF.v01.rpl.htf_inst_15mn_g1440x721_x1.{day.strftime('%Y%m%d')}_{hour:02d}{minute:02d}z.nc4"
            urls.append(url)

    # Download the files
    downloaded_paths = []
    for url in urls:
        filename = os.path.join(archive_folder, os.path.basename(url))
        finished = False
        if not os.path.exists(filename):
            while not finished:
                try:
                    # Download the file using fsspec
                    with fsspec.open(url, "rb") as f:
                        with open(filename, "wb") as f2:
                            f2.write(f.read())
                    downloaded_paths.append(filename)
                    finished = True
                except Exception as e:
                    print(f"Error downloading {url}: {e}")
        else:
            print(f"File {filename} already exists, skipping download.")

    return downloaded_paths

def preprocess_geos(ds: xr.Dataset) -> xr.Dataset:
    """
    Preprocess the GEOS dataset by renaming variables and converting units.

    Args:
        ds (xr.Dataset): The input xarray dataset.

    Returns:
        xr.Dataset: The preprocessed dataset.
    """
    ds = ds.rename({"lon": "longitude", "lat": "latitude"})
    ds = ds.drop_vars(["lev"])
    # Cut precision in half, for everything other than SLP
    for var in ds.data_vars:
        if var != "SLP":
            ds[var] = ds[var].astype("float16")
    return ds

def save_to_icechunk(ds: xr.Dataset) -> None:

    pass

ds = xr.open_dataset(
    "/Users/jacob/Downloads/GEOS-CF.v01.rpl.htf_inst_15mn_g1440x721_x1.20180101_0015z.nc4")
data = preprocess_geos(ds)
print(ds)

# Make encoding
variables = list(data.data_vars)
encoding = {
    "time": {
        "units": "seconds since 1970-01-01",
        "calendar": "standard",
        "dtype": "int64",
    }
}
encoding = {
    v: {"compressors": zarr.codecs.BloscCodec(cname='zstd', clevel=9,
                                              shuffle=zarr.codecs.BloscShuffle.bitshuffle)}
    for v in variables}

repo = icechunk.Repository.create(storage)
print(repo)
session = repo.writable_session("main")
#to_icechunk(data.chunk({"time": 1, "latitude": -1, "longitude": -1}), session, append_dim="time")
to_icechunk(data.chunk({"time": 1, "latitude": -1, "longitude": -1}), session, encoding=encoding)
session.commit("Add second timestep")

# TODO: Idea: Write each day in parallel, so write out metadata to the store once, with the day added, then parallelize writing the data, or do for 1 week at a time?
# Should be fairly quick, and not have same issues as for MRMS or others where the metadata is written out to years in advance, and is just NaNs if loaded.