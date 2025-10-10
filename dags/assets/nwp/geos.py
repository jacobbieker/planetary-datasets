import os

os.environ["AWS_REQUEST_CHECKSUM_CALCULATION"] = "WHEN_REQUIRED"
import icechunk
import os
import fsspec
import pandas as pd
import xarray as xr
import tempfile
import dask.array
import numpy as np
import multiprocessing as mp
from pathlib import Path
import zarr
from icechunk.xarray import to_icechunk
import icechunk as ic


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
            # v2 URL
            #url = f"https://portal.nccs.nasa.gov/datashare/gmao/geos-cf/v2/ana/Y{day.year:04d}/M{day.month:02d}/D{day.day:02d}/GEOS.cf.ana.htf_inst_15mn_L1440x721_slv.{day.strftime('%Y%m%d')}_{hour:02d}{minute:02d}z.RO.nc4"
            urls.append(url)

    # Download the files
    downloaded_paths = []
    for url in urls:
        filename = os.path.join(archive_folder, os.path.basename(url))
        finished = False
        tries = 0
        if not os.path.exists(filename):
            while not finished and tries < 20:
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
    ds = ds.isel(lev=0)
    ds = ds.drop_vars("lev")
    # Cut precision in half, for everything other than SLP
    for var in ds.data_vars:
        if var != "SLP":
            ds[var] = ds[var].astype("float16")
    return ds


def write_single_timestep(file: str) -> None:
    try:
        storage = icechunk.s3_storage(
            bucket="bkr",
            prefix="geos/geos_15min.icechunk",
            endpoint_url="https://data.source.coop",
            access_key_id="SC11A9JDAZLVTF959664D1NI",
            secret_access_key="P0qxms7SFORhGJOqBPjQoygRVIdrt0M542l9grr08XF9Kwk5XJzj9lZQXxS3YKsT",
            allow_http=True,
            region="us-west-2",
            force_path_style=True,
        )
        repo = ic.Repository.open(storage)
        data = xr.open_dataset(file)
        data = preprocess_geos(data)
        data.load()
    except:
        print(f"Failed to open {file}")
        return
    while True:
        try:
            session = repo.writable_session("main")
            to_icechunk(
                data.chunk({"time": 1, "latitude": -1, "longitude": -1}), session, region="auto"
            )
            session.commit(
                f"add {data.time.values} data to store", rebase_with=icechunk.ConflictDetector()
            )
            os.remove(file)
            break
        except Exception as e:
            print(f"Failed to write {file}: {e}, trying again")


if __name__ == "__main__":
    # mp.set_start_method('forkserver')
    # storage = icechunk.local_filesystem_storage("/run/media/jacob/Tester/geos_15min2.icechunk")
    storage = icechunk.s3_storage(
        bucket="bkr",
        prefix="geos/geos_15min.icechunk",
        endpoint_url="https://data.source.coop",
        access_key_id="SC11A9JDAZLVTF959664D1NI",
        secret_access_key="P0qxms7SFORhGJOqBPjQoygRVIdrt0M542l9grr08XF9Kwk5XJzj9lZQXxS3YKsT",
        allow_http=True,
        region="us-west-2",
        force_path_style=True,
    )
    if True:  # Path("/run/media/jacob/Tester/geos_15min2.icechunk").exists():
        print("Found existing icechunk repository, using it.")
        repo = icechunk.Repository.open(storage)
    else:
        print("No existing icechunk repository, creating it it.")
        repo = icechunk.Repository.create(storage)
        print(dummy_dataset)
        session = repo.writable_session("main")
        dummy_dataset.chunk({"time": 1, "latitude": -1, "longitude": -1}).to_zarr(
            session.store, compute=False, encoding=encoding
        )
        session.commit("Wrote metadata")

    files = sorted(
        list(
            Path(
                "/Volumes/T7 Shield/portal.nccs.nasa.gov/datashare/gmao/geos-cf/v1/ana/Y2020/"
            ).rglob("*.nc4")
        )
    )
    files = files + sorted(
        list(
            Path(
                "/Volumes/T7 Shield/portal.nccs.nasa.gov/datashare/gmao/geos-cf/v1/ana/Y2023/"
            ).rglob("*.nc4")
        )
    )
    # files = files + sorted(
    #    list(Path("/Volumes/T7 Shield/portal.nccs.nasa.gov/datashare/gmao/geos-cf/v1/ana/Y2020/").rglob("*.nc4")))
    # Filter out the ones that have /._ in the names
    files = [f for f in files if "._GEOS" not in str(f)]
    pool = mp.Pool(mp.cpu_count())
    import tqdm

    for _ in tqdm.tqdm(pool.imap_unordered(write_single_timestep, files), total=len(files)):
        pass
