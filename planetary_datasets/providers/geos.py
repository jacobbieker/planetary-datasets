import os
import pathlib
from typing import List

os.environ["AWS_REQUEST_CHECKSUM_CALCULATION"] = "WHEN_REQUIRED"
import multiprocessing as mp
import os
import tempfile
import fsspec
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider


class GeosProvider(BaseProvider):
    name = "geos"
    append_dim = "time"
    icechunk_path = "s3://us-west-2.opendata.source.coop/bkr/geos/geos_v2_15min.icechunk"

    def fetch(self, it: pd.Timestamp, **kwargs) -> list[str]:
        return get_geos_day(it, version=2)

    def process(self, input_files: List[str], it: pd.Timestamp, tmpdir: pathlib.Path, **kwargs):
        repo = self.get_icechunk_repo()
        ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
        for file in input_files:
            processed = xr.open_dataset(file)
            processed = preprocess_geos(processed)
            # Check to make sure all data vars in repo is in data
            for var in ds.data_vars.keys():
                if var not in processed.data_vars:
                    print(f"Variable {var} not in data, cannot write {file}")
                    os.remove(file)
                    return
            processed.load()
            self.write_to_icechunk(self.get_icechunk_repo(), processed.chunk({"time": 1, "latitude": -1, "longitude": -1}))
            # Clean up file here
            os.remove(file)

def download_file(archive_folder: str, url: str) -> None:
    filename = os.path.join(archive_folder, url.split("https://")[-1])
    # Create any necessary directories that don't exist already
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    finished = False
    tries = 0
    if not os.path.exists(filename):
        while not finished and tries < 20:
            try:
                # Download the file using fsspec
                tries += 1
                with fsspec.open(url, "rb") as f:
                    with open(filename, "wb") as f2:
                        f2.write(f.read())
                finished = True
            except Exception as e:
                continue
    else:
        logger.debug(f"File {filename} already exists, skipping download.")
    return filename

def get_geos_day(day: pd.Timestamp, archive_folder: str | None = None, version: int = 2) -> list[str]:
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
            if version == 1:
                url = f"https://portal.nccs.nasa.gov/datashare/gmao/geos-cf/v1/ana/Y{day.year:04d}/M{day.month:02d}/D{day.day:02d}/GEOS-CF.v01.rpl.htf_inst_15mn_g1440x721_x1.{day.strftime('%Y%m%d')}_{hour:02d}{minute:02d}z.nc4"
                urls.append(url)
            # v2 URL
            else:
                url = f"https://portal.nccs.nasa.gov/datashare/gmao/geos-cf/v2/ana/Y{day.year:04d}/M{day.month:02d}/D{day.day:02d}/GEOS.cf.ana.htf_inst_15mn_glo_L1440x721_slv.{day.strftime('%Y%m%d')}_{hour:02d}{minute:02d}z.R1.nc4"
                urls.append(url)
                url = f"https://portal.nccs.nasa.gov/datashare/gmao/geos-cf/v2/ana/Y{day.year:04d}/M{day.month:02d}/D{day.day:02d}/GEOS.cf.ana.htf_inst_15mn_glo_L1440x721_slv.{day.strftime('%Y%m%d')}_{hour:02d}{minute:02d}z.R0.nc4"
                urls.append(url)
                url = f"https://portal.nccs.nasa.gov/datashare/gmao/geos-cf/v2/ana/Y{day.year:04d}/M{day.month:02d}/D{day.day:02d}/GEOS.cf.ana.htf_inst_15mn_glo_L1440x721_slv.{day.strftime('%Y%m%d')}_{hour:02d}{minute:02d}z.nc4"
                urls.append(url)


    # Download the files
    downloaded_paths = []
    pool = mp.Pool(mp.cpu_count())
    downloaded_paths = pool.starmap(download_file, [(archive_folder, url) for url in urls])
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
    return ds