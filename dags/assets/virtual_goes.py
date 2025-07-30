import sys
import xarray as xr
from obstore.store import from_url

from virtualizarr import open_virtual_dataset, open_virtual_mfdataset
from virtualizarr.parsers import HDFParser
from virtualizarr.registry import ObjectStoreRegistry
import icechunk
import s3fs
import os
from concurrent.futures import ThreadPoolExecutor

import warnings

warnings.filterwarnings(
    "ignore",
    message="Numcodecs codecs are not in the Zarr version 3 specification*",
    category=UserWarning,
)

parser = HDFParser()
coords = [
    'y',
    'x',
    't',
    'y_image',
    'x_image',
    'band_wavelength_C01',
    'band_wavelength_C02',
    'band_wavelength_C03',
    'band_wavelength_C04',
    'band_wavelength_C05',
    'band_wavelength_C06',
    'band_wavelength_C07',
    'band_wavelength_C08',
    'band_wavelength_C09',
    'band_wavelength_C10',
    'band_wavelength_C11',
    'band_wavelength_C12',
    'band_wavelength_C13',
    'band_wavelength_C14',
    'band_wavelength_C15',
    'band_wavelength_C16',
    'band_id_C01',
    'band_id_C02',
    'band_id_C03',
    'band_id_C04',
    'band_id_C05',
    'band_id_C06',
    'band_id_C07',
    'band_id_C08',
    'band_id_C09',
    'band_id_C10',
    'band_id_C11',
    'band_id_C12',
    'band_id_C13',
    'band_id_C14',
    'band_id_C15',
    'band_id_C16'
]


def preprocess(vds: xr.Dataset) -> xr.Dataset:
    """Preprocess the dataset to ensure it has the correct dimensions and variables."""
    # Set y and x as data variables
    vds["y_coordinates"] = xr.DataArray(
        vds["y"].values,
        dims=["y"],
    )
    vds["x_coordinates"] = xr.DataArray(
        vds["x"].values,
        dims=["x"],
    )
    vds = vds.rename({"t": "time"})
    return vds


def process_year(satellite: str):
    fs = s3fs.S3FileSystem(anon=True, client_kwargs={}, asynchronous=False)
    bucket = f"s3://noaa-{satellite}"
    store = from_url(bucket, skip_signature=True)
    registry = ObjectStoreRegistry({bucket: store})
    start_day = 1
    """Process a year of GOES data."""
    print(f"Processing {satellite}")
    # By default, local virtual references and public remote virtual references can be read without extra configuration.
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(f"s3://noaa-{satellite}", icechunk.s3_store(region="us-east-1", anonymous=True)),
    )
    storage = icechunk.local_filesystem_storage(
        f"icechunk_append/{satellite}_mcmipf.icechunk")
    repo = icechunk.Repository.open_or_create(
        storage, config=config, authorize_virtual_chunk_access={f"s3://noaa-{satellite}": None},
    )
    repo.save_config()
    first_write = True
    for year in range(2018, 2026):
        files = []
        for day in range(start_day, 366):
            day_str = f"{day:03d}"
            for hour in range(0, 24):
                # Format the hour as a two-digit string
                hour_str = f"{hour:02d}"
                # Construct the path for the current day and hour
                path = f"ABI-L2-MCMIPF/{year}/{day_str}/{hour_str}/"
                # List files in the directory
                try:
                    new_files = fs.ls(f"{bucket}/{path}")
                    files = new_files
                except FileNotFoundError:
                    print(f"Directory {path} not found, skipping.")
                    continue
            if len(files) == 0:
                continue
            try:
                print(f"Processing {len(files)} files...")
                vds = open_virtual_mfdataset(
                    ["s3://" + f for f in files],
                    parser=parser,
                    registry=registry,
                    loadable_variables=coords,
                    decode_times=True,
                    combine="nested",
                    concat_dim="time",
                    data_vars="minimal",
                    coords="minimal",
                    compat="override",
                    parallel=ThreadPoolExecutor,
                    preprocess=preprocess,
                )
            except Exception as e:
                print(f"Failed to process {year}-{day_str}: {e}")
                continue

            session = repo.writable_session("main")
            # write the virtual dataset to the session with the IcechunkStore
            try:
                if first_write:
                    vds.vz.to_icechunk(session.store)
                    first_write = False
                else:
                    vds.vz.to_icechunk(session.store, append_dim="time")
                snapshot_id = session.commit(f"Wrote {year} {start_day} day of {satellite} data for {year}")
                print(snapshot_id)
            except Exception as e:
                print(f"Failed to write {year}-{day_str} data: {e}")
                continue

if __name__ == "__main__":
    import multiprocessing as mp
    #mp.set_start_method("forkserver")
    satellites = [ "goes18", "goes19", "goes16", "goes17",]
    #satellites = [ "goes18"]
    #satellites = ["goes19"]
    #satellites = ["goes16"]
    #satellites = ["goes17"]
    pool = mp.Pool(mp.cpu_count())
    for _ in pool.map(process_year, satellites):
        pass