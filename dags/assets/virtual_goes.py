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


def process_year(year: str, satellite: str, start_day: int = 1):
    fs = s3fs.S3FileSystem(anon=True, client_kwargs={}, asynchronous=False)
    bucket = f"s3://noaa-{satellite}"
    store = from_url(bucket, skip_signature=True)
    registry = ObjectStoreRegistry({bucket: store})
    """Process a year of GOES data."""
    print(f"Processing {satellite} data for the year {year} starting from day {start_day}.")
    # Iterate over each day of the year
    day_of_year = start_day
    # Format the day of year as a three-digit string
    day_str = f"{day_of_year:03d}"
    files = []
    for day in range(start_day, start_day + 30):
        day_str = f"{day:03d}"
        for hour in range(0, 24):
            # Format the hour as a two-digit string
            hour_str = f"{hour:02d}"
            if os.path.exists(f"/raid/icechunk_30/{satellite}_mcmipf_{year}_{start_day}_{start_day+30}.icechunk"):
                print(f"Dataset for {year}-{day_str} hour {hour} already exists, skipping...")
                continue
            # Construct the path for the current day and hour
            path = f"ABI-L2-MCMIPF/{year}/{day_str}/{hour_str}/"
            # List files in the directory
            try:
                new_files = fs.ls(f"{bucket}/{path}")
                files.extend(new_files)
            except FileNotFoundError:
                print(f"Directory {path} not found, skipping.")
                continue
    if len(files) == 0:
        return
    try:
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
        return

    print(vds)
    # By default, local virtual references and public remote virtual references can be read without extra configuration.
    storage = icechunk.local_filesystem_storage(
        f"/raid/icechunk_30/{satellite}_mcmipf_{year}_{start_day}_{start_day+30}.icechunk")
    repo = icechunk.Repository.open_or_create(storage)
    session = repo.writable_session("main")
    # write the virtual dataset to the session with the IcechunkStore
    vds.vz.to_icechunk(session.store)
    snapshot_id = session.commit(f"Wrote {start_day} - {start_day + 30} days of {satellite} data for {year}")
    print(snapshot_id)

def process_year_wrapper(args):
    """Wrapper function to unpack arguments for multiprocessing."""
    year, satellite, start_day = args
    process_year(year, satellite, start_day)

if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else "2022"
    arg2 = sys.argv[2] if len(sys.argv) > 2 else "goes16"
    satellite = arg2.lower()
    year = arg
    start_day = sys.argv[3] if len(sys.argv) > 3 else 1
    start_day = int(start_day)
    satellites = ["goes17", "goes18", "goes19", "goes16"]
    years = ["2020", "2021", "2022", "2023", "2024", "2025"]
    start_days = list(range(1, 366, 30))
    import multiprocessing as mp
    pool = mp.Pool(mp.cpu_count())
    for _ in pool.imap_unordered(process_year_wrapper, [(y, sat, sd) for y in years for sat in satellites for sd in start_days]):
        pass