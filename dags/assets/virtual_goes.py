import virtualizarr
import xarray as xr
from obstore.store import from_url
import numpy as np

from virtualizarr import open_virtual_dataset, open_virtual_mfdataset
from virtualizarr.parsers import HDFParser
from virtualizarr.registry import ObjectStoreRegistry
import icechunk
import s3fs
from concurrent.futures import ThreadPoolExecutor

fs = s3fs.S3FileSystem(anon=True, client_kwargs={}, asynchronous=False)
bucket = "s3://noaa-goes19"
path = "ABI-L2-MCMIPF/2025/002/01/OR_ABI-L2-MCMIPF-M6_G19_s20250020100206_e20250020109526_c20250020110001.nc"
#path = "ABI-L2-MCMIPF/2025/002/01/OR_ABI-L2-MCMIPF-M6_G18_s20250020100217_e20250020109532_c20250020110021.nc"
url = f"{bucket}/{path}"
store = from_url(bucket, skip_signature=True)
registry = ObjectStoreRegistry({bucket: store})

import warnings

warnings.filterwarnings(
    "ignore",
    message="Numcodecs codecs are not in the Zarr version 3 specification*",
    category=UserWarning,
)

parser = HDFParser()


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

storage = icechunk.local_filesystem_storage("/raid/goes19_mcmipf.icechunk")
repo = icechunk.Repository.open_or_create(storage)

for day_of_year in range(1, 205):
    # Format the day of year as a three-digit string
    day_str = f"{day_of_year:03d}"
    for hour in range(0, 24):
        # Format the hour as a two-digit string
        hour_str = f"{hour:02d}"
        # Construct the path for the current day and hour
        path = f"ABI-L2-MCMIPF/2025/{day_str}/{hour_str}/"
        # List files in the directory
        try:
            files = fs.ls(f"{bucket}/{path}")
            vds = open_virtual_mfdataset(
                ["s3://" + f for f in files],
                parser=parser,
                registry=registry,
                loadable_variables=["y", "x", "t", "number_of_time_bounds", "number_of_image_bounds", "band"],
                decode_times=True,
                combine="nested",
                concat_dim="time",
                data_vars="minimal",
                coords="minimal",
                compat="override",
                parallel=ThreadPoolExecutor,
                preprocess=preprocess,
            )
            print(vds)

            # By default, local virtual references and public remote virtual references can be read without extra configuration.
            session = repo.writable_session("main")

            # write the virtual dataset to the session with the IcechunkStore
            if day_of_year == 1 and hour == 0:
                # If this is the first dataset, write it without appending
                vds.vz.to_icechunk(session.store)
            else:
                vds.vz.to_icechunk(session.store, append_dim="time")
            snapshot_id = session.commit(f"Wrote {day_str} {hour_str}")
            print(snapshot_id)
        except FileNotFoundError:
            print(f"Directory {path} not found, skipping.")
            continue