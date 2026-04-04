import xarray as xr
import icechunk
from icechunk.xarray import to_icechunk
import pandas as pd
import numpy as np
import glob
import os
import warnings
import zarr.codecs
warnings.filterwarnings("ignore", category=FutureWarning)

base_path = "/Volumes/T9/norway/meps_analysis"

def check_files(file: str) -> bool:
    if os.path.exists(file.replace("hl", "pl")) and os.path.exists(file.replace("hl", "sfc")) \
        and os.path.exists(file.replace("_00_", "_01_")) and os.path.exists(file.replace("_00_", "_02_")) \
            and os.path.exists(file.replace("_00_", "_01_").replace("hl", "pl")) and os.path.exists(file.replace("_00_", "_01_").replace("hl", "sfc")) \
                and os.path.exists(file.replace("_00_", "_02_").replace("hl", "pl")) and os.path.exists(file.replace("_00_", "_02_").replace("hl", "sfc")):
        return True
    else:
        return False


files = glob.glob(f"{base_path}/meps_hl_00*.nc")
first_write = False
for f in files:
    used_files = [f]
    if not check_files(f):
        print(f"Missing files for {f}, skipping")
        continue
    step_dses = []
    try:
        for step in range(3):
            dsh = xr.open_dataset(f.replace("_00_", f"_{str(step).zfill(2)}_").replace("hl", "hl"))
            dspl = xr.open_dataset(f.replace("_00_", f"_{str(step).zfill(2)}_").replace("hl", "pl"))
            dsfc = xr.open_dataset(f.replace("_00_", f"_{str(step).zfill(2)}_").replace("hl", "sfc"))
            used_files.append(f.replace("_00_", f"_{str(step).zfill(2)}_").replace("hl", "hl"))
            used_files.append(f.replace("_00_", f"_{str(step).zfill(2)}_").replace("hl", "pl"))
            used_files.append(f.replace("_00_", f"_{str(step).zfill(2)}_").replace("hl", "sfc"))
            ds = xr.merge([dsh, dspl, dsfc])
            # Process each of the coords other than x, y, time, pressure, longitude, latitude
            icing_dims = ds["icing_index"].dims
            for dim in ds.dims:
                if dim not in ["x", "y", "time", "pressure", "longitude", "latitude"] and dim not in icing_dims:
                    ds = ds.isel({dim: 0})
                    ds = ds.drop_vars(dim)
            for dim in icing_dims:
                if dim not in ["x", "y", "time", "pressure", "longitude", "latitude"]:
                    ds = ds.rename({dim: f"height"})
            step_dses.append(ds)

        storage = icechunk.s3_storage(bucket="us-west-2.opendata.source.coop",
                                          prefix="bkr/dmi/meps.icechunk",

                                          region="us-west-2", )
        #storage = icechunk.local_filesystem_storage(store_path)
        repo = icechunk.Repository.open_or_create(
            storage, config=icechunk.RepositoryConfig.default()
        )
        times = xr.open_zarr(repo.readonly_session("main").store, consolidated=False).coords["time"].values

        ds = xr.concat(step_dses, dim="time").chunk({"time": 1, "y": -1, "x": -1, "pressure": -1, "height": -1}).sortby("pressure").sortby("height")
    except Exception as e:
        print(f"Failed to process {f}: {e}")
        continue
    if ds.time.values[0] in times:
        print(f"MEPS data for {ds.time.values[0]} already exists, skipping...")
        # Delete files here
        for f in used_files:
            try:
                os.remove(f)
            except:
                continue
        continue
    session = repo.writable_session("main")
    if first_write:
        encoding = {}
        for dv in ds.data_vars:
            encoding[dv] = {
                "compressors": zarr.codecs.BloscCodec(
                    cname="zstd", clevel=9, shuffle=zarr.codecs.BloscShuffle.bitshuffle
                )
            }
        encoding["time"] = {"units": "seconds since 1970-01-01", "calendar": "standard", "dtype": "int64"}
        print(ds)
        to_icechunk(ds, session, encoding=encoding)
        session.commit("Initial write of MEPS data")
        first_write = False
    else:
        to_icechunk(ds, session, append_dim="time")
        session.commit(f"Appended MEPS data for time {ds.time.values[0]}")



