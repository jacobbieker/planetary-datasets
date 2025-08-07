import xarray as xr
import icechunk as icechunk
from icechunk.xarray import to_icechunk
import glob
import zarr
import numpy as np

files = sorted(list(glob.glob("/Volumes/T7 Shield/IGRA_XR2/*.nc")))
ds = (
    xr.open_mfdataset(files, combine="nested", concat_dim="station", parallel=True)
    .sortby("time")
    .chunk({"station": -1, "time": 100})
    .astype(np.float32)
)
print(ds)
storage = icechunk.local_filesystem_storage("igra_v2_16_levels.icechunk")
repo = icechunk.Repository.open_or_create(storage)
encoding = {
    "time": {
        "units": "milliseconds since 1970-01-01",
        "calendar": "standard",
        "dtype": "int64",
    }
}
variables = []
for var in ds.data_vars:
    if var not in ["orbital_parameters", "start_time", "end_time", "area"]:
        variables.append(var)
encoding.update(
    {
        v: {
            "compressors": zarr.codecs.BloscCodec(
                cname="zstd", clevel=9, shuffle=zarr.codecs.BloscShuffle.bitshuffle
            )
        }
        for v in variables
    }
)
for i in range(0, len(ds.time.values), 1000):
    print(f"Processing time slice {i} to {i + 1000}")
    ds_slice = ds.isel(time=slice(i, i + 1000)).load()
    if len(ds_slice.time) == 0:
        continue
    ds_slice = ds_slice.chunk({"station": -1, "time": 100})
    print(ds_slice)
    session = repo.writable_session("main")
    if i == 0:
        to_icechunk(ds_slice, session, encoding=encoding)
    else:
        to_icechunk(ds_slice, session, append_dim="time")
    session.commit("Add all data for all sites with end date >= 2020")
