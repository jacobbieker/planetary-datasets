import icechunk
import xarray as xr
import numpy as np
from icechunk.xarray import to_icechunk
import zarr

# Open the data from the zarr store
storage = icechunk.local_filesystem_storage("/data/AOML/aoml_bouys.icechunk")
repo = icechunk.Repository.open(storage)
session = repo.readonly_session("main")
ds = xr.open_zarr(session.store, consolidated=False)
# Select only timestamps from 2012 onwards, others are probably not useful
ds = ds.where(ds.time >= np.datetime64("2012-01-01"), drop=True)
print(ds)

# Get all unique platform_ids
platform_ids = ds.platform_id.values
unique_platform_ids = np.unique(platform_ids)
print(f"Found {len(unique_platform_ids)} unique platform IDs.")
# Create a new Dataset for each platform_id
dses = []
times = []
for platform_id in unique_platform_ids:
    platform_ds = ds.where((ds.platform_id == platform_id).compute(), drop=True).compute().drop_duplicates("time")
    if len(platform_ds.time) > 0:
        print(f"Processing platform_id: {platform_id} with {len(platform_ds.time)} time steps.")
        platform_type = platform_ds.platform_type.values[0]
        platform_ds = platform_ds.drop_vars(["platform_id", "platform_type"])
        platform_ds = platform_ds.assign_coords({"platform_type": [platform_type]})
        platform_ds = platform_ds.expand_dims({"platform_id": [platform_id]})
        platform_ds = platform_ds.chunk({"time": 1000})
        print(platform_ds)
        times.extend(platform_ds.time.values)
        dses.append(platform_ds)
# Combine all datasets into a single dataset, concatenating along the platform_id dimension
# Get the union of all time values
times = np.unique(times)
# Order the times
times = np.sort(times)
# Make the times the same across all datasets
for i, platform_ds in enumerate(dses):
    platform_ds = platform_ds.reindex({"time": times})
    dses[i] = platform_ds

combined_ds = xr.concat(dses, dim="platform_id", join="outer")
print(combined_ds)
# Save the combined dataset back to icechunk
storage = icechunk.local_filesystem_storage("/data/AOML/aoml_bouys_per_id.icechunk")
repo = icechunk.Repository.open_or_create(storage)
session = repo.writable_session("main")
to_icechunk(
    combined_ds.chunk({"time": 1000, "platform_id": -1}),
    session,
    encoding={
        "time": {
            "units": "nanoseconds since 1970-01-01",
            "calendar": "standard",
            "dtype": "int64",
        },
        **{
            v: {
                "compressors": zarr.codecs.BloscCodec(
                    cname="zstd", clevel=9, shuffle=zarr.codecs.BloscShuffle.bitshuffle
                )
            }
            for v in combined_ds.data_vars
        },
    },
)
session.commit("Reorganized AOML bouys data by platform_id")
