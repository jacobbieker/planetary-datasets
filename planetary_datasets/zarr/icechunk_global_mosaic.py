import icechunk
import zarr
import os
import glob
import xarray as xr
import tqdm
import pandas as pd

def get_timestamps_from_name(name) -> list[pd.Timestamp]:
    # 20210814.zarr.zip
    day = pd.to_datetime(name.split("/")[-1].split(".")[0], format="%Y%m%d")
    # Now get all hours from 00:00 to 23:00
    return [day + pd.Timedelta(hours=i) for i in range(24)]


if __name__ == "__main__":
    storage_config = icechunk.StorageConfig.filesystem("./global_mosaic")
    if not os.path.exists("./global_mosaic"):
        store = icechunk.IcechunkStore.create(storage_config)
    else:
        store = icechunk.IcechunkStore.open_existing(
            storage=storage_config,
            read_only=True,
        )
        new_store = icechunk.IcechunkStore.create(icechunk.StorageConfig.filesystem("./global_mosaic_of_geostationary_images"))
    files = sorted(list(glob.glob("data/*/*.zarr.zip")))
    # Get the first one
    first = files.pop(0)
    with zarr.storage.ZipStore(first, mode="r") as zipped_store:
        ds = xr.open_zarr(zipped_store)
        # Remove all chunking here
        for var in ds:
            del ds[var].encoding['chunks']
            del ds[var].encoding['compressor']
        for var in ds.coords:
            del ds[var].encoding['chunks']
            del ds[var].encoding['compressor']
        encoding = {
            variable: {
                "codecs": [zarr.codecs.BytesCodec(), zarr.codecs.ZstdCodec()],
            }
            for variable in ds.data_vars
        }
        ds = ds.rename({"lat": "latitude", "lon": "longitude"}).load()
        print(ds)
        ds.chunk({"time": 1, "yc": -1, "xc": -1}).to_zarr("global_mosaic.zarr", zarr_format=3, consolidated=True,encoding=encoding, mode="w", compute=True)
        #store.commit(f"Append file: {first}")
    for file in tqdm.tqdm(files, total=len(files)):
        with zarr.storage.ZipStore(first, mode="r") as zipped_store:
            ds = xr.open_zarr(zipped_store)
            for var in ds:
                del ds[var].encoding['chunks']
                del ds[var].encoding['compressor']
            ds = ds.rename({"lat": "latitude", "lon": "longitude"}).load()
            print(ds)
            ds.chunk({"time": 1, "yc": -1, "xc": -1}).to_zarr("global_mosaic.zarr", append_dim='time', mode="a", compute=True)
            #store.commit(f"Append file: {file}")
            print(f"Append file: {file}")
