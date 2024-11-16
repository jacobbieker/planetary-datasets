import icechunk
import zarr
import os
import glob
import xarray as xr
import tqdm


if __name__ == "__main__":
    storage_config = icechunk.StorageConfig.filesystem("./global_mosaic")
    store = icechunk.IcechunkStore.create(storage_config)
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
        ds.chunk({"time": 1, "yc": -1, "xc": -1}).to_zarr(store, zarr_format=3, consolidated=False,encoding=encoding)
        store.commit(f"Append file: {first}")
    for file in tqdm.tqdm(files, total=len(files)):
        with zarr.storage.ZipStore(first, mode="r") as zipped_store:
            ds = xr.open_zarr(zipped_store)
            for var in ds:
                del ds[var].encoding['chunks']
                del ds[var].encoding['compressor']
            ds = ds.rename({"lat": "latitude", "lon": "longitude"}).load()
            ds.chunk({"time": 1, "yc": -1, "xc": -1}).to_zarr(store, append_dim='time')
            store.commit(f"Append file: {file}")
            print(f"Append file: {file}")
