import icechunk
import zarr
import xarray as xr
import pandas as pd
import datetime as dt
import fsspec
import os

"""
https://thredds.silam.fmi.fi/thredds/fileServer/dust_glob01_v5_7_2/files/SILAM-dust-glob01_v5_7_2_2024111300_001.nc4

SILAM-dust-glob01_v5_7_2_2024111400
"""

def construct_urls_from_datetime(date: dt.datetime) -> list[str]:
    urls = []
    for i in range(1, 121):
        urls.append(f"https://thredds.silam.fmi.fi/thredds/fileServer/dust_glob01_v5_7_2/files/SILAM-dust-glob01_v5_7_2_{date.strftime('%Y%m%d%H')}_{str(i).zfill(3)}.nc4")
    return urls

def download_forecast(date: dt.datetime) -> list[str]:
    urls = construct_urls_from_datetime(date)
    downloaded_paths = []
    for url in urls:
        url_dir = url.split("/")[-1].split("_")[-2]
        # Create the directory
        if not os.path.exists(url_dir):
            os.makedirs(url_dir)
        downloaded_url_path = os.path.join(url_dir, url.split("/")[-1])
        # Download the file to a local directory, try 3 times before giving up
        finished = False
        if os.path.exists(downloaded_url_path):
            # Open to check its valid
            print(f"Already downloaded {url}")
            downloaded_paths.append(downloaded_url_path)
            finished = True
        for i in range(3):
            if finished:
                break
            try:
                with fsspec.open(url, "rb") as f:
                    with open(downloaded_url_path, "wb") as f2:
                        f2.write(f.read())
                finished = True
                downloaded_paths.append(downloaded_url_path)
                break
            except Exception as e:
                continue
        if not finished:
            print(f"Failed to download {url}")
        else:
            print(f"Downloaded {url}")
    return downloaded_paths


def get_silam_dust_forecast_xr(date: dt.datetime) -> xr.Dataset | None:
    downloaded_paths = download_forecast(date)
    print(downloaded_paths)
    if len(downloaded_paths) == 0:
        return None
    datasets = []
    for url in downloaded_paths:
        ds = xr.open_dataset(url)
        datasets.append(ds)
    ds = xr.concat(datasets, dim="time")
    # Get timedelta from all the forecasts from the time dimension
    steps = ds.time.values - ds.time.values[0]
    ds["init_time"] = ds.time.values[0]
    ds["time"] = steps
    ds.rename({"time": "step"})
    # Set init_time as a dimension
    ds = ds.expand_dims("init_time")
    # Rename lat/lon to latitude and longitude
    ds = ds.rename({"lat": "latitude", "lon": "longitude"})
    ds = ds.chunk({"init_time": 1, "step": 1, "latitude": -1, "longitude": -1})
    print(ds)
    return ds


def add_forecast_to_icechunk_store(date: dt.datetime):
    ds = get_silam_dust_forecast_xr(date)
    if ds is None or len(ds.time) < 120:
        return
    if not os.path.exists("./silam_dust"):
        storage_config = icechunk.StorageConfig.filesystem("./silam_dust")
        store = icechunk.IcechunkStore.create(storage_config)
        encoding = {
            variable: {
                "codecs": [zarr.codecs.BytesCodec(), zarr.codecs.ZstdCodec()],
            }
            for variable in ds.data_vars
        }
        ds.to_zarr(store, zarr_format=3, consolidated=False, encoding=encoding)
    else:
        storage_config = icechunk.StorageConfig.filesystem("./silam_dust")
        store = icechunk.IcechunkStore.open_existing(
            storage=storage_config,
            read_only=False,
            mode="a",
        )
        ds.to_zarr(store, append_dim='init_time')
    store.commit(f"Append forecast: {date.strftime('%Y-%m-%d')}")


if __name__ == "__main__":
    # Do it from a week ago to now
    date_range = pd.date_range(
        start=(dt.datetime.now() - pd.Timedelta(days=7)).strftime("%Y-%m-%d"), end=dt.datetime.now().strftime("%Y-%m-%d"), freq="D"
    )
    for day in date_range:
        # If the day is in init_time, then skip it
        if os.path.exists("./silam_dust"):
            storage_config = icechunk.StorageConfig.filesystem("./silam_dust")
            store = icechunk.IcechunkStore.open_existing(
                storage=storage_config,
                read_only=False,
                mode="r",
            )
            current_ds = xr.open_zarr(store, consolidated=False)
            current_times = current_ds.init_time.values
            if day in current_times:
                continue
        try:
            add_forecast_to_icechunk_store(day)
        except Exception as e:
            print(e)
            continue
