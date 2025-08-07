import datetime as dt
import os

import dask.array
import fsspec
import numpy as np
import pandas as pd
import xarray as xr
import zarr

FSSPEC_S3_ENDPOINT_URL = "https://data.source.coop"


def open_netcdf(filename: str) -> xr.Dataset:
    data = xr.open_dataset(filename)
    data = data.rename({"lat": "latitude", "lon": "longitude"})
    # Add init time as a dimension
    timestamp = filename.split("/")[-1].split("_")[4]
    data.coords["init_time"] = pd.Timestamp(
        f"{timestamp[:4]}-{timestamp[4:6]}-{timestamp[6:8]}T{timestamp[8:10]}:00:00"
    )
    # Change time to step by getting timedelta
    steps = data.time.values - data.init_time.values
    data["time"] = steps
    # rename time to step
    data = data.rename({"time": "step"})
    data = data.drop_dims("hybrid_half")
    data = data.drop_vars(["a", "b", "da", "db"])
    data = data.isel(hybrid=0)
    # Drop hybrid
    data = data.drop_vars("hybrid")
    # Set init_time as dimension
    data = data.expand_dims("init_time")
    # Drop any variables that aren't (step, hybrid, latitude, longitude) or (step, latitude, longitude)
    data = data.chunk(({"init_time": 1, "step": -1, "latitude": -1, "longitude": -1}))
    return data


"""
https://thredds.silam.fmi.fi/thredds/fileServer/dust_glob01_v5_7_2/files/SILAM-dust-glob01_v5_7_2_2024111300_001.nc4

https://thredds.silam.fmi.fi/thredds/fileServer/dust_glob01_v5_7_2/files/SILAM-dust-glob01_v5_7_2_2024112700_060.nc4

SILAM-dust-glob01_v5_7_2_2024111400
"""


def construct_urls_from_datetime(date: dt.datetime) -> list[str]:
    urls = []
    for i in range(1, 121):
        urls.append(
            f"https://thredds.silam.fmi.fi/thredds/fileServer/dust_glob01_v5_7_2/files/SILAM-dust-glob01_v5_7_2_{date.strftime('%Y%m%d00')}_{str(i).zfill(3)}.nc4"
        )
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
            except Exception:
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
    return ds


if __name__ == "__main__":
    # Do it from a week ago to now
    zarr_date_range = pd.date_range(start="2024-11-14", end="2026-12-31", freq="D")

    date_range = pd.date_range(
        start="2024-11-14", end=dt.datetime.now().strftime("%Y-%m-%d"), freq="D"
    )
    # for day in date_range:
    #    download_forecast(day)

    s3_path = "s3://bkr/silam-dust/silam_global_dust.zarr"
    path = "silam_global_dust.zarr"
    import glob

    if not os.path.exists(path):
        print("Path Not Existing")
        files = list(sorted(glob.glob("*2024111400*.nc4")))

        ds = []
        for f in files:
            data = open_netcdf(f)
            ds.append(data)

        data = xr.concat(ds, dim="step").sortby("step")
        print(data)

        variables = list(data.data_vars)

        encoding = {
            v: {
                "compressors": zarr.codecs.BloscCodec(
                    cname="zstd", clevel=9, shuffle=zarr.codecs.BloscShuffle.bitshuffle
                )
            }
            for v in variables
        }

        # Create empty dask arrays of the same size as the data
        dask_arrays = []
        dummies_4d_var = dask.array.zeros(
            (len(zarr_date_range), len(data.step), data.latitude.shape[0], data.longitude.shape[0]),
            chunks=(1, 1, -1, -1),
            dtype=np.float32,
        )
        default_dataarray = xr.DataArray(
            dummies_4d_var,
            coords={
                "init_time": zarr_date_range,
                "step": data.step.values,
                "latitude": data.latitude.values,
                "longitude": data.longitude.values,
            },
            dims=["init_time", "step", "latitude", "longitude"],
        )
        dummy_dataset = xr.Dataset(
            {v: default_dataarray for v in variables},
            coords={
                "init_time": zarr_date_range,
                "step": data.step.values,
                "latitude": data.latitude.values,
                "longitude": data.longitude.values,
            },
        )
        print(dummy_dataset)
        # storage_options={"endpoint_url": "https://data.source.coop"}
        dummy_dataset.chunk({"init_time": 1, "step": 1, "latitude": -1, "longitude": -1}).to_zarr(
            path,
            mode="w",
            compute=False,
            zarr_format=3,
            encoding=encoding,
        )  # storage_options={"endpoint_url": "https://data.source.coop"})
    zarr_dates = xr.open_zarr(path).init_time.values
    for day in date_range:
        print(day)
        # If the day is in init_time, then skip it
        # download_forecast(day)
        # Local path of the downloaded files
        files = list(sorted(glob.glob(f"*{day.strftime('%Y%m%d00')}*.nc4")))
        if not files:
            files = list(sorted(glob.glob(f"{day.strftime('%Y%m%d00')}/*.nc4")))
            if not files:
                continue
        ds = []
        failed = False
        for f in files:
            try:
                data = open_netcdf(f)
                ds.append(data)
            except ValueError as e:
                print(f"Corrupted File: {e}, skipping init time, filename: {f}")
                failed = True
                for d in ds:
                    d.close()
                continue
        if failed:
            continue
        data = (
            xr.concat(ds, dim="step")
            .sortby("step")
            .chunk({"step": 1, "latitude": -1, "longitude": -1, "init_time": 1})
        )
        # print(data)
        # Get the time idx in the zarr in the init_time dimension
        # Get index of the day in the zarr times
        time_idx = np.where(zarr_dates == day)[0][0]
        print(time_idx)
        # Write the data to the zarr
        data.to_zarr(
            path,
            region={
                "init_time": slice(time_idx, time_idx + 1),
                "latitude": "auto",
                "longitude": "auto",
                "step": 1,
            },
        )
        print(f"Finished Writing: {day} to location: {time_idx}")
        # Now close the dataset to close all files
        data.close()
