"""This gather and uses the global mosaic of geostationary satellites from NOAA on AWS"""
import xarray as xr
import s3fs
import os
import pandas as pd
import datetime as dt
from huggingface_hub import HfApi, HfFileSystem
import random
import zarr
from typing import Optional
from numcodecs import Blosc
import dask.array
from tqdm import tqdm
import numpy as np
import fsspec


def get_global_mosaic(time: dt.datetime, channels: Optional[list[str]] = None) -> xr.Dataset:
    """Gather the global mosaic of geostationary satellites from NOAA on AWS

    Args:
        time: datetime object
        channels: list of channels to include, if None, all channels are included

    Returns:
        xarray object with global mosaic
    """
    if channels is None:
        channels = ["vis", "ssr", "wv", "lwir", "swir"]
    base_url = "/run/media/jacob/Tester/gmgsi/"
    datasets_to_merge = []
    for channel in channels:
        if channel == "vis":
            with fsspec.open(
                f"{base_url}GMGSI_VIS/{time.year}/{time.month:02}/{time.day:02}/{time.hour:02}/GLOBCOMPVIS_nc.{time.strftime('%Y%m%d%H')}"
            ) as f:
                ds = xr.open_dataset(f).load()
                # rename data to vis
                ds = ds.rename({"data": "vis"}).astype(np.uint8)
                datasets_to_merge.append(ds)
        elif channel == "ssr":
            with fsspec.open(
                f"{base_url}GMGSI_SSR/{time.year}/{time.month:02}/{time.day:02}/{time.hour:02}/GLOBCOMPSSR_nc.{time.strftime('%Y%m%d%H')}"
            ) as f:
                ds_ir = xr.open_dataset(f).load()
                ds_ir = ds_ir.rename({"data": "ssr"}).astype(np.uint8)
                datasets_to_merge.append(ds_ir)
        elif channel == "wv":
            with fsspec.open(
                f"{base_url}GMGSI_WV/{time.year}/{time.month:02}/{time.day:02}/{time.hour:02}/GLOBCOMPWV_nc.{time.strftime('%Y%m%d%H')}"
            ) as f:
                ds_wv = xr.open_dataset(f).load()
                ds_wv = ds_wv.rename({"data": "wv"}).astype(np.uint8)
                datasets_to_merge.append(ds_wv)
        elif channel == "lwir":
            with fsspec.open(
                f"{base_url}GMGSI_LW/{time.year}/{time.month:02}/{time.day:02}/{time.hour:02}/GLOBCOMPLIR_nc.{time.strftime('%Y%m%d%H')}"
            ) as f:
                ds_lw = xr.open_dataset(f).load()
                ds_lw = ds_lw.rename({"data": "lwir"}).astype(np.uint8)
                datasets_to_merge.append(ds_lw)
        elif channel == "swir":
            with fsspec.open(
                f"{base_url}GMGSI_SW/{time.year}/{time.month:02}/{time.day:02}/{time.hour:02}/GLOBCOMPSIR_nc.{time.strftime('%Y%m%d%H')}"
            ) as f:
                ds_sw = xr.open_dataset(f).load()
                # Set the dtype to
                ds_sw = ds_sw.rename({"data": "swir"}).astype(np.uint8)
                datasets_to_merge.append(ds_sw)
        else:
            raise ValueError(f"Channel {channel=} not recognized")

    # Merge the datasets
    ds = xr.merge(datasets_to_merge)
    return ds


variables = ["vis", "ssr", "wv", "lwir", "swir"]
encoding = {v: {"codecs": [zarr.codecs.BytesCodec(), zarr.codecs.ZstdCodec()]} for v in variables}
encoding["time"] = {"units": "nanoseconds since 1970-01-01"}

if __name__ == "__main__":
    date_range = pd.date_range(
        start="2021-07-13", end=(dt.datetime.now() - pd.Timedelta(days=1)).strftime("%Y-%m-%d"), freq="D"
    )
    # Check which days are complete (i.e. have 24 files in it, for all channels)
    valid_days = []
    for date in date_range:
        all_exists = True
        for hour in range(0, 24):
            if not os.path.exists(
                f"/run/media/jacob/Tester/gmgsi/GMGSI_VIS/{date.year}/{date.month:02}/{date.day:02}/{date.hour:02}/GLOBCOMPVIS_nc.{date.strftime('%Y%m%d%H')}"
            ) or not os.path.exists(f"/run/media/jacob/Tester/gmgsi/GMGSI_SSR/{date.year}/{date.month:02}/{date.day:02}/{date.hour:02}/GLOBCOMPSSR_nc.{date.strftime('%Y%m%d%H')}")\
                    or not os.path.exists(f"/run/media/jacob/Tester/gmgsi/GMGSI_WV/{date.year}/{date.month:02}/{date.day:02}/{date.hour:02}/GLOBCOMPWV_nc.{date.strftime('%Y%m%d%H')}")\
                    or not os.path.exists(f"/run/media/jacob/Tester/gmgsi/GMGSI_LW/{date.year}/{date.month:02}/{date.day:02}/{date.hour:02}/GLOBCOMPLIR_nc.{date.strftime('%Y%m%d%H')}")\
                    or not os.path.exists(f"/run/media/jacob/Tester/gmgsi/GMGSI_SW/{date.year}/{date.month:02}/{date.day:02}/{date.hour:02}/GLOBCOMPSIR_nc.{date.strftime('%Y%m%d%H')}"):
                print(f"Missing {date.strftime('%Y%m%d')} {hour:02}")
                all_exists = False
        if all_exists:
            valid_days.append(date)

    # Make all available days and datetime for time
    timestamps = []
    for day in valid_days:
        for hour in range(0, 24):
            timestamps.append(day + pd.Timedelta(hours=hour))

    path = "gmgsi.zarr"

    data = get_global_mosaic(timestamps[0])
    data = data.rename({"lat": "latitude", "lon": "longitude"})
    print(data)
    print(data["vis"])
    # Create empty dask arrays of the same size as the data
    dask_arrays = []

    dummies = dask.array.zeros((len(timestamps), data.xc.shape[0], data.yc.shape[0]), chunks=(1, -1, -1), dtype=np.uint8)
    default_dataarray = xr.DataArray(dummies, coords={"time": timestamps, "latitude": (["yc", "xc"], data.latitude.values),
                                       "longitude": (["yc", "xc"], data.longitude.values)},
                     dims=["time", "xc", "yc"])
    dummy_dataset = xr.Dataset({v: default_dataarray for v in variables},
                               coords={"time": timestamps, "latitude": (["yc", "xc"], data.latitude.values), "longitude": (["yc", "xc"], data.longitude.values)})
    print(dummy_dataset)
    dummy_dataset.to_zarr(path, mode="w", compute=False, zarr_format=3, consolidated=True, encoding=encoding)

    for time_idx, day in tqdm(enumerate(timestamps)):
        data = get_global_mosaic(day)
        data.chunk({"time": 1, "yc": -1, "xc": -1}).rename({"lat": "latitude", "lon": "longitude"}).to_zarr(path,region={"time": slice(time_idx, time_idx+1),"xc": "auto", "yc": "auto"}, )
