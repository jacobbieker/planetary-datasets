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
    # Load the global mosaic
    fs = s3fs.S3FileSystem(anon=True)
    base_url = "s3://noaa-gmgsi-pds/"
    datasets_to_merge = []
    for channel in channels:
        if channel == "vis":
            with fs.open(
                f"{base_url}GMGSI_VIS/{time.year}/{time.month:02}/{time.day:02}/{time.hour:02}/GLOBCOMPVIS_nc.{time.strftime('%Y%m%d%H')}"
            ) as f:
                ds = xr.open_dataset(f).load()
                # rename data to vis
                ds = ds.rename({"data": "vis"})
                datasets_to_merge.append(ds)
        elif channel == "ssr":
            with fs.open(
                f"{base_url}GMGSI_SSR/{time.year}/{time.month:02}/{time.day:02}/{time.hour:02}/GLOBCOMPSSR_nc.{time.strftime('%Y%m%d%H')}"
            ) as f:
                ds_ir = xr.open_dataset(f).load()
                ds_ir = ds_ir.rename({"data": "ssr"})
                datasets_to_merge.append(ds_ir)
        elif channel == "wv":
            with fs.open(
                f"{base_url}GMGSI_WV/{time.year}/{time.month:02}/{time.day:02}/{time.hour:02}/GLOBCOMPWV_nc.{time.strftime('%Y%m%d%H')}"
            ) as f:
                ds_wv = xr.open_dataset(f).load()
                ds_wv = ds_wv.rename({"data": "wv"})
                datasets_to_merge.append(ds_wv)
        elif channel == "lwir":
            with fs.open(
                f"{base_url}GMGSI_LW/{time.year}/{time.month:02}/{time.day:02}/{time.hour:02}/GLOBCOMPLIR_nc.{time.strftime('%Y%m%d%H')}"
            ) as f:
                ds_lw = xr.open_dataset(f).load()
                ds_lw = ds_lw.rename({"data": "lwir"})
                datasets_to_merge.append(ds_lw)
        elif channel == "swir":
            with fs.open(
                f"{base_url}GMGSI_SW/{time.year}/{time.month:02}/{time.day:02}/{time.hour:02}/GLOBCOMPSIR_nc.{time.strftime('%Y%m%d%H')}"
            ) as f:
                ds_sw = xr.open_dataset(f).load()
                ds_sw = ds_sw.rename({"data": "swir"})
                datasets_to_merge.append(ds_sw)
        else:
            raise ValueError(f"Channel {channel=} not recognized")

    # Merge the datasets
    ds = xr.merge(datasets_to_merge)
    return ds


if __name__ == "__main__":
    date_range = pd.date_range(
        start="2021-07-13", end=(dt.datetime.now() - pd.Timedelta(days=1)).strftime("%Y-%m-%d"), freq="D"
    )
    date_range = pd.date_range(start=(dt.datetime.now() - dt.timedelta(days=30)), end=(dt.datetime.now() - dt.timedelta(days=1)).strftime("%Y-%m-%d"), freq = "D")
    os.environ["HF_TOKEN"] = "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"
    fs = HfFileSystem(token=os.environ["HF_TOKEN"])
    api = HfApi(token=os.environ["HF_TOKEN"])
    #fs = HfFileSystem()
    #api = HfApi()
    start_idx = random.randint(0, len(date_range))
    for day in date_range[::-1]:
        day_outname = day.strftime("%Y%m%d")
        year = day.year
        dses = []
        if fs.exists(
            f"datasets/jacobbieker/global-mosaic-of-geostationary-images/data/{year}/{day_outname}.zarr.zip"
        ):
            # Check there are 24 timesteps in the file, if not, redo it
            # Download file to disk and then open it
            print(f"Skipping {day_outname} as it exists")
            continue
        for hour in range(0, 24):
            timestep = day + dt.timedelta(hours=hour)
            ds = get_global_mosaic(timestep)
            dses.append(ds)
        if len(dses) != 24:
            print(f"Skipping {day_outname} as it has {len(dses)} timesteps")
            continue
        ds = xr.concat(dses, dim="time")
        with zarr.storage.ZipStore(day_outname + ".zarr.zip", mode="w") as store:
            # encodings
            enc = {
                variable: {
                    "codecs": [zarr.codecs.BytesCodec(), zarr.codecs.ZstdCodec()],
                }
                for variable in ds.data_vars
            }
            ds.to_zarr(store, mode="w", compute=True, encoding=enc, zarr_format=3, consolidated=True)
        api.upload_file(
            path_or_fileobj=day_outname + ".zarr.zip",
            path_in_repo=f"data/{year}/{day_outname}.zarr.zip",
            repo_id="jacobbieker/global-mosaic-of-geostationary-images",
            repo_type="dataset",
        )
        ds.close()
        del ds
        os.remove(day_outname + ".zarr.zip")
        print(f"Saved {day_outname}")
