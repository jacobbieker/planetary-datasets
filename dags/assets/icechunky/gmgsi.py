import icechunk
import xarray as xr
import zarr
from icechunk.xarray import to_icechunk
import numpy as np
import glob
import fsspec
import datetime as dt
import os
import shutil
from typing import Optional
import tqdm


base_url = "/data/gmgsi/"

def get_global_mosaic_v3(time: dt.datetime, channels: Optional[list[str]] = None) -> xr.Dataset:
    """Gather the global mosaic of geostationary satellites from NOAA on AWS

    Args:
        time: datetime object
        channels: list of channels to include, if None, all channels are included

    Returns:
        xarray object with global mosaic
    """
    if channels is None:
        channels = ["vis", "wv", "lwir", "swir"]
    datasets_to_merge = []
    for channel in channels:
        if channel == "vis":
            # Get the full file name
            filepath = list(
                glob.glob(
                    f"{base_url}GMGSI_VIS/{time.year}/{time.month:02}/{time.day:02}/{time.hour:02}/GLOBCOMPVIS_v3r0_blend_s{time.strftime('%Y%m%d%H')}*"
                )
            )[0]
            with fsspec.open(filepath) as f:
                ds = xr.open_dataset(f).load()
                ds["dqf"] = ds["dqf"].fillna(255)
                ds = ds.rename({"data": "vis", "dqf": "vis_dqf"})
                # Drop other variables
                for dv in ds.data_vars:
                    if dv not in ["vis", "vis_dqf"]:
                        ds = ds.drop_vars(dv)
                datasets_to_merge.append(ds.astype(np.uint8))
        elif channel == "wv":
            filepath = list(
                glob.glob(
                    f"{base_url}GMGSI_WV/{time.year}/{time.month:02}/{time.day:02}/{time.hour:02}/GLOBCOMPWV_v3r0_blend_s{time.strftime('%Y%m%d%H')}*"
                )
            )[0]
            with fsspec.open(filepath) as f:
                ds_wv = xr.open_dataset(f).load()
                ds_wv["dqf"] = ds_wv["dqf"].fillna(255)
                ds_wv = ds_wv.rename({"data": "wv", "dqf": "wv_dqf"})
                for dv in ds_wv.data_vars:
                    if dv not in ["wv", "wv_dqf"]:
                        ds_wv = ds_wv.drop_vars(dv)
                datasets_to_merge.append(ds_wv.astype(np.uint8))
        elif channel == "lwir":
            filepath = list(
                glob.glob(
                    f"{base_url}GMGSI_LW/{time.year}/{time.month:02}/{time.day:02}/{time.hour:02}/GLOBCOMPLIR_v3r0_blend_s{time.strftime('%Y%m%d%H')}*"
                )
            )[0]
            with fsspec.open(filepath) as f:
                ds_lw = xr.open_dataset(f).load()
                ds_lw["dqf"] = ds_lw["dqf"].fillna(255)
                ds_lw = ds_lw.rename({"data": "lwir", "dqf": "lwir_dqf"})
                for dv in ds_lw.data_vars:
                    if dv not in ["lwir", "lwir_dqf"]:
                        ds_lw = ds_lw.drop_vars(dv)
                datasets_to_merge.append(ds_lw.astype(np.uint8))
        elif channel == "swir":
            filepath = list(
                glob.glob(
                    f"{base_url}GMGSI_SW/{time.year}/{time.month:02}/{time.day:02}/{time.hour:02}/GLOBCOMPSIR_v3r0_blend_s{time.strftime('%Y%m%d%H')}*"
                )
            )[0]
            with fsspec.open(filepath) as f:
                ds_sw = xr.open_dataset(f).load()
                ds_sw["dqf"] = ds_sw["dqf"].fillna(255)
                ds_sw = ds_sw.rename({"data": "swir", "dqf": "swir_dqf"})
                for dv in ds_sw.data_vars:
                    if dv not in ["swir", "swir_dqf"]:
                        ds_sw = ds_sw.drop_vars(dv)
                datasets_to_merge.append(ds_sw.astype(np.uint8))
        else:
            raise ValueError(f"Channel {channel=} not recognized")

    # Merge the datasets
    ds = xr.merge(datasets_to_merge)
    return ds


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

import pandas as pd

v3_range = pd.date_range("2025-03-10-17:00", pd.Timestamp.now(), freq="1H")
v1_range = pd.date_range("2021-07-13-00:00", pd.Timestamp.now(), freq="1H")

for store_path, date_range in [("/data/gmgsi/gmgsi_v3.icechunk", v3_range),]:
    if os.path.exists(store_path):
        shutil.rmtree(store_path)
    storage = icechunk.local_filesystem_storage(store_path)
    repo = icechunk.Repository.open_or_create(
        storage, config=icechunk.RepositoryConfig.default()
    )
    first_write = True
    get_mosaic = get_global_mosaic if "v1" in store_path else get_global_mosaic_v3
    for date in tqdm.tqdm(date_range, desc=f"Processing GMGSI data for {store_path}", total=len(date_range)):
        try:
            data = get_mosaic(date)
        except Exception as e:
            print(f"Failed to get GMGSI data for {date}: {e}")
            continue
        data = data.rename({"lat": "latitude", "lon": "longitude"})
        variables = list(data.data_vars)
        encoding = {
            v: {
                "compressors": zarr.codecs.BloscCodec(
                    cname="zstd", clevel=9, shuffle=zarr.codecs.BloscShuffle.bitshuffle
                )
            }
            for v in variables
        }
        encoding["time"] = {"units": "nanoseconds since 1970-01-01"}
        #data = data.assign_coords({"time": date})
        if first_write:
            session = repo.writable_session("main")
            to_icechunk(data, session, encoding=encoding)
            session.commit("Initial write of GMGSI data")
            first_write = False
        else:
            session = repo.writable_session("main")
            to_icechunk(data, session, append_dim="time")
            session.commit(f"Added GMGSI data for {date}")
