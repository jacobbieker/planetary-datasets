"""This gather and uses the global mosaic of geostationary satellites from NOAA on AWS"""
import datetime as dt
import os
from typing import TYPE_CHECKING, Optional

import dagster
import dagster as dg
import dask.array
import fsspec
import s3fs
import numpy as np
import pandas as pd
import xarray as xr
import zarr
import glob

if TYPE_CHECKING:
    import datetime as dt

"""Zarr archive of satellite image data from GMGSI global mosaic of geostationary satellites from NOAA on AWS"""

ARCHIVE_FOLDER = "/ext_data/gmgsi/"
BASE_URL = "s3://noaa-gmgsi-pds/"
ZARR_PATH = "/ext_data/gmgsi.zarr"
ZARR_V3_PATH = "/ext_data/gmgsi_v3blend.zarr"
if os.getenv("ENVIRONMENT", "local") == "pb":
    ARCHIVE_FOLDER = "/ext_data/gmgsi/"

partitions_def: dg.TimeWindowPartitionsDefinition = dg.HourlyPartitionsDefinition(
    start_date="2021-07-13-00:00",
    end_offset=-2,
)

partitions_def_v3: dg.TimeWindowPartitionsDefinition = dg.HourlyPartitionsDefinition(
    start_date="2025-03-10-17:00",
    end_offset=-2,
)


@dg.asset(name="gmgsi-download", description="Download GMGSI global mosaic of geostationary satellites from NOAA on AWS",
          tags={
              "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
              "dagster/priority": "1",
              "dagster/concurrency_key": "download",
          },
          partitions_def=partitions_def,
automation_condition=dg.AutomationCondition.eager(),
          )
def gmgsi_download_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    it: dt.datetime = context.partition_time_window.start

    fs = s3fs.S3FileSystem(anon=True)
    # Check if the local file exists before downloading
    for channel in ["VIS", "SSR", "WV"]:
        s3_uri = f"{BASE_URL}GMGSI_{channel}/{it.year}/{it.month:02}/{it.day:02}/{it.hour:02}/GLOBCOMP{channel}_nc.{it.strftime('%Y%m%d%H')}"
        local_uri = f"{ARCHIVE_FOLDER}GMGSI_{channel}/{it.year}/{it.month:02}/{it.day:02}/{it.hour:02}/GLOBCOMP{channel}_nc.{it.strftime('%Y%m%d%H')}"
        if not os.path.exists(local_uri):
            fs.get(s3_uri, local_uri)
            context.log.info(msg=f"Downloaded {s3_uri} to {local_uri}")
        else:
            context.log.info(msg=f"Already downloaded {s3_uri} to {local_uri}")
    for channel, name in [("LW", "LIR"), ("SW", "SIR")]:
        s3_uri = f"{BASE_URL}GMGSI_{channel}/{it.year}/{it.month:02}/{it.day:02}/{it.hour:02}/GLOBCOMP{name}_nc.{it.strftime('%Y%m%d%H')}"
        local_uri = f"{ARCHIVE_FOLDER}GMGSI_{channel}/{it.year}/{it.month:02}/{it.day:02}/{it.hour:02}/GLOBCOMP{name}_nc.{it.strftime('%Y%m%d%H')}"
        if not os.path.exists(local_uri):
            fs.get(s3_uri, local_uri)
            context.log.info(msg=f"Downloaded {s3_uri} to {local_uri}")
        else:
            context.log.info(msg=f"Already downloaded {s3_uri} to {local_uri}")
    # Return the paths as a materialization
    return dg.MaterializeResult(
        metadata={
            "vis": f"{ARCHIVE_FOLDER}GMGSI_VIS/{it.year}/{it.month:02}/{it.day:02}/{it.hour:02}/GLOBCOMPVIS_nc.{it.strftime('%Y%m%d%H')}",
            "ssr": f"{ARCHIVE_FOLDER}GMGSI_SSR/{it.year}/{it.month:02}/{it.day:02}/{it.hour:02}/GLOBCOMPSSR_nc.{it.strftime('%Y%m%d%H')}",
            "wv": f"{ARCHIVE_FOLDER}GMGSI_WV/{it.year}/{it.month:02}/{it.day:02}/{it.hour:02}/GLOBCOMPWV_nc.{it.strftime('%Y%m%d%H')}",
            "lwir": f"{ARCHIVE_FOLDER}GMGSI_LW/{it.year}/{it.month:02}/{it.day:02}/{it.hour:02}/GLOBCOMPLIR_nc.{it.strftime('%Y%m%d%H')}",
            "swir": f"{ARCHIVE_FOLDER}GMGSI_SW/{it.year}/{it.month:02}/{it.day:02}/{it.hour:02}/GLOBCOMPSIR_nc.{it.strftime('%Y%m%d%H')}",
        },
    )

@dg.asset(name="gmgsi-v3-download", description="Download GMGSI global mosaic of geostationary satellites from NOAA on AWS",
          tags={
              "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
              "dagster/priority": "1",
              "dagster/concurrency_key": "download",
          },
          partitions_def=partitions_def_v3,
automation_condition=dg.AutomationCondition.eager(),
          )
def gmgsi_v3_download_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    it: dt.datetime = context.partition_time_window.start

    fs = s3fs.S3FileSystem(anon=True)
    # Check if the local file exists before downloading
    downloaded_files = []
    for channel in ["VIS", "WV"]:
        # Get the S3 URI by glob, as it has extra info in it
        s3_uri = "s3://"+list(fs.glob(f"{BASE_URL}GMGSI_{channel}/{it.year}/{it.month:02}/{it.day:02}/{it.hour:02}/GLOBCOMP{channel}_v3r0_blend_s{it.strftime('%Y%m%d%H')}*"))[0]
        local_uri = s3_uri.replace(BASE_URL, ARCHIVE_FOLDER)
        if not os.path.exists(local_uri):
            fs.get(s3_uri, local_uri)
            context.log.info(msg=f"Downloaded {s3_uri} to {local_uri}")
            downloaded_files.append(local_uri)
        else:
            context.log.info(msg=f"Already downloaded {s3_uri} to {local_uri}")
    for channel, name in [("LW", "LIR"), ("SW", "SIR")]:
        # Get the S3 URI by glob, as it has extra info in it
        s3_uri = "s3://"+list(fs.glob(f"{BASE_URL}GMGSI_{channel}/{it.year}/{it.month:02}/{it.day:02}/{it.hour:02}/GLOBCOMP{name}_v3r0_blend_s{it.strftime('%Y%m%d%H')}*"))[0]
        local_uri = s3_uri.replace(BASE_URL, ARCHIVE_FOLDER)
        if not os.path.exists(local_uri):
            fs.get(s3_uri, local_uri)
            context.log.info(msg=f"Downloaded {s3_uri} to {local_uri}")
            downloaded_files.append(local_uri)
        else:
            context.log.info(msg=f"Already downloaded {s3_uri} to {local_uri}")
    # Return the paths as a materialization
    return dg.MaterializeResult(
        metadata={
            "files": downloaded_files
        },
    )

@dg.asset(name="gmgsi-dummy-zarr",
          deps=[gmgsi_download_asset],
          description="Dummy Zarr archive of satellite image data from GMGSI global mosaic of geostationary satellites from NOAA on AWS",
          partitions_def=partitions_def,
          automation_condition=dg.AutomationCondition.eager(),)
def gmgsi_dummy_zarr_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    if os.path.exists(ZARR_PATH):
        return dg.MaterializeResult(
            metadata={"zarr_path": ZARR_PATH},
        )

    data = get_global_mosaic(context.partition_time_window.start)
    data = data.rename({"lat": "latitude", "lon": "longitude"})
    variables = ["vis", "ssr", "wv", "lwir", "swir"]
    encoding = {
        v: {"compressors": zarr.codecs.BloscCodec(cname='zstd', clevel=9, shuffle=zarr.codecs.BloscShuffle.bitshuffle)}
        for v in variables}
    encoding["time"] = {"units": "nanoseconds since 1970-01-01"}
    # Get the number of partitions to create
    date_range = pd.date_range(start="2021-07-13", end="2026-12-31", freq="H")
    dummies = dask.array.zeros((len(date_range), data.xc.shape[0], data.yc.shape[0]), chunks=(1, -1, -1),
                               dtype=np.uint8)
    default_dataarray = xr.DataArray(dummies,
                                     coords={"time": date_range, "latitude": (["yc", "xc"], data.latitude.values),
                                             "longitude": (["yc", "xc"], data.longitude.values)},
                                     dims=["time", "xc", "yc"])
    dummy_dataset = xr.Dataset({v: default_dataarray for v in variables},
                               coords={"time": date_range, "latitude": (["yc", "xc"], data.latitude.values),
                                       "longitude": (["yc", "xc"], data.longitude.values)})
    dummy_dataset.to_zarr(ZARR_PATH, mode="w", compute=False, zarr_format=3, encoding=encoding)
    return dg.MaterializeResult(
        metadata={"zarr_path": ZARR_PATH},
    )

@dg.asset(name="gmgsi-v3-dummy-zarr",
          deps=[gmgsi_v3_download_asset],
          description="Dummy Zarr archive of satellite image data from GMGSI global mosaic of geostationary satellites from NOAA on AWS",
          partitions_def=partitions_def_v3,
          automation_condition=dg.AutomationCondition.eager(),)
def gmgsi_v3_dummy_zarr_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    if os.path.exists(ZARR_V3_PATH):
        return dg.MaterializeResult(
            metadata={"zarr_path": ZARR_V3_PATH},
        )

    data = get_global_mosaic_v3(context.partition_time_window.start)
    data = data.rename({"lat": "latitude", "lon": "longitude"})
    variables = ["vis", "wv", "lwir", "swir", "vis_dqf", "wv_dqf", "lwir_dqf", "swir_dqf"]
    encoding = {
        v: {"compressors": zarr.codecs.BloscCodec(cname='zstd', clevel=9, shuffle=zarr.codecs.BloscShuffle.bitshuffle)}
        for v in variables}
    encoding["time"] = {"units": "nanoseconds since 1970-01-01"}
    # Get the number of partitions to create
    date_range = pd.date_range(start="2025-03-10-17:00", end="2026-12-31", freq="H")
    dummies = dask.array.zeros((len(date_range), data.xc.shape[0], data.yc.shape[0]), chunks=(1, -1, -1),
                               dtype=np.uint8)
    default_dataarray = xr.DataArray(dummies,
                                     coords={"time": date_range, "latitude": (["yc", "xc"], data.latitude.values),
                                             "longitude": (["yc", "xc"], data.longitude.values)},
                                     dims=["time", "xc", "yc"])
    dummy_dataset = xr.Dataset({v: default_dataarray for v in variables},
                               coords={"time": date_range, "latitude": (["yc", "xc"], data.latitude.values),
                                       "longitude": (["yc", "xc"], data.longitude.values)})
    dummy_dataset.to_zarr(ZARR_V3_PATH, mode="w", compute=False, zarr_format=3, encoding=encoding)
    return dg.MaterializeResult(
        metadata={"zarr_path": ZARR_V3_PATH},
    )

@dg.asset(
        name="gmgsi-zarr",
        description=__doc__,
        metadata={
            "archive_folder": dg.MetadataValue.text(ARCHIVE_FOLDER),
            "area": dg.MetadataValue.text("global"),
            "source": dg.MetadataValue.text("noaa-aws"),
            "expected_runtime": dg.MetadataValue.text("1 hour"),
        },
        deps=[gmgsi_download_asset, gmgsi_dummy_zarr_asset],
        tags={
            "dagster/max_runtime": str(60 * 60 * 10), # Should take 6 ish hours
            "dagster/priority": "1",
            "dagster/concurrency_key": "zarr-creation",
        },
    partitions_def=partitions_def,
automation_condition=dg.AutomationCondition.eager(),
)
def gmgsi_zarr_asset(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    """Dagster asset for NOAA's GMGSI global mosaic of geostationary satellites."""
    it: dt.datetime = context.partition_time_window.start
    dataset: xr.Dataset = get_global_mosaic(it)
    dataset.chunk({"time": 1, "yc": -1, "xc": -1}).rename({"lat": "latitude", "lon": "longitude"}).to_zarr(ZARR_PATH,
                                                                                                                region={
                                                                                                                    "time": "auto",
                                                                                                                    "xc": "auto",
                                                                                                                    "yc": "auto"}, )
    return dg.MaterializeResult(
        metadata={"zarr_path": ZARR_PATH},
    )

@dg.asset(
        name="gmgsi-v3-zarr",
        description=__doc__,
        metadata={
            "archive_folder": dg.MetadataValue.text(ARCHIVE_FOLDER),
            "area": dg.MetadataValue.text("global"),
            "source": dg.MetadataValue.text("noaa-aws"),
            "expected_runtime": dg.MetadataValue.text("1 hour"),
        },
        deps=[gmgsi_v3_download_asset, gmgsi_v3_dummy_zarr_asset],
        tags={
            "dagster/max_runtime": str(60 * 60 * 10), # Should take 6 ish hours
            "dagster/priority": "1",
            "dagster/concurrency_key": "zarr-creation",
        },
    partitions_def=partitions_def_v3,
automation_condition=dg.AutomationCondition.eager(),
)
def gmgsi_v3_zarr_asset(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    """Dagster asset for NOAA's GMGSI global mosaic of geostationary satellites."""
    it: dt.datetime = context.partition_time_window.start
    dataset: xr.Dataset = get_global_mosaic_v3(it)
    dataset.chunk({"time": 1, "yc": -1, "xc": -1}).rename({"lat": "latitude", "lon": "longitude"}).to_zarr(ZARR_V3_PATH,
                                                                                                                region={
                                                                                                                    "time": "auto",
                                                                                                                    "xc": "auto",
                                                                                                                    "yc": "auto"}, )
    return dg.MaterializeResult(
        metadata={"zarr_path": ZARR_PATH},
    )


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
    base_url = ARCHIVE_FOLDER
    datasets_to_merge = []
    for channel in channels:
        if channel == "vis":
            # Get the full file name
            filepath = list(glob.glob(f"{base_url}GMGSI_VIS/{time.year}/{time.month:02}/{time.day:02}/{time.hour:02}/GLOBCOMPVIS_v3r0_blend_s{time.strftime('%Y%m%d%H')}*"))[0]
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
            filepath = list(glob.glob(
                                f"{base_url}GMGSI_WV/{time.year}/{time.month:02}/{time.day:02}/{time.hour:02}/GLOBCOMPWV_v3r0_blend_s{time.strftime('%Y%m%d%H')}*"))[0]
            with fsspec.open(filepath) as f:
                ds_wv = xr.open_dataset(f).load()
                ds_wv = ds_wv["dqf"].fillna(255)
                ds_wv = ds_wv.rename({"data": "wv", "dqf": "wv_dqf"})
                for dv in ds_wv.data_vars:
                    if dv not in ["wv", "wv_dqf"]:
                        ds_wv = ds_wv.drop_vars(dv)
                datasets_to_merge.append(ds_wv.astype(np.uint8))
        elif channel == "lwir":
            filepath = list(glob.glob(
                                f"{base_url}GMGSI_LW/{time.year}/{time.month:02}/{time.day:02}/{time.hour:02}/GLOBCOMPLIR_v3r0_blend_s{time.strftime('%Y%m%d%H')}*"))[0]
            with fsspec.open(filepath) as f:
                ds_lw = xr.open_dataset(f).load()
                ds_lw = ds_lw["dqf"].fillna(255)
                ds_lw = ds_lw.rename({"data": "lwir", "dqf": "lwir_dqf"})
                for dv in ds_lw.data_vars:
                    if dv not in ["lwir", "lwir_dqf"]:
                        ds_lw = ds_lw.drop_vars(dv)
                datasets_to_merge.append(ds_lw.astype(np.uint8))
        elif channel == "swir":
            filepath = list(glob.glob(
                                f"{base_url}GMGSI_SW/{time.year}/{time.month:02}/{time.day:02}/{time.hour:02}/GLOBCOMPSIR_v3r0_blend_s{time.strftime('%Y%m%d%H')}*"))[0]
            with fsspec.open(filepath) as f:
                ds_sw = xr.open_dataset(f).load()
                ds_sw = ds_sw["dqf"].fillna(255)
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
    base_url = ARCHIVE_FOLDER
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

