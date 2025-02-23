"""This gather and uses the global mosaic of geostationary satellites from NOAA on AWS"""
import datetime as dt
import os
from typing import TYPE_CHECKING, Optional

import dagster as dg
import dask.array
import fsspec
import numpy as np
import pandas as pd
import xarray as xr
import zarr

if TYPE_CHECKING:
    import datetime as dt

"""Zarr archive of satellite image data from GMGSI global mosaic of geostationary satellites from NOAA on AWS"""

ARCHIVE_FOLDER = "/ext_data/gmgsi/"
BASE_URL = "s3://noaa-gmgsi-pds/"
ZARR_PATH = "/ext_data/gmgsi.zarr"
if os.getenv("ENVIRONMENT", "local") == "pb":
    ARCHIVE_FOLDER = "/ext_data/gmgsi/"

partitions_def: dg.TimeWindowPartitionsDefinition = dg.HourlyPartitionsDefinition(
    start_date="2021-07-13-00:00",
    end_offset=-1,
)


@dg.asset(name="gmgsi-download", description="Download GMGSI global mosaic of geostationary satellites from NOAA on AWS",
          tags={
              "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
              "dagster/priority": "1",
              "dagster/concurrency_key": "aws",
          },
          partitions_def=partitions_def,
          )
def gmgsi_download_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    it: dt.datetime = context.partition_time_window.start

    fs = fsspec.filesystem("s3")
    fs.get(f"{BASE_URL}GLOBCOMP_VIS/{it.year}/{it.month:02}/{it.day:02}/{it.hour:02}/GLOBCOMPVIS_nc.{it.strftime('%Y%m%d%H')}", f"{ARCHIVE_FOLDER}GMGSI_VIS/{it.year}/{it.month:02}/{it.day:02}/{it.hour:02}/GLOBCOMPVIS_nc.{it.strftime('%Y%m%d%H')}")
    fs.get(f"{BASE_URL}GMGSI_SSR/{it.year}/{it.month:02}/{it.day:02}/{it.hour:02}/GLOBCOMPSSR_nc.{it.strftime('%Y%m%d%H')}", f"{ARCHIVE_FOLDER}GMGSI_SSR/{it.year}/{it.month:02}/{it.day:02}/{it.hour:02}/GLOBCOMPSSR_nc.{it.strftime('%Y%m%d%H')}")
    fs.get(f"{BASE_URL}GMGSI_WV/{it.year}/{it.month:02}/{it.day:02}/{it.hour:02}/GLOBCOMPWV_nc.{it.strftime('%Y%m%d%H')}", f"{ARCHIVE_FOLDER}GMGSI_WV/{it.year}/{it.month:02}/{it.day:02}/{it.hour:02}/GLOBCOMPWV_nc.{it.strftime('%Y%m%d%H')}")
    fs.get(f"{BASE_URL}GMGSI_LW/{it.year}/{it.month:02}/{it.day:02}/{it.hour:02}/GLOBCOMPLIR_nc.{it.strftime('%Y%m%d%H')}", f"{ARCHIVE_FOLDER}GMGSI_LW/{it.year}/{it.month:02}/{it.day:02}/{it.hour:02}/GLOBCOMPLIR_nc.{it.strftime('%Y%m%d%H')}")
    fs.get(f"{BASE_URL}GMGSI_SW/{it.year}/{it.month:02}/{it.day:02}/{it.hour:02}/GLOBCOMPSIR_nc.{it.strftime('%Y%m%d%H')}", f"{ARCHIVE_FOLDER}GMGSI_SW/{it.year}/{it.month:02}/{it.day:02}/{it.hour:02}/GLOBCOMPSIR_nc.{it.strftime('%Y%m%d%H')}")

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


@dg.asset(name="gmgsi-dummy-zarr",
          deps=[gmgsi_download_asset],
          description="Dummy Zarr archive of satellite image data from GMGSI global mosaic of geostationary satellites from NOAA on AWS", )
def gmgsi_dummy_zarr_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    if os.path.exists(ZARR_PATH):
        return dg.MaterializeResult(
            metadata={"zarr_path": ZARR_PATH},
        )

    data = get_global_mosaic(context.partition_time_window.start)
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
            "dagster/concurrency_key": "aws",
        },
    partitions_def=partitions_def,
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

