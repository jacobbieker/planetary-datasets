"""This gather and uses the global mosaic of geostationary satellites from NOAA on AWS"""
import datetime as dt
import os
from typing import TYPE_CHECKING

import dagster as dg
import dask.array
import s3fs
import numpy as np
import pandas as pd
import xarray as xr
import zarr

import datetime as dt

"""Zarr archive of satellite image data from GMGSI global mosaic of geostationary satellites from NOAA on AWS"""

ARCHIVE_FOLDER = "/run/media/jacob/Tester/GHE/"
BASE_URL = "s3://noaa-ghe-pds/"
ZARR_PATH = "/run/media/jacob/Tester/ghe.zarr"

partitions_def: dg.TimeWindowPartitionsDefinition = dg.HourlyPartitionsDefinition(
    start_date="2019-06-24-19:00",
    end_offset=-2,
)


@dg.asset(name="ghe-download", description="Download GHE global mosaic of geostationary satellites from NOAA on AWS",
          tags={
              "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
              "dagster/priority": "1",
              "dagster/concurrency_key": "download",
          },
          partitions_def=partitions_def,
automation_condition=dg.AutomationCondition.eager(),
          )
def ghe_download_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    it: dt.datetime = context.partition_time_window.start

    fs = s3fs.S3FileSystem(anon=True)
    # Get it for the full hour for each one of the 15 minute intervals
    files_downloaded = []
    for minute_offset in range(0, 60, 15):
        it = it.replace(minute=minute_offset)
        local_uri = f"{ARCHIVE_FOLDER}rain_rate/{it.year}/{it.month:02}/{it.day:02}/NPR.GEO.GHE.v1.S{it.strftime('%Y%m%d%H%M')}.nc.gz"
        s3_uri = f"{BASE_URL}rain_rate/{it.year}/{it.month:02}/{it.day:02}/NPR.GEO.GHE.v1.S{it.strftime('%Y%m%d%H%M')}.nc.gz"
        # Check if the local file exists before downloading
        if not os.path.exists(local_uri):
            try:
                fs.get(s3_uri, local_uri)
                files_downloaded.append(local_uri)
                context.log.info(msg=f"Downloaded {s3_uri} to {local_uri}")
            except FileNotFoundError:
                context.log.error(f"File not found {s3_uri}")
                continue
        else:
            files_downloaded.append(local_uri)
            context.log.info(msg=f"Already downloaded {s3_uri} to {local_uri}")
    if len(files_downloaded) == 0:
        raise FileNotFoundError("No files downloaded")
    # Return the paths as a materialization
    return dg.MaterializeResult(
        metadata={
            "files": files_downloaded,
        },
    )

@dg.asset(name="ghe-download-navigation", description="Download GHE global mosaic of geostationary satellites navigation file from NOAA on AWS",
          tags={
              "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
              "dagster/priority": "1",
              "dagster/concurrency_key": "download",
          },
automation_condition=dg.AutomationCondition.eager(),
          )
def ghe_download_navigation(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    fs = s3fs.S3FileSystem(anon=True)
    # Check if the local file exists before downloading
    if not os.path.exists(f"{ARCHIVE_FOLDER}NPR.GEO.GHE.v1.Navigation.netcdf"):
        fs.get(f"{BASE_URL}NPR.GEO.GHE.v1.Navigation.netcdf", f"{ARCHIVE_FOLDER}NPR.GEO.GHE.v1.Navigation.netcdf")
    # Return the paths as a materialization
    return dg.MaterializeResult(
        metadata={
            "file": f"{ARCHIVE_FOLDER}NPR.GEO.GHE.v1.Navigation.netcdf",
        },
    )

@dg.asset(name="ghe-dummy-zarr",
          deps=[ghe_download_asset, ghe_download_navigation],
          partitions_def=partitions_def,
          description="Dummy Zarr archive of satellite image data from GMGSI global mosaic of geostationary satellites from NOAA on AWS",
          automation_condition=dg.AutomationCondition.eager(),)
def ghe_dummy_zarr_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    if os.path.exists(ZARR_PATH):
        return dg.MaterializeResult(
            metadata={"zarr_path": ZARR_PATH},
        )

    data = get_ghe_mosaic(context.partition_time_window.start)
    encoding = {"rainfall_estimate": {"compressors": zarr.codecs.BloscCodec(cname='zstd', clevel=9, shuffle=zarr.codecs.BloscShuffle.bitshuffle)}}
    encoding["time"] = {"units": "nanoseconds since 1970-01-01"}
    # Get the number of partitions to create
    date_range = pd.date_range(start="2019-06-24", end="2026-12-31", freq="15min")
    dummies = dask.array.zeros((len(date_range), data.lines.shape[0], data.elems.shape[0]), chunks=(1, -1, -1),
                               dtype=np.float16)
    default_dataarray = xr.DataArray(dummies,
                                     coords={"time": date_range, "latitude": (["lines", "elems"], data.latitude.values),
                                             "longitude": (["lines", "elems"], data.longitude.values)},
                                     dims=["time", "lines", "elems"])
    dummy_dataset = xr.Dataset({v: default_dataarray for v in ["rainfall_estimate"]},
                               coords={"time": date_range, "latitude": (["lines", "elems"], data.latitude.values),
                                       "longitude": (["lines", "elems"], data.longitude.values)})
    dummy_dataset.to_zarr(ZARR_PATH, mode="w", compute=False, zarr_format=3, encoding=encoding)
    return dg.MaterializeResult(
        metadata={"zarr_path": ZARR_PATH},
    )

@dg.asset(
        name="ghe-zarr",
        description=__doc__,
        metadata={
            "archive_folder": dg.MetadataValue.text(ARCHIVE_FOLDER),
            "area": dg.MetadataValue.text("global"),
            "source": dg.MetadataValue.text("noaa-aws"),
            "expected_runtime": dg.MetadataValue.text("1 hour"),
        },
        deps=[ghe_download_asset, ghe_dummy_zarr_asset],
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
    dataset: xr.Dataset = get_ghe_mosaic(it)
    dataset.chunk({"time": 1, "lines": -1, "elems": -1}).to_zarr(ZARR_PATH,
                                                                    region={
                                                                    "time": "auto",
                                                                    "xc": "auto",
                                                                    "yc": "auto"}, )
    return dg.MaterializeResult(
        metadata={"zarr_path": ZARR_PATH},
    )



def get_ghe_mosaic(time: dt.datetime) -> xr.Dataset:
    """Gather the global mosaic of geostationary satellites hydrology estimation from NOAA on AWS

    Args:
        time: datetime object

    Returns:
        xarray object with global mosaic
    """
    # Do it for each of the 4 files for the hour interval
    dses = []
    for minute_offset in range(0, 60, 15):
        time = time.replace(minute=minute_offset)
        ds = xr.open_dataset(f"{ARCHIVE_FOLDER}rain_rate/{time.year}/{time.month:02}/{time.day:02}/NPR.GEO.GHE.v1.S{time.strftime('%Y%m%d%H%M')}.nc.gz").load()
        # rename data to vis
        ds = ds.rename({"data": "vis"})
        # Convert fill value to NaN
        ds = ds.where(ds.vis != ds.vis.attrs["_FillValue"])
        # Add in coordinates from navigation one
        nav = xr.open_dataset(f"{ARCHIVE_FOLDER}NPR.GEO.GHE.v1.Navigation.netcdf").load()
        ds = ds.assign_coords({"latitude": nav.latitude, "longitude": nav.longitude})
        # Add in timestamp
        ds = ds.assign_coords({"time": pd.Timestamp(time)})
        ds = ds.rename({"rain": "rainfall_estimate"})
        # Convert to float16 to save some space
        ds = ds.astype(np.float16)
        dses.append(ds)
    return xr.concat(dses, dim="time").sortby("time")
