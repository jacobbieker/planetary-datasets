import datetime as dt
import os
from subprocess import Popen
from typing import TYPE_CHECKING

import aiohttp.client_exceptions
import dagster as dg
import dask.array
import fsspec
import numpy as np
import pandas as pd
import xarray as xr
import zarr

if TYPE_CHECKING:
    import datetime as dt

def construct_url_from_datetime(date: dt.datetime, step: int) -> list[str]:
    return f"https://thredds.silam.fmi.fi/thredds/fileServer/dust_glob01_v5_7_2/files/SILAM-dust-glob01_v5_7_2_{date.strftime('%Y%m%d00')}_{str(step).zfill(3)}.nc4"

def download_forecast_step(date: dt.datetime, step: int) -> list[str]:
    url = construct_url_from_datetime(date, step)
    url_dir = url.split("/")[-1].split("_")[-2]
    url_dir = os.path.join(ARCHIVE_FOLDER, url_dir)
    if not os.path.exists(url_dir):
        os.makedirs(url_dir)
    downloaded_url_path = os.path.join(url_dir, url.split("/")[-1])
    if not os.path.exists(downloaded_url_path):
        try:
            with fsspec.open(url, "rb") as f:
                with open(downloaded_url_path, "wb") as f2:
                    f2.write(f.read())
        except aiohttp.client_exceptions.ClientPayloadError:
            os.remove(downloaded_url_path)
            raise Exception(f"Failed to download {url}")
    else:
        print(f"Already Downloaded {url}")
    return downloaded_url_path


def open_netcdf(filename: str) -> xr.Dataset:
    data = xr.open_dataset(filename)
    data = data.rename({"lat": "latitude", "lon": "longitude"})
    # Add init time as a dimension
    timestamp = filename.split("/")[-1].split("_")[4]
    data.coords["init_time"] = pd.Timestamp(f"{timestamp[:4]}-{timestamp[4:6]}-{timestamp[6:8]}T{timestamp[8:10]}:00:00")
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


def get_silam_dust_forecast_xr(downloaded_paths) -> xr.Dataset | None:
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

"""This gather and uses the global mosaic of geostationary satellites from NOAA on AWS"""


"""Zarr archive of satellite image data from GMGSI global mosaic of geostationary satellites from NOAA on AWS"""

ARCHIVE_FOLDER = "/ext_data/SILAM_Dust/silam/"
BASE_URL = "s3://noaa-gmgsi-pds/"
SOURCE_COOP_PATH = "s3://bkr/silam-dust/silam_global.zarr"
ZARR_PATH = "/ext_data/SILAM_Dust/silam/silam_dust.zarr"
if os.getenv("ENVIRONMENT", "local") == "pb":
    ARCHIVE_FOLDER = "/ext_data/SILAM_Dust/silam/"

partitions_def: dg.TimeWindowPartitionsDefinition = dg.DailyPartitionsDefinition(
    start_date="2021-07-13",
    end_offset=-1,
)


@dg.asset(name="silam-dust-download", description="Download SILAM 10km dust forecast from FMI Thredds server",
          tags={
              "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
              "dagster/priority": "1",
              "dagster/concurrency_key": "download",
          },
          partitions_def=partitions_def,
automation_condition=dg.AutomationCondition.eager(),

          )
def silam_dust_download_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    it: dt.datetime = context.partition_time_window.start
    downloaded_paths = []
    for i in range(1, 121):
        downloaded_paths.append(download_forecast_step(it, i))
    return dg.MaterializeResult(
        metadata={"downloaded_paths": downloaded_paths},
    )

@dg.asset(name="silam-dust-dummy-zarr",
          deps=[silam_dust_download_asset],
          description="Dummy Zarr archive of SILAM Dust from FMI",
          automation_condition=dg.AutomationCondition.eager(),)
def silam_dust_dummy_zarr_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    if os.path.exists(ZARR_PATH):
        return dg.MaterializeResult(
            metadata={"zarr_path": ZARR_PATH},
        )

    zarr_date_range = pd.date_range(
        start="2024-11-14", end="2026-12-31", freq="D"
    )

    files = []
    for i in range(1, 121):
        # These should already be downloaded
        files.append(download_forecast_step(context.partition_time_window.start, i))
    assert len(files) == 120
    ds = []
    for f in files:
        data = open_netcdf(f)
        ds.append(data)

    data = xr.concat(ds, dim="step").sortby("step")

    variables = list(data.data_vars)

    encoding = {
        v: {"compressors": zarr.codecs.BloscCodec(cname='zstd', clevel=9, shuffle=zarr.codecs.BloscShuffle.bitshuffle)}
        for v in variables}

    # Create empty dask arrays of the same size as the data
    dummies_4d_var = dask.array.zeros(
        (len(zarr_date_range), len(data.step), data.latitude.shape[0], data.longitude.shape[0]), chunks=(1, 1, -1, -1),
        dtype=np.float32)
    default_dataarray = xr.DataArray(dummies_4d_var, coords={"init_time": zarr_date_range, "step": data.step.values,
                                                             "latitude": data.latitude.values,
                                                             "longitude": data.longitude.values},
                                     dims=["init_time", "step", "latitude", "longitude"])
    dummy_dataset = xr.Dataset({v: default_dataarray for v in variables},
                               coords={"init_time": zarr_date_range, "step": data.step.values,
                                       "latitude": data.latitude.values,
                                       "longitude": data.longitude.values})
    dummy_dataset.chunk({"init_time": 1, "step": 1, "latitude": -1, "longitude": -1}).to_zarr(ZARR_PATH, mode="w",
                                                                                              compute=False,
                                                                                              zarr_format=3,
                                                                                              encoding=encoding, )  # storage_options={"endpoint_url": "https://data.source.coop"})
    return dg.MaterializeResult(
        metadata={"zarr_path": ZARR_PATH},
    )

@dg.asset(
        name="silam-dust-zarr",
        description=__doc__,
        metadata={
            "archive_folder": dg.MetadataValue.text(ARCHIVE_FOLDER),
            "area": dg.MetadataValue.text("global"),
            "source": dg.MetadataValue.text("silam-dust"),
            "expected_runtime": dg.MetadataValue.text("24 hours"),
        },
        deps=[silam_dust_download_asset, silam_dust_dummy_zarr_asset],
        tags={
            "dagster/max_runtime": str(60 * 60 * 10), # Should take 6 ish hours
            "dagster/priority": "1",
            "dagster/concurrency_key": "heavy-zarr-creation",
            "large_zarr_creation": "true",
        },
    partitions_def=partitions_def,
automation_condition=dg.AutomationCondition.eager(),
)
def silam_zarr_asset(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    """Dagster asset for NOAA's GMGSI global mosaic of geostationary satellites."""
    it: dt.datetime = context.partition_time_window.start
    files = []
    for i in range(1, 121):
        # These should already be downloaded
        files.append(download_forecast_step(context.partition_time_window.start, i))
    assert len(files) == 120
    ds = []
    for f in files:
        data = open_netcdf(f)
        ds.append(data)

    zarr_dates = xr.open_zarr(ZARR_PATH).time.values
    time_idx = np.where(zarr_dates == it)[0][0]
    data = xr.concat(ds, dim="step").sortby("step").chunk({"step": 1, "latitude": -1, "longitude": -1, "init_time": 1})
    data.to_zarr(ZARR_PATH, region={"init_time": slice(time_idx, time_idx + 1), "latitude": "auto", "longitude": "auto",
                               "step": 1}, )
    data.close()

    return dg.MaterializeResult(
        metadata={"zarr_path": ZARR_PATH},
    )

@dg.asset(name="silam-dust-upload-source-coop",
          deps=[silam_zarr_asset],
          description="Upload SILAM Dust to Source Coop",
          automation_condition=dg.AutomationCondition.eager(),)
def silam_upload_source_coop(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    # Sync the Zarr to Source Coop
    args = ["aws", "s3", "sync", ZARR_PATH+"/", SOURCE_COOP_PATH+"/", "--profile=sc"]
    process = Popen(args)
    process.wait()
    return dg.MaterializeResult(metadata={"zarr_path": SOURCE_COOP_PATH})