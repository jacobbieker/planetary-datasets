import glob
from numcodecs.zarr3 import BitRound
import zarr.codecs

"""This gather and uses the global mosaic of geostationary satellites from NOAA on AWS"""
import datetime as dt
import os
from typing import TYPE_CHECKING, Optional

import dagster as dg
import dask.array
import s3fs
import numpy as np
import pandas as pd
import xarray as xr
import zarr

import datetime as dt

ARCHIVE_FOLDER = "/ext_data/metoffice/"
BASE_URL = "s3://met-office-atmospheric-model-data/global-deterministic-10km"
ZARR_PATH = "/ext_data/metoffice.zarr"
if os.getenv("ENVIRONMENT", "local") == "pb":
    ARCHIVE_FOLDER = "/ext_data/metoffice/"

partitions_def: dg.TimeWindowPartitionsDefinition = dg.DailyPartitionsDefinition(
    start_date="2023-03-01",
    end_offset=-1,
)

def preprocess(ds: xr.Dataset) -> xr.Dataset:
    # Promote the latitude_longitude, latitude_bnds, and longitude_bnds to coordinates
    ds = ds.set_coords(["latitude_longitude", "latitude_bnds", "longitude_bnds"])
    # Drop flag
    if "flag" in ds:
        ds = ds.drop_vars("flag")
    if "forecast_period_bnds" in ds:
        ds = ds.drop_vars("forecast_period_bnds")
    if "time_bnds" in ds:
        ds = ds.drop_vars("time_bnds")
    # If flag in it, then add "_{height}" to the variable name, and remove it from the coordinate
    for var in ds.data_vars:
        if "height" in ds:
            ds = ds.rename({var: f"{var}_{ds['height'].values}"})
    # Drop height
    if "height" in ds:
        ds = ds.drop_vars("height")
    # None are higher precision than float16, so set that to float16
    ds = ds.astype("float16")
    return ds

VARIABLES = { # Value is the number of bits to round in float32, based off https://www.metoffice.gov.uk/binaries/content/assets/metofficegovuk/pdf/data/global-nwp-asdi-datasheet.pdf
    "precipitation_rate": 9,
    "pressure_at_mean_sea_level": 0,
    "cloud_amount_of_total_cloud": 2,
    "cloud_amount_of_low_cloud": 2,
    "cloud_amount_of_medium_cloud": 2,
    "cloud_amount_of_high_cloud": 2,
    "fog_fraction_at_screen_level": 1,
    "snowfall_rate_from_convection": 9,
    "radiation_flux_in_uv_downward_at_surface": 2,
    "temperature_at_screen_level": 2,
    "temperature_at_surface": 2,
    "temperature_of_dew_point_at_screen_level": 2,
    "wind_direction_at_10m": 2,
    "wind_gust_at_10m": 2,
    "wind_gust_at_10m_max-PT01H": 2,
    "relative_humidity_at_screen_level": 4,
    "radiation_flux_in_shortwave_direct_downward_at_surface": 3,
    "radiation_flux_in_longwave_downward_at_surface": 3,
    "snow_depth_water_equivalent": 5,
    "rainfall_rate": 9,
    "rainfall_rate_from_convection": 9,
    "precipitation_accumulation-PT01H": 6,
    "precipitation_accumulation-PT06H": 6,
    "snowfall_rate": 9,
    "cloud_amount_of_total_convective_cloud": 3,
    "CAPE_surface": 2,
    "CAPE_most_unstable_below_500hPa": 2,
    "CIN_surface": 2,
    "CIN_most_unstable_below_500hPa": 2,
    "wind_speed_at_10m": 2,
}

PRESSURE_VARIABLES = {
    "wind_vertical_velocity_on_pressure_levels": 4,
    "wind_speed_on_pressure_levels": 2,
    "wind_direction_on_pressure_levels": 2,
    "relative_humidity_on_pressure_levels": 4,
    "height_ASL_on_pressure_levels": 0,
    "temperature_on_pressure_levels": 2,
}


def download_metoffice_global_init_time(it: dt.datetime) -> list[str]:
    # Get the year, month, day, and hour
    year, month, day, hour = it.year, it.month, it.day, it.hour
    # Get the path
    path = f"{BASE_URL}/{year}{month:02}{day:02}T{hour:02}00Z/"
    # Get the files
    fs = s3fs.S3FileSystem(anon=True)
    available_files = fs.ls(path)
    available_files = ["s3://"+f for f in available_files]
    downloaded_files = []
    if not os.path.exists(f"{ARCHIVE_FOLDER}{it.strftime("%Y%m%dT%H%MZ")}"):
        os.makedirs(f"{ARCHIVE_FOLDER}{it.strftime("%Y%m%dT%H%MZ")}")
    # Download each of the pressure level ones
    for forecast_time in range(0, 55):
        for k, v in PRESSURE_VARIABLES.items():
            valid_time = it + dt.timedelta(hours=forecast_time)
            valid = valid_time.strftime("%Y%m%dT%H%MZ")
            file = f"{path}{valid}-PT{str(forecast_time).zfill(4)}H00M-{k}.nc"
            if file in available_files:
                fs.get(file, f"{ARCHIVE_FOLDER}{it.strftime("%Y%m%dT%H%MZ")}/{valid}-PT{str(forecast_time).zfill(4)}H00M-{k}.nc")
                downloaded_files.append(f"{ARCHIVE_FOLDER}{it.strftime("%Y%m%dT%H%MZ")}/{valid}-PT{str(forecast_time).zfill(4)}H00M-{k}.nc")
        # Download each of the surface ones
        for k, v in VARIABLES.items():
            valid_time = it + dt.timedelta(hours=forecast_time)
            valid = valid_time.strftime("%Y%m%dT%H%MZ")
            file = f"{path}{valid}-PT{str(forecast_time).zfill(4)}H00M-{k}.nc"
            if file in available_files:
                fs.get(file, f"{ARCHIVE_FOLDER}{it.strftime("%Y%m%dT%H%MZ")}/{valid}-PT{str(forecast_time).zfill(4)}H00M-{k}.nc")
                downloaded_files.append(f"{ARCHIVE_FOLDER}{it.strftime("%Y%m%dT%H%MZ")}/{valid}-PT{str(forecast_time).zfill(4)}H00M-{k}.nc")
    return downloaded_files


def list_metoffice_downloaded_files(it: dt.datetime) -> list[str]:
    # Get the path
    path = f"{ARCHIVE_FOLDER}{it.strftime("%Y%m%dT%H%MZ")}"
    return sorted(list(glob.glob(f"{path}/*.nc")))


@dg.asset(name="metoffice-global-download", description="Download MetOffice 10km Deterministic global model on AWS",
          tags={
              "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
              "dagster/priority": "1",
              "dagster/concurrency_key": "download",
          },
          partitions_def=partitions_def,
automation_condition=dg.AutomationCondition.eager(),
          )
def metoffice_global_download_asset(context: dg.AssetExecutionContext) -> list[str]:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    it: dt.datetime = context.partition_time_window.start
    downloaded_files = download_metoffice_global_init_time(it)

    return downloaded_files


@dg.asset(name="metoffice-dummy-zarr",
          deps=[metoffice_global_download_asset],
          description="Dummy Zarr archive of NWPs from MetOffice 10km Deterministic global model on AWS",
          automation_condition=dg.AutomationCondition.eager(),)
def metoffice_dummy_zarr_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    if os.path.exists(ZARR_PATH):
        return dg.MaterializeResult(
            metadata={"zarr_path": ZARR_PATH},
        )
    files = list_metoffice_downloaded_files(it=context.partition_time_window.start)
    data, encodings = combine_metoffice_to_dataset(files)
    encodings["init_time"] = {"units": "nanoseconds since 1970-01-01"}
    # Get the number of partitions to create
    date_range = pd.date_range(start="2023-03-01", end="2026-12-31", freq="6h")
    # Create empty dask arrays of the same size as the data
    dummies_4d_var = dask.array.zeros(
        (len(date_range), len(data.step), data.latitude.shape[0], data.longitude.shape[0]), chunks=(1, 1, -1, -1),
        dtype=np.float16)
    dummies_5d_var = dask.array.zeros(
        (len(date_range), len(data.step), len(data.pressure), data.latitude.shape[0], data.longitude.shape[0]), chunks=(1, 1, -1, -1, -1),
        dtype=np.float16)
    default_dataarray = xr.DataArray(dummies_4d_var, coords={"init_time": date_range, "step": data.step.values,
                                                             "latitude": data.latitude.values,
                                                             "longitude": data.longitude.values},
                                     dims=["init_time", "step", "latitude", "longitude"])
    default_pressure_dataarray = xr.DataArray(dummies_5d_var, coords={"init_time": date_range, "step": data.step.values, "pressure": data.pressure.values,
                                                                        "latitude": data.latitude.values,
                                                                        "longitude": data.longitude.values},
                                                dims=["init_time", "step", "pressure", "latitude", "longitude"])
    dummy_dataset = xr.Dataset({v: default_pressure_dataarray for v in data.data_vars if "pressure" in data[v].coords},
                               coords={"init_time": date_range, "step": data.step.values, "pressure": data.pressure.values,
                                       "latitude": data.latitude.values,
                                       "longitude": data.longitude.values})
    dummy_dataset.update({v: default_dataarray for v in data.data_vars if "pressure" not in data[v].coords})
    dummy_dataset.chunk({"init_time": 1, "step": 1, "pressure": -1, "latitude": -1, "longitude": -1}).to_zarr(ZARR_PATH, mode="w",
                                                                                              compute=False,
                                                                                              zarr_format=3,
                                                                                              encoding=encodings, )
    return dg.MaterializeResult(
        metadata={"zarr_path": ZARR_PATH},
    )

@dg.asset(
        name="metoffice-zarr",
        description=__doc__,
        metadata={
            "archive_folder": dg.MetadataValue.text(ARCHIVE_FOLDER),
            "area": dg.MetadataValue.text("global"),
            "source": dg.MetadataValue.text("noaa-aws"),
            "expected_runtime": dg.MetadataValue.text("1 hour"),
        },
        deps=[metoffice_global_download_asset, metoffice_dummy_zarr_asset],
        tags={
            "dagster/max_runtime": str(60 * 60 * 10), # Should take 6 ish hours
            "dagster/priority": "1",
            "dagster/concurrency_key": "heavy-zarr-creation",
        },
    partitions_def=partitions_def,
automation_condition=dg.AutomationCondition.eager(),
)
def metoffice_zarr_asset(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    """Dagster asset for NOAA's GMGSI global mosaic of geostationary satellites."""
    it: dt.datetime = context.partition_time_window.start
    downloaded_files = list_metoffice_downloaded_files(it)
    dataset: xr.Dataset = combine_metoffice_to_dataset(downloaded_files)
    dataset.chunk({"init_time": 1, "step": 1, "pressure": -1, "latitude": -1, "longitude": -1}).to_zarr(ZARR_PATH,
                                                                                                                region={
                                                                                                                    "init_time": "auto",
                                                                                                                    "step": "auto",
                                                                                                                    "latitude": "auto",
                                                                                                                    "longitude": "auto",
                                                                                                                    "pressure": "auto"}, )
    # Remove downloaded files after zarr creation
    for f in downloaded_files:
        os.remove(f)
    return dg.MaterializeResult(
        metadata={"zarr_path": ZARR_PATH},
    )


def combine_metoffice_to_dataset(downloaded_files: str) -> tuple[xr.Dataset, dict]:
    encoding = {}
    surface_dses = []
    for k, v in VARIABLES.items():
        variable_files = [f for f in downloaded_files if "-"+k+".nc" in f]
        files = sorted(variable_files)
        ds = xr.open_mfdataset(files, combine="nested", concat_dim="time", preprocess=preprocess, decode_timedelta=False).sortby("time")
        if "max-PT01H" in k:
            ds = ds.rename({"wind_speed_of_gust_10.0": "wind_gust_at_10.0_max_1h"})
        for dv in ds.data_vars:
            encoding[dv] = {"compressors": zarr.codecs.BloscCodec(cname='zstd', clevel=9, shuffle=zarr.codecs.BloscShuffle.bitshuffle)}
            encoding[dv]["filters"] = [BitRound(keepbits=v+1)]
        surface_dses.append(ds)
    # Merge them
    surface_ds = xr.merge(surface_dses)

    pressure_dses = []
    for k, v in PRESSURE_VARIABLES.items():
        variable_files = [f for f in downloaded_files if "-"+k+".nc" in f]
        files = sorted(variable_files)
        ds = xr.open_mfdataset(files, combine="nested", concat_dim="time", preprocess=preprocess, decode_timedelta=False).sortby("time")
        for dv in ds.data_vars:
            encoding[dv] = {
                "compressors": zarr.codecs.BloscCodec(cname='zstd', clevel=9, shuffle=zarr.codecs.BloscShuffle.bitshuffle)}
            encoding[dv]["filters"] = [BitRound(keepbits=v+1)]
        pressure_dses.append(ds)
    # Merge them
    pressure_ds = xr.merge(pressure_dses)
    # Merge the two
    ds = xr.merge([surface_ds, pressure_ds])
    # Forecast period is the step
    # And should be a coordinate instead of time, which should just be the init time
    ds["init_time"] = ds["time"].values[0]
    ds["time"] = ds["time"] - ds["init_time"]
    ds = ds.rename({"time": "step"})
    ds = ds.drop_vars(["forecast_period", "forecast_reference_time", "latitude_bnds", "longitude_bnds", "latitude_longitude"])
    # Set init_time as a coordinate
    ds = ds.set_coords("init_time").expand_dims("init_time")
    # Chunk it
    ds = ds.chunk({"init_time": 1, "step": 1, "latitude": -1, "longitude": -1, "pressure": -1}).sortby("step")
    return ds, encoding
