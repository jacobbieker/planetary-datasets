from subprocess import Popen
import glob
from pathlib import Path
import tempfile
import gzip
import shutil
import datetime as dt
import os
from typing import TYPE_CHECKING

import dagster as dg
import dask.array
import numpy as np
import xarray as xr
import pandas as pd
import zarr

if TYPE_CHECKING:
    import datetime as dt

"""Zarr archive of satellite image data from GMGSI global mosaic of geostationary satellites from NOAA on AWS"""

ARCHIVE_FOLDER = "MRMS/"
ZARR_PATH = "mrms.zarr"
if os.getenv("ENVIRONMENT", "local") == "pb":
    ARCHIVE_FOLDER = "MRMS/"

partitions_def: dg.TimeWindowPartitionsDefinition = dg.DailyPartitionsDefinition(
    start_date="2000-01-01",
    end_offset=-1,
)
zarr_date_range = pd.date_range("2000-01-01", "2026-12-31", freq="2min")


def download_mrms(day: dt.datetime, measurement_type: str) -> list[str]:
    assert measurement_type in ["GaugeCorr_QPE_01H", "PrecipFlag", "PrecipRate", "RadarOnly_QPE_01H", "MultiSensor_QPE_01H_Pass2"]
    path = f"https://mtarchive.geol.iastate.edu/{day.strftime('%Y')}/{day.strftime('%m')}/{day.strftime('%d')}/mrms/ncep/{measurement_type}/"
    args = ["wget", "-r", "-nc", "--no-parent", path, "-P", ARCHIVE_FOLDER]
    process = Popen(args)
    process.wait()
    # Get the paths that have been downloaded
    return sorted(list(glob.glob(f"{ARCHIVE_FOLDER}/mtarchive.geol.iastate.edu/{day.strftime('%Y')}/{day.strftime('%m')}/{day.strftime('%d')}/mrms/ncep/{measurement_type}/*.grib2.gz")))

def get_mrms(day: dt.datetime, measurement_type: str) -> list[str]:
    return sorted(list(glob.glob(f"{ARCHIVE_FOLDER}/mtarchive.geol.iastate.edu/{day.strftime('%Y')}/{day.strftime('%m')}/{day.strftime('%d')}/mrms/ncep/{measurement_type}/*.grib2.gz")))


@dg.asset(name="mrms-precip-download", description="Download MRMS radar precipitation from Iowa State University",
          tags={
              "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
              "dagster/priority": "1",
              "dagster/concurrency_key": "download",
          },
          partitions_def=partitions_def,
          )
def mrms_preciprate_download_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    it: dt.datetime = context.partition_time_window.start
    downloaded_files = download_mrms(it, "PrecipRate")
    # Return the paths as a materialization
    return dg.MaterializeResult(
        metadata={
            "files": downloaded_files
        },
    )

@dg.asset(name="mrms-flag-download", description="Download MRMS radar precipitation from Iowa State University",
          tags={
              "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
              "dagster/priority": "1",
              "dagster/concurrency_key": "download",
          },
          partitions_def=partitions_def,
          )
def mrms_precipflag_download_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    it: dt.datetime = context.partition_time_window.start
    downloaded_files = download_mrms(it, "PrecipFlag")
    # Return the paths as a materialization
    return dg.MaterializeResult(
        metadata={
            "files": downloaded_files
        },
    )


@dg.asset(name="mrms-dummy-zarr",
          deps=[mrms_precipflag_download_asset, mrms_preciprate_download_asset],
        partitions_def=partitions_def,
          description="Dummy Zarr archive of MRMS PrecipFlag", )
def mrms_precipflag_dummy_zarr_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    if os.path.exists(ZARR_PATH):
        return dg.MaterializeResult(
            metadata={"zarr_path": ZARR_PATH},
        )

    files = get_mrms(context.partition_time_window.start, "PrecipFlag")

    timestamps = zarr_date_range
    # open a single file and get the coordinate metadata
    data = load_mrms_flag(files[0])
    latitudes = data.latitude.values
    longitudes = data.longitude.values
    dummies = -99 * dask.array.ones((len(timestamps), len(latitudes), len(longitudes)),
                                    chunks=(1, len(latitudes), len(longitudes)), dtype="int8")
    rate_dummies = dask.array.zeros((len(timestamps), len(latitudes), len(longitudes)),
                               chunks=(1, len(latitudes), len(longitudes)), dtype="float16")
    ds = xr.Dataset({"precipitation_flag": (("time", "latitude", "longitude"), dummies),
                     "precipitation_rate": (("time", "latitude", "longitude"), rate_dummies)},
                    coords={"time": timestamps, "latitude": latitudes, "longitude": longitudes})
    encoding = {v: {"compressors": zarr.codecs.BloscCodec(cname='zstd', clevel=9, shuffle=zarr.codecs.BloscShuffle.bitshuffle)} for v in ds.data_vars}
    encoding["time"] = {"units": "nanoseconds since 1970-01-01"}
    ds.to_zarr(ZARR_PATH, compute=False, zarr_format=3, encoding=encoding)

    return dg.MaterializeResult(
        metadata={"zarr_path": ZARR_PATH},
    )

@dg.asset(
        name="mrms-zarr",
        description=__doc__,
        metadata={
            "archive_folder": dg.MetadataValue.text(ARCHIVE_FOLDER),
            "area": dg.MetadataValue.text("global"),
            "source": dg.MetadataValue.text("noaa-aws"),
            "expected_runtime": dg.MetadataValue.text("1 hour"),
        },
        deps=[mrms_precipflag_dummy_zarr_asset, mrms_precipflag_download_asset, mrms_preciprate_download_asset],
        tags={
            "dagster/max_runtime": str(60 * 60 * 10), # Should take 6 ish hours
            "dagster/priority": "1",
            "dagster/concurrency_key": "zarr-creation",
        },
    partitions_def=partitions_def,
)
def mrms_zarr_asset(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    """Dagster asset for NOAA's GMGSI global mosaic of geostationary satellites."""
    it: dt.datetime = context.partition_time_window.start
    files = get_mrms(it, "PrecipFlag")
    rate_files = get_mrms(it, "PrecipRate")
    zarr_dates = xr.open_zarr(ZARR_PATH).time.values
    for f in files:
        data = load_mrms_flag(f)
        time_idx = np.where(zarr_dates == data.time.values)[0][0]
        data.transpose("time", "latitude", "longitude").to_zarr(ZARR_PATH,
                                                        region={
                                                            "time": "auto",
                                                            "latitude": "auto", "longitude": "auto"}, )
    for f in rate_files:
        data = load_mrms_rate(f)
        time_idx = np.where(zarr_dates == data.time.values)[0][0]
        data.transpose("time", "latitude", "longitude").to_zarr(ZARR_PATH,
                                                        region={
                                                            "time": "auto",
                                                            "latitude": "auto", "longitude": "auto"}, )

    return dg.MaterializeResult(
        metadata={"zarr_path": ZARR_PATH},
    )

def construct_timestamps_from_filename(filename):
    # 20160122-194600
    timestamp = filename.split("_")[-1].split(".")[0]
    return pd.to_datetime(timestamp, format="%Y%m%d-%H%M%S")

def load_mrms_flag(filepath) -> xr.Dataset:
    rand_num = np.random.randint(0, 10000)
    timestamp = construct_timestamps_from_filename(filepath)
    with tempfile.TemporaryDirectory() as tmp:
        with gzip.open(filepath, 'rb') as f:
            with open(os.path.join(tmp, f'{rand_num}.grib2'), 'wb') as f_out:
                shutil.copyfileobj(f, f_out)
        data = xr.load_dataset(os.path.join(tmp, f'{rand_num}.grib2'), engine="cfgrib", decode_timedelta=False)
        data = data.rename({"unknown": "precipitation_flag"})
        data = data.expand_dims(dim="time")
        data["time"] = [timestamp]
        data = data.chunk({"latitude": -1, "longitude": -1})
        data = data.astype("int8")
        # Drop 'step' 'heightAboveSea' and 'valid_time'
        data = data.drop_vars(["step", "heightAboveSea", "valid_time"])
        data.load()
        if os.path.exists(os.path.join(tmp, f'{rand_num}.grib2')):
            os.remove(os.path.join(tmp, f'{rand_num}.grib2'))
        return data

def load_mrms_rate(filepath) -> xr.Dataset:
    rand_num = np.random.randint(0, 10000)
    timestamp = construct_timestamps_from_filename(filepath)
    with tempfile.TemporaryDirectory() as tmp:
        with gzip.open(filepath, 'rb') as f:
            with open(os.path.join(tmp, f'{rand_num}.grib2'), 'wb') as f_out:
                shutil.copyfileobj(f, f_out)
        data = xr.load_dataset(os.path.join(tmp, f'{rand_num}.grib2'), engine="cfgrib", decode_timedelta=False)
        data = data.rename({"unknown": "precipitation_rate"})
        data = data.expand_dims(dim="time")
        data["time"] = [timestamp]
        data = data.chunk({"latitude": -1, "longitude": -1})
        data = data.astype("float16")
        # Drop 'step' 'heightAboveSea' and 'valid_time'
        data = data.drop_vars(["step", "heightAboveSea", "valid_time"])
        data.load()
        if os.path.exists(os.path.join(tmp, f'{rand_num}.grib2')):
            os.remove(os.path.join(tmp, f'{rand_num}.grib2'))
        return data
