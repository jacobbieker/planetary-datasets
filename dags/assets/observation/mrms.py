from subprocess import Popen
import subprocess
import glob
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
import datetime as dt

"""Zarr archive of satellite image data from GMGSI global mosaic of geostationary satellites from NOAA on AWS"""

ARCHIVE_FOLDER = "/data/MRMS/"
ZARR_PATH = "/data/MRMS/mrms.zarr"
CARIB_ZARR_PATH = "/nvme/mrms_caribbean.zarr"
ALASKA_ZARR_PATH = "/nvme/mrms_alaska.zarr"
HAWAII_ZARR_PATH = "/nvme/mrms_hawaii.zarr"
GUAM_ZARR_PATH = "/nvme/mrms_guam.zarr"
AWS_ARCHIVE_FOLDER = "/data/MRMS/aws/"
SOURCE_COOP_PATH = "s3://bkr/mrms/mrms.zarr"
if os.getenv("ENVIRONMENT", "local") == "pb":
    ARCHIVE_FOLDER = "/data/MRMS/"

partitions_def: dg.TimeWindowPartitionsDefinition = dg.DailyPartitionsDefinition(
    start_date="2000-01-01",
    end_offset=-1,
)
zarr_date_range = pd.date_range("2000-01-01", "2026-12-31", freq="2min")

aws_partitions_def: dg.TimeWindowPartitionsDefinition = dg.DailyPartitionsDefinition(
    start_date="2020-10-14",
    end_offset=-1,
)
region_partitions = dg.StaticPartitionsDefinition(["HAWAII", "CARIB", "GUAM", "ALASKA"])
two_dimensional_partitions = dg.MultiPartitionsDefinition(
    {"date": aws_partitions_def, "region": region_partitions}
)
aws_zarr_date_range = pd.date_range("2020-10-14", "2026-12-31", freq="2min")


def download_mrms(day: dt.datetime, measurement_type: str) -> list[str]:
    assert measurement_type in [
        "GaugeCorr_QPE_01H",
        "PrecipFlag",
        "PrecipRate",
        "RadarOnly_QPE_01H",
        "MultiSensor_QPE_01H_Pass2",
    ]
    path = f"https://mtarchive.geol.iastate.edu/{day.strftime('%Y')}/{day.strftime('%m')}/{day.strftime('%d')}/mrms/ncep/{measurement_type}/"
    args = ["wget", "-r", "-nc", "--no-parent", path, "-P", ARCHIVE_FOLDER]
    process = Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    process.wait()
    # Get the paths that have been downloaded
    return sorted(
        list(
            glob.glob(
                f"{ARCHIVE_FOLDER}/mtarchive.geol.iastate.edu/{day.strftime('%Y')}/{day.strftime('%m')}/{day.strftime('%d')}/mrms/ncep/{measurement_type}/*.grib2.gz"
            )
        )
    )


def download_aws_mrms(day: dt.datetime, measurement_type: str, area: str) -> list[str]:
    path = f"s3://noaa-mrms-pds/{area}/{measurement_type}_00.00/{day.strftime('%Y')}{day.strftime('%m')}{day.strftime('%d')}/"
    args = [
        "aws",
        "s3",
        "cp",
        "--recursive",
        "--no-sign-request",
        path,
        f"{AWS_ARCHIVE_FOLDER}/{area}/{day.strftime('%Y')}{day.strftime('%m')}{day.strftime('%d')}/",
    ]
    process = Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    process.wait()
    # Get the paths that have been downloaded
    return sorted(
        list(
            glob.glob(
                f"{AWS_ARCHIVE_FOLDER}/{area}/{day.strftime('%Y')}{day.strftime('%m')}{day.strftime('%d')}/*.grib2.gz"
            )
        )
    )


def get_mrms(day: dt.datetime, measurement_type: str) -> list[str]:
    return sorted(
        list(
            glob.glob(
                f"{ARCHIVE_FOLDER}/mtarchive.geol.iastate.edu/{day.strftime('%Y')}/{day.strftime('%m')}/{day.strftime('%d')}/mrms/ncep/{measurement_type}/*.grib2.gz"
            )
        )
    )


def get_aws_mrms(day: dt.datetime, measurement_type: str, area: str) -> list[str]:
    return sorted(
        list(
            glob.glob(
                f"{AWS_ARCHIVE_FOLDER}/{area}/{day.strftime('%Y')}{day.strftime('%m')}{day.strftime('%d')}/MRMS_{measurement_type}_*.grib2.gz"
            )
        )
    )


@dg.asset(
    name="mrms-precip-download",
    description="Download MRMS radar precipitation from Iowa State University",
    tags={
        "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
        "dagster/priority": "1",
        "dagster/concurrency_key": "download",
    },
    partitions_def=partitions_def,
    automation_condition=dg.AutomationCondition.eager(),
)
def mrms_preciprate_download_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    it: dt.datetime = context.partition_time_window.start
    downloaded_files = download_mrms(it, "PrecipRate")
    # Return the paths as a materialization
    return dg.MaterializeResult(
        metadata={"files": downloaded_files},
    )


@dg.asset(
    name="aws-mrms-download",
    description="Download MRMS radar precipitation from Iowa State University",
    tags={
        "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
        "dagster/priority": "1",
        "dagster/concurrency_key": "download",
    },
    partitions_def=two_dimensional_partitions,
    automation_condition=dg.AutomationCondition.eager(),
)
def aws_mrms_download_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    it: dt.datetime = context.partition_time_window.start
    keys_by_dimension: dg.MultiPartitionKey = context.partition_key.keys_by_dimension

    date = keys_by_dimension["date"]
    region = keys_by_dimension["region"]
    downloaded_files = download_aws_mrms(it, "PrecipRate", region)
    download_flag_files = download_aws_mrms(it, "PrecipFlag", region)
    # Return the paths as a materialization
    return dg.MaterializeResult(
        metadata={
            "rate_files": downloaded_files,
            "flag_files": download_flag_files,
        },
    )


@dg.asset(
    name="mrms-flag-download",
    description="Download MRMS radar precipitation from Iowa State University",
    tags={
        "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
        "dagster/priority": "1",
        "dagster/concurrency_key": "download",
    },
    partitions_def=partitions_def,
    automation_condition=dg.AutomationCondition.eager(),
)
def mrms_precipflag_download_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    it: dt.datetime = context.partition_time_window.start
    downloaded_files = download_mrms(it, "PrecipFlag")
    # Return the paths as a materialization
    return dg.MaterializeResult(
        metadata={"files": downloaded_files},
    )


@dg.asset(
    name="mrms-dummy-zarr",
    deps=[mrms_precipflag_download_asset, mrms_preciprate_download_asset],
    description="Dummy Zarr archive of MRMS PrecipFlag",
    automation_condition=dg.AutomationCondition.eager(),
)
def mrms_precipflag_dummy_zarr_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    if os.path.exists(ZARR_PATH):
        return dg.MaterializeResult(
            metadata={"zarr_path": ZARR_PATH},
        )
    for date in zarr_date_range:
        files = get_mrms(date, "PrecipFlag")
        if len(files) > 0:
            break
    if len(files) == 0:
        raise FileNotFoundError("No files found")
    timestamps = zarr_date_range
    # open a single file and get the coordinate metadata
    data = load_mrms_flag(files[0])
    latitudes = data.latitude.values
    longitudes = data.longitude.values
    dummies = -99 * dask.array.ones(
        (len(timestamps), len(latitudes), len(longitudes)),
        chunks=(1, len(latitudes), len(longitudes)),
        dtype="int8",
    )
    rate_dummies = dask.array.zeros(
        (len(timestamps), len(latitudes), len(longitudes)),
        chunks=(1, len(latitudes), len(longitudes)),
        dtype="float16",
    )
    ds = xr.Dataset(
        {
            "precipitation_flag": (("time", "latitude", "longitude"), dummies),
            "precipitation_rate": (("time", "latitude", "longitude"), rate_dummies),
        },
        coords={"time": timestamps, "latitude": latitudes, "longitude": longitudes},
    )
    encoding = {
        v: {
            "compressors": zarr.codecs.BloscCodec(
                cname="zstd", clevel=9, shuffle=zarr.codecs.BloscShuffle.bitshuffle
            )
        }
        for v in ds.data_vars
    }
    encoding["time"] = {"units": "nanoseconds since 1970-01-01"}
    ds.to_zarr(ZARR_PATH, compute=False, zarr_format=3, encoding=encoding)

    return dg.MaterializeResult(
        metadata={"zarr_path": ZARR_PATH},
    )


@dg.asset(
    name="mrms-area-dummy-zarr",
    partitions_def=two_dimensional_partitions,
    # deps=[aws_mrms_download_asset],
    description="Dummy Zarr archive of MRMS PrecipFlag Area",
    automation_condition=dg.AutomationCondition.eager(),
)
def aws_mrms_precipflag_dummy_zarr_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    # partition_key looks like "2024-01-01|us"
    keys_by_dimension: dg.MultiPartitionKey = context.partition_key.keys_by_dimension

    date = keys_by_dimension["date"]
    region = keys_by_dimension["region"]
    if region == "HAWAII":
        zarr_path = HAWAII_ZARR_PATH
    elif region == "CARIB":
        zarr_path = CARIB_ZARR_PATH
    elif region == "GUAM":
        zarr_path = GUAM_ZARR_PATH
    elif region == "ALASKA":
        zarr_path = ALASKA_ZARR_PATH
    if os.path.exists(zarr_path):
        return dg.MaterializeResult(
            metadata={"zarr_path": zarr_path},
        )
    for date in zarr_date_range:
        files = get_aws_mrms(date, "PrecipFlag", region)
        if len(files) > 0:
            break
    if len(files) == 0:
        raise FileNotFoundError("No files found")
    timestamps = aws_zarr_date_range
    # open a single file and get the coordinate metadata
    data = load_mrms_flag(files[0])
    latitudes = data.latitude.values
    longitudes = data.longitude.values
    dummies = -99 * dask.array.ones(
        (len(timestamps), len(latitudes), len(longitudes)),
        chunks=(1, len(latitudes), len(longitudes)),
        dtype="int8",
    )
    rate_dummies = dask.array.zeros(
        (len(timestamps), len(latitudes), len(longitudes)),
        chunks=(1, len(latitudes), len(longitudes)),
        dtype="float16",
    )
    ds = xr.Dataset(
        {
            "precipitation_flag": (("time", "latitude", "longitude"), dummies),
            "precipitation_rate": (("time", "latitude", "longitude"), rate_dummies),
        },
        coords={"time": timestamps, "latitude": latitudes, "longitude": longitudes},
    )
    encoding = {
        v: {
            "compressors": zarr.codecs.BloscCodec(
                cname="zstd", clevel=9, shuffle=zarr.codecs.BloscShuffle.bitshuffle
            )
        }
        for v in ds.data_vars
    }
    encoding["time"] = {"units": "nanoseconds since 1970-01-01"}
    ds.to_zarr(zarr_path, compute=False, zarr_format=3, encoding=encoding)

    return dg.MaterializeResult(
        metadata={"zarr_path": zarr_path},
    )


@dg.asset(
    name="region-mrms-zarr",
    description=__doc__,
    metadata={
        "archive_folder": dg.MetadataValue.text(ARCHIVE_FOLDER),
        "area": dg.MetadataValue.text("global"),
        "source": dg.MetadataValue.text("noaa-aws"),
        "expected_runtime": dg.MetadataValue.text("1 hour"),
    },
    deps=[aws_mrms_precipflag_dummy_zarr_asset],
    tags={
        "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
        "dagster/priority": "1",
        "dagster/concurrency_key": "zarr-creation",
    },
    partitions_def=two_dimensional_partitions,
    # executor=dg.multiprocess_executor.configured({"max_concurrent": 10}),
    automation_condition=dg.AutomationCondition.eager(),
)
def aws_mrms_zarr_asset(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    """Dagster asset for NOAA's GMGSI global mosaic of geostationary satellites."""
    keys_by_dimension: dg.MultiPartitionKey = context.partition_key.keys_by_dimension

    date = keys_by_dimension["date"]
    region = keys_by_dimension["region"]
    if region == "HAWAII":
        zarr_path = HAWAII_ZARR_PATH
    elif region == "CARIB":
        zarr_path = CARIB_ZARR_PATH
    elif region == "GUAM":
        zarr_path = GUAM_ZARR_PATH
    elif region == "ALASKA":
        zarr_path = ALASKA_ZARR_PATH
    it: dt.datetime = context.partition_time_window.start
    files = get_aws_mrms(it, "PrecipFlag", region)
    rate_files = get_aws_mrms(it, "PrecipRate", region)
    zarr_dates = xr.open_zarr(zarr_path).time.values
    for f in files:
        data = load_mrms_flag(f)
        data.transpose("time", "latitude", "longitude").to_zarr(
            zarr_path,
            region={"time": "auto", "latitude": "auto", "longitude": "auto"},
        )
    for f in rate_files:
        data = load_mrms_rate(f)
        data.transpose("time", "latitude", "longitude").to_zarr(
            zarr_path,
            region={"time": "auto", "latitude": "auto", "longitude": "auto"},
        )

    return dg.MaterializeResult(
        metadata={"zarr_path": zarr_path, "time": it.strftime("%Y-%m-%d")},
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
    deps=[
        mrms_precipflag_dummy_zarr_asset,
        mrms_precipflag_download_asset,
        mrms_preciprate_download_asset,
    ],
    tags={
        "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
        "dagster/priority": "1",
        "dagster/concurrency_key": "zarr-creation",
    },
    partitions_def=partitions_def,
    # executor=dg.multiprocess_executor.configured({"max_concurrent": 10}),
    automation_condition=dg.AutomationCondition.eager(),
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
        data.transpose("time", "latitude", "longitude").to_zarr(
            ZARR_PATH,
            region={"time": "auto", "latitude": "auto", "longitude": "auto"},
        )
    for f in rate_files:
        data = load_mrms_rate(f)
        data.transpose("time", "latitude", "longitude").to_zarr(
            ZARR_PATH,
            region={"time": "auto", "latitude": "auto", "longitude": "auto"},
        )

    return dg.MaterializeResult(
        metadata={"zarr_path": ZARR_PATH, "time": it.strftime("%Y-%m-%d")},
    )


@dg.asset(
    name="mrms-upload-source-coop",
    deps=[mrms_zarr_asset],
    description="Upload MRMS radar precipitation to Source Coop",
    automation_condition=dg.AutomationCondition.eager(),
)
def mrms_upload_source_coop(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    # Sync the Zarr to Source Coop
    args = ["aws", "s3", "sync", ZARR_PATH + "/", SOURCE_COOP_PATH + "/", "--profile=sc"]
    process = Popen(args)
    process.wait()
    return dg.MaterializeResult(metadata={"zarr_path": SOURCE_COOP_PATH})


def construct_timestamps_from_filename(filename):
    # 20160122-194600
    if "_00.00_" in filename:
        # Extract the timestamp from the filename
        timestamp = filename.split("/")[-1].split("_00.00_")[-1].split(".")[0]
    else:
        timestamp = filename.split("_")[-1].split(".")[0]
    return pd.to_datetime(timestamp, format="%Y%m%d-%H%M%S")


def load_mrms_flag(filepath) -> xr.Dataset:
    rand_num = np.random.randint(0, 10000)
    timestamp = construct_timestamps_from_filename(filepath)
    with tempfile.TemporaryDirectory() as tmp:
        with gzip.open(filepath, "rb") as f:
            with open(os.path.join(tmp, f"{rand_num}.grib2"), "wb") as f_out:
                shutil.copyfileobj(f, f_out)
        data = xr.load_dataset(
            os.path.join(tmp, f"{rand_num}.grib2"), engine="cfgrib", decode_timedelta=False
        )
        data = data.rename({"unknown": "precipitation_flag"})
        data = data.expand_dims(dim="time")
        data["time"] = [timestamp]
        data = data.chunk({"latitude": -1, "longitude": -1})
        data = data.astype("int8")
        # Drop 'step' 'heightAboveSea' and 'valid_time'
        data = data.drop_vars(["step", "heightAboveSea", "valid_time"])
        data.load()
        if os.path.exists(os.path.join(tmp, f"{rand_num}.grib2")):
            os.remove(os.path.join(tmp, f"{rand_num}.grib2"))
        return data


def load_mrms_rate(filepath) -> xr.Dataset:
    rand_num = np.random.randint(0, 10000)
    timestamp = construct_timestamps_from_filename(filepath)
    with tempfile.TemporaryDirectory() as tmp:
        with gzip.open(filepath, "rb") as f:
            with open(os.path.join(tmp, f"{rand_num}.grib2"), "wb") as f_out:
                shutil.copyfileobj(f, f_out)
        data = xr.load_dataset(
            os.path.join(tmp, f"{rand_num}.grib2"), engine="cfgrib", decode_timedelta=False
        )
        data = data.rename({"unknown": "precipitation_rate"})
        data = data.expand_dims(dim="time")
        data["time"] = [timestamp]
        data = data.chunk({"latitude": -1, "longitude": -1})
        data = data.astype("float16")
        # Drop 'step' 'heightAboveSea' and 'valid_time'
        data = data.drop_vars(["step", "heightAboveSea", "valid_time"])
        data.load()
        if os.path.exists(os.path.join(tmp, f"{rand_num}.grib2")):
            os.remove(os.path.join(tmp, f"{rand_num}.grib2"))
        return data
