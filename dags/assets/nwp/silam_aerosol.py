import glob

import xarray as xr
import os
import datetime as dt
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

# data = xr.open_mfdataset("/Users/jacob/Development/planetary-datasets/AWS_FMI/global/20250326/silam_glob_v5_7_1_20250326_*_d*.nc")
# print(data)


def open_forecast_and_prepare(glob_pattern: str) -> xr.Dataset:
    data = xr.open_mfdataset(glob_pattern, parallel=True)
    data = data.rename({"lat": "latitude", "lon": "longitude"})
    # get init time from the first timestamp
    init_time = data.time.values[0]
    # Add init time as a dimension
    data.coords["init_time"] = init_time
    # Change time to step by getting timedelta
    steps = data.time.values - init_time
    data["time"] = steps
    # rename time to step
    data = data.rename({"time": "step"})
    # Convert to float16 to save some space
    data = data.astype(np.float16)
    return data


# print(open_forecast_and_prepare(("/Users/jacob/Development/planetary-datasets/AWS_FMI/global/20250326/silam_glob_v5_7_1_20250326_*_d*.nc")))
"""
folders = sorted(list(glob.glob("/Users/jacob/Development/planetary-datasets/AWS_FMI/global/*")))
for folder in folders:
    try:
        data = xr.open_mfdataset(os.path.join(folder, "silam_glob_v*_*_d*.nc"), parallel=True)
    except:
        print(f"Failed to open {folder}")
        continue
    data = data.rename({"lat": "latitude", "lon": "longitude"})
    # get init time from the first timestamp
    init_time = data.time.values[0]
    # Add init time as a dimension
    data.coords["init_time"] = init_time
    # Expand dims to add init_time
    data = data.expand_dims("init_time")
    # Change time to step by getting timedelta
    steps = data.time.values - init_time
    data["time"] = steps
    # rename time to step
    data = data.rename({"time": "step"})
    # Convert to float16 to save some space
    data = data.astype(np.float16)
    if os.path.exists("silam_aerosol_zarr.zarr"):
        data.chunk({"init_time": 1, "step": -1, "latitude": -1, "longitude": -1}).to_zarr("silam_aerosol_zarr.zarr", mode="a", append_dim="init_time")
    else:
        variables = list(data.data_vars)
        encoding = {
            v: {"compressors": zarr.codecs.BloscCodec(cname='zstd', clevel=9,
                                                      shuffle=zarr.codecs.BloscShuffle.bitshuffle)}
            for v in variables}
        data.chunk({"init_time": 1, "step": -1, "latitude": -1, "longitude": -1}).to_zarr("silam_aerosol_zarr.zarr", mode="w", zarr_format=3, compute=True)
exit()
"""
ARCHIVE_FOLDER = "/Users/jacob/Development/planetary-datasets/AWS_FMI/global"
BASE_URL = "s3://fmi-opendata-silam-surface-netcdf/"
SOURCE_COOP_PATH = "s3://bkr/silam-aerosol/silam_global_surface_aerosol.zarr"
ZARR_PATH = "silam_global_surface_aerosol.zarr"
if os.getenv("ENVIRONMENT", "local") == "pb":
    ARCHIVE_FOLDER = "/ext_data/SILAM_Dust/silam/"

partitions_def: dg.TimeWindowPartitionsDefinition = dg.DailyPartitionsDefinition(
    start_date="2025-04-26",
    end_offset=-1,
)


@dg.asset(
    name="silam-aerosol-dummy-zarr",
    description="Dummy Zarr archive of SILAM Aerosol from FMI",
    automation_condition=dg.AutomationCondition.eager(),
    partitions_def=partitions_def,
)
def silam_aerosol_dummy_zarr_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    if os.path.exists(ZARR_PATH):
        return dg.MaterializeResult(
            metadata={"zarr_path": ZARR_PATH},
        )

    zarr_date_range = pd.date_range(
        start="2025-04-26T01:00:00", end="2026-12-31T01:00:00", freq="D"
    )
    date = pd.Timestamp(context.partition_time_window.start)
    date_str = date.strftime("%Y%m%d")
    glob_pattern = os.path.join(ARCHIVE_FOLDER, f"{date_str}", f"silam_glob_v*_{date_str}_*_d*.nc")
    data = open_forecast_and_prepare(glob_pattern)

    variables = list(data.data_vars)

    encoding = {
        v: {
            "compressors": zarr.codecs.BloscCodec(
                cname="zstd", clevel=9, shuffle=zarr.codecs.BloscShuffle.bitshuffle
            )
        }
        for v in variables
    }

    # Create empty dask arrays of the same size as the data
    dummies_4d_var = dask.array.zeros(
        (len(zarr_date_range), len(data.step), data.latitude.shape[0], data.longitude.shape[0]),
        chunks=(1, 1, -1, -1),
        dtype=np.float16,
    )
    default_dataarray = xr.DataArray(
        dummies_4d_var,
        coords={
            "init_time": zarr_date_range,
            "step": data.step.values,
            "latitude": data.latitude.values,
            "longitude": data.longitude.values,
        },
        dims=["init_time", "step", "latitude", "longitude"],
    )
    dummy_dataset = xr.Dataset(
        {v: default_dataarray for v in variables},
        coords={
            "init_time": zarr_date_range,
            "step": data.step.values,
            "latitude": data.latitude.values,
            "longitude": data.longitude.values,
        },
    )
    dummy_dataset.chunk({"init_time": 1, "step": 1, "latitude": -1, "longitude": -1}).to_zarr(
        ZARR_PATH,
        mode="w",
        compute=False,
        zarr_format=3,
        encoding=encoding,
    )  # storage_options={"endpoint_url": "https://data.source.coop"})
    return dg.MaterializeResult(
        metadata={"zarr_path": ZARR_PATH},
    )


@dg.asset(
    name="silam-aerosol-zarr",
    description=__doc__,
    metadata={
        "archive_folder": dg.MetadataValue.text(ARCHIVE_FOLDER),
        "area": dg.MetadataValue.text("global"),
        "source": dg.MetadataValue.text("silam-aerosol"),
        "expected_runtime": dg.MetadataValue.text("24 hours"),
    },
    deps=[silam_aerosol_dummy_zarr_asset],
    tags={
        "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
        "dagster/priority": "2",
        "dagster/concurrency_key": "heavy-zarr-creation",
        "large_zarr_creation": "true",
    },
    partitions_def=partitions_def,
    automation_condition=dg.AutomationCondition.eager(),
)
def silam_aerosol_zarr_asset(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    """Dagster asset for NOAA's GMGSI global mosaic of geostationary satellites."""
    it: dt.datetime = context.partition_time_window.start
    date = pd.Timestamp(context.partition_time_window.start)
    date_str = date.strftime("%Y%m%d")
    glob_pattern = os.path.join(
        ARCHIVE_FOLDER, f"{date_str}", f"silam_glob_v5_7_2_{date_str}_*_d*.nc"
    )
    ds = open_forecast_and_prepare(glob_pattern)

    zarr_dates = xr.open_zarr(ZARR_PATH).time.values
    time_idx = np.where(zarr_dates == it)[0][0]
    data = (
        xr.concat(ds, dim="step")
        .sortby("step")
        .chunk({"step": 1, "latitude": -1, "longitude": -1, "init_time": 1})
    )
    data.to_zarr(
        ZARR_PATH,
        region={
            "init_time": slice(time_idx, time_idx + 1),
            "latitude": "auto",
            "longitude": "auto",
            "step": 1,
        },
    )
    data.close()

    return dg.MaterializeResult(
        metadata={"zarr_path": ZARR_PATH},
    )
