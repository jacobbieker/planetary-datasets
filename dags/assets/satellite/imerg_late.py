"""This gather and uses the global mosaic of geostationary satellites from NOAA on AWS"""
import dask.array
from subprocess import Popen
import subprocess
import glob
import datetime as dt
import os
from typing import TYPE_CHECKING

import dagster as dg
import dask.array
import numpy as np
import pandas as pd
import xarray as xr
import zarr

if TYPE_CHECKING:
    import datetime as dt

"""
wget --load-cookies ~/.urs_cookies --save-cookies ~/.urs_cookies --keep-session-cookies --content-disposition -r -c --no-parent https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGHH.07/

IMERG Late

"""

ARCHIVE_FOLDER = "/run/media/jacob/Tester/IMERG_final/"
BASE_URL = "s3://noaa-gmgsi-pds/"
SOURCE_COOP_PATH = "s3://bkr/imerg/imerg_final.zarr"
ZARR_PATH = "/ext_data/GPM_final/imerg-late.zarr"
ZARR_DATE_RANGE = pd.date_range("2000-01-01", "2026-12-31", freq="30min")
if os.getenv("ENVIRONMENT", "local") == "pb":
    ARCHIVE_FOLDER = "/data/imerg-late/"

partitions_def: dg.TimeWindowPartitionsDefinition = dg.DailyPartitionsDefinition(
    start_date="2000-01-01",
    end_offset=-1,
)


@dg.asset(name="imerg-late-download", description="Download IMERG early satellite precipitation data from NASA",
          tags={
              "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
              "dagster/priority": "2",
              "dagster/concurrency_key": "download",
          },
          partitions_def=partitions_def,
          automation_condition=dg.AutomationCondition.eager(),

          )
def imerg_late_download_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    it: dt.datetime = context.partition_time_window.start
    downloaded_files = downlod_gpm_late(it)
    # Return the paths as a materialization
    return dg.MaterializeResult(
        metadata={
            "files": downloaded_files,
        }
    )


@dg.asset(name="imerg-late-dummy-zarr",
          deps=[imerg_late_download_asset],
          description="Dummy Zarr archive of satellite image data from IMERG late precipitation",
          automation_condition=dg.AutomationCondition.eager(),)
def imerg_late_dummy_zarr(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    if os.path.exists(ZARR_PATH):
        return dg.MaterializeResult(
            metadata={"zarr_path": ZARR_PATH},
        )

    data: xr.Dataset = open_h5(get_imerg_late_files(context.partition_time_window.start)[0])
    variables = ["precipitation", "randomError", "probabilityLiquidPrecipitation", "precipitationQualityIndex"]
    encoding = {
        v: {"compressors": zarr.codecs.BloscCodec(cname='zstd', clevel=9, shuffle=zarr.codecs.BloscShuffle.bitshuffle)}
        for v in variables}
    encoding["time"] = {"units": "nanoseconds since 1970-01-01"}
    # Get the number of partitions to create
    date_range = pd.date_range(start="2000-01-01", end="2026-12-31", freq="30min")
    dummies = dask.array.zeros((len(date_range), data.latitude.shape[0], data.longitude.shape[0]), chunks=(1, -1, -1),
                               dtype=np.float16)
    default_dataarray = xr.DataArray(dummies, coords={"time": date_range, "latitude": data.latitude.values,
                                                      "longitude": data.longitude.values},
                                     dims=["time", "latitude", "longitude"])
    dummy_dataset = xr.Dataset({v: default_dataarray for v in variables},
                               coords={"time": date_range, "latitude": data.latitude.values,
                                       "longitude": data.longitude.values})
    dummy_dataset.to_zarr(ZARR_PATH, mode="w", compute=False, zarr_format=3, encoding=encoding)
    return dg.MaterializeResult(
        metadata={"zarr_path": ZARR_PATH},
    )

@dg.asset(
        name="imerg-late-zarr",
        description=__doc__,
        metadata={
            "archive_folder": dg.MetadataValue.text(ARCHIVE_FOLDER),
            "area": dg.MetadataValue.text("global"),
            "source": dg.MetadataValue.text("noaa-aws"),
            "expected_runtime": dg.MetadataValue.text("1 hour"),
        },
        deps=[imerg_late_download_asset, imerg_late_dummy_zarr],
        tags={
            "dagster/max_runtime": str(60 * 60 * 10), # Should take 6 ish hours
            "dagster/priority": "2",
            "dagster/concurrency_key": "zarr-creation",
        },
    partitions_def=partitions_def,
automation_condition=dg.AutomationCondition.eager(),
)
def imerg_late_zarr_asset(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    """Dagster asset for NOAA's GMGSI global mosaic of geostationary satellites."""
    it: dt.datetime = context.partition_time_window.start
    files = get_imerg_late_files(it)
    for f in files:
        data = open_h5(f)
        data.chunk({"time": 1, "longitude": -1, "latitude": -1}).to_zarr(ZARR_PATH,
                                                                         region={"time": "auto",
                                                                                 "latitude": "auto", "longitude": "auto"})
        os.remove(f)
        # Remove xml additional file if available
        xml_file = f.replace(".HDF5", ".HDF5.xml")
        if os.path.exists(xml_file):
            os.remove(xml_file)
    return dg.MaterializeResult(
        metadata={"zarr_path": ZARR_PATH},
    )

@dg.asset(name="imerg-final-upload-source-coop",
          deps=[imerg_late_zarr_asset],
          description="Upload GPM IMERG Final to Source Coop",
          automation_condition=dg.AutomationCondition.eager(),)
def silam_upload_source_coop(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    # Sync the Zarr to Source Coop
    args = ["aws", "s3", "sync", ZARR_PATH+"/", SOURCE_COOP_PATH+"/", "--profile=sc"]
    process = Popen(args)
    process.wait()
    return dg.MaterializeResult(metadata={"zarr_path": SOURCE_COOP_PATH})

def downlod_gpm_late(day: dt.datetime) -> list[str]:
    # Get day of year from day
    day_of_year = day.timetuple().tm_yday
    path = f"https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGHH.07/{day.strftime('%Y')}/{str(day_of_year).zfill(3)}/"
    args = ["wget", "--load-cookies", "~/.urs_cookies", "--save-cookies", "~/.urs_cookies", "--keep-session-cookies", "--content-disposition", "-r", "-c", "--no-parent", path, "-P", ARCHIVE_FOLDER]
    process = Popen(args,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                    )
    process.wait()
    return get_imerg_late_files(day)

def get_imerg_late_files(day: dt.datetime) -> list[str]:
    day_of_year = day.timetuple().tm_yday
    return sorted(list(glob.glob(f"{ARCHIVE_FOLDER}gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGHH.07/{day.strftime('%Y')}/{str(day_of_year).zfill(3)}/*.HDF5")))


def open_h5(filename: str) -> xr.Dataset:
    data = xr.open_dataset(filename, group="/Grid")
    # Drop latv, lonv, nv
    data = data.drop_dims(["latv", "lonv", "nv"]).rename({"lat": "latitude", "lon": "longitude"}).chunk({"time": 1, "latitude": -1, "longitude": -1})
    return data.astype("float16")

from datetime import datetime as dt

def convert_to_dt(x):
    return dt.strptime(str(x), '%Y-%m-%d %H:%M:%S')