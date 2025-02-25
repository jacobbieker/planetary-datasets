"""This gather and uses the global mosaic of geostationary satellites from NOAA on AWS"""
from subprocess import Popen
import subprocess
import glob
import datetime as dt
import os
from typing import TYPE_CHECKING
import pandas as pd

import dagster as dg
if TYPE_CHECKING:
    import datetime as dt

ARCHIVE_FOLDER = "/ext_data/GPDS/"
ZARR_PATH = "/ext_data/GPDS/gpds.zarr"
ZARR_DATE_RANGE = pd.date_range("2023-01-01", "2026-12-31", freq="12h")
if os.getenv("ENVIRONMENT", "local") == "pb":
    ARCHIVE_FOLDER = "/ext_data/GPDS/"

partitions_def: dg.TimeWindowPartitionsDefinition = dg.DailyPartitionsDefinition(
    start_date="2023-01-01",
    end_offset=-1,
)


@dg.asset(name="gpds-download", description="Download GPDS from CMCC",
          tags={
              "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
              "dagster/priority": "2",
              "dagster/concurrency_key": "download",
          },
          partitions_def=partitions_def,
          automation_condition=dg.AutomationCondition.eager(),

          )
def gpds_download_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    it: dt.datetime = context.partition_time_window.start
    downloaded_files = download_gpds(it)
    # Return the paths as a materialization
    return dg.MaterializeResult(
        metadata={
            "files": downloaded_files,
        }
    )

def download_gpds(day: dt.datetime) -> list[str]:
    # Get day of year from day
    path = f"https://dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/"
    args = ["wget2", "-r", "-c", "--no-parent", path, "-P", ARCHIVE_FOLDER]
    process = Popen(args,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                    )
    process.wait()
    return get_gpds_files(day)

def get_gpds_files(day: dt.datetime) -> list[str]:
    return sorted(list(glob.glob(f"{ARCHIVE_FOLDER}dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/*/*/*{day.strftime('%Y%m%d')}*.grib2")))

