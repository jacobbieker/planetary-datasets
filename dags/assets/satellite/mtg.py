import eumdac
import shutil
import fnmatch

import os
from typing import TYPE_CHECKING, Optional

import dagster as dg
import datetime as dt

consumer_key = os.getenv("EUMETSAT_CONSUMER_KEY")
consumer_secret = os.getenv("EUMETSAT_CONSUMER_SECRET")


# This function checks if a product entry is part of the requested coverage
def select_netcdf(filenames):
    chunks = []
    for file in filenames:
        if fnmatch.fnmatch(file, "*.nc"):
            chunks.append(file)
    return chunks


ARCHIVE_FOLDER = "/run/media/jacob/Square1/mtg/"
ZARR_PATH = "/run/media/jacob/Square1/mtg.zarr"

partitions_def: dg.TimeWindowPartitionsDefinition = dg.HourlyPartitionsDefinition(
    start_date="2024-09-01-00:00",
    end_offset=-3,
)


@dg.asset(
    name="mtg-fdhi-download",
    description="Download MTG High Res global geostationary satellites from EUMETSAT",
    tags={
        "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
        "dagster/priority": "1",
        "dagster/concurrency_key": "download",
    },
    partitions_def=partitions_def,
    automation_condition=dg.AutomationCondition.eager(),
)
def mtg_high_res_download_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    it: dt.datetime = context.partition_time_window.start
    downloaded_files = []
    # Get all 10 minute chunks
    # Feed the token object with your credentials, find yours at https://api.eumetsat.int/api-key/
    credentials = (consumer_key, consumer_secret)
    token = eumdac.AccessToken(credentials)

    # Create datastore object with with your token
    datastore = eumdac.DataStore(token)

    # Select an FCI collection, eg "FCI Level 1c High Resolution Image Data - MTG - 0 degree" - "EO:EUM:DAT:0665"
    selected_collection = datastore.get_collection("EO:EUM:DAT:0665")
    # 0662 is regular resolution

    # Set sensing start and end time
    start = it
    end = it + dt.timedelta(hours=1)

    # Retrieve datasets that match the filter
    products = selected_collection.search(dtstart=start, dtend=end)

    for product in products:
        # Make directories if needed
        if not os.path.exists(os.path.join(ARCHIVE_FOLDER, it.strftime("%Y%m%d%H"), "FDHI")):
            os.makedirs(os.path.join(ARCHIVE_FOLDER, it.strftime("%Y%m%d%H"), "FDHI"))
            context.log.info(
                f"Created directory {os.path.join(ARCHIVE_FOLDER, it.strftime('%Y%m%d%H'), 'FDHI')}"
            )
        product = datastore.get_product(product_id=product, collection_id="EO:EUM:DAT:0665")
        for file in select_netcdf(product.entries):
            with (
                product.open(entry=file) as fsrc,
                open(
                    os.path.join(ARCHIVE_FOLDER, it.strftime("%Y%m%d%H"), "FDHI", fsrc.name),
                    mode="wb",
                ) as fdst,
            ):
                shutil.copyfileobj(fsrc, fdst)
                downloaded_files.append(
                    os.path.join(ARCHIVE_FOLDER, it.strftime("%Y%m%d%H"), "FDHI", fsrc.name)
                )
                context.log.info(
                    f"Downloaded {fsrc.name} to {os.path.join(ARCHIVE_FOLDER, it.strftime('%Y%m%d%H'), 'FDHI', fsrc.name)}"
                )

    # Return the paths as a materialization
    return dg.MaterializeResult(
        metadata={
            "files": downloaded_files,
        },
    )


@dg.asset(
    name="mtg-fdlr-download",
    description="Download MTG Low Res global geostationary satellites from EUMETSAT",
    tags={
        "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
        "dagster/priority": "1",
        "dagster/concurrency_key": "download",
    },
    partitions_def=partitions_def,
    automation_condition=dg.AutomationCondition.eager(),
)
def mtg_low_res_download_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    it: dt.datetime = context.partition_time_window.start
    downloaded_files = []
    # Get all 10 minute chunks
    # Feed the token object with your credentials, find yours at https://api.eumetsat.int/api-key/
    credentials = (consumer_key, consumer_secret)
    token = eumdac.AccessToken(credentials)

    # Create datastore object with with your token
    datastore = eumdac.DataStore(token)

    # Select an FCI collection, eg "FCI Level 1c High Resolution Image Data - MTG - 0 degree" - "EO:EUM:DAT:0665"
    selected_collection = datastore.get_collection("EO:EUM:DAT:0662")
    # 0662 is regular resolution

    # Set sensing start and end time
    start = it
    end = it + dt.timedelta(hours=1)

    # Retrieve datasets that match the filter
    products = selected_collection.search(dtstart=start, dtend=end)

    for product in products:
        # Make directories if needed
        if not os.path.exists(os.path.join(ARCHIVE_FOLDER, it.strftime("%Y%m%d%H"), "FDLR")):
            os.makedirs(os.path.join(ARCHIVE_FOLDER, it.strftime("%Y%m%d%H"), "FDLR"))
            context.log.info(
                f"Created directory {os.path.join(ARCHIVE_FOLDER, it.strftime('%Y%m%d%H'), 'FDLR')}"
            )
        product = datastore.get_product(product_id=product, collection_id="EO:EUM:DAT:0662")
        for file in select_netcdf(product.entries):
            with (
                product.open(entry=file) as fsrc,
                open(
                    os.path.join(ARCHIVE_FOLDER, it.strftime("%Y%m%d%H"), "FDLR", fsrc.name),
                    mode="wb",
                ) as fdst,
            ):
                shutil.copyfileobj(fsrc, fdst)
                downloaded_files.append(
                    os.path.join(ARCHIVE_FOLDER, it.strftime("%Y%m%d%H"), "FDLR", fsrc.name)
                )
                context.log.info(
                    f"Downloaded {fsrc.name} to {os.path.join(ARCHIVE_FOLDER, it.strftime('%Y%m%d%H'), 'FDLR', fsrc.name)}"
                )

    # Return the paths as a materialization
    return dg.MaterializeResult(
        metadata={
            "files": downloaded_files,
        },
    )
