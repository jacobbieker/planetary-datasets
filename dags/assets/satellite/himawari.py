"""This gather and uses the global mosaic of geostationary satellites from NOAA on AWS"""

import datetime as dt
import os
from typing import TYPE_CHECKING, Optional

import dagster as dg
import dask.array
import fsspec
import s3fs
import numpy as np
import pandas as pd
import xarray as xr
import zarr
import icechunk

from virtualizarr import open_virtual_dataset

if TYPE_CHECKING:
    import datetime as dt

"""Icechunk of virtualizarr of the GOES satellites from NOAA on AWS"""

ARCHIVE_FOLDER = "/ext_data/himawari/"

himawari8_partitions_def: dg.TimeWindowPartitionsDefinition = dg.HourlyPartitionsDefinition(
    start_date="2020-01-21-04:00",
    end_date="2022-12-12-18:00",
)  # This is for the ISatSS NetCDF files, the original data goes back to 2015 in AWS, but not virtualizable

himawari9_partitions_def: dg.TimeWindowPartitionsDefinition = dg.HourlyPartitionsDefinition(
    start_date="2022-12-12-18:00",
    end_offset=-2,
)  # Day 209 of 2022 00:00 is first day of data

# Add a different partition, by band
bands = [str(i) for i in range(1, 17)]
band_partition: dg.StaticPartitionsDefinition = dg.StaticPartitionsDefinition(bands)

himawari8_two_dimensional_partitions = dg.MultiPartitionsDefinition(
    {"date": himawari8_partitions_def, "band": band_partition}
)

himawari9_two_dimensional_partitions = dg.MultiPartitionsDefinition(
    {"date": himawari9_partitions_def, "band": band_partition}
)


@dg.asset(
    name="himawari8-virtualizarr",
    description="Create Virtualizarr reference of Himawari-8 satellite data from NOAA on AWS",
    tags={
        "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
        "dagster/priority": "1",
        "dagster/concurrency_key": "icechunk",
    },
    partitions_def=himawari8_two_dimensional_partitions,
    # automation_condition=dg.AutomationCondition.eager(),
)
def himawari8_virtualizarr_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    it: dt.datetime = context.partition_time_window.start
    # partition_key looks like "2024-01-01|us"
    keys_by_dimension: dg.MultiPartitionKey = context.partition_key.keys_by_dimension

    band = keys_by_dimension["band"]

    fs = s3fs.S3FileSystem(anon=True)
    # Get the day of year
    it: pd.Timestamp = pd.Timestamp(it)
    satellite = "himawari8"
    for tile in range(1, 89):
        files = sorted(
            fs.glob(
                f"noaa-{satellite}/AHI-L2-FLDK-ISatSS/{it.year}/{str(it.day).zfill(2)}/{str(it.hour).zfill(2)}*0/OR_HFD-*-M1C{str(band).zfill(2)}-T0{str(tile).zfill(2)}*.nc"
            )
        )
        create_and_write_virtualizarr(files, satellite, band, tile)

    return dg.MaterializeResult(
        metadata={"date": it, "band": band},
    )


@dg.asset(
    name="himawari9-virtualizarr",
    description="Create Virtualizarr reference of Himawari-9 satellite data from NOAA on AWS",
    tags={
        "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
        "dagster/priority": "1",
        "dagster/concurrency_key": "icechunk",
    },
    partitions_def=himawari9_two_dimensional_partitions,
    # automation_condition=dg.AutomationCondition.eager(),
)
def himawari9_virtualizarr_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    it: dt.datetime = context.partition_time_window.start
    # partition_key looks like "2024-01-01|us"
    keys_by_dimension: dg.MultiPartitionKey = context.partition_key.keys_by_dimension

    band = keys_by_dimension["band"]

    fs = s3fs.S3FileSystem(anon=True)
    # Get the day of year
    it: pd.Timestamp = pd.Timestamp(it)
    satellite = "himawari9"
    for tile in range(1, 89):
        files = sorted(
            fs.glob(
                f"noaa-{satellite}/AHI-L2-FLDK-ISatSS/{it.year}/{str(it.day).zfill(2)}/{str(it.hour).zfill(2)}*0/OR_HFD-*-M1C{str(band).zfill(2)}-T0{str(tile).zfill(2)}*.nc"
            )
        )
        create_and_write_virtualizarr(files, satellite, band, tile)

    return dg.MaterializeResult(
        metadata={"date": it, "band": band},
    )


def create_and_write_virtualizarr(files: list[str], satellite: str, band: int, tile: int):
    files = ["s3://" + f for f in files]
    virtual_datasets = [
        open_virtual_dataset(
            filepath,
            loadable_variables=["y", "x"],
            reader_options={"storage_options": {"anon": True}},
        )
        for filepath in files
    ]

    # Add time and attrs as part of the object
    for i, vd in enumerate(virtual_datasets):
        vd["time"] = pd.to_datetime(vd.attrs["start_date_time"], format="%Y%j%H%M%S")
        ds = ds.set_coords("time")
        for key, value in vd.attrs.items():
            if "satellite" in key:
                vd[key] = value
        vd["band"] = band
        vd = vd.set_coords("band")
        virtual_datasets[i] = vd

    vd = xr.concat(
        virtual_datasets, dim="time", coords="minimal", compat="override", combine_attrs="override"
    )

    store_location = os.path.join(
        f"{ARCHIVE_FOLDER}", f"{satellite}_band{str(band).zfill(2)}_tile{str(tile).zfill(2)}"
    )

    # Try loading existing store
    if os.path.exists(store_location):
        repo = icechunk.Repository.open(icechunk.local_filesystem_storage(store_location))
        session = repo.writable_session("main")
        # write the virtual dataset to the session with the IcechunkStore
        vd.virtualize.to_icechunk(session.store, append_dim="t")
    else:
        storage = icechunk.local_filesystem_storage(
            path=store_location,
        )
        config = icechunk.RepositoryConfig.default()
        config.set_virtual_chunk_container(
            icechunk.VirtualChunkContainer("s3", "s3://", icechunk.s3_store(region="us-east-1"))
        )
        credentials = icechunk.containers_credentials(s3=icechunk.s3_credentials(anonymous=True))
        repo = icechunk.Repository.create(storage, config, credentials)
        session = repo.writable_session("main")
        vd.virtualize.to_icechunk(session.store)
