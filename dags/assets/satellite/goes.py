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

ARCHIVE_FOLDER = "/ext_data/goes/"

goes19_partitions_def: dg.TimeWindowPartitionsDefinition = dg.HourlyPartitionsDefinition(
    start_date="2024-10-10-00:00",
    end_offset=-2,
) # Day 284 of 2024 is first day of data, hour 19:00

goes18_partitions_def: dg.TimeWindowPartitionsDefinition = dg.HourlyPartitionsDefinition(
    start_date="2022-07-28-00:00",
    end_offset=-2,
) # Day 209 of 2022 00:00 is first day of data

goes17_partitions_def: dg.TimeWindowPartitionsDefinition = dg.HourlyPartitionsDefinition(
    start_date="2024-08-28-00:00",
    end_date="2024-01-15-10:00",
) # Day 240 of 2018 00:00 until 2023 15 10:00

goes16_partitions_def: dg.TimeWindowPartitionsDefinition = dg.HourlyPartitionsDefinition(
    start_date="2017-02-28-00:00",
    end_offset=-2,
) # Day 059 of 2017 00:00 is first day of data, '-Reproc' data is available from 2018 004 until 2024 363 22:00

goes16_reproc_partitions_def: dg.TimeWindowPartitionsDefinition = dg.HourlyPartitionsDefinition(
    start_date="2018-01-04-00:00",
    end_date="2024-12-28-22:00",
) # Day 059 of 2017 00:00 is first day of data, '-Reproc' data is available from 2018 004 until 2024 363 22:00

# Add a different partition, by band
bands = [str(i) for i in range(1,17)]
band_partition: dg.StaticPartitionsDefinition = dg.StaticPartitionsDefinition(bands)

goes16_two_dimensional_partitions = dg.MultiPartitionsDefinition(
    {"date": goes16_partitions_def, "band": band_partition}
)
goes17_two_dimensional_partitions = dg.MultiPartitionsDefinition(
    {"date": goes17_partitions_def, "band": band_partition}
)
goes18_two_dimensional_partitions = dg.MultiPartitionsDefinition(
    {"date": goes18_partitions_def, "band": band_partition}
)
goes19_two_dimensional_partitions = dg.MultiPartitionsDefinition(
    {"date": goes19_partitions_def, "band": band_partition}
)
goes16_reproc_two_dimensional_partitions = dg.MultiPartitionsDefinition(
    {"date": goes16_reproc_partitions_def, "band": band_partition}
)

@dg.asset(name="goes16-virtualizarr", description="Create Virtualizarr reference of GOES-16 satellite data from NOAA on AWS",
          tags={
              "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
              "dagster/priority": "1",
              "dagster/concurrency_key": "goes-icechunk",
          },
          partitions_def=goes16_two_dimensional_partitions,
automation_condition=dg.AutomationCondition.eager(),
          )
def goes16_virtualizarr_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    it: dt.datetime = context.partition_time_window.start
    # partition_key looks like "2024-01-01|us"
    keys_by_dimension: dg.MultiPartitionKey = context.partition_key.keys_by_dimension

    band = keys_by_dimension["band"]

    fs = s3fs.S3FileSystem(anon=True)
    # Get the day of year
    it: pd.Timestamp = pd.Timestamp(it)
    satellite = "goes16"

    files = sorted(
        fs.glob(f"noaa-{satellite}/ABI-L1b-RadF/{it.year}/{str(it.dayofyear).zfill(3)}/{str(it.hour).zfill(2)}/OR_ABI-L1b-RadF-M6C{str(band).zfill(2)}_*.nc"))
    create_and_write_virtualizarr(files, satellite, band)

    return dg.MaterializeResult(
        metadata={"date": it,
                  "band": band},
    )

@dg.asset(name="goes17-virtualizarr", description="Create Virtualizarr reference of GOES-17 satellite data from NOAA on AWS",
          tags={
              "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
              "dagster/priority": "1",
              "dagster/concurrency_key": "goes-icechunk",
          },
          partitions_def=goes17_two_dimensional_partitions,
automation_condition=dg.AutomationCondition.eager(),
          )
def goes17_virtualizarr_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    it: dt.datetime = context.partition_time_window.start
    # partition_key looks like "2024-01-01|us"
    keys_by_dimension: dg.MultiPartitionKey = context.partition_key.keys_by_dimension

    band = keys_by_dimension["band"]

    fs = s3fs.S3FileSystem(anon=True)
    # Get the day of year
    it: pd.Timestamp = pd.Timestamp(it)
    satellite = "goes17"

    files = sorted(
        fs.glob(f"noaa-{satellite}/ABI-L1b-RadF/{it.year}/{str(it.dayofyear).zfill(3)}/{str(it.hour).zfill(2)}/OR_ABI-L1b-RadF-M6C{str(band).zfill(2)}_*.nc"))
    create_and_write_virtualizarr(files, satellite, band)

    return dg.MaterializeResult(
        metadata={"date": it,
                  "band": band},
    )

@dg.asset(name="goes18-virtualizarr", description="Create Virtualizarr reference of GOES-18 satellite data from NOAA on AWS",
          tags={
              "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
              "dagster/priority": "1",
              "dagster/concurrency_key": "goes-icechunk",
          },
          partitions_def=goes18_two_dimensional_partitions,
automation_condition=dg.AutomationCondition.eager(),
          )
def goes18_virtualizarr_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    it: dt.datetime = context.partition_time_window.start
    # partition_key looks like "2024-01-01|us"
    keys_by_dimension: dg.MultiPartitionKey = context.partition_key.keys_by_dimension

    band = keys_by_dimension["band"]

    fs = s3fs.S3FileSystem(anon=True)
    # Get the day of year
    it: pd.Timestamp = pd.Timestamp(it)
    satellite = "goes18"

    files = sorted(
        fs.glob(f"noaa-{satellite}/ABI-L1b-RadF/{it.year}/{str(it.dayofyear).zfill(3)}/{str(it.hour).zfill(2)}/OR_ABI-L1b-RadF-M6C{str(band).zfill(2)}_*.nc"))
    create_and_write_virtualizarr(files, satellite, band)

    return dg.MaterializeResult(
        metadata={"date": it,
                  "band": band},
    )

@dg.asset(name="goes19-virtualizarr", description="Create Virtualizarr reference of GOES-19 satellite data from NOAA on AWS",
          tags={
              "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
              "dagster/priority": "1",
              "dagster/concurrency_key": "goes-icechunk",
          },
          partitions_def=goes18_two_dimensional_partitions,
automation_condition=dg.AutomationCondition.eager(),
          )
def goes19_virtualizarr_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    it: dt.datetime = context.partition_time_window.start
    # partition_key looks like "2024-01-01|us"
    keys_by_dimension: dg.MultiPartitionKey = context.partition_key.keys_by_dimension

    band = keys_by_dimension["band"]

    fs = s3fs.S3FileSystem(anon=True)
    # Get the day of year
    it: pd.Timestamp = pd.Timestamp(it)
    satellite = "goes19"

    files = sorted(
        fs.glob(f"noaa-{satellite}/ABI-L1b-RadF/{it.year}/{str(it.dayofyear).zfill(3)}/{str(it.hour).zfill(2)}/OR_ABI-L1b-RadF-M6C{str(band).zfill(2)}_*.nc"))
    create_and_write_virtualizarr(files, satellite, band)

    return dg.MaterializeResult(
        metadata={"date": it,
                  "band": band},
    )

@dg.asset(name="goes16-reproc-virtualizarr", description="Create Virtualizarr reference of GOES-16 Reproc satellite data from NOAA on AWS",
          tags={
              "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
              "dagster/priority": "1",
              "dagster/concurrency_key": "goes-icechunk",
          },
          partitions_def=goes16_reproc_two_dimensional_partitions,
automation_condition=dg.AutomationCondition.eager(),
          )
def goes16_virtualizarr_reproc_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    it: dt.datetime = context.partition_time_window.start
    # partition_key looks like "2024-01-01|us"
    keys_by_dimension: dg.MultiPartitionKey = context.partition_key.keys_by_dimension

    band = keys_by_dimension["band"]

    fs = s3fs.S3FileSystem(anon=True)
    # Get the day of year
    it: pd.Timestamp = pd.Timestamp(it)
    satellite = "goes16"

    files = sorted(
        fs.glob(f"noaa-{satellite}/ABI-L1b-RadF/{it.year}/{str(it.dayofyear).zfill(3)}/{str(it.hour).zfill(2)}/OR_ABI-L1b-RadF-M6C{str(band).zfill(2)}_*.nc"))
    create_and_write_virtualizarr(files, satellite+"_reproc", band)

    return dg.MaterializeResult(
        metadata={"date": it,
                  "band": band},
    )

def create_and_write_virtualizarr(
        files: list[str],
        satellite: str,
        band: int,

):
    files = ["s3://" + f for f in files]
    virtual_datasets = [
        open_virtual_dataset(filepath, loadable_variables=["t", "y", "x", "band", "band_id"],
                             reader_options={'storage_options': {"anon": True}}) for filepath in files
    ]

    vd = xr.concat(
        virtual_datasets,
        dim='t',
        coords='minimal',
        compat='override',
        combine_attrs='override'
    )

    store_location = os.path.join(f'{ARCHIVE_FOLDER}', f'{satellite}_band{str(band).zfill(2)}')

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
        config.set_virtual_chunk_container(icechunk.VirtualChunkContainer("s3", "s3://", icechunk.s3_store(region="us-east-1")))
        credentials = icechunk.containers_credentials(s3=icechunk.s3_credentials(anonymous=True))
        repo = icechunk.Repository.create(storage, config, credentials)
        session = repo.writable_session("main")
        vd.virtualize.to_icechunk(session.store)