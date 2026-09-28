"""Dagster assets for the NOAA MRMS radar mosaics.

One asset per store, each partitioned by hour so a partition maps exactly onto one
:meth:`~planetary_datasets.base.BaseProvider.run_partition` call:

``mrms_conus``
    CONUS ``PrecipFlag`` + ``PrecipRate`` into ``bkr/mrms/mrms.icechunk``.
``mrms_preciprate``
    CONUS ``PrecipRate`` only into ``bkr/mrms/mrms_preciprate.icechunk``.
``mrms_region``
    Alaska, Caribbean, Guam and Hawaii into ``bkr/mrms/mrms_<REGION>.icechunk``.
"""

# NB: no `from __future__ import annotations` here. Dagster inspects the raw
# `context` annotation on an asset function and rejects it once PEP 563 turns it
# into a string.
import datetime as dt

import dagster as dg
import pandas as pd
from dagster import AssetExecutionContext

from planetary_datasets.providers.mrms import (
    AWS_ARCHIVE_START,
    NON_CONUS_REGIONS,
    MRMSPrecipRateProvider,
    MRMSProvider,
)

# CONUS reaches back to the Iowa State mtarchive mirror; the regional domains only exist
# in the AWS archive, which starts in October 2020.
CONUS_START = "2015-01-01-00:00"
REGION_START = AWS_ARCHIVE_START.strftime("%Y-%m-%d-%H:%M")

conus_partitions = dg.HourlyPartitionsDefinition(start_date=CONUS_START, end_offset=-1)
region_partitions = dg.MultiPartitionsDefinition(
    {
        "date": dg.HourlyPartitionsDefinition(start_date=REGION_START, end_offset=-1),
        "region": dg.StaticPartitionsDefinition(list(NON_CONUS_REGIONS)),
    }
)

ASSET_TAGS = {
    "dagster/max_runtime": str(60 * 60),
    "dagster/priority": "1",
    "dagster/concurrency_key": "mrms",
}


def _materialize(provider: MRMSProvider, it: dt.datetime) -> dg.MaterializeResult:
    """Run one partition and report what happened as Dagster metadata."""
    timestamp = pd.Timestamp(it)
    written = provider.run_partition(timestamp)
    return dg.MaterializeResult(
        metadata={
            "store": dg.MetadataValue.text(provider.store_path),
            "region": dg.MetadataValue.text(provider.region),
            "products": dg.MetadataValue.text(", ".join(provider.products)),
            "time": dg.MetadataValue.text(timestamp.isoformat()),
            "written": dg.MetadataValue.bool(written),
        }
    )


@dg.asset(
    name="mrms_conus",
    description="MRMS CONUS PrecipFlag and PrecipRate, two-minute fields written hourly",
    partitions_def=conus_partitions,
    tags=ASSET_TAGS,
)
def mrms_conus_asset(context: AssetExecutionContext) -> dg.MaterializeResult:
    """Append one hour of CONUS MRMS to the main store."""
    return _materialize(MRMSProvider(), context.partition_time_window.start)


@dg.asset(
    name="mrms_preciprate",
    description="MRMS CONUS PrecipRate only, for consumers that do not need the type flag",
    partitions_def=conus_partitions,
    tags=ASSET_TAGS,
)
def mrms_preciprate_asset(context: AssetExecutionContext) -> dg.MaterializeResult:
    """Append one hour of CONUS precipitation rate to the rate-only store."""
    return _materialize(MRMSPrecipRateProvider(), context.partition_time_window.start)


@dg.asset(
    name="mrms_region",
    description="MRMS Alaska, Caribbean, Guam and Hawaii mosaics, one store per domain",
    partitions_def=region_partitions,
    tags=ASSET_TAGS,
)
def mrms_region_asset(context: AssetExecutionContext) -> dg.MaterializeResult:
    """Append one hour of one regional MRMS domain to its store."""
    region = context.partition_key.keys_by_dimension["region"]
    return _materialize(MRMSProvider(region=region), context.partition_time_window.start)


mrms_assets = [mrms_conus_asset, mrms_preciprate_asset, mrms_region_asset]
