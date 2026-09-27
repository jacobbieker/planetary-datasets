"""Dagster assets for the SILAM dust and surface aerosol forecasts from FMI.

Both assets are thin wrappers: the provider owns the download, the reshaping and the
append-or-create write, and the asset only maps a Dagster partition onto
:meth:`~planetary_datasets.base.BaseProvider.run_partition`.
"""

import dagster as dg
import pandas as pd

from planetary_datasets.providers.silam import SILAMAerosolProvider, SILAMDustProvider


def _partition_timestamp(context: dg.AssetExecutionContext) -> pd.Timestamp:
    """The partition start as a naive UTC timestamp.

    Dagster hands out timezone-aware datetimes; the stores hold naive UTC, and comparing
    the two is what decides whether a partition has already been written.
    """
    stamp = pd.Timestamp(context.partition_time_window.start)
    if stamp.tzinfo is not None:
        stamp = stamp.tz_convert("UTC").tz_localize(None)
    return stamp


dust_partitions_def: dg.TimeWindowPartitionsDefinition = dg.DailyPartitionsDefinition(
    start_date="2024-11-14",
    end_offset=-1,
)

aerosol_partitions_def: dg.TimeWindowPartitionsDefinition = dg.DailyPartitionsDefinition(
    start_date="2025-04-26",
    end_offset=-1,
)


@dg.asset(
    name="silam_dust",
    description="SILAM global dust forecast from the FMI THREDDS server",
    metadata={
        "area": dg.MetadataValue.text("global"),
        "source": dg.MetadataValue.text("silam-dust"),
        "expected_runtime": dg.MetadataValue.text("6 hours"),
    },
    tags={
        "dagster/max_runtime": str(60 * 60 * 10),
        "dagster/priority": "1",
        "dagster/concurrency_key": "download",
    },
    partitions_def=dust_partitions_def,
    retry_policy=dg.RetryPolicy(max_retries=3, delay=60),
    automation_condition=dg.AutomationCondition.eager(),
)
def silam_dust_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Write one SILAM dust forecast run to icechunk."""
    provider = SILAMDustProvider()
    written = provider.run_partition(_partition_timestamp(context))
    return dg.MaterializeResult(
        metadata={"store": provider.store_path, "written": written},
    )


@dg.asset(
    name="silam_aerosol",
    description="SILAM global surface aerosol forecast from the FMI open data S3 bucket",
    metadata={
        "area": dg.MetadataValue.text("global"),
        "source": dg.MetadataValue.text("silam-aerosol"),
        "expected_runtime": dg.MetadataValue.text("1 hour"),
    },
    tags={
        "dagster/max_runtime": str(60 * 60 * 6),
        "dagster/priority": "1",
        "dagster/concurrency_key": "download",
    },
    partitions_def=aerosol_partitions_def,
    retry_policy=dg.RetryPolicy(max_retries=3, delay=60),
    automation_condition=dg.AutomationCondition.eager(),
)
def silam_aerosol_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Write one SILAM surface aerosol forecast run to icechunk."""
    provider = SILAMAerosolProvider()
    written = provider.run_partition(_partition_timestamp(context))
    return dg.MaterializeResult(
        metadata={"store": provider.store_path, "written": written},
    )


silam_assets = [silam_dust_asset, silam_aerosol_asset]
