"""Dagster asset for the NOAA Enterprise blended rain rate (EEPS).

Kept beside the MRMS assets because it is the other precipitation-rate observation store,
but it is a separate dataset with its own provider and its own cadence: one global 5 km
field every ten minutes, read from a local CLASS download.
"""

# NB: no `from __future__ import annotations` here. Dagster inspects the raw
# `context` annotation on an asset function and rejects it once PEP 563 turns it
# into a string.
import dagster as dg
import pandas as pd
from dagster import AssetExecutionContext

from planetary_datasets.providers.eeps import EEPSProvider

eeps_partitions = dg.TimeWindowPartitionsDefinition(
    start=pd.Timestamp("2023-01-01"),
    fmt="%Y-%m-%d-%H:%M",
    cron_schedule="*/10 * * * *",
    end_offset=-1,
)


@dg.asset(
    name="eeps_rainrate",
    description="NOAA Enterprise blended global rain rate (RRQPE) and its quality flag",
    partitions_def=eeps_partitions,
    tags={
        "dagster/max_runtime": str(60 * 30),
        "dagster/priority": "1",
        "dagster/concurrency_key": "eeps",
    },
)
def eeps_rainrate_asset(context: AssetExecutionContext) -> dg.MaterializeResult:
    """Append one EEPS timestep to the store."""
    provider = EEPSProvider()
    timestamp = pd.Timestamp(context.partition_time_window.start)
    written = provider.run_partition(timestamp)
    return dg.MaterializeResult(
        metadata={
            "store": dg.MetadataValue.text(provider.store_path),
            "source_dir": dg.MetadataValue.path(str(provider.source_dir)),
            "time": dg.MetadataValue.text(timestamp.isoformat()),
            "written": dg.MetadataValue.bool(written),
        }
    )


eeps_assets = [eeps_rainrate_asset]
