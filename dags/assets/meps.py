"""Dagster assets for the met.no Nordic NWP and radar stores.

One asset per variant in :data:`planetary_datasets.providers.meps.PROVIDERS`. Each asset
materialises a single partition by handing the timestamp to the provider, which fetches,
processes and appends it to the icechunk store.

This module is picked up by the code location's asset discovery; it deliberately does not
register anything in ``dags/definitions.py`` itself.

The Andoya subsets write to a private bucket and are only runnable when
``MEPS_ANDOYA_BUCKET`` is set (or ``ICECHUNK_LOCAL_PATH``, which sends every store to the
local filesystem). Their assets are always defined; they fail with a clear message if the
bucket is not configured.
"""

import datetime as dt
from typing import Type

import dagster as dg
import pandas as pd
from dagster import AssetExecutionContext

from planetary_datasets.providers.meps import (
    AndoyaMEPSPostProcessedProvider,
    AndoyaMEPSProvider,
    AndoyaReflectivityProvider,
    MEPSAnalysisProvider,
    MEPSDetProvider,
    MEPSPostProcessedProvider,
    MetNoProvider,
    NordicReflectivityProvider,
)

#: The radar mosaic and the post-processed analysis are published one day at a time.
daily_partitions = dg.DailyPartitionsDefinition(start_date="2020-01-01", end_offset=-1)

#: MEPS cycles run every three hours. The key format matches Dagster's own hourly
#: partitions; ``|`` is reserved as the multi-partition delimiter and is avoided here.
cycle_partitions = dg.TimeWindowPartitionsDefinition(
    start=dt.datetime(2024, 1, 1),
    cron_schedule="0 0/3 * * *",
    fmt="%Y-%m-%d-%H:%M",
    end_offset=-1,
)


def _build_asset(
    provider_cls: Type[MetNoProvider],
    partitions_def: dg.PartitionsDefinition,
    description: str,
) -> dg.AssetsDefinition:
    """Build the single-partition asset for one provider."""

    @dg.asset(
        name=provider_cls.name,
        description=description,
        partitions_def=partitions_def,
        compute_kind="python",
        metadata={
            "store": dg.MetadataValue.text(provider_cls.store_prefix),
            "source": dg.MetadataValue.url("https://thredds.met.no/thredds/catalog.html"),
        },
        tags={"dagster/concurrency_key": "met-no-thredds"},
    )
    def _asset(context: AssetExecutionContext) -> dg.MaterializeResult:
        it = pd.Timestamp(context.partition_time_window.start).tz_localize(None)
        provider = provider_cls()
        written = provider.run_partition(it)
        context.log.info(f"{provider.name}: {it} -> {provider.store_path} (written={written})")
        return dg.MaterializeResult(
            metadata={
                "written": dg.MetadataValue.bool(written),
                "partition": dg.MetadataValue.text(str(it)),
                "store_path": dg.MetadataValue.path(provider.store_path),
            }
        )

    return _asset


nordic_reflectivity = _build_asset(
    NordicReflectivityProvider,
    daily_partitions,
    "Daily Nordic radar reflectivity mosaic from met.no.",
)
andoya_reflectivity = _build_asset(
    AndoyaReflectivityProvider,
    daily_partitions,
    "Nordic radar reflectivity cropped to the Andoya box.",
)
meps_analysis = _build_asset(
    MEPSAnalysisProvider,
    cycle_partitions,
    "MEPS 2.5 km control member analysis, first three lead times of each cycle.",
)
andoya_meps = _build_asset(
    AndoyaMEPSProvider,
    cycle_partitions,
    "MEPS analysis cropped to the Andoya box.",
)
meps_postprocessed = _build_asset(
    MEPSPostProcessedProvider,
    daily_partitions,
    "Post-processed 1 km Nordic analysis, one day of hourly files per partition.",
)
andoya_meps_postprocessed = _build_asset(
    AndoyaMEPSPostProcessedProvider,
    daily_partitions,
    "Post-processed Nordic analysis cropped to the Andoya box.",
)
meps_det = _build_asset(
    MEPSDetProvider,
    cycle_partitions,
    "MEPS 2.5 km deterministic model-level fields, read over OPeNDAP.",
)

meps_assets = [
    nordic_reflectivity,
    andoya_reflectivity,
    meps_analysis,
    andoya_meps,
    meps_postprocessed,
    andoya_meps_postprocessed,
    meps_det,
]

__all__ = [
    "andoya_meps",
    "andoya_meps_postprocessed",
    "andoya_reflectivity",
    "meps_analysis",
    "meps_assets",
    "meps_det",
    "meps_postprocessed",
    "nordic_reflectivity",
]
