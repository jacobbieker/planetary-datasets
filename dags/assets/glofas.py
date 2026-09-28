"""Dagster assets for the GloFAS river discharge products.

One asset per CDS product, each backed by the matching provider in
:mod:`planetary_datasets.providers.glofas`. The assets hold no download or processing logic
of their own: they map a partition key to a timestamp, call ``run_partition`` and report
what happened.
"""

import dagster as dg
import pandas as pd
from dagster import AssetExecutionContext

from planetary_datasets.providers.glofas import (
    GloFASForecastProvider,
    GloFASHistoricalProvider,
    GloFASProvider,
    GloFASReforecastProvider,
)

forecast_partitions = dg.DailyPartitionsDefinition(
    start_date=GloFASForecastProvider.start_date.strftime("%Y-%m-%d"),
    end_offset=0,
)

reforecast_partitions = dg.MonthlyPartitionsDefinition(
    start_date=GloFASReforecastProvider.start_date.strftime("%Y-%m-%d"),
    # Dagster's end_date is exclusive, so the last hindcast month needs one more month on
    # top of it or it would never be partitioned.
    end_date=(GloFASReforecastProvider.end_date + pd.DateOffset(months=1)).strftime("%Y-%m-%d"),
)

historical_partitions = dg.MonthlyPartitionsDefinition(
    start_date=GloFASHistoricalProvider.start_date.strftime("%Y-%m-%d"),
    end_offset=-2,
)

COMMON_TAGS = {
    # CDS throttles per user, so every GloFAS request queues behind the same key.
    "dagster/concurrency_key": "copernicus-cds",
    "dagster/max_runtime": str(60 * 60 * 12),
}


def _run(context: AssetExecutionContext, provider: GloFASProvider) -> dg.MaterializeResult:
    """Run one partition and turn the outcome into Dagster metadata."""
    it = pd.Timestamp(context.partition_time_window.start).tz_localize(None)
    context.log.info(f"{provider.name}: running {it} into {provider.store_path}")
    written = provider.run_partition(it)
    return dg.MaterializeResult(
        metadata={
            "partition": dg.MetadataValue.text(str(it)),
            "store": dg.MetadataValue.text(provider.store_path),
            "archive_dir": dg.MetadataValue.path(str(provider.archive_dir)),
            "written": dg.MetadataValue.bool(written),
            "cds_dataset": dg.MetadataValue.text(provider.dataset),
        },
    )


@dg.asset(
    name="glofas_forecast",
    description=GloFASForecastProvider.__doc__,
    key_prefix=["river"],
    partitions_def=forecast_partitions,
    compute_kind="python",
    metadata={"source": dg.MetadataValue.text("copernicus-cds")},
    tags=COMMON_TAGS,
)
def glofas_forecast_asset(context: AssetExecutionContext) -> dg.MaterializeResult:
    """Operational GloFAS ensemble forecast for one initialisation day."""
    return _run(context, GloFASForecastProvider())


@dg.asset(
    name="glofas_reforecast",
    description=GloFASReforecastProvider.__doc__,
    key_prefix=["river"],
    partitions_def=reforecast_partitions,
    compute_kind="python",
    metadata={"source": dg.MetadataValue.text("copernicus-cds")},
    tags=COMMON_TAGS,
)
def glofas_reforecast_asset(context: AssetExecutionContext) -> dg.MaterializeResult:
    """GloFAS version 4.0 reforecast for one hindcast month."""
    return _run(context, GloFASReforecastProvider())


@dg.asset(
    name="glofas_historical",
    description=GloFASHistoricalProvider.__doc__,
    key_prefix=["river"],
    partitions_def=historical_partitions,
    compute_kind="python",
    metadata={"source": dg.MetadataValue.text("copernicus-cds")},
    tags=COMMON_TAGS,
)
def glofas_historical_asset(context: AssetExecutionContext) -> dg.MaterializeResult:
    """Consolidated GloFAS reanalysis for one month."""
    return _run(context, GloFASHistoricalProvider())


assets = [glofas_forecast_asset, glofas_reforecast_asset, glofas_historical_asset]
