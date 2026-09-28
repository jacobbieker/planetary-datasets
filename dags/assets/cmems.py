"""Dagster assets for the Copernicus Marine Service (CMEMS) ocean datasets.

One daily-partitioned asset per regional wave product, plus the two global stores and a
mirror job for the native global analysis/forecast files. Every asset is a thin wrapper:
the work lives in :mod:`planetary_datasets.providers.cmems`, so the same code runs from
Dagster, a cron entry or a shell.
"""

import dagster as dg
import pandas as pd
from dagster import AssetExecutionContext

from planetary_datasets.providers.cmems import (
    ANALYSIS_FORECAST_DATASETS,
    WAVE_REGIONS,
    CMEMSGlobalOceanForecastProvider,
    CMEMSGlobalWaveReanalysisProvider,
    CMEMSWaveProvider,
    WaveRegion,
    download_datasets,
    resolve_dataset_id,
)

COMMON_TAGS = {
    "dagster/max_runtime": str(60 * 60 * 3),
    "dagster/concurrency_key": "copernicus-marine",
}


def _partition_timestamp(context: AssetExecutionContext) -> pd.Timestamp:
    """The partition key as a midnight-UTC, timezone-naive timestamp.

    Partition keys are plain dates, but a timezone-aware key would otherwise compare
    unequal to the naive timestamps stored along ``time``, so any offset is normalised to
    UTC and dropped.
    """
    ts = pd.Timestamp(context.partition_key)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts.floor("D")


def _run(context: AssetExecutionContext, provider) -> dg.MaterializeResult:
    """Run one partition of a provider and report what happened to Dagster."""
    it = _partition_timestamp(context)
    written = provider.run_partition(it)
    if not written:
        context.log.info(f"{provider.name}: nothing written for {it:%Y-%m-%d}")
    return dg.MaterializeResult(
        metadata={
            "partition": dg.MetadataValue.text(f"{it:%Y-%m-%d}"),
            "store": dg.MetadataValue.text(provider.store_path),
            "written": dg.MetadataValue.bool(bool(written)),
        }
    )


def _wave_asset(region: WaveRegion) -> dg.AssetsDefinition:
    """Build the daily asset for one regional wave product."""
    partitions_def = dg.DailyPartitionsDefinition(start_date=region.start_date)

    @dg.asset(
        name=region.name,
        description=f"{region.description} ({region.product_id}).",
        key_prefix=["ocean"],
        partitions_def=partitions_def,
        compute_kind="python",
        metadata={
            "source": dg.MetadataValue.text("copernicus-marine"),
            "product_id": dg.MetadataValue.text(region.product_id),
            "dataset_id": dg.MetadataValue.text(region.dataset_id),
            "store_prefix": dg.MetadataValue.text(region.store_prefix),
            "format": dg.MetadataValue.text("icechunk"),
        },
        tags=COMMON_TAGS,
        automation_condition=dg.AutomationCondition.on_cron(
            partitions_def.get_cron_schedule(hour_of_day=6),
        ),
    )
    def _asset(context: AssetExecutionContext) -> dg.MaterializeResult:
        return _run(context, CMEMSWaveProvider(region))

    return _asset


cmems_wave_assets: "list[dg.AssetsDefinition]" = [
    _wave_asset(region) for region in WAVE_REGIONS.values()
]

_reanalysis_partitions = dg.DailyPartitionsDefinition(start_date="1980-01-01")


@dg.asset(
    name=CMEMSGlobalWaveReanalysisProvider.name,
    description="Global wave reanalysis, 0.2 degree 3 hourly (GLOBAL_MULTIYEAR_WAV_001_032).",
    key_prefix=["ocean"],
    partitions_def=_reanalysis_partitions,
    compute_kind="python",
    metadata={
        "source": dg.MetadataValue.text("copernicus-marine"),
        "product_id": dg.MetadataValue.text("GLOBAL_MULTIYEAR_WAV_001_032"),
        "dataset_id": dg.MetadataValue.text(CMEMSGlobalWaveReanalysisProvider.dataset_id),
        "store_prefix": dg.MetadataValue.text(CMEMSGlobalWaveReanalysisProvider.store_prefix),
        "format": dg.MetadataValue.text("icechunk"),
    },
    tags=COMMON_TAGS,
)
def cmems_global_wave_reanalysis(context: AssetExecutionContext) -> dg.MaterializeResult:
    """Append one day of the global wave reanalysis to its Icechunk store."""
    return _run(context, CMEMSGlobalWaveReanalysisProvider())


_forecast_partitions = dg.DailyPartitionsDefinition(start_date="2022-01-01")


@dg.asset(
    name=CMEMSGlobalOceanForecastProvider.name,
    description=(
        "Merged global ocean analysis and forecast: hourly means, surface currents and "
        "sea level (GLOBAL_ANALYSISFORECAST_PHY_001_024)."
    ),
    key_prefix=["ocean"],
    partitions_def=_forecast_partitions,
    compute_kind="python",
    deps=[dg.AssetKey(["ocean", "cmems_global_ocean_mirror"])],
    metadata={
        "source": dg.MetadataValue.text("copernicus-marine"),
        "product_id": dg.MetadataValue.text("GLOBAL_ANALYSISFORECAST_PHY_001_024"),
        "store_prefix": dg.MetadataValue.text(CMEMSGlobalOceanForecastProvider.store_prefix),
        "format": dg.MetadataValue.text("icechunk"),
    },
    tags=COMMON_TAGS,
    automation_condition=dg.AutomationCondition.on_cron(
        _forecast_partitions.get_cron_schedule(hour_of_day=8),
    ),
)
def cmems_global_ocean_forecast(context: AssetExecutionContext) -> dg.MaterializeResult:
    """Merge one day of the three global analysis/forecast products into Icechunk."""
    return _run(context, CMEMSGlobalOceanForecastProvider())


@dg.asset(
    name="cmems_global_ocean_mirror",
    description=(
        "Local mirror of the native GLOBAL_ANALYSISFORECAST_PHY_001_024 NetCDF files, "
        "the input to the merged global ocean forecast store."
    ),
    key_prefix=["ocean"],
    partitions_def=_forecast_partitions,
    compute_kind="python",
    metadata={
        "source": dg.MetadataValue.text("copernicus-marine"),
        "product_id": dg.MetadataValue.text("GLOBAL_ANALYSISFORECAST_PHY_001_024"),
        "format": dg.MetadataValue.text("netcdf"),
    },
    tags=COMMON_TAGS,
    automation_condition=dg.AutomationCondition.on_cron(
        _forecast_partitions.get_cron_schedule(hour_of_day=5),
    ),
)
def cmems_global_ocean_mirror(context: AssetExecutionContext) -> dg.MaterializeResult:
    """Download one day of the native global analysis/forecast files."""
    it = _partition_timestamp(context)
    results = download_datasets(
        ANALYSIS_FORECAST_DATASETS,
        file_filter=f"*{it:%Y%m%d}*",
    )
    counts = {dataset_id: len(paths) for dataset_id, paths in results.items()}
    missing = sorted(
        resolve_dataset_id(name)
        for name in ANALYSIS_FORECAST_DATASETS
        if not results.get(resolve_dataset_id(name))
    )
    if missing:
        context.log.warning(f"no files downloaded for {missing} on {it:%Y-%m-%d}")
    return dg.MaterializeResult(
        metadata={
            "partition": dg.MetadataValue.text(f"{it:%Y-%m-%d}"),
            "files_per_dataset": dg.MetadataValue.json(counts),
            "total_files": dg.MetadataValue.int(sum(counts.values())),
        }
    )


all_assets: "list[dg.AssetsDefinition]" = [
    *cmems_wave_assets,
    cmems_global_wave_reanalysis,
    cmems_global_ocean_mirror,
    cmems_global_ocean_forecast,
]
