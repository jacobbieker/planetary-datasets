"""Dagster assets for CAMS atmospheric composition.

CAMS is the Copernicus Atmosphere Monitoring Service. Data is retrieved from the
Copernicus Atmosphere Data Store (https://ads.atmosphere.copernicus.eu) with the cdsapi
client and written to Icechunk.

All of the work lives in :mod:`planetary_datasets.providers.cams`; these assets only map
a Dagster partition onto a provider run so the same code can be driven from a shell.
"""

# NOTE: no ``from __future__ import annotations`` here. Dagster inspects the *string* of
# the ``context`` annotation and rejects the dotted ``dg.AssetExecutionContext`` form once
# annotations are lazily evaluated.

import datetime as dt

import dagster as dg
import pandas as pd

from planetary_datasets.providers.cams import (
    CAMSEuropeAirQualityProvider,
    CAMSGlobalAODProvider,
    CAMSGlobalCompositionProvider,
)

COMPOSITION_PARTITIONS = dg.HourlyPartitionsDefinition(
    start_date="2016-01-01-00:00",
    end_offset=-1,
)

GLOBAL_AOD_PARTITIONS = dg.WeeklyPartitionsDefinition(
    start_date="2015-01-01",
    end_offset=-2,
)

EUROPE_PARTITIONS = dg.WeeklyPartitionsDefinition(
    start_date="2020-02-08",
    end_offset=-2,
)

_COMMON_TAGS = {
    "dagster/priority": "1",
    "dagster/concurrency_key": "copernicus-ads",
}


def _run(provider, context: dg.AssetExecutionContext) -> dg.Output[bool]:
    """Run one partition of a CAMS provider and report what happened."""
    it = pd.Timestamp(context.partition_time_window.start).tz_localize(None)
    started = dt.datetime.now(tz=dt.timezone.utc)
    written = provider.run_partition(it)
    elapsed = dt.datetime.now(tz=dt.timezone.utc) - started
    return dg.Output(
        value=written,
        metadata={
            "timestamp": dg.MetadataValue.text(str(it)),
            "written": dg.MetadataValue.bool(written),
            "store": dg.MetadataValue.text(provider.store_path),
            "elapsed_hours": dg.MetadataValue.float(elapsed / dt.timedelta(hours=1)),
        },
    )


@dg.asset(
    name="cams_global_composition",
    description=(
        "Global CAMS atmospheric composition forecast on pressure levels, one valid hour "
        "per partition, served by the freshest of the 00Z and 12Z runs."
    ),
    key_prefix=["air"],
    compute_kind="python",
    partitions_def=COMPOSITION_PARTITIONS,
    metadata={
        "source": dg.MetadataValue.text("copernicus-ads"),
        "model": dg.MetadataValue.text("cams"),
        "area": dg.MetadataValue.text("global"),
        "format": dg.MetadataValue.text("icechunk"),
    },
    automation_condition=dg.AutomationCondition.on_cron(
        cron_schedule=COMPOSITION_PARTITIONS.get_cron_schedule(),
    ),
    tags={**_COMMON_TAGS, "dagster/max_runtime": str(60 * 60 * 6)},
)
def cams_global_composition_asset(context: dg.AssetExecutionContext) -> dg.Output[bool]:
    """Ingest one valid hour of the global CAMS composition forecast."""
    return _run(CAMSGlobalCompositionProvider(), context)


@dg.asset(
    name="cams_global_aod",
    description=(
        "Global CAMS aerosol optical depth and radiation forecasts out to 120 hours, "
        "a week per partition."
    ),
    key_prefix=["air"],
    compute_kind="python",
    partitions_def=GLOBAL_AOD_PARTITIONS,
    metadata={
        "source": dg.MetadataValue.text("copernicus-ads"),
        "model": dg.MetadataValue.text("cams"),
        "area": dg.MetadataValue.text("global"),
        "format": dg.MetadataValue.text("icechunk"),
        "expected_runtime": dg.MetadataValue.text("6 hours"),
    },
    automation_condition=dg.AutomationCondition.on_cron(
        cron_schedule=GLOBAL_AOD_PARTITIONS.get_cron_schedule(hour_of_day=7),
    ),
    tags={**_COMMON_TAGS, "dagster/max_runtime": str(60 * 60 * 24 * 4)},
)
def cams_global_aod_asset(context: dg.AssetExecutionContext) -> dg.Output[bool]:
    """Ingest one week of global CAMS aerosol optical depth and radiation."""
    return _run(CAMSGlobalAODProvider(), context)


@dg.asset(
    name="cams_europe_air_quality",
    description=(
        "CAMS European air quality ensemble forecast out to 96 hours, a week per "
        "partition."
    ),
    key_prefix=["air"],
    compute_kind="python",
    partitions_def=EUROPE_PARTITIONS,
    metadata={
        "source": dg.MetadataValue.text("copernicus-ads"),
        "model": dg.MetadataValue.text("cams"),
        "area": dg.MetadataValue.text("europe"),
        "format": dg.MetadataValue.text("icechunk"),
        "expected_runtime": dg.MetadataValue.text("6 hours"),
    },
    automation_condition=dg.AutomationCondition.on_cron(
        cron_schedule=EUROPE_PARTITIONS.get_cron_schedule(hour_of_day=7),
    ),
    tags={**_COMMON_TAGS, "dagster/max_runtime": str(60 * 60 * 24 * 4)},
)
def cams_europe_air_quality_asset(context: dg.AssetExecutionContext) -> dg.Output[bool]:
    """Ingest one week of the CAMS European air quality ensemble forecast."""
    return _run(CAMSEuropeAirQualityProvider(), context)


cams_assets = [
    cams_global_composition_asset,
    cams_global_aod_asset,
    cams_europe_air_quality_asset,
]
