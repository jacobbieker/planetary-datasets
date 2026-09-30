"""Dagster assets for the UK Met Office atmospheric, ocean and wave stores.

Every asset is the same three lines: resolve the partition timestamp, hand it to a
:class:`~planetary_datasets.base.BaseProvider`, report where it went. All of the work,
including skipping partitions that are already stored, lives in the provider.
"""

from __future__ import annotations

import datetime as dt
from typing import Callable

import dagster as dg
import pandas as pd

from planetary_datasets.base import BaseProvider
from planetary_datasets.providers.metoffice import (
    MetOfficeGlobal10km6Hourly24HourProvider,
    MetOfficeGlobal10kmProvider,
    MetOfficeGlobalOceanHourlyProvider,
    MetOfficeGlobalWaveProvider,
    MetOfficeNWSOceanDepthHourlyProvider,
    MetOfficeNWSOceanSurfaceHourlyProvider,
    MetOfficeNWSWaveProvider,
    MetOfficeUK2kmProvider,
)

#: The atmospheric models publish four runs a day; only those are archived.
six_hourly_partitions = dg.TimeWindowPartitionsDefinition(
    start=dt.datetime(2023, 3, 1),
    cron_schedule="0 0,6,12,18 * * *",
    fmt="%Y-%m-%d-%H:%M",
    end_offset=-1,
)

#: The UK 2 km archive on the public bucket starts almost eighteen months later.
uk_partitions = dg.TimeWindowPartitionsDefinition(
    start=dt.datetime(2024, 9, 27),
    cron_schedule="0 0,6,12,18 * * *",
    fmt="%Y-%m-%d-%H:%M",
    end_offset=-1,
)

#: The global ocean and wave buckets are archived a day at a time.
daily_partitions = dg.DailyPartitionsDefinition(start_date="2025-04-09", end_offset=-1)

#: The North West Shelf buckets only go back to 2025-03.
nws_partitions = dg.DailyPartitionsDefinition(start_date="2025-03-13", end_offset=-1)


def _naive(timestamp) -> pd.Timestamp:
    """Dagster hands out timezone-aware UTC partition starts; the stores are naive UTC."""
    stamp = pd.Timestamp(timestamp)
    return stamp.tz_convert(None) if stamp.tz is not None else stamp


def build_metoffice_asset(
    provider_factory: Callable[[], BaseProvider],
    name: str,
    description: str,
    partitions_def: dg.PartitionsDefinition,
    runtime_hours: int = 4,
):
    """Wrap a provider in a partitioned Dagster asset."""

    @dg.asset(
        name=name,
        description=description,
        partitions_def=partitions_def,
        tags={
            "dagster/max_runtime": str(60 * 60 * runtime_hours),
            "dagster/concurrency_key": "metoffice",
        },
        automation_condition=dg.AutomationCondition.eager(),
    )
    # `context` is deliberately unannotated: dagster inspects the raw annotation and
    # `from __future__ import annotations` turns it into a string it cannot resolve.
    def _asset(context) -> dg.MaterializeResult:
        it = _naive(context.partition_time_window.start)
        provider = provider_factory()
        written = provider.run_partition(it)
        context.log.info(f"{name}: {'wrote' if written else 'skipped'} {it}")
        return dg.MaterializeResult(
            metadata={
                "store": dg.MetadataValue.text(provider.store_path),
                "partition": dg.MetadataValue.text(str(it)),
                "written": dg.MetadataValue.bool(written),
            }
        )

    return _asset


metoffice_global_10km_asset = build_metoffice_asset(
    MetOfficeGlobal10kmProvider,
    name="metoffice-global-deterministic-10km",
    description="Met Office global 10km deterministic model, first six hourly steps per run",
    partitions_def=six_hourly_partitions,
    runtime_hours=6,
)

metoffice_global_10km_6hourly_24hr_asset = build_metoffice_asset(
    MetOfficeGlobal10km6Hourly24HourProvider,
    name="metoffice-global-deterministic-10km-6hourly-24hr",
    description="Met Office global 10km deterministic model, six-hourly steps out to 24 hours",
    partitions_def=six_hourly_partitions,
    runtime_hours=6,
)

metoffice_uk_2km_asset = build_metoffice_asset(
    MetOfficeUK2kmProvider,
    name="metoffice-uk-deterministic-2km",
    description="Met Office UK 2km deterministic model, kept as a continuous hourly analysis",
    partitions_def=uk_partitions,
    runtime_hours=6,
)

metoffice_global_ocean_hourly_asset = build_metoffice_asset(
    MetOfficeGlobalOceanHourlyProvider,
    name="metoffice-global-ocean-hourly",
    description=(
        "Met Office global ORCA025 ocean analysis as a continuous hourly series. Only the "
        "hourly instantaneous products are read; the rest of each run is daily means."
    ),
    partitions_def=daily_partitions,
)

metoffice_global_wave_asset = build_metoffice_asset(
    MetOfficeGlobalWaveProvider,
    name="metoffice-global-wave",
    description="Met Office global wave model, four runs tiled into an hourly series",
    partitions_def=daily_partitions,
)

metoffice_nws_wave_asset = build_metoffice_asset(
    MetOfficeNWSWaveProvider,
    name="metoffice-nws-wave",
    description="Met Office North West Shelf (AMM15) wave model, tiled into an hourly series",
    partitions_def=nws_partitions,
)

metoffice_nws_ocean_surface_asset = build_metoffice_asset(
    MetOfficeNWSOceanSurfaceHourlyProvider,
    name="metoffice-nws-ocean-surface-hourly",
    description=(
        "Met Office North West Shelf (AMM15) ocean analysis, hourly surface fields. "
        "About 2.3 GB a day before compression."
    ),
    partitions_def=nws_partitions,
)

# The heaviest store in the code location: 58 GB a day before compression, and roughly a
# third of that on disk once bitrounded. `memory_gb` is what keeps it from being dequeued
# alongside anything else; the run reads and writes one chunk at a time, so the figure is
# the working set rather than the day.
metoffice_nws_ocean_depth_asset = build_metoffice_asset(
    MetOfficeNWSOceanDepthHourlyProvider,
    name="metoffice-nws-ocean-depth-hourly",
    description=(
        "Met Office North West Shelf (AMM15) ocean analysis on all 51 depth levels, "
        "hourly. About 58 GB a day before compression."
    ),
    partitions_def=nws_partitions,
    runtime_hours=12,
)

metoffice_assets = [
    metoffice_global_10km_asset,
    metoffice_global_10km_6hourly_24hr_asset,
    metoffice_uk_2km_asset,
    metoffice_global_ocean_hourly_asset,
    metoffice_global_wave_asset,
    metoffice_nws_wave_asset,
    metoffice_nws_ocean_surface_asset,
    metoffice_nws_ocean_depth_asset,
]
