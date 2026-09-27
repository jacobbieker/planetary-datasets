"""Dagster assets for flight tracking and marine float observations.

Two openly-accessible point archives:

* **OpenSky Network state vectors** — hourly ADS-B samples. The public sample set covers
  every Monday from 2016-06-06 to 2022-06-27, so the partition here is *weekly, anchored
  on Monday*, and each run ingests that day's 24 hourly archives. An hourly partition
  definition would create ~90% empty partitions for the six days a week with no sample.
* **NOAA OSMC GTS marine reports** — monthly pulls from AOML's ERDDAP, one asset per
  platform group (drifters, ships, moored buoys, tide gauges, ...).

Neither source needs a credential. Set ``ICECHUNK_LOCAL_PATH`` to write to a local
directory instead of the public bucket.

The Eurocontrol R&D Archive is licence-gated and hand-downloaded, so it has helpers in
``planetary_datasets.providers.observations.eurocontrol`` but no asset.
"""

import dagster as dg
import pandas as pd

from planetary_datasets.providers.observations.opensky import OpenSkyStatesProvider
from planetary_datasets.providers.observations.osmc import (
    PLATFORM_TYPES,
    OSMCProvider,
    store_prefix_for,
)

# OpenSky publishes its public samples for Mondays only. day_offset=1 anchors the weekly
# window on Monday. end_offset=-1 keeps the still-open current week out of the set.
opensky_partitions = dg.WeeklyPartitionsDefinition(
    start_date="2016-06-06",
    day_offset=1,
    end_offset=-1,
)

osmc_partitions = dg.MonthlyPartitionsDefinition(
    start_date="2012-01-01",
    end_offset=-1,
)


@dg.asset(
    name="opensky_states",
    description=__doc__,
    group_name="flight_marine",
    partitions_def=opensky_partitions,
    metadata={
        "source": dg.MetadataValue.text("opensky-network"),
        "store": dg.MetadataValue.text(OpenSkyStatesProvider.store_prefix),
    },
    compute_kind="python",
    tags={"dagster/concurrency_key": "opensky"},
)
# context is intentionally unannotated: Dagster inspects the raw annotation, and both a
# `from __future__ import annotations` in this module and an explicit
# `context: dg.AssetExecutionContext` under one raise DagsterInvalidDefinitionError.
def opensky_states_asset(context) -> dg.MaterializeResult:
    """Ingest the 24 hourly OpenSky state-vector archives for one Monday."""
    day = pd.Timestamp(context.partition_time_window.start).normalize()
    provider = OpenSkyStatesProvider()
    hours = pd.date_range(day, periods=24, freq="h")
    written = provider.run_range(hours)
    context.log.info(f"wrote {written} of 24 hourly partitions for {day:%Y-%m-%d}")
    return dg.MaterializeResult(
        metadata={
            "hours_written": dg.MetadataValue.int(written),
            "store_path": dg.MetadataValue.text(provider.store_path),
        }
    )


def _osmc_asset(dataset: str):
    """Build the monthly OSMC asset for one platform group."""
    provider_prefix = store_prefix_for(dataset)

    @dg.asset(
        name=f"osmc_{dataset}",
        description=f"NOAA OSMC GTS marine observations: {dataset}.\n\n{__doc__}",
        group_name="flight_marine",
        partitions_def=osmc_partitions,
        metadata={
            "source": dg.MetadataValue.text("noaa-osmc-erddap"),
            "platform_type": dg.MetadataValue.text(str(PLATFORM_TYPES[dataset] or "all types")),
            "store": dg.MetadataValue.text(provider_prefix),
        },
        compute_kind="python",
        tags={"dagster/concurrency_key": "osmc-erddap"},
    )
    # context intentionally unannotated; see opensky_states_asset above.
    def _asset(context) -> dg.MaterializeResult:
        it = pd.Timestamp(context.partition_time_window.start)
        provider = OSMCProvider(dataset)
        wrote = provider.run_partition(it)
        return dg.MaterializeResult(
            metadata={
                "wrote": dg.MetadataValue.bool(wrote),
                "month": dg.MetadataValue.text(f"{it:%Y-%m}"),
                "store_path": dg.MetadataValue.text(provider.store_path),
            }
        )

    return _asset


# A plain module-level list: dagster's load_assets_from_modules picks up AssetsDefinition
# objects held in a list as well as bound directly to a module attribute.
osmc_assets = [_osmc_asset(dataset) for dataset in PLATFORM_TYPES]
