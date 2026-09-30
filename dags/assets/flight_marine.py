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

from planetary_datasets.providers.observations._points import naive_utc
from planetary_datasets.providers.observations.opensky import (
    SAMPLE_END,
    SAMPLE_START,
    OpenSkyStatesProvider,
)
from planetary_datasets.providers.observations.osmc import (
    PLATFORM_TYPES,
    OSMCFloat32Provider,
    OSMCProvider,
)

# OpenSky publishes its public samples for Mondays only, and the set is closed: it ends
# 2022-06-27. day_offset=1 anchors the weekly window on Monday; the end_date stops the
# definition generating hundreds of partitions that can only ever download nothing.
opensky_partitions = dg.WeeklyPartitionsDefinition(
    start_date=SAMPLE_START.strftime("%Y-%m-%d"),
    # end_date bounds the window's *end*, so the last Monday needs a full week beyond it
    # to be included at all.
    end_date=(SAMPLE_END + pd.Timedelta(7, "D")).strftime("%Y-%m-%d"),
    day_offset=1,
)

# end_offset is left at its default: MonthlyPartitionsDefinition already excludes the
# in-progress month, and OSMC_RealTime serves a rolling window, so delaying by a further
# month risks the data ageing out before the partition becomes available.
osmc_partitions = dg.MonthlyPartitionsDefinition(start_date="2012-01-01")


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
    """Ingest the 24 hourly OpenSky state-vector archives for one Monday.

    The hours are run individually rather than through ``run_range`` so a failure is
    surfaced: ``run_range`` logs and swallows every per-partition exception, which would
    let a week where all 24 downloads failed materialize green and never be retried.
    """
    day = naive_utc(context.partition_time_window.start).normalize()
    provider = OpenSkyStatesProvider()
    hours = pd.date_range(day, periods=24, freq="h")

    written = 0
    absent = 0
    failures: list[tuple[pd.Timestamp, Exception]] = []
    for hour in provider.missing_timesteps(hours):
        try:
            if provider.run_partition(hour, check_present=False):
                written += 1
            else:
                absent += 1
        except Exception as exc:  # noqa: BLE001 - collected and re-raised below
            context.log.exception(f"hour {hour} failed: {exc}")
            failures.append((hour, exc))

    context.log.info(
        f"{day:%Y-%m-%d}: {written} hour(s) written, {absent} unpublished, "
        f"{len(failures)} failed"
    )
    if failures:
        hour, exc = failures[0]
        raise RuntimeError(
            f"{len(failures)} of 24 hours failed for {day:%Y-%m-%d}; first was {hour}: {exc}"
        ) from exc

    return dg.MaterializeResult(
        metadata={
            "hours_written": dg.MetadataValue.int(written),
            "hours_unpublished": dg.MetadataValue.int(absent),
            "store_path": dg.MetadataValue.text(provider.store_path),
        }
    )


def _osmc_asset(dataset: str, provider_cls: type[OSMCProvider] = OSMCProvider):
    """Build the monthly OSMC asset for one platform group."""
    template = provider_cls(dataset)

    @dg.asset(
        name=template.name,
        description=f"NOAA OSMC GTS marine observations: {dataset}.\n\n{__doc__}",
        group_name="flight_marine",
        partitions_def=osmc_partitions,
        metadata={
            "source": dg.MetadataValue.text("noaa-osmc-erddap"),
            "platform_type": dg.MetadataValue.text(str(PLATFORM_TYPES[dataset] or "all types")),
            "store": dg.MetadataValue.text(template.store_prefix),
        },
        compute_kind="python",
        tags={"dagster/concurrency_key": "osmc-erddap"},
    )
    # context intentionally unannotated; see opensky_states_asset above.
    def _asset(context) -> dg.MaterializeResult:
        it = naive_utc(context.partition_time_window.start)
        provider = provider_cls(dataset)
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
osmc_assets = [
    _osmc_asset(dataset, cls)
    for cls in (OSMCProvider, OSMCFloat32Provider)
    for dataset in PLATFORM_TYPES
]
