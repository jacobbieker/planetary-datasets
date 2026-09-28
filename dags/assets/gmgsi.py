"""Dagster assets for NOAA's Global Mosaic of Geostationary Satellite Imagery.

One hourly partition per asset run. All of the work — downloading, merging the channels,
skipping hours already stored and appending to icechunk — happens in
:class:`~planetary_datasets.providers.gmgsi.GMGSIProvider`; the assets below only translate
a Dagster partition into a timestamp and report what happened.

``gmgsi_assets`` is the list to register in ``dags/definitions.py``.
"""

# No `from __future__ import annotations` here: Dagster inspects the `context` parameter's
# annotation object, and stringised annotations make that check fail.
import dagster as dg
import pandas as pd
from dagster import AssetExecutionContext

from planetary_datasets.providers.gmgsi import (
    V1_START,
    V3_START,
    GMGSILegacyProvider,
    GMGSIProvider,
)

# GMGSI lands roughly 40 minutes after the hour, so the two most recent hours are held back
# rather than materialised and found missing.
END_OFFSET = -2

gmgsi_v3_partitions = dg.HourlyPartitionsDefinition(
    start_date=V3_START.strftime("%Y-%m-%d-%H:%M"),
    end_offset=END_OFFSET,
)

gmgsi_v1_partitions = dg.HourlyPartitionsDefinition(
    start_date=V1_START.strftime("%Y-%m-%d-%H:%M"),
    end_offset=END_OFFSET,
)

_TAGS = {
    "dagster/max_runtime": str(60 * 30),
    "dagster/priority": "1",
    "dagster/concurrency_key": "gmgsi",
}


def _partition_timestamp(context: AssetExecutionContext) -> pd.Timestamp:
    """The partition start as a naive UTC timestamp.

    Dagster hands back a timezone-aware datetime, but the ``time`` coordinate in the store
    is naive, and comparing the two raises rather than silently mismatching.
    """
    it = pd.Timestamp(context.partition_time_window.start)
    if it.tzinfo is not None:
        it = it.tz_convert("UTC").tz_localize(None)
    return it


def _materialize(context: AssetExecutionContext, provider: GMGSIProvider) -> dg.MaterializeResult:
    """Run one partition and turn the outcome into asset metadata.

    A partition that writes nothing is only a success if the hour is already in the store.
    If it is still missing — NOAA published late, or a channel was absent — the run fails
    so Dagster retries it instead of marking an empty hour materialised for good.
    """
    it = _partition_timestamp(context)
    written = provider.run_partition(it)
    if not written:
        if provider.missing_timesteps(pd.DatetimeIndex([it])):
            raise dg.Failure(
                description=f"No GMGSI data written for {it} and it is not in {provider.store_path}"
            )
        context.log.info(f"{it} already present in {provider.store_path}, nothing to do")
    return dg.MaterializeResult(
        metadata={
            "time": dg.MetadataValue.text(it.isoformat()),
            "store": dg.MetadataValue.text(provider.store_path),
            "written": dg.MetadataValue.bool(written),
        }
    )


@dg.asset(
    name="gmgsi_v3",
    description="Hourly NOAA GMGSI v3 blended global geostationary mosaic, appended to icechunk",
    metadata={
        "area": dg.MetadataValue.text("global"),
        "source": dg.MetadataValue.text("noaa-gmgsi-pds"),
        "channels": dg.MetadataValue.text("vis, wv, lwir, swir (+ dqf)"),
    },
    partitions_def=gmgsi_v3_partitions,
    tags=_TAGS,
    retry_policy=dg.RetryPolicy(max_retries=3, delay=60),
    automation_condition=dg.AutomationCondition.eager(),
)
def gmgsi_v3_asset(context: AssetExecutionContext) -> dg.MaterializeResult:
    return _materialize(context, GMGSIProvider())


@dg.asset(
    name="gmgsi_v1",
    description="Hourly NOAA GMGSI v1 global geostationary mosaic (pre-2025 archive)",
    metadata={
        "area": dg.MetadataValue.text("global"),
        "source": dg.MetadataValue.text("noaa-gmgsi-pds"),
        "channels": dg.MetadataValue.text("vis, ssr, wv, lwir, swir"),
    },
    partitions_def=gmgsi_v1_partitions,
    tags=_TAGS,
    retry_policy=dg.RetryPolicy(max_retries=3, delay=60),
)
def gmgsi_v1_asset(context: AssetExecutionContext) -> dg.MaterializeResult:
    return _materialize(context, GMGSILegacyProvider())


gmgsi_assets = [gmgsi_v3_asset, gmgsi_v1_asset]

__all__ = ["gmgsi_assets", "gmgsi_v1_asset", "gmgsi_v3_asset"]
