"""Dagster assets for the GPM and TRMM CORRA combined precipitation retrievals.

One daily-partitioned asset per satellite, backed by the providers in
:mod:`planetary_datasets.providers.corra`. The TRMM asset is bounded by the end of the
mission; the GPM one runs to yesterday.
"""

import dagster as dg
import pandas as pd
from dagster import AssetExecutionContext

from planetary_datasets.providers.corra import (
    CorraProvider,
    GPMCorraProvider,
    TRMMCorraProvider,
)

gpm_partitions = dg.DailyPartitionsDefinition(
    start_date=GPMCorraProvider.start_date.strftime("%Y-%m-%d"),
    end_offset=-1,
)

trmm_partitions = dg.DailyPartitionsDefinition(
    start_date=TRMMCorraProvider.start_date.strftime("%Y-%m-%d"),
    # Dagster's end_date is exclusive but the provider treats its end_date as the last day
    # it covers, so the final day of the mission needs one more day on top of it.
    end_date=(TRMMCorraProvider.end_date + pd.Timedelta(1, "D")).strftime("%Y-%m-%d"),
)

COMMON_TAGS = {
    # PPS rate-limits per account, so both satellites share one concurrency slot.
    "dagster/concurrency_key": "nasa-pps",
    "dagster/max_runtime": str(60 * 60 * 6),
}


def _run(context: AssetExecutionContext, provider: CorraProvider) -> dg.MaterializeResult:
    """Run one day and turn the outcome into Dagster metadata."""
    it = pd.Timestamp(context.partition_time_window.start).tz_localize(None).normalize()
    context.log.info(f"{provider.name}: running {it:%Y-%m-%d} into {provider.store_path}")
    written = provider.run_partition(it)
    return dg.MaterializeResult(
        metadata={
            "partition": dg.MetadataValue.text(f"{it:%Y-%m-%d}"),
            "store": dg.MetadataValue.text(provider.store_path),
            "archive_dir": dg.MetadataValue.path(str(provider.base_dir)),
            "written": dg.MetadataValue.bool(written),
            "product": dg.MetadataValue.text(provider.product),
            "scan_mode": dg.MetadataValue.text(provider.scan_mode),
        },
    )


@dg.asset(
    name="gpm_corra",
    description=GPMCorraProvider.__doc__,
    key_prefix=["precipitation"],
    partitions_def=gpm_partitions,
    compute_kind="python",
    metadata={"source": dg.MetadataValue.text("nasa-pps")},
    tags=COMMON_TAGS,
)
def gpm_corra_asset(context: AssetExecutionContext) -> dg.MaterializeResult:
    """One day of GPM DPR + GMI combined retrievals."""
    return _run(context, GPMCorraProvider())


@dg.asset(
    name="trmm_corra",
    description=TRMMCorraProvider.__doc__,
    key_prefix=["precipitation"],
    partitions_def=trmm_partitions,
    compute_kind="python",
    metadata={"source": dg.MetadataValue.text("nasa-pps")},
    tags=COMMON_TAGS,
)
def trmm_corra_asset(context: AssetExecutionContext) -> dg.MaterializeResult:
    """One day of TRMM PR + TMI combined retrievals."""
    return _run(context, TRMMCorraProvider())


assets = [gpm_corra_asset, trmm_corra_asset]
