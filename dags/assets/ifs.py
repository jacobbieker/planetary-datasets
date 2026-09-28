"""Dagster assets for the ECMWF IFS HRES operational analysis.

One daily partition per asset run; the provider does the fetching, merging and writing.
"""

# No `from __future__ import annotations` here: dagster matches the `context` parameter's
# annotation by identity, and stringified annotations fail that check.
import dagster as dg
import pandas as pd
from dagster import AssetExecutionContext

from planetary_datasets.providers.ifs import (
    IFSAnalysisProvider,
    IFSRegriddedAnalysisProvider,
)

# ds113.1 on GDEX starts in 2016. The archive is published a day or more behind real
# time, so end_offset=-1 holds back the most recent day as well as the current one.
ifs_partitions_def = dg.DailyPartitionsDefinition(start_date="2016-01-01", end_offset=-1)


def _materialize(context: AssetExecutionContext, provider) -> dg.MaterializeResult:
    it = pd.Timestamp(context.partition_time_window.start)
    if it.tzinfo is not None:
        # The store's time coordinate is naive UTC, so partition keys must be too.
        it = it.tz_convert("UTC").tz_localize(None)

    partition = pd.DatetimeIndex([it])
    already_stored = not provider.missing_timesteps(partition)
    written = provider.run_partition(it)

    if not written and not already_stored:
        # run_partition returns False both for "already there" and for "nothing was
        # written". Only the first is a success; failing here keeps the retry policy in
        # play instead of marking an empty partition materialised.
        raise RuntimeError(
            f"{provider.name}: nothing was written for {it} and it is still missing from "
            f"{provider.store_path}"
        )

    return dg.MaterializeResult(
        metadata={
            "partition": dg.MetadataValue.text(str(it)),
            "store": dg.MetadataValue.text(provider.store_path),
            "status": dg.MetadataValue.text("written" if written else "already-present"),
        }
    )


@dg.asset(
    name="ifs-hres-analysis",
    description="ECMWF IFS HRES analysis on its native grid, appended to Icechunk.",
    partitions_def=ifs_partitions_def,
    tags={
        "dagster/max_runtime": str(60 * 60 * 6),
        "dagster/concurrency_key": "ifs",
    },
    retry_policy=dg.RetryPolicy(max_retries=3, delay=60),
)
def ifs_hres_analysis_asset(context: AssetExecutionContext) -> dg.MaterializeResult:
    """Append one day of native-grid IFS HRES analysis."""
    return _materialize(context, IFSAnalysisProvider())


@dg.asset(
    name="ifs-hres-analysis-regrid-1deg",
    description="ECMWF IFS HRES analysis regridded to 1 degree, appended to Icechunk.",
    partitions_def=ifs_partitions_def,
    tags={
        "dagster/max_runtime": str(60 * 60 * 6),
        "dagster/concurrency_key": "ifs",
    },
    retry_policy=dg.RetryPolicy(max_retries=3, delay=60),
)
def ifs_hres_analysis_regrid_1deg_asset(
    context: AssetExecutionContext,
) -> dg.MaterializeResult:
    """Append one day of IFS HRES analysis regridded to 1 degree."""
    provider = IFSRegriddedAnalysisProvider(
        grid=(1.0, 1.0),
        store_prefix="ifs_ana/hres_analysis_1deg.icechunk",
    )
    return _materialize(context, provider)


@dg.asset(
    name="ifs-hres-analysis-regrid-025deg",
    description="ECMWF IFS HRES analysis regridded to 0.25 degrees, appended to Icechunk.",
    partitions_def=ifs_partitions_def,
    tags={
        "dagster/max_runtime": str(60 * 60 * 12),
        "dagster/concurrency_key": "ifs",
    },
    retry_policy=dg.RetryPolicy(max_retries=3, delay=60),
)
def ifs_hres_analysis_regrid_025deg_asset(
    context: AssetExecutionContext,
) -> dg.MaterializeResult:
    """Append one day of IFS HRES analysis regridded to 0.25 degrees."""
    provider = IFSRegriddedAnalysisProvider(
        grid=(0.25, 0.25),
        store_prefix="ifs_ana/hres_analysis_025deg.icechunk",
    )
    return _materialize(context, provider)
