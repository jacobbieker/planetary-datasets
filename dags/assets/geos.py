"""Dagster assets for the NASA GEOS-CF 15-minute global analysis.

One partition is one day. Each run fills whichever of that day's 96 instants are still
missing from the Icechunk store, so a re-run after a partial day is cheap and safe.

The heavy lifting lives in :class:`planetary_datasets.providers.geos.GEOSProvider`; these
assets only map a Dagster partition key onto it.

This module lives outside the ``assets.nwp`` package that ``dags/definitions.py`` loads
automatically, so ``geos_assets`` has to be added to the code location's ``Definitions``
explicitly. Until that happens these assets will not appear in Dagster.
"""

# NB: no ``from __future__ import annotations`` here. Dagster compares the ``context``
# parameter's annotation against the AssetExecutionContext class by identity, which fails
# when annotations are stringified.
import dagster as dg
import pandas as pd
from dagster import AssetExecutionContext

from planetary_datasets.providers.geos import GEOSProvider

# Earliest instant in each store, read from the published archives on source.coop.
V1_START_DATE = "2018-01-01"
V2_START_DATE = "2026-01-01"

v1_partitions_def = dg.DailyPartitionsDefinition(start_date=V1_START_DATE, end_offset=1)
v2_partitions_def = dg.DailyPartitionsDefinition(start_date=V2_START_DATE, end_offset=1)


def _materialise(context: AssetExecutionContext, version: int) -> dg.MaterializeResult:
    """Fill the missing 15-minute instants of one day for one GEOS-CF version."""
    provider = GEOSProvider(version=version)
    day = pd.Timestamp(context.partition_key)
    result = provider.run_day(day)

    context.log.info(
        f"{day.date()}: wrote {result.written}, failed {result.failed}, "
        f"not yet published {result.unavailable} -> {provider.store_path}"
    )
    if result.failed and not result.written:
        # Every instant we tried raised. Succeeding here would mark the day done and it
        # would never be retried, so surface the outage instead.
        raise dg.Failure(
            description=(
                f"all {result.failed} attempted instant(s) for {day.date()} failed; "
                "see the logs for the underlying errors"
            )
        )

    return dg.MaterializeResult(
        metadata={
            "timesteps_written": dg.MetadataValue.int(result.written),
            "timesteps_failed": dg.MetadataValue.int(result.failed),
            "timesteps_unavailable": dg.MetadataValue.int(result.unavailable),
            "store": dg.MetadataValue.text(provider.store_path),
            "geos_cf_version": dg.MetadataValue.int(version),
        }
    )


@dg.asset(
    name="geos_cf_v1_15min",
    description=__doc__,
    key_prefix=["nwp"],
    compute_kind="python",
    partitions_def=v1_partitions_def,
    metadata={
        "source": dg.MetadataValue.text("nasa-nccs-datashare"),
        "model": dg.MetadataValue.text("geos-cf-v1"),
        "format": dg.MetadataValue.text("icechunk"),
    },
    tags={"dagster/concurrency_key": "nasa-nccs"},
)
def geos_cf_v1_15min(context: AssetExecutionContext) -> dg.MaterializeResult:
    """GEOS-CF v1 15-minute analysis appended to ``bkr/geos/geos_15min.icechunk``."""
    return _materialise(context, version=1)


@dg.asset(
    name="geos_cf_v2_15min",
    description=__doc__,
    key_prefix=["nwp"],
    compute_kind="python",
    partitions_def=v2_partitions_def,
    metadata={
        "source": dg.MetadataValue.text("nasa-nccs-datashare"),
        "model": dg.MetadataValue.text("geos-cf-v2"),
        "format": dg.MetadataValue.text("icechunk"),
    },
    tags={"dagster/concurrency_key": "nasa-nccs"},
)
def geos_cf_v2_15min(context: AssetExecutionContext) -> dg.MaterializeResult:
    """GEOS-CF v2 15-minute analysis appended to ``bkr/geos/geos_v2_15min.icechunk``."""
    return _materialise(context, version=2)


geos_assets = [geos_cf_v1_15min, geos_cf_v2_15min]
