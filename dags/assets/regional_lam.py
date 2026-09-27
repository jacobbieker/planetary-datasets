"""Icechunk archives of the regional limited-area NWP models.

Four kilometre-scale limited-area models, all built on the same GRIB engine in
:mod:`planetary_datasets.providers.regional_lam_common`:

* DMI HARMONIE over Greenland and Iceland, pressure/surface and model levels.
* The NOAA HRRR Alaska nest.
* The NOAA NAM Hawaii nest.
* MeteoSwiss KENDA-CH1, analysis and one-hour forecast.

Each asset materialises one init time: the provider fetches that init time's GRIB files,
merges them and appends the result to its icechunk store. A partition whose inputs are not
in the archive reports itself as skipped rather than failing, because these archives all
have gaps and a rolling retention window.

Note: this module deliberately does not use ``from __future__ import annotations``.
Dagster validates the ``context`` parameter of an asset against the real class object,
which PEP 563 would turn into a string.
"""

import datetime as dt

import dagster as dg
import pandas as pd
from dagster import AssetExecutionContext

# Note: no ``from __future__ import annotations`` here. Dagster validates the ``context``
# parameter against the real class object, which PEP 563 would turn into a string.
from planetary_datasets.base import BaseProvider
from planetary_datasets.providers.dmi_harmonie import (
    DMIHarmonieModelLevelProvider,
    DMIHarmonieProvider,
)
from planetary_datasets.providers.hawaii_nam import HawaiiNAMProvider
from planetary_datasets.providers.hrrr_alaska import AlaskaHRRRProvider
from planetary_datasets.providers.kenda import KENDAAnalysisProvider, KENDAForecastProvider

PARTITION_FORMAT = "%Y-%m-%d-%H:%M"


def _partitions(start: str, cron_schedule: str) -> dg.TimeWindowPartitionsDefinition:
    """Time-window partitions on ``cron_schedule``, one partition per init time."""
    return dg.TimeWindowPartitionsDefinition(
        start=start,
        fmt=PARTITION_FORMAT,
        cron_schedule=cron_schedule,
        end_offset=0,
    )


# DMI publishes every 3 hours but keeps only the last few days, so backfills further back
# than the retention window will simply find nothing and skip.
harmonie_partitions = _partitions("2026-01-01-00:00", "0 0/3 * * *")
alaska_partitions = _partitions("2018-08-01-00:00", "0 0/3 * * *")
hawaii_partitions = _partitions("2021-01-01-00:00", "0 0/6 * * *")
kenda_partitions = _partitions("2026-06-20-00:00", "0 * * * *")


def partition_timestamp(context: AssetExecutionContext) -> pd.Timestamp:
    """The init time a partition stands for, as a naive UTC timestamp.

    Dagster hands back timezone-aware datetimes; the stores are written with naive UTC
    times, and comparing the two raises rather than silently mismatching.
    """
    start: dt.datetime = context.partition_time_window.start
    timestamp = pd.Timestamp(start)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert("UTC").tz_localize(None)
    return timestamp


def build_lam_asset(
    provider: BaseProvider,
    partitions_def: dg.TimeWindowPartitionsDefinition,
    description: str,
    source: str,
) -> dg.AssetsDefinition:
    """Build the Dagster asset for one regional model provider.

    The provider instance is captured rather than re-created per run so that the store
    prefix shows up in the asset's static metadata.
    """

    @dg.asset(
        name=provider.name,
        description=description,
        partitions_def=partitions_def,
        compute_kind="python",
        metadata={
            "store": dg.MetadataValue.text(provider.store_prefix),
            "source": dg.MetadataValue.text(source),
            "append_dim": dg.MetadataValue.text(provider.append_dim),
        },
        tags={"dagster/concurrency_key": "regional-lam"},
    )
    def _asset(context: AssetExecutionContext) -> dg.MaterializeResult:
        it = partition_timestamp(context)
        written = provider.run_partition(it)
        if not written:
            context.log.info(f"{provider.name}: nothing to write for {it}")
        return dg.MaterializeResult(
            metadata={
                "init_time": dg.MetadataValue.text(it.isoformat()),
                "written": dg.MetadataValue.bool(bool(written)),
                "store_path": dg.MetadataValue.text(provider.store_path),
            }
        )

    return _asset


dmi_harmonie_asset = build_lam_asset(
    DMIHarmonieProvider(),
    harmonie_partitions,
    "DMI HARMONIE Greenland/Iceland pressure-level and surface fields, 0-2 h steps.",
    "dmi-opendata-s3",
)

dmi_harmonie_model_level_asset = build_lam_asset(
    DMIHarmonieModelLevelProvider(),
    harmonie_partitions,
    "DMI HARMONIE Greenland/Iceland native model levels, 0-2 h steps.",
    "dmi-opendata-s3",
)

alaska_hrrr_asset = build_lam_asset(
    AlaskaHRRRProvider(),
    alaska_partitions,
    "NOAA HRRR Alaska nest, surface/native/pressure levels merged, 0-2 h steps.",
    "noaa-s3",
)

hawaii_nam_asset = build_lam_asset(
    HawaiiNAMProvider(),
    hawaii_partitions,
    "NOAA NAM Hawaii nest, hourly 0-5 h steps folded into a continuous time axis.",
    "noaa-s3",
)

kenda_analysis_asset = build_lam_asset(
    KENDAAnalysisProvider(),
    kenda_partitions,
    "MeteoSwiss KENDA-CH1 analysis, merged with the horizontal and vertical constants.",
    "meteoswiss-local-archive",
)

kenda_forecast_asset = build_lam_asset(
    KENDAForecastProvider(),
    kenda_partitions,
    "MeteoSwiss KENDA-CH1 one-hour forecast, merged with the constants.",
    "meteoswiss-local-archive",
)

regional_lam_assets = [
    dmi_harmonie_asset,
    dmi_harmonie_model_level_asset,
    alaska_hrrr_asset,
    hawaii_nam_asset,
    kenda_analysis_asset,
    kenda_forecast_asset,
]
