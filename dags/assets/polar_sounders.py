"""Dagster assets for the polar-orbiting sounders and imagers.

One partitioned asset per instrument. Each asset does nothing but hand its partition to the
matching provider in :mod:`planetary_datasets.providers.polar`; the fetching, deduping,
memory guarding and store handling all live there.

Partition widths match each provider's ``window``, because that window is what decides
whether a partition has already been ingested.

These assets are not picked up by ``dg.load_assets_from_package_module``, which only walks
the ``nwp``, ``satellite`` and ``observation`` subpackages. The code location has to add
:data:`polar_sounder_assets` to its ``dg.Definitions`` explicitly.
"""


# No ``from __future__ import annotations`` here: dagster inspects the annotation on the
# ``context`` parameter at decoration time and rejects it once it is a string.

import datetime as dt

import dagster as dg
import pandas as pd

from dags.staged import make_staged_download_asset, make_staged_publish_asset
from planetary_datasets.base import BaseProvider
from planetary_datasets.config import get_config
from planetary_datasets.providers.polar import (
    JpssAtmsProvider,
    MetopAmsuaProvider,
    MetopAscatProvider,
    MetopAvhrrProvider,
    MetopGomeProvider,
    MetopIasiProvider,
)

#: ATMS on Suomi-NPP and NOAA-20 in the NOAA open data buckets starts here.
JPSS_START = "2022-11-07"
#: MetOp-A became operational at the start of March 2008.
METOP_START = "2008-03-01"

_HOURLY_FMT = "%Y-%m-%d-%H:%M"


def _multi_hour_partitions(start: str, hours: int) -> dg.TimeWindowPartitionsDefinition:
    """Partitions ``hours`` wide, for instruments whose granules are sparse."""
    return dg.TimeWindowPartitionsDefinition(
        start=dt.datetime.fromisoformat(start),
        cron_schedule=f"0 */{hours} * * *",
        fmt=_HOURLY_FMT,
        end_offset=-1,
    )


jpss_atms_partitions = dg.HourlyPartitionsDefinition(
    start_date=f"{JPSS_START}-00:00", end_offset=-1
)
metop_gome_partitions = dg.HourlyPartitionsDefinition(
    start_date=f"{METOP_START}-00:00", end_offset=-1
)
metop_iasi_partitions = _multi_hour_partitions(METOP_START, 2)
metop_avhrr_partitions = _multi_hour_partitions(METOP_START, 4)
metop_daily_partitions = dg.DailyPartitionsDefinition(start_date=METOP_START, end_offset=-1)


def _run(context: dg.AssetExecutionContext, provider: BaseProvider) -> dg.MaterializeResult:
    """Run one partition of ``provider`` and report what it did."""
    it = pd.Timestamp(context.partition_time_window.start).tz_localize(None)
    written = provider.run_partition(it)
    if not written:
        context.log.info(f"{provider.name}: nothing written for {it}")
    return dg.MaterializeResult(
        metadata={
            "store": dg.MetadataValue.text(provider.store_path),
            "partition_start": dg.MetadataValue.text(str(it)),
            "written": dg.MetadataValue.bool(written),
        }
    )


_TAGS = {
    "dagster/max_runtime": str(60 * 60 * 6),
    "dagster/concurrency_key": "polar-sounders",
}
_RETRY = dg.RetryPolicy(max_retries=3, delay=60)


@dg.asset(
    name="jpss-atms",
    description="ATMS microwave sounder granules from NOAA-21, NOAA-20 and Suomi-NPP.",
    partitions_def=jpss_atms_partitions,
    tags=_TAGS,
    retry_policy=_RETRY,
)
def jpss_atms_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Ingest one hour of JPSS ATMS granules."""
    return _run(context, JpssAtmsProvider())


def _epct_assets(name: str, provider_cls, description: str):
    """Download/tailor in the ``docker/epct`` image, then publish on the host."""
    provider = provider_cls()

    def command(it: pd.Timestamp) -> list[str]:
        start, end = provider.partition_window(it)
        return [
            "--collection", provider.collection_id,
            "--product", provider.epct_product,
            "--start", start.isoformat(),
            "--end", end.isoformat(),
            "--target", EPCT_CONTAINER_DIR,
        ]  # fmt: skip

    def env() -> dict[str, str]:
        keys = ("eumetsat_consumer_key", "eumetsat_consumer_secret")
        return dict(zip((k.upper() for k in keys), get_config().credentials.require(*keys)))

    download = make_staged_download_asset(
        name=f"{name}-download",
        description=f"Stage one day of {description}, tailored to netCDF by the Data Tailor.",
        partitions_def=metop_daily_partitions,
        publisher=provider_cls,
        image_env="EPCT_IMAGE",
        default_image="planetary-datasets/epct:latest",
        container_dir=EPCT_CONTAINER_DIR,
        command=command,
        container_memory_gb=8,
        env=env,
        retry_policy=_RETRY,
        tags=_TAGS,
    )
    publish = make_staged_publish_asset(
        name=name,
        description=description,
        partitions_def=metop_daily_partitions,
        download=download,
        publisher=provider_cls,
        memory_gb=8,
        tags=_TAGS,
    )
    return download, publish


EPCT_CONTAINER_DIR = "/data/epct"

metop_amsua_download, metop_amsua_asset = _epct_assets(
    "metop-amsua", MetopAmsuaProvider, "MetOp AMSU-A microwave sounder orbits"
)
metop_ascat_download, metop_ascat_asset = _epct_assets(
    "metop-ascat", MetopAscatProvider, "MetOp ASCAT scatterometer orbits"
)


@dg.asset(
    name="metop-avhrr",
    description="MetOp AVHRR level 1B imagery from the EUMETSAT Data Store.",
    partitions_def=metop_avhrr_partitions,
    tags=_TAGS,
    retry_policy=_RETRY,
)
def metop_avhrr_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Ingest four hours of MetOp AVHRR orbits."""
    return _run(context, MetopAvhrrProvider())


@dg.asset(
    name="metop-gome",
    description="MetOp GOME-2 level 1 radiances from the EUMETSAT Data Store.",
    partitions_def=metop_gome_partitions,
    tags=_TAGS,
    retry_policy=_RETRY,
)
def metop_gome_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Ingest one hour of MetOp GOME-2 products."""
    return _run(context, MetopGomeProvider())


@dg.asset(
    name="metop-iasi",
    description="MetOp IASI level 1C spectra from the EUMETSAT Data Store.",
    partitions_def=metop_iasi_partitions,
    tags=_TAGS,
    retry_policy=_RETRY,
)
def metop_iasi_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Ingest two hours of MetOp IASI products."""
    return _run(context, MetopIasiProvider())


#: Every asset in this module, for the code location to load.
polar_sounder_assets = [
    jpss_atms_asset,
    metop_amsua_download,
    metop_amsua_asset,
    metop_ascat_download,
    metop_ascat_asset,
    metop_avhrr_asset,
    metop_gome_asset,
    metop_iasi_asset,
]
