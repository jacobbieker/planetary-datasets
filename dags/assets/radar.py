"""Dagster assets for the ground radar stores: UK, Finland and the OPERA composite.

One asset per provider in :data:`planetary_datasets.providers.radar.PROVIDERS`. Each asset
materialises a single hourly partition by handing the partition start to the provider,
which locates that hour's files in the local archive, processes them and appends them to
the icechunk store.

Both sources are read from a staging directory rather than downloaded; see the provider
module docstring for ``UK_RADAR_ARCHIVE_DIR`` and ``FMI_RADAR_ARCHIVE_DIR``. The assets are
always defined and fail with a clear message when the directory is not configured.

OPERA is fetched rather than staged by hand. Its decoder cannot be installed alongside this
project, so each OPERA product has a ``*_download`` asset that runs the
``docker/opera-radar`` image to stage one hour as netCDF, and a processing asset that
depends on it, appends that hour to the store and then deletes the staged file.

Registration is deliberately left to ``dags/definitions.py``, which is owned separately:
that module currently loads only the ``nwp``, ``satellite`` and ``observation`` *packages*,
so these assets are not live until :data:`radar_assets` is added there alongside the other
top-level ``dags/assets/*.py`` modules.
"""

import datetime as dt
from typing import Type

import dagster as dg
import pandas as pd

from dags.staged import make_staged_download_asset, make_staged_publish_asset
from planetary_datasets.providers.opera import (
    OPERAProvider,
    OPERARainfallProvider,
    OPERAReflectivityProvider,
)
from planetary_datasets.providers.opera_download import DEFAULT_TARGET as OPERA_CONTAINER_ARCHIVE
from planetary_datasets.providers.radar import (
    FMIRadarProvider,
    LocalArchiveRadarProvider,
    UKRadarProvider,
)

#: Both composites are indexed by the hour. The UK partition covers twelve five-minute
#: frames; the FMI partition covers the three accumulation windows published on the hour.
hourly_partitions = dg.HourlyPartitionsDefinition(start_date=dt.datetime(2020, 1, 1), end_offset=-1)


def _build_asset(
    provider_cls: Type[LocalArchiveRadarProvider],
    description: str,
    source_url: str,
) -> dg.AssetsDefinition:
    """Build the single-partition asset for one radar provider."""

    @dg.asset(
        name=provider_cls.name,
        description=description,
        partitions_def=hourly_partitions,
        compute_kind="python",
        metadata={
            "store": dg.MetadataValue.text(provider_cls.store_prefix),
            "archive_dir_env": dg.MetadataValue.text(provider_cls.archive_env),
            "source": dg.MetadataValue.url(source_url),
        },
    )
    # `context` is intentionally left unannotated: Dagster inspects the raw annotation, and
    # both a `dg.AssetExecutionContext` hint under `from __future__ import annotations` and
    # a stringified one raise DagsterInvalidDefinitionError.
    def _asset(context) -> dg.MaterializeResult:
        # Dagster hands the partition start over tz-aware; the provider normalises it to
        # naive UTC, which is how the radar times are stored.
        it = pd.Timestamp(context.partition_time_window.start)
        provider = provider_cls()
        written = provider.run_partition(it)
        context.log.info(f"{provider.name}: {it} -> {provider.store_path} (written={written})")
        return dg.MaterializeResult(
            metadata={
                "written": dg.MetadataValue.bool(written),
                "partition": dg.MetadataValue.text(str(it)),
                "store_path": dg.MetadataValue.path(provider.store_path),
            }
        )

    return _asset


uk_radar = _build_asset(
    UKRadarProvider,
    "Met Office RADARNET 1 km rain-rate composite, twelve five-minute ODIM frames per hour.",
    "https://catalogue.ceda.ac.uk/uuid/82adec1f896af6169112d09cc1174499",
)

fmi_radar = _build_asset(
    FMIRadarProvider,
    "FMI 1 km precipitation accumulations for Finland, 1 h / 12 h / 24 h windows per hour.",
    "https://en.ilmatieteenlaitos.fi/open-data",
)

# --------------------------------------------------------------------------------------
# EUMETNET OPERA, staged by the docker/opera-radar image.
# --------------------------------------------------------------------------------------

#: Image built by ``docker/opera-radar/build.sh``.
OPERA_IMAGE_ENV = "OPERA_RADAR_IMAGE"
DEFAULT_OPERA_IMAGE = "planetary-datasets/opera-radar:latest"
OPERA_SOURCE_URL = "https://eumetnet.github.io/openradardata-documentation/"
#: A reflectivity hour is twelve 3800x4400 frames plus their lat/lon grid.
OPERA_CONTAINER_MEMORY_GB = 6
#: Publishing reads one staged hour back.
OPERA_PUBLISH_MEMORY_GB = 4

#: The partitions start where the existing stores do. Appends must be in time order, so
#: an earlier start would only produce hours the store refuses.
opera_rainfall_partitions = dg.HourlyPartitionsDefinition(
    start_date=dt.datetime(2018, 1, 1), end_offset=-1
)
opera_dbz_partitions = dg.HourlyPartitionsDefinition(
    start_date=dt.datetime(2025, 1, 1), end_offset=-1
)


def opera_download_command(provider: OPERAProvider, it: pd.Timestamp) -> list[str]:
    """Arguments for the downloader image's entrypoint, for one hour."""
    return [
        "--product",
        provider.product,
        "--time",
        it.strftime("%Y-%m-%dT%H:%M"),
        "--target",
        OPERA_CONTAINER_ARCHIVE,
    ]


def _build_opera_assets(
    provider_cls: Type[OPERAProvider],
    partitions_def: dg.PartitionsDefinition,
    download_description: str,
    description: str,
) -> tuple[dg.AssetsDefinition, dg.AssetsDefinition]:
    """The download and publish assets for one OPERA product."""
    source = {"source": dg.MetadataValue.url(OPERA_SOURCE_URL)}
    download = make_staged_download_asset(
        name=f"{provider_cls.name}_download",
        description=download_description,
        partitions_def=partitions_def,
        publisher=provider_cls,
        image_env=OPERA_IMAGE_ENV,
        default_image=DEFAULT_OPERA_IMAGE,
        container_dir=OPERA_CONTAINER_ARCHIVE,
        command=lambda it: opera_download_command(provider_cls, it),
        container_memory_gb=OPERA_CONTAINER_MEMORY_GB,
        # The archive can lag real time; a failed hour is retried rather than left.
        retry_policy=dg.RetryPolicy(max_retries=3, delay=1800, backoff=dg.Backoff.EXPONENTIAL),
        tags={"dagster/concurrency_key": "opera-download"},
        metadata=source,
    )
    publish = make_staged_publish_asset(
        name=provider_cls.name,
        description=description,
        partitions_def=partitions_def,
        download=download,
        publisher=provider_cls,
        memory_gb=OPERA_PUBLISH_MEMORY_GB,
        metadata={
            **source,
            "store": dg.MetadataValue.text(provider_cls.store_prefix),
            "archive_dir_env": dg.MetadataValue.text(provider_cls.archive_env),
        },
    )
    return download, publish


opera_rainfall_download, opera_rainfall = _build_opera_assets(
    OPERARainfallProvider,
    opera_rainfall_partitions,
    "One hour of OPERA rain rate and 1 h accumulation (15-minute frames) staged as netCDF "
    "by the docker/opera-radar image.",
    "EUMETNET OPERA pan-European rain rate and 1 h accumulation, 2 km, 15-minute frames.",
)
opera_dbz_download, opera_dbz = _build_opera_assets(
    OPERAReflectivityProvider,
    opera_dbz_partitions,
    "One hour of OPERA composite reflectivity (5-minute frames) staged as netCDF by the "
    "docker/opera-radar image.",
    "EUMETNET OPERA pan-European composite reflectivity, 1 km, 5-minute frames.",
)

radar_assets = [
    uk_radar,
    fmi_radar,
    opera_rainfall_download,
    opera_rainfall,
    opera_dbz_download,
    opera_dbz,
]

__all__ = [
    "fmi_radar",
    "hourly_partitions",
    "opera_dbz",
    "opera_dbz_download",
    "opera_rainfall",
    "opera_rainfall_download",
    "radar_assets",
    "uk_radar",
]
