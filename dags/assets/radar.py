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
import os
from typing import Type

import dagster as dg
import pandas as pd
from dagster_docker import PipesDockerClient

from planetary_datasets.providers.opera import (
    OPERAProvider,
    OPERARainfallProvider,
    OPERAReflectivityProvider,
)
from planetary_datasets.providers.radar import (
    FMIRadarProvider,
    LocalArchiveRadarProvider,
    UKRadarProvider,
    to_naive_utc,
)

#: Both composites are indexed by the hour. The UK partition covers twelve five-minute
#: frames; the FMI partition covers the three accumulation windows published on the hour.
hourly_partitions = dg.HourlyPartitionsDefinition(
    start_date=dt.datetime(2020, 1, 1), end_offset=-1
)


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
#: Where the image expects the staging root to be mounted.
OPERA_CONTAINER_ARCHIVE = "/data/opera"
OPERA_SOURCE_URL = "https://eumetnet.github.io/openradardata-documentation/"

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


def opera_container_kwargs(archive_root) -> dict:
    """``docker run`` options: mount the staging root, and write into it as this user."""
    kwargs: dict = {
        "volumes": {str(archive_root): {"bind": OPERA_CONTAINER_ARCHIVE, "mode": "rw"}},
        # A reflectivity hour is twelve 3800x4400 frames plus their lat/lon grid.
        "mem_limit": "6g",
    }
    if hasattr(os, "getuid"):
        kwargs["user"] = f"{os.getuid()}:{os.getgid()}"
    return kwargs


def _build_opera_download_asset(
    provider_cls: Type[OPERAProvider],
    partitions_def: dg.PartitionsDefinition,
    description: str,
) -> dg.AssetsDefinition:
    """Build the asset that stages one hour of an OPERA product."""

    @dg.asset(
        name=f"{provider_cls.name}_download",
        description=description,
        partitions_def=partitions_def,
        compute_kind="docker",
        metadata={
            "source": dg.MetadataValue.url(OPERA_SOURCE_URL),
            "image_env": dg.MetadataValue.text(OPERA_IMAGE_ENV),
        },
        # The archive can lag real time; a failed hour is retried rather than left.
        retry_policy=dg.RetryPolicy(max_retries=3, delay=1800, backoff=dg.Backoff.EXPONENTIAL),
        tags={"dagster/concurrency_key": "opera-download"},
    )
    def _asset(
        context: dg.AssetExecutionContext, pipes_docker_client: PipesDockerClient
    ) -> dg.MaterializeResult:
        it = to_naive_utc(context.partition_time_window.start)
        provider = provider_cls()
        # Checked here, not only downstream, so a backfill over hours already in the
        # store does not download them all again just to throw them away.
        if provider.partition_stored(it):
            return dg.MaterializeResult(
                metadata={"skipped": dg.MetadataValue.text(f"{it} already in the store")}
            )
        if not provider.appendable(it):
            context.log.warning(
                f"{provider.name}: {it} is before the latest time in {provider.store_path}; "
                "the store only accepts appends in time order, so it cannot be filled"
            )
            return dg.MaterializeResult(
                metadata={"skipped": dg.MetadataValue.text(f"{it} predates the store's end")}
            )
        root = provider.archive_root.expanduser().resolve()
        root.mkdir(parents=True, exist_ok=True)
        return pipes_docker_client.run(
            image=os.environ.get(OPERA_IMAGE_ENV, DEFAULT_OPERA_IMAGE),
            command=opera_download_command(provider, it),
            container_kwargs=opera_container_kwargs(root),
            context=context,
        ).get_materialize_result()

    return _asset


def _build_opera_asset(
    provider_cls: Type[OPERAProvider],
    partitions_def: dg.PartitionsDefinition,
    download: dg.AssetsDefinition,
    description: str,
) -> dg.AssetsDefinition:
    """Build the asset that appends a staged OPERA hour to its store."""

    @dg.asset(
        name=provider_cls.name,
        description=description,
        partitions_def=partitions_def,
        deps=[download],
        compute_kind="python",
        metadata={
            "store": dg.MetadataValue.text(provider_cls.store_prefix),
            "archive_dir_env": dg.MetadataValue.text(provider_cls.archive_env),
            "source": dg.MetadataValue.url(OPERA_SOURCE_URL),
        },
    )
    def _asset(context) -> dg.MaterializeResult:
        it = to_naive_utc(context.partition_time_window.start)
        provider = provider_cls()
        written = provider.run_partition(it)
        removed = provider.discard_staged(it)
        context.log.info(f"{provider.name}: {it} -> {provider.store_path} (written={written})")
        return dg.MaterializeResult(
            metadata={
                "written": dg.MetadataValue.bool(written),
                "partition": dg.MetadataValue.text(str(it)),
                "store_path": dg.MetadataValue.path(provider.store_path),
                "staged_files_removed": dg.MetadataValue.int(len(removed)),
            }
        )

    return _asset


opera_rainfall_download = _build_opera_download_asset(
    OPERARainfallProvider,
    opera_rainfall_partitions,
    "One hour of OPERA rain rate and 1 h accumulation (15-minute frames) staged as netCDF "
    "by the docker/opera-radar image.",
)
opera_rainfall = _build_opera_asset(
    OPERARainfallProvider,
    opera_rainfall_partitions,
    opera_rainfall_download,
    "EUMETNET OPERA pan-European rain rate and 1 h accumulation, 2 km, 15-minute frames.",
)

opera_dbz_download = _build_opera_download_asset(
    OPERAReflectivityProvider,
    opera_dbz_partitions,
    "One hour of OPERA composite reflectivity (5-minute frames) staged as netCDF by the "
    "docker/opera-radar image.",
)
opera_dbz = _build_opera_asset(
    OPERAReflectivityProvider,
    opera_dbz_partitions,
    opera_dbz_download,
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
