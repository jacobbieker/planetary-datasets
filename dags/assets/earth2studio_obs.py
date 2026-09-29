"""Dagster assets for the earth2studio "direct observation" data sources.

Two assets per dataset in
:data:`planetary_datasets.providers.earth2studio_download.DATASETS`:

``<name>_download``
    Runs the ``docker/earth2studio`` image through Dagster Pipes to stage one partition.
    Skipped without starting a container when the partition is already stored, or when
    the icechunk store has moved past it and could no longer accept it.
``<name>``
    Depends on the download. Publishes the staged partition, to a Parquet dataset for the
    DataFrame sources or an icechunk store for the gridded ones, then deletes the staging.

Everything about a dataset (partition length, first partition, publication lag, memory,
whether it is scheduled) comes from its :class:`~planetary_datasets.providers.
earth2studio_download.Dataset` entry, so adding one is a registry change only.

Datasets whose every partition is heavy (full-disk imagers, hyperspectral sounders,
station-by-station networks) are tagged ``planetary/schedule: manual`` and left out of the
scheduled jobs; run them as backfills.

Note: no ``from __future__ import annotations``; Dagster needs the real annotation
objects on the asset functions.
"""

import datetime as dt
import os
from typing import Dict, Optional

import dagster as dg

from dags.factory import SCHEDULE_TAG
from dags.staged import make_staged_download_asset, make_staged_publish_asset
from planetary_datasets.config import MissingCredential, get_config
from planetary_datasets.providers.earth2studio_download import DATASETS, DEFAULT_TARGET, Dataset
from planetary_datasets.providers.earth2studio_obs import STORE_ROOT, provider_for

#: Image built by ``docker/earth2studio/build.sh``; OPERA runs it too.
IMAGE_ENV = "EARTH2STUDIO_IMAGE"
DEFAULT_IMAGE = "planetary-datasets/earth2studio:latest"
CATALOG_URL = "https://nvidia.github.io/earth2studio/main/userguide/about/catalog/"

#: Publishing a Parquet partition uploads a file; it needs no more than this.
PARQUET_PUBLISH_MEMORY_GB = 2

#: Cron and key format for each partition length a dataset may declare.
PARTITIONINGS: Dict[str, tuple[str, str]] = {
    "10min": ("*/10 * * * *", "%Y-%m-%d-%H:%M"),
    "1h": ("0 * * * *", "%Y-%m-%d-%H:%M"),
    "6h": ("0 0,6,12,18 * * *", "%Y-%m-%d-%H:%M"),
    "1D": ("0 0 * * *", "%Y-%m-%d"),
    "1MS": ("0 0 1 * *", "%Y-%m-%d"),
    "1YS": ("0 0 1 1 *", "%Y-%m-%d"),
}


def partitions_for(dataset: Dataset) -> dg.TimeWindowPartitionsDefinition:
    """The time-window partitioning a dataset declares."""
    cron, fmt = PARTITIONINGS[dataset.freq]
    return dg.TimeWindowPartitionsDefinition(
        start=dt.datetime.fromisoformat(dataset.start),
        end=dt.datetime.fromisoformat(dataset.end) if dataset.end else None,
        cron_schedule=cron,
        fmt=fmt,
        end_offset=dataset.end_offset,
    )


def asset_tags(dataset: Dataset) -> dict[str, str]:
    """Concurrency pool and schedule opt-out for a dataset's assets."""
    # One partition per source at a time: several rate-limit or share one upstream account.
    tags = {"dagster/concurrency_key": f"earth2studio-{dataset.source}"}
    if not dataset.scheduled:
        tags[SCHEDULE_TAG] = "manual"
    return tags


def download_command(dataset: Dataset, it) -> list[str]:
    """Arguments for the image's entrypoint, for one partition."""
    return [
        "obs",
        "--dataset",
        dataset.name,
        "--time",
        it.strftime("%Y-%m-%dT%H:%M"),
        "--target",
        DEFAULT_TARGET,
    ]


def container_env(dataset: Dataset) -> dict[str, str]:
    """Environment for the container: a private cache, and only the credentials it needs.

    Several earth2studio sources use a fixed temporary cache directory, so containers
    sharing one would delete each other's files; each run gets its own inside the
    container instead. Credential variables map onto the config field of the same name.
    """
    env = {"EARTH2STUDIO_CACHE": "/tmp/earth2studio-cache"}
    if dataset.credentials:
        try:
            values = get_config().credentials.require(*(v.lower() for v in dataset.credentials))
        except MissingCredential as exc:
            # A configuration error: retrying on the asset's backoff would only wait.
            raise dg.Failure(description=str(exc), allow_retries=False) from exc
        env.update(zip(dataset.credentials, values))
    # Optional: raises Planetary Computer's anonymous rate limit.
    if os.environ.get("PC_SDK_SUBSCRIPTION_KEY"):
        env["PC_SDK_SUBSCRIPTION_KEY"] = os.environ["PC_SDK_SUBSCRIPTION_KEY"]
    return env


def build_download_asset(
    dataset: Dataset, partitions_def: Optional[dg.TimeWindowPartitionsDefinition] = None
) -> dg.AssetsDefinition:
    """The asset that stages one partition of ``dataset`` in the earth2studio image."""
    return make_staged_download_asset(
        name=f"{dataset.name}_download",
        description=f"Stage one partition: {dataset.description}",
        partitions_def=partitions_def or partitions_for(dataset),
        publisher=lambda: provider_for(dataset.name),
        image_env=IMAGE_ENV,
        default_image=DEFAULT_IMAGE,
        container_dir=DEFAULT_TARGET,
        command=lambda it: download_command(dataset, it),
        # Headroom over the partition's working set for Python and the libraries.
        container_memory_gb=dataset.memory_gb + 4,
        env=lambda: container_env(dataset),
        retry_policy=dg.RetryPolicy(max_retries=3, delay=1800, backoff=dg.Backoff.EXPONENTIAL),
        tags=asset_tags(dataset),
        metadata={
            "source": dg.MetadataValue.text(f"earth2studio.data.{dataset.source}"),
            "catalog": dg.MetadataValue.url(CATALOG_URL),
        },
    )


def build_publish_asset(
    dataset: Dataset,
    download: dg.AssetsDefinition,
    partitions_def: Optional[dg.TimeWindowPartitionsDefinition] = None,
) -> dg.AssetsDefinition:
    """The asset that publishes a staged partition of ``dataset`` and clears the staging."""
    parquet = dataset.publish == "parquet"
    return make_staged_publish_asset(
        name=dataset.name,
        description=dataset.description,
        partitions_def=partitions_def or partitions_for(dataset),
        download=download,
        publisher=lambda: provider_for(dataset.name),
        # A Parquet partition is uploaded as-is; a grid is read back one staged file at a
        # time, which is what the dataset declares.
        memory_gb=PARQUET_PUBLISH_MEMORY_GB if parquet else dataset.memory_gb,
        compute_kind=dataset.publish,
        tags=asset_tags(dataset),
        metadata={
            "store": dg.MetadataValue.text(f"{STORE_ROOT}/{dataset.name}.{dataset.publish}"),
            "format": dg.MetadataValue.text(dataset.publish),
            "source": dg.MetadataValue.text(f"earth2studio.data.{dataset.source}"),
        },
    )


def _build_all() -> list[dg.AssetsDefinition]:
    built = []
    for dataset in DATASETS.values():
        partitions_def = partitions_for(dataset)
        download = build_download_asset(dataset, partitions_def)
        built += [download, build_publish_asset(dataset, download, partitions_def)]
    return built


earth2studio_obs_assets = _build_all()

__all__ = [
    "build_download_asset",
    "build_publish_asset",
    "earth2studio_obs_assets",
    "partitions_for",
]
