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
from typing import Dict

import dagster as dg
import pandas as pd
from dagster_docker import PipesDockerClient

from dags.factory import MEMORY_CLASS_TAG, MEMORY_GB_TAG, SCHEDULE_TAG, memory_class_for
from planetary_datasets.config import MissingCredential, get_config
from planetary_datasets.providers.earth2studio_download import DATASETS, Dataset
from planetary_datasets.providers.earth2studio_obs import provider_for
from planetary_datasets.providers.radar import to_naive_utc

#: Image built by ``docker/earth2studio/build.sh``, shared with OPERA.
IMAGE_ENV = "EARTH2STUDIO_IMAGE"
DEFAULT_IMAGE = "planetary-datasets/earth2studio:latest"
#: Where the image expects the staging root to be mounted.
CONTAINER_ARCHIVE = "/data/earth2studio"
CATALOG_URL = "https://nvidia.github.io/earth2studio/main/userguide/about/catalog/"

#: Cron and key format for each partition length a dataset may declare.
PARTITIONINGS: Dict[str, tuple[str, str]] = {
    "10min": ("*/10 * * * *", "%Y-%m-%d-%H:%M"),
    "1h": ("0 * * * *", "%Y-%m-%d-%H:%M"),
    "6h": ("0 0,6,12,18 * * *", "%Y-%m-%d-%H:%M"),
    "1D": ("0 0 * * *", "%Y-%m-%d"),
    "1MS": ("0 0 1 * *", "%Y-%m-%d"),
    "1YS": ("0 0 1 1 *", "%Y-%m-%d"),
}

#: Environment variables the container may need, and the config field holding each.
CREDENTIALS = {
    "EUMETSAT_CONSUMER_KEY": "eumetsat_consumer_key",
    "EUMETSAT_CONSUMER_SECRET": "eumetsat_consumer_secret",
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
    """Memory class, concurrency pool and schedule opt-out for a dataset's assets."""
    tags = {
        MEMORY_CLASS_TAG: memory_class_for(dataset.memory_gb),
        MEMORY_GB_TAG: str(dataset.memory_gb),
        # One partition per source at a time: several of them rate-limit or share a
        # single upstream account.
        "dagster/concurrency_key": f"earth2studio-{dataset.source}",
    }
    if not dataset.scheduled:
        tags[SCHEDULE_TAG] = "manual"
    return tags


def download_command(dataset: Dataset, it: pd.Timestamp) -> list[str]:
    """Arguments for the image's entrypoint, for one partition."""
    return [
        "obs",
        "--dataset",
        dataset.name,
        "--time",
        it.strftime("%Y-%m-%dT%H:%M"),
        "--target",
        CONTAINER_ARCHIVE,
    ]


def container_env(dataset: Dataset) -> dict[str, str]:
    """Environment for the container: a private cache, and only the credentials it needs.

    Several earth2studio sources use a fixed temporary cache directory, so containers
    sharing one would delete each other's files; each run gets its own inside the
    container instead.
    """
    env = {"EARTH2STUDIO_CACHE": "/tmp/earth2studio-cache"}
    if dataset.credentials:
        fields = [CREDENTIALS[name] for name in dataset.credentials]
        try:
            values = get_config().credentials.require(*fields)
        except MissingCredential as exc:
            # A configuration error: retrying on the asset's backoff would only wait.
            raise dg.Failure(description=str(exc), allow_retries=False) from exc
        env.update(dict(zip(dataset.credentials, values)))
    # Optional: raises Planetary Computer's anonymous rate limit.
    if os.environ.get("PC_SDK_SUBSCRIPTION_KEY"):
        env["PC_SDK_SUBSCRIPTION_KEY"] = os.environ["PC_SDK_SUBSCRIPTION_KEY"]
    return env


def container_kwargs(dataset: Dataset, archive_root) -> dict:
    """``docker run`` options: the staging mount, the host user and a memory cap."""
    kwargs: dict = {
        "volumes": {str(archive_root): {"bind": CONTAINER_ARCHIVE, "mode": "rw"}},
        "mem_limit": f"{int(dataset.memory_gb) + 4}g",
    }
    if hasattr(os, "getuid"):
        kwargs["user"] = f"{os.getuid()}:{os.getgid()}"
    return kwargs


def build_download_asset(dataset: Dataset) -> dg.AssetsDefinition:
    """The asset that stages one partition of ``dataset`` in the earth2studio image."""
    tags = asset_tags(dataset)

    @dg.asset(
        name=f"{dataset.name}_download",
        description=f"Stage one partition: {dataset.description}",
        partitions_def=partitions_for(dataset),
        compute_kind="docker",
        metadata={
            "source": dg.MetadataValue.text(f"earth2studio.data.{dataset.source}"),
            "catalog": dg.MetadataValue.url(CATALOG_URL),
            "image_env": dg.MetadataValue.text(IMAGE_ENV),
        },
        retry_policy=dg.RetryPolicy(max_retries=3, delay=1800, backoff=dg.Backoff.EXPONENTIAL),
        tags=tags,
        op_tags={MEMORY_CLASS_TAG: tags[MEMORY_CLASS_TAG]},
    )
    def _asset(
        context: dg.AssetExecutionContext, pipes_docker_client: PipesDockerClient
    ) -> dg.MaterializeResult:
        it = to_naive_utc(context.partition_time_window.start)
        provider = provider_for(dataset.name)
        if provider.partition_stored(it):
            return dg.MaterializeResult(
                metadata={"skipped": dg.MetadataValue.text(f"{it} already stored")}
            )
        if not provider.appendable(it):
            context.log.warning(
                f"{dataset.name}: {it} is before the end of {provider.store_path}; the "
                "store only accepts appends in time order, so it cannot be filled"
            )
            return dg.MaterializeResult(
                metadata={"skipped": dg.MetadataValue.text(f"{it} predates the store's end")}
            )
        root = provider.archive_root.expanduser().resolve()
        root.mkdir(parents=True, exist_ok=True)
        return pipes_docker_client.run(
            image=os.environ.get(IMAGE_ENV, DEFAULT_IMAGE),
            command=download_command(dataset, it),
            env=container_env(dataset),
            container_kwargs=container_kwargs(dataset, root),
            context=context,
        ).get_materialize_result()

    return _asset


def build_publish_asset(dataset: Dataset, download: dg.AssetsDefinition) -> dg.AssetsDefinition:
    """The asset that publishes a staged partition of ``dataset`` and clears the staging."""
    tags = asset_tags(dataset)
    provider = provider_for(dataset.name)

    @dg.asset(
        name=dataset.name,
        description=dataset.description,
        partitions_def=partitions_for(dataset),
        deps=[download],
        compute_kind=dataset.publish,
        metadata={
            "store": dg.MetadataValue.text(provider.store_prefix),
            "format": dg.MetadataValue.text(dataset.publish),
            "source": dg.MetadataValue.text(f"earth2studio.data.{dataset.source}"),
        },
        tags=tags,
        op_tags={MEMORY_CLASS_TAG: tags[MEMORY_CLASS_TAG]},
    )
    def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        it = to_naive_utc(context.partition_time_window.start)
        publisher = provider_for(dataset.name)
        staged = publisher.manifest(it) is not None
        written = publisher.run_partition(it)
        if not written and not staged and publisher.appendable(it):
            if not publisher.partition_stored(it):
                raise dg.Failure(
                    description=f"{dataset.name}: nothing staged for {it}; run the download first"
                )
        removed = publisher.discard_staged(it)
        return dg.MaterializeResult(
            metadata={
                "written": dg.MetadataValue.bool(bool(written)),
                "partition": dg.MetadataValue.text(str(it)),
                "store_path": dg.MetadataValue.text(publisher.store_path),
                "staged_files_removed": dg.MetadataValue.int(len(removed)),
            }
        )

    return _asset


def _build_all() -> list[dg.AssetsDefinition]:
    built = []
    for dataset in DATASETS.values():
        download = build_download_asset(dataset)
        built += [download, build_publish_asset(dataset, download)]
    return built


earth2studio_obs_assets = _build_all()

__all__ = [
    "build_download_asset",
    "build_publish_asset",
    "earth2studio_obs_assets",
    "partitions_for",
]
