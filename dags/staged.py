"""Assets for pipelines that stage a partition in a Docker image and publish it here.

Some sources can only be read with libraries this project cannot install, so their
download runs in an image of its own and leaves the partition on a shared volume. Each
such pipeline is two assets:

``<name>_download``
    Runs the image through Dagster Pipes to stage one partition. Returns without starting
    a container when the store would not accept the partition anyway, because it holds it
    already or has moved past it.
``<name>``
    Depends on the download. Publishes what was staged and removes it.

Both are built from a *publisher*: a provider that knows its store and its staging
directory. :class:`StagedPublisher` lists what the builders need from one.

Note: no ``from __future__ import annotations``; Dagster needs the real annotation
objects on the asset functions.
"""

import os
import pathlib
from typing import Callable, Mapping, Optional, Protocol, Sequence

import dagster as dg
import pandas as pd
from dagster_docker import PipesDockerClient

from dags.factory import MEMORY_CLASS_TAG, MEMORY_GB_TAG, _format_gb, memory_class_for
from planetary_datasets.providers._timestamps import to_naive_utc


class StagedPublisher(Protocol):
    """What the staged-asset builders need from a provider."""

    store_path: str
    archive_root: pathlib.Path

    def appendable(self, start: pd.Timestamp) -> bool:
        """True when the store would accept the partition starting at ``start``.

        Partitions may arrive in any order, so this is False only for one already stored.
        """

    def partition_stored(self, start: pd.Timestamp) -> bool:
        """True when the partition is in the store."""

    def has_staged(self, start: pd.Timestamp) -> bool:
        """True when the partition is fully staged, ready to publish."""

    def run_partition(self, start: pd.Timestamp, check_present: bool = True) -> bool:
        """Publish the staged partition; True if anything was written."""

    def discard_staged(self, start: pd.Timestamp, settled: bool = False) -> list:
        """Remove the staging once the partition needs no further work."""


def partition_start(context: dg.AssetExecutionContext) -> pd.Timestamp:
    """The partition's start as naive UTC, which is how every store holds its times."""
    return to_naive_utc(context.partition_time_window.start)


def memory_tags(memory_gb: float) -> dict[str, str]:
    """Run-queue tags for a step that needs ``memory_gb``, as the provider factory sets."""
    return {MEMORY_CLASS_TAG: memory_class_for(memory_gb), MEMORY_GB_TAG: _format_gb(memory_gb)}


def host_user_container_kwargs(host_dir, container_dir: str, memory_gb: float) -> dict:
    """``docker run`` options: mount ``host_dir``, cap memory, and run as this user.

    Running as the host uid keeps the staged files owned by whoever runs Dagster, so the
    publish step (and anyone cleaning up) can remove them.
    """
    kwargs: dict = {
        "volumes": {str(host_dir): {"bind": container_dir, "mode": "rw"}},
        "mem_limit": f"{int(memory_gb)}g",
    }
    if hasattr(os, "getuid"):
        kwargs["user"] = f"{os.getuid()}:{os.getgid()}"
    return kwargs


def make_staged_download_asset(
    *,
    name: str,
    description: str,
    partitions_def: dg.PartitionsDefinition,
    publisher: Callable[[], StagedPublisher],
    image_env: str,
    default_image: str,
    container_dir: str,
    command: Callable[[pd.Timestamp], Sequence[str]],
    container_memory_gb: float,
    env: Optional[Callable[[], Mapping[str, str]]] = None,
    retry_policy: Optional[dg.RetryPolicy] = None,
    tags: Optional[Mapping[str, str]] = None,
    metadata: Optional[Mapping[str, object]] = None,
) -> dg.AssetsDefinition:
    """Build the asset that stages one partition by running ``image_env`` in Docker.

    ``publisher`` builds the provider that will publish the partition, and is asked first
    whether the store would accept it at all. ``command`` gives the container's arguments
    for a partition start, ``container_memory_gb`` both caps the container and sets the
    asset's memory class, and ``env`` supplies extra container environment per run
    (credentials, say). The rest are passed to :func:`dagster.asset`.
    """
    all_tags = {**memory_tags(container_memory_gb), **(tags or {})}

    @dg.asset(
        name=name,
        description=description,
        partitions_def=partitions_def,
        compute_kind="docker",
        metadata={"image_env": dg.MetadataValue.text(image_env), **(metadata or {})},
        retry_policy=retry_policy,
        tags=all_tags,
        op_tags={MEMORY_CLASS_TAG: all_tags[MEMORY_CLASS_TAG]},
    )
    def _asset(
        context: dg.AssetExecutionContext, pipes_docker_client: PipesDockerClient
    ) -> dg.MaterializeResult:
        it = partition_start(context)
        provider = publisher()
        # Checked here, not only downstream, so a backfill over partitions the store
        # holds or has moved past does not download them just to throw them away.
        # The only partition refused is one already stored: writes append out of order, so
        # one behind the store's end is ordinary work rather than a gap that can never be
        # filled. The axis is put back in order afterwards by the store's reorder asset.
        if not provider.appendable(it):
            reason = f"{it} already stored"
            return dg.MaterializeResult(metadata={"skipped": dg.MetadataValue.text(reason)})

        root = provider.archive_root.expanduser().resolve()
        root.mkdir(parents=True, exist_ok=True)
        return pipes_docker_client.run(
            image=os.environ.get(image_env, default_image),
            command=list(command(it)),
            env=dict(env()) if env else None,
            container_kwargs=host_user_container_kwargs(root, container_dir, container_memory_gb),
            context=context,
        ).get_materialize_result()

    return _asset


def make_staged_publish_asset(
    *,
    name: str,
    description: str,
    partitions_def: dg.PartitionsDefinition,
    download: dg.AssetsDefinition,
    publisher: Callable[[], StagedPublisher],
    memory_gb: float,
    compute_kind: str = "python",
    tags: Optional[Mapping[str, str]] = None,
    metadata: Optional[Mapping[str, object]] = None,
) -> dg.AssetsDefinition:
    """Build the asset that publishes a staged partition and clears the staging.

    Fails when the store would accept the partition but nothing is staged: the download
    either did not run or lost its output, and succeeding would leave a hole nothing
    revisits.
    """
    all_tags = {**memory_tags(memory_gb), **(tags or {})}

    @dg.asset(
        name=name,
        description=description,
        partitions_def=partitions_def,
        deps=[download],
        compute_kind=compute_kind,
        metadata=dict(metadata or {}),
        tags=all_tags,
        op_tags={MEMORY_CLASS_TAG: all_tags[MEMORY_CLASS_TAG]},
    )
    def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        it = partition_start(context)
        provider = publisher()
        written = False
        accepts = provider.appendable(it)
        if accepts:
            if not provider.has_staged(it):
                raise dg.Failure(description=f"{name}: nothing staged for {it}; run the download")
            # appendable already says the partition is not stored.
            written = provider.run_partition(it, check_present=False)
        removed = provider.discard_staged(it, settled=written or not accepts)
        context.log.info(f"{name}: {it} -> {provider.store_path} (written={written})")
        return dg.MaterializeResult(
            metadata={
                "written": dg.MetadataValue.bool(written),
                "partition": dg.MetadataValue.text(str(it)),
                "store_path": dg.MetadataValue.text(provider.store_path),
                "staged_files_removed": dg.MetadataValue.int(len(removed)),
            }
        )

    return _asset


__all__ = [
    "StagedPublisher",
    "host_user_container_kwargs",
    "make_staged_download_asset",
    "make_staged_publish_asset",
    "memory_tags",
    "partition_start",
]
