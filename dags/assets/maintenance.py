"""Manual maintenance on the icechunk stores. Nothing here runs on a schedule.

``reorder_store``
    Sort a store's append axis after out-of-order writes.

Writes append out of order: a step that sorts before the end of the store still goes on
the end, because refusing it is what used to lose it. A backfill therefore leaves the
store holding the right values in the wrong sequence — every timestamp still paired with
its own data, so ``.sel(time=t)`` is exact, but ``.sel(time=slice(...))`` over the store
is unreliable until the axis is put back in order.

This asset does that, by remapping chunks through
:meth:`icechunk.Session.reindex_array`: a manifest rewrite, so it costs the same whether
the store holds a day or a decade, and no chunk is read or copied.

**It must not run while anything is writing to the store**, which is why it is manual and
takes its target from run config rather than existing as one asset per ingest. Reordering
moves chunks, so a writer that had already looked up a timestep's position would write it
to the wrong index. Materialise it once a backfill has finished and the ingest is quiet.

In the Dagster UI: *Materialize* → *Open launchpad*, and give it the store::

    ops:
      reorder_store:
        config:
          store_prefix: bkr/mrms/mrms.icechunk
          append_dim: time

``store_prefix`` is the store's location relative to the configured bucket — the same
string a provider carries as ``store_prefix``, which
:meth:`planetary_datasets.config.Config.store_path` resolves to S3 or a local directory.
"""

# NB: no `from __future__ import annotations`. Dagster inspects the raw `context`
# annotation on an asset function and rejects it once PEP 563 turns it into a string.

import dagster as dg
from dagster import AssetExecutionContext

from dags.factory import MAINTENANCE_GROUP, SCHEDULE_TAG
from planetary_datasets.common.store import (
    axis_is_sorted,
    existing_times,
    sort_append_axis,
)
from planetary_datasets.config import get_config


class ReorderConfig(dg.Config):
    """Which store to reorder, and along which dimension."""

    store_prefix: str
    append_dim: str = "time"
    #: Report what would change without writing anything. The check is a coordinate read,
    #: so this is cheap and safe to run against a store that is being written.
    dry_run: bool = False


@dg.asset(
    name="reorder_store",
    description="Sort an icechunk store's append axis after out-of-order writes (manual)",
    group_name=MAINTENANCE_GROUP,
    # Keeps it out of every scheduled job; see dags.loader.build_memory_class_jobs.
    tags={SCHEDULE_TAG: "manual"},
    kinds={"icechunk"},
)
def reorder_store(context: AssetExecutionContext, config: ReorderConfig) -> dg.MaterializeResult:
    """Put one store's append axis back in order."""
    cfg = get_config()
    repo = cfg.icechunk_repo(config.store_prefix)
    store_path = cfg.store_path(config.store_prefix)

    steps = existing_times(repo, append_dim=config.append_dim).size
    if axis_is_sorted(repo, append_dim=config.append_dim):
        context.log.info(
            f"{store_path}: {config.append_dim} is already in order over {steps} step(s)"
        )
        return dg.MaterializeResult(
            metadata={
                "store": dg.MetadataValue.text(store_path),
                "append_dim": dg.MetadataValue.text(config.append_dim),
                "steps": dg.MetadataValue.int(int(steps)),
                "was_sorted": dg.MetadataValue.bool(True),
                "changed": dg.MetadataValue.bool(False),
            }
        )

    if config.dry_run:
        context.log.warning(
            f"{store_path}: {config.append_dim} is out of order over {steps} step(s); "
            "dry_run is set, so nothing was written"
        )
        return dg.MaterializeResult(
            metadata={
                "store": dg.MetadataValue.text(store_path),
                "append_dim": dg.MetadataValue.text(config.append_dim),
                "steps": dg.MetadataValue.int(int(steps)),
                "was_sorted": dg.MetadataValue.bool(False),
                "changed": dg.MetadataValue.bool(False),
                "dry_run": dg.MetadataValue.bool(True),
            }
        )

    context.log.info(
        f"{store_path}: sorting {config.append_dim} over {steps} step(s) by remapping "
        "chunks. Nothing else may be writing to this store while this runs."
    )
    changed = sort_append_axis(repo, append_dim=config.append_dim)
    return dg.MaterializeResult(
        metadata={
            "store": dg.MetadataValue.text(store_path),
            "append_dim": dg.MetadataValue.text(config.append_dim),
            "steps": dg.MetadataValue.int(int(steps)),
            "was_sorted": dg.MetadataValue.bool(False),
            "changed": dg.MetadataValue.bool(changed),
        }
    )


__all__ = ["ReorderConfig", "reorder_store"]
