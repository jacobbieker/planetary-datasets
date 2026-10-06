"""Dagster assets for the NSRDB virtual Icechunk stores.

One unpartitioned asset per dataset in the NSRDB catalog. A materialisation
lists NREL's bucket and ingests every year the store does not hold yet, oldest
first, one commit per year — so a run interrupted part way resumes from the
next year, and a rerun with nothing new is a cheap no-op.

The data stays in NREL's bucket: each store holds only virtual references,
``time``, a ``valid`` mask and the site table. The expensive part is finding
where every chunk lives, which for a full-disc year runs to hours; chunk
indexes are cached under the configured scratch directory so a failed run
does not start over. A run cut short by the instance's run timeout resumes
from the next uncommitted year with those indexes.

These are tagged manual: a new NSRDB year appears once a year, and an
ingest is heavy enough that nobody wants it fired by a timer. Each dataset has
its own concurrency key, because two writers on one store race.

Run tags ``nsrdb/workers`` and ``nsrdb/groups`` (comma separated) override the
indexing processes and the variable groups for one run.
"""

# No `from __future__ import annotations` here: Dagster validates the
# `context` parameter against the *unevaluated* annotation, so stringified
# annotations make it reject the asset.

from typing import Optional

import dagster as dg
from dagster import AssetExecutionContext

from dags.factory import SCHEDULE_TAG
from planetary_datasets.config import get_config
from planetary_datasets.providers.virtualized import nsrdb

#: Processes indexing chunks over S3. The work is latency bound, not CPU bound.
DEFAULT_WORKERS = 8


def _run_options(tags: dict) -> tuple[int, Optional[list[str]]]:
    """Read ``nsrdb/workers`` and ``nsrdb/groups`` off the run tags."""
    workers = DEFAULT_WORKERS
    groups: Optional[list[str]] = None
    for key, value in (tags or {}).items():
        if key == "nsrdb/workers":
            workers = int(value)
        elif key == "nsrdb/groups":
            groups = [g.strip() for g in value.split(",") if g.strip()] or None
    return workers, groups


def _run_ingest(context: AssetExecutionContext, key: str) -> dg.MaterializeResult:
    cfg = get_config()
    workers, groups = _run_options(getattr(context.run, "tags", {}))
    context.log.info(
        f"NSRDB {key}: ingesting missing years with {workers} workers, "
        f"groups {groups or 'all'}, store {cfg.store_path(nsrdb.store_prefix_for(key))}"
    )
    report = nsrdb.ingest(
        key,
        groups=groups,
        workers=workers,
        cache_dir=cfg.scratch_dir / "nsrdb_chunk_index",
    )
    layouts = {str(y): p for y, p in report.layouts.items() if p}
    return dg.MaterializeResult(
        metadata={
            "dataset": key,
            "store": dg.MetadataValue.path(report.store),
            "years_written": dg.MetadataValue.json(report.written),
            "years_backfilled": dg.MetadataValue.json(report.backfilled),
            "years_already_present": dg.MetadataValue.json(report.already_present),
            "years_incomplete_in_bucket": dg.MetadataValue.json(report.incomplete),
            "years_held_back": dg.MetadataValue.json(report.held_back),
            "layout_groups": dg.MetadataValue.json(layouts),
            "peak_memory_gb": round(report.peak_memory_gb, 2),
            "seconds": round(report.seconds, 1),
        }
    )


def _build_asset(key: str) -> dg.AssetsDefinition:
    dataset = nsrdb.DATASETS[key]

    @dg.asset(
        name=f"nsrdb_{key}",
        description=(
            f"Virtual Icechunk store of {dataset.description}: every variable group and "
            "year on one time axis, referencing NREL's HDF5 files in s3://nrel-pds-nsrdb."
        ),
        compute_kind="virtualizarr",
        tags={SCHEDULE_TAG: "manual"},
        op_tags={"dagster/concurrency_key": f"nsrdb-{key}"},
    )
    def _asset(context: AssetExecutionContext) -> dg.MaterializeResult:
        return _run_ingest(context, key)

    return _asset


nsrdb_goes_full_disc_v4 = _build_asset("goes_full_disc_v4")
nsrdb_msg_v4 = _build_asset("msg_v4")
nsrdb_polar_v4 = _build_asset("polar_v4")
nsrdb_himawari8 = _build_asset("himawari8")
nsrdb_himawari7 = _build_asset("himawari7")
nsrdb_meteosat = _build_asset("meteosat")

all_assets = [
    nsrdb_goes_full_disc_v4,
    nsrdb_msg_v4,
    nsrdb_polar_v4,
    nsrdb_himawari8,
    nsrdb_himawari7,
    nsrdb_meteosat,
]
