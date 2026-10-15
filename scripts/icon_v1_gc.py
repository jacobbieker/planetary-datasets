"""Find, and optionally delete, the orphaned chunks in an ICON v1 variant's stores.

A run is written chunk by chunk and committed at the end. A run that never commits - the
backfill killed, the host restarted, a worker out of memory - leaves the chunks it had
already uploaded in the store, referenced by no snapshot. Nothing reads them, but they are
stored and billed. icechunk's garbage collection removes objects no branch or tag can
reach.

Measured on the real stores (read only): ``icon_global_25082026.icechunk`` held 1.04 TB of
which 1.026 TB is referenced (~14 GB orphaned, ~1.4%, three committed runs and a few
killed ones); ``icon_eu_art.icechunk`` 1.00 TB of which 0.994 TB (~6 GB, ~0.6%).

Dry run by default: it reports what would go. ``--delete`` deletes. Only objects older
than ``--older-than-hours`` (24 by default) are considered, so a run being written right
now - the longest, ICON-Global, takes hours, never a day - keeps its chunks even if the
backfill is running. Snapshots are never expired: every committed run stays.

Usage::

    python scripts/icon_v1_gc.py global              # report
    python scripts/icon_v1_gc.py eu_art --delete     # delete orphans older than a day
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
import time

from loguru import logger

from planetary_datasets.common.generations import existing_generations
from planetary_datasets.config import get_config
from planetary_datasets.providers.icon_v1 import STORE_PREFIXES, VARIANT_MODELS

#: Least age an object must have before it may be deleted, unless overridden.
DEFAULT_OLDER_THAN_HOURS = 24.0


def collect(prefix: str, older_than: dt.timedelta, delete: bool) -> dict:
    """Garbage-collect one store; return what was (or would be) deleted.

    Args:
        prefix: Store prefix.
        older_than: Only objects older than this are candidates.
        delete: Delete them; otherwise only report.
    """
    config = get_config()
    repo = config.icechunk_repo(prefix)
    cutoff = dt.datetime.now(dt.UTC) - older_than
    started = time.monotonic()
    summary = repo.garbage_collect(cutoff, dry_run=not delete)
    found = {
        "prefix": prefix,
        "chunks": summary.chunks_deleted,
        "bytes": summary.bytes_deleted,
        "manifests": summary.manifests_deleted,
        "snapshots": summary.snapshots_deleted,
        "transaction_logs": summary.transaction_logs_deleted,
        "seconds": time.monotonic() - started,
    }
    logger.info(
        f"{config.store_path(prefix)}: {'deleted' if delete else 'would delete'} "
        f"{found['chunks']:,} orphaned chunk(s), {found['bytes'] / 1e9:.2f} GB, "
        f"{found['manifests']} manifest(s), {found['snapshots']} snapshot(s) "
        f"(older than {cutoff:%Y-%m-%dT%H:%M}Z; {found['seconds']:.1f}s)"
    )
    return found


def main(argv: list[str] | None = None) -> int:
    """Command-line entry point. Returns the exit code."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("variant", choices=sorted(VARIANT_MODELS))
    parser.add_argument(
        "--older-than-hours",
        type=float,
        default=DEFAULT_OLDER_THAN_HOURS,
        help="only objects older than this are deleted (default 24)",
    )
    parser.add_argument("--delete", action="store_true", help="delete; default is a dry run")
    args = parser.parse_args(argv)

    if args.delete and args.older_than_hours < 12:
        logger.warning(
            f"--older-than-hours {args.older_than_hours}: a run being written now may have "
            "chunks that old; only do this with the backfill and Dagster asset stopped"
        )
    older_than = dt.timedelta(hours=args.older_than_hours)
    prefixes = existing_generations(get_config(), STORE_PREFIXES[args.variant])
    total = sum(collect(prefix, older_than, args.delete)["bytes"] for prefix in prefixes)
    logger.info(
        f"{args.variant}: {total / 1e9:.2f} GB {'deleted' if args.delete else 'deletable'} "
        f"across {len(prefixes)} store(s)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
