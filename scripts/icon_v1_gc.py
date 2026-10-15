"""Find, and optionally delete, the orphaned chunks in an ICON v1 variant's stores.

A run is written chunk by chunk and committed at the end. A run that never commits - the
backfill killed, the host restarted, a worker out of memory - leaves behind the chunks it
had already uploaded, referenced by no snapshot, and the multipart uploads it had begun
but not finished. Nothing reads either, but both are stored and billed.

Measured on the real stores (read only): ``icon_global_25082026.icechunk`` held 1.04 TB of
which 1.026 TB is referenced (~14 GB orphaned, ~1.4%, three committed runs and a few
killed ones); ``icon_eu_art.icechunk`` 1.00 TB of which 0.994 TB (~6 GB, ~0.6%).

For each generation of the variant this runs icechunk's garbage collection, which deletes
objects no branch or tag reaches, and lists the store's incomplete multipart uploads,
which are invisible to garbage collection and are aborted instead. If the bucket allows
it, an ``AbortIncompleteMultipartUpload`` lifecycle rule does the second part by itself.

Dry run by default: it reports and changes nothing - stores are opened, never created,
and generations are found by listing the store root, never by probing names. ``--delete``
deletes. Only objects and uploads older than ``--older-than-hours`` (24 by default) are
touched, so a run being written right now - the longest, ICON-Global, takes hours, never
a day - keeps its chunks even with the backfill running. Snapshots are never expired:
every committed run stays.

Usage::

    python scripts/icon_v1_gc.py global              # report
    python scripts/icon_v1_gc.py eu_art --delete     # delete orphans older than a day
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
import sys
import time
from typing import Callable

import icechunk
from loguru import logger

from planetary_datasets.config import Config, get_config
from planetary_datasets.providers.icon_v1 import STORE_PREFIXES, VARIANT_MODELS

#: Least age an object must have before it may be deleted, unless overridden.
DEFAULT_OLDER_THAN_HOURS = 24.0

#: Calls an S3 API operation by name with keyword arguments and returns its response.
S3Call = Callable[..., dict]


def list_generations(config: Config, base_prefix: str) -> list[str]:
    """Every store that is a generation of ``base_prefix``, by listing the store root.

    Unlike ``existing_generations``, never falls back to probing numbered names, which
    opens - and so creates - a repository at each.

    Raises:
        OSError: If the store root cannot be listed.
    """
    import fsspec

    parent, _, name = base_prefix.removesuffix(".icechunk").rpartition("/")
    pattern = re.compile(rf"^{re.escape(name)}(?:_[0-9_]+)?\.icechunk$")
    root = config.store_path(parent)
    options = {} if config.use_local_store else config.fsspec_storage_options()
    filesystem, path = fsspec.core.url_to_fs(root, **options)
    entries = [str(e).rstrip("/").rpartition("/")[2] for e in filesystem.ls(path, detail=False)]
    return sorted(f"{parent}/{entry}" for entry in entries if pattern.match(entry))


def _s3_call(config: Config) -> S3Call:
    import s3fs

    filesystem = s3fs.S3FileSystem(**config.fsspec_storage_options())
    return lambda operation, **kwargs: filesystem.call_s3(operation, **kwargs)


def stale_uploads(
    config: Config,
    prefix: str,
    cutoff: dt.datetime,
    delete: bool,
    call: S3Call | None = None,
) -> int:
    """Count, and with ``delete`` abort, a store's multipart uploads begun before ``cutoff``.

    Returns:
        How many there were. Always 0 for a local store, which has no multipart uploads.
    """
    if config.use_local_store:
        return 0
    call = call or _s3_call(config)
    key_prefix = config.full_prefix(prefix).rstrip("/") + "/"
    found = 0
    markers: dict = {}
    while True:
        page = call("list_multipart_uploads", Bucket=config.bucket, Prefix=key_prefix, **markers)
        for upload in page.get("Uploads", []):
            if upload["Initiated"] >= cutoff:
                continue
            found += 1
            if delete:
                call(
                    "abort_multipart_upload",
                    Bucket=config.bucket,
                    Key=upload["Key"],
                    UploadId=upload["UploadId"],
                )
        if not page.get("IsTruncated"):
            return found
        markers = {
            "KeyMarker": page["NextKeyMarker"],
            "UploadIdMarker": page["NextUploadIdMarker"],
        }


def collect(
    prefix: str,
    older_than: dt.timedelta,
    delete: bool,
    call: S3Call | None = None,
) -> dict:
    """Garbage-collect one store; return what was (or would be) deleted.

    Args:
        prefix: Store prefix. The store must exist; it is opened, never created.
        older_than: Only objects older than this are candidates.
        delete: Delete them; otherwise only report.
        call: S3 API caller, for tests.
    """
    config = get_config()
    repo = icechunk.Repository.open(config.icechunk_storage(prefix))
    cutoff = dt.datetime.now(dt.UTC) - older_than
    started = time.monotonic()
    summary = repo.garbage_collect(cutoff, dry_run=not delete)
    found = {
        "prefix": prefix,
        "chunks": summary.chunks_deleted,
        "bytes": summary.bytes_deleted,
        "manifests": summary.manifests_deleted,
        "snapshots": summary.snapshots_deleted,
        "uploads": stale_uploads(config, prefix, cutoff, delete, call),
        "seconds": time.monotonic() - started,
    }
    logger.info(
        f"{config.store_path(prefix)}: {'deleted' if delete else 'would delete'} "
        f"{found['chunks']:,} orphaned chunk(s), {found['bytes'] / 1e9:.2f} GB, "
        f"{found['manifests']} manifest(s), {found['snapshots']} unreferenced snapshot(s); "
        f"{'aborted' if delete else 'would abort'} {found['uploads']} unfinished upload(s) "
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
    prefixes = list_generations(get_config(), STORE_PREFIXES[args.variant])
    total = failed = 0
    for prefix in prefixes:
        try:
            total += collect(prefix, older_than, args.delete)["bytes"]
        except Exception as exc:  # noqa: BLE001 - one bad store must not hide the others
            failed += 1
            logger.exception(f"{prefix}: garbage collection failed: {exc}")
    logger.info(
        f"{args.variant}: {total / 1e9:.2f} GB {'deleted' if args.delete else 'deletable'} "
        f"across {len(prefixes)} store(s), {failed} failed"
    )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
