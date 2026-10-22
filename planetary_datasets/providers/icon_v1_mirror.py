"""Mirror whole ICON runs from DWD's open data server into the Source Cooperative archive.

DWD keeps about a day of each model under ``https://opendata.dwd.de/weather/nwp/v1/m``.
The archive at :data:`~planetary_datasets.providers.icon_v1_catalog.ARCHIVE_ROOT` holds a
byte-for-byte copy going back further, in the same layout with the run directory's colon
decoded::

    <model>/p/<PARAM>/[lvt1/<type>/lv1/<level>/]r/<YYYY-MM-DDTHH:MM>/s/PT<HHH>H<MM>M.grib2

and :mod:`scripts.icon_v1_backfill` builds the ICON v1 stores from it. This keeps that copy
growing: every pass, for each model, every run the server lists has every step of every
field it lacks copied across, straight from the server to the archive. Fields and their
steps are discovered as in :mod:`~planetary_datasets.providers.icon_d2_ruc_mirror`; what a
run already has is read from one archive listing per field, so a pass over runs already
mirrored makes no downloads.

Sizes, measured 2026-10-22: ICON-ART-EU ~15,100 files and 12 GB a run, ICON-ART ~26,900
files and 33 GB, four runs a day each. ICON-Global (~151,000 files, ~400 GB a run) works
the same way but is not mirrored by default.

    python -m planetary_datasets.providers.icon_v1_mirror icon-art icon-art-eu [--once]
"""

from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Sequence, Tuple

import fsspec
import pandas as pd
from loguru import logger

from planetary_datasets.common.download import read_bytes
from planetary_datasets.config import get_config
from planetary_datasets.providers.icon_d2_ruc_mirror import Field, Mirror
from planetary_datasets.providers.icon_v1_catalog import ARCHIVE_ROOT, LIVE_ROOT, ListingError

#: Mirror every step: no lead time is beyond this.
ALL_STEPS = pd.Timedelta(10_000, "h")
DEFAULT_MODELS = ("icon-art", "icon-art-eu")
_RUN_FORMAT = "%Y-%m-%dT%H:%M"


class ArchiveMirror(Mirror):
    """Copies whole runs of one model from DWD into the archive bucket."""

    def __init__(self, model: str, fs, archive_root: str = ARCHIVE_ROOT, workers: int = 16):
        """Build a mirror for one model.

        Args:
            model: DWD model directory, e.g. ``icon-art``.
            fs: Writable filesystem of the archive bucket, with listings cache off.
            archive_root: ``s3://`` root holding one directory per model.
            workers: Concurrent listings and copies.
        """
        super().__init__(
            archive_root.removeprefix("s3://").rstrip("/") + f"/{model}/p",
            base_url=f"{LIVE_ROOT}/{model}/p",
            workers=workers,
            max_lead=ALL_STEPS,
        )
        self.model = model
        self.label = model
        self.fs = fs
        self.dest = str(self.root)

    def key(self, field: Field, run: pd.Timestamp, step: str) -> str:
        """Archive key of one file."""
        return f"{self.dest}/{field.path}/r/{run.strftime(_RUN_FORMAT)}/s/{step}"

    def present(self, field: Field, run: pd.Timestamp) -> set[str]:
        """Step files the archive already has for ``field`` in ``run``."""
        prefix = f"{self.dest}/{field.path}/r/{run.strftime(_RUN_FORMAT)}/s"
        try:
            return {key.rsplit("/", 1)[-1] for key in self.fs.ls(prefix, detail=False)}
        except FileNotFoundError:
            return set()

    def missing(self, run: pd.Timestamp) -> List[Tuple[str, str]]:
        """``(url, key)`` of every file of ``run`` the archive lacks."""
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            present = list(pool.map(lambda field: self.present(field, run), self.fields))
        return [
            (self.url(field, run, step), self.key(field, run, step))
            for field, have in zip(self.fields, present)
            for step in field.steps
            if step not in have
        ]

    def copy(self, url: str, key: str) -> bool:
        """Copy one file; False when the server does not have it (yet)."""
        data = read_bytes(url, retries=3, timeout=300)
        if data is None:
            return False
        self.fs.pipe_file(key, data)
        return True

    def mirror_run(self, run: pd.Timestamp) -> Tuple[int, int]:
        """Copy what ``run`` lacks. Returns ``(copied, still missing)``."""
        todo = self.missing(run)
        if not todo:
            self.complete.add(run)
            return 0, 0

        def attempt(pair: Tuple[str, str]) -> bool:
            try:
                return self.copy(*pair)
            except Exception as exc:  # noqa: BLE001 - one file must not stop the run
                logger.warning(f"{self.model}: could not copy {pair[0]}: {exc!r}")
                return False

        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            copied = sum(pool.map(attempt, todo))
        left = len(todo) - copied
        if not left:
            self.complete.add(run)
        return copied, left


def archive_filesystem_rw():
    """The archive bucket with write credentials and no listings cache."""
    return fsspec.filesystem(
        "s3",
        **get_config().fsspec_storage_options(),
        use_listings_cache=False,
        skip_instance_cache=True,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Mirror the given models until interrupted, or once with ``--once``."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("models", nargs="*", default=list(DEFAULT_MODELS))
    parser.add_argument("--workers", type=int, default=16, help="concurrent copies")
    parser.add_argument("--pause", type=int, default=10800, help="seconds between passes")
    parser.add_argument("--refresh-hours", type=float, default=6.0, help="re-walk the trees")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)

    fs = archive_filesystem_rw()
    mirrors = [ArchiveMirror(model, fs, workers=args.workers) for model in args.models]
    walked = {mirror.model: 0.0 for mirror in mirrors}
    while True:
        began = time.time()
        for mirror in mirrors:
            try:
                if began - walked[mirror.model] > args.refresh_hours * 3600 or not mirror.fields:
                    mirror.discover()
                    walked[mirror.model] = began
                results = mirror.run_pass()
            except ListingError as exc:
                logger.warning(f"{mirror.model}: listing failed, retrying next pass: {exc}")
                continue
            copied = sum(c for c, _ in results.values())
            left = sum(m for _, m in results.values())
            logger.info(
                f"{mirror.model}: pass done, {len(results)} run(s) visited, "
                f"{copied} copied, {left} missing"
            )
        logger.info(f"icon v1 mirror: all models done in {time.time() - began:.0f}s")
        if args.once:
            return 0
        time.sleep(args.pause)


if __name__ == "__main__":
    sys.exit(main())
