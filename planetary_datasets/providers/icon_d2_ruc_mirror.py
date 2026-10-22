"""Keep a local mirror of the first hour of every DWD ICON-D2-RUC run.

:class:`~planetary_datasets.providers.icon.ICOND2RUCProvider` reads the rapid update cycle
from a local copy of DWD's open data tree rather than from the server, and DWD keeps only
about a day of runs. This mirrors, for every run the server still lists, every step from
the analysis (``PT000H00M``) up to and including ``PT001H00M`` of every field, in the same
layout under the provider's :attr:`~planetary_datasets.providers.icon.ICOND2RUCProvider.grib_dir`::

    <PARAM>/[lvt1/<type>/lv1/<level>/]r/<YYYY-MM-DDTHH:MM>/s/PT<HHH>H<MM>M.grib2

Hourly fields contribute two steps per run, the 15-minute fields five and the 5-minute
fields thirteen. Which steps a field has is read once from the listing of its oldest run
on the server, which is finished; later runs are requested by name, so a steady pass makes
no listings at all. The tree of fields and levels is walked again every
``--refresh-hours``. Files are written atomically and never re-downloaded.

A run's first hour is about 2,800 files and 1.2 GB, roughly 29 GB a day, so local runs
older than ``--keep-days`` are deleted; the store lanes write each hour within a few.

    python -m planetary_datasets.providers.icon_d2_ruc_mirror [--once] [--pause 600]
"""

from __future__ import annotations

import argparse
import dataclasses
import pathlib
import re
import shutil
import sys
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Sequence, Tuple

import pandas as pd
from loguru import logger

from planetary_datasets.common.download import download_one
from planetary_datasets.providers.icon import ICOND2RUCProvider
from planetary_datasets.providers.icon_v1_catalog import (
    ListingError,
    _http_get,
    clean_names,
    parse_index_html,
)

LIVE_ROOT = "https://opendata.dwd.de/weather/nwp/v1/m/icon-d2-ruc/p"
#: Steps up to this lead time are mirrored, inclusive.
MAX_LEAD = pd.Timedelta(60, "min")
#: Field whose run listing stands for the runs on the server.
REFERENCE_PARAM = "T_2M"
_STEP = re.compile(r"^PT(\d{3})H(\d{2})M\.grib2$")
_RUN_FORMAT = "%Y-%m-%dT%H:%M"


def step_lead(name: str) -> pd.Timedelta | None:
    """Lead time of a step file name such as ``PT000H15M.grib2``; None for anything else."""
    match = _STEP.match(name)
    if not match:
        return None
    return pd.Timedelta(int(match[1]), "h") + pd.Timedelta(int(match[2]), "min")


def first_hour_steps(names: Sequence[str], max_lead: pd.Timedelta = MAX_LEAD) -> List[str]:
    """The step files in ``names`` with a lead time of at most ``max_lead``, in lead order."""
    leads = {name: step_lead(name) for name in names}
    return sorted(
        (name for name, lead in leads.items() if lead is not None and lead <= max_lead),
        key=lambda name: leads[name],
    )


def run_url_name(run: pd.Timestamp) -> str:
    """A run's directory name as the server addresses it, with the colon encoded."""
    return urllib.parse.quote(run.strftime(_RUN_FORMAT), safe="")


@dataclasses.dataclass(frozen=True)
class Field:
    """One field directory holding an ``r/<run>/s/`` tree.

    Attributes:
        path: Path below the ``p/`` root, e.g. ``T/lvt1/150/lv1/40``.
        steps: First-hour step file names the field publishes for each run.
    """

    path: str
    steps: Tuple[str, ...]


class Mirror:
    """Mirrors the first hour of each live ICON-D2-RUC run into ``root``."""

    def __init__(
        self,
        root: pathlib.Path,
        base_url: str = LIVE_ROOT,
        workers: int = 8,
        max_lead: pd.Timedelta = MAX_LEAD,
    ):
        """Build a mirror.

        Args:
            root: Local ``p/`` directory, the provider's ``grib_dir``.
            base_url: Server ``p/`` directory.
            workers: Concurrent listings and downloads.
            max_lead: Last step mirrored, inclusive.
        """
        self.root = pathlib.Path(root)
        #: Prefix of this mirror's log lines.
        self.label = "icon_d2_ruc mirror"
        self.base_url = base_url.rstrip("/")
        self.workers = workers
        self.max_lead = max_lead
        self.fields: List[Field] = []
        #: Runs every field of which is mirrored; never visited again.
        self.complete: set[pd.Timestamp] = set()

    def list_dir(self, relative: str) -> List[str]:
        """Child names of a server directory; empty when it does not exist."""
        relative = relative.strip("/")
        url = f"{self.base_url}/{relative}/" if relative else f"{self.base_url}/"
        try:
            return clean_names(parse_index_html(_http_get(url).text))
        except FileNotFoundError:
            return []

    def runs(self) -> List[pd.Timestamp]:
        """Runs the server lists, oldest first."""
        names = self.list_dir(f"{REFERENCE_PARAM}/r")
        return sorted(pd.Timestamp(name) for name in names)

    def discover(self) -> List[Field]:
        """Walk the tree for every field directory and the first-hour steps it publishes.

        A field's steps are the union over its three oldest listed runs, which DWD has
        finished; the oldest alone may be part-way through being deleted. A field none of
        whose runs shows a first-hour step yet is skipped until the next walk.
        """

        def describe(path: str) -> Field | None:
            runs = sorted(pd.Timestamp(name) for name in self.list_dir(f"{path}/r"))
            names: set[str] = set()
            for run in runs[:3]:
                names.update(self.list_dir(f"{path}/r/{run_url_name(run)}/s"))
            steps = first_hour_steps(sorted(names), self.max_lead)
            return Field(path, tuple(steps)) if steps else None

        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            layer = self.list_dir("")
            parents: List[str] = []
            while layer:
                listed = list(pool.map(self.list_dir, layer))
                parents.extend(path for path, kids in zip(layer, listed) if "r" in kids)
                layer = [
                    f"{path}/{kid}"
                    for path, kids in zip(layer, listed)
                    if "r" not in kids
                    for kid in kids
                ]
            fields = [f for f in pool.map(describe, sorted(parents)) if f is not None]
        logger.info(
            f"{self.label}: {len(fields)} field(s), "
            f"{sum(len(f.steps) for f in fields)} first-hour file(s) per run"
        )
        self.fields = fields
        return fields

    def local_path(self, field: Field, run: pd.Timestamp, step: str) -> pathlib.Path:
        """Where a file lands, laid out as the provider reads it."""
        return self.root / field.path / "r" / run.strftime(_RUN_FORMAT) / "s" / step

    def url(self, field: Field, run: pd.Timestamp, step: str) -> str:
        """Server URL of one file."""
        return f"{self.base_url}/{field.path}/r/{run_url_name(run)}/s/{step}"

    def missing(self, run: pd.Timestamp) -> List[Tuple[str, pathlib.Path]]:
        """``(url, destination)`` of every first-hour file of ``run`` not yet mirrored."""
        return [
            (self.url(field, run, step), path)
            for field in self.fields
            for step in field.steps
            if not (path := self.local_path(field, run, step)).is_file()
        ]

    def mirror_run(self, run: pd.Timestamp) -> Tuple[int, int]:
        """Download what ``run`` lacks. Returns ``(downloaded, still missing)``."""
        todo = self.missing(run)
        if not todo:
            self.complete.add(run)
            return 0, 0
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            got = list(pool.map(lambda pair: download_one(*pair, retries=2), todo))
        downloaded = sum(path is not None for path in got)
        left = len(todo) - downloaded
        if not left:
            self.complete.add(run)
        return downloaded, left

    def prune(self, keep: pd.Timedelta, now: pd.Timestamp | None = None) -> int:
        """Delete local runs initialised more than ``keep`` before ``now``. Returns how many.

        Every field directory under ``root`` is visited, not only the ones the last walk
        found, so a field DWD has since dropped is pruned too.
        """
        now = now if now is not None else pd.Timestamp.now(tz="UTC").tz_localize(None)
        cutoff = now - keep
        removed = 0
        for runs_dir in self.root.glob("**/r"):
            for run_dir in runs_dir.iterdir():
                try:
                    run = pd.Timestamp(run_dir.name)
                except ValueError:
                    continue
                if run < cutoff:
                    shutil.rmtree(run_dir, ignore_errors=True)
                    removed += 1
        if removed:
            logger.info(f"{self.label}: pruned {removed} run director(ies) before {cutoff}")
        return removed

    def run_pass(self) -> Dict[pd.Timestamp, Tuple[int, int]]:
        """Mirror every listed run not already complete, oldest first."""
        listed = self.runs()
        self.complete &= set(listed)
        results = {}
        for run in listed:
            if run in self.complete:
                continue
            results[run] = self.mirror_run(run)
            downloaded, left = results[run]
            if downloaded or left:
                logger.info(f"{self.label}: {run}: {downloaded} downloaded, {left} missing")
        return results


def main(argv: Sequence[str] | None = None) -> int:
    """Mirror until interrupted, or once with ``--once``."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--root", type=pathlib.Path, default=None, help="local p/ directory")
    parser.add_argument("--workers", type=int, default=8, help="concurrent downloads")
    parser.add_argument("--pause", type=int, default=600, help="seconds between passes")
    parser.add_argument("--refresh-hours", type=float, default=6.0, help="re-walk the tree")
    parser.add_argument(
        "--keep-days", type=float, default=3.0, help="delete local runs older than this"
    )
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)

    root = args.root or ICOND2RUCProvider().grib_dir
    mirror = Mirror(root, workers=args.workers)
    logger.info(f"icon_d2_ruc mirror: {LIVE_ROOT} -> {root}")
    walked = 0.0
    while True:
        began = time.time()
        try:
            if began - walked > args.refresh_hours * 3600 or not mirror.fields:
                mirror.discover()
                walked = began
            results = mirror.run_pass()
            mirror.prune(pd.Timedelta(args.keep_days, "D"))
        except ListingError as exc:
            logger.warning(f"icon_d2_ruc mirror: listing failed, retrying next pass: {exc}")
            results = {}
        downloaded = sum(d for d, _ in results.values())
        left = sum(m for _, m in results.values())
        logger.info(
            f"icon_d2_ruc mirror: pass done in {time.time() - began:.0f}s, "
            f"{len(results)} run(s) visited, {downloaded} downloaded, {left} missing"
        )
        if args.once:
            return 0
        time.sleep(args.pause)


if __name__ == "__main__":
    sys.exit(main())
