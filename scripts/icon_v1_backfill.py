"""Backfill an ICON v1 store from the Source Cooperative archive, oldest run first.

The Dagster assets ``icon_global``, ``icon_global_art`` and ``icon_eu_art`` write one run
per partition through :class:`~planetary_datasets.providers.icon_v1.ICONV1Provider`. This
walks the archive's runs with the same provider, in time order, one run at a time: the
provider already reads, decodes and writes in parallel inside a memory ceiling, and a
second writer would only race the first.

**Each run is written by a fresh process.** A worker is spawned for one partition and
exits when it is done (:func:`~planetary_datasets.providers.icon_v1.run_partition_child`),
so nothing a run leaves behind in the process can slow the next one. That was not
hypothetical: s3fs kept every directory listing it made, and a backfill left running for a
day slowed to a quarter of its starting speed as garbage collection walked millions of
cached entries. The listing cache is now off, and the fresh process guards against
whatever else accumulates; it costs a few seconds of imports per run, against minutes of
writing. Still one partition at a time, so still one writer per store. ``--in-process``
runs everything in this process instead, for debugging.

Runs already stored, in any generation of the store, are skipped, so the script resumes
where it stopped. A run that fails is logged and the walk goes on; run the script again to
retry it.

**Incomplete runs are skipped, not written.** The archive mirror lacks whole parameters of
some runs. Each run's directories are listed first, and a run holding less than
``--min-completeness`` of its expected fields (95% by default) is left out, so it can be
written later if the mirror fills in. Skipped runs are listed at the end and appended to
``--skipped-log`` (a TSV, ``logs/icon_v1_<variant>_skipped.tsv`` by default), with their
fraction and the parameters missing most. A run at or above the threshold is written with
NaN where its fields are missing.

**Grid eras.** ICON-ART and ICON-ART-EU changed grids with the 2026-10-06T00 run. Walking
forward from the archive's start, the old-grid runs fill the base store
(``icon_global_art.icechunk``, ``icon_eu_art.icechunk``) and the first new-grid run starts
a generation named after its day (``..._06102026.icechunk``), because its grid, and so its
schema fingerprint, differs.

Before running it:

* **Make it the only writer.** Pause the variant's Dagster asset (or its schedule and any
  backfill of it) for as long as this runs. Two writers on one store race their commits.
* **Run it before enabling the live schedule.** A new generation is named after the first
  run written to it, and appends are cheapest in time order. Backfilling first means the
  ICON-Global generation is named after its earliest written run and the live runs then
  append after it.
* **No empty generations.** An interrupted first write leaves a repository named for its
  day with nothing committed; the script refuses to start while one exists (see
  :meth:`~planetary_datasets.providers.icon_v1.ICONV1Provider.empty_generations`) and
  names it. Delete it by hand once sure nothing is writing to it.
* Runs written out of order (for example a gap filled after later runs were stored) leave
  ``init_time`` unsorted. Materialise the manual ``reorder_store`` asset
  (``dags/assets/maintenance.py``) with this store's prefix and ``append_dim: init_time``
  once nothing is writing to it.

Usage::

    python scripts/icon_v1_backfill.py global --limit 10 --dry-run   # completeness only
    python scripts/icon_v1_backfill.py eu_art --start 2025-09-30
    python scripts/icon_v1_backfill.py global --min-completeness 0.9
    python scripts/icon_v1_backfill.py global_art --params T_2M TOT_PREC  # a test subset

``--params`` writes a subset of the model, which has a different schema from the full
store and so goes to a generation of its own; it is for trying the pipeline out, not for
filling the production stores.
"""

from __future__ import annotations

import argparse
import pathlib
import sys
import time
from typing import Callable

import pandas as pd
from loguru import logger

from planetary_datasets.common.store import axis_is_sorted
from planetary_datasets.memory import process_tree_rss_gb
from planetary_datasets.providers.icon_v1 import (
    DEFAULT_MIN_COMPLETENESS,
    VARIANT_MODELS,
    ICONV1Provider,
    PartitionResult,
    RunCompleteness,
    run_in_child,
    run_partition_child,
)
from planetary_datasets.providers.icon_v1_catalog import list_runs

#: Least memory a child is given, whatever the parent holds, in GB.
MIN_CHILD_CEILING_GB = 1.0

#: Columns of the skipped-runs log.
SKIPPED_COLUMNS = (
    "logged_at",
    "init_time",
    "source",
    "fraction",
    "present",
    "expected",
    "most_missing",
)


def plan(
    provider: ICONV1Provider,
    start: pd.Timestamp | None,
    end: pd.Timestamp | None,
    live: bool = True,
) -> tuple[list[pd.Timestamp], list[pd.Timestamp]]:
    """The runs in ``[start, end]``, and those of them not stored yet, in the order to write.

    Runs come from the archive and, with ``live``, from DWD's live server too, which the
    provider reads a run from while it is under a day old. Runs only the live server has
    are written first, newest last: DWD deletes them after about a day, and a long archive
    backlog walked first would let them expire. The rest follow oldest first. Writing out of
    order leaves ``init_time`` unsorted until the store's reorder asset runs.

    Args:
        provider: Provider of the variant to backfill.
        start: First run to consider, or None for the earliest available.
        end: Last run to consider, or None for the latest available.
        live: Also consider runs DWD's live server lists.

    Returns:
        ``(available, missing)``: every run in range oldest first, and the missing ones in
        the order to write them.
    """
    archived = set(list_runs(provider.model, "archive"))
    listed = set(list_runs(provider.model, "live")) if live else set()
    runs = pd.DatetimeIndex(sorted(archived | listed))
    if start is not None:
        runs = runs[runs >= start]
    if end is not None:
        runs = runs[runs <= end]
    missing = set(provider.missing_timesteps(runs)) if len(runs) else set()
    live_only = sorted(run for run in missing if run not in archived)
    return list(runs), live_only + sorted(missing - set(live_only))


def log_skipped(path: pathlib.Path, completeness: RunCompleteness) -> None:
    """Append one skipped run to the TSV at ``path``, writing a header for a new file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    new = not path.exists()
    with path.open("a", encoding="utf-8") as out:
        if new:
            out.write("\t".join(SKIPPED_COLUMNS) + "\n")
        row = (
            pd.Timestamp.now("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
            completeness.init_time.isoformat(),
            completeness.source,
            f"{completeness.fraction:.4f}",
            str(completeness.present),
            str(completeness.expected),
            completeness.most_missing(),
        )
        out.write("\t".join(row) + "\n")


def report_completeness(provider: ICONV1Provider, runs: list[pd.Timestamp]) -> None:
    """List each run's directories and log how complete it is. Nothing is read or written.

    Each run is listed in the source the backfill itself would read it from.
    """
    for it in runs:
        source = provider.source_for(it)
        if source is None:
            logger.info(f"{provider.name}: {it:%Y-%m-%dT%H:%M} is in no source")
            continue
        _, listed = provider.list_run(it, source)
        verdict = "write" if listed.fraction >= provider.min_completeness else "SKIP"
        era = provider.plan_for(it).era
        logger.info(
            f"{provider.name}: {it:%Y-%m-%dT%H:%M} grid {era.grid_number}, {source}: "
            f"{listed.present:,}/{listed.expected:,} fields ({listed.fraction:.1%}) "
            f"listed in {listed.seconds:.1f}s -> {verdict}"
            + (f"; most missing: {listed.most_missing(5)}" if listed.fraction < 1 else "")
        )


class PartitionRunner:
    """Runs one partition at a time, each in a freshly spawned process unless told not to.

    The child (:func:`~planetary_datasets.providers.icon_v1.run_in_child`) imports the
    library afresh, builds its provider, writes its run and exits. It lives no longer than
    this process: it ends itself if this one is killed, and an interrupt here terminates
    it, so a restarted backfill never finds a second writer still at work.

    The child's memory ceiling is the configured one less what this process holds, so the
    two together stay within ``--ceiling-gb``.
    """

    def __init__(
        self,
        variant: str,
        provider_kwargs: dict,
        isolated: bool = True,
        target: Callable[[str, pd.Timestamp, dict], PartitionResult] = run_partition_child,
    ) -> None:
        """Set up a runner.

        Args:
            variant: Provider variant.
            provider_kwargs: Keyword arguments for the provider each run builds.
            isolated: Run each partition in a fresh spawned process.
            target: What runs one partition; module-level so a child can import it.
        """
        self.variant = variant
        self.provider_kwargs = provider_kwargs
        self.isolated = isolated
        self.target = target

    def child_kwargs(self) -> dict:
        """The provider arguments for a child: its ceiling less this process's memory."""
        kwargs = dict(self.provider_kwargs)
        ceiling = kwargs.get("ceiling_gb")
        if ceiling is not None:
            kwargs["ceiling_gb"] = max(MIN_CHILD_CEILING_GB, ceiling - process_tree_rss_gb())
        return kwargs

    def __call__(self, it: pd.Timestamp) -> PartitionResult:
        """Write one run; raises whatever the run raised, or ChildDied."""
        if not self.isolated:
            return self.target(self.variant, it, self.provider_kwargs)
        return run_in_child(self.target, self.variant, it, self.child_kwargs())


def main(argv: list[str] | None = None) -> int:
    """Command-line entry point. Returns the exit code."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("variant", choices=sorted(VARIANT_MODELS))
    parser.add_argument("--start", type=pd.Timestamp, help="first run, e.g. 2026-10-01T00:00")
    parser.add_argument("--end", type=pd.Timestamp, help="last run, inclusive")
    parser.add_argument("--limit", type=int, help="consider only the first N runs to do")
    parser.add_argument("--params", nargs="+", help="only these DWD parameters (a test subset)")
    parser.add_argument("--ceiling-gb", type=float, default=4.0, help="memory ceiling per run")
    parser.add_argument("--fetch-workers", type=int, default=8, help="reads per block")
    parser.add_argument(
        "--min-completeness",
        type=float,
        default=DEFAULT_MIN_COMPLETENESS,
        help="fraction of a run's fields the archive must list for it to be written",
    )
    parser.add_argument(
        "--skipped-log",
        type=pathlib.Path,
        help="TSV skipped runs are appended to (default logs/icon_v1_<variant>_skipped.tsv)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="list each run and report its completeness; write nothing",
    )
    parser.add_argument(
        "--archive-only",
        action="store_true",
        help="consider only archived runs, not those DWD's live server still lists",
    )
    parser.add_argument(
        "--in-process",
        action="store_true",
        help="write every run in this process rather than a fresh one per run",
    )
    args = parser.parse_args(argv)

    provider_kwargs = {
        "params": args.params,
        "ceiling_gb": args.ceiling_gb,
        "fetch_workers": args.fetch_workers,
        "min_completeness": args.min_completeness,
    }
    provider = ICONV1Provider(args.variant, **provider_kwargs)
    skipped_log = args.skipped_log or pathlib.Path("logs") / f"icon_v1_{args.variant}_skipped.tsv"

    empty = provider.empty_generations()
    if empty:
        logger.error(
            f"{provider.name}: {len(empty)} generation(s) exist with nothing committed: "
            f"{', '.join(provider.config.store_path(p) for p in empty)}. An interrupted write "
            "left them; a run of the same day would be written into one. Delete them (and "
            "make sure nothing is writing to them) before backfilling."
        )
        return 2

    archived, missing = plan(provider, args.start, args.end, live=not args.archive_only)
    if args.limit is not None:
        missing = missing[: args.limit]
    logger.info(
        f"{provider.name}: {len(archived)} run(s) in range, {len(missing)} to do, "
        f"stores: {provider.describe_store()}"
    )
    if missing:
        logger.info(f"{provider.name}: first {missing[0]}, last {missing[-1]}")
    if args.dry_run:
        report_completeness(provider, missing)
        return 0

    written = failed = 0
    skipped: list[RunCompleteness] = []
    started = time.monotonic()
    runner = PartitionRunner(
        args.variant, provider_kwargs, isolated=not args.in_process, target=run_partition_child
    )
    for number, it in enumerate(missing, 1):
        run_started = time.monotonic()
        try:
            result = runner(it)
        except Exception as exc:  # noqa: BLE001 - one bad run must not stop the backfill
            failed += 1
            logger.exception(f"{provider.name}: run {it} failed: {exc}")
            # A run that failed while starting a new generation leaves it empty, and
            # the next run of that day would be written into it; stop instead, as the
            # startup check would.
            empty = provider.empty_generations()
            if empty:
                logger.error(
                    f"{provider.name}: run {it} left {', '.join(empty)} with nothing "
                    "committed; stopping. Delete it before restarting the backfill."
                )
                break
            continue
        written += int(result.written)
        listed = result.completeness
        if not result.written and listed is not None and listed.fraction < args.min_completeness:
            skipped.append(listed)
            log_skipped(skipped_log, listed)
        took = time.monotonic() - run_started
        elapsed = time.monotonic() - started
        logger.info(
            f"{provider.name}: [{number}/{len(missing)}] {it} "
            f"{'written' if result.written else 'skipped'} in {took / 60:.1f} min "
            f"({took - result.seconds:.1f}s of it starting the worker); "
            f"{elapsed / 3600:.1f} h so far, "
            f"~{elapsed / number * (len(missing) - number) / 3600:.1f} h to go"
        )
    logger.info(
        f"{provider.name}: {written} run(s) written, {len(skipped)} skipped as incomplete, "
        f"{failed} failed, {len(missing) - written - failed - len(skipped)} with nothing to "
        f"do, in {(time.monotonic() - started) / 3600:.1f} h"
    )
    for listed in skipped:
        logger.info(
            f"  skipped {listed.init_time:%Y-%m-%dT%H:%M}: {listed.fraction:.1%} "
            f"({listed.present:,}/{listed.expected:,}); most missing {listed.most_missing(5)}"
        )
    if skipped:
        logger.info(f"{provider.name}: skipped runs appended to {skipped_log}")
    if written:
        for prefix in provider.generations():
            if not axis_is_sorted(provider.config.icechunk_repo(prefix), append_dim="init_time"):
                logger.warning(
                    f"{provider.name}: init_time in {prefix} is out of order; run the "
                    "reorder_store asset on it once nothing is writing to it"
                )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
