"""Backfill an ICON v1 store from the Source Cooperative archive, oldest run first.

The Dagster assets ``icon_global``, ``icon_global_art`` and ``icon_eu_art`` write one run
per partition through :class:`~planetary_datasets.providers.icon_v1.ICONV1Provider`. This
walks the archive's runs with the same provider, in time order, in one process: the
provider already reads, decodes and writes in parallel inside a memory ceiling, so there is
nothing to gain from more processes and a second writer would only race the first.

Runs already stored, in any generation of the store, are skipped, so the script resumes
where it stopped. A run that fails is logged and the walk goes on; run the script again to
retry it.

Before running it:

* **Make it the only writer.** Pause the variant's Dagster asset (or its schedule and any
  backfill of it) for as long as this runs. Two writers on one store race their commits.
* **Run it before enabling the live schedule.** A new generation is named after the first
  run written to it, and appends are cheapest in time order. Backfilling first means the
  ICON-Global generation is named after its earliest archived run and the live runs then
  append after it.
* Runs written out of order (for example a gap filled after later runs were stored) leave
  ``init_time`` unsorted. Materialise the manual ``reorder_store`` asset
  (``dags/assets/maintenance.py``) with this store's prefix and ``append_dim: init_time``
  once nothing is writing to it.

Usage::

    python scripts/icon_v1_backfill.py eu_art --start 2026-10-01 --end 2026-10-12 --dry-run
    python scripts/icon_v1_backfill.py global --ceiling-gb 4
    python scripts/icon_v1_backfill.py global_art --params T_2M TOT_PREC  # a test subset

``--params`` writes a subset of the model, which has a different schema from the full
store and so goes to a generation of its own; it is for trying the pipeline out, not for
filling the production stores.
"""

from __future__ import annotations

import argparse
import sys
import time

import pandas as pd
from loguru import logger

from planetary_datasets.providers.icon_v1 import VARIANT_MODELS, ICONV1Provider
from planetary_datasets.providers.icon_v1_catalog import list_runs


def plan(
    provider: ICONV1Provider, start: pd.Timestamp | None, end: pd.Timestamp | None
) -> tuple[list[pd.Timestamp], list[pd.Timestamp]]:
    """The archived runs in ``[start, end]``, and those of them not stored yet, in order.

    Args:
        provider: Provider of the variant to backfill.
        start: First run to consider, or None for the archive's first.
        end: Last run to consider, or None for the archive's last.

    Returns:
        ``(archived, missing)``, both sorted oldest first.
    """
    runs = pd.DatetimeIndex(list_runs(provider.model, "archive"))
    if start is not None:
        runs = runs[runs >= start]
    if end is not None:
        runs = runs[runs <= end]
    missing = provider.missing_timesteps(runs) if len(runs) else []
    return list(runs), sorted(missing)


def main(argv: list[str] | None = None) -> int:
    """Command-line entry point. Returns the exit code."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("variant", choices=sorted(VARIANT_MODELS))
    parser.add_argument("--start", type=pd.Timestamp, help="first run, e.g. 2026-10-01T00:00")
    parser.add_argument("--end", type=pd.Timestamp, help="last run, inclusive")
    parser.add_argument("--params", nargs="+", help="only these DWD parameters (a test subset)")
    parser.add_argument("--ceiling-gb", type=float, default=4.0, help="memory ceiling per run")
    parser.add_argument("--fetch-workers", type=int, default=8, help="reads per block")
    parser.add_argument("--dry-run", action="store_true", help="print the plan and stop")
    args = parser.parse_args(argv)

    provider = ICONV1Provider(
        args.variant,
        params=args.params,
        ceiling_gb=args.ceiling_gb,
        fetch_workers=args.fetch_workers,
    )
    archived, missing = plan(provider, args.start, args.end)
    logger.info(
        f"{provider.name}: {len(archived)} archived run(s) in range, {len(missing)} not yet in "
        f"{provider.describe_store()}"
    )
    if missing:
        logger.info(f"{provider.name}: first {missing[0]}, last {missing[-1]}")
    if args.dry_run:
        for it in missing:
            logger.info(f"would write {it.isoformat()}")
        return 0

    written = failed = 0
    started = time.monotonic()
    for number, it in enumerate(missing, 1):
        run_started = time.monotonic()
        try:
            ok = provider.run_partition(it, check_present=False)
        except Exception as exc:  # noqa: BLE001 - one bad run must not stop the backfill
            failed += 1
            logger.exception(f"{provider.name}: run {it} failed: {exc}")
            continue
        written += int(ok)
        elapsed = time.monotonic() - started
        logger.info(
            f"{provider.name}: [{number}/{len(missing)}] {it} "
            f"{'written' if ok else 'skipped'} in {(time.monotonic() - run_started) / 60:.1f} "
            f"min; {elapsed / 3600:.1f} h so far, "
            f"~{elapsed / number * (len(missing) - number) / 3600:.1f} h to go"
        )
    logger.info(
        f"{provider.name}: {written} run(s) written, {failed} failed, "
        f"{len(missing) - written - failed} skipped, in {(time.monotonic() - started) / 3600:.1f} h"
    )
    if written and not provider.axis_sorted():
        logger.warning(
            f"{provider.name}: init_time in {provider.store_prefix} is out of order; run the "
            "reorder_store asset on it once nothing is writing to it"
        )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
