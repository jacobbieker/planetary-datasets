"""Backfill an MTG IRS L1 store from the EUMETSAT Data Store into source.coop.

    python _backfill_mtg_irs.py <provider> [--start ISO] [--end ISO] [--follow]
                                [--threads N] [--downloads N]

``provider`` is ``mtg_irs_l1`` (EO:EUM:DAT:0741) or ``mtg_irs_l1_lac4_hourly``
(EO:EUM:DAT:1199). ``--start`` defaults to the start of the archive and ``--end`` to the
newest whole hour the Data Store has had time to finish publishing. With ``--follow`` the
lane does not exit at the end of the range: it sleeps and picks up the hours published
since, so one lane both backfills and keeps the store current.

One lane per store, never two: a store takes one writer. The lane walks forward in time;
hours already stored are skipped, so a killed lane resumes where it stopped. Each hour is
~290 dwell files and ~20 GB, staged in the temp directory and deleted once written.

Stops early (exit 2) after :data:`CONSECUTIVE_FAILURE_LIMIT` failures in a row, which is
what an outage looks like, and at once (exit 3) when the Data Store cannot search the
collection at all — retrying a collection that is not served yet only fills the log.

Run from the repository root so ``planetary_datasets`` resolves here rather than to the
editable install's worktree; the resolved path is printed to prove it.
"""

import argparse
import os
import sys
import time
import traceback

CONSECUTIVE_FAILURE_LIMIT = 20
EXIT_FAILURE_STREAK = 2
EXIT_COLLECTION_UNAVAILABLE = 3

#: Hours to stay behind "now": a repeat cycle is published a few minutes after it ends,
#: and an hour half-published would be stored half-full and then counted as done.
LIVE_LAG_HOURS = 2

#: How long a following lane sleeps once it has caught up.
FOLLOW_PAUSE_SECONDS = 1800


def configure_threads(threads: int) -> None:
    """Cap the pools that would otherwise size themselves from the core count."""
    for variable in (
        "OMP_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "MKL_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
        "VECLIB_MAXIMUM_THREADS",
    ):
        os.environ[variable] = str(threads)


def parse_args(argv):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("provider")
    parser.add_argument("--start", help="first hour, ISO; default: the archive start")
    parser.add_argument("--end", help="last hour (exclusive), ISO; default: the live edge")
    parser.add_argument("--follow", action="store_true", help="keep going as hours publish")
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--downloads", type=int, default=8, help="dwell files fetched at once")
    return parser.parse_args(argv)


def live_edge():
    import pandas as pd

    return pd.Timestamp.now(tz="UTC").tz_localize(None).floor("h") - pd.Timedelta(
        LIVE_LAG_HOURS, "h"
    )


def main(argv) -> int:
    args = parse_args(argv)
    configure_threads(args.threads)

    import dask
    import pandas as pd

    import planetary_datasets
    from planetary_datasets.providers.eumetsat.mtg_irs import (
        ARCHIVE_START,
        PROVIDERS,
        CollectionUnavailable,
    )

    dask.config.set(scheduler="threads", num_workers=args.threads)

    provider = PROVIDERS[args.provider]()
    provider.download_workers = args.downloads
    start = pd.Timestamp(args.start) if args.start else pd.Timestamp(ARCHIVE_START)
    print(f"package : {planetary_datasets.__file__}", flush=True)
    print(f"provider: {provider.name} ({provider.collection_id})", flush=True)
    print(f"store   : {provider.store_path}", flush=True)
    print(f"threads : {args.threads}, downloads: {args.downloads}", flush=True)

    written = skipped = failed = 0
    consecutive = 0
    failed_hours = []
    began = time.time()

    while True:
        end = pd.Timestamp(args.end) if args.end else live_edge()
        hours = pd.date_range(start, end, freq="1h", inclusive="left")
        # Read the store's time axis once for the range, not once per hour.
        todo = provider.missing_timesteps(hours) if len(hours) else []
        print(
            f"range   : {start} -> {end}, {len(hours)} hour(s), {len(todo)} not yet stored\n",
            flush=True,
        )

        for index, hour in enumerate(todo, start=1):
            started = time.time()
            try:
                did_write = provider.run_partition(hour, check_present=False)
            except CollectionUnavailable as exc:
                print(f"\n{exc}\nStopping: nothing in this collection can be fetched.", flush=True)
                return EXIT_COLLECTION_UNAVAILABLE
            except Exception as exc:  # noqa: BLE001 - one bad hour must not stop the rest
                failed += 1
                consecutive += 1
                failed_hours.append(hour)
                print(
                    f"[{index:5d}/{len(todo)}] {hour} FAILED "
                    f"{type(exc).__name__}: {str(exc)[:200]}",
                    flush=True,
                )
                traceback.print_exc()
                if consecutive >= CONSECUTIVE_FAILURE_LIMIT:
                    print(
                        f"\n{consecutive} failures in a row at {hour}: stopping. Fix the host "
                        "and rerun; stored hours are skipped.",
                        flush=True,
                    )
                    return EXIT_FAILURE_STREAK
                continue

            consecutive = 0
            written += did_write
            skipped += not did_write
            took = time.time() - started
            remaining = pd.Timedelta(seconds=took * (len(todo) - index)).round("min")
            print(
                f"[{index:5d}/{len(todo)}] {hour} {'wrote' if did_write else 'empty'} "
                f"{took:7.1f}s  ~{remaining} left at this rate",
                flush=True,
            )

        elapsed = pd.Timedelta(seconds=time.time() - began).round("s")
        print(
            f"\ncaught up to {end} after {elapsed}: {written} written, "
            f"{skipped} with no data, {failed} failed",
            flush=True,
        )
        if not args.follow or args.end:
            return 1 if failed else 0
        # The next pass retries this pass's failures, which are still missing from the
        # store, and re-checks the last few hours in case a late repeat cycle appeared. It
        # does not re-search every empty hour back to the archive start.
        start = min(failed_hours, default=end - pd.Timedelta(LIVE_LAG_HOURS * 3, "h"))
        failed_hours = []
        time.sleep(FOLLOW_PAUSE_SECONDS)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
