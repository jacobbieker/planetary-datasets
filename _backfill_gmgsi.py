r"""Catch GMGSI v3 up to realtime, outside Dagster.

    python _backfill_gmgsi.py                      # v3, first missing hour -> now
    python _backfill_gmgsi.py --provider gmgsi_v1
    python _backfill_gmgsi.py --start 2026-07-22T15:00 --end 2026-08-01
    python _backfill_gmgsi.py --follow              # keep catching up every 30 min

Serial, because Icechunk takes one writer per store, and **forward in time**, which here is
a correctness requirement rather than a preference. GMGSI v3 changed grid on 2026-07-22
(3000x5000 -> 3000x4999), so the hours after it belong in a new generation that does not
exist until the first of them is written. A generation is named after the first timestamp
that did not fit anywhere, so an hour run in isolation names a store after *its own* day:
starting at 2026-10-09 would create ``gmgsi_v3_09102026.icechunk`` and leave July through
October scattered across a store per starting point. Walking forward means the first narrow
hour creates ``gmgsi_v3_22072026.icechunk`` and every later hour matches its fingerprint
and lands in it.

``--max-new-generations`` is the guard on that. Creating one new store is the expected
outcome of this backfill; creating several means the fingerprint is splitting on something
it should ignore, and the run stops rather than fanning a single grid across many stores.

While this runs, Dagster must not also write GMGSI: the asset is eager and the two would be
two writers on one store. The Dagster pool is set to zero slots for the duration and
restored on the way out, including on Ctrl-C. If this process is killed outright, restore
it by hand -- a pool left at zero makes the source go quiet rather than red:

    curl -s -X POST http://127.0.0.1:3000/graphql -H 'Content-Type: application/json' \\
      -d '{"query":"mutation{setConcurrencyLimit(concurrencyKey:\\"gmgsi_gmgsi\\",limit:1)}"}'

Run from the repository root so `planetary_datasets` resolves here rather than to the
editable install's worktree; the resolved path is printed to prove it.
"""

import argparse
import os
import sys
import time
import traceback

DEFAULT_THREADS = 4
GRAPHQL = "http://127.0.0.1:3000/graphql"
POOL = "gmgsi_gmgsi"

#: GMGSI lands roughly 40 minutes after the hour; the Dagster assets hold back two hours
#: rather than materialise an hour and find it missing, and so does this.
PUBLISH_LAG_HOURS = 2


def configure_threads(threads: int) -> None:
    """Cap every thread pool this process would otherwise size from the core count.

    Set before numpy, dask or the storage client are imported: the BLAS libraries read
    their environment once, at load.
    """
    for variable in (
        "OMP_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "MKL_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
        "VECLIB_MAXIMUM_THREADS",
    ):
        os.environ[variable] = str(threads)


def set_pool_limit(limit: int) -> bool:
    """Set the Dagster concurrency pool for GMGSI. Returns False if it could not be set."""
    import requests

    query = (
        "mutation($key: String!, $limit: Int!) "
        "{ setConcurrencyLimit(concurrencyKey: $key, limit: $limit) }"
    )
    try:
        response = requests.post(
            GRAPHQL,
            json={"query": query, "variables": {"key": POOL, "limit": limit}},
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("errors"):
            print(f"!! could not set pool {POOL} to {limit}: {payload['errors']}", flush=True)
            return False
    except Exception as exc:  # noqa: BLE001 - Dagster being down is not a reason to stop
        print(f"!! could not reach Dagster to set pool {POOL} to {limit}: {exc}", flush=True)
        return False
    print(f"dagster pool {POOL} -> {limit} slot(s)", flush=True)
    return True


def run_pass(provider, partitions, max_new_generations: int) -> tuple[int, int, int]:
    """Run every partition in order. Returns (written, skipped, failed)."""
    import pandas as pd

    written = skipped = failed = 0
    began = time.time()
    seen_prefixes = {provider.store_prefix}

    for index, partition in enumerate(partitions, start=1):
        started = time.time()
        try:
            did_write = provider.run_partition(partition)
        except Exception as exc:  # noqa: BLE001 - one bad hour must not stop the rest
            failed += 1
            print(
                f"[{index:5d}/{len(partitions)}] {partition} FAILED "
                f"{type(exc).__name__}: {str(exc)[:160]}",
                flush=True,
            )
            traceback.print_exc()
            continue

        written += did_write
        skipped += not did_write

        prefix = provider.store_prefix
        if prefix not in seen_prefixes:
            seen_prefixes.add(prefix)
            print(f"   -> new generation: {prefix}", flush=True)
            new_generations = len(seen_prefixes) - 1
            if new_generations > max_new_generations:
                print(
                    f"\n!! {new_generations} new generations created, limit is "
                    f"{max_new_generations}: {sorted(seen_prefixes)}\n"
                    "!! stopping. One grid should not need a store per day -- check "
                    "schema_fingerprint before continuing.",
                    flush=True,
                )
                break

        rate = (time.time() - began) / index
        eta = pd.Timedelta(seconds=rate * (len(partitions) - index)).round("s")
        print(
            f"[{index:5d}/{len(partitions)}] {partition} "
            f"{'wrote' if did_write else 'skip '} {time.time() - started:6.1f}s "
            f"-> {prefix.rpartition('/')[2]}  eta {eta}",
            flush=True,
        )

    elapsed = pd.Timedelta(seconds=time.time() - began).round("s")
    print(
        f"\npass done in {elapsed}: {written} written, {skipped} skipped, {failed} failed",
        flush=True,
    )
    return written, skipped, failed


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider", choices=["gmgsi_v3", "gmgsi_v1"], default="gmgsi_v3")
    parser.add_argument("--start", default=None, help="first hour; default is the archive start")
    parser.add_argument("--end", default=None, help="last hour; default is now minus the lag")
    parser.add_argument("--threads", type=int, default=DEFAULT_THREADS)
    parser.add_argument("--max-new-generations", type=int, default=1)
    parser.add_argument(
        "--follow", action="store_true", help="keep catching up rather than exiting after one pass"
    )
    parser.add_argument("--period", type=int, default=1800, help="seconds between --follow passes")
    parser.add_argument(
        "--no-pause-dagster",
        action="store_true",
        help="do not zero the Dagster pool (only safe if GMGSI is paused there)",
    )
    args = parser.parse_args(argv[1:])

    configure_threads(args.threads)

    import pandas as pd

    import planetary_datasets
    from planetary_datasets.providers.gmgsi import GMGSILegacyProvider, GMGSIProvider

    provider = (GMGSIProvider if args.provider == "gmgsi_v3" else GMGSILegacyProvider)()
    print(f"package  : {planetary_datasets.__file__}", flush=True)
    print(f"provider : {provider.name}", flush=True)
    print(f"store    : {provider.store_path}", flush=True)
    print(f"threads  : {args.threads}", flush=True)

    paused = False if args.no_pause_dagster else set_pool_limit(0)
    try:
        while True:
            start = pd.Timestamp(args.start) if args.start else provider.archive_start
            end = (
                pd.Timestamp(args.end)
                if args.end
                else pd.Timestamp.now("UTC").tz_localize(None).floor("h")
                - pd.Timedelta(hours=PUBLISH_LAG_HOURS)
            )
            desired = pd.date_range(max(start, provider.archive_start), end, freq="1h")

            # Asked across every generation, so hours already in the pre-2026-07-22 store
            # are not re-fetched. Sorted because the order is what keeps one grid in one
            # generation; see the module docstring.
            missing = pd.DatetimeIndex(sorted(provider.missing_timesteps(desired)))
            print(f"\ngenerations: {provider.describe_store()}", flush=True)
            if len(missing) == 0:
                print(
                    f"range    : nothing missing through {end}; level with the archive", flush=True
                )
            else:
                print(
                    f"range    : {len(missing)} of {len(desired)} hours missing, "
                    f"{missing[0]} -> {missing[-1]}\n",
                    flush=True,
                )
                run_pass(provider, missing, args.max_new_generations)

            if not args.follow:
                break
            print(f"=== next catch-up in {args.period}s", flush=True)
            time.sleep(args.period)
    finally:
        if paused:
            set_pool_limit(1)

    print(f"final store: {provider.store_path}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
