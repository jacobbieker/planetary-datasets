"""One-off: backfill a range of a Met Office archive into source.coop.

    python _backfill_metoffice.py <provider> <start> <count> [freq] [threads]
    python _backfill_metoffice.py uk_marine catchup 0 1h [threads]

With ``catchup`` in place of a start date the range is discovered instead of given: see
:func:`catchup_partitions`. ``count`` is ignored in that form but still has to be passed,
so the argument positions stay the same.

Serial within a provider, because Icechunk takes one writer per store. Several of these
may run at once against *different* stores — but only with `threads` set well below the
core count.

That caveat is not theoretical. Four of these running unconstrained drove a 16-core
machine to a load average of 112: dask's threaded scheduler defaults to one worker per
core, so each process alone tried to fill the machine. The CPU starvation then starved the
network, and 250 partitions failed with EndpointConnectionError and StorageError against
buckets that were perfectly healthy — they answer a listing in one second. Capping the
pool keeps the jobs additive instead of competing.

Run from the repository root so `planetary_datasets` resolves here rather than to the
editable install's worktree; the resolved path is printed to prove it.
"""

import os
import sys
import time
import traceback

DEFAULT_THREADS = 4

#: Give up the run after this many failures in a row, rather than working through what is
#: left of the range.
#:
#: Not a tuning knob so much as a lesson. When this host lost DNS at 02:34 on 2026-10-13,
#: every remaining partition failed the instant it was tried, and because one bad partition
#: must not stop the rest, four lanes cheerfully burned their whole backlog in minutes:
#: 14043 "failures" against buckets that were perfectly healthy, and a 16296-partition run
#: marked complete. Partitions fail one at a time; infrastructure fails all at once, and
#: twenty in a row with nothing in between is the signature of the second. Stopping costs
#: one retry pause, because the lane resumes where it stopped.
CONSECUTIVE_FAILURE_LIMIT = 20

#: Exit status meaning "stopped early on a failure streak". The queue script retries this
#: rather than moving on to the next store.
EXIT_FAILURE_STREAK = 2


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


def catchup_partitions(provider):
    """The hours this provider could still fill: published upstream, absent from the store.

    Only the marine observations need this, and they need it badly. Their bucket is a
    rolling ten-day window, so "where did the backfill get to" is not a date anyone can
    write down in advance — it moves, and an hour that ages out is gone for good. The other
    archives here are complete back to their first run, so a fixed start date is fine for
    them and this would only be a slower way to compute one.

    Asking the bucket what it still holds and the store what it still lacks is also what
    keeps a repeating catch-up cheap: it fetches the genuinely missing hours and nothing
    else, and it never spends a request on an hour the Met Office simply never published.
    """
    import pathlib

    import pandas as pd

    from planetary_datasets.providers.observations.uk_marine import BATCH_DIR, BUCKET

    if provider.name != "uk_marine":
        raise SystemExit(
            f"catchup is only implemented for uk_marine, not {provider.name!r}: "
            "the other archives are complete back to their first run, so give a start date"
        )

    entries = provider.filesystem().ls(BUCKET, detail=False)
    published = {
        pd.to_datetime(parsed.group(2), format="%Y%m%d%H%M")
        for name in entries
        if (parsed := BATCH_DIR.match(pathlib.PurePosixPath(name).name)) is not None
    }
    if not published:
        raise SystemExit(f"no batches found in s3://{BUCKET}; refusing to guess a range")

    retained = pd.DatetimeIndex(sorted(published))
    missing = pd.DatetimeIndex(sorted(provider.missing_timesteps(retained)))
    print(f"retained: {len(retained)} hour(s), {retained[0]} -> {retained[-1]}", flush=True)
    return missing


def main(argv: list[str]) -> int:
    name, start, count = argv[1], argv[2], int(argv[3])
    freq = argv[4] if len(argv) > 4 else "1D"
    threads = int(argv[5]) if len(argv) > 5 else DEFAULT_THREADS

    configure_threads(threads)

    import dask
    import pandas as pd

    import planetary_datasets
    from planetary_datasets.providers.metoffice import PROVIDERS
    from planetary_datasets.providers.observations.uk_marine import (
        UKMarineObservationsProvider,
    )

    dask.config.set(scheduler="threads", num_workers=threads)

    # The marine observations live outside the metoffice module's registry, but they are
    # the same Met Office archive and backfill identically, so they are driven from here
    # too. Their bucket is a rolling ten-day window: a partition older than that cannot be
    # recovered, which is why this one is run first when several are queued.
    providers = {**PROVIDERS, UKMarineObservationsProvider.name: UKMarineObservationsProvider}

    provider = providers[name]()
    print(f"package : {planetary_datasets.__file__}", flush=True)
    print(f"provider: {name}", flush=True)
    print(f"store   : {provider.store_path}", flush=True)
    print(f"threads : {threads}", flush=True)

    if start == "catchup":
        partitions = catchup_partitions(provider)
        if len(partitions) == 0:
            print("range   : nothing missing, store is level with the bucket\n", flush=True)
            return 0
    else:
        partitions = pd.date_range(pd.Timestamp(start), periods=count, freq=freq)
    print(
        f"range   : {len(partitions)} partitions, {partitions[0]} -> {partitions[-1]}\n", flush=True
    )

    written = skipped = failed = 0
    consecutive = 0
    stopped_early = False
    began = time.time()

    for index, partition in enumerate(partitions, start=1):
        started = time.time()
        try:
            did_write = provider.run_partition(partition)
        except Exception as exc:  # noqa: BLE001 - one bad partition must not stop the rest
            failed += 1
            consecutive += 1
            print(
                f"[{index:4d}/{len(partitions)}] {partition} FAILED "
                f"{type(exc).__name__}: {str(exc)[:160]}",
                flush=True,
            )
            traceback.print_exc()
            if consecutive >= CONSECUTIVE_FAILURE_LIMIT:
                stopped_early = True
                print(
                    f"\n{consecutive} failures in a row at {partition}: stopping with "
                    f"{len(partitions) - index} partition(s) untried. This is what an "
                    f"outage looks like, not a bad partition — fix the host and rerun, "
                    f"the stored partitions will be skipped.",
                    flush=True,
                )
                break
            continue

        consecutive = 0
        written += did_write
        skipped += not did_write
        rate = (time.time() - began) / index
        eta = pd.Timedelta(seconds=rate * (len(partitions) - index)).round("s")
        print(
            f"[{index:4d}/{len(partitions)}] {partition} "
            f"{'wrote' if did_write else 'skip '} {time.time() - started:6.1f}s "
            f"-> {provider.store_prefix.rpartition('/')[2]}  eta {eta}",
            flush=True,
        )

    elapsed = pd.Timedelta(seconds=time.time() - began).round("s")
    print(
        f"\n{'stopped early' if stopped_early else 'done'} in {elapsed}: "
        f"{written} written, {skipped} skipped, {failed} failed",
        flush=True,
    )
    print(f"final store: {provider.store_path}", flush=True)
    if stopped_early:
        return EXIT_FAILURE_STREAK
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
