"""Keep the MeteoSwiss KENDA-CH1 stores current on the backfill box, outside Dagster.

    python _backfill_kenda.py [--hours 36] [--pause 1800] [--once]

MeteoSwiss's open-data API keeps only about a day of KENDA, so this is a short rolling
window rather than a backfill: every pass looks at the last ``--hours`` hours, and for each
hour either store lacks it stages the hour with the ``docker/meteoswiss-kenda`` image (the
download half, which cannot share the project environment) and then publishes it with
:class:`~planetary_datasets.providers.kenda.KENDAAnalysisProvider` and
:class:`~planetary_datasets.providers.kenda.KENDAForecastProvider`, exactly as the Dagster
``kenda_download`` / ``kenda_analysis`` / ``kenda_forecast`` assets do. An hour MeteoSwiss
has not finished publishing fails to stage and is tried again next pass; staging it
partially would fix the store's schema to a subset of its fields.

One writer per store: nothing else may write the KENDA stores while this runs.

Staged files (about 40 GB a day) are deleted hour by hour at the end of each pass once both
stores hold that hour; the shared ``horizontal_constants`` file is kept.
"""

import argparse
import os
import re
import subprocess
import sys
import time

#: Matches the Dagster asset's container memory cap; the download streams to disk.
CONTAINER_MEMORY_GB = 2


def stage(it, archive, image: str) -> bool:
    """Download one hour into ``archive`` with the downloader image. True on success."""
    from planetary_datasets.providers.kenda_download import DEFAULT_TARGET

    command = [
        "docker", "run", "--rm",
        "--volume", f"{archive}:{DEFAULT_TARGET}",
        "--memory", f"{CONTAINER_MEMORY_GB}g",
        "--user", f"{os.getuid()}:{os.getgid()}",
        image,
        "--ref-time", it.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "--target", DEFAULT_TARGET,
    ]  # fmt: skip
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        tail = (result.stderr or result.stdout).strip().splitlines()[-2:]
        print(f"  {it} not staged: {' | '.join(tail)[:300]}", flush=True)
        return False
    return True


#: Staged files carry their reference time: ``kenda-ch1-<YYYYMMDDHHMM>-<lead>-<field>-...``.
STAGED_HOUR = re.compile(r"^kenda-ch1-(\d{12})-")


def discard_stored(providers, archive) -> int:
    """Delete the staged files of every hour both stores already hold. Returns how many."""
    import pandas as pd

    by_hour: dict = {}
    for path in archive.iterdir():
        match = STAGED_HOUR.match(path.name)
        if match:
            by_hour.setdefault(pd.Timestamp(match[1]), []).append(path)
    if not by_hour:
        return 0
    hours = pd.DatetimeIndex(sorted(by_hour))
    pending = set().union(*(set(p.missing_timesteps(hours)) for p in providers))
    removed = 0
    for hour, paths in by_hour.items():
        if hour in pending:
            continue
        for path in paths:
            path.unlink(missing_ok=True)
            removed += 1
    if removed:
        print(f"discarded {removed} staged file(s) already in both stores", flush=True)
    return removed


def main(argv) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--hours", type=int, default=36, help="rolling window to check")
    parser.add_argument("--lag", type=int, default=2, help="hours to stay behind now")
    parser.add_argument("--pause", type=int, default=1800, help="seconds between passes")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)

    import pandas as pd

    from planetary_datasets.providers.kenda import KENDAAnalysisProvider, KENDAForecastProvider

    image = os.environ.get("METEOSWISS_KENDA_IMAGE", "planetary-datasets/meteoswiss-kenda:latest")
    providers = (KENDAAnalysisProvider(), KENDAForecastProvider())
    archive = providers[0].archive_path.expanduser().resolve()
    archive.mkdir(parents=True, exist_ok=True)
    print(f"archive : {archive}\nimage   : {image}", flush=True)

    while True:
        now = pd.Timestamp.now(tz="UTC").tz_localize(None).floor("h")
        hours = pd.date_range(
            now - pd.Timedelta(args.hours, "h"), now - pd.Timedelta(args.lag, "h"), freq="1h"
        )
        missing = {p.name: set(p.missing_timesteps(hours)) for p in providers}
        todo = sorted(set().union(*missing.values()))
        print(f"\npass    : {hours[0]} -> {hours[-1]}, {len(todo)} hour(s) to do", flush=True)
        written = failed = 0
        for it in todo:
            if not stage(it, archive, image):
                continue
            for provider in providers:
                if it not in missing[provider.name]:
                    continue
                try:
                    ok = provider.run_partition(it, check_present=False)
                    written += bool(ok)
                    print(
                        f"  {it} {provider.name}: {'wrote' if ok else 'nothing written'}",
                        flush=True,
                    )
                except Exception as exc:  # noqa: BLE001 - one bad hour must not stop the rest
                    failed += 1
                    print(
                        f"  {it} {provider.name}: FAILED {type(exc).__name__}: {str(exc)[:200]}",
                        flush=True,
                    )
        print(f"pass done: {written} written, {failed} failed", flush=True)
        discard_stored(providers, archive)
        if args.once:
            return 1 if failed else 0
        time.sleep(args.pause)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
