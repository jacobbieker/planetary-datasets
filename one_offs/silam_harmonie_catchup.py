"""Catch the short-retention regional stores up to now.

SILAM dust is a daily 00Z run, so the last four days are four partitions. HARMONIE runs
every three hours and DMI only keeps a few days, so three days back is about as far as a
catch-up can usefully reach; anything already expired upstream is reported as skipped.

MEPS model levels and the AROME paquets are bounded by what a run can chew through rather
than by their archives: met.no keeps years of ``meps25epsarchive``, and data.gouv.fr was
still serving AROME fourteen days back when this was written, so ``--days`` turns either
into a deeper backfill.

After them come the long-archive global stores, which are here to close the gaps a stopped
Dagster leaves rather than to beat an expiry: GEOS-CF v2 and GMGSI four weeks back, the
NCAR IFS analysis three months, the two OPERA composites two months, and last the MetOp-SG
level 1b products four weeks. OPERA cannot be fetched in this environment, so each missing
hour is first staged by the ``docker/earth2studio`` image, exactly as the Dagster
``*_download`` asset does, and the staged file is removed once it is written.

Each window stops short of the present by the hours that model needs to publish, so a
cycle still being written upstream is left for the next run instead of being reported as
a failure. ``run_range`` then filters to the partitions the store is missing and logs,
rather than raises, on a partition that fails, so one gap does not stop the rest.

Run from the repo root so ``.env`` is picked up::

    pixi run python one_offs/silam_harmonie_catchup.py                  # every store
    pixi run python one_offs/silam_harmonie_catchup.py --list
    pixi run python one_offs/silam_harmonie_catchup.py meps_det
    pixi run python one_offs/silam_harmonie_catchup.py arome --days 10
    pixi run python one_offs/silam_harmonie_catchup.py geos gmgsi ifs opera metop_sg

Writing a partition older than the store's last one leaves the time axis out of order.
The data is correct, but a ``.sel`` over a *slice* of the store is unreliable until the
store's ``<name>-reorder`` asset is materialised, which is a manual step; the run ends by
naming the stores that need it.
"""

from __future__ import annotations

import argparse
import os
import pathlib
import subprocess
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import List, Sequence

import pandas as pd
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.providers.arome import (
    OVERSEAS_REGIONS,
    AromeFranceHDProvider,
    AromeFranceProvider,
    AromeOverseasProvider,
)
from planetary_datasets.providers.dmi_harmonie import (
    DMIHarmonieModelLevelProvider,
    DMIHarmonieProvider,
)
from planetary_datasets.providers.geos import GEOSProvider
from planetary_datasets.providers.gmgsi import GMGSIProvider
from planetary_datasets.providers.ifs import IFSAnalysisProvider
from planetary_datasets.providers.meps import MEPSDetProvider
from planetary_datasets.providers.opera import (
    OPERAProvider,
    OPERARainfallProvider,
    OPERAReflectivityProvider,
)
from planetary_datasets.providers.opera_download import DEFAULT_TARGET as OPERA_CONTAINER_DIR
from planetary_datasets.providers.polar.metop_sg import (
    MetopSgMetimageProvider,
    MetopSgMwsProvider,
    MetopSgRoProvider,
)
from planetary_datasets.providers.silam import SILAMDustProvider

SILAM_DAYS = 4
HARMONIE_DAYS = 3
#: Two days is sixteen cycles of full-domain model-level fields pulled over OPeNDAP, which
#: is already a long run. The archive itself reaches back years, so raise it with --days.
MEPS_DAYS = 13
AROME_DAYS = 13

#: Hours a cycle needs before it is on the server. Measured rather than guessed: at 19:45Z
#: the 18Z cycle was absent from both met.no and data.gouv.fr while 15Z was present.
MEPS_LAG_HOURS = 3
AROME_LAG_HOURS = 4

GEOS_DAYS = 28
GMGSI_DAYS = 28
#: Three calendar months, at their longest.
IFS_DAYS = 92
OPERA_DAYS = 61
METOP_SG_DAYS = 28

#: At 13:20Z on 2026-10-15 the newest GEOS-CF v2 instant on the portal was 2026-10-14 09:00.
#: An unpublished instant is not cheap to skip: every revision is retried with backoff.
GEOS_LAG_HOURS = 30
#: GMGSI lands about 40 minutes after the hour; _backfill_gmgsi.py holds back two hours too.
GMGSI_LAG_HOURS = 2
#: GDEX posts a day of analysis after it ends; the Dagster partitions hold back two days.
IFS_LAG_HOURS = 48
#: The last frames of an hour are a few minutes behind it upstream.
OPERA_LAG_HOURS = 2
#: A granule partition counts as stored once it holds *any* granule, so a window run before
#: its last products reach the Data Store is closed short for good. These leave a whole
#: window plus several hours of dissemination latency.
METOP_SG_DAILY_LAG_HOURS = 48
METOP_SG_LAG_HOURS = 6

#: The OPERA downloader image and its limits, as the Dagster ``*_download`` assets run it
#: (``dags/assets/radar.py``); not imported from there because that pulls in Dagster.
OPERA_IMAGE_ENV = "EARTH2STUDIO_IMAGE"
OPERA_DEFAULT_IMAGE = "planetary-datasets/earth2studio:latest"
OPERA_CONTAINER_MEMORY_GB = 6
#: Seconds one hour's staging container may run. One hung on 2024-07-01 11:00 for hours,
#: twice, and nothing behind it in the pass moved.
OPERA_STAGE_TIMEOUT = 1800
#: Hours staged at once. Each container is capped at OPERA_CONTAINER_MEMORY_GB.
OPERA_DOWNLOAD_WORKERS = 3


@dataclass(frozen=True)
class CatchUp:
    """One store's catch-up window.

    Attributes:
        provider: The provider to run. Constructing one is cheap and touches neither the
            network nor the store, so the whole table is built up front.
        days: How far back the window reaches.
        freq: Spacing of the partitions: ``D`` for a daily run, ``3h``/``6h`` for cycles.
        lag: Hours an init time needs before the model publishes it. The window ends
            there rather than at the present.
    """

    provider: BaseProvider
    days: int
    freq: str
    lag: int = 0

    @property
    def name(self) -> str:
        """Store name, as the provider reports it."""
        return self.provider.name

    def timestamps(self, now: pd.Timestamp, days: int | None = None) -> pd.DatetimeIndex:
        """The partitions of this window, oldest first.

        Args:
            now: Current time, naive UTC.
            days: Override for :attr:`days`, from ``--days``.
        """
        end = (now - pd.Timedelta(self.lag, "h")).floor(self.freq)
        start = end - pd.Timedelta(self.days if days is None else days, "D")
        return pd.date_range(start, end, freq=self.freq)

    def run(self, times: pd.DatetimeIndex) -> int:
        """Write the partitions of ``times`` the store is missing. Returns how many."""
        return self.provider.run_range(times)


@dataclass(frozen=True)
class OperaCatchUp(CatchUp):
    """An OPERA store, whose hours must be staged by the downloader image before writing.

    Attributes:
        workers: Hours staged concurrently, each in its own container.
    """

    provider: OPERAProvider
    workers: int = OPERA_DOWNLOAD_WORKERS

    def stage(self, it: pd.Timestamp, root: pathlib.Path) -> bool:
        """Stage one hour with the downloader image. True once a file is staged.

        An hour staged by an earlier, interrupted run is used as it is.
        """
        provider = self.provider
        if provider.has_staged(it):
            return True
        container = f"opera-{provider.product}-{it:%Y%m%d%H}-{os.getpid()}"
        command = [
            "docker",
            "run",
            "--rm",
            "--name",
            container,
            "--volume",
            f"{root}:{OPERA_CONTAINER_DIR}",
            "--memory",
            f"{OPERA_CONTAINER_MEMORY_GB}g",
            "--user",
            f"{os.getuid()}:{os.getgid()}",
            os.environ.get(OPERA_IMAGE_ENV, OPERA_DEFAULT_IMAGE),
            "opera",
            "--product",
            provider.product,
            "--time",
            it.strftime("%Y-%m-%dT%H:%M"),
            "--target",
            OPERA_CONTAINER_DIR,
            *(["--allow-partial"] if provider.allow_partial else []),
        ]
        try:
            result = subprocess.run(
                command, capture_output=True, text=True, timeout=OPERA_STAGE_TIMEOUT
            )
        except subprocess.TimeoutExpired:
            # Stopping the docker client leaves the container running; remove it too.
            subprocess.run(["docker", "rm", "-f", container], capture_output=True)
            logger.warning(
                f"{provider.name}: staging {it} took over {OPERA_STAGE_TIMEOUT}s; gave up, "
                "it is retried next pass"
            )
            return False
        if result.returncode != 0:
            tail = (result.stderr or result.stdout).strip().splitlines()[-3:]
            logger.warning(f"{provider.name}: staging {it} failed: {' | '.join(tail)}")
            return False
        return provider.has_staged(it)

    def run(self, times: pd.DatetimeIndex) -> int:
        """Stage and write each missing hour, a batch of ``workers`` hours at a time.

        Batches bound the staging directory to a handful of hours however far behind the
        writes fall. A staged hour that fails to write is kept for the next run.
        """
        provider = self.provider
        root = provider.archive_root.expanduser().resolve()
        root.mkdir(parents=True, exist_ok=True)
        missing = provider.missing_timesteps(times)
        logger.info(f"{provider.name}: {len(missing)} hours to stage under {root}")

        written = 0
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            for offset in range(0, len(missing), self.workers):
                batch = missing[offset : offset + self.workers]
                staged = list(pool.map(lambda it: self.stage(it, root), batch))
                for it, ok in zip(batch, staged):
                    if not ok:
                        continue
                    try:
                        wrote = provider.run_partition(it, check_present=False)
                    except Exception as exc:  # noqa: BLE001 - one bad hour must not stop the rest
                        logger.exception(f"{provider.name}: partition {it} failed: {exc}")
                        continue
                    written += wrote
                    provider.discard_staged(it, settled=wrote)
        return written


def now_utc() -> pd.Timestamp:
    """Current time as a naive UTC timestamp, which is what the stores hold."""
    return pd.Timestamp.now(tz="UTC").tz_localize(None)


def build_catchups() -> List[CatchUp]:
    """Every store the catch-up covers, in the order it runs them.

    SILAM and HARMONIE come first because they expire upstream soonest, then each AROME
    domain, then the MEPS model levels. The long-archive global stores follow, with the
    MetOp-SG products last and METimage, thousands of ten-minute windows, at the very end.
    """
    return [
        CatchUp(SILAMDustProvider(), days=SILAM_DAYS, freq="D"),
        CatchUp(DMIHarmonieModelLevelProvider(), days=HARMONIE_DAYS, freq="3h"),
        CatchUp(DMIHarmonieProvider(), days=HARMONIE_DAYS, freq="3h"),
        CatchUp(AromeFranceProvider(), days=AROME_DAYS, freq="3h", lag=AROME_LAG_HOURS),
        CatchUp(AromeFranceHDProvider(), days=AROME_DAYS, freq="3h", lag=AROME_LAG_HOURS),
        # The overseas domains run every six hours and archive six hours of each run.
        *(
            CatchUp(
                AromeOverseasProvider(region=region),
                days=AROME_DAYS,
                freq="6h",
                lag=AROME_LAG_HOURS,
            )
            for region in OVERSEAS_REGIONS
        ),
        CatchUp(MEPSDetProvider(), days=MEPS_DAYS, freq="3h", lag=MEPS_LAG_HOURS),
        CatchUp(GEOSProvider(version=2), days=GEOS_DAYS, freq="15min", lag=GEOS_LAG_HOURS),
        # Generational, so it must walk forward in time; run_range does, oldest first.
        CatchUp(GMGSIProvider(), days=GMGSI_DAYS, freq="1h", lag=GMGSI_LAG_HOURS),
        CatchUp(IFSAnalysisProvider(), days=IFS_DAYS, freq="D", lag=IFS_LAG_HOURS),
        OperaCatchUp(OPERARainfallProvider(), days=OPERA_DAYS, freq="1h", lag=OPERA_LAG_HOURS),
        OperaCatchUp(OPERAReflectivityProvider(), days=OPERA_DAYS, freq="1h", lag=OPERA_LAG_HOURS),
        CatchUp(MetopSgMwsProvider(), days=METOP_SG_DAYS, freq="D", lag=METOP_SG_DAILY_LAG_HOURS),
        CatchUp(MetopSgRoProvider(), days=METOP_SG_DAYS, freq="1h", lag=METOP_SG_LAG_HOURS),
        CatchUp(
            MetopSgMetimageProvider(), days=METOP_SG_DAYS, freq="10min", lag=METOP_SG_LAG_HOURS
        ),
    ]


def select(catchups: Sequence[CatchUp], patterns: Sequence[str]) -> List[CatchUp]:
    """Keep the stores whose name contains any of ``patterns``; all of them when empty.

    A pattern starting with ``=`` must match a store name exactly, so ``=dmi_harmonie``
    picks the surface store without ``dmi_harmonie_model_level``.

    Raises:
        SystemExit: when a pattern matches nothing, rather than silently running a
            shorter list than was asked for.
    """
    if not patterns:
        return list(catchups)

    def matches(pattern: str, name: str) -> bool:
        return pattern[1:] == name if pattern.startswith("=") else pattern in name

    chosen = [c for c in catchups if any(matches(p, c.name) for p in patterns)]
    unmatched = [p for p in patterns if not any(matches(p, c.name) for c in catchups)]
    if unmatched:
        raise SystemExit(
            f"no store matches {', '.join(unmatched)}; known stores are "
            f"{', '.join(c.name for c in catchups)}"
        )
    return chosen


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse the command line."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "patterns",
        nargs="*",
        metavar="STORE",
        help="only run stores whose name contains this, e.g. arome, meps_det",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="override how far back every selected store reaches",
    )
    parser.add_argument(
        "--list", action="store_true", help="print the stores and their windows, then exit"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="report the window each store would run, without downloading or writing",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    """Run each selected store over its catch-up window, one after the other."""
    args = parse_args(argv)
    now = now_utc()
    catchups = select(build_catchups(), args.patterns)

    if args.list:
        for catchup in catchups:
            times = catchup.timestamps(now, args.days)
            logger.info(
                f"{catchup.name}: {len(times)} partitions every {catchup.freq} "
                f"{times[0]} to {times[-1]}"
            )
        return

    touched: List[CatchUp] = []
    for catchup in catchups:
        provider = catchup.provider
        times = catchup.timestamps(now, args.days)
        logger.info(
            f"{provider.name}: {len(times)} partitions "
            f"{times[0]} to {times[-1]} -> {provider.describe_store()}"
        )
        if args.dry_run:
            continue
        written = catchup.run(times)
        logger.info(f"{provider.name}: wrote {written}/{len(times)} partitions")
        if written:
            touched.append(catchup)

    # Only the stores this run wrote to are checked: reading the append axis of one it
    # skipped entirely costs a store open for an answer that cannot have changed here.
    unsorted = [c.name for c in touched if not c.provider.axis_sorted()]
    if unsorted:
        logger.warning(
            "time axis out of order after backfilling "
            f"{', '.join(unsorted)}; materialise each store's reorder asset before "
            "reading a slice of it"
        )


if __name__ == "__main__":
    main()
