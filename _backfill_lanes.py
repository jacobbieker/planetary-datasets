"""Run one store's backfill lane: fill it forward from a start point, then keep it current.

    python _backfill_lanes.py <lane> [--threads N] [--once]
    python _backfill_lanes.py --list

A lane is one store and one writer. Each pass works out what the store is missing between
the lane's start and the live edge (the present, less the hours the source needs to
publish) with a single store read, then writes those partitions oldest first. With
nothing left it sleeps and starts another pass, which picks up whatever has been published
since and retries anything that failed. ``--once`` stops after one pass.

Stops (exit 2) after :data:`CONSECUTIVE_FAILURE_LIMIT` failures in a row, which is what an
outage looks like rather than a bad partition; the wrapper in ``_backfill_box.sh`` waits
and restarts it, and the restart resumes because stored partitions are skipped.

The long-archive stores reuse the table in ``one_offs/silam_harmonie_catchup.py`` for their
providers, cadence and publication lag, and its Docker staging for OPERA; only their start
points differ, because a lane fills the whole archive rather than the last few weeks.

Run from the repository root so ``planetary_datasets`` resolves here rather than to an
editable install elsewhere; the resolved path is printed to prove it.
"""

import argparse
import os
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable

CONSECUTIVE_FAILURE_LIMIT = 20
EXIT_FAILURE_STREAK = 2
PASS_PAUSE_SECONDS = 1800


@dataclass(frozen=True)
class Lane:
    """One store's lane.

    Attributes:
        provider: Builds the provider. Called once the thread caps are in place.
        start: First partition, ISO; or ``-<n>D`` for a window trailing the present, for
            sources that only keep the last few days.
        freq: Partition spacing.
        lag_hours: Hours a partition needs before the source has finished publishing it.
        opera: Stage each hour with the ``docker/earth2studio`` image before writing it.
        earth2studio: Stage each partition with the same image's earth2studio downloader;
            the provider is that dataset's publisher.
    """

    provider: Callable[[], Any]
    start: str
    freq: str
    lag_hours: int = 0
    opera: bool = False
    earth2studio: bool = False


def _metoffice(name: str) -> Callable[[], Any]:
    def build():
        from planetary_datasets.providers.metoffice import PROVIDERS

        return PROVIDERS[name]()

    return build


def _uk_marine():
    from planetary_datasets.providers.observations.uk_marine import (
        UKMarineObservationsProvider,
    )

    return UKMarineObservationsProvider()


def _osmc_ragged():
    from planetary_datasets.providers.observations.osmc_ragged import OSMCRaggedProvider

    return OSMCRaggedProvider()


def _nest(name: str) -> Callable[[], Any]:
    def build():
        from planetary_datasets.providers.virtualized.noaa_nests import NestVirtualProvider

        return NestVirtualProvider(name)

    return build


def _hafs(name: str) -> Callable[[], Any]:
    def build():
        from planetary_datasets.providers.virtualized.hafs import HafsVirtualProvider

        return HafsVirtualProvider(name)

    return build


def _earth2studio(name: str) -> Callable[[], Any]:
    def build():
        from planetary_datasets.providers.earth2studio_obs import provider_for

        return provider_for(name)

    return build


class Earth2StudioStager:
    """Stages an earth2studio dataset's partitions with the downloader image, as Dagster does.

    Attributes:
        provider: The dataset's publisher, which knows where a staged partition lands.
        workers: Partitions staged concurrently, each in its own container.
    """

    #: Gives up on a partition that has not staged by then; the next pass retries it.
    timeout = 1800

    def __init__(self, provider, workers: int = 2):
        self.provider = provider
        self.workers = workers

    def stage(self, it, root) -> bool:
        """Stage one partition. True once its manifest is written.

        A partition staged by an earlier, interrupted run is used as it is.
        """
        import subprocess

        from planetary_datasets.providers.earth2studio_download import DEFAULT_TARGET

        provider = self.provider
        if provider.has_staged(it):
            return True
        container = f"e2s-{provider.name}-{it:%Y%m%d%H%M}-{os.getpid()}"
        command = [
            "docker",
            "run",
            "--rm",
            "--name",
            container,
            "--volume",
            f"{root}:{DEFAULT_TARGET}",
            "--memory",
            f"{provider.dataset.memory_gb + 4:g}g",
            "--user",
            f"{os.getuid()}:{os.getgid()}",
            os.environ.get("EARTH2STUDIO_IMAGE", "planetary-datasets/earth2studio:latest"),
            "obs",
            "--dataset",
            provider.name,
            "--time",
            it.strftime("%Y-%m-%dT%H:%M"),
            "--target",
            DEFAULT_TARGET,
        ]
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=self.timeout)
        except subprocess.TimeoutExpired:
            # Stopping the docker client leaves the container running; remove it too.
            subprocess.run(["docker", "rm", "-f", container], capture_output=True)
            print(f"WARNING {provider.name}: staging {it} took over {self.timeout}s", flush=True)
            return False
        if result.returncode != 0:
            tail = (result.stderr or result.stdout).strip().splitlines()[-3:]
            print(f"WARNING {provider.name}: staging {it} failed: {' | '.join(tail)}", flush=True)
            return False
        return provider.has_staged(it)


def unstored_partitions(provider, partitions) -> list:
    """The partitions none of whose frames is stored, from one read of the store's axis.

    For stores of sub-partition frames: the store holding the partition's first frame is
    not enough, since a partition upstream lacks that frame for would be written again
    every pass.
    """
    import pandas as pd

    from planetary_datasets.common.store import existing_times

    stored = pd.DatetimeIndex(existing_times(provider.get_icechunk_repo(), provider.append_dim))
    stored = stored.sort_values()
    keep = []
    for it in partitions:
        start, end = provider.window(it)
        if stored.searchsorted(start) == stored.searchsorted(end):
            keep.append(it)
    return keep


def _ncep_upper_air():
    from planetary_datasets.providers.observations.ncep_prepbufr import NCEPUpperAirProvider

    # One cycle staged at a time: GDEX resets the box's connections under heavier load.
    return NCEPUpperAirProvider(prefetch_workers=1)


def _provider(module: str, cls: str, **kwargs) -> Callable[[], Any]:
    """A provider class from ``planetary_datasets.providers.<module>``, built lazily."""

    def build():
        import importlib

        found = importlib.import_module(f"planetary_datasets.providers.{module}")
        return getattr(found, cls)(**kwargs)

    return build


def _catchup(name: str) -> Callable[[], Any]:
    """A provider from the catch-up table, by store name."""

    def build():
        from one_offs.silam_harmonie_catchup import build_catchups

        for catchup in build_catchups():
            if catchup.name == name:
                return catchup.provider
        raise SystemExit(f"{name} is not in the catch-up table")

    return build


#: Met Office atmospheric starts are where each store stood when its lane was paused on
#: 2026-10-15 (see _backfill_queue.sh). Those stores are generational, so they keep walking
#: forward from there rather than reaching back. The ocean depth lane starts at the
#: six-month hole (Nov 2025 - Apr 2026) so one writer fills it and then carries on forward.
#: Everything else starts at its archive's first partition, as the Dagster partitions have it.
LANES: dict[str, Lane] = {
    "metoffice_global_deterministic_10km": Lane(
        _metoffice("metoffice_global_deterministic_10km"), "2025-01-14", "6h", 12
    ),
    "metoffice_global_deterministic_10km_6hourly_24hr": Lane(
        _metoffice("metoffice_global_deterministic_10km_6hourly_24hr"), "2025-02-12", "6h", 12
    ),
    "metoffice_uk_deterministic_2km": Lane(
        _metoffice("metoffice_uk_deterministic_2km"), "2025-04-27T09:00", "1h", 3
    ),
    "metoffice_global_wave": Lane(_metoffice("metoffice_global_wave"), "2025-04-09", "1D", 48),
    "metoffice_global_ocean_hourly": Lane(
        _metoffice("metoffice_global_ocean_hourly"), "2025-04-09", "1D", 48
    ),
    "metoffice_nws_wave": Lane(_metoffice("metoffice_nws_wave"), "2025-03-13", "1D", 48),
    "metoffice_nws_ocean_surface_hourly": Lane(
        _metoffice("metoffice_nws_ocean_surface_hourly"), "2025-03-13", "1D", 48
    ),
    "metoffice_nws_ocean_depth_hourly": Lane(
        _metoffice("metoffice_nws_ocean_depth_hourly"), "2025-10-25", "1D", 48
    ),
    # NCEP ADP upper air (GDEX d337000): aircraft to bkr/obs/ncep_amdar, sondes to
    # bkr/obs/ncep_adpupa. The full-aircraft 48 h files land ~58-70 h after the cycle.
    "ncep_upper_air": Lane(_ncep_upper_air, "1997-05-01T00:00", "6h", 72),
    # MRMS outside CONUS (CONUS is archived by dynamical.org). The stores already hold
    # 2020-10-14 to 2026-10-02; this fills the gap since Dagster stopped and keeps them
    # current, without the hour-by-hour completeness check over six years of archive.
    **{
        f"mrms_{region.lower()}": Lane(
            _provider("mrms", "MRMSProvider", region=region), "2026-09-25", "1h", 2
        )
        for region in ("ALASKA", "CARIB", "GUAM", "HAWAII")
    },
    "silam_aerosol": Lane(_provider("silam", "SILAMAerosolProvider"), "2025-04-26", "1D", 48),
    # From 2024 only: from 2008 it ran at ~16 min per 4 h window, over a year of backfill.
    "metop_avhrr": Lane(_provider("polar", "MetopAvhrrProvider"), "2024-01-01", "4h", 6),
    # From 2024, like AVHRR: from 2008 it was 163k hourly partitions, years of backfill.
    "metop_gome": Lane(_provider("polar", "MetopGomeProvider"), "2024-01-01", "1h", 6),
    "nordic_radar": Lane(_provider("meps", "NordicReflectivityProvider"), "2020-01-01", "1D", 48),
    # JPSS products NOAA deletes ~90 days after publication (VIIRS M-band SDR, ATMS snowfall
    # rate, MiRS imaging/sounding): a trailing window inside the retention, oldest first.
    **{
        f"{store}_{sat}": Lane(_provider("polar", cls, satellite=sat), "-90D", "1h", 3)
        for store, cls in (
            ("jpss_viirs_mband", "JpssViirsMbandProvider"),
            ("jpss_atms_sfr", "JpssAtmsSfrProvider"),
            ("jpss_mirs_img", "JpssMirsImgProvider"),
            ("jpss_mirs_snd", "JpssMirsSndProvider"),
        )
        for sat in ("n20", "n21", "snpp")
    },
    # AMPS (WRF24, from 2017-10-10T12; DRAKE and NZ from 2020-08-08), first 48 h of each
    # 00/12 UTC run, to bkr/dmi/amps_<domain>. GDEX archives a run within about a day. The
    # nests d3, d5 and d6 start from 2024 by request; from 2017 they were years of backfill.
    **{
        f"amps_{domain}": Lane(
            _provider("amps", "AMPSProvider", domain=domain),
            {
                "drake": "2020-08-08",
                "nz": "2020-08-08",
                "d3": "2024-01-01",
                "d5": "2024-01-01",
                "d6": "2024-01-01",
            }.get(domain, "2017-10-10T12:00"),
            "12h",
            36,
        )
        for domain in ("d1", "d2", "d3", "d5", "d6", "drake", "nz")
    },
    # A rolling ten-day bucket: anything older has already gone.
    "uk_marine": Lane(_uk_marine, "-10D", "1h", 2),
    # JPSS ATMS SDR, every satellite in one store (NOAA keeps the SDRs). The store is
    # complete from 2022-11 to 2025-09 and nearly empty since; this fills that and keeps
    # it current.
    "jpss_atms": Lane(_provider("polar", "JpssAtmsProvider"), "2025-08-01", "1h", 3),
    # GOES-East GLM 0.1 degree lightning density, staged by the earth2studio image like
    # the Dagster asset, which had only just started the store when it stopped on
    # 2026-10-02. Five-minute frames, an hour per partition.
    # West never had a store; it starts alongside East.
    **{
        f"goes_glm_grid_{slot}": Lane(
            _earth2studio(f"goes_glm_grid_{slot}"), "2026-09-25", "1h", 2, earth2studio=True
        )
        for slot in ("east", "west")
    },
    # DWD ICON-D2-RUC, one store per output cadence, read from the local first-hour mirror
    # that the icon_d2_ruc_mirror lane keeps (DWD keeps about a day). Both run every two
    # hours, so a run can reach the mirror ~3 h after it starts; five hours behind, every
    # field of a run has been mirrored before the run is written.
    **{
        f"icon_d2_ruc_{timescale}": Lane(
            _provider("icon", "ICOND2RUCProvider", timescale=timescale), "-2D", "1h", 5
        )
        for timescale in ("60min", "15min", "5min")
    },
    # NOAA OSMC / AOML marine observations, ragged: osmc_all plus one store per platform
    # type. Daily; a day is written 36 h after it starts, so late GTS reports have arrived.
    "osmc_ragged": Lane(_osmc_ragged, "2012-01-01", "1D", 36),
    # Virtual references to the NOAA HRRR Alaska and NAM Hawaii nests (gribberish). A run
    # is taken once every step is published: ~2 h for HRRR's 48 h runs, ~3 h for NAM's.
    "hrrr_alaska_sfc": Lane(_nest("hrrr_alaska_sfc"), "2018-07-11T18:00", "3h", 4),
    "hrrr_alaska_prs": Lane(_nest("hrrr_alaska_prs"), "2018-07-11T18:00", "3h", 4),
    "hrrr_alaska_nat": Lane(_nest("hrrr_alaska_nat"), "2018-07-11T18:00", "3h", 4),
    "nam_hawaii": Lane(_nest("nam_hawaii"), "2021-09-16", "6h", 5),
    # Virtual references to HAFS-A / HAFS-B, one storm-run per row. A partition is a cycle
    # (every storm in it); the last step lands ~6.3 h after the cycle.
    **{
        f"hafs_{v}_{d}": Lane(_hafs(f"hafs_{v}_{d}"), "2023-06-19T18:00", "6h", 7)
        for v in ("a", "b")
        for d in ("storm", "parent")
    },
    "geos_v2": Lane(_catchup("geos_v2"), "2026-01-01", "15min", 30),
    "ifs_analysis": Lane(_catchup("ifs_analysis"), "2016-01-01", "1D", 48),
    "opera_rainfall": Lane(_catchup("opera_rainfall"), "2018-01-01", "1h", 2, opera=True),
    "opera_dbz": Lane(_catchup("opera_dbz"), "2025-01-01", "1h", 2, opera=True),
    "metop_sg_mws": Lane(_catchup("metop_sg_mws"), "2026-06-11", "1D", 48),
    "metop_sg_ro": Lane(_catchup("metop_sg_ro"), "2026-02-26", "1h", 6),
    "metop_sg_metimage": Lane(_catchup("metop_sg_metimage"), "2026-08-27", "10min", 6),
}


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


def window(lane: Lane):
    """This pass's partitions, oldest first."""
    import pandas as pd

    now = pd.Timestamp.now(tz="UTC").tz_localize(None)
    end = (now - pd.Timedelta(lane.lag_hours, "h")).floor(lane.freq)
    if lane.start.startswith("-"):
        start = (now - pd.Timedelta(lane.start[1:])).floor(lane.freq)
    else:
        start = pd.Timestamp(lane.start)
    return pd.date_range(start, end, freq=lane.freq)


def main(argv) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("lane", nargs="?")
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--once", action="store_true", help="one pass, then exit")
    parser.add_argument(
        "--pause",
        type=int,
        default=PASS_PAUSE_SECONDS,
        help="seconds between passes once caught up",
    )
    args = parser.parse_args(argv)
    if args.list or not args.lane:
        print("\n".join(LANES))
        return 0
    lane = LANES[args.lane]
    configure_threads(args.threads)

    import dask
    import pandas as pd

    import planetary_datasets

    dask.config.set(scheduler="threads", num_workers=args.threads)
    provider = lane.provider()
    stager = None
    if lane.opera:
        from one_offs.silam_harmonie_catchup import OperaCatchUp

        stager = OperaCatchUp(provider, days=0, freq=lane.freq)
    elif lane.earth2studio:
        stager = Earth2StudioStager(provider)

    print(f"package : {planetary_datasets.__file__}", flush=True)
    print(f"lane    : {args.lane} ({type(provider).__name__})", flush=True)
    print(f"store   : {provider.store_path}", flush=True)
    print(f"start   : {lane.start}, every {lane.freq}, {lane.lag_hours} h behind", flush=True)

    consecutive = 0
    while True:
        began = time.time()
        hours = window(lane)
        todo = list(provider.missing_timesteps(hours)) if len(hours) else []
        if lane.earth2studio and todo:
            todo = unstored_partitions(provider, todo)
        print(
            f"\npass    : {hours[0] if len(hours) else '-'} -> "
            f"{hours[-1] if len(hours) else '-'}, {len(hours)} partition(s), "
            f"{len(todo)} missing",
            flush=True,
        )
        written = empty = failed = 0
        batch_size = stager.workers if stager else 1
        root = None
        if stager:
            root = provider.archive_root.expanduser().resolve()
            root.mkdir(parents=True, exist_ok=True)

        for offset in range(0, len(todo), batch_size):
            batch = todo[offset : offset + batch_size]
            staged = [True] * len(batch)
            if stager:
                with ThreadPoolExecutor(max_workers=batch_size) as pool:
                    staged = list(pool.map(lambda it: stager.stage(it, root), batch))
            for index, (it, ok) in enumerate(zip(batch, staged), start=offset + 1):
                started = time.time()
                if not ok:
                    # Usually a frame missing upstream rather than an outage, so it does not
                    # count toward the failure streak: a run of gaps would otherwise stop the
                    # lane at the same hours on every restart. The hour stays missing and is
                    # tried again next pass.
                    empty += 1
                    print(
                        f"[{index:6d}/{len(todo)}] {it} unstaged (see the warning above)",
                        flush=True,
                    )
                    continue
                try:
                    did_write = provider.run_partition(it, check_present=False)
                    if stager:
                        provider.discard_staged(it, settled=did_write)
                except Exception as exc:  # noqa: BLE001 - one bad partition must not stop the rest
                    failed += 1
                    consecutive += 1
                    print(
                        f"[{index:6d}/{len(todo)}] {it} FAILED "
                        f"{type(exc).__name__}: {str(exc)[:200]}",
                        flush=True,
                    )
                    traceback.print_exc()
                    if consecutive >= CONSECUTIVE_FAILURE_LIMIT:
                        print(f"\n{consecutive} failures in a row: stopping.", flush=True)
                        return EXIT_FAILURE_STREAK
                    continue
                consecutive = 0
                written += did_write
                empty += not did_write
                print(
                    f"[{index:6d}/{len(todo)}] {it} {'wrote' if did_write else 'empty'} "
                    f"{time.time() - started:7.1f}s",
                    flush=True,
                )

        elapsed = pd.Timedelta(seconds=time.time() - began).round("s")
        print(
            f"pass done in {elapsed}: {written} written, {empty} with no data, {failed} failed",
            flush=True,
        )
        if args.once:
            return 1 if failed else 0
        time.sleep(args.pause)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
