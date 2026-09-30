"""Retrieve ECMWF MARS data and stream it into the Icechunk stores as it lands.

Combines the retrieval of `mars.py` with the GRIB-to-Icechunk combine of
`planetary_datasets.providers.mars_icechunk` into one long-running process:

* Several download threads (``--download-threads``, 2 by default) work
  through the MARS retrievals in the same order, chunking and file names as
  `mars.py`, so files it already fetched are recognised. Each takes the next
  request as soon as its last one finishes, so requests are always queued at
  MARS behind the ones transferring, and the files already on disk are
  processed meanwhile. MARS itself caps how many of a user's requests run at
  once; the rest simply wait in its queue.
* Each file that lands is indexed, and every (timestep, family) it completes is
  ingested into the native O1280 store by a pool of worker processes, which
  write the regridded stores (0.25 and 1 degree, conservative) from the same
  decoded fields. Each timestep's messages are first staged on fast local disk
  (``<PLANETARY_DATASETS_SCRATCH_DIR>/mars_staging``) with one sequential read
  per source file; see `mars_icechunk.stage_timestep`.
* Downloaded GRIB is kept only while it is useful: once the files on disk
  total more than `keep_bytes` (0 by default, so as soon as they are safe to
  go), the oldest are deleted -- but only files every one of whose (timestep,
  family) pairs is flagged ingested in *every* store, the 0.25 and 1 degree
  regrids included. Data not yet uploaded is never deleted, and downloading
  pauses while free disk space is below `min_free_bytes`, so a backlog cannot
  fill the disk either.

A retrieval is skipped when its file is already on disk, or when every
timestep it would supply is already ingested into the native store: that is
what stops a deleted file from being downloaded a second time, and it skips
data that reached the store some other way (the August data ingested on
another machine, for instance).

Where things live
-----------------

Every path comes from :func:`planetary_datasets.config.get_config`, so the
same invocation works on the Linux box and on a laptop by pointing the two
directory variables somewhere that exists:

* GRIB: ``<PLANETARY_DATASETS_DATA_DIR>/mars``, or ``--source-dir``.
* Staging: ``<PLANETARY_DATASETS_SCRATCH_DIR>/mars_staging``, or
  ``--staging-dir``. This wants to be fast local disk, not a RAM-backed
  ``/tmp``: about 12GB per timestep in flight.
* Stores: ``ICECHUNK_BUCKET``/``ICECHUNK_PREFIX``, or ``--store-prefix``.
  ``ICECHUNK_LOCAL_PATH`` redirects them to a local directory.
* Credentials: ``AWS_PROFILE`` (or ``--aws-profile``) and
  ``ECMWF_API_KEY``/``ECMWF_API_EMAIL``.

Downloading is opt-in (``--download``): a laptop has nowhere near the free
space for a MARS retrieval, and the usual job there is to drain GRIB that is
already on disk into the stores and delete it.

Usage (from the repository root)::

    # Ingest what is on disk into all three stores, deleting each file once
    # every store has it. This is the laptop's normal run.
    pixi run python -m planetary_datasets.providers.mars_pipeline

    # Retrieve as well, on a machine with the disk for it.
    pixi run python -m planetary_datasets.providers.mars_pipeline \
        --download --start 2026-01-01 --end 2026-09-14

With no ``--start``/``--end`` the range is taken from the names of the
``output_*.grib`` files in the source directory, so a run over a directory
holding only August plans and checks only August.

The stores' time axes must already cover the requested range; extend them
first with ``python -m planetary_datasets.providers.mars_icechunk
--init-only`` (and ``--regrid 0.25 1 --init-only``), with nothing else
writing. This process must be the only one writing the GRIB index cache, and
the only one issuing the MARS retrievals, so stop `mars.py` and any separate
index build first. Regrid workers in ``--follow`` mode may run alongside it.

Another machine may write the same stores at the same time, as long as the
two work on different timesteps: every array is chunked one timestep per
chunk and commits rebase past each other (`mars_icechunk._commit`), so
disjoint timesteps never conflict. Extending a time axis is the exception and
must not run while anything else writes.

Requires ECMWF credentials: ``ECMWF_API_KEY`` and ``ECMWF_API_EMAIL`` in
``.env`` or the environment, falling back to ``~/.ecmwfapirc``. See
`planetary_datasets.providers.mars.mars_service`.
"""

from __future__ import annotations

import concurrent.futures
import dataclasses
import heapq
import os
import pathlib
import queue
import re
import shutil
import socket
import sys
import threading
import time
import traceback
from typing import Iterable, List, Sequence

import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.config import Config, get_config, reset_config_cache
from planetary_datasets.memory import (
    MemoryLimitExceeded,
    configure_malloc_arenas,
    memory_guard,
    total_memory_gb,
)
from planetary_datasets.providers import mars_icechunk as mi

# ----------------------------------------------------------------------
# MARS request vocabulary (as in mars.py)
# ----------------------------------------------------------------------

#: All 137 model levels
MODEL_LEVELS = "/".join(str(level) for level in range(1, 138))

#: Analysis parameters (GRIB codes)
ANALYSIS_ML_PARAMS = "75/76/77/129/130/131/132/133/135/138/152/155/203/246/247/248"

ANALYSIS_TIMES = "00:00:00/06:00:00/12:00:00/18:00:00"

#: Forecast parameters (GRIB codes) used to fill the hours between analyses
FORECAST_ML_PARAMS = "75/76/77/130/131/132/133/135/138/152/155/246/247/248/260290"

#: Steps 1-5 from each analysis time fill the gaps to the next analysis
FORECAST_STEPS = "1/2/3/4/5"

#: Up to IFS cycle 50r1, only the 00 and 12 UTC forecasts were archived in the
#: "oper"/"wave" streams; the 06 and 18 UTC runs were short cut-off forecasts,
#: archived in "scda" (atmosphere) and "scwv" (ocean wave).
SHORT_CUTOFF_STREAMS = {"oper": "scda", "wave": "scwv"}

#: IFS cycle 50r1 went operational with the 06 UTC run on 12 May 2026 and
#: retired both short cut-off streams: the 06 and 18 UTC forecasts moved into
#: "oper" and "wave" alongside 00 and 12. From this initialisation onwards a
#: request for "scda"/"scwv" matches nothing, and MARS fails it with
#: "ERROR 89 (MARS_EXPECTED_FIELDS): Expected <n>, got 0". Before it, the
#: reverse holds, so the stream depends on the initialisation time rather than
#: being fixed. Confirmed against the archive catalogue, which lists scwv as a
#: discontinued dataset with no month past May 2026, and by retrieving 06 and
#: 18 UTC model levels and 2D spectra from oper/wave for August 2026.
SHORT_CUTOFF_RETIRED = pd.Timestamp("2026-05-12 06:00")

#: Seconds a single socket read in a MARS child may block for. Bounds the
#: hang described in `_mars_execute`; must stay well under `retrieve`'s
#: ``stall_seconds`` so the child gives up before the parent kills it, and
#: well over the time any one chunk read takes.
MARS_SOCKET_TIMEOUT = 600

#: Ocean wave analysis parameters (GRIB codes, table 140)
WAVE_PARAMS = (
    "98.140/99.140/100.140/101.140/102.140/103.140/104.140/105.140/112.140/113.140/"
    "114.140/115.140/116.140/117.140/118.140/119.140/120.140/121.140/122.140/123.140/"
    "124.140/125.140/126.140/127.140/128.140/129.140/207.140/208.140/209.140/211.140/"
    "212.140/214.140/215.140/216.140/217.140/218.140/219.140/220.140/221.140/222.140/"
    "223.140/224.140/225.140/226.140/227.140/228.140/229.140/230.140/231.140/232.140/"
    "233.140/234.140/235.140/236.140/237.140/238.140/239.140/244.140/245.140/246.140/"
    "247.140/248.140/249.140/252.140/253.140/254.140/140131/140132/140133/140134"
)

#: Wave analyses are 3-hourly
WAVE_TIMES = "00:00:00/03:00:00/06:00:00/09:00:00/12:00:00/15:00:00/18:00:00/21:00:00"

#: 2D wave spectra: param and its direction/frequency bins
SPECTRA_PARAM = "251.140"
SPECTRA_DIRECTIONS = "/".join(str(d) for d in range(1, 37))
SPECTRA_FREQUENCIES = "/".join(str(f) for f in range(1, 30))

TB = 1000**4
GB = 1000**3


# ----------------------------------------------------------------------
# Where things live on this machine
# ----------------------------------------------------------------------


#: Re-exported so the pipeline and the combine cannot drift apart on where
#: they look for GRIB and where they stage it.
default_source_dir = mi.default_source_dir
default_staging_dir = mi.default_staging_dir


def build_provider(
    source_dir: str | pathlib.Path | None = None,
    store_prefix: str | None = None,
    aws_profile: str | None = None,
    staging_dir: str | pathlib.Path | None = None,
    mir_processes: int | None = None,
    config: Config | None = None,
) -> mi.MARSIcechunkProvider:
    """The provider this pipeline ingests with; see `mars_icechunk.build_provider`."""
    return mi.build_provider(
        source_dir=source_dir,
        store_prefix=store_prefix,
        aws_profile=aws_profile,
        staging_dir=staging_dir,
        mir_processes=mir_processes,
        config=config,
    )


#: ``output_<product>_<start>_<end>.grib``; the two dates are what is wanted.
_FILE_DATES = re.compile(r"_(\d{8})_(\d{8})\.grib$")


def dates_on_disk(source_dir: str | pathlib.Path) -> tuple[pd.Timestamp, pd.Timestamp] | None:
    """The span of dates the GRIB file names in `source_dir` cover, if any.

    Used as the default retrieval range, so a run over a directory holding one
    month plans and range-checks that month rather than the whole year.
    """
    dates = [
        pd.Timestamp(d)
        for path in pathlib.Path(source_dir).glob("output_*.grib")
        if (match := _FILE_DATES.search(path.name))
        for d in match.groups()
    ]
    return (min(dates), max(dates)) if dates else None


def forecast_stream(time: str, stream: str = "oper", date: pd.Timestamp | None = None) -> str:
    """The MARS stream holding the forecast initialised at `time` ("HH:MM:SS") on `date`.

    00 and 12 UTC are always in the base stream. 06 and 18 UTC were in the
    short cut-off stream until `SHORT_CUTOFF_RETIRED` and in the base stream
    from then on; `date` decides which. With no `date`, the short cut-off
    stream is returned, which is the answer for the whole archive before
    May 2026.
    """
    if not time.startswith(("06", "18")):
        return stream
    if date is not None:
        init = pd.Timestamp(date).normalize() + pd.Timedelta(int(time[:2]), unit="h")
        if init >= SHORT_CUTOFF_RETIRED:
            return stream
    return SHORT_CUTOFF_STREAMS[stream]


def build_mars_date(start: pd.Timestamp, end: pd.Timestamp | None = None) -> str:
    """A MARS date value, either a single date or a from/to range."""
    if end is None or end == start:
        return start.strftime("%Y-%m-%d")
    return f"{start.strftime('%Y-%m-%d')}/to/{end.strftime('%Y-%m-%d')}"


def build_mars_request(
    start: pd.Timestamp,
    end: pd.Timestamp | None = None,
    *,
    mars_class: str = "od",
    stream: str = "oper",
    expver: str = "1",
    type_: str = "an",
    levtype: str | None = "ml",
    levelist: str = MODEL_LEVELS,
    param: str = ANALYSIS_ML_PARAMS,
    time: str = ANALYSIS_TIMES,
    step: str | None = None,
    domain: str | None = None,
    direction: str | None = None,
    frequency: str | None = None,
) -> dict[str, str]:
    """A MARS request dictionary; see `mars.build_mars_request`."""
    request = {
        "class": mars_class,
        "date": build_mars_date(start, end),
        "expver": expver,
        "param": param,
        "stream": stream,
        "time": time,
        "type": type_,
    }
    if levtype is not None:
        request["levtype"] = levtype
        if levtype != "sfc":
            request["levelist"] = levelist
    for key, value in (
        ("step", step),
        ("domain", domain),
        ("direction", direction),
        ("frequency", frequency),
    ):
        if value is not None:
            request[key] = value
    return request


# ----------------------------------------------------------------------
# Retrieval plan
# ----------------------------------------------------------------------


@dataclasses.dataclass
class MarsJob:
    """One MARS retrieval: the request, its target file, and what it supplies."""

    target: pathlib.Path
    request: dict[str, str]
    #: The store family the file feeds (`mi.GROUP_ML`, `GROUP_WAVE` or `GROUP_SPECTRA`).
    group: str
    #: Every valid time the file holds data for.
    valid_times: pd.DatetimeIndex


def _hours(times: str) -> List[int]:
    return [int(t[:2]) for t in times.split("/")]


def _chunked(
    days: pd.DatetimeIndex,
    days_per_request: int,
    template: str,
    source_dir: pathlib.Path,
    group: str,
    init_hours: Sequence[int],
    steps: Sequence[int],
    init_time: str | None = None,
    base_stream: str | None = None,
    **request_kwargs,
) -> List[MarsJob]:
    """Split `days` into requests the way `mars.retrieve_mars_chunked` does.

    With `init_time` and `base_stream` given, each request's stream is resolved
    from its own dates by `forecast_stream`, since which stream holds a 06/18
    UTC forecast changed at `SHORT_CUTOFF_RETIRED`. A request whose days
    straddle that change would need two streams at once and is rejected rather
    than silently retrieved from the wrong one.
    """
    jobs = []
    for i in range(0, len(days), days_per_request):
        chunk = days[i : i + days_per_request]
        if init_time is not None:
            first = forecast_stream(init_time, base_stream, chunk[0])
            last = forecast_stream(init_time, base_stream, chunk[-1])
            if first != last:
                raise ValueError(
                    f"a request covering {chunk[0]:%Y-%m-%d} to {chunk[-1]:%Y-%m-%d} at "
                    f"{init_time} straddles the {SHORT_CUTOFF_RETIRED} stream change "
                    f"({first} before, {last} after); split the range at that date"
                )
            request_kwargs = {**request_kwargs, "stream": first}
        target = source_dir / template.format(
            start=chunk[0].strftime("%Y%m%d"), end=chunk[-1].strftime("%Y%m%d")
        )
        valid = pd.DatetimeIndex(
            sorted(
                {
                    d + pd.Timedelta(h + s, unit="h")
                    for d in chunk
                    for h in init_hours
                    for s in steps
                }
            )
        )
        request = build_mars_request(chunk[0], chunk[-1], **request_kwargs)
        jobs.append(MarsJob(target, request, group, valid))
    return jobs


def plan_jobs(
    start: pd.Timestamp,
    end: pd.Timestamp,
    source_dir: str | pathlib.Path,
    days_per_group: int = 6,
    analysis_days_per_request: int = 3,
    forecast_days_per_request: int = 2,
    wave_days_per_request: int = 6,
    spectra_days_per_request: int = 2,
    spectra_forecast_days_per_request: int = 3,
) -> List[MarsJob]:
    """Every retrieval between `start` and `end`, in the order `mars.py` makes them.

    Grouped by date: all products for one group of days come before the next
    group. Per group, in order: model level analyses, model level forecast
    steps 1-5 per init time, 3-hourly wave analyses, 3-hourly 2D spectra
    analyses, and 2D spectra forecast steps 1-5 per init time. Request sizes
    are chosen to stay under MARS's 75GB limit; see `mars.retrieve_mars_hourly`
    for the arithmetic.
    """
    source_dir = pathlib.Path(source_dir)
    days = pd.date_range(start=pd.Timestamp(start).normalize(), end=end, freq="1D")
    analysis_hours = _hours(ANALYSIS_TIMES)
    wave_hours = _hours(WAVE_TIMES)
    steps = [int(s) for s in FORECAST_STEPS.split("/")]
    wave = {"stream": "wave", "domain": "g", "levtype": None}
    spectra = {
        **wave,
        "param": SPECTRA_PARAM,
        "direction": SPECTRA_DIRECTIONS,
        "frequency": SPECTRA_FREQUENCIES,
    }

    jobs: List[MarsJob] = []
    for i in range(0, len(days), days_per_group):
        group = days[i : i + days_per_group]
        jobs += _chunked(
            group, analysis_days_per_request, "output_an_{start}_{end}.grib", source_dir,
            mi.GROUP_ML, analysis_hours, [0],
        )  # fmt: skip
        for t in ANALYSIS_TIMES.split("/"):
            jobs += _chunked(
                group, forecast_days_per_request, f"output_fc_{t[:2]}z_{{start}}_{{end}}.grib",
                source_dir, mi.GROUP_ML, [int(t[:2])], steps,
                init_time=t, base_stream="oper",
                type_="fc", param=FORECAST_ML_PARAMS, time=t,
                step=FORECAST_STEPS,
            )  # fmt: skip
        jobs += _chunked(
            group, wave_days_per_request, "output_wave_an_{start}_{end}.grib", source_dir,
            mi.GROUP_WAVE, wave_hours, [0], **wave, param=WAVE_PARAMS, time=WAVE_TIMES,
        )  # fmt: skip
        jobs += _chunked(
            group, spectra_days_per_request, "output_spectra_an_{start}_{end}.grib", source_dir,
            mi.GROUP_SPECTRA, wave_hours, [0], **spectra, time=WAVE_TIMES,
        )  # fmt: skip
        for t in ANALYSIS_TIMES.split("/"):
            jobs += _chunked(
                group, spectra_forecast_days_per_request,
                f"output_spectra_fc_{t[:2]}z_{{start}}_{{end}}.grib", source_dir,
                mi.GROUP_SPECTRA, [int(t[:2])], steps,
                init_time=t, base_stream="wave",
                **{k: v for k, v in spectra.items() if k != "stream"}, type_="fc", time=t,
                step=FORECAST_STEPS,
            )  # fmt: skip
    return jobs


def _mars_execute(request: dict[str, str], target: str) -> None:
    """Child-process body of `retrieve`.

    The service is built here rather than in the parent because it is a
    ``spawn`` child: it re-imports the module and re-reads the configuration,
    and an ``ECMWFService`` would not survive being pickled across.

    Sets a socket timeout first, because ecmwfapi builds none of its HTTP
    calls with one. Once a transfer finishes, ``execute`` ends by asking the
    server to drop the request, and that call has been seen to block on an SSL
    read that never returns: the target is complete and closed, but the child
    sits in ``read(2)`` forever. `retrieve` watches the file rather than the
    client, so such a child is indistinguishable from a dead transfer, and
    `stall_seconds` later the request is killed and a finished retrieval
    deleted -- measured at eleven whole files, each killed 30 minutes to the
    second after its "Done". The timeout is far longer than any single chunk
    read (transfers measured 19-71 MB/s against 1MB chunks) and shorter than
    `stall_seconds`, so it only ever fires on a hang. ecmwfapi already
    discards whatever that last call raises, so timing it out simply skips
    the tidy-up and lets the retrieval finish.

    Exits through ``os._exit`` rather than returning, so that a client which
    leaves a thread behind cannot hang interpreter shutdown either. Nothing
    here needs that cleanup: ecmwfapi has written and closed the target
    already, and the buffers that do matter are flushed first.
    """
    # Deferred so importing this module does not need ecmwfapi at all.
    from planetary_datasets.providers.mars import mars_service  # noqa: PLC0415

    socket.setdefaulttimeout(MARS_SOCKET_TIMEOUT)
    code = 0
    try:
        mars_service().execute(request, target)
    except BaseException:
        traceback.print_exc()
        code = 1
    finally:
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(code)


def retrieve(
    job: MarsJob,
    attempts: int = 5,
    retry_seconds: int = 600,
    stall_seconds: int = 1800,
    poll_seconds: int = 30,
    execute=_mars_execute,
) -> bool:
    """Run one MARS request, writing `job.target` only once it is complete.

    Downloads to ``<target>.tmp`` and renames on success, so the target's
    existence means a finished file. A failed request is retried after
    `retry_seconds`, since most failures are transient (queue limits, tape
    drives); after `attempts` failures the job is given up on and False
    returned, so one bad request does not stall the rest.

    The request runs in a child process so that a stalled transfer can be
    killed: ecmwfapi reads the transfer socket without a timeout, and a
    transfer that stops sending leaves it waiting forever -- seen in practice,
    a 58GB file stuck at 46GB with the connection still open. Once bytes have
    started arriving, `stall_seconds` without the file growing counts as a
    failed attempt. Time before the first byte is not counted: MARS can queue
    a request for hours while it reads from tape.
    """
    import multiprocessing  # noqa: PLC0415

    tmp = job.target.with_name(job.target.name + ".tmp")
    for attempt in range(1, attempts + 1):
        logger.info(f"MARS request ({attempt}/{attempts}) -> {job.target.name}: {job.request}")
        tmp.unlink(missing_ok=True)
        child = multiprocessing.get_context("spawn").Process(
            target=execute, args=(job.request, str(tmp)), name=f"mars-{job.target.name}"
        )
        child.start()
        size, last_growth, stalled = -1, time.monotonic(), False
        while child.is_alive():
            child.join(poll_seconds)
            current = tmp.stat().st_size if tmp.exists() else 0
            if current != size:
                size, last_growth = current, time.monotonic()
            elif size > 0 and time.monotonic() - last_growth > stall_seconds:
                logger.error(
                    f"{job.target.name}: transfer stalled at {size / GB:.1f}GB for "
                    f"{stall_seconds // 60} minutes; killing the request"
                )
                child.kill()
                child.join()
                stalled = True
        if not stalled and child.exitcode == 0 and tmp.exists():
            tmp.rename(job.target)
            logger.info(f"MARS retrieval complete: {job.target.name} ({size / GB:.1f}GB)")
            return True
        if not stalled:
            logger.error(f"MARS request for {job.target.name} failed (exit code {child.exitcode})")
        if attempt < attempts:
            time.sleep(retry_seconds)
    tmp.unlink(missing_ok=True)
    return False


# ----------------------------------------------------------------------
# Ingest flags of every store, held locally
# ----------------------------------------------------------------------


def _read_flags(repo) -> dict[str, pd.Series]:
    ds = xr.open_zarr(
        repo.readonly_session("main").store, consolidated=False, decode_timedelta=True
    )
    times = pd.DatetimeIndex(ds["time"].values)
    return {g: pd.Series(ds[mi._INGESTED[g]].values.astype(bool), index=times) for g in mi.GROUPS}


class FlagBook:
    """A local copy of every store's ``ingested_*`` flags.

    Read once at startup and updated from each finished ingest, so scheduling
    and the deletion check do not have to read three stores from S3 for every
    decision. `refresh` re-reads everything, which picks up what other writers
    (the follow-mode regrid) have done since.
    """

    def __init__(self, repos: dict[str, object]):
        self._repos = repos
        self._lock = threading.Lock()
        self.refresh()

    def refresh(self) -> None:
        fresh = {name: _read_flags(repo) for name, repo in self._repos.items()}
        with self._lock:
            self._flags = fresh

    def update(self, flags: dict[str, dict[str, bool]], it: pd.Timestamp) -> None:
        """Record the flags one store has at `it`, as read back by a worker."""
        with self._lock:
            for store, groups in flags.items():
                for g, value in groups.items():
                    self._flags[store][g].loc[it] = value

    def native_done(self, group: str, times: Iterable[pd.Timestamp]) -> bool:
        with self._lock:
            series = self._flags["native"][group]
            return all(bool(series.get(t, False)) for t in times)

    def done_everywhere(self, group: str, times: Iterable[pd.Timestamp]) -> bool:
        """Whether every store -- native and both regrids -- has `group` at `times`.

        What scheduling goes on, rather than `native_done`: the regridded
        stores are written from the same decoded fields as the native one, so a
        timestep the native store already has but a regrid does not is still
        work, and its source file cannot be deleted until every store has it.
        Re-writing the native store's copy in the process is harmless -- the
        same values land in the same chunks.
        """
        times = list(times)
        with self._lock:
            return all(
                bool(flags[group].get(t, False)) for flags in self._flags.values() for t in times
            )

    def store_names(self) -> List[str]:
        with self._lock:
            return sorted(self._flags)

    def missing_stores(self, group: str, it: pd.Timestamp) -> List[str]:
        """Names of the stores still lacking `group` at `it`, for logging."""
        with self._lock:
            return [
                name
                for name, flags in self._flags.items()
                if not bool(flags[group].get(it, False))
            ]

    def all_done(self, pairs: Iterable[tuple[str, pd.Timestamp]]) -> bool:
        with self._lock:
            return all(
                bool(flags[g].get(t, False)) for flags in self._flags.values() for g, t in pairs
            )


# ----------------------------------------------------------------------
# Worker processes
# ----------------------------------------------------------------------

#: Per-process state for the ingest workers: providers, open repositories and
#: regridding weights (a few seconds to build) are reused across timesteps.
_WORKER: dict = {}


def _worker_state(cfg: dict) -> dict:
    if not _WORKER:
        # This worker's share of the memory budget, published to the whole
        # child process rather than only to the provider: `require_memory`
        # inside `_load_fields` reads the process-wide config, and sizing a
        # GRIB block against the *host* budget while the guard around it
        # enforces a third of that is how a block passes the precheck and is
        # then killed anyway.
        os.environ["MEMORY_CEILING_GB"] = repr(cfg["memory_ceiling_gb"])
        reset_config_cache()
        # The parent's own Config, not whatever this ``spawn`` child would
        # read from its environment: a pipeline built against an explicit
        # Config (a test's, or one Dagster supplied) must have its workers
        # write to the same stores `_check_stores` validated, not to the
        # ambient ICECHUNK_BUCKET.
        config = dataclasses.replace(
            cfg["config"], memory_ceiling_gb=cfg["memory_ceiling_gb"]
        )
        native = build_provider(
            source_dir=cfg["source_dir"],
            store_prefix=cfg["store_prefix"],
            aws_profile=cfg["aws_profile"],
            staging_dir=cfg["staging_dir"],
            mir_processes=cfg["mir_processes"],
            config=config,
        )
        targets = [mi.MARSRegridProvider(native, resolution=r) for r in cfg["resolutions"]]
        _WORKER.update(
            native=native,
            repo=native.get_icechunk_repo(),
            targets=[(t, t.get_icechunk_repo()) for t in targets],
        )
    return _WORKER


def _spawn_pool(max_workers: int) -> concurrent.futures.ProcessPoolExecutor:
    """A process pool whose workers are spawned, never forked.

    The pool is created after the download threads are running and after
    `_check_stores` has opened three icechunk repositories, so the parent
    holds logging locks and a tokio runtime with its own threads. ``fork``,
    the default on Linux, copies those locks in whatever state they happen to
    be in and the child deadlocks the first time it logs or touches S3. The
    MARS retrieval child in `retrieve` already spawns for the same reason.
    """
    import multiprocessing  # noqa: PLC0415

    return concurrent.futures.ProcessPoolExecutor(
        max_workers, mp_context=multiprocessing.get_context("spawn")
    )


def _ingest_task(cfg: dict, it: pd.Timestamp, groups: List[str], rows: pd.DataFrame) -> dict:
    """Ingest the `groups` of one timestep, then read back every store's flags there.

    `rows` is the part of the GRIB index for `it`, which is all the provider
    needs to find the messages; passing it avoids each worker loading the
    whole index.

    The work is wrapped in a `memory_guard`. It takes no explicit ceiling:
    `_worker_state` has already published this worker's share of the budget as
    ``MEMORY_CEILING_GB``, so the guard's default of *baseline RSS plus the
    budget* is the right limit -- an explicit ``ceiling_gb`` is an absolute RSS
    limit, which would be compared against a number that already includes the
    interpreter's own footprint.

    The guard is deliberately *inside* the worker rather than around the pool:
    killing a pool worker raises ``BrokenProcessPool`` in the parent and fails
    every other timestep in flight.

    A breach is logged rather than raised. Writing a timestep is not separable
    from committing it -- the variables are written and committed together, so
    that data and flags land atomically -- so by the time the guard fires the
    work is already durable. Failing the task would make the parent re-ingest a
    timestep that succeeded, repeating the most expensive work in the pipeline
    for no gain. The flags read back below are the truth either way.
    """
    state = _worker_state(cfg)
    native = state["native"]
    native._index = rows.reset_index(drop=True)
    files = sorted(rows["path"].unique())
    try:
        with memory_guard(what=f"mars ingest {it}"):
            native.write_to_icechunk(
                state["repo"], native.process(files, it, groups=groups), regrids=state["targets"]
            )
    except MemoryLimitExceeded as exc:
        logger.warning(f"{exc} The timestep was written; reduce --ingest-workers.")

    stores = {"native": state["repo"], **{str(t.resolution): r for t, r in state["targets"]}}
    flags = {}
    for name, repo in stores.items():
        ds = xr.open_zarr(
            repo.readonly_session("main").store, consolidated=False, decode_timedelta=True
        )
        at = int(pd.DatetimeIndex(ds["time"].values).get_loc(it))
        flags[name] = {g: bool(ds[mi._INGESTED[g]].isel(time=at).values) for g in mi.GROUPS}
    return flags


# ----------------------------------------------------------------------
# The pipeline
# ----------------------------------------------------------------------


class MarsPipeline:
    """Download, index, ingest and clean up, all concurrently; see the module docstring."""

    def __init__(
        self,
        start: pd.Timestamp | None = None,
        end: pd.Timestamp | None = None,
        source_dir: str | pathlib.Path | None = None,
        store_prefix: str | None = None,
        aws_profile: str | None = None,
        resolutions: Sequence[float] = (0.25, 1.0),
        keep_bytes: int = 0,
        min_free_bytes: int | None = None,
        ingest_workers: int = 3,
        download_threads: int = 4,
        scan_workers: int = 8,
        download: bool = False,
        refresh_seconds: int = 1800,
        staging_dir: str | pathlib.Path | None = None,
        mir_processes: int | None = None,
        config: Config | None = None,
    ):
        self.config = config or get_config()
        # Resolved, because the index keys on the path string: the provider
        # records resolved paths, and a relative --source-dir would otherwise
        # make every file on disk look unindexed.
        self.source_dir = pathlib.Path(
            source_dir or default_source_dir(self.config)
        ).expanduser().resolve()
        # With no range given, cover exactly what is on disk.
        span = dates_on_disk(self.source_dir)
        if (start is None or end is None) and span is None:
            raise ValueError(
                f"no output_*.grib files in {self.source_dir} to take a date range from; "
                "pass --start and --end, or --source-dir"
            )
        self.start = pd.Timestamp(start) if start is not None else span[0]
        self.end = pd.Timestamp(end) if end is not None else span[1]
        self.staging_dir = pathlib.Path(
            staging_dir or default_staging_dir(self.config)
        ).expanduser()
        self.native = build_provider(
            source_dir=self.source_dir,
            store_prefix=store_prefix,
            aws_profile=aws_profile,
            staging_dir=self.staging_dir,
            mir_processes=mir_processes,
            config=self.config,
        )
        self.targets = [mi.MARSRegridProvider(self.native, resolution=r) for r in resolutions]
        self.keep_bytes = keep_bytes
        # Enough headroom for every thread's transfer (a MARS request is capped
        # at 75GB) plus the staging directories, or downloads would start and
        # then stall against their own output.
        self.min_free_bytes = (
            min_free_bytes if min_free_bytes is not None else download_threads * 80 * GB + 50 * GB
        )
        self.ingest_workers = ingest_workers
        self.scan_workers = scan_workers
        self.download = download
        self.download_threads = download_threads
        self._job_iter = iter(())
        self._jobs_lock = threading.Lock()
        self._download_threads_left = 0
        self.refresh_seconds = refresh_seconds
        # Each worker gets an equal share of the budget, because they run at
        # the same time: a per-worker guard at the whole budget would let
        # three of them together take three times what the host has. Resolved
        # from this pipeline's own config, not the ambient one, so an injected
        # Config's MEMORY_CEILING_GB is not silently ignored.
        self.memory_ceiling_gb = mi.budget_gb(self.config) / max(1, ingest_workers)
        self.cfg = {
            "source_dir": str(self.source_dir),
            "store_prefix": self.native.store_prefix,
            "aws_profile": aws_profile,
            "resolutions": list(resolutions),
            "staging_dir": str(self.staging_dir),
            "mir_processes": mir_processes,
            "memory_ceiling_gb": self.memory_ceiling_gb,
            # Carried rather than re-read in the worker; `Config` is a frozen
            # dataclass of strings and paths, so it pickles to a spawn child.
            "config": self.native.config,
        }
        self.jobs = plan_jobs(self.start, self.end, self.source_dir)
        self._arrived: queue.Queue[pathlib.Path] = queue.Queue()
        self._downloading = threading.Event()
        #: (time, group) -> the source files an ingest last used. A group is
        #: only retried once its files change, so a truncated file (whose
        #: timestep can never complete) is not re-ingested in a loop.
        self._attempted: dict[tuple[pd.Timestamp, str], frozenset] = {}
        #: Failed ingests per timestep; each is retried up to `max_retries` times.
        self._failures: dict[pd.Timestamp, int] = {}
        self.max_retries = 3
        # The pieces that touch MARS, the disk and the stores, as attributes so
        # they can be swapped for fakes when testing the scheduling.
        self.retrieve_fn = retrieve
        self.scan_fn = mi.scan_grib
        self.ingest_fn = _ingest_task
        self.executor = _spawn_pool
        self.idle_seconds = 30

    def describe(self) -> str:
        """Everything the run resolved from the configuration, without doing any of it.

        Behind ``--dry-run``, and the cheapest way to check on a new machine
        that the paths, the store and the plan are what was meant before
        committing a MARS retrieval to them. Touches no network and opens no
        store.
        """
        stores = [self.native, *self.targets]
        lines = [
            f"range:       {self.start:%Y-%m-%d} to {self.end:%Y-%m-%d}",
            f"source dir:  {self.source_dir}",
            f"staging dir: {self.staging_dir}",
            f"index:       {self.native.index_path}",
            f"downloads:   {'on' if self.download else 'off'} "
            f"({self.download_threads} thread(s), "
            f"pausing below {self.min_free_bytes / GB:.0f}GB free)",
            f"ingest:      {self.ingest_workers} worker(s), "
            f"{self.memory_ceiling_gb:.1f} GB each",
            f"keep:        {self.keep_bytes / TB:.2f}TB of GRIB on disk",
            f"retrievals:  {len(self.jobs)} planned",
        ]
        lines += [f"store:       {store.icechunk_path}" for store in stores]
        return "\n".join(lines)

    # -- index ---------------------------------------------------------

    def _load_index(self) -> None:
        path = self.native.index_path
        self.cache = pd.read_parquet(path) if path.exists() else pd.DataFrame(columns=mi._INDEX_COLUMNS)
        self._prune_index()
        self._reindex()

    def _prune_index(self) -> None:
        """Drop index rows for files that are no longer on disk.

        Normally `_clean_up` prunes as it deletes, but a file can go without
        that -- deleted by hand, or lost with the run that was writing it. A
        stale row makes the pipeline offer a timestep whose messages cannot be
        read, and every attempt at it fails in staging.
        """
        if not len(self.cache):
            return
        paths = self.cache["path"].unique()
        gone = [p for p in paths if not pathlib.Path(p).exists()]
        if not gone:
            return
        self.cache = self.cache[~self.cache["path"].isin(gone)].reset_index(drop=True)
        self._save_index()
        logger.warning(
            f"Dropped {len(gone)} file(s) from the index that are no longer on disk, "
            f"e.g. {pathlib.Path(gone[0]).name}"
        )

    def _reindex(self) -> None:
        self.index = self.cache[~self.cache["short_name"].isin(self.native.exclude)]
        self._by_time = {t: rows for t, rows in self.index.groupby("valid_time")}
        self._indexed = set(self.cache["path"].unique())
        pairs = self.index[["path", "group", "valid_time"]].drop_duplicates()
        self._pairs_by_path = {
            path: set(zip(rows["group"], pd.DatetimeIndex(rows["valid_time"])))
            for path, rows in pairs.groupby("path")
        }

    def _save_index(self) -> None:
        path = self.native.index_path
        partial = path.with_name(path.name + ".tmp")
        self.cache.to_parquet(partial, index=False)
        partial.replace(path)

    def _add_to_index(self, scanned: pd.DataFrame) -> None:
        if len(scanned):
            frames = [f for f in (self.cache, scanned) if len(f)]
            self.cache = pd.concat(frames, ignore_index=True)
            self._save_index()
            self._reindex()

    # -- scheduling ----------------------------------------------------

    def _pending(self, it: pd.Timestamp) -> List[str]:
        """Families at `it` that the index offers and some store still lacks.

        Every store counts, not just the native one: a family the native store
        has but a regrid does not still has to be decoded from GRIB, both to
        fill that regrid and so the source file becomes deletable.
        """
        rows = self._by_time.get(it)
        if rows is None:
            return []
        todo = []
        for g, group_rows in rows.groupby("group"):
            files = frozenset(group_rows["path"])
            if self.flags.done_everywhere(g, [it]) or self._attempted.get((it, g)) == files:
                continue
            todo.append(g)
        return sorted(todo)

    # -- downloads -----------------------------------------------------

    def _next_job(self) -> MarsJob | None:
        with self._jobs_lock:
            return next(self._job_iter, None)

    def _download_loop(self) -> None:
        """Take retrievals in plan order and run them, feeding `_arrived`.

        `download_threads` of these share one iterator over the plan, so
        while some requests transfer, the others wait in the MARS queue --
        MARS queues and stages a request from tape before any byte moves,
        which can take longer than the transfer itself. The disk-space check
        runs before each request starts; `min_free_bytes` should leave room
        for every thread's file (up to 75GB each).
        """
        try:
            while (job := self._next_job()) is not None:
                if job.target.exists():
                    self._arrived.put(job.target)
                    continue
                if self.flags.native_done(job.group, job.valid_times):
                    logger.info(f"{job.target.name}: already ingested, not downloading")
                    continue
                while shutil.disk_usage(self.source_dir).free < self.min_free_bytes:
                    logger.warning(
                        f"Less than {self.min_free_bytes / GB:.0f}GB free on {self.source_dir}; "
                        "pausing downloads until processing frees space"
                    )
                    time.sleep(600)
                if self.retrieve_fn(job):
                    self._arrived.put(job.target)
        except Exception:
            logger.exception(f"{threading.current_thread().name} failed")
        finally:
            with self._jobs_lock:
                self._download_threads_left -= 1
                last = self._download_threads_left == 0
            logger.info(f"{threading.current_thread().name} finished")
            if last:
                self._downloading.clear()

    # -- clean-up ------------------------------------------------------

    def _source_files(self) -> List[pathlib.Path]:
        return sorted(p.resolve() for p in self.source_dir.glob(self.native.pattern))

    def _uploaded(self, path: pathlib.Path) -> bool:
        """Whether every (family, timestep) `path` holds is flagged in every store."""
        pairs = self._pairs_by_path.get(str(path))
        return bool(pairs) and self.flags.all_done(pairs)

    def _clean_up(self) -> None:
        """Delete the oldest fully uploaded files while more than `keep_bytes` are on disk.

        A file goes only once every (family, timestep) it holds is flagged
        ingested in *all three* stores -- native, 0.25 and 1 degree -- and only
        after those flags have been re-read from the stores themselves. The
        `FlagBook` is a cache, fed by worker read-backs and a half-hourly
        refresh, and a stale entry is not a good enough reason to destroy a
        retrieval that costs hours to fetch again. The re-read is one pass over
        the three stores per clean-up, not per file.
        """
        files = [(p.stat().st_mtime, p.stat().st_size, p) for p in self._source_files()]
        total = sum(size for _, size, _ in files)
        if total <= self.keep_bytes:
            return
        candidates = [entry for entry in sorted(files) if self._uploaded(entry[2])]
        if not candidates:
            if total > self.keep_bytes:
                logger.debug(
                    f"{total / TB:.2f}TB of GRIB on disk, over the {self.keep_bytes / TB:.1f}TB "
                    "budget, but nothing more is fully uploaded yet"
                )
            return

        self.flags.refresh()
        removed = []
        for _, size, path in candidates:
            if total <= self.keep_bytes:
                break
            if not self._uploaded(path):
                logger.warning(
                    f"Not deleting {path.name}: the cached flags said every store had it, "
                    "but a fresh read of the stores disagrees. Keeping the file."
                )
                continue
            pairs = self._pairs_by_path[str(path)]
            path.unlink()
            total -= size
            removed.append(str(path))
            logger.info(
                f"Deleted {path.name} ({size / GB:.0f}GB): all {len(pairs)} (family, timestep) "
                f"pair(s) confirmed in {', '.join(self.flags.store_names())}; "
                f"{total / TB:.2f}TB of GRIB left on disk"
            )
        if removed:
            self.cache = self.cache[~self.cache["path"].isin(removed)].reset_index(drop=True)
            self._save_index()
            self._reindex()

    # -- main loop -----------------------------------------------------

    def _check_stores(self) -> dict[str, object]:
        """Open every store and check it can take everything this run may write.

        That is the planned retrievals *and* whatever the index already holds:
        with downloads off there are no jobs, and the files on disk may reach
        outside the requested range anyway.
        """
        planned = {t for job in self.jobs for t in job.valid_times}
        indexed = set(pd.DatetimeIndex(self.index["valid_time"])) if len(self.index) else set()
        wanted = pd.DatetimeIndex(sorted(planned | indexed))
        if not len(wanted):
            raise ValueError(
                f"nothing to do: no retrievals planned and no indexed GRIB in {self.source_dir}"
            )
        repo = self.native.get_icechunk_repo()
        outside = wanted.difference(self.native.store_times(repo))
        if len(outside):
            raise ValueError(
                f"{len(outside)} planned timestep(s) fall outside {self.native.icechunk_path}'s "
                f"time axis, e.g. {outside[0]}. Extend it first with mars_icechunk --init-only."
            )
        repos = {"native": repo}
        for target in self.targets:
            repos[str(target.resolution)] = mi._open_regrid_target(target, wanted)
        return repos

    def _warn_if_staging_is_memory_backed(self) -> None:
        """Warn when the staging directory looks like a RAM disk.

        A timestep stages 10-12GB, and several are in flight, so staging into
        a tmpfs quietly spends tens of gigabytes of RAM. Worse, it is invisible
        to the memory guard: shared memory is not counted in a process's RSS,
        so the run is killed by the kernel with every guard still reporting a
        healthy footprint.
        """
        staging = self.staging_dir
        filesystem = ""
        try:  # Linux only; there is no tmpfs to fall into on macOS.
            import subprocess  # noqa: PLC0415

            filesystem = subprocess.run(
                ["stat", "-f", "-c", "%T", str(staging)],
                capture_output=True,
                text=True,
                timeout=10,
            ).stdout.strip()
        except Exception:  # noqa: BLE001 - a diagnostic must never stop the run
            pass
        if filesystem in {"tmpfs", "ramfs"}:
            logger.warning(
                f"Staging directory {staging} is on {filesystem}, which is memory-backed. "
                "Each timestep stages 10-12GB there and it does not show up in the memory "
                "guard. Point PLANETARY_DATASETS_SCRATCH_DIR, or --staging-dir, at real disk."
            )

    def _clear_staging(self) -> None:
        """Remove timestep staging directories left behind by killed workers.

        Safe only at startup, before this pipeline's workers exist: another
        pipeline or ingest sharing the staging directory would lose its files.
        """
        staging = self.native.staging_dir
        for stale in staging.glob("mars_stage_*") if staging.exists() else ():
            shutil.rmtree(stale, ignore_errors=True)
            logger.info(f"Removed stale staging directory {stale}")

    def _sweep_partial_downloads(self) -> None:
        """Remove ``<target>.tmp`` files left by a killed retrieval.

        `retrieve` unlinks its own partial on every exit path, but a process that
        is killed outright leaves one behind, and nothing else can see it: every
        discovery and clean-up path here globs ``output_*.grib``, which does not
        match ``output_*.grib.tmp``. A few of those (up to 75GB each) fill the
        data array, and `_download_loop` then parks forever below
        `min_free_bytes` because `_clean_up` has nothing it is willing to delete.

        Safe only at startup, before this pipeline's download threads exist, for
        the same reason as `_clear_staging`.
        """
        if not self.source_dir.exists():
            return
        for stale in sorted(self.source_dir.glob(f"{self.native.pattern}.tmp")):
            size = stale.stat().st_size if stale.exists() else 0
            stale.unlink(missing_ok=True)
            logger.info(f"Removed partial download {stale.name} ({size / GB:.1f}GB)")

    def run(self) -> None:
        # Bound glibc arena growth in the workers and MIR children started
        # below; unbounded arenas inflate their RSS well past the real working
        # set, which is what the per-worker memory guard measures.
        configure_malloc_arenas()
        self.source_dir.mkdir(parents=True, exist_ok=True)
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        self._clear_staging()
        self._sweep_partial_downloads()
        # The index first: `_check_stores` range-checks the times it holds, and
        # pruning it drops files that went away since the last run.
        self._load_index()
        self.flags = FlagBook(self._check_stores())
        on_disk = self._source_files()
        logger.info(
            f"Source {self.source_dir}, staging {self.staging_dir}, "
            f"store {self.native.icechunk_path}; "
            f"{len(on_disk)} GRIB file(s) on disk "
            f"({sum(p.stat().st_size for p in on_disk) / TB:.2f}TB), "
            f"{len(self._indexed)} indexed"
        )
        logger.info(
            f"{self.ingest_workers} ingest worker(s), "
            f"{self.memory_ceiling_gb:.1f} GB each of a {total_memory_gb():.1f} GB host"
        )
        self._warn_if_staging_is_memory_backed()
        logger.info(
            f"{len(self.jobs)} MARS retrievals planned from {self.start:%Y-%m-%d} to "
            f"{self.end:%Y-%m-%d}"
            + ("" if self.download else " (downloads off; processing what is on disk)")
        )

        # Anything already in every store is deletable before a single ingest
        # runs -- the previous run may have been killed between the two.
        self._clean_up()

        # Files already on disk go first, oldest dates first.
        for path in sorted(self._source_files(), key=mi._date_order):
            self._arrived.put(path)
        if self.download:
            self._downloading.set()
            self._job_iter = iter(self.jobs)
            self._download_threads_left = self.download_threads
            for n in range(self.download_threads):
                threading.Thread(
                    target=self._download_loop, name=f"mars-download-{n}", daemon=True
                ).start()

        to_scan: List[pathlib.Path] = []
        seen: set[pathlib.Path] = set()
        ready: List[pd.Timestamp] = []  # heap of timesteps that may have work
        queued: set[pd.Timestamp] = set()
        scans: dict[concurrent.futures.Future, pathlib.Path] = {}
        ingests: dict[concurrent.futures.Future, tuple[pd.Timestamp, List[str]]] = {}
        in_flight: set[pd.Timestamp] = set()
        last_refresh = time.monotonic()

        def offer(times: Iterable[pd.Timestamp]) -> None:
            for it in times:
                it = pd.Timestamp(it)
                if it not in queued:
                    queued.add(it)
                    heapq.heappush(ready, it)

        with (
            self.executor(self.scan_workers) as scan_pool,
            self.executor(self.ingest_workers) as ingest_pool,
        ):
            while True:
                # Newly arrived files: index what is not indexed yet.
                while True:
                    try:
                        path = self._arrived.get_nowait()
                    except queue.Empty:
                        break
                    if path in seen:
                        continue
                    seen.add(path)
                    if str(path) in self._indexed:
                        offer(self._by_time_for(path))
                    else:
                        to_scan.append(path)
                to_scan.sort(key=mi._date_order)
                while to_scan and len(scans) < self.scan_workers:
                    path = to_scan.pop(0)
                    scans[scan_pool.submit(self.scan_fn, str(path))] = path

                # Timesteps whose families are now available, earliest first.
                while ready and len(ingests) < self.ingest_workers:
                    it = heapq.heappop(ready)
                    queued.discard(it)
                    if it in in_flight:
                        continue
                    groups = self._pending(it)
                    if not groups:
                        continue
                    rows = self._by_time[it]
                    for g in groups:
                        self._attempted[(it, g)] = frozenset(rows[rows["group"] == g]["path"])
                    detail = ", ".join(
                        f"{g} -> {'+'.join(self.flags.missing_stores(g, it))}" for g in groups
                    )
                    logger.info(f"Ingesting {it} ({detail})")
                    future = ingest_pool.submit(self.ingest_fn, self.cfg, it, groups, rows)
                    ingests[future] = (it, groups)
                    in_flight.add(it)

                if not (scans or ingests or ready or to_scan) and not self._downloading.is_set():
                    if self._arrived.empty():
                        break

                if not (scans or ingests):
                    # Waiting on downloads: nothing to collect.
                    time.sleep(self.idle_seconds)
                done, _ = concurrent.futures.wait(
                    list(scans) + list(ingests),
                    timeout=self.idle_seconds,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                for future in done:
                    if future in scans:
                        path = scans.pop(future)
                        try:
                            scanned = future.result()
                        except Exception:
                            logger.exception(f"Indexing {path.name} failed")
                            continue
                        self._add_to_index(scanned)
                        offer(scanned["valid_time"].unique())
                    else:
                        it, groups = ingests.pop(future)
                        in_flight.discard(it)
                        try:
                            self.flags.update(future.result(), it)
                            logger.info(f"Ingested {it} ({', '.join(groups)})")
                        except Exception:
                            failures = self._failures.get(it, 0) + 1
                            self._failures[it] = failures
                            retry = failures <= self.max_retries
                            logger.exception(
                                f"Ingesting {it} ({', '.join(groups)}) failed "
                                f"({failures}/{self.max_retries + 1})"
                                f"{'; will retry' if retry else '; giving up until restart'}"
                            )
                            if retry:
                                for g in groups:
                                    self._attempted.pop((it, g), None)
                        # Families may have arrived while it was in flight.
                        offer([it])
                        self._clean_up()

                if time.monotonic() - last_refresh > self.refresh_seconds:
                    self.flags.refresh()
                    last_refresh = time.monotonic()
                    self._clean_up()

        logger.info("Nothing left to download or ingest")

    def _by_time_for(self, path: pathlib.Path) -> Iterable[pd.Timestamp]:
        return sorted({t for _, t in self._pairs_by_path.get(str(path), ())})


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--start", default=None, help="First date to retrieve (default: earliest GRIB on disk)"
    )
    parser.add_argument(
        "--end",
        default=None,
        help="Last date to retrieve, inclusive (default: latest GRIB on disk)",
    )
    parser.add_argument(
        "--source-dir",
        default=None,
        help=f"Directory holding the output_*.grib files (default: {default_source_dir()})",
    )
    parser.add_argument(
        "--staging-dir",
        default=None,
        help=(
            "Fast local disk to stage each timestep's messages on, about 12GB per "
            f"timestep in flight (default: {default_staging_dir()})"
        ),
    )
    parser.add_argument(
        "--store-prefix",
        default=mi.STORE_PREFIX,
        help="Store location relative to ICECHUNK_BUCKET (default: %(default)s)",
    )
    parser.add_argument(
        "--aws-profile",
        default=None,
        help="Profile with write access to the bucket (default: $AWS_PROFILE)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Resolve the configuration, plan the retrievals and print what would "
            "happen, without opening a store, downloading or ingesting anything"
        ),
    )
    parser.add_argument(
        "--regrid",
        type=float,
        nargs="*",
        default=[0.25, 1.0],
        metavar="DEGREES",
        help="Regridded stores to write alongside the native one (default: 0.25 1)",
    )
    parser.add_argument(
        "--keep-tb",
        type=float,
        default=0.0,
        help=(
            "Keep this many TB of GRIB on disk, deleting the oldest fully uploaded files "
            "beyond it. 0, the default, deletes each file as soon as every store has it"
        ),
    )
    parser.add_argument(
        "--min-free-gb",
        type=float,
        default=None,
        help=(
            "Pause downloads while less than this much disk space is free "
            "(default: 80GB per download thread plus 50GB)"
        ),
    )
    parser.add_argument("--ingest-workers", type=int, default=3)
    parser.add_argument(
        "--mir-processes",
        type=int,
        default=None,
        help=(
            "MIR processes per spectral variable, per ingest worker "
            f"(default: $MARS_MIR_PROCESSES or {mi.DEFAULT_MIR_PROCESSES})"
        ),
    )
    parser.add_argument(
        "--download-threads",
        type=int,
        default=4,
        help=(
            "MARS requests in flight at once. MARS runs only a couple of a user's requests "
            "at a time and queues the rest, so more threads keep the queue primed and the "
            "transfers back to back; raise --min-free-gb to match"
        ),
    )
    parser.add_argument("--scan-workers", type=int, default=8)
    downloads = parser.add_mutually_exclusive_group()
    downloads.add_argument(
        "--download",
        dest="download",
        action="store_true",
        default=False,
        help="Also retrieve from MARS. Off by default: only process what is on disk",
    )
    downloads.add_argument(
        "--no-download",
        dest="download",
        action="store_false",
        help="Only process the files already on disk (the default)",
    )
    parser.add_argument("--log-file", default=None, help="Also log to this file")
    args = parser.parse_args()

    if args.log_file:
        logger.add(args.log_file, level="INFO", enqueue=True)
    logger.remove(0)
    logger.add(sys.stderr, level="INFO")

    pipeline = MarsPipeline(
        start=pd.Timestamp(args.start) if args.start else None,
        end=pd.Timestamp(args.end) if args.end else None,
        source_dir=args.source_dir,
        staging_dir=args.staging_dir,
        store_prefix=args.store_prefix,
        aws_profile=args.aws_profile,
        resolutions=args.regrid,
        keep_bytes=int(args.keep_tb * TB),
        min_free_bytes=int(args.min_free_gb * GB) if args.min_free_gb is not None else None,
        ingest_workers=args.ingest_workers,
        mir_processes=args.mir_processes,
        download_threads=args.download_threads,
        scan_workers=args.scan_workers,
        download=args.download,
    )
    if args.dry_run:
        print(pipeline.describe())
    else:
        pipeline.run()
