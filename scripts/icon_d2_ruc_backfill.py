"""Backfill the ICON-D2-RUC icechunk stores from the local DWD mirror.

The Dagster ``icon_d2_ruc_<cadence>`` assets archive one hour per run, which is fine for
keeping up but slow for a backlog of thousands of hours: decoding an hour's ~1500 GRIB
files with cfgrib is single-threaded and takes about a minute.

This runs the same :class:`~planetary_datasets.providers.icon.ICOND2RUCProvider` in two
stages. Worker processes decode hours ahead and park each processed hour as a lightly
compressed local Zarr under the scratch directory, a bounded number at a time. The main
process is the only writer: it appends each parked hour to its store in time order through
the provider's own ``write_to_icechunk``, so commits never race, and deletes it.

A store is only ever created from a complete hour, since the first write fixes the
variable set and every later hour must match it exactly. Stores this creates use the zstd
level in :data:`CREATE_CLEVEL`; existing stores keep the level they were created with.

The script resumes where it left off: hours already in a store are skipped. Hours that
sort before a store's last one (filling gaps) are appended out of order, so once it
finishes, run ``ICOND2RUCProvider(cadence).sort_axis()`` on those stores with nothing else
writing to them.

Usage::

    pixi run python scripts/icon_d2_ruc_backfill.py --cadences 60min --workers 1
    pixi run python scripts/icon_d2_ruc_backfill.py --cadences 15min,5min --workers 1
"""

from __future__ import annotations

import argparse
import multiprocessing as mp
import pathlib
import shutil
import sys
import time
import warnings

import dask
import numpy as np
import pandas as pd
import psutil
import xarray as xr
import zarr.codecs
from icechunk.xarray import to_icechunk
from loguru import logger

from planetary_datasets.common.store import (
    build_encoding,
    has_committed_data,
    quiet_timedelta_decoding,
)
from planetary_datasets.config import get_config
from planetary_datasets.providers.icon import ICOND2RUCProvider

CADENCES = ("60min", "15min", "5min")

#: Variables a complete hour has, per cadence.
FULL_VARS = {"60min": 59, "15min": 41, "5min": 4}

#: zstd level for stores this script creates. zstd 9 with bitshuffle runs at ~7 MB/s on
#: these fields against ~500 MB/s at 5, for a ~6% smaller store; at 9 the hourly store's
#: 2.5 GB per hour made writing, not GRIB decoding, the bottleneck. The level is fixed in
#: the array metadata, so every later append, Dagster's included, uses it.
CREATE_CLEVEL = {"60min": 5}

#: Hours decoded ahead of the writer, beyond one per worker. Bounds the parked backlog.
MAX_IN_FLIGHT = 3

warnings.filterwarnings("ignore", category=FutureWarning)

_grib_dir: pathlib.Path | None = None
_scratch: pathlib.Path | None = None
_providers: dict[str, ICOND2RUCProvider] = {}


def _init(grib_dir: pathlib.Path, scratch: pathlib.Path) -> None:
    """Set the mirror and scratch locations in this process."""
    global _grib_dir, _scratch
    _grib_dir, _scratch = grib_dir, scratch


def provider(cadence: str) -> ICOND2RUCProvider:
    """This process's provider for ``cadence``, cached so its directory index is reused."""
    if cadence not in _providers:
        _providers[cadence] = ICOND2RUCProvider(cadence, grib_dir=_grib_dir)
    return _providers[cadence]


def park(hour: pd.Timestamp, cadences: list[str]) -> dict:
    """Process one hour for each of ``cadences`` and park the results on scratch.

    Returns:
        Per cadence, ``(status, detail, seconds, *extra)``: status ``parked`` with the
        parked path and variable count, or ``nofiles``, ``empty`` or ``error``.
    """
    out: dict = {}
    for cadence in cadences:
        p = provider(cadence)
        t0 = time.time()
        try:
            files = p.fetch(hour)
            if not files:
                out[cadence] = ("nofiles", None, 0.0)
                continue
            ds = p.process(files, hour)
        except ValueError as exc:
            out[cadence] = ("empty", str(exc), time.time() - t0)
            continue
        except Exception as exc:  # noqa: BLE001 - one bad hour must not stop the backfill
            out[cadence] = ("error", f"{type(exc).__name__}: {exc}", time.time() - t0)
            continue
        path = _scratch / f"{hour:%Y%m%dT%H%M}_{cadence}.zarr"
        shutil.rmtree(path, ignore_errors=True)
        fast = zarr.codecs.BloscCodec(cname="lz4", clevel=1, shuffle="shuffle")
        with dask.config.set(scheduler="threads", num_workers=4):
            ds.to_zarr(
                path,
                mode="w",
                encoding={v: {"compressors": fast} for v in ds.data_vars},
                consolidated=True,
                zarr_format=3,
            )
        out[cadence] = ("parked", str(path), time.time() - t0, len(ds.data_vars))
    return out


def wait_for_memory(min_available_gb: float) -> None:
    """Block while less than ``min_available_gb`` of memory is available."""
    while (available := psutil.virtual_memory().available / 1e9) < min_available_gb:
        logger.warning(f"only {available:.1f} GB available, holding off")
        time.sleep(60)


def create_store(p: ICOND2RUCProvider, ds: xr.Dataset, clevel: int) -> bool:
    """First write of an empty store, as ``write_to_icechunk`` does it but at ``clevel``."""
    ds = p.prepare_for_write(ds)
    session = p.get_icechunk_repo().writable_session("main")
    with quiet_timedelta_decoding():
        to_icechunk(ds, session, encoding=build_encoding(ds, append_dim="time", clevel=clevel))
    session.commit(f"{p.name}: {np.atleast_1d(ds['time'].values)[0]}")
    logger.info(f"{p.name}: created store at zstd level {clevel}")
    return True


def write_parked(p: ICOND2RUCProvider, path: pathlib.Path, nvars: int, threads: int) -> bool:
    """Write one parked hour to its store. Returns True if it was committed."""
    ds = xr.open_zarr(path, consolidated=True, decode_timedelta=True)
    cadence = p.timescale
    empty = not has_committed_data(p.get_icechunk_repo())
    if empty and nvars != FULL_VARS[cadence]:
        logger.warning(f"{p.name}: {nvars} vars, not creating the store from an incomplete hour")
        return False
    with dask.config.set(scheduler="threads", num_workers=threads):
        if empty:
            return create_store(p, ds, CREATE_CLEVEL.get(cadence, 9))
        return p.write_to_icechunk(p.get_icechunk_repo(), ds)


def main() -> None:
    """Parse arguments and run the backfill."""
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--start", default="2026-04-18T00:00")
    ap.add_argument("--end", default=None, help="inclusive; default: second-newest run")
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--cadences", default=",".join(CADENCES))
    ap.add_argument("--write-threads", type=int, default=4)
    ap.add_argument("--min-available-gb", type=float, default=4.0)
    ap.add_argument("--grib-dir", type=pathlib.Path, default=None)
    ap.add_argument("--scratch", type=pathlib.Path, default=None)
    args = ap.parse_args()

    cadences = args.cadences.split(",")
    grib_dir = args.grib_dir or ICOND2RUCProvider().grib_dir
    scratch = args.scratch or get_config().scratch_dir / "icon_d2_ruc_parked"
    scratch.mkdir(parents=True, exist_ok=True)
    _init(grib_dir, scratch)

    runs = sorted(pd.Timestamp(d.name) for d in (grib_dir / "T_2M" / "r").iterdir())
    # The newest run may still be arriving.
    end = pd.Timestamp(args.end) if args.end else runs[-2]
    hours = pd.DatetimeIndex([r for r in runs if pd.Timestamp(args.start) <= r <= end])

    todo: dict[pd.Timestamp, list[str]] = {}
    for cadence in cadences:
        missing = provider(cadence).missing_timesteps(hours)
        logger.info(f"{cadence}: {len(missing)} of {len(hours)} mirrored hours not stored")
        for hour in missing:
            todo.setdefault(pd.Timestamp(hour), []).append(cadence)
    order = sorted(todo)
    if not order:
        logger.info("nothing to do")
        return
    logger.info(f"{len(order)} hours to do, {order[0]} .. {order[-1]}")

    stats = {c: {"written": 0, "skipped": 0, "failed": 0} for c in cadences}
    started = time.time()
    ctx = mp.get_context("spawn")
    with ctx.Pool(
        args.workers, initializer=_init, initargs=(grib_dir, scratch), maxtasksperchild=25
    ) as pool:
        pending: dict = {}
        submitted = 0
        for done, hour in enumerate(order, start=1):
            while submitted < len(order) and len(pending) < MAX_IN_FLIGHT + args.workers:
                wait_for_memory(args.min_available_gb)
                nxt = order[submitted]
                pending[nxt] = pool.apply_async(park, (nxt, todo[nxt]))
                submitted += 1
            result = pending.pop(hour).get()
            for cadence, (status, detail, secs, *extra) in result.items():
                if status != "parked":
                    level = "ERROR" if status == "error" else "WARNING"
                    logger.log(level, f"{cadence} {hour}: {status} {detail or ''}")
                    stats[cadence]["failed" if status == "error" else "skipped"] += 1
                    continue
                path, nvars = pathlib.Path(detail), extra[0]
                t0 = time.time()
                try:
                    ok = write_parked(provider(cadence), path, nvars, args.write_threads)
                except Exception as exc:  # noqa: BLE001 - logged, and the hour stays missing
                    stats[cadence]["failed"] += 1
                    logger.exception(f"{cadence} {hour}: write failed: {exc}")
                    continue
                finally:
                    shutil.rmtree(path, ignore_errors=True)
                stats[cadence]["written" if ok else "skipped"] += 1
                logger.log(
                    "INFO" if ok else "WARNING",
                    f"{cadence} {hour}: {'written' if ok else 'NOT written'} ({nvars} vars, "
                    f"process {secs:.0f}s, write {time.time() - t0:.0f}s)",
                )
            rate = (time.time() - started) / done
            logger.info(
                f"progress {done}/{len(order)} hours, {rate:.0f}s/hour, "
                f"eta {(len(order) - done) * rate / 3600:.1f}h | {stats}"
            )
    logger.info(f"finished: {stats}")


if __name__ == "__main__":
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    main()
