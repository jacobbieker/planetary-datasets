"""Backfill the KENDA-CH1 icechunk stores from the local GRIB archive, in parallel.

The Dagster assets in ``dags/assets/nwp/regional_lam.py`` archive one hour per run through
:meth:`~planetary_datasets.base.BaseProvider.run_partition`. That is fine for keeping up,
but a backlog of thousands of hours is compression-bound: the stores use zstd level 9 on
whole-field chunks, so one analysis hour costs about three minutes of CPU, nearly all of it
inside a single process.

This runs the same providers, with the same era routing and the same ``process``, but
spreads the hours over a pool of worker processes:

1. The coordinator moves a scratch branch to ``main``, extends every time-dimensioned array
   by one slot per hour in the batch, writes the new ``time`` values, and commits that on
   the branch.
2. Each worker gets a fork of a session on that branch, processes one hour and writes it
   into its slot one variable at a time, so a worker holds one decoded field at once
   (about 1 GB for an 80-level field) rather than the whole hour.
3. The coordinator merges the forks, commits, checks the result, and only then moves
   ``main`` to it, conditional on ``main`` not having moved. A batch that fails anywhere
   leaves ``main`` exactly as it was.

The GRIB archive lives on one machine and the processing can run on another, so there are
two commands:

``stage`` (on the machine with the archive)
    Works out which complete hours each store is missing and pushes them, a bounded number
    ahead, into ``<staging>/<product>/<YYYYMMDDHHMM>/`` on the processing host.
``run`` (on the processing host)
    Consumes staged hours in batches, deletes each hour once it is in ``main``, and when the
    stager has finished, sorts the time axis of every store it appended to.

Nothing else may write to the KENDA stores while this runs: the final sort moves chunks.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import multiprocessing
import os
import pathlib
import shlex
import shutil
import subprocess
import sys
import time

import numpy as np
import pandas as pd
import xarray as xr
import zarr
from loguru import logger
from xarray.coding.times import encode_cf_datetime

from planetary_datasets.common.store import sort_append_axis
from planetary_datasets.memory import memory_guard, process_tree_rss_gb
from planetary_datasets.providers.kenda import (
    HORIZONTAL_CONSTANTS,
    VERTICAL_CONSTANTS,
    KENDAAnalysisProvider,
    KENDAEra,
    KENDAForecastProvider,
    KENDAProviderBase,
    KENDASchemaChanged,
)

PRODUCTS: dict[str, type[KENDAProviderBase]] = {
    "analysis": KENDAAnalysisProvider,
    "forecast": KENDAForecastProvider,
}

#: Branch batches are assembled on before ``main`` is moved to them.
BRANCH = "kenda-backfill"
STAMP = "%Y%m%d%H%M"
DONE = "DONE"
TOTAL = "TOTAL"


# ------------------------------------------------------------------------------- stage


def _ssh(host: str, command: str) -> str:
    return subprocess.run(
        ["ssh", "-o", "BatchMode=yes", host, command],
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def _complete_hours(provider: KENDAProviderBase, names: list[str]) -> dict[pd.Timestamp, list]:
    """Hours in the listing that carry exactly their era's fields, with their filenames."""
    by_hour: dict[pd.Timestamp, list[str]] = {}
    for name in names:
        parsed = provider.parse_filename(name)
        if parsed is not None and parsed[1] == provider.step:
            by_hour.setdefault(parsed[0], []).append(name)
    complete = {}
    for it, files in sorted(by_hour.items()):
        found = {provider.parse_filename(f)[2] for f in files}
        if provider.is_complete(it, found):
            complete[it] = sorted(files)
    return complete


def stage(args: argparse.Namespace) -> None:
    """Push the hours each store is missing to the processing host, a few at a time."""
    archive = pathlib.Path(args.archive)
    names = os.listdir(archive)
    start, end = pd.Timestamp(args.start), pd.Timestamp(args.end)
    host, root = args.remote, args.remote_dir.rstrip("/")

    queues: dict[str, list[tuple[pd.Timestamp, list[str]]]] = {}
    for product in args.products:
        provider = PRODUCTS[product]()
        complete = _complete_hours(provider, names)
        wanted = pd.DatetimeIndex([it for it in complete if start <= it <= end])
        todo = provider.missing_timesteps(wanted)
        queues[product] = [(it, complete[it]) for it in todo]
        by_era: dict[str, int] = {}
        for it in todo:
            prefix = provider.era_for(it).store_prefix
            by_era[prefix] = by_era.get(prefix, 0) + 1
        logger.info(
            f"{product}: {len(complete)} complete hours in the archive, {len(wanted)} in "
            f"range, {len(todo)} missing from the stores: {by_era}"
        )

    if args.dry_run:
        return

    product_dirs = " ".join(shlex.quote(f"{root}/{p}") for p in args.products)
    _ssh(host, f"mkdir -p {shlex.quote(root)}/constants {product_dirs}")
    for product, queue in queues.items():
        _ssh(host, f"rm -f {root}/{product}/{DONE} && echo {len(queue)} > {root}/{product}/{TOTAL}")

    constants = [
        str(archive / name)
        for name in names
        if name.startswith((HORIZONTAL_CONSTANTS, VERTICAL_CONSTANTS))
    ]
    subprocess.run(["rsync", "-a", *constants, f"{host}:{root}/constants/"], check=True)

    pushed = {product: 0 for product in queues}
    while any(queues.values()):
        moved = False
        for product, queue in queues.items():
            if not queue:
                continue
            staged = _ssh(host, f"ls {root}/{product}").split()
            ready = [s for s in staged if s.isdigit()]
            if len(ready) >= args.lookahead:
                continue
            it, files = queue.pop(0)
            stamp = it.strftime(STAMP)
            incoming = f"{root}/{product}/.incoming-{stamp}"
            started = time.time()
            # The .idx files carry cfgrib's index, which saves re-scanning each message.
            sources = [str(archive / f) for f in files]
            sources += [s + ".5b7b6.idx" for s in sources if os.path.exists(s + ".5b7b6.idx")]
            subprocess.run(["rsync", "-a", *sources, f"{host}:{incoming}/"], check=True)
            _ssh(
                host,
                f"(cd {incoming} && ln -sf ../../constants/* .) && "
                f"mv {incoming} {root}/{product}/{stamp}",
            )
            pushed[product] += 1
            moved = True
            logger.info(
                f"{product}: staged {it} ({pushed[product]} pushed, {len(queue)} to go, "
                f"{len(ready) + 1} waiting) in {time.time() - started:.0f}s"
            )
        if not moved:
            time.sleep(args.poll)

    for product in queues:
        _ssh(host, f"touch {root}/{product}/{DONE}")
    logger.info(f"staging finished: {pushed}")


# --------------------------------------------------------------------------------- run


def _time_arrays(group: zarr.Group) -> dict[str, zarr.Array]:
    """Every array in the store that runs along ``time``, including ``time`` itself."""
    arrays = {}
    for name, array in group.arrays():
        dims = array.metadata.dimension_names or ()
        if "time" in dims:
            if dims[0] != "time":
                raise ValueError(f"{name}: time is not the leading dimension ({dims})")
            arrays[name] = array
    return arrays


def _write_hour(
    fork,
    product: str,
    hour_dir: str,
    it: pd.Timestamp,
    index: int,
    expected: dict[str, tuple[str, tuple[str, ...]]],
    memory_gb: float,
    blosc_threads: int,
):
    """Worker: process one hour and write it into slot ``index`` of ``fork``.

    Returns the fork, for the coordinator to merge, and the variables written.
    """
    import dask
    import numcodecs

    numcodecs.blosc.set_nthreads(blosc_threads)
    dask.config.set(scheduler="synchronous")
    logger.remove()
    logger.add(sys.stderr, level="INFO")

    provider = PRODUCTS[product](archive_path=hour_dir)
    files = provider.fetch(it)
    if not files:
        raise FileNotFoundError(f"{product}: {it} is not complete in {hour_dir}")

    started = time.time()
    group = zarr.open_group(fork.store, mode="r+")
    with memory_guard(ceiling_gb=memory_gb, what=f"{product} {it}"):
        ds = provider.process(files, it)
        fields = {str(v) for v in ds.data_vars if "time" in ds[v].dims}
        if fields != set(expected):
            raise KENDASchemaChanged(
                f"{product} {it}: only in the data {sorted(fields - set(expected))}, only "
                f"in the store {sorted(set(expected) - fields)}"
            )
        for name in sorted(fields):
            dtype, dims = expected[name]
            values = ds[name].transpose(*dims).values
            if str(values.dtype) != dtype:
                raise KENDASchemaChanged(f"{name}: {values.dtype} in the data, {dtype} stored")
            group[name][index : index + 1] = values
            del values
    logger.info(
        f"{product} {it}: wrote {len(fields)} fields into slot {index} in "
        f"{time.time() - started:.0f}s (rss {process_tree_rss_gb():.1f} GB)"
    )
    return fork, sorted(fields)


class Runner:
    """Consume staged hours for one product and write them in parallel batches."""

    def __init__(self, args: argparse.Namespace):
        """Set up the queue directory and the worker pool from the ``run`` arguments."""
        self.args = args
        self.product = args.product
        self.provider = PRODUCTS[self.product]()
        self.queue_dir = pathlib.Path(args.staging_dir).expanduser() / self.product
        self.failed_dir = self.queue_dir / "failed"
        self.touched: set[str] = set()
        self.written = 0
        self.started = time.time()
        self.pool = cf.ProcessPoolExecutor(
            max_workers=args.workers,
            mp_context=multiprocessing.get_context("spawn"),
            max_tasks_per_child=args.tasks_per_worker,
        )

    def ready(self) -> list[tuple[pd.Timestamp, pathlib.Path]]:
        """Staged hours that the stager has finished pushing, oldest first."""
        if not self.queue_dir.is_dir():
            return []
        hours = []
        for entry in sorted(self.queue_dir.iterdir()):
            if entry.is_dir() and entry.name.isdigit():
                hours.append((pd.to_datetime(entry.name, format=STAMP), entry))
        return hours

    def run(self) -> None:
        """Write batches until the stager is done and the queue is empty, then finish."""
        waiting_since = None
        while True:
            ready = self.ready()
            done = (self.queue_dir / DONE).exists()
            if not ready:
                if done:
                    break
                time.sleep(self.args.poll)
                continue

            # One era per batch: they are different stores.
            era = self.provider.era_for(ready[0][0])
            batch = [h for h in ready if self.provider.era_for(h[0]) == era]
            batch = batch[: self.args.batch_size]

            # A batch takes as long as its slowest hour, so a short one idles most of the
            # pool. Give the stager a while to fill it, unless it has nothing more to send
            # or the era ends here.
            short = len(batch) < self.args.batch_size and len(batch) == len(ready)
            waiting_since = waiting_since or time.time()
            if short and not done and time.time() - waiting_since < self.args.fill_wait:
                time.sleep(self.args.poll)
                continue
            waiting_since = None

            missing = set(self.provider.missing_timesteps(pd.DatetimeIndex([h[0] for h in batch])))
            present = [h for h in batch if h[0] not in missing]
            for it, path in present:
                logger.info(f"{self.product}: {it} is already stored; dropping it")
                shutil.rmtree(path)
            batch = [h for h in batch if h not in present]
            if batch:
                self.write_batch(era, batch)

        self.finish()

    def write_batch(self, era: KENDAEra, batch: list[tuple[pd.Timestamp, pathlib.Path]]) -> None:
        """Write one era's batch of hours and publish it to ``main``; see the module docstring."""
        repo = self.provider.repo_for(era)
        hours = [it for it, _ in batch]
        label = f"{self.provider.name}: {len(hours)} step(s) {hours[0]}..{hours[-1]}"
        started = time.time()

        main = repo.lookup_branch("main")
        if BRANCH in repo.list_branches():
            repo.reset_branch(BRANCH, main)
        else:
            repo.create_branch(BRANCH, main)

        # 1. Reserve one slot per hour along time, on the branch.
        session = repo.writable_session(BRANCH)
        group = zarr.open_group(session.store, mode="r+")
        arrays = _time_arrays(group)
        lengths = {name: array.shape[0] for name, array in arrays.items()}
        if len(set(lengths.values())) != 1:
            raise RuntimeError(f"{era.store_prefix}: time lengths disagree: {lengths}")
        start = lengths["time"]
        for array in arrays.values():
            array.resize((start + len(hours), *array.shape[1:]))
        time_array = arrays["time"]
        encoded, _, _ = encode_cf_datetime(
            np.array(hours, dtype="datetime64[ns]"),
            units=time_array.attrs["units"],
            calendar=time_array.attrs.get("calendar", "standard"),
        )
        time_array[start:] = np.asarray(encoded).astype(time_array.dtype)
        expected = {
            name: (str(array.dtype), tuple(array.metadata.dimension_names))
            for name, array in arrays.items()
            if name != "time"
        }
        for name, array in arrays.items():
            if any(key in array.attrs for key in ("scale_factor", "add_offset")):
                raise RuntimeError(f"{name} is packed; this writer only handles plain floats")
        session.commit(f"{label}: reserve slots {start}..{start + len(hours) - 1}")

        # 2. Workers fill the slots, each in its own fork.
        session = repo.writable_session(BRANCH)
        futures = {
            self.pool.submit(
                _write_hour,
                session.fork(),
                self.product,
                str(path),
                it,
                start + i,
                expected,
                self.args.worker_memory_gb,
                self.args.blosc_threads,
            ): (it, path)
            for i, (it, path) in enumerate(batch)
        }
        forks, failed = [], []
        for future in cf.as_completed(futures):
            it, path = futures[future]
            try:
                fork, fields = future.result()
            except Exception as exc:  # noqa: BLE001 - one bad hour must not stop the backfill
                logger.exception(f"{self.product}: {it} failed: {exc}")
                failed.append((it, path))
                if isinstance(exc, cf.process.BrokenProcessPool):
                    self.pool = cf.ProcessPoolExecutor(
                        max_workers=self.args.workers,
                        mp_context=multiprocessing.get_context("spawn"),
                        max_tasks_per_child=self.args.tasks_per_worker,
                    )
                continue
            if set(fields) != set(expected):
                failed.append((it, path))
                continue
            forks.append(fork)

        if failed:
            # The branch is abandoned; main never saw it. Set the bad hours aside and let
            # the rest go round again in the next batch.
            self.failed_dir.mkdir(exist_ok=True)
            for it, path in failed:
                target = self.failed_dir / path.name
                if target.exists():
                    shutil.rmtree(target)
                path.rename(target)
            logger.error(
                f"{label}: {len(failed)} hour(s) failed and were moved to {self.failed_dir}; "
                "retrying the rest"
            )
            return

        # 3. Merge, commit, check, and only then publish to main.
        session.merge(*forks)
        tip = session.commit(label)
        check = xr.open_zarr(repo.readonly_session(branch=BRANCH).store, consolidated=False)
        stored = pd.DatetimeIndex(check.time.values[start:])
        if list(stored) != hours:
            raise RuntimeError(f"{label}: branch holds {list(stored)}, expected {hours}")
        probe = min(
            (v for v in check.data_vars if "time" in check[v].dims),
            key=lambda v: check[v].isel(time=0).size,
        )
        values = check[probe].isel(time=slice(start, None)).values
        empty = [h for h, v in zip(hours, values) if not np.isfinite(v).any()]
        if empty:
            raise RuntimeError(f"{label}: {probe} is all-NaN for {empty}")
        repo.reset_branch("main", tip, from_snapshot_id=main)

        for _, path in batch:
            shutil.rmtree(path)
        self.touched.add(era.store_prefix)
        self.written += len(hours)
        self.log_progress(label, time.time() - started)

    def log_progress(self, label: str, took: float) -> None:
        """Log the rate so far and, when the stager left a total, an ETA."""
        elapsed_h = (time.time() - self.started) / 3600
        rate = self.written / elapsed_h if elapsed_h else 0.0
        total_file = self.queue_dir / TOTAL
        total = int(total_file.read_text()) if total_file.exists() else None
        eta = ""
        if total and rate:
            remaining = max(total - self.written, 0)
            eta = f", ~{remaining} left, ETA {remaining / rate:.1f} h"
        logger.success(
            f"{label} committed to main in {took:.0f}s. {self.written} written, {rate:.1f} h/h{eta}"
        )

    def finish(self) -> None:
        """Stop the pool and sort the time axis of every store appended to."""
        self.pool.shutdown()
        if self.args.no_sort:
            logger.info(f"{self.product}: done; not sorting {sorted(self.touched)}")
            return
        for prefix in sorted(self.touched):
            repo = self.provider.config.icechunk_repo(prefix)
            logger.info(f"{prefix}: sorting the time axis")
            sort_append_axis(repo, append_dim="time")
        logger.success(f"{self.product}: done, {self.written} hours written")


def run(args: argparse.Namespace) -> None:
    """The ``run`` command."""
    Runner(args).run()


# -------------------------------------------------------------------------------- main


def main(argv: list[str] | None = None) -> None:
    """Parse the command line and run the chosen command."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)

    s = sub.add_parser("stage", help="push missing hours to the processing host")
    s.add_argument("--archive", default="/data/meteoswiss")
    s.add_argument("--remote", required=True, help="ssh host to push to")
    s.add_argument("--remote-dir", default="planetary-fleet/work/kenda-backfill")
    s.add_argument("--products", nargs="+", choices=sorted(PRODUCTS), default=sorted(PRODUCTS))
    s.add_argument("--start", default="2026-06-01")
    s.add_argument("--end", default=str(pd.Timestamp.now(tz="UTC").tz_localize(None)))
    s.add_argument("--lookahead", type=int, default=16, help="hours staged ahead per product")
    s.add_argument("--poll", type=float, default=10.0)
    s.add_argument("--dry-run", action="store_true", help="report what is missing and stop")
    s.set_defaults(func=stage)

    r = sub.add_parser("run", help="write staged hours to the stores")
    r.add_argument("--product", choices=sorted(PRODUCTS), required=True)
    r.add_argument("--staging-dir", default="~/planetary-fleet/work/kenda-backfill")
    r.add_argument("--workers", type=int, default=6)
    r.add_argument("--batch-size", type=int, default=None, help="defaults to --workers")
    r.add_argument("--worker-memory-gb", type=float, default=3.0)
    r.add_argument("--blosc-threads", type=int, default=2)
    r.add_argument("--tasks-per-worker", type=int, default=8)
    r.add_argument("--poll", type=float, default=15.0)
    r.add_argument(
        "--fill-wait", type=float, default=600.0, help="seconds to wait for a full batch"
    )
    r.add_argument("--no-sort", action="store_true", help="leave the time axis unsorted")
    r.set_defaults(func=run)

    args = parser.parse_args(argv)
    if getattr(args, "batch_size", "unset") is None:
        args.batch_size = args.workers
    args.func(args)


if __name__ == "__main__":
    main()
