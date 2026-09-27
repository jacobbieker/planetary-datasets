"""Memory estimation and guards so a job cannot take the host down.

These pipelines routinely open lazy datasets far larger than RAM and only discover the
problem when the kernel kills the process. The helpers here make the failure early, cheap
and legible instead:

``require_memory``
    Refuse to start work whose estimated footprint exceeds the memory actually available.
``memory_guard``
    Watch resident memory while work runs and abort past a ceiling.
``estimate_dataset_gb``
    Size a lazy :class:`xarray.Dataset` without loading it.

The design follows ``docker/goes-virtual/memory_watchdog.py``, which already solved this for
the EC2 ingests. Two lessons from it are carried over deliberately:

* Shed load by stopping the **parent** process, never a pool worker. Killing a
  ``ProcessPoolExecutor`` worker raises ``BrokenProcessPool`` in its parent and fails every
  remaining task, which silently lost a whole satellite in an earlier build.
* Budget against *available* memory rather than total, because these hosts run several
  ingests at once.
"""

from __future__ import annotations

import contextlib
import os
import resource
import threading
import time
from dataclasses import dataclass

import psutil
from loguru import logger

BYTES_PER_GB = 1024**3


class MemoryLimitExceeded(RuntimeError):
    """Raised when a job needs, or has grown to use, more memory than it is allowed."""


def total_memory_gb() -> float:
    """Total physical memory on the host, in GB."""
    return psutil.virtual_memory().total / BYTES_PER_GB


def available_memory_gb() -> float:
    """Memory currently available for allocation without swapping, in GB."""
    return psutil.virtual_memory().available / BYTES_PER_GB


def process_tree_rss_gb(pid: int | None = None) -> float:
    """Resident memory of a process and all of its descendants, in GB.

    Pool workers carry a different command line from their parent, so this walks the
    process tree rather than matching on name.
    """
    try:
        proc = psutil.Process(pid) if pid is not None else psutil.Process()
    except psutil.NoSuchProcess:
        return 0.0

    total = 0
    for p in [proc, *proc.children(recursive=True)]:
        # A child can exit between listing and reading it; that is normal, not an error.
        with contextlib.suppress(psutil.NoSuchProcess, psutil.AccessDenied):
            total += p.memory_info().rss
    return total / BYTES_PER_GB


def estimate_dataset_gb(ds) -> float:
    """Total in-memory size of an xarray Dataset in GB, without loading it.

    Uses ``nbytes``, computed from dtype and shape, so it is valid for lazy dask-backed
    variables. This is what the dataset costs if fully materialised; for a lazy dataset
    written chunk by chunk see :func:`estimate_peak_gb`.
    """
    try:
        return float(ds.nbytes) / BYTES_PER_GB
    except (AttributeError, TypeError):
        total = 0
        for var in getattr(ds, "data_vars", {}).values():
            total += getattr(var, "nbytes", 0)
        return float(total) / BYTES_PER_GB


def largest_chunk_gb(ds) -> float:
    """Size of the largest single chunk across the dataset's variables, in GB.

    An eagerly loaded variable has no chunks and is resident in full, so it counts as one
    chunk of its whole size. Skipping those would let a dataset mixing one small chunked
    variable with a large in-memory one be sized at nearly zero.

    Returns 0.0 for an empty dataset.
    """
    largest = 0
    for var in getattr(ds, "data_vars", {}).values():
        itemsize = getattr(getattr(var, "dtype", None), "itemsize", 0)
        chunksizes = getattr(var, "chunksizes", None)
        if not chunksizes:
            # Not chunked: the whole variable is the unit of residency.
            largest = max(largest, getattr(var, "nbytes", 0))
            continue
        # chunksizes maps dim -> tuple of chunk lengths; the largest chunk takes the
        # biggest length along each dim.
        elements = 1
        for lengths in chunksizes.values():
            if lengths:
                elements *= max(lengths)
        largest = max(largest, elements * itemsize)
    return float(largest) / BYTES_PER_GB


def estimate_peak_gb(ds, concurrency: int = 4) -> float:
    """Estimate peak memory for writing a dataset, in GB.

    An eagerly loaded dataset is already resident, so its total size is the answer. A
    dask-backed dataset is written chunk by chunk, so the peak is roughly the largest
    chunk times the number in flight — not the total, which for a long lazy concatenation
    can be hundreds of GB for a job that never exceeds a few.

    Args:
        ds: Dataset to size.
        concurrency: Assumed number of chunks resident at once.
    """
    chunk = largest_chunk_gb(ds)
    if chunk == 0.0:
        return estimate_dataset_gb(ds)
    return min(estimate_dataset_gb(ds), chunk * max(1, concurrency))


def memory_budget_gb(fraction: float | None = None) -> float:
    """The amount of memory a single job is allowed to use, in GB.

    Defaults to ``MEMORY_FRACTION`` (0.8) of currently available memory, or the explicit
    ``MEMORY_CEILING_GB`` when set.
    """
    from planetary_datasets.config import get_config

    cfg = get_config()
    if cfg.memory_ceiling_gb is not None:
        return cfg.memory_ceiling_gb
    return available_memory_gb() * (fraction if fraction is not None else cfg.memory_fraction)


def require_memory(needed_gb: float, what: str = "job") -> None:
    """Raise before starting work that will not fit in the available memory budget.

    Args:
        needed_gb: Estimated peak footprint in GB.
        what: Name used in the error message.
    """
    budget = memory_budget_gb()
    if needed_gb > budget:
        raise MemoryLimitExceeded(
            f"{what} needs ~{needed_gb:.1f} GB but only {budget:.1f} GB is budgeted "
            f"({available_memory_gb():.1f} GB available of {total_memory_gb():.1f} GB total). "
            "Reduce the chunk or partition size, or raise MEMORY_CEILING_GB."
        )
    logger.debug(f"{what}: ~{needed_gb:.1f} GB estimated, {budget:.1f} GB budgeted")


def require_dataset_fits(ds, what: str = "dataset", concurrency: int = 4) -> float:
    """Check a dataset's estimated peak write footprint against the budget.

    Uses :func:`estimate_peak_gb`, so a lazy dataset is judged on the chunks in flight
    rather than its total size. Sizing a 120-step lazy concatenation by its total would
    reject a job that peaks at a few GB, which is why providers were disabling the guard.
    """
    needed = estimate_peak_gb(ds, concurrency=concurrency)
    require_memory(needed, what)
    return needed


@dataclass
class MemoryUsage:
    """Resident memory observed by :func:`memory_guard`."""

    peak_gb: float = 0.0
    final_gb: float = 0.0
    baseline_gb: float = 0.0

    @property
    def growth_gb(self) -> float:
        """How much the guarded block added on top of what was already resident."""
        return max(0.0, self.peak_gb - self.baseline_gb)


@contextlib.contextmanager
def memory_guard(
    ceiling_gb: float | None = None,
    what: str = "job",
    interval: float = 5.0,
    strikes: int = 3,
):
    """Watch process-tree memory while the block runs and abort if it exceeds a ceiling.

    Yields a :class:`MemoryUsage` that is populated as the block runs, so callers can log
    or attach the peak as Dagster metadata.

    Requires ``strikes`` consecutive over-budget samples before acting, so a brief spike
    during a concatenate does not kill an otherwise healthy run. On breach the exception is
    raised in the *calling* thread when the block exits, leaving any pool workers intact.
    Put only the memory-hungry work inside the block and do irreversible work such as a
    commit after it, so a breach prevents the write rather than following it.

    When no explicit ceiling is given, the limit is the memory already resident at entry
    plus the available-memory budget. Comparing absolute RSS against an available-derived
    budget would abort immediately for any process that is already large, which is exactly
    the case this is meant to protect.

    Args:
        ceiling_gb: Absolute RSS limit in GB. Defaults to baseline RSS plus the budget
            from :func:`memory_budget_gb`.
        what: Name used in log lines and the error message.
        interval: Seconds between samples.
        strikes: Consecutive over-budget samples required before flagging a breach.
    """
    baseline = process_tree_rss_gb()
    if ceiling_gb is not None:
        ceiling = ceiling_gb
    else:
        ceiling = baseline + memory_budget_gb()
        logger.debug(
            f"{what}: ceiling {ceiling:.1f} GB (baseline {baseline:.1f} GB + budget "
            f"{ceiling - baseline:.1f} GB)"
        )
    usage = MemoryUsage(baseline_gb=baseline)
    stop = threading.Event()
    breached: list[float] = []

    def watch() -> None:
        consecutive = 0
        while not stop.is_set():
            rss = process_tree_rss_gb()
            usage.peak_gb = max(usage.peak_gb, rss)
            if rss > ceiling:
                consecutive += 1
                logger.warning(
                    f"{what}: {rss:.1f} GB exceeds {ceiling:.1f} GB ceiling "
                    f"({consecutive}/{strikes})"
                )
                if consecutive >= strikes and not breached:
                    breached.append(rss)
            else:
                consecutive = 0
            stop.wait(interval)

    watcher = threading.Thread(target=watch, name=f"memory-guard-{what}", daemon=True)
    watcher.start()
    try:
        yield usage
    finally:
        stop.set()
        watcher.join(timeout=interval + 1)
        usage.final_gb = process_tree_rss_gb()
        usage.peak_gb = max(usage.peak_gb, usage.final_gb)
        logger.info(f"{what}: peak memory {usage.peak_gb:.1f} GB (ceiling {ceiling:.1f} GB)")

    if breached:
        raise MemoryLimitExceeded(
            f"{what} exceeded its {ceiling:.1f} GB memory ceiling "
            f"(peak {usage.peak_gb:.1f} GB) for {strikes} consecutive samples."
        )


def limit_address_space(gb: float) -> None:
    """Cap this process's address space with ``RLIMIT_AS``.

    Intended for child processes: once set, an over-budget allocation raises
    ``MemoryError`` in the child instead of inviting the OOM killer to pick a victim.
    Lowering the limit is irreversible within the process.
    """
    limit = int(gb * BYTES_PER_GB)
    soft, hard = resource.getrlimit(resource.RLIMIT_AS)
    if hard != resource.RLIM_INFINITY and limit > hard:
        limit = hard
    resource.setrlimit(resource.RLIMIT_AS, (limit, hard))
    logger.debug(f"RLIMIT_AS set to {gb:.1f} GB")


def configure_malloc_arenas(max_arenas: int = 2) -> None:
    """Bound glibc arena growth for child processes.

    Only affects processes started afterwards. Carried over from the goes-virtual image,
    where unbounded arenas inflated RSS well past the real working set.
    """
    os.environ.setdefault("MALLOC_ARENA_MAX", str(max_arenas))


def log_memory(what: str = "") -> float:
    """Log and return current process-tree RSS in GB."""
    rss = process_tree_rss_gb()
    logger.info(f"{what + ': ' if what else ''}{rss:.1f} GB RSS, {available_memory_gb():.1f} GB available")
    return rss


def wait_for_memory(needed_gb: float, timeout: float = 0.0, interval: float = 10.0) -> bool:
    """Block until ``needed_gb`` is available, up to ``timeout`` seconds.

    Returns True if the memory became available, False on timeout. With ``timeout=0`` this
    is a single non-blocking check.
    """
    deadline = time.monotonic() + timeout
    while True:
        if available_memory_gb() >= needed_gb:
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(interval)
