"""The 06:00 priority run: a handful of ingests that jump the queue once a day.

Four sources are wanted before anything else each morning — SILAM, DMI HARMONIE, the MEPS
model-level fields and the AROME family. They are scheduled here, separately from the
memory-class jobs in :mod:`dags.loader`, so that they can carry a run priority the rest of
the queue does not.

How the priority actually binds
------------------------------
Dagster's ``QueuedRunCoordinator`` orders the queue by the ``dagster/priority`` **run**
tag, highest first. A job's ``tags`` become its runs' tags, which is why the priority is
set on the job here rather than on the assets: ``@dg.asset(tags=...)`` sets an *asset* tag,
which the queue never reads — the same trap that left every op unpooled, see
:func:`dags.loader.pool_name_for`. Several asset modules already carry a
``"dagster/priority": "1"`` asset tag that has never done anything.

Priority decides *ordering*, not capacity: a priority run still waits for a memory-class
slot from ``dags/dagster.yaml`` and for its store's pool. It goes to the front of the
queue, not around it.

Sources are worked through in the order :data:`PRIORITY_ORDER` lists them, but a source's
own partitions are free to run alongside each other: the queue holds at most
:data:`SOURCE_CONCURRENCY` runs of any one source, so a slow source cannot fill the queue
and stall the ones behind it, and cannot hold it either. Writes are still safe because
that is not where write safety comes from — one writer per store is the concurrency pool's
job (:func:`dags.loader.pool_name_for`), at the step level, whatever the queue does.

Why more than one job
---------------------
Every asset in a partitioned job must share one ``partitions_def``, so these eleven assets
cannot be selected together: SILAM is daily, HARMONIE and MEPS are three-hourly on
different offsets, and AROME is three- or six-hourly depending on the region. They are
therefore grouped by partitioning into one job and one schedule each, all firing at 06:00.
A source split across two partitionings — AROME is — still shares one priority, so the
order between sources holds.

What each tick asks for
-----------------------
:data:`LOOKBACK_DAYS` days of partitions, oldest first, ending at the source's newest
*available* partition. That is a gap-filling window rather than a rewrite: a partition
already in the store costs a skip, because ``run_partition`` reads the store first.

One run is requested per partition, so a three-hourly source offers up to 40 of them each
morning against a daily source's 5. Nearly all of those skip on an ordinary day; a morning
after an outage does real work, which is the point of the window.
"""

from __future__ import annotations

import datetime as dt
from typing import NamedTuple

import dagster as dg
from loguru import logger

#: When the priority run fires. The same hour as the ordinary daily schedules
#: (:data:`dags.factory.DAILY_CRON_HOUR`) — being first in the queue is the point, not
#: being early.
PRIORITY_CRON = "0 6 * * *"

#: Base ``dagster/priority``. Any positive number outranks the untagged rest of the queue,
#: which the coordinator treats as zero; the sources below sit above this, and the gap to
#: zero leaves room to slot something between them and the default later.
PRIORITY_BASE = 100

#: Run tag naming which source of the batch a run belongs to, e.g. ``silam_dust``.
#: ``dags/dagster.yaml`` limits it *per unique value*, so a source's own partitions run
#: alongside each other up to :data:`SOURCE_CONCURRENCY` while the queue still works
#: through the sources in :data:`PRIORITY_ORDER`.
PRIORITY_SOURCE_TAG = "planetary/priority_source"

#: How many runs of one source may be in flight at once. A source is still bounded below
#: this by its assets' concurrency pools, which allow one writer per store; this caps how
#: much of the queue a single source can hold while the rest of the batch waits.
SOURCE_CONCURRENCY = 4

#: How far back each morning's batch reaches: the last four days and the newest partition
#: on top, five days in all, ending at each source's own newest *available* partition
#: rather than at wall-clock today. Several of these partitionings carry ``end_offset=-1``,
#: which holds their newest partition a period back; anchoring on the calendar would ask
#: for keys that do not exist and skip the tick entirely.
#:
#: The window is a safety net, not a rewrite: every partition in it is offered each
#: morning, and one already in the store costs a skip, so the effect is to close whatever
#: gaps the source's own cadence left behind over the last few days.
LOOKBACK_DAYS = 5


class PrioritySource(NamedTuple):
    """One source of the morning batch.

    Attributes:
        label: Names the job, ``priority_<label>``.
        keys: Asset keys, as they appear after :mod:`dags.loader` has prefixed them by
            family.
        prefixes: Key prefixes taken wholesale, for a family whose membership grows.
    """

    label: str
    keys: tuple[str, ...] = ()
    prefixes: tuple[str, ...] = ()


#: The sources of the morning batch, **in the order they are to run**.
#:
#: The earlier a source sits here the higher its ``dagster/priority``, which is what the
#: queue orders by: every SILAM run outranks every HARMONIE run, so the queue takes them
#: first. Sources may still overlap at the boundary — Dagster's run queue can cap runs per
#: source but cannot express "only one source in flight at a time" — so this is an
#: ordering, not a barrier. Ordering is what matters here; write safety is the pools'.
#:
#: AROME is matched by prefix rather than listed, so a new region joins the morning batch
#: with its module rather than silently missing it until someone remembers this file.
PRIORITY_ORDER: tuple[PrioritySource, ...] = (
    # The global 0.1-degree dust forecast only; the aerosol store keeps its own cadence.
    PrioritySource("silam_dust", keys=("silam/silam_dust",)),
    PrioritySource("dmi_harmonie", keys=("nwp/dmi_harmonie", "nwp/dmi_harmonie_model_level")),
    # The model-level member of the MEPS family; the others keep their own cadence.
    PrioritySource("meps_det", keys=("meps/meps_det",)),
    PrioritySource("arome", prefixes=("arome/",)),
)


def priority_rank(key: dg.AssetKey) -> int | None:
    """Where ``key`` sits in :data:`PRIORITY_ORDER`, or None if it is not in the batch."""
    name = key.to_user_string()
    for rank, source in enumerate(PRIORITY_ORDER):
        if name in source.keys or (source.prefixes and name.startswith(source.prefixes)):
            return rank
    return None


def is_priority_asset(key: dg.AssetKey) -> bool:
    """Whether ``key`` belongs in the 06:00 priority batch."""
    return priority_rank(key) is not None


def priority_for_rank(rank: int) -> str:
    """The ``dagster/priority`` value for a source at ``rank``; earlier ranks are higher."""
    return str(PRIORITY_BASE + len(PRIORITY_ORDER) - rank)


def recent_partition_keys(
    partitions_def: dg.PartitionsDefinition,
    now: dt.datetime,
    days: int = LOOKBACK_DAYS,
) -> list[str]:
    """The last ``days`` days of partitions that actually exist at ``now``, oldest first.

    Anchored on the last partition the definition offers rather than on today's date.
    Several of these partitionings carry ``end_offset=-1``, which holds their newest
    partition a whole period back: asking for "yesterday" found a key that does not exist
    yet and the tick skipped, so SILAM would never have run at all.

    Returns an empty list for a partitioning with no time windows and for one with no
    partitions yet, which makes the schedule skip rather than request a run with nothing
    to do.
    """
    if not isinstance(partitions_def, dg.TimeWindowPartitionsDefinition):
        return []
    try:
        # `get_last_partition_key` takes no `current_time`; it already honours end_offset.
        last = partitions_def.get_last_partition_key()
        if last is None:
            return []
        end = partitions_def.time_window_for_partition_key(last).end
        window = dg.TimeWindow(start=end - dt.timedelta(days=days), end=end)
        keys = list(partitions_def.get_partition_keys_in_time_window(window))
    except Exception as exc:  # noqa: BLE001 - a partitioning that cannot answer is skipped
        logger.warning(
            f"dagster: no priority partitions for {partitions_def}: {type(exc).__name__}: {exc}"
        )
        return []
    if last not in keys:
        keys.append(last)
    return sorted(keys)


def _group_label(keys: list[dg.AssetKey]) -> str:
    """A readable name for a group of assets that share a partitioning.

    The longest common prefix of their names, so the two AROME groups come out
    ``arome_france`` and ``arome``, and the HARMONIE pair comes out ``dmi_harmonie``,
    rather than the partitioning hash :func:`dags.loader._partitions_key` produces. That
    hash is right for the memory-class jobs, which group unrelated assets; these groups
    are small and curated, and their names are read every morning.
    """
    names = sorted(key.path[-1] for key in keys)
    if len(names) == 1:
        return names[0]
    first, last = names[0], names[-1]
    common = first[: next((i for i, (a, b) in enumerate(zip(first, last)) if a != b), len(first))]
    return common.rstrip("_") or first


def build_priority_jobs(
    assets: list[dg.AssetsDefinition],
) -> tuple[list[dg.JobDefinition], list[dg.ScheduleDefinition]]:
    """One 06:00 job and schedule per partitioning among the priority assets.

    Args:
        assets: Every asset in the code location, already loaded and namespaced.

    Returns:
        The jobs and their schedules, ready to go into :class:`dagster.Definitions`.
    """
    from dags.loader import _partitions_key

    # Grouped by (rank, partitioning): one job may only hold assets that share a
    # partitions_def, and only one source, so that each job carries a single priority.
    groups: dict[tuple[int, str], tuple[dg.PartitionsDefinition, list[dg.AssetKey]]] = {}
    for asset in assets:
        partitions_def = getattr(asset, "partitions_def", None)
        if partitions_def is None:
            continue
        for key in getattr(asset, "keys", ()) or ():
            rank = priority_rank(key)
            if rank is None:
                continue
            group = (rank, _partitions_key(partitions_def))
            groups.setdefault(group, (partitions_def, []))[1].append(key)

    jobs: list[dg.JobDefinition] = []
    schedules: list[dg.ScheduleDefinition] = []
    used: set[str] = set()
    for (rank, partitions_name), (partitions_def, keys) in sorted(groups.items()):
        label = _group_label(keys)
        # Two groups could still reduce to one label — AROME's two partitionings do; fall
        # back to the partitioning key, which is unique by construction, rather than
        # defining two jobs of one name.
        job_name = (
            f"priority_{label}" if label not in used else f"priority_{label}_{partitions_name}"
        )
        used.add(label)
        priority = priority_for_rank(rank)
        names = sorted(key.to_user_string() for key in keys)
        source_label = PRIORITY_ORDER[rank].label
        job = dg.define_asset_job(
            name=job_name,
            selection=dg.AssetSelection.assets(*keys),
            # Run tags: the queue orders by dagster/priority, and dags/dagster.yaml caps
            # how many runs of one source are in flight through PRIORITY_SOURCE_TAG.
            tags={"dagster/priority": priority, PRIORITY_SOURCE_TAG: source_label},
            description=(
                f"06:00 priority run {rank + 1} of {len(PRIORITY_ORDER)} "
                f"(priority {priority}): " + ", ".join(names)
            ),
        )
        jobs.append(job)
        schedules.append(
            _build_priority_schedule(job, job_name, partitions_def, priority, source_label)
        )

    logger.info(
        f"dagster: {len(jobs)} priority job(s) at {PRIORITY_CRON} covering "
        f"{sum(len(keys) for _, keys in groups.values())} asset(s), in the order "
        + " -> ".join(source.label for source in PRIORITY_ORDER)
    )
    return jobs, schedules


def _build_priority_schedule(
    job: dg.JobDefinition,
    job_name: str,
    partitions_def: dg.PartitionsDefinition,
    priority: str,
    source_label: str,
) -> dg.ScheduleDefinition:
    """The 06:00 schedule for one priority job, one run request per partition.

    Requests are emitted oldest partition first, so that a source whose day is several
    partitions long fills in time order as the queue works through them.
    """

    @dg.schedule(
        name=f"{job_name}_schedule",
        cron_schedule=PRIORITY_CRON,
        job=job,
        default_status=dg.DefaultScheduleStatus.RUNNING,
        execution_timezone="UTC",
    )
    def _schedule(context: dg.ScheduleEvaluationContext):
        fired = context.scheduled_execution_time
        keys = recent_partition_keys(partitions_def, fired)
        if not keys:
            return dg.SkipReason(f"{job_name}: no partitions available yet")
        # The tick's date is in the run key so that each morning genuinely re-offers its
        # whole window. A run key fixed to the partition alone is seen once for all time,
        # which would quietly turn the look-back into "only partitions nobody has
        # requested yet" and never retry one whose run failed.
        stamp = fired.strftime("%Y-%m-%d")
        return [
            dg.RunRequest(
                run_key=f"{job_name}-{stamp}-{key}",
                partition_key=key,
                tags={"dagster/priority": priority, PRIORITY_SOURCE_TAG: source_label},
            )
            for key in keys
        ]

    return _schedule


__all__ = [
    "LOOKBACK_DAYS",
    "PRIORITY_BASE",
    "PRIORITY_CRON",
    "PRIORITY_ORDER",
    "PRIORITY_SOURCE_TAG",
    "SOURCE_CONCURRENCY",
    "PrioritySource",
    "build_priority_jobs",
    "is_priority_asset",
    "priority_for_rank",
    "priority_rank",
    "recent_partition_keys",
]
