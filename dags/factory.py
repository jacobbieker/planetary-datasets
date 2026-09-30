"""Turn a :class:`~planetary_datasets.base.BaseProvider` into a Dagster asset.

Every provider-backed asset in this code location should be built with
:func:`make_provider_asset` rather than hand-rolling an ``@dg.asset``. The factory is
what makes the fleet of ingests uniform:

* one daily partition per asset, so backfills and gap-filling are ordinary Dagster runs;
* a declared memory need, which is enforced twice — once before any work starts
  (:func:`~planetary_datasets.memory.require_memory`) and once while it runs
  (:func:`~planetary_datasets.memory.memory_guard`);
* tags the run queue and the executor can size concurrency from, so several ingests on
  one host cannot collectively exceed its RAM;
* a per-provider concurrency pool, so one provider never runs against itself.

A minimal use looks like::

    from dags.factory import make_provider_asset
    from planetary_datasets.providers.dmi_harmonie import DMIHarmonieProvider

    dmi_harmonie = make_provider_asset(
        DMIHarmonieProvider,
        name="dmi-harmonie",
        freq="3h",
        memory_gb=16,
        description="DMI HARMONIE Greenland/Iceland NWP",
    )

``dags/definitions.py`` discovers the module, applies the group name and key prefix, and
wires the asset into a scheduled job sized for its memory class. Nothing else is needed.
"""

# No `from __future__ import annotations` here: Dagster inspects the raw annotation on
# the asset function's `context` parameter, and a stringified one is rejected.

import math
from typing import Any, Iterable, Mapping, Sequence

import dagster as dg
import pandas as pd
from dagster import AssetExecutionContext
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.memory import (
    available_memory_gb,
    memory_budget_gb,
    memory_guard,
    process_tree_rss_gb,
    require_memory,
    total_memory_gb,
)

#: When a daily scheduled job fires. Late enough that the previous UTC day is complete at
#: every upstream provider we pull from. ``dags/loader.py`` builds its schedules with
#: ``build_schedule_from_partitioned_job``, which takes the hour and minute rather than a
#: cron string so that partitionings finer than a day keep their own cadence.
DAILY_CRON_HOUR = 6
DAILY_CRON_MINUTE = 0
DAILY_CRON = f"{DAILY_CRON_MINUTE} {DAILY_CRON_HOUR} * * *"

#: Earliest partition. Providers with shorter archives simply produce empty partitions
#: before their data starts, which ``run_partition`` treats as "nothing to do".
DEFAULT_START_DATE = "2024-01-01"

#: Memory a provider is assumed to need when it does not declare one. Matches the 8 GB
#: container limit the GFS Docker pipe already used.
DEFAULT_MEMORY_GB = 8.0

#: Extra headroom over the declared need before the guard trips, covering the Dagster
#: run process itself rather than only the provider's working set.
MEMORY_HEADROOM_GB = 1.0

MEMORY_CLASS_TAG = "planetary/memory_class"
MEMORY_GB_TAG = "planetary/memory_gb"

#: Set to ``"manual"`` on an asset to keep it out of the scheduled jobs: it is then only
#: materialised by hand or by a backfill. For ingests whose every partition is heavy
#: enough that running the newest one on a timer is not wanted by default.
SCHEDULE_TAG = "planetary/schedule"

#: Group and key prefix the reorder assets are collected under, so the ops tooling for
#: every store sits together in the UI rather than scattered through the data groups.
MAINTENANCE_GROUP = "maintenance"

#: Memory classes, as (name, inclusive upper bound in GB). Concurrency limits in
#: ``dags/dagster.yaml`` and in the executor are derived from these bounds.
MEMORY_CLASSES: tuple[tuple[str, float], ...] = (
    ("small", 4.0),
    ("medium", 16.0),
    ("large", 48.0),
    ("xlarge", math.inf),
)

#: The partitioning every factory-built asset shares. ``end_offset=-1`` drops one whole
#: partition from the end of the set, so the newest partition is D-2, not D-1: the day
#: that ended yesterday is left out until today is over. That is deliberate — several
#: upstreams are still publishing the previous UTC day well into the next one — but it is
#: a day more lag than "keeps the in-progress day out", which is what ``end_offset=0``
#: already does.
daily_partitions = dg.DailyPartitionsDefinition(start_date=DEFAULT_START_DATE, end_offset=-1)


def memory_class_for(memory_gb: float) -> str:
    """Return the memory class a declared footprint falls into."""
    for name, ceiling in MEMORY_CLASSES:
        if memory_gb <= ceiling:
            return name
    return MEMORY_CLASSES[-1][0]


def memory_class_ceiling(name: str) -> float:
    """Return the upper memory bound of a class, in GB."""
    for class_name, ceiling in MEMORY_CLASSES:
        if class_name == name:
            return ceiling
    raise KeyError(f"Unknown memory class {name!r}")


def concurrency_limit_for(memory_class: str, budget_gb: float | None = None) -> int:
    """How many ops of a memory class fit in the budget at once.

    ``xlarge`` has no upper bound, so it is always serialised to one at a time.
    """
    if memory_class == "xlarge":
        return 1
    budget = budget_gb if budget_gb is not None else memory_budget_gb()
    ceiling = memory_class_ceiling(memory_class)
    return max(1, int(budget // ceiling))


def executor_tag_concurrency_limits(budget_gb: float | None = None) -> list[dict[str, Any]]:
    """Per-memory-class op concurrency limits for the multiprocess executor.

    These bound how many steps of each class run at once *within* a run; the matching
    limits in ``dags/dagster.yaml`` bound how many runs of each class are dequeued.
    """
    budget = budget_gb if budget_gb is not None else memory_budget_gb()
    return [
        {"key": MEMORY_CLASS_TAG, "value": name, "limit": concurrency_limit_for(name, budget)}
        for name, _ in MEMORY_CLASSES
    ]


def _asset_name(value: str) -> str:
    """Make a string usable as a Dagster asset key element.

    Asset keys may contain hyphens and dots, so the names these datasets are known by
    ("arome-france-0025") survive intact; anything else becomes an underscore.
    """
    cleaned = "".join(c if c.isalnum() or c in "-_." else "_" for c in value)
    return cleaned.strip("-_.") or "asset"


def _pool_name(value: str) -> str:
    """Make a string usable as a Dagster concurrency pool name.

    Pool names are validated against ``^[A-Za-z0-9_]+$`` — hyphens are rejected, unlike
    in asset keys — so ``arome-france-0025`` pools as ``arome_france_0025``.
    """
    cleaned = "".join(c if c.isalnum() else "_" for c in value)
    return cleaned.strip("_") or "default"


def _format_gb(memory_gb: float) -> str:
    return f"{memory_gb:g}"


def _naive_utc(value) -> pd.Timestamp:
    """Return ``value`` as a timezone-naive UTC timestamp.

    Dagster hands out timezone-aware partition boundaries, but every store in this repo
    indexes time as naive UTC. Appending a ``datetime64[ns, UTC]`` coordinate fails in
    zarr, so the conversion has to happen before the provider ever sees the timestamp.
    """
    ts = pd.Timestamp(value)
    return ts.tz_convert("UTC").tz_localize(None) if ts.tzinfo is not None else ts


def _partition_window(
    context: AssetExecutionContext,
) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Return the [start, end) window of the partition being materialised, in naive UTC.

    Falls back to parsing the partition key for partitionings that carry no time window,
    so a provider asset still works if someone swaps in a custom ``partitions_def``.

    Raises:
        RuntimeError: When the run has no partition at all. Both lookups raise the same
            Dagster error in that case, so without this the fallback simply re-raises it
            and the cause — a schedule or a manual launch that forgot the partition key —
            is buried in a ``DagsterInvariantViolationError`` about ``partition_key``.
    """
    try:
        window = context.partition_time_window
    except Exception as exc:  # noqa: BLE001 - not a time-window partitioning
        try:
            key = context.partition_key
        except Exception as key_exc:  # noqa: BLE001 - no partition on this run at all
            raise RuntimeError(
                "this asset is partitioned but the run has no partition key; launch it "
                "for a partition, or build its schedule with "
                "dagster.build_schedule_from_partitioned_job"
            ) from key_exc
        logger.debug(f"{key}: not a time-window partitioning ({type(exc).__name__})")
        start = _naive_utc(key)
        return start, start + pd.Timedelta(1, "D")
    return _naive_utc(window.start), _naive_utc(window.end)


def make_provider_asset(
    provider_cls: type[BaseProvider],
    *,
    name: str | None = None,
    freq: str = "1D",
    description: str | None = None,
    memory_gb: float = DEFAULT_MEMORY_GB,
    group_name: str | None = None,
    max_runtime_hours: float = 12.0,
    pool: str | None = None,
    partitions_def: dg.PartitionsDefinition | None = None,
    automation_cron: str | None = None,
    provider_kwargs: Mapping[str, Any] | None = None,
    tags: Mapping[str, str] | None = None,
    metadata: Mapping[str, Any] | None = None,
    kinds: Iterable[str] | None = None,
    deps: Sequence[Any] | None = None,
) -> dg.AssetsDefinition:
    """Build a daily-partitioned Dagster asset that runs a provider over one day.

    Args:
        provider_cls: The :class:`BaseProvider` subclass to run.
        name: Dagster asset name. Defaults to the provider's own ``name``.
        freq: Pandas frequency for init times inside the day, e.g. ``"1h"``, ``"6h"``,
            ``"1D"``. One ``run_partition`` call is made per init time.
        description: Shown in the Dagster UI. Defaults to the provider's docstring.
        memory_gb: Peak memory this ingest is expected to need. Used to pick the memory
            class, to refuse to start when the host cannot spare it, and as the ceiling
            the running guard enforces.
        group_name: Dagster group. Defaults to the group ``dags/definitions.py`` derives
            from the module's location.
        max_runtime_hours: Run-monitoring timeout for the asset.
        pool: Concurrency pool name. Defaults to the asset name, which serialises a
            provider against itself while leaving different providers free to overlap.
        partitions_def: Override the daily partitioning.
        automation_cron: Attach a declarative ``AutomationCondition.on_cron`` as well as
            the code location's schedule. Off by default: ``dags/definitions.py`` already
            builds a running schedule per memory class, and enabling both would launch
            each partition twice.
        provider_kwargs: Attributes set on the provider instance after construction, for
            providers that vary by region or product.
        tags: Extra asset tags, merged over the ones the factory sets.
        metadata: Extra asset metadata.
        kinds: Dagster "kind" badges, e.g. ``{"icechunk", "s3"}``.
        deps: Upstream asset dependencies.

    Returns:
        An :class:`dagster.AssetsDefinition` ready to be picked up by auto-discovery.
    """
    if not isinstance(provider_cls, type) or not issubclass(provider_cls, BaseProvider):
        raise TypeError(f"{provider_cls!r} is not a BaseProvider subclass")
    if memory_gb <= 0:
        raise ValueError(f"memory_gb must be positive, got {memory_gb}")

    asset_name = _asset_name(name or getattr(provider_cls, "name", provider_cls.__name__))
    memory_class = memory_class_for(memory_gb)

    asset_tags: dict[str, str] = {
        "dagster/max_runtime": str(int(max_runtime_hours * 3600)),
        "dagster/concurrency_key": asset_name,
        MEMORY_CLASS_TAG: memory_class,
        MEMORY_GB_TAG: _format_gb(memory_gb),
    }
    asset_tags.update(tags or {})

    # The executor matches on *op* tags, the run queue on *run* tags, and the UI shows
    # asset tags. The memory class has to be present as both to bound either.
    op_tags = {
        MEMORY_CLASS_TAG: memory_class,
        MEMORY_GB_TAG: _format_gb(memory_gb),
        "dagster/max_runtime": str(int(max_runtime_hours * 3600)),
    }

    @dg.asset(
        name=asset_name,
        description=description or (provider_cls.__doc__ or "").strip().split("\n")[0] or None,
        group_name=group_name,
        partitions_def=partitions_def or daily_partitions,
        tags=asset_tags,
        op_tags=op_tags,
        pool=_pool_name(pool or asset_name),
        metadata=dict(metadata or {}),
        kinds=set(kinds) if kinds else None,
        deps=list(deps) if deps else None,
        automation_condition=(
            dg.AutomationCondition.on_cron(automation_cron) if automation_cron else None
        ),
    )
    def _provider_asset(context: AssetExecutionContext) -> dg.MaterializeResult:
        day_start, day_end = _partition_window(context)

        # Refuse up front rather than discovering it through the OOM killer half way
        # through a backfill.
        require_memory(memory_gb, what=f"{asset_name} {day_start:%Y-%m-%d}")

        provider = provider_cls()
        for key, value in (provider_kwargs or {}).items():
            setattr(provider, key, value)

        init_times = pd.date_range(start=day_start, end=day_end, freq=freq, inclusive="left")
        context.log.info(
            f"{asset_name}: {len(init_times)} init time(s) for {day_start:%Y-%m-%d}, "
            f"declared {memory_gb:g} GB ({memory_class}), "
            f"{available_memory_gb():.1f} GB available of {total_memory_gb():.1f} GB"
        )

        written = 0
        skipped = 0
        failures: list[str] = []

        # The guard's ceiling is an absolute RSS figure, so it has to be anchored to what
        # is already resident in this run process; the declared need is growth on top of
        # that, plus a little headroom for the Dagster machinery itself.
        ceiling_gb = process_tree_rss_gb() + memory_gb + MEMORY_HEADROOM_GB

        with memory_guard(ceiling_gb=ceiling_gb, what=asset_name) as usage:
            for it in init_times:
                try:
                    if provider.run_partition(it):
                        written += 1
                    else:
                        skipped += 1
                except Exception as exc:  # noqa: BLE001 - one bad init time must not
                    # abandon the rest of the day; the count is reported as metadata and
                    # the asset fails below if nothing at all got through.
                    logger.exception(f"{asset_name}: {it} failed")
                    context.log.error(f"{asset_name}: {it} failed: {exc}")
                    failures.append(f"{it}: {exc}")

        if failures:
            # Any raised init time fails the partition. A provider that simply has no
            # data for a timestep returns no input files, which counts as skipped rather
            # than failed, so everything that lands here is a real error. Marking the
            # day green because most of it worked would retire the partition with holes
            # in it; failing lets Dagster retry, and the steps that did land are skipped
            # on the way through.
            raise RuntimeError(
                f"{asset_name}: {len(failures)} of {len(init_times)} init time(s) failed "
                f"for {day_start:%Y-%m-%d} ({written} written, {skipped} already stored). "
                f"First failure: {failures[0]}"
            )

        # Cheap (it reads one coordinate) and the only place the state is visible: an
        # out-of-order write is otherwise silent, and the store stays unsorted until
        # someone notices. Surfaced as metadata so the day that caused it is the day that
        # says so.
        try:
            axis_sorted = provider.axis_sorted()
        except Exception as exc:  # noqa: BLE001 - diagnostics must not fail the partition
            context.log.warning(f"{asset_name}: could not check axis order: {exc}")
            axis_sorted = None
        if axis_sorted is False:
            context.log.warning(
                f"{asset_name}: {provider.append_dim} is out of order in "
                f"{provider.store_path}; slice selections are unreliable until the "
                f"{asset_name}-reorder asset is materialised"
            )

        return dg.MaterializeResult(
            metadata={
                "init_times": len(init_times),
                "written": written,
                "skipped": skipped,
                "failed": len(failures),
                "axis_sorted": axis_sorted if axis_sorted is not None else "unknown",
                "peak_memory_gb": round(usage.peak_gb, 2),
                "memory_growth_gb": round(usage.growth_gb, 2),
                "declared_memory_gb": memory_gb,
                "memory_class": memory_class,
                "store": provider.store_path,
                "partition": day_start.strftime("%Y-%m-%d"),
            }
        )

    return _provider_asset


def make_provider_assets(
    provider_cls: type[BaseProvider],
    variants: Mapping[str, Mapping[str, Any]],
    **common: Any,
) -> list[dg.AssetsDefinition]:
    """Build one asset per variant of a provider that is parameterised by region/product.

    ``variants`` maps an asset name to the keyword arguments for
    :func:`make_provider_asset`; ``common`` supplies defaults shared by all of them::

        mrms_assets = make_provider_assets(
            MRMSProvider,
            {f"mrms-{a}": {"provider_kwargs": {"area": a}} for a in AREAS},
            freq="1h",
            memory_gb=8,
        )
    """
    assets = []
    for asset_name, overrides in variants.items():
        kwargs: dict[str, Any] = {**common, **overrides}
        kwargs["name"] = asset_name
        assets.append(make_provider_asset(provider_cls, **kwargs))
    return assets


__all__ = [
    "DAILY_CRON",
    "DAILY_CRON_HOUR",
    "DAILY_CRON_MINUTE",
    "DEFAULT_MEMORY_GB",
    "DEFAULT_START_DATE",
    "MAINTENANCE_GROUP",
    "MEMORY_CLASSES",
    "MEMORY_CLASS_TAG",
    "MEMORY_GB_TAG",
    "concurrency_limit_for",
    "daily_partitions",
    "executor_tag_concurrency_limits",
    "make_provider_asset",
    "make_provider_assets",
    "memory_class_ceiling",
    "memory_class_for",
]
