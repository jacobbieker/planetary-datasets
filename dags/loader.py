"""Discovery and assembly of the planetary-datasets Dagster code location.

``dags/definitions.py`` is the entry point Dagster loads; everything it needs is built
here so that the pieces can be imported and tested without the side effect of importing
every asset module.

Every module under ``dags/assets/`` is discovered and loaded automatically. Nothing has
to be registered here by hand, which matters because asset modules are added
continuously and by many people at once. Two consequences follow from that:

* **A broken module is skipped, not fatal.** One module that fails to import used to
  take the whole code location down with it, hiding every other asset. Import errors are
  logged and collected into the ``asset_module_import_failures`` metadata on the
  definitions instead.
* **Group, key prefix and op name come from the file's location.** A module's *family*
  is the directory it sits in under ``dags/assets``, or its own filename when it sits
  directly there: ``dags/assets/nwp/gfs.py`` is family ``nwp``, ``dags/assets/arome.py``
  is family ``arome``. Assets are grouped under the family and their keys are prefixed
  with it, so ``gfs_download`` becomes ``nwp/gfs_download``. An asset that declared its
  own ``key_prefix``, or a module that declared its own ``group_name``, keeps it. The op
  behind each asset is namespaced the same way, ``nwp__gfs_download``, so that two
  modules may define an asset of the same name; see :func:`_qualify_op_name`.

Concurrency is sized from the host's memory. A factory-built asset declares what it needs
(see :mod:`dags.factory`); an asset that declares nothing is assumed to need
``DEFAULT_MEMORY_GB``. Assets are grouped into one scheduled job per (partitioning,
memory class), which puts the class on every run's tags, and the run queue in
``dags/dagster.yaml`` limits how many runs of each class are dequeued.

The executor's per-class limit, which bounds steps *within* a run, matches on **op** tags
rather than asset tags, and Dagster offers no way to attach those after the fact. It
therefore only binds for assets built by :func:`~dags.factory.make_provider_asset`, which
sets them. A hand-rolled ``@dg.asset`` needs ``op_tags={MEMORY_CLASS_TAG: ...}`` of its
own to take part in that layer; it is covered by the run-queue layer either way.

Separately from memory, **every asset is put in a concurrency pool** so that only one
writer touches an icechunk store at a time; see :func:`pool_name_for`. Two writers on one
store race, and the loser rebases or loses its timestep. This has to happen here because
a hand-rolled ``@dg.asset(tags={"dagster/concurrency_key": ...})`` sets an *asset* tag,
which Dagster never reads for concurrency — before this, every op in the code location
had ``pool = None`` and nothing was serialised at all.
"""

from __future__ import annotations

import contextlib
import hashlib
import importlib
import os
import pkgutil
import signal
import sys
import threading
import time
from collections import defaultdict
from pathlib import Path
from types import ModuleType
from typing import Iterator

import dagster as dg
import pandas as pd
from loguru import logger

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    # Lets `dagster dev -f dags/definitions.py` work as well as `-m dags.definitions`.
    sys.path.insert(0, str(_REPO_ROOT))

from dagster._core.storage.tags import GLOBAL_CONCURRENCY_TAG  # noqa: E402

from dags import assets as assets_package  # noqa: E402
from dags.factory import (  # noqa: E402
    DAILY_CRON_HOUR,
    DAILY_CRON_MINUTE,
    DEFAULT_MEMORY_GB,
    MEMORY_CLASS_TAG,
    SCHEDULE_TAG,
    executor_tag_concurrency_limits,
    memory_class_for,
    pool_name,
)
from dags.resources import MemoryResource, PlanetaryConfigResource  # noqa: E402
from planetary_datasets.memory import memory_budget_gb  # noqa: E402

#: Subpackages under ``dags/assets`` that are not Dagster asset modules. ``virt`` holds
#: standalone PEP-723 scripts that are run with ``uv run``, not imported. Matched at
#: package boundaries by :func:`_is_skipped`, not with a bare ``startswith``: the latter
#: also swallowed ``dags.assets.virtual_goes`` and ``dags.assets.virtual_gk2a``, which are
#: ordinary modules that merely share a prefix, and did so with no trace in ``failures``.
SKIP_MODULE_PREFIXES: tuple[str, ...] = ("dags.assets.virt",)

#: Seconds a single module gets to import before it is abandoned. Several of these
#: modules began life as scripts and still open network connections at import time; one
#: of them blocking forever would leave the code location permanently "loading".
#: ``dags/assets/observation/gnss.py`` submits a Copernicus CDS request from module
#: scope, which is what motivated this.
IMPORT_TIMEOUT_SECONDS = float(os.environ.get("DAGSTER_ASSET_IMPORT_TIMEOUT", "10"))

#: The first module imported also pays for the shared dependency tree — xarray, satpy,
#: iris, dagster's own plugins — which is far slower than anything after it. Giving it
#: its own grace period is what makes the short per-module deadline above safe.
FIRST_IMPORT_TIMEOUT_SECONDS = float(os.environ.get("DAGSTER_ASSET_FIRST_IMPORT_TIMEOUT", "60"))

#: Total seconds discovery may spend importing modules. Per-module deadlines alone do
#: not bound a cold start: a handful of slow modules can still add minutes and push the
#: code server past Dagster's gRPC load timeout. Once the budget is gone the remaining
#: modules are recorded as skipped rather than imported.
DISCOVERY_BUDGET_SECONDS = float(os.environ.get("DAGSTER_ASSET_DISCOVERY_BUDGET", "120"))


class AssetModuleImportTimeout(BaseException):
    """Raised when a module takes longer than :data:`IMPORT_TIMEOUT_SECONDS` to import.

    Deliberately derived from ``BaseException``, not ``Exception``: the modules this has
    to interrupt are the ones doing network I/O at import, and their retry loops catch
    ``Exception`` and carry on. An ordinary ``TimeoutError`` gets swallowed by
    ``cdsapi``'s retry handling and the import never ends.
    """


def _is_skipped(module_name: str) -> bool:
    """Whether ``module_name`` is inside one of :data:`SKIP_MODULE_PREFIXES`."""
    return any(
        module_name == prefix or module_name.startswith(f"{prefix}.")
        for prefix in SKIP_MODULE_PREFIXES
    )


@contextlib.contextmanager
def _import_deadline(module_name: str, seconds: float) -> Iterator[None]:
    """Interrupt the import of ``module_name`` if it outlasts ``seconds``.

    Implemented with ``SIGALRM`` because the only way to break out of a blocking socket
    read inside an import is to raise in the thread that is running it. Falls back to no
    timeout where that is not possible (non-main thread, or a platform without SIGALRM),
    which is no worse than the behaviour this replaces.
    """
    if (
        seconds <= 0
        or not hasattr(signal, "SIGALRM")
        or threading.current_thread() is not threading.main_thread()
    ):
        yield
        return

    finished = False

    def _on_alarm(signum, frame):  # noqa: ANN001, ARG001
        # A repeating timer can fire in the window between the import returning and the
        # timer being disarmed; that must not turn a successful import into a failure.
        if finished:
            return
        raise AssetModuleImportTimeout(f"{module_name} did not import within {seconds:g}s")

    previous = signal.signal(signal.SIGALRM, _on_alarm)
    # Repeat every second after the deadline: if the module is inside a loop that keeps
    # retrying, one signal is not enough to get it to let go.
    signal.setitimer(signal.ITIMER_REAL, seconds, 1.0)
    try:
        yield
    finally:
        finished = True
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def discover_asset_modules(
    package: ModuleType = assets_package,
) -> tuple[list[ModuleType], dict[str, str]]:
    """Import every module under ``package``, tolerating modules that fail.

    Returns:
        The successfully imported modules, and a mapping of module name to the reason it
        could not be imported.
    """
    modules: list[ModuleType] = []
    failures: dict[str, str] = {}
    deadline = time.monotonic() + DISCOVERY_BUDGET_SECONDS
    first_import = True

    def on_walk_error(name: str) -> None:
        # walk_packages re-raises a sub-package's __init__ failure from inside the
        # generator, which would escape the per-module try below and take the whole code
        # location with it. Record it and keep walking.
        failures[name] = "ImportError: package __init__ failed to import"
        logger.warning(f"dagster: could not walk into {name}")

    walker = pkgutil.walk_packages(
        package.__path__, prefix=f"{package.__name__}.", onerror=on_walk_error
    )
    for info in walker:
        if info.ispkg:
            continue
        if _is_skipped(info.name):
            logger.debug(f"dagster: skipping non-asset module {info.name}")
            continue
        if info.name.rsplit(".", 1)[-1].startswith("_"):
            continue
        if info.name in sys.modules:
            # Already imported (usually by a sibling); no need to pay the deadline again.
            modules.append(sys.modules[info.name])
            continue

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            failures[info.name] = (
                f"TimeoutError: discovery budget of {DISCOVERY_BUDGET_SECONDS:g}s exhausted "
                "before this module was reached"
            )
            continue

        per_module = FIRST_IMPORT_TIMEOUT_SECONDS if first_import else IMPORT_TIMEOUT_SECONDS
        first_import = False
        try:
            with _import_deadline(info.name, min(per_module, remaining)):
                modules.append(importlib.import_module(info.name))
        except KeyboardInterrupt:
            raise
        except BaseException as exc:  # noqa: BLE001 - these modules began life as
            # scripts; some call sys.exit() or raise at import. None of that may be
            # allowed to break the code location for everyone else.
            failures[info.name] = f"{type(exc).__name__}: {exc}"
            logger.warning(f"dagster: skipping {info.name}: {type(exc).__name__}: {exc}")

    return modules, failures


def _family(module_name: str) -> str:
    """The group and key prefix for a module: its first path component under ``assets``.

    Asset modules arrive in two shapes and both are supported:
    ``dags/assets/nwp/gfs.py`` belongs to the ``nwp`` family, and a module placed
    directly at ``dags/assets/arome.py`` is its own family, ``arome``.
    """
    parts = module_name.split(".")
    family = parts[2] if len(parts) > 3 else parts[-1]
    return "".join(c if c.isalnum() or c == "_" else "_" for c in family)


def _node_def(asset: dg.AssetsDefinition):
    """The op backing ``asset``, or None when it has none.

    ``getattr(asset, "node_def", None)`` is not enough. A spec-only
    ``AssetsDefinition`` — an external asset, or one built from ``AssetSpec``s — has no
    op, and Dagster signals that by raising its own ``CheckError`` from the property
    rather than by not defining it, which the ``getattr`` default never catches. One such
    asset anywhere under ``dags/assets`` used to take the whole code location down, since
    this is consulted outside the per-module ``try`` in :func:`load_assets`.
    """
    try:
        return asset.node_def
    except Exception:  # noqa: BLE001 - CheckError, AttributeError, anything: no op either way
        return None


def _qualify_op_name(asset: dg.AssetsDefinition, family: str) -> dg.AssetsDefinition:
    """Return ``asset`` with its op renamed ``<family>__<op>``.

    Asset *keys* are namespaced by family, but op names used not to be, and Dagster
    builds one implicit job over every asset in the code location, which puts all of
    those ops into a single graph. Two modules that ingest the same source then collide:
    ``earth2studio_obs.py`` and ``polar_sounders.py`` each build a ``jpss_atms`` op, and
    so do ``earth2studio_obs.py`` and ``mrms.py`` for ``mrms_conus``. Dagster aliases
    repeated *invocations* of one op, but the graph's node definitions are keyed by name,
    so with two distinct ops of the same name one definition won and the other's inputs
    were wired onto it. The whole code location failed to load - hiding every asset,
    not just the four - with::

        Invalid dependencies: op "jpss_atms" does not have input
        "jpss_atms_download". Available inputs: []

    Qualifying by family makes the name as unique as the asset key it belongs to.

    Only the op's name changes: asset keys, input names, tags and pools are untouched,
    so the concurrency pools in ``dags/dagster.yaml`` and the executor's op-tag limits
    keep matching. Step keys in the event log do change, which costs the *display* of
    past materialisations nothing - they are recorded against asset keys.
    """
    node_def = _node_def(asset)
    # A spec-only asset has no op at all, and a graph-backed asset's GraphDefinition has
    # no rename of its own; leave both as they are.
    if not isinstance(node_def, dg.OpDefinition):
        return asset
    prefix = f"{family}__"
    if node_def.name.startswith(prefix):
        return asset
    attributes = asset.get_attributes_dict()
    attributes["node_def"] = node_def.with_replaced_properties(name=f"{prefix}{node_def.name}")
    return dg.AssetsDefinition.dagster_internal_init(**attributes)


def pool_name_for(asset: dg.AssetsDefinition, family: str) -> str | None:
    """The concurrency pool an asset's op belongs to, or None to leave it alone.

    Every asset that writes to an icechunk store must share a pool with everything else
    writing to the *same* store: two writers on one store race, and the loser spends its
    time rebasing or loses its timestep outright. With ``default_limit: 1`` in
    ``dags/dagster.yaml`` a pool means exactly one in-flight step, across every run, so
    one pool per source is one writer per source.

    Nothing is assigned when the op already says how it wants to be limited — it has a
    ``pool`` of its own (:func:`dags.factory.make_provider_asset` sets one), or it carries
    the legacy ``dagster/concurrency_key`` **op** tag, which Dagster still honours:
    ``active.py`` resolves a step's key as ``step.pool or step.tags.get(...)``, and the
    ``default_limit`` in ``dags/dagster.yaml`` applies to either spelling. Overriding one
    would be rejected anyway; a pool must equal the op tag when both are set, and the
    declared keys contain hyphens, which pool names may not.

    Otherwise the pool is ``<family>_<declared key>`` when the asset declares
    ``dagster/concurrency_key`` in its **asset** tags, and ``<family>_<asset name>`` when
    it declares nothing. Both are qualified by the module the asset comes from, because a
    store is written from one module: that groups everything writing to one store — all
    three MRMS assets, both GEOS-CF assets — without merging modules that merely picked
    the same generic word (``silam.py`` and ``imerg.py`` both say ``download``, and they
    share no store).

    An asset tag is the spelling that does *nothing* today: Dagster reads concurrency from
    the op, and ``@dg.asset(tags=...)`` sets neither a pool nor an op tag. That is the bug
    this repairs — every GEOS-CF op had ``pool = None``, so four writers ran at once on one
    store and spent their time rebasing.
    """
    node_def = _node_def(asset)
    if not isinstance(node_def, dg.OpDefinition):
        return None
    if node_def.pool or node_def.tags.get(GLOBAL_CONCURRENCY_TAG):
        return None

    declared = ""
    for spec in getattr(asset, "specs", ()) or ():
        declared = (spec.tags or {}).get(GLOBAL_CONCURRENCY_TAG, "")
        if declared:
            break

    if declared:
        return pool_name(f"{family}_{declared}")
    keys = sorted(getattr(asset, "keys", ()) or (), key=lambda k: k.to_user_string())
    if not keys:
        return None
    return pool_name(f"{family}_{keys[0].path[-1]}")


def _with_pool(asset: dg.AssetsDefinition, family: str) -> dg.AssetsDefinition:
    """Return ``asset`` with its op bound to the pool :func:`pool_name_for` picks.

    Rebuilt through ``dagster_internal_init`` because ``OpDefinition.with_replaced_properties``
    carries the existing pool over rather than taking a new one.
    """
    pool = pool_name_for(asset, family)
    if pool is None:
        return asset

    node_def = _node_def(asset)
    attributes = asset.get_attributes_dict()
    attributes["node_def"] = dg.OpDefinition.dagster_internal_init(
        compute_fn=node_def.compute_fn,
        name=node_def.name,
        ins={input_def.name: dg.In.from_definition(input_def) for input_def in node_def.input_defs},
        outs={
            output_def.name: dg.Out.from_definition(output_def)
            for output_def in node_def.output_defs
        },
        description=node_def.description,
        config_schema=node_def.config_schema,
        required_resource_keys=node_def.required_resource_keys,
        tags=node_def.tags,
        version=None,  # code_version replaces version
        retry_policy=node_def.retry_policy,
        code_version=node_def.version,
        pool=pool,
    )
    return dg.AssetsDefinition.dagster_internal_init(**attributes)


def _load_grouped(
    module: ModuleType, family: str, key_prefix: str | None
) -> list[dg.AssetsDefinition]:
    """Load a module's assets into the ``family`` group, tolerating its own group name."""
    try:
        return dg.load_assets_from_modules([module], group_name=family, key_prefix=key_prefix)
    except Exception:  # noqa: BLE001 - the module set its own group_name; keep it
        return dg.load_assets_from_modules([module], key_prefix=key_prefix)


def _load_one_module(module: ModuleType, family: str) -> list[dg.AssetsDefinition]:
    """Load one module's assets, namespacing the ones that did not namespace themselves.

    The convention is that an asset's key is prefixed with its family — the directory it
    lives in under ``dags/assets``, or its own filename for a module placed directly
    there. An asset that already declares a ``key_prefix`` has made a deliberate choice
    and keeps it, so ``@dg.asset(key_prefix="ocean")`` stays ``ocean/...`` rather than
    becoming ``cmems/ocean/...``.
    """
    plain = _load_grouped(module, family, key_prefix=None)

    def is_bare(asset: dg.AssetsDefinition) -> bool:
        keys = list(getattr(asset, "keys", ()))
        return bool(keys) and all(len(key.path) == 1 for key in keys)

    if all(is_bare(asset) for asset in plain):
        return _load_grouped(module, family, key_prefix=family)

    # Mixed module: pair the two loads by op identity, which ``key_prefix`` preserves.
    prefixed = _load_grouped(module, family, key_prefix=family)
    by_node = {
        id(node): asset
        for asset in prefixed
        if (node := _node_def(asset)) is not None
    }
    result: list[dg.AssetsDefinition] = []
    for asset in plain:
        node_def = _node_def(asset)
        if is_bare(asset) and node_def is not None and id(node_def) in by_node:
            result.append(by_node[id(node_def)])
        else:
            result.append(asset)
    return result


def load_assets(
    modules: list[ModuleType],
) -> tuple[list[dg.AssetsDefinition], dict[str, str]]:
    """Collect the assets defined by each module, namespaced by its family.

    Assets already seen under another module are dropped. Deduplication is by the
    identity of the underlying op, not by asset key: ``load_assets_from_modules`` picks
    up anything in a module's namespace, so a module that does ``from .sibling import
    thing`` yields a second copy of that asset under its own key prefix. The two copies
    share one op, which is how they are told apart from two genuinely different assets
    that happen to share a name.

    Renaming the op (:func:`_qualify_op_name`) therefore happens *after* that check: it
    builds a new op per module, and pairs sharing an op would no longer be recognisable
    as copies of one asset.
    """
    loaded: list[dg.AssetsDefinition] = []
    failures: dict[str, str] = {}
    seen_ops: set[int] = set()
    seen_keys: set[dg.AssetKey] = set()

    for module in modules:
        family = _family(module.__name__)
        try:
            found = _load_one_module(module, family)
        except Exception as exc:  # noqa: BLE001
            failures[module.__name__] = f"{type(exc).__name__}: {exc}"
            logger.warning(f"dagster: could not load assets from {module.__name__}: {exc}")
            continue

        for asset in found:
            node_def = _node_def(asset)
            if node_def is not None:
                if id(node_def) in seen_ops:
                    continue
                seen_ops.add(id(node_def))

            keys = set(getattr(asset, "keys", ()) or ())
            if keys & seen_keys:
                clashing = sorted(k.to_user_string() for k in keys & seen_keys)
                logger.warning(
                    f"dagster: {module.__name__} redefines {clashing}; "
                    "keeping the first definition"
                )
                continue
            seen_keys |= keys
            try:
                loaded.append(_with_pool(_qualify_op_name(asset, family), family))
            except Exception as exc:  # noqa: BLE001 - an unrenamed op beats a lost module
                logger.warning(
                    f"dagster: could not namespace or pool the op behind "
                    f"{sorted(k.to_user_string() for k in keys)}: {exc}"
                )
                loaded.append(asset)

    return loaded, failures


def _partitions_key(partitions_def: dg.PartitionsDefinition) -> str:
    """A short, stable name for a partitioning, used in job and schedule names.

    Assets in one partitioned job must share a ``partitions_def``, so jobs are keyed by
    partitioning as well as by memory class. The name has to be stable across code server
    restarts — it is what a schedule and its run history are addressed by — so it is
    derived from the partitioning's own configuration, never from ``id()``.

    The readable part (kind and start date) is for whoever reads the Dagster UI; the digest
    is what makes it correct. Two daily partitionings that differ only in ``end_offset``
    must not land in one job, and Dagster would reject the mixed selection if they did.
    """
    kind = type(partitions_def).__name__.replace("PartitionsDefinition", "").lower() or "custom"
    parts = [kind]
    start = getattr(partitions_def, "start", None)
    if start is not None:
        with contextlib.suppress(ValueError, TypeError):
            parts.append(pd.Timestamp(start).strftime("%Y%m%d"))
    parts.append(hashlib.sha1(repr(partitions_def).encode()).hexdigest()[:6])
    name = "_".join(parts)
    return "".join(c if c.isalnum() or c == "_" else "_" for c in name)


def _time_partitions_def(partitions_def: dg.PartitionsDefinition):
    """The time-window partitioning inside ``partitions_def``, or None if it has none.

    A ``MultiPartitionsDefinition`` carries its cadence on one dimension; the others (a
    band, a region) are static. A wholly static partitioning has no cadence at all.
    """
    if isinstance(partitions_def, dg.TimeWindowPartitionsDefinition):
        return partitions_def
    if isinstance(partitions_def, dg.MultiPartitionsDefinition):
        # Raises rather than returning None when every dimension is static.
        with contextlib.suppress(Exception):
            return partitions_def.time_window_dimension.partitions_def
    return None


def _schedule_offsets(partitions_def: dg.PartitionsDefinition) -> dict[str, int]:
    """The ``hour_of_day``/``minute_of_hour`` that this partitioning will accept.

    ``build_schedule_from_partitioned_job`` derives its cron from the partitioning and
    lets you shift it within the period — but only where that makes sense, and it says so
    by raising at *resolve* time rather than when the schedule is built, so the decision
    has to be made here. An hourly partitioning refuses ``hour_of_day`` ("Cannot set hour
    parameter with hourly partitions"). Passing nothing leaves the partitioning's own cron
    untouched, which is the right answer for anything with no regular period.
    """
    inner = _time_partitions_def(partitions_def)
    schedule_type = getattr(inner, "schedule_type", None)
    if schedule_type is None:
        return {}
    if getattr(schedule_type, "name", str(schedule_type)).upper() == "HOURLY":
        return {"minute_of_hour": DAILY_CRON_MINUTE}
    return {"hour_of_day": DAILY_CRON_HOUR, "minute_of_hour": DAILY_CRON_MINUTE}


def asset_memory_class(asset: dg.AssetsDefinition, spec: dg.AssetSpec) -> str:
    """The memory class an asset's runs should be queued under.

    Factory-built assets declare it (see :func:`~dags.factory.make_provider_asset`). The
    hand-rolled assets in ``dags/assets`` do not, and treating "undeclared" as "exempt"
    was the bug this replaces: the selection matched nothing, so the code location shipped
    no jobs, no schedules, and the ``tag_concurrency_limits`` in ``dags/dagster.yaml``
    matched no run. An undeclared asset is assumed to need :data:`DEFAULT_MEMORY_GB`,
    which is the same assumption the factory makes.
    """
    declared = (spec.tags or {}).get(MEMORY_CLASS_TAG)
    return declared or memory_class_for(DEFAULT_MEMORY_GB)


def build_memory_class_jobs(
    assets: list[dg.AssetsDefinition],
) -> tuple[list[dg.JobDefinition], list[dg.ScheduleDefinition]]:
    """One scheduled job per (partitioning, memory class) over the partitioned assets.

    Grouping by memory class is what lets the run queue apply a different limit to a
    32 GB reanalysis ingest than to a 2 GB station download: the class is carried on the
    job's run tags, which is what ``tag_concurrency_limits`` matches on.

    Grouping by partitioning as well is not a refinement but a requirement — Dagster
    refuses a partitioned asset job whose assets do not share one ``partitions_def`` —
    and it is what lets the schedule be built with
    :func:`dagster.build_schedule_from_partitioned_job`. That matters: a plain
    ``ScheduleDefinition`` over a partitioned job emits ``RunRequest(partition_key=None)``,
    and executing a partitioned job without a partition key fails outright, so every tick
    produced a failed run. It also gives each partitioning the cadence it actually wants,
    so hourly assets are not asked for one partition a day.

    Assets carrying their own ``AutomationCondition`` are left out. They have opted into
    declarative automation and scheduling them here as well would launch each partition
    twice. So are assets tagged ``SCHEDULE_TAG: "manual"``, which are backfilled by hand.
    """
    by_group: dict[tuple[str, str], list[dg.AssetKey]] = defaultdict(list)
    partitions_by_key: dict[str, dg.PartitionsDefinition] = {}

    for asset in assets:
        partitions_def = getattr(asset, "partitions_def", None)
        if partitions_def is None:
            # Unpartitioned assets have no window to schedule; they are materialised on
            # demand or by their own automation condition.
            continue
        partitions_key = _partitions_key(partitions_def)
        partitions_by_key.setdefault(partitions_key, partitions_def)
        for spec in asset.specs:
            if spec.automation_condition is not None:
                continue
            if (spec.tags or {}).get(SCHEDULE_TAG) == "manual":
                continue
            by_group[(partitions_key, asset_memory_class(asset, spec))].append(spec.key)

    jobs: list[dg.JobDefinition] = []
    schedules: list[dg.ScheduleDefinition] = []
    for (partitions_key, memory_class), keys in sorted(by_group.items()):
        name = f"providers_{partitions_key}_{memory_class}"
        # No `partitions_def=` here: Dagster infers it from the selection, and passing it
        # as well is deprecated. The grouping above is what guarantees the selection is
        # uniform enough for that inference to succeed.
        job = dg.define_asset_job(
            name=name,
            selection=dg.AssetSelection.assets(*keys),
            tags={MEMORY_CLASS_TAG: memory_class},
            description=(
                f"Ingest of every {memory_class}-memory asset partitioned by "
                f"{partitions_key} ({len(keys)} assets)."
            ),
        )
        jobs.append(job)

        partitions_def = partitions_by_key[partitions_key]
        if _time_partitions_def(partitions_def) is None:
            # A wholly static partitioning has no cadence to schedule from, and
            # `build_schedule_from_partitioned_job` fails to resolve for one. The job is
            # still built so it can be launched by hand with the right run tags.
            logger.debug(f"dagster: {name} has no time partitioning, leaving it unscheduled")
            continue

        schedules.append(
            dg.build_schedule_from_partitioned_job(
                job,
                name=f"{name}_schedule",
                default_status=dg.DefaultScheduleStatus.RUNNING,
                # The execution timezone comes from the partitioning itself; passing it as
                # well as an offset is rejected.
                **_schedule_offsets(partitions_def),
            )
        )

    return jobs, schedules


def sizing_budget_gb() -> float:
    """The memory figure the static executor configuration is sized from, in GB.

    Deliberately derived from *total* memory rather than what happens to be free right
    now: this value is baked into the executor when the code location loads and then
    holds for the life of the process. Sizing it from available memory would mean a
    code-server reload while the host is busy permanently collapses every limit to one.
    The moment-to-moment check belongs to ``require_memory`` at run time, which does
    look at what is actually free.
    """
    from planetary_datasets.config import get_config
    from planetary_datasets.memory import total_memory_gb

    cfg = get_config()
    if cfg.memory_ceiling_gb is not None:
        return cfg.memory_ceiling_gb
    return total_memory_gb() * cfg.memory_fraction


def build_executor(budget_gb: float | None = None) -> dg.ExecutorDefinition:
    """A multiprocess executor whose parallelism is derived from host memory.

    ``max_concurrent`` is how many default-sized (8 GB) steps fit in the budget, capped
    by the CPU count. The per-class ``tag_concurrency_limits`` then stop a handful of
    large steps from filling those slots and blowing past the budget anyway.
    """
    budget = budget_gb if budget_gb is not None else sizing_budget_gb()
    slots = max(1, int(budget // DEFAULT_MEMORY_GB))
    max_concurrent = max(1, min(slots, os.cpu_count() or 1))
    return dg.multiprocess_executor.configured(
        {
            "max_concurrent": max_concurrent,
            "tag_concurrency_limits": executor_tag_concurrency_limits(budget),
        }
    )


def build_resources() -> dict[str, object]:
    """Resources available to every asset in this code location."""
    resources: dict[str, object] = {
        "config": PlanetaryConfigResource(),
        "memory": MemoryResource(),
        "pipes_subprocess_client": dg.PipesSubprocessClient(),
    }
    try:
        from dagster_docker import PipesDockerClient

        resources["pipes_docker_client"] = PipesDockerClient()
    except Exception as exc:  # noqa: BLE001 - Docker is optional on dev machines
        logger.warning(f"dagster: pipes_docker_client unavailable: {exc}")
    return resources


def build_definitions() -> dg.Definitions:
    """Assemble the code location."""
    modules, import_failures = discover_asset_modules()
    assets, load_failures = load_assets(modules)
    failures = {**import_failures, **load_failures}

    jobs, schedules = build_memory_class_jobs(assets)

    logger.info(
        f"dagster: loaded {len(assets)} asset definition(s) from {len(modules)} module(s); "
        f"{len(failures)} module(s) skipped; {len(jobs)} scheduled job(s)"
    )

    return dg.Definitions(
        assets=assets,
        jobs=jobs,
        schedules=schedules,
        resources=build_resources(),
        executor=build_executor(),
        metadata={
            "asset_modules_loaded": len(modules),
            "asset_module_import_failures": dg.MetadataValue.json(failures),
            "memory_budget_gb": round(memory_budget_gb(), 1),
        },
    )

