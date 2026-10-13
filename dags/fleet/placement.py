"""Deciding which host a run goes to. Pure functions; no SSH, no Dagster instance.

A run is placed where it fits on two independent counts, and both must hold:

* **The ledger.** Every run the fleet has launched and not yet seen finish holds a
  reservation on its host: the memory and disk worked out here, recorded on the run's
  tags. A host's reservations may not exceed its capacity (``Host.capacity_gb``) or its
  ``max_runs``. The ledger is what stops a burst of launches from all landing on the
  same host before any of them has allocated anything.
* **The live probe.** The host must actually have the memory free right now, with
  ``reserve_gb`` to spare, and the disk with ``min_free_disk_gb`` to spare. This is what
  accounts for everything that is not a Dagster run: an IDE, a RAM-backed ``/tmp``, a
  job someone started by hand.

When nothing fits *now*, the launcher puts the run back in the queue. When nothing could
*ever* fit — the run needs more than any capable host's capacity, or a capability no host
offers — it fails the run instead of requeueing it forever.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

from dags.fleet.hosts import Host, Probe

#: Op tag listing what a step needs from its host, comma-separated (e.g. ``docker``).
REQUIRES_TAG = "planetary/requires"
#: Op tag giving a step's scratch-disk footprint in GB.
DISK_GB_TAG = "planetary/disk_gb"
#: Run tag pinning a run to one host by name.
HOST_PIN_TAG = "planetary/host"

#: Run tags the launcher writes, which the ledger and health checks read back.
FLEET_HOST_TAG = "planetary/fleet_host"
FLEET_PID_TAG = "planetary/fleet_pid"
FLEET_MEMORY_TAG = "planetary/fleet_memory_gb"
FLEET_DISK_TAG = "planetary/fleet_disk_gb"

#: Mirrors dags.factory; duplicated so this module does not import Dagster.
MEMORY_CLASS_TAG = "planetary/memory_class"
MEMORY_GB_TAG = "planetary/memory_gb"
CLASS_CEILINGS_GB = {"small": 4.0, "medium": 16.0, "large": 48.0, "xlarge": 64.0}
DEFAULT_STEP_MEMORY_GB = 8.0
DEFAULT_STEP_DISK_GB = 10.0

#: Per-run overhead on top of the steps: the run worker and each step's process load
#: the whole code location, about 0.35 GB apiece.
RUN_OVERHEAD_GB = 1.0

#: Steps a multi-asset run is sized to run at once. The run's executor is then held to
#: that reservation through MEMORY_CEILING_GB, so this both sizes the reservation and
#: bounds the run's parallelism.
STEPS_PER_RUN = 4


@dataclass(frozen=True)
class RunNeeds:
    """What a run needs from the host it is placed on."""

    memory_gb: float
    disk_gb: float
    requires: frozenset[str] = frozenset()
    pinned_host: str | None = None
    preferred_host: str | None = None


@dataclass(frozen=True)
class Load:
    """Reservations held on one host by runs in flight."""

    runs: int = 0
    memory_gb: float = 0.0
    disk_gb: float = 0.0


@dataclass(frozen=True)
class Placement:
    """The host chosen for a run, and the headroom left there after it."""

    host: Host
    headroom: float


@dataclass(frozen=True)
class NoFit:
    """Why a run was not placed. ``permanent`` means waiting will not help."""

    reason: str
    permanent: bool


def _float_tag(tags: Mapping[str, str], key: str) -> float | None:
    try:
        value = float(tags[key])
    except (KeyError, TypeError, ValueError):
        return None
    return value if value > 0 and math.isfinite(value) else None


def step_memory_gb(tags: Mapping[str, str]) -> float:
    """A step's declared memory: exact if given, else its class ceiling, else the default."""
    exact = _float_tag(tags, MEMORY_GB_TAG)
    if exact is not None:
        return exact
    return CLASS_CEILINGS_GB.get(tags.get(MEMORY_CLASS_TAG, ""), DEFAULT_STEP_MEMORY_GB)


def step_disk_gb(tags: Mapping[str, str]) -> float:
    """A step's declared scratch-disk footprint, or the default."""
    return _float_tag(tags, DISK_GB_TAG) or DEFAULT_STEP_DISK_GB


def parse_requires(value: str | None) -> frozenset[str]:
    """Split a comma-separated ``planetary/requires`` value."""
    return frozenset(part.strip() for part in (value or "").split(",") if part.strip())


def needs_for_run(
    step_tags: Sequence[Mapping[str, str]],
    run_tags: Mapping[str, str],
    preferred_host: str | None = None,
    steps_per_run: int = STEPS_PER_RUN,
) -> RunNeeds:
    """Work out what a run needs from the tags of the steps it will execute.

    The run executes up to ``steps_per_run`` steps at once, so it reserves the sum of
    its heaviest that many — enough for its worst concurrent moment, and no more.
    """
    if not step_tags:
        step_tags = [run_tags]
    memories = sorted((step_memory_gb(tags) for tags in step_tags), reverse=True)
    disks = sorted((step_disk_gb(tags) for tags in step_tags), reverse=True)
    width = max(1, min(steps_per_run, len(memories)))

    requires = parse_requires(run_tags.get(REQUIRES_TAG))
    for tags in step_tags:
        requires |= parse_requires(tags.get(REQUIRES_TAG))

    return RunNeeds(
        memory_gb=round(sum(memories[:width]) + RUN_OVERHEAD_GB, 2),
        disk_gb=round(sum(disks[:width]), 2),
        requires=requires,
        pinned_host=run_tags.get(HOST_PIN_TAG) or None,
        preferred_host=preferred_host,
    )


def ledger(in_flight_tags: Iterable[Mapping[str, str]]) -> dict[str, Load]:
    """Sum the reservations of runs in flight, per host, from their tags."""
    loads: dict[str, Load] = {}
    for tags in in_flight_tags:
        name = tags.get(FLEET_HOST_TAG)
        if not name:
            continue
        current = loads.get(name, Load())
        loads[name] = Load(
            runs=current.runs + 1,
            memory_gb=current.memory_gb + (_float_tag(tags, FLEET_MEMORY_TAG) or 0.0),
            disk_gb=current.disk_gb + (_float_tag(tags, FLEET_DISK_TAG) or 0.0),
        )
    return loads


def _why_not(host: Host, probe: Probe, load: Load, needs: RunNeeds) -> str | None:
    """None when the run fits on ``host`` now, else the reason it does not."""
    if not probe.reachable:
        return f"unreachable ({probe.error})"
    capacity = host.capacity_gb(probe.total_gb)
    if load.runs >= host.max_runs:
        return f"{load.runs}/{host.max_runs} runs already"
    if load.memory_gb + needs.memory_gb > capacity:
        return (
            f"{load.memory_gb:.0f} GB reserved + {needs.memory_gb:.0f} GB "
            f"> {capacity:.0f} GB capacity"
        )
    if probe.available_gb - needs.memory_gb < host.reserve_gb:
        return (
            f"only {probe.available_gb:.0f} GB free now, need {needs.memory_gb:.0f} "
            f"+ {host.reserve_gb:.0f} reserve"
        )
    if probe.disk_free_gb - load.disk_gb - needs.disk_gb < host.min_free_disk_gb:
        return (
            f"{probe.disk_free_gb:.0f} GB disk free, {load.disk_gb:.0f} reserved, need "
            f"{needs.disk_gb:.0f} + {host.min_free_disk_gb:.0f} spare"
        )
    return None


def _headroom(host: Host, probe: Probe, load: Load, needs: RunNeeds) -> float:
    """Fraction of the tighter of ledger and live memory left after placing the run."""
    capacity = host.capacity_gb(probe.total_gb)
    ledger_left = (capacity - load.memory_gb - needs.memory_gb) / capacity
    live_left = (probe.available_gb - needs.memory_gb) / max(probe.total_gb, 1e-9)
    return min(ledger_left, live_left)


def choose_host(
    needs: RunNeeds,
    hosts: Sequence[Host],
    probes: Mapping[str, Probe],
    loads: Mapping[str, Load],
) -> Placement | NoFit:
    """Pick the host with the most headroom that fits the run now.

    The previous attempt's host is preferred when it fits, because a retry usually wants
    the scratch and staged files its first attempt left there.
    """
    candidates = [host for host in hosts if host.enabled]
    if needs.pinned_host:
        candidates = [host for host in candidates if host.name == needs.pinned_host]
        if not candidates:
            return NoFit(f"pinned to {needs.pinned_host!r}, not an enabled host", True)

    capable = [host for host in candidates if needs.requires <= host.capabilities]
    if not capable:
        return NoFit(f"no enabled host offers {sorted(needs.requires)}", True)

    # A host's capacity is only known once it has been probed; an unreachable host
    # might be big enough, so "never fits" is only claimed when every capable host
    # has answered and none could hold the run even empty.
    reachable = [host for host in capable if probes.get(host.name, Probe(False)).reachable]
    if reachable and len(reachable) == len(capable):
        largest = max(host.capacity_gb(probes[host.name].total_gb) for host in reachable)
        if needs.memory_gb > largest:
            return NoFit(
                f"needs {needs.memory_gb:.0f} GB; the largest capable host can commit "
                f"{largest:.0f} GB",
                True,
            )

    fits: list[Placement] = []
    reasons: list[str] = []
    for host in capable:
        probe = probes.get(host.name, Probe(False, error="not probed"))
        load = loads.get(host.name, Load())
        why = _why_not(host, probe, load, needs)
        if why is None:
            fits.append(Placement(host, _headroom(host, probe, load, needs)))
        else:
            reasons.append(f"{host.name}: {why}")

    if not fits:
        return NoFit("; ".join(reasons), False)
    for placement in fits:
        if placement.host.name == needs.preferred_host:
            return placement
    # max() keeps the first of equals, so ties go to the host listed first.
    return max(fits, key=lambda placement: placement.headroom)


__all__ = [
    "DISK_GB_TAG",
    "FLEET_DISK_TAG",
    "FLEET_HOST_TAG",
    "FLEET_MEMORY_TAG",
    "FLEET_PID_TAG",
    "HOST_PIN_TAG",
    "REQUIRES_TAG",
    "Load",
    "NoFit",
    "Placement",
    "RunNeeds",
    "choose_host",
    "ledger",
    "needs_for_run",
    "parse_requires",
    "step_disk_gb",
    "step_memory_gb",
]
