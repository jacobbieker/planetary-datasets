"""A one-asset code location that reports where its run landed: a smoke test for the fleet.

Materialise ``fleet_host_report`` and its metadata says which host the launcher chose,
what memory it reserved there, and where scratch went. Pin it to a host with the run
tag ``planetary/host`` to check one machine end to end::

    pixi run fleet exec -- dagster job launch -m dags.fleet.smoke -j fleet_check
        --tags '{"planetary/host": "aero"}'

(one command; wrapped here).

It is also part of the main code location (``dags/assets/fleet_check.py``). Launching
it from this module instead keeps every other schedule and sensor out of the daemon,
which is how to exercise the fleet without starting any real ingest.
"""

import os
import platform
import shutil
import socket
import time

import dagster as dg
import psutil

from dags.factory import MEMORY_CLASS_TAG, MEMORY_GB_TAG
from planetary_datasets.config import get_config


class FleetCheckConfig(dg.Config):
    """Options for the smoke test."""

    #: Stay alive this long after reporting, to exercise cancellation and health checks.
    sleep_seconds: int = 0


@dg.asset(
    name="fleet_host_report",
    description="Where this run was placed, and what it was given there.",
    op_tags={MEMORY_GB_TAG: "1", MEMORY_CLASS_TAG: "small"},
    kinds={"python"},
)
def fleet_host_report(
    context: dg.AssetExecutionContext, config: FleetCheckConfig
) -> dg.MaterializeResult:
    """Record the host, memory and scratch this run was given."""
    cfg = get_config()
    scratch = cfg.scratch_dir
    scratch.mkdir(parents=True, exist_ok=True)
    memory = psutil.virtual_memory()
    report = {
        "fleet_host": os.environ.get("PLANETARY_FLEET_HOST", "(not launched by the fleet)"),
        "hostname": socket.gethostname(),
        "platform": f"{platform.system()} {platform.machine()}",
        "memory_ceiling_gb": cfg.memory_ceiling_gb if cfg.memory_ceiling_gb else "unset",
        "memory_available_gb": round(memory.available / 1e9, 1),
        "memory_total_gb": round(memory.total / 1e9, 1),
        "scratch_dir": str(scratch),
        "scratch_free_gb": round(shutil.disk_usage(scratch).free / 1e9, 1),
        "code_dir": os.getcwd(),
    }
    context.log.info(f"fleet: ran on {report['fleet_host']} ({report['hostname']})")
    if config.sleep_seconds:
        context.log.info(f"fleet: holding for {config.sleep_seconds}s")
        time.sleep(config.sleep_seconds)
    return dg.MaterializeResult(metadata=report)


fleet_check = dg.define_asset_job("fleet_check", selection=[fleet_host_report])

defs = dg.Definitions(assets=[fleet_host_report], jobs=[fleet_check])
