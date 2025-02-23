"""All dagster definitions to be surfaced in this code location."""

import dagster as dg
from dagster_docker import PipesDockerClient

from assets import nwp, observation, satellite

nwp_assets = dg.load_assets_from_package_module(
    package_module=nwp,
    group_name="nwp",
    key_prefix="nwp",
)

sat_assets = dg.load_assets_from_package_module(
    package_module=satellite,
    group_name="satellite",
    key_prefix="satellite",
)

pv_assets = dg.load_assets_from_package_module(
    package_module=observation,
    group_name="observation",
    key_prefix="observation",
)

defs = dg.Definitions(
    assets=[*nwp_assets, *sat_assets, *pv_assets],
    resources={
        "pipes_subprocess_client": dg.PipesSubprocessClient(),
        "pipes_docker_client": PipesDockerClient(),
    },
    jobs=[],
    schedules=[],
)
