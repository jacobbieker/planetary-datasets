"""Dagster assets for NCEP's Global Forecast System.

Two assets, from two different sources:

``gfs-icechunk``
    The consolidated pipeline in :mod:`planetary_datasets.providers.gfs`. Downloads the
    0.25 degree GRIB2 output for one init time from the NCAR/NSF GDEX archive, merges it
    with cfgrib and appends it to the icechunk store. Public data, no credentials needed.

``ncep-gfs-global``
    The monthly Zarr archive built by the nwp-consumer Docker image from NOAA's S3 open
    data bucket (https://noaa-gfs-bdp-pds.s3.amazonaws.com). Kept as-is; it is a different
    source and a different output format from the icechunk store above.
"""

import datetime as dt
import os

import dagster as dg
import pandas as pd
from dagster_docker import PipesDockerClient

from planetary_datasets.providers.gfs import GFSProvider

# --------------------------------------------------------------------------------------
# Icechunk store, built in-process by GFSProvider.
# --------------------------------------------------------------------------------------

# The GDEX archive publishes a few days behind real time, so the most recent partitions
# would only ever fail. Holding the window back two days keeps the automation honest.
GDEX_LAG_PARTITIONS = -8

gfs_partitions_def = dg.TimeWindowPartitionsDefinition(
    start=dt.datetime(2016, 1, 1, 0, 0, tzinfo=dt.timezone.utc),
    cron_schedule="0 0,6,12,18 * * *",
    fmt="%Y-%m-%d|%H:%M",
    end_offset=GDEX_LAG_PARTITIONS,
)


def run_gfs_partition(it: pd.Timestamp) -> dg.MaterializeResult:
    """Append one GFS init time to the icechunk store and describe what happened.

    Raises ``dg.Failure`` rather than returning when the init time is still absent
    afterwards. A successful-but-empty materialisation would mark the partition done and
    leave a permanent hole in the store, because nothing would ever ask for it again.

    Kept separate from the asset so it can be exercised without spinning up a Dagster
    instance.
    """
    provider = GFSProvider()
    written = provider.run_partition(it)

    if not written and provider.missing_timesteps(pd.DatetimeIndex([it])):
        raise dg.Failure(
            description=(
                f"GFS init time {it} was not written to {provider.store_path}. It is "
                "either not published in the NCAR GDEX archive yet, or it does not line "
                "up with the existing store; see the logs above."
            )
        )

    return dg.MaterializeResult(
        metadata={
            "init_time": dg.MetadataValue.text(str(it)),
            "store": dg.MetadataValue.text(provider.store_path),
            "written": dg.MetadataValue.bool(written),
        }
    )


@dg.asset(
    name="gfs-icechunk",
    description=__doc__,
    partitions_def=gfs_partitions_def,
    metadata={
        "area": dg.MetadataValue.text("global"),
        "source": dg.MetadataValue.text("ncar-gdex"),
        "model": dg.MetadataValue.text("ncep-gfs"),
        "expected_runtime": dg.MetadataValue.text("20 minutes"),
    },
    compute_kind="python",
    automation_condition=dg.AutomationCondition.eager(),
    retry_policy=dg.RetryPolicy(max_retries=2, delay=600),
    tags={
        "dagster/max_runtime": str(60 * 60 * 3),
        "dagster/concurrency_key": "gfs",
    },
)
def gfs_icechunk_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Append one GFS init time to the icechunk store."""
    it = pd.Timestamp(context.partition_time_window.start).tz_localize(None)
    context.log.info(f"materialising GFS init time {it}")
    return run_gfs_partition(it)


# --------------------------------------------------------------------------------------
# Monthly Zarr archive, built by the nwp-consumer container.
# --------------------------------------------------------------------------------------

ARCHIVE_FOLDER = "/var/dagster-storage/nwp/ncep-gfs-global"
if os.getenv("ENVIRONMENT", "local") == "leo":
    ARCHIVE_FOLDER = "/mnt/storage_ssd_4tb/nwp/ncep-gfs-global"

archive_partitions_def: dg.TimeWindowPartitionsDefinition = dg.MonthlyPartitionsDefinition(
    start_date="2021-01-01",
    end_offset=-1,
)


@dg.asset(
    name="ncep-gfs-global",
    description=__doc__,
    partitions_def=archive_partitions_def,
    metadata={
        "archive_folder": dg.MetadataValue.text(ARCHIVE_FOLDER),
        "area": dg.MetadataValue.text("global"),
        "source": dg.MetadataValue.text("noaa-s3"),
        "model": dg.MetadataValue.text("ncep-gfs"),
        "expected_runtime": dg.MetadataValue.text("6 hours"),
    },
    compute_kind="docker",
    automation_condition=dg.AutomationCondition.on_cron(
        cron_schedule=archive_partitions_def.get_cron_schedule(
            hour_of_day=21,
            day_of_week=1,
        ),
    ),
    tags={
        "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
        "dagster/priority": "1",
        "dagster/concurrency_key": "nwp-consumer",
    },
)
def ncep_gfs_global_asset(
    context: dg.AssetExecutionContext,
    pipes_docker_client: PipesDockerClient,
) -> dg.MaterializeResult:
    """Archive one month of NCEP GFS global forecasts with the nwp-consumer image."""
    it: dt.datetime = context.partition_time_window.start
    return pipes_docker_client.run(
        image="ghcr.io/openclimatefix/nwp-consumer:1.0.12",
        command=["archive", "-y", str(it.year), "-m", str(it.month)],
        env={
            "MODEL_REPOSITORY": "gfs",
            "NOTIFICATION_REPOSITORY": "dagster-pipes",
            "CONCURRENCY": "true",
        },
        # See https://docker-py.readthedocs.io/en/stable/containers.html#docker.models.containers.ContainerCollection.run
        # The memory and CPU caps keep one archive run from starving the rest of the
        # daemon; nwp-consumer will spill to disk rather than grow past them.
        container_kwargs={
            "volumes": [f"{ARCHIVE_FOLDER}:/work"],
            "mem_limit": "8g",
            "nano_cpus": int(4e9),
        },
        context=context,
    ).get_materialize_result()


assets = [gfs_icechunk_asset, ncep_gfs_global_asset]
