"""Dagster assets for the non-GOES geostationary imagers.

Three archives, three shapes of work:

* **GK-2A AMI** and **Himawari AHI** are ingested as *virtual references*. No
  pixel data is copied — each partition records the chunk layout of one day of
  source netCDF into an Icechunk store, which is why a day of a 22000x22000
  band costs kilobytes rather than terabytes. The heavy lifting lives in
  :mod:`planetary_datasets.providers.virtualized`.
* **MTG FCI** is pulled from the EUMETSAT Data Store over ``eumdac``, an hour
  of repeat cycles at a time, and lands as netCDF on local disk.
* **MSG RSS (IODC)** is produced by the ``sat-etl`` container, which this
  module only drives through Dagster Pipes.

Every store location, archive directory and credential comes from
:func:`planetary_datasets.config.get_config`, so the same definitions run
against the public bucket, a scratch bucket or a local directory with no code
change.

Registration: ``dags/definitions.py`` builds its code location with
``load_assets_from_package_module`` over the ``nwp``, ``observation`` and
``satellite`` packages. This module sits directly under ``dags/assets`` and so is
not picked up by any of them — it must be added to ``definitions.py`` explicitly,
via ``ASSETS`` below.

Note: this module deliberately does not use ``from __future__ import annotations``.
Dagster resolves the ``context`` parameter by its runtime annotation object, and a
postponed (string) annotation is rejected outright.
"""

import datetime as dt

import dagster as dg
from dagster import AssetExecutionContext
from dagster_docker import PipesDockerClient
from loguru import logger

from planetary_datasets.config import get_config
from planetary_datasets.providers.eumetsat import mtg as mtg_provider
from planetary_datasets.providers.virtualized import gk2a_ami_fd, himawari_isatss

# =============================================================================
# Partitions
# =============================================================================
#: Bands are a partition dimension rather than a loop so one bad band cannot
#: fail the other fifteen, and so a single band can be backfilled on its own.
GK2A_BANDS = dg.StaticPartitionsDefinition(list(gk2a_ami_fd.BANDS))
AHI_BANDS = dg.StaticPartitionsDefinition(list(himawari_isatss.BANDS))

gk2a_dates = dg.DailyPartitionsDefinition(
    start_date=gk2a_ami_fd.ARCHIVE_START_DATE.isoformat(),
    end_offset=-1,
)
gk2a_partitions = dg.MultiPartitionsDefinition({"date": gk2a_dates, "band": GK2A_BANDS})

himawari8_dates = dg.DailyPartitionsDefinition(
    start_date=himawari_isatss.ARCHIVE_START_DATE["himawari8"].isoformat(),
    # The two satellites overlap: Himawari-9 starts publishing on 2022-12-01 but
    # Himawari-8 stays operational until the 13th, so the ranges are not a
    # clean split and both cover that fortnight.
    end_date=himawari_isatss.ARCHIVE_END_DATE["himawari8"].isoformat(),
)
himawari8_partitions = dg.MultiPartitionsDefinition(
    {"date": himawari8_dates, "band": AHI_BANDS}
)

himawari9_dates = dg.DailyPartitionsDefinition(
    start_date=himawari_isatss.ARCHIVE_START_DATE["himawari9"].isoformat(),
    end_offset=-1,
)
himawari9_partitions = dg.MultiPartitionsDefinition(
    {"date": himawari9_dates, "band": AHI_BANDS}
)

#: The Data Store publishes a repeat cycle a few minutes after sensing, so the
#: most recent hours are not yet complete.
mtg_partitions = dg.HourlyPartitionsDefinition(
    start_date=mtg_provider.ARCHIVE_START.strftime("%Y-%m-%d-%H:%M"),
    end_offset=-3,
)

msg_partitions = dg.MonthlyPartitionsDefinition(start_date="2019-01-01", end_offset=-1)

_LONG_RUNNING = {
    "dagster/max_runtime": str(60 * 60 * 10),
    "dagster/priority": "1",
}


def _partition_date(context: AssetExecutionContext) -> dt.date:
    """The date half of a (date, band) multi-partition key."""
    return dt.date.fromisoformat(context.partition_key.keys_by_dimension["date"])


def _partition_band(context: AssetExecutionContext) -> str:
    """The band half of a (date, band) multi-partition key."""
    return context.partition_key.keys_by_dimension["band"]


# =============================================================================
# GK-2A AMI L1B full disk (noaa-gk2a-pds)
# =============================================================================
@dg.asset(
    name="gk2a_ami_fd_virtual",
    description="Virtual-reference Icechunk store of GK-2A AMI L1B full-disk radiance.",
    metadata={
        "source": dg.MetadataValue.text(gk2a_ami_fd.BUCKET),
        "product": dg.MetadataValue.text(gk2a_ami_fd.PRODUCT_LABEL),
    },
    partitions_def=gk2a_partitions,
    tags={**_LONG_RUNNING, "dagster/concurrency_key": "icechunk"},
)
def gk2a_ami_fd_virtual_asset(context: AssetExecutionContext) -> dg.MaterializeResult:
    """Reference one day of one GK-2A band into its Icechunk store."""
    date = _partition_date(context)
    band = _partition_band(context)

    repo = gk2a_ami_fd.open_repo(band)
    n_steps = gk2a_ami_fd.ingest_day(date, band, repo=repo, branch="main", group="")

    return dg.MaterializeResult(
        metadata={
            "date": dg.MetadataValue.text(date.isoformat()),
            "band": dg.MetadataValue.text(band),
            "timesteps_stored": dg.MetadataValue.int(n_steps),
            "store": dg.MetadataValue.text(
                get_config().store_path(gk2a_ami_fd.store_prefix_for(band))
            ),
        },
    )


# =============================================================================
# Himawari AHI L2 full disk, ISatSS tiles (noaa-himawari8 / noaa-himawari9)
# =============================================================================
def _himawari_materialize(
    context: AssetExecutionContext, satellite: str
) -> dg.MaterializeResult:
    """Reference one day of one AHI band into its Icechunk store."""
    date = _partition_date(context)
    band = _partition_band(context)

    repo = himawari_isatss.open_repo(satellite, band)
    n_scenes = himawari_isatss.ingest_day(satellite, date, band, repo=repo)

    return dg.MaterializeResult(
        metadata={
            "satellite": dg.MetadataValue.text(himawari_isatss.SATELLITE_NAMES[satellite]),
            "date": dg.MetadataValue.text(date.isoformat()),
            "band": dg.MetadataValue.text(band),
            "scenes_stored": dg.MetadataValue.int(n_scenes),
            "store": dg.MetadataValue.text(
                get_config().store_path(himawari_isatss.store_prefix_for(satellite, band))
            ),
        },
    )


@dg.asset(
    name="himawari8_isatss_virtual",
    description="Virtual-reference Icechunk store of Himawari-8 AHI full-disk ISatSS tiles.",
    metadata={
        "source": dg.MetadataValue.text(himawari_isatss.SATELLITE_BUCKET["himawari8"]),
        "product": dg.MetadataValue.text(himawari_isatss.PRODUCT_LABEL),
    },
    partitions_def=himawari8_partitions,
    tags={**_LONG_RUNNING, "dagster/concurrency_key": "icechunk"},
)
def himawari8_isatss_virtual_asset(context: AssetExecutionContext) -> dg.MaterializeResult:
    """Reference one day of one Himawari-8 band."""
    return _himawari_materialize(context, "himawari8")


@dg.asset(
    name="himawari9_isatss_virtual",
    description="Virtual-reference Icechunk store of Himawari-9 AHI full-disk ISatSS tiles.",
    metadata={
        "source": dg.MetadataValue.text(himawari_isatss.SATELLITE_BUCKET["himawari9"]),
        "product": dg.MetadataValue.text(himawari_isatss.PRODUCT_LABEL),
    },
    partitions_def=himawari9_partitions,
    tags={**_LONG_RUNNING, "dagster/concurrency_key": "icechunk"},
)
def himawari9_isatss_virtual_asset(context: AssetExecutionContext) -> dg.MaterializeResult:
    """Reference one day of one Himawari-9 band."""
    return _himawari_materialize(context, "himawari9")


# =============================================================================
# MTG FCI L1c (EUMETSAT Data Store)
# =============================================================================
def _mtg_materialize(
    context: AssetExecutionContext, product: str
) -> dg.MaterializeResult:
    """Download one hour of one MTG FCI product."""
    it: dt.datetime = context.partition_time_window.start
    files = mtg_provider.download_hour(product, it)

    return dg.MaterializeResult(
        metadata={
            "product": dg.MetadataValue.text(mtg_provider.PRODUCT_NAMES[product]),
            "collection": dg.MetadataValue.text(mtg_provider.collection_id(product)),
            "hour": dg.MetadataValue.text(it.isoformat()),
            "file_count": dg.MetadataValue.int(len(files)),
            "archive_folder": dg.MetadataValue.path(str(mtg_provider.archive_dir(product, it))),
        },
    )


@dg.asset(
    name="mtg_fdhi_download",
    description=(
        "MTG FCI L1c High Resolution Fast Imagery, from the EUMETSAT Data Store."
    ),
    metadata={"source": dg.MetadataValue.text("eumetsat-datastore")},
    partitions_def=mtg_partitions,
    tags={**_LONG_RUNNING, "dagster/concurrency_key": "eumetsat"},
)
def mtg_fdhi_download_asset(context: AssetExecutionContext) -> dg.MaterializeResult:
    """Download one hour of MTG FCI high-resolution full disk."""
    return _mtg_materialize(context, "fdhi")


@dg.asset(
    name="mtg_fdlr_download",
    description=(
        "MTG FCI L1c Full Disk High Spectral Imagery, from the EUMETSAT Data Store."
    ),
    metadata={"source": dg.MetadataValue.text("eumetsat-datastore")},
    partitions_def=mtg_partitions,
    tags={**_LONG_RUNNING, "dagster/concurrency_key": "eumetsat"},
)
def mtg_fdlr_download_asset(context: AssetExecutionContext) -> dg.MaterializeResult:
    """Download one hour of MTG FCI normal-resolution full disk."""
    return _mtg_materialize(context, "fdlr")


@dg.asset(
    name="mtg_hub_upload",
    description="Publish the local MTG archive to the Hugging Face Hub.",
    metadata={"destination": dg.MetadataValue.text("huggingface-hub")},
    deps=[mtg_fdhi_download_asset, mtg_fdlr_download_asset],
    tags={**_LONG_RUNNING, "dagster/concurrency_key": "huggingface"},
)
def mtg_hub_upload_asset(context: AssetExecutionContext) -> dg.MaterializeResult:
    """Upload the MTG archive directory to the repo named by ``HF_REPO_ID``.

    Publishes exactly what the two download assets produce —
    ``<data_dir>/eumetsat/mtg`` — rather than a store no asset in this repo
    builds. Skipped, not failed, when there is nothing configured or nothing
    downloaded yet: publishing is optional and most deployments do not want it.
    """
    from planetary_datasets.common.hub import upload_folder

    cfg = get_config()
    folder = mtg_provider.archive_root(cfg)

    if not cfg.hf_repo_id:
        logger.info("HF_REPO_ID is not set, skipping the Hugging Face upload")
        return dg.MaterializeResult(metadata={"skipped": dg.MetadataValue.text("no HF_REPO_ID")})
    if not folder.is_dir():
        logger.info(f"{folder} does not exist yet, skipping the Hugging Face upload")
        return dg.MaterializeResult(
            metadata={"skipped": dg.MetadataValue.text(f"no archive at {folder}")}
        )

    repo_id = upload_folder(folder, repo_type="dataset", config=cfg)
    return dg.MaterializeResult(
        metadata={
            "repo_id": dg.MetadataValue.text(repo_id),
            "folder": dg.MetadataValue.path(str(folder)),
        },
    )


# =============================================================================
# MSG SEVIRI Rapid Scan Service, Indian Ocean (EUMETSAT, via the sat-etl image)
# =============================================================================
#: Where the sat-etl container writes, inside the container.
MSG_CONTAINER_WORKDIR = "/work"
MSG_IMAGE = "ghcr.io/openclimatefix/sat-etl:main"
#: Subdirectory of ``cfg.data_dir`` the monthly Zarr stores land in.
MSG_DATA_SUBDIR = "eumetsat/iodc-lrv"


@dg.asset(
    name="eumetsat_iodc_lrv",
    description=(
        "Monthly Zarr of EUMETSAT SEVIRI Rapid Scan Service imagery over the Indian "
        "Ocean, low resolution. Produced by the sat-etl container."
    ),
    metadata={
        "area": dg.MetadataValue.text("india"),
        "source": dg.MetadataValue.text("eumetsat"),
        "expected_runtime": dg.MetadataValue.text("6 hours"),
    },
    compute_kind="docker",
    partitions_def=msg_partitions,
    tags={**_LONG_RUNNING, "dagster/concurrency_key": "eumetsat"},
)
def eumetsat_iodc_lrv_asset(
    context: AssetExecutionContext,
    pipes_docker_client: PipesDockerClient,
) -> dg.MaterializeResult:
    """Build one month of the EUMETSAT IODC low-resolution archive."""
    it: dt.datetime = context.partition_time_window.start
    archive_folder = get_config().data_dir / MSG_DATA_SUBDIR
    archive_folder.mkdir(parents=True, exist_ok=True)

    return pipes_docker_client.run(
        image=MSG_IMAGE,
        command=["iodc", "--month", f"{it:%Y-%m}", "--path", MSG_CONTAINER_WORKDIR, "--rm"],
        container_kwargs={"volumes": [f"{archive_folder}:{MSG_CONTAINER_WORKDIR}"]},
        context=context,
    ).get_materialize_result()


ASSETS = [
    gk2a_ami_fd_virtual_asset,
    himawari8_isatss_virtual_asset,
    himawari9_isatss_virtual_asset,
    mtg_fdhi_download_asset,
    mtg_fdlr_download_asset,
    mtg_hub_upload_asset,
    eumetsat_iodc_lrv_asset,
]
