"""Dagster assets for the DWD ICON models.

Four families of asset live here:

* ``icon_global``, ``icon_global_art`` and ``icon_eu_art`` — one partition per run of
  ICON-Global, ICON-ART and ICON-ART-EU, every level of every field, streamed from DWD's
  ``v1`` trees (the live server for a day, then the Source Cooperative archive) by
  :class:`~planetary_datasets.providers.icon_v1.ICONV1Provider` under a 4 GB ceiling.
  Partitions start at each model's first archived run.
* ``icon_<variant>`` — one partition per model run of the other ICON products: ICON-EU,
  the ensembles, the global model-level store and the ICON-ART analyses. Partitions follow
  DWD's 00/06/12/18 UTC run schedule.
* ``icon_d2_ruc_<cadence>`` — hourly partitions of the ICON-D2 rapid update cycle, split
  into the hourly, 15-minute and 5-minute stores DWD publishes interleaved.
* ``icon_global_model_level_half_heights`` — the static half-level height field, written
  once.

All of the work lives in :mod:`planetary_datasets.providers.icon` and
:mod:`planetary_datasets.providers.icon_v1`; these assets only pick a partition and report
metadata. Publishing to the Hugging Face Hub is opt-in per run via
the ``publish_to_hf`` run config, so a normal materialisation never uploads anything.
"""

import dagster as dg
import pandas as pd

from dags.factory import MEMORY_CLASS_TAG, MEMORY_GB_TAG, memory_class_for
from planetary_datasets.providers.icon import (
    VARIANTS,
    ICOND2RUCProvider,
    ICONProvider,
    write_model_level_half_heights,
)
from planetary_datasets.providers.icon_v1 import (
    ARCHIVE_STARTS,
    STORE_PREFIXES,
    VARIANT_MODELS,
    ICONV1Provider,
)
from planetary_datasets.providers.icon_v1_catalog import ARCHIVE_ROOT, LIVE_ROOT

#: DWD runs the deterministic and ensemble models four times a day.
RUN_PARTITIONS = dg.TimeWindowPartitionsDefinition(
    start="2026-01-01-00:00",
    fmt="%Y-%m-%d-%H:%M",
    cron_schedule="0 0,6,12,18 * * *",
    end_offset=0,
)

#: The variants :class:`ICONV1Provider` writes, and the asset name of each. They replace the
#: ``global`` and ``global_art`` variants of :class:`ICONProvider`, which wrote a few
#: hand-picked fields; the asset names are kept for the two that existed.
ICON_V1_ASSETS: dict[str, str] = {
    "global": "icon_global",
    "global_art": "icon_global_art",
    "eu_art": "icon_eu_art",
}

#: Memory each ICON v1 run is held to, in GB. The provider sizes its write to fit.
ICON_V1_MEMORY_GB = 4

#: A live run younger than the provider's settle time and still being published raises
#: IncompleteRun; DWD finishes a global run within a few hours, so retrying over about
#: four hours covers it without holding every partition back by the worst case.
ICON_V1_RETRY = dg.RetryPolicy(max_retries=4, delay=900, backoff=dg.Backoff.EXPONENTIAL)

#: The rapid update cycle produces a new analysis every hour.
RUC_PARTITIONS = dg.HourlyPartitionsDefinition(start_date="2026-04-18-00:00", end_offset=0)


class ICONPublishConfig(dg.Config):
    """Opt-in Hugging Face publishing for a single materialisation.

    Publishing uploads a directory, so it needs a store on local disk: either run with
    ``ICECHUNK_LOCAL_PATH`` set, or point ``publish_path`` at a local copy.
    """

    publish_to_hf: bool = False
    publish_path: str | None = None


def _partition_timestamp(context: dg.AssetExecutionContext) -> pd.Timestamp:
    """The initialisation time this partition covers."""
    return pd.Timestamp(context.partition_time_window.start).tz_localize(None)


def _forecast_asset(variant: str):
    """Build the Dagster asset for one ICON variant."""
    provider_config = VARIANTS[variant]

    @dg.asset(
        name=f"icon_{variant}",
        description=f"DWD ICON '{variant}' forecast, stored in Icechunk.",
        partitions_def=RUN_PARTITIONS,
        compute_kind="python",
        metadata={
            "store_prefix": dg.MetadataValue.text(provider_config.store_prefix),
            "source": dg.MetadataValue.url("https://opendata.dwd.de/weather/nwp"),
            "hf_repo_id": dg.MetadataValue.text(provider_config.hf_repo_id or "unset"),
        },
        tags={"dagster/concurrency_key": "icon"},
    )
    def _asset(
        context: dg.AssetExecutionContext,
        config: ICONPublishConfig,
    ) -> dg.MaterializeResult:
        provider = ICONProvider(variant)
        it = _partition_timestamp(context)
        written = provider.run_partition(it)

        repo_id = None
        if config.publish_to_hf and not written:
            # Nothing changed, so re-uploading the whole store would be pure cost.
            context.log.info("nothing written for this partition, skipping the upload")
        elif config.publish_to_hf:
            repo_id = provider.publish(
                config.publish_path or provider.store_path,
                path_in_repo=provider.store_prefix,
            )

        return dg.MaterializeResult(
            metadata={
                "written": dg.MetadataValue.bool(written),
                "init_time": dg.MetadataValue.text(str(it)),
                "store": dg.MetadataValue.path(provider.store_path),
                "published_to": dg.MetadataValue.text(repo_id or "not published"),
            }
        )

    return _asset


def _v1_partitions(variant: str) -> dg.TimeWindowPartitionsDefinition:
    """Six-hourly runs from the variant's first archived run."""
    return dg.TimeWindowPartitionsDefinition(
        start=ARCHIVE_STARTS[variant].strftime("%Y-%m-%d-%H:%M"),
        fmt="%Y-%m-%d-%H:%M",
        cron_schedule="0 0,6,12,18 * * *",
        end_offset=0,
    )


def _v1_asset(variant: str):
    """Build the Dagster asset for one :class:`ICONV1Provider` variant.

    Each has a pool of its own, so one run at a time writes each store, and the memory
    tags the fleet's queue and executor size concurrency by.
    """
    name = ICON_V1_ASSETS[variant]
    model = VARIANT_MODELS[variant]
    memory_tags = {
        MEMORY_CLASS_TAG: memory_class_for(ICON_V1_MEMORY_GB),
        MEMORY_GB_TAG: str(ICON_V1_MEMORY_GB),
    }

    @dg.asset(
        name=name,
        description=(
            f"DWD {model}, every field on every level, from the v1 GRIB trees, stored in Icechunk."
        ),
        partitions_def=_v1_partitions(variant),
        compute_kind="python",
        metadata={
            "store_prefix": dg.MetadataValue.text(STORE_PREFIXES[variant]),
            "archive": dg.MetadataValue.url(f"{ARCHIVE_ROOT}/{model}"),
            "live": dg.MetadataValue.url(f"{LIVE_ROOT}/{model}"),
        },
        pool=f"icon_v1_{variant}",
        retry_policy=ICON_V1_RETRY,
        tags=memory_tags,
        op_tags=memory_tags,
    )
    def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        provider = ICONV1Provider(variant)
        it = _partition_timestamp(context)
        written = provider.run_partition(it)
        return dg.MaterializeResult(
            metadata={
                "written": dg.MetadataValue.bool(written),
                "init_time": dg.MetadataValue.text(str(it)),
                "store": dg.MetadataValue.text(provider.store_prefix),
            }
        )

    return _asset


def _ruc_asset(timescale: str):
    """Build the Dagster asset for one ICON-D2-RUC cadence."""

    @dg.asset(
        name=f"icon_d2_ruc_{timescale}",
        description=(
            f"DWD ICON-D2 rapid update cycle, {timescale} fields, read from the local "
            "open data mirror and stored in Icechunk."
        ),
        partitions_def=RUC_PARTITIONS,
        compute_kind="python",
        metadata={
            "store_prefix": dg.MetadataValue.text(f"bkr/icon/icon_d2_ruc_{timescale}.icechunk"),
            "source": dg.MetadataValue.url(
                "https://opendata.dwd.de/weather/nwp/v1/m/icon-d2-ruc/p"
            ),
        },
        tags={"dagster/concurrency_key": "icon"},
    )
    def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        provider = ICOND2RUCProvider(timescale)
        it = _partition_timestamp(context)
        written = provider.run_partition(it)
        return dg.MaterializeResult(
            metadata={
                "written": dg.MetadataValue.bool(written),
                "time": dg.MetadataValue.text(str(it)),
                "store": dg.MetadataValue.path(provider.store_path),
                "mirror": dg.MetadataValue.path(str(provider.grib_dir)),
            }
        )

    return _asset


@dg.asset(
    name="icon_global_model_level_half_heights",
    description=(
        "Static half-level heights (HHL) for the ICON global grid. Time-invariant, so this "
        "is a no-op once the store exists."
    ),
    compute_kind="python",
)
def icon_global_model_level_half_heights() -> dg.MaterializeResult:
    """Write the static ICON global half-level height field if it is not stored yet."""
    written = write_model_level_half_heights()
    return dg.MaterializeResult(metadata={"written": dg.MetadataValue.bool(written)})


icon_forecast_assets = [
    _forecast_asset(variant) for variant in VARIANTS if variant not in ICON_V1_ASSETS
]
icon_v1_assets = [_v1_asset(variant) for variant in ICON_V1_ASSETS]
icon_ruc_assets = [_ruc_asset(timescale) for timescale in ICOND2RUCProvider.TIMESTEPS]

#: Every ICON asset, for loading into a Definitions object.
icon_assets = [
    *icon_v1_assets,
    *icon_forecast_assets,
    *icon_ruc_assets,
    icon_global_model_level_half_heights,
]
