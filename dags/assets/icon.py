"""Dagster assets for the DWD ICON models.

Three families of asset live here:

* ``icon_<variant>`` — one partition per model run of ICON global, ICON-EU, their
  ensembles and ICON-ART. Partitions follow DWD's 00/06/12/18 UTC run schedule.
* ``icon_d2_ruc_<cadence>`` — hourly partitions of the ICON-D2 rapid update cycle, split
  into the hourly, 15-minute and 5-minute stores DWD publishes interleaved.
* ``icon_global_model_level_half_heights`` — the static half-level height field, written
  once.

All of the work lives in :mod:`planetary_datasets.providers.icon`; these assets only pick
a partition and report metadata. Publishing to the Hugging Face Hub is opt-in per run via
the ``publish_to_hf`` run config, so a normal materialisation never uploads anything.
"""

import dagster as dg
import pandas as pd

from planetary_datasets.providers.icon import (
    VARIANTS,
    ICOND2RUCProvider,
    ICONProvider,
    write_model_level_half_heights,
)

#: DWD runs the deterministic and ensemble models four times a day.
RUN_PARTITIONS = dg.TimeWindowPartitionsDefinition(
    start="2026-01-01-00:00",
    fmt="%Y-%m-%d-%H:%M",
    cron_schedule="0 0,6,12,18 * * *",
    end_offset=0,
)

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
            "store_prefix": dg.MetadataValue.text(
                f"bkr/icon/icon_d2_ruc_{timescale}.icechunk"
            ),
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


icon_forecast_assets = [_forecast_asset(variant) for variant in VARIANTS]
icon_ruc_assets = [_ruc_asset(timescale) for timescale in ICOND2RUCProvider.TIMESTEPS]

#: Every ICON asset, for loading into a Definitions object.
icon_assets = [*icon_forecast_assets, *icon_ruc_assets, icon_global_model_level_half_heights]
