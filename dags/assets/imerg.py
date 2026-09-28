"""Dagster assets for IMERG, NASA's half-hourly global satellite precipitation product.

Three chained assets per run (early, late, final), one UTC day per partition:

``download``
    Stage the day's 48 granules from the GES DISC archive. Carries the ``download``
    concurrency key so the whole fleet of ingests shares a single download slot budget.
``zarr``
    Open the staged granules, concatenate the day and append it to the icechunk store,
    then delete the staged files.
``publish``
    Confirm where the store is published and how far it now runs.

The originals had a fourth asset that pre-allocated a "dummy" Zarr spanning 2000-2026 so
days could be written into it by region. Appending to icechunk needs no such placeholder,
so it is gone, along with the ``aws s3 sync --profile=sc`` the publish step used to shell
out to: the provider writes straight to the configured store.
"""

from __future__ import annotations

import datetime as dt

import dagster as dg
import pandas as pd

from planetary_datasets.providers.imerg import PRODUCTS, IMERGProduct, IMERGProvider

partitions_def: dg.TimeWindowPartitionsDefinition = dg.DailyPartitionsDefinition(
    start_date="2000-06-01",
    end_offset=-2,
)

#: Ten hours; a full backfill day of 48 granules over a slow Earthdata link is nowhere
#: near that, but the archive throttles hard at times.
MAX_RUNTIME = str(60 * 60 * 10)


# The ``context`` parameter of the asset bodies below is deliberately left unannotated:
# dagster compares the raw annotation object against AssetExecutionContext, and under
# ``from __future__ import annotations`` it only ever sees the string.
def _partition_day(context: dg.AssetExecutionContext) -> pd.Timestamp:
    """The UTC day a partitioned run covers."""
    start: dt.datetime = context.partition_time_window.start
    return pd.Timestamp(start).normalize()


def build_imerg_assets(product: IMERGProduct) -> list[dg.AssetsDefinition]:
    """Build the download -> zarr -> publish chain for one IMERG run."""
    code = product.code

    @dg.asset(
        name=f"imerg-{code}-download",
        description=f"Download {product.description} from NASA GES DISC",
        metadata={
            "collection": dg.MetadataValue.text(product.collection),
            "area": dg.MetadataValue.text("global"),
            "source": dg.MetadataValue.text("nasa-gesdisc"),
            "latency": dg.MetadataValue.text(product.latency),
        },
        tags={
            "dagster/max_runtime": MAX_RUNTIME,
            "dagster/priority": "2",
            "dagster/concurrency_key": "download",
        },
        partitions_def=partitions_def,
        automation_condition=dg.AutomationCondition.eager(),
    )
    def download_asset(context) -> dg.MaterializeResult:
        day = _partition_day(context)
        provider = IMERGProvider(product)
        files = provider.fetch(day)
        if not files:
            raise FileNotFoundError(f"No IMERG {code} granules available for {day.date()}")
        return dg.MaterializeResult(
            metadata={
                "day": dg.MetadataValue.text(str(day.date())),
                "granules": dg.MetadataValue.int(len(files)),
                "staging_dir": dg.MetadataValue.path(str(provider.staging_dir_for(day))),
            }
        )

    @dg.asset(
        name=f"imerg-{code}-zarr",
        description=f"Append {product.description} to its icechunk store",
        metadata={
            "collection": dg.MetadataValue.text(product.collection),
            "area": dg.MetadataValue.text("global"),
            "source": dg.MetadataValue.text("nasa-gesdisc"),
            "expected_runtime": dg.MetadataValue.text("1 hour"),
        },
        deps=[download_asset],
        tags={
            "dagster/max_runtime": MAX_RUNTIME,
            "dagster/priority": "2",
            "dagster/concurrency_key": "zarr-creation",
        },
        partitions_def=partitions_def,
        automation_condition=dg.AutomationCondition.eager(),
    )
    def zarr_asset(context) -> dg.MaterializeResult:
        day = _partition_day(context)
        provider = IMERGProvider(product)
        written = provider.write_staged(day)
        return dg.MaterializeResult(
            metadata={
                "day": dg.MetadataValue.text(str(day.date())),
                "written": dg.MetadataValue.bool(written),
                "store_path": dg.MetadataValue.path(provider.store_path),
            }
        )

    @dg.asset(
        name=f"imerg-{code}-publish",
        description=f"Confirm the published location of the IMERG {code} store",
        deps=[zarr_asset],
        automation_condition=dg.AutomationCondition.eager(),
    )
    def publish_asset(context) -> dg.MaterializeResult:
        info = IMERGProvider(product).publish()
        return dg.MaterializeResult(
            metadata={
                "store_path": dg.MetadataValue.path(info["store_path"]),
                "published": dg.MetadataValue.bool(info["published"]),
                "timesteps": dg.MetadataValue.int(info["timesteps"]),
                "last_time": dg.MetadataValue.text(str(info["last_time"])),
            }
        )

    return [download_asset, zarr_asset, publish_asset]


imerg_assets: list[dg.AssetsDefinition] = [
    asset for product in PRODUCTS.values() for asset in build_imerg_assets(product)
]

# Module-level names so ``dg.load_assets_from_modules`` finds them too.
(
    imerg_early_download_asset,
    imerg_early_zarr_asset,
    imerg_early_publish_asset,
    imerg_late_download_asset,
    imerg_late_zarr_asset,
    imerg_late_publish_asset,
    imerg_final_download_asset,
    imerg_final_zarr_asset,
    imerg_final_publish_asset,
) = imerg_assets
