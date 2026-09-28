"""Icechunk stores of Météo-France AROME forecasts published on data.gouv.fr.

One asset per AROME domain. Each partition is a single model init time: the assets fetch
the GRIB2 paquets for that init time, merge them and append the resulting hourly steps to
the domain's icechunk store. See :mod:`planetary_datasets.providers.arome`.
"""

from typing import Callable

import dagster as dg
import pandas as pd

from planetary_datasets.providers.arome import (
    OVERSEAS_REGIONS,
    AromeFranceHDProvider,
    AromeFranceProvider,
    AromeOverseasProvider,
    AromeProvider,
)

# The overseas domains run four times a day and six hours of each run are archived. Both
# France models run eight times a day, and only the three hours up to the next run are
# archived, so consecutive partitions tile the hours without gaps or overlaps.
SIX_HOURLY = dg.TimeWindowPartitionsDefinition(
    cron_schedule="0 0,6,12,18 * * *",
    start="2026-04-01-00:00",
    fmt="%Y-%m-%d-%H:%M",
)
THREE_HOURLY = dg.TimeWindowPartitionsDefinition(
    cron_schedule="0 0,3,6,9,12,15,18,21 * * *",
    start="2026-04-01-00:00",
    fmt="%Y-%m-%d-%H:%M",
)


def _arome_asset(
    name: str,
    factory: Callable[[], AromeProvider],
    partitions_def: dg.TimeWindowPartitionsDefinition,
    description: str,
) -> dg.AssetsDefinition:
    """Build the asset that materialises one AROME domain, one init time per partition."""

    @dg.asset(
        name=name,
        description=description,
        partitions_def=partitions_def,
        compute_kind="python",
        metadata={
            "source": dg.MetadataValue.text("meteofrance-data.gouv.fr"),
            "model": dg.MetadataValue.text("meteofrance-arome"),
            "format": dg.MetadataValue.text("icechunk"),
        },
        tags={"dagster/concurrency_key": "arome"},
    )
    def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        provider = factory()
        init_time = pd.Timestamp(context.partition_time_window.start).tz_localize(None)
        written = provider.run_partition(init_time)
        return dg.MaterializeResult(
            metadata={
                "init_time": dg.MetadataValue.text(str(init_time)),
                "store": dg.MetadataValue.text(provider.store_path),
                "written": dg.MetadataValue.bool(written),
            }
        )

    return _asset


arome_france_0025 = _arome_asset(
    "arome_france_0025",
    AromeFranceProvider,
    THREE_HOURLY,
    "AROME France on the 0.025 degree grid: surface, pressure and height level fields.",
)

arome_france_hd = _arome_asset(
    "arome_france_hd",
    AromeFranceHDProvider,
    THREE_HOURLY,
    "AROME France on the 0.01 degree grid: surface and height level fields.",
)


def _overseas_asset(region: str) -> dg.AssetsDefinition:
    domain = OVERSEAS_REGIONS[region]
    return _arome_asset(
        f"arome_{domain}",
        lambda: AromeOverseasProvider(region=region),
        SIX_HOURLY,
        f"AROME Overseas {domain.replace('_', ' ')} on the 0.025 degree grid: "
        "surface, pressure and height level fields.",
    )


# A list of AssetsDefinition is picked up by dagster's module loader just like a single
# one, so the five overseas domains do not each need their own module-level name.
arome_overseas_assets = [_overseas_asset(region) for region in OVERSEAS_REGIONS]
