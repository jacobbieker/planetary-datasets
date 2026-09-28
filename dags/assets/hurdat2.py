"""Dagster assets for the NOAA HURDAT2 best track archive.

One partition per hurricane season, one asset per basin. See
:mod:`planetary_datasets.providers.hurdat2` for the store layout and for why a season is
only worth materialising after the NHC's post-season reanalysis.
"""

import dagster as dg
import pandas as pd

from planetary_datasets.providers.hurdat2 import BASINS, HURDAT2Provider

# A season ends in November and the NHC's reanalysis lands the following spring, so the
# partition for a year is not offered until well after that year is over.
_YEARLY_CRON = "0 0 1 1 *"


def _partitions_def(first_year: int) -> dg.TimeWindowPartitionsDefinition:
    return dg.TimeWindowPartitionsDefinition(
        start=f"{first_year}-01-01",
        cron_schedule=_YEARLY_CRON,
        fmt="%Y-%m-%d",
        end_offset=-1,
    )


def _build_asset(basin: str) -> dg.AssetsDefinition:
    partitions_def = _partitions_def(BASINS[basin]["first_year"])

    @dg.asset(
        name=f"hurdat2_{basin}",
        description=f"NOAA HURDAT2 best tracks for the {basin} basin, one season per partition.",
        partitions_def=partitions_def,
        compute_kind="python",
        metadata={
            "source": dg.MetadataValue.url("https://www.nhc.noaa.gov/data/#hurdat"),
            "basin": dg.MetadataValue.text(basin),
        },
        tags={"dagster/concurrency_key": "nhc"},
    )
    # `context` is deliberately unannotated: Dagster reads the raw annotation, and either
    # `from __future__ import annotations` or an explicit AssetExecutionContext hint here
    # makes it raise DagsterInvalidDefinitionError.
    def _asset(context) -> dg.MaterializeResult:
        season = pd.Timestamp(context.partition_time_window.start)
        provider = HURDAT2Provider(basin=basin)
        written = provider.run_partition(season)
        return dg.MaterializeResult(
            metadata={
                "season": dg.MetadataValue.int(season.year),
                "written": dg.MetadataValue.bool(written),
                "store": dg.MetadataValue.text(provider.store_path),
                "release": dg.MetadataValue.url(provider.release_url()),
            }
        )

    return _asset


hurdat2_atlantic = _build_asset("atlantic")
hurdat2_pacific = _build_asset("pacific")
