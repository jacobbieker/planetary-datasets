"""Dagster assets for the UK and Finnish ground radar stores.

One asset per provider in :data:`planetary_datasets.providers.radar.PROVIDERS`. Each asset
materialises a single hourly partition by handing the partition start to the provider,
which locates that hour's files in the local archive, processes them and appends them to
the icechunk store.

Both sources are read from a staging directory rather than downloaded; see the provider
module docstring for ``UK_RADAR_ARCHIVE_DIR`` and ``FMI_RADAR_ARCHIVE_DIR``. The assets are
always defined and fail with a clear message when the directory is not configured.

Registration is deliberately left to ``dags/definitions.py``, which is owned separately:
that module currently loads only the ``nwp``, ``satellite`` and ``observation`` *packages*,
so these assets are not live until :data:`radar_assets` is added there alongside the other
top-level ``dags/assets/*.py`` modules.
"""

import datetime as dt
from typing import Type

import dagster as dg
import pandas as pd

from planetary_datasets.providers.radar import (
    FMIRadarProvider,
    LocalArchiveRadarProvider,
    UKRadarProvider,
)

#: Both composites are indexed by the hour. The UK partition covers twelve five-minute
#: frames; the FMI partition covers the three accumulation windows published on the hour.
hourly_partitions = dg.HourlyPartitionsDefinition(
    start_date=dt.datetime(2020, 1, 1), end_offset=-1
)


def _build_asset(
    provider_cls: Type[LocalArchiveRadarProvider],
    description: str,
    source_url: str,
) -> dg.AssetsDefinition:
    """Build the single-partition asset for one radar provider."""

    @dg.asset(
        name=provider_cls.name,
        description=description,
        partitions_def=hourly_partitions,
        compute_kind="python",
        metadata={
            "store": dg.MetadataValue.text(provider_cls.store_prefix),
            "archive_dir_env": dg.MetadataValue.text(provider_cls.archive_env),
            "source": dg.MetadataValue.url(source_url),
        },
    )
    # `context` is intentionally left unannotated: Dagster inspects the raw annotation, and
    # both a `dg.AssetExecutionContext` hint under `from __future__ import annotations` and
    # a stringified one raise DagsterInvalidDefinitionError.
    def _asset(context) -> dg.MaterializeResult:
        # Dagster hands the partition start over tz-aware; the provider normalises it to
        # naive UTC, which is how the radar times are stored.
        it = pd.Timestamp(context.partition_time_window.start)
        provider = provider_cls()
        written = provider.run_partition(it)
        context.log.info(f"{provider.name}: {it} -> {provider.store_path} (written={written})")
        return dg.MaterializeResult(
            metadata={
                "written": dg.MetadataValue.bool(written),
                "partition": dg.MetadataValue.text(str(it)),
                "store_path": dg.MetadataValue.path(provider.store_path),
            }
        )

    return _asset


uk_radar = _build_asset(
    UKRadarProvider,
    "Met Office RADARNET 1 km rain-rate composite, twelve five-minute ODIM frames per hour.",
    "https://catalogue.ceda.ac.uk/uuid/82adec1f896af6169112d09cc1174499",
)

fmi_radar = _build_asset(
    FMIRadarProvider,
    "FMI 1 km precipitation accumulations for Finland, 1 h / 12 h / 24 h windows per hour.",
    "https://en.ilmatieteenlaitos.fi/open-data",
)

radar_assets = [uk_radar, fmi_radar]

__all__ = ["fmi_radar", "hourly_partitions", "radar_assets", "uk_radar"]
