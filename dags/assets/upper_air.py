"""Dagster assets for upper-air and aircraft observations: AMDAR, IGRA and Sondehub.

Deliberately no ``from __future__ import annotations`` here. Dagster resolves the
``context: dg.AssetExecutionContext`` annotation by identity at decoration time, and
stringised annotations break that check.

The IGRA CDS route is two chained assets: ``igra_cds_raw`` pulls the month's NetCDF from
the Climate Data Store into the data directory, and ``igra_cds_observations`` reads it
into the store. Running the second alone still works — it downloads what it needs — but
splitting them keeps a slow, rate-limited request from being repeated by a retry of the
write.

This module is not inside one of the packages ``dags/definitions.py`` loads with
``load_assets_from_package_module``, so nothing here is registered until that file adds
it. :data:`upper_air_assets` is exported for exactly that, so the wiring is one import::

    from assets.upper_air import upper_air_assets
"""

import dagster as dg
import pandas as pd

from planetary_datasets.config import get_config
from planetary_datasets.providers.observations.amdar import AMDARProvider, pb2nc_available
from planetary_datasets.providers.observations.igra import (
    IGRACDSProvider,
    IGRAStationArchive,
    download_igra_month,
)
from planetary_datasets.providers.observations.sondehub import SondehubProvider

# Sondehub's archive opens in July 2018; AMDAR is a local feed so its start is arbitrary.
SONDEHUB_START = "2018-07-01"
AMDAR_START = "2020-01-01"
IGRA_START = "1979-01-01"

daily_partitions = dg.DailyPartitionsDefinition(start_date=SONDEHUB_START)
amdar_partitions = dg.DailyPartitionsDefinition(start_date=AMDAR_START)
monthly_partitions = dg.MonthlyPartitionsDefinition(start_date=IGRA_START)


def _partition_timestamp(context) -> pd.Timestamp:
    """The partition key as a timestamp, for providers keyed on time."""
    return pd.Timestamp(context.partition_key)


@dg.asset(
    name="amdar_observations",
    partitions_def=amdar_partitions,
    description="AMDAR aircraft reports decoded from local PREPBUFR files with MET pb2nc.",
    compute_kind="icechunk",
)
def amdar_observations(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Decode and store one day of AMDAR aircraft reports."""
    provider = AMDARProvider()
    if not pb2nc_available():
        # Fail loudly and name the tool rather than writing an empty partition that the
        # window check would then treat as permanently done.
        raise RuntimeError(
            "MET's pb2nc is not installed, so AMDAR PREPBUFR files cannot be decoded. "
            "Install the MET toolkit or set PB2NC_BINARY."
        )
    written = provider.run_partition(_partition_timestamp(context))
    return dg.MaterializeResult(
        metadata={
            "written": written,
            "store": provider.store_path,
            "bufr_dir": str(provider.bufr_dir),
        }
    )


@dg.asset(
    name="igra_cds_raw",
    partitions_def=monthly_partitions,
    description="One month of IGRA v2 soundings downloaded from the Copernicus CDS.",
    compute_kind="download",
)
def igra_cds_raw(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Download one month of IGRA from the CDS into the data directory."""
    provider = IGRACDSProvider()
    it = _partition_timestamp(context)
    path = download_igra_month(it.year, it.month, provider.raw_dir, config=get_config())
    return dg.MaterializeResult(
        metadata={"path": str(path), "bytes": path.stat().st_size}
    )


@dg.asset(
    name="igra_cds_observations",
    partitions_def=monthly_partitions,
    deps=[igra_cds_raw],
    description="IGRA v2 soundings from the CDS, written as a table of timed observations.",
    compute_kind="icechunk",
)
def igra_cds_observations(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Write one month of downloaded IGRA soundings to the store."""
    provider = IGRACDSProvider()
    written = provider.run_partition(_partition_timestamp(context))
    return dg.MaterializeResult(metadata={"written": written, "store": provider.store_path})


@dg.asset(
    name="igra_station_archive",
    description=(
        "IGRA v2 soundings on 16 standard pressure levels, built from NCEI's per-station "
        "archive files. Not partitioned: each file covers a station's whole record."
    ),
    compute_kind="icechunk",
)
def igra_station_archive(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Rebuild the station-by-time-by-level IGRA store from NCEI's archive files."""
    archive = IGRAStationArchive()
    sounding_times = archive.build()
    return dg.MaterializeResult(
        metadata={
            "sounding_times_written": sounding_times,
            "store": get_config().store_path(archive.store_prefix),
        }
    )


@dg.asset(
    name="sondehub_observations",
    partitions_def=daily_partitions,
    description="One UTC day of Sondehub amateur radiosonde telemetry from the S3 history archive.",
    compute_kind="icechunk",
)
def sondehub_observations(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Store one UTC day of Sondehub radiosonde telemetry."""
    provider = SondehubProvider()
    written = provider.run_partition(_partition_timestamp(context))
    return dg.MaterializeResult(metadata={"written": written, "store": provider.store_path})


#: Every asset in this module, for ``dags/definitions.py`` to register.
upper_air_assets = [
    amdar_observations,
    igra_cds_raw,
    igra_cds_observations,
    igra_station_archive,
    sondehub_observations,
]
