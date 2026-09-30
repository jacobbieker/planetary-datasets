"""Dagster assets for the surface observation networks.

Point observations from ASOS, ISD, GHCN-hourly, Meteostat, AERONET, the NOAA and NREL
radiation networks, PV-Live and the two CDS in-situ collections. Every asset is one
partition of one network; the work lives in
:mod:`planetary_datasets.providers.observations`.

Partition cadences differ by network because the upstream file layout does: the
one-minute networks publish a file per site-day, ISD and Meteostat a file per site-year,
PV-Live a ZIP per month and GHCNh a whole region per request.
"""

# NOTE: no ``from __future__ import annotations`` here. Dagster compares the *string* of
# the ``context`` annotation against the class and rejects the dotted
# ``dg.AssetExecutionContext`` form once annotations are lazily evaluated.

import datetime as dt

import dagster as dg
import pandas as pd

from planetary_datasets.providers.observations.aeronet import AeronetProvider
from planetary_datasets.providers.observations.asos import ASOSOneMinuteProvider
from planetary_datasets.providers.observations.ghcn import GHCNHourlyProvider
from planetary_datasets.providers.observations.gnss import GNSSProvider
from planetary_datasets.providers.observations.isd import ISDProvider
from planetary_datasets.providers.observations.meteostat import MeteostatHourlyProvider
from planetary_datasets.providers.observations.midc import MIDCProvider
from planetary_datasets.providers.observations.pvlive import PVLiveProvider
from planetary_datasets.providers.observations.rahm import RAHMProvider
from planetary_datasets.providers.observations.solrad import SolradProvider
from planetary_datasets.providers.observations.surfrad import SurfradProvider
from planetary_datasets.providers.observations.uk_marine import UKMarineObservationsProvider

DAILY_ASOS = dg.DailyPartitionsDefinition(start_date="2000-01-01", end_offset=-1)
DAILY_AERONET = dg.DailyPartitionsDefinition(start_date="1993-01-01", end_offset=-1)
DAILY_SOLRAD = dg.DailyPartitionsDefinition(start_date="2001-01-01", end_offset=-1)
DAILY_SURFRAD = dg.DailyPartitionsDefinition(start_date="1995-01-01", end_offset=-1)
DAILY_MIDC = dg.DailyPartitionsDefinition(start_date="2010-01-01", end_offset=-1)

MONTHLY_ISD = dg.MonthlyPartitionsDefinition(start_date="1901-01-01", end_offset=-1)
MONTHLY_METEOSTAT = dg.MonthlyPartitionsDefinition(start_date="1990-01-01", end_offset=-1)
MONTHLY_PVLIVE = dg.MonthlyPartitionsDefinition(start_date="2020-09-01", end_offset=-1)
MONTHLY_GNSS = dg.MonthlyPartitionsDefinition(start_date="2000-01-01", end_offset=-2)
MONTHLY_RAHM = dg.MonthlyPartitionsDefinition(start_date="1979-01-01", end_offset=-2)

# GHCNh is fetched a quarter at a time, which is what the meteora client is sized for.
QUARTERLY_GHCN = dg.TimeWindowPartitionsDefinition(
    start="2000-01-01",
    cron_schedule="0 0 1 1,4,7,10 *",
    fmt="%Y-%m-%d",
    end_offset=-1,
)

# The Met Office bucket is a rolling window of roughly ten days, so an hour that is not
# ingested within that window is lost; end_offset=-1 keeps the in-progress hour out and
# nothing more.
HOURLY_UK_MARINE = dg.HourlyPartitionsDefinition(start_date="2026-09-21-00:00", end_offset=-1)

# The CDS applies a per-user queue; keeping both CDS assets on one key stops a backfill
# of one starving the other.
CDS_TAGS = {"dagster/concurrency_key": "copernicus-cds", "dagster/priority": "1"}
HTTP_TAGS = {"dagster/concurrency_key": "observation-http"}
S3_TAGS = {"dagster/concurrency_key": "observation-s3"}


def _run(provider, context: dg.AssetExecutionContext) -> dg.Output[bool]:
    """Run one partition of an observation provider and report what happened."""
    it = pd.Timestamp(context.partition_time_window.start).tz_localize(None)
    started = dt.datetime.now(tz=dt.timezone.utc)
    written = provider.run_partition(it)
    elapsed = dt.datetime.now(tz=dt.timezone.utc) - started
    return dg.Output(
        value=written,
        metadata={
            "timestamp": dg.MetadataValue.text(str(it)),
            "written": dg.MetadataValue.bool(written),
            "store": dg.MetadataValue.text(provider.store_path),
            "elapsed_minutes": dg.MetadataValue.float(elapsed / dt.timedelta(minutes=1)),
        },
    )


def _metadata(source: str, network: str) -> dict:
    return {
        "source": dg.MetadataValue.text(source),
        "network": dg.MetadataValue.text(network),
        "format": dg.MetadataValue.text("icechunk"),
    }


@dg.asset(
    name="asos_one_minute",
    description="One-minute ASOS surface observations from the Iowa Environmental Mesonet.",
    key_prefix=["observation"],
    compute_kind="python",
    partitions_def=DAILY_ASOS,
    op_tags=HTTP_TAGS,
    metadata=_metadata("iem", "asos"),
    automation_condition=dg.AutomationCondition.on_cron(DAILY_ASOS.get_cron_schedule()),
)
def asos_one_minute(context: dg.AssetExecutionContext) -> dg.Output[bool]:
    return _run(ASOSOneMinuteProvider(), context)


@dg.asset(
    name="isd_hourly",
    description=(
        "NOAA Integrated Surface Database hourly observations, one month per partition. "
        "Covers the whole ~30,000-station roster: the year files a month needs are cached "
        "under the data directory and shared with that year's other eleven partitions, so "
        "the first partition of a year is far more expensive than the rest."
    ),
    key_prefix=["observation"],
    compute_kind="python",
    partitions_def=MONTHLY_ISD,
    op_tags=HTTP_TAGS,
    metadata=_metadata("noaa-isd-pds", "isd"),
    automation_condition=dg.AutomationCondition.on_cron(MONTHLY_ISD.get_cron_schedule()),
)
def isd_hourly(context: dg.AssetExecutionContext) -> dg.Output[bool]:
    return _run(ISDProvider(), context)


@dg.asset(
    name="ghcn_hourly",
    description=(
        "GHCN-hourly for the whole globe, one quarter per partition, via meteora. A "
        "global quarter is a large request; construct the provider with a region for "
        "anything narrower."
    ),
    key_prefix=["observation"],
    compute_kind="python",
    partitions_def=QUARTERLY_GHCN,
    op_tags=HTTP_TAGS,
    metadata=_metadata("ncei", "ghcnh"),
    automation_condition=dg.AutomationCondition.on_cron(QUARTERLY_GHCN.get_cron_schedule()),
)
def ghcn_hourly(context: dg.AssetExecutionContext) -> dg.Output[bool]:
    return _run(GHCNHourlyProvider(), context)


@dg.asset(
    name="meteostat_hourly",
    description=(
        "Meteostat bulk hourly station observations, one month per partition. Stations "
        "whose hourly inventory does not overlap the month are not requested; the rest "
        "share a cached year file across the year's twelve partitions."
    ),
    key_prefix=["observation"],
    compute_kind="python",
    partitions_def=MONTHLY_METEOSTAT,
    op_tags=HTTP_TAGS,
    metadata=_metadata("meteostat", "meteostat"),
    automation_condition=dg.AutomationCondition.on_cron(MONTHLY_METEOSTAT.get_cron_schedule()),
)
def meteostat_hourly(context: dg.AssetExecutionContext) -> dg.Output[bool]:
    return _run(MeteostatHourlyProvider(), context)


@dg.asset(
    name="aeronet_aod",
    description="AERONET version 3 direct-sun aerosol optical depth, level 1.5.",
    key_prefix=["observation"],
    compute_kind="python",
    partitions_def=DAILY_AERONET,
    op_tags=HTTP_TAGS,
    metadata=_metadata("nasa-aeronet", "aeronet"),
    automation_condition=dg.AutomationCondition.on_cron(DAILY_AERONET.get_cron_schedule()),
)
def aeronet_aod(context: dg.AssetExecutionContext) -> dg.Output[bool]:
    return _run(AeronetProvider(), context)


@dg.asset(
    name="solrad",
    description="NOAA SOLRAD one-minute surface irradiance.",
    key_prefix=["observation"],
    compute_kind="python",
    partitions_def=DAILY_SOLRAD,
    op_tags=HTTP_TAGS,
    metadata=_metadata("noaa-gml", "solrad"),
    automation_condition=dg.AutomationCondition.on_cron(DAILY_SOLRAD.get_cron_schedule()),
)
def solrad(context: dg.AssetExecutionContext) -> dg.Output[bool]:
    return _run(SolradProvider(), context)


@dg.asset(
    name="surfrad",
    description="NOAA SURFRAD one-minute surface radiation budget.",
    key_prefix=["observation"],
    compute_kind="python",
    partitions_def=DAILY_SURFRAD,
    op_tags=HTTP_TAGS,
    metadata=_metadata("noaa-gml", "surfrad"),
    automation_condition=dg.AutomationCondition.on_cron(DAILY_SURFRAD.get_cron_schedule()),
)
def surfrad(context: dg.AssetExecutionContext) -> dg.Output[bool]:
    return _run(SurfradProvider(), context)


@dg.asset(
    name="midc",
    description="NREL Measurement and Instrumentation Data Center raw station data.",
    key_prefix=["observation"],
    compute_kind="python",
    partitions_def=DAILY_MIDC,
    op_tags=HTTP_TAGS,
    metadata=_metadata("nrel-midc", "midc"),
    automation_condition=dg.AutomationCondition.on_cron(DAILY_MIDC.get_cron_schedule()),
)
def midc(context: dg.AssetExecutionContext) -> dg.Output[bool]:
    return _run(MIDCProvider(), context)


@dg.asset(
    name="pvlive",
    description="PV-Live one-minute irradiance from the 40-station Baden-Wurttemberg network.",
    key_prefix=["observation"],
    compute_kind="python",
    partitions_def=MONTHLY_PVLIVE,
    op_tags=HTTP_TAGS,
    metadata=_metadata("zenodo", "pvlive"),
    automation_condition=dg.AutomationCondition.on_cron(MONTHLY_PVLIVE.get_cron_schedule()),
)
def pvlive(context: dg.AssetExecutionContext) -> dg.Output[bool]:
    return _run(PVLiveProvider(), context)


@dg.asset(
    name="gnss_water_vapour",
    description="GNSS total column water vapour and zenith total delay from the CDS.",
    key_prefix=["observation"],
    compute_kind="python",
    partitions_def=MONTHLY_GNSS,
    op_tags=CDS_TAGS,
    metadata=_metadata("copernicus-cds", "gnss"),
    automation_condition=dg.AutomationCondition.on_cron(MONTHLY_GNSS.get_cron_schedule()),
)
def gnss_water_vapour(context: dg.AssetExecutionContext) -> dg.Output[bool]:
    return _run(GNSSProvider(), context)


@dg.asset(
    name="rahm_radiosonde",
    description="Harmonised IGRA baseline radiosonde soundings from the CDS.",
    key_prefix=["observation"],
    compute_kind="python",
    partitions_def=MONTHLY_RAHM,
    op_tags=CDS_TAGS,
    metadata=_metadata("copernicus-cds", "rahm"),
    automation_condition=dg.AutomationCondition.on_cron(MONTHLY_RAHM.get_cron_schedule()),
)
def rahm_radiosonde(context: dg.AssetExecutionContext) -> dg.Output[bool]:
    return _run(RAHMProvider(), context)


@dg.asset(
    name="uk_marine",
    description=(
        "Met Office UK marine surface observations: 58 buoys, light vessels and ship-borne "
        "weather stations, one hour per partition, from "
        "s3://met-office-marine-observations-data. Position is a data variable rather than "
        "a station coordinate because most of the network is under way. Includes the "
        "quality flags and the buoys' 32-band directional wave spectra."
    ),
    key_prefix=["observation"],
    compute_kind="python",
    partitions_def=HOURLY_UK_MARINE,
    op_tags=S3_TAGS,
    metadata=_metadata("met-office-marine-observations-data", "uk_marine"),
    automation_condition=dg.AutomationCondition.on_cron(HOURLY_UK_MARINE.get_cron_schedule()),
)
def uk_marine(context: dg.AssetExecutionContext) -> dg.Output[bool]:
    return _run(UKMarineObservationsProvider(), context)
