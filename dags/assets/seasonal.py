"""Dagster assets for seasonal forecasts and the Destination Earth mirror.

Three unrelated sources that happen to share a cadence of "one long-range run at a time":

* C3S seasonal single-level forecasts from the CDS, one asset per originating centre and
  one partition per initialisation month.
* NOAA CFS, ingested from local NetCDF, unpartitioned because the source is whatever is
  on disk.
* Destination Earth hourly ERA5-Land, one partition per day.

Each needs its own credentials; see the provider modules for which environment variables.
"""

import dagster as dg
import pandas as pd

from planetary_datasets.providers.cfs import CFSSeasonalProvider
from planetary_datasets.providers.destine import DestinEERA5LandProvider
from planetary_datasets.providers.seasonal import CENTRE_SYSTEMS, SeasonalForecastProvider

#: Earliest initialisation month offered per centre, matching the first system recorded in
#: :data:`~planetary_datasets.providers.seasonal.CENTRE_SYSTEMS`.
SEASONAL_START = {centre: transitions[0][0] for centre, transitions in CENTRE_SYSTEMS.items()}


def _build_seasonal_asset(centre: str) -> dg.AssetsDefinition:
    """Build the asset for one originating centre.

    Only ECMWF, NCEP and UKMO are wired up below; the other centres in
    :data:`~planetary_datasets.providers.seasonal.CENTRE_SYSTEMS` work the same way and
    need only a module-level call here.
    """
    partitions_def = dg.MonthlyPartitionsDefinition(
        start_date=SEASONAL_START[centre],
        # A seasonal run is not published on the CDS until several days into the month.
        end_offset=-1,
    )

    @dg.asset(
        name=f"seasonal_{centre}",
        description=(
            f"C3S seasonal single-level forecast from {centre.upper()}, "
            "one initialisation month per partition."
        ),
        partitions_def=partitions_def,
        compute_kind="python",
        metadata={
            "source": dg.MetadataValue.url(
                "https://cds.climate.copernicus.eu/datasets/seasonal-original-single-levels"
            ),
            "originating_centre": dg.MetadataValue.text(centre),
        },
        tags={
            "dagster/concurrency_key": "copernicus-cds",
            "dagster/max_runtime": str(60 * 60 * 12),
        },
    )
    # `context` is deliberately unannotated: Dagster reads the raw annotation, and either
    # `from __future__ import annotations` or an explicit AssetExecutionContext hint here
    # makes it raise DagsterInvalidDefinitionError.
    def _asset(context) -> dg.MaterializeResult:
        init_month = pd.Timestamp(context.partition_time_window.start)
        provider = SeasonalForecastProvider(centre=centre)
        written = provider.run_partition(init_month)
        return dg.MaterializeResult(
            metadata={
                "init_month": dg.MetadataValue.text(f"{init_month:%Y-%m}"),
                "system": dg.MetadataValue.text(provider.system_number(init_month)),
                "written": dg.MetadataValue.bool(written),
                "store": dg.MetadataValue.text(provider.store_path),
            }
        )

    return _asset


seasonal_ecmwf = _build_seasonal_asset("ecmwf")
seasonal_ncep = _build_seasonal_asset("ncep")
seasonal_ukmo = _build_seasonal_asset("ukmo")


@dg.asset(
    name="cfs_seasonal",
    description=(
        "NOAA CFS seasonal forecasts ingested from local NetCDF into icechunk. "
        "Unpartitioned: every initialisation found on disk and not already stored is "
        "appended."
    ),
    compute_kind="python",
    metadata={
        "source": dg.MetadataValue.url(
            "https://www.ncei.noaa.gov/products/weather-climate-models/climate-forecast-system"
        ),
    },
)
# `context` is deliberately unannotated, as above.
def cfs_seasonal(context) -> dg.MaterializeResult:
    provider = CFSSeasonalProvider()
    written = provider.run_all()
    return dg.MaterializeResult(
        metadata={
            "initialisations_found": dg.MetadataValue.int(len(provider.discover())),
            "initialisations_written": dg.MetadataValue.int(written),
            "source_dir": dg.MetadataValue.path(str(provider.source_dir)),
            "store": dg.MetadataValue.text(provider.store_path),
        }
    )


@dg.asset(
    name="destine_era5_land",
    description=(
        "Hourly ERA5-Land mirrored from the Destination Earth Data Lake, one day per "
        "partition. Needs DESTINE_PAT."
    ),
    partitions_def=dg.DailyPartitionsDefinition(start_date="1950-01-01", end_offset=-5),
    compute_kind="python",
    metadata={"source": dg.MetadataValue.url("https://earthdatahub.destine.eu")},
    tags={"dagster/concurrency_key": "destination-earth"},
)
# `context` is deliberately unannotated, as above.
def destine_era5_land(context) -> dg.MaterializeResult:
    day = pd.Timestamp(context.partition_time_window.start)
    provider = DestinEERA5LandProvider()
    written = provider.run_partition(day)
    return dg.MaterializeResult(
        metadata={
            "day": dg.MetadataValue.text(f"{day:%Y-%m-%d}"),
            "written": dg.MetadataValue.bool(written),
            "variables": dg.MetadataValue.text(", ".join(provider.variables or ["all"])),
            "store": dg.MetadataValue.text(provider.store_path),
        }
    )
