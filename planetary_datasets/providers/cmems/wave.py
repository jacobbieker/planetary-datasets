"""Regional CMEMS wave analysis/forecast providers.

Five near-identical modules (``cmems_wave_arctic``, ``_baltic``, ``_ibi``, ``_medsea``,
``_nwshelf``) differed only in the dataset id, the store name and a start date. They are
replaced by one engine, :class:`CMEMSWaveProvider`, plus the :data:`WAVE_REGIONS` table.

Every region serves the same hourly wave spectrum variables, so :data:`WAVE_VARIABLES` is
shared. The Arctic product is the odd one out in two ways, both handled here rather than in
a separate class: it still uses its legacy catalogue id, and it is served on a rotated grid
whose horizontal coordinates may be two-dimensional.
"""

from __future__ import annotations

import pathlib
from dataclasses import dataclass, field
from typing import List, Sequence

import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.providers.cmems.client import subset_day

#: Wave variables served by every regional product: significant heights, directions and
#: periods for the total sea, the wind sea and the two primary swell partitions, plus the
#: Stokes drift components. Checked against ``copernicusmarine.describe`` for all five
#: datasets on 2026-09-27: every one serves all of these. Each also serves ``VPED``, which
#: is deliberately not ingested.
WAVE_VARIABLES: tuple[str, ...] = (
    "VHM0",
    "VMDR",
    "VTPK",
    "VTM02",
    "VTM10",
    "VMXL",
    "VCMX",
    "VHM0_WW",
    "VTM01_WW",
    "VMDR_WW",
    "VHM0_SW1",
    "VTM01_SW1",
    "VMDR_SW1",
    "VHM0_SW2",
    "VTM01_SW2",
    "VMDR_SW2",
    "VSDX",
    "VSDY",
)


@dataclass(frozen=True)
class WaveRegion:
    """One regional CMEMS wave product.

    Attributes:
        key: Short region name, used in the provider name and the store path.
        dataset_id: Catalogue dataset id passed to ``copernicusmarine.subset``.
        product_id: Catalogue product the dataset belongs to, for documentation.
        description: Human-readable summary shown in logs and Dagster metadata.
        start_date: First day the dataset covers, used as the partition start. Taken from
            the time axis reported by ``copernicusmarine.describe``.
        variables: Variables to request. Defaults to :data:`WAVE_VARIABLES`.
    """

    key: str
    dataset_id: str
    product_id: str
    description: str
    start_date: str
    variables: tuple[str, ...] = field(default=WAVE_VARIABLES)

    @property
    def name(self) -> str:
        """Provider/asset name, e.g. ``cmems_wave_arctic``."""
        return f"cmems_wave_{self.key}"

    @property
    def store_prefix(self) -> str:
        """Store location relative to the configured bucket."""
        return f"bkr/cmems-wave/{self.key}.icechunk"


#: The regional wave products, keyed by short region name. Dataset ids and the first
#: covered day were read from ``copernicusmarine.describe`` on 2026-09-27.
WAVE_REGIONS: dict[str, WaveRegion] = {
    region.key: region
    for region in (
        WaveRegion(
            key="arctic",
            # This product still uses its legacy dataset id in the catalogue; the
            # modern-style ``cmems_mod_arc_wav_anfc_3km_PT1H-i`` does not exist for it.
            dataset_id="dataset-wam-arctic-1hr3km-be",
            product_id="ARCTIC_ANALYSIS_FORECAST_WAV_002_014",
            description="Arctic Ocean 3 km hourly wave analysis and forecast",
            start_date="2022-08-01",
        ),
        WaveRegion(
            key="baltic",
            dataset_id="cmems_mod_bal_wav_anfc_PT1H-i",
            product_id="BALTICSEA_ANALYSISFORECAST_WAV_003_010",
            description="Baltic Sea 2 km hourly wave analysis and forecast",
            start_date="2021-10-01",
        ),
        WaveRegion(
            key="ibi",
            dataset_id="cmems_mod_ibi_wav_anfc_0.027deg_PT1H-i",
            product_id="IBI_ANALYSISFORECAST_WAV_005_005",
            description="Iberia-Biscay-Ireland 3 km hourly wave analysis and forecast",
            start_date="2022-11-26",
        ),
        WaveRegion(
            key="medsea",
            dataset_id="cmems_mod_med_wav_anfc_4.2km_PT1H-i",
            product_id="MEDSEA_ANALYSISFORECAST_WAV_006_017",
            description="Mediterranean Sea 4.2 km hourly wave analysis and forecast",
            start_date="2021-11-30",
        ),
        WaveRegion(
            key="nwshelf",
            # Not ``..._0.027deg_PT1H-i``: that id belongs to the IBI product and
            # does not exist for NWSHELF (DatasetNotFound from the catalogue).
            dataset_id="cmems_mod_nws_wav_anfc_1.5km_PT1H-i",
            product_id="NWSHELF_ANALYSISFORECAST_WAV_004_014",
            description="Northwest European Shelf 1.5 km hourly wave analysis and forecast",
            start_date="2024-08-06",
        ),
    )
}


class CMEMSWaveProvider(BaseProvider):
    """Daily partitions of one regional CMEMS hourly wave analysis.

    One partition is one UTC day (24 hourly steps) downloaded as a single NetCDF with
    ``copernicusmarine.subset`` and appended to the region's Icechunk store along ``time``.

    Args:
        region: A :class:`WaveRegion` or its key in :data:`WAVE_REGIONS`.
        config: Configuration override, mostly for tests.
    """

    append_dim = "time"

    def __init__(self, region: str | WaveRegion, config=None):
        super().__init__(config=config)
        if isinstance(region, str):
            try:
                region = WAVE_REGIONS[region]
            except KeyError:
                raise KeyError(
                    f"unknown CMEMS wave region {region!r}; "
                    f"known regions: {sorted(WAVE_REGIONS)}"
                ) from None
        self.region = region
        self.name = region.name
        self.store_prefix = region.store_prefix
        self.dataset_id = region.dataset_id
        self.variables: Sequence[str] = region.variables

    def __repr__(self) -> str:
        return f"{type(self).__name__}(region={self.region.key!r})"

    def fetch(
        self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs
    ) -> List[str]:
        """Download one UTC day of hourly wave fields as a single NetCDF.

        Args:
            it: Partition timestamp (midnight UTC of the day to download).
            temp_dir: Directory to download into. Falls back to the configured scratch
                directory when omitted.

        Returns:
            Single-element list with the downloaded path, or an empty list on failure.
        """
        if temp_dir is None:
            temp_dir = self.config.scratch_dir / self.name
        path = subset_day(
            dataset_id=self.dataset_id,
            variables=self.variables,
            day=it,
            output_directory=temp_dir,
            output_filename=f"{self.name}_{it:%Y%m%d}.nc",
            config=self.config,
        )
        if path is None:
            return []
        return [str(path)]

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Open the downloaded day and shape it for the Icechunk store.

        Spatial dimensions are discovered from the data rather than hardcoded, because the
        Arctic product is served on a rotated grid: only dimensions backed by a
        one-dimensional coordinate are sorted, and any two-dimensional latitude/longitude
        coordinate variables are left untouched.

        Args:
            input_files: Paths returned by :meth:`fetch`.
            it: Partition timestamp, unused but part of the interface.
            temp_dir: Scratch directory, unused but part of the interface.

        Returns:
            Dataset chunked one timestep per chunk over the full spatial domain.
        """
        ds = xr.open_mfdataset(input_files, combine="nested", concat_dim=self.append_dim)
        ds = ds.sortby(self.append_dim)
        spatial_dims = [d for d in ds.dims if d != self.append_dim]
        for dim in spatial_dims:
            if dim in ds.coords and ds[dim].ndim == 1:
                ds = ds.sortby(dim)
        logger.debug(f"{self.name}: processed {it} with dims {dict(ds.sizes)}")
        return ds.chunk({self.append_dim: 1, **{d: -1 for d in spatial_dims}})


def wave_providers(config=None) -> list[CMEMSWaveProvider]:
    """One provider per region in :data:`WAVE_REGIONS`."""
    return [CMEMSWaveProvider(region, config=config) for region in WAVE_REGIONS.values()]
