"""Copernicus Marine Service (CMEMS) providers.

This package replaces twelve standalone scripts:

* seven bulk download scripts that each hardcoded the same credentials and a
  machine-specific output directory — now :mod:`.client`,
* a duplicate pair of post-download Icechunk writers for the global products — now
  :mod:`.global_ocean`,
* five near-identical regional wave providers — now one engine plus a region table in
  :mod:`.wave`.

Credentials come from ``COPERNICUSMARINE_SERVICE_USERNAME`` /
``COPERNICUSMARINE_SERVICE_PASSWORD`` through
:class:`~planetary_datasets.config.Config`, and stores resolve through the same config, so
nothing here is tied to one machine.
"""

from planetary_datasets.providers.cmems.client import (
    ANALYSIS_FORECAST_DATASETS,
    BULK_DATASETS,
    credentials,
    dataset_dir,
    download_dataset,
    download_datasets,
    resolve_dataset_id,
    subset_day,
)
from planetary_datasets.providers.cmems.global_ocean import (
    CMEMSGlobalOceanForecastProvider,
    CMEMSGlobalWaveReanalysisProvider,
    rename_to_standard_names,
)
from planetary_datasets.providers.cmems.wave import (
    WAVE_REGIONS,
    WAVE_VARIABLES,
    CMEMSWaveProvider,
    WaveRegion,
    wave_providers,
)

__all__ = [
    "ANALYSIS_FORECAST_DATASETS",
    "BULK_DATASETS",
    "CMEMSGlobalOceanForecastProvider",
    "CMEMSGlobalWaveReanalysisProvider",
    "CMEMSWaveProvider",
    "WAVE_REGIONS",
    "WAVE_VARIABLES",
    "WaveRegion",
    "credentials",
    "dataset_dir",
    "download_dataset",
    "download_datasets",
    "rename_to_standard_names",
    "resolve_dataset_id",
    "subset_day",
    "wave_providers",
]
