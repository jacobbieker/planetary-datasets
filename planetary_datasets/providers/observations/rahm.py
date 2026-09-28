"""RAHM, the radiosounding harmonisation dataset, from the Copernicus Climate Data Store.

RAHM is a homogenised reanalysis of the IGRA baseline radiosonde network, served by the
CDS as ``insitu-observations-igra-baseline-network`` with
``archive="radiosounding_harmonization_dataset_version_1"``. Unlike the other networks
here the observations have a vertical coordinate, so the cube gains a ``level`` axis and
each report is assigned to the nearest mandatory pressure level.

Replaces ``dags/assets/observation/rahm.py``, a module-level loop over 1979-2019 whose
"file missing" and "file present but unreadable" branches repeated the same 40-line
request. Behaviour changes: partitions are monthly, the retrieval is atomic, and the
response is pivoted into an Icechunk store rather than left as one NetCDF per month.

Environment: ``CDSAPI_URL`` and ``CDSAPI_KEY``, or a ``~/.cdsapirc``.
"""

from __future__ import annotations

import pandas as pd

from planetary_datasets.providers.observations.cds import ALL_DAYS, CDSInsituProvider

DATASET = "insitu-observations-igra-baseline-network"
ARCHIVE = "radiosounding_harmonization_dataset_version_1"

VARIABLES = (
    "air_dewpoint_depression",
    "air_temperature",
    "ascent_speed",
    "eastward_wind_speed",
    "frost_point_temperature",
    "geopotential_height",
    "northward_wind_speed",
    "relative_humidity",
    "solar_zenith_angle",
    "water_vapour_volume_mixing_ratio",
    "wind_from_direction",
    "wind_speed",
)

#: WMO mandatory pressure levels in Pa, which is the unit the CDS reports
#: ``z_coordinate`` in for this archive. Reports are snapped to the nearest one.
MANDATORY_LEVELS_PA = (
    100000.0,
    92500.0,
    85000.0,
    70000.0,
    50000.0,
    40000.0,
    30000.0,
    25000.0,
    20000.0,
    15000.0,
    10000.0,
    7000.0,
    5000.0,
    3000.0,
    2000.0,
    1000.0,
)


class RAHMProvider(CDSInsituProvider):
    """Monthly RAHM radiosonde retrievals from the CDS."""

    name = "rahm"
    store_prefix = "bkr/obs/rahm.icechunk"
    dataset = DATASET

    partition_freq = "MS"
    variables = VARIABLES
    # Baseline soundings are nominally 00Z and 12Z; a six-hourly grid absorbs the
    # intermediate ascents some stations fly without inventing a dense axis.
    sample_freq = "6h"
    levels = MANDATORY_LEVELS_PA

    def __init__(self, config=None, stations=None, archive: str = ARCHIVE):
        super().__init__(config=config, stations=stations)
        self.archive = archive

    def build_request(self, it: pd.Timestamp) -> dict:
        month = pd.Timestamp(it)
        return {
            "archive": self.archive,
            "variable": list(VARIABLES),
            "year": f"{month.year}",
            "month": f"{month.month:02d}",
            "day": list(ALL_DAYS),
            "data_format": "netcdf",
        }
