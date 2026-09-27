"""GNSS precipitable water vapour from the Copernicus Climate Data Store.

The ``insitu-observations-gnss`` collection carries total column water vapour and zenith
total delay derived from the IGS ground station network. Both are column quantities, so
the result is a plain ``(time, station)`` cube.

Replaces ``dags/assets/observation/gnss.py``, a module-level loop over years and
half-years whose two branches — "file missing" and "file present but unreadable" — held
the same request body twice. Behaviour changes: partitions are monthly, the retrieval is
atomic, the response is pivoted into an Icechunk store instead of being left as loose
NetCDF, and the dataset name is no longer shadowed by the IGRA collection the original
assigned first and then overwrote.

Environment: ``CDSAPI_URL`` and ``CDSAPI_KEY``, or a ``~/.cdsapirc``.
"""

from __future__ import annotations

import pandas as pd

from planetary_datasets.providers.observations.cds import ALL_DAYS, CDSInsituProvider

DATASET = "insitu-observations-gnss"

#: IGS daily solutions. The other network types in this collection (``epn_repro2``,
#: ``igs_repro1``) are historical reprocessings on their own time axes.
NETWORK_TYPE = "igs_daily"

VARIABLES = ("total_column_water_vapour", "zenith_total_delay")


class GNSSProvider(CDSInsituProvider):
    """Monthly GNSS water-vapour retrievals from the CDS."""

    name = "gnss"
    store_prefix = "bkr/obs/gnss.icechunk"
    dataset = DATASET

    partition_freq = "MS"
    variables = VARIABLES
    # IGS daily products are delivered as five-minute solutions; hourly keeps the store
    # to a sensible size and matches how the other observation stores are sampled.
    sample_freq = "1h"

    def __init__(self, config=None, stations=None, network_type: str = NETWORK_TYPE):
        super().__init__(config=config, stations=stations)
        self.network_type = network_type

    def build_request(self, it: pd.Timestamp) -> dict:
        month = pd.Timestamp(it)
        return {
            "network_type": self.network_type,
            "variable": list(VARIABLES),
            "year": f"{month.year}",
            "month": f"{month.month:02d}",
            # The CDS ignores days that do not exist in the month.
            "day": list(ALL_DAYS),
            "data_format": "netcdf",
        }
