"""MetOp MHS microwave humidity sounder orbits from the EUMETSAT Data Store.

Tailored to netCDF like AMSU-A, and padded like ASCAT: orbits vary in length, so each is
padded to the along-track length the store was created with.
"""

from __future__ import annotations

from planetary_datasets.providers.polar.metop_ascat import MetopAscatProvider


class MetopMhsProvider(MetopAscatProvider):
    """MHS level 1B brightness temperatures for the MetOp series."""

    name = "metop_mhs"
    store_prefix = "bkr/polar/metop_mhs.icechunk"
    collection_id = "EO:EUM:DAT:METOP:MHSL1"
    epct_product = "MHSL1"
