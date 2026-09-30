"""Helpers shared by the data providers.

These were copy-pasted across dozens of standalone scripts before consolidation; the
canonical versions now live here. Import from this package rather than re-implementing:

* :mod:`planetary_datasets.common.download` — retrying downloads, skip-if-present.
* :mod:`planetary_datasets.common.dataset` — precision reduction, coordinate normalisation.
* :mod:`planetary_datasets.common.store` — icechunk encoding and append-or-create writes.
"""

from planetary_datasets.common.dataset import (
    make_lat_lon_coords_consistent,
    make_spatial_coords_increasing,
    lon_to_m180,
    reduce_precision,
)
from planetary_datasets.common.download import download_many, download_one
from planetary_datasets.common.store import (
    axis_is_sorted,
    build_encoding,
    existing_times,
    has_timestep,
    sort_append_axis,
    write_to_icechunk,
)

__all__ = [
    "axis_is_sorted",
    "build_encoding",
    "download_many",
    "download_one",
    "existing_times",
    "has_timestep",
    "lon_to_m180",
    "make_lat_lon_coords_consistent",
    "make_spatial_coords_increasing",
    "reduce_precision",
    "sort_append_axis",
    "write_to_icechunk",
]
