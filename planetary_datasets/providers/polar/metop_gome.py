"""MetOp GOME-2 level 1 products from the EUMETSAT Data Store.

GOME-2 native products are read with ``harp``, which flattens them to one row per
measurement on a ``time`` dimension. Replaces ``dags/assets/icechunky/gome.py``, which
wrote to a store path relative to the working directory and referenced its EUMETSAT
credentials before defining them.
"""

from __future__ import annotations

import pathlib
from typing import List

import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.providers.polar._eumdac import EumdacProvider
from planetary_datasets.providers.polar.metop_iasi import harp_to_dataset

#: Measurements per chunk along ``time``.
CHUNK_MEASUREMENTS = 1000


class MetopGomeProvider(EumdacProvider):
    """GOME-2 level 1 radiances for the MetOp series."""

    name = "metop_gome"
    append_dim = "time"
    store_prefix = "bkr/polar/metop_gome.icechunk"
    collection_id = "EO:EUM:DAT:METOP:GOMEL1"
    window = pd.Timedelta(1, "h")
    chunk_measurements = CHUNK_MEASUREMENTS

    def process_granule(self, filename: str) -> xr.Dataset:
        """Read one GOME-2 native product with harp."""
        ds = harp_to_dataset(filename)
        if "time" not in ds.coords and "datetime" in ds.variables:
            ds = ds.assign_coords(time=ds["datetime"].values).drop_vars("datetime")
        return ds

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Unpack, read and concatenate every GOME-2 product in the partition."""
        datasets = []
        for native in self.iter_natives(input_files, temp_dir=temp_dir):
            try:
                datasets.append(self.process_granule(native))
            except Exception as exc:  # noqa: BLE001 - one bad product must not lose the hour
                logger.warning(f"{self.name}: {native} failed: {exc}")

        ds = self.restrict_to_window(self.concat_granules(datasets), it)
        if ds.sizes.get("time", 0) == 0:
            return ds

        chunks: dict[str, int] = {"time": self.chunk_measurements}
        for dim in ("x", "y", "spectral"):
            if dim in ds.dims:
                chunks[dim] = -1
        return ds.chunk(chunks)
