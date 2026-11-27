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

    def process_granule(self, filename: str, keep_in: pathlib.Path | None = None) -> xr.Dataset:
        """Read one GOME-2 native product with harp, lazily from ``keep_in`` when given."""
        ds = harp_to_dataset(filename, keep_in=keep_in, chunks={"time": self.chunk_measurements})
        if "time" not in ds.coords and "datetime" in ds.variables:
            ds = ds.assign_coords(time=ds["datetime"].values).drop_vars("datetime")
        # harp gives per-product fields such as ``orbit_index`` once, with no dimension.
        # Concatenating several products broadcasts them along ``time``, which is how the
        # store holds them, so an hour with a single product has to match that itself.
        scalars = [name for name, var in ds.data_vars.items() if var.ndim == 0]
        for name in scalars:
            ds[name] = ds[name].expand_dims(time=ds.sizes["time"])
        return ds

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Unpack, read and concatenate every GOME-2 product in the partition.

        Each product stays on disk in the partition's temporary directory, which outlasts
        the write, and only its rows inside the window are kept. Read into memory, the
        copies concat and sortby made took an hour of 2.9 GB to 26.7 GB, and the lane
        peaked at up to 37 GB (2026-11); lazily it peaked at 3.7 GB in the same time.
        """
        datasets = []
        products = self.workdir(temp_dir) / "harp"
        for native in self.iter_natives(input_files, temp_dir=temp_dir):
            try:
                granule = self.restrict_to_window(self.process_granule(native, products), it)
            except Exception as exc:  # noqa: BLE001 - one bad product must not lose the hour
                logger.warning(f"{self.name}: {native} failed: {exc}")
                continue
            if granule.sizes.get("time", 0):
                datasets.append(granule)

        ds = self.concat_granules(datasets)
        if ds.sizes.get("time", 0) == 0:
            return ds

        chunks: dict[str, int] = {"time": self.chunk_measurements}
        for dim in ("x", "y", "spectral"):
            if dim in ds.dims:
                chunks[dim] = -1
        return ds.chunk(chunks)
