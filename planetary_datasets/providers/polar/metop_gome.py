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

#: Rows harp keeps either side of the partition window; the exact cut is
#: :meth:`~planetary_datasets.providers.polar._granule.GranuleProvider.restrict_to_window`'s.
HARP_MARGIN = pd.Timedelta(1, "min")


def harp_window(start: pd.Timestamp, end: pd.Timestamp) -> str:
    """The harp operations keeping the rows from ``start`` to ``end``, plus a margin.

    A GOME-2 product is a whole orbit (~100 min), so an hour keeps about a third of each
    of its three or four products. Read whole, the orbits peaked at 37 GB (2026-11-27).
    """
    epoch = pd.Timestamp("2000-01-01")
    low = (start - HARP_MARGIN - epoch).total_seconds()
    high = (end + HARP_MARGIN - epoch).total_seconds()
    unit = "[s since 2000-01-01]"
    return f"datetime >= {low:.0f} {unit}; datetime < {high:.0f} {unit}"


class MetopGomeProvider(EumdacProvider):
    """GOME-2 level 1 radiances for the MetOp series."""

    name = "metop_gome"
    append_dim = "time"
    store_prefix = "bkr/polar/metop_gome.icechunk"
    collection_id = "EO:EUM:DAT:METOP:GOMEL1"
    window = pd.Timedelta(1, "h")
    chunk_measurements = CHUNK_MEASUREMENTS

    def process_granule(self, filename: str, operations: str = "") -> xr.Dataset:
        """Read one GOME-2 native product with harp, applying harp ``operations``."""
        ds = harp_to_dataset(filename, operations=operations)
        # harp records the operations as ``history``, which unfiltered products never had.
        ds.attrs.pop("history", None)
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

        Each product is cut to the partition's window as it is read, so only the hour's
        rows are held while the rest are read.
        """
        import harp

        operations = harp_window(*self.partition_window(it))
        datasets = []
        for native in self.iter_natives(input_files, temp_dir=temp_dir):
            try:
                granule = self.process_granule(native, operations=operations)
            except harp.NoDataError:
                logger.debug(f"{self.name}: {native} has no rows in the window")
                continue
            except Exception as exc:  # noqa: BLE001 - one bad product must not lose the hour
                logger.warning(f"{self.name}: {native} failed: {exc}")
                continue
            granule = self.restrict_to_window(granule, it)
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
