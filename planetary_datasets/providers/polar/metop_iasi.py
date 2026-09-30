"""MetOp IASI level 1C spectra from the EUMETSAT Data Store.

IASI native products are read with ``harp``, which understands the EPS format and hands
back one row per sounding.

Replaces ``dags/assets/icechunky/iasi.py``. That file carried a live AWS access key and
secret inline; the key has been removed here and stores are resolved through
:class:`~planetary_datasets.config.Config`.

It also padded every product out to a whole number of chunks with rows carrying a fixed
sentinel time. That is dropped: the sentinel repeats across partitions, so the store's
dedupe drops the padding of every partition after the first and the multiple it exists to
maintain is lost anyway — while leaving fake soundings and a non-monotonic ``time`` in the
store. A partial final chunk is the cheaper problem.
"""

from __future__ import annotations

import os
import pathlib
import tempfile
from typing import List

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.providers.polar._eumdac import EumdacProvider

#: EPS filename infix to platform name.
SPACECRAFT_BY_INFIX = {
    "_M01_": "Metop-B",
    "_M02_": "Metop-A",
    "_M03_": "Metop-C",
}

#: Soundings per chunk along ``time``.
CHUNK_SOUNDINGS = 9180


def platform_from_filename(filename: str) -> str:
    """Platform name encoded in an EPS product filename, or ``"unknown"``."""
    name = pathlib.Path(filename).name
    for infix, platform in SPACECRAFT_BY_INFIX.items():
        if infix in name:
            return platform
    logger.warning(f"could not identify the platform of {name}")
    return "unknown"


def harp_to_dataset(filename: str) -> xr.Dataset:
    """Import an EPS product with harp and return it as an in-memory dataset.

    harp has no direct xarray export, so the product goes out to a temporary netCDF file
    and is read straight back in.
    """
    import harp

    product = harp.import_product(filename)
    handle, tmp_path = tempfile.mkstemp(suffix=".nc")
    os.close(handle)
    try:
        harp.export_product(product, tmp_path)
        with xr.open_dataset(tmp_path) as raw:
            return raw.load()
    finally:
        pathlib.Path(tmp_path).unlink(missing_ok=True)


class MetopIasiProvider(EumdacProvider):
    """IASI level 1C spectra for the MetOp series."""

    name = "metop_iasi"
    append_dim = "time"
    store_prefix = "bkr/polar/metop_iasi.icechunk"
    collection_id = "EO:EUM:DAT:METOP:IASIL1C-ALL"
    window = pd.Timedelta(2, "h")
    chunk_soundings = CHUNK_SOUNDINGS

    def process_granule(self, filename: str) -> xr.Dataset:
        """Read one IASI native product into a dataset indexed by sounding time."""
        ds = harp_to_dataset(filename)
        ds["platform_name"] = xr.DataArray(
            [platform_from_filename(filename)] * ds.sizes["time"], dims=["time"]
        )
        times = np.asarray(ds["datetime"].values)
        ds = ds.drop_vars(["datetime", "orbit_index"], errors="ignore")
        return ds.assign_coords(time=times)

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Unpack, read and concatenate every IASI product in the partition."""
        datasets = []
        for native in self.iter_natives(input_files, temp_dir=temp_dir):
            try:
                datasets.append(self.process_granule(native))
            except Exception as exc:  # noqa: BLE001 - one bad product must not lose the window
                logger.warning(f"{self.name}: {native} failed: {exc}")

        ds = self.restrict_to_window(self.concat_granules(datasets), it)
        if ds.sizes.get("time", 0) == 0:
            return ds

        chunks = {"time": self.chunk_soundings}
        if "spectral" in ds.dims:
            chunks["spectral"] = -1
        return ds.chunk(chunks)
