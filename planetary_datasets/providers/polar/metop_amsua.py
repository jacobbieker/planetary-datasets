"""MetOp AMSU-A microwave sounder orbits from the EUMETSAT Data Store.

AMSU-A level 1 ships as EPS native products with no reader in this stack, so each orbit is
converted to netCDF with the EUMETSAT Data Tailor before being shaped and appended.

Replaces ``dags/assets/icechunky/amsua_to_icechunk.py`` (which globbed ``/data/polar/AMSUA``
and wrote to a store chosen by whichever ``storage =`` line was last uncommented) and the
Data Tailor invocation at the top of ``dags/assets/icechunky/ascat.py``.
"""

from __future__ import annotations

import pathlib
from typing import List

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.providers.polar._eumdac import EumdacProvider
from planetary_datasets.providers.polar._granule import process_eps_netcdf

#: Longest AMSU-A orbit seen in the archive; shorter orbits are padded up to it.
ALONG_TRACK_LENGTH = 1100


class MetopAmsuaProvider(EumdacProvider):
    """AMSU-A level 1 brightness temperatures for the MetOp series."""

    name = "metop_amsua"
    append_dim = "time"
    store_prefix = "bkr/polar/metop_amsua.icechunk"
    collection_id = "EO:EUM:DAT:METOP:AMSUL1"
    epct_product = "AMSAL1"
    window = pd.Timedelta("1D")
    along_track_length = ALONG_TRACK_LENGTH

    def open_tailored(self, path: str) -> xr.Dataset | None:
        """Open one tailored netCDF orbit and shape it for the store.

        Returns None for an orbit longer than :attr:`along_track_length`, which cannot be
        concatenated with the rest. That length is the longest seen in the archive, not a
        guarantee, so an over-long orbit is dropped rather than losing the whole day.
        """
        # The record time variables are outside the range numpy datetimes cover at the
        # netCDF file's stated units, so they decode to cftime and are cast afterwards.
        coder = xr.coders.CFDatetimeCoder(use_cftime=True)
        with xr.open_dataset(path, decode_times=coder) as raw:
            ds = raw.load()
        ds = process_eps_netcdf(ds, pad_along_track=self.along_track_length)
        if self.along_track_length is not None and ds.sizes.get("y", 0) > self.along_track_length:
            logger.warning(
                f"{self.name}: orbit has y={ds.sizes['y']} > {self.along_track_length}, skipping"
            )
            return None
        for var in ds.data_vars:
            if str(var).startswith("channel"):
                ds[var] = ds[var].astype(np.float16)
        return ds

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Concatenate the staged, tailored orbits on time."""
        datasets = []
        for path in input_files:
            try:
                orbit = self.open_tailored(path)
            except Exception as exc:  # noqa: BLE001 - one bad orbit must not lose the day
                logger.warning(f"{self.name}: {path} failed: {exc}")
                continue
            if orbit is not None:
                datasets.append(orbit)
        return self.restrict_to_window(self.concat_granules(datasets), it)
