"""MetOp ASCAT scatterometer orbits from the EUMETSAT Data Store.

Same route as AMSU-A: EPS native products converted to netCDF by the EUMETSAT Data Tailor,
then shaped onto a ``time`` dimension. Replaces ``dags/assets/icechunky/ascat.py``, which
ran a Data Tailor chain over a fixed directory of zips and stopped there.
"""

from __future__ import annotations

import pathlib
from typing import List

import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.providers.polar._eumdac import EumdacProvider
from planetary_datasets.providers.polar._granule import process_eps_netcdf


class MetopAscatProvider(EumdacProvider):
    """ASCAT level 1 sigma-0 full-resolution orbits for the MetOp series."""

    name = "metop_ascat"
    append_dim = "time"
    store_prefix = "bkr/polar/metop_ascat.icechunk"
    collection_id = "EO:EUM:DAT:METOP:ASCSZF1B"
    obs_store = True
    epct_product = "ASCATL1SZR"
    window = pd.Timedelta(1, "D")

    #: Unlike AMSU-A there is no measured upper bound on an ASCAT orbit to pad to, so the
    #: length is taken from the store once it exists and from the partition before that.
    #: Set this to pin it instead.
    along_track_length: int | None = None

    def open_tailored(self, path: str) -> xr.Dataset:
        """Open one tailored netCDF orbit and shape it, leaving its length alone."""
        coder = xr.coders.CFDatetimeCoder(use_cftime=True)
        with xr.open_dataset(path, decode_times=coder) as raw:
            ds = raw.load()
        return process_eps_netcdf(ds)

    def target_length(self, orbits: List[xr.Dataset]) -> int:
        """Along-track length every orbit is padded to before being concatenated.

        Orbits differ in length, and a store's dimensions are fixed by its first write, so
        the target has to be whatever the store already uses. Falling back to the longest
        orbit in the partition is only for the very first write.
        """
        if self.along_track_length is not None:
            return self.along_track_length
        stored = self.stored_sizes().get("y")
        if stored is not None:
            return int(stored)
        return max(orbit.sizes.get("y", 0) for orbit in orbits)

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Concatenate the staged, tailored orbits on time."""
        orbits = []
        for path in input_files:
            try:
                orbits.append(self.open_tailored(path))
            except Exception as exc:  # noqa: BLE001 - one bad orbit must not lose the day
                logger.warning(f"{self.name}: {path} failed: {exc}")
        if not orbits:
            return self.concat_granules([])

        target = self.target_length(orbits)
        fitted = (self.fit_to_shape(orbit, {"y": target}) for orbit in orbits)
        padded = [orbit for orbit in fitted if orbit is not None]
        return self.restrict_to_window(self.concat_granules(padded), it)
