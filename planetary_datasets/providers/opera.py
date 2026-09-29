"""EUMETNET OPERA pan-European radar composites.

The processing half of the OPERA pipeline. The download half,
:mod:`planetary_datasets.providers.opera_download`, runs in the ``docker/opera-radar``
image (its ``earth2studio`` dependency cannot be installed here) and stages each hour as
one netCDF file; the providers below read those files and append them to the icechunk
stores the original scripts built:

``OPERARainfallProvider``
    Rain rate and one-hour accumulation, four 15-minute frames per hour, on the 2 km grid.
``OPERAReflectivityProvider``
    Composite reflectivity, twelve 5-minute frames per hour, on the 1 km CIRRUS-era grid.

Staged files live under ``OPERA_ARCHIVE_DIR`` (default ``<data_dir>/opera``), in
``<product>/YYYY/MM/DD/``. An hour of reflectivity is tens of MB even compressed, so the
Dagster assets delete a staged hour once it is safely in the store; see
:meth:`OPERAProvider.discard_staged`.

The stores only accept appends in time order, and both existing ones were written out of
order by the original scripts, so each has gaps before its latest time that can no longer
be filled. :meth:`~planetary_datasets.base.BaseProvider.appendable` identifies those hours
so they are neither downloaded nor left staged.
"""

from __future__ import annotations

import pathlib
from typing import List

import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.providers._timestamps import to_naive_utc
from planetary_datasets.providers.opera_download import PRODUCTS, Product
from planetary_datasets.providers.radar import LocalArchiveRadarProvider

#: Environment variable naming the staging root shared by every OPERA product.
ARCHIVE_ENV = "OPERA_ARCHIVE_DIR"
ARCHIVE_SUBDIR = "opera"


class OPERAProvider(LocalArchiveRadarProvider):
    """Shared reading of the staged hourly OPERA files.

    Subclasses set :attr:`product` to a key of
    :data:`~planetary_datasets.providers.opera_download.PRODUCTS`.
    """

    product: str
    archive_env = ARCHIVE_ENV
    archive_subdir = ARCHIVE_SUBDIR

    @property
    def spec(self) -> Product:
        """The product definition shared with the downloader."""
        return PRODUCTS[self.product]

    @property
    def file_pattern(self) -> str:  # type: ignore[override]
        """One staged file per hour, stamped with the hour it starts."""
        return f"opera_{self.product}_{{stamp}}.nc"

    @property
    def archive_root(self) -> pathlib.Path:
        """The staging root the downloader image is given, shared by every product."""
        return super().archive_dir

    @property
    def archive_dir(self) -> pathlib.Path:
        """Directory this product's hours are staged in."""
        return self.archive_root / self.product

    def frame_times(self, it: pd.Timestamp) -> pd.DatetimeIndex:
        """The frames a complete hour contains."""
        start, _ = self.window(it)
        return pd.DatetimeIndex(self.spec.frame_times(start.to_pydatetime()))

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Read one staged hour and check it is the hour and product it claims to be."""
        (path,) = input_files
        with xr.open_dataset(path, engine="h5netcdf") as staged:
            ds = staged.load()
        ds = ds.drop_encoding()

        expected = set(self.spec.variables)
        if set(ds.data_vars) != expected:
            raise ValueError(
                f"{self.name}: {pathlib.Path(path).name} holds {sorted(ds.data_vars)}, "
                f"expected {sorted(expected)}"
            )

        times = pd.DatetimeIndex(ds["time"].values)
        start, end = self.window(it)
        if not ((times >= start) & (times < end)).all():
            raise ValueError(f"{self.name}: {pathlib.Path(path).name} has frames outside {start}")
        if not self.allow_partial:
            absent = self.frame_times(it).difference(times)
            if len(absent):
                raise ValueError(
                    f"{self.name}: {pathlib.Path(path).name} is missing frames "
                    f"{[t.strftime('%H:%M') for t in absent]}"
                )
        # sortby copies the whole hour even when it is already in order, which it
        # normally is.
        return ds if times.is_monotonic_increasing else ds.sortby("time")

    def has_staged(self, it: pd.Timestamp) -> bool:
        """True when this hour's file has been staged."""
        return bool(self.candidates(to_naive_utc(it)))

    def discard_staged(self, it: pd.Timestamp, settled: bool = False) -> List[pathlib.Path]:
        """Delete this hour's staged file once it has nowhere left to go.

        That is once the store can no longer accept it: it is stored, or the store has
        moved past it. ``settled=True`` says the caller already knows, having just
        written it or found the store closed to it, so the store is not read again.
        Otherwise a staged file that failed to write stays behind for inspection and the
        next attempt.
        """
        it = to_naive_utc(it)
        if not settled and self.appendable(it):
            return []
        removed = []
        for path in self.candidates(it):
            path.unlink(missing_ok=True)
            removed.append(path)
        if removed:
            logger.debug(f"{self.name}: removed staged {[p.name for p in removed]}")
        return removed


class OPERARainfallProvider(OPERAProvider):
    """OPERA rain rate and one-hour accumulation, four 15-minute frames per hour."""

    name = "opera_rainfall"
    product = "rainfall"
    store_prefix = "bkr/precipradar/opera_rainfall.icechunk"


class OPERAReflectivityProvider(OPERAProvider):
    """OPERA composite reflectivity, twelve 5-minute frames per hour."""

    name = "opera_dbz"
    product = "dbz"
    store_prefix = "bkr/precipradar/opera_dbz.icechunk"


PROVIDERS: tuple[type[OPERAProvider], ...] = (OPERARainfallProvider, OPERAReflectivityProvider)

__all__ = [
    "OPERAProvider",
    "OPERARainfallProvider",
    "OPERAReflectivityProvider",
    "PROVIDERS",
]
