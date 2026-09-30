"""NOAA Climate Forecast System (CFS) seasonal forecasts held as local NetCDF.

CFSv2 output arrives as one NetCDF file per initialisation, downloaded out of band (from
NOMADS or the NCEI archive) onto local disk. This provider is the second half of that
pipeline: it finds those files, lines their forecast steps up and appends each
initialisation to an icechunk store.

Set ``PLANETARY_DATASETS_DATA_DIR`` (the files are looked for in ``<data_dir>/cfs``) or
pass ``source_dir=`` explicitly. No credentials are needed.

Why the padding
---------------
CFS runs are truncated at different lead times, so the files do not share a ``step``
length and cannot be appended to one store as they are. :func:`pad_to_steps` extends the
short ones with NaN steps up to a common length. That length is fixed for the life of the
store: pass ``max_steps=`` so it does not drift as new files arrive, because a store
written at one length cannot take an append at another. Left unset, it is the longest
``step`` found in the source directory, which is only safe for a one-shot ingest.

This replaces ``cfs_to_zarr.py``, which used ``icechunk.StorageConfig`` and
``icechunk.IcechunkStore`` - an API that no longer exists - and whose padding step never
ran because it tested ``ds.steps`` rather than ``ds.step``.
"""

from __future__ import annotations

import pathlib
import re
from typing import Dict, List, Sequence

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.providers._timestamps import NaiveUTCPartitions, to_naive_utc

#: ``YYYYMMDDHH`` or ``YYYYMMDD`` somewhere in the filename, as CFS names them.
FILENAME_TIMESTAMP = re.compile(r"(?<!\d)(\d{10}|\d{8})(?!\d)")

STEP_DIM = "step"


def timestamp_from_name(name: str) -> pd.Timestamp | None:
    """Pull the initialisation time out of a CFS filename.

    CFS filenames repeat the initialisation time (``flxf2011010100.01.2011010100.nc``);
    the first stamp is the one that matters. Returns None when there is none.
    """
    for token in FILENAME_TIMESTAMP.findall(name):
        fmt = "%Y%m%d%H" if len(token) == 10 else "%Y%m%d"
        try:
            return pd.Timestamp(pd.to_datetime(token, format=fmt))
        except ValueError:
            continue
    return None


def promote_to_float(ds: xr.Dataset) -> xr.Dataset:
    """Promote non-float data variables to float so NaN can mean "missing".

    Applied to every file, not only the short ones. Padding alone would promote the runs
    that need extending and leave the longest run - the one that usually creates the
    store - integer, so the store would be created with integer arrays and then take
    float appends full of NaN padding that an integer array cannot represent.
    """
    out = ds
    for name, var in ds.data_vars.items():
        if np.issubdtype(var.dtype, np.floating):
            continue
        if not (np.issubdtype(var.dtype, np.number) or var.dtype == bool):
            continue
        target = "float32" if var.dtype.itemsize <= 2 else "float64"
        out = out.assign({name: var.astype(target, keep_attrs=True)})
    return out


def pad_to_steps(ds: xr.Dataset, n_steps: int, step_dim: str = STEP_DIM) -> xr.Dataset:
    """Extend ``ds`` along ``step_dim`` to ``n_steps`` entries, filling with NaN.

    The new step coordinate values continue the existing spacing. Variables are put
    through :func:`promote_to_float` first, whether or not any padding is needed, so
    every file in a store ends up with the same dtypes.

    Raises:
        ValueError: If the dataset has no such dimension, or already has more steps than
            asked for - silently truncating a forecast would lose data.
    """
    if step_dim not in ds.dims:
        raise ValueError(f"dataset has no {step_dim!r} dimension to pad")
    current = int(ds.sizes[step_dim])
    if current == 0:
        # There is nothing to extrapolate the step spacing from, and a forecast with no
        # steps is a broken file rather than a short one.
        raise ValueError(f"dataset has an empty {step_dim!r} dimension")
    if current > n_steps:
        raise ValueError(
            f"dataset has {current} steps, more than the {n_steps} the store is laid out for"
        )
    ds = promote_to_float(ds)
    if current == n_steps:
        return ds

    if step_dim not in ds.coords:
        ds = ds.assign_coords({step_dim: np.arange(current)})
    steps = ds[step_dim].values
    if current >= 2:
        delta = steps[-1] - steps[-2]
    elif np.issubdtype(steps.dtype, np.timedelta64):
        delta = np.timedelta64(6, "h").astype(steps.dtype)
    else:
        delta = np.array(1, dtype=steps.dtype)
    extra = np.array([steps[-1] + delta * (i + 1) for i in range(n_steps - current)])
    logger.debug(f"padding {current} steps up to {n_steps}")
    return ds.reindex({step_dim: np.concatenate([steps, extra.astype(steps.dtype)])})


def max_step_count(paths: Sequence[str | pathlib.Path], step_dim: str = STEP_DIM) -> int:
    """Longest ``step`` dimension across a set of NetCDF files.

    Opening a NetCDF file does not read its data, so this only costs a header read each.
    """
    longest = 0
    for path in paths:
        with xr.open_dataset(path) as ds:
            longest = max(longest, int(ds.sizes.get(step_dim, 0)))
    return longest


class CFSSeasonalProvider(NaiveUTCPartitions, BaseProvider):
    """One CFS initialisation per partition, read from local NetCDF files.

    Args:
        source_dir: Directory searched (recursively) for the NetCDF files. Defaults to
            ``<data_dir>/cfs``.
        pattern: Glob applied within ``source_dir``.
        max_steps: Fixed length of the padded ``step`` dimension. Strongly recommended;
            see the module docstring.
        step_dim: Name of the forecast-step dimension.
        store_prefix: Override the store location.
    """

    name = "cfs_seasonal"
    append_dim = "time"

    def __init__(
        self,
        source_dir: str | pathlib.Path | None = None,
        pattern: str = "*.nc",
        max_steps: int | None = None,
        step_dim: str = STEP_DIM,
        store_prefix: str | None = None,
        config=None,
    ):
        super().__init__(config=config)
        self._source_dir = pathlib.Path(source_dir) if source_dir is not None else None
        self.pattern = pattern
        self.step_dim = step_dim
        self._max_steps = max_steps
        self.store_prefix = store_prefix or "bkr/noaa/cfs_seasonal.icechunk"
        self._index: Dict[pd.Timestamp, List[pathlib.Path]] | None = None

    @property
    def source_dir(self) -> pathlib.Path:
        return self._source_dir or (self.config.data_dir / "cfs")

    def discover(self, refresh: bool = False) -> Dict[pd.Timestamp, List[pathlib.Path]]:
        """Map initialisation time to the files holding it.

        Raises:
            FileNotFoundError: If the source directory does not exist. That is a
                misconfiguration, not an empty archive, so it is not reported as
                "nothing to do".
        """
        if self._index is not None and not refresh:
            return self._index
        if not self.source_dir.is_dir():
            raise FileNotFoundError(
                f"CFS source directory {self.source_dir} does not exist; set source_dir= "
                "or PLANETARY_DATASETS_DATA_DIR"
            )
        index: Dict[pd.Timestamp, List[pathlib.Path]] = {}
        for path in sorted(self.source_dir.rglob(self.pattern)):
            stamp = timestamp_from_name(path.name)
            if stamp is None:
                logger.warning(f"{self.name}: no initialisation time in {path.name}, ignoring")
                continue
            index.setdefault(stamp, []).append(path)
        logger.info(f"{self.name}: found {len(index)} initialisation(s) under {self.source_dir}")
        self._index = index
        return index

    def initialisation_times(self) -> pd.DatetimeIndex:
        """Every initialisation time present in the source directory, sorted."""
        return pd.DatetimeIndex(sorted(self.discover()))

    @property
    def max_steps(self) -> int:
        """Fixed length of the padded ``step`` dimension."""
        if self._max_steps is None:
            files = [p for paths in self.discover().values() for p in paths]
            self._max_steps = max_step_count(files, step_dim=self.step_dim)
            logger.info(
                f"{self.name}: max_steps was not set, using {self._max_steps} measured from "
                f"{len(files)} file(s). Pass max_steps= to keep the store stable."
            )
        return self._max_steps

    def fetch(self, it, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        it = to_naive_utc(it)
        paths = self.discover().get(it, [])
        if not paths:
            logger.info(f"{self.name}: no local CFS file for {it}")
            return []
        return [str(p) for p in paths]

    def process(
        self,
        input_files: List[str],
        it,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        it = to_naive_utc(it)
        target_steps = self.max_steps
        opened = [xr.open_dataset(path) for path in input_files]
        padded = [pad_to_steps(ds, target_steps, step_dim=self.step_dim) for ds in opened]
        ds = padded[0] if len(padded) == 1 else xr.merge(padded, compat="no_conflicts")

        if self.append_dim not in ds.coords:
            ds = ds.expand_dims({self.append_dim: pd.DatetimeIndex([it])})
        elif self.append_dim not in ds.dims:
            ds = ds.expand_dims(self.append_dim)

        stored = pd.DatetimeIndex(np.atleast_1d(ds[self.append_dim].values))
        if it not in stored:
            raise ValueError(
                f"{self.name}: files for {it} carry {list(stored)} on {self.append_dim!r}; "
                "the filename and the file disagree"
            )
        return ds.sel({self.append_dim: [it]})

    def run_all(self) -> int:
        """Ingest every initialisation found in the source directory.

        The bulk equivalent of the original script, minus the rewrite-everything
        behaviour: initialisations already in the store are skipped.
        """
        return self.run_range(self.initialisation_times())
