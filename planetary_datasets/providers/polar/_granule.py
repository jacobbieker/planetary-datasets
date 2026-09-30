"""Shared plumbing for polar-orbiting instruments.

Polar instruments do not produce one field per synoptic hour; they produce a stream of
granules (ATMS SDRs, EPS orbit products, GOME PDUs) whose timestamps fall wherever the
spacecraft happened to be. That breaks two assumptions the generic
:class:`~planetary_datasets.base.BaseProvider` makes:

* a partition timestamp is never itself a stored timestamp, so the default
  "is this timestep already stored?" check would always say no and append duplicates.
  :class:`GranuleProvider` replaces it with a check over the partition's time *window*.
* granules vary in size between orbits, so they have to be padded to a fixed shape before
  they can be concatenated and appended to a store.

Everything here is import-cheap; ``satpy``, ``harp``, ``pyresample`` and ``eumdac`` are
imported inside the functions that use them.
"""

from __future__ import annotations

import datetime as dt
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common import existing_times
from planetary_datasets.common.store import STORE_READ_ERRORS, has_committed_data

#: Platform name written into rows that only exist because a granule was padded. A real
#: MetOp platform is never called Z, so padded rows stay identifiable after the fact.
PAD_PLATFORM = "MetOp-Z"

#: Fill value for the record time variables of padded rows.
PAD_TIME = np.datetime64("2000-01-01T00:00:00")


def serialize_attrs(attrs: Mapping[str, Any]) -> dict[str, Any]:
    """Flatten dataset attributes into values a zarr store can hold.

    satpy hands back ``datetime`` objects, numpy bools and whole
    :class:`pyresample.geometry.AreaDefinition` instances in ``.attrs``; none of those
    survive being written to zarr. Areas are dumped to their YAML representation so the
    geometry is still recoverable, everything unrecognised becomes its ``str``.
    """
    try:
        import pyresample
        import yaml
    except ImportError:  # pragma: no cover - satpy always brings both in
        pyresample = None
        yaml = None

    out: dict[str, Any] = {}
    for key, value in attrs.items():
        if isinstance(value, dt.datetime):
            out[key] = value.isoformat()
        elif isinstance(value, (bool, np.bool_)):
            out[key] = str(value)
        elif pyresample is not None and isinstance(value, pyresample.geometry.AreaDefinition):
            out[key] = yaml.load(value.dump(), Loader=yaml.SafeLoader)
        elif isinstance(value, Mapping):
            out[key] = serialize_attrs(value)
        else:
            out[key] = str(value)
    return out


def serialize_dataset_attrs(ds: xr.Dataset, drop: Sequence[str] = ()) -> xr.Dataset:
    """Apply :func:`serialize_attrs` to a dataset and each of its variables."""
    for key in drop:
        ds.attrs.pop(key, None)
    ds.attrs = serialize_attrs(ds.attrs)
    for var in ds.data_vars:
        ds[var].attrs = serialize_attrs(ds[var].attrs)
    return ds


def mid_time(start: Any, end: Any, round_to: str | None = "s") -> pd.Timestamp:
    """Midpoint between two timestamps, used as the nominal time of a granule.

    Rounded to whole seconds by default: the store encodes ``time`` as seconds since the
    epoch, and rounding here keeps the value that comes back out of the store identical to
    the one the dedupe check looked for. Granules are tens of seconds apart, so rounding
    cannot merge two of them.
    """
    start = pd.Timestamp(start)
    end = pd.Timestamp(end)
    middle = start + (end - start) / 2
    return middle.round(round_to) if round_to else middle


def pad_dim(
    ds: xr.Dataset,
    dim: str,
    size: int,
    constant_values: Any = np.nan,
) -> xr.Dataset:
    """Right-pad ``dim`` up to ``size``. A dimension already at or past ``size`` is left alone."""
    current = ds.sizes.get(dim, 0)
    if current == 0 or current >= size:
        return ds
    return ds.pad({dim: (0, size - current)}, mode="constant", constant_values=constant_values)


def pad_values(ds: xr.Dataset, overrides: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Per-variable pad values that preserve each variable's dtype.

    Padding an integer flag or a datetime with NaN silently promotes it to float. Whether
    a granule needed padding at all depends on its length, so a NaN default makes the
    store's dtypes depend on which granule happened to be written first. Choosing the pad
    value from the dtype keeps that from happening.
    """
    overrides = overrides or {}
    values: dict[str, Any] = {}
    for name, var in ds.data_vars.items():
        key = str(name)
        if key in overrides:
            values[name] = overrides[key]
            continue
        kind = var.dtype.kind
        if kind == "b":
            values[name] = False
        elif kind in "iu":
            values[name] = -1
        elif kind == "M":
            values[name] = PAD_TIME
        elif kind in "US":
            values[name] = ""
        else:
            values[name] = np.nan
    return values


def eps_pad_values(ds: xr.Dataset) -> dict[str, Any]:
    """Pad values for an EPS product, with the padded rows marked by platform name."""
    return pad_values(ds, {"platform_name": PAD_PLATFORM})


def process_eps_netcdf(ds: xr.Dataset, pad_along_track: int | None = None) -> xr.Dataset:
    """Shape a Data Tailor ``netcdf4_satellite`` EPS product for appending to a store.

    The tailored products (AMSU-A, ASCAT and friends) carry one orbit each, with 2D
    ``lat``/``lon`` coordinates and the sensing window only in the global attributes. This
    lifts the sensing window onto a ``time`` dimension, renames the swath dims to the
    ``y``/``x`` used everywhere else, and optionally pads the along-track dimension so
    orbits of different lengths concatenate.
    """
    start = pd.Timestamp(ds.attrs["start_sensing_data_time"]).tz_localize(None)
    end = pd.Timestamp(ds.attrs["end_sensing_data_time"]).tz_localize(None)

    renames = {
        name: new
        for name, new in (("lat", "latitude"), ("lon", "longitude"))
        if name in ds.variables
    }
    if renames:
        ds = ds.rename(renames)
        ds = ds.reset_coords([v for v in renames.values() if v in ds.coords])

    ds = ds.expand_dims("time")
    ds["time"] = [mid_time(start, end)]
    ds["start_time"] = xr.DataArray([start], dims=["time"]).astype("datetime64[ns]")
    ds["end_time"] = xr.DataArray([end], dims=["time"]).astype("datetime64[ns]")
    for name in ("record_start_time", "record_stop_time"):
        if name in ds.data_vars:
            ds[name] = ds[name].astype("datetime64[ms]")
    # ``source`` reads e.g. "MetOp-C AMSUA"; the first seven characters are the platform.
    platform = xr.DataArray([str(ds.attrs.get("source", ""))], dims=["time"])
    ds["platform_name"] = platform.astype("U7")

    if pad_along_track is not None:
        ds = pad_dim(ds, "along_track", pad_along_track, constant_values=eps_pad_values(ds))

    swath_renames = {
        name: new
        for name, new in (("along_track", "y"), ("across_track", "x"))
        if name in ds.dims
    }
    if swath_renames:
        ds = ds.rename(swath_renames)
    return ds


class GranuleProvider(BaseProvider):
    """A provider whose stored timestamps fall anywhere inside the partition window.

    A partition counts as ingested when the store holds a timestamp inside its window.
    That only works if every timestamp a partition writes is inside its own window, which
    granules do not respect on their own: a search returns every product *overlapping* the
    window, and an orbit straddling a boundary would otherwise be written by both
    neighbouring partitions and make each of them look done. :meth:`restrict_to_window`
    enforces the invariant, and every subclass applies it before writing.

    Attributes:
        window: Width of one partition. Must match the Dagster partition definition, since
            it is what decides whether a partition has already been ingested.
    """

    window: pd.Timedelta = pd.Timedelta(1, "h")

    def partition_window(self, it: pd.Timestamp) -> tuple[pd.Timestamp, pd.Timestamp]:
        """Half-open ``[start, end)`` interval covered by the partition starting at ``it``."""
        start = pd.Timestamp(it)
        if start.tzinfo is not None:
            start = start.tz_localize(None)
        return start, start + self.window

    def missing_timesteps(self, desired: pd.DatetimeIndex) -> list[pd.Timestamp]:
        """Return the partitions with no stored granule inside their time window.

        A store that holds data but cannot be read raises out of ``existing_times`` rather
        than being reported as empty; that must not be caught here, or a re-run would
        overwrite the archive.
        """
        times = existing_times(self.get_icechunk_repo(), append_dim=self.append_dim)
        if times.size == 0:
            return list(desired)

        missing = []
        for it in desired:
            start, end = self.partition_window(it)
            covered = (times >= np.datetime64(start)) & (times < np.datetime64(end))
            if not bool(np.any(covered)):
                missing.append(it)
        return missing

    def stored_sizes(self) -> dict[str, int]:
        """Dimension sizes already in the store, or ``{}`` when it is empty.

        Used to pad new granules to the shape the store was created with, for instruments
        whose granule length is not known ahead of time.
        """
        repo = self.get_icechunk_repo()
        try:
            ds = xr.open_zarr(
                repo.readonly_session("main").store, consolidated=False, decode_timedelta=True
            )
        except STORE_READ_ERRORS as exc:
            if has_committed_data(repo):
                raise
            logger.debug(f"{self.name}: store is empty ({type(exc).__name__})")
            return {}
        return dict(ds.sizes)

    def concat_granules(self, datasets: Sequence[xr.Dataset]) -> xr.Dataset:
        """Concatenate granules along the append dimension, oldest first.

        An empty input gives an empty dataset rather than an error: a partition whose
        products all failed to read, or all belong to a neighbour, is nothing to do.
        """
        if not datasets:
            logger.warning(f"{self.name}: no granules to concatenate")
            return xr.Dataset()
        if len(datasets) == 1:
            return datasets[0].sortby(self.append_dim)
        return xr.concat(list(datasets), dim=self.append_dim).sortby(self.append_dim)

    def restrict_to_window(self, ds: xr.Dataset, it: pd.Timestamp) -> xr.Dataset:
        """Drop rows whose timestamp falls outside the partition's own window.

        Keeps each timestamp the property of exactly one partition, so a boundary-crossing
        orbit is stored once and neither partition is wrongly reported as done.
        """
        if self.append_dim not in ds.coords or ds.sizes.get(self.append_dim, 0) == 0:
            return ds
        start, end = self.partition_window(it)
        times = ds[self.append_dim].values
        keep = (times >= np.datetime64(start)) & (times < np.datetime64(end))
        dropped = int((~keep).sum())
        if not dropped:
            return ds
        logger.info(
            f"{self.name}: dropping {dropped} row(s) outside {start} - {end}; "
            "the neighbouring partition owns them"
        )
        return ds.isel({self.append_dim: keep})

    def fit_to_shape(self, ds: xr.Dataset, shape: Mapping[str, int]) -> xr.Dataset | None:
        """Pad a granule up to ``shape``, or return None when it is larger than that.

        Dropping an oversized granule loses one granule; letting it through would make
        :meth:`concat_granules` raise and lose the whole partition.
        """
        for dim, size in shape.items():
            if ds.sizes.get(dim, 0) > size:
                logger.warning(
                    f"{self.name}: granule has {dim}={ds.sizes[dim]} > {size}, skipping it"
                )
                return None
        for dim, size in shape.items():
            ds = pad_dim(ds, dim, size, constant_values=pad_values(ds))
        return ds

    def write_to_icechunk(self, repo, processed: xr.Dataset) -> bool:
        """Write a processed partition, treating an empty one as nothing to do."""
        if processed.sizes.get(self.append_dim, 0) == 0:
            logger.info(f"{self.name}: nothing inside the partition window, skipping write")
            return False
        return super().write_to_icechunk(repo, processed)
