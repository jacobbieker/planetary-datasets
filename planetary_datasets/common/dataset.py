"""Dataset shaping helpers shared by the providers.

``reduce_precision`` was duplicated in seven scripts; the coordinate helpers were copied
from ``ocf_data_sampler/load/utils.py`` into five more.
"""

from __future__ import annotations

import numpy as np
import xarray as xr
from loguru import logger

# Variables matching these substrings do not carry meaningful precision beyond float16 and
# dominate store size, so they are downcast before writing.
DEFAULT_LOW_PRECISION_HINTS = ("wind", "fraction", "temperature")


def reduce_precision(
    ds: xr.Dataset,
    hints: tuple[str, ...] = DEFAULT_LOW_PRECISION_HINTS,
    dtype: str = "float16",
) -> xr.Dataset:
    """Downcast variables whose names match ``hints`` to ``dtype``.

    Returns a new dataset; the input is not modified.
    """
    out = ds.copy()
    for var in out.data_vars:
        if any(hint in str(var) for hint in hints):
            out[var] = out[var].astype(dtype)
    return out


def lon_to_m180(ds: xr.Dataset, lon_name: str = "longitude") -> xr.Dataset:
    """Convert a 0-360 longitude coordinate to -180-180 and sort ascending."""
    if lon_name not in ds.coords:
        return ds
    ds = ds.assign_coords({lon_name: ((ds[lon_name] + 180) % 360) - 180})
    return ds.sortby(lon_name)


def make_spatial_coords_increasing(
    ds: xr.Dataset,
    lat_name: str = "latitude",
    lon_name: str = "longitude",
) -> xr.Dataset:
    """Sort latitude and longitude into ascending order if present."""
    for name in (lat_name, lon_name):
        if name in ds.coords and ds[name].ndim == 1 and ds[name].size > 1:
            values = ds[name].values
            if values[0] > values[-1]:
                ds = ds.sortby(name)
    return ds


def make_lat_lon_coords_consistent(
    ds: xr.Dataset,
    lat_name: str = "latitude",
    lon_name: str = "longitude",
) -> xr.Dataset:
    """Normalise spatial coordinates so datasets from different sources can be concatenated.

    Longitudes are put on -180-180 and both axes sorted ascending. Appending to an existing
    icechunk store fails if the coordinate order differs, which is what this prevents.
    """
    ds = lon_to_m180(ds, lon_name=lon_name)
    return make_spatial_coords_increasing(ds, lat_name=lat_name, lon_name=lon_name)


def sort_vertical_coords(
    ds: xr.Dataset,
    level_name: str = "level",
    height_name: str = "height",
) -> xr.Dataset:
    """Sort pressure levels descending and heights ascending, when those dims exist."""
    if level_name in ds.coords:
        ds = ds.sortby(level_name, ascending=False)
    if height_name in ds.coords:
        ds = ds.sortby(height_name, ascending=True)
    return ds


def rename_vars_by_long_name(ds: xr.Dataset, suffix: str = "") -> xr.Dataset:
    """Rename data variables to their ``long_name`` attribute, slugified.

    GRIB shortnames collide between level types, so several providers rename by long name
    and append a suffix such as ``_at_surface`` to keep them distinct.
    """
    renames: dict[str, str] = {}
    taken: set[str] = set()
    for var in ds.data_vars:
        long_name = str(ds[var].attrs.get("long_name", var))
        slug = long_name.lower().replace(" ", "_").replace("(", "").replace(")", "")
        target = f"{slug}{suffix}"
        if target in taken:
            # Two variables can share a long_name (GRIB does this across level types).
            # Renaming both to it would raise, so keep the original name for the later one.
            logger.debug(f"long_name {target!r} already used, keeping {var!r} unchanged")
            target = str(var)
            if target in taken:
                continue
        taken.add(target)
        renames[str(var)] = target
    return ds.rename(renames)


def coords_match(a: xr.Dataset, b: xr.Dataset, coords: tuple[str, ...]) -> tuple[bool, str | None]:
    """Check that shared coordinates are identical between two datasets.

    Returns ``(True, None)`` when they match, otherwise ``(False, name)`` naming the first
    coordinate that differs.
    """
    for coord in coords:
        # Check coords, not just dims: geostationary grids carry latitude/longitude as
        # 2-D non-dimension coordinates, and keying on `dims` meant the alignment guard
        # never fired for them.
        if coord in a.coords and coord in b.coords:
            if a[coord].shape != b[coord].shape or not np.array_equal(a[coord].values, b[coord].values):
                return False, coord
    return True, None
