"""Virtual concatenation of Himawari-9 AHI full-disk tiles.

JAXA publishes each AHI full disk as 88 north-to-south strips per band
(``OR_HFD-<res>-B<nn>-M1C<bb>-T0<tt>_GH9_*.nc``). The strips are not all the same height,
so a single :func:`xarray.combine_by_coords` over all of them fails. Concatenating in
three bands — dense centre, the shoulders either side, and the sparse polar edges — keeps
each group internally uniform, which is what the virtualizarr references need.

These helpers were extracted from the exploratory ``list_all_aws_paths.py`` script; the
tile-index boundaries below are empirical, measured from the 0.05 and 0.10 degree products.
"""

from __future__ import annotations

from typing import Sequence

import pandas as pd
import xarray as xr

#: Tile index ranges for each concat group, as ``(centre, shoulders, edges)``. The 0.10
#: degree product is missing one polar tile, hence the different edge bounds.
TILE_GROUPS_005 = {"centre": (14, 74), "top": (6, 14), "bottom": (74, 82), "top_edge": (0, 6), "bottom_edge": (82, None)}
TILE_GROUPS_010 = {"centre": (14, 74), "top": (6, 14), "bottom": (74, 82), "top_edge": (1, 6), "bottom_edge": (83, None)}

_CONCAT_KWARGS = {"coords": "minimal", "compat": "override"}


def open_h9_tile(filepath: str, loadable_variables: Sequence[str] = ("x", "y")) -> xr.Dataset:
    """Open one AHI tile as a virtual dataset with time, band and wavelength as coordinates.

    The AHI files carry the observation time, band number and central wavelength only as
    global attributes, so they are promoted to coordinates here; without that
    :func:`xarray.combine_by_coords` has nothing to align on. Satellite metadata attributes
    are kept and everything else dropped, since conflicting per-tile attributes block the
    concatenation.
    """
    from virtualizarr import open_virtual_dataset

    ds = open_virtual_dataset(filepath, loadable_variables=list(loadable_variables))
    ds["time"] = pd.to_datetime(ds.attrs["start_date_time"], format="%Y%j%H%M%S")
    ds["band"] = int(ds.attrs["channel_id"])
    ds["central_wavelength"] = [float(ds.attrs["central_wavelength"])]
    ds = ds.set_coords(["time", "band", "central_wavelength", "fixedgrid_projection"])
    ds.attrs = {
        key: value
        for key, value in ds.attrs.items()
        if "satellite" in key and "satellite_id" not in key
    }
    return ds


def hierarchical_concat_h9(
    tiles: Sequence[xr.Dataset],
    resolution: str = "005",
) -> tuple[xr.Dataset, xr.Dataset, xr.Dataset]:
    """Concatenate AHI tiles into centre, shoulder and edge datasets.

    Args:
        tiles: Tiles for one band, in ascending tile-index order.
        resolution: ``"005"`` or ``"010"``, selecting the tile boundaries to use.

    Returns:
        ``(centre, shoulders, edges)``, ordered from the middle of the disk outwards.

    Raises:
        ValueError: If ``resolution`` is unknown, or if the tile list is short enough that
            a group would come out empty. Slicing would silently produce an empty dataset
            there, and the resulting mosaic would be wrong rather than obviously broken.
    """
    try:
        groups = {"005": TILE_GROUPS_005, "010": TILE_GROUPS_010}[resolution]
    except KeyError:
        raise ValueError(f"unknown Himawari tile resolution {resolution!r}") from None

    required = max(stop if stop is not None else start + 1 for start, stop in groups.values())
    if len(tiles) < required:
        raise ValueError(
            f"need at least {required} tiles for the {resolution} layout, got {len(tiles)}"
        )

    def combine(name: str) -> xr.Dataset:
        start, stop = groups[name]
        group = list(tiles[start:stop])
        if not group:
            raise ValueError(f"tile group {name!r} is empty for {len(tiles)} tiles")
        return xr.combine_by_coords(group, **_CONCAT_KWARGS)

    centre = combine("centre")
    shoulders = xr.concat(
        [combine("top"), combine("bottom")], dim="y", combine_attrs="override", **_CONCAT_KWARGS
    )
    edges = xr.concat(
        [combine("top_edge"), combine("bottom_edge")],
        dim="y",
        combine_attrs="override",
        **_CONCAT_KWARGS,
    )
    return centre, shoulders, edges


__all__ = ["TILE_GROUPS_005", "TILE_GROUPS_010", "hierarchical_concat_h9", "open_h9_tile"]
