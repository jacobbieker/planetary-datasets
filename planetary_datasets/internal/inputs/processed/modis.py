import xarray as xr
import pystac_client
import planetary_computer
import datetime as dt
import fsspec
from typing import Union, Optional


def get_modis(
    start_datetime: dt.datetime,
    end_datetime: dt.datetime,
    bands: Optional[list[str]] = None,
) -> Union[list[xr.Dataset], None]:
    """
    Get the GOES images for the given time period

    Args:
        start_datetime: datetime to start getting images from
        end_datetime: End datetime to get images from
        bands_to_use: Which bands to get, if None, all bands are included
    """
    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
    )
    search = catalog.search(
        collections=["modis-09Q1-061"] if bands == "250m" else ["modis-09A1-061"],
        datetime=[start_datetime, end_datetime],
    )

    if bands == "all":
        catalog_values = [
            "sur_refl_b01",
            "sur_refl_b02",
            "sur_refl_b03",
            "sur_refl_b04",
            "sur_refl_b05",
            "sur_refl_b06",
            "sur_refl_b07",
        ]
    if bands == "250m":
        catalog_values = ["sur_refl_b01", "sur_refl_b02",]

    dses = []
    for i, item in enumerate(search.items()):
        datasets = [
            xr.open_dataset(fsspec.open(item.assets[k].href).open()) for k in catalog_values
        ]
        ds = xr.combine_by_coords(datasets, join="exact", combine_attrs="drop_conflicts")
        dses.append(ds)
    return dses


if __name__ == "__main__":
    pass
