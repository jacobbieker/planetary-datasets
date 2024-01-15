import xarray as xr
import pystac_client
import planetary_computer
import datetime as dt
import fsspec
from typing import Union, Optional


def get_landsat(
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
        full_resolution: Whether to get the full resolution images,
            which are variable in band, or the fixed resolution images, which are 2km
    """
    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
    )
    search = catalog.search(
        collections=["sentinel-3-olci-lfr-l2-netcdf"],
        datetime=[start_datetime, end_datetime],
        query={
            # Avoid scan line issues in the landsat-7 data
            "platform": {"in": ["landsat-8", "landsat-9", "landsat-4", "landsat-5"]},
        },
    )

    if bands == "all":
        catalog_values = [
            "nir08",
            "red",
            "green",
            "blue",
            "drad",
            "emis",
            "trad",
            "urad",
            "atran",
            "cdist",
            "swir16",
            "swir22",
            "qa_pixel",
            "lwir11",
        ]
    if bands == "rgb":
        catalog_values = ["red", "green", "blue"]

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
