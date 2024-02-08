import xarray as xr
import pystac_client
import planetary_computer
import rioxarray
import datetime as dt
import numpy as np
import pyproj
import fsspec
from typing import Union, Optional


def get_sentinel3_aod(
    start_datetime: dt.datetime,
    end_datetime: dt.datetime,
    image_type: str = "FULL DISK",
    bands_to_use: Optional[list[int]] = None,
    full_resolution: bool = False,
) -> Union[list[xr.Dataset], None]:
    """
    Get the GOES images for the given time period

    Args:
        start_datetime: datetime to start getting images from
        end_datetime: End datetime to get images from
        image_type: Which GOES image type to get, either 'CONUS' or 'FULL DISK'
        bands_to_use: Which bands to get, if None, all bands are included
        full_resolution: Whether to get the full resolution images,
            which are variable in band, or the fixed resolution images, which are 2km


    Returns:
        Xarray Dataset containing the GOES images
    """
    assert image_type in ["CONUS", "FULL DISK"], ValueError(
        f"Image type {image_type=} not recognized"
    )
    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
    )
    search = catalog.search(
        collections=["sentinel-3-synergy-aod-l2-netcdf"],
        datetime=[start_datetime, end_datetime],
    )
    dses = []
    for i, item in enumerate(search.items()):
        ds = xr.open_dataset(fsspec.open(item.assets["ntc-aod"].href).open())
        dses.append(ds)
    return dses


def get_sentinel3_water(
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


    Returns:
        Xarray Dataset containing the GOES images
    """
    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
    )
    search = catalog.search(
        collections=["sentinel-3-olci-wfr-l2-netcdf"],
        datetime=[start_datetime, end_datetime],
    )
    if bands == "reflectance":
        catalog_values = [
            "oa01-reflectance",
            "oa02-reflectance",
            "oa03-reflectance",
            "oa04-reflectance",
            "oa05-reflectance",
            "oa06-reflectance",
            "oa07-reflectance",
            "oa08-reflectance",
            "oa09-reflectance",
            "oa10-reflectance",
            "oa11-reflectance",
            "oa12-reflectance",
            "oa16-reflectance",
            "oa17-reflectance",
            "oa18-reflectance",
            "oa21-reflectance",
            "geo-coordinates",
        ]
    elif bands == "derived":
        catalog_values = ["iwv", "par", "trsp", "wqsf", "w-aer", "geo-coordinates"]
    elif bands == "all":
        catalog_values = [
            "oa01-reflectance",
            "oa02-reflectance",
            "oa03-reflectance",
            "oa04-reflectance",
            "oa05-reflectance",
            "oa06-reflectance",
            "oa07-reflectance",
            "oa08-reflectance",
            "oa09-reflectance",
            "oa10-reflectance",
            "oa11-reflectance",
            "oa12-reflectance",
            "oa16-reflectance",
            "oa17-reflectance",
            "oa18-reflectance",
            "oa21-reflectance",
            "iwv",
            "par",
            "trsp",
            "wqsf",
            "w-aer",
            "geo-coordinates",
        ]
    else:
        catalog_values = bands
    dses = []
    for i, item in enumerate(search.items()):
        datasets = [
            xr.open_dataset(fsspec.open(item.assets[k].href).open()) for k in catalog_values
        ]
        ds = xr.combine_by_coords(datasets, join="exact", combine_attrs="drop_conflicts")
        dses.append(ds)
    return dses


def get_sentinel_3_land(
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
    )

    if bands == "all":
        catalog_values = ["iwv", "otci", "gifapar", "geo-coordinates"]

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
