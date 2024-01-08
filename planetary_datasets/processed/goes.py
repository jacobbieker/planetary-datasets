import xarray as xr
import pystac_client
import planetary_computer
import rioxarray
import datetime as dt
import numpy as np
import pyproj
from typing import Union, Optional


def get_goes_image(
    start_datetime: dt.datetime,
    end_datetime: dt.datetime,
    image_type: str = "FULL DISK",
    bands: Optional[list[int]] = None,
    full_resolution: bool = False,
) -> Union[xr.Dataset, None]:
    """
    Get the GOES images for the given time period

    Args:
        start_datetime: datetime to start getting images from
        end_datetime: End datetime to get images from
        image_type: Which GOES image type to get, either 'CONUS' or 'FULL DISK'
        bands: Which bands to get, if None, all bands are included
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
        collections=["goes-cmi"],
        datetime=[start_datetime, end_datetime],
        query={"goes:image-type": {"eq": image_type}},
    )
    timesteps = []
    for i, item in enumerate(search.items()):
        bands = []
        for idx in range(1, 7):
            bands.append(f"C{idx:02d}_2km")
        common_names = [
            item.assets[band].extra_fields["eo:bands"][0]["common_name"]
            for band in bands
            if "common_name" in item.assets[band].extra_fields["eo:bands"][0]
        ]
        ds = xr.concat(
            [rioxarray.open_rasterio(item.assets[band].href) for band in bands], dim="band"
        ).assign_coords(band=common_names)
        # Add created date as a coordinate
        ds = ds.assign_coords(
            {"time": dt.datetime.strptime(ds.attrs["date_created"], "%Y-%m-%dT%H:%M:%S.%fZ")}
        )
        timesteps.append(ds)
    ds = xr.concat(timesteps, dim="time")
    ds = ds.sortby("time").transpose("time", "band", "x", "y")
    # Add lat/lon coordinates
    ds = calc_latlon(ds)
    # Add CRS to main attributes
    ds.attrs["crs"] = ds.rio.crs
    return ds


def calc_latlon(ds: xr.Dataset) -> xr.Dataset:
    """
    Calculate the latitude and longitude coordinates for the given dataset

    Args:
        ds: Xarray Dataset to calculate the lat/lon coordinates for, with x and y coordinates

    Returns:
        Xarray Dataset with the latitude and longitude coordinates added
    """
    XX, YY = np.meshgrid(ds.x.data, ds.y.data)
    lons, lats = convert_x_y_to_lat_lon(ds.rio.crs, XX, YY)
    # Check if lons and lons_trans are close in value
    # Set inf to NaN values
    lons[lons == np.inf] = np.nan
    lats[lats == np.inf] = np.nan

    ds = ds.assign_coords({"latitude": (["y", "x"], lats), "longitude": (["y", "x"], lons)})
    ds.latitude.attrs["units"] = "degrees_north"
    ds.longitude.attrs["units"] = "degrees_east"
    return ds


def convert_x_y_to_lat_lon(crs: str, lon: list[float], lat: list[float]) -> tuple[float, float]:
    """Convert the given x/y coordinates to lat/lon in the given CRS"""
    transformer = pyproj.Transformer.from_crs(crs, "epsg:4326")
    xs, ys = transformer.transform(lon, lat)
    return xs, ys


def convert_lat_lon_to_x_y(crs: str, x: list[float], y: list[float]) -> tuple[float, float]:
    """Convert the given lat/lon to x/y coordinates in the given CRS"""
    transformer = pyproj.Transformer.from_crs("epsg:4326", crs)
    lons, lats = transformer.transform(x, y)
    return lons, lats
