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
    if bands_to_use is None:
        bands_to_use = list(range(1, 17))  # 1-16
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
        for idx in bands_to_use:
            if full_resolution:
                if idx in [1, 3, 5]:
                    bands.append(f"C{idx:02d}_1km")
                elif idx in [
                    2,
                ]:
                    bands.append(f"C{idx:02d}_0.5km")
                else:
                    bands.append(f"C{idx:02d}_2km")
            else:
                bands.append(f"C{idx:02d}_2km")

        if full_resolution:
            ds_20 = xr.concat(
                [rioxarray.open_rasterio(item.assets[band].href) for band in bands if "_2km"],
                dim="band",
            )
            ds_05 = xr.concat(
                [
                    rioxarray.open_rasterio(item.assets[band].href)
                    for band in bands
                    if "0.5km" in band
                ],
                dim="band",
            )
            # Load the 1km bands
            ds_1 = xr.concat(
                [
                    rioxarray.open_rasterio(item.assets[band].href)
                    for band in bands
                    if "_1km" in band
                ],
                dim="band",
            )
            dses = [ds_20, ds_05, ds_1]
        else:
            ds_20 = xr.concat(
                [rioxarray.open_rasterio(item.assets[band].href) for band in bands], dim="band"
            )
            dses = [ds_20]
        for idx, ds in enumerate(dses):
            # Add created date as a coordinate
            dses[idx] = ds.assign_coords(
                {"time": dt.datetime.strptime(ds.attrs["date_created"], "%Y-%m-%dT%H:%M:%S.%fZ")}
            )
        timesteps.append(dses)
    # Concatentate along the same index in each sublist
    dses = [
        xr.concat([timestep[idx] for timestep in timesteps], dim="time")
        for idx in range(len(timesteps[0]))
    ]
    for idx, ds in enumerate(dses):
        # Add the band names as coordinates
        ds = ds.sortby("time").transpose("time", "band", "x", "y")
        # Add lat/lon coordinates
        ds = calc_latlon(ds)
        # Add CRS to main attributes
        ds.attrs["crs"] = ds.rio.crs
        dses[idx] = ds
    return dses


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
