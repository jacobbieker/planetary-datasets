import sys
import xarray as xr
from obstore.store import from_url

from virtualizarr import open_virtual_dataset, open_virtual_mfdataset
from virtualizarr.parsers import HDFParser
from virtualizarr.registry import ObjectStoreRegistry
import icechunk
import s3fs
import os
from concurrent.futures import ThreadPoolExecutor
import dask
import numpy as np
import xarray as xr
from pyresample import geometry
import datetime as dt
import functools

import warnings

warnings.filterwarnings(
    "ignore",
    message="Numcodecs codecs are not in the Zarr version 3 specification*",
    category=UserWarning,
)

parser = HDFParser()
coords = ["x", "y", "t", "nominal_satellite_subpoint_lat", "nominal_satellite_subpoint_lon", "nominal_satellite_height",
          "yaw_flip_flag"]


def get_area_def(vds):
    """Get the area definition of the data at hand."""
    if "goes_imager_projection" in vds:
        return _get_areadef_fixedgrid(vds)
    if "goes_lat_lon_projection" in vds:
        return _get_areadef_latlon(vds)
    raise ValueError("Unsupported projection found in the dataset")


def _get_areadef_latlon(vds):
    """Get the area definition of the data at hand."""
    projection = vds["goes_lat_lon_projection"]

    a = projection.attrs["semi_major_axis"]
    b = projection.attrs["semi_minor_axis"]
    fi = projection.attrs["inverse_flattening"]
    pm = projection.attrs["longitude_of_prime_meridian"]

    proj_ext = vds["geospatial_lat_lon_extent"]

    w_lon = proj_ext.attrs["geospatial_westbound_longitude"]
    e_lon = proj_ext.attrs["geospatial_eastbound_longitude"]
    n_lat = proj_ext.attrs["geospatial_northbound_latitude"]
    s_lat = proj_ext.attrs["geospatial_southbound_latitude"]

    lat_0 = proj_ext.attrs["geospatial_lat_center"]
    lon_0 = proj_ext.attrs["geospatial_lon_center"]

    area_extent = (w_lon, s_lat, e_lon, n_lat)
    proj_dict = {"proj": "latlong",
                 "lon_0": float(lon_0),
                 "lat_0": float(lat_0),
                 "a": float(a),
                 "b": float(b),
                 "fi": float(fi),
                 "pm": float(pm)}

    ll_area_def = geometry.AreaDefinition(
        vds.attrs.get("orbital_slot", "abi_geos"),
        vds.attrs.get("spatial_resolution", "ABI file area"),
        "abi_latlon",
        proj_dict,
        vds["x"].size,
        vds["y"].size,
        np.asarray(area_extent))

    return ll_area_def


def _get_areadef_fixedgrid(vds):
    """Get the area definition of the data at hand.

    Note this method takes special care to round and cast numbers to new
    data types so that the area definitions for different resolutions
    (different bands) should be equal. Without the special rounding in
    `__getitem__` and this method the area extents can be 0 to 1.0 meters
    off depending on how the calculations are done.

    """
    projection = vds["goes_imager_projection"]
    a = projection.attrs["semi_major_axis"]
    b = projection.attrs["semi_minor_axis"]
    h = projection.attrs["perspective_point_height"]

    lon_0 = projection.attrs["longitude_of_projection_origin"]
    sweep_axis = projection.attrs["sweep_angle_axis"][0]

    # compute x and y extents in m
    h = np.float64(h)
    x = vds["x"]
    y = vds["y"]
    x_l = x[0].values
    x_r = x[-1].values
    y_l = y[-1].values
    y_u = y[0].values
    x_half = (x_r - x_l) / (vds["x"].size - 1) / 2.
    y_half = (y_u - y_l) / (vds["y"].size - 1) / 2.
    area_extent = (x_l - x_half, y_l - y_half, x_r + x_half, y_u + y_half)
    area_extent = tuple(np.round(h * val, 6) for val in area_extent)

    proj_dict = {"proj": "geos",
                 "lon_0": float(lon_0),
                 "a": float(a),
                 "b": float(b),
                 "h": h,
                 "units": "m",
                 "sweep": sweep_axis}

    fg_area_def = geometry.AreaDefinition(
        vds.attrs.get("orbital_slot", "abi_geos"),
        vds.attrs.get("spatial_resolution", "ABI file area"),
        "abi_fixed_grid",
        proj_dict,
        vds["x"].size,
        vds["y"].size,
        np.asarray(area_extent))

    return fg_area_def


def start_time(vds):
    """Start time of the current file's observations."""
    return dt.datetime.strptime(vds.attrs["time_coverage_start"], "%Y-%m-%dT%H:%M:%S.%fZ")


def end_time(vds):
    """End time of the current file's observations."""
    return dt.datetime.strptime(vds.attrs["time_coverage_end"], "%Y-%m-%dT%H:%M:%S.%fZ")


def _adjust_attrs(vds, satellite):
    vds.attrs.update({"platform_name": satellite,
                       "sensor": "abi"})
    # Add orbital parameters
    projection = vds["goes_imager_projection"]
    vds.attrs["orbital_parameters"] = {
        "projection_longitude": float(projection.attrs["longitude_of_projection_origin"]),
        "projection_latitude": float(projection.attrs["latitude_of_projection_origin"]),
        "projection_altitude": float(projection.attrs["perspective_point_height"]),
        "satellite_nominal_latitude": float(vds["nominal_satellite_subpoint_lat"]),
        "satellite_nominal_longitude": float(vds["nominal_satellite_subpoint_lon"]),
        "satellite_nominal_altitude": float(vds["nominal_satellite_height"]) * 1000.,
        "yaw_flip": bool(vds["yaw_flip_flag"]),
    }



def preprocess(vds: xr.Dataset, satellite) -> xr.Dataset:
    """Preprocess the dataset to ensure it has the correct dimensions and variables."""
    # Set y and x as data variables
    vds["y_coordinates"] = xr.DataArray(
        vds["y"].values,
        dims=["y"],
    )
    vds["x_coordinates"] = xr.DataArray(
        vds["x"].values,
        dims=["x"],
    )
    vds = vds.rename({"t": "time"})
    vds = vds.expand_dims("time")
    _adjust_attrs(vds, satellite)
    vds["start_time"] = xr.DataArray([start_time(vds)],
                                     dims=("time",),)
    vds["end_time"] = xr.DataArray([end_time(vds)],
                                   dims=("time",),)
    area_def = get_area_def(vds)
    orbital_parameters = vds.attrs.pop("orbital_parameters")
    # Expand coords for data to have time dimension
    vds["orbital_parameters"] = xr.DataArray(
        [orbital_parameters],
        dims=("time",),
    ).astype(f"U512")
    vds["area"] = xr.DataArray(
        [str(area_def)],
        dims=("time",),
    ).astype(f"U512")
    return vds


def process_year(satellite: str, channel: str):
    fs = s3fs.S3FileSystem(anon=True, client_kwargs={}, asynchronous=False)
    bucket = f"s3://noaa-{satellite}"
    store = from_url(bucket, skip_signature=True)
    registry = ObjectStoreRegistry({bucket: store})
    start_day = 1
    """Process a year of GOES data."""
    print(f"Processing {satellite}")
    # By default, local virtual references and public remote virtual references can be read without extra configuration.
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(f"s3://noaa-{satellite}/", icechunk.s3_store(region="us-east-1", anonymous=True)),
    )
    storage = icechunk.local_filesystem_storage(
        f"/data/virtual_icechunk/{satellite}_{channel}.icechunk")
    repo = icechunk.Repository.open_or_create(
        storage, config=config, authorize_virtual_chunk_access={f"s3://noaa-{satellite}/": None},
    )
    repo.save_config()
    # Try getting times from the repo, if any, then get those so it isn't written multiple times
    first_write = False
    try:
        session = repo.readonly_session("main")
        ds = xr.open_zarr(session.store, consolidated=False)
        times = ds["time"].values
        print(ds)
        print(times)
        first_write = False
    except:
        print(f"No existing data found for {satellite}, starting from scratch.")
        times = []
    pre_func = functools.partial(preprocess, satellite=satellite)
    for year in range(2019, 2026):
        for day in range(start_day, 366):
            files = []
            day_str = f"{day:03d}"
            for hour in range(0, 24):
                # Format the hour as a two-digit string
                hour_str = f"{hour:02d}"
                # Construct the path for the current day and hour
                path = f"ABI-L1b-RadF/{year}/{day_str}/{hour_str}/"
                # List files in the directory
                try:
                    new_files = fs.ls(f"{bucket}/{path}")
                    new_files = [f for f in new_files if channel in f]
                    files.extend(new_files)
                except FileNotFoundError:
                    #print(f"Directory {path} not found, skipping.")
                    continue
            if len(files) == 0:
                continue
            try:
                #print(f"Processing {len(files)} files...")
                vds = open_virtual_mfdataset(
                    ["s3://" + f for f in files][:10],
                    parser=parser,
                    registry=registry,
                    decode_times=True,
                    combine="nested",
                    concat_dim="time",
                    data_vars="minimal",
                    coords="minimal",
                    compat="override",
                    parallel=ThreadPoolExecutor,
                    loadable_variables=coords,
                    preprocess=pre_func,
                )
            except Exception as e:
                print(f"Failed to process {year}-{day_str}: {e}")
                continue
            for time in vds["time"].values:
                if time in times:
                    print(f"Skipping {year}-{day_str} {time} as it already exists.")
                    continue
            session = repo.writable_session("main")
            # write the virtual dataset to the session with the IcechunkStore
            #try:
            if first_write:
                vds.vz.to_icechunk(session.store)
                first_write = False
            else:
                vds.vz.to_icechunk(session.store, append_dim="time")
            snapshot_id = session.commit(f"Wrote {year} {start_day} day of {satellite} data for {year}")
            print(snapshot_id)
            #except Exception as e:
            #    print(f"Failed to write {year}-{day_str} data: {e}")
            #    return

def process_year_wrap(sat_and_channel):
    satellite, channel = sat_and_channel
    process_year(satellite, channel)

if __name__ == "__main__":
    import multiprocessing as mp
    #mp.set_start_method("forkserver")
    satellites = [ "goes16", "goes19", "goes16", "goes18",]
    high_res_channels = ["C02", "C01", "C03", "C05",
        "C04",
        "C06",
        "C07",
        "C08",
        "C09",
        "C10",
        "C11",
        "C12",
        "C13",
        "C14",
        "C15",
        "C16",
    ]
    pool = mp.Pool(4*16)
    for _ in pool.map(process_year_wrap, [(sat, channel) for sat in satellites for channel in high_res_channels]):
        pass