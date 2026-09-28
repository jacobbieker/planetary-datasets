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
from satpy.readers.core._geos_area import get_area_definition, get_area_extent
import datetime as dt
import warnings
import numpy as np
import pyproj

warnings.filterwarnings(
    "ignore",
    message="Numcodecs codecs are not in the Zarr version 3 specification*",
    category=UserWarning,
)

parser = HDFParser()

def get_area_def(vds):
    """Get area definition for this file."""
    pdict = {}
    pdict["a"] = vds.attrs["earth_equatorial_radius"]
    pdict["b"] = vds.attrs["earth_polar_radius"]
    pdict["h"] = vds.attrs["nominal_satellite_height"] - pdict["a"]
    pdict["ssp_lon"] = vds.attrs["sub_longitude"] * 180 / np.pi  # it's in radians?
    pdict["ncols"] = vds.attrs["number_of_columns"]
    pdict["nlines"] = vds.attrs["number_of_lines"]
    obs_mode = vds.attrs["observation_mode"]
    resolution = vds.attrs["channel_spatial_resolution"]

    # Example offset: 11000.5
    # the 'get_area_extent' will handle this half pixel for us
    pdict["cfac"] = vds.attrs["cfac"]
    pdict["coff"] = vds.attrs["coff"]
    pdict["lfac"] = -vds.attrs["lfac"]
    pdict["loff"] = vds.attrs["loff"]
    pdict["scandir"] = "N2S"
    pdict["a_name"] = "ami_geos_{}".format(obs_mode.lower())
    pdict["a_desc"] = "AMI {} Area at {} resolution".format(obs_mode, resolution)
    pdict["p_id"] = "ami_fixed_grid"

    area_extent = get_area_extent(pdict)
    fg_area_def = get_area_definition(pdict, area_extent)
    return fg_area_def

def get_orbital_parameters(vds):
    """Collect orbital parameters for this file."""
    a = float(vds.attrs["earth_equatorial_radius"])
    b = float(vds.attrs["earth_polar_radius"])
    # nominal_satellite_height seems to be from the center of the earth
    h = float(vds.attrs["nominal_satellite_height"]) - a
    lon_0 = vds.attrs["sub_longitude"] * 180 / np.pi  # it's in radians?
    sc_position = vds["sc_position"].attrs["sc_position_center_pixel"]

    # convert ECEF coordinates to lon, lat, alt
    ecef = pyproj.CRS.from_dict({"proj": "geocent", "a": a, "b": b})
    lla = pyproj.CRS.from_dict({"proj": "latlong", "a": a, "b": b})
    transformer = pyproj.Transformer.from_crs(ecef, lla)
    sc_position = transformer.transform(sc_position[0], sc_position[1], sc_position[2])

    orbital_parameters = {
        "projection_longitude": float(lon_0),
        "projection_latitude": 0.0,
        "projection_altitude": h,
        "satellite_actual_longitude": sc_position[0],
        "satellite_actual_latitude": sc_position[1],
        "satellite_actual_altitude": sc_position[2],  # meters
    }
    return orbital_parameters

def preprocess(vds: xr.Dataset) -> xr.Dataset:
    """Preprocess the dataset to ensure it has the correct dimensions and variables."""
    # Set y and x as data variables
    vds = vds.rename({"dim_image_y": "y", "dim_image_x": "x"})
    vds["y_coordinates"] = xr.DataArray(
        vds["y"].values,
        dims=["y"],
    )
    vds["x_coordinates"] = xr.DataArray(
        vds["x"].values,
        dims=["x"],
    )
    base = dt.datetime(2000, 1, 1, 12, 0, 0)
    mid_time = (dt.timedelta(seconds=vds.attrs["observation_start_time"]) + dt.timedelta(seconds=vds.attrs["observation_end_time"])) / 2
    vds = vds.drop_dims(["dim_boa_swaths", "dim_matched_lmks", "dim_inr_perform", "dim_vis_stars", "dim_ir_stars"])
    # Drop any dims that are not time, y, x, or band
    vds = vds.expand_dims({"time": [base + mid_time]})
    vds["start_time"] = xr.DataArray([base + dt.timedelta(seconds=vds.attrs["observation_start_time"])], coords={"time": vds.coords["time"]})
    vds["end_time"] = xr.DataArray([base + dt.timedelta(seconds=vds.attrs["observation_end_time"])], coords={"time": vds.coords["time"]})
    orbital_parameters = get_orbital_parameters(vds)
    area_def = get_area_def(vds)
    #"""
    # Expand coords for data to have time dimension
    vds["orbital_parameters"] = xr.DataArray(
        [orbital_parameters],
        dims=("time",),
    ).astype(f"U512")
    vds["area"] = xr.DataArray(
        [str(area_def)],
        dims=("time",),
    ).astype(f"U512")
    #"""
    return vds


def process_year(band: str):
    fs = s3fs.S3FileSystem(anon=True, client_kwargs={}, asynchronous=False)
    bucket = "s3://noaa-gk2a-pds"
    band = band.lower()
    store = from_url(bucket, skip_signature=True)
    registry = ObjectStoreRegistry({bucket: store})
    start_day = 1
    """Process a year of GOES data."""
    print(f"Processing {band}")
    # By default, local virtual references and public remote virtual references can be read without extra configuration.
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer("s3://noaa-gk2a-pds/", icechunk.s3_store(region="us-east-1", anonymous=True)),
    )
    storage = icechunk.local_filesystem_storage(
        f"gk2a_{band}.icechunk")
    repo = icechunk.Repository.open_or_create(
        storage, config=config, authorize_virtual_chunk_access={"s3://noaa-gk2a-pds": None},
    )
    repo.save_config()
    first_write = False
    for year in range(2024, 2026):
        for month in range(1, 13):
            for day in range(start_day, 31):
                files = []
                for hour in range(0, 24):
                    # Format the hour as a two-digit string
                    hour_str = f"{hour:02d}"
                    # Construct the path for the current day and hour
                    path = f"AMI/L1B/FD/{year}{month:02d}/{day:02d}/{hour_str}/"
                    # List files in the directory
                    try:
                        new_files = fs.ls(f"{bucket}/{path}")
                        new_files = [f for f in new_files if band in f]
                        files.extend(new_files)
                    except FileNotFoundError:
                        #print(f"Directory {path} not found, skipping.")
                        continue
                if len(files) == 0:
                    continue
                try:
                    print(f"Processing {len(files)} files...")
                    vds = open_virtual_mfdataset(
                        ["s3://" + f for f in files],
                        parser=parser,
                        registry=registry,
                        decode_times=True,
                        combine="nested",
                        concat_dim="time",
                        data_vars="minimal",
                        coords="minimal",
                        compat="override",
                        parallel=ThreadPoolExecutor,
                        preprocess=preprocess,
                    )
                except Exception as e:
                    print(f"Failed to process {year}-{day}: {e}")
                    continue

                session = repo.writable_session("main")
                print(vds)
                # write the virtual dataset to the session with the IcechunkStore
                #try:
                if first_write:
                    vds.vz.to_icechunk(session.store)
                    first_write = False
                else:
                    vds.vz.to_icechunk(session.store, append_dim="time")
                snapshot_id = session.commit(f"Wrote {year} {start_day} day of {band} data for {year}")
                print(snapshot_id)
                #except Exception as e:
                #    print(f"Failed to write {year}-{day} data: {e}")
                #    continue

if __name__ == "__main__":
    import multiprocessing as mp
    high_res_channels = ["VI006","VI008", "VI005", "VI004",
        "IR087",
        "IR096",
        "IR105",
        "IR112",
        "IR123",
        "IR133",
        "NR013",
        "NR016",
        "SW038",
        "WV063",
        "WV069",
        "WV073",
    ]
    pool = mp.Pool(5)
    pool.map(process_year, high_res_channels)