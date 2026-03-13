import http.client
import multiprocessing as mp
import os
import shutil
import ssl
from typing import List
from urllib.request import build_opener

import icechunk
import metview as mv
import numpy as np
import pandas as pd
import tqdm
import xarray as xr
from icechunk.xarray import to_icechunk
from loguru import logger

from planetary_datasets.base import BaseProvider

ssl._create_default_https_context = ssl._create_unverified_context

class IFSAnalysisProvider(BaseProvider):
    name = "ifs_analysis"
    append_dim = "time"
    icechunk_path = "s3://us-west-2.opendata.source.coop/bkr/ifs/hres_analysis.icechunk"

    def fetch(self, it: pd.Timestamp, **kwargs) -> list[str]:
        date, times_in_repo, data_vars, temp_dir, grid = date_times_in_repo_and_data_vars
        if date.to_numpy() in times_in_repo:
            logger.debug(f"HRES Analysis data for {date} already exists, skipping...")
            return None, None
        temp_folder = f"{temp_dir}/{date.strftime('%Y%m%d')}_hres_temp"
        if not os.path.exists(temp_folder):
            os.makedirs(temp_folder)
        surface_ds, atmos_ds = download_raw_files(date, temp_folder=temp_folder, grid=grid)
        if surface_ds is None or atmos_ds is None:
            logger.debug(f"Failed to download raw files for {date}, skipping...")
            shutil.rmtree(temp_folder)
            return None, temp_folder
        ds = merge_and_rename_vars(surface_ds, atmos_ds)
        if data_vars is not None and set(ds.data_vars.keys()) != set(data_vars):
            logger.debug(f"Data variables do not match for {date}, skipping...")
            logger.error(set(ds.data_vars.keys()) - set(data_vars))
            logger.debug(data_vars)
            shutil.rmtree(temp_folder)
            return None, temp_folder
        return ds, temp_folder

    def process(self, input_files: List[str], it: pd.Timestamp, grid: list[float] | None = None,):
        data_vars = xr.open_zarr(self.get_icechunk_repo().readonly_session("main").store, consolidated=False).data_vars.keys()
        surface_files, atmosphere_files = input_files
        surface_ds = xr.open_mfdataset(surface_files, engine="cfgrib", compat="override", chunks={})
        # Atmosphere files must be merged per-hour (across variables), then concatenated
        # along time. open_mfdataset with combine="by_coords" fails because cfgrib doesn't
        # always expose dimension coordinates that xarray can use for ordering.
        n_vars = len(atmosphere_var_codes)
        hour_datasets = []
        for i in range(0, len(atmosphere_files), n_vars):
            hour_files = atmosphere_files[i: i + n_vars]
            dsets = [xr.open_dataset(f, engine="cfgrib", chunks={}) for f in hour_files]
            hour_datasets.append(xr.merge(dsets, compat="override"))
        atmos_ds = xr.concat(hour_datasets, dim="time")

        ds = merge_and_rename_vars(surface_ds, atmos_ds)
        if data_vars is not None and set(ds.data_vars.keys()) != set(data_vars):
            logger.debug(f"Data variables do not match for {it}, skipping...")
            logger.error(set(ds.data_vars.keys()) - set(data_vars))
            logger.debug(data_vars)
            return
        self.write_to_icechunk(self.get_icechunk_repo(), ds.chunk({"time": 1, "latitude": -1, "longitude": -1}))


# Utils functions (from assimilation.data.load.utils)
def lon_to_m180(lon):
    return (lon + 180) % 360 - 180


def make_lat_lon_coords_consistent(ds: xr.Dataset, lat_lon_coords: bool = True) -> xr.Dataset:
    if lat_lon_coords:
        if "latitude" not in ds.coords or "longitude" not in ds.coords:
            raise ValueError("Dataset must have 'latitude' and 'longitude' coordinates")

    if ds["latitude"].min() < -90 or ds["latitude"].max() > 90:
        raise ValueError("Latitude values must be between -90 and 90 degrees")
    if ds["longitude"].min() < -180 or ds["longitude"].max() > 180:
        if (ds["longitude"] >= 0).all() and (ds["longitude"] <= 360).all():
            ds["longitude"] = lon_to_m180(ds["longitude"])
        else:
            raise ValueError("Longitude values must be between -180 and 180 degrees")

    return ds


def make_spatial_coords_increasing(ds: xr.Dataset, x_coord: str, y_coord: str) -> xr.Dataset:
    ds = ds.sortby(x_coord, ascending=True).sortby(y_coord, ascending=True)

    if not (ds[x_coord].diff(dim=x_coord) > 0).all():
        raise ValueError(f"'{x_coord}' coordinate must be increasing")
    if not (ds[y_coord].diff(dim=y_coord) > 0).all():
        raise ValueError(f"'{y_coord}' coordinate must be increasing")

    return ds

vars_to_keep_float32 = [
    "specific_humidity",
    "geopotential",
    "geopotential_sfc",
    "surface_pressure",
    "surface_pressure_sfc",
    "mean_sea_level_pressure",
    "mean_sea_level_pressure_sfc",
    "ozone_mass_mixing_ratio",
]
static_vars = [
    "low_vegetation_cover",
    "high_vegetation_cover",
    "type_of_low_vegetation",
    "type_of_high_vegetation",
    "soil_type",
    "standard_deviation_of_filtered_subgrid_orography",
    "geopotential_at_the_surface",
    "standard_deviation_of_orography",
    "anisotropy_of_sub-gridscale_orography",
    "angle_of_sub-gridscale_orography",
    "slope_of_sub-gridscale_orography",
    "land-sea_mask",
    "surface_roughness",
    "logarithm_of_surface_roughness_length_for_heat",
]
vars_to_drop = [
    "ozone_mass_mixing_ratio",
    "divergence",
    "vorticity_relative",
    "potential_vorticity",
    "charnock",
    "uv_visible_albedo_for_direct_radiation",
    "uv_visible_albedo_for_diffuse_radiation",
    "near_ir_albedo_for_direct_radiation",
    "near_ir_albedo_for_diffuse_radiation",
    "leaf_area_index,_high_vegetation",
    "leaf_area_index,_low_vegetation",
]

atmosphere_var_codes = [
    ("129", "z"),
    ("130", "t"),
    ("131", "u"),
    ("132", "v"),
    ("133", "q"),
    ("135", "w"),
    ("157", "r"),
]
surface_var_codes = [
    ("031", "ci"),
    ("032", "asn"),
    ("033", "rsn"),
    ("034", "sstk"),
    ("035", "istl1"),
    ("036", "istl2"),
    ("037", "istl3"),
    ("038", "istl4"),
    ("039", "swvl1"),
    ("040", "swvl2"),
    ("041", "swvl3"),
    ("042", "swvl4"),
    ("043", "slt"),
    ("066", "lailv"),
    ("067", "laihv"),
    ("074", "sdfor"),
    ("129", "z"),
    ("134", "sp"),
    ("136", "tcw"),
    ("137", "tcwv"),
    ("139", "stl1"),
    ("141", "sd"),
    ("148", "chnk"),
    ("151", "msl"),
    ("160", "sdor"),
    ("161", "isor"),
    ("162", "anor"),
    ("163", "slor"),
    ("164", "tcc"),
    ("165", "10u"),
    ("166", "10v"),
    ("167", "2t"),
    ("168", "2d"),
    ("170", "stl2"),
    ("173", "sr"),
    ("174", "al"),
    ("183", "stl3"),
    ("186", "lcc"),
    ("187", "mcc"),
    ("188", "hcc"),
    ("198", "src"),
    ("206", "tco3"),
    ("234", "lsrh"),
    ("235", "skt"),
    ("236", "stl4"),
    ("238", "tsn"),
    ("246", "100u"),
    ("247", "100v"),
]


def time_deserialize(time):
    year = str(time.year).zfill(4)
    month = str(time.month).zfill(2)
    day = str(time.day).zfill(2)
    return year, month, day


def build_surface_url(time: pd.Timestamp, var_code: str, var: str) -> str:
    year, month, day = time_deserialize(time)
    table = "128" if var not in ["100u", "100v"] else "228"
    return (
        f"https://data.gdex.ucar.edu/d113001/ec.oper.an.sfc/"
        f"{year}{month}/ec.oper.an.sfc.{table}_{var_code}_{var}.regn1280sc.{year}{month}{day}.grb"
    )


def build_atmosphere_url(time: pd.Timestamp, hour: str, var_code: str, var: str) -> str:
    year, month, day = time_deserialize(time)
    grid_type = "sc" if var not in ["u", "v"] else "uv"
    return (
        f"https://data.gdex.ucar.edu/d113001/ec.oper.an.pl/"
        f"{year}{month}/ec.oper.an.pl.128_{var_code}_{var}.regn1280{grid_type}.{year}{month}{day}{hour}.grb"
    )


def regrid_grib(input_path: str, grid: list[float]) -> str:
    """Regrid a GRIB file to a regular lat/lon grid using Metview (default params)."""
    output_path = input_path.replace(".grb", f".regrid_{grid[0]}.grb")
    data = mv.read(input_path)
    regridded = mv.regrid(data=data, grid=grid)
    mv.write(output_path, regridded)
    return output_path


def download_file(url, output_path, opener, max_retries: int = 3) -> bool:
    successful = False
    while not successful:
        try:
            with open(output_path, "wb") as outfile:
                f = opener.open(url)
                outfile.write(f.read())
            successful = True
        except http.client.IncompleteRead:
            logger.debug(f"incomplete read of {url}, retrying...")
        except Exception as e:
            logger.debug(f"failed to download {url}, Error: {e}")
            return False
    return successful


def download_raw_files(
    time: pd.Timestamp, temp_folder: str, grid: list[float] | None = None
) -> tuple[xr.Dataset, xr.Dataset]:
    """Download the raw HRES Analysis files for a given time

    Args:
        time: Time to download
        grid: Target grid resolution [lat, lon] in degrees. If None, no regridding.

    Returns:
        Tuple of xarray datasets (surface, atmos)
    """
    opener = build_opener()
    surface_files = []
    for var_code, var in tqdm.tqdm(
        surface_var_codes, desc="Downloading surface variables"
    ):
        surface_url = build_surface_url(time, var_code, var)
        ofile = os.path.join(temp_folder, os.path.basename(surface_url))
        regridded_ofile = ofile.replace(".grb", f".regrid_{grid[0]}.grb") if grid else None
        if regridded_ofile and os.path.exists(regridded_ofile):
            surface_files.append(regridded_ofile)
            continue
        if os.path.exists(ofile):
            if grid is not None:
                ofile = regrid_grib(ofile, grid)
            surface_files.append(ofile)
            continue
        if download_file(surface_url, ofile, opener):
            if grid is not None:
                ofile = regrid_grib(ofile, grid)
            surface_files.append(ofile)
        else:
            return None, None
    atmosphere_files = []
    for hour in ["00", "06", "12", "18"]:
        for var_code, var in tqdm.tqdm(
            atmosphere_var_codes,
            desc=f"Downloading atmosphere variables for hour {hour}",
        ):
            atmosphere_url = build_atmosphere_url(time, hour, var_code, var)
            ofile = os.path.join(temp_folder, os.path.basename(atmosphere_url))
            regridded_ofile = ofile.replace(".grb", f".regrid_{grid[0]}.grb") if grid else None
            if regridded_ofile and os.path.exists(regridded_ofile):
                atmosphere_files.append(regridded_ofile)
                continue
            if os.path.exists(ofile):
                if grid is not None:
                    ofile = regrid_grib(ofile, grid)
                atmosphere_files.append(ofile)
                continue
            if download_file(atmosphere_url, ofile, opener):
                if grid is not None:
                    ofile = regrid_grib(ofile, grid)
                atmosphere_files.append(ofile)
            else:
                return None, None
    return surface_files, atmosphere_files


def merge_and_rename_vars(surface_ds: xr.Dataset, atmos_ds: xr.Dataset) -> xr.Dataset:
    # Rename surface variables to have _sfc suffix
    renamed = {}
    for var in surface_ds.data_vars:
        renamed[var] = var + "_sfc"
    surface_ds = surface_ds.rename(renamed)
    ds = xr.merge([surface_ds, atmos_ds])
    ds = ds.drop_vars(
        ["utc_date", "quantization_info", "utc_date_sfc", "quantization_info_sfc"],
        errors="ignore",
    )
    renamed = {}
    for var in ds.data_vars:
        long_name = ds[var].attrs.get("long_name", var)
        long_name = long_name.lower().replace(" ", "_").replace("(", "").replace(")", "")
        if var.endswith("_sfc"):
            long_name += "_sfc"
        renamed[var] = long_name
    ds = ds.rename(renamed)
    ds = ds.drop_vars(vars_to_drop, errors="ignore")
    ds = ds.drop_vars(static_vars, errors="ignore")
    for var in ds.data_vars:
        if var in vars_to_keep_float32 or var in static_vars:
            continue
        ds[var] = ds[var].astype(np.float16)
    ds = make_spatial_coords_increasing(ds, x_coord="longitude", y_coord="latitude")
    ds = make_lat_lon_coords_consistent(ds)
    level_dim = "isobaricInhPa" if "isobaricInhPa" in ds.dims else "level"
    ds = ds.chunk({"time": 1, level_dim: -1, "latitude": -1, "longitude": -1})
    return ds


def download_and_process_date(
    date_times_in_repo_and_data_vars: tuple[
        pd.Timestamp, list[pd.Timestamp], list[str], str, list[float] | None
    ],
) -> tuple[xr.Dataset, str]:
    date, times_in_repo, data_vars, temp_dir, grid = date_times_in_repo_and_data_vars
    if date.to_numpy() in times_in_repo:
        logger.debug(f"HRES Analysis data for {date} already exists, skipping...")
        return None, None
    temp_folder = f"{temp_dir}/{date.strftime('%Y%m%d')}_hres_temp"
    if not os.path.exists(temp_folder):
        os.makedirs(temp_folder)
    surface_ds, atmos_ds = download_raw_files(date, temp_folder=temp_folder, grid=grid)
    if surface_ds is None or atmos_ds is None:
        logger.debug(f"Failed to download raw files for {date}, skipping...")
        shutil.rmtree(temp_folder)
        return None, temp_folder
    ds = merge_and_rename_vars(surface_ds, atmos_ds)
    if data_vars is not None and set(ds.data_vars.keys()) != set(data_vars):
        logger.debug(f"Data variables do not match for {date}, skipping...")
        logger.error(set(ds.data_vars.keys()) - set(data_vars))
        logger.debug(data_vars)
        shutil.rmtree(temp_folder)
        return None, temp_folder
    return ds, temp_folder