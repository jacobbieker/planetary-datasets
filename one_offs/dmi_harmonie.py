import os.path

import fsspec
import icechunk
import xarray as xr
import pandas as pd
import glob
import cfgrib
import warnings
import numpy as np
import zarr.codecs
from icechunk.xarray import to_icechunk
import dask
warnings.filterwarnings("ignore", category=FutureWarning)
"""
Paths are in the format:
s3://dmi-opendata/forecastdata/HARMONIE_IG_PL/HARMONIE_IG_PL_2026-03-28T000000Z_2026-03-30T020000Z.grib
and 
s3://dmi-opendata/forecastdata/HARMONIE_IG_SF/HARMONIE_IG_SF_2026-03-28T000000Z_2026-03-30T020000Z.grib

A new init time is every 3 hours, so we just want the 0, 1, and 2 hour forecasts for each init time, which means we want to get the files with the following patterns:
HARMONIE_IG_PL_2026-03-28T000000Z_2026-03-30T020000Z.grib
HARMONIE_IG_SF_2026-03-28T000000Z_2026-03-30T020000Z.grib

"""

def process_ml_timestep(date: pd.Timestamp, forecast_step: int) -> xr.Dataset | None:
    init_time = date.strftime("%Y-%m-%dT%H%M%SZ")
    forecast_time = (date + pd.Timedelta(hours=forecast_step)).strftime("%Y-%m-%dT%H%M%SZ")
    print(forecast_time)
    files = list(sorted(glob.glob(f"HARMONIE_IG_ML_{init_time}_{forecast_time}.grib")))
    if len(files) == 0:
        print(f"No files found for init time {init_time}")
        return
    ml_ds = cfgrib.open_datasets(files[0])
    ds = xr.merge(ml_ds)
    for var in ds.data_vars:
        long_name = ds[var].attrs.get("long_name", var).lower().replace(" ", "_").replace("(","").replace(")","")
        ds = ds.rename({var: long_name})
    return ds

def process_timestep(date: pd.Timestamp, forecast_step: int) -> xr.Dataset | None:
    init_time = date.strftime("%Y-%m-%dT%H%M%SZ")
    forecast_time = (date + pd.Timedelta(hours=forecast_step)).strftime("%Y-%m-%dT%H%M%SZ")
    print(forecast_time)
    files = list(sorted(glob.glob(f"HARMONIE_IG_PL_{init_time}_{forecast_time}.grib")))
    if len(files) == 0:
        print(f"No files found for init time {init_time}")
        return
    pl_ds = cfgrib.open_datasets(files[0])
    # print(pl_ds)
    ds = xr.merge(pl_ds)
    files = list(sorted(glob.glob(f"HARMONIE_IG_SF_{init_time}_{forecast_time}.grib")))
    if len(files) == 0:
        print(f"No files found for init time {init_time}")
        return
    pl_ds = cfgrib.open_datasets(files[0])
    # print(pl_ds)
    renamed_ds = []
    for sub_ds in pl_ds:
        if "h" in sub_ds.data_vars:
            if "neutralBuoyancy" in sub_ds.coords:
                renamed_ds.append(sub_ds.rename({"h": "height_of_neutral_buoyancy"}))
            elif "isothermal" in sub_ds.coords:
                renamed_ds.append(sub_ds.rename({"h": "height_of_isothermal_layer"}))
            elif "isothermZero" in sub_ds.coords:
                renamed_ds.append(sub_ds.rename({"h": "height_of_zero_isotherm"}))
            elif "freeConvection" in sub_ds.coords:
                renamed_ds.append(sub_ds.rename({"h": "height_of_free_convection"}))
            elif "adiabaticCondensation" in sub_ds.coords:
                renamed_ds.append(sub_ds.rename({"h": "height_of_adiabatic_condensation_level"}))
            else:
                renamed_ds.append(sub_ds.rename({"h": "height"}))
        elif "unknown" in sub_ds.data_vars:
            continue
        elif "pres" in sub_ds.data_vars:
            if "heightAboveSea" in sub_ds.coords:
                renamed_ds.append(sub_ds.rename({"pres": "pressure_at_sea_level"}))
            else:
                height = sub_ds.coords["heightAboveGround"].values
                for var in sub_ds.data_vars:
                    if "[" in f"{height}":
                        for h in height:
                            sub_ds[f"{var}_at_height_above_ground_{h}"] = sub_ds[var].sel(heightAboveGround=h)
                        # Drop original one
                        sub_ds = sub_ds.drop_vars(var)
                sub_ds = sub_ds.drop_vars("heightAboveGround")
                renamed_ds.append(sub_ds)
        elif "t" in sub_ds.data_vars or "u" in sub_ds.data_vars or "v" in sub_ds.data_vars or "q" in sub_ds.data_vars or "gh" in sub_ds.data_vars or "r" in sub_ds.data_vars or "cc" in sub_ds.data_vars:
            if "heightAboveGround" in sub_ds.coords:
                height = sub_ds.coords["heightAboveGround"].values
                renames = {}
                for var in sub_ds.data_vars:
                    if "[" in f"{height}":
                        for h in height:
                            sub_ds[f"{var}_at_height_above_ground_{h}"] = sub_ds[var].sel(heightAboveGround=h)
                        # Drop original one
                        sub_ds = sub_ds.drop_vars(var)
                    else:
                        renames[var] = f"{var}_at_height_above_ground_{height}"
                sub_ds = sub_ds.rename(renames)
                # Now drop heightAboveGround coordinate
                sub_ds = sub_ds.drop_vars("heightAboveGround")
                renamed_ds.append(sub_ds)
            elif "heightAboveSea" in sub_ds.coords:
                height = sub_ds.coords["heightAboveSea"].values
                renames = {}
                for var in sub_ds.data_vars:
                    if "[" in f"{height}":
                        for h in height:
                            sub_ds[f"{var}_at_height_above_sea_{h}"] = sub_ds[var].sel(heightAboveSea=h)
                        # Drop original one
                        sub_ds = sub_ds.drop_vars(var)
                    else:
                        renames[var] = f"{var}_at_height_above_sea_{height}"
                sub_ds = sub_ds.rename(renames)
                sub_ds = sub_ds.drop_vars("heightAboveSea")
                renamed_ds.append(sub_ds)
            elif "hybrid" in sub_ds.coords:
                continue
            else:
                sub_ds = sub_ds.drop_vars(["heightAboveSea", "heightAboveGround", "hybrid"], errors="ignore")
                for var in sub_ds.data_vars:
                    long_name = sub_ds[var].attrs.get("long_name", var).lower().replace(" ", "_").replace("(","").replace(")","")
                    sub_ds = sub_ds.rename({var: long_name})
                renamed_ds.append(sub_ds)
        else:
            for var in sub_ds.data_vars:
                long_name = sub_ds[var].attrs.get("long_name", var).lower().replace(" ", "_").replace("(","").replace(")","")
                sub_ds = sub_ds.rename({var: long_name})
            ones_to_remove = {}
            for coord in sub_ds.coords:
                if coord not in ["latitude", "longitude", "time", "valid_time", "step"] and coord not in sub_ds.dims:
                    ones_to_remove[coord] = sub_ds.coords[coord]
            sub_ds = sub_ds.drop_vars(ones_to_remove)
            renamed_ds.append(sub_ds)
    ds_sfc = xr.merge(renamed_ds)
    for var in ds.data_vars:
        if "2m" in var or "10m" in var:
            continue
        long_name = ds[var].attrs.get("long_name", var).lower().replace(" ", "_").replace("(","").replace(")","")
        long_name = f"{long_name}_at_surface"
        ds = ds.rename({var: long_name})
    # Rename to the long name
    ds = xr.merge([ds, ds_sfc])
    # Now remove all coordinates that are not latitude, longitude, time, valid_time, step, or dims
    ones_to_remove = {}
    for coord in ds.coords:
        if coord not in ["latitude", "longitude", "time", "step"] and coord not in ds.dims:
            ones_to_remove[coord] = ds.coords[coord]
    ds = ds.drop_vars(ones_to_remove)
    return ds

def download_forecast(date: pd.Timestamp) -> list[str]:
    init_time = date.strftime("%Y-%m-%dT%H%M%SZ")
    # Now the 3 forecast steps we want, one is init time, one is init time + 1 hour, and one is init time + 2 hours
    plus_1 = (date + pd.Timedelta(hours=1)).strftime("%Y-%m-%dT%H%M%SZ")
    plus_2 = (date + pd.Timedelta(hours=2)).strftime("%Y-%m-%dT%H%M%SZ")
    file_pattern_pl_1 = f"HARMONIE_IG_PL_{init_time}_{plus_1}.grib"
    file_pattern_sf_1 = f"HARMONIE_IG_SF_{init_time}_{plus_1}.grib"
    file_pattern_pl_2 = f"HARMONIE_IG_PL_{init_time}_{plus_2}.grib"
    file_pattern_sf_2 = f"HARMONIE_IG_SF_{init_time}_{plus_2}.grib"
    file_pattern_pl_0 = f"HARMONIE_IG_PL_{init_time}_{init_time}.grib"
    file_pattern_sf_0 = f"HARMONIE_IG_SF_{init_time}_{init_time}.grib"

    file_pattern_ml_0 = f"HARMONIE_IG_ML_{init_time}_{init_time}.grib"
    file_pattern_ml_1 = f"HARMONIE_IG_ML_{init_time}_{plus_1}.grib"
    file_pattern_ml_2 = f"HARMONIE_IG_ML_{init_time}_{plus_2}.grib"
    # Download that data
    pl_files = []
    sf_files = []
    ml_files = []
    for file_pattern in [file_pattern_pl_0, file_pattern_pl_1, file_pattern_pl_2]:
        pl_files.extend(fs.glob(f"s3://dmi-opendata/forecastdata/HARMONIE_IG_PL/{file_pattern}"))
    for file_pattern in [file_pattern_sf_0, file_pattern_sf_1, file_pattern_sf_2]:
        sf_files.extend(fs.glob(f"s3://dmi-opendata/forecastdata/HARMONIE_IG_SF/{file_pattern}"))
    for file_pattern in [file_pattern_ml_0, file_pattern_ml_1, file_pattern_ml_2]:
        ml_files.extend(fs.glob(f"s3://dmi-opendata/forecastdata/HARMONIE_IG_ML/{file_pattern}"))
    if len(pl_files) != 3:
        print(f"No PL files found for init time {init_time}")
        return
    if len(sf_files) != 3:
        print(f"No SF files found for init time {init_time}")
        return
    print(f"Init time: {init_time}")
    print(f"PL files: {pl_files}")
    print(f"SF files: {sf_files}")
    # Download those files to the local disk
    for ml_file in ml_files:
        if os.path.exists(ml_file.split("/")[-1]):
            print(f"File {ml_file.split('/')[-1]} already exists, skipping download")
            continue
        successful = False
        while not successful:
            try:
                fs.get(ml_file, ml_file.split("/")[-1])
                successful = True
            except Exception as e:
                print(f"Failed to download {ml_file}: {e}, retrying...")
    for pl_file in pl_files:
        if os.path.exists(pl_file.split("/")[-1]):
            print(f"File {pl_file.split('/')[-1]} already exists, skipping download")
            continue
        successful = False
        while not successful:
            try:
                fs.get(pl_file, pl_file.split("/")[-1])
                successful = True
            except Exception as e:
                print(f"Failed to download {pl_file}: {e}, retrying...")
    for sf_file in sf_files:
        if os.path.exists(sf_file.split("/")[-1]):
            print(f"File {sf_file.split('/')[-1]} already exists, skipping download")
            continue
        successful = False
        while not successful:
            try:
                fs.get(sf_file, sf_file.split("/")[-1])
                successful = True
            except Exception as e:
                print(f"Failed to download {sf_file}: {e}, retrying...")
    return

if __name__ == "__main__":
    for prefix, proc in [("bkr/dmi/harmonie_greenland_iceland.icechunk", process_timestep), ("bkr/dmi/harmonie_greenland_iceland_model_level.icechunk", process_ml_timestep)]:
        storage = icechunk.s3_storage(bucket="us-west-2.opendata.source.coop",
                                      prefix=prefix,

                                      region="us-west-2", )
        # storage = icechunk.local_filesystem_storage(store_path)
        repo = icechunk.Repository.open_or_create(
            storage, config=icechunk.RepositoryConfig.default()
        )
        times = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
        print(times)
        times = times.coords["time"].values
        # Now make that happen with fsspec and anonymous access
        # Only available for the last 2 days, so just check those
        fs = fsspec.filesystem("s3", anon=True)
        # Round now to the nearest 3 hours
        now = pd.Timestamp.now().floor("3H")
        date_range = pd.date_range(start=now - pd.Timedelta(days=5), end=now, freq="3H")
        date_dses = []
        first_write = False
        for date in date_range:
            if date in times:
                print(f"Data for init time {date} already exists, skipping...")
                continue
            download_forecast(date)
            init_time_dses = []
            for forecast_step in range(3):
                #ds = process_timestep(date, forecast_step)
                ds = proc(date, forecast_step)
                if ds is not None:
                    init_time_dses.append(ds)
            if len(init_time_dses) > 0:
                try:
                    ds = xr.concat(init_time_dses, dim="step").sortby("step")
                except:
                    continue
                ds = ds.expand_dims("time")
                # rename isobaricInhPa to level
                encoding = {var: {
                    "compressors": zarr.codecs.BloscCodec(cname="zstd", clevel=9, shuffle=zarr.codecs.BloscShuffle.bitshuffle)}
                            for var in ds.data_vars}
                encoding["time"] = {"units": "seconds since 1970-01-01"}
                storage = icechunk.s3_storage(bucket="us-west-2.opendata.source.coop",
                                              prefix=prefix,

                                              region="us-west-2", )
                # storage = icechunk.local_filesystem_storage(store_path)
                repo = icechunk.Repository.open_or_create(
                    storage, config=icechunk.RepositoryConfig.default()
                )
                if "model_level" in prefix:
                    ds = ds.chunk({"time": 1, "step": 1, "y": -1, "x": -1, "hybrid": -1})
                else:
                    ds = ds.rename({"isobaricInhPa": "level"})
                    ds = ds.chunk({"time": 1, "step": 1, "y": -1, "x": -1, "level": -1})
                session = repo.writable_session("main")
                if first_write:
                    print(ds)
                    to_icechunk(ds, session, encoding=encoding)
                    first_write = False
                else:
                    to_icechunk(ds, session, append_dim="time")
                session.commit(f"Writing HARMONIE data for init time {date}")
                # Make time a dimension
                #date_dses.append(ds)
                # Remove the files from the local disk
            else:
                continue

