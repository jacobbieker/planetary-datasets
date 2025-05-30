import xarray as xr
import numpy as np
import xarray as xr
import iris
import h5netcdf
import netCDF4
import rioxarray as rxr
import pandas as pd
import numpy as np
import icechunk
from icechunk.xarray import to_icechunk
import zarr
import tqdm
import h5py

def open_and_clean_uk_radar(filename: str) -> xr.Dataset:
    datatree = xr.open_datatree(filename, engine="h5netcdf", phony_dims='sort')
    data_attrs = datatree["/dataset1/data1/what"].attrs
    data = datatree["/dataset1/data1"].to_dataset()
    attributes = datatree["/where"].attrs
    attributes.update(data_attrs)
    data.attrs = attributes
    time = pd.Timestamp(f'{datatree["/what"].attrs["date"]}{datatree["/what"].attrs["time"]}')
    data["time"] = [time]
    # Convert rainfall_rate to float16
    data = data.rename({"data": "rainfall_rate"})
    data["rainfall_rate"] = data["rainfall_rate"].astype(np.float16)
    # Set nodata to NaN
    data["rainfall_rate"] = data["rainfall_rate"].where(data["rainfall_rate"] != -1.0, np.nan)
    # Add time as a coordinate
    data = data.assign_coords(time=data["time"])
    data = data.rename({"phony_dim_0": "y", "phony_dim_1": "x"})
    # Expand dims to add time
    for var in data.data_vars:
        data[var] = data[var].expand_dims("time")
    return data

def open_and_clean_fmi_radar(filename: str) -> xr.Dataset:
    data = rxr.open_rasterio(
       filename,
    )
    # Convert GDAL metadata into a time and unit
    timestamp = pd.Timestamp(data.attrs["GDAL_METADATA"].split('hhmm">')[1].split("</")[0])
    gain = float(data.attrs["GDAL_METADATA"].split('Gain">')[1].split('</')[0])
    accum_hour = str(int(data.attrs["GDAL_METADATA"].split('Accumulation time" unit="h">')[1].split('</')[0]))
    data_name = f"rainfall_rate_accumulation_{accum_hour}h"
    # Convert to xarray dataset
    data = data.to_dataset(name=data_name)
    data[data_name] = data[data_name].where(data[data_name] != 65535, np.nan).astype(np.float32)
    data = data.sel(band=1).drop_vars("band")  # Select the first band
    data[data_name] = (data[data_name] * gain).astype(np.float16)  # Apply gain
    # Add the datetime
    data = data.assign_coords(time=timestamp)
    # Add time to all data vars
    for var in data.data_vars:
        data[var] = data[var].expand_dims("time")
    return data

