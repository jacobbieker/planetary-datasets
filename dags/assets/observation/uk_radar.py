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

data = h5py.File("/Users/jacob/Development/planetary-datasets/202505292000_ODIM_ng_radar_rainrate_composite_1km_UK.h5", 'r')
print(data.keys())
exit()

data = xr.open_dataset("/Users/jacob/Development/planetary-datasets/202505292000_ODIM_ng_radar_rainrate_composite_1km_UK.h5", engine="h5netcdf")
print(data)

exit()

data = xr.open_dataset("/Users/jacob/Development/planetary-datasets/nsrdb_india_2014.h5")
print(data.time_index.values)

def open_and_clean_india_nsrdb(filename: str) -> xr.Dataset:
    data = xr.open_dataset(filename)
    # Set phony_dim_0 to time_index
    data["phony_dim_0"] = data["time_index"]
    # Set time_index to be the time coordinate
    data = data.set_coords("phony_dim_0")
    data = data.rename({"phony_dim_0": "time"})
    # Drop time_index
    data = data.drop_vars("time_index")
    # Convert 'time' coordinate to datetime64
    data["time"] = [pd.to_datetime(t).to_datetime64() for t in data["time"].values]
    # Make 'coordinates' a coordinate
    data = data.set_coords("coordinates")
    data = data.rename({"phony_dim_1": "index", "phony_dim_2": "latitude_longitude"})
    # Index the latitude and longitude on 'index'
    return data


dses = []
storage = icechunk.local_filesystem_storage("nsrdb_india.icechunk")
repo = icechunk.Repository.create(storage)
for i, year in tqdm.tqdm(enumerate(range(2000, 2015))):
    data = open_and_clean_india_nsrdb(f"/Users/jacob/Development/planetary-datasets/nsrdb_india_{year}.h5").chunk({"time": 1, "index": -1, "latitude_longitude": -1})
    if i > 0:
        session = repo.writable_session("main")
        to_icechunk(data.chunk({"time": 1, "index": -1, "latitude_longitude": -1}), session, append_dim='time')
        print(session.commit(f"add {data.time.values} data to store"))
    else:
        variables = list(data.data_vars)
        encoding = {
            v: {"compressors": zarr.codecs.BloscCodec(cname='zstd', clevel=9,
                                                      shuffle=zarr.codecs.BloscShuffle.bitshuffle)}
            for v in variables}
        encoding["time"] = {"units": "nanoseconds since 1970-01-01"}
        session = repo.writable_session("main")
        to_icechunk(data.chunk({"time": 1, "index": -1, "latitude_longitude": -1}), session, encoding=encoding)
        print(session.commit(f"add {data.time.values} data to store"))

exit()

data = rxr.open_rasterio(
    "/Users/jacob/Development/planetary-datasets/202102020100_FIN-ACRR24H-3067-1KM.tif",
)
print(data)
print(data.attrs)
# Convert GDAL metadata into a time and unit
timestamp = pd.Timestamp(data.attrs["GDAL_METADATA"].split('hhmm">')[1].split("</")[0])
print(timestamp)
gain = float(data.attrs["GDAL_METADATA"].split('Gain">')[1].split('</')[0])
print(gain)
accum_hour = str(int(data.attrs["GDAL_METADATA"].split('Accumulation time" unit="h">')[1].split('</')[0]))
data_name = f"rainfall_rate_accumulation_{accum_hour}h"
# Convert to xarray dataset
data = data.to_dataset(name=data_name)
# Set 65535 to NaN
data[data_name] = data[data_name].where(data[data_name] != 65535, np.nan).astype(np.float32)
data = data.sel(band=1).drop_vars("band")  # Select the first band
data[data_name] = (data[data_name] * gain).astype(np.float16)  # Apply gain
# Add the datetime
data = data.assign_coords(time=timestamp)
# Add time to all data vars
for var in data.data_vars:
    data[var] = data[var].expand_dims("time")
print(data)

# Calculate the latitude and longitude from the affine transform

exit()
#data = xr.open_dataset(
#    "/Users/jacob/Development/planetary-datasets/202505292000_ODIM_ng_radar_rainrate_composite_1km_UK.h5",
#    engine="netcdf4",
#)

data = xr.open_dataset("/Users/jacob/Development/planetary-datasets/202505292000_ODIM_ng_radar_rainrate_composite_1km_UK.h5", engine="h5netcdf")
print(data)
