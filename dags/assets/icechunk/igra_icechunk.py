import xarray as xr
import icechunk
from icechunk.xarray import to_icechunk
import zarr
import glob
import numpy as np
import igra
import warnings

warnings.filterwarnings('ignore')


stations = igra.read.stationlist('/Users/jacob/Development/planetary-datasets/dags/assets/icechunk/igra2-station-list.txt')
print(stations)
# Get all ones that end in 2025
stations = stations[stations.end >= 2020]
stations = stations[stations.total > 100]  # Only keep stations with more than 100 reports
# get only ones that are after RSM00031300 index
#stations = stations[stations.index > "CIM00085469"]
# Remove any station with NaN lat/lon
stations = stations.dropna(subset=['lat', 'lon'])
print(stations)

# Get all the values for the index "id" in the stations DataFrame
for station in stations.index:
    try:
        igra.download.station(station, "IGRA")
    except Exception as e:
        print(f"Error downloading station {station}: {e}")
        continue
dses = []
station_dses = []
times = []

for station in stations.index: # Do 50 at a time to avoid memory issues, saving to disk each time
    try:
        data, station = igra.read.igra(station, f"IGRA_2020/{station}-data.txt.zip")
        print(data)
        times += data.date.values.tolist()
        #dses.append(data)
        #station_dses.append(station)
    except KeyError:
        print(f"KeyError for station {station}, skipping...")
        continue

times = sorted(times)
# Remove duplicates from times
times = sorted(list(set(times)))
np.save("igra_2020_times.npy", times)

# Read the times from the saved file
times = np.load("igra_2020_times.npy", allow_pickle=True)
for t in times:
    t = str(t)
    # Convert to datetime64[ns] if not already
    if isinstance(t, str):
        t = np.datetime64(t)
    elif isinstance(t, np.datetime64):
        pass  # Already in the correct format
    else:
        raise ValueError(f"Unexpected time format: {t}")
t_da = xr.DataArray(times, dims=["date"], coords={"date": times.astype("datetime64[ns]")}).astype("datetime64[ns]")
print(t_da)
for station in stations.index:
    try:
        data, station_data = igra.read.igra(station, f"IGRA_2020/{station}-data.txt.zip")
        data.coords["station"] = xr.DataArray(station)
        data["lat"] = station_data.lat
        data["lon"] = station_data.lon
        data["numlev"] = station_data.numlev
        data = data.rename(
            {"pres": "level", "gph": "geopotential_height", "temp": "temperature", "rhumi": "relative_humidity",
             "windd": "wind_direction", "winds": "wind_speed", "dpd": "dew_point_depression", })
        data = data.rename(
            {"date": "time", "numlev": "number_of_measured_levels", "lat": "latitude", "lon": "longitude"})
        data = data.expand_dims("station")
        # Now align to the times
        data, _ = xr.align(data, t_da, join="outer", fill_value=np.nan)
        # Save to disk
        data.to_netcdf(f"/Volumes/T7 Shield/IGRA_XR/{station}.nc", mode="w")
    except Exception as e:
        print(f"{e} for station {station}, skipping...")
        continue

import xarray as xr
import icechunk as icechunk
from icechunk.xarray import to_icechunk
import glob
import zarr
import numpy as np
files = sorted(list(glob.glob("/Volumes/T7 Shield/IGRA_XR/*.nc")))

for i in range(0, len(files), 50):
    ds = xr.open_mfdataset(files[i:i+50], combine="nested", concat_dim="station", parallel=True).sortby("time")
    print(ds)
    ds = ds.chunk({"station": 1, "time": 10000})
    ds.to_netcdf(f"/Volumes/T7 Shield/IGRA_XR2/{i}.nc")

files = sorted(list(glob.glob("/Volumes/T7 Shield/IGRA_XR2/*.nc")))
ds = xr.open_mfdataset(files, combine="nested", concat_dim="station", parallel=True).sortby("time").chunk({"station": -1, "time": 100}).astype(np.float32)
print(ds)
storage = icechunk.local_filesystem_storage("igra_v2_16_levels.icechunk")
repo = icechunk.Repository.open_or_create(storage)
session = repo.writable_session("main")
encoding = {
                "time": {
                    "units": "milliseconds since 1970-01-01",
                    "calendar": "standard",
                    "dtype": "int64",
                }
            }
variables = []
for var in ds.data_vars:
    if var not in ["orbital_parameters", "start_time", "end_time", "area"]:
        variables.append(var)
encoding.update({
    v: {"compressors": zarr.codecs.BloscCodec(cname='zstd', clevel=9,
                                              shuffle=zarr.codecs.BloscShuffle.bitshuffle)}
    for v in variables})
to_icechunk(ds, session, encoding=encoding)
session.commit("Add all data for all sites with end date >= 2020")


