from collections import defaultdict

import cfgrib
import pandas as pd
import polars
import xarray as xr

df = polars.read_parquet("/Users/jacob/Development/part-00000*.snappy.parquet")
# Drop any that have no lat or lon, as we need that at the very least
df = df.drop_nulls(subset=["lat", "lon"])
print(df)
print(df.columns)
# Collate by the icao24 identifier, which is the unique identifier for each flight, and create a new dataframe where each row is a flight and the columns are lists of the different variables for that flight
grouped = df.group_by("icao24")
print(grouped)
for icao24, group in grouped:
    # Convert times to datetime objects
    times = pd.to_datetime(group["time"], unit="s")
    ds = xr.Dataset(
        coords={"time": times, "icao24": icao24[0]},
        data_vars={
            "latitude": ("time", group["lat"]),
            "longitude": ("time", group["lon"]),
            "altitude": ("time", group["geoAltitude"]),
            "velocity": ("time", group["velocity"]),
            "heading": ("time", group["heading"]),
            "vertical_rate": ("time", group["vertRate"]),
            "on_ground": ("time", group["onGround"]),
            "alert": ("time", group["alert"]),
            "barometer_altitude": ("time", group["baroAltitude"]),
        },
    )
    print(ds)
    # Save to disk
    ds.to_netcdf(
        f"/Users/jacob/Development/opensky_trajectories/opensky_trajectories_03-01-2026_{icao24[0]}.nc",
        mode="w",
    )
exit()

ds = cfgrib.open_datasets("/Users/jacob/Development/hrrr.t00z.wrfsubhf00.ak.grib2")
print(ds)
print(len(ds))
ds2 = cfgrib.open_datasets("/Users/jacob/Development/hrrr.t00z.wrfsubhf01.ak.grib2")
print(ds)
print(len(ds))
