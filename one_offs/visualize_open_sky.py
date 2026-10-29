import glob
import os

import geopandas as gpd
import icechunk
import matplotlib.pyplot as plt
import pandas as pd
import xarray as xr

airmets = gpd.read_parquet(
    "/Users/jacob/Development/planetary-datasets/airmets_202201010000_202604010000.parquet"
)
sigmets = gpd.read_parquet(
    "/Users/jacob/Development/planetary-datasets/sigmets_200501010000_202604010000.parquet"
)
print(airmets)
print(sigmets)
print(airmets.columns)
print(sigmets.columns)
# Make those datetimes
sigmets["ISSUE"] = pd.to_datetime(sigmets["ISSUE"])
sigmets["EXPIRE"] = pd.to_datetime(sigmets["EXPIRE"])
airmets["VALID_FM"] = pd.to_datetime(airmets["VALID_FM"])
airmets["VALID_TO"] = pd.to_datetime(airmets["VALID_TO"])
# Make the above timezone-naive by converting to UTC and then removing the timezone information, as the flight times are also timezone-naive
sigmets["ISSUE"] = sigmets["ISSUE"].dt.tz_convert("UTC").dt.tz_localize(None)
sigmets["EXPIRE"] = sigmets["EXPIRE"].dt.tz_convert("UTC").dt.tz_localize(None)
airmets["VALID_FM"] = airmets["VALID_FM"].dt.tz_convert("UTC").dt.tz_localize(None)
airmets["VALID_TO"] = airmets["VALID_TO"].dt.tz_convert("UTC").dt.tz_localize(None)
print(sigmets["ISSUE"])
storage = icechunk.s3_storage(
    bucket="us-west-2.opendata.source.coop",
    prefix="bkr/dmi/meps.icechunk",
    access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    region="us-west-2",
)
# storage = icechunk.local_filesystem_storage(store_path)
repo = icechunk.Repository.open_or_create(storage, config=icechunk.RepositoryConfig.default())
meps = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)

lat_min = meps.latitude.min().values
lat_max = meps.latitude.max().values
lon_min = meps.longitude.min().values
lon_max = meps.longitude.max().values

# Change this to bounds for Europe
lat_min = 30
lat_max = 75
lon_min = -25
lon_max = 45

print(f"MEPS area: lat {lat_min} to {lat_max}, lon {lon_min} to {lon_max}")

files = glob.glob("/Volumes/T9/opensky_trajectories/*.nc")

# Split by velocity, if velocity is < 10 then most likely is not flying, or large change of velocity indicates takeoff or landing, so we can split the trajectory into different segments based on that
# Large changes in Altitude are the main one, so when the change in Altitude is more than 50% of a previous value, then we have the section for takeoff or landing, continue expanding section until the
# change in altitude stops being as severe
num_shown = 0
num_in_meps = 0
num_files = 0
for i, f in enumerate(files):
    num_files += 1
    ds = xr.open_dataset(f)
    # Check coordinates are within MEPS
    # print(ds)
    # Drop points outside MEPS area
    mask = (
        (ds.latitude >= lat_min)
        & (ds.latitude <= lat_max)
        & (ds.longitude >= lon_min)
        & (ds.longitude <= lon_max)
    ).load()
    ds = ds.where(mask, drop=True)
    # If no points, then skip
    if len(ds.latitude) < 1000:
        continue

    # Check if flight path goes near any active SIGMETs or AIRMETs
    # Select first on
    # Make a GeoSeries of the longitude and latitude points
    # Make into a series matching the length of sigments, for the while line each time
    temp_sigments = sigmets.cx[
        ds.longitude.min().values - 2 : ds.longitude.max().values + 2,
        ds.latitude.min().values - 2 : ds.latitude.max().values + 2,
    ]
    temp_airmets = airmets.cx[
        ds.longitude.min().values - 2 : ds.longitude.max().values + 2,
        ds.latitude.min().values - 2 : ds.latitude.max().values + 2,
    ]
    flight_times = pd.to_datetime(ds.time.values)

    # Fix the comparison below so that it doesn't throw the following error: TypeError: Invalid comparison between dtype=datetime64[ns, UTC] and Timestamp
    # Find ones active during the flight time, so where the start time is before the end of the flight and the end time is after the start of the flight
    temp_sigments = temp_sigments[
        (temp_sigments["ISSUE"] < flight_times[-1]) & (temp_sigments["EXPIRE"] > flight_times[0])
    ]
    # Find ones active during the flight time
    temp_airmets = temp_airmets[
        (temp_airmets["VALID_FM"] < flight_times[-1]) & (temp_airmets["VALID_TO"] > flight_times[0])
    ]
    if len(temp_sigments) == 0 and len(temp_airmets) == 0:
        continue
    num_in_meps += 1
    # continue
    # Get it by altitude
    altitude = ds.altitude.values
    # Only select ones where its a high change of vertical rate, so over 50
    # ds = ds.where(abs(altitude) > 25, drop=True)
    # altitude = ds.vertical_rate.values
    print(ds)
    # Split into different flight segments,
    plt.figure(figsize=(10, 5))
    # Plot the trajectory, coloring by altitude
    plt.scatter(ds.longitude, ds.latitude, c=altitude, cmap="viridis", alpha=0.5, s=1)
    plt.plot(ds.longitude, ds.latitude, color="gray", alpha=0.25, linewidth=0.5)
    # Plot the SIGMETs and AIRMETs as polygons
    if len(temp_sigments) > 0:
        temp_sigments.plot(
            ax=plt.gca(), facecolor="red", edgecolor="red", linewidth=2, alpha=0.25, label="SIGMET"
        )
    if len(temp_airmets) > 0:
        temp_airmets.plot(
            ax=plt.gca(),
            facecolor="blue",
            edgecolor="blue",
            linewidth=2,
            alpha=0.25,
            label="AIRMET",
        )
    # Limit the axes to the area around the flight path
    plt.xlim(ds.longitude.min().values - 1, ds.longitude.max().values + 1)
    plt.ylim(ds.latitude.min().values - 1, ds.latitude.max().values + 1)
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.title(f"Trajectory for {f}")
    plt.colorbar(label="Altitude (m)")
    plt.legend(loc="best")
    plt.show()
    num_shown += 1
    if num_shown > 20:
        exit()
print(f"Number of trajectories inside MEPS: {num_in_meps} out of {num_files}")
