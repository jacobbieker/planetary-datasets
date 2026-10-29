import os

import icechunk
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr

flights = xr.open_dataset(
    "/Users/jacob/Development/planetary-datasets/amdar_observations_andoya_nearby.nc"
)
# Slice on time, with mask
start_time = np.datetime64("2025-01-31T00:00:00")
end_time = np.datetime64("2026-04-01T02:00:00")
mask = ((flights.time >= start_time) & (flights.time <= end_time)).load()
flights = flights.where(mask, drop=True)

# Also only keep those with an elevation above 100m
flights = flights.where(flights.elevation > 100, drop=True)
# Only keep those with a max elevation of 7000m
flights = flights.where(flights.elevation < 7000, drop=True)
# Get all unique station IDs
unique_station_ids = np.unique(flights.station_id.values)
print(f"len(unique_station_ids): {len(unique_station_ids)}")
# print(unique_station_ids)
# Print all ones that start with flight number
flight_numbers = [s for s in unique_station_ids if s.startswith("EU")]
print(f"len(flight_numbers): {len(flight_numbers)}")

# only keep ones where the flight number is in the list of flight numbers that start with EU, and drop the rest
flights = flights.where(flights.station_id.isin(flight_numbers), drop=True)
# Unique observation types
unique_observation_types = np.unique(flights.observation_type.values)
print(f"Unique observation types: {unique_observation_types}")
observations_to_keep = [
    "derived_dewpoint_observation__kelvin",
    "derived_mixing_ratio_observation__kg_per_kg",
    "derived_relative_humidity_observation__percent",
    "derived_wind_direction_observation__degrees",
    "derived_wind_speed_observation__m_per_s",
    "temperature_observation__kelvin",
    "u_component_wind_observation__m_per_s",
    "v_component_wind_observation__m_per_s",
]
flights = flights.where(flights.observation_type.isin(observations_to_keep), drop=True)
# print(flight_numbers)
# Now, get all observations for a single flight number, and plot the lat/lon in time order
# Get min and max lat and lon for all flight numbers
min_lat = flights.latitude.min().values
max_lat = flights.latitude.max().values
min_lon = flights.longitude.min().values
max_lon = flights.longitude.max().values

# Only take min and max elevation from this
min_elevation = flights.elevation.min().values
max_elevation = flights.elevation.max().values
print(
    f"Min lat: {min_lat}, Max lat: {max_lat}, Min lon: {min_lon}, Max lon: {max_lon} Min elevation: {min_elevation}, Max elevation: {max_elevation}"
)

# Now get the valid time ranges, and find where it overlaps with the MEPS data


storage = icechunk.s3_storage(
    bucket="us-west-2.opendata.source.coop",
    prefix="bkr/dmi/meps.icechunk",
    access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    region="us-west-2",
)
# storage = icechunk.local_filesystem_storage(store_path)
repo = icechunk.Repository.open_or_create(storage, config=icechunk.RepositoryConfig.default())
ds = (
    xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    .drop_duplicates("time")
    .sortby("time")
)
print(ds)
# Select only icing index
ds = ds[["icing_index"]]
print(ds["icing_index"].attrs)
print(ds)
# Slice only around the min and max lat and lon of the flight observations, with a buffer of 0.25 degree
lat_buffer = 0.25
lon_buffer = 0.25
# Use a where clause as latitude and longitude are 2D
mask = (
    (ds.latitude >= min_lat - lat_buffer)
    & (ds.latitude <= max_lat + lat_buffer)
    & (ds.longitude >= min_lon - lon_buffer)
    & (ds.longitude <= max_lon + lon_buffer)
).load()
ds = ds.where(mask, drop=True)
print(ds)
# Save the short cut locally
ds.to_netcdf("meps_icing_index_subset.nc")
exit()
# Mask based on height above ground, only keep levels that are between the min and max elevation of the flight observations, with a buffer of 500 m
# height_buffer = 500
# mask = ((ds.height >= min_elevation - height_buffer) & (ds.height <= max_elevation + height_buffer)).load()
# ds = ds.where(mask, drop=True)
# Take 100 random times, and get icing_index min, max, mean, and std per height for those times
# times = np.random.choice(ds.time.values, size=1000, replace=False)
ds_subset = ds  # .sel(time=times)
print(ds_subset)
print(ds_subset.height.values)
# mins = ds_subset.min(dim=["time", "x", "y"]).load()
# maxs = ds_subset.max(dim=["time", "x", "y"]).load()
# print("Min:")
# print(mins["icing_index"].values)
# print("Max:")
# print(maxs["icing_index"].values)
# means = ds_subset.mean(dim=["time", "x", "y"]).load()
# stds = ds_subset.std(dim=["time", "x", "y"]).load()
# print("Means:")
# print(means["icing_index"].wvalues)
# print("Stds:")
# print(stds["icing_index"].values)

# Better way might be going through MEPS and finding corresponding flights when there is icing going on, looking at 6 hourly chunks
# If MEPS doesn't exist with 6 hourly chunks, then skip it
for time in ds_subset.time.values[::3]:  # Every 3rd one, as that is new init time
    ds_subset_time = ds_subset.sel(time=slice(time, time + np.timedelta64(6, "h")))
    if len(ds_subset_time.time) < 6:
        print(f"Skipping time {time} because it doesn't have 6 hourly chunks")
        continue
    # Now check if icing is going on in this time period, by checking if the max icing index is above 2 for at least 2 timesteps
    icing_check = ds_subset_time.max(dim=["x", "y", "height"]).load()
    icing_times = icing_check.where(icing_check.icing_index > 2, drop=True).time
    if len(icing_times) < 2:
        continue
    # Now there is icing in at least one place across 2 timesteps, so find any flights that overlap with this time period
    mask = ((flights.time >= time) & (flights.time <= time + np.timedelta64(6, "h"))).load()
    flights_subset = flights.where(mask, drop=True)
    if len(flights_subset.time) == 0:
        continue
    # Now announce which flights overlap with this time period, and the max icing index during this time period
    print(
        f"Time period {time} to {time + np.timedelta64(6, 'h')} has max icing index of {icing_check.icing_index.max().values}, and the following flights overlap with this time period:"
    )
    print(np.unique(flights_subset.station_id.values))
    print("Each flight has:")
    for flight_number in np.unique(flights_subset.station_id.values):
        flight_ds = flights_subset.where(flights_subset.station_id == flight_number, drop=True)
        # Only do this if there is at least 2 unique flight times
        if len(np.unique(flight_ds.time.values)) < 2:
            continue
        print(
            f"Flight {flight_number} has {len(flight_ds.time)} observations between {flight_ds.time.min().values} and {flight_ds.time.max().values}"
        )
        # See if the flight intersects with any of the icing_index > 2 latitude and longitude points?
        # Plot one of the timesteps of MEPS data with the highest icing index
        meps_forecast = ds_subset_time.sel(time=icing_times[0])
        # Plot the icing indexes for this timestep, one panel for each height level, with the flight path overlaid on top of each of them
        meps_forecast = meps_forecast.sortby("height")
        # meps_forecast = meps_forecast.where(meps_forecast.icing_index > 2)
        fig, axes = plt.subplots(nrows=2, ncols=5, figsize=(20, 10))
        for i, height in enumerate(meps_forecast.height.values):
            ax = axes[i // 5, i % 5]
            meps_forecast_height = meps_forecast.sel(height=height)
            im = ax.imshow(
                meps_forecast_height.icing_index.values,
                origin="lower",
                extent=(
                    meps_forecast_height.longitude.min().values,
                    meps_forecast_height.longitude.max().values,
                    meps_forecast_height.latitude.min().values,
                    meps_forecast_height.latitude.max().values,
                ),
                vmin=0,
                vmax=4,
            )
            ax.set_title(f"Height: {height:.2f} m")
            ax.set_xlabel("Longitude")
            ax.set_ylabel("Latitude")
            # Only add colorbar to the ones on the end
            if i % 5 == 4:
                # Add a title to the colorbar saying "MEPS Icing Index"
                plt.colorbar(im, ax=ax, label="MEPS Icing Index")
            # Get flight elevation and change color to bright yellow if the flight elevation near a height level, within 500m, or the last one if above 6000m
            flight_elevation = flight_ds.elevation.mean().values
            if height > 5999 and flight_elevation > 6000:
                flight_color = "pink"
            elif abs(flight_elevation - height) <= 500:
                flight_color = "pink"
            else:
                flight_color = "white"
            # Add flight path on top of this, only for the points that are within the lat/lon range of the MEPS data
            ax.plot(flight_ds.longitude, flight_ds.latitude, linewidth=4, color=flight_color)
        # add overall title to the figure with the time period and flight height
        fig.suptitle(
            f"MEPS Icing Index from {time} to {time + np.timedelta64(6, 'h')} with Flight {flight_number} Path Overlayed",
            fontsize=16,
        )
        plt.show()


exit()
# 0 is none, 1 is traces, 2 is feeble, 3 is moderate, and 4 is severe icing, so anything above 2 is moderate or severe icing.
# Now find flights where the icing index is above 2 for at least 3 consecutive time steps, and plot the lat/lon of those flights
for flight in unique_station_ids:
    flight_ds = flights.where(flights.station_id == flight, drop=True)
    # For this, drop duplicate times, if there are any, by keeping the first one
    flight_ds = flight_ds.drop_duplicates("time", keep="first")
    # Only plot ones that have more than 5 observations
    if len(flight_ds.station_id) < 5:
        continue
    # Only keep ones within 1 day of the first one, rolling through them all
    for first_time in flight_ds.time.values:
        mask = (
            (flight_ds.time >= first_time) & (flight_ds.time <= first_time + np.timedelta64(1, "D"))
        ).load()
        flight_ds_subset = flight_ds.where(mask, drop=True)
        # Now find where the icing index is above 2 for at least 3 consecutive time steps for this flight
        # Select ds subset based on start and end of flight_ds time + 1 hour on either side
        mask = (
            (ds_subset.time >= flight_ds_subset.time.min() - np.timedelta64(1, "h"))
            & (ds_subset.time <= flight_ds_subset.time.max() + np.timedelta64(1, "h"))
        ).load()
        flight_subset = ds_subset.where(mask, drop=True)
        print(
            f"Flight {flight} has {len(flight_ds_subset.time)} observations between {flight_ds_subset.time.min().values} and {flight_ds_subset.time.max().values}, and the corresponding MEPS data has {len(flight_subset.time)} time steps"
        )
        if len(flight_subset.time) == 0:
            continue
        mask = (ds_subset.icing_index > 2).rolling(time=3).sum() >= 2
        mask = mask.where(mask, drop=True)
        if len(mask.time) > 0:
            print(f"Flight {flight} has icing index above 2 for at least 3 consecutive time steps")
            print(mask)
