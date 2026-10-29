import matplotlib.pyplot as plt
import xarray as xr

meps_grid = xr.open_dataset(
    "/Users/jacob/Development/planetary-datasets/nordic_analysis/met_analysis_1_0km_nordic_20260209T01Z.nc"
)
radar_grid = xr.open_dataset(
    "/Users/jacob/Development/planetary-datasets/nordic_reflectivity/yrwms-nordic.mos.pcappi-0-dbz.noclass-clfilter-novpr-clcorr-block.nordiclcc-1000.20260322.nc"
)
radar_grid = radar_grid.rename({"lat": "latitude", "lon": "longitude"})
andoya_latitude_longitude = [69.114, 15.7679]
meps_grid = meps_grid.where(
    (meps_grid.latitude >= andoya_latitude_longitude[0] - 1)
    & (meps_grid.latitude <= andoya_latitude_longitude[0] + 1)
    & (meps_grid.longitude >= andoya_latitude_longitude[1] - 1)
    & (meps_grid.longitude <= andoya_latitude_longitude[1] + 1),
    drop=True,
)
radar_grid = radar_grid.where(
    (radar_grid.latitude >= andoya_latitude_longitude[0] - 1)
    & (radar_grid.latitude <= andoya_latitude_longitude[0] + 1)
    & (radar_grid.longitude >= andoya_latitude_longitude[1] - 1)
    & (radar_grid.longitude <= andoya_latitude_longitude[1] + 1),
    drop=True,
)
# Select around Andoya
print(meps_grid)
print(radar_grid)

# Plot latitude and longitude of both grids to see how they compare
plt.figure(figsize=(10, 10))
plt.scatter(meps_grid.longitude, meps_grid.latitude, label="MEPS", alpha=0.5)
plt.scatter(radar_grid.longitude, radar_grid.latitude, label="Radar", alpha=0.5)
plt.legend()
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.title("MEPS vs Radar Grid Comparison")
plt.show()
