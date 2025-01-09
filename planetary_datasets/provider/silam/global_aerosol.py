import xarray as xr

data = xr.open_dataset("/Users/jacob/Development/SILAM/silam_glob_v5_7_1_20241119_PM10_d1.nc")
print(data)
