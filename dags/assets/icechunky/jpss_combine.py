import xarray as xr


ds = xr.open_mfdataset("/home/jacob/Elements/jpss_atms*.zarr", engine="zarr", concat_dim="time", combine="nested")
print(ds)