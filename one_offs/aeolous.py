import xarray as xr

ds = xr.open_datatree("/Volumes/T7/AE_OPER_ALD_U_N_2B_20201001T000000_20201003T000000_vires.nc")
print(ds)
print(ds.data_vars)
