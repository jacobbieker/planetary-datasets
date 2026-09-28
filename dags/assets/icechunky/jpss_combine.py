import icechunk
import xarray as xr
import zarr
import glob


ds = xr.open_mfdataset("/home/jacob/Elements/jpss_atms*.zarr", engine="zarr", concat_dim="time", combine="nested")
print(ds)