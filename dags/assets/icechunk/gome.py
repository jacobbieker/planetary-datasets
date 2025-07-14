import harp
import xarray as xr

product = harp.import_product("/Users/jacob/Downloads/GOME_xxx_1B_M01_20250714061755Z_20250714075955Z_N_O_20250714070837Z/GOME_xxx_1B_M01_20250714061755Z_20250714075955Z_N_O_20250714070837Z.nat")
print(product)
harp.export_product(product, "gome_harp.nc")
ds = xr.open_dataset("gome_harp.nc")
print(ds)
