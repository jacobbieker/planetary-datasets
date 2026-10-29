import cfgrib
import xarray as xr

ds = cfgrib.open_datasets("/Volumes/T7/90l.2025082118.hfsa.storm.atm.f000.grb2")
print(ds)
exit()

ds = xr.open_dataset("/Volumes/T7/90w.2025082118.hfsa.mom6.f000.nc")
print(ds)
print(ds.data_vars)
for var in ds.data_vars:
    long_name = (
        ds[var]
        .attrs.get("long_name", var)
        .lower()
        .replace(" ", "_")
        .replace("(", "")
        .replace(")", "")
    )
    print(f"{var} -> {long_name}")
    long_name = f"{long_name}_at_pressure"
    ds = ds.rename({var: long_name})
