import xarray as xr
from satpy import Scene
import numpy as np
import pandas as pd
import zarr
import pyresample
import datetime as dt
from typing import Any
import yaml



def process_goes(goes_files: str, variables_to_load: list[str]) -> xr.Dataset:
    scn = Scene(
        reader="abi_l1b",
        filenames=goes_files,
        reader_kwargs={"storage_options": {"anon": True}},
    )
    scn.load(variables_to_load)
    dataset = scn.to_xarray_dataset().astype(np.float16)
    """
    longitude, latitude = scn[variables_to_load[0]].attrs["area"].get_lonlats()
    lat_da = xr.DataArray(
        latitude,
        dims=("x", "y"),
    ).astype(np.float32)
    lon_da = xr.DataArray(
        longitude,
        dims=("x", "y"),
    ).astype(np.float32)
    dataset['longitude'] = lon_da
    dataset['latitude'] = lat_da
    """
    start_time = pd.Timestamp(dataset.attrs['start_time'])
    end_time = pd.Timestamp(dataset.attrs['end_time'])
    # Get the middle time of two times
    mid_time = start_time + (end_time - start_time) / 2
    dataset.coords['time'] = mid_time
    dataset = dataset.expand_dims("time")
    dataset["orbital_parameters"] = xr.DataArray(
        [dataset.attrs["orbital_parameters"]],
        dims=("time",),
    ).astype(f"U{len(dataset.attrs['orbital_parameters'])}")
    dataset["area"] = xr.DataArray(
        [str(dataset.attrs["area"])],
        dims=("time",),
    ).astype(f"U{len(str(dataset.attrs['area']))}")
    dataset["start_time"] = xr.DataArray(
        [start_time],
        dims=("time",),
    )
    dataset["end_time"] = xr.DataArray(
        [end_time],
        dims=("time",),
    )
    # Add x and y coords per time as well, so that the exact location can be reconstructed
    dataset["x_geostationary_coord"] = xr.DataArray(
        [dataset.x.values],
        dims=("time", "x"),
        coords={"time": dataset.time, "x": dataset.x},
    )
    dataset["y_geostationary_coord"] = xr.DataArray(
        [dataset.y.values],
        dims=("time", "y"),
        coords={"time": dataset.time, "y": dataset.y},
    )

    def _serialize(d: dict[str, Any]) -> dict[str, Any]:
        sd: dict[str, Any] = {}
        for key, value in d.items():
            if isinstance(value, dt.datetime):
                sd[key] = value.isoformat()
            elif isinstance(value, bool | np.bool_):
                sd[key] = str(value)
            elif isinstance(value, pyresample.geometry.AreaDefinition):
                sd[key] = yaml.load(value.dump(), Loader=yaml.SafeLoader)  # type:ignore
            elif isinstance(value, dict):
                sd[key] = _serialize(value)
            else:
                sd[key] = str(value)
        return sd

    dataset.attrs = _serialize(dataset.attrs)
    for var in dataset.data_vars:
        dataset[var].attrs = _serialize(dataset[var].attrs)
    dataset = dataset.drop_vars("crs")
    dataset = dataset.chunk({"time": 1, "y": -1, "x": -1})
    return dataset

high_res = ["/Users/jacob/Development/planetary-datasets/OR_ABI-L1b-RadF-M6C02_G19_s20250040300203_e20250040309512_c20250040309556.nc",]
high_channel = ["C02"]
med_res = ["/Users/jacob/Development/planetary-datasets/OR_ABI-L1b-RadF-M6C01_G19_s20250040300203_e20250040309511_c20250040309552.nc",
            "/Users/jacob/Development/planetary-datasets/OR_ABI-L1b-RadF-M6C03_G19_s20250040300203_e20250040309511_c20250040309543.nc",
           "/Users/jacob/Development/planetary-datasets/OR_ABI-L1b-RadF-M6C05_G19_s20250040300203_e20250040309512_c20250040309567.nc"]
med_channels = ["C01", "C03", "C05"]
high_ds = process_goes(high_res, high_channel)
med_ds = process_goes(med_res, med_channels)
print(high_ds)
print(med_ds)

names = ["high_res", "med_res"]
for i, ds in enumerate([high_ds, med_ds]):
    encoding = {
            "time": {
                "units": "milliseconds since 1970-01-01",
                "calendar": "standard",
                "dtype": "int64",
            }
        }
    variables = []
    for var in ds.data_vars:
        if var not in ["orbital_parameters", "start_time", "end_time", "area"]:
            variables.append(var)
    encoding.update({
        v: {"compressors": zarr.codecs.BloscCodec(cname='zstd', clevel=9, shuffle=zarr.codecs.BloscShuffle.bitshuffle)}
        for v in variables})

    ds.chunk({"time": 1, "x": -1, "y": -1}).to_zarr(f"{names[i]}_comp9_nolonlat.zarr", mode="w", compute=True, zarr_format=3, encoding=encoding)
    encoding.update({
        v: {"compressors": zarr.codecs.BloscCodec(cname='zstd', clevel=5, shuffle=zarr.codecs.BloscShuffle.bitshuffle)}
        for v in variables})
    ds.chunk({"time": 1, "x": -1, "y": -1}).to_zarr(f"{names[i]}_comp5_nolonlat.zarr", mode="w", compute=True, zarr_format=3, encoding=encoding)

# So need to do it for GOES-East, and GOES-West