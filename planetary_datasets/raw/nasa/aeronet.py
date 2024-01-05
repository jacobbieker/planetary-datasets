import monetio
import pandas as pd
import xarray as xr
from monetio import aeronet
import datetime as dt

def get_aeronet_observations(start_time: dt.datetime, end_time: dt.datetime, parameter: str = "AOD10", inversion: bool = False) -> xr.Dataset:
    """Fetch Aeronet data for a given day."""
    assert parameter in ["AOD10", "AOD15", "AOD20", "SDA10", "SDA15", "SDA20", "TOT10", "TOT15", "TOT20"], ValueError(f"Parameter {parameter=} not recognized")
    dates = pd.date_range(start=start_time, end=end_time, freq='H')
    if inversion:
        df = aeronet.add_data(dates=dates, product='ALL', inv_type='ALM20')
    else:
        df = aeronet.add_data(dates=dates, product=parameter)
    ds = df.to_xarray()
    ds = ds.set_coords(("time", "siteid", "data_quality_level", "latitude", "longitude"))
    # Rename data variables to not have spaces
    ds = ds.rename_vars({k: k.replace(" ", "_") for k in ds.data_vars.keys()})
    return ds


if __name__ == "__main__":
    pass