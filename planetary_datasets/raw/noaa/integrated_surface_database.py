import monetio
import pandas as pd
import xarray as xr
from monetio.obs import ish
import datetime as dt
import s3fs


def get_isd_observations_monetio(
    start_time: dt.datetime, end_time: dt.datetime, resample: bool = False
) -> xr.Dataset:
    """Fetch ISD observations data for a given day using Monetio (Can be quite slow)."""
    dates = pd.date_range(start=start_time, end=end_time, freq="D")
    df = ish.add_data(dates, resample=resample, window="1H")
    ds = df.to_xarray()
    ds = ds.rename_vars({k: k.replace(" ", "_") for k in ds.data_vars.keys()})
    return ds


def get_isd_observations(start_time: dt.datetime, end_time: dt.datetime) -> xr.Dataset:
    """Fetch ISD observations data for a given day from AWS."""
    dates = pd.date_range(start=start_time, end=end_time, freq="D")
    df = ish.add_data(dates, resample=False, window="1H")
    ds = df.to_xarray()
    ds = ds.rename_vars({k: k.replace(" ", "_") for k in ds.data_vars.keys()})
    return ds


if __name__ == "__main__":
    print(get_isd_observations(dt.datetime(2021, 7, 13), dt.datetime(2021, 7, 14)))
