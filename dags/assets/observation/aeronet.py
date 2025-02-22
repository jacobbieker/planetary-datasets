import datetime as dt
from typing import Optional

import pandas as pd
import xarray as xr
from monetio import aeronet


def get_aeronet_observations(
    start_time: dt.datetime,
    end_time: dt.datetime,
    parameter: Optional[str] = None,
    inversion: bool = False,
) -> xr.Dataset:
    """Fetch Aeronet data for a given day."""
    if parameter is not None:
        assert parameter in [
            "AOD10",
            "AOD15",
            "AOD20",
            "SDA10",
            "SDA15",
            "SDA20",
            "TOT10",
            "TOT15",
            "TOT20",
        ], ValueError(f"Parameter {parameter=} not recognized")
    dates = pd.date_range(start=start_time, end=end_time, freq="H")
    if inversion:
        df = aeronet.add_data(dates=dates, product="ALL", inv_type="ALM20")
    else:
        df = aeronet.add_data(dates=dates, product=parameter)
    ds = df.to_xarray()
    ds = ds.set_coords(("time", "siteid", "data_quality_level", "latitude", "longitude"))
    # Rename data variables to not have spaces
    ds = ds.rename_vars({k: k.replace(" ", "_") for k in ds.data_vars.keys()})
    return ds


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--start_time", type=str, required=True)
    parser.add_argument("--end_time", type=str, required=True)
    parser.add_argument("--parameter", type=str, required=True)
    parser.add_argument("--inversion", action="store_true")
    args = parser.parse_args()
    start_time = dt.datetime.strptime(args.start_time, "%Y-%m-%d")
    end_time = dt.datetime.strptime(args.end_time, "%Y-%m-%d")
    ds = get_aeronet_observations(
        start_time, end_time, parameter=args.parameter, inversion=args.inversion
    )
    ds.to_netcdf(f"{args.parameter}_{args.start_time}_{args.end_time}.nc")
