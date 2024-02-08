import xarray as xr
import cfgrib
import datetime as dt


def get_hrrr(time: dt.datetime, forecast_timesteps: list[int], variables=None, source="aws"):
    """
    Get the HRRR data for the given time period

    Args:
        time: datetime object
        forecast_timesteps: list of forecast timesteps to include
        variables: list of variables to include, if None, all variables are included

    Returns:
        xarray object with HRRR data
    """
    assert source in ["aws", "azure", "gcp"], ValueError(f"Source {source=} not recognized")
    # Uses the Kerchunk generated in conversion/hrrr.py, and switches between AWS, Azure, and GCP sources of HRRR
    # Kerchunk was generated with the AWS dataset, so if things are missing, should use that one instead.
    return NotImplementedError("HRRR is not yet implemented")
