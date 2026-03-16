import xarray as xr
import icechunk

def open_icechunk_store(storage_config: dict) -> icechunk.Repository:
    """
    Open an icechunk repository using the provided storage configuration.

    Args:
        storage_config (dict): Configuration for the icechunk storage.

    Returns:
        icechunk.Repository: The opened icechunk repository.
    """
    storage = icechunk.s3_storage(**storage_config)
    return icechunk.Repository.open(storage)

def open_xarray_from_icechunk(repo: icechunk.Repository) -> xr.Dataset:
    """
    Open an xarray dataset from an icechunk repository.

    Args:
        repo (icechunk.Repository): The icechunk repository.

    Returns:
        xr.Dataset: The xarray dataset.
    """
    session = repo.readonly_session("main")
    return xr.open_zarr(session.store, consolidated=False)

def get_latest_time_and_timestamps(ds: xr.Dataset) -> tuple:
    """
    Get the latest time and list of timestamps from an xarray dataset.

    Args:
        ds (xr.Dataset): The xarray dataset.

    Returns:
        tuple: A tuple containing the latest time and a list of timestamps.
    """
    time_dim = "init_time" if "init_time" in ds.dims else "time"
    timestamps = ds[time_dim].values
    latest_time = timestamps[-1]
    return latest_time, timestamps