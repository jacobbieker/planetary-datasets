import xarray as xr
import pystac_client
import planetary_computer
import datetime as dt
import odc.stac


def get_mrms_mosaic(
    start_datetime: dt.datetime,
    end_datetime: dt.datetime,
    mosaic_type: str = "1HPass1",
    area: str = "CONUS",
) -> xr.Dataset:
    """
    Get the GOES images for the given time period

    Args:
        start_datetime: datetime to start getting images from
        end_datetime: End datetime to get images from
        mosaic_type: Which GOES image type to get, for 1HPass1, 1HPass2, or 24HPass2
        area: Which area to get, "ALASKA", "CARIB", "CONUS", "GUAM", or "HAWAII"


    Returns:
        Xarray Dataset containing the MRMS mosaics
    """
    assert mosaic_type in ["1HPass1", "1HPass2", "24HPass2"], ValueError(
        f"Image type {mosaic_type=} not recognized"
    )
    if mosaic_type == "1HPass1":
        collection = "noaa-mrms-qpe-1h-pass1"
    if mosaic_type == "1HPass2":
        collection = "noaa-mrms-qpe-1h-pass2"
    elif mosaic_type == "24HPass2":
        collection = "noaa-mrms-qpe-24h-pass2"
    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
    )
    search = catalog.search(
        collections=[collection],
        datetime=[start_datetime, end_datetime],
        query={"noaa_mrms_qpe:region": {"eq": area}},
    )
    items = list(search.get_items())
    ds = odc.stac.load(
        items,
        crs="EPSG:4326",
        chunks={"latitude": 1024, "longitude": 1024},
    )
    return ds
