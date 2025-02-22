import xarray as xr

"""
This is to convert the HURDAT2 hurricane track and wind speed data into a more useful format
"""

raw_file_location_pacific = "https://www.nhc.noaa.gov/data/hurdat/hurdat2-nepac-1949-2022-050423.txt"
raw_file_location_atlantic = "https://www.nhc.noaa.gov/data/hurdat/hurdat2-1851-2022-050423.txt"

def download_and_process_hurdat2(raw_location: str, output_location: str) -> xr.Dataset:
    pass

