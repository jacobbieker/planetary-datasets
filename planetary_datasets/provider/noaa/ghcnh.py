"""
GHCNh is the hourly global historical climatology network dataset. It is the replacement for the Integrated Surface Dataset
"""
import os.path

import pandas as pd
import xarray as xr
import fsspec
import requests
import pooch

STATION_LIST = "https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/doc/ghcnh-station-list.txt"
FILE_PATH_TEMPLATE = "https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/access/by-year/{year}/parquet/GHCNh_{station_id}_{year}.parquet"

def load_ghcnh_data(station_id: str, year: int) -> xr.Dataset:
    """
    Load the GHCNh data for the given station id

    Args:
        station_id: The station id to load the data for
        year: The year to load the data for

    Returns:
        Xarray Dataset containing the GHCNh data
    """
    file_path = FILE_PATH_TEMPLATE.format(station_id=station_id, year=year)
    with fsspec.open(file_path) as f:
        df = pd.read_parquet(f)
    return df


def get_list_of_stations() -> pd.DataFrame:
    """
    Get the list of stations available in the GHCNh dataset

    Returns:
        Dataframe containing the list of stations
    """
    df = pd.DataFrame()
    station_ids = []
    lats = []
    lons = []
    elevs = []
    with fsspec.open(STATION_LIST) as f:
        # Build it up iteratively
        for line in f:
            line = line.decode("utf-8")
            parts = line.split(" ")
            # Only keep non-empty parts
            parts = [part for part in parts if len(part) > 0]
            station_id = parts[0]
            lat = float(parts[2])
            lon = float(parts[1])
            elev = float(parts[3])
            station_ids.append(station_id)
            lats.append(lat)
            lons.append(lon)
            elevs.append(elev)
    df["station_id"] = station_ids
    df["latitude"] = lats
    df["longitude"] = lons
    df["elevation"] = elevs
    return df

def download_with_pooch(station_df):
    base_path = "https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/access/by-year/"
    files = {}
    for station_id in station_df["station_id"]:
        files[f"{base_path}GHCNh_{station_id}_por.psv"] = None
    odie = pooch.create(
        # Use the default cache folder for the operating system
        path="/mnt/storage/GHCNh/",
        base_url=base_path,
        # The registry specifies the files that can be fetched
        registry=files,
    )
    for f in files.keys():
        try:
            odie.fetch(f, progressbar=True)
        except requests.exceptions.ReadTimeout:
            print(f"Failed to download {f}")

def download_file(url):
    local_filename = url.split('/')[-1]
    # NOTE the stream=True parameter below
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk:
                f.write(chunk)
    return local_filename

if __name__ == "__main__":
    station_df = get_list_of_stations()
    for year in range(1950, 2024):
        for station_id in station_df["station_id"]:
            if os.path.exists(f"{station_id}_{year}.parquet"):
                continue
            try:
                download_file(FILE_PATH_TEMPLATE.format(station_id=station_id, year=year))
            except Exception as e:
                print(f"Failed to download {station_id} for {year}, {e}")
                continue
            """
            try:
                df = load_ghcnh_data(station_id, year=year)
                df.to_parquet(f"{station_id}_{year}.parquet")
            except FileNotFoundError:
                print(f"Failed to download {station_id} for {year}")
                continue
            except Exception:
                print(f"Failed to download {station_id} for {year} with error: {e}")
                continue
            """

