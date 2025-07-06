import json
from pathlib import Path
import pandas as pd
import aiohttp
import fsspec
from aiohttp.client_exceptions import ClientPayloadError


def download_file(remote_path, local_path):
    full_local_path = local_path
    if full_local_path.exists():
        return
    try:
        with fsspec.open(remote_path, "rb") as infile:
            with fsspec.open(str(full_local_path), "wb") as outfile:
                outfile.write(infile.read())
    except aiohttp.client_exceptions.ClientResponseError:
        os.remove(full_local_path)

import tqdm
import os
def download_meteostat():
    with fsspec.open("https://bulk.meteostat.net/v2/stations/lite.json.gz", compression="infer") as f:
        weather_stations = json.load(f)
    station_ids = [s['id'] for s in weather_stations]
    inventory = [s['inventory'] for s in weather_stations]
    starts = [s['hourly']['start'] for s in inventory]
    ends = [s['hourly']['end'] for s in inventory]

    for year in range(2025, 1990, -1):
        for i, station_id in tqdm.tqdm(enumerate(station_ids), total=len(station_ids)):
            try:
                starts[i] = pd.to_datetime(starts[i]).strftime("%Y-%m-%d")
                ends[i] = pd.to_datetime(ends[i]).strftime("%Y-%m-%d")
            except AttributeError:
                # If the date conversion fails, skip this station
                continue
            if starts[i] > pd.to_datetime(f"{year}-12-31").strftime("%Y-%m-%d") or ends[i] < pd.to_datetime(f"{year}-01-01").strftime("%Y-%m-%d"):
                continue
            remote_path = f"https://data.meteostat.net/hourly/{year}/{station_id}.csv.gz"
            #local_path = Path(os.path.basename(remote_path))
            local_path = Path(f"/Users/jacob/Development/meteostat_data/{year}_{station_id}.csv.gz")
            if os.path.exists(local_path):
                continue
            try:
                download_file(remote_path, local_path)
            except FileNotFoundError:
                continue
            except ClientPayloadError:
                # Try once more
                try:
                    download_file(remote_path, local_path)
                except:
                    continue
    for station_id in tqdm.tqdm(station_ids, total=len(station_ids)):
        remote_path = f"https://data.meteostat.net/hourly/{station_id}.map.csv.gz"
        # local_path = Path(os.path.basename(remote_path))
        local_path = Path(f"/Users/jacob/Development/meteostat_data/{station_id}.map.csv.gz")
        try:
            download_file(remote_path, local_path)
        except FileNotFoundError:
            continue
        except ClientPayloadError:
            # Try once more
            try:
                download_file(remote_path, local_path)
            except:
                continue

download_meteostat()
