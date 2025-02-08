import os
import json
import fsspec
from pathlib import Path


def download_file(remote_path, local_path):
    full_local_path = local_path
    print(remote_path)
    if full_local_path.exists():
        return full_local_path
    with fsspec.open(remote_path, "rb") as infile:
        with fsspec.open(str(full_local_path), "wb") as outfile:
            outfile.write(infile.read())


def download_meteostat():
    with fsspec.open("https://bulk.meteostat.net/v2/stations/full.json.gz", compression="infer") as f:
        weather_stations = json.load(f)
    station_ids = [s['id'] for s in weather_stations]
    for station_id in station_ids:
        remote_path = f"https://bulk.meteostat.net/v2/hourly/{station_id}.csv.gz"
        #local_path = Path(os.path.basename(remote_path))
        local_path = Path(f"data/{station_id}.csv.gz")
        try:
            download_file(remote_path, local_path)
        except FileNotFoundError:
            continue

download_meteostat()