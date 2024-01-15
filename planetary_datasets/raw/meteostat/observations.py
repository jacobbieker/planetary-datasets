import subprocess
import os
import json
import fsspec


with fsspec.open("https://bulk.meteostat.net/v2/stations/full.json.gz", compression="infer") as f:
    weather_stations = json.load(f)

# Download all stations
for station in weather_stations:
    station_id = station["id"]
    subprocess.check_call(["curl", f"https://bulk.meteostat.net/v2/hourly/{station_id}.csv.gz", "--output", f"{station_id}.csv.gz"])
