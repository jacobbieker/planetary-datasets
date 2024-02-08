import subprocess
import os
import json
import fsspec
from planetary_datasets.base import AbstractSource, AbstractConvertor
import datetime as dt
from typing import Optional, Union
from pathlib import Path


class MeteostatObservationsSource(AbstractSource):
    def get(self, timestamp: dt.datetime, station_ids: Optional[Union[str, list[str]]] = None) -> str:
        if station_ids is None:
            station_ids = [station["id"] for station in self.gather_stations()]
        elif isinstance(station_ids, str):
            station_ids = [station_ids]
        for station_id in station_ids:
            remote_path = f"https://bulk.meteostat.net/v2/hourly/{station_id}.csv.gz"
            local_path = Path(os.path.basename(remote_path))
            self.download_file(Path(remote_path), local_path)

    @staticmethod
    def gather_stations():
        with fsspec.open("https://bulk.meteostat.net/v2/stations/full.json.gz", compression="infer") as f:
            weather_stations = json.load(f)
        return weather_stations

    def process(self) -> str:
        pass
