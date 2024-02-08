import sys
import os
from urllib.request import build_opener
import datetime as dt
import pandas as pd
import random
from planetary_datasets.base import AbstractSource, AbstractConvertor
from pathlib import Path
import xarray as xr
import fsspec

BASEURL = "https://data.rda.ucar.edu/ds336.0"
opener = build_opener()

class UniDataObservationsSource(AbstractSource):

    def __init__(self, observation_type: str, raw_location: str, processed_location: str, **kwargs):
        super().__init__(raw_location, processed_location, **kwargs)
        self.observation_type = observation_type

    def create_correct_remote_path(self, timestamp: dt.datetime) -> str:
        if self.observation_type == "metar":
            return (
                f"surface/{timestamp.strftime('%Y%m')}/{timestamp.strftime('%Y%m%d')}/Surface_METAR_{timestamp.strftime('%Y%m%d')}_0000.nc"
            )
        elif self.observation_type == "buoy":
            return (
                f"surface/{timestamp.strftime('%Y%m')}/{timestamp.strftime('%Y%m%d')}/Surface_Buoy_{timestamp.strftime('%Y%m%d')}_0000.nc"
            )
        elif self.observation_type == "synoptic":
            return (
                f"surface/{timestamp.strftime('%Y%m')}/{timestamp.strftime('%Y%m%d')}/Surface_Synoptic_{timestamp.strftime('%Y%m%d')}_0000.nc"
            )
        elif self.observation_type == "upper_air":
            return (
                f"upperair/{timestamp.strftime('%Y%m')}/{timestamp.strftime('%Y%m%d')}/Upperair_{timestamp.strftime('%Y%m%d')}_0000.nc"
            )
        else:
            raise ValueError("Observation type not recognized")

    def get(self, timestamp: dt.datetime) -> str:
        remote_path = self.create_correct_remote_path(timestamp)
        local_path = Path(os.path.basename(remote_path))
        self.download_file(Path(remote_path), local_path)

    def process(self) -> str:
        pass

    def check_integrity(self, local_path: Path) -> bool:
        try:
            xr.open_dataset(local_path)
            return True
        except Exception as e:
            print(e)
            return False


if __name__ == "__main__":
    # From 2009 to now
    date_range = pd.date_range(
        start="2009-01-01", end=dt.datetime.now().strftime("%Y-%m-%d"), freq="D"
    )
    start_idx = random.randint(0, len(date_range))
    for day in date_range[start_idx:]:
        day_outname = day.strftime("%Y%m%d")
        year = day.year
        if os.path.exists(f"{day_outname}.nc"):
            continue
        try:
            metar_obs = get_metar_observations(day)
            buoy_obs = get_buoy_observations(day)
            synoptic_obs = get_surface_synoptic_observations(day)
            air_obs = get_upper_air_observations(day)
        except Exception as e:
            print(e)
            continue
