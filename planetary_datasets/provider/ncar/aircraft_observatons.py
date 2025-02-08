import sys
import os
from urllib.request import build_opener
import datetime as dt
import pandas as pd
from pathlib import Path
import random
from planetary_datasets.base import AbstractSource, AbstractConvertor

BASEURL = "https://data.rda.ucar.edu/ds337.0/prep48h/"
opener = build_opener()


class AircraftObservationsSource(AbstractSource):
    def get(self, timestamp: dt.datetime) -> str:
        remote_path = (
            f"{timestamp.strftime('%Y')}/prepbufr.gdas.{timestamp.strftime('%Y%m%d')}.t{timestamp.strftime('%H')}z.nr.48h"
        )
        local_path = Path(os.path.basename(remote_path))
        self.download_file(Path(remote_path), local_path)

    def process(self) -> str:
        pass


if __name__ == "__main__":
    # From 2009 to now
    date_range = pd.date_range(
        start="2016-01-01", end=dt.datetime.now().strftime("%Y-%m-%d"), freq="6H"
    )
    obs = AircraftObservationsSource(source_location=BASEURL, raw_location="aircraft_observations/", start_date=date_range[0], end_date=date_range[-1])
    start_idx = random.randint(0, len(date_range))
    for day in date_range[start_idx:]:
        day_outname = day.strftime("%Y%m%d%H")
        year = day.year
        try:
            aircraft_obs = obs.get(day)
        except Exception as e:
            print(e)
            continue
