import sys
import os
from urllib.request import build_opener
import datetime as dt
import pandas as pd
import random
from pathlib import Path
from planetary_datasets.base import AbstractSource, AbstractConvertor


BASEURL = "https://data.rda.ucar.edu/ds337.0/tarfiles/"
opener = build_opener()


class NCARWeatherObservationsSource(AbstractSource):
    def check_integrity(self, local_path: Path) -> bool:
        pass

    def get(self, timestamp: dt.datetime) -> str:
        for remote_path in [
            f"{timestamp.strftime('%Y')}/prepbufr.{timestamp.strftime('%Y%m%d')}.nr.tar.gz",
            f"{timestamp.strftime('%Y')}/prepbufr.{timestamp.strftime('%Y%m%d')}.wo40.tar.gz",
        ]:
            try:
                local_path = Path(os.path.basename(remote_path))
                self.download_file(Path(remote_path), local_path)
            except Exception as e:
                print(e)
                continue

    def process(self) -> str:
        pass


if __name__ == "__main__":
    # From 2009 to now
    date_range = pd.date_range(
        start="1998-01-01", end=dt.datetime.now().strftime("%Y-%m-%d"), freq="D"
    )
    start_idx = random.randint(0, len(date_range))
    for day in date_range[start_idx:]:
        day_outname = day.strftime("%Y%m%d")
        year = day.year
        try:
            weather_obs = get_early_weather_observations(day)
            get_weather_observations(day)
        except Exception as e:
            print(e)
            continue
