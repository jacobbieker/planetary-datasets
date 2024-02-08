import sys
import os
from urllib.request import build_opener
import datetime as dt
import pandas as pd
import random
from pathlib import Path
from planetary_datasets.base import AbstractSource, AbstractConvertor


BASEURL = "https://data.rda.ucar.edu/ds461.0/tarfiles/"
opener = build_opener()


class SurfaceObservationsSource(AbstractSource):
    def get(self, timestamp: dt.datetime) -> str:
        remote_path = (
            f"{timestamp.strftime('%Y')}/gdassfcobs.{timestamp.strftime('%Y%m%d')}.tar.gz"
        )
        local_path = Path(os.path.basename(remote_path))
        self.download_file(Path(remote_path), local_path)

    def process(self) -> str:
        pass


if __name__ == "__main__":
    # From 2009 to now
    date_range = pd.date_range(
        start="2000-01-01", end=dt.datetime.now().strftime("%Y-%m-%d"), freq="6H"
    )
    for day in date_range:
        day_outname = day.strftime("%Y%m%d%H")
        year = day.year
        try:
            surface_obs = get_surface_observations(day)
        except Exception as e:
            print(e)
            continue
