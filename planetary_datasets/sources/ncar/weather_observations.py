import sys
import os
from urllib.request import build_opener
import datetime as dt
import pandas as pd
import random

BASEURL = "https://data.rda.ucar.edu/ds337.0/tarfiles/"
opener = build_opener()


def _download_file(remote_path: str, local_path: str) -> str:
    sys.stdout.write("downloading " + remote_path + " ... ")
    sys.stdout.flush()
    infile = opener.open(remote_path)
    outfile = open(local_path, "wb")
    outfile.write(infile.read())
    outfile.close()
    sys.stdout.write("done\n")
    return local_path


def get_weather_observations(timestep: dt.datetime) -> str:
    remote_path = BASEURL + f"{timestep.strftime('%Y')}/prepbufr.{timestep.strftime('%Y%m%d')}.wo40.tar.gz"
    local_path = os.path.basename(remote_path)
    return _download_file(remote_path, local_path)


if __name__ == "__main__":
    # From 2009 to now
    date_range = pd.date_range(start="1998-01-01", end=dt.datetime.now().strftime("%Y-%m-%d"), freq="D")
    start_idx = random.randint(0, len(date_range))
    for day in date_range[start_idx:]:
        day_outname = day.strftime("%Y%m%d")
        year = day.year
        try:
            weather_obs = get_weather_observations(day)
        except Exception as e:
            print(e)
            continue
