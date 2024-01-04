import sys
import os
from urllib.request import build_opener
import datetime as dt
import pandas as pd
import random

BASEURL = "https://data.rda.ucar.edu/ds337.0/prep48h/"
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


def get_aircraft_observations(timestep: dt.datetime) -> str:
    remote_path = BASEURL + f"{timestep.strftime('%Y')}/prepbufr.gdas.{timestep.strftime('%Y%m%d')}.t{timestep.strftime('%H')}z.nr.48h"
    local_path = os.path.basename(remote_path)
    return _download_file(remote_path, local_path)


if __name__ == "__main__":
    # From 2009 to now
    date_range = pd.date_range(start="2016-01-01", end=dt.datetime.now().strftime("%Y-%m-%d"), freq="6H")
    start_idx = random.randint(0, len(date_range))
    for day in date_range[start_idx:]:
        day_outname = day.strftime("%Y%m%d%H")
        year = day.year
        try:
            aircraft_obs = get_aircraft_observations(day)
        except Exception as e:
            print(e)
            continue
