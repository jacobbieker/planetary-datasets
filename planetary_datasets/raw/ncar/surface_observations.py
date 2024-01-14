import sys
import os
from urllib.request import build_opener
import datetime as dt
import pandas as pd
import random

BASEURL = "https://data.rda.ucar.edu/ds461.0/tarfiles/"
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


def get_surface_observations(timestep: dt.datetime) -> str:
    remote_path = (
        BASEURL + f"{timestep.strftime('%Y')}/gdassfcobs.{timestep.strftime('%Y%m%d')}.tar.gz"
    )
    local_path = os.path.basename(remote_path)
    if not os.path.exists(local_path):
        return _download_file(remote_path, local_path)
    else:
        return local_path


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
