import sys
import os
from urllib.request import build_opener
import datetime as dt
import pandas as pd
import random

BASEURL = "https://data.rda.ucar.edu/ds336.0"
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


def get_metar_observations(timestep: dt.datetime) -> str:
    remote_path = (
        BASEURL
        + f"/surface/{timestep.strftime('%Y%m')}/{timestep.strftime('%Y%m%d')}/Surface_METAR_{timestep.strftime('%Y%m%d')}_0000.nc"
    )
    local_path = os.path.basename(remote_path)
    return _download_file(remote_path, local_path)


def get_buoy_observations(timestep):
    remote_path = (
        BASEURL
        + f"/surface/{timestep.strftime('%Y%m')}/{timestep.strftime('%Y%m%d')}/Surface_Buoy_{timestep.strftime('%Y%m%d')}_0000.nc"
    )
    local_path = os.path.basename(remote_path)
    return _download_file(remote_path, local_path)


def get_surface_synoptic_observations(timestep):
    remote_path = (
        BASEURL
        + f"/surface/{timestep.strftime('%Y%m')}/{timestep.strftime('%Y%m%d')}/Surface_Synoptic_{timestep.strftime('%Y%m%d')}_0000.nc"
    )
    local_path = os.path.basename(remote_path)
    return _download_file(remote_path, local_path)


def get_upper_air_observations(timestep):
    remote_path = (
        BASEURL
        + f"/upperair/{timestep.strftime('%Y%m')}/{timestep.strftime('%Y%m%d')}/Upperair_{timestep.strftime('%Y%m%d')}_0000.nc"
    )
    local_path = os.path.basename(remote_path)
    return _download_file(remote_path, local_path)


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
