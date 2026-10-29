"""

https://s3.opensky-network.org/data-samples/states/.2020-05-11/00/states_2020-05-11-00.csv.tar

2016-06-05 to 2022-06-27 every Monday for all 24 hours from 00 to 23

"""

import datetime as dt
import os

import fsspec
import pandas as pd


def construct_opensky_url_from_datetime(date: dt.datetime) -> str:
    return f"https://s3.opensky-network.org/data-samples/states/.{date.strftime('%Y-%m-%d')}/{date.hour:02}/states_{date.strftime('%Y-%m-%d-%H')}.csv.tar"


# Now download it

date_range = pd.date_range(start="2017-06-05", end="2022-06-27", freq="7D")
for date in date_range:
    for hour in range(24):
        url = construct_opensky_url_from_datetime(date.replace(hour=hour))
        filename = url.split("/")[-1]
        if os.path.exists(filename):
            print(f"{filename} already exists, skipping download")
            continue
        try:
            with fsspec.open(url, "rb") as f:
                with open(filename, "wb") as f2:
                    f2.write(f.read())
            print(f"Downloaded {url} to {filename}")
        except Exception as e:
            print(f"Error downloading {url}: {e}")
