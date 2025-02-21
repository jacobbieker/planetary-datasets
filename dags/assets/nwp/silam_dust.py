import pandas as pd
import datetime as dt
import fsspec
import os
from subprocess import Popen
import subprocess

"""
https://thredds.silam.fmi.fi/thredds/fileServer/dust_glob01_v5_7_2/files/SILAM-dust-glob01_v5_7_2_2024111300_001.nc4

https://thredds.silam.fmi.fi/thredds/fileServer/dust_glob01_v5_7_2/files/SILAM-dust-glob01_v5_7_2_2024112700_060.nc4

SILAM-dust-glob01_v5_7_2_2024111400
"""

def construct_urls_from_datetime(date: dt.datetime) -> list[str]:
    urls = []
    for i in range(1, 121):
        urls.append(f"https://thredds.silam.fmi.fi/thredds/fileServer/dust_glob01_v5_7_2/files/SILAM-dust-glob01_v5_7_2_{date.strftime('%Y%m%d00')}_{str(i).zfill(3)}.nc4")
    return urls

def download_forecast(date: dt.datetime) -> list[str]:
    urls = construct_urls_from_datetime(date)
    downloaded_paths = []
    for url in urls:
        url_dir = url.split("/")[-1].split("_")[-2]
        # Create the directory
        if not os.path.exists(url_dir):
            os.makedirs(url_dir)
        downloaded_url_path = os.path.join(url_dir, url.split("/")[-1])
        print(downloaded_url_path)
        # Download the file to a local directory, try 3 times before giving up
        finished = False
        if os.path.exists(downloaded_url_path):
            # Open to check its valid

            print(f"Already downloaded {url}")
            downloaded_paths.append(downloaded_url_path)
            finished = True
        while not finished:
            try:
                #MERGE_COMMAND = f"/bin/zsh && wget -nc {url} -O {downloaded_url_path}"
                #subprocess.run(MERGE_COMMAND, shell=True)
                #args = ["wget2", "-nc", url, "-O", downloaded_url_path]
                #process = Popen(args)
                # Run wget to get it
                with fsspec.open(url, "rb") as f:
                    with open(downloaded_url_path, "wb") as f2:
                        f2.write(f.read())
                finished = True
                downloaded_paths.append(downloaded_url_path)
                break
            except Exception as e:
                print(e)
                #continue
        #if not finished:
        #    print(f"Failed to download {url}")
        else:
            print(f"Downloaded {url}")
    return downloaded_paths

if __name__ == "__main__":
    # Do it from a week ago to now
    zarr_date_range = pd.date_range(
        start="2024-11-14", end="2026-12-31", freq="D"
    )

    date_range = pd.date_range(
        start="2025-02-14", end=dt.datetime.now().strftime("%Y-%m-%d"), freq="D"
    )
    for day in date_range:
        download_forecast(day)
