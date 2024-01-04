import os
import datetime as dt
import pandas as pd
import random
from ftplib import FTP_TLS


def get_gpm(timestep: dt.datetime, ftps: FTP_TLS) -> list[str]:
    ftps.cwd(f"/gpmallversions/V07/{timestep.strftime('%Y')}/{timestep.strftime('%m')}/{timestep.strftime('%d')}/gis")
    files = ftps.nlst()
    local_paths = []
    for f in files:
        if "HHR" in f and "7B.zip" in f:
            print(f"Downloading: {f}")
            if os.path.exists(f):
                continue
            with open(f, "wb") as localfile:
                ftps.retrbinary("RETR " + f, localfile.write, 1024)
            local_paths.append(f)
    return local_paths


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--username", type=str, required=True)
    parser.add_argument("--password", type=str, required=True)
    args = parser.parse_args()
    ftps = FTP_TLS()
    ftps.connect("arthurhouftps.pps.eosdis.nasa.gov")
    ftps.login(args.username, args.password)
    date_range = pd.date_range(start="2000-01-01", end=dt.datetime.now().strftime("%Y-%m-%d"), freq="D")
    start_idx = random.randint(0, len(date_range))
    for day in date_range[start_idx:]:
        day_outname = day.strftime("%Y%m%d")
        year = day.year
        try:
            gpm_obs = get_gpm(day, ftps)
        except Exception as e:
            print(e)
            continue
