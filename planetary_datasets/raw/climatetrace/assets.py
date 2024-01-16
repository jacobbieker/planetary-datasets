import os
import pandas as pd
import subprocess

paths = ["/run/media/jacob/data/Climate TRACE download links - raster.csv",
         "/run/media/jacob/data/Climate TRACE download links - road-segments.csv",
         "/run/media/jacob/data/Climate TRACE download links - shipping.csv",
         "/run/media/jacob/data/Climate TRACE download links - ownership.csv"]

for p in paths:
    data = pd.read_csv(p)
    print(data)
    for i, row in data.iterrows():
        if len(row['link']) == 0:
            continue
        print(row["link"])
        subprocess.check_call(["wget", row["link"], "--output-document", f"/run/media/jacob/data/ClimateTrace/{row['link'].split('/')[-1]}"])
        print(f"Downloaded: {row['link']}")
