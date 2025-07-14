import harp
import numpy as np
import xarray as xr
import zarr

import pandas as pd
from satpy import Scene
import numpy as np
import xarray as xr
import tempfile
import zipfile
import datetime
import shutil
import os
import zarr
import warnings
from typing import Any
import pyresample
import yaml
import datetime as dt

"""

Things to get:

Microwave Sounder (5 channels): EO:EUM:DAT:METOP:MHSL1
AVHRR: EO:EUM:DAT:METOP:AVHRRL1 (Quite large, like VIIRS) -> Starts in 01/03/2008
AMSU-A: EO:EUM:DAT:METOP:AMSUL1
ASCAT: EO:EUM:DAT:METOP:ASCSZF1B
IASI: EO:EUM:DAT:METOP:IASIL1C-ALL (1.5GB per file, so quite large)
GOME-2: EO:EUM:DAT:METOP:GOMEL1

commands:
eumdac download -c EO:EUM:DAT:METOP:IASIL1C-ALL --limit 3 --threads 3 --end 2025-06-30T23:59
eumdac download -c EO:EUM:DAT:METOP:MHSL1 --limit 30 --threads 3
eumdac download -c EO:EUM:DAT:METOP:AMSUL1 --limit 1000 --threads 10
eumdac download -c EO:EUM:DAT:METOP:ASCSZF1B --limit 100 --threads 10
eumdac download -c EO:EUM:DAT:METOP:AVHRRL1 --limit 100 --threads 10
"""
warnings.filterwarnings("ignore", category=RuntimeWarning)

import eumdac
import icechunk
from icechunk.xarray import to_icechunk

def _serialize(d: dict[str, Any]) -> dict[str, Any]:
    sd: dict[str, Any] = {}
    for key, value in d.items():
        if isinstance(value, dt.datetime):
            sd[key] = value.isoformat()
        elif isinstance(value, bool | np.bool_):
            sd[key] = str(value)
        elif isinstance(value, pyresample.geometry.AreaDefinition):
            sd[key] = yaml.load(value.dump(), Loader=yaml.SafeLoader)  # type:ignore
        elif isinstance(value, dict):
            sd[key] = _serialize(value)
        else:
            sd[key] = str(value)
    return sd

def process_gome(filename) -> xr.Dataset:
    iasi_prod = harp.import_product(filename)
    tmp_file = tempfile.mktemp(suffix=".nc")
    harp.export_product(iasi_prod, tmp_file)
    ds = xr.open_dataset(tmp_file).load()
    os.remove(tmp_file)
    return ds

date_range = pd.date_range("2008-03-01", "2025-06-30", freq="15min")[::-1]

# Icechunk
storage = icechunk.local_filesystem_storage("metop_gome.icechunk")
repo = icechunk.Repository.open_or_create(storage)
session = repo.readonly_session("main")
try:
    ds = xr.open_zarr(session.store, consolidated=False)
    print(ds)
    times = ds.time.values
    # Check number of unique times
    print(f"Number of unique times in the store: {len(np.unique(times))}")
    # Check to when the last one is in there
    for d in date_range:
        if d > ds.time.values[-1]:
            print(f"Last date in the store is {ds.time.values[-1]}, skipping dates after {d}")
            date_range = date_range[date_range <= ds.time.values[-1]]
            print(date_range)
            break
except:
    times = []

# Insert your personal key and secret

credentials = (consumer_key, consumer_secret)
used_product_names = []
# Read used product names from file if it exists
if os.path.exists("used_product_names_gome.txt"):
    with open("used_product_names_gome.txt", "r") as f:
        used_product_names = [line.strip() for line in f.readlines()]
for idx, date in enumerate(date_range):
    # If it exists, check if times are already covered, if so, then skip
    token = eumdac.AccessToken(credentials)

    datastore = eumdac.DataStore(token)

    selected_collection = datastore.get_collection('EO:EUM:DAT:METOP:GOMEL1')

    # Set sensing start and end time
    start = datetime.datetime(date.year, date.month, date.day, date.hour, 0)
    end = datetime.datetime(date.year, date.month, date.day, (date+pd.Timedelta("15min")).hour, 0)

    products = selected_collection.search(
        dtstart=start,
        dtend=end,)

    print(f'Found Datasets: {products.total_results} datasets for the given time range')

    dses = []
    for product in products:
        product_tmpdir = tempfile.mkdtemp()
        finished = False
        while not finished:
            try:
                with product.open() as fsrc:
                    if fsrc.name in used_product_names:
                        print(f"Skipping {fsrc.name}, already downloaded")
                        continue
                    # Download the file if it does not exist
                    with open(os.path.join(product_tmpdir, fsrc.name), mode='wb') as fdst:
                        shutil.copyfileobj(fsrc, fdst)
                        used_product_names.append(fsrc.name)
                        finished = True
            except Exception as e:
                print(f"Failed to download {fsrc.name}: {e}, trying again")
                continue
        tmpdir = tempfile.mkdtemp()
        with zipfile.ZipFile(os.path.join(product_tmpdir, fsrc.name), 'r') as zip_ref:
            zip_ref.extractall(tmpdir)
        path_to_filename = os.path.join(tmpdir, fsrc.name.replace('.zip', '.nat'))
        ds = process_gome(path_to_filename)
        dses.append(ds)
        shutil.rmtree(tmpdir)
        shutil.rmtree(product_tmpdir)
    if len(dses) == 0:
        print(f"No datasets found for {date}, skipping...")
        continue
    ds = xr.concat(dses, dim="time")
    # Save the dataset to a Zarr file
    encoding = {
            "time": {
                "units": "nanoseconds since 2000-01-01",
                "calendar": "standard",
                "dtype": "int64",
            }
        }
    variables = []
    for var in ds.data_vars:
        variables.append(var)
    encoding.update({
        v: {"compressors": zarr.codecs.BloscCodec(cname='zstd', clevel=9, shuffle=zarr.codecs.BloscShuffle.bitshuffle)}
        for v in variables})

    if len(times) == 0:
        session = repo.writable_session("main")
        to_icechunk(ds.chunk({"time": 1000, "x": -1, "y": -1}), session, encoding=encoding)
        print(session.commit(f"add {date} data to store"))
    else:
        session = repo.writable_session("main")
        to_icechunk(ds.chunk({"time": 1000, "x": -1, "y": -1}), session, append_dim="time")
        print(session.commit(f"add {date} data to store"))
    # Write out the used product names to a file
    with open("used_product_names_gome.txt", "w") as f:
        for name in used_product_names:
            f.write(f"{name}\n")
