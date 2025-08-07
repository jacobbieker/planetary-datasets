import harp
import numpy as np
import xarray as xr
import zarr


#iasi_prod = harp.import_product("/Users/jacob/Downloads/IASI_xxx_1C_M01_20250713182356Z_20250713200259Z_N_O_20250713191712Z/IASI_xxx_1C_M01_20250713182356Z_20250713200259Z_N_O_20250713191712Z.nat")
#print(iasi_prod)
#harp.export_product(iasi_prod, "iasi_harp.nc")
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

spacecrafts = {"_M01_": "Metop-B",
                   "_M02_": "Metop-A",
                   "_M03_": "Metop-C", }

def process_iasi(filename) -> xr.Dataset:
    # get platform_name from filename
    for spacecraft, platform_name in spacecrafts.items():
        if spacecraft in filename:
            spacecraft_name = platform_name
            break
    iasi_prod = harp.import_product(filename)
    tmp_file = tempfile.mktemp(suffix=".nc")
    harp.export_product(iasi_prod, tmp_file)
    ds = xr.open_dataset(tmp_file).load()
    ds["platform_name"] = xr.DataArray(
        [spacecraft_name] * len(ds.time),
        dims=["time"],
    )
    ds["time"] = ds["datetime"]
    ds = ds.drop_vars(["datetime", "orbit_index"])
    # Pad to a multiple of 9180
    if len(ds.time) % 9180 != 0:
        print(ds)
        padding_length = 9180 - (len(ds.time) % 9180)
        padding_values = {v: (np.nan,) for v in ds.data_vars if v not in ["platform_name", "time", "index", "scan_subindex"]}
        padding_values["platform_name"] = ("Metop-Z",)
        padding_values["time"] = (np.datetime64("2000-01-01T00:00:00"),)
        padding_values["index"] = (-1,)
        padding_values["scan_subindex"] = (-1,)
        ds = ds.pad({"time": (0, padding_length)}, mode="constant", constant_values=padding_values)
        print(ds)
    os.remove(tmp_file)
    return ds

date_range = pd.date_range("2008-03-01", "2025-06-30", freq="2h")[::-1]

# Icechunk
storage = icechunk.s3_storage(bucket="bkr",
                                      prefix="polar/metop_iasi.icechunk",
                                      endpoint_url="https://data.source.coop",
                                      access_key_id="SC11A9JDAZLVTF959664D1NI",
                                      secret_access_key="P0qxms7SFORhGJOqBPjQoygRVIdrt0M542l9grr08XF9Kwk5XJzj9lZQXxS3YKsT",
                                      allow_http=True,
                                      region="us-west-2",
                                      force_path_style=True, )
#repo = ic.Repository.open(storage)
#storage = icechunk.local_filesystem_storage("/Volumes/Passport/metop_iasi.icechunk")
repo = icechunk.Repository.open(storage)
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
except Exception as e:
    print(f"Could not open times from store: {e}")
    times = []

# Insert your personal key and secret


credentials = (consumer_key, consumer_secret)
used_product_names = []
first_write = True
# Read used product names from file if it exists
if os.path.exists("used_product_names_iasi.txt"):
    first_write = False
    with open("used_product_names_iasi.txt", "r") as f:
        used_product_names = [line.strip() for line in f.readlines()]
for idx, date in enumerate(date_range):
    # If it exists, check if times are already covered, if so, then skip
    token = eumdac.AccessToken(credentials)

    datastore = eumdac.DataStore(token)

    selected_collection = datastore.get_collection('EO:EUM:DAT:METOP:IASIL1C-ALL')

    # Set sensing start and end time
    start = datetime.datetime(date.year, date.month, date.day, date.hour, 0)
    end = datetime.datetime(date.year, date.month, date.day, (date+pd.Timedelta("2h")).hour, 0)

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
                        finished = True
                        continue
                    # Download the file if it does not exist
                    with open(os.path.join(product_tmpdir, fsrc.name), mode='wb') as fdst:
                        shutil.copyfileobj(fsrc, fdst)
                        used_product_names.append(fsrc.name)
                        finished = True
            except Exception as e:
                print(f"Failed to download {fsrc.name}: {e}, trying again")
                continue
        if not os.path.exists(os.path.join(product_tmpdir, fsrc.name)):
            continue
        tmpdir = tempfile.mkdtemp()
        with zipfile.ZipFile(os.path.join(product_tmpdir, fsrc.name), 'r') as zip_ref:
            zip_ref.extractall(tmpdir)
        path_to_filename = os.path.join(tmpdir, fsrc.name.replace('.zip', '.nat'))
        ds = process_iasi(path_to_filename)
        dses.append(ds)
        shutil.rmtree(tmpdir)
        shutil.rmtree(product_tmpdir)
    if len(dses) == 0:
        print(f"No datasets found for {date}, skipping...")
        continue
    elif len(dses) == 1:
        ds = dses[0]
    else:
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
    print(ds)
    if first_write:
        session = repo.writable_session("main")
        to_icechunk(ds.chunk({"time": 9180, "spectral": -1}), session, encoding=encoding)
        print(session.commit(f"add {date} data to store"))
        first_write = False
    else:
        session = repo.writable_session("main")
        to_icechunk(ds.chunk({"time": 9180, "spectral": -1}), session, append_dim="time")
        print(session.commit(f"add {date} data to store"))
    # Write out the used product names to a file
    with open("used_product_names_iasi.txt", "w") as f:
        for name in used_product_names:
            f.write(f"{name}\n")
