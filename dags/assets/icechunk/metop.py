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

def process_avhrr(filename) -> xr.Dataset:
    scn = Scene([filename], reader="avhrr_l1b_eps")
    scn.load(['1', '2', '3a', '3b', '4', '5', 'cloud_flags', 'latitude', 'longitude', 'satellite_azimuth_angle', 'satellite_zenith_angle', 'solar_azimuth_angle', 'solar_zenith_angle'])
    ds = scn.to_xarray_dataset()
    lon, lat = scn['1'].attrs["area"].get_lonlats()
    ds['latitude'] = (("y", "x"), lat)
    ds['longitude'] = (("y", "x"), lon)
    # Add time coordinate as mid point of the start_time and end_time
    import pandas as pd
    start_time = pd.Timestamp(ds.attrs['start_time'])
    end_time = pd.Timestamp(ds.attrs['end_time'])
    # Get the middle time of two times
    mid_time = start_time + (end_time - start_time) / 2
    ds['time'] = mid_time
    ds = ds.assign_coords({"time": ds['time']})
    # Expand coords for data to have time dimension
    ds = ds.expand_dims("time")
    ds["start_time"] = xr.DataArray([start_time], coords={"time": ds["time"]})
    ds["end_time"] = xr.DataArray([end_time], coords={"time": ds["time"]})
    ds["platform_name"] = xr.DataArray([ds.attrs['platform_name']], coords={"time": ds["time"]})
    ds = ds.load()
    # Now reduce to float16 for everything other than latitude/longitude
    for var in ds.data_vars:
        if var not in ['latitude', 'longitude', 'start_time', 'end_time', 'platform_name']:
            ds[var] = ds[var].astype(np.float16)
        if var in ["latitude", "longitude"]:
            ds[var] = ds[var].astype(np.float32)
    # Drop a few attributes
    ds.attrs.pop('end_time')
    ds.attrs.pop('start_time')
    ds.attrs.pop('platform_name')
    ds.attrs = _serialize(ds.attrs)
    for var in ds.data_vars:
        ds[var].attrs = _serialize(ds[var].attrs)
    ds = ds.drop_vars("crs")
    return ds

date_range = pd.date_range("2008-03-01", "2025-06-30", freq="4h")[::-1]

# Icechunk
storage = icechunk.local_filesystem_storage("metop_avhrr.icechunk")
storage = icechunk.s3_storage(bucket="bkr",
                                      prefix="polar/metop_avhrr.icechunk",
                                      endpoint_url="https://data.source.coop",
                                      allow_http=True,
                                      region="us-west-2",
                                      force_path_style=True, )
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
consumer_key = 'SWdEnLvOlVTVGli1An1nKJ3NcV0a'
consumer_secret = 'gUQe0ej7H_MqQVGF4cd7wfQWcawa'

credentials = (consumer_key, consumer_secret)

for idx, date in enumerate(date_range):
    # If it exists, check if times are already covered, if so, then skip
    token = eumdac.AccessToken(credentials)

    datastore = eumdac.DataStore(token)

    selected_collection = datastore.get_collection('EO:EUM:DAT:METOP:AVHRRL1')

    # Set sensing start and end time
    start = datetime.datetime(date.year, date.month, date.day, date.hour, 0)
    end = datetime.datetime(date.year, date.month, date.day, (date+pd.Timedelta("4h")).hour, 0)

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
                    # Download the file if it does not exist
                    with open(os.path.join(product_tmpdir, fsrc.name), mode='wb') as fdst:
                        shutil.copyfileobj(fsrc, fdst)
                        finished = True
            except Exception as e:
                print(f"Failed to download {fsrc.name}: {e}, trying again")
                continue
        tmpdir = tempfile.mkdtemp()
        with zipfile.ZipFile(os.path.join(product_tmpdir, fsrc.name), 'r') as zip_ref:
            zip_ref.extractall(tmpdir)
        path_to_filename = os.path.join(tmpdir, fsrc.name.replace('.zip', '.nat'))
        ds = process_avhrr(path_to_filename)
        if ds["time"].values[0] in times:
            print(f"Dataset for {date} already exists, skipping...")
            shutil.rmtree(tmpdir)
            shutil.rmtree(product_tmpdir)
            continue
        dses.append(ds)
        shutil.rmtree(tmpdir)
        shutil.rmtree(product_tmpdir)
    if len(dses) == 0:
        print(f"No datasets found for {date}, skipping...")
        continue
    # Align all datasets to the one with the largest dimensions
    aligned_dses = []
    max_y = 37800
    max_x = 2048
    for dataset in dses:
        if len(dataset.y.values) < max_y:
            pad_y = max_y - dataset.y.size
            dataset = dataset.pad(
                {"y": (0, pad_y)}, mode="constant", constant_values=np.nan
            )
        if len(dataset.x.values) < max_x:
            pad_x = max_x - dataset.x.size
            dataset = dataset.pad(
                {"x": (0, pad_x)}, mode="constant", constant_values=np.nan
            )
        aligned_dses.append(dataset)
    ds = xr.concat(aligned_dses, dim="time")
    # Save the dataset to a Zarr file
    encoding = {
            "time": {
                "units": "milliseconds since 1970-01-01",
                "calendar": "standard",
                "dtype": "int64",
            }
        }
    variables = []
    for var in ds.data_vars:
        if var not in ["orbital_parameters", "start_time", "end_time", "area"]:
            variables.append(var)
    encoding.update({
        v: {"compressors": zarr.codecs.BloscCodec(cname='zstd', clevel=9, shuffle=zarr.codecs.BloscShuffle.bitshuffle)}
        for v in variables})

    if len(times) == 0:
        session = repo.writable_session("main")
        to_icechunk(ds.chunk({"time": 1, "x": -1, "y": -1}), session, encoding=encoding)
        print(session.commit(f"add {date} data to store"))
    else:
        session = repo.writable_session("main")
        to_icechunk(ds.chunk({"time": 1, "x": -1, "y": -1}), session, append_dim="time")
        print(session.commit(f"add {date} data to store"))
    session = repo.readonly_session("main")
    ds = xr.open_zarr(session.store, consolidated=False)
    times = ds.time.values
