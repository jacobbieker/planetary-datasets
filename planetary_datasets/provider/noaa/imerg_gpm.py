"""This gather and uses the global mosaic of geostationary satellites from NOAA on AWS"""
import datetime

import xarray as xr
import pandas as pd
import datetime as dt
#import zarr
from subprocess import Popen
import subprocess
import glob
# import numcodecs.zarr3
import xbitinfo as xb

"""
wget --load-cookies ~/.urs_cookies --save-cookies ~/.urs_cookies --keep-session-cookies --content-disposition -r -c --no-parent https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGHH.07/

IMERG Late

"""


def get_gpm(day: dt.datetime) -> list[str]:
    # Get day of year from day
    day_of_year = day.timetuple().tm_yday
    path = f"https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGHHE.07/{day.strftime('%Y')}/{str(day_of_year).zfill(3)}/"
    args = ["wget", "--load-cookies", "~/.urs_cookies", "--save-cookies", "~/.urs_cookies", "--keep-session-cookies", "--content-disposition", "-r", "-c", "--no-parent", path]
    process = Popen(args,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                    )
    process.wait()
    return sorted(list(glob.glob(f"gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGHHE.07/{day.strftime('%Y')}/{str(day_of_year).zfill(3)}/*.HDF5")))


variables = ["precipitation", "randomError", "probabilityLiquidPrecipitation", "precipitationQualityIndex"]
#encoding = {v: {"codecs": [zarr.codecs.BytesCodec(), zarr.codecs.ZstdCodec()]} for v in variables}
#encoding["time"] = {"units": "nanoseconds since 1970-01-01"}


def open_h5(filename: str) -> xr.Dataset:
    data = xr.open_dataset(filename, group="/Grid")
    # Drop latv, lonv, nv
    data = data.drop_dims(["latv", "lonv", "nv"]).rename({"lat": "latitude", "lon": "longitude"}).chunk({"time": 1, "latitude": -1, "longitude": -1})
    return data

from datetime import datetime as dt

def convert_to_dt(x):
    return dt.strptime(str(x), '%Y-%m-%d %H:%M:%S')


if __name__ == "__main__":
    date_range = pd.date_range(
        start="2000-01-01", end="2025-12-31", freq="30min"
    )
    path = "gpm_early.zarr"
    data = open_h5("/Users/jacob/Development/3B-HHR.MS.MRG.3IMERG.20001230-S143000-E145959.0870.V07B.HDF5")
    #bitinfo = xb.get_bitinformation(data, dim="longitude", implementation="python")  # calling bitinformation.jl.bitinformation
    #keepbits = xb.get_keepbits(bitinfo, inflevel=0.99)  # get number of mantissa bits to keep fo
    #print(keepbits)
    #t = data.time
    #print(t)
    #print(t[0])
    #dset_time = pd.Timestamp(t[0].isoformat())
    #time_idx = np.where(date_range == dset_time)[0][0]
    #print(time_idx)
    exit()
    data = open_h5("/run/media/jacob/Tester/GPM_final/gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGHH.07/1998/001/3B-HHR.MS.MRG.3IMERG.19980101-S120000-E122959.0720.V07B.HDF5")

    # Create empty dask arrays of the same size as the data
    dask_arrays = []
    dummies = dask.array.zeros((len(date_range), data.latitude.shape[0], data.longitude.shape[0]), chunks=(1, -1, -1), dtype=np.float32)
    default_dataarray = xr.DataArray(dummies, coords={"time": date_range, "latitude": data.latitude.values, "longitude": data.longitude.values},
                     dims=["time", "latitude", "longitude"])
    dummy_dataset = xr.Dataset({v: default_dataarray for v in variables},
                               coords={"time": date_range, "latitude": data.latitude.values, "longitude": data.longitude.values})
    print(dummy_dataset)
    dummy_dataset.to_zarr(path, mode="w", compute=False, zarr_format=3, consolidated=True, encoding=encoding)

    day_range = pd.date_range(
        start="2000-01-01", end="2025-12-31", freq="1D"
    )
    for day in tqdm(day_range):
        file_list = get_gpm(day)
        for f in file_list:
            try:
                data = open_h5(f)
                # Get the IDX of the timestamp in the dataset
                dset_time = pd.Timestamp(data['time'].values[0].isoformat())
                time_idx = np.where(date_range == dset_time)
                time_idx = time_idx[0][0]
                data.chunk({"time": 1, "longitude": -1, "latitude": -1}).to_zarr(path,region={"time": slice(time_idx, time_idx+1),"latitude": "auto", "longitude": "auto"})
                os.remove(f)
            except Exception as e:
                print(e)
                continue
