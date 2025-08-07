import gzip
import os
import shutil
import tempfile
from glob import glob
from multiprocessing import Pool

import numcodecs
import xarray as xr
from tqdm import tqdm

zarr_mode_to_extra_kwargs = {
    "a": {"append_dim": "time"},
    "w": {
        "encoding": {
            "unknown": {
                "compressor": numcodecs.get_codec(dict(id="bz2", level=5)),
            },
            "time": {"units": "nanoseconds since 1970-01-01"},
        }
    },
}

extra_kwarfs = zarr_mode_to_extra_kwargs["w"]


def unzip_and_load_xarray(filepath_and_num):
    with gzip.open(filepath_and_num[0], "rb") as f:
        with open(os.path.join(tmp, f"{filepath_and_num[1]}.grib2"), "wb") as f_out:
            shutil.copyfileobj(f, f_out)
    data = xr.load_dataset(
        os.path.join(tmp, f"{filepath_and_num[1]}.grib2"), engine="cfgrib"
    ).load()
    os.remove(os.path.join(tmp, f"{filepath_and_num[1]}.grib2"))
    return data


year = 2018
with tempfile.TemporaryDirectory() as tmp:
    mrms_files = sorted(
        list(glob(f"mtarchive.geol.iastate.edu/{year}/*/*/mrms/ncep/PrecipRate/*.gz"))
    )
    print(len(mrms_files))
    num = range(len(mrms_files))
    datasets = []
    first = False
    pool = Pool(processes=4)
    for data in tqdm(pool.imap(unzip_and_load_xarray, zip(mrms_files, num))):
        datasets.append(data)
        if len(datasets) >= 15:
            dataset = xr.concat(datasets, "time")
            dataset = dataset.chunk({"time": 15, "latitude": 700, "longitude": 1000})
            print(dataset)
            if first:
                dataset.to_zarr(
                    f"{year}.zarr", consolidated=True, compute=True, mode="w", **extra_kwarfs
                )
                first = False
            else:
                dataset.to_zarr(
                    f"{year}.zarr",
                    consolidated=True,
                    compute=True,
                    mode="a",
                    **zarr_mode_to_extra_kwargs["a"],
                )
            datasets = []
