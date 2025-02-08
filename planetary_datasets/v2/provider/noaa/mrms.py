from planetary_datasets.v2.core.datamodel import Source
import xarray as xr
import numpy as np
import gzip
import tempfile
import shutil
import os
from glob import glob
from tqdm import tqdm
import pandas as pd
import dask.array
from subprocess import Popen
import datetime as dt

MRMS_ARCHIVE_SITE = "mtarchive.geol.iastate.edu"
VARIABLE_OPTIONS = ["GaugeCorr_QPE_01H", "PrecipFlag", "PrecipRate", "RadarOnly_QPE_01H", "MultiSensor_QPE_01H_Pass2"]
ZARR_VARIABLE_NAMES = ["precipitation", "precipitation_flag", "precipitation_rate", "radar", "multi_sensor"]


class MRMSPrecipDataset(Source):
    def __init__(self, source: str, storage_options: dict | None, variable: str, precision: str = "float32"):
        super().__init__(source, storage_options)
        # Assert variable is only one of a few options
        assert variable in VARIABLE_OPTIONS, f"Variable must be one of {VARIABLE_OPTIONS}"
        self.variable = variable
        self.precision = precision
        # Get all the source files from the source location
        self.source_files = sorted(list(glob(f"{source}/*.gz")))

    def persist_files_being_processed(self) -> str:
        with open(f"mrms_{self.variable}_files.txt", "w") as f:
            for file in self.source_files:
                f.write(file + "\n")
        return f"mrms_{self.variable}_files.txt"

    def load_files_being_processed(self) -> list[str]:
        if os.path.exists(f"mrms_{self.variable}_files.txt"):
            with open(f"mrms_{self.variable}_files.txt", "r") as f:
                mrms_files = f.readlines()
                mrms_files = [x.strip() for x in mrms_files]
            return mrms_files
        else:
            return []

    def download_files(self) -> list[str]:
        pass

    def create_dask_array(self) -> xr.Dataset:
        pass

    def write_to_region(self) -> None:
        pass

    def open_archive(self, idx) -> xr.Dataset:
        filepath_and_num = (self.source_files[idx], idx)
        with tempfile.TemporaryDirectory() as tmp:
            with gzip.open(filepath_and_num[0], 'rb') as f:
                with open(os.path.join(tmp, f'{filepath_and_num[1]}.grib2'), 'wb') as f_out:
                    shutil.copyfileobj(f, f_out)
            data = xr.load_dataset(os.path.join(tmp, f'{filepath_and_num[1]}.grib2'), engine="cfgrib")
            data = data.rename({"unknown": self.variable})
            data = data.expand_dims(dim="time")
            data = data.chunk({"latitude": -1, "longitude": -1})
            # Change dtype to int16 as this is flags
            data = data.astype(self.precision)
            # Drop 'step' 'heightAboveSea' and 'valid_time'
            data = data.drop_vars(["step", "heightAboveSea", "valid_time"])
        if os.path.exists(os.path.join(tmp, f'{filepath_and_num[1]}.grib2')):
            os.remove(os.path.join(tmp, f'{filepath_and_num[1]}.grib2'))
        return data

    def get_xarray(self) -> xr.Dataset:
        pass

    def timestamps(self) -> list[dt.datetime]:
        return [pd.to_datetime(filename.split("_")[-1].split(".")[0], format="%Y%m%d-%H%M%S") for filename in self.source_files]



def get_mrms(day: dt.datetime):
    path = f"https://mtarchive.geol.iastate.edu/{day.strftime('%Y')}/{day.strftime('%m')}/{day.strftime('%d')}/mrms/ncep/GaugeCorr_QPE_01H/"
    args = ["wget", "-r", "-nc", "--no-parent", path]
    process = Popen(args)
    process.wait()
    path = f"https://mtarchive.geol.iastate.edu/{day.strftime('%Y')}/{day.strftime('%m')}/{day.strftime('%d')}/mrms/ncep/PrecipFlag/"
    args = ["wget", "-r", "-nc", "--no-parent", path]
    process = Popen(args)
    process.wait()
    path = f"https://mtarchive.geol.iastate.edu/{day.strftime('%Y')}/{day.strftime('%m')}/{day.strftime('%d')}/mrms/ncep/PrecipRate/"
    args = ["wget", "-r", "-nc", "--no-parent", path]
    process = Popen(args)
    process.wait()
    path = f"https://mtarchive.geol.iastate.edu/{day.strftime('%Y')}/{day.strftime('%m')}/{day.strftime('%d')}/mrms/ncep/RadarOnly_QPE_01H/"
    args = ["wget", "-r", "-nc", "--no-parent", path]
    process = Popen(args)
    process.wait()
    path = f"https://mtarchive.geol.iastate.edu/{day.strftime('%Y')}/{day.strftime('%m')}/{day.strftime('%d')}/mrms/ncep/MultiSensor_QPE_01H_Pass2/"
    args = ["wget", "-r", "-nc", "--no-parent", path]
    process = Popen(args)
    process.wait()


if __name__ == "__main__":
    date_range = pd.date_range(
        start="2024-01-01", end=dt.datetime.now().strftime("%Y-%m-%d"), freq="D"
    )
    for day in date_range:
        try:
            mrms_obs = get_mrms(day)
        except Exception as e:
            print(e)
            continue
