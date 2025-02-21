import monetio
import pandas as pd
import xarray as xr
from monetio.obs import ish
import datetime as dt
import s3fs
from planetary_datasets.base import AbstractSource, AbstractConvertor
from isd import Batch
from pathlib import Path


class ISDSource(AbstractSource):

    def get(self, timestamp: dt.datetime) -> xr.Dataset:
        # Fetch the ISD data for the given year
        files = self.get_isd_s3_files(timestamp)
        # Download the files
        local_files = []
        for file in files:
            local_files.append(self.download_file(f"s3://noaa-isd-pds/data/{timestamp.strftime('%Y')}/{file}", file))
        return self.process(local_files)


    @staticmethod
    def get_isd_s3_files(timestamp: dt.datetime) -> list:
        fs = s3fs.S3FileSystem(anon=True)
        return fs.listdir(f"s3://noaa-isd-pds/data/{timestamp.strftime('%Y')}/")

    def process(self, files: list[Path]) -> xr.Dataset:
        datasets = []
        # Group the files by the station IDs
        station_ids = set(["-".join(file.split("/")[-1].split("-")[:2]) for file in files])
        for station_id in station_ids:
            station_files = [file for file in files if station_id in file]
            datasets.append(self.process_station(station_files))

        # Concatenate all datasets together that have the same station_id
        return xr.concat(datasets, dim="station_id")

    def process_station(self, files: list[Path]) -> xr.Dataset:
        datasets = []
        for file in files:
            batch = Batch.from_path(file).to_data_frame()
            # Convert pandas to xarray
            records = batch.to_xarray()
            records = records.set_coords(["latitude", "longitude", "datetime"])
            # Set datetime as the dimension, and remove index
            records = records.set_index({"index": "datetime"}).rename_dims({"index": "datetime"})
            # Add coordinate that is the ID of the station
            # Create a combination of the usaf_id and ncei_id as the station_id
            records = records.assign_coords(station_id=records["usaf_id"] + "-" + records["ncei_id"])
            # Drop the id data variables
            records = records.drop_vars(["usaf_id", "ncei_id"])
            # Set the station_id as a single element
            records["station_id"] = records["station_id"].isel(datetime=0)
            # Do the same for latitude and longitude
            records["latitude"] = records["latitude"].isel(datetime=0)
            records["longitude"] = records["longitude"].isel(datetime=0)
            # Rename index coordinate to time_utc
            records = records.rename_dims({"datetime": "time"}).rename_vars({"index": "time"})
            datasets.append(records)
        return xr.concat(datasets, dim="time").sortby("time")

    def check_integrity(self, local_path: str) -> bool:
        pass

    def download_file(self, remote_path: str, local_path: str) -> str:
        fs = s3fs.S3FileSystem(anon=True)
        with fs.open(f"s3://noaa-isd-pds/data/{remote_path}", "rb") as infile:
            with open(local_path, "wb") as outfile:
                outfile.write(infile.read())
        return local_path


def get_isd_observations_monetio(
    start_time: dt.datetime, end_time: dt.datetime, resample: bool = False
) -> xr.Dataset:
    """Fetch ISD observations data for a given day using Monetio (Can be quite slow)."""
    dates = pd.date_range(start=start_time, end=end_time, freq="D")
    df = ish.add_data(dates, resample=resample, window="1H")
    ds = df.to_xarray()
    ds = ds.rename_vars({k: k.replace(" ", "_") for k in ds.data_vars.keys()})
    return ds

