"""
This is the interface for a new Source for Planetary Datasets, which defines how to get raw data from a remote
and get the raw files for processing. If the remote is already processed (i.e. cloud zarrs), then the source can
returned the already-processed data in Xarray, but otherwise returns the files. Its by design quite broad, to be flexible
for the different types of geospatial data that is being used, and ways of accessing time
"""
from abc import ABC, abstractmethod
from typing import Union, List
from datetime import datetime, timedelta
from pathlib import Path
import xarray as xr
import fsspec


class AbstractSource(ABC):
    def __init__(self, source_location: Path, raw_location: Path, start_date: datetime, end_date: datetime, **kwargs):
        self.source_location = source_location
        self.raw_location = raw_location
        self.start_date = start_date
        self.end_date = end_date
        # If there is a start and end date to the source, then we can use this to filter the data
        self.data_start_date = kwargs.get("data_start_date", None)
        self.data_end_date = kwargs.get("data_end_date", None)

    @abstractmethod
    def get(self, timestamp: datetime) -> Union[Path, xr.Dataset, xr.DataArray]:
        """Get the data, either returning paths to the downloaded raw data, or the Xarray dataset/dataarray if the data is already processed"""
        raise NotImplementedError

    def download_file(self, remote_path: Path, local_path: Path) -> Path:
        """Download a file from a remote location, first checking if local path exists and is valid"""
        full_local_path = self.raw_location / local_path
        if full_local_path.exists() and self.check_integrity(full_local_path):
            return full_local_path
        with fsspec.open(str(self.source_location / remote_path), "rb") as infile:
            with fsspec.open(str(full_local_path), "wb") as outfile:
                outfile.write(infile.read())
        return full_local_path

    @abstractmethod
    def process(self, files: list[Path]) -> Union[Path, xr.Dataset, xr.DataArray]:
        """Get the data, either returning paths to the downloaded raw data, or the Xarray dataset/dataarray if the data is already processed"""
        raise NotImplementedError

    @abstractmethod
    def check_integrity(self, local_path: Path) -> bool:
        """Check the integrity of the file"""
        raise NotImplementedError

