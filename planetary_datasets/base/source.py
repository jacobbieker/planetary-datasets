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


class AbstractSource(ABC):
    def __init__(self, source_location: Path, output_location: Path, start_date: datetime, end_date: datetime, **kwargs):
        self.source_location = source_location
        self.output_location = output_location
        self.start_date = start_date
        self.end_date = end_date

    @abstractmethod
    def get(self) -> Union[Path, xr.Dataset, xr.DataArray]:
        """Get the data, either returning paths to the downloaded raw data, or the Xarray dataset/dataarray if the data is already processed"""
        raise NotImplementedError


