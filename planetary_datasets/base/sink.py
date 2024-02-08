from abc import ABC, abstractmethod
from typing import Union, List
from datetime import datetime, timedelta
from pathlib import Path
import xarray as xr


class AbstractSink(ABC):
    def __init__(self, processed_location: Path, output_location: Path, start_date: datetime, end_date: datetime,
                 **kwargs):
        self.processed_location = processed_location
        self.output_location = output_location
        self.start_date = start_date
        self.end_date = end_date

    @abstractmethod
    def save(self, paths: Union[List[xr.DataArray], List[xr.Dataset], List[Path]]) -> None:
        """Save files to a location, local or remote"""
        raise NotImplementedError

    @abstractmethod
    def compress(self) -> None:
        """Compress the files"""
        raise NotImplementedError

    @abstractmethod
    def load(self) -> Union[xr.Dataset, xr.DataArray]:
        """Load the files"""
        raise NotImplementedError