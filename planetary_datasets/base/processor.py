from abc import ABC, abstractmethod
from typing import Union, List
from datetime import datetime, timedelta
from pathlib import Path
import xarray as xr


class AbstractProcessor(ABC):
    def __init__(self,
                 **kwargs):
        pass
    @abstractmethod
    def process(self) -> Union[Path, xr.Dataset, xr.DataArray]:
        """Get the data, either returning paths to the downloaded raw data, or the Xarray dataset/dataarray if the data is already processed"""
        raise NotImplementedError



