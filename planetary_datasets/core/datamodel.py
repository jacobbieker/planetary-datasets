import dataclasses
import dacite
import xarray as xr
from abc import ABC, abstractmethod

"""

Design:

There is a few different types of datasets that we want to access, potentially transform, 
potentially save out to disk, or to S3 at Source Cooperative / Hugging Face

The end result is everything in Zarr/kerchunk/Icechunk

We want a few data models:
1. Accessing Zarr / cloud optimized source
2. Acceessing NetCDF / Grib / HDF5 files, either locally, or over the network, to save to Zarr
3. Dataset that matches a Paper, accessing the previous two data models as needed

"""


class Dataset(ABC):

    @abstractmethod
    def open(self):
        pass

    @abstractmethod
    def save(self):
        pass

    @abstractmethod
    def transform(self):
        pass



@dataclasses.dataclass
class ARCODataset:
    location: str
    storage_options: dict | None
    regex: str | None

    def open(self):
        return xr.open_zarr(self.location, storage_options=self.storage_options)


@dataclasses.dataclass
class LegacyDataset:
    location: str
    storage_options: dict | None
    regex: str | None

