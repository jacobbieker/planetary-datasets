"""
New way of doing the sources, sinks, and data writing
"""
import xarray as xr
import dataclasses
from abc import ABC, abstractmethod


@dataclasses.dataclass
class Source(ABC):
    source: str
    storage_options: dict | None

    @abstractmethod
    def timestamps(self) -> xr.Dataset:
        pass

    @abstractmethod
    def persist_files_being_processed(self) -> str:
        pass

    @abstractmethod
    def load_files_being_processed(self) -> list[str]:
        pass

    @abstractmethod
    def download_files(self) -> list[str]:
        pass


    @abstractmethod
    def create_dask_array(self) -> xr.Dataset:
        pass


    @abstractmethod
    def write_to_region(self) -> None:
        pass


    @abstractmethod
    def open_archive(self) -> xr.Dataset:
        pass


    @abstractmethod
    def get_xarray(self) -> xr.Dataset:
        pass
