"""NOAA Enterprise Precipitation Rate (EEPS) blended rain rate.

The EEPS "RainRate-Blend" product is a global, 5 km instantaneous rain-rate field
(``RRQPE``) with a companion data-quality flag (``DQF``), delivered as one netCDF per
timestep. The grid is not stored in the file: it has to be reconstructed from the
``geospatial_*`` attributes, which is the fiddly part this module encapsulates.

Files are read from a local directory (``<data_dir>/EEPS`` by default) because EEPS is
distributed through NOAA CLASS orders rather than an open bucket.
"""

from __future__ import annotations

import pathlib
from typing import List

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider

#: Files carrying the global 5 km blend. Other resolutions use a different infix.
DEFAULT_GLOB = "**/*GLB-5*.nc"


def timestamp_from_filename(filename: str | pathlib.Path) -> pd.Timestamp:
    """Extract the coverage start time from an EEPS filename.

    Names look like ``..._blend_s202401011200_e202401011210_....nc``; the ``s`` field is
    ``YYYYMMDDHHMM``.
    """
    start = str(filename).split("blend_s")[-1].split("_e")[0]
    return pd.Timestamp(
        f"{start[:4]}-{start[4:6]}-{start[6:8]}T{start[8:10]}:{start[10:12]}:00"
    )


def naive_utc(value) -> pd.Timestamp:
    """Normalise a timestamp to timezone-naive UTC.

    The CF coverage attributes carry a trailing ``Z`` and Dagster hands partition starts
    over as timezone-aware datetimes, while filenames and the stored ``time`` coordinate
    are naive. Converting explicitly stops numpy silently dropping the offset and stops
    naive/aware comparisons from quietly never matching.
    """
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts


def _axis(start: float, stop: float, resolution: float) -> np.ndarray:
    """Build an inclusive axis from ``start`` to ``stop`` at ``resolution`` spacing.

    The count is rounded before the axis is built rather than letting ``np.arange``
    accumulate float error towards the endpoint, which decides between 2401 and 2402
    points on the real global 5 km grid.
    """
    count = int(round((stop - start) / resolution)) + 1
    return np.linspace(start, stop, count)


def _grid_from_attrs(attrs: dict) -> tuple[np.ndarray, np.ndarray]:
    """Rebuild the latitude and longitude axes from the geospatial attributes.

    Latitudes are returned high to low, matching the row order of the arrays in the file.
    """
    latitudes = _axis(
        attrs["geospatial_lat_min"],
        attrs["geospatial_lat_max"],
        attrs["geospatial_lat_resolution"],
    )[::-1]
    longitudes = _axis(
        attrs["geospatial_lon_min"],
        attrs["geospatial_lon_max"],
        attrs["geospatial_lon_resolution"],
    )
    return latitudes, longitudes


def open_eeps_file(filename: str | pathlib.Path) -> xr.Dataset:
    """Open one EEPS netCDF and return it as a single-timestep, georeferenced dataset."""
    ds = xr.open_dataset(filename)
    latitudes, longitudes = _grid_from_attrs(ds.attrs)

    if ds.sizes.get("Rows") != latitudes.size or ds.sizes.get("Columns") != longitudes.size:
        raise ValueError(
            f"{pathlib.Path(filename).name}: grid from attributes is "
            f"{latitudes.size}x{longitudes.size} but the arrays are "
            f"{ds.sizes.get('Rows')}x{ds.sizes.get('Columns')}"
        )

    ds = ds.rename({"Rows": "latitude", "Columns": "longitude"})
    ds = ds.assign_coords(latitude=("latitude", latitudes), longitude=("longitude", longitudes))
    ds["DQF"] = ds["DQF"].astype(np.uint8)
    ds["RRQPE"] = ds["RRQPE"].astype(np.float16)

    metadata = ds["MonitoringMetaData"].attrs if "MonitoringMetaData" in ds else {}
    ds = ds.drop_vars(["quality_information", "MonitoringMetaData"], errors="ignore")

    start = np.datetime64(naive_utc(ds.attrs["time_coverage_start"]), "ns")
    end = np.datetime64(naive_utc(ds.attrs["time_coverage_end"]), "ns")

    ds = ds.expand_dims(dim="time")
    ds = ds.assign_coords(time=[start])
    ds["time_start"] = ("time", [start])
    ds["time_end"] = ("time", [end])
    ds["number_of_inputs"] = (
        "time",
        np.array([int(metadata.get("Number_of_Input_Files", 0))], dtype="int32"),
    )
    # The list of missing inputs is provenance, not data: keeping it as an attribute avoids
    # a variable-length string array in the store for something nothing reads per-pixel.
    ds.attrs["missing_inputs"] = str(metadata.get("Missing_Inputs", ""))
    return ds


class EEPSProvider(BaseProvider):
    """Blended global rain rate, one partition per available file.

    Args:
        source_dir: Directory holding the netCDF files. Defaults to ``<data_dir>/EEPS``.
        pattern: Glob used to find them within ``source_dir``.
        config: Optional configuration override.
    """

    name = "eeps"
    append_dim = "time"
    store_prefix = "bkr/eeps/eeps.icechunk"

    def __init__(
        self,
        source_dir: str | pathlib.Path | None = None,
        pattern: str = DEFAULT_GLOB,
        config=None,
    ):
        super().__init__(config=config)
        self._source_dir = pathlib.Path(source_dir) if source_dir is not None else None
        self.pattern = pattern

    @property
    def source_dir(self) -> pathlib.Path:
        """Directory the netCDF files are read from."""
        if self._source_dir is not None:
            return self._source_dir
        return self.config.data_dir / "EEPS"

    def available_timestamps(self) -> pd.DatetimeIndex:
        """Timestamps of every file currently on disk, sorted."""
        stamps = sorted({timestamp_from_filename(p) for p in self.source_dir.glob(self.pattern)})
        return pd.DatetimeIndex(stamps)

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Return the file covering ``it``, if it has been downloaded."""
        it = naive_utc(it)
        matches = [
            str(p)
            for p in sorted(self.source_dir.glob(self.pattern))
            if timestamp_from_filename(p) == it
        ]
        if not matches:
            logger.debug(f"{self.name}: no EEPS file for {it} under {self.source_dir}")
        return matches

    def run_partition(self, it: pd.Timestamp, check_present: bool = True) -> bool:
        """Run one timestep, accepting a timezone-aware partition start from Dagster.

        ``check_present`` is forwarded rather than dropped; :meth:`BaseProvider.run_range`
        passes it explicitly, so an override without it breaks every backfill.
        """
        return super().run_partition(naive_utc(it), check_present=check_present)

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Load the file for this partition and chunk it for writing."""
        if not input_files:
            raise FileNotFoundError(f"no EEPS file for {it} under {self.source_dir}")
        ds = open_eeps_file(input_files[0])
        return ds.chunk({"time": 1, "latitude": -1, "longitude": -1})
