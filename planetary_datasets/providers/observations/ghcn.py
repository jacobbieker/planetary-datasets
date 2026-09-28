"""GHCN-hourly (GHCNh), NOAA's successor to the Integrated Surface Database.

Two routes to the same network are kept, because the repository had a script for each:

* :class:`GHCNHourlyProvider` pulls a whole region in one request through the ``meteora``
  ``GHCNHourlyClient``, which is what ``dags/assets/icechunky/meteora_ghcnh.py`` did. This
  is the one wired up as a Dagster asset: a single quarterly request beats one per
  station across a roster of nearly 39,000.
* :class:`GHCNHourlyStationProvider` reads the NCEI by-year parquet files one station at a
  time, which is what ``dags/assets/observation/ghcn.py`` did. It requires an explicit
  station list — the full roster is nearly 39,000 sites and a dense cube of all of them
  is neither downloadable nor writable in one partition.

``load_ghcnh_data`` and ``get_list_of_stations`` are kept as module functions so the
original ad hoc use still works.

Environment: none. Both routes are anonymous HTTP.
"""

from __future__ import annotations

import pathlib

import fsspec
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common.store import write_to_icechunk as _write_to_icechunk
from planetary_datasets.providers.observations.base import (
    OBSERVATION_ALIGNMENT_COORDS,
    NoStationDataError,
    Station,
    StationObservationProvider,
    frames_to_dataset,
    partial_path,
    partition_time_index,
)

#: Name of the station level in meteora's ``(station_id, time)`` MultiIndex.
STATION_LEVEL = "station_id"

STATION_LIST_URL = (
    "https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/doc/"
    "ghcnh-station-list.txt"
)
PARQUET_URL = (
    "https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/access/"
    "by-year/{year}/parquet/GHCNh_{station_id}_{year}.parquet"
)

#: Variables requested through meteora. These are the ones the original script asked for.
METEORA_VARIABLES = (
    "temperature",
    "dew_point_temperature",
    "sea_level_pressure",
    "wind_direction",
    "wind_speed",
    "wind_gust",
    "relative_humidity",
    "visibility",
    "snow_depth",
    "precipitation_3_hour",
    "precipitation_12_hour",
    "precipitation_24_hour",
)

#: Numeric columns kept from the NCEI parquet files.
PARQUET_VARIABLES = (
    "temperature",
    "dew_point_temperature",
    "station_level_pressure",
    "sea_level_pressure",
    "wind_direction",
    "wind_speed",
    "wind_gust",
    "relative_humidity",
    "visibility",
    "precipitation",
    "snow_depth",
)


def load_ghcnh_data(station_id: str, year: int) -> pd.DataFrame:
    """Read one station-year of GHCNh from the NCEI parquet archive."""
    with fsspec.open(PARQUET_URL.format(station_id=station_id, year=year)) as handle:
        return pd.read_parquet(handle)


def get_list_of_stations(url: str = STATION_LIST_URL) -> pd.DataFrame:
    """Read the GHCNh station list into a dataframe.

    The file is whitespace-delimited with the id first, then latitude, longitude and
    elevation. The original parsed latitude and longitude the wrong way round; they are
    read here in the file's own order.
    """
    rows = []
    with fsspec.open(url) as handle:
        for raw in handle:
            parts = raw.decode("utf-8", errors="replace").split()
            if len(parts) < 4:
                continue
            try:
                rows.append((parts[0], float(parts[1]), float(parts[2]), float(parts[3])))
            except ValueError:
                # The header and any malformed line are simply not station records.
                continue
    return pd.DataFrame(rows, columns=["station_id", "latitude", "longitude", "elevation"])


def read_station_roster(df: pd.DataFrame) -> list[Station]:
    """Turn meteora's station table into the canonical station axis.

    Sorted by identifier so the axis does not depend on the order the region query
    happened to return.
    """
    stations = {}
    for station_id, row in df.iterrows():
        identifier = str(station_id)
        stations[identifier] = Station(
            id=identifier,
            latitude=_as_float(row.get("LATITUDE")),
            longitude=_as_float(row.get("LONGITUDE")),
            elevation=_as_float(row.get("ELEVATION")),
            name=None if pd.isna(row.get("NAME")) else str(row.get("NAME")),
        )
    return [stations[key] for key in sorted(stations)]


def _as_float(value) -> float | None:
    number = pd.to_numeric(value, errors="coerce")
    return None if pd.isna(number) else float(number)


class GHCNHourlyStationProvider(StationObservationProvider):
    """GHCNh read station by station from the NCEI by-year parquet archive."""

    name = "ghcnh_stations"
    store_prefix = "bkr/obs/ghcnh_stations.icechunk"
    source_url = PARQUET_URL

    partition_freq = "MS"
    sample_freq = "1h"
    align_how = "last"
    variables = PARQUET_VARIABLES
    max_workers = 8

    def __init__(self, config=None, stations=None):
        """Build the provider.

        Args:
            config: Optional configuration override.
            stations: Required. Ids or :class:`Station` objects.
        """
        if stations is None:
            raise ValueError(
                "GHCNHourlyStationProvider needs an explicit station list: the full GHCNh "
                "roster is nearly 39,000 sites, which is neither downloadable per partition "
                "nor writable as a dense cube. Use GHCNHourlyProvider for whole regions."
            )
        super().__init__(config=config, stations=stations)

    @property
    def cache_dir(self) -> pathlib.Path:
        return self.config.data_dir / "ghcnh"

    def all_stations(self) -> list[Station]:
        df = get_list_of_stations()
        return [
            Station(
                id=row.station_id,
                latitude=row.latitude,
                longitude=row.longitude,
                elevation=row.elevation,
            )
            for row in df.itertuples()
        ]

    def station_filename(self, station: Station, it: pd.Timestamp) -> str:
        return f"{station.safe_id}-{pd.Timestamp(it).year}.parquet"

    def fetch_station(
        self,
        station: Station,
        it: pd.Timestamp,
        temp_dir: pathlib.Path,
    ) -> pathlib.Path | None:
        """Download the station-year parquet, cached so a year is fetched once."""
        year = pd.Timestamp(it).year
        destination = self.cache_dir / str(year) / self.station_filename(station, it)
        if destination.is_file() and destination.stat().st_size > 0:
            return destination

        url = PARQUET_URL.format(station_id=station.id, year=year)
        destination.parent.mkdir(parents=True, exist_ok=True)
        part = partial_path(destination)
        try:
            with fsspec.open(url, "rb") as src, open(part, "wb") as out:
                out.write(src.read())
        except FileNotFoundError:
            # Most stations have no file for most years.
            part.unlink(missing_ok=True)
            return None
        part.replace(destination)
        return destination

    def read_station(
        self,
        path: pathlib.Path,
        station: Station,
        it: pd.Timestamp,
    ) -> pd.DataFrame | None:
        df = pd.read_parquet(path)
        if df.empty:
            return None
        if "DATE" not in df.columns:
            logger.warning(f"{self.name}: {path.name} has no DATE column")
            return None
        index = pd.to_datetime(df["DATE"], errors="coerce", utc=True).dt.tz_localize(None)
        df = df.set_index(pd.DatetimeIndex(index))
        df = df[df.index.notna()]
        keep = [v for v in self.variables if v in df.columns]
        if not keep:
            logger.warning(f"{self.name}: {path.name} has none of the expected columns")
            return None
        times = self.partition_times(it)
        window = df.loc[
            (df.index >= times[0]) & (df.index < times[-1] + pd.Timedelta(self.sample_freq))
        ]
        return window[keep] if not window.empty else None


class GHCNHourlyProvider(BaseProvider):
    """GHCNh for a whole region, fetched in one meteora request per partition.

    ``meteora`` returns a long-format frame indexed by ``(station_id, time)`` plus a
    station GeoDataFrame. Both are parked in the partition's temporary directory by
    :meth:`fetch` and reshaped into the usual ``(time, station)`` cube by :meth:`process`.

    The original script piped the frame through ``meteora.utils.long_to_cube`` and
    ``xvec.encode_cf`` before writing NetCDF. That path is not used here: it needs the
    optional ``xvec`` dependency, and the CF encoder rejects the tz-aware time axis
    meteora produces. Reshaping directly also puts this store in the same layout as the
    rest of the subpackage.
    """

    name = "ghcnh"
    store_prefix = "bkr/obs/ghcnh.icechunk"
    append_dim = "time"

    partition_freq = "QS"
    #: GHCNh is delivered at sub-hourly resolution; the last report in each hour is kept.
    sample_freq = "1h"
    align_how = "last"

    #: Bounding box as ``(west, south, east, north)``. Global by default, as in the
    #: original script. A global quarter is a very large request; pass a region for
    #: anything smaller.
    default_region = (-180.0, -90.0, 180.0, 90.0)

    def __init__(self, config=None, region=None, variables=METEORA_VARIABLES):
        """Build the provider.

        Args:
            config: Optional configuration override.
            region: Bounding box, GeoSeries or path meteora can turn into a region.
            variables: meteora variable names to request.
        """
        super().__init__(config=config)
        self.region = list(region) if region is not None else list(self.default_region)
        self.variables = list(variables)

    def partition_times(self, it: pd.Timestamp) -> pd.DatetimeIndex:
        """The hourly grid covering the partition's quarter."""
        return partition_time_index(it, self.partition_freq, self.sample_freq)

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> list[str]:
        """Pull one quarter and park the observations and station table on disk.

        meteora does the HTTP itself, so there is nothing to download here; writing the
        two frames out keeps fetch and process separable and the inputs inspectable.
        """
        from meteora.clients import GHCNHourlyClient

        directory = pathlib.Path(temp_dir) if temp_dir is not None else self.config.scratch_dir
        directory.mkdir(parents=True, exist_ok=True)

        times = self.partition_times(it)
        start, end = times[0], times[-1]
        logger.info(f"{self.name}: requesting {start:%Y-%m-%d} to {end:%Y-%m-%d}")

        client = GHCNHourlyClient(self.region)
        ts_df = client.get_ts_df(self.variables, start, end)
        if ts_df is None or len(ts_df) == 0:
            logger.info(f"{self.name}: no observations returned for {start:%Y-%m}")
            return []

        observations = directory / f"ghcnh_{start:%Y%m}.parquet"
        stations = directory / f"ghcnh_{start:%Y%m}_stations.parquet"
        ts_df.to_parquet(observations)
        # The geometry column duplicates LATITUDE/LONGITUDE and does not round-trip
        # through plain parquet, so it is dropped.
        client.stations_gdf.drop(columns="geometry", errors="ignore").to_parquet(stations)
        return [str(observations), str(stations)]

    def process(
        self,
        input_files: list[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Reshape the long-format quarter into a ``(time, station)`` cube."""
        observations, stations = (pathlib.Path(p) for p in input_files[:2])
        ts_df = pd.read_parquet(observations)
        roster = read_station_roster(pd.read_parquet(stations))

        frames = {
            str(station_id): group.reset_index(level=STATION_LEVEL, drop=True)
            for station_id, group in ts_df.groupby(level=STATION_LEVEL)
        }
        known = {s.id for s in roster}
        unexpected = sorted(set(frames) - known)
        if unexpected:
            raise NoStationDataError(
                f"{self.name}: {len(unexpected)} station(s) reported but are absent from the "
                f"station table, e.g. {unexpected[:5]}"
            )

        return frames_to_dataset(
            frames,
            roster,
            self.partition_times(it),
            variables=self.variables,
            sample_freq=self.sample_freq,
            how=self.align_how,
            attrs={"network": self.name, "source": "ghcnh-via-meteora"},
        )

    def write_to_icechunk(self, repo, processed: xr.Dataset) -> bool:
        """Write, refusing to append when the station axis has drifted."""
        return _write_to_icechunk(
            repo,
            processed,
            append_dim=self.append_dim,
            message=f"{self.name}: {processed[self.append_dim].values[0]}",
            alignment_coords=OBSERVATION_ALIGNMENT_COORDS,
        )
