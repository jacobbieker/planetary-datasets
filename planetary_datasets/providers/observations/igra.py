"""IGRA v2 radiosonde soundings.

Two independent routes to the same archive, both kept because they answer different
questions:

**CDS route** — the Copernicus Climate Data Store serves IGRA as the
``insitu-observations-igra-baseline-network`` dataset, one NetCDF per request period.
This is the raw-download stage (:func:`download_igra_month`) and the icechunk stage
(:class:`IGRACDSProvider`), chained: the provider reuses a file the downloader already
placed in the data directory, and downloads it itself when run alone. Partitions are
calendar months, matching how the CDS bills and rate-limits requests.

**Station route** — NCEI publishes one archive file per station, which is how the
existing ``igra_v2_16_levels.icechunk`` store was built: :class:`IGRAStationArchive`
reproduces that build. It is a whole-archive job rather than a partitioned one, because
each downloaded file spans a station's entire record and the store is shaped
``(station, time, level)``.

Consolidates ``igra_stuff.py``, ``dags/assets/observation/igra.py``,
``dags/assets/icechunky/igra_icechunk.py`` and ``dags/assets/icechunky/amusa.py`` (which
is IGRA despite its name), and ``dags/assets/nwp/igra_writer.py``.

Environment:
    ``CDSAPI_URL`` / ``CDSAPI_KEY``  Credentials for the CDS route. A ``~/.cdsapirc`` file
                                     is used instead when these are unset.
    ``IGRA_STATION_DIR``             Where the station route caches NCEI archive files.
                                     Defaults to ``<PLANETARY_DATASETS_DATA_DIR>/igra``.
"""

from __future__ import annotations

import hashlib
import os
import pathlib
from typing import Iterable, List, Sequence

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.common.download import download_one
from planetary_datasets.common.store import write_to_icechunk
from planetary_datasets.config import Config, get_config
from planetary_datasets.memory import estimate_dataset_gb, require_memory
from planetary_datasets.providers.observations.upper_air_common import (
    PointObservationProvider,
    table_to_dataset,
)

# --- CDS route ----------------------------------------------------------------------

IGRA_CDS_DATASET = "insitu-observations-igra-baseline-network"
IGRA_CDS_ARCHIVE = "integrated_global_radiosonde_archive_version_2"

#: The six sounding variables the original request asked for.
IGRA_CDS_VARIABLES = (
    "air_dewpoint_depression",
    "air_temperature",
    "geopotential_height",
    "relative_humidity",
    "wind_from_direction",
    "wind_speed",
)

ALL_DAYS = tuple(f"{day:02d}" for day in range(1, 32))

#: CDS serves IGRA as a long-form observation table. Columns are renamed onto the same
#: names the AMDAR and Sondehub tables use, so the three stores read alike.
#:
#: Several targets have more than one possible source, so this is a list of candidates in
#: preference order rather than a plain mapping: a CDS file carries both
#: ``primary_station_id`` and ``station_name``, and renaming both to ``station_id`` would
#: leave the frame with two identically-named columns.
IGRA_CDS_RENAMES = (
    ("observation", ("observation_value",)),
    ("observation_type", ("observed_variable",)),
    ("pressure", ("z_coordinate", "air_pressure")),
    ("station_id", ("primary_station_id", "station_id")),
    ("report_id", ("report_id",)),
    ("latitude", ("latitude|station_configuration", "latitude")),
    ("longitude", ("longitude|station_configuration", "longitude")),
    ("elevation", ("height_of_station_above_sea_level",)),
)

#: Candidate names for the observation time, in preference order.
IGRA_CDS_TIME_COLUMNS = ("report_timestamp", "date_time", "datetime", "time")

IGRA_CDS_SCHEMA = {
    "observation": "float32",
    "observation_type": "str",
    "pressure": "float32",
    "latitude": "float32",
    "longitude": "float32",
    "elevation": "float32",
    "station_id": "str",
    "report_id": "str",
}


def cds_client(config: Config | None = None):
    """Build a ``cdsapi`` client from the configured credentials.

    Falls back to ``~/.cdsapirc``, which ``cdsapi`` reads itself. When neither is present
    the credential requirement is raised by name rather than letting ``cdsapi`` fail with
    a bare exception about a missing URL.
    """
    import cdsapi

    cfg = config or get_config()
    creds = cfg.credentials
    if creds.cdsapi_url and creds.cdsapi_key:
        return cdsapi.Client(url=creds.cdsapi_url, key=creds.cdsapi_key)
    if not (pathlib.Path.home() / ".cdsapirc").is_file():
        creds.require("cdsapi_url", "cdsapi_key")
    return cdsapi.Client()


def igra_cds_request(
    year: int,
    months: Sequence[str],
    variables: Sequence[str] | None = None,
) -> dict:
    """Build the CDS request body for a period."""
    return {
        "archive": IGRA_CDS_ARCHIVE,
        "variable": list(variables or IGRA_CDS_VARIABLES),
        "year": [str(year)],
        "month": [f"{int(m):02d}" for m in months],
        "day": list(ALL_DAYS),
        "data_format": "netcdf",
    }


def _readable_netcdf(path: pathlib.Path) -> bool:
    """True when the file exists and xarray can open it.

    The original re-requested any file that failed to open; a CDS download interrupted
    part-way leaves a plausible-looking but truncated NetCDF behind.
    """
    if not path.is_file() or path.stat().st_size == 0:
        return False
    try:
        with xr.open_dataset(path):
            return True
    except Exception as exc:  # noqa: BLE001 - any failure to open means "fetch it again"
        logger.warning(f"{path.name} exists but could not be opened ({exc}), re-downloading")
        return False


def download_igra_month(
    year: int,
    month: int,
    dest_dir: str | os.PathLike,
    config: Config | None = None,
    variables: Sequence[str] | None = None,
    overwrite: bool = False,
) -> pathlib.Path:
    """Download one month of IGRA from the CDS, returning the local NetCDF path.

    An existing readable file is reused. The download lands on a ``.part`` sidecar and is
    renamed only once complete, so an interrupted run never leaves a file the reuse check
    would accept.
    """
    dest_dir = pathlib.Path(dest_dir).expanduser()
    dest_dir.mkdir(parents=True, exist_ok=True)
    target = dest_dir / f"igra_{year}{int(month):02d}.nc"

    if not overwrite and _readable_netcdf(target):
        logger.debug(f"reusing {target.name}")
        return target

    partial = target.with_name(target.name + ".part")
    partial.unlink(missing_ok=True)
    client = cds_client(config)
    logger.info(f"requesting IGRA {year}-{int(month):02d} from the CDS")
    request = igra_cds_request(year, [month], variables)
    client.retrieve(IGRA_CDS_DATASET, request, target=str(partial))
    if not partial.is_file() or partial.stat().st_size == 0:
        partial.unlink(missing_ok=True)
        raise RuntimeError(f"CDS returned no data for IGRA {year}-{int(month):02d}")
    os.replace(partial, target)
    return target


def cds_to_table(ds: xr.Dataset) -> pd.DataFrame:
    """Flatten a CDS IGRA NetCDF into a table of individually-timed observations.

    The CDS observation tables carry one row per measured value with the quantity in an
    ``observed_variable`` column, so the layout maps straight onto the long form used by
    the other upper-air stores. Columns outside :data:`IGRA_CDS_SCHEMA` are dropped.
    """
    frame = ds.to_dataframe().reset_index()

    renames: dict[str, str] = {}
    for target, candidates in IGRA_CDS_RENAMES:
        source = next((c for c in candidates if c in frame.columns), None)
        # Only the first candidate present is taken, so two source columns can never end
        # up sharing a name — which would make frame[name] a DataFrame, not a Series.
        if source is not None and source != target:
            renames[source] = target
    frame = frame.drop(columns=[t for t in renames.values() if t in frame.columns])
    frame = frame.rename(columns=renames)

    time_column = next((c for c in IGRA_CDS_TIME_COLUMNS if c in frame.columns), None)
    if time_column is None:
        raise ValueError(
            "no recognised time column in the CDS IGRA file; looked for "
            f"{IGRA_CDS_TIME_COLUMNS}, found {sorted(frame.columns)[:15]}"
        )
    if time_column != "time":
        frame = frame.rename(columns={time_column: "time"})

    # Byte strings are how NetCDF carries text; decode before they reach the store.
    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = frame[column].map(
                lambda v: v.decode("utf-8", "ignore").strip() if isinstance(v, bytes) else v
            )

    keep = ["time", *(c for c in IGRA_CDS_SCHEMA if c in frame.columns)]
    return frame[keep]


class IGRACDSProvider(PointObservationProvider):
    """Monthly IGRA soundings from the Copernicus Climate Data Store.

    ``fetch`` reuses a NetCDF already sitting in the raw directory — which is what the
    download asset puts there — and requests it from the CDS otherwise.
    """

    name = "igra_cds"
    append_dim = "time"
    store_prefix = "bkr/observation/igra_cds.icechunk"
    partition_freq = "MS"

    def __init__(
        self,
        config: Config | None = None,
        raw_dir: str | os.PathLike | None = None,
        variables: Sequence[str] | None = None,
    ):
        super().__init__(config=config)
        self._raw_dir = pathlib.Path(raw_dir).expanduser() if raw_dir else None
        self.variables = tuple(variables or IGRA_CDS_VARIABLES)

    @property
    def raw_dir(self) -> pathlib.Path:
        """Directory the downloaded CDS NetCDFs live in."""
        if self._raw_dir is not None:
            return self._raw_dir
        return self.config.data_dir / "igra_cds"

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Return the month's CDS NetCDF, downloading it if it is not already local."""
        it = pd.Timestamp(it)
        path = download_igra_month(
            it.year,
            it.month,
            self.raw_dir,
            config=self.config,
            variables=self.variables,
        )
        return [str(path)]

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Flatten the month's soundings and trim them to the partition's month."""
        tables = []
        for path in input_files:
            with xr.open_dataset(path) as ds:
                tables.append(cds_to_table(ds))
        table = pd.concat(tables, ignore_index=True) if tables else pd.DataFrame({"time": []})
        dataset = table_to_dataset(
            table,
            IGRA_CDS_SCHEMA,
            attrs={"source": f"CDS {IGRA_CDS_DATASET}", "archive": IGRA_CDS_ARCHIVE},
        )
        return self.trim_to_window(dataset, it)


# --- Station route ---------------------------------------------------------------------

#: NCEI column names to the names used in the store. Applied in one pass; the original
#: did it in two, which made the pressure/level rename easy to miss.
IGRA_STATION_RENAMES = {
    "pres": "level",
    "gph": "geopotential_height",
    "temp": "temperature",
    "rhumi": "relative_humidity",
    "windd": "wind_direction",
    "winds": "wind_speed",
    "dpd": "dew_point_depression",
    "date": "time",
    "numlev": "number_of_measured_levels",
    "lat": "latitude",
    "lon": "longitude",
}

#: Sounding variables placed on the ``(time, level)`` grid, in store order.
IGRA_STATION_VARIABLES = (
    "geopotential_height",
    "temperature",
    "relative_humidity",
    "wind_direction",
    "wind_speed",
    "dew_point_depression",
)

#: Per-sounding metadata columns, which vary with time but not with level.
IGRA_STATION_METADATA_FIELDS = (
    ("lat", "latitude"),
    ("lon", "longitude"),
    ("numlev", "number_of_measured_levels"),
)

#: NCEI's public IGRA v2 archive. Anonymous; nothing here is a credential.
IGRA_ARCHIVE_URL = "https://www1.ncdc.noaa.gov/pub/data/igra/data/data-por"
IGRA_STATION_LIST_URL = "https://www1.ncdc.noaa.gov/pub/data/igra/igra2-station-list.txt"


#: Fixed-width layout of ``igra2-station-list.txt``, from NCEI's format document.
STATION_LIST_COLSPECS = [
    (0, 11),
    (12, 20),
    (21, 30),
    (31, 37),
    (38, 40),
    (41, 71),
    (72, 76),
    (77, 81),
    (82, 88),
]
STATION_LIST_COLUMNS = ["id", "lat", "lon", "alt", "state", "name", "start", "end", "total"]

#: Sentinels the station list uses for an unknown position or elevation, from NCEI's format
#: document: latitude -98.8888, longitude -998.8888, elevation -999.9 or -998.8. The
#: longitude and second elevation sentinels were previously absent, so a station with an
#: unknown longitude was placed at -98.8888 degrees east rather than dropped.
STATION_LIST_MISSING = (-98.8888, -998.8888, -999.9, -998.8, -99.9999, -999.0)


def parse_station_list(path: str | os.PathLike) -> pd.DataFrame:
    """Parse ``igra2-station-list.txt`` into a table indexed by station identifier.

    Parsed here rather than with ``igra.read.stationlist``: that reader casts every
    latitude field with a bare ``float()`` and the current list has blank fields, so it
    raises part-way through. Missing-value sentinels become NaN.
    """
    stations = pd.read_fwf(
        path,
        colspecs=STATION_LIST_COLSPECS,
        names=STATION_LIST_COLUMNS,
        dtype={"id": str, "state": str, "name": str},
    )
    for column in ("lat", "lon", "alt", "start", "end", "total"):
        stations[column] = pd.to_numeric(stations[column], errors="coerce")
    for column in ("lat", "lon", "alt"):
        stations.loc[stations[column].isin(STATION_LIST_MISSING), column] = np.nan
    return stations.set_index("id")


def station_list(
    directory: str | os.PathLike | None = None,
    config: Config | None = None,
) -> pd.DataFrame:
    """Return the IGRA v2 station table, downloading the list if it is not cached.

    The download goes through :func:`~planetary_datasets.common.download.download_one`
    rather than ``igra.download.stationlist``: that helper calls ``urllib.request``
    having imported only ``urllib``, so it raises ``AttributeError`` unless something
    else has already imported the submodule.
    """
    cfg = config or get_config()
    directory = pathlib.Path(directory).expanduser() if directory else _station_dir(cfg)
    directory.mkdir(parents=True, exist_ok=True)
    listing = directory / "igra2-station-list.txt"
    if not listing.is_file():
        logger.info("downloading the IGRA station list")
        if download_one(IGRA_STATION_LIST_URL, listing) is None:
            raise RuntimeError(
                f"could not download the IGRA station list from {IGRA_STATION_LIST_URL}"
            )
    return parse_station_list(listing)


def select_stations(
    stations: pd.DataFrame,
    min_end_year: int = 2020,
    min_reports: int = 100,
) -> pd.DataFrame:
    """Filter the station table the way the original archive build did.

    Stations still reporting recently, with enough soundings to be worth a request, and
    with a usable position.
    """
    selected = stations
    if "end" in selected.columns:
        selected = selected[selected["end"] >= min_end_year]
    if "total" in selected.columns:
        selected = selected[selected["total"] > min_reports]
    return selected.dropna(subset=["lat", "lon"])


def _station_dir(config: Config) -> pathlib.Path:
    configured = os.environ.get("IGRA_STATION_DIR")
    if configured:
        return pathlib.Path(configured).expanduser()
    return config.data_dir / "igra"


def download_station(
    ident: str,
    directory: str | os.PathLike | None = None,
    config: Config | None = None,
    overwrite: bool = False,
) -> pathlib.Path:
    """Download one station's whole-record archive file from NCEI.

    Returns the path to ``<ident>-data.txt.zip``. An existing file is reused.
    """
    cfg = config or get_config()
    directory = pathlib.Path(directory).expanduser() if directory else _station_dir(cfg)
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"{ident}-data.txt.zip"
    url = f"{IGRA_ARCHIVE_URL}/{ident}-data.txt.zip"
    if download_one(url, target, overwrite=overwrite) is None:
        raise FileNotFoundError(f"NCEI has no archive file for station {ident}")
    return target


def standard_levels(levels: Sequence[float] | None = None) -> List[float]:
    """The pressure levels a station is placed on. Defaults to IGRA's 16 standard levels."""
    if levels is not None:
        return list(levels)
    import igra as igra_lib

    return list(igra_lib.std_plevels)


def station_table(path: str | os.PathLike) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Parse one station's archive file into its sounding table and its metadata table.

    Uses ``igra.read.ascii_to_dataframe`` rather than ``igra.read.igra``: the latter runs
    the package's own interpolation onto standard levels, which calls ``np.in1d`` and so
    raises under numpy 2. IGRA already reports the standard levels directly, so selecting
    them is enough and no interpolation is needed.
    """
    import igra as igra_lib

    return igra_lib.read.ascii_to_dataframe(str(path), all_columns=True)


def read_station(
    ident: str,
    path: str | os.PathLike,
    levels: Sequence[float] | None = None,
) -> xr.Dataset:
    """Read one station's archive file into a ``(station, time, level)`` dataset.

    Args:
        ident: IGRA station identifier.
        path: The station's ``-data.txt.zip`` file.
        levels: Pressure levels to keep. Defaults to IGRA's 16 standard levels, which is
            what the published store holds. Every station is placed on the full set, so
            stations that never reported a level still line up for the append.
    """
    levels = standard_levels(levels)
    frame, metadata = station_table(path)
    frame = frame.rename(columns=IGRA_STATION_RENAMES)
    frame.index.name = "time"

    keep = [c for c in IGRA_STATION_VARIABLES if c in frame.columns]
    frame = frame.loc[frame["level"].isin(levels), ["level", *keep]]
    # A sounding occasionally reports the same level twice; keep the first, as a duplicate
    # index entry cannot be reshaped onto a (time, level) grid.
    frame = frame.set_index("level", append=True)
    frame = frame[~frame.index.duplicated(keep="first")]

    data = frame.to_xarray().reindex(level=levels)

    for source, target in IGRA_STATION_METADATA_FIELDS:
        if source in metadata.columns:
            per_sounding = metadata[source]
            per_sounding = per_sounding[~per_sounding.index.duplicated(keep="first")]
            data[target] = xr.DataArray(
                per_sounding.reindex(data["time"].to_index()).to_numpy(dtype="float64"),
                dims="time",
            )

    data.coords["station"] = xr.DataArray(str(ident))
    data = data.expand_dims("station")

    for var in data.data_vars:
        if np.issubdtype(data[var].dtype, np.floating):
            data[var] = data[var].astype(np.float32)
    return data


class IGRAStationArchive:
    """Build the ``(station, time, level)`` IGRA store from NCEI's per-station files.

    This is not a :class:`~planetary_datasets.base.BaseProvider`: a partition of it is a
    station, not a time window, and each downloaded file spans a station's whole record.

    The build runs in two phases, following the originals:

    1. Each station is parsed and staged as a NetCDF on *its own* time axis. Staging on
       the shared axis instead — which one of the original scripts did — makes every
       station a dense array over every sounding time any station ever reported:
       hundreds of gigabytes for a cube that is almost entirely missing.
    2. The staged files are opened lazily with an outer join, which aligns them onto the
       union time axis without materialising it, and are written to the store in slices
       along ``time``. One slice is loaded at a time, so peak memory is set by
       ``slice_size`` rather than by the size of the archive.

    Appending along ``time`` rather than ``station`` is what the published store does. The
    consequence is that adding a station later means rebuilding, because the station axis
    is fixed by the first write.

    Args:
        config: Configuration override.
        directory: Where archive files are cached. Defaults to ``IGRA_STATION_DIR``.
        levels: Pressure levels. Defaults to IGRA's 16 standard levels.
        store_prefix: Destination store.
        stage_dir: Where the per-station NetCDFs are staged. Defaults to a directory under
            the configured scratch directory.
    """

    name = "igra_stations"
    default_store_prefix = "bkr/observation/igra_v2_16_levels.icechunk"

    def __init__(
        self,
        config: Config | None = None,
        directory: str | os.PathLike | None = None,
        levels: Sequence[float] | None = None,
        store_prefix: str | None = None,
        stage_dir: str | os.PathLike | None = None,
    ):
        self._config = config
        self._directory = pathlib.Path(directory).expanduser() if directory else None
        self._levels = list(levels) if levels is not None else None
        self._stage_dir = pathlib.Path(stage_dir).expanduser() if stage_dir else None
        self.store_prefix = store_prefix or self.default_store_prefix

    @property
    def config(self) -> Config:
        """Configuration for this build, defaulting to the process-wide config."""
        return self._config if self._config is not None else get_config()

    @property
    def directory(self) -> pathlib.Path:
        """Directory the NCEI archive files are cached in."""
        return self._directory if self._directory is not None else _station_dir(self.config)

    @property
    def stage_dir(self) -> pathlib.Path:
        """Directory the per-station NetCDFs are staged in.

        Keyed by the level set: staging AGM00060355 on 16 levels and then on 32 would
        otherwise reuse the first file, and the outer join would silently mix two level
        axes into one store.
        """
        base = self._stage_dir
        if base is None:
            base = self.config.scratch_dir / "igra_stations"
        digest = hashlib.blake2b(repr(self.levels).encode(), digest_size=4).hexdigest()
        return base / f"levels-{len(self.levels)}-{digest}"

    @property
    def levels(self) -> List[float]:
        """Pressure levels every station is placed on."""
        return standard_levels(self._levels)

    def download(self, idents: Iterable[str], overwrite: bool = False) -> dict:
        """Download every station, skipping and logging the ones NCEI does not have."""
        paths = {}
        for ident in idents:
            try:
                paths[ident] = download_station(
                    ident, self.directory, config=self.config, overwrite=overwrite
                )
            except Exception as exc:  # noqa: BLE001 - a station gap must not stop the archive
                logger.warning(f"could not download station {ident}: {exc}")
        return paths

    def stage(self, paths: dict, overwrite: bool = False) -> List[pathlib.Path]:
        """Parse each station into a NetCDF on its own time axis. Returns the staged files.

        Stations that cannot be parsed are logged and skipped: a handful of the archive
        files are malformed, and one of them must not fail the whole build.
        """
        stage_dir = self.stage_dir
        stage_dir.mkdir(parents=True, exist_ok=True)
        staged = []
        for ident, path in paths.items():
            target = stage_dir / f"{ident}.nc"
            if target.is_file() and target.stat().st_size > 0 and not overwrite:
                staged.append(target)
                continue
            try:
                dataset = read_station(ident, path, levels=self.levels)
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"could not read station {ident}: {exc}")
                continue
            if dataset.sizes.get("time", 0) == 0:
                logger.warning(f"station {ident} has no soundings on the requested levels")
                continue
            partial = target.with_name(target.name + ".part")
            dataset.to_netcdf(partial, mode="w")
            os.replace(partial, target)
            staged.append(target)
        return sorted(staged)

    def open_staged(self, staged: Sequence[pathlib.Path]) -> xr.Dataset:
        """Open the staged stations lazily, aligned onto the union of their time axes."""
        combined = xr.open_mfdataset(
            [str(p) for p in staged],
            combine="nested",
            concat_dim="station",
            join="outer",
            # Opened one at a time on purpose. The netCDF4 C library is not thread-safe,
            # and parallel=True hands the opens to dask threads: handles were crossed
            # between files, so a station could come back carrying its neighbour's
            # identifier, and an invalidated handle surfaced as "NetCDF: Not a valid ID".
            # It failed roughly four runs in ten, more often on a loaded machine.
            parallel=False,
        )
        return combined.sortby("time")

    def build(
        self,
        idents: Sequence[str] | None = None,
        min_end_year: int = 2020,
        min_reports: int = 100,
        slice_size: int = 1000,
        time_chunk: int = 100,
        refresh: bool = False,
    ) -> int:
        """Download, stage and write the station archive. Returns the time steps written.

        Args:
            idents: Stations to build. Defaults to every station passing
                :func:`select_stations`.
            min_end_year: Passed to :func:`select_stations`.
            min_reports: Passed to :func:`select_stations`.
            slice_size: Sounding times loaded and committed at once.
            time_chunk: Zarr chunk length along ``time``.
            refresh: Re-download and re-parse every station. Both the NCEI archive file
                and the staged NetCDF are otherwise reused when present, so without this
                a rebuild after NCEI publishes new soundings would see none of them.
        """
        if idents is None:
            stations = station_list(self.directory, self.config)
            idents = list(select_stations(stations, min_end_year, min_reports).index)
        paths = self.download(idents, overwrite=refresh)
        if not paths:
            logger.warning("no IGRA station files available, nothing to build")
            return 0

        staged = self.stage(paths, overwrite=refresh)
        if not staged:
            logger.warning("no IGRA stations could be staged, nothing to build")
            return 0

        repo = self.config.icechunk_repo(self.store_prefix)
        written = 0
        # Closed explicitly: the staged files stay open behind xarray's handle cache
        # otherwise, and a later open of the same paths gets an invalidated handle.
        with self.open_staged(staged) as combined:
            total = combined.sizes["time"]
            logger.info(f"{len(staged)} station(s) over {total} sounding times")

            for start in range(0, total, slice_size):
                piece = combined.isel(time=slice(start, start + slice_size))
                # A slice is station_count x slice_size x levels, so slice_size alone does
                # not bound it: 1500 stations at the default is already most of a
                # gigabyte. Check before loading rather than finding out via the OOM
                # killer.
                require_memory(estimate_dataset_gb(piece), what=f"{self.name} slice at {start}")
                piece = piece.load()
                piece = piece.chunk({"station": -1, "time": min(time_chunk, piece.sizes["time"])})
                if write_to_icechunk(
                    repo,
                    piece,
                    append_dim="time",
                    message=f"IGRA stations, soundings {start}-{start + piece.sizes['time']}",
                    alignment_coords=("station", "level"),
                ):
                    written += piece.sizes["time"]
        return written


