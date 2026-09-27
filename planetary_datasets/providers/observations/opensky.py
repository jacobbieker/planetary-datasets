"""OpenSky Network ADS-B state vectors.

The OpenSky Network publishes hourly samples of its global state-vector table as
``.csv.tar`` archives at https://s3.opensky-network.org/data-samples/states/. The sample
set covers every Monday from 2016-06-06 to 2022-06-27, all 24 hours of each day, and is
openly accessible with no credentials. Nothing here reads a credential, so no new
environment variable is introduced; the store location follows the usual
``ICECHUNK_*`` settings and scratch downloads land under
``PLANETARY_DATASETS_DATA_DIR``.

Two shapes of the same data are produced:

``OpenSkyStatesProvider``
    One icechunk store of flat state vectors, ``time`` as a ragged 1-D coordinate. This
    is the archive form: cheap to append an hour at a time, queryable by time window.

:func:`iter_trajectories` / :func:`write_trajectories`
    Per-aircraft trajectories, one dataset per ``icao24``. This is what
    ``one_offs/open_sky_to_icechunk.py`` set out to build and left commented out; it is
    implemented here. It is deliberately *not* an icechunk store: an archive keyed by
    airframe has tens of thousands of short, wildly differently-shaped members, which is
    a poor fit for a single chunked array. netCDF-per-airframe is what the downstream
    analysis (hazard correlation, below) actually consumes.

Column names differ between the CSV samples (``lat``, ``geoaltitude``, ``vertrate``) and
the newer parquet exports (``lat``, ``geoAltitude``, ``vertRate``). Both are accepted;
see :data:`COLUMN_ALIASES`.
"""

from __future__ import annotations

import gzip
import pathlib
import tarfile
from typing import Iterable, Iterator, List

import fsspec
import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.common.download import download_one
from planetary_datasets.providers.observations._points import (
    STRING_WIDTH,
    PointObservationProvider,
    clip_to_window,
    widen_string_vars,
)

STATES_BASE_URL = "https://s3.opensky-network.org/data-samples/states"

#: First and last Monday covered by the public state-vector samples.
SAMPLE_START = pd.Timestamp("2016-06-06")
SAMPLE_END = pd.Timestamp("2022-06-27")

#: Upstream column name (lower-cased) -> the name used in the store.
COLUMN_ALIASES = {
    "time": "time",
    "icao24": "icao24",
    "callsign": "callsign",
    "lat": "latitude",
    "latitude": "latitude",
    "lon": "longitude",
    "longitude": "longitude",
    "velocity": "velocity",
    "heading": "heading",
    "true_track": "heading",
    "vertrate": "vertical_rate",
    "geoaltitude": "altitude",
    "baroaltitude": "barometer_altitude",
    "onground": "on_ground",
    "alert": "alert",
    "spi": "spi",
    "squawk": "squawk",
    "lastposupdate": "last_position_update",
    "lastcontact": "last_contact",
    "hour": "hour",
}

#: Continuous variables, stored as float32.
FLOAT_VARIABLES = (
    "latitude",
    "longitude",
    "altitude",
    "barometer_altitude",
    "velocity",
    "heading",
    "vertical_rate",
)

#: Flags, stored as bool.
BOOL_VARIABLES = ("on_ground", "alert", "spi")

#: Identifiers, stored as fixed-width unicode.
STRING_VARIABLES = ("icao24", "callsign")


def states_archive_url(it) -> str:
    """URL of the OpenSky state-vector archive for the hour starting at ``it``.

    The date component carries a leading dot (``.2020-05-11``); that is how the bucket
    is laid out upstream, not a typo.
    """
    ts = pd.Timestamp(it)
    return (
        f"{STATES_BASE_URL}/.{ts.strftime('%Y-%m-%d')}/{ts.hour:02d}/"
        f"states_{ts.strftime('%Y-%m-%d-%H')}.csv.tar"
    )


def _read_member(tar: tarfile.TarFile, member: tarfile.TarInfo) -> pd.DataFrame:
    handle = tar.extractfile(member)
    if handle is None:
        raise ValueError(f"{member.name} is not a readable file")
    # Some archives hold a gzipped CSV, others a plain one. Sniff rather than trust the
    # extension: several members in the sample set are named .csv but gzip-compressed.
    magic = handle.read(2)
    handle.seek(0)
    if magic == b"\x1f\x8b":
        with gzip.open(handle, "rt") as unzipped:
            return pd.read_csv(unzipped, header=0)
    return pd.read_csv(handle, header=0)


def _is_csv_member(member: tarfile.TarInfo) -> bool:
    name = member.name.lower()
    return member.isfile() and (name.endswith(".csv") or name.endswith(".csv.gz"))


def read_states_archive(path: str | pathlib.Path) -> pd.DataFrame:
    """Read an OpenSky ``states_*.csv.tar`` archive into a dataframe.

    The archives ship ``LICENSE.txt`` and ``README.txt`` alongside the data, so members
    are selected by extension rather than taken wholesale.
    """
    with tarfile.open(path, "r") as tar:
        members = [m for m in tar.getmembers() if _is_csv_member(m)]
        if not members:
            raise ValueError(f"{path} contains no CSV members")
        frames = [_read_member(tar, member) for member in members]
    return frames[0] if len(frames) == 1 else pd.concat(frames, ignore_index=True)


def read_states(path: str | pathlib.Path) -> pd.DataFrame:
    """Read state vectors from a ``.csv.tar`` archive, a ``.parquet`` file or a CSV."""
    path = pathlib.Path(path)
    if path.name.endswith(".csv.tar") or path.suffix == ".tar":
        return read_states_archive(path)
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def normalise_states(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns to the canonical names, parse times and drop unusable rows.

    Rows with no latitude or longitude are dropped: a state vector without a position is
    of no use to any downstream consumer here, and carrying it forward would put NaNs in
    the coordinate columns.
    """
    renamed = {}
    for column in df.columns:
        target = COLUMN_ALIASES.get(str(column).lower())
        if target is not None:
            renamed[column] = target
    df = df.rename(columns=renamed)
    # A camelCase and a snake_case spelling of the same field can both be present; keep
    # the first so the resulting frame has unique column labels.
    df = df.loc[:, ~df.columns.duplicated()]

    missing = {"time", "icao24", "latitude", "longitude"} - set(df.columns)
    if missing:
        raise ValueError(f"state vectors are missing required columns: {sorted(missing)}")

    times = df["time"]
    if pd.api.types.is_numeric_dtype(times):
        # OpenSky reports epoch seconds.
        df = df.assign(time=pd.to_datetime(times, unit="s"))
    else:
        df = df.assign(time=pd.to_datetime(times))

    df = df.dropna(subset=["latitude", "longitude", "time"])
    df = df.sort_values("time", kind="stable").reset_index(drop=True)
    return df


def states_to_dataset(df: pd.DataFrame) -> xr.Dataset:
    """Turn normalised state vectors into a flat point dataset indexed by ``time``."""
    df = normalise_states(df)
    data_vars: dict[str, tuple[str, np.ndarray]] = {}

    for name in FLOAT_VARIABLES:
        if name in df.columns:
            data_vars[name] = ("time", pd.to_numeric(df[name], errors="coerce").to_numpy("float32"))
    for name in BOOL_VARIABLES:
        if name in df.columns:
            # Missing flags mean "not asserted"; a NaN would force the column to float.
            data_vars[name] = ("time", df[name].fillna(False).astype(bool).to_numpy())
    for name in STRING_VARIABLES:
        if name in df.columns:
            values = df[name].fillna("").astype(str).str.strip()
            data_vars[name] = ("time", values.to_numpy(f"<U{STRING_WIDTH}"))

    return xr.Dataset(
        data_vars,
        coords={"time": pd.DatetimeIndex(df["time"])},
        attrs={
            "source": "OpenSky Network state vectors",
            "source_url": STATES_BASE_URL,
            "license": "CC BY-SA 4.0 (OpenSky Network)",
        },
    )


def trajectory_dataset(icao24: str, group: pd.DataFrame) -> xr.Dataset:
    """Build the single-aircraft trajectory dataset for one ``icao24`` group."""
    group = group.sort_values("time", kind="stable")
    ds = states_to_dataset(group)
    # icao24 is constant within the group, so it belongs on the dataset rather than
    # repeated once per sample.
    ds = ds.drop_vars([v for v in ("icao24",) if v in ds.data_vars])
    ds = ds.assign_coords(icao24=str(icao24))
    ds.attrs["icao24"] = str(icao24)
    return ds


def iter_trajectories(df: pd.DataFrame, min_points: int = 1) -> Iterator[tuple[str, xr.Dataset]]:
    """Yield ``(icao24, dataset)`` for each aircraft in ``df``.

    This is the per-flight extraction that ``one_offs/open_sky_to_icechunk.py`` described
    but left commented out.

    Args:
        df: Raw or normalised state vectors.
        min_points: Skip aircraft with fewer than this many positions. A handful of
            samples is not a trajectory and inflates the file count enormously.
    """
    df = normalise_states(df)
    for icao24, group in df.groupby("icao24", sort=True):
        if len(group) < min_points:
            continue
        yield str(icao24), trajectory_dataset(str(icao24), group)


def write_trajectories(
    df: pd.DataFrame,
    out_dir: str | pathlib.Path,
    label: str,
    min_points: int = 1,
    overwrite: bool = False,
) -> List[pathlib.Path]:
    """Write one netCDF per aircraft into ``out_dir``. Returns the paths written.

    Args:
        df: State vectors for the period being extracted.
        out_dir: Destination directory, created if needed.
        label: Period label used in the filename, e.g. ``2017-06-05``.
        min_points: Minimum positions for an aircraft to be written.
        overwrite: Rewrite files that already exist.
    """
    out_dir = pathlib.Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    written: List[pathlib.Path] = []
    for icao24, ds in iter_trajectories(df, min_points=min_points):
        path = out_dir / f"opensky_trajectory_{label}_{icao24}.nc"
        if path.exists() and not overwrite:
            continue
        # Write to a sidecar first so an interrupted run leaves no truncated file that
        # the skip-if-exists check above would take for a finished one.
        part = path.with_suffix(".nc.part")
        ds.to_netcdf(part, mode="w")
        part.replace(path)
        written.append(path)
    logger.info(f"wrote {len(written)} trajectory file(s) to {out_dir}")
    return written


def _as_naive_utc(series: pd.Series) -> pd.Series:
    """Parse a column of timestamps to timezone-naive UTC.

    The AIRMET/SIGMET exports carry tz-aware timestamps while the flight times taken
    from the state vectors are naive; comparing the two raises. Normalising both to
    naive UTC is the fix for the ``Invalid comparison between dtype=datetime64[ns, UTC]
    and Timestamp`` error the exploratory script hit.
    """
    parsed = pd.to_datetime(series, utc=True, errors="coerce")
    return parsed.dt.tz_localize(None)


def hazards_near_trajectory(
    hazards,
    trajectory: xr.Dataset,
    valid_from: str,
    valid_to: str,
    pad_degrees: float = 2.0,
):
    """Subset a hazard-polygon GeoDataFrame to those a flight could have encountered.

    Salvaged from ``one_offs/visualize_open_sky.py``: a hazard is kept when its bounding
    box comes within ``pad_degrees`` of the trajectory's bounding box *and* its validity
    window overlaps the flight's time span.

    Args:
        hazards: GeoDataFrame of AIRMET or SIGMET polygons.
        trajectory: Dataset with ``latitude``, ``longitude`` and ``time``.
        valid_from: Name of the hazard start-time column, e.g. ``VALID_FM`` or ``ISSUE``.
        valid_to: Name of the hazard end-time column, e.g. ``VALID_TO`` or ``EXPIRE``.
        pad_degrees: Spatial slack around the flight's bounding box, in degrees.

    Returns:
        The filtered GeoDataFrame, empty when nothing applies.
    """
    if trajectory.sizes.get("time", 0) == 0:
        return hazards.iloc[[]]

    lons = np.asarray(trajectory["longitude"].values, dtype="float64")
    lats = np.asarray(trajectory["latitude"].values, dtype="float64")
    nearby = hazards.cx[
        float(np.nanmin(lons)) - pad_degrees : float(np.nanmax(lons)) + pad_degrees,
        float(np.nanmin(lats)) - pad_degrees : float(np.nanmax(lats)) + pad_degrees,
    ]
    if len(nearby) == 0:
        return nearby

    times = pd.DatetimeIndex(trajectory["time"].values)
    starts = _as_naive_utc(nearby[valid_from])
    ends = _as_naive_utc(nearby[valid_to])
    # Overlapping intervals: the hazard starts before the flight ends and ends after the
    # flight starts.
    return nearby[(starts < times[-1]) & (ends > times[0])]


def within_bounds(ds: xr.Dataset, lat_min, lat_max, lon_min, lon_max) -> xr.Dataset:
    """Keep only the samples inside a lat/lon box, e.g. a model domain."""
    mask = (
        (ds["latitude"] >= lat_min)
        & (ds["latitude"] <= lat_max)
        & (ds["longitude"] >= lon_min)
        & (ds["longitude"] <= lon_max)
    )
    return ds.isel(time=np.flatnonzero(np.asarray(mask.values)))


def sample_hours(start=SAMPLE_START, end=SAMPLE_END) -> pd.DatetimeIndex:
    """Every hour for which a public OpenSky sample archive exists in a date range.

    The samples are published for Mondays only, 24 hours each.
    """
    mondays = pd.date_range(start=start, end=end, freq="W-MON")
    if len(mondays) == 0:
        return pd.DatetimeIndex([])
    return pd.DatetimeIndex(
        np.concatenate([pd.date_range(day, periods=24, freq="h").to_numpy() for day in mondays])
    )


def _url_exists(url: str) -> bool:
    """True when the archive is published. Errors other than 404 propagate."""
    return fsspec.filesystem("https").exists(url)


class OpenSkyStatesProvider(PointObservationProvider):
    """Hourly OpenSky state vectors as a flat point store."""

    name = "opensky_states"
    append_dim = "time"
    store_prefix = "bkr/opensky/opensky_states.icechunk"
    partition_freq = "h"

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Download the hour's archive. Returns ``[]`` only when it is not published."""
        url = states_archive_url(it)
        dest_dir = pathlib.Path(temp_dir) if temp_dir is not None else self.local_dir("opensky")
        path = download_one(url, pathlib.Path(dest_dir) / url.rsplit("/", 1)[-1])
        if path is not None:
            return [str(path)]
        # download_one returns None both for "not there" and for "the network gave up".
        # Only the first may be reported as an empty partition: the base class treats an
        # empty fetch as permanently done and would never retry a transient failure.
        if not _url_exists(url):
            logger.info(f"no OpenSky states archive published for {pd.Timestamp(it)}")
            return []
        raise RuntimeError(f"failed to download {url}")

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Read the archive(s) and shape them into the point store's layout."""
        frames = [read_states(path) for path in input_files]
        df = frames[0] if len(frames) == 1 else pd.concat(frames, ignore_index=True)
        ds = states_to_dataset(df)
        start, end = self.partition_bounds(it)
        clipped = clip_to_window(ds, start, end)
        if clipped.sizes["time"] == 0 and ds.sizes["time"] > 0:
            raise ValueError(
                f"{input_files} holds {ds.sizes['time']} state vectors but none fall in "
                f"{start} .. {end}; the archive does not match its filename"
            )
        rows = clipped.sizes["time"]
        return widen_string_vars(clipped).chunk({"time": min(100_000, max(1, rows))})

    def extract_trajectories(
        self,
        input_files: Iterable[str],
        label: str,
        out_dir: str | pathlib.Path | None = None,
        min_points: int = 1,
    ) -> List[pathlib.Path]:
        """Write per-aircraft trajectory netCDFs for the given archives.

        Defaults to ``<data_dir>/opensky_trajectories``.
        """
        frames = [read_states(path) for path in input_files]
        if not frames:
            return []
        df = frames[0] if len(frames) == 1 else pd.concat(frames, ignore_index=True)
        default = self.local_dir("opensky_trajectories")
        target = pathlib.Path(out_dir) if out_dir is not None else default
        return write_trajectories(df, target, label=label, min_points=min_points)
