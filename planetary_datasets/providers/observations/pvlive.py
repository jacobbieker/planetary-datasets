"""PV-Live, the Baden-Württemberg solar irradiance measurement network.

Forty stations across the German state measure global horizontal irradiance with a
pyranometer, module temperature, and global tilted irradiance east, south and west at 25
degrees with reference cells, all at one-minute resolution. The archive is published on
Zenodo as one ZIP per month holding a tab-separated file per station, plus a station
metadata table.

Replaces ``dags/assets/observation/pvlive.py``, a vendored copy of the ``get_pvlive``
function pvlib never merged, plus a loop that wrote one CSV per month into the working
directory. Behaviour changes: a month is a partition, the ZIP is streamed to disk rather
than held in memory twice, station coordinates are taken from the archive's own metadata
table instead of being dropped, and the result is written to Icechunk.

Reference: Lorenz et al. 2022, Solar Energy, doi:10.1016/j.solener.2021.11.023.
Data: https://doi.org/10.5281/zenodo.4036728

Environment: none. Zenodo is anonymous.
"""

from __future__ import annotations

import io
import json
import pathlib
import re
import zipfile

import pandas as pd
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common.store import write_to_icechunk as _write_to_icechunk
from planetary_datasets.providers.observations.base import (
    OBSERVATION_ALIGNMENT_COORDS,
    NoStationDataError,
    Station,
    download_or_none,
    frames_to_dataset,
    partition_time_index,
)

BASE_URL = "https://zenodo.org/record/15013388/files/"
MONTH_URL = BASE_URL + "pvlive_{year}-{month:02d}.zip?download=1"

#: The network has 40 numbered stations, identified in the archive as ``tng00001`` to
#: ``tng00040``. Fixing the axis here rather than taking whichever stations a given
#: month's archive happens to contain keeps every partition appendable.
STATION_COUNT = 40

#: ``tng00013_2020-09.tsv`` and friends.
STATION_FILE = re.compile(r"^(tng(\d+))_.*\.tsv$")
METADATA_FILE = re.compile(r"^metadata_stations_.*\.tsv$")

#: Measured quantities. Each has a companion ``flag_*`` quality column, and there is a
#: ``flag_shading`` column; those are dropped.
VARIABLES = ("Gg_pyr", "Gg_si_south", "Gg_si_east", "Gg_si_west", "T_pyr")


def station_id(number: int) -> str:
    """Archive identifier for a PV-Live station number."""
    return f"tng{number:05d}"


CANONICAL_IDS = tuple(station_id(n) for n in range(1, STATION_COUNT + 1))


def read_month_zip(path: pathlib.Path) -> tuple[dict[str, pd.DataFrame], dict[str, Station]]:
    """Read one monthly archive.

    Returns the per-station observation frames and, when the archive carries its station
    metadata table, the station records keyed by identifier.
    """
    frames: dict[str, pd.DataFrame] = {}
    stations: dict[str, Station] = {}

    with zipfile.ZipFile(path) as archive:
        for member in archive.namelist():
            basename = pathlib.PurePosixPath(member).name
            match = STATION_FILE.match(basename)
            if match:
                frames[match.group(1)] = pd.read_csv(
                    io.StringIO(archive.read(member).decode("utf-8")),
                    sep="\t",
                    index_col=0,
                    parse_dates=[0],
                )
            elif METADATA_FILE.match(basename):
                stations = read_station_metadata(archive.read(member).decode("utf-8"))

    return frames, stations


def read_station_metadata(text: str) -> dict[str, Station]:
    """Parse the archive's ``metadata_stations_*.tsv`` into station records."""
    df = pd.read_csv(io.StringIO(text), sep="\t")
    stations: dict[str, Station] = {}
    for row in df.itertuples(index=False):
        values = dict(zip(df.columns, row))
        identifier = str(values.get("LocationID", "")).strip()
        if not identifier:
            continue
        stations[identifier] = Station(
            id=identifier,
            latitude=_as_float(values.get("Latitude [deg]")),
            longitude=_as_float(values.get("Longitude [deg]")),
            elevation=_as_float(values.get("Altitude from EU-DEM [m]")),
            name=str(values.get("Name")) if values.get("Name") is not None else None,
        )
    return stations


def _as_float(value) -> float | None:
    number = pd.to_numeric(value, errors="coerce")
    return None if pd.isna(number) else float(number)


class PVLiveProvider(BaseProvider):
    """One-minute PV-Live irradiance, one partition per calendar month."""

    name = "pvlive"
    store_prefix = "bkr/obs/pvlive.icechunk"
    append_dim = "time"
    source_url = BASE_URL

    partition_freq = "MS"
    sample_freq = "1min"

    def __init__(self, config=None, variables: tuple[str, ...] | None = VARIABLES):
        """Build the provider.

        Args:
            config: Optional configuration override.
            variables: Columns to write. Pass None to take the sorted union of whatever
                the archive contains, which also pulls in the quality flags.
        """
        super().__init__(config=config)
        self.variables = tuple(variables) if variables is not None else None

    @property
    def roster_cache(self) -> pathlib.Path:
        """Where the station metadata table is kept between partitions."""
        return self.config.data_dir / "pvlive" / "stations.json"

    def stations(self, metadata: dict[str, Station] | None = None) -> list[Station]:
        """The fixed 40-station axis, with coordinates from the archive's metadata table.

        The first archive that carries a metadata table is cached, and every later
        partition reuses it. Without that, a month whose ZIP omits the table would write
        a dataset with no ``latitude``/``longitude`` coordinates at all, which is a
        different coordinate set from its neighbours and not safely appendable.
        """
        cache = self.roster_cache
        if metadata:
            if not cache.is_file():
                cache.parent.mkdir(parents=True, exist_ok=True)
                cache.write_text(json.dumps({k: v.__dict__ for k, v in metadata.items()}))
                logger.info(f"{self.name}: cached station metadata to {cache}")
        elif cache.is_file():
            try:
                metadata = {k: Station(**v) for k, v in json.loads(cache.read_text()).items()}
            except (json.JSONDecodeError, TypeError, ValueError) as exc:
                logger.warning(f"{self.name}: ignoring unreadable metadata cache {cache} ({exc})")
                metadata = {}

        metadata = metadata or {}
        return [metadata.get(sid, Station(id=sid)) for sid in CANONICAL_IDS]

    def partition_times(self, it: pd.Timestamp) -> pd.DatetimeIndex:
        return partition_time_index(it, self.partition_freq, self.sample_freq)

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> list[str]:
        directory = pathlib.Path(temp_dir) if temp_dir is not None else self.config.scratch_dir
        month = pd.Timestamp(it)
        url = MONTH_URL.format(year=month.year, month=month.month)
        path = download_or_none(url, directory / f"pvlive_{month:%Y-%m}.zip", timeout=900.0)
        if path is None:
            logger.info(f"{self.name}: no archive published for {month:%Y-%m}")
            return []
        return [str(path)]

    def process(
        self,
        input_files: list[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ):
        frames, metadata = read_month_zip(pathlib.Path(input_files[0]))
        if not frames:
            raise NoStationDataError(f"{self.name}: {input_files[0]} held no station files")

        unexpected = sorted(set(frames) - set(CANONICAL_IDS))
        if unexpected:
            # A station outside tng00001-tng00040 means the network changed; writing it
            # would widen the station axis and break every later append.
            logger.warning(f"{self.name}: ignoring station(s) outside the known axis: {unexpected}")
            frames = {sid: df for sid, df in frames.items() if sid in set(CANONICAL_IDS)}

        return frames_to_dataset(
            frames,
            self.stations(metadata),
            self.partition_times(it),
            variables=self.variables,
            sample_freq=self.sample_freq,
            how="exact",
            attrs={"network": self.name, "source": BASE_URL},
        )

    def write_to_icechunk(self, repo, processed):
        return _write_to_icechunk(
            repo,
            processed,
            append_dim=self.append_dim,
            message=f"{self.name}: {processed[self.append_dim].values[0]}",
            alignment_coords=OBSERVATION_ALIGNMENT_COORDS,
        )
