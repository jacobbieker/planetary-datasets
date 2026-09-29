"""AMDAR aircraft observations, decoded from PREPBUFR with the MET ``pb2nc`` tool.

Aircraft report PREPBUFR files are not self-describing to xarray, so they are first
converted to MET's point-observation NetCDF with ``pb2nc`` and then flattened into a
table of individually-timed observations.

Consolidates ``amdar_process.py`` (rename-by-long-name, the ``pb2nc`` shell-out, the
scan over converted files) and ``pb2nc_runner.py`` (the same shell-out under a process
pool). Three behaviours of the originals were deliberately changed:

* ``os.system`` is replaced by :mod:`subprocess`, so a non-zero exit is an error with the
  tool's own stderr attached rather than a silent success.
* The absence of ``pb2nc`` raises :class:`MissingToolError` naming the MET toolkit,
  instead of the opaque shell "command not found" the original produced.
* The record scan indexed the global observation arrays with a loop counter that ran over
  a *subset*, so every header after the first read another header's observations. It is
  now a vectorised gather through the header-index array.

The ``amdar_download`` asset fetches GDAS PREPBUFR from NCAR GDEX and runs ``pb2nc`` in
the MET Docker image (:data:`STAGE_SCRIPT`), staging NetCDF under ``<bufr_dir>/<YYYYMMDD>``.
Raw PREPBUFR files already in ``bufr_dir`` are still converted with a local ``pb2nc``.

Environment:
    ``AMDAR_BUFR_DIR``      Directory holding the raw PREPBUFR files. Defaults to
                            ``<PLANETARY_DATASETS_DATA_DIR>/amdar``.
    ``PB2NC_BINARY``        Path to MET's ``pb2nc``. Defaults to ``pb2nc`` on ``PATH``.
    ``AMDAR_PB2NC_CONFIG``  A MET pb2nc configuration file. When unset, the aircraft-only
                            configuration in this module is written to the scratch
                            directory and used.
"""

from __future__ import annotations

import os
import pathlib
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor
from typing import List, Sequence

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.common.staged import StagedFilesMixin
from planetary_datasets.config import Config
from planetary_datasets.providers.observations.upper_air_common import (
    PointObservationProvider,
    concat_tables,
    table_to_dataset,
)

#: MET pb2nc configuration restricted to the aircraft message types, derived from the
#: ``pb2nc_config.txt`` the original scripts pointed at. The mask section is dropped: it
#: referenced a polygon file that was never part of the repository, so any run on another
#: machine failed. ``tmp_dir`` is filled in from the configured scratch directory.
PB2NC_CONFIG_TEMPLATE = """\
////////////////////////////////////////////////////////////////////////////////
//
// PB2NC configuration file for AMDAR aircraft reports.
//
////////////////////////////////////////////////////////////////////////////////

message_type = [ "AIRCAR", "AIRCFT" ];

message_type_group_map = [
   {{ key = "SURFACE"; val = "ADPSFC,SFCSHP,MSONET";               }},
   {{ key = "ANYAIR";  val = "AIRCAR,AIRCFT";                      }},
   {{ key = "ANYSFC";  val = "ADPSFC,SFCSHP,ADPUPA,PROFLR,MSONET"; }},
   {{ key = "ONLYSF";  val = "ADPSFC,SFCSHP";                      }}
];

message_type_map = [];

station_id = [];

obs_window = {{
   beg = {obs_window_beg};
   end = {obs_window_end};
}}

mask = {{
   grid = "";
   poly = "";
}}

elevation_range = {{
   beg =  -1000;
   end = 100000;
}}

pb_report_type  = [];
in_report_type  = [];
instrument_type = [];

level_range = {{
   beg = 1;
   end = 511;
}}

level_category = [];

obs_bufr_var = [];
obs_bufr_map = [];

obs_prepbufr_map = [
   {{ key = "POB";      val = "PRES";   }},
   {{ key = "QOB";      val = "SPFH";   }},
   {{ key = "TOB";      val = "TMP";    }},
   {{ key = "ZOB";      val = "HGT";    }},
   {{ key = "UOB";      val = "UGRD";   }},
   {{ key = "VOB";      val = "VGRD";   }},
   {{ key = "HOVI";     val = "VIS";    }},
   {{ key = "TOCC";     val = "TCDC";   }},
   {{ key = "D_DPT";    val = "DPT";    }},
   {{ key = "D_WDIR";   val = "WDIR";   }},
   {{ key = "D_WIND";   val = "WIND";   }},
   {{ key = "D_RH";     val = "RH";     }},
   {{ key = "D_MIXR";   val = "MIXR";   }},
   {{ key = "D_PRMSL";  val = "PRMSL";  }},
   {{ key = "D_PBL";    val = "PBL";    }},
   {{ key = "D_CAPE";   val = "CAPE";   }},
   {{ key = "D_MLCAPE"; val = "MLCAPE"; }}
];

quality_mark_thresh = 5;
event_stack_flag    = TOP;

time_summary = {{
  flag = FALSE;
  raw_data = TRUE;
  beg = "000000";
  end = "235959";
  step = 300;
  width = 600;
  grib_code = [];
  obs_var   = [ "TMP", "WDIR", "RH" ];
  type = [ "min", "max", "range", "mean", "stdev", "median", "p80", "sum" ];
  vld_freq = 0;
  vld_thresh = 0.0;
}}

tmp_dir = "{tmp_dir}";
version = "{version}";

////////////////////////////////////////////////////////////////////////////////
"""

#: MET config files carry the version of the tool that reads them.
PB2NC_CONFIG_VERSION = "V12.2.0"

#: NCAR GDEX ds337.0 GDAS PREPBUFR, published about 48 hours after the cycle.
GDEX_PREPBUFR_URL = (
    "https://osdf-director.osg-htc.org/ncar/gdex/d337000/prep48h/"
    "{day:%Y}/prepbufr.gdas.{day:%Y%m%d}.t{hour:02d}z.nr.48h"
)
GDAS_CYCLES = (0, 6, 12, 18)

#: Run in the MET image (``dtcenter/met``) as ``bash -c STAGE_SCRIPT amdar <out_dir> <url>...``
#: with the pb2nc configuration in ``$PB2NC_CONFIG``, whose ``@WORK_DIR@`` becomes a private
#: scratch directory. Only finished NetCDF is left behind.
STAGE_SCRIPT = """\
set -euo pipefail
out=$1; shift
mkdir -p "$out"
work=$(mktemp -d)
printf '%s' "$PB2NC_CONFIG" | sed "s|@WORK_DIR@|$work|" > "$work/pb2nc.cfg"
for url in "$@"; do
  raw="$work/$(basename "$url")"
  nc="$out/$(basename "$url").nc"
  [ -s "$nc" ] && continue
  wget -q --no-hsts --tries=5 -O "$raw" "$url"
  pb2nc "$raw" "$work/out.nc" "$work/pb2nc.cfg"
  mv "$work/out.nc" "$nc"
  rm -f "$raw"
done
"""


def prepbufr_urls(day: pd.Timestamp) -> List[str]:
    """The GDAS PREPBUFR files covering one UTC day.

    Each cycle holds a +-3 h window, so the next day's 00z carries this day's last three hours.
    """
    day = pd.Timestamp(day).normalize()
    cycles = [(day, h) for h in GDAS_CYCLES] + [(day + pd.Timedelta(days=1), 0)]
    return [GDEX_PREPBUFR_URL.format(day=d, hour=h) for d, h in cycles]

#: Observation types whose "unit" makes them a code rather than a measurement. The
#: original dropped these and so do we: mixing a code table into a float column of
#: physical values produces nonsense on averaging.
NON_PHYSICAL_UNIT_SUFFIXES = ("__numeric", "__hours", "__code_table")

#: Every AMDAR partition writes exactly these variables, so appends are never refused
#: because one day happened not to report a field.
AMDAR_SCHEMA = {
    "observation": "float32",
    "observation_type": "str",
    "pressure": "float32",
    "height": "float32",
    "quality_flag": "float32",
    "latitude": "float32",
    "longitude": "float32",
    "elevation": "float32",
    "station_id": "str",
    "message_type": "str",
}

# Names the MET NetCDF long_names collapse to, after rename_by_long_name.
_HEADER_INDEX = "index_of_matching_header_data"
_VARIABLE_INDEX = "index_of_bufr_variable_corresponding_to_the_observation_type"
_VALID_TIME_INDEX = "index_of_valid_time"
_STATION_INDEX = "index_of_station_identification"
_MESSAGE_TYPE_INDEX = "index_of_message_type"
_OBS_VALUE = "observation_value"
_OBS_LEVEL = "pressure_level_hpa_or_accumulation_interval_sec"
_OBS_HEIGHT = "height_in_meters_above_sea_level_msl"
_QUALITY = "quality_flag"


class MissingToolError(RuntimeError):
    """An external command-line tool this provider shells out to is not installed."""


def slugify(text: str, unit: bool = False) -> str:
    """Normalise a BUFR description or unit into an identifier-safe token."""
    token = str(text).strip().lower()
    for old, new in ((" ", "_"), ("(", ""), (")", ""), ("-", "_")):
        token = token.replace(old, new)
    if unit:
        token = token.replace("/", "_per_")
    return token


def rename_by_long_name(ds: xr.Dataset) -> xr.Dataset:
    """Rename variables to a slug of their ``long_name``.

    MET names its point-observation variables ``hdr_lat``, ``obs_val`` and so on; the
    ``long_name`` attributes are what make the file readable. Collisions keep the first
    winner and leave the later variable under its original name.
    """
    renames: dict[str, str] = {}
    taken = set(ds.variables)
    for var in ds.data_vars:
        target = slugify(ds[var].attrs.get("long_name", var))
        if not target or target in taken or target in renames.values():
            continue
        renames[var] = target
    return ds.rename(renames)


def find_pb2nc(binary: str | None = None) -> str:
    """Locate MET's ``pb2nc``, raising a message that names what is missing.

    Args:
        binary: Explicit executable name or path. Defaults to ``PB2NC_BINARY`` in the
            environment, then to ``pb2nc`` on ``PATH``.
    """
    name = binary or os.environ.get("PB2NC_BINARY") or "pb2nc"
    resolved = shutil.which(name)
    if resolved is None:
        candidate = pathlib.Path(name).expanduser()
        if candidate.is_file() and os.access(candidate, os.X_OK):
            resolved = str(candidate)
    if resolved is None:
        raise MissingToolError(
            f"the MET tool 'pb2nc' is required to decode AMDAR PREPBUFR files but {name!r} "
            "was not found. Install the MET toolkit (https://dtcenter.org/community-code/"
            "model-evaluation-tools-met) and put pb2nc on PATH, or set PB2NC_BINARY to its "
            "full path."
        )
    return resolved


def pb2nc_available(binary: str | None = None) -> bool:
    """True when :func:`find_pb2nc` can locate the tool. For skipping, not for control flow."""
    try:
        find_pb2nc(binary)
    except MissingToolError:
        return False
    return True


def write_pb2nc_config(
    dest: str | os.PathLike,
    tmp_dir: str | os.PathLike,
    obs_window: tuple[int, int] = (-21600, 21600),
    version: str = PB2NC_CONFIG_VERSION,
) -> pathlib.Path:
    """Write the built-in aircraft pb2nc configuration and return its path."""
    dest = pathlib.Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(render_pb2nc_config(tmp_dir, obs_window, version))
    return dest


def render_pb2nc_config(
    tmp_dir: str | os.PathLike,
    obs_window: tuple[int, int] = (-21600, 21600),
    version: str = PB2NC_CONFIG_VERSION,
) -> str:
    """The built-in aircraft pb2nc configuration as text."""
    return PB2NC_CONFIG_TEMPLATE.format(
        obs_window_beg=int(obs_window[0]),
        obs_window_end=int(obs_window[1]),
        tmp_dir=str(tmp_dir),
        version=version,
    )


def run_pb2nc(
    input_path: str | os.PathLike,
    output_path: str | os.PathLike,
    config_path: str | os.PathLike,
    binary: str | None = None,
    overwrite: bool = False,
    timeout: float | None = None,
) -> pathlib.Path:
    """Convert one PREPBUFR file to MET point-observation NetCDF.

    Returns the output path. An existing output is reused unless ``overwrite``.

    Raises:
        MissingToolError: ``pb2nc`` is not installed.
        RuntimeError: ``pb2nc`` ran but failed, or produced no output.
    """
    input_path = pathlib.Path(input_path)
    output_path = pathlib.Path(output_path)
    if output_path.is_file() and output_path.stat().st_size > 0 and not overwrite:
        logger.debug(f"reusing existing {output_path.name}")
        return output_path

    executable = find_pb2nc(binary)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    # Write to a sidecar and rename, so an interrupted run cannot leave a truncated file
    # that the reuse check above would accept.
    partial = output_path.with_name(output_path.name + ".part")
    partial.unlink(missing_ok=True)

    command = [executable, str(input_path), str(partial), str(config_path)]
    logger.debug("running " + " ".join(command))
    result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        partial.unlink(missing_ok=True)
        raise RuntimeError(
            f"pb2nc failed on {input_path.name} (exit {result.returncode}): "
            f"{result.stderr.strip() or result.stdout.strip()}"
        )
    if not partial.is_file() or partial.stat().st_size == 0:
        partial.unlink(missing_ok=True)
        raise RuntimeError(f"pb2nc reported success but wrote no output for {input_path.name}")

    os.replace(partial, output_path)
    return output_path


def convert_many(
    input_paths: Sequence[str | os.PathLike],
    output_dir: str | os.PathLike,
    config_path: str | os.PathLike,
    workers: int | None = None,
    binary: str | None = None,
    suffix: str = ".nc",
) -> List[pathlib.Path]:
    """Convert many PREPBUFR files concurrently, returning the outputs that succeeded.

    Threads rather than processes: every worker spends its life waiting on a subprocess,
    and a thread pool cannot poison its parent the way a crashed pool worker can.
    """
    input_paths = [pathlib.Path(p) for p in input_paths]
    output_dir = pathlib.Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    # Fail before starting the pool if the tool is missing, rather than once per file.
    find_pb2nc(binary)

    def _one(path: pathlib.Path) -> pathlib.Path | None:
        try:
            return run_pb2nc(path, output_dir / (path.stem + suffix), config_path, binary=binary)
        except RuntimeError as exc:
            logger.warning(f"skipping {path.name}: {exc}")
            return None

    max_workers = workers or min(8, (os.cpu_count() or 1))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        results = list(pool.map(_one, input_paths))
    return [p for p in results if p is not None]


def _decode(value) -> str:
    """Turn a NetCDF character scalar or fixed-width byte string into a Python string."""
    if isinstance(value, bytes):
        return value.decode("utf-8", "ignore").strip("\x00").strip()
    return str(value).strip("\x00").strip()


def _string_table(da: xr.DataArray) -> np.ndarray:
    """Read a MET lookup table of strings, which may be bytes or a char matrix."""
    values = da.values
    if values.ndim >= 2:
        joined = []
        for row in values.reshape(values.shape[0], -1):
            joined.append("".join(_decode(c) for c in row))
        return np.array([s.strip() for s in joined], dtype=object)
    return np.array([_decode(v) for v in values], dtype=object)


def _parse_valid_times(raw: np.ndarray) -> pd.DatetimeIndex:
    """Parse MET's ``YYYYMMDD_HHMMSS`` valid-time table, tolerating a date-only form."""
    parsed = []
    for value in raw:
        text = str(value).strip()
        if "_" in text:
            parsed.append(pd.to_datetime(text, format="%Y%m%d_%H%M%S", errors="coerce"))
        else:
            parsed.append(pd.to_datetime(text, format="%Y%m%d", errors="coerce"))
    return pd.DatetimeIndex(parsed)


def _quality_column(ds: xr.Dataset, n_obs: int) -> np.ndarray | None:
    """Read the per-observation quality mark, whichever layout the file uses.

    MET 12 writes ``obs_qty`` as an *index* into a lookup table of quality-mark strings,
    so the variable that ``rename_by_long_name`` calls ``quality_flag`` is the table and
    is as long as the number of distinct marks, not the number of observations. Taking it
    directly gives a column of the wrong length. Older files store the mark per
    observation, which is handled by the length check.
    """
    index_name = "index_of_quality_flag"
    if index_name in ds.variables and _QUALITY in ds.variables:
        table = _string_table(ds[_QUALITY])
        index = np.asarray(ds[index_name].values, dtype=int)
        return pd.to_numeric(pd.Series(table[index]), errors="coerce").to_numpy()
    if _QUALITY in ds.variables:
        values = np.asarray(ds[_QUALITY].values).ravel()
        if values.size != n_obs:
            logger.warning(
                f"{_QUALITY} has {values.size} values for {n_obs} observations, dropping it"
            )
            return None
        return pd.to_numeric(pd.Series(values), errors="coerce").to_numpy()
    return None


def pb2nc_to_table(ds: xr.Dataset) -> pd.DataFrame:
    """Flatten one MET point-observation dataset into a table of observations.

    Each row is a single observation joined to its header: time, position, station and
    the name of the physical quantity. Rows whose quantity is a code table rather than a
    measurement are dropped, matching the original.
    """
    ds = rename_by_long_name(ds)

    required = (
        _HEADER_INDEX,
        _VARIABLE_INDEX,
        _VALID_TIME_INDEX,
        _STATION_INDEX,
        _OBS_VALUE,
        "valid_time",
        "station_identification",
        "variable_descriptions",
        "variable_units",
        "latitude",
        "longitude",
        "elevation",
    )
    missing = [name for name in required if name not in ds.variables]
    if missing:
        raise ValueError(
            f"not a MET point-observation file: missing {missing}. "
            f"Present: {sorted(ds.variables)[:15]}"
        )

    header_index = np.asarray(ds[_HEADER_INDEX].values, dtype=int)
    variable_index = np.asarray(ds[_VARIABLE_INDEX].values, dtype=int)
    if header_index.size == 0:
        return pd.DataFrame()

    valid_times = _parse_valid_times(_string_table(ds["valid_time"]))
    time_index = np.asarray(ds[_VALID_TIME_INDEX].values, dtype=int)
    station_table = _string_table(ds["station_identification"])
    station_index = np.asarray(ds[_STATION_INDEX].values, dtype=int)

    descriptions = _string_table(ds["variable_descriptions"])
    units = _string_table(ds["variable_units"])
    observation_types = np.array(
        [
            f"{slugify(descriptions[i])}__{slugify(units[i], unit=True)}"
            for i in range(len(descriptions))
        ],
        dtype=object,
    )

    # Gather header fields onto the observation axis in one step. The original looped per
    # header and then indexed the global arrays with the inner loop counter, which read
    # the wrong observations for every header but the first.
    frame = pd.DataFrame(
        {
            "time": valid_times[time_index][header_index],
            "observation": np.asarray(ds[_OBS_VALUE].values, dtype="float64"),
            "observation_type": observation_types[variable_index],
            "latitude": np.asarray(ds["latitude"].values, dtype="float64")[header_index],
            "longitude": np.asarray(ds["longitude"].values, dtype="float64")[header_index],
            "elevation": np.asarray(ds["elevation"].values, dtype="float64")[header_index],
            "station_id": station_table[station_index][header_index],
        }
    )
    if _OBS_LEVEL in ds.variables:
        frame["pressure"] = np.asarray(ds[_OBS_LEVEL].values, dtype="float64")
    if _OBS_HEIGHT in ds.variables:
        frame["height"] = np.asarray(ds[_OBS_HEIGHT].values, dtype="float64")
    quality = _quality_column(ds, header_index.size)
    if quality is not None:
        frame["quality_flag"] = quality
    if _MESSAGE_TYPE_INDEX in ds.variables and "message_type" in ds.variables:
        message_types = _string_table(ds["message_type"])
        frame["message_type"] = message_types[
            np.asarray(ds[_MESSAGE_TYPE_INDEX].values, dtype=int)
        ][header_index]

    physical = ~frame["observation_type"].str.endswith(NON_PHYSICAL_UNIT_SUFFIXES)
    return frame.loc[physical].reset_index(drop=True)


def read_pb2nc_files(paths: Sequence[str | os.PathLike]) -> pd.DataFrame:
    """Flatten several converted files into one table, skipping any that cannot be read."""
    tables = []
    for path in paths:
        try:
            with xr.open_dataset(path) as ds:
                tables.append(pb2nc_to_table(ds))
        except Exception as exc:  # noqa: BLE001 - a corrupt file must not stop the rest
            logger.warning(f"could not read {path}: {exc}")
    return concat_tables(tables)


class AMDARProvider(StagedFilesMixin, PointObservationProvider):
    """Daily AMDAR aircraft observations decoded from local PREPBUFR files.

    Args:
        config: Configuration override, mainly for tests.
        bufr_dir: Directory of raw PREPBUFR files. Defaults to ``AMDAR_BUFR_DIR``.
        pb2nc_config: MET configuration file. Defaults to ``AMDAR_PB2NC_CONFIG``, then to
            the built-in aircraft configuration.
        bbox: Optional ``(min_lon, min_lat, max_lon, max_lat)`` crop. The original scripts
            carried a hardcoded box around Andoya; it is a parameter here.
        workers: Concurrent ``pb2nc`` conversions.
    """

    name = "amdar"
    append_dim = "time"
    store_prefix = "bkr/observation/amdar.icechunk"
    partition_freq = "1D"

    #: Glob applied to :attr:`bufr_dir`, formatted with the partition timestamp.
    file_pattern = "*{date:%Y%m%d}*"

    def __init__(
        self,
        config: Config | None = None,
        bufr_dir: str | os.PathLike | None = None,
        pb2nc_config: str | os.PathLike | None = None,
        bbox: tuple[float, float, float, float] | None = None,
        workers: int | None = None,
    ):
        super().__init__(config=config)
        self._bufr_dir = pathlib.Path(bufr_dir).expanduser() if bufr_dir else None
        self._pb2nc_config = pathlib.Path(pb2nc_config).expanduser() if pb2nc_config else None
        self.bbox = bbox
        self.workers = workers

    @property
    def bufr_dir(self) -> pathlib.Path:
        """Directory the raw PREPBUFR files are read from."""
        if self._bufr_dir is not None:
            return self._bufr_dir
        configured = os.environ.get("AMDAR_BUFR_DIR")
        if configured:
            return pathlib.Path(configured).expanduser()
        return self.config.data_dir / "amdar"

    @property
    def archive_root(self) -> pathlib.Path:
        """Host directory mounted into the MET image."""
        return self.bufr_dir

    def staged_dir(self, it: pd.Timestamp) -> pathlib.Path:
        """Where the MET image leaves a day's converted NetCDF."""
        return self.bufr_dir / pd.Timestamp(it).strftime("%Y%m%d")

    def pb2nc_config_text(self) -> str:
        """The pb2nc configuration to hand the MET image."""
        if self._pb2nc_config is not None or os.environ.get("AMDAR_PB2NC_CONFIG"):
            return self.pb2nc_config_path().read_text()
        return render_pb2nc_config("@WORK_DIR@")

    def pb2nc_config_path(self, temp_dir: pathlib.Path | None = None) -> pathlib.Path:
        """Return the MET configuration file to use, writing the built-in one if needed."""
        if self._pb2nc_config is not None:
            return self._pb2nc_config
        configured = os.environ.get("AMDAR_PB2NC_CONFIG")
        if configured:
            path = pathlib.Path(configured).expanduser()
            if not path.is_file():
                raise FileNotFoundError(f"AMDAR_PB2NC_CONFIG points at a missing file: {path}")
            return path
        target_dir = pathlib.Path(temp_dir) if temp_dir else self.config.scratch_dir
        return write_pb2nc_config(target_dir / "pb2nc_amdar.cfg", tmp_dir=target_dir)

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Return the day's staged NetCDF, else the raw PREPBUFR files carrying its date."""
        directory = self.bufr_dir
        if not directory.is_dir():
            # Misconfiguration, not absent data: returning [] here would mark every
            # partition permanently done.
            raise FileNotFoundError(
                f"AMDAR PREPBUFR directory {directory} does not exist. Set AMDAR_BUFR_DIR "
                "or pass bufr_dir="
            )
        staged = self.staged_files(it)
        if staged:
            return [str(p) for p in staged]
        pattern = self.file_pattern.format(date=pd.Timestamp(it))
        matches = sorted(p for p in directory.glob(pattern) if p.is_file())
        if not matches:
            logger.info(f"no AMDAR PREPBUFR files matching {pattern!r} in {directory}")
        return [str(p) for p in matches]

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Convert the PREPBUFR files and flatten them into one day of observations."""
        converted = [pathlib.Path(p) for p in input_files if p.endswith(".nc")]
        raw = [p for p in input_files if not p.endswith(".nc")]
        if raw:
            work_dir = pathlib.Path(temp_dir) if temp_dir else self.config.scratch_dir
            config_path = self.pb2nc_config_path(work_dir)
            converted += convert_many(
                raw, work_dir / "amdar_nc", config_path, workers=self.workers
            )
        if not converted:
            raise RuntimeError(f"pb2nc produced no output for any of {len(input_files)} file(s)")

        table = read_pb2nc_files(converted)
        if self.bbox is not None and not table.empty:
            min_lon, min_lat, max_lon, max_lat = self.bbox
            table = table.loc[
                table["longitude"].between(min_lon, max_lon)
                & table["latitude"].between(min_lat, max_lat)
            ]

        ds = table_to_dataset(
            table if not table.empty else pd.DataFrame({"time": []}),
            AMDAR_SCHEMA,
            attrs={"source": "AMDAR PREPBUFR via MET pb2nc", "partition": str(pd.Timestamp(it))},
        )
        return self.trim_to_window(ds, it)
