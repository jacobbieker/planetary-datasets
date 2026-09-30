"""Met Office UK marine observations: buoys, light vessels and ship-borne weather stations.

The Met Office publishes its marine surface network hourly to a public S3 bucket, one
pipe-delimited CSV per platform per hour. Fifty-eight platforms report: moored data buoys
(``k1_buoy`` through ``k7_buoy``, ``brittany_buoy``, ``e1_buoy``, ``l4_buoy``, ``pap_buoy``),
light vessels (``sandettie``, ``greenwich``, ``seven_stones``…), and automatic weather
stations aboard ferries and research ships (``queen_mary_2``, ``pride_of_hull``, the
``stena_*`` fleet, ``rrs_sir_david_attenborough``, ``james_cook``…).

Layout
------
Objects are laid out one directory per published batch::

    s3://met-office-marine-observations-data/{published}_{window_start}_{window_end}/
        brittany_buoy_11d94ae4-fa21-42e0-9a38-72fe5595213a.csv
        k1_buoy_6e5ce91d-3f72-4847-a3c7-7cc4e598f638.csv
        ...

``window_start`` is ``YYYYMMDDhh00`` and is what identifies the hour; ``published`` is when
the Met Office cut the batch and drifts, so a partition's prefix is *found* by listing
rather than constructed. The UUID in each filename changes between batches and is not a
platform identifier — the identifier is the slug in front of it, which is why
:data:`STATIONS` is keyed on that.

The bucket is a rolling window of roughly ten days, so the partitioning starts where the
retained data does and old partitions cannot be backfilled once they age out.

Mobile platforms
----------------
Most of the network is ships, so ``latitude`` and ``longitude`` are written as ``(time,
station)`` *data variables* rather than station coordinates: a ferry's position is an
observation, not metadata. The moored buoys simply report the same position every hour.
That is also why :class:`~planetary_datasets.providers.observations.base.Station` entries
here carry no coordinates.

Quality flags
-------------
Each measurement has a companion ``<name>_qc`` column holding a JSON verdict
(``{"good": []}``, ``{"suspect": ["qc range check failed: ..."]}``). The prose does not fit
a dense numeric cube, but the verdict does, so it is encoded with :data:`QC_FLAG_VALUES`
and written as a ``<name>_qc`` variable alongside its measurement. The explanatory strings
are dropped.

Wave spectra
------------
The buoys also report ``wave_spectral_data_collection``: a nested JSON object of 32
frequency bands, each with an energy density and the four directional Fourier coefficients
``a1``, ``a2``, ``b1``, ``b2``. That is expanded into ``(time, station, frequency_band)``
variables rather than dropped — it is the directional wave spectrum, and it is the reason
several of these buoys exist. Platforms that do not report it are NaN.

Environment: none. The bucket is read anonymously.

Source: https://www.metoffice.gov.uk/services/data/external-data-channels
"""

from __future__ import annotations

import json
import pathlib
import re
from typing import Mapping, Sequence

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common.store import write_to_icechunk as _write_to_icechunk
from planetary_datasets.providers.observations._points import naive_utc
from planetary_datasets.providers.observations.base import (
    OBSERVATION_ALIGNMENT_COORDS,
    STATION_DIM,
    TIME_DIM,
    NoStationDataError,
    Station,
    frames_to_dataset,
    partition_time_index,
)

BUCKET = "met-office-marine-observations-data"
SOURCE_URL = "https://www.metoffice.gov.uk/services/data/external-data-channels"

#: Batch directories are ``{published}_{window_start}_{window_end}``; the middle field is
#: the hour the batch covers, which is the only one a partition can be matched on.
BATCH_DIR = re.compile(r"^(\d{12})_(\d{12})_(\d{12})$")

#: ``brittany_buoy_11d94ae4-....csv`` -> ``brittany_buoy``. The UUID is per file, not per
#: platform, so it has to come off before the slug can be used as a station id.
FILE_UUID = re.compile(r"_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.csv$")

#: Column holding the observation time.
TIME_COLUMN = "timestep"

#: Column holding the directional wave spectrum as nested JSON.
SPECTRUM_COLUMN = "wave_spectral_data_collection"

#: The platform's own position. Written as data variables because most of the network moves.
POSITION_VARIABLES: tuple[str, ...] = ("longitude", "latitude")

#: Measured quantities, in the order the upstream header lists them. Declared here rather
#: than inferred so every partition writes the same variable set; a station that reports a
#: subset simply leaves the rest NaN.
MEASUREMENT_VARIABLES: tuple[str, ...] = (
    "dew_point_humidity_marine_mean",
    "horizontal_visibility_marine",
    "humidity_relative_marine",
    "pressure_marine",
    "pressure_marine_msl",
    "significant_wave_height_h_third",
    "significant_wave_height_heave",
    "significant_wave_height_hm_0",
    "significant_wave_height_hm_0_swell",
    "significant_wave_height_hm_0_windsea",
    "temperature_air_marine",
    "temperature_sea_surface_marine",
    "wave_height_maximum",
    "wave_height_maximum_crest",
    "wave_height_maximum_trough",
    "wave_mean_direction",
    "wave_mean_period_heave",
    "wave_mean_period_tm_02",
    "wave_mean_period_tz",
    "wave_mean_spreading_angle",
    "wave_peak_direction",
    "wave_peak_direction_swell",
    "wave_peak_direction_windsea",
    "wave_peak_period",
    "wave_peak_period_swell",
    "wave_peak_period_windsea",
    "wave_period_tmax",
    "wind_direction_marine",
    "wind_gust_direction_marine",
    "wind_gust_speed_marine",
    "wind_speed_marine",
)

#: Quality verdicts, worst last: a flag object naming several is reduced to the worst one.
#: Written as ``<measurement>_qc``, with these values recorded in the store's attributes.
QC_FLAG_VALUES: Mapping[str, int] = {"good": 0, "suspect": 1, "bad": 2, "unknown": 3}

QC_VARIABLES: tuple[str, ...] = tuple(f"{name}_qc" for name in MEASUREMENT_VARIABLES)

#: Every ``(time, station)`` variable written, in order.
VARIABLES: tuple[str, ...] = (*POSITION_VARIABLES, *MEASUREMENT_VARIABLES, *QC_VARIABLES)

#: Number of frequency bands in the directional wave spectrum.
SPECTRUM_BANDS = 32
SPECTRUM_DIM = "frequency_band"

#: Fields each band carries: the energy density plus the four directional Fourier
#: coefficients. ``bin_number_energy`` and ``energy_field`` are the upstream's own names.
SPECTRUM_FIELDS: tuple[str, ...] = (
    "bin_number_energy",
    "energy_field",
    "a1",
    "a2",
    "b1",
    "b2",
)

SPECTRUM_VARIABLES: tuple[str, ...] = tuple(f"wave_spectrum_{f}" for f in SPECTRUM_FIELDS)

#: The reporting platforms, as (identifier, display name). Fixing the axis here rather than
#: taking whichever platforms a given hour happens to contain is what keeps every partition
#: appendable; see the module docstring of ``observations.base``. A platform the Met Office
#: adds later is logged and dropped until it is added here, because widening the station
#: axis would be refused by every subsequent append.
STATIONS: tuple[tuple[str, str], ...] = (
    ("alert_automatic_weather_station", "Alert Automatic Weather Station"),
    ("brittany_buoy", "Brittany Buoy"),
    ("cefas_endeavour_automatic_weather_station", "Cefas Endeavour Automatic Weather Station"),
    ("channel_light_buoy", "Channel Light Buoy"),
    ("channel_test_buoy", "Channel Test Buoy"),
    ("clansman_automatic_weather_station", "Clansman Automatic Weather Station"),
    ("corystes_automatic_weather_station", "Corystes Automatic Weather Station"),
    ("discovery_automatic_weather_station", "Discovery Automatic Weather Station"),
    ("e1_buoy", "E1 Buoy"),
    ("f3_light_vessel_automatic_weather_station", "F3 Light Vessel Automatic Weather Station"),
    ("finlaggan_automatic_weather_station", "Finlaggan Automatic Weather Station"),
    ("galatea_automatic_weather_station", "Galatea Automatic Weather Station"),
    ("galicia_automatic_weather_station", "Galicia Automatic Weather Station"),
    (
        "greenwich_light_vessel_automatic_weather_station",
        "Greenwich Light Vessel Automatic Weather Station",
    ),
    ("hamnavoe_automatic_weather_station", "Hamnavoe Automatic Weather Station"),
    ("hebrides_automatic_weather_station", "Hebrides Automatic Weather Station"),
    ("helliar_automatic_weather_station", "Helliar Automatic Weather Station"),
    ("hildasay_automatic_weather_station", "Hildasay Automatic Weather Station"),
    ("hjaltland_automatic_weather_station", "Hjaltland Automatic Weather Station"),
    ("hrossey_automatic_weather_station", "Hrossey Automatic Weather Station"),
    ("james_cook_automatic_weather_station", "James Cook Automatic Weather Station"),
    ("k1_buoy", "K1 Buoy"),
    ("k2_buoy", "K2 Buoy"),
    ("k4_buoy", "K4 Buoy"),
    ("k5_buoy", "K5 Buoy"),
    ("k7_buoy", "K7 Buoy"),
    ("l4_buoy", "L4 Buoy"),
    ("loch_seaforth_automatic_weather_station", "Loch Seaforth Automatic Weather Station"),
    ("lord_of_the_isles_automatic_weather_station", "Lord Of The Isles Automatic Weather Station"),
    ("manxman_automatic_weather_station", "Manxman Automatic Weather Station"),
    ("mazarine_automatic_weather_station", "Mazarine Automatic Weather Station"),
    (
        "national_geographic_endurance_automatic_weather_station",
        "National Geographic Endurance Automatic Weather Station",
    ),
    (
        "national_geographic_explorer_automatic_weather_station",
        "National Geographic Explorer Automatic Weather Station",
    ),
    (
        "national_geographic_orion_automatic_weather_station",
        "National Geographic Orion Automatic Weather Station",
    ),
    (
        "national_geographic_resolution_automatic_weather_station",
        "National Geographic Resolution Automatic Weather Station",
    ),
    ("pap_buoy", "PAP Buoy"),
    ("patricia_automatic_weather_station", "Patricia Automatic Weather Station"),
    ("pharos_automatic_weather_station", "Pharos Automatic Weather Station"),
    ("precision_automatic_weather_station", "Precision Automatic Weather Station"),
    ("pride_of_hull_automatic_weather_station", "Pride Of Hull Automatic Weather Station"),
    ("princess_seaways_automatic_weather_station", "Princess Seaways Automatic Weather Station"),
    ("queen_mary_2_automatic_weather_station", "Queen Mary 2 Automatic Weather Station"),
    (
        "rrs_sir_david_attenborough_automatic_weather_station",
        "RRS Sir David Attenborough Automatic Weather Station",
    ),
    ("salamanca_automatic_weather_station", "Salamanca Automatic Weather Station"),
    (
        "sandettie_light_vessel_automatic_weather_station",
        "Sandettie Light Vessel Automatic Weather Station",
    ),
    ("santona_automatic_weather_station", "Santona Automatic Weather Station"),
    ("scotia_automatic_weather_station", "Scotia Automatic Weather Station"),
    (
        "seatruck_performance_automatic_weather_station",
        "Seatruck Performance Automatic Weather Station",
    ),
    (
        "seven_stones_light_vessel_automatic_weather_station",
        "Seven Stones Light Vessel Automatic Weather Station",
    ),
    ("stena_adventurer_automatic_weather_station", "Stena Adventurer Automatic Weather Station"),
    ("stena_embla_automatic_weather_station", "Stena Embla Automatic Weather Station"),
    ("stena_hibernia_automatic_weather_station", "Stena Hibernia Automatic Weather Station"),
    ("stena_nordica_automatic_weather_station", "Stena Nordica Automatic Weather Station"),
    ("stena_superfast_8_automatic_weather_station", "Stena Superfast 8 Automatic Weather Station"),
    (
        "sunk_inner_light_vessel_automatic_weather_station",
        "Sunk Inner Light Vessel Automatic Weather Station",
    ),
    ("transporter_automatic_weather_station", "Transporter Automatic Weather Station"),
    ("vespertine_automatic_weather_station", "Vespertine Automatic Weather Station"),
    ("w_b_yeats_automatic_weather_station", "W B Yeats Automatic Weather Station"),
)

CANONICAL_IDS: tuple[str, ...] = tuple(slug for slug, _ in STATIONS)


def station_slug(key: str) -> str:
    """The platform identifier encoded in an object key or filename."""
    return FILE_UUID.sub("", pathlib.PurePosixPath(key).name)


def qc_code(value) -> float:
    """Reduce one JSON quality verdict to its numeric code.

    The upstream flag is an object mapping a verdict to the list of checks that produced
    it. An object naming several verdicts is reduced to the worst, so a measurement is
    never reported cleaner than its worst failing check. An absent or unparseable flag is
    NaN rather than ``good``: "we do not know" must not read as "we checked and it passed".
    """
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return float("nan")
    text = str(value).strip()
    if not text:
        return float("nan")
    try:
        verdicts = json.loads(text)
    except (TypeError, ValueError):
        logger.debug(f"uk_marine: unparseable quality flag {text[:60]!r}")
        return float(QC_FLAG_VALUES["unknown"])
    if not isinstance(verdicts, dict) or not verdicts:
        return float("nan")
    unknown = QC_FLAG_VALUES["unknown"]
    return float(max(QC_FLAG_VALUES.get(str(k).lower(), unknown) for k in verdicts))


def spectrum_values(value) -> np.ndarray | None:
    """Turn one ``wave_spectral_data_collection`` cell into a ``(field, band)`` array.

    Returns None when the platform did not report a spectrum, which is most of them.
    Bands the upstream omits, and the ``-2`` sentinel it uses for an unmeasured
    coefficient, both come back as NaN.
    """
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        bands = json.loads(text)
    except (TypeError, ValueError):
        logger.debug(f"uk_marine: unparseable wave spectrum {text[:60]!r}")
        return None
    if not isinstance(bands, dict) or not bands:
        return None

    out = np.full((len(SPECTRUM_FIELDS), SPECTRUM_BANDS), np.nan, dtype="float32")
    for band in range(1, SPECTRUM_BANDS + 1):
        entry = bands.get(f"frequency_band_{band}")
        if not isinstance(entry, dict):
            continue
        for row, field in enumerate(SPECTRUM_FIELDS):
            number = pd.to_numeric(entry.get(field), errors="coerce")
            if pd.isna(number):
                continue
            # -2 is the upstream's "coefficient not measured" sentinel; a real a1/b1 lies
            # in [-1, 1], so letting it through would drag any average off the scale.
            out[row, band - 1] = np.nan if float(number) == -2.0 else float(number)
    return out


def read_station_csv(path: str | pathlib.Path) -> pd.DataFrame:
    """Read one platform-hour CSV into a frame indexed by observation time.

    The returned frame carries the position, the measurements, the encoded quality codes
    and the raw spectrum column; :func:`frames_to_dataset` picks out the numeric ones it
    was asked for and :meth:`UKMarineObservationsProvider.spectra` reads the rest.
    """
    frame = pd.read_csv(path, sep="|")
    if TIME_COLUMN not in frame.columns:
        raise ValueError(f"{path} has no {TIME_COLUMN!r} column")

    times = pd.to_datetime(frame[TIME_COLUMN], utc=True, errors="coerce")
    frame = frame.loc[times.notna()].copy()
    frame.index = pd.DatetimeIndex(times.dropna()).tz_convert("UTC").tz_localize(None)

    for name in MEASUREMENT_VARIABLES:
        flag = f"{name}_qc"
        frame[flag] = [qc_code(v) for v in frame[flag]] if flag in frame.columns else np.nan

    keep = [c for c in (*VARIABLES, SPECTRUM_COLUMN) if c in frame.columns]
    return frame[keep]


class UKMarineObservationsProvider(BaseProvider):
    """Met Office UK marine surface observations, one partition per hour."""

    name = "uk_marine"
    store_prefix = "bkr/obs/uk_marine.icechunk"
    append_dim = TIME_DIM
    station_dim = STATION_DIM
    source_url = SOURCE_URL

    #: One batch covers one hour and holds one observation per platform.
    partition_freq = "1h"
    sample_freq = "1h"

    #: Station tables align on the station axis as well as the grid coordinates.
    alignment_coords = OBSERVATION_ALIGNMENT_COORDS

    def __init__(self, config=None):
        super().__init__(config=config)
        self._filesystem_cache = None

    def filesystem(self):
        """Anonymous handle on the bucket, cached across the partitions of one run."""
        if self._filesystem_cache is None:
            import s3fs

            self._filesystem_cache = s3fs.S3FileSystem(anon=True)
        return self._filesystem_cache

    def stations(self) -> list[Station]:
        """The fixed platform axis.

        No coordinates: most of the network is ships, so position is a per-hour
        observation and is written as a data variable instead. See the module docstring.
        """
        return [Station(id=slug, name=name) for slug, name in STATIONS]

    def partition_times(self, it: pd.Timestamp) -> pd.DatetimeIndex:
        return partition_time_index(naive_utc(it), self.partition_freq, self.sample_freq)

    def run_partition(self, it: pd.Timestamp, check_present: bool = True) -> bool:
        """Run one partition, normalising the timestamp first.

        Dagster passes a tz-aware partition start. Stored times are naive UTC, and a
        tz-aware one never compares equal to them, so without this the "already stored?"
        check never matches and every run appends the hour again.
        """
        return super().run_partition(naive_utc(it), check_present=check_present)

    def batch_prefix(self, it: pd.Timestamp) -> str | None:
        """Find the batch directory covering the hour starting at ``it``.

        The publication timestamp in the directory name drifts by a few minutes, so the
        directory is matched on its window-start field rather than constructed. Returns
        None when the hour was never published or has aged out of the rolling window.
        """
        stamp = naive_utc(it).strftime("%Y%m%d%H00")
        try:
            entries = self.filesystem().ls(BUCKET, detail=False)
        except Exception as exc:  # noqa: BLE001 - a listing failure is a fetch failure
            raise RuntimeError(f"{self.name}: could not list s3://{BUCKET}: {exc}") from exc

        matches = [
            name
            for name in entries
            if (parsed := BATCH_DIR.match(pathlib.PurePosixPath(name).name)) is not None
            and parsed.group(2) == stamp
        ]
        if not matches:
            return None
        # Several batches may cover one hour when the Met Office republishes it; the last
        # one published is the corrected copy.
        return sorted(matches)[-1]

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> list[str]:
        """Download the hour's per-platform CSVs, or return nothing if it was not published."""
        hour = naive_utc(it)
        prefix = self.batch_prefix(hour)
        if prefix is None:
            logger.info(f"{self.name}: no batch published for {hour}")
            return []

        directory = pathlib.Path(temp_dir) if temp_dir is not None else self.config.scratch_dir
        directory = directory / hour.strftime("%Y%m%dT%H")
        directory.mkdir(parents=True, exist_ok=True)

        filesystem = self.filesystem()
        keys = [k for k in filesystem.ls(prefix, detail=False) if str(k).endswith(".csv")]
        if not keys:
            logger.warning(f"{self.name}: batch s3://{prefix} holds no CSVs")
            return []

        paths: list[str] = []
        for key in sorted(keys):
            local = directory / pathlib.PurePosixPath(key).name
            try:
                filesystem.get(key, str(local))
            except FileNotFoundError:
                logger.debug(f"{self.name}: {key} vanished mid-fetch")
                continue
            paths.append(str(local))
        return paths

    def frames(self, input_files: Sequence[str]) -> dict[str, pd.DataFrame]:
        """Read each downloaded CSV, keyed by platform, dropping any platform not on the axis."""
        known = set(CANONICAL_IDS)
        frames: dict[str, pd.DataFrame] = {}
        unknown: set[str] = set()

        for path in input_files:
            slug = station_slug(path)
            if slug not in known:
                unknown.add(slug)
                continue
            try:
                frame = read_station_csv(path)
            except (ValueError, pd.errors.ParserError) as exc:
                logger.warning(f"{self.name}: skipping unreadable {path} ({exc})")
                continue
            if frame.empty:
                continue
            # One platform can appear twice in a batch under two UUIDs; keep both rows and
            # let align_to_grid's duplicate handling take the later one.
            frames[slug] = pd.concat([frames[slug], frame]) if slug in frames else frame

        if unknown:
            # Writing these would widen the station axis and break every later append.
            logger.warning(
                f"{self.name}: ignoring {len(unknown)} platform(s) not in STATIONS: "
                f"{sorted(unknown)[:5]}"
            )
        return frames

    def spectra(
        self, frames: Mapping[str, pd.DataFrame], times: pd.DatetimeIndex
    ) -> dict[str, xr.DataArray]:
        """Build the ``(time, station, frequency_band)`` wave-spectrum variables.

        Always returns the full set, all-NaN when no platform reported a spectrum, so the
        store's variable set does not depend on which buoys happened to be reporting.
        """
        shape = (len(times), len(CANONICAL_IDS), SPECTRUM_BANDS)
        arrays = {name: np.full(shape, np.nan, dtype="float32") for name in SPECTRUM_VARIABLES}
        position = {slug: i for i, slug in enumerate(CANONICAL_IDS)}
        row_of = {when: i for i, when in enumerate(times)}

        for slug, frame in frames.items():
            if SPECTRUM_COLUMN not in frame.columns:
                continue
            column = position[slug]
            for when, value in frame[SPECTRUM_COLUMN].items():
                row = row_of.get(pd.Timestamp(when))
                if row is None:
                    continue
                values = spectrum_values(value)
                if values is None:
                    continue
                for field, name in zip(range(len(SPECTRUM_FIELDS)), SPECTRUM_VARIABLES):
                    arrays[name][row, column, :] = values[field]

        dims = (TIME_DIM, STATION_DIM, SPECTRUM_DIM)
        return {name: xr.DataArray(values, dims=dims) for name, values in arrays.items()}

    def process(
        self,
        input_files: list[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Stack the hour's platform files into one ``(time, station)`` cube."""
        frames = self.frames(input_files)
        if not frames:
            raise NoStationDataError(
                f"{self.name}: {len(input_files)} file(s) for {naive_utc(it)} held no usable rows"
            )

        times = self.partition_times(it)
        dataset = frames_to_dataset(
            frames,
            self.stations(),
            times,
            variables=VARIABLES,
            sample_freq=self.sample_freq,
            how="exact",
            attrs={
                "network": self.name,
                "source": SOURCE_URL,
                "bucket": f"s3://{BUCKET}",
                "qc_flag_values": " ".join(str(v) for v in QC_FLAG_VALUES.values()),
                "qc_flag_meanings": " ".join(QC_FLAG_VALUES),
            },
        )
        dataset = dataset.assign(self.spectra(frames, times))
        dataset = dataset.assign_coords(
            {SPECTRUM_DIM: np.arange(1, SPECTRUM_BANDS + 1, dtype="int16")}
        )
        return dataset

    def write_to_icechunk(self, repo, processed):
        return _write_to_icechunk(
            repo,
            processed,
            append_dim=self.append_dim,
            message=f"{self.name}: {processed[self.append_dim].values[0]}",
            alignment_coords=self.alignment_coords,
        )


__all__ = [
    "BUCKET",
    "CANONICAL_IDS",
    "MEASUREMENT_VARIABLES",
    "QC_FLAG_VALUES",
    "SPECTRUM_BANDS",
    "SPECTRUM_VARIABLES",
    "STATIONS",
    "UKMarineObservationsProvider",
    "VARIABLES",
    "qc_code",
    "read_station_csv",
    "spectrum_values",
    "station_slug",
]
