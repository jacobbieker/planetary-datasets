"""In-situ ocean observations from the WIS2 Global Cache, as one Icechunk store.

An archive target of :mod:`planetary_datasets.providers.observations.wis2_archive`: while
a 12-hour window of the cache streams into its tar, the objects carrying ocean
observations are also decoded (BUFR, with ecCodes) and appended to
``bkr/obs/wis2_ocean.icechunk``. The store has two groups, opened together with
``xr.open_datatree(store, engine="zarr", consolidated=False)`` or one at a time with
``xr.open_zarr(store, group=..., consolidated=False)``:

``/surface``
    One row per report (dimension ``observation``): moored and drifting buoys, ships,
    uncrewed surface vehicles and wave buoys. Position, time, platform identifiers, sea
    surface temperature and salinity, air pressure, temperature, humidity, wind, the bulk
    wave parameters (ERA5's names) and surface currents, as reported, and the arrival
    times (below).
``/profiles``
    CF contiguous ragged arrays: one row per profile (dimension ``profile``: platform,
    time, position, cycle, arrival times) and one per level (dimension ``level``: pressure or depth,
    water temperature, salinity, dissolved oxygen, current). ``level_count`` gives each
    profile's levels, in order (CF ``sample_dimension``). Argo floats, and the subsurface
    temperature and salinity some buoys report.

Sources, by WIS2 topic or (for the DWD and JMA relays of the GTS) by the TTAAii of the
bulletin in the object key:

* ``ocean/surface-based-observations/*`` (moored and drifting buoys, ocean profilers)
* ``weather/surface-based-observations/ship``
* GTS BUFR ``IOB``/``IOD`` (buoys), ``IOP`` (profiling floats), ``IOR`` (uncrewed
  surface vehicles), ``IOW`` (wave buoys), ``ISS`` (ships)

Not decoded: wave spectra (only the bulk parameters are kept), DART tsunameter water
column heights (``IOZ``), traditional alphanumeric (TAC) SHIP/BUOY text, and gridded ocean
model products, which are not observations.

The same report reaches the cache from several centres (a buoy's own WIS2 node, and the
DWD and JMA GTS relays), sometimes in different windows. A report is identified by its
platform, time to the minute and position to 0.01 degree (a profile likewise); within a
window the copy with the most values is kept, and reports the store already holds (its
recent rows are read back) are not written again. Values are as decoded, lossless:
floats stay float64 unless every value survives float32 exactly.

Each row of either group records when its report reached the cache, so the live stream
can be replayed: ``arrival_time``, when the copy kept arrived, and ``first_arrival_time``,
the earliest arrival of any copy the window collapsed into the row. Both are UTC, to the
second; rows written before they were added read NaT.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import math
import re
from typing import Iterable, Sequence

import numpy as np
import xarray as xr
from loguru import logger

from planetary_datasets.config import Config, get_config
from planetary_datasets.providers.observations.wis2_targets import (
    QueuedTarget,
    register,
    split_bufr,
)

STORE_PREFIX = "bkr/obs/wis2_ocean.icechunk"
SURFACE = "surface"
PROFILES = "profiles"

#: ecCodes' missing values.
MISSING_DOUBLE = -1e100
MISSING_LONG = 2147483647

#: Rows read back from each group to find reports a window repeats.
DEDUP_TAIL_ROWS = {SURFACE: 400_000, PROFILES: 40_000}
#: Rounding of positions in a report's identity: copies through different centres can be
#: encoded at different precisions.
POSITION_DECIMALS = 2

#: WIS2 topic paths (after ``/data/core/``) carrying ocean observations, and their kind.
TOPICS = {
    "ocean/surface-based-observations/moored-buoys-and-moorings": "moored_buoy",
    "ocean/surface-based-observations/drifting-buoys": "drifting_buoy",
    "ocean/surface-based-observations/drifting-ocean-profilers": "profiling_float",
    "ocean/surface-based-observations": "ocean",
    "weather/surface-based-observations/ship": "ship",
}
#: GTS BUFR bulletins (T1 T2 A1 of the TTAAii) carrying ocean observations, and their kind.
BULLETINS = {
    "IOB": "buoy",
    "IOD": "buoy",
    "IOP": "profiling_float",
    "IOR": "uncrewed_surface_vehicle",
    "IOW": "wave_buoy",
    "ISS": "ship",
}
GTS_KEY = re.compile(r"^data/[^/]+-gts-to-wis2/data/core/([A-Z])/([A-Z])/([A-Z])/")


def kind_of(key: str) -> str | None:
    """The kind of ocean platform an object reports from, or None if not ocean in-situ."""
    gts = GTS_KEY.match(key)
    if gts:
        return BULLETINS.get("".join(gts.groups()))
    _, _, path = key.partition("/data/core/")
    for topic, kind in TOPICS.items():
        if path.startswith(topic + "/"):
            return kind
    return None


# ----------------------------------------------------------------------------- decoding

#: Keys that start a new level of a profile: the vertical coordinate.
LEVEL_KEYS = {"waterPressure", "depthBelowWaterSurface"}
#: Keys read per level, and their variable in ``/profiles``.
LEVEL_VARIABLES = {
    "waterPressure": "sea_water_pressure",
    "depthBelowWaterSurface": "depth",
    "oceanographicWaterTemperature": "sea_water_temperature",
    "salinity": "sea_water_salinity",
    "oceanographicDissolvedOxygen": "dissolved_oxygen",
    "currentDirection": "current_direction",
    "speedOfCurrent": "current_speed",
}

#: Report-level keys, first value present wins, and their variable in ``/surface``.
SURFACE_VARIABLES: dict[str, tuple[str, ...]] = {
    "surface_air_pressure": ("nonCoordinatePressure",),
    "mean_sea_level_pressure": ("pressureReducedToMeanSeaLevel",),
    "pressure_tendency_3h": ("3HourPressureChange",),
    "air_temperature": ("airTemperature",),
    "dewpoint_temperature": ("dewpointTemperature",),
    "relative_humidity": ("relativeHumidity",),
    "wind_speed": ("windSpeed",),
    "wind_direction": ("windDirection",),
    "wind_gust": ("maximumWindGustSpeed",),
    "horizontal_visibility": ("horizontalVisibility",),
    "total_cloud_cover": ("cloudCoverTotal",),
    # A water temperature or salinity reported before any depth is the surface's (drifters).
    "sea_surface_temperature": ("seaSurfaceTemperature", "oceanographicWaterTemperature"),
    "skin_temperature": ("skinTemperature",),
    "sea_surface_salinity": ("seaSurfaceSalinity", "salinity"),
    "significant_height_of_combined_wind_waves_and_swell": (
        "significantWaveHeight",
        "heightOfWaves",
    ),
    "maximum_individual_wave_height": ("maximumWaveHeight",),
    "mean_wave_period": ("averageWavePeriod", "periodOfWaves"),
    "peak_wave_period": ("spectralPeakWavePeriod",),
    "mean_wave_direction": ("directionFromWhichDominantWavesAreComing", "wavesDirection"),
    "wave_directional_spread": ("directionalSpreadOfDominantWave",),
    "significant_height_of_wind_waves": ("heightOfWindWaves",),
    "mean_period_of_wind_waves": ("periodOfWindWaves",),
    "mean_direction_of_wind_waves": ("windWavesDirection",),
    "significant_height_of_first_swell_partition": ("heightOfSwellWaves",),
    "mean_period_of_first_swell_partition": ("periodOfSwellWaves",),
    "mean_direction_of_first_swell_partition": ("swellWavesDirection",),
    "sea_surface_current_speed": ("speedOfSeaSurfaceCurrent",),
    "sea_surface_current_direction": ("seaSurfaceCurrentDirection",),
    "sea_floor_depth": ("totalWaterDepth",),
    "platform_direction": ("directionOfMotionOfMovingObservingPlatform",),
    "platform_speed": ("movingObservingPlatformSpeed", "platformDriftSpeed"),
}
#: Code-table and count keys, stored as integers with -1 for missing.
CODE_VARIABLES: dict[str, tuple[str, ...]] = {
    "data_buoy_type": ("dataBuoyType",),
    "float_cycle_number": ("floatCycleNumber",),
    "direction_of_profile": ("directionOfProfile",),
    "profile_instrument_type": ("instrumentTypeForWaterTemperatureOrSalinityProfileMeasurement",),
}
IDENTIFIER_KEYS = {
    "wmo_platform_id": ("marineObservingPlatformIdentifier", "buoyOrPlatformIdentifier"),
    "call_sign": ("shipOrMobileLandStationIdentifier",),
    "station_name": ("stationOrSiteName", "longStationName"),
}
WIGOS_KEYS = (
    "wigosIdentifierSeries",
    "wigosIssuerOfIdentifier",
    "wigosIssueNumber",
    "wigosLocalIdentifierCharacter",
)
TIME_KEYS = ("year", "month", "day", "hour", "minute", "second")

UNITS = {
    "latitude": "degrees_north",
    "longitude": "degrees_east",
    "surface_air_pressure": "Pa",
    "mean_sea_level_pressure": "Pa",
    "pressure_tendency_3h": "Pa",
    "air_temperature": "K",
    "dewpoint_temperature": "K",
    "relative_humidity": "%",
    "wind_speed": "m s-1",
    "wind_direction": "degree",
    "wind_gust": "m s-1",
    "horizontal_visibility": "m",
    "total_cloud_cover": "%",
    "sea_surface_temperature": "K",
    "skin_temperature": "K",
    "sea_surface_salinity": "1e-3",
    "significant_height_of_combined_wind_waves_and_swell": "m",
    "maximum_individual_wave_height": "m",
    "mean_wave_period": "s",
    "peak_wave_period": "s",
    "mean_wave_direction": "degree",
    "wave_directional_spread": "degree",
    "significant_height_of_wind_waves": "m",
    "mean_period_of_wind_waves": "s",
    "mean_direction_of_wind_waves": "degree",
    "significant_height_of_first_swell_partition": "m",
    "mean_period_of_first_swell_partition": "s",
    "mean_direction_of_first_swell_partition": "degree",
    "sea_surface_current_speed": "m s-1",
    "sea_surface_current_direction": "degree",
    "sea_floor_depth": "m",
    "platform_direction": "degree",
    "platform_speed": "m s-1",
    "sea_water_pressure": "Pa",
    "depth": "m",
    "sea_water_temperature": "K",
    "sea_water_salinity": "1e-3",
    "dissolved_oxygen": "umol kg-1",
    "current_direction": "degree",
    "current_speed": "m s-1",
}

#: Descriptions of the arrival times, worded the same in every WIS2 observation store.
ARRIVAL_ATTRS = {
    "arrival_time": {
        "long_name": "time the copy of this observation kept here arrived in the WIS2 Global Cache",
    },
    "first_arrival_time": {
        "long_name": "earliest time any copy of this observation arrived in the WIS2 Global Cache",
        "comment": "minimum over the copies (GTS relays, the originating centre's topic) "
        "collapsed into this row within its window; rows written before this variable was "
        "added have no value (NaT)",
    },
}
ARRIVAL_KEYS = tuple(ARRIVAL_ATTRS)

#: Everything the walker reads; other keys are skipped without being fetched.
READ_KEYS = (
    set(LEVEL_VARIABLES)
    | {k for keys in SURFACE_VARIABLES.values() for k in keys}
    | {k for keys in CODE_VARIABLES.values() for k in keys}
    | {k for keys in IDENTIFIER_KEYS.values() for k in keys}
    | set(WIGOS_KEYS)
    | set(TIME_KEYS)
    | {"latitude", "longitude"}
)
RANK = re.compile(r"^#\d+#")


def _value(handle, name: str):
    import eccodes

    value = eccodes.codes_get(handle, name)
    if isinstance(value, str):
        value = value.strip().strip("\x00").strip()
        return value or None
    if isinstance(value, float):
        return None if value == MISSING_DOUBLE or math.isnan(value) else value
    return None if value == MISSING_LONG else value


def _walk(handle) -> list[tuple[str, object]]:
    """The subset's wanted keys, in message order, as ``(name, value)``; missing skipped."""
    import eccodes

    pairs = []
    it = eccodes.codes_bufr_keys_iterator_new(handle)
    try:
        while eccodes.codes_bufr_keys_iterator_next(it):
            name = eccodes.codes_bufr_keys_iterator_get_name(it)
            base = RANK.sub("", name)
            if base not in READ_KEYS or "->" in name:
                continue
            try:
                value = _value(handle, name)
            except eccodes.CodesInternalError:
                continue
            if value is not None or base in LEVEL_KEYS:
                pairs.append((base, value))
    finally:
        eccodes.codes_bufr_keys_iterator_delete(it)
    return pairs


def decode_subsets(message: bytes) -> list[list[tuple[str, object]]]:
    """Every subset of one BUFR message as its ``(name, value)`` stream.

    Raises:
        eccodes.CodesInternalError: The message cannot be decoded.
    """
    import eccodes

    handle = eccodes.codes_new_from_message(message)
    try:
        eccodes.codes_set(handle, "unpack", 1)
        count = eccodes.codes_get(handle, "numberOfSubsets")
        if count == 1:
            return [_walk(handle)]
        subsets = []
        for number in range(1, count + 1):
            eccodes.codes_set(handle, "extractSubset", number)
            eccodes.codes_set(handle, "doExtractSubsets", 1)
            one = eccodes.codes_clone(handle)
            try:
                eccodes.codes_set(one, "unpack", 1)
                subsets.append(_walk(one))
            finally:
                eccodes.codes_release(one)
        return subsets
    finally:
        eccodes.codes_release(handle)


# ----------------------------------------------------------------------------- reports


@dataclasses.dataclass
class Report:
    """One subset: its surface values and, if it reports below the surface, a profile."""

    surface: dict
    levels: list[dict]


def _first(values: dict[str, list], keys: Sequence[str]):
    for key in keys:
        for value in values.get(key, ()):
            if value is not None:
                return value
    return None


def to_report(pairs: list[tuple[str, object]], kind: str, key: str) -> Report | None:
    """A subset's report, or None if it has no time or position."""
    values: dict[str, list] = {}
    levels: list[dict] = []
    level: dict | None = None
    for name, value in pairs:
        if name in LEVEL_KEYS:
            # A vertical coordinate opens a level; the values after it belong to it.
            level = {LEVEL_VARIABLES[name]: value}
            levels.append(level)
            continue
        if name in LEVEL_VARIABLES and level is not None:
            if LEVEL_VARIABLES[name] not in level:
                level[LEVEL_VARIABLES[name]] = value
            continue
        values.setdefault(name, []).append(value)

    when = [_first(values, (k,)) for k in TIME_KEYS]
    latitude, longitude = _first(values, ("latitude",)), _first(values, ("longitude",))
    if any(v is None for v in when[:5]) or latitude is None or longitude is None:
        return None
    try:
        time = dt.datetime(*(int(v) for v in when[:5]), int(when[5] or 0))
    except (TypeError, ValueError):
        return None

    surface: dict = {
        "time": np.datetime64(time, "s"),
        "latitude": float(latitude),
        "longitude": float(longitude),
        "platform_kind": kind,
        "source_key": key,
        "source_centre": key.split("/")[1] if key.count("/") > 1 else "",
    }
    for name, keys in IDENTIFIER_KEYS.items():
        value = _first(values, keys)
        surface[name] = None if value is None else str(value).strip()
    wigos = [_first(values, (k,)) for k in WIGOS_KEYS]
    surface["wigos_id"] = (
        "-".join(str(v) for v in wigos) if all(v is not None for v in wigos) else None
    )
    for name, keys in SURFACE_VARIABLES.items():
        value = _first(values, keys)
        surface[name] = np.nan if value is None else float(value)
    for name, keys in CODE_VARIABLES.items():
        value = _first(values, keys)
        surface[name] = -1 if value is None else int(value)

    # A level holding nothing but its coordinate (a sensor height, an unused replication)
    # is not a measurement.
    measured = [
        lv
        for lv in levels
        if any(lv.get(v) is not None for v in LEVEL_VARIABLES.values())
        and any(lv.get(v) is not None for k, v in LEVEL_VARIABLES.items() if k not in LEVEL_KEYS)
    ]
    # A water temperature or salinity at or above 1 m with nothing deeper is the surface's.
    if measured and all(
        (lv.get("depth") or 0.0) <= 1.0 and lv.get("sea_water_pressure") is None for lv in measured
    ):
        top = measured[0]
        if (
            math.isnan(surface["sea_surface_temperature"])
            and top.get("sea_water_temperature") is not None
        ):
            surface["sea_surface_temperature"] = float(top["sea_water_temperature"])
        if (
            math.isnan(surface["sea_surface_salinity"])
            and top.get("sea_water_salinity") is not None
        ):
            surface["sea_surface_salinity"] = float(top["sea_water_salinity"])
        if len(measured) == 1:
            measured = []
    return Report(surface=surface, levels=_merge_levels(measured))


def _merge_levels(levels: list[dict]) -> list[dict]:
    """Levels at the same coordinate (Argo repeats pressure for its oxygen) as one."""
    merged: dict[tuple, dict] = {}
    for lv in levels:
        coordinate = (lv.get("sea_water_pressure"), lv.get("depth"))
        into = merged.setdefault(coordinate, {})
        for name, value in lv.items():
            if value is not None and into.get(name) is None:
                into[name] = value
    return list(merged.values())


def platform_id(row: dict) -> str:
    """The platform's identity: its WMO number, else call sign, WIGOS id or name."""
    for name in ("wmo_platform_id", "call_sign", "wigos_id", "station_name"):
        if row.get(name):
            return str(row[name])
    return ""


def identity(row: dict) -> tuple:
    """A report's identity across centres: platform, minute, rounded position."""
    return (
        platform_id(row),
        np.datetime64(row["time"], "m"),
        round(float(row["latitude"]), POSITION_DECIMALS),
        round(float(row["longitude"]), POSITION_DECIMALS),
    )


def _richness(row: dict) -> int:
    return sum(
        1
        for k, v in row.items()
        if not (
            k in ARRIVAL_KEYS
            or v is None
            or (isinstance(v, float) and math.isnan(v))
            or (k in CODE_VARIABLES and v == -1)
        )
    )


# ----------------------------------------------------------------------------- collecting


@dataclasses.dataclass
class Counts:
    objects: int = 0
    messages: int = 0
    bad_messages: int = 0
    reports: int = 0
    without_position: int = 0


class Collector:
    """Decodes objects into reports, keeping the richest copy of each."""

    def __init__(self) -> None:
        self.surface: dict[tuple, dict] = {}
        self.profiles: dict[tuple, tuple[dict, list[dict]]] = {}
        self.counts = Counts()
        self.duplicates = {SURFACE: 0, PROFILES: 0}
        #: Per group and identity, the earliest arrival among the copies seen.
        self.first_arrival: dict[str, dict[tuple, np.datetime64]] = {SURFACE: {}, PROFILES: {}}

    def add(self, key: str, body: bytes, arrived: dt.datetime | None = None) -> None:
        kind = kind_of(key)
        if kind is None:
            return
        stamp = _utc_seconds(arrived)
        self.counts.objects += 1
        for message in split_bufr(body):
            self.counts.messages += 1
            try:
                subsets = decode_subsets(message)
            except Exception as exc:  # noqa: BLE001 - one bad message must not stop the rest
                self.counts.bad_messages += 1
                logger.debug(f"wis2 ocean: {key}: undecodable message ({exc})")
                continue
            for pairs in subsets:
                report = to_report(pairs, kind, key)
                if report is None:
                    self.counts.without_position += 1
                    continue
                self.counts.reports += 1
                report.surface["arrival_time"] = stamp
                self._keep(report)

    def _arrived(self, group: str, ident: tuple, arrived: np.datetime64 | None) -> None:
        earliest = self.first_arrival[group].get(ident)
        if arrived is not None and (earliest is None or arrived < earliest):
            self.first_arrival[group][ident] = arrived

    def _keep(self, report: Report) -> None:
        ident = identity(report.surface)
        arrived = report.surface.get("arrival_time")
        if report.levels:
            meta = {k: v for k, v in report.surface.items() if k in PROFILE_FIELDS}
            held = self.profiles.get(ident)
            if held is not None:
                self.duplicates[PROFILES] += 1
            if held is None or len(report.levels) > len(held[1]):
                self.profiles[ident] = (meta, report.levels)
            self._arrived(PROFILES, ident, arrived)
        if any(not math.isnan(report.surface[name]) for name in SURFACE_VARIABLES):
            held = self.surface.get(ident)
            if held is not None:
                self.duplicates[SURFACE] += 1
            if held is None or _richness(report.surface) > _richness(held):
                self.surface[ident] = report.surface
            self._arrived(SURFACE, ident, arrived)

    def surface_rows(self, held: set[tuple] = frozenset()) -> list[dict]:
        """The kept reports not in ``held``, each with its ``first_arrival_time``."""
        return [
            {**row, "first_arrival_time": self.first_arrival[SURFACE].get(ident)}
            for ident, row in self.surface.items()
            if ident not in held
        ]

    def profile_rows(self, held: set[tuple] = frozenset()) -> list[tuple[dict, list[dict]]]:
        """The kept profiles not in ``held``, each with its ``first_arrival_time``."""
        return [
            ({**meta, "first_arrival_time": self.first_arrival[PROFILES].get(ident)}, levels)
            for ident, (meta, levels) in self.profiles.items()
            if ident not in held
        ]


def _utc_seconds(when: dt.datetime | None) -> np.datetime64 | None:
    if when is None:
        return None
    if when.tzinfo is not None:
        when = when.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return np.datetime64(when, "s")


PROFILE_FIELDS = (
    "time",
    "latitude",
    "longitude",
    "platform_kind",
    "source_key",
    "source_centre",
    "wmo_platform_id",
    "call_sign",
    "station_name",
    "wigos_id",
    "data_buoy_type",
    "float_cycle_number",
    "direction_of_profile",
    "profile_instrument_type",
    "arrival_time",
)
SURFACE_STRINGS = (
    "platform_kind",
    "source_key",
    "source_centre",
    "wmo_platform_id",
    "call_sign",
    "station_name",
    "wigos_id",
)


def _strings(values: Iterable) -> np.ndarray:
    return np.array(["" if v is None else str(v) for v in values], dtype=object)


def _times(values: Iterable) -> np.ndarray:
    return np.array(
        [np.datetime64("NaT", "s") if v is None else v for v in values], dtype="datetime64[s]"
    )


def surface_dataset(rows: Sequence[dict]) -> xr.Dataset:
    """``/surface`` rows as a dataset along ``observation``, in time order."""
    rows = sorted(rows, key=lambda r: (r["time"], platform_id(r)))
    data = {
        "time": ("observation", np.array([r["time"] for r in rows], dtype="datetime64[s]")),
        "latitude": ("observation", np.array([r["latitude"] for r in rows], dtype="float64")),
        "longitude": ("observation", np.array([r["longitude"] for r in rows], dtype="float64")),
    }
    for name in SURFACE_STRINGS:
        data[name] = ("observation", _strings(r[name] for r in rows))
    for name in SURFACE_VARIABLES:
        data[name] = ("observation", np.array([r[name] for r in rows], dtype="float64"))
    for name in CODE_VARIABLES:
        data[name] = ("observation", np.array([r[name] for r in rows], dtype="int32"))
    for name in ARRIVAL_KEYS:
        data[name] = ("observation", _times(r.get(name) for r in rows))
    ds = xr.Dataset(data)
    return _attributes(ds)


def profiles_dataset(profiles: Sequence[tuple[dict, list[dict]]]) -> xr.Dataset:
    """``/profiles`` as CF contiguous ragged arrays along ``profile`` and ``level``."""
    profiles = sorted(profiles, key=lambda p: (p[0]["time"], platform_id(p[0])))
    metas = [m for m, _ in profiles]
    data = {
        "time": ("profile", np.array([m["time"] for m in metas], dtype="datetime64[s]")),
        "latitude": ("profile", np.array([m["latitude"] for m in metas], dtype="float64")),
        "longitude": ("profile", np.array([m["longitude"] for m in metas], dtype="float64")),
        "level_count": ("profile", np.array([len(lv) for _, lv in profiles], dtype="int32")),
    }
    for name in SURFACE_STRINGS:
        data[name] = ("profile", _strings(m.get(name) for m in metas))
    for name in CODE_VARIABLES:
        data[name] = ("profile", np.array([m.get(name, -1) for m in metas], dtype="int32"))
    for name in ARRIVAL_KEYS:
        data[name] = ("profile", _times(m.get(name) for m in metas))
    levels = [lv for _, lvs in profiles for lv in lvs]
    for name in LEVEL_VARIABLES.values():
        values = [lv.get(name) for lv in levels]
        data[name] = (
            "level",
            np.array([np.nan if v is None else float(v) for v in values], dtype="float64"),
        )
    ds = xr.Dataset(data)
    ds["level_count"].attrs.update(
        sample_dimension="level",
        long_name="number of levels of each profile, stored contiguously along level",
    )
    return _attributes(ds)


def _attributes(ds: xr.Dataset) -> xr.Dataset:
    for name, units in UNITS.items():
        if name in ds:
            ds[name].attrs["units"] = units
    for name in CODE_VARIABLES:
        if name in ds:
            ds[name].attrs["_FillValue"] = -1
    for name, attrs in ARRIVAL_ATTRS.items():
        ds[name].attrs.update(attrs)
    ds["platform_kind"].attrs["long_name"] = "platform kind, from the WIS2 topic or GTS bulletin"
    ds["source_key"].attrs["long_name"] = "WIS2 Global Cache key of the object the report came from"
    return ds


# ----------------------------------------------------------------------------- the store


def stored_identities(repo, group: str) -> set[tuple]:
    """Identities of the reports (or profiles) in the tail of ``group``."""
    import zarr

    try:
        root = zarr.open_group(repo.readonly_session("main").store, mode="r")
        node = root[group]
    except (KeyError, FileNotFoundError, zarr.errors.GroupNotFoundError, ValueError):
        return set()
    if "time" not in node:
        return set()
    size = node["time"].shape[0]
    start = max(0, size - DEDUP_TAIL_ROWS[group])
    columns = {name: node[name][start:] for name in ("time", "latitude", "longitude")}
    ids = {
        name: node[name][start:] if name in node else np.full(size - start, "", dtype=object)
        for name in ("wmo_platform_id", "call_sign", "wigos_id", "station_name")
    }
    units = node["time"].attrs.get("units", "seconds since 1970-01-01")
    times = xr.coding.times.decode_cf_datetime(columns["time"], units)
    out = set()
    for i in range(size - start):
        row = {name: (str(ids[name][i]) or None) for name in ids}
        row.update(
            time=times[i], latitude=columns["latitude"][i], longitude=columns["longitude"][i]
        )
        out.add(identity(row))
    return out


def write(collector: Collector, partition: str, config: Config | None = None) -> dict:
    """Append a window's reports to the store, skipping any it already holds."""
    from planetary_datasets.common.multidim import append_partition, describe_variables

    config = config or get_config()
    repo = config.icechunk_repo(STORE_PREFIX)
    summary = {"partition": partition, **dataclasses.asdict(collector.counts)}
    summary["duplicates_in_window"] = dict(collector.duplicates)

    rows = collector.surface_rows(stored_identities(repo, SURFACE))
    summary["surface_already_stored"] = len(collector.surface) - len(rows)
    summary["surface_written"] = 0
    if rows:
        append_partition(
            repo, surface_dataset(rows), partition, label="wis2 ocean surface", group=SURFACE
        )
        describe_variables(repo, ARRIVAL_ATTRS, label="wis2 ocean surface", group=SURFACE)
        summary["surface_written"] = len(rows)

    profiles = collector.profile_rows(stored_identities(repo, PROFILES))
    summary["profiles_already_stored"] = len(collector.profiles) - len(profiles)
    summary["profiles_written"] = 0
    if profiles:
        append_partition(
            repo, profiles_dataset(profiles), partition, label="wis2 ocean profiles", group=PROFILES
        )
        describe_variables(repo, ARRIVAL_ATTRS, label="wis2 ocean profiles", group=PROFILES)
        summary["profiles_written"] = len(profiles)
        summary["levels_written"] = sum(len(lv) for _, lv in profiles)
    _describe_root(repo)
    return summary


ROOT_ATTRS = {
    "title": "In-situ ocean observations from the WMO WIS2 Global Cache",
    "source": "s3://wis2globalcache (WIS2 topics and DWD/JMA GTS relays), decoded with ecCodes",
    "groups": "surface: one row per report along observation; profiles: CF contiguous ragged "
    "arrays, profile metadata along profile and levels along level (level_count)",
    "featureType": "point (surface), profile (profiles)",
    "Conventions": "CF-1.11",
}


def _describe_root(repo) -> None:
    import icechunk
    import zarr

    session = repo.writable_session("main")
    root = zarr.open_group(session.store, mode="a")
    if all(root.attrs.get(k) == v for k, v in ROOT_ATTRS.items()):
        return
    root.attrs.update(ROOT_ATTRS)
    session.commit("wis2 ocean: describe the store", rebase_with=icechunk.ConflictDetector())


# ----------------------------------------------------------------------------- the target


@register("ocean")
class OceanTarget(QueuedTarget):
    """The archive target: ocean objects decoded as the window streams, written after."""

    store_prefix = STORE_PREFIX

    def __init__(self, config: Config):
        super().__init__(config)
        self.collector = Collector()

    def wants(self, key: str, size: int) -> bool:
        return kind_of(key) is not None

    def decode(self, key: str, body: bytes, arrived: dt.datetime | None) -> None:
        self.collector.add(key, body, arrived)

    def write(
        self, window_start: dt.datetime | None, window_end: dt.datetime | None, partition: str
    ) -> dict:
        collector, self.collector = self.collector, Collector()
        return write(collector, partition, self.config)


def collect(
    objects: Iterable[tuple[str, bytes] | tuple[str, bytes, dt.datetime | None]],
) -> Collector:
    """A collector fed every ocean object of ``objects``: ``(key, body[, arrived])``."""
    collector = Collector()
    for key, body, *arrived in objects:
        collector.add(key, body, *arrived)
    return collector


__all__ = [
    "OceanTarget",
    "STORE_PREFIX",
    "collect",
    "decode_subsets",
    "kind_of",
    "profiles_dataset",
    "surface_dataset",
    "to_report",
    "write",
]
