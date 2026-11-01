"""Land surface station reports from the WIS2 Global Cache, as one Icechunk store.

An archive target of :mod:`planetary_datasets.providers.observations.wis2_archive`: as
each 12-hour window streams into its tar, the objects that can hold land surface reports
are also handed here, decoded with ecCodes, and appended to::

    bkr/obs/wis2_land.icechunk

one row per station report along ``observation``.

What is read
    BUFR in data category 0 ("surface data - land"): SYNOP (templates 3 07 080, 3 07 096),
    hourly and n-minute automatic-station reports (3 07 091, 3 07 086), METAR in BUFR and
    national variants, whatever template a centre uses, because values are found by
    their element names, not their position. The candidates are the WIS2
    ``surface-based-observations/synop`` topics of every centre and the GTS relays'
    land-surface BUFR bulletins (TTAAii ``ISA``, ``ISI``, ``ISM``, ``ISN``). Ship and buoy
    reports (category 1), upper air, aircraft, satellite data and CLIMAT monthly
    summaries are left to other targets or not read; text (TAC) SYNOP bulletins are not
    decoded, as their content also reaches WIS2 as BUFR.

Values
    Units are the BUFR ones, which are SI and match ERA5's where ERA5 has the quantity
    (K, Pa, m s-1, m), except precipitation, kept in kg m-2 as reported (1 kg m-2 is
    1 mm). Cloud cover is stored as the reported percentage and decodes to ERA5's (0 - 1)
    through ``scale_factor``. Precipitation is reported over different periods (1, 3, 6,
    12, 24 h), often two in one report; each common period has its own variable rather
    than one variable with a period column, so a reader can take the 6-hour totals
    without filtering, and a report with two totals keeps both in one row. Extreme
    temperatures come with whatever period the station reports, so they carry it in a
    ``*_period`` variable. A value a report does not have is NaN (or -1 for codes).
    Values are archived as reported, without quality control: some stations send
    impossible values (a 194 K temperature, a 315 m s-1 wind), and BUFR's special values
    are kept: precipitation -0.1 is a trace, snow depth -0.01 is snow cover that is not
    continuous and -0.02 a depth that could not be measured. Periods come with their own
    units (hours for SYNOP's, minutes for most automatic stations'), so a 1-hour maximum
    from an automatic station and a 12-hour one from a SYNOP read as 1 and 12.

Reports kept
    A report needs a station (WIGOS identifier, or WMO block and station number) and a
    time; NIL reports, with nothing observed, are skipped. One without a date takes its
    message's. One without a position (some centres omit it) is kept, with the position of
    another report of the station in the window when there is one, NaN otherwise.

Duplicates
    The same report reaches the cache from its national centre and from one or more GTS
    relays, often in different templates. A report is identified by station, observation
    time and BUFR update sequence number: relays of one report collapse to the copy with
    the most values, while a correction (a higher sequence number) is kept as its own row,
    so readers wanting the final version take the highest ``update_sequence_number`` per
    station and time. Reports already in the store's last :data:`DEDUP_ROWS` rows (a few
    days) are not written again.

Arrival times
    Each row records when its report reached the cache, so the live stream can be
    replayed: ``arrived``, when the copy kept arrived (``arrival_time`` in the other WIS2
    stores; the name predates them), and ``first_arrival_time``, the earliest arrival of
    any copy the window collapsed into the row. Both are UTC, to the second; rows written
    before ``first_arrival_time`` was added read NaT there.
"""

from __future__ import annotations

import datetime as dt
import math
import os
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from typing import Iterable, Sequence

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.providers.observations.wis2_targets import (
    MISSING_DOUBLE,
    MISSING_LONG,
    ArchiveTarget,
    register,
    split_bufr,
)

STORE_PREFIX = "bkr/obs/wis2_land.icechunk"
#: Rows at the end of the store whose reports a new window is checked against.
DEDUP_ROWS = 3_000_000
#: Objects decoded per worker task.
DECODE_BATCH = 200
DECODE_WORKERS = min(16, os.cpu_count() or 1)

#: BUFR data category of land surface reports.
LAND_CATEGORY = 0
#: GTS bulletin ``T1T2A1`` designators of land surface BUFR (automatic, intermediate, main,
#: non-standard hours).
GTS_DESIGNATORS = ("I/S/A/", "I/S/I/", "I/S/M/", "I/S/N/")
#: Precipitation periods (hours) given variables of their own.
PRECIPITATION_HOURS = (1, 3, 6, 12, 24)
SUNSHINE_HOURS = (1, 24)


def wants(key: str, size: int = 0) -> bool:
    """Whether a cache object can hold land surface reports (cheap; by key alone)."""
    if "/surface-based-observations/synop/" in key:
        return True
    if "-gts-to-wis2/data/core/" in key:
        tail = key.split("/data/core/", 1)[1]
        return tail.startswith(GTS_DESIGNATORS)
    return False


# ------------------------------------------------------------------------------ schema


@dataclass(frozen=True)
class Column:
    """One stored variable: its dtype, attributes and the value a report without it has."""

    dtype: str
    attrs: dict = field(default_factory=dict)

    @property
    def missing(self):
        return -1 if self.dtype.startswith("int") else (None if self.dtype == "str" else np.nan)


def _f(units: str, long_name: str, **extra) -> Column:
    return Column("float64", {"units": units, "long_name": long_name, **extra})


def _code(long_name: str, table: str) -> Column:
    return Column("int32", {"long_name": long_name, "bufr_code_table": table, "_FillValue": -1})


COLUMNS: dict[str, Column] = {
    "station_id": Column("str", {"long_name": "WIGOS station identifier"}),
    "station_name": Column("str", {"long_name": "station or site name"}),
    "time": Column("datetime64[s]", {"long_name": "observation time"}),
    "latitude": _f("degrees_north", "latitude"),
    "longitude": _f("degrees_east", "longitude"),
    "station_elevation": _f("m", "height of station ground above mean sea level"),
    "barometer_elevation": _f("m", "height of barometer above mean sea level"),
    "station_type": _code("type of station (0 automatic, 1 manned, 2 hybrid)", "002001"),
    "surface_pressure": _f("Pa", "station level pressure"),
    "mean_sea_level_pressure": _f("Pa", "pressure reduced to mean sea level"),
    "pressure_change_3h": _f("Pa", "3-hour pressure change"),
    "pressure_tendency": _code("characteristic of pressure tendency", "010063"),
    "pressure_change_24h": _f("Pa", "24-hour pressure change"),
    "2m_temperature": _f("K", "air temperature"),
    "2m_dewpoint_temperature": _f("K", "dewpoint temperature"),
    "2m_relative_humidity": _f("%", "relative humidity"),
    "temperature_sensor_height": _f("m", "height of the temperature sensor above ground"),
    "maximum_2m_temperature": _f("K", "maximum temperature over the period"),
    "maximum_2m_temperature_period": _f("h", "period of maximum_2m_temperature"),
    "minimum_2m_temperature": _f("K", "minimum temperature over the period"),
    "minimum_2m_temperature_period": _f("h", "period of minimum_2m_temperature"),
    "10m_wind_direction": _f("degree", "wind direction"),
    "10m_wind_speed": _f("m s-1", "wind speed"),
    "10m_wind_gust": _f("m s-1", "maximum wind gust speed"),
    "10m_wind_gust_period": _f("min", "period of 10m_wind_gust"),
    "wind_sensor_height": _f("m", "height of the wind sensor above ground"),
    "horizontal_visibility": _f("m", "horizontal visibility"),
    **{
        f"total_precipitation_{h}h": _f(
            "kg m-2", f"total precipitation over the past {h} h (1 kg m-2 = 1 mm)"
        )
        for h in PRECIPITATION_HOURS
    },
    "total_cloud_cover": Column(
        "int16",
        {
            "long_name": "total cloud cover",
            "units": "(0 - 1)",
            "scale_factor": 0.01,
            "_FillValue": -1,
            "comment": "stored as the reported percentage",
        },
    ),
    "sky_obscured": Column(
        "int8",
        {"long_name": "sky obscured by fog or other phenomena (1), else -1", "_FillValue": -1},
    ),
    "low_or_middle_cloud_amount": _code("cloud amount of the lowest layer (oktas)", "020011"),
    "cloud_base_height": _f("m", "height of base of the lowest cloud"),
    "present_weather": _code("present weather", "020003"),
    "past_weather_1": _code("past weather (1)", "020004"),
    "past_weather_2": _code("past weather (2)", "020005"),
    "state_of_ground": _code("state of ground", "020062"),
    "snow_depth": _f("m", "total snow depth"),
    **{
        f"sunshine_duration_{h}h": _f("s", f"total sunshine over the past {h} h")
        for h in SUNSHINE_HOURS
    },
    "update_sequence_number": Column(
        "int16", {"long_name": "BUFR update sequence number (0 original, >0 correction)"}
    ),
    "bufr_template": Column(
        "int32", {"long_name": "first observation sequence descriptor of the message"}
    ),
    "data_sub_category": Column(
        "int16", {"long_name": "BUFR international data sub-category", "_FillValue": -1}
    ),
    "centre": Column("str", {"long_name": "WIS2 centre the copy kept was received from"}),
    # Named ``arrival_time`` in the other WIS2 stores; renaming it here would rewrite the store.
    "arrived": Column(
        "datetime64[s]",
        {
            "long_name": "time the copy of this observation kept here arrived in the WIS2 "
            "Global Cache",
        },
    ),
    "first_arrival_time": Column(
        "datetime64[s]",
        {
            "long_name": "earliest time any copy of this observation arrived in the WIS2 "
            "Global Cache",
            "comment": "minimum over the copies (GTS relays, the originating centre's topic) "
            "collapsed into this row within its window; rows written before this variable "
            "was added have no value (NaT)",
        },
    ),
}
#: The arrival times' descriptions, set on an existing store's arrays too.
ARRIVAL_ATTRS = {name: COLUMNS[name].attrs for name in ("arrived", "first_arrival_time")}

#: Element names read from each subset; everything else is skipped without a lookup.
_READ = {
    "wigosIdentifierSeries",
    "wigosIssuerOfIdentifier",
    "wigosIssueNumber",
    "wigosLocalIdentifierCharacter",
    "blockNumber",
    "stationNumber",
    "stationOrSiteName",
    "longStationName",
    "stationType",
    "year",
    "month",
    "day",
    "hour",
    "minute",
    "second",
    "latitude",
    "longitude",
    "heightOfStationGroundAboveMeanSeaLevel",
    "heightOfBarometerAboveMeanSeaLevel",
    "nonCoordinatePressure",
    "pressureReducedToMeanSeaLevel",
    "3HourPressureChange",
    "characteristicOfPressureTendency",
    "24HourPressureChange",
    "heightOfSensorAboveLocalGroundOrDeckOfMarinePlatform",
    "airTemperature",
    "dewpointTemperature",
    "relativeHumidity",
    "timePeriod",
    "maximumTemperatureAtHeightAndOverPeriodSpecified",
    "minimumTemperatureAtHeightAndOverPeriodSpecified",
    "windDirection",
    "windSpeed",
    "maximumWindGustSpeed",
    "horizontalVisibility",
    "totalPrecipitationOrTotalWaterEquivalent",
    "totalPrecipitationPast24Hours",
    "totalPrecipitationPast1Hour",
    "totalPrecipitationPast3Hours",
    "totalPrecipitationPast6Hours",
    "totalPrecipitationPast12Hours",
    "cloudCoverTotal",
    "cloudAmount",
    "heightOfBaseOfCloud",
    "presentWeather",
    "pastWeather1",
    "pastWeather2",
    "stateOfGround",
    "totalSnowDepth",
    "totalSunshine",
}

_DIRECT_PRECIPITATION = {
    "totalPrecipitationPast1Hour": 1,
    "totalPrecipitationPast3Hours": 3,
    "totalPrecipitationPast6Hours": 6,
    "totalPrecipitationPast12Hours": 12,
    "totalPrecipitationPast24Hours": 24,
}


def _missing(value) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip() or value.strip("\xff ") == ""
    if isinstance(value, (int, np.integer)):
        return int(value) == MISSING_LONG
    return not math.isfinite(value) or value <= MISSING_DOUBLE / 10


# ------------------------------------------------------------------------------ decoding


class _Report:
    """Reads one subset's elements in order, keeping the context a value depends on."""

    def __init__(self) -> None:
        self.row: dict = {}
        self.periods: list[tuple] = []  # recent (timePeriod value, its units)
        self.sensor_height: float | None = None

    def first(self, name: str, value) -> None:
        if name not in self.row and not _missing(value):
            self.row[name] = value

    def _period_hours(self, n: int = 1, minutes: bool = False) -> float | None:
        """Length of the period the last ``n`` timePeriods describe, in hours.

        A period's units come with it (0 04 024 hours, 0 04 025 minutes, ...). Where they
        could not be read, wind and gust periods are taken as minutes and others as hours,
        which is what the WMO templates use.
        """
        recent = self.periods[-n:]
        if len(recent) < n or any(_missing(v) for v, _ in recent):
            return None
        scale = {"h": 1.0, "min": 1 / 60, "s": 1 / 3600, "d": 24.0}
        default = 1 / 60 if minutes else 1.0
        hours = [float(v) * scale.get(u, default) for v, u in recent]
        return abs(hours[-1] - hours[0]) if n == 2 else abs(hours[0])

    def add(self, name: str, value, units: str = "") -> None:
        if name == "timePeriod":
            self.periods.append((value, units))
            return
        if name == "heightOfSensorAboveLocalGroundOrDeckOfMarinePlatform":
            self.sensor_height = None if _missing(value) else float(value)
            return
        if name in ("wigosIdentifierSeries", "wigosIssuerOfIdentifier", "wigosIssueNumber"):
            self.first(name, value)
        elif name == "wigosLocalIdentifierCharacter":
            self.first(name, value.strip() if isinstance(value, str) else value)
        elif name in ("blockNumber", "stationNumber", "stationType"):
            self.first(name, value)
        elif name in ("stationOrSiteName", "longStationName"):
            self.first("station_name", value.strip() if isinstance(value, str) else value)
        elif name in ("year", "month", "day", "hour", "minute", "second"):
            self.first(name, value)
        elif name == "latitude":
            self.first("latitude", value)
        elif name == "longitude":
            self.first("longitude", value)
        elif name == "heightOfStationGroundAboveMeanSeaLevel":
            self.first("station_elevation", value)
        elif name == "heightOfBarometerAboveMeanSeaLevel":
            self.first("barometer_elevation", value)
        elif name == "nonCoordinatePressure":
            self.first("surface_pressure", value)
        elif name == "pressureReducedToMeanSeaLevel":
            self.first("mean_sea_level_pressure", value)
        elif name == "3HourPressureChange":
            self.first("pressure_change_3h", value)
        elif name == "characteristicOfPressureTendency":
            self.first("pressure_tendency", value)
        elif name == "24HourPressureChange":
            self.first("pressure_change_24h", value)
        elif name == "airTemperature":
            # Some templates also carry temperatures near the ground (5 cm); keep the first
            # at screen height (1-3 m), else the first reported.
            if _missing(value):
                return
            held = self.row.get("temperature_sensor_height")
            screen = self.sensor_height is not None and 1 <= self.sensor_height <= 3
            if "2m_temperature" not in self.row or (
                screen and held is not None and not 1 <= held <= 3
            ):
                self.row["2m_temperature"] = value
                if self.sensor_height is not None:
                    self.row["temperature_sensor_height"] = self.sensor_height
        elif name == "dewpointTemperature":
            self.first("2m_dewpoint_temperature", value)
        elif name == "relativeHumidity":
            self.first("2m_relative_humidity", value)
        elif name in (
            "maximumTemperatureAtHeightAndOverPeriodSpecified",
            "minimumTemperatureAtHeightAndOverPeriodSpecified",
        ):
            which = "maximum" if name.startswith("maximum") else "minimum"
            if f"{which}_2m_temperature" not in self.row and not _missing(value):
                self.row[f"{which}_2m_temperature"] = value
                # SYNOP (3 02 041) gives the period as a start and an end (-12 h, 0 h);
                # the automatic-station templates give one period (-60 min).
                pair = (
                    len(self.periods) >= 2
                    and not _missing(self.periods[-1][0])
                    and self.periods[-1][0] == 0
                )
                period = self._period_hours(2 if pair else 1)
                if period is not None:
                    self.row[f"{which}_2m_temperature_period"] = period
        elif name == "windDirection":
            self.first("10m_wind_direction", value)
        elif name == "windSpeed":
            if "10m_wind_speed" not in self.row and not _missing(value):
                self.row["10m_wind_speed"] = value
                if self.sensor_height is not None:
                    self.row["wind_sensor_height"] = self.sensor_height
        elif name == "maximumWindGustSpeed":
            if "10m_wind_gust" not in self.row and not _missing(value):
                self.row["10m_wind_gust"] = value
                period = self._period_hours(1, minutes=True)
                if period is not None:
                    self.row["10m_wind_gust_period"] = period * 60
        elif name == "horizontalVisibility":
            self.first("horizontal_visibility", value)
        elif name in _DIRECT_PRECIPITATION:
            self.first(f"total_precipitation_{_DIRECT_PRECIPITATION[name]}h", value)
        elif name == "totalPrecipitationOrTotalWaterEquivalent":
            period = self._period_hours(1)
            if period is not None and not _missing(value):
                hours = int(round(period))
                if hours in PRECIPITATION_HOURS and abs(period - hours) < 1e-6:
                    self.first(f"total_precipitation_{hours}h", value)
        elif name == "cloudCoverTotal":
            # 0-100 %, or 113 "sky obscured by fog or other phenomena" (code table 0 20 010).
            if not _missing(value) and "total_cloud_cover" not in self.row:
                if value == 113:
                    self.row["sky_obscured"] = 1
                elif 0 <= value <= 100:
                    self.row["total_cloud_cover"] = value
        elif name == "cloudAmount":
            self.first("low_or_middle_cloud_amount", value)
        elif name == "heightOfBaseOfCloud":
            self.first("cloud_base_height", value)
        elif name == "presentWeather":
            self.first("present_weather", value)
        elif name == "pastWeather1":
            self.first("past_weather_1", value)
        elif name == "pastWeather2":
            self.first("past_weather_2", value)
        elif name == "stateOfGround":
            self.first("state_of_ground", value)
        elif name == "totalSnowDepth":
            self.first("snow_depth", value)
        elif name == "totalSunshine":
            period = self._period_hours(1)
            if period is not None and not _missing(value):
                hours = int(round(period))
                if hours in SUNSHINE_HOURS and abs(period - hours) < 1e-6:
                    # Reported in minutes; ERA5's sunshine duration is in seconds.
                    self.first(f"sunshine_duration_{hours}h", float(value) * 60)

    def finish(self, typical: tuple | None = None) -> dict | None:
        """The report as a row, or None for a NIL report or one with no station or time.

        A subset without a date takes the message's (section 1) date, with its own hour
        and minute where it has them. One without a position is kept (the station
        identifies it); :meth:`LandTarget.finish` fills the position from another report
        of the station where one has it.
        """
        r = self.row
        date = [r.get(k) for k in ("year", "month", "day")]
        if None in date and typical:
            date = list(typical[:3])
        try:
            when = dt.datetime(
                int(date[0]),
                int(date[1]),
                int(date[2]),
                int(r.get("hour", typical[3] if typical and "year" not in r else 0)),
                int(r.get("minute", typical[4] if typical and "year" not in r else 0)),
                int(r.get("second", 0)),
            )
        except (TypeError, ValueError, OverflowError):
            return None
        station = _station_id(r)
        if station is None:
            return None
        row = {k: v for k, v in r.items() if k in COLUMNS}
        if not any(name in row for name in _VALUES):
            return None  # a NIL report: the station, and nothing observed
        row["station_id"] = station
        row["time"] = np.datetime64(when, "s")
        return row


#: Columns a report must have one of to be more than NIL.
_VALUES = (
    "surface_pressure",
    "mean_sea_level_pressure",
    "2m_temperature",
    "2m_dewpoint_temperature",
    "2m_relative_humidity",
    "10m_wind_speed",
    "10m_wind_direction",
    "horizontal_visibility",
    "total_cloud_cover",
    "present_weather",
    "snow_depth",
    *(f"total_precipitation_{h}h" for h in PRECIPITATION_HOURS),
    "maximum_2m_temperature",
    "minimum_2m_temperature",
    "10m_wind_gust",
    *(f"sunshine_duration_{h}h" for h in SUNSHINE_HOURS),
)


def _station_id(r: dict) -> str | None:
    local = r.get("wigosLocalIdentifierCharacter")
    if local:
        series = int(r.get("wigosIdentifierSeries", 0))
        issuer = int(r.get("wigosIssuerOfIdentifier", 20000))
        return f"{series}-{issuer}-{int(r.get('wigosIssueNumber', 0))}-{local}"
    if "blockNumber" in r and "stationNumber" in r:
        return f"0-20000-0-{int(r['blockNumber']):02d}{int(r['stationNumber']):03d}"
    return None


def _value(eccodes, h, key: str):
    """One key's values: an array for a compressed message, else a scalar."""
    if eccodes.codes_get_native_type(h, key) is str:
        if eccodes.codes_get_size(h, key) > 1:
            return eccodes.codes_get_string_array(h, key)
        return eccodes.codes_get_string(h, key)
    return eccodes.codes_get_array(h, key)


def _units(eccodes, h, key: str) -> str:
    try:
        return eccodes.codes_get(h, f"{key}->units")
    except eccodes.CodesInternalError:
        return ""


def _all_values(eccodes, h, name: str) -> list:
    """Every occurrence of an element in an uncompressed message, in iteration order."""
    if eccodes.codes_get_native_type(h, name) is str:
        return list(eccodes.codes_get_string_array(h, name))
    return [v.item() for v in eccodes.codes_get_array(h, name)]


def _subsets(eccodes, h) -> list[_Report]:
    """The reports in one unpacked message, compressed or not.

    One pass over the key names finds which elements each subset holds and in what
    order. An uncompressed message then reads each element once, as the array of all its
    occurrences (looking keys up one ``#rank#`` at a time is quadratic in the subsets: a
    2,000-station message would take minutes). A compressed message has one structure
    for every subset, so each key's array runs across the subsets.
    """
    subsets = eccodes.codes_get(h, "numberOfSubsets")
    compressed = bool(eccodes.codes_get(h, "compressedData"))
    it = eccodes.codes_bufr_keys_iterator_new(h)
    order: list[list[str]] = []  # per subset (uncompressed), the element names read
    keys: list[tuple[str, str]] = []  # compressed: (key, name)
    try:
        while eccodes.codes_bufr_keys_iterator_next(it):
            key = eccodes.codes_bufr_keys_iterator_get_name(it)
            if key == "subsetNumber" and not compressed:
                order.append([])
                continue
            if not key.startswith("#"):
                continue
            name = key.split("#", 2)[2]
            if name not in _READ:
                continue
            if compressed:
                keys.append((key, name))
            elif order:
                order[-1].append(name)
    finally:
        eccodes.codes_bufr_keys_iterator_delete(it)

    if compressed:
        reports = [_Report() for _ in range(subsets)]
        for key, name in keys:
            try:
                value = _value(eccodes, h, key)
            except eccodes.CodesInternalError:
                continue
            values = list(value) if np.ndim(value) else [value]
            if len(values) == 1:
                values = values * subsets
            units = _units(eccodes, h, key) if name == "timePeriod" else ""
            for report, one in zip(reports, values):
                report.add(name, one.item() if hasattr(one, "item") else one, units)
        return reports

    columns: dict[str, list] = {}
    for name in {n for names in order for n in names}:
        try:
            columns[name] = _all_values(eccodes, h, name)
        except eccodes.CodesInternalError:
            columns[name] = []
    taken = dict.fromkeys(columns, 0)
    # Subsets laid out alike share their periods' units: read them once per layout, from
    # the first subset with it.
    layouts: dict[tuple, list[str]] = {}
    reports = []
    for names in order:
        layout = tuple(names)
        if layout not in layouts:
            first = taken.get("timePeriod", 0)
            count = layout.count("timePeriod")
            layouts[layout] = [
                _units(eccodes, h, f"#{first + k + 1}#timePeriod") for k in range(count)
            ]
        units = iter(layouts[layout])
        report = _Report()
        for name in names:
            index = taken[name]
            taken[name] += 1
            if index < len(columns[name]):
                report.add(
                    name, columns[name][index], next(units, "") if name == "timePeriod" else ""
                )
            elif name == "timePeriod":
                next(units, "")
        reports.append(report)
    return reports


@dataclass
class Decoded:
    rows: list[dict]
    messages: int = 0
    other_category: int = 0
    failed: int = 0
    incomplete: int = 0


def decode(body: bytes, centre: str = "", arrived: dt.datetime | None = None) -> Decoded:
    """Every land surface report in one object (one or more BUFR messages).

    A message that fails to decode is counted and skipped, never raised.
    """
    import eccodes

    out = Decoded(rows=[])
    for message in split_bufr(body):
        out.messages += 1
        try:
            h = eccodes.codes_new_from_message(message)
        except Exception:  # noqa: BLE001 - malformed input of any kind
            h = None
        if h is None:
            out.failed += 1
            continue
        try:
            if eccodes.codes_get(h, "dataCategory") != LAND_CATEGORY:
                out.other_category += 1
                continue
            usn = eccodes.codes_get(h, "updateSequenceNumber")
            sub = eccodes.codes_get(h, "internationalDataSubCategory")
            eccodes.codes_set(h, "unpack", 1)
            descriptors = eccodes.codes_get_array(h, "unexpandedDescriptors")
            template = next(
                (int(d) for d in descriptors if 300000 <= d < 400000 and d != 301150), 0
            )
            stamp = np.datetime64(arrived.replace(tzinfo=None), "s") if arrived else None
            typical = tuple(
                eccodes.codes_get(h, k)
                for k in (
                    "typicalYear",
                    "typicalMonth",
                    "typicalDay",
                    "typicalHour",
                    "typicalMinute",
                )
            )
            for report in _subsets(eccodes, h):
                row = report.finish(typical)
                if row is None:
                    out.incomplete += 1
                    continue
                row.update(
                    update_sequence_number=int(usn),
                    bufr_template=template,
                    data_sub_category=int(sub) if sub != 255 else -1,
                    centre=centre,
                    arrived=stamp,
                )
                out.rows.append(row)
        except Exception:  # noqa: BLE001 - ecCodes raises many types on bad data
            out.failed += 1
        finally:
            eccodes.codes_release(h)
    return out


def _decode_batch(batch: Sequence[tuple[str, bytes, dt.datetime | None]]) -> Decoded:
    total = Decoded(rows=[])
    for key, body, arrived in batch:
        centre = key.split("/")[1] if key.startswith("data/") else ""
        one = decode(body, centre, arrived)
        total.rows += one.rows
        total.messages += one.messages
        total.other_category += one.other_category
        total.failed += one.failed
        total.incomplete += one.incomplete
    return total


# ------------------------------------------------------------------------------ dataset


def fill_positions(rows: Sequence[dict]) -> int:
    """Give reports without a position their station's from another report. Returns how many."""
    known = {}
    for row in rows:
        if row.get("latitude") is not None and row.get("longitude") is not None:
            known.setdefault(row["station_id"], row)
    filled = 0
    for row in rows:
        if row.get("latitude") is None or row.get("longitude") is None:
            other = known.get(row["station_id"])
            if other is not None:
                for name in ("latitude", "longitude", "station_elevation", "station_name"):
                    if row.get(name) is None and other.get(name) is not None:
                        row[name] = other[name]
                filled += 1
    return filled


def _completeness(row: dict) -> int:
    return sum(
        1 for v in row.values() if v is not None and not (isinstance(v, float) and math.isnan(v))
    )


def identity(station: str, time: np.datetime64, usn: int) -> tuple:
    return (station, np.datetime64(time, "s").astype("int64").item(), int(usn))


def deduplicate(rows: Iterable[dict], seen: set[tuple] = frozenset()) -> list[dict]:
    """One row per report identity, the most complete copy, none already in ``seen``.

    Each row returned also has ``first_arrival_time``, the earliest ``arrived`` among the
    copies collapsed into it.
    """
    best: dict[tuple, dict] = {}
    first_arrival: dict[tuple, np.datetime64] = {}
    for row in rows:
        ident = identity(row["station_id"], row["time"], row["update_sequence_number"])
        if ident in seen:
            continue
        held = best.get(ident)
        if held is None or _completeness(row) > _completeness(held):
            best[ident] = row
        arrived, earliest = row.get("arrived"), first_arrival.get(ident)
        if (
            arrived is not None
            and not np.isnat(arrived)
            and (earliest is None or arrived < earliest)
        ):
            first_arrival[ident] = arrived
    kept = [{**row, "first_arrival_time": first_arrival.get(ident)} for ident, row in best.items()]
    return sorted(kept, key=lambda r: (r["time"], r["station_id"]))


def to_dataset(rows: Sequence[dict]) -> xr.Dataset:
    """Rows as the store's variables along ``observation``, every column present."""
    data = {}
    for name, column in COLUMNS.items():
        values = [r.get(name) for r in rows]
        if column.dtype == "str":
            array = np.array([v if v is not None else "" for v in values], dtype=object)
        elif column.dtype.startswith("datetime64"):
            array = np.array(
                [v if v is not None else np.datetime64("NaT") for v in values], dtype=column.dtype
            )
        else:
            missing = column.missing
            array = np.array([missing if v is None else v for v in values], dtype=column.dtype)
        attrs = dict(column.attrs)
        if column.dtype.startswith("datetime64"):
            attrs = {"long_name": attrs.get("long_name", name)} | (
                {"comment": attrs["comment"]} if "comment" in attrs else {}
            )
        data[name] = xr.Variable(("observation",), array, attrs)
    ds = xr.Dataset(data)
    ds.attrs = {
        "title": "WIS2 Global Cache land surface station reports",
        "source": "WMO WIS2 Global Cache (s3://wis2globalcache), BUFR category 0",
        "deduplication": (
            "one row per (station_id, time, update_sequence_number); corrections are kept "
            "as further rows with a higher update_sequence_number"
        ),
    }
    return ds


# ------------------------------------------------------------------------------ target


@register("land")
class LandTarget(ArchiveTarget):
    """Archive target: a window's land surface reports into :data:`STORE_PREFIX`.

    :meth:`feed` only keeps the bytes (a window's land objects come to ~100 MB); they are
    decoded at :meth:`finish` over a process pool, so decoding never competes with the
    tar stream for the interpreter.
    """

    store_prefix = STORE_PREFIX

    def __init__(self, config=None, workers: int = DECODE_WORKERS):
        super().__init__(config)
        self.workers = workers
        self.pending: list[tuple[str, bytes, dt.datetime | None]] = []

    def wants(self, key: str, size: int = 0) -> bool:
        return wants(key, size)

    def feed(self, key: str, body: bytes, arrived: dt.datetime | None = None) -> None:
        """Keep the object for decoding at :meth:`finish`; never raises."""
        try:
            self.pending.append((key, bytes(body), arrived))
        except Exception as exc:  # noqa: BLE001 - a target must never break the archive
            logger.warning(f"wis2 land: could not keep {key}: {exc}")

    def decode_pending(self) -> Decoded:
        """Decode everything fed so far, over a process pool for large windows."""
        pending, self.pending = self.pending, []
        batches = [pending[i : i + DECODE_BATCH] for i in range(0, len(pending), DECODE_BATCH)]
        total = Decoded(rows=[])
        if self.workers <= 1 or len(batches) <= 1:
            results = [_decode_batch(b) for b in batches]
        else:
            with ProcessPoolExecutor(self.workers) as pool:
                results = list(pool.map(_decode_batch, batches))
        for one in results:
            total.rows += one.rows
            total.messages += one.messages
            total.other_category += one.other_category
            total.failed += one.failed
            total.incomplete += one.incomplete
        return total

    def _repo(self):
        from planetary_datasets.config import get_config

        return (self.config or get_config()).icechunk_repo(self.store_prefix)

    def stored_identities(self, repo) -> set[tuple]:
        """Identities of the reports in the store's last :data:`DEDUP_ROWS` rows."""
        import zarr

        try:
            group = zarr.open_group(repo.readonly_session("main").store, mode="r")
            rows = group["station_id"].shape[0]
        except Exception:  # noqa: BLE001 - no store yet
            return set()
        start = max(0, rows - DEDUP_ROWS)
        stations = group["station_id"][start:rows]
        sequence = group["update_sequence_number"][start:rows]
        # Times decoded by xarray (whatever units it stored them in), lazily: only the
        # tail is read.
        others = [n for n in group.array_keys() if n != "time"]
        times = (
            xr.open_zarr(
                repo.readonly_session("main").store,
                consolidated=False,
                chunks={},
                drop_variables=others,
            )["time"]
            .isel(observation=slice(start, rows))
            .values.astype("datetime64[s]")
            .astype("int64")
        )
        return {(str(s), int(t), int(u)) for s, t, u in zip(stations, times, sequence)}

    def finish(
        self,
        window_start: dt.datetime | None,
        window_end: dt.datetime | None,
        partition: str,
    ) -> dict:
        """Decode, deduplicate and append the window's reports. Returns a summary."""
        from planetary_datasets.common.multidim import append_partition, describe_variables

        objects = len(self.pending)
        decoded = self.decode_pending()
        fill_positions(decoded.rows)
        repo = self._repo()
        rows = deduplicate(decoded.rows, self.stored_identities(repo))
        written = False
        if rows:
            written = append_partition(repo, to_dataset(rows), partition, label="wis2_land")
            describe_variables(repo, ARRIVAL_ATTRS, label="wis2_land")
        return {
            "objects": objects,
            "messages": decoded.messages,
            "other_category_messages": decoded.other_category,
            "undecodable_messages": decoded.failed,
            "skipped_reports": decoded.incomplete,  # NIL, or no station or time
            "reports": len(decoded.rows),
            "rows_written": len(rows) if written else 0,
        }


def rows_frame(rows: Sequence[dict]) -> pd.DataFrame:
    """Rows as a DataFrame, for inspection."""
    return pd.DataFrame(rows)
