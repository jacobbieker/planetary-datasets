"""Wind profiler and radar wind profiles (GTS ``IUP*``) for the WIS2 upper-air store.

Part of the ``upper_air`` archive target
(:mod:`planetary_datasets.providers.observations.wis2_upper_air`): the GTS relays' BUFR
bulletins with T1T2A1 = ``IUP`` (wind profiles) are decoded here and written to their own
group, ``wind_profiler``, of ``bkr/obs/wis2_upper_air.icechunk``. The soundings at the root
of the store are untouched.

Seen in the cache (objects per 12-hour window, both relays together)::

    IUPA EGRR  ~2,300  UK profilers, BUFR category 6 (radar), 3-01-022 + 0-07-007 levels
    IUPA LPMG     ~70  Portugal, the same with frequency and pulse details
    IUPC RJTD    ~240  JMA WINDAS: category 2/10, 3 stations x 18 ten-minute profiles,
                       u/v/w and signal-to-noise per height above the station
    IUPC VHHH     ~45  Hong Kong, category 2/10 in the PILOT template 3-09-051
    IUPD EDZW  ~4,300  German radar VAD winds, category 6
    IUPD EGRR  ~1,800  UK profilers, category 2, 3-21-021/3-21-022 (with quality fields)
    IUPD ENMI  ~1,200  Norway, category 6;  IUPD EFKL, LSSW, LFPW (<50 each) alike
    IUPK AMMC  ~1,100  Australian profilers, category 2/10, 3-21-022-like levels
    IUPK NZKL     ~40  New Zealand, category 6
    IUPN LFPW    ~290  French profilers, category 2, 3-21-021/3-21-022

Each bulletin is relayed by both the DWD and JMA gateways; many radar bulletins are empty
(no levels reported) and are counted, not stored.

Layout (``xr.open_zarr(store, group="wind_profiler", consolidated=False)``)::

    profile (one row per station and observation time): profile_id, station_id,
        wmo_station_id, wigos_station_id, short_station_name, time, time_significance,
        time_period, latitude, longitude, station_elevation, station_type,
        measuring_equipment_type, antenna_type, beamwidth, mean_frequency,
        range_gate_length, pulse_repetition_frequency, number_of_integrated_pulses,
        mean_speed_estimation, wind_computation_enhancement, data_category,
        data_sub_category, local_data_sub_category, bufr_template, originating_centre,
        bulletin_heading, source_key, source_centre, arrival_time, first_arrival_time,
        first_level, level_count
    profile_level (one row per range gate): profile_index, height, height_above_station,
        geopotential_height, wind_direction, wind_speed, eastward_wind, northward_wind,
        upward_air_velocity, signal_to_noise_ratio, wind_quality,
        wind_quality_significance, vertical_wind_quality, vertical_wind_quality_significance

Heights are as reported: ``height`` above mean sea level (0 07 007), or
``height_above_station`` (0 07 006, JMA and Australia), or ``geopotential_height`` (the
PILOT template); the others read NaN. Values are the BUFR values in SI units, stored
losslessly as in the soundings (each float records its ``decimal_places``).

A profile's identity is its station, time, averaging period and a fingerprint of its
levels, so the relayed copies, and the overlapping ten-minute profiles of consecutive
WINDAS bulletins, collapse to one row while profiles of different modes (different gates)
and corrected re-issues are each kept.
"""

from __future__ import annotations

import dataclasses
import hashlib
import re
from collections import Counter
from typing import Iterable, Sequence

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.config import Config
from planetary_datasets.providers.observations import wis2_upper_air as ua
from planetary_datasets.providers.observations.wis2_targets import clean, split_bufr

#: The group of the upper-air store holding the profiles.
GROUP = "wind_profiler"
#: GTS BUFR wind-profile bulletins (T1T2A1 = IUP), any A2.
GTS_PROFILER_PATH = re.compile(r"/data/core/I/U/P/[A-Z]/")
#: BUFR Table A categories profiles come in: vertical soundings (2) and radar (6).
PROFILE_CATEGORIES = (2, 6)
#: Templates of the sounding family (3-09-050 to 3-09-057), decoded as soundings are.
SOUNDING_TEMPLATES = range(309050, 309058)

#: How many stored profile ids to read back to skip ones already stored.
RECENT_IDS = 200_000

#: ecCodes element -> profile (header) variable.
HEADER_KEYS = {
    "shortStationName": "short_station_name",
    "latitude": "latitude",
    "longitude": "longitude",
    "heightOfStation": "station_elevation",
    "heightOfStationGroundAboveMeanSeaLevel": "station_elevation",
    "stationType": "station_type",
    "measuringEquipmentType": "measuring_equipment_type",
    "antennaType": "antenna_type",
    "3DbBeamwidth": "beamwidth",
    "meanFrequency": "mean_frequency",
    "rangeGateLength": "range_gate_length",
    "pulseRepetitionFrequency": "pulse_repetition_frequency",
    "numberOfIntegratedPulses": "number_of_integrated_pulses",
    "meanSpeedEstimation": "mean_speed_estimation",
    "windComputationEnhancement": "wind_computation_enhancement",
    "timeSignificance": "time_significance",
}
TIME_KEYS = ("year", "month", "day", "hour", "minute", "second")
#: ecCodes element starting a level -> its height variable.
LEVEL_START = {
    "height": "height",
    "heightAboveStation": "height_above_station",
}
#: ecCodes element -> level variable.
LEVEL_KEYS = {
    "windDirection": "wind_direction",
    "windSpeed": "wind_speed",
    "u": "eastward_wind",
    "v": "northward_wind",
    "w": "upward_air_velocity",
    "signalToNoiseRatio": "signal_to_noise_ratio",
}
#: Level elements whose quality (0 33 002 after them, or an associated field) is the
#: horizontal wind's; ``w``'s is the vertical wind's.
HORIZONTAL = {"windDirection", "windSpeed", "u", "v"}
#: Station identity elements, kept for every profile of a subset.
STATION_KEYS = (
    "blockNumber",
    "stationNumber",
    "wigosIdentifierSeries",
    "wigosIssuerOfIdentifier",
    "wigosIssueNumber",
    "wigosLocalIdentifierCharacter",
    "shipOrMobileLandStationIdentifier",
)
REPLICATION_KEYS = (
    "delayedDescriptorReplicationFactor",
    "extendedDelayedDescriptorReplicationFactor",
)

FLOAT_LEVELS = (
    "height",
    "height_above_station",
    "geopotential_height",
    "wind_direction",
    "wind_speed",
    "eastward_wind",
    "northward_wind",
    "upward_air_velocity",
    "signal_to_noise_ratio",
)
INT_LEVELS = (
    "wind_quality",
    "wind_quality_significance",
    "vertical_wind_quality",
    "vertical_wind_quality_significance",
)

#: Decimal places of each float: at least the BUFR element's scale (see the soundings).
PLACES = {
    "height": 0,
    "height_above_station": 0,
    "geopotential_height": 1,
    "wind_direction": 0,
    "wind_speed": 1,
    "eastward_wind": 1,
    "northward_wind": 1,
    "upward_air_velocity": 2,
    "signal_to_noise_ratio": 0,
    "latitude": 5,
    "longitude": 5,
    "station_elevation": 1,
    "beamwidth": 1,
    "mean_frequency": 0,
    "range_gate_length": 0,
    "pulse_repetition_frequency": 0,
}

LEVEL_ATTRS = {
    "profile_index": {"long_name": "row of this level's profile along the profile dimension"},
    "height": {"long_name": "height above mean sea level (BUFR 0 07 007)", "units": "m"},
    "height_above_station": {"long_name": "height above station (BUFR 0 07 006)", "units": "m"},
    "geopotential_height": {
        "long_name": "geopotential height (BUFR 0 07 009, PILOT template)",
        "units": "m",
    },
    "wind_direction": {"long_name": "wind direction (from)", "units": "degree"},
    "wind_speed": {"long_name": "wind speed", "units": "m s-1"},
    "eastward_wind": {
        "long_name": "u-component of wind",
        "standard_name": "eastward_wind",
        "units": "m s-1",
    },
    "northward_wind": {
        "long_name": "v-component of wind",
        "standard_name": "northward_wind",
        "units": "m s-1",
    },
    "upward_air_velocity": {
        "long_name": "w-component of wind (vertical velocity)",
        "standard_name": "upward_air_velocity",
        "units": "m s-1",
    },
    "signal_to_noise_ratio": {"long_name": "signal to noise ratio", "units": "dB"},
    "wind_quality": {
        "long_name": "quality of the horizontal wind: BUFR 0 33 002 (0 not suspect, "
        "1 suspect, 3 not checked) or the associated field on wind direction",
        "_FillValue": -1,
    },
    "wind_quality_significance": {
        "long_name": "associated field significance of wind_quality (BUFR 0 31 021); "
        "-1 where wind_quality is a 0 33 002 quality information",
        "_FillValue": -1,
    },
    "vertical_wind_quality": {
        "long_name": "quality of the vertical wind: BUFR 0 33 002 or the associated field on w",
        "_FillValue": -1,
    },
    "vertical_wind_quality_significance": {
        "long_name": "associated field significance of vertical_wind_quality (BUFR 0 31 021)",
        "_FillValue": -1,
    },
}
PROFILE_ATTRS = {
    "profile_id": {
        "long_name": "station|time|time period|level fingerprint (dedup identity)",
    },
    "station_id": {
        "long_name": "WMO block and station, else WIGOS id, else short station name, else "
        "ship/mobile id"
    },
    "wmo_station_id": {"long_name": "WMO block and station number (IIiii)"},
    "wigos_station_id": {"long_name": "WIGOS station identifier"},
    "short_station_name": {"long_name": "short station or site name (BUFR 0 01 018)"},
    "time": {"long_name": "observation time (UTC), as reported"},
    "time_significance": {
        "long_name": "time significance (BUFR 0 08 021; 2 = time averaged)",
        "_FillValue": -1,
    },
    "time_period": {
        "long_name": "time period or displacement (BUFR 0 04 025), e.g. the averaging period "
        "ending at time when negative",
        "units": "s",
        "_FillValue": np.iinfo("int32").min,
    },
    "latitude": {"long_name": "station latitude", "units": "degrees_north"},
    "longitude": {"long_name": "station longitude", "units": "degrees_east"},
    "station_elevation": {"long_name": "height of station above mean sea level", "units": "m"},
    "station_type": {"long_name": "type of station (BUFR 0 02 001)", "_FillValue": -1},
    "measuring_equipment_type": {
        "long_name": "type of measuring equipment used (BUFR 0 02 003)",
        "_FillValue": -1,
    },
    "antenna_type": {"long_name": "type of antenna (BUFR 0 02 101)", "_FillValue": -1},
    "beamwidth": {"long_name": "3-dB beamwidth (BUFR 0 02 106)", "units": "degree"},
    "mean_frequency": {"long_name": "mean frequency (BUFR 0 02 121)", "units": "Hz"},
    "range_gate_length": {"long_name": "range-gate length (BUFR 0 25 001)", "units": "m"},
    "pulse_repetition_frequency": {
        "long_name": "pulse repetition frequency (BUFR 0 02 125)",
        "units": "Hz",
    },
    "number_of_integrated_pulses": {
        "long_name": "number of integrated pulses (BUFR 0 25 003)",
        "_FillValue": -1,
    },
    "mean_speed_estimation": {
        "long_name": "mean speed estimation (BUFR 0 25 020)",
        "_FillValue": -1,
    },
    "wind_computation_enhancement": {
        "long_name": "wind computation enhancement (BUFR 0 25 021 flag table)",
        "_FillValue": -1,
    },
    "data_category": {"long_name": "BUFR data category (Table A; 2 soundings, 6 radar)"},
    "data_sub_category": {
        "long_name": "international data sub-category (BUFR Common Code Table C-13); "
        "-1 in BUFR edition 3",
        "_FillValue": -1,
    },
    "local_data_sub_category": {"long_name": "local data sub-category", "_FillValue": -1},
    "bufr_template": {"long_name": "BUFR unexpanded descriptors"},
    "originating_centre": {"long_name": "BUFR originating centre (Common Code Table C-11)"},
    "bulletin_heading": {"long_name": "GTS abbreviated heading TTAAiiCCCCYYGGgg[BBB]"},
    "source_key": {"long_name": "object key in s3://wis2globalcache"},
    "source_centre": {"long_name": "WIS2 centre the object came through"},
    "arrival_time": dict(ua.SOUNDING_ATTRS["arrival_time"]),
    "first_arrival_time": {
        "long_name": ua.SOUNDING_ATTRS["first_arrival_time"]["long_name"],
        "comment": "minimum over the copies (the DWD and JMA GTS relays) collapsed into "
        "this row within its window",
    },
    "first_level": {"long_name": "row of this profile's first level along profile_level"},
    "level_count": {"long_name": "number of levels"},
}

PROFILE_KINDS = {
    "profile_id": "str",
    "station_id": "str",
    "wmo_station_id": "str",
    "wigos_station_id": "str",
    "short_station_name": "str",
    "time": "time",
    "time_significance": "int",
    "time_period": "int",
    "latitude": "float",
    "longitude": "float",
    "station_elevation": "float",
    "station_type": "int",
    "measuring_equipment_type": "int",
    "antenna_type": "int",
    "beamwidth": "float",
    "mean_frequency": "float",
    "range_gate_length": "float",
    "pulse_repetition_frequency": "float",
    "number_of_integrated_pulses": "int",
    "mean_speed_estimation": "int",
    "wind_computation_enhancement": "int",
    "data_category": "int",
    "data_sub_category": "int",
    "local_data_sub_category": "int",
    "bufr_template": "str",
    "originating_centre": "int",
    "bulletin_heading": "str",
    "source_key": "str",
    "source_centre": "str",
    "arrival_time": "time",
    "first_arrival_time": "time",
}


def wanted(key: str) -> bool:
    """Whether an object of the cache is a GTS wind-profile bulletin (by its key alone)."""
    return GTS_PROFILER_PATH.search(key) is not None and not key.endswith(".txt")


@dataclasses.dataclass
class Profile:
    """One station's winds at one time: header values and its levels."""

    header: dict
    levels: dict[str, np.ndarray]

    @property
    def level_count(self) -> int:
        """Number of levels (range gates)."""
        return len(self.levels["wind_speed"])

    def fingerprint(self) -> str:
        """A short hash of every float level column."""
        digest = hashlib.sha1()
        for name in FLOAT_LEVELS:
            digest.update(np.nan_to_num(self.levels[name], nan=-1e9).astype("<f8").tobytes())
        return digest.hexdigest()[:16]

    @property
    def identity(self) -> str:
        """Station, time, time period and level fingerprint: the dedup identity."""
        when = self.header.get("time")
        stamp = "NaT" if when is None else pd.Timestamp(when).strftime("%Y%m%dT%H%M%S")
        return (
            f"{self.header['station_id']}|{stamp}|{self.header.get('time_period')}|"
            f"{self.fingerprint()}"
        )


# --------------------------------------------------------------------------- decoding


def _station_id(header: dict) -> str:
    return (
        header.get("wmo_station_id")
        or header.get("wigos_station_id")
        or header.get("short_station_name")
        or header.get("ship_or_mobile_station_id")
        or f"{header.get('latitude')}_{header.get('longitude')}"
    )


class _Walker:
    """Builds profiles from a message's data elements, in the order the template has them.

    A subset starts at the template's first element; a profile at a date after levels
    (JMA's WINDAS bulletins repeat date and levels within a subset); a level at a height
    after a replication factor. Quality information (0 33 002) and associated fields
    belong to the horizontal wind after wind direction or speed and to the vertical wind
    after ``w``.
    """

    def __init__(self, base: dict):
        self.base = base
        self.profiles: list[Profile] = []
        self.station: dict = {}
        self.header: dict = {}
        self.rows: list[dict] = []
        self.replicated = False
        self.last = ""

    def close(self) -> None:
        if self.rows:
            self.profiles.append(_profile({**self.base, **self.header}, self.rows))
        self.rows = []

    def new_subset(self) -> None:
        self.close()
        self.station, self.header, self.replicated, self.last = {}, {}, False, ""

    def take(self, name: str, value) -> None:
        if name in REPLICATION_KEYS:
            self.replicated = True
            return
        if "->" in name:
            self._associated(name, value)
            return
        if name in TIME_KEYS or name in ("timePeriod", "timeSignificance"):
            if self.rows:
                if name != "year":
                    return  # a per-level time in a template this walker does not expect
                self.close()
                self.header = dict(self.station)
            self.header[name] = value
            return
        if name in LEVEL_START and self.replicated:
            self.rows.append({LEVEL_START[name]: value})
            self.last = name
            return
        if self.rows and name in LEVEL_KEYS:
            self.rows[-1].setdefault(LEVEL_KEYS[name], value)
            self.last = name
            return
        if self.rows and name == "qualityInformation":
            column = "wind_quality" if self.last in HORIZONTAL else "vertical_wind_quality"
            self.rows[-1].setdefault(column, value)
            return
        if not self.rows and (name in HEADER_KEYS or name in STATION_KEYS):
            self.header.setdefault(name, value)
            self.station.setdefault(name, value)

    def _associated(self, name: str, value) -> None:
        if not self.rows:
            return
        element, _, rest = name.partition("->")
        column = "wind_quality" if element in HORIZONTAL else "vertical_wind_quality"
        if element not in HORIZONTAL and element != "w":
            return
        if rest == "associatedField":
            self.rows[-1].setdefault(column, value)
        elif rest == "associatedField->associatedFieldSignificance":
            self.rows[-1].setdefault(f"{column}_significance", value)


def _round(value, name: str):
    return None if value is None else round(float(value), PLACES[name])


def _integer(value) -> int | None:
    return None if value is None else int(value)


def _time(header: dict) -> np.datetime64 | None:
    parts = [header.get(k) for k in TIME_KEYS[:5]]
    if any(p is None for p in parts):
        return None
    try:
        return np.datetime64(
            pd.Timestamp(*(int(p) for p in parts), int(header.get("second") or 0)), "s"
        )
    except (ValueError, OverflowError):
        return None


def _profile(raw: dict, rows: list[dict]) -> Profile:
    """A profile from the walker's raw header (ecCodes names) and level rows."""
    block, station = raw.get("blockNumber"), raw.get("stationNumber")
    wmo = f"{int(block):02d}{int(station):03d}" if block is not None and station is not None else ""
    wigos = ""
    if raw.get("wigosIdentifierSeries") is not None:
        wigos = "-".join(
            str(v)
            for v in (
                _integer(raw["wigosIdentifierSeries"]),
                _integer(raw.get("wigosIssuerOfIdentifier")),
                _integer(raw.get("wigosIssueNumber")),
                raw.get("wigosLocalIdentifierCharacter") or "",
            )
        )
    header = {k: v for k, v in raw.items() if k in PROFILE_KINDS}
    header.update(
        {
            "wmo_station_id": wmo,
            "wigos_station_id": wigos,
            "ship_or_mobile_station_id": raw.get("shipOrMobileLandStationIdentifier") or "",
            "time": _time(raw),
            "time_period": raw.get("timePeriod"),
        }
    )
    for key, name in HEADER_KEYS.items():
        if raw.get(key) is not None and header.get(name) is None:
            value = raw[key]
            header[name] = value.strip() if isinstance(value, str) else value
    for name, kind in PROFILE_KINDS.items():
        if kind == "float" and header.get(name) is not None:
            header[name] = _round(header[name], name)
    header["station_id"] = _station_id(header)
    levels = {}
    for name in FLOAT_LEVELS:
        column = np.array([np.nan if r.get(name) is None else r[name] for r in rows], "float64")
        levels[name] = np.round(column, PLACES[name])
    for name in INT_LEVELS:
        levels[name] = np.array([-1 if r.get(name) is None else int(r[name]) for r in rows])
    return Profile(header, levels)


#: Seconds in the units ecCodes gives a time period (0 04 024, 0 04 025, 0 04 026) in.
PERIOD_UNITS = {"h": 3600, "min": 60, "s": 1}


def _seconds(handle, name: str) -> int | None:
    """A time period element in seconds, whatever its units; None when missing."""
    import eccodes

    from planetary_datasets.providers.observations.wis2_targets import _get

    value = clean(_get(handle, name))
    if value is None:
        return None
    try:
        units = eccodes.codes_get_string(handle, f"{name}->units")
    except eccodes.KeyValueNotFoundError:
        units = "min"
    return int(round(float(value) * PERIOD_UNITS.get(units.strip(), 60)))


def _walk(handle, base: dict) -> list[Profile]:
    import eccodes

    from planetary_datasets.providers.observations.wis2_targets import _get

    walker = _Walker(base)
    first = None
    iterator = eccodes.codes_bufr_keys_iterator_new(handle)
    try:
        while eccodes.codes_bufr_keys_iterator_next(iterator):
            name = eccodes.codes_bufr_keys_iterator_get_name(iterator)
            if not name.startswith("#"):
                continue
            bare = name.split("#", 2)[2]
            if first is None:
                first = bare
            if bare == first:
                walker.new_subset()
            if bare == "timePeriod":
                walker.take(bare, _seconds(handle, name))
            elif bare in WALKED or "->associatedField" in bare:
                walker.take(bare, clean(_get(handle, name)))
    finally:
        eccodes.codes_bufr_keys_iterator_delete(iterator)
    walker.close()
    return walker.profiles


WALKED = frozenset(
    [*HEADER_KEYS, *TIME_KEYS, *LEVEL_START, *LEVEL_KEYS, *STATION_KEYS, *REPLICATION_KEYS]
    + ["timePeriod", "qualityInformation"]
)


def _from_sounding(sounding: ua.Sounding) -> Profile:
    """A profile from a profiler reported in the PILOT template (Hong Kong)."""
    h = sounding.header
    header = {k: h.get(k) for k in PROFILE_KINDS if k in h}
    header.update(time=h.get("launch_time"), station_elevation=h.get("station_elevation"))
    if header.get("station_elevation") is None and h.get("release_height") is not None:
        header["station_elevation"] = h["release_height"]
    header["station_id"] = h["station_id"]
    count = sounding.level_count
    nan = np.full(count, np.nan)
    levels = {name: nan.copy() for name in FLOAT_LEVELS}
    levels["geopotential_height"] = np.round(
        sounding.levels["geopotential_height"], PLACES["geopotential_height"]
    )
    levels["wind_direction"] = sounding.levels["wind_direction"]
    levels["wind_speed"] = sounding.levels["wind_speed"]
    for name in INT_LEVELS:
        levels[name] = np.full(count, -1)
    return Profile(header, levels)


def decode_message(message: bytes, base: dict, counts: Counter) -> list[Profile]:
    """The profiles in one BUFR message; [] for anything else."""
    import eccodes

    handle = eccodes.codes_new_from_message(message)
    try:
        category = int(eccodes.codes_get(handle, "dataCategory"))
        if category not in PROFILE_CATEGORIES:
            counts["other_category"] += 1
            return []
        edition = int(eccodes.codes_get(handle, "edition"))
        base = {
            **base,
            "data_category": category,
            "data_sub_category": (
                int(eccodes.codes_get(handle, "internationalDataSubCategory"))
                if edition >= 4
                else None
            ),
            "local_data_sub_category": int(eccodes.codes_get(handle, "dataSubCategory")),
            "originating_centre": int(eccodes.codes_get(handle, "bufrHeaderCentre")),
        }
        eccodes.codes_set(handle, "unpack", 1)
        descriptors = [int(d) for d in eccodes.codes_get_array(handle, "unexpandedDescriptors")]
        base["bufr_template"] = " ".join(f"{d:06d}" for d in descriptors)
        subsets = int(eccodes.codes_get(handle, "numberOfSubsets"))
        if subsets > 1 and eccodes.codes_get(handle, "compressedData"):
            counts["compressed_multi_subset"] += 1
            return []
    finally:
        eccodes.codes_release(handle)
    if descriptors[0] in SOUNDING_TEMPLATES:
        found = [_from_sounding(s) for s in ua.decode_message(message, base, counts)]
    else:
        handle = eccodes.codes_new_from_message(message)
        try:
            eccodes.codes_set(handle, "unpack", 1)
            found = _walk(handle, base)
        finally:
            eccodes.codes_release(handle)
    if not found:
        counts["no_levels"] += 1
    return found


def decode_object(key: str, body: bytes, arrived, counts: Counter) -> list[Profile]:
    """Every profile with levels in one cache object. Never raises: failures are counted."""
    heading = ua.HEADING.search(key.rsplit("/", 1)[-1])
    if heading is None:
        # Some relays drop the A_ prefix (``IUPD40ENMI050706_C_EDZW_...``).
        heading = re.match(r"([A-Z]{4}\d{2}[A-Z]{4}\d{6}(?:[A-Z]{3})?)_", key.rsplit("/", 1)[-1])
    parts = key.split("/")
    base = {
        "source_key": key,
        "source_centre": parts[1] if len(parts) > 1 and parts[0] == "data" else "",
        "bulletin_heading": heading.group(1) if heading else "",
        "arrival_time": ua._utc_seconds(arrived),
    }
    found: list[Profile] = []
    messages = 0
    for message in split_bufr(body):
        messages += 1
        try:
            profiles = decode_message(message, base, counts)
        except Exception as exc:  # noqa: BLE001 - one bad message must not stop the rest
            counts["bad_message"] += 1
            logger.debug(f"wis2 wind profiler: {key}: {type(exc).__name__}: {exc}")
            continue
        found += [p for p in profiles if p.level_count]
    if not messages:
        counts["not_bufr"] += 1
    counts["messages"] += messages
    return found


# --------------------------------------------------------------------------- dataset


def deduplicate(profiles: Iterable[Profile], stored: set[str], counts: Counter) -> list[Profile]:
    """One copy of each profile not already ``stored``, in time order.

    The first copy is kept; its ``first_arrival_time`` is the earliest ``arrival_time`` of
    all the copies.
    """
    unique: dict[str, Profile] = {}
    first_arrival: dict[str, np.datetime64] = {}
    for profile in profiles:
        identity = profile.identity
        if identity in stored:
            counts["already_stored"] += 1
            continue
        if identity in unique:
            counts["duplicate"] += 1
        else:
            unique[identity] = profile
        arrived, earliest = profile.header.get("arrival_time"), first_arrival.get(identity)
        if (
            arrived is not None
            and not np.isnat(arrived)
            and (earliest is None or arrived < earliest)
        ):
            first_arrival[identity] = arrived
    for identity, profile in unique.items():
        profile.header["first_arrival_time"] = first_arrival.get(identity)
    keep = list(unique.values())
    keep.sort(key=lambda p: (p.header.get("time") or np.datetime64(0, "s"), p.identity))
    return keep


def to_dataset(profiles: Sequence[Profile], first_profile: int, first_level: int) -> xr.Dataset:
    """Profiles as rows of ``profile`` and their levels as rows of ``profile_level``.

    ``first_profile`` and ``first_level`` are the group's current lengths, so the indexes
    linking the two dimensions are absolute.
    """
    counts = np.array([p.level_count for p in profiles], dtype="int64")
    starts = first_level + np.concatenate([[0], np.cumsum(counts)[:-1]]).astype("int64")
    data: dict[str, tuple] = {}
    for name, kind in PROFILE_KINDS.items():
        values = [p.identity if name == "profile_id" else p.header.get(name) for p in profiles]
        attrs = dict(PROFILE_ATTRS[name])
        fill = attrs.get("_FillValue", -1)
        column = ua._column(values, kind, fill=fill)
        if kind == "float":
            column = ua.narrow(column, PLACES[name])
            attrs["decimal_places"] = PLACES[name]
        elif kind == "int":
            attrs.setdefault("_FillValue", -1)
        data[name] = ("profile", column, attrs)
    data["first_level"] = ("profile", starts, dict(PROFILE_ATTRS["first_level"]))
    data["level_count"] = (
        "profile",
        counts.astype("int32"),
        dict(PROFILE_ATTRS["level_count"]),
    )
    index = np.repeat(np.arange(len(profiles), dtype="int64") + first_profile, counts)
    data["profile_index"] = ("profile_level", index, dict(LEVEL_ATTRS["profile_index"]))
    for name in (*FLOAT_LEVELS, *INT_LEVELS):
        parts = [p.levels[name] for p in profiles]
        attrs = dict(LEVEL_ATTRS[name])
        if name in INT_LEVELS:
            values = np.concatenate(parts).astype("int32") if parts else np.array([], "int32")
        else:
            values = np.concatenate(parts).astype("float64") if parts else np.array([])
            values = ua.narrow(values, PLACES[name])
            attrs["decimal_places"] = PLACES[name]
        data[name] = ("profile_level", values, attrs)
    ds = xr.Dataset(data)
    ds.attrs = {
        "title": "Wind profiler and radar wind profiles from the WMO WIS2 Global Cache",
        "source": "s3://wis2globalcache GTS IUP* bulletins (BUFR categories 2 and 6), "
        "decoded with ecCodes",
        "layout": (
            "ragged: one row per station and time along 'profile', one row per range gate "
            "along 'profile_level'; profile_level.profile_index -> profile row, "
            "profile.first_level/level_count -> levels"
        ),
    }
    return ds


# --------------------------------------------------------------------------- store


def _stored_ids(repo) -> tuple[set[str], int, int]:
    """Recent stored profile ids, and the group's profile and level lengths."""
    import zarr

    from planetary_datasets.common.store import STORE_READ_ERRORS

    try:
        group = zarr.open_group(repo.readonly_session("main").store, path=GROUP, mode="r")
        ids = group["profile_id"]
    except (*STORE_READ_ERRORS, KeyError, zarr.errors.GroupNotFoundError):
        return set(), 0, 0
    profiles = ids.shape[0]
    levels = group["profile_index"].shape[0]
    recent = ids[max(0, profiles - RECENT_IDS) :]
    return {str(v) for v in recent}, profiles, levels


def written(repo, label: str) -> bool:
    """Whether the group's ledger lists window ``label``."""
    from planetary_datasets.common.multidim import ledger

    return label in ledger(repo, group=GROUP)


def write(profiles: Sequence[Profile], config: Config, label: str | None, counts: Counter) -> int:
    """Append the profiles the group lacks, as window ``label``; the number written.

    The group keeps its own ledger, so a window already holding soundings still gets its
    profiles, and a window whose profiles are written is skipped outright.
    """
    from planetary_datasets.common.multidim import (
        append_partition,
        describe_variables,
        mark_written,
    )

    counts["decoded"] += len(profiles)
    repo = config.icechunk_repo(ua.STORE_PREFIX)
    if label is not None and written(repo, label):
        counts["window_already_written"] += 1
        return 0
    stored, first_profile, first_level = _stored_ids(repo)
    keep = deduplicate(profiles, stored, counts)
    if not keep:
        if label is not None and first_profile:
            mark_written(repo, label, label=f"wis2_upper_air {GROUP}", group=GROUP)
        return 0
    ds = to_dataset(keep, first_profile, first_level)
    if not append_partition(repo, ds, label, label=f"wis2_upper_air {GROUP}", group=GROUP):
        counts["window_already_written"] += 1
        return 0
    describe_variables(
        repo,
        {name: PROFILE_ATTRS[name] for name in ("arrival_time", "first_arrival_time")},
        label=f"wis2_upper_air {GROUP}",
        group=GROUP,
    )
    counts["levels_written"] += int(ds.sizes["profile_level"])
    return len(keep)
