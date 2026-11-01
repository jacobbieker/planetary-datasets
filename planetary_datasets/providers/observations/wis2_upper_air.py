"""Upper-air soundings from the WIS2 Global Cache, as one ragged Icechunk store.

An archive target of :mod:`planetary_datasets.providers.observations.wis2_archive`: as each
12-hour window of the cache is streamed into its tar, the objects that can hold soundings
are handed here, decoded with ecCodes, and appended to ``bkr/obs/wis2_upper_air.icechunk``.

What counts as a sounding is decided by each BUFR message's own header, BUFR Table A data
category 2 ("vertical soundings, other than satellite"), whatever the object is called:

=======================  =================================================================
Sub-category (C-13)      Seen in the cache as
=======================  =================================================================
1  PILOT                 GTS ``IUJ*``/``IUW*`` (309050/309051), NWS ``wind-profile`` topic
4  TEMP                  GTS ``IUK*``/``IUS*`` (309052/309057), every ``.../temp`` topic
7  TEMP DROP             GTS ``IUD*`` (309053)
10 wind profiler (radar) HKO ``wind-profile-radar`` topic (309051)
14 radiosonde descent    GTS ``IUD*``/``IUX*`` (309056)
=======================  =================================================================

Objects are picked out cheaply by key first (:func:`wanted`): the GTS relays' BUFR upper-air
bulletins (``.../I/U/<A1>/...`` with A1 in ``D J K S W X``; ``IUA`` is aircraft, ``IUE``/
``IUF``/``IUT``/``IUV`` satellite, ``IUP``/``IUC`` radar and other) and the WIS2 topics for
soundings and profilers. TAC text (``U*`` bulletins, ``.txt``) is not parsed.

Layout (``xr.open_zarr`` opens it directly)::

    sounding (one row per launch):  sounding_id, station_id, wmo_station_id,
        wigos_station_id, ship_or_mobile_station_id, flight_number, launch_time,
        latitude, longitude, release_height, station_elevation, radiosonde_type,
        radiosonde_serial_number, solar_and_infrared_radiation_correction,
        tracking_technique, measuring_equipment_type, data_sub_category, bufr_template,
        originating_centre, bulletin_heading, source_key, source_centre, arrival_time,
        first_arrival_time, first_level, level_count
    level (one row per reported level): sounding_index, time_since_launch,
        vertical_sounding_significance, pressure, geopotential_height,
        latitude_displacement, longitude_displacement, temperature,
        dewpoint_temperature, wind_direction, wind_speed

``sounding_index`` is the level's row in ``sounding``; ``first_level`` and ``level_count``
give the reverse. Values are the BUFR values, in SI units (Pa, K, m, m s-1, degrees). BUFR
values are decimals (an integer times a power of ten); each float variable records its
``decimal_places`` and is stored as float32 when every value, rounded to those places,
reads back as the same decimal, and as float64 otherwise, so nothing is lost.

The same sounding reaches the cache several times: through the DWD and JMA GTS relays and
the issuing centre's own topic, and again in a later window. Its identity is the station,
launch time, sub-category and a fingerprint of its levels, so relayed copies (identical
content) collapse to one row while TEMP parts A-D, a TAC-converted part A beside the
high-resolution BUFR, and corrected re-issues (different levels or values) are each kept.

Each sounding records when it reached the cache, so the live stream can be replayed:
``arrival_time``, when the object kept (the window's first copy) arrived, and
``first_arrival_time``, the earliest arrival of any copy the window collapsed into the row.
Both are UTC, to the second; rows written before ``first_arrival_time`` was added read NaT
there.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
import re
from collections import Counter
from typing import Iterable, Sequence

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.config import Config, get_config
from planetary_datasets.providers.observations.wis2_targets import (
    QueuedTarget,
    register,
    split_bufr,
)

STORE_PREFIX = "bkr/obs/wis2_upper_air.icechunk"

#: BUFR Table A: vertical soundings (other than satellite).
SOUNDING_CATEGORY = 2
#: GTS A1 letters of BUFR upper-air bulletins (T1T2 = IU) that carry soundings.
GTS_SOUNDING_A1 = frozenset("DJKSWX")
GTS_PATH = re.compile(r"/data/core/I/U/([A-Z])/")
#: WIS2 topic directories that carry soundings or profiles.
TOPIC_PARTS = ("/temp/", "/wind-profile/", "/wind-profile-radar/", "/pibal/", "/profiler")
#: A GTS bulletin's TTAAiiCCCCYYGGgg[BBB] in a file name.
HEADING = re.compile(r"A_([A-Z]{4}\d{2}[A-Z]{4}\d{6}(?:[A-Z]{3})?)_")

#: How many stored sounding ids to read back to skip ones already stored. A sounding is
#: relayed within hours, so the most recent rows are the only ones a new copy can match.
RECENT_IDS = 100_000

#: Level elements, in the order the BUFR level sequences (3-03-054 and kin) carry them.
LEVEL_KEYS = {
    "time_since_launch": "timePeriod",
    "vertical_sounding_significance": "extendedVerticalSoundingSignificance",
    "pressure": "pressure",
    "latitude_displacement": "latitudeDisplacement",
    "longitude_displacement": "longitudeDisplacement",
    "temperature": "airTemperature",
    "dewpoint_temperature": "dewpointTemperature",
    "wind_direction": "windDirection",
    "wind_speed": "windSpeed",
}
#: TEMP sequences give height as a non-coordinate; PILOT-by-height as the coordinate.
HEIGHT_KEYS = ("nonCoordinateGeopotentialHeight", "geopotentialHeight")

#: Decimal places of each value: at least the BUFR element's scale, so rounding to them
#: recovers the exact BUFR decimal from ecCodes' binary double. A column is stored as
#: float32 when float32 still gives back that decimal for every value, else float64.
PLACES = {
    "time_since_launch": 0,
    "pressure": 0,
    "geopotential_height": 1,
    "latitude_displacement": 5,
    "longitude_displacement": 5,
    "temperature": 2,
    "dewpoint_temperature": 2,
    "wind_direction": 0,
    "wind_speed": 1,
    "latitude": 5,
    "longitude": 5,
    "release_height": 1,
    "station_elevation": 1,
}

LEVEL_ATTRS = {
    "sounding_index": {"long_name": "row of this level's sounding along the sounding dimension"},
    "time_since_launch": {"long_name": "time since launch", "units": "s"},
    "vertical_sounding_significance": {
        "long_name": "extended vertical sounding significance (BUFR 0 08 042 flag table)",
        "_FillValue": -1,
    },
    "pressure": {"long_name": "pressure", "units": "Pa"},
    "geopotential_height": {"long_name": "geopotential height", "units": "m"},
    "latitude_displacement": {
        "long_name": "latitude displacement since launch",
        "units": "degrees_north",
    },
    "longitude_displacement": {
        "long_name": "longitude displacement since launch",
        "units": "degrees_east",
    },
    "temperature": {"long_name": "air temperature", "units": "K"},
    "dewpoint_temperature": {"long_name": "dewpoint temperature", "units": "K"},
    "wind_direction": {"long_name": "wind direction (from)", "units": "degree"},
    "wind_speed": {"long_name": "wind speed", "units": "m s-1"},
}
SOUNDING_ATTRS = {
    "sounding_id": {
        "long_name": "station|launch time|data sub-category|level fingerprint (dedup identity)"
    },
    "station_id": {"long_name": "WMO block and station, else WIGOS id, else ship/mobile id"},
    "wmo_station_id": {"long_name": "WMO block and station number (IIiii)"},
    "wigos_station_id": {"long_name": "WIGOS station identifier"},
    "ship_or_mobile_station_id": {"long_name": "ship or mobile land station identifier"},
    "flight_number": {"long_name": "aircraft flight number (dropsondes)"},
    "launch_time": {"long_name": "launch time (UTC)"},
    "latitude": {"long_name": "launch latitude", "units": "degrees_north"},
    "longitude": {"long_name": "launch longitude", "units": "degrees_east"},
    "release_height": {"long_name": "height of release above mean sea level", "units": "m"},
    "station_elevation": {
        "long_name": "height of station ground above mean sea level",
        "units": "m",
    },
    "radiosonde_type": {"long_name": "radiosonde type (BUFR 0 02 011)", "_FillValue": -1},
    "radiosonde_serial_number": {"long_name": "radiosonde serial number"},
    "solar_and_infrared_radiation_correction": {
        "long_name": "solar and infrared radiation correction (BUFR 0 02 013)",
        "_FillValue": -1,
    },
    "tracking_technique": {
        "long_name": "tracking technique / status of system (BUFR 0 02 014)",
        "_FillValue": -1,
    },
    "measuring_equipment_type": {
        "long_name": "type of measuring equipment used (BUFR 0 02 003)",
        "_FillValue": -1,
    },
    "data_sub_category": {
        "long_name": "international data sub-category (BUFR Common Code Table C-13, category 2)",
        "flag_values": [1, 4, 7, 10, 14],
        "flag_meanings": "pilot temp temp_drop wind_profiler radiosonde_descent",
    },
    "bufr_template": {"long_name": "BUFR unexpanded descriptors"},
    "originating_centre": {"long_name": "BUFR originating centre (Common Code Table C-11)"},
    "bulletin_heading": {"long_name": "GTS abbreviated heading TTAAiiCCCCYYGGgg[BBB]"},
    "source_key": {"long_name": "object key in s3://wis2globalcache"},
    "source_centre": {"long_name": "WIS2 centre the object came through"},
    "arrival_time": {
        "long_name": "time the copy of this observation kept here arrived in the WIS2 Global Cache",
    },
    "first_arrival_time": {
        "long_name": "earliest time any copy of this observation arrived in the WIS2 Global Cache",
        "comment": "minimum over the copies (GTS relays, the originating centre's topic) "
        "collapsed into this row within its window; rows written before this variable was "
        "added have no value (NaT)",
    },
    "first_level": {"long_name": "row of this sounding's first level along the level dimension"},
    "level_count": {"long_name": "number of levels"},
}


def wanted(key: str) -> bool:
    """Whether an object of the cache may hold soundings (decided by its key alone)."""
    if key.endswith(".txt") or "/lightning/" in key:
        return False
    match = GTS_PATH.search(key)
    if match:
        return match.group(1) in GTS_SOUNDING_A1
    return "gts-to-wis2" not in key and any(part in key for part in TOPIC_PARTS)


@dataclasses.dataclass
class Sounding:
    """One decoded launch: header values and its levels."""

    header: dict
    levels: dict[str, np.ndarray]

    @property
    def level_count(self) -> int:
        return len(self.levels["pressure"])

    def fingerprint(self) -> str:
        digest = hashlib.sha1()
        for name in ("pressure", "geopotential_height", "temperature", "wind_speed"):
            digest.update(np.nan_to_num(self.levels[name], nan=-1e9).astype("<f8").tobytes())
        return digest.hexdigest()[:16]

    @property
    def identity(self) -> str:
        launch = self.header["launch_time"]
        stamp = "NaT" if launch is None else pd.Timestamp(launch).strftime("%Y%m%dT%H%M%S")
        return (
            f"{self.header['station_id']}|{stamp}|"
            f"{self.header['data_sub_category']}|{self.fingerprint()}"
        )


# --------------------------------------------------------------------------- decoding


def _values(handle, key: str) -> np.ndarray | None:
    import eccodes

    try:
        size = eccodes.codes_get_size(handle, key)
    except eccodes.KeyValueNotFoundError:
        return None
    if size == 0:
        return None
    values = np.asarray(eccodes.codes_get_array(handle, key))
    if values.dtype.kind == "f":
        values = np.where(values == eccodes.CODES_MISSING_DOUBLE, np.nan, values)
    elif values.dtype.kind in "iu":
        values = np.where(values == eccodes.CODES_MISSING_LONG, np.nan, values.astype("float64"))
    return values


def _first(handle, key: str):
    values = _values(handle, key)
    if values is None or not len(values):
        return None
    value = values[0]
    if isinstance(value, (float, np.floating)) and np.isnan(value):
        return None
    return value


def _text(handle, key: str) -> str:
    import eccodes

    try:
        value = eccodes.codes_get_string(handle, key)
    except eccodes.KeyValueNotFoundError:
        return ""
    return "" if value is None else value.strip()


def _round(value, name: str):
    return None if value is None else round(float(value), PLACES[name])


def _integer(value) -> int | None:
    return None if value is None else int(value)


def _launch_time(handle, prefix: str) -> np.datetime64 | None:
    parts = [_first(handle, prefix + k) for k in ("year", "month", "day", "hour", "minute")]
    if any(p is None for p in parts):
        return None
    second = _first(handle, prefix + "second") or 0
    try:
        when = dt.datetime(*(int(p) for p in parts), int(second))
    except ValueError:
        return None
    return np.datetime64(when, "s")


def _subset(handle, prefix: str, base: dict) -> Sounding | None:
    replications = _values(handle, prefix + "extendedDelayedDescriptorReplicationFactor")
    if replications is None:
        replications = _values(handle, prefix + "delayedDescriptorReplicationFactor")
    if replications is None or not len(replications) or np.isnan(replications[0]):
        return None
    count = int(replications[0])
    levels: dict[str, np.ndarray] = {}
    for name, key in LEVEL_KEYS.items():
        values = _values(handle, prefix + key)
        column = np.full(count, np.nan)
        if values is not None:
            column[: min(count, len(values))] = values[:count]
        levels[name] = column
    height = np.full(count, np.nan)
    for key in HEIGHT_KEYS:
        values = _values(handle, prefix + key)
        if values is not None:
            height[: min(count, len(values))] = values[:count]
            break
    levels["geopotential_height"] = height
    for name, column in levels.items():
        if name in PLACES:
            levels[name] = np.round(column, PLACES[name])

    block, station = (
        _first(handle, prefix + "blockNumber"),
        _first(handle, prefix + "stationNumber"),
    )
    wmo = f"{int(block):02d}{int(station):03d}" if block is not None and station is not None else ""
    series = _first(handle, prefix + "wigosIdentifierSeries")
    wigos = ""
    if series is not None:
        wigos = "-".join(
            str(v)
            for v in (
                int(series),
                _integer(_first(handle, prefix + "wigosIssuerOfIdentifier")),
                _integer(_first(handle, prefix + "wigosIssueNumber")),
                _text(handle, prefix + "wigosLocalIdentifierCharacter"),
            )
        )
    ship = _text(handle, prefix + "shipOrMobileLandStationIdentifier")
    flight = _text(handle, prefix + "aircraftFlightNumber")
    latitude = _first(handle, prefix + "latitude")
    longitude = _first(handle, prefix + "longitude")
    station_id = wmo or wigos or ship or flight
    if not station_id:
        station_id = f"{latitude}_{longitude}"
    header = {
        **base,
        "station_id": station_id,
        "wmo_station_id": wmo,
        "wigos_station_id": wigos,
        "ship_or_mobile_station_id": ship,
        "flight_number": flight,
        "launch_time": _launch_time(handle, prefix),
        "latitude": _round(latitude, "latitude"),
        "longitude": _round(longitude, "longitude"),
        "release_height": _round(_first(handle, prefix + "height"), "release_height"),
        "station_elevation": _round(
            _first(handle, prefix + "heightOfStationGroundAboveMeanSeaLevel"), "station_elevation"
        ),
        "radiosonde_type": _integer(_first(handle, prefix + "radiosondeType")),
        "radiosonde_serial_number": _text(handle, prefix + "radiosondeSerialNumber"),
        "solar_and_infrared_radiation_correction": _integer(
            _first(handle, prefix + "solarAndInfraredRadiationCorrection")
        ),
        "tracking_technique": _integer(
            _first(handle, prefix + "trackingTechniqueOrStatusOfSystem")
        ),
        "measuring_equipment_type": _integer(_first(handle, prefix + "measuringEquipmentType")),
    }
    return Sounding(header, levels)


def decode_message(message: bytes, base: dict, counts: Counter) -> list[Sounding]:
    """The soundings in one BUFR message; [] for anything else."""
    import eccodes

    handle = eccodes.codes_new_from_message(message)
    try:
        if eccodes.codes_get(handle, "dataCategory") != SOUNDING_CATEGORY:
            counts["other_category"] += 1
            return []
        base = {
            **base,
            "data_sub_category": int(eccodes.codes_get(handle, "internationalDataSubCategory")),
            "originating_centre": int(eccodes.codes_get(handle, "bufrHeaderCentre")),
        }
        eccodes.codes_set(handle, "unpack", 1)
        base["bufr_template"] = " ".join(
            f"{int(d):06d}" for d in eccodes.codes_get_array(handle, "unexpandedDescriptors")
        )
        subsets = int(eccodes.codes_get(handle, "numberOfSubsets"))
        if subsets > 1 and eccodes.codes_get(handle, "compressedData"):
            counts["compressed_multi_subset"] += 1
            return []
        found = []
        for number in range(1, subsets + 1):
            prefix = f"/subsetNumber={number}/" if subsets > 1 else ""
            sounding = _subset(handle, prefix, base)
            if sounding is None or sounding.level_count == 0:
                counts["no_levels"] += 1
                continue
            found.append(sounding)
        return found
    finally:
        eccodes.codes_release(handle)


def _utc_seconds(when) -> np.datetime64 | None:
    if when is None:
        return None
    stamp = pd.Timestamp(when)
    if stamp.tzinfo is not None:
        stamp = stamp.tz_convert("UTC").tz_localize(None)
    return np.datetime64(stamp, "s")


def decode_object(key: str, body: bytes, arrived, counts: Counter) -> list[Sounding]:
    """Every sounding in one cache object. Never raises: failures are counted."""
    heading = HEADING.search(key.rsplit("/", 1)[-1])
    parts = key.split("/")
    base = {
        "source_key": key,
        "source_centre": parts[1] if len(parts) > 1 and parts[0] == "data" else "",
        "bulletin_heading": heading.group(1) if heading else "",
        "arrival_time": _utc_seconds(arrived),
    }
    found: list[Sounding] = []
    messages = 0
    for message in split_bufr(body):
        messages += 1
        try:
            found += decode_message(message, base, counts)
        except Exception as exc:  # noqa: BLE001 - one bad message must not stop the rest
            counts["bad_message"] += 1
            logger.debug(f"wis2 upper air: {key}: {type(exc).__name__}: {exc}")
    if not messages:
        counts["not_bufr"] += 1
    counts["messages"] += messages
    return found


# --------------------------------------------------------------------------- dataset


def _column(values: Sequence, kind: str, fill=None) -> np.ndarray:
    if kind == "str":
        return np.array(["" if v is None else str(v) for v in values], dtype=object)
    if kind == "time":
        return np.array(
            [np.datetime64("NaT", "s") if v is None else v for v in values], dtype="datetime64[s]"
        )
    if kind == "int":
        return np.array([fill if v is None else int(v) for v in values], dtype="int32")
    return np.array([np.nan if v is None else float(v) for v in values], dtype="float64")


SOUNDING_KINDS = {
    "sounding_id": "str",
    "station_id": "str",
    "wmo_station_id": "str",
    "wigos_station_id": "str",
    "ship_or_mobile_station_id": "str",
    "flight_number": "str",
    "launch_time": "time",
    "latitude": "float",
    "longitude": "float",
    "release_height": "float",
    "station_elevation": "float",
    "radiosonde_type": "int",
    "radiosonde_serial_number": "str",
    "solar_and_infrared_radiation_correction": "int",
    "tracking_technique": "int",
    "measuring_equipment_type": "int",
    "data_sub_category": "int",
    "bufr_template": "str",
    "originating_centre": "int",
    "bulletin_heading": "str",
    "source_key": "str",
    "source_centre": "str",
    "arrival_time": "time",
    "first_arrival_time": "time",
}


def narrow(values: np.ndarray, places: int) -> np.ndarray:
    """``values`` as float32 if every one reads back as the same decimal, else float64."""
    single = values.astype("float32")
    with np.errstate(invalid="ignore"):
        same = np.array_equal(np.round(single.astype("float64"), places), values, equal_nan=True)
    return single if same else values


def to_dataset(soundings: Sequence[Sounding], first_sounding: int, first_level: int) -> xr.Dataset:
    """Soundings as rows of ``sounding`` and their levels as rows of ``level``.

    ``first_sounding`` and ``first_level`` are the store's current lengths, so the indexes
    linking the two dimensions are absolute.
    """
    counts = np.array([s.level_count for s in soundings], dtype="int64")
    starts = first_level + np.concatenate([[0], np.cumsum(counts)[:-1]]).astype("int64")
    data: dict[str, tuple] = {}
    for name, kind in SOUNDING_KINDS.items():
        values = [s.identity if name == "sounding_id" else s.header.get(name) for s in soundings]
        column = _column(values, kind, fill=-1)
        attrs = dict(SOUNDING_ATTRS[name])
        if kind == "float":
            column = narrow(column, PLACES[name])
            attrs["decimal_places"] = PLACES[name]
        data[name] = ("sounding", column, attrs)
    data["first_level"] = ("sounding", starts, dict(SOUNDING_ATTRS["first_level"]))
    data["level_count"] = ("sounding", counts.astype("int32"), dict(SOUNDING_ATTRS["level_count"]))
    index = np.repeat(np.arange(len(soundings), dtype="int64") + first_sounding, counts)
    data["sounding_index"] = ("level", index, dict(LEVEL_ATTRS["sounding_index"]))
    for name in [*LEVEL_KEYS, "geopotential_height"]:
        values = np.concatenate([s.levels[name] for s in soundings]) if soundings else np.array([])
        attrs = dict(LEVEL_ATTRS[name])
        if name == "vertical_sounding_significance":
            values = np.where(np.isnan(values), -1, values).astype("int32")
        else:
            values = narrow(values.astype("float64"), PLACES[name])
            attrs["decimal_places"] = PLACES[name]
        data[name] = ("level", values, attrs)
    ds = xr.Dataset(data)
    for name, kind in SOUNDING_KINDS.items():
        if kind == "int":
            ds[name].attrs.setdefault("_FillValue", -1)
    ds.attrs = {
        "title": "Upper-air soundings from the WMO WIS2 Global Cache",
        "source": "s3://wis2globalcache (BUFR data category 2), decoded with ecCodes",
        "layout": (
            "ragged: one row per launch along 'sounding', one row per level along 'level'; "
            "level.sounding_index -> sounding row, sounding.first_level/level_count -> levels"
        ),
    }
    return ds


def deduplicate(soundings: Iterable[Sounding], stored: set[str], counts: Counter) -> list[Sounding]:
    """One copy of each sounding not already ``stored``, in launch-time order.

    The first copy is kept; its ``first_arrival_time`` is the earliest ``arrival_time`` of
    all the copies.
    """
    unique: dict[str, Sounding] = {}
    first_arrival: dict[str, np.datetime64] = {}
    for sounding in soundings:
        identity = sounding.identity
        if identity in stored:
            counts["already_stored"] += 1
            continue
        if identity in unique:
            counts["duplicate"] += 1
        else:
            unique[identity] = sounding
        arrived, earliest = sounding.header.get("arrival_time"), first_arrival.get(identity)
        if (
            arrived is not None
            and not np.isnat(arrived)
            and (earliest is None or arrived < earliest)
        ):
            first_arrival[identity] = arrived
    for identity, sounding in unique.items():
        sounding.header["first_arrival_time"] = first_arrival.get(identity)
    keep = list(unique.values())
    keep.sort(key=lambda s: (s.header["launch_time"] or np.datetime64(0, "s"), s.identity))
    return keep


# --------------------------------------------------------------------------- target


@register("upper_air")
class UpperAirTarget(QueuedTarget):
    """The archive target: soundings decoded as objects stream past, written per window."""

    store_prefix = STORE_PREFIX

    def __init__(self, config: Config | None = None):
        super().__init__(config or get_config())
        self.counts: Counter = Counter()
        self.soundings: list[Sounding] = []

    def wants(self, key: str, size: int) -> bool:
        return wanted(key)

    def decode(self, key: str, body: bytes, arrived) -> None:
        """On the target's worker thread: keep the object's soundings."""
        self.soundings += decode_object(key, body, arrived, self.counts)

    def write(self, window_start, window_end, partition: str | None) -> dict:
        """Drop duplicates and append the window's soundings to the store."""
        found, self.soundings = self.soundings, []
        written = write(found, self.config, partition, self.counts)
        return {"soundings": written, "levels": self.counts["levels_written"], **self.counts}


# --------------------------------------------------------------------------- store


def _stored_ids(repo) -> tuple[set[str], int, int]:
    """Recent stored sounding ids, and the store's sounding and level lengths."""
    import zarr

    from planetary_datasets.common.store import STORE_READ_ERRORS

    try:
        group = zarr.open_group(repo.readonly_session("main").store, mode="r")
        ids = group["sounding_id"]
    except (*STORE_READ_ERRORS, KeyError, zarr.errors.GroupNotFoundError):
        return set(), 0, 0
    soundings = ids.shape[0]
    levels = group["sounding_index"].shape[0]
    recent = ids[max(0, soundings - RECENT_IDS) :]
    return {str(v) for v in recent}, soundings, levels


def write(soundings: Sequence[Sounding], config: Config, label: str | None, counts: Counter) -> int:
    """Append the soundings the store lacks; the number written."""
    from planetary_datasets.common.multidim import (
        append_partition,
        describe_variables,
        ledger,
        mark_written,
    )

    counts["decoded"] += len(soundings)
    repo = config.icechunk_repo(STORE_PREFIX)
    if label is not None and label in ledger(repo):
        counts["window_already_written"] += 1
        return 0
    stored, first_sounding, first_level = _stored_ids(repo)
    keep = deduplicate(soundings, stored, counts)
    if not keep:
        if label is not None and first_sounding:
            mark_written(repo, label, label="wis2_upper_air")
        return 0
    ds = to_dataset(keep, first_sounding, first_level)
    append_partition(repo, ds, label, label="wis2_upper_air")
    describe_variables(
        repo,
        {name: SOUNDING_ATTRS[name] for name in ("arrival_time", "first_arrival_time")},
        label="wis2_upper_air",
    )
    counts["levels_written"] += int(ds.sizes["level"])
    return len(keep)
