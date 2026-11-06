"""Traditional alphanumeric (TAC) SYNOP and SHIP reports from the WIS2 GTS relays.

An archive target of :mod:`planetary_datasets.providers.observations.wis2_archive`: the
text SYNOP bulletins the DWD and JMA relays put on WIS2 (``data/<relay>-gts-to-wis2/
data/core/S/{M,I,N}/...``: main, intermediate and non-standard hours) are decoded with
`pymetdecoder <https://github.com/antarctica/pymetdecoder>`_ and appended, as groups
of the existing stores, to::

    bkr/obs/wis2_land.icechunk   group synop_tac   land stations (FM-12 AAXX, FM-14 OOXX)
    bkr/obs/wis2_ocean.icechunk  group ship_tac    ships (FM-13 BBXX)

one row per report along ``observation``. Open one with
``xr.open_zarr(store, group="synop_tac", consolidated=False)``.

What is kept
    Only reports the store does not already have as BUFR: a land report whose station
    and time the land store's BUFR reports (its root group, recent rows) already hold is
    counted and dropped, and likewise a ship report whose call sign and time the ocean
    store's ``surface`` group holds. Many stations send both; the TAC groups hold the
    stations and times that reach WIS2 only as text. A BUFR copy arriving in a later
    window than its TAC twin can still leave both.

Values
    Converted to the units and names of the BUFR groups beside them (K, Pa, m s-1, m,
    kg m-2; land: ``2m_temperature``, ``10m_wind_speed``, ...; ocean:
    ``air_temperature``, ``wind_speed``, ...). Wind reported in knots is converted with
    1 kt = 1852/3600 m s-1. Cloud cover stays in oktas (``total_cloud_cover_oktas``),
    visibility and cloud base are the lower bound of their coded class, precipitation
    traces are -0.1 kg m-2 as in BUFR, and present and past weather from automatic
    stations (code tables 4680, 4531) are offset into BUFR's ranges (100+, 10+), so they
    compare with the BUFR groups' codes. Maximum and minimum temperatures are kept
    without a period, which depends on regional practice. The full report is kept in
    ``report`` and its bulletin heading in ``bulletin``, so anything not decoded or
    converted here can be read from the text.

Times
    A report gives only its day and hour (and, from automatic stations, minute); the
    month and year are the arrival's (the previous month for a day after the arrival).

Duplicates
    A report's identity is its text (``observation_identity``, a 64-bit BLAKE2b hash of
    the section header and report): the copies from both relays and the delayed (RR*)
    re-sends are stored once with the earliest arrival; a correction (CC*) changes the
    text and is stored as a further row, so readers wanting the final version take the
    last ``arrival_time`` per station and time. Identities in the group's last
    :data:`RECENT_IDENTITIES` rows are not written again.

Not decoded: TEMP/PILOT text (``U*``; upper air), BUOY text (``SS*``, FM-18, which
pymetdecoder does not read), METAR/SPECI (``SA``/``SP``: none are relayed to WIS2), and
the other ``S*`` bulletins (sea level, tsunami, DCP and miscellaneous texts).
"""

from __future__ import annotations

import datetime as dt
import hashlib
import re
import warnings
from collections import Counter
from typing import Any

import numpy as np
import xarray as xr
from loguru import logger

from planetary_datasets.config import Config, get_config
from planetary_datasets.providers.observations.wis2_targets import QueuedTarget, register

LAND_STORE = "bkr/obs/wis2_land.icechunk"
OCEAN_STORE = "bkr/obs/wis2_ocean.icechunk"
LAND_GROUP = "synop_tac"
SHIP_GROUP = "ship_tac"
#: Rows at the end of a TAC group whose identities a window is checked against.
RECENT_IDENTITIES = 2_000_000
#: Rows of the BUFR groups read back to find reports they already hold (a few days).
BUFR_TAIL_ROWS = {LAND_STORE: 3_000_000, OCEAN_STORE: 400_000}
KNOT = 1852 / 3600

#: GTS text bulletins of surface synoptic reports: main, intermediate, non-standard hours.
TAC_KEY = re.compile(r"^data/[^/]+-gts-to-wis2/data/core/S/([MIN])/[A-Z]/[A-Z]/\d\d/[A-Z]{4}/")
HEADING = re.compile(r"\b([A-Z]{4}\d\d) ([A-Z]{4}) (\d{6})(?: ([A-Z]{3}))?\b")
SECTIONS = ("AAXX", "BBXX", "OOXX")
PRECIPITATION_HOURS = (1, 3, 6, 12, 24)
SUNSHINE_HOURS = (1, 24)


def wants(key: str, size: int = 0) -> bool:
    """Whether an object is a relayed SYNOP text bulletin (cheap; by key alone)."""
    return TAC_KEY.match(key) is not None and not key.endswith(".bufr")


def identity(text: str) -> int:
    """A report's identity: the 64-bit BLAKE2b hash of its text, as a signed integer."""
    digest = hashlib.blake2b(text.encode(), digest_size=8).digest()
    return int.from_bytes(digest, "little", signed=True)


def split_bulletin(body: bytes) -> tuple[str, list[tuple[str, str]]]:
    """A bulletin's heading and its reports, each as (section, report text).

    The section is the code form's header (``AAXX 01004``, ``BBXX``, ``OOXX``) that the
    report needs to be decoded; reports are whitespace-normalised and lose their ``=``.
    """
    text = body.decode("latin-1").replace("\r", "\n")
    text = re.sub(r"[\x00-\x09\x0b-\x1f]", " ", text)
    heading = HEADING.search(text[:200])
    if heading is None:
        return "", []
    title = " ".join(g for g in heading.groups() if g)
    rest = text[heading.end() :]
    section = ""
    reports = []
    for chunk in rest.split("="):
        tokens = chunk.split()
        while tokens and tokens[0] in SECTIONS:
            if tokens[0] == "AAXX" and len(tokens) > 1:
                section, tokens = " ".join(tokens[:2]), tokens[2:]
            else:
                section, tokens = tokens[0], tokens[1:]
        if tokens and section:
            reports.append((section, " ".join(tokens)))
    return title, reports


def _value(node: Any, *path: str):
    for key in path:
        if not isinstance(node, dict):
            return None
        node = node.get(key)
    return node


def _number(node, *path) -> float:
    value = _value(node, *path)
    try:
        return float(value) if value is not None else np.nan
    except (TypeError, ValueError):
        return np.nan


def _kelvin(decoded: dict, name: str) -> float:
    return _number(decoded, name, "value") + 273.15


def _pascal(decoded: dict, *path: str) -> float:
    return _number(decoded, *path) * 100.0


def _speed(node: dict | None) -> float:
    speed = _number(node, "value")
    unit = _value(node, "unit")
    return speed * KNOT if unit == "KT" else speed


def _code(node, *path) -> int:
    value = _value(node, *path)
    return int(value) if isinstance(value, (int, np.integer)) else -1


def _weather(decoded: dict) -> dict:
    """Present and past weather as BUFR codes (automatic stations' tables offset)."""
    out = {}
    present = decoded.get("present_weather")
    if isinstance(present, dict) and isinstance(present.get("value"), int):
        offset = 100 if present.get("_table") == "4680" else 0
        out["present_weather"] = present["value"] + offset
    past = decoded.get("past_weather") or []
    for i, item in enumerate(past[:2], start=1):
        if isinstance(item, dict) and isinstance(item.get("value"), int):
            offset = 10 if item.get("_table") == "4531" else 0
            out[f"past_weather_{i}"] = item["value"] + offset
    return out


def _precipitation(decoded: dict) -> dict:
    """Precipitation totals by period, in kg m-2; a trace is -0.1 as in BUFR."""
    out = {}
    for name in ("precipitation_s1", "precipitation_s3", "precipitation_24h"):
        node = decoded.get(name)
        if not isinstance(node, dict) or not isinstance(node.get("amount"), dict):
            continue
        hours = 24 if name == "precipitation_24h" else _number(node, "time_before_obs", "value")
        if not np.isfinite(hours) or int(hours) not in PRECIPITATION_HOURS:
            continue
        amount = node["amount"]
        value = -0.1 if amount.get("trace") else _number(amount, "value")
        if np.isfinite(value):
            out.setdefault(f"total_precipitation_{int(hours)}h", value)
    return out


def _sunshine(decoded: dict) -> dict:
    out = {}
    for item in decoded.get("sunshine") or []:
        hours = _number(item, "duration", "value")
        value = _number(item, "amount", "value")
        if np.isfinite(hours) and int(hours) in SUNSHINE_HOURS and np.isfinite(value):
            out[f"sunshine_duration_{int(hours)}h"] = value * 3600.0
    return out


def common_values(decoded: dict) -> dict:
    """Values a land and a ship report share, in SI units (no names yet)."""
    wind = decoded.get("surface_wind") or {}
    gusts = decoded.get("highest_gust") or []
    gust = gusts[0] if isinstance(gusts, list) and gusts else {}
    cover = decoded.get("cloud_cover") or {}
    cloud_types = decoded.get("cloud_types") or {}
    amount = _value(cloud_types, "low_cloud_amount", "value")
    if amount is None:
        amount = _value(cloud_types, "middle_cloud_amount", "value")
    return {
        "station_pressure": _pascal(decoded, "station_pressure", "value"),
        "mean_sea_level_pressure": _pascal(decoded, "sea_level_pressure", "value"),
        "pressure_change_3h": _pascal(decoded, "pressure_tendency", "change", "value"),
        "pressure_tendency": _code(decoded, "pressure_tendency", "tendency", "value"),
        "temperature": _kelvin(decoded, "air_temperature"),
        "dewpoint": _kelvin(decoded, "dewpoint_temperature"),
        "relative_humidity": _number(decoded, "relative_humidity", "value"),
        "wind_direction": np.nan
        if _value(wind, "direction", "varAllUnknown")
        else _number(wind, "direction", "value"),
        "wind_speed": _speed(wind.get("speed")),
        "wind_gust": _speed(gust.get("speed")) if isinstance(gust, dict) else np.nan,
        "visibility": _number(decoded, "visibility", "value"),
        "total_cloud_cover_oktas": _code(cover, "value") if not cover.get("obscured") else -1,
        "sky_obscured": 1 if cover.get("obscured") else -1,
        "low_or_middle_cloud_amount": int(amount) if isinstance(amount, int) else -1,
        "cloud_base_height": _number(decoded, "lowest_cloud_base", "min"),
        **_weather(decoded),
    }


def resolve_time(day: int, hour: int, minute: int, reference: dt.datetime) -> np.datetime64:
    """A report's day, hour and minute as a UTC time near ``reference`` (NaT if invalid).

    The month is the reference's, or the previous one for a day later than the
    reference's next day (a report sent just after a month turns).
    """
    year, month = reference.year, reference.month
    if day > reference.day + 1:
        month -= 1
        if month == 0:
            year, month = year - 1, 12
    try:
        when = dt.datetime(year, month, day, hour, minute)
    except ValueError:
        return np.datetime64("NaT", "s")
    return np.datetime64(when, "s")


def decode_report(section: str, report: str, reference: dt.datetime) -> dict | None:
    """One report as a row, or None when it cannot be decoded (or is NIL).

    Raises:
        ImportError: pymetdecoder is not installed.
    """
    from pymetdecoder import synop

    tokens = report.split()
    if len(tokens) < 2 or tokens[1] == "NIL":
        return None
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            decoded = synop.SYNOP().decode(f"{section} {report}")
        except Exception:  # noqa: BLE001 - malformed text of any kind
            return None
    if not isinstance(decoded, dict):
        return None
    day = _value(decoded, "obs_time", "day", "value")
    hour = _value(decoded, "obs_time", "hour", "value")
    if not isinstance(day, int) or not isinstance(hour, int):
        return None
    exact = decoded.get("exact_obs_time") or {}
    minute = _value(exact, "minute", "value")
    if isinstance(_value(exact, "hour", "value"), int):
        hour = exact["hour"]["value"]
    time = resolve_time(day, hour, minute if isinstance(minute, int) else 0, reference)
    if np.isnat(time):
        return None
    kind = section[:4]
    row = {"kind": kind, "time": time, **common_values(decoded)}
    position = decoded.get("station_position") or {}
    row["latitude"] = _number(position, "latitude")
    row["longitude"] = _number(position, "longitude")
    if kind == "AAXX":
        station = _value(decoded, "station_id", "value")
        if not station:
            return None
        row["station_id"] = f"0-20000-0-{station}"
    else:
        callsign = _value(decoded, "callsign", "value")
        if not callsign:
            return None
        row["station_id"] = str(callsign)
    if kind == "BBXX":
        row["sea_surface_temperature"] = _kelvin(decoded, "sea_surface_temperature")
        waves = decoded.get("wind_waves") or []
        wave = waves[0] if waves and isinstance(waves[0], dict) else {}
        row["wind_wave_height"] = _number(wave, "height", "value")
        row["wind_wave_period"] = _number(wave, "period", "value")
    else:
        row.update(_precipitation(decoded))
        row.update(_sunshine(decoded))
        row["maximum_2m_temperature"] = _kelvin(decoded, "maximum_temperature")
        row["minimum_2m_temperature"] = _kelvin(decoded, "minimum_temperature")
        row["state_of_ground"] = _code(decoded, "ground_state", "state", "value")
        snow = decoded.get("ground_state_snow") or {}
        depth = _value(snow, "depth")
        if isinstance(depth, dict):
            row["snow_depth"] = _number(depth, "depth") / 100.0
        row["automatic_station"] = 1 if _value(decoded, "weather_indicator", "automatic") else 0
    return row


# ---------------------------------------------------------------------------- schema


def _f(units: str, long_name: str) -> tuple[str, dict]:
    return "float64", {"units": units, "long_name": long_name}


def _i(long_name: str, table: str = "") -> tuple[str, dict]:
    attrs = {"long_name": long_name, "_FillValue": -1}
    if table:
        attrs["code_table"] = table
    return "int16", attrs


#: Shared columns: (land name, ship name) -> (dtype, attributes).
SHARED = {
    ("surface_pressure", "surface_air_pressure"): (
        "station_pressure",
        _f("Pa", "station level pressure"),
    ),
    ("mean_sea_level_pressure", "mean_sea_level_pressure"): (
        "mean_sea_level_pressure",
        _f("Pa", "pressure reduced to mean sea level"),
    ),
    ("pressure_change_3h", "pressure_tendency_3h"): (
        "pressure_change_3h",
        _f("Pa", "3-hour pressure change"),
    ),
    ("pressure_tendency", "pressure_tendency"): (
        "pressure_tendency",
        _i("characteristic of pressure tendency", "WMO 0200 (BUFR 010063)"),
    ),
    ("2m_temperature", "air_temperature"): ("temperature", _f("K", "air temperature")),
    ("2m_dewpoint_temperature", "dewpoint_temperature"): (
        "dewpoint",
        _f("K", "dewpoint temperature"),
    ),
    ("2m_relative_humidity", "relative_humidity"): (
        "relative_humidity",
        _f("%", "relative humidity"),
    ),
    ("10m_wind_direction", "wind_direction"): ("wind_direction", _f("degree", "wind direction")),
    ("10m_wind_speed", "wind_speed"): ("wind_speed", _f("m s-1", "wind speed")),
    ("10m_wind_gust", "wind_gust"): ("wind_gust", _f("m s-1", "highest gust")),
    ("horizontal_visibility", "horizontal_visibility"): (
        "visibility",
        _f("m", "horizontal visibility (lower bound of the coded class)"),
    ),
    ("total_cloud_cover_oktas", "total_cloud_cover_oktas"): (
        "total_cloud_cover_oktas",
        _i("total cloud cover (oktas; 9 sky obscured is sky_obscured)", "WMO 2700"),
    ),
    ("sky_obscured", "sky_obscured"): (
        "sky_obscured",
        (
            "int8",
            {"long_name": "sky obscured by fog or other phenomena (1), else -1", "_FillValue": -1},
        ),
    ),
    ("low_or_middle_cloud_amount", "low_or_middle_cloud_amount"): (
        "low_or_middle_cloud_amount",
        _i("cloud amount of the lowest layer (oktas)", "WMO 2700 (BUFR 020011)"),
    ),
    ("cloud_base_height", "cloud_base_height"): (
        "cloud_base_height",
        _f("m", "height of base of the lowest cloud (lower bound of the coded class)"),
    ),
    ("present_weather", "present_weather"): (
        "present_weather",
        _i("present weather", "BUFR 020003 (WMO 4677; 4680 + 100)"),
    ),
    ("past_weather_1", "past_weather_1"): (
        "past_weather_1",
        _i("past weather (1)", "BUFR 020004 (WMO 4561; 4531 + 10)"),
    ),
    ("past_weather_2", "past_weather_2"): (
        "past_weather_2",
        _i("past weather (2)", "BUFR 020005 (WMO 4561; 4531 + 10)"),
    ),
}

LAND_ONLY = {
    "maximum_2m_temperature": _f("K", "maximum temperature (period as regional practice)"),
    "minimum_2m_temperature": _f("K", "minimum temperature (period as regional practice)"),
    **{
        f"total_precipitation_{h}h": _f(
            "kg m-2", f"total precipitation over the past {h} h (1 kg m-2 = 1 mm)"
        )
        for h in PRECIPITATION_HOURS
    },
    **{
        f"sunshine_duration_{h}h": _f("s", f"total sunshine over the past {h} h")
        for h in SUNSHINE_HOURS
    },
    "state_of_ground": _i("state of ground", "WMO 0901"),
    "snow_depth": _f("m", "total snow depth"),
    "automatic_station": ("int8", {"long_name": "1 automatic station, 0 manned", "_FillValue": -1}),
}
SHIP_ONLY = {
    "sea_surface_temperature": _f("K", "sea surface temperature"),
    "significant_height_of_wind_waves": _f("m", "height of wind waves"),
    "mean_period_of_wind_waves": _f("s", "period of wind waves"),
}
SHIP_SOURCES = {
    "significant_height_of_wind_waves": "wind_wave_height",
    "mean_period_of_wind_waves": "wind_wave_period",
}
ARRIVAL_ATTRS = {
    "arrival_time": {
        "long_name": "time the earliest copy of this report arrived in the WIS2 Global Cache",
    },
}


def columns(group: str) -> dict[str, tuple[str, tuple[str, dict]]]:
    """The group's variables: name -> (row key, (dtype, attributes))."""
    ship = group == SHIP_GROUP
    station = (
        ("str", {"long_name": "ship call sign"})
        if ship
        else ("str", {"long_name": "WIGOS station identifier (or OOXX call sign)"})
    )
    out = {
        "call_sign" if ship else "station_id": ("station_id", station),
        "time": ("time", ("datetime64[s]", {"long_name": "observation time"})),
        "latitude": ("latitude", _f("degrees_north", "latitude")),
        "longitude": ("longitude", _f("degrees_east", "longitude")),
    }
    if not ship:
        out["station_elevation"] = (
            "station_elevation",
            _f("m", "height of station ground above mean sea level (from the BUFR reports)"),
        )
    for (land_name, ship_name), (key, spec) in SHARED.items():
        out[ship_name if ship else land_name] = (key, spec)
    for name, spec in (SHIP_ONLY if ship else LAND_ONLY).items():
        out[name] = (SHIP_SOURCES.get(name, name), spec)
    out.update(
        report=("report", ("str", {"long_name": "the report as sent, with its section header"})),
        bulletin=(
            "bulletin",
            ("str", {"long_name": "GTS heading of the bulletin: TTAAii CCCC YYGGgg BBB"}),
        ),
        centre=(
            "centre",
            ("str", {"long_name": "WIS2 centre (GTS relay) the copy kept came from"}),
        ),
        arrival_time=("arrival_time", ("datetime64[s]", ARRIVAL_ATTRS["arrival_time"])),
        observation_identity=(
            "observation_identity",
            ("int64", {"long_name": "64-bit BLAKE2b hash of the section header and report"}),
        ),
    )
    return out


def to_dataset(rows: list[dict], group: str) -> xr.Dataset:
    """Rows as the group's variables along ``observation``."""
    data = {}
    for name, (key, (dtype, attrs)) in columns(group).items():
        values = [r.get(key) for r in rows]
        if dtype == "str":
            array = np.array([v if v is not None else "" for v in values], dtype=object)
        elif dtype.startswith("datetime64"):
            array = np.array(
                [v if v is not None else np.datetime64("NaT") for v in values], dtype=dtype
            )
        elif dtype.startswith("int"):
            array = np.array([-1 if v is None else v for v in values], dtype=dtype)
        else:
            array = np.array([np.nan if v is None else v for v in values], dtype=dtype)
        data[name] = xr.Variable(("observation",), array, dict(attrs))
    ds = xr.Dataset(data)
    what = "ship" if group == SHIP_GROUP else "land station"
    ds.attrs = {
        "title": f"WIS2 Global Cache {what} reports received only as TAC text",
        "source": "WMO WIS2 Global Cache (s3://wis2globalcache), GTS text bulletins S[MIN]",
        "decoder": "pymetdecoder",
    }
    return ds


# --------------------------------------------------------------------------- storage


def _tail(repo, group: str | None, names: tuple[str, ...], rows: int) -> dict[str, np.ndarray]:
    """The last ``rows`` of some variables of a group; times decoded to datetime64[s]."""
    import zarr

    try:
        node = zarr.open_group(repo.readonly_session("main").store, path=group or "", mode="r")
        size = node[names[0]].shape[0]
    except (KeyError, zarr.errors.GroupNotFoundError):  # not written yet; others propagate
        return {}
    start = max(0, size - rows)
    out = {}
    for name in names:
        if name not in node:
            out[name] = np.full(size - start, "", dtype=object)
            continue
        values = node[name][start:size]
        units = str(node[name].attrs.get("units", ""))
        if name == "time" and " since " in units:
            values = _seconds(values, units)
        out[name] = values
    return out


def _seconds(values: np.ndarray, units: str) -> np.ndarray:
    """Stored CF times as int64 seconds since 1970 (fill and NaT as int64 minimum)."""
    import pandas as pd

    from planetary_datasets.common.multidim import _UNIT_NS, NAT

    unit, _, since = units.partition(" since ")
    reference = pd.Timestamp(since.strip())
    reference = reference.tz_convert(None) if reference.tzinfo else reference
    offset = int((reference - pd.Timestamp("1970-01-01")).value)
    values = np.asarray(values).astype("int64")
    ns = values * _UNIT_NS[unit.strip()] + offset
    return np.where(values == NAT, NAT, ns // 10**9)


def bufr_reports(repo, group: str | None, station: str, rows: int) -> set[tuple[str, int]]:
    """(station, time in seconds) of the BUFR reports in the tail of a group."""
    tail = _tail(repo, group, (station, "time"), rows)
    if not tail:
        return set()
    return set(zip(map(str, tail[station]), tail["time"].tolist(), strict=True))


def stored_identities(repo, group: str) -> np.ndarray:
    """Sorted identities of the group's last :data:`RECENT_IDENTITIES` rows."""
    tail = _tail(repo, group, ("observation_identity",), RECENT_IDENTITIES)
    return np.unique(tail["observation_identity"]) if tail else np.empty(0, "int64")


def land_positions(repo, rows: int) -> dict[str, tuple[float, float, float]]:
    """Station positions from the land store's BUFR reports: id -> (lat, lon, elevation)."""
    tail = _tail(repo, None, ("station_id", "latitude", "longitude", "station_elevation"), rows)
    out = {}
    if not tail:
        return out
    for station, lat, lon, height in zip(
        tail["station_id"], tail["latitude"], tail["longitude"], tail["station_elevation"]
    ):
        if np.isfinite(lat) and np.isfinite(lon):
            elevation = float(height) if isinstance(height, (float, np.floating)) else np.nan
            out[str(station)] = (float(lat), float(lon), elevation)
    return out


def write_group(repo, rows: list[dict], group: str, partition: str | None, label: str) -> int:
    """Append a group's new rows and add the window to its ledger. Returns rows written."""
    from planetary_datasets.common.multidim import (
        append_partition,
        describe_variables,
        ledger,
    )

    if partition is not None and partition in ledger(repo, group=group):
        return 0
    if not rows:
        return 0
    rows = sorted(rows, key=lambda r: (r["time"], r["station_id"]))
    if not append_partition(repo, to_dataset(rows, group), partition, label=label, group=group):
        return 0
    describe_variables(repo, ARRIVAL_ATTRS, label=label, group=group)
    return len(rows)


# ---------------------------------------------------------------------------- target


@register("tac")
class TacTarget(QueuedTarget):
    """Archive target: relayed SYNOP and SHIP text into the land and ocean stores."""

    store_prefix = LAND_STORE

    def __init__(self, config: Config | None = None):
        super().__init__(config or get_config())
        self.counts: Counter = Counter()
        #: identity -> row, the earliest copy of each report.
        self.rows: dict[int, dict] = {}
        self._undecodable: set[int] = set()
        self._written: set[str] = set()

    def prepare(self, partition: str) -> None:
        """Note the groups that already hold ``partition``; with both, nothing is taken."""
        from planetary_datasets.common.multidim import ledger

        written = set()
        for prefix, group in ((LAND_STORE, LAND_GROUP), (OCEAN_STORE, SHIP_GROUP)):
            try:
                if partition in ledger(self.config.icechunk_repo(prefix), group=group):
                    written.add(group)
            except Exception:  # noqa: BLE001 - a group not created yet holds nothing
                continue
        self._written = written

    def wants(self, key: str, size: int) -> bool:
        """Relayed SYNOP text bulletins, unless both groups already hold the window."""
        return wants(key, size) and len(self._written) < 2

    def decode(self, key: str, body: bytes, arrived: dt.datetime | None) -> None:
        """Split a bulletin and decode its reports not seen yet (on the worker thread)."""
        title, reports = split_bulletin(body)
        if not title:
            self.counts["bulletins_without_heading"] += 1
            return
        self.counts["bulletins"] += 1
        reference = arrived or dt.datetime.now(dt.timezone.utc)
        stamp = np.datetime64(reference.astimezone(dt.timezone.utc).replace(tzinfo=None), "s")
        centre = key.split("/")[1]
        for section, report in reports:
            self.counts["reports"] += 1
            ident = identity(f"{section} {report}")
            if ident in self._undecodable:
                self.counts["copies"] += 1
                continue
            held = self.rows.get(ident)
            if held is not None:
                self.counts["copies"] += 1
                if stamp < held["arrival_time"]:
                    held.update(arrival_time=stamp, centre=centre, bulletin=title)
                continue
            row = decode_report(section, report, reference)
            if row is None:
                self.counts["nil_or_undecodable_reports"] += 1
                self._undecodable.add(ident)
                continue
            row.update(
                report=f"{section} {report}",
                bulletin=title,
                centre=centre,
                arrival_time=stamp,
                observation_identity=ident,
            )
            self.rows[ident] = row

    def _write_part(
        self, prefix: str, group: str, part: list[dict], station: str, partition: str
    ) -> dict[str, int]:
        """Drop one group's reports held already (as TAC or as BUFR); append the rest."""
        repo = self.config.icechunk_repo(prefix)
        ids = stored_identities(repo, group)
        fresh = [r for r in part if not _contains(ids, r["observation_identity"])]
        bufr_group = None if group == LAND_GROUP else "surface"
        held = bufr_reports(repo, bufr_group, station, BUFR_TAIL_ROWS[prefix])
        kept = [r for r in fresh if (r["station_id"], int(r["time"].astype("int64"))) not in held]
        if group == LAND_GROUP and kept:
            positions = land_positions(repo, BUFR_TAIL_ROWS[prefix])
            for row in kept:
                known = positions.get(row["station_id"])
                if known and not np.isfinite(row["latitude"]):
                    row["latitude"], row["longitude"], row["station_elevation"] = known
        written = write_group(repo, kept, group, partition, f"wis2_tac {group}")
        return {
            "reports": len(part),
            "already_stored_as_tac": len(part) - len(fresh),
            "held_as_bufr": len(fresh) - len(kept),
            "rows_written": written,
        }

    def write(self, window_start, window_end, partition: str) -> dict[str, Any]:
        """Drop reports held already (as TAC, or as BUFR) and append the rest."""
        rows, self.rows, self._undecodable = list(self.rows.values()), {}, set()
        land = [r for r in rows if r["kind"] != "BBXX"]
        ships = [r for r in rows if r["kind"] == "BBXX"]
        summary: dict[str, Any] = {**dict(self.counts)}
        self.counts = Counter()
        for prefix, group, part, station in (
            (LAND_STORE, LAND_GROUP, land, "station_id"),
            (OCEAN_STORE, SHIP_GROUP, ships, "call_sign"),
        ):
            if group in self._written:
                summary[group] = "already written"
                continue
            try:
                summary[group] = self._write_part(prefix, group, part, station, partition)
            except Exception as exc:  # noqa: BLE001 - one store must not lose the other
                logger.exception(f"wis2 tac {group}: write failed: {exc}")
                summary[group] = {"error": f"{type(exc).__name__}: {exc}"}
        return summary


def _contains(sorted_values: np.ndarray, value: int) -> bool:
    if not sorted_values.size:
        return False
    at = int(np.searchsorted(sorted_values, value))
    return at < sorted_values.size and int(sorted_values[at]) == value
