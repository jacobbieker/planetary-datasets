"""Aircraft observations (AMDAR) from the WIS2 Global Cache, as one Icechunk store.

WIS2 has no aircraft topic of its own yet: aircraft reports reach the Global Cache as GTS
bulletins relayed into WIS2, ``IUA``\\ *A2ii* BUFR bulletins under
``data/<relay>/data/core/I/U/A/<A2>/<ii>/<CCCC>/`` (in October 2026 only the JMA relay
carries them: ~5,000 bulletins and ~4 MB per 12 hours, about 50,000 reports, 5% of them
repeats; E-AMDAR via EGRR, then JMA, Hong Kong, Korea and New Zealand). Each bulletin holds
one or more BUFR messages, each a few reports (subsets), in one of two layouts:

* the AMDAR template 3-11-010 (identity, time, position, flight level and GNSS altitude,
  phase of flight, wind, temperature, humidity, icing, turbulence/EDR), or
* a legacy sequence (registration, position, time, flight level, wind, temperature,
  dewpoint, mixing ratio, phase of flight) from Hong Kong.

Both decode into the same rows; a field a layout lacks is missing. TAC aircraft reports
(``UA``/``UD`` AIREP and AMDAR bulletins) are not in the cache, and no other IU bulletin
type carries aircraft data (data category 4), so neither is parsed.

Rows go to ``bkr/obs/wis2_aircraft.icechunk`` along one ``observation`` dimension,
appended window by window (each WIS2 archive window is a ledger entry, so a window is
never written twice). A report relayed more than once (several bulletins, relays or
windows) is stored once: ``report_id`` hashes the aircraft, time, position and flight
level, and a report whose id is in the batch already or among the store's most recent
rows is dropped.

Values are as reported, in SI/CF units (K, m s-1, Pa where pressure appears, m): floats
are stored as float32 only when that reproduces every value at its BUFR precision, so the
store is lossless. Code-table values are int16 with -1 for missing.

Registered as the ``aircraft`` archive target (see
:mod:`~planetary_datasets.providers.observations.wis2_targets`): fed by
:mod:`~planetary_datasets.providers.observations.wis2_archive` as each window streams, or
over an archived window::

    python -m planetary_datasets.providers.observations.wis2_archive \\
        --from-tar bkr/obs/wis2/wis2_20261027T1200Z.tar --targets aircraft
    python -m planetary_datasets.providers.observations.wis2_archive --all-archived --targets aircraft
"""

from __future__ import annotations

import datetime as dt
import hashlib
import re
from dataclasses import dataclass, field
from typing import Any, Sequence

import numpy as np
import xarray as xr
from loguru import logger

from planetary_datasets.config import Config, get_config
from planetary_datasets.providers.observations.wis2_targets import (
    QueuedTarget,
    register,
    rows_from_bufr,
    split_bufr,
)

STORE_PREFIX = "bkr/obs/wis2_aircraft.icechunk"
#: GTS-relayed IUA bulletins: aircraft reports in BUFR.
AIRCRAFT_KEY = re.compile(
    r"^data/(?P<relay>[^/]+)/data/core/I/U/A/(?P<a2>.)/(?P<ii>\d\d)/(?P<cccc>\w{4})/"
)
#: Recent stored rows a new report's id is checked against. Duplicates come from relays and
#: re-issued bulletins within a day or so; a window holds ~100k reports at most.
DEDUP_TAIL = 1_000_000
CODE_FILL = -1
INT_FILL = -2147483648


@dataclass(frozen=True)
class Field:
    """One BUFR element and how it is stored."""

    key: str
    name: str
    kind: str  # "str", "int", "code" or "float"
    attrs: dict = field(default_factory=dict)


FIELDS: tuple[Field, ...] = (
    Field(
        "aircraftRegistrationNumberOrOtherIdentification",
        "aircraft_identifier",
        "str",
        {"long_name": "aircraft registration number or other identification"},
    ),
    Field("aircraftFlightNumber", "flight_number", "str", {"long_name": "aircraft flight number"}),
    Field("aircraftTailNumber", "tail_number", "str", {"long_name": "aircraft tail number"}),
    Field("originationAirport", "origin_airport", "str", {"long_name": "origination airport"}),
    Field("destinationAirport", "destination_airport", "str", {"long_name": "destination airport"}),
    Field(
        "observationSequenceNumber",
        "observation_sequence_number",
        "int",
        {"long_name": "observation sequence number"},
    ),
    Field("latitude", "latitude", "float", {"units": "degrees_north", "standard_name": "latitude"}),
    Field(
        "longitude", "longitude", "float", {"units": "degrees_east", "standard_name": "longitude"}
    ),
    Field(
        "flightLevel",
        "flight_level",
        "int",
        {"units": "m", "long_name": "flight level (pressure altitude, BUFR 0 07 010)"},
    ),
    Field(
        "globalNavigationSatelliteSystemAltitude",
        "gnss_altitude",
        "int",
        {"units": "m", "long_name": "GNSS altitude"},
    ),
    Field(
        "detailedPhaseOfFlight",
        "detailed_phase_of_flight",
        "code",
        {"long_name": "detailed phase of flight (BUFR code table 0 08 009)"},
    ),
    Field(
        "phaseOfAircraftFlight",
        "phase_of_flight",
        "code",
        {"long_name": "phase of aircraft flight (BUFR code table 0 08 004)"},
    ),
    Field(
        "windDirection",
        "wind_direction",
        "int",
        {"units": "degree", "standard_name": "wind_from_direction"},
    ),
    Field("windSpeed", "wind_speed", "float", {"units": "m s-1", "standard_name": "wind_speed"}),
    Field(
        "aircraftRollAngleQuality",
        "roll_angle_quality",
        "code",
        {"long_name": "aircraft roll angle quality (BUFR code table 0 02 064)"},
    ),
    Field(
        "aircraftTrueAirspeed",
        "true_airspeed",
        "float",
        {"units": "m s-1", "long_name": "aircraft true airspeed"},
    ),
    Field(
        "aircraftGroundSpeedUComponent",
        "ground_velocity_u",
        "float",
        {"units": "m s-1", "long_name": "aircraft ground speed u-component"},
    ),
    Field(
        "aircraftGroundSpeedVComponent",
        "ground_velocity_v",
        "float",
        {"units": "m s-1", "long_name": "aircraft ground speed v-component"},
    ),
    Field(
        "aircraftGroundSpeedWComponent",
        "ground_velocity_w",
        "float",
        {"units": "m s-1", "long_name": "aircraft ground speed w-component"},
    ),
    Field(
        "aircraftTrueHeading",
        "true_heading",
        "int",
        {"units": "degree", "long_name": "aircraft true heading"},
    ),
    Field(
        "airTemperature", "temperature", "float", {"units": "K", "standard_name": "air_temperature"}
    ),
    Field(
        "dewpointTemperature",
        "dewpoint_temperature",
        "float",
        {"units": "K", "standard_name": "dew_point_temperature"},
    ),
    Field(
        "mixingRatio",
        "humidity_mixing_ratio",
        "float",
        {"units": "kg kg-1", "standard_name": "humidity_mixing_ratio"},
    ),
    Field(
        "relativeHumidity",
        "relative_humidity",
        "float",
        {"units": "%", "standard_name": "relative_humidity"},
    ),
    Field(
        "aircraftHumiditySensors",
        "humidity_sensor",
        "code",
        {"long_name": "aircraft humidity sensors (BUFR code table 0 02 170)"},
    ),
    Field(
        "moistureQuality",
        "moisture_quality",
        "code",
        {"long_name": "moisture quality (BUFR code table 0 33 026)"},
    ),
    Field(
        "airframeIcingPresent",
        "airframe_icing",
        "code",
        {"long_name": "airframe icing present (BUFR code table 0 20 042)"},
    ),
    Field(
        "peakLiquidWaterContent",
        "peak_liquid_water_content",
        "float",
        {"units": "kg m-3", "long_name": "peak liquid water content"},
    ),
    Field(
        "averageLiquidWaterContent",
        "mean_liquid_water_content",
        "float",
        {"units": "kg m-3", "long_name": "average liquid water content"},
    ),
    Field(
        "supercooledLargeDropletConditions",
        "supercooled_large_droplets",
        "code",
        {"long_name": "supercooled large droplet conditions (BUFR code table 0 20 045)"},
    ),
    Field(
        "acarsInterpolatedValuesIndicator",
        "interpolated_values_indicator",
        "code",
        {"long_name": "ACARS interpolated values indicator (BUFR code table 0 33 025)"},
    ),
    Field(
        "turbulenceIndex",
        "turbulence_index",
        "code",
        {"long_name": "turbulence index (BUFR code table 0 11 077)"},
    ),
    Field(
        "meanTurbulenceIntensityEddyDissipationRate",
        "mean_eddy_dissipation_rate",
        "float",
        {"units": "m2/3 s-1", "long_name": "mean turbulence intensity (eddy dissipation rate)"},
    ),
    Field(
        "peakTurbulenceIntensityEddyDissipationRate",
        "peak_eddy_dissipation_rate",
        "float",
        {"units": "m2/3 s-1", "long_name": "peak turbulence intensity (eddy dissipation rate)"},
    ),
    Field(
        "reportingIntervalOrAveragingTimeForEddyDissipationRate",
        "eddy_dissipation_rate_interval",
        "int",
        {"units": "s", "long_name": "averaging time for eddy dissipation rate"},
    ),
    Field(
        "verticalGustVelocity",
        "derived_vertical_gust_velocity",
        "float",
        {"units": "m s-1", "long_name": "vertical gust velocity"},
    ),
    Field(
        "verticalGustAcceleration",
        "vertical_gust_acceleration",
        "float",
        {"units": "m s-2", "long_name": "vertical gust acceleration"},
    ),
    Field(
        "maximumDerivedEquivalentVerticalGustSpeed",
        "maximum_equivalent_vertical_gust_speed",
        "float",
        {"units": "m s-1", "long_name": "maximum derived equivalent vertical gust speed"},
    ),
)
TIME_KEYS = ("year", "month", "day", "hour", "minute", "second")
BY_KEY = {f.key: f for f in FIELDS}
#: The first element of every layout: where a new report (subset) starts.
SUBSET_START = "aircraftRegistrationNumberOrOtherIdentification"
WANTED = set(BY_KEY) | set(TIME_KEYS)


def is_aircraft(key: str) -> bool:
    """Whether a cache key is an aircraft (IUA) bulletin."""
    return AIRCRAFT_KEY.match(key) is not None


def decode_message(message: bytes) -> list[dict]:
    """The reports (one per subset) of one BUFR message, keyed by ecCodes element name."""
    return rows_from_bufr(message, WANTED, subset_start=SUBSET_START)


def report_id(row: dict) -> int:
    """A report's identity: aircraft, time, position (1e-4 degree) and flight level."""
    lat, lon = row.get("latitude"), row.get("longitude")
    text = "|".join(
        str(part)
        for part in (
            (row.get(SUBSET_START) or "").strip(),
            row["time"],
            None if lat is None else round(lat, 4),
            None if lon is None else round(lon, 4),
            row.get("flightLevel"),
        )
    )
    return int.from_bytes(
        hashlib.blake2b(text.encode(), digest_size=8).digest(), "big", signed=True
    )


@dataclass
class Batch:
    """Reports decoded from a set of bulletins."""

    rows: list[dict] = field(default_factory=list)
    bulletins: int = 0
    messages: int = 0
    failed: int = 0
    dropped: int = 0

    def add(self, key: str, body: bytes, arrived: dt.datetime | None = None) -> None:
        """Decode one bulletin; a message that fails is counted and skipped."""
        match = AIRCRAFT_KEY.match(key)
        if match is None:
            return
        self.bulletins += 1
        bulletin = key.rsplit("/", 1)[-1].split("_C_")[0]
        for message in split_bufr(body):
            self.messages += 1
            try:
                reports = decode_message(message)
            except Exception as exc:  # noqa: BLE001 - one bad message must not stop a window
                self.failed += 1
                if self.failed <= 20:
                    logger.warning(f"wis2 aircraft: {key}: undecodable message ({exc})")
                continue
            for report in reports:
                when = _time(report)
                if (
                    when is None
                    or report.get("latitude") is None
                    or report.get("longitude") is None
                ):
                    self.dropped += 1
                    continue
                report.update(
                    time=when,
                    arrival_time=arrived,
                    source_key=key,
                    bulletin=bulletin,
                    relay=match["relay"],
                    originating_centre=match["cccc"],
                )
                self.rows.append(report)


def _time(row: dict) -> np.datetime64 | None:
    parts = [row.get(k) for k in TIME_KEYS]
    if any(p is None for p in parts[:5]):
        return None
    try:
        return np.datetime64(dt.datetime(*parts[:5], int(parts[5] or 0)).replace(tzinfo=None), "s")
    except (ValueError, TypeError):
        return None


def _float_column(values: list) -> np.ndarray:
    """float32 when it reproduces every value to 10 significant digits, else float64.

    ecCodes scales BUFR integers into doubles (278.4 K arrives as 278.40000000000003); ten
    digits is past any BUFR element's precision, so a float32 that prints back as the same
    ten-digit value loses nothing.
    """
    wide = np.array([np.nan if v is None else v for v in values], dtype="float64")
    narrow = wide.astype("float32")
    exact = all(
        float(str(n)) == float(f"{w:.10g}") for w, n in zip(wide.tolist(), narrow) if np.isfinite(w)
    )
    return narrow if exact else wide


#: Times are stored as these (int64, CF-decoded by xarray), so every window's values go
#: to the store as they are rather than re-encoded against units chosen by the first.
TIME_UNITS = "seconds since 1970-01-01 00:00:00"
TIME_FILL = int(np.iinfo("int64").min)


def _seconds(values: list, attrs: dict) -> xr.Variable:
    stamps = np.array(
        [np.datetime64("NaT", "s") if v is None else np.datetime64(v, "s") for v in values],
        dtype="datetime64[s]",
    )
    seconds = stamps.astype("int64")
    seconds[np.isnat(stamps)] = TIME_FILL
    return xr.Variable(
        "observation",
        seconds,
        {**attrs, "units": TIME_UNITS, "calendar": "proleptic_gregorian", "_FillValue": TIME_FILL},
    )


def to_dataset(rows: Sequence[dict]) -> xr.Dataset:
    """Reports as one ``observation`` dimension."""
    dim = "observation"
    variables: dict[str, xr.Variable] = {
        "report_id": xr.Variable(
            dim,
            np.array([r["report_id"] for r in rows], dtype="int64"),
            {"long_name": "hash of aircraft, time, position and flight level (deduplication)"},
        ),
        "time": _seconds(
            [r["time"] for r in rows], {"standard_name": "time", "long_name": "observation time"}
        ),
        "arrival_time": _seconds(
            [
                None
                if r.get("arrival_time") is None
                else r["arrival_time"].astimezone(dt.timezone.utc).replace(tzinfo=None)
                for r in rows
            ],
            {"long_name": "time the bulletin arrived in the WIS2 Global Cache"},
        ),
    }
    for name in ("source_key", "bulletin", "relay", "originating_centre"):
        variables[name] = xr.Variable(dim, np.array([r[name] for r in rows], dtype=object))
    for f in FIELDS:
        values = [r.get(f.key) for r in rows]
        if f.kind == "str":
            data = np.array(["" if v is None else str(v) for v in values], dtype=object)
            attrs = dict(f.attrs)
        elif f.kind == "float":
            data = _float_column(values)
            attrs = dict(f.attrs)
        else:
            fill, dtype = (CODE_FILL, "int16") if f.kind == "code" else (INT_FILL, "int32")
            data = np.array([fill if v is None else v for v in values], dtype=dtype)
            attrs = {**f.attrs, "_FillValue": fill}
        variables[f.name] = xr.Variable(dim, data, attrs)
    ds = xr.Dataset(variables)
    ds.attrs.update(
        title="Aircraft observations (AMDAR) from the WIS2 Global Cache",
        source="IUA BUFR bulletins relayed from the GTS into WIS2 (s3://wis2globalcache)",
        featureType="point",
    )
    return ds


# ----------------------------------------------------------------------------- store


def stored_ids(repo, tail: int = DEDUP_TAIL) -> set[int]:
    """``report_id`` of the store's most recent ``tail`` rows."""
    import zarr

    try:
        group = zarr.open_group(repo.readonly_session("main").store, mode="r")
    except Exception:  # noqa: BLE001 - no store yet
        return set()
    if "report_id" not in group:
        return set()
    ids = group["report_id"]
    return set(ids[max(0, ids.shape[0] - tail) :].tolist())


def write(batch: Batch, partition: str, config: Config | None = None) -> int:
    """Append ``batch``'s new reports as ledger entry ``partition``. Returns rows written."""
    from planetary_datasets.common.multidim import append_partition, ledger, mark_written

    config = config or get_config()
    repo = config.icechunk_repo(STORE_PREFIX)
    if partition in ledger(repo):
        logger.info(f"wis2 aircraft: {partition} already written")
        return 0
    seen = stored_ids(repo)
    rows = []
    for row in batch.rows:
        row["report_id"] = report_id(row)
        if row["report_id"] in seen:
            continue
        seen.add(row["report_id"])
        rows.append(row)
    duplicates = len(batch.rows) - len(rows)
    logger.info(
        f"wis2 aircraft {partition}: {batch.bulletins:,} bulletin(s), {batch.messages:,} "
        f"message(s) ({batch.failed} undecodable), {len(batch.rows):,} report(s), "
        f"{duplicates:,} duplicate(s), {batch.dropped} without time or position"
    )
    if not rows:
        if ledger(repo) or stored_ids(repo):
            mark_written(repo, partition, label="wis2 aircraft")
        return 0
    rows.sort(key=lambda r: (r["time"], r["report_id"]))
    append_partition(repo, to_dataset(rows), partition, label="wis2 aircraft")
    return len(rows)


@register("aircraft")
class AircraftTarget(QueuedTarget):
    """The ``aircraft`` archive target: IUA bulletins into :data:`STORE_PREFIX`."""

    store_prefix = STORE_PREFIX

    def __init__(self, config: Config | None):
        super().__init__(config)
        self.batch = Batch()

    def wants(self, key: str, size: int) -> bool:
        return is_aircraft(key)

    def decode(self, key: str, body: bytes, arrived: dt.datetime | None) -> None:
        self.batch.add(key, body, arrived)

    def write(
        self, window_start: dt.datetime | None, window_end: dt.datetime | None, partition: str
    ) -> dict[str, Any]:
        batch, self.batch = self.batch, Batch()
        written = write(batch, partition, self.config)
        return {
            "bulletins": batch.bulletins,
            "messages": batch.messages,
            "undecodable_messages": batch.failed,
            "reports": len(batch.rows),
            "rows": written,
        }
