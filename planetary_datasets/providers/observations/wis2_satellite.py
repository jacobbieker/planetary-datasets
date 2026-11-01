"""Satellite observations from the WIS2 Global Cache, as one grouped Icechunk store.

Sounder radiances, one group per instrument, as below; every other satellite product
(GNSS radio occultation, AMVs, altimetry, scatterometer and SAR winds, AMI clear-sky
radiances, OLCI water vapour) one group per product, see
:mod:`~planetary_datasets.providers.observations.wis2_satellite_products`. Every row of
every group carries ``arrival_time`` (when the copy kept reached the cache) and
``first_arrival_time`` (the earliest arrival of the copies collapsed into it), so the live
stream can be replayed; rows written before ``first_arrival_time`` was added read NaT.

Radiances:

An archive target of :mod:`planetary_datasets.providers.observations.wis2_archive`: as each
12-hour window of the cache streams into its tar, the space-based observation objects of
the instruments below are decoded with ecCodes and appended to
``bkr/obs/wis2_satellite.icechunk``, one zarr group per instrument::

    xr.open_datatree(store, engine="zarr", consolidated=False)    # every instrument
    xr.open_zarr(store, group="atms", consolidated=False)          # one

==========  ==========================================  ==========================
Group       Instruments (WIS2 topic directory)          Values per channel
==========  ==========================================  ==========================
atms        ATMS (``atms``)                             brightness_temperature (K)
amsua       AMSU-A (``amsu-a``)                         brightness_temperature (K)
mhs         MHS (``mhs``)                               brightness_temperature (K)
mwhs2       FY-3 MWHS-2 (``mwhs-2``)                    brightness_temperature (K)
mwts2       FY-3 MWTS-2 (``mwts-2``)                    brightness_temperature (K)
mws         Metop-SG MWS (``mws``)                      brightness_temperature (K)
cris        CrIS (``cris``)                             radiance (W m-2 sr-1 m)
iasi        IASI L1C (``iasi``)                         scaled_radiance, radiance_scale_factor
==========  ==========================================  ==========================

Each group has an ``observation`` dimension (one field of view) and a ``channel``
dimension (index = channel number - 1, so every source's channels line up; a source that
sends a subset leaves the others as fill). Per observation: ``time``, ``latitude``,
``longitude``, ``satellite_identifier`` (BUFR C-5), ``orbit_number``,
``scan_line_number``, ``field_of_view_number`` (and ``field_of_regard_number`` for CrIS),
``satellite_zenith_angle``, ``satellite_azimuth_angle``, ``solar_zenith_angle``,
``solar_azimuth_angle``, instrument quality flags where the template has them, and
provenance: ``source_centre``, ``source_key``, ``arrival_time``, ``observation_identity``.
Each group also holds the channel table: ``channel_number`` and, where the BUFR gives it
per channel, ``channel_centre_frequency`` (Hz) or ``channel_central_wavenumber`` (m-1).

Values are the BUFR values. IASI radiances are kept as BUFR sends them, integers with a
per-band power-of-ten scale: ``radiance = scaled_radiance * 10 ** -radiance_scale_factor``
in W m-2 sr-1 m (per unit wavenumber in m-1); both are integers, so nothing is lost and the
8,700-channel spectra stay two bytes a value. Floats are float32 when every value,
rounded to its BUFR decimal places, reads back unchanged, else float64.

The same field of view reaches the cache several times: from several DBNet stations
receiving the same pass, from EUMETSAT and NOAA/CIMSS relays, and again in a later window.
Its identity is the satellite, time (to 0.1 s), position (to 0.01 degrees) and field of
view number; within a window the copy with the most channels is kept (a CIMSS full
spectrum beats a Météo-France subset), and one already stored is dropped.

Not parsed (counted, and skipped cheaply by key): IASI and AVHRR L2 products (trace gases,
clouds: ``*_l2*``) and the GTS radiance bulletins (``IE*``, ``IN*`` other than the AMV
headings), which repeat what the topics above carry.

The tar stream never waits on decoding: :meth:`SatelliteTarget.feed` writes each object to
a spool directory and hands its path to a process pool; workers decode to per-object
``.npz`` files; :meth:`SatelliteTarget.finish` deduplicates and appends them in batches.
"""

from __future__ import annotations

import concurrent.futures
import dataclasses
import datetime as dt
import multiprocessing
import os
import pathlib
import re
import shutil
import tempfile
import time
from collections import Counter
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.config import Config, get_config
from planetary_datasets.providers.observations import wis2_satellite_products as products
from planetary_datasets.providers.observations.wis2_targets import (
    ArchiveTarget,
    register,
    split_bufr,
)

STORE_PREFIX = "bkr/obs/wis2_satellite.icechunk"
TOPIC = re.compile(
    r"^data/(?P<centre>[^/]+)/data/core/weather/(?:experimental/)?space-based-observations/"
    r"(?P<platform>[^/]+)/(?P<instrument>[^/]+)/(?P<name>[^/]+)$"
)
#: Products under an instrument's topic that are not its radiances.
NOT_RADIANCES = re.compile(r"(_l2|_amv|AMV|_EDR)", re.IGNORECASE)
#: Decoding processes (``WIS2_SATELLITE_WORKERS``).
WORKERS = int(os.environ.get("WIS2_SATELLITE_WORKERS", "8"))
#: Rows times channels per append, bounding memory while writing.
BATCH_VALUES = 40_000_000
#: Groups written at once (each commits on its own, rebasing over the others).
WRITE_THREADS = 4
#: Channel numbers past this are not channels (a missing value, a stray element).
MAX_CHANNEL = 20_000
#: Stored identities read back per group to drop repeats from earlier windows.
RECENT_IDENTITIES = 5_000_000
#: Rows past this many valid channels are always preferred; used to rank copies.
MISSING_LONG = 2147483647


@dataclasses.dataclass(frozen=True)
class Instrument:
    """How one instrument's BUFR maps to a group."""

    group: str
    topics: tuple[str, ...]
    channel_key: str
    #: (BUFR key, variable, units) of the per-channel values.
    values: tuple[tuple[str, str, str], ...]
    #: (BUFR key, variable, units, scale to SI) of per-channel metadata, constant per channel.
    channel_info: tuple[tuple[str, str, str, float], ...] = ()
    #: (BUFR key, variable) of extra per-observation keys (quality flags, ...).
    extra: tuple[tuple[str, str], ...] = ()
    #: IASI: values are scaled integers with per-band scale factors.
    scaled: bool = False
    #: ATOVS numbers AMSU-A 28-42 and MHS 43-47 across HIRS/MSU/AMSU; channels here are
    #: numbered from 1 in message order and the ATOVS number kept in the channel table.
    renumber: bool = False


INSTRUMENTS = {
    i.group: i
    for i in (
        Instrument(
            "atms",
            ("atms",),
            "channelNumber",
            (("brightnessTemperature", "brightness_temperature", "K"),),
            (("satelliteChannelCentreFrequency", "channel_centre_frequency", "Hz", 1.0),),
            (
                ("granuleLevelQualityFlags", "granule_quality_flags"),
                ("scanLevelQualityFlags", "scan_quality_flags"),
                ("geolocationQuality", "geolocation_quality"),
            ),
        ),
        Instrument(
            renumber=True,
            group="amsua",
            topics=("amsu-a",),
            channel_key="tovsOrAtovsOrAvhrrInstrumentationChannelNumber",
            values=(("brightnessTemperature", "brightness_temperature", "K"),),
            extra=(
                ("scanLineQualityFlagsForAtovs", "scan_quality_flags"),
                ("fieldOfViewQualityFlagsForAtovs", "field_of_view_quality_flags"),
            ),
        ),
        Instrument(
            renumber=True,
            group="mhs",
            topics=("mhs",),
            channel_key="tovsOrAtovsOrAvhrrInstrumentationChannelNumber",
            values=(("brightnessTemperature", "brightness_temperature", "K"),),
            extra=(
                ("scanLineQualityFlagsForAtovs", "scan_quality_flags"),
                ("fieldOfViewQualityFlagsForAtovs", "field_of_view_quality_flags"),
            ),
        ),
        Instrument(
            "mwhs2",
            ("mwhs-2",),
            "channelNumber",
            (("brightnessTemperature", "brightness_temperature", "K"),),
            (),
            (("surfaceFlag", "surface_flag"), ("rainFlag", "rain_flag")),
        ),
        Instrument(
            "mwts2",
            ("mwts-2",),
            "channelNumber",
            (("brightnessTemperature", "brightness_temperature", "K"),),
            (),
            (("surfaceFlag", "surface_flag"), ("rainFlag", "rain_flag")),
        ),
        Instrument(
            "mws",
            ("mws",),
            "channelNumber",
            (("brightnessTemperature", "brightness_temperature", "K"),),
            (("satelliteChannelCentreFrequency", "channel_centre_frequency", "Hz", 1.0),),
            (
                ("mwsOverallQualityFlags", "overall_quality_flags"),
                ("mwsScanLineProcessingFlags", "scan_line_processing_flags"),
                ("mwsNavigationStatusFlags", "navigation_status_flags"),
                ("landFraction", "land_fraction"),
            ),
        ),
        Instrument(
            "cris",
            ("cris",),
            "channelNumber",
            (("channelRadiance", "radiance", "W m-2 sr-1 m"),),
            (),
            (
                ("fieldOfRegardNumber", "field_of_regard_number"),
                ("scanLevelQualityFlags", "scan_quality_flags"),
                ("geolocationQuality", "geolocation_quality"),
                ("landFraction", "land_fraction"),
            ),
        ),
        Instrument(
            "iasi",
            ("iasi",),
            "channelNumber",
            (("scaledIasiRadiance", "scaled_radiance", "1"),),
            (),
            (
                ("gqisFlagQual", "gqis_quality_flag"),
                ("gqisQualIndex", "gqis_quality_index"),
                ("majorFrameCount", "major_frame_count"),
            ),
            scaled=True,
        ),
    )
}
BY_TOPIC = {topic: i for i in INSTRUMENTS.values() for topic in i.topics}

#: Per-observation keys every template has: (BUFR key, variable).
COMMON = (
    ("satelliteIdentifier", "satellite_identifier"),
    ("orbitNumber", "orbit_number"),
    ("scanLineNumber", "scan_line_number"),
    ("fieldOfViewNumber", "field_of_view_number"),
    ("latitude", "latitude"),
    ("longitude", "longitude"),
    ("satelliteZenithAngle", "satellite_zenith_angle"),
    ("bearingOrAzimuth", "satellite_azimuth_angle"),
    ("solarZenithAngle", "solar_zenith_angle"),
    ("solarAzimuth", "solar_azimuth_angle"),
)
TIME_KEYS = ("year", "month", "day", "hour", "minute", "second")
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
UNITS = {
    "latitude": "degrees_north",
    "longitude": "degrees_east",
    "satellite_zenith_angle": "degree",
    "satellite_azimuth_angle": "degree",
    "solar_zenith_angle": "degree",
    "solar_azimuth_angle": "degree",
    "land_fraction": "1",
}


def instrument_for(key: str) -> Instrument | None:
    """The instrument whose radiances the object at ``key`` holds, or None."""
    match = TOPIC.match(key)
    if match is None or NOT_RADIANCES.search(match["name"]):
        return None
    return BY_TOPIC.get(match["instrument"])


# ----------------------------------------------------------------------------- decoding


def _exists(handle, key: str) -> bool:
    import eccodes

    return bool(eccodes.codes_is_defined(handle, key))


def _occurrences(handle, key: str, cache: dict) -> int:
    """How many times ``key`` occurs in each subset (binary search on ``#n#key``)."""
    if key in cache:
        return cache[key]
    if not _exists(handle, f"#1#{key}"):
        cache[key] = 0
        return 0
    low, high = 1, 2
    while _exists(handle, f"#{high}#{key}"):
        low, high = high, high * 2
    while high - low > 1:
        middle = (low + high) // 2
        low, high = (middle, high) if _exists(handle, f"#{middle}#{key}") else (low, middle)
    cache[key] = low
    return low


def _array(handle, name: str, subsets: int) -> np.ndarray:
    """One occurrence's values for every subset (a constant one is broadcast)."""
    import eccodes

    values = np.atleast_1d(eccodes.codes_get_array(handle, name))
    if values.size == 1 and subsets > 1:
        values = np.repeat(values, subsets)
    return values


def _clean(values: np.ndarray) -> np.ndarray:
    """ecCodes' missing markers as NaN (floats) or the integer missing value."""
    if values.dtype.kind == "f":
        return np.where(values <= -1e99, np.nan, values)
    return values.astype("int64")


def _decimals(handle, name: str) -> int:
    import eccodes

    try:
        return max(0, int(eccodes.codes_get(handle, f"{name}->scale")))
    except Exception:  # noqa: BLE001 - not every element has a scale key
        return 6


def _channel_values(handle, key: str, count: int, subsets: int, cache: dict) -> np.ndarray:
    """``(subsets, count)`` values of the first ``count`` occurrences of ``key``."""
    import eccodes

    whole = np.atleast_1d(eccodes.codes_get_array(handle, key))
    total = _occurrences(handle, key, cache)
    if whole.size == total * subsets:
        return whole.reshape(total, subsets)[:count].T
    return np.stack([_array(handle, f"#{i}#{key}", subsets) for i in range(1, count + 1)], 1)


def _iasi_scales(handle, channels: np.ndarray, subsets: int) -> np.ndarray:
    """``(subsets, channels)`` power-of-ten scale factors from the template's band table.

    Template 340007/340008 carries three quality bands, then up to ten scale bands of
    (start channel, end channel, scale factor): ``startChannel``/``endChannel``
    occurrences 4-13 paired with ``channelScaleFactor`` occurrences 1-10.
    """
    scales = np.full((subsets, channels.size), MISSING_LONG, dtype="int64")
    for band in range(1, 11):
        names = (f"#{band + 3}#startChannel", f"#{band + 3}#endChannel")
        if not all(_exists(handle, n) for n in names) or not _exists(
            handle, f"#{band}#channelScaleFactor"
        ):
            break
        start, end = (_array(handle, n, subsets) for n in names)
        factor = _array(handle, f"#{band}#channelScaleFactor", subsets)
        valid = (start != MISSING_LONG) & (end != MISSING_LONG) & (factor != MISSING_LONG)
        if not valid.any():
            continue
        inside = (channels[None, :] >= start[:, None]) & (channels[None, :] <= end[:, None])
        inside &= valid[:, None]
        scales = np.where(inside, factor[:, None], scales)
    return scales


def decode_message(message: bytes, instrument: Instrument) -> dict[str, np.ndarray] | None:
    """One BUFR message as arrays: per-observation columns and ``(obs, channel)`` values.

    None when the message is not this instrument's radiances (no channel values).
    """
    import eccodes

    handle = eccodes.codes_new_from_message(message)
    try:
        eccodes.codes_set(handle, "unpack", 1)
        subsets = int(eccodes.codes_get(handle, "numberOfSubsets"))
        cache: dict[str, int] = {}
        value_key = instrument.values[0][0]
        count = min(
            _occurrences(handle, instrument.channel_key, cache),
            _occurrences(handle, value_key, cache),
        )
        # The channel loop's own replication count: later occurrences of the same
        # elements belong to other sequences (CrIS's VIIRS cluster statistics).
        loop = "#1#extendedDelayedDescriptorReplicationFactor"
        if count and _exists(handle, loop):
            replicated = int(np.atleast_1d(eccodes.codes_get_array(handle, loop))[0])
            if 0 < replicated < count:
                count = replicated
        if count == 0:
            return None
        out: dict[str, np.ndarray] = {}
        whole = np.atleast_1d(eccodes.codes_get_array(handle, instrument.channel_key))
        if whole.size == _occurrences(handle, instrument.channel_key, cache):
            channels = whole[:count].astype("int64")
        else:
            channels = np.array(
                [
                    int(_array(handle, f"#{i}#{instrument.channel_key}", subsets)[0])
                    for i in range(1, count + 1)
                ]
            )
        # A missing channel number (or a stray element past the loop) is not a channel.
        good = (channels > 0) & (channels <= MAX_CHANNEL)
        if not good.any():
            return None
        if instrument.renumber:
            out["channel_atovs_number"] = channels[good].astype("float64")
        out["channel_number"] = (
            np.arange(1, int(good.sum()) + 1) if instrument.renumber else channels[good]
        )
        for key, name, _ in instrument.values:
            raw = _clean(_channel_values(handle, key, count, subsets, cache))[:, good]
            if instrument.scaled and raw.dtype.kind == "f":
                raw = np.where(np.isnan(raw), MISSING_LONG, raw).astype("int64")
            out[name] = raw
            if raw.dtype.kind == "f":
                out[f"{name}.decimals"] = np.array(_decimals(handle, f"#1#{key}"))
        if instrument.scaled:
            out["radiance_scale_factor"] = _iasi_scales(handle, channels[good], subsets)
        for key, name, _, factor in instrument.channel_info:
            if _occurrences(handle, key, cache) >= count:
                info = [_array(handle, f"#{i}#{key}", subsets)[0] for i in range(1, count + 1)]
                out[name] = _clean(np.asarray(info, dtype="float64")) * factor
        stamp = {k: _clean(_array(handle, f"#1#{k}", subsets)) for k in TIME_KEYS}
        seconds = np.asarray(stamp["second"], dtype="float64")
        whole = np.floor(np.nan_to_num(seconds)).astype("int64")
        times = pd.to_datetime(
            {
                "year": stamp["year"],
                "month": stamp["month"],
                "day": stamp["day"],
                "hour": stamp["hour"],
                "minute": stamp["minute"],
                "second": whole,
            },
            errors="coerce",
        ).to_numpy("datetime64[ms]")
        times = times + np.round((np.nan_to_num(seconds) - whole) * 1000).astype("timedelta64[ms]")
        out["time"] = times
        for key, name in COMMON + instrument.extra:
            if _exists(handle, f"#1#{key}"):
                column = _clean(_array(handle, f"#1#{key}", subsets))
                out[name] = column
                if column.dtype.kind == "f":
                    out[f"{name}.decimals"] = np.array(_decimals(handle, f"#1#{key}"))
        out["originating_centre"] = np.full(
            subsets, int(eccodes.codes_get(handle, "bufrHeaderCentre")), dtype="int64"
        )
        return out
    finally:
        eccodes.codes_release(handle)


def _concat(parts: list[dict]) -> dict[str, np.ndarray]:
    """Messages of one object joined, channels aligned to channel number - 1."""
    width = max(int(p["channel_number"].max()) for p in parts)
    joined: dict[str, list] = {}
    decimals: dict[str, int] = {}
    info: dict[str, np.ndarray] = {}
    for part in parts:
        index = part["channel_number"] - 1
        rows = part["time"].size
        for name, values in part.items():
            if name.endswith(".decimals"):
                decimals[name] = max(decimals.get(name, 0), int(values))
                continue
            if name == "channel_number":
                continue
            if values.ndim == 2:
                fill = np.nan if values.dtype.kind == "f" else MISSING_LONG
                wide = np.full((rows, width), fill, dtype=values.dtype)
                wide[:, index] = values
                values = wide
            elif name.startswith("channel_"):
                table = info.setdefault(name, np.full(width, np.nan))
                table[index] = values
                continue
            joined.setdefault(name, []).append(values)
    out = {name: np.concatenate(values) for name, values in joined.items()}
    out.update(info)
    out.update({k: np.array(v) for k, v in decimals.items()})
    out["channel_number"] = np.arange(1, width + 1)
    return out


def identities(satellite, times, latitude, longitude, fov) -> np.ndarray:
    """A field of view's identity: satellite, time (0.1 s), position (0.01 deg), FOV."""
    frame = pd.DataFrame(
        {
            "satellite": np.nan_to_num(np.asarray(satellite, dtype="float64"), nan=-1).astype(
                "int64"
            ),
            "time": (np.asarray(times).astype("datetime64[ms]").astype("int64") // 100),
            "latitude": np.round(np.nan_to_num(np.asarray(latitude, dtype="float64")) * 100).astype(
                "int64"
            ),
            "longitude": np.round(
                np.nan_to_num(np.asarray(longitude, dtype="float64")) * 100
            ).astype("int64"),
            "fov": np.nan_to_num(np.asarray(fov, dtype="float64"), nan=-1).astype("int64"),
        }
    )
    return pd.util.hash_pandas_object(frame, index=False).to_numpy().view("int64")


def decode_object(
    key: str, body: bytes, arrived: dt.datetime | None
) -> tuple[str | None, dict[str, np.ndarray] | None, Counter]:
    """One cache object: its group and arrays (None when nothing decodes), and counts."""
    counts: Counter = Counter()
    instrument = instrument_for(key)
    if instrument is None:
        product = products.product_for(key)
        if product is None:
            counts["skipped_objects"] += 1
            return None, None, counts
        arrays, counts = products.decode_object(key, body, product)
        if arrays is None:
            return product.group, None, counts
        _provenance(arrays, key, arrived)
        return product.group, arrays, counts
    parts = []
    for message in split_bufr(body):
        counts["messages"] += 1
        try:
            decoded = decode_message(message, instrument)
        except Exception:  # noqa: BLE001 - one bad message must not lose the object
            counts["undecodable_messages"] += 1
            continue
        if decoded is None or decoded["time"].size == 0:
            counts["messages_without_radiances"] += 1
            continue
        parts.append(decoded)
    if not parts:
        return instrument.group, None, counts
    arrays = _concat(parts)
    located = np.isfinite(arrays.get("latitude", np.full(arrays["time"].size, np.nan)))
    if not located.all():
        counts["observations_without_position"] += int((~located).sum())
        arrays = {
            name: values
            if name.startswith("channel_") or name.endswith(".decimals")
            else values[located]
            for name, values in arrays.items()
        }
    rows = arrays["time"].size
    if rows == 0:
        return instrument.group, None, counts
    _provenance(arrays, key, arrived)
    value = instrument.values[0][1]
    arrays["valid_channels"] = np.sum(
        ~np.isnan(arrays[value])
        if arrays[value].dtype.kind == "f"
        else arrays[value] != MISSING_LONG,
        axis=1,
    )
    arrays["observation_identity"] = identities(
        arrays.get("satellite_identifier", np.full(rows, -1)),
        arrays["time"],
        arrays.get("latitude", np.full(rows, np.nan)),
        arrays.get("longitude", np.full(rows, np.nan)),
        arrays.get("field_of_view_number", np.full(rows, -1)),
    )
    counts["observations"] += rows
    return instrument.group, arrays, counts


def _provenance(arrays: dict[str, np.ndarray], key: str, arrived: dt.datetime | None) -> None:
    """The object's key, centre and arrival time on every row."""
    rows = arrays["time"].size
    arrays["source_key"] = np.full(rows, key, dtype=object)
    arrays["source_centre"] = np.full(rows, key.split("/")[1], dtype=object)
    when = pd.Timestamp(arrived) if arrived is not None else None
    if when is not None and when.tzinfo is not None:
        when = when.tz_convert(None)
    stamp = np.datetime64(when, "s") if when is not None else np.datetime64("NaT", "s")
    arrays["arrival_time"] = np.full(rows, stamp, dtype="datetime64[s]")


def _compact(arrays: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
    """Per-channel arrays in the smallest exact type before they leave the worker.

    Floats become float32 where every value rounds back to its BUFR decimal; integers
    become int32, which holds ecCodes' missing marker (2**31 - 1) exactly.
    """
    out = dict(arrays)
    for name, values in arrays.items():
        if values.ndim != 2:
            continue
        if values.dtype.kind == "f":
            out[name] = _float(values, int(arrays.get(f"{name}.decimals", 6)))
        elif values.dtype.kind in "iu" and values.max(initial=0) <= MISSING_LONG:
            out[name] = values.astype("int32")
    return out


def decode_file(path: str, key: str, arrived: dt.datetime | None, out_dir: str) -> dict:
    """Process-pool entry: decode a spooled object to ``out_dir/<group>/<n>.npz``."""
    started = time.perf_counter()
    body = pathlib.Path(path).read_bytes()
    try:
        group, arrays, counts = decode_object(key, body, arrived)
    except Exception as exc:  # noqa: BLE001 - reported back, never raised
        return {"counts": {"undecodable_objects": 1}, "error": f"{type(exc).__name__}: {exc}"}
    finally:
        pathlib.Path(path).unlink(missing_ok=True)
    result: dict[str, Any] = {"counts": dict(counts), "seconds": time.perf_counter() - started}
    if arrays is not None:
        arrays = _compact(arrays)
        target = pathlib.Path(out_dir) / group
        target.mkdir(parents=True, exist_ok=True)
        stem = target / pathlib.Path(path).stem
        np.savez(stem, **{k.replace(".", "__"): v for k, v in arrays.items()}, allow_pickle=True)
        result.update(group=group, path=f"{stem}.npz", rows=int(arrays["time"].size))
    return result


# ----------------------------------------------------------------------------- writing


def _load(path: str) -> dict[str, np.ndarray]:
    with np.load(path, allow_pickle=True) as data:
        return {k.replace("__", "."): data[k] for k in data.files}


def _float(values: np.ndarray, decimals: int) -> np.ndarray:
    """float32 when every value rounds back to the same decimal, else float64."""
    narrow = values.astype("float32")
    with np.errstate(invalid="ignore"):
        same = np.array_equal(
            np.round(narrow.astype("float64"), decimals), np.round(values, decimals), equal_nan=True
        )
    return narrow if same else values


def _integer(values: np.ndarray) -> tuple[np.ndarray, int]:
    """The narrowest signed integer type holding ``values`` and a fill below them."""
    present = values[values != MISSING_LONG] if values.size else values
    low = int(present.min()) if present.size else 0
    high = int(present.max()) if present.size else 0
    for dtype in ("int8", "int16", "int32", "int64"):
        info = np.iinfo(dtype)
        if info.min < low and high <= info.max:
            return np.where(values == MISSING_LONG, info.min, values).astype(dtype), int(info.min)
    raise ValueError("integer out of range")


def to_dataset(batch: list[dict], width: int, instrument: Instrument) -> xr.Dataset:
    """A batch of decoded objects (already filtered) as one dataset on the group's axes."""
    names = [n for n in batch[0] if not n.endswith(".decimals") and not n.startswith("channel_")]
    data: dict[str, Any] = {}
    for name in names:
        columns = [b[name] for b in batch if name in b]
        if len(columns) != len(batch):
            continue
        if columns[0].ndim == 2:
            fill = np.nan if columns[0].dtype.kind == "f" else MISSING_LONG
            wide = []
            for c in columns:
                pad = np.full((c.shape[0], width), fill, dtype=c.dtype)
                pad[:, : c.shape[1]] = c
                wide.append(pad)
            values = np.concatenate(wide)
            dims = ("observation", "channel")
        else:
            values = np.concatenate(columns)
            dims = ("observation",)
        attrs: dict[str, Any] = {}
        decimals = max((int(b.get(f"{name}.decimals", 0)) for b in batch), default=0)
        if values.dtype.kind == "f":
            values = _float(values.astype("float64"), decimals)
            attrs["decimal_places"] = decimals
        elif values.dtype.kind in "iu":
            values, fill_value = _integer(values.astype("int64"))
            attrs["_FillValue"] = fill_value
        elif values.dtype.kind == "M":
            # Stored as integer counts with CF units, written like any other integer:
            # multidim's datetime path re-encodes appended rows wrongly.
            values, time_attrs = _time_counts(values, "ms" if name == "time" else "s", TIME_FILL)
            attrs.update(time_attrs)
        else:
            values = values.astype(object)
        units = {n: u for _, n, u in instrument.values}.get(name) or UNITS.get(name)
        if units:
            attrs["units"] = units
        attrs.update(ARRIVAL_ATTRS.get(name, {}))
        data[name] = xr.Variable(dims, values, attrs)
    return xr.Dataset(data)


CHANNEL_UNITS = {
    "channel_centre_frequency": "Hz",
    "channel_central_wavenumber": "m-1",
    "channel_atovs_number": "1",
}


def _channel_table(group, decoded: list[dict], width: int, instrument: Instrument) -> None:
    """Write (or widen) the group's ``channel_number`` and per-channel metadata."""
    tables = {"channel_number": np.arange(1, width + 1, dtype="int32")}
    names = {n for b in decoded for n in b if n.startswith("channel_") and n != "channel_number"}
    for name in sorted(names):
        column = np.full(width, np.nan)
        if name in group and group[name].shape[0]:
            stored = group[name][:]
            column[: min(stored.size, width)] = stored[:width]
        for b in decoded:
            if name in b:
                values = np.asarray(b[name], dtype="float64")[:width]
                head = column[: values.size]
                column[: values.size] = np.where(np.isnan(values), head, values)
        tables[name] = column
    if instrument.group == "iasi":
        tables["channel_central_wavenumber"] = 64500.0 + 25.0 * np.arange(width)
    for name, values in tables.items():
        if name in group and group[name].shape == values.shape:
            group[name][:] = values
        else:
            if name in group:
                del group[name]
            group.create_array(
                name, shape=values.shape, dtype=values.dtype, dimension_names=("channel",)
            )[:] = values
        if name in CHANNEL_UNITS:
            group[name].attrs["units"] = CHANNEL_UNITS[name]
        if name == "channel_central_wavenumber":
            group[name].attrs["comment"] = (
                "IASI spectral sampling: 645 cm-1 + 0.25 cm-1 per channel"
            )


def _stored_identities(repo, group: str) -> set[int]:
    import zarr

    try:
        node = zarr.open_group(repo.readonly_session("main").store, path=group, mode="r")
        array = node["observation_identity"]
    except Exception:  # noqa: BLE001 - a group not written yet has none
        return set()
    rows = array.shape[0]
    return set(array[max(0, rows - RECENT_IDENTITIES) :].tolist())


def write_group(
    repo, instrument: Instrument, paths: Sequence[str], partition: str | None, counts: Counter
) -> int:
    """Deduplicate and append one group's decoded objects. Returns rows written."""
    import icechunk
    import zarr

    from planetary_datasets.common.multidim import (
        append_partition,
        describe_variables,
        ledger,
        mark_written,
    )

    group = instrument.group
    if partition is not None and partition in ledger(repo, group=group):
        counts[f"{group}_already_written"] += 1
        return 0
    # Identities, channel counts and arrivals only, to choose the copy to keep.
    keys = []
    for index, path in enumerate(paths):
        with np.load(path, allow_pickle=True) as data:
            ids, valid = data["observation_identity"], data["valid_channels"]
            arrived = data["arrival_time"]
        keys.append(
            pd.DataFrame(
                {
                    "id": ids,
                    "valid": valid,
                    "arrival": arrived,
                    "file": index,
                    "row": np.arange(ids.size),
                }
            )
        )
    if not keys:
        return 0
    table = pd.concat(keys, ignore_index=True)
    counts[f"{group}_decoded"] += len(table)
    stored = _stored_identities(repo, group)
    table = table[~table["id"].isin(stored)]
    first_arrival = table.groupby("id")["arrival"].min()
    table = table.sort_values(["id", "valid"], ascending=[True, False]).drop_duplicates("id")
    counts[f"{group}_duplicates"] += counts[f"{group}_decoded"] - len(table)
    keep = {file: np.sort(rows["row"].to_numpy()) for file, rows in table.groupby("file")}

    written = 0
    batch: list[dict] = []
    width = 0
    values_in_batch = 0

    def flush() -> None:
        nonlocal batch, values_in_batch, written
        if not batch:
            return
        ds = to_dataset(batch, width, instrument)
        # The append widens every channel array first; the channel table follows it.
        append_partition(
            repo, ds, None, fixed_dims=["channel"], label=f"wis2_satellite {group}", group=group
        )
        session = repo.writable_session("main")
        node = zarr.open_group(session.store, path=group, mode="a")
        value = next(n for _, n, _ in instrument.values if n in node)
        _channel_table(node, batch, node[value].shape[1], instrument)
        node.attrs.update(
            {
                "title": f"WIS2 Global Cache {instrument.group} radiances",
                "instrument_topics": list(instrument.topics),
            }
        )
        session.commit(
            f"wis2_satellite {group}: channel table", rebase_with=icechunk.ConflictDetector()
        )
        written += ds.sizes["observation"]
        batch, values_in_batch = [], 0

    for file, rows in sorted(keep.items()):
        arrays = _load(paths[file])
        subset = {
            name: values
            if name.startswith("channel_") or name.endswith(".decimals")
            else values[rows]
            for name, values in arrays.items()
        }
        subset["first_arrival_time"] = _first_arrivals(first_arrival, subset)
        width = max(width, int(arrays["channel_number"].size))
        batch.append(subset)
        values_in_batch += rows.size * width
        if values_in_batch >= BATCH_VALUES:
            flush()
    flush()
    if partition is not None:
        try:
            mark_written(repo, partition, label=f"wis2_satellite {group}", group=group)
        except Exception as exc:  # noqa: BLE001 - group not created when nothing was new
            logger.debug(f"wis2_satellite {group}: ledger not updated ({exc})")
    if written:
        describe_variables(repo, ARRIVAL_ATTRS, label=f"wis2_satellite {group}", group=group)
    return written


#: Fill of every time column (read back as NaT).
TIME_FILL = int(np.iinfo("int64").min)


def _time_counts(values: np.ndarray, unit: str, fill: int) -> tuple[np.ndarray, dict]:
    """Datetimes as integer counts of ``unit`` since 1970 with CF attributes."""
    stamps = values.astype(f"datetime64[{unit}]")
    counts = np.where(np.isnat(stamps), fill, stamps.astype("int64"))
    spelled = "milliseconds" if unit == "ms" else "seconds"
    attrs = {
        "units": f"{spelled} since 1970-01-01",
        "calendar": "proleptic_gregorian",
        "_FillValue": fill,
    }
    return counts.astype("int64"), attrs


def _first_arrivals(first: pd.Series, subset: dict[str, np.ndarray]) -> np.ndarray:
    """Each row's earliest arrival among the copies of its observation."""
    values = first.reindex(subset["observation_identity"]).to_numpy()
    return np.asarray(values, dtype="datetime64[s]")


def _stored_dims(repo, group: str) -> dict[str, int]:
    """The number of dimensions of each array a product group already holds."""
    import zarr

    try:
        node = zarr.open_group(repo.readonly_session("main").store, path=group, mode="r")
    except Exception:  # noqa: BLE001 - a group not written yet holds nothing
        return {}
    return {name: len(node[name].shape) for name in node.array_keys()}


def product_dataset(batch: list[dict], stored_dims: dict[str, int]) -> xr.Dataset:
    """Decoded product rows as one dataset on ``observation`` and each ``<name>_n``.

    An element repeated in the batch but stored as a single column keeps its first
    occurrence there and the rest under ``<name>_all`` (all occurrences); one single here
    but stored repeated becomes a width-one row of it.
    """
    joined = products.concat(batch)
    data: dict[str, Any] = {}
    for name, values in joined.items():
        if name.endswith(".decimals"):
            continue
        stored = stored_dims.get(name)
        pairs = [(name, values)]
        if stored == 2 and values.ndim == 1:
            pairs = [(name, values[:, None])]
        elif stored == 1 and values.ndim == 2:
            pairs = [(name, values[:, 0]), (f"{name}_all", values)]
        for out_name, out in pairs:
            dims = ("observation",) if out.ndim == 1 else ("observation", f"{out_name}_n")
            attrs: dict[str, Any] = {}
            if out.dtype.kind == "f":
                decimals = int(joined.get(f"{name}.decimals", 6))
                out = _float(out.astype("float64"), decimals)
                attrs["decimal_places"] = decimals
            elif out.dtype.kind in "iu":
                # One fill for every window: ecCodes' own missing value, in int32 whenever
                # the values allow, so later appends never meet a different fill.
                big = out.max(initial=0) > MISSING_LONG or out.min(initial=0) < -MISSING_LONG
                out = out.astype("int64" if big else "int32")
                attrs["_FillValue"] = MISSING_LONG
            elif out.dtype.kind == "M":
                out, time_attrs = _time_counts(out, "ms" if name == "time" else "s", TIME_FILL)
                attrs.update(time_attrs)
            else:
                out = out.astype(object)
            attrs.update(ARRIVAL_ATTRS.get(out_name, {}))
            data[out_name] = xr.Variable(dims, out, attrs)
    return xr.Dataset(data)


def write_product_group(
    repo, product: products.Product, paths: Sequence[str], partition: str | None, counts: Counter
) -> int:
    """Deduplicate and append one product group's decoded objects. Returns rows written."""
    from planetary_datasets.common.multidim import (
        append_partition,
        describe_variables,
        ledger,
        mark_written,
    )

    group = product.group
    label = f"wis2_satellite {group}"
    if partition is not None and partition in ledger(repo, group=group):
        counts[f"{group}_already_written"] += 1
        return 0
    keys = []
    for index, path in enumerate(paths):
        with np.load(path, allow_pickle=True) as data:
            ids, arrived = data["observation_identity"], data["arrival_time"]
        keys.append(
            pd.DataFrame({"id": ids, "arrival": arrived, "file": index, "row": np.arange(ids.size)})
        )
    if not keys:
        return 0
    table = pd.concat(keys, ignore_index=True)
    counts[f"{group}_decoded"] += len(table)
    table = table[~table["id"].isin(_stored_identities(repo, group))]
    first_arrival = table.groupby("id")["arrival"].min()
    # Identical copies: the earliest to arrive is kept.
    table = table.sort_values(["id", "arrival", "file", "row"]).drop_duplicates("id")
    counts[f"{group}_duplicates"] += counts[f"{group}_decoded"] - len(table)
    keep = {file: np.sort(rows["row"].to_numpy()) for file, rows in table.groupby("file")}
    stored_dims = _stored_dims(repo, group)

    written = 0
    batch: list[dict] = []
    values_in_batch = 0

    def flush() -> None:
        nonlocal batch, values_in_batch, written
        if not batch:
            return
        ds = product_dataset(batch, stored_dims)
        fixed = [d for d in ds.dims if d != "observation"]
        append_partition(repo, ds, None, fixed_dims=fixed, label=label, group=group)
        stored_dims.update({name: ds[name].ndim for name in ds.data_vars})
        written += ds.sizes["observation"]
        batch, values_in_batch = [], 0

    for file, rows in sorted(keep.items()):
        arrays = _load(paths[file])
        subset = {
            name: values if name.endswith(".decimals") else values[rows]
            for name, values in arrays.items()
        }
        subset["first_arrival_time"] = _first_arrivals(first_arrival, subset)
        batch.append(subset)
        values_in_batch += sum(
            int(np.prod(v.shape)) for k, v in subset.items() if not k.endswith(".decimals")
        )
        if values_in_batch >= BATCH_VALUES:
            flush()
    flush()
    if partition is not None:
        try:
            mark_written(repo, partition, label=label, group=group)
        except Exception as exc:  # noqa: BLE001 - group not created when nothing was new
            logger.debug(f"{label}: ledger not updated ({exc})")
    if written:
        describe_variables(repo, ARRIVAL_ATTRS, label=label, group=group)
    return written


def ensure_root(repo) -> None:
    """The store's root attributes, written before any group and whenever they change."""
    import icechunk
    import zarr

    attrs = {
        "title": "WIS2 Global Cache satellite observations",
        "source": "WMO WIS2 Global Cache (s3://wis2globalcache)",
        "layout": (
            "one group per sounder's radiances (" + ", ".join(INSTRUMENTS) + ") and per "
            "product (" + ", ".join(products.PRODUCTS) + "); open all with "
            "xr.open_datatree(store, engine='zarr', consolidated=False), one with "
            "xr.open_zarr(store, group='atms', consolidated=False)"
        ),
    }
    session = repo.writable_session("main")
    root = zarr.open_group(session.store, mode="a")
    if all(root.attrs.get(k) == v for k, v in attrs.items()):
        return
    root.attrs.update(attrs)
    session.commit("wis2_satellite: root", rebase_with=icechunk.ConflictDetector())


# ----------------------------------------------------------------------------- target


@dataclasses.dataclass
class _Job:
    """One spooled object and, while it has one, its decoding future."""

    path: str
    key: str
    arrived: dt.datetime | None
    future: concurrent.futures.Future | None = None


def _pool(workers: int) -> concurrent.futures.ProcessPoolExecutor:
    context = multiprocessing.get_context("forkserver")
    return concurrent.futures.ProcessPoolExecutor(workers, mp_context=context)


@register("satellite")
class SatelliteTarget(ArchiveTarget):
    """Sounder radiances into ``bkr/obs/wis2_satellite.icechunk``, decoded off the stream."""

    store_prefix = STORE_PREFIX

    def __init__(self, config: Config | None = None, workers: int = WORKERS):
        super().__init__(config or get_config())
        self.workers = workers
        self._pool: concurrent.futures.ProcessPoolExecutor | None = None
        self._jobs: list[_Job] = []
        self._spool: pathlib.Path | None = None
        self.counts: Counter = Counter()
        self.feed_seconds = 0.0
        #: Groups whose ledger already lists the coming window; see :meth:`prepare`.
        self._written: set[str] = set()

    def prepare(self, partition: str) -> None:
        """Note the groups that already hold ``partition``, so their objects are declined.

        Re-running the target over an archived window (a backfill of new groups) then
        neither spools nor decodes the radiances written the first time.
        """
        from planetary_datasets.common.multidim import ledger

        repo = self.config.icechunk_repo(self.store_prefix)
        written = set()
        for group in [*INSTRUMENTS, *products.PRODUCTS]:
            try:
                if partition in ledger(repo, group=group):
                    written.add(group)
            except Exception:  # noqa: BLE001 - a group not created yet holds nothing
                continue
        self._written = written
        if written:
            logger.info(
                f"wis2 satellite: {partition} already in {', '.join(sorted(written))}; "
                "their objects are skipped"
            )

    def wants(self, key: str, size: int) -> bool:
        instrument = instrument_for(key)
        product = None if instrument is not None else products.product_for(key)
        group = instrument.group if instrument else (product.group if product else None)
        return group is not None and group not in getattr(self, "_written", ())

    def _start(self) -> None:
        root = pathlib.Path(self.config.data_dir) / "wis2_satellite_spool"
        root.mkdir(parents=True, exist_ok=True)
        self._spool = pathlib.Path(tempfile.mkdtemp(prefix=f"{os.getpid()}-", dir=root))
        (self._spool / "decoded").mkdir()
        self._pool = _pool(self.workers)

    def _submit(self, pool, job: _Job) -> None:
        job.future = pool.submit(
            decode_file, job.path, job.key, job.arrived, str(self._spool / "decoded")
        )

    def feed(self, key: str, body: bytes, arrived: dt.datetime | None) -> None:
        """Spool the object and queue its decoding; returns at disk-write speed.

        A pool a crashing decode has broken takes no more work; :meth:`finish` decodes
        what it could not.
        """
        started = time.perf_counter()
        try:
            if self._spool is None:
                self._start()
            path = self._spool / f"{len(self._jobs) + 1:08d}.bufr"
            path.write_bytes(body)
            job = _Job(str(path), key, arrived)
            self._jobs.append(job)
            if self._pool is not None:
                try:
                    self._submit(self._pool, job)
                except concurrent.futures.process.BrokenProcessPool:
                    self._pool.shutdown(cancel_futures=True)
                    self._pool = None
        except Exception as exc:  # noqa: BLE001 - feed never raises
            self.counts["unspooled_objects"] += 1
            logger.warning(f"wis2 satellite: {key}: {exc}")
        finally:
            self.feed_seconds += time.perf_counter() - started

    def _collect(self, jobs: list[_Job], results: list[dict]) -> list[_Job]:
        """Results of ``jobs`` with futures; those a broken pool lost come back."""
        lost = [job for job in jobs if job.future is None]
        for job in [j for j in jobs if j.future is not None]:
            try:
                results.append(job.future.result())
            except concurrent.futures.process.BrokenProcessPool:
                job.future = None
                lost.append(job)
        return lost

    def _decode_all(self) -> list[dict]:
        """Every job's result, decoding again what a crashed worker took down with it.

        Lost jobs are retried in a fresh pool while that makes progress, then one per
        process, so an object that crashes ecCodes is isolated and counted.
        """
        results: list[dict] = []
        pending = self._collect(self._jobs, results)
        if self._pool is not None:
            self._pool.shutdown()
            self._pool = None
        while pending:
            before = len(pending)
            with _pool(self.workers) as pool:
                for job in pending:
                    self._submit(pool, job)
                pending = self._collect(pending, results)
            if len(pending) == before:
                break
        for job in pending:
            with _pool(1) as pool:
                self._submit(pool, job)
                if self._collect([job], results):
                    self.counts["crashed_objects"] += 1
                    logger.warning(f"wis2 satellite: decoding {job.key} crashed; skipped")
                    pathlib.Path(job.path).unlink(missing_ok=True)
        return results

    def finish(self, window_start, window_end, partition: str) -> dict[str, Any]:
        if self._spool is None:
            return {"objects": 0}
        started = time.perf_counter()
        paths: dict[str, list[str]] = {}
        decode_seconds = 0.0
        try:
            for result in self._decode_all():
                self.counts.update(result.get("counts", {}))
                decode_seconds += result.get("seconds", 0.0)
                if result.get("error"):
                    logger.warning(f"wis2 satellite: {result['error']}")
                if result.get("path"):
                    paths.setdefault(result["group"], []).append(result["path"])
            decoded_at = time.perf_counter()
            repo = self.config.icechunk_repo(self.store_prefix)
            rows = {}
            if paths:
                ensure_root(repo)
            counts = {group: Counter() for group in paths}
            # icechunk's local filesystem storage is not safe for concurrent commits.
            threads = 1 if self.config.icechunk_local_path else WRITE_THREADS
            with concurrent.futures.ThreadPoolExecutor(threads) as writers:
                futures = {
                    group: writers.submit(
                        write_group if group in INSTRUMENTS else write_product_group,
                        repo,
                        INSTRUMENTS[group] if group in INSTRUMENTS else products.PRODUCTS[group],
                        sorted(files),
                        partition,
                        counts[group],
                    )
                    for group, files in sorted(paths.items())
                }
                for group, future in futures.items():
                    try:
                        rows[group] = future.result()
                    except Exception as exc:  # noqa: BLE001 - one group must not lose the rest
                        logger.exception(f"wis2 satellite {group}: write failed: {exc}")
                        rows[group] = f"error: {type(exc).__name__}: {exc}"
            for group_counts in counts.values():
                self.counts.update(group_counts)
            return {
                "objects": len(self._jobs),
                "rows": rows,
                **dict(self.counts),
                "feed_seconds": round(self.feed_seconds, 1),
                "decode_cpu_seconds": round(decode_seconds, 1),
                "wait_seconds": round(decoded_at - started, 1),
                "write_seconds": round(time.perf_counter() - decoded_at, 1),
            }
        finally:
            if self._pool is not None:
                self._pool.shutdown(cancel_futures=True)
            if self._spool is not None:
                shutil.rmtree(self._spool, ignore_errors=True)
            self._pool, self._jobs, self._spool = None, [], None


def decode_paths(paths: Iterable[pathlib.Path]) -> dict[str, dict[str, np.ndarray]]:
    """Decode local files (named like cache keys) in-process: a debugging aid."""
    out = {}
    for path in paths:
        group, arrays, _ = decode_object(str(path), path.read_bytes(), None)
        if arrays is not None:
            out[str(path)] = arrays
    return out
