"""Weather-radar polar volumes (ODIM_H5) for the WIS2 land store.

Part of the land target (:mod:`~planetary_datasets.providers.observations.wis2_land`):
ODIM_H5 polar volumes are spooled to disk as the window streams past and appended, after
the tar is complete, to ``bkr/obs/wis2_land.icechunk`` under ``radar/``, one group per
radar and quantity::

    radar/bewid_dbzh    radar/bewid_th    radar/bewid_vrad    radar/behel_vradh  ...

named for the ODIM node (``NOD:`` of ``what/source``) and quantity, one volume per row
along ``volume``. Open one with
``xr.open_zarr(store, group="radar/bewid_dbzh", consolidated=False)``.

What is read
    The polar volumes RMI publishes on ``be-rmib/.../experimental/bel-radar`` (three
    radars, reflectivity ``DBZH``, total reflectivity ``TH`` and radial velocity
    ``VRAD``/``VRADH``, each every 5 minutes, ~1,200 objects or 0.5 GB a window) and any
    other ``*.pvol.*`` file on a radar topic. A file with several quantities adds a volume
    to each quantity's group. Also read, as a part of its own (:data:`GAMIC_LEDGER`): the
    Cyprus Department of Meteorology's long-range volumes (``LONGRANGE_PFO_*.h5``,
    ``LONGRANGE_LCA_*.h5``; GAMIC HDF5, 13 moments, 3 to 9 GB a window), mapped onto the
    same layout (see GAMIC below). Not read: the Met Office's radar BUFR (category 6, local
    sequence 3 21 193 that ecCodes cannot expand).

Layout
    ``counts`` (``volume``, ``sweep``, ``azimuth``, ``range``) holds the raw ODIM values,
    unchanged (``uint8`` or ``uint16``). The physical value is
    ``offset + gain * counts``, with ``gain`` and ``offset`` per sweep as the file gives
    them; ``counts == nodata`` was not scanned (and reads as missing) and
    ``counts == undetect`` is below detection. Sweeps keep the file's order; rays are
    stored by azimuth index (ray 0 points north, as ODIM orders them) and range bins from
    the radar outwards. Volumes with fewer sweeps, rays or bins than the group's largest
    are padded with ``nodata``; ``nrays`` and ``nbins`` give each sweep's real extent.
    Per sweep: ``elevation``, ``sweep_start_time``, ``sweep_end_time``, ``range_start``
    (km), ``range_step`` (m), ``first_ray`` (``a1gate``), ``nyquist_velocity``; per ray,
    where the file has them, ``azimuth_start`` and ``azimuth_stop``. Per volume: nominal
    ``time``, ``start_time``, ``end_time``, the radar's ``latitude``, ``longitude`` and
    ``antenna_height``, ``source``, ``scan_task``, ``beamwidth``, ``wavelength``,
    ``odim_version``, ``centre`` and ``arrival_time``.

GAMIC
    A GAMIC volume's group is named for the centre's country and the site
    (``radar/cypfo_dbzh``, ``radar/cylca_vradh``, ...), its moments by their ODIM names
    (:data:`GAMIC_QUANTITIES`; the GAMIC name is kept in the ``counts`` attribute
    ``gamic_moment``). ``counts`` are the raw ``UV8``/``UV16`` values, unchanged: 0 is no
    data (``nodata``, the fill), and ``value = offset + gain * counts`` with
    ``gain = (dyn_range_max - dyn_range_min) / (2**bits - 2)`` and
    ``offset = dyn_range_min - gain``, so 1 is ``dyn_range_min`` and ``2**bits - 1`` is
    ``dyn_range_max``. Rays are stored in the order scanned, not by azimuth (the first is
    wherever the antenna was; ``first_ray`` is 0), with each ray's ``azimuth_start`` and
    ``azimuth_stop``; a sweep can have 361 rays. Sweep times are the scan's ``timestamp``
    and its last ray's time. All times are UTC: LCA stamps its scans and rays in local
    time (Asia/Nicosia, UTC+2 in winter, UTC+3 in summer). Per volume, the file's times
    are compared with the UTC time in its name (``LONGRANGE_LCA_20261104053656_00.h5``):
    a time already within minutes of it is kept, one ahead by exactly the Nicosia offset
    at that instant is converted, and anything else is stored as given and counted
    (``unrecognised_time_offset`` in the window summary). ``time_offset_removed`` and
    ``nominal_time_offset_removed`` record the seconds subtracted (NaN: left as given).

Duplicates
    RMI publishes each volume under two names (``T_PAGW41_C_EBUM_...hdf`` and
    ``be-rmib_be-radar_T_PAGW41_...hdf``). A volume is identified by its group and nominal
    time; the earliest arriving copy is kept and volumes already in the group are skipped.
"""

from __future__ import annotations

import datetime as dt
import re
from collections import Counter
from typing import Any

import numpy as np
import xarray as xr
from loguru import logger

from planetary_datasets.providers.observations.wis2_land_hydrometric import Spool

#: Parent group of the radar groups; its ledger lists the windows written to all of them.
PARENT = "radar"
#: Attribute of :data:`PARENT` listing the windows whose GAMIC volumes are written: a ledger
#: of its own, so archived windows already written for the ODIM volumes still get them.
GAMIC_LEDGER = "gamic_partitions_written"
#: GAMIC moment names and the ODIM quantities they are stored as.
GAMIC_QUANTITIES = {
    "Zh": "DBZH",
    "Zv": "DBZV",
    "UZh": "TH",
    "UZv": "TV",
    "ZDR": "ZDR",
    "Vh": "VRADH",
    "Vv": "VRADV",
    "Wh": "WRADH",
    "Wv": "WRADV",
    "PHIDP": "PHIDP",
    "KDP": "KDP",
    "SNRh": "SNRH",
    "SNRv": "SNRV",
}
GAMIC_BITS = {"UV8": 8, "UV16": 16}
#: Uncompressed bytes of ``counts`` per append, bounding memory while writing.
BATCH_BYTES = 256 * 2**20
#: Times that can be missing (padded sweeps) are float seconds, exact for whole seconds
#: and read as NaT where NaN; xarray cannot read NaT back from int64 nanoseconds.
EPOCH = {"units": "seconds since 1970-01-01", "calendar": "proleptic_gregorian"}
#: Fill of the integer columns.
INT_FILL = -1

ARRIVAL_ATTRS = {
    "arrival_time": {
        "long_name": "time the earliest copy of this volume arrived in the WIS2 Global Cache",
    },
}


def wants(key: str, size: int = 0) -> bool:
    """Whether an object can be an ODIM polar volume (cheap; by key alone)."""
    if "/bel-radar/" in key:
        return True
    name = key.rsplit("/", 1)[-1]
    return "radar" in key and ".pvol." in name


def wants_gamic(key: str, size: int = 0) -> bool:
    """Whether an object can be a GAMIC volume: an HDF5 name on a radar topic, not ODIM's."""
    name = key.rsplit("/", 1)[-1]
    return (
        "/radar/" in key
        and "/bel-radar/" not in key
        and ".pvol." not in name
        and re.search(r"\.h5(\.|$)", name) is not None
    )


def _text(value) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace").strip()
    if isinstance(value, np.ndarray) and value.dtype.kind == "S":
        return value.tobytes().decode("utf-8", errors="replace").strip("\x00 ")
    return str(value).strip() if value is not None else ""


def _attr(node, name: str, default=None):
    if node is None or name not in node.attrs:
        return default
    value = node.attrs[name]
    if isinstance(value, np.ndarray) and value.size == 1 and value.dtype.kind != "S":
        return value.item()
    return value


def _number(node, name: str, default=np.nan) -> float:
    value = _attr(node, name)
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _child(node, name: str):
    return node[name] if name in node else None


def _seconds(date, time) -> float:
    """ODIM ``YYYYMMDD`` and ``HHmmss`` as seconds since 1970 (NaN when absent)."""
    stamp = _stamp(date, time)
    return np.nan if np.isnat(stamp) else float(stamp.astype("int64"))


def _stamp(date, time) -> np.datetime64:
    """ODIM ``YYYYMMDD`` and ``HHmmss`` as a datetime (NaT when absent or malformed)."""
    date, time = _text(date), _text(time)
    if not re.fullmatch(r"\d{8}", date) or not re.fullmatch(r"\d{6}", time):
        return np.datetime64("NaT", "s")
    return np.datetime64(
        f"{date[:4]}-{date[4:6]}-{date[6:]}T{time[:2]}:{time[2:4]}:{time[4:]}", "s"
    )


def _epoch(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return np.nan


def node_name(source: str) -> str | None:
    """The radar's name for a group: ``NOD:`` of ``what/source``, else ``RAD:``/``WMO:``."""
    parts = dict(p.split(":", 1) for p in source.split(",") if ":" in p)
    for key in ("NOD", "RAD", "WMO"):
        if parts.get(key):
            return re.sub(r"[^a-z0-9]+", "_", parts[key].lower()).strip("_") or None
    return None


def group_name(node: str, quantity: str) -> str:
    """The group of a radar's quantity, e.g. ``radar/bewid_dbzh``."""
    return f"{PARENT}/{node}_{re.sub(r'[^a-z0-9]+', '_', quantity.lower()).strip('_')}"


def _sweeps(handle) -> list[str]:
    names = [k for k in handle if re.fullmatch(r"dataset\d+", k)]
    return sorted(names, key=lambda k: int(k[7:]))


def _quantities(dataset) -> dict[str, str]:
    """``{quantity: dataN}`` of one sweep."""
    out = {}
    for name in sorted(
        (k for k in dataset if re.fullmatch(r"data\d+", k)), key=lambda k: int(k[4:])
    ):
        quantity = _text(_attr(_child(dataset[name], "what"), "quantity"))
        if quantity and "data" in dataset[name]:
            out.setdefault(quantity, name)
    return out


def header(path: str, country: str = "", key: str = "") -> dict[str, Any] | None:
    """A volume's node, nominal time and quantities; None if it is not a polar volume.

    Args:
        path: The spooled file (ODIM_H5 or GAMIC HDF5).
        country: Prefix of a GAMIC radar's node (its centre's country, ``cy``).
        key: The object's WIS2 key, whose file name gives a GAMIC volume's UTC time.
    """
    import h5py

    try:
        with h5py.File(path, "r") as handle:
            what = _child(handle, "what")
            if _text(_attr(what, "object")).upper() != "PVOL":
                return None
            if _gamic_scans(handle):
                return _gamic_header(handle, country, key)
            source = _text(_attr(what, "source"))
            node = node_name(source)
            time = _stamp(_attr(what, "date"), _attr(what, "time"))
            quantities = set()
            for name in _sweeps(handle):
                quantities |= set(_quantities(handle[name]))
    except (OSError, KeyError, ValueError):
        return None
    if node is None or np.isnat(time) or not quantities:
        return None
    return {"node": node, "time": time, "quantities": sorted(quantities), "source": source}


def read_volume(path: str, quantity: str, shifts: dict | None = None) -> dict[str, Any]:
    """One quantity of a polar volume: the raw counts per sweep and their metadata.

    Args:
        path: The spooled file.
        quantity: The ODIM quantity to read.
        shifts: For a GAMIC volume, the local-time offsets :func:`header` found
            (``nominal``, ``scans``; seconds to subtract, NaN when unrecognised).
    """
    import h5py

    with h5py.File(path, "r") as handle:
        if _gamic_scans(handle):
            return _gamic_volume(handle, quantity, shifts or {})
        what, where, how = (_child(handle, n) for n in ("what", "where", "how"))
        sweeps = []
        for name in _sweeps(handle):
            dataset = handle[name]
            data = _quantities(dataset).get(quantity)
            if data is None:
                continue
            node = dataset[data]
            counts = node["data"][()]
            dwhat, swhat = _child(node, "what"), _child(dataset, "what")
            swhere, show = _child(dataset, "where"), _child(dataset, "how")

            def calibration(key, default=np.nan, dwhat=dwhat, swhat=swhat):
                for holder in (dwhat, swhat, what):
                    if holder is not None and key in holder.attrs:
                        return _number(holder, key, default)
                return default

            nrays, nbins = counts.shape
            sweeps.append(
                {
                    "counts": counts,
                    "elevation": _number(swhere, "elangle"),
                    "sweep_start_time": _seconds(
                        _attr(swhat, "startdate"), _attr(swhat, "starttime")
                    ),
                    "sweep_end_time": _seconds(_attr(swhat, "enddate"), _attr(swhat, "endtime")),
                    "range_start": _number(swhere, "rstart"),
                    "range_step": _number(swhere, "rscale"),
                    "nrays": nrays,
                    "nbins": nbins,
                    "first_ray": int(_number(swhere, "a1gate", INT_FILL)),
                    "gain": calibration("gain"),
                    "offset": calibration("offset"),
                    "nodata": calibration("nodata"),
                    "undetect": calibration("undetect"),
                    "nyquist_velocity": _number(show, "NI"),
                    "azimuth_start": _per_ray(show, "startazA", nrays),
                    "azimuth_stop": _per_ray(show, "stopazA", nrays),
                }
            )
        return {
            "time": _stamp(_attr(what, "date"), _attr(what, "time")),
            "start_time": _epoch(_attr(how, "startepochs")),
            "end_time": _epoch(_attr(how, "endepochs")),
            "latitude": _number(where, "lat"),
            "longitude": _number(where, "lon"),
            "antenna_height": _number(where, "height"),
            "source": _text(_attr(what, "source")),
            "scan_task": _text(_attr(how, "task")),
            "beamwidth": _number(how, "beamwidth"),
            "wavelength": _number(how, "wavelength"),
            "odim_version": _text(_attr(what, "version")),
            "sweeps": sweeps,
        }


def _gamic_scans(handle) -> list[str]:
    """A GAMIC file's sweeps (``scan0``, ``scan1``, ...); empty for an ODIM file."""
    names = [k for k in handle if re.fullmatch(r"scan\d+", k)]
    return sorted(names, key=lambda k: int(k[4:]))


def _gamic_moments(scan) -> dict[str, tuple[str, Any]]:
    """``{ODIM quantity: (dataset name, attributes)}`` of one GAMIC sweep."""
    out = {}
    for name in sorted((k for k in scan if k.startswith("moment_")), key=lambda k: int(k[7:])):
        attrs = scan[name].attrs
        moment = _text(attrs.get("moment"))
        if moment in GAMIC_QUANTITIES and _text(attrs.get("format")) in GAMIC_BITS:
            out.setdefault(GAMIC_QUANTITIES[moment], (name, attrs))
    return out


def _iso_seconds(value) -> float:
    """An ISO 8601 UTC time (``2026-11-06T06:41:41.295Z``) as float seconds since 1970."""
    text = _text(value).rstrip("Z")
    try:
        stamp = np.datetime64(text, "ms")
    except ValueError:
        return np.nan
    return np.nan if np.isnat(stamp) else float(stamp.astype("int64")) / 1000.0


#: Time zone of the GAMIC radars that stamp local time (the Cyprus Department of
#: Meteorology's), keyed by node prefix.
LOCAL_ZONES = {"cy": "Asia/Nicosia"}
#: How far a volume's first scan may start from its nominal time.
SCAN_START_TOLERANCE = 600.0
NAME_TIME = re.compile(r"_(\d{14})(?:_|\.|$)")


def name_time(key: str) -> float:
    """The UTC time in a GAMIC file name as seconds since 1970; NaN when it has none.

    The Cyprus volumes are named for their nominal time in UTC
    (``LONGRANGE_LCA_20261104053656_00.h5``), which matches their arrival.
    """
    match = NAME_TIME.search(key.rsplit("/", 1)[-1])
    if not match:
        return np.nan
    try:
        stamp = dt.datetime.strptime(match[1], "%Y%m%d%H%M%S").replace(tzinfo=dt.timezone.utc)
    except ValueError:
        return np.nan
    return stamp.timestamp()


def local_offset(given: float, utc: float, zone: str | None, tolerance: float = 0.0) -> float:
    """Seconds to subtract from ``given`` to make it UTC, or NaN if that is unclear.

    0 when ``given`` already agrees with the UTC time ``utc`` (within ``tolerance``); the
    zone's UTC offset at that instant when ``given`` is that many whole hours ahead; NaN
    for any other difference, or a missing time.
    """
    from zoneinfo import ZoneInfo

    if not (np.isfinite(given) and np.isfinite(utc)):
        return np.nan
    delta = given - utc
    if abs(delta) <= tolerance:
        return 0.0
    if zone is None:
        return np.nan
    when = dt.datetime.fromtimestamp(utc, dt.timezone.utc)
    offset = when.astimezone(ZoneInfo(zone)).utcoffset()
    if offset is None:
        return np.nan
    offset = offset.total_seconds()
    return offset if abs(delta - offset) <= tolerance else np.nan


def gamic_shifts(handle, zone: str | None, reference: float) -> dict[str, float]:
    """The local-time offsets of a GAMIC volume's times, from the UTC ``reference``.

    ``nominal`` is for ``what/date``; ``scans`` for the scan and ray timestamps, judged
    by the first scan's start (minutes after the nominal time). Each is the seconds to
    subtract: 0 for UTC, the zone's offset for local time, NaN when neither fits.
    """
    nominal = _iso_seconds(_attr(_child(handle, "what"), "date"))
    if not np.isfinite(reference):
        reference = nominal
    scans = _gamic_scans(handle)
    first = _iso_seconds(_attr(_child(handle[scans[0]], "how"), "timestamp")) if scans else np.nan
    return {
        "nominal": local_offset(nominal, reference, zone),
        "scans": local_offset(first, reference, zone, SCAN_START_TOLERANCE),
        "zone": zone,
    }


def _zone_offset(zone: str, utc: float) -> float:
    from zoneinfo import ZoneInfo

    when = dt.datetime.fromtimestamp(utc, dt.timezone.utc)
    return when.astimezone(ZoneInfo(zone)).utcoffset().total_seconds()


def local_to_utc(local: float, zone: str, after: float, shift: float) -> tuple[float, float]:
    """A local time stamped as if UTC (seconds since 1970), converted to UTC.

    Each time takes the zone's offset at its own instant, so a volume scanned across a
    clock change converts correctly. In the repeated hour after clocks go back, the
    reading at or after ``after`` (the previous time converted, NaN for the first) is
    taken, else the one with the volume's ``shift``.

    Returns:
        The UTC time and the offset removed.
    """
    if not np.isfinite(local):
        return np.nan, np.nan
    offsets = {_zone_offset(zone, local - hours * 3600) for hours in (-27, -3, 0, 3, 27)}
    candidates = sorted(
        (local - offset, offset)
        for offset in offsets
        if _zone_offset(zone, local - offset) == offset
    )
    if not candidates:  # a local time the clock skipped: keep the volume's offset
        return local - shift, shift
    if np.isfinite(after):
        later = [c for c in candidates if c[0] >= after - SCAN_START_TOLERANCE]
        if later:
            return later[0]
    same = [c for c in candidates if c[1] == shift]
    return (same or candidates)[0]


def _applied(shift: float) -> float:
    """Seconds to subtract: the shift, or 0 when it is unrecognised (NaN)."""
    return shift if np.isfinite(shift) else 0.0


def _gamic_header(handle, country: str, key: str = "") -> dict[str, Any] | None:
    site = re.sub(r"[^a-z0-9]+", "", _text(_attr(_child(handle, "how"), "site_name")).lower())
    stamp = _iso_seconds(_attr(_child(handle, "what"), "date"))
    quantities = set()
    for name in _gamic_scans(handle):
        quantities |= set(_gamic_moments(handle[name]))
    if not site or not np.isfinite(stamp) or not quantities:
        return None
    shifts = gamic_shifts(handle, LOCAL_ZONES.get(country), name_time(key))
    stamp -= _applied(shifts["nominal"])
    return {
        "node": f"{country}{site}",
        "time": np.datetime64(int(round(stamp)), "s"),
        "quantities": sorted(quantities),
        "source": f"GAMIC site:{site.upper()}",
        "shifts": shifts,
    }


def _gamic_volume(handle, quantity: str, shifts: dict) -> dict[str, Any]:
    """One ODIM quantity of a GAMIC volume, in :func:`read_volume`'s form, in UTC."""
    what, where, how = (_child(handle, n) for n in ("what", "where", "how"))
    nominal_shift = shifts.get("nominal", 0.0)
    scan_shift = shifts.get("scans", 0.0)
    # An unrecognised offset is left as given (and recorded as NaN).
    nominal_by, scan_by = _applied(nominal_shift), _applied(scan_shift)
    zone = shifts.get("zone")
    # Local times are converted one by one (a volume can span a clock change).
    local = bool(zone) and scan_by > 0
    previous = np.nan
    sweeps = []
    moment = ""
    wavelength = np.nan
    for name in _gamic_scans(handle):
        scan = handle[name]
        found = _gamic_moments(scan).get(quantity)
        if found is None:
            continue
        dataset, attrs = found
        moment = _text(attrs.get("moment"))
        counts = scan[dataset][()]
        nrays, nbins = counts.shape
        bits = GAMIC_BITS[_text(attrs.get("format"))]
        low = float(attrs["dyn_range_min"])
        high = float(attrs["dyn_range_max"])
        gain = (high - low) / (2**bits - 2)
        show = _child(scan, "how")
        rays = scan["ray_header"][()] if "ray_header" in scan else None
        names = rays.dtype.names if rays is not None else ()
        if np.isnan(wavelength):
            wavelength = _number(show, "radar_wave_length") * 100.0  # m to cm
        start = _iso_seconds(_attr(show, "timestamp"))
        end = float(rays["timestamp"][-1]) / 1e6 if "timestamp" in names and len(rays) else np.nan
        if local:
            start, removed = local_to_utc(start, zone, previous, scan_by)
            end, _ = local_to_utc(end, zone, start, removed)
        else:
            start, end, removed = start - scan_by, end - scan_by, scan_shift
        previous = end if np.isfinite(end) else start
        sweeps.append(
            {
                "counts": counts,
                "elevation": _number(show, "elevation"),
                "sweep_start_time": start,
                "sweep_end_time": end,
                "sweep_time_offset_removed": removed,
                "range_start": _number(show, "range_start") / 1000.0,
                "range_step": _number(show, "range_step"),
                "nrays": nrays,
                "nbins": nbins,
                "first_ray": 0,
                "gain": gain,
                "offset": low - gain,
                "nodata": 0.0,
                "undetect": np.nan,
                "nyquist_velocity": _number(show, "nyquist_velocity"),
                "azimuth_start": _ray(rays, "azimuth_start", nrays),
                "azimuth_stop": _ray(rays, "azimuth_stop", nrays),
            }
        )
    starts = [s["sweep_start_time"] for s in sweeps]
    ends = [s["sweep_end_time"] for s in sweeps]
    site = _text(_attr(how, "site_name"))
    host = _text(_attr(how, "host_name"))
    return {
        "time": np.datetime64(int(round(_iso_seconds(_attr(what, "date")) - nominal_by)), "s"),
        "nominal_time_offset_removed": nominal_shift,
        "time_offset_removed": scan_shift,
        "start_time": starts[0] if starts else np.nan,
        "end_time": ends[-1] if ends else np.nan,
        "latitude": _number(where, "lat"),
        "longitude": _number(where, "lon"),
        "antenna_height": _number(where, "height"),
        "source": f"GAMIC site:{site} host:{host}",
        "scan_task": _text(_attr(how, "template_name")),
        "beamwidth": _number(how, "azimuth_beam"),
        "wavelength": wavelength,
        "odim_version": f"GAMIC {_text(_attr(what, 'version'))}".strip(),
        "gamic_moment": moment,
        "sweeps": sweeps,
    }


def _ray(rays, name: str, nrays: int) -> np.ndarray:
    if rays is None or name not in (rays.dtype.names or ()) or len(rays) != nrays:
        return np.full(nrays, np.nan)
    return np.asarray(rays[name], dtype="float64")


def _per_ray(how, name: str, nrays: int) -> np.ndarray:
    values = _attr(how, name)
    if values is None:
        return np.full(nrays, np.nan)
    values = np.asarray(values, dtype="float64").ravel()
    return values if values.size == nrays else np.full(nrays, np.nan)


#: Per-sweep variables: (dtype, attributes); the fill is NaN for floats, else as declared.
SWEEP_VARIABLES = {
    "elevation": ("float64", {"units": "degree", "long_name": "elevation angle"}),
    "sweep_start_time": ("float64", {**EPOCH, "long_name": "start of the sweep"}),
    "sweep_end_time": ("float64", {**EPOCH, "long_name": "end of the sweep"}),
    "range_start": ("float64", {"units": "km", "long_name": "range of the start of the first bin"}),
    "range_step": ("float64", {"units": "m", "long_name": "distance between bins"}),
    "nrays": ("int32", {"long_name": "number of rays (azimuths) of the sweep"}),
    "nbins": ("int32", {"long_name": "number of range bins of the sweep"}),
    "first_ray": ("int32", {"long_name": "index of the first ray scanned (ODIM a1gate)"}),
    "gain": ("float64", {"long_name": "ODIM gain: value = offset + gain * counts"}),
    "offset": ("float64", {"long_name": "ODIM offset: value = offset + gain * counts"}),
    "nodata": ("float64", {"long_name": "counts value of bins not scanned"}),
    "undetect": ("float64", {"long_name": "counts value of bins scanned, nothing detected"}),
    "nyquist_velocity": ("float64", {"units": "m s-1", "long_name": "unambiguous velocity (NI)"}),
}
RAY_VARIABLES = {
    "azimuth_start": ("float64", {"units": "degree", "long_name": "azimuth at the ray's start"}),
    "azimuth_stop": ("float64", {"units": "degree", "long_name": "azimuth at the ray's end"}),
}


def to_dataset(volumes: list[dict], quantity: str, centre: list[str], arrivals: list) -> xr.Dataset:
    """Volumes of one group along ``volume``, padded to the largest sweep set among them."""
    sweeps = max(len(v["sweeps"]) for v in volumes)
    rays = max((s["nrays"] for v in volumes for s in v["sweeps"]), default=0)
    bins = max((s["nbins"] for v in volumes for s in v["sweeps"]), default=0)
    first = next(s for v in volumes for s in v["sweeps"])
    dtype = np.result_type(*(s["counts"].dtype for v in volumes for s in v["sweeps"]))
    nodata = first["nodata"]
    others = {s["nodata"] for v in volumes for s in v["sweeps"]} - {nodata}
    if others:
        # The fill would be a valid count in the other sweeps, and read as missing.
        raise ValueError(f"{quantity}: nodata differs between sweeps ({nodata}, {others})")
    fill = (
        int(nodata) if np.isfinite(nodata) and float(nodata).is_integer() else np.iinfo(dtype).max
    )
    counts = np.full((len(volumes), sweeps, rays, bins), fill, dtype=dtype)
    per_sweep = {
        name: np.full(
            (len(volumes), sweeps),
            np.nan if kind == "float64" else INT_FILL,
            dtype=kind,
        )
        for name, (kind, _) in SWEEP_VARIABLES.items()
    }
    per_ray = {name: np.full((len(volumes), sweeps, rays), np.nan) for name in RAY_VARIABLES}
    for i, volume in enumerate(volumes):
        for j, sweep in enumerate(volume["sweeps"]):
            n, b = sweep["counts"].shape
            counts[i, j, :n, :b] = sweep["counts"]
            for name, (kind, _) in SWEEP_VARIABLES.items():
                per_sweep[name][i, j] = sweep[name]
            for name in RAY_VARIABLES:
                per_ray[name][i, j, : sweep[name].size] = sweep[name]
    moment = volumes[0].get("gamic_moment")
    attrs = {
        "long_name": f"raw ODIM {quantity} counts",
        "quantity": quantity,
        "comment": (
            "physical value = offset + gain * counts (per sweep); counts == undetect "
            "is below detection; nodata, the fill, was not scanned"
        ),
        "_FillValue": fill,
    }
    if moment:
        attrs.update(
            long_name=f"raw GAMIC {moment} counts (ODIM {quantity})",
            gamic_moment=moment,
            comment=(
                "physical value = offset + gain * counts (per sweep), i.e. dyn_range_min at "
                "1 and dyn_range_max at 2**bits - 1; 0, the fill, is no data; rays in scan "
                "order (see azimuth_start)"
            ),
        )
    data = {"counts": xr.Variable(("volume", "sweep", "azimuth", "range"), counts, attrs)}
    for name, (kind, attrs) in SWEEP_VARIABLES.items():
        attrs = dict(attrs)
        if kind == "int32":
            attrs["_FillValue"] = INT_FILL
        data[name] = xr.Variable(("volume", "sweep"), per_sweep[name], attrs)
    for name, (_, attrs) in RAY_VARIABLES.items():
        data[name] = xr.Variable(("volume", "sweep", "azimuth"), per_ray[name], dict(attrs))
    scalars = {
        "time": ("datetime64[s]", {"long_name": "nominal time of the volume"}),
        "start_time": ("float64", {**EPOCH, "long_name": "start of the volume scan"}),
        "end_time": ("float64", {**EPOCH, "long_name": "end of the volume scan"}),
        "latitude": ("float64", {"units": "degrees_north", "long_name": "radar latitude"}),
        "longitude": ("float64", {"units": "degrees_east", "long_name": "radar longitude"}),
        "antenna_height": (
            "float64",
            {"units": "m", "long_name": "antenna height above sea level"},
        ),
        "source": ("str", {"long_name": "ODIM what/source"}),
        "scan_task": ("str", {"long_name": "scan strategy (ODIM how/task)"}),
        "beamwidth": ("float64", {"units": "degree", "long_name": "half-power beam width"}),
        "wavelength": ("float64", {"units": "cm", "long_name": "radar wavelength"}),
        "odim_version": ("str", {"long_name": "ODIM what/version"}),
    }
    if "time_offset_removed" in volumes[0]:
        removed = np.full((len(volumes), sweeps), np.nan)
        for i, volume in enumerate(volumes):
            for j, sweep in enumerate(volume["sweeps"]):
                removed[i, j] = sweep.get("sweep_time_offset_removed", np.nan)
        data["sweep_time_offset_removed"] = xr.Variable(
            ("volume", "sweep"),
            removed,
            {
                "units": "s",
                "long_name": "seconds subtracted from this sweep's times to make them UTC",
                "comment": "each local time takes the zone's offset at its own instant, so "
                "this differs from time_offset_removed only in a volume scanned across a "
                "clock change",
            },
        )
        # GAMIC groups only: what was done to make the file's times UTC.
        scalars["nominal_time_offset_removed"] = (
            "float64",
            {
                "units": "s",
                "long_name": "seconds subtracted from the file's nominal time to make it UTC",
                "comment": "0: the file gave UTC; NaN: offset unrecognised, time as given",
            },
        )
        scalars["time_offset_removed"] = (
            "float64",
            {
                "units": "s",
                "long_name": "seconds subtracted from the file's scan and ray times "
                "(sweep_start_time, sweep_end_time, start_time, end_time) to make them UTC, "
                "judged by the first scan (per sweep: sweep_time_offset_removed)",
                "comment": "the station's local UTC offset (Asia/Nicosia: 7200 in winter, "
                "10800 in summer) when its times were found to be local; 0 when they "
                "were UTC; NaN when the offset matched neither, times as given",
            },
        )
    for name, (kind, attrs) in scalars.items():
        values = [v[name] for v in volumes]
        array = np.array(values, dtype=object if kind == "str" else kind)
        data[name] = xr.Variable(("volume",), array, dict(attrs))
    data["centre"] = xr.Variable(
        ("volume",),
        np.array(centre, dtype=object),
        {"long_name": "WIS2 centre the copy kept was received from"},
    )
    data["arrival_time"] = xr.Variable(
        ("volume",), np.array(arrivals, dtype="datetime64[s]"), dict(ARRIVAL_ATTRS["arrival_time"])
    )
    ds = xr.Dataset(data)
    ds.attrs = {
        "title": f"WIS2 Global Cache weather-radar polar volumes, {quantity}",
        "source": "WMO WIS2 Global Cache (s3://wis2globalcache), ODIM_H5 PVOL",
        "quantity": quantity,
    }
    return ds


#: Dimensions every volume of a group is padded to.
FIXED_DIMS = ("sweep", "azimuth", "range")


def grow_fixed_dims(repo, group: str, ds: xr.Dataset, label: str) -> bool:
    """Widen a group's sweep, azimuth or range dimension to fit ``ds``.

    Stored volumes are padded with their variables' fill (``nodata`` for ``counts``). The
    shared append resizes such arrays itself, but the new positions would read as
    the array's zarr fill value (0, which for ``counts`` means "nothing detected").
    Returns whether it committed.
    """
    import icechunk
    import zarr

    from planetary_datasets.common.multidim import pad_value

    session = repo.writable_session("main")
    try:
        node = zarr.open_group(session.store, path=group, mode="a")
    except (FileNotFoundError, KeyError, zarr.errors.GroupNotFoundError):
        return False
    grown = False
    for name in node.array_keys():
        array = node[name]
        dims = tuple(array.metadata.dimension_names or ())
        old = array.shape
        new = tuple(
            max(size, ds.sizes.get(dim, 0)) if dim in FIXED_DIMS else size
            for dim, size in zip(dims, old)
        )
        if new == old:
            continue
        array.resize(new)
        fill = pad_value(dict(array.attrs), array.dtype, array.fill_value)
        rows = max(1, array.chunks[0])
        for start in range(0, old[0], rows):
            stop = min(old[0], start + rows)
            for axis in range(1, len(old)):
                if new[axis] > old[axis]:
                    region = [slice(start, stop)] + [slice(None)] * (len(old) - 1)
                    region[axis] = slice(old[axis], new[axis])
                    array[tuple(region)] = fill
        grown = True
    if grown:
        session.commit(
            f"{label}: widen {', '.join(FIXED_DIMS)}", rebase_with=icechunk.ConflictDetector()
        )
    return grown


def _stored_fill(repo, group: str):
    """The fill value of a group's stored ``counts`` (None before it is written)."""
    import zarr

    try:
        node = zarr.open_group(repo.readonly_session("main").store, path=group, mode="r")
        return node["counts"].attrs.get("_FillValue")
    except (KeyError, zarr.errors.GroupNotFoundError):
        return None


def _stored_times(repo, group: str) -> set[int]:
    """Nominal times (int64 seconds) of the volumes a group holds."""
    import zarr

    try:
        ds = xr.open_zarr(
            repo.readonly_session("main").store, group=group, consolidated=False, chunks=None
        )
        times = ds["time"].values
    except (KeyError, zarr.errors.GroupNotFoundError):  # not written yet; others propagate
        return set()
    return set(times.astype("datetime64[s]").astype("int64").tolist())


def _arrival(arrived: dt.datetime | None) -> np.datetime64:
    if arrived is None:
        return np.datetime64("NaT", "s")
    return np.datetime64(arrived.astimezone(dt.timezone.utc).replace(tzinfo=None), "s")


def parent_ledger(repo, attr: str, group: str = PARENT) -> set[str]:
    """Windows listed in ``group``'s ledger ``attr`` (empty before it exists)."""
    import zarr

    from planetary_datasets.common.multidim import LEDGER_ATTR, ledger

    if attr == LEDGER_ATTR:
        return ledger(repo, group=group)
    try:
        node = zarr.open_group(repo.readonly_session("main").store, path=group, mode="r")
    except (KeyError, zarr.errors.GroupNotFoundError):
        return set()
    return set(node.attrs.get(attr, []))


def mark_parent(repo, partition: str, attr: str, group: str = PARENT) -> None:
    """Add ``partition`` to ``group``'s ledger ``attr`` (:data:`PARENT`'s by default)."""
    import icechunk
    import zarr

    from planetary_datasets.common.multidim import COMMIT_ATTEMPTS, LEDGER_ATTR, mark_written

    if attr == LEDGER_ATTR:
        mark_written(repo, partition, label=f"wis2_land {group}", group=group)
        return
    for attempt in range(1, COMMIT_ATTEMPTS + 1):
        session = repo.writable_session("main")
        node = zarr.open_group(session.store, path=group, mode="a")
        written = set(node.attrs.get(attr, []))
        if partition in written:
            return
        node.attrs[attr] = sorted(written | {partition})
        try:
            session.commit(
                f"wis2_land {group}: {partition} ({attr})",
                rebase_with=icechunk.ConflictDetector(),
            )
            return
        except (icechunk.ConflictError, icechunk.RebaseFailedError):
            if attempt == COMMIT_ATTEMPTS:
                raise


def write(
    repo, spool: Spool, partition: str | None, ledger_attr: str | None = None
) -> dict[str, Any]:
    """Deduplicate the spooled volumes and append the new ones to their groups.

    Each group is appended in batches of :data:`BATCH_BYTES` and its own ledger updated;
    the window goes into :data:`PARENT`'s ledger ``ledger_attr`` (the shared one by
    default, :data:`GAMIC_LEDGER` for the GAMIC part) once every group is written.

    Returns:
        Counts: objects, volumes written per group, and objects skipped.
    """
    from planetary_datasets.common.multidim import (
        LEDGER_ATTR,
        append_partition,
        describe_variables,
    )

    ledger_attr = ledger_attr or LEDGER_ATTR
    counts: Counter = Counter(objects=len(spool.items))
    if partition is not None and partition in parent_ledger(repo, ledger_attr):
        counts["already_written"] = 1
        return dict(counts)
    # Pass 1: headers only, to choose one copy of each volume.
    chosen: dict[str, dict[int, tuple]] = {}
    for path, key, arrived in spool.in_arrival_order():
        centre = key.split("/")[1] if key.startswith("data/") else ""
        head = header(path, country=centre.split("-", 1)[0], key=key)
        if head is None:
            counts["not_polar_volumes"] += 1
            continue
        shifts = head.get("shifts")
        if shifts is not None:
            offsets = (shifts["nominal"], shifts["scans"])
            if not all(np.isfinite(v) for v in offsets):
                counts["unrecognised_time_offset"] += 1
            elif any(offsets):
                counts["local_time_converted"] += 1
        stamp = int(head["time"].astype("int64"))
        for quantity in head["quantities"]:
            group = group_name(head["node"], quantity)
            if stamp in chosen.setdefault(group, {}):
                counts["duplicate_copies"] += 1
                continue
            chosen[group][stamp] = (path, quantity, centre, _arrival(arrived), shifts)
    # Pass 2: per group, read the new volumes in time order and append them in batches.
    written: dict[str, Any] = {}
    failed = False
    for group, by_time in sorted(chosen.items()):
        # Each format keeps its own group ledgers too, so one never skips the other.
        if partition is not None and partition in parent_ledger(repo, ledger_attr, group):
            written[group] = "already written"
            continue
        try:
            stored = _stored_times(repo, group)
            new = [by_time[t] for t in sorted(by_time) if t not in stored]
            counts["volumes_already_stored"] += len(by_time) - len(new)
            rows = 0
            batch: list[tuple] = []
            size = 0

            def flush() -> int:
                volumes = [b[0] for b in batch]
                ds = to_dataset(volumes, batch[0][1], [b[2] for b in batch], [b[3] for b in batch])
                stored = _stored_fill(repo, group)
                fill = ds["counts"].attrs["_FillValue"]
                if stored is not None and stored != fill:
                    raise ValueError(f"nodata {fill} differs from the group's fill {stored}")
                grow_fixed_dims(repo, group, ds, f"wis2_land {group}")
                append_partition(
                    repo,
                    ds,
                    None,
                    fixed_dims=list(FIXED_DIMS),
                    label=f"wis2_land {group}",
                    group=group,
                )
                return len(volumes)

            for path, quantity, centre, arrival, shifts in new:
                try:
                    volume = read_volume(path, quantity, shifts)
                except (OSError, KeyError, ValueError) as exc:
                    counts["unreadable_volumes"] += 1
                    logger.warning(f"wis2_land {group}: {path}: {exc}")
                    continue
                if not volume["sweeps"]:
                    continue
                batch.append((volume, quantity, centre, arrival))
                size += sum(s["counts"].nbytes for s in volume["sweeps"])
                if size >= BATCH_BYTES:
                    rows += flush()
                    batch, size = [], 0
            if batch:
                rows += flush()
            if rows:
                describe_variables(repo, ARRIVAL_ATTRS, label=f"wis2_land {group}", group=group)
            if partition is not None and (rows or stored):
                mark_parent(repo, partition, ledger_attr, group)
            written[group] = rows
        except Exception as exc:  # noqa: BLE001 - one group must not lose the others
            failed = True
            logger.exception(f"wis2_land {group}: write failed: {exc}")
            written[group] = f"error: {type(exc).__name__}: {exc}"
    if partition is not None and chosen and not failed:
        mark_parent(repo, partition, ledger_attr)
    return {**dict(counts), "volumes_written": written}
