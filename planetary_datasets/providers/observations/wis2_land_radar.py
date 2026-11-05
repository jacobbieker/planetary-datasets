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
    to each quantity's group. Not read: the Cyprus Department of Meteorology's volumes,
    which are GAMIC HDF5 rather than ODIM and come to 3 to 9 GB a window, and the Met
    Office's radar BUFR (category 6, local sequence 3 21 193 that ecCodes cannot expand).

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


def header(path: str) -> dict[str, Any] | None:
    """A volume's node, nominal time and quantities; None if it is not an ODIM PVOL."""
    import h5py

    try:
        with h5py.File(path, "r") as handle:
            what = _child(handle, "what")
            if _text(_attr(what, "object")).upper() != "PVOL":
                return None
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


def read_volume(path: str, quantity: str) -> dict[str, Any]:
    """One quantity of a polar volume: the raw counts per sweep and their metadata."""
    import h5py

    with h5py.File(path, "r") as handle:
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
    data = {
        "counts": xr.Variable(
            ("volume", "sweep", "azimuth", "range"),
            counts,
            {
                "long_name": f"raw ODIM {quantity} counts",
                "quantity": quantity,
                "comment": (
                    "physical value = offset + gain * counts (per sweep); counts == undetect "
                    "is below detection; nodata, the fill, was not scanned"
                ),
                "_FillValue": fill,
            },
        )
    }
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


def _stored_times(repo, group: str) -> set[int]:
    """Nominal times (int64 seconds) of the volumes a group holds."""
    try:
        ds = xr.open_zarr(
            repo.readonly_session("main").store, group=group, consolidated=False, chunks=None
        )
        times = ds["time"].values
    except Exception:  # noqa: BLE001 - a group not written yet holds none
        return set()
    return set(times.astype("datetime64[s]").astype("int64").tolist())


def _arrival(arrived: dt.datetime | None) -> np.datetime64:
    if arrived is None:
        return np.datetime64("NaT", "s")
    return np.datetime64(arrived.astimezone(dt.timezone.utc).replace(tzinfo=None), "s")


def write(repo, spool: Spool, partition: str | None) -> dict[str, Any]:
    """Deduplicate the spooled volumes and append the new ones to their groups.

    Each group is appended in batches of :data:`BATCH_BYTES` and its own ledger updated;
    the window goes into :data:`PARENT`'s ledger once every group is written.

    Returns:
        Counts: objects, volumes written per group, and objects skipped.
    """
    from planetary_datasets.common.multidim import (
        append_partition,
        describe_variables,
        ledger,
        mark_written,
    )

    counts: Counter = Counter(objects=len(spool.items))
    if partition is not None and partition in ledger(repo, group=PARENT):
        counts["already_written"] = 1
        return dict(counts)
    # Pass 1: headers only, to choose one copy of each volume.
    chosen: dict[str, dict[int, tuple]] = {}
    for path, key, arrived in spool.in_arrival_order():
        head = header(path)
        if head is None:
            counts["not_odim_polar_volumes"] += 1
            continue
        centre = key.split("/")[1] if key.startswith("data/") else ""
        stamp = int(head["time"].astype("int64"))
        for quantity in head["quantities"]:
            group = group_name(head["node"], quantity)
            if stamp in chosen.setdefault(group, {}):
                counts["duplicate_copies"] += 1
                continue
            chosen[group][stamp] = (path, quantity, centre, _arrival(arrived))
    # Pass 2: per group, read the new volumes in time order and append them in batches.
    written: dict[str, Any] = {}
    failed = False
    for group, by_time in sorted(chosen.items()):
        if partition is not None and partition in ledger(repo, group=group):
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

            for path, quantity, centre, arrival in new:
                try:
                    volume = read_volume(path, quantity)
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
                mark_written(repo, partition, label=f"wis2_land {group}", group=group)
            written[group] = rows
        except Exception as exc:  # noqa: BLE001 - one group must not lose the others
            failed = True
            logger.exception(f"wis2_land {group}: write failed: {exc}")
            written[group] = f"error: {type(exc).__name__}: {exc}"
    if partition is not None and chosen and not failed:
        mark_written(repo, partition, label=f"wis2_land {PARENT}", group=PARENT)
    return {**dict(counts), "volumes_written": written}
