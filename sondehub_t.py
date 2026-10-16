"""Build per-serial radiosonde flight files from the SondeHub history bucket.

``sondehub.download(serial=...)`` stopped working because it enumerates the bucket with
``ListObjectsV2`` and ``s3://sondehub-history/serial/`` no longer grants ``ListBucket``
(Access Denied). The full-resolution objects themselves are still public: a direct
``GetObject`` on ``serial/<serial>.json.gz`` returns the complete flight.

So this walks ``s3://sondehub-history/date/YYYY/MM/DD/<serial>.json`` -- which *is*
listable -- purely as the index of which serials flew on which day, then pulls each
flight's full-rate telemetry from ``serial/<serial>.json.gz``. The date objects hold only
a 3-frame daily summary (first / highest / last), so they are never the data source, just
the serial list.

Days are walked newest-first (default: yesterday backwards to 2018), so the most recent
flights land first and an interrupted run still leaves you with the useful end of the
archive.

Every receiver that hears a sonde uploads its own copy of each frame, so raw frame counts
run 2-20x the number of distinct observations. Duplicates are merged per timestamp rather
than dropped -- receivers disagree about which optional fields they report, so merging
recovers pressure/humidity readings a first-wins dedupe would lose.

Flights are appended straight into an Icechunk store (see sondehub_icechunk.py for the
layout), committed once per day of archive, so the run produces one queryable store rather
than a million loose files. Pass --netcdf to also drop a NetCDF per flight.

Outputs:

  sondehub_flights.icechunk                      ragged store, selectable by serial
  sondehub_frame_stats.npy                       (serial, n_frames, start, end, max_alt)
  sondehub_pressure_flights/flight_<serial>.nc   only with --netcdf

Each flight carries its telemetry as variables and the sonde/launch-site/receiver metadata
as attributes, including launch site name and position joined from the SondeHub sites
catalogue.

The run is resumable: ``sondehub_date_state.json`` records which days are complete and
``sondehub_processed_serials.txt`` which serials were already pulled.

Examples
--------
    python sondehub_t.py                                   # yesterday backwards to 2018
    python sondehub_t.py --start 2024-01-01 --end 2024-01-31
    python sondehub_t.py --no-require-pressure --min-frames 100
    python sondehub_t.py --plot V2030227                   # plot a written flight
"""

import argparse
import gzip
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import date, datetime, timedelta, timezone

import boto3
import botocore
import numpy as np
import pandas as pd
import xarray as xr
from botocore import UNSIGNED
from botocore.config import Config

from sondehub_icechunk import DEFAULT_STORE, FlightWriter, WriterError

BUCKET = "sondehub-history"
SITES_URL = "https://api.v2.sondehub.org/sites"
ARCHIVE_START = date(2018, 1, 1)

DEFAULT_OUT_DIR = "sondehub_pressure_flights"
DEFAULT_STATS = "sondehub_frame_stats.npy"
DEFAULT_STATE = "sondehub_date_state.json"
DEFAULT_SEEN = "sondehub_processed_serials.txt"

# Per-frame telemetry: NetCDF variable name -> SondeHub field.
VARIABLES = {
    "latitude": "lat",
    "longitude": "lon",
    "altitude": "alt",
    "vertical_velocity": "vel_v",
    "horizontal_velocity": "vel_h",
    "heading": "heading",
    "pressure": "pressure",
    "temperature": "temp",
    "humidity": "humidity",
    "satellites": "sats",
    "battery": "batt",
    "snr": "snr",
    "rssi": "rssi",
    "frame_counter": "frame",
}

VARIABLE_ATTRS = {
    "latitude": {"units": "degrees_north", "standard_name": "latitude"},
    "longitude": {"units": "degrees_east", "standard_name": "longitude"},
    "altitude": {"units": "m", "standard_name": "altitude", "positive": "up"},
    "vertical_velocity": {"units": "m s-1", "long_name": "ascent rate (SondeHub vel_v)"},
    "horizontal_velocity": {"units": "m s-1", "long_name": "ground speed (SondeHub vel_h)"},
    "heading": {"units": "degrees", "long_name": "direction of travel"},
    "pressure": {"units": "hPa", "standard_name": "air_pressure"},
    "temperature": {"units": "degC", "standard_name": "air_temperature"},
    "humidity": {"units": "%", "standard_name": "relative_humidity"},
    "satellites": {"long_name": "GNSS satellites used"},
    "battery": {"units": "V", "long_name": "sonde battery voltage"},
    "snr": {"units": "dB", "long_name": "receiver signal-to-noise ratio"},
    "rssi": {"units": "dBm", "long_name": "receiver signal strength"},
    "frame_counter": {"long_name": "sonde frame counter"},
    "receiver_count": {"long_name": "distinct receivers that uploaded this frame"},
}

# Flight-constant fields copied straight into the NetCDF global attributes.
FLIGHT_ATTRS = (
    "serial",
    "manufacturer",
    "type",
    "subtype",
    "dfmcode",
    "frequency",
    "tx_frequency",
    "burst_timer",
    "rs41_mainboard",
    "rs41_mainboard_fw",
    "ref_datetime",
    "ref_position",
    "launch_site",
    "launch_site_range_estimate",
)

# Bulky or redundant per-frame fields, never needed downstream.
DROP_FIELDS = frozenset({"rs41_subframe", "position", "user-agent", "uploader_position"})

_SAFE_SERIAL = re.compile(r"[^A-Za-z0-9._-]+")

# Fetch outcomes with nothing left to retry, as opposed to network failures.
PERMANENT_ERRORS = frozenset({"NoSuchKey", "AccessDenied", "Unreadable"})
# Consecutive day listings that may fail before the run gives up. Per-day flakiness is
# normal; a long unbroken run of failures means the environment is broken (a vanishing CA
# bundle did exactly this) and racing through the remaining years would only mark them bad.
MAX_LIST_FAILURES = 10
# Seconds to wait on a single S3 body read before giving up.
READ_TIMEOUT = 180


def make_client(workers, read_timeout=READ_TIMEOUT):
    return boto3.client(
        "s3",
        config=Config(
            signature_version=UNSIGNED,
            max_pool_connections=max(workers * 2, 10),
            retries={"max_attempts": 5, "mode": "standard"},
            # botocore defaults to 60s, which is fine on a healthy link but turns a merely
            # slow connection into a wall of timeouts: on a degraded line a single flight
            # took 57s uncontended, and the workers share that line.
            connect_timeout=30,
            read_timeout=read_timeout,
        ),
    )


def fetch_sites():
    """SondeHub launch-site catalogue keyed by station id, or {} if unreachable."""
    request = urllib.request.Request(SITES_URL, headers={"Accept-Encoding": "gzip"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = response.read()
            if response.headers.get("Content-Encoding") == "gzip":
                payload = gzip.decompress(payload)
        return json.loads(payload)
    except (urllib.error.URLError, OSError, ValueError) as err:
        print(f"! launch site catalogue unavailable ({err}); continuing without", file=sys.stderr)
        return {}


def daterange_desc(start, end):
    day = end
    while day >= start:
        yield day
        day -= timedelta(days=1)


def list_serials(s3, day):
    """Serials that flew on `day`, from the listable date/ index."""
    serials = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=f"date/{day:%Y/%m/%d}/"):
        for obj in page.get("Contents", []):
            name = obj["Key"].rsplit("/", 1)[-1]
            if name.endswith(".json"):
                serials.append(name[: -len(".json")])
    return serials


def fetch_flight(s3, serial, attempts=5):
    """Full-resolution frames for a serial, or an error-code string on failure.

    botocore's own retries cover establishing the request, but the body is streamed after
    `get_object` returns, so a connection dropped mid-download lands outside them -- and a
    read timeout is not a ClientError, so it would otherwise escape the worker thread and
    take the whole run down. Retried here with backoff instead.
    """
    key = f"serial/{serial}.json.gz"
    for attempt in range(attempts):
        try:
            raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
            break
        except botocore.exceptions.ClientError as err:
            return err.response["Error"]["Code"]
        except Exception as err:
            if attempt == attempts - 1:
                return f"{type(err).__name__} after {attempts} attempts"
            time.sleep(2**attempt)
    try:
        frames = json.loads(gzip.decompress(raw))
    except (OSError, ValueError, UnicodeDecodeError):
        return "Unreadable"
    return frames if isinstance(frames, list) else [frames]


def _as_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return np.nan


def merge_frames(frames, day=None, trim=False):
    """Collapse multi-receiver uploads into one record per timestamp.

    Receivers disagree about which optional fields they decode, so fields missing from the
    first upload of a timestamp are filled from later ones. Returns (merged, counts,
    receivers) ordered by time.
    """
    merged = {}
    counts = Counter()
    receivers = Counter()
    for frame in frames:
        if not isinstance(frame, dict):
            continue
        time = _timestamp(frame.get("datetime"))
        if pd.isna(time):
            continue
        counts[time] += 1
        callsign = frame.get("uploader_callsign")
        if callsign:
            receivers[callsign] += 1
        record = merged.get(time)
        if record is None:
            merged[time] = {k: v for k, v in frame.items() if k not in DROP_FIELDS}
        else:
            for key, value in frame.items():
                if value is not None and key not in DROP_FIELDS and record.get(key) is None:
                    record[key] = value
    times = plausible_times(sorted(merged), day, trim)
    return [merged[t] for t in times], times, counts, receivers


MAX_FLIGHT_SPAN = pd.Timedelta(days=1)


def plausible_times(times, day=None, trim=False):
    """Frames whose clock agrees with the day the flight was indexed under.

    Some sondes report a timestamp years off -- P1440495 flew on 2026-09-19 but most of
    its frames claim 2019-05-28 -- which stretches the time coordinate across the gap.

    Trimming is off by default and deliberately so: for P1440495 the *misdated* frames are
    the real 25 km ascent and the correctly dated ones are 59 frames of ground noise, so
    discarding either side loses real telemetry. Flights like this are instead flagged with
    a `time_anomaly` attribute and left intact for the caller to judge. Pass --trim-to-day
    if you would rather have a clean time axis than every frame.
    """
    if not times or not trim or day is None:
        return times
    anchor = pd.Timestamp(day) + pd.Timedelta(hours=12)
    return [t for t in times if abs(t - anchor) <= MAX_FLIGHT_SPAN]


def _timestamp(value):
    try:
        time = pd.Timestamp(value)
    except (TypeError, ValueError):
        return pd.NaT
    if pd.isna(time):
        return pd.NaT
    # SondeHub stamps are UTC ("...Z"); xarray cannot encode tz-aware times.
    return time.tz_convert(None) if time.tzinfo is not None else time


def flight_stats(serial, frames, day=None, trim=False):
    """(serial, n_raw_uploads, start, end, max_alt), or None if nothing parses.

    Uploaders are not consistent about types -- `alt` arrives as both number and string --
    and frames are not guaranteed to be in time order, so everything here is coerced and
    the extremes taken rather than trusting the first and last entries.
    """
    times = []
    altitudes = []
    for frame in frames:
        if not isinstance(frame, dict):
            continue
        time = _timestamp(frame.get("datetime"))
        if not pd.isna(time):
            times.append(time)
        altitude = _as_float(frame.get("alt"))
        if not np.isnan(altitude):
            altitudes.append(altitude)
    times = plausible_times(sorted(times), day, trim)
    if not times:
        return None
    return (
        serial,
        len(frames),
        times[0],
        times[-1],
        max(altitudes) if altitudes else np.nan,
    )


def _mode(records, field):
    values = Counter(r[field] for r in records if r.get(field) is not None)
    return values.most_common(1)[0][0] if values else None


def build_attrs(serial, records, times, receivers, n_raw, sites, day=None):
    attrs = {"serial": serial}
    for field in FLIGHT_ATTRS:
        value = _mode(records, field)
        if value is not None:
            attrs[field] = value

    site = sites.get(str(attrs.get("launch_site", "")))
    if isinstance(site, dict):
        if site.get("station_name"):
            attrs["launch_site_name"] = site["station_name"]
        position = site.get("position") or []
        if len(position) == 2:
            attrs["launch_site_longitude"] = float(position[0])
            attrs["launch_site_latitude"] = float(position[1])
        if site.get("alt") is not None:
            attrs["launch_site_altitude"] = float(site["alt"])

    software = _mode(records, "software_name")
    if software:
        attrs["software"] = software
        version = _mode(records, "software_version")
        if version:
            attrs["software"] = f"{software} {version}"

    altitudes = np.array([_as_float(r.get("alt")) for r in records])
    if len(altitudes) and not np.isnan(altitudes).all():
        burst = int(np.nanargmax(altitudes))
        attrs["max_altitude"] = float(altitudes[burst])
        attrs["burst_time"] = times[burst].isoformat()

    attrs.update(
        {
            "start_time": times[0].isoformat(),
            "end_time": times[-1].isoformat(),
            "duration_seconds": float((times[-1] - times[0]).total_seconds()),
            "n_frames": len(records),
            "n_raw_uploads": int(n_raw),
            "n_receivers": len(receivers),
            "receivers": ",".join(name for name, _ in receivers.most_common(50)),
            "indexed_day": day.isoformat() if day is not None else None,
            "source": f"s3://{BUCKET}/serial/{serial}.json.gz",
            "index_source": f"s3://{BUCKET}/date/",
            "created": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
    )

    # A balloon flight lasts hours; a longer span means some receiver decoded a bad clock
    # (GPS week rollover and similar). The frames are kept -- they are usually real
    # telemetry under a wrong timestamp -- but the flight is marked so callers can filter.
    span = times[-1] - times[0]
    if span > MAX_FLIGHT_SPAN:
        attrs["time_anomaly"] = (
            f"frames span {span.total_seconds() / 86400:.1f} days, far longer than a "
            f"flight; some timestamps are wrong (sonde clock/GPS rollover)"
        )
    # NetCDF attributes must be strings or numbers.
    return {
        k: (v if isinstance(v, (str, int, float, np.number)) else str(v))
        for k, v in attrs.items()
        if v is not None and v != ""
    }


def to_dataset(serial, frames, sites, require_pressure, day=None, trim=False):
    """Flight dataset with telemetry variables and metadata attrs, or None if unusable."""
    records, times, counts, receivers = merge_frames(frames, day, trim)
    keep = [
        i
        for i, r in enumerate(records)
        if r.get("lat") is not None
        and r.get("lon") is not None
        and r.get("alt") is not None
        and (not require_pressure or r.get("pressure") is not None)
    ]
    if not keep:
        return None
    records = [records[i] for i in keep]
    times = [times[i] for i in keep]

    data = {
        name: (
            "time",
            np.array([_as_float(r.get(field)) for r in records], dtype="float64"),
            VARIABLE_ATTRS.get(name, {}),
        )
        for name, field in VARIABLES.items()
    }
    data["receiver_count"] = (
        "time",
        np.array([counts[t] for t in times], dtype="int32"),
        VARIABLE_ATTRS["receiver_count"],
    )
    # Columns no receiver ever reported carry no information.
    always = ("latitude", "longitude", "altitude", "receiver_count")
    data = {
        name: values
        for name, values in data.items()
        if name in always or not np.isnan(values[1]).all()
    }

    ds = xr.Dataset(data, coords={"time": pd.DatetimeIndex(times)})
    ds.attrs = build_attrs(serial, records, times, receivers, sum(counts.values()), sites, day)
    return ds


def flight_path(out_dir, serial):
    return os.path.join(out_dir, f"flight_{_SAFE_SERIAL.sub('_', serial)}.nc")


def write_flight(path, ds):
    tmp = f"{path}.tmp"
    encoding = {name: {"zlib": True, "complevel": 4} for name in ds.data_vars}
    ds.to_netcdf(tmp, encoding=encoding)
    os.replace(tmp, path)


def load_stats(path):
    if not os.path.exists(path):
        return {}
    return {str(row[0]): tuple(row) for row in np.load(path, allow_pickle=True).tolist()}


def save_stats(path, stats):
    tmp = f"{path}.tmp.npy"  # np.save appends .npy unless the name already ends in it
    np.save(tmp, np.array(list(stats.values()), dtype=object), allow_pickle=True)
    os.replace(tmp, path)


def load_seen(path):
    if not os.path.exists(path):
        return set()
    with open(path) as handle:
        return {line.strip() for line in handle if line.strip()}


def load_done_days(path):
    if not os.path.exists(path):
        return set()
    with open(path) as handle:
        return {date.fromisoformat(d) for d in json.load(handle).get("completed_days", [])}


def save_done_days(path, done):
    tmp = f"{path}.tmp"
    with open(tmp, "w") as handle:
        json.dump({"completed_days": sorted(d.isoformat() for d in done)}, handle)
    os.replace(tmp, path)


def run(args):
    s3 = make_client(args.workers, args.read_timeout)
    if args.netcdf:
        os.makedirs(args.out_dir, exist_ok=True)
    sites = {} if args.no_sites else fetch_sites()

    writer = FlightWriter(args.icechunk) if args.icechunk else None
    stats = load_stats(args.stats)
    seen = load_seen(args.seen) if args.resume else set()
    if writer is not None:
        # The store's own serial list is the authority on what has already landed; the
        # seen file only records what was fetched, which can run ahead after a crash.
        seen |= writer.seen
    done_days = load_done_days(args.state) if args.resume else set()
    days = [d for d in daterange_desc(args.start, args.end) if d not in done_days]
    if not days:
        print(f"Nothing to do: {args.start} -> {args.end} already complete")
        return

    print(
        f"Walking {days[0]} back to {days[-1]} ({len(days)} days, newest first); "
        f"{len(seen)} serials already pulled, {len(stats)} in stats, {len(sites)} launch sites; "
        f"sink {args.icechunk or 'none'}" + (f" + {args.out_dir}" if args.netcdf else ""),
        flush=True,
    )

    written = short = missing = failed = anomalous = 0
    since_save = day_failed = 0
    seen_file = open(args.seen, "a")

    def mark_seen(serial):
        # Held back until the day's commit lands: recording a serial whose flight was
        # discarded would skip it forever on every later run.
        seen.add(serial)
        day_seen.append(serial)

    def handle(serial, frames, day):
        nonlocal written, short, missing, failed, anomalous, since_save, day_failed
        if isinstance(frames, str):
            # A serial is only written off as seen when there is nothing left to try: a
            # missing object stays missing, but a network failure must stay retryable or a
            # blip silently erases those flights from the archive for good.
            if frames in PERMANENT_ERRORS:
                missing += 1
                mark_seen(serial)
            else:
                failed += 1
                day_failed += 1
                print(f"  ! {serial}: {frames}", file=sys.stderr)
            return 0
        mark_seen(serial)
        try:
            ds = to_dataset(serial, frames, sites, args.require_pressure, day, args.trim_to_day)
            # A flight spanning far more than a day has frames under a wrong clock; its
            # timestamps cannot be trusted, so it is dropped outright rather than written
            # with a caveat. --keep-anomalies writes it with a `time_anomaly` attribute.
            if ds is not None and "time_anomaly" in ds.attrs and not args.keep_anomalies:
                anomalous += 1
                return 0
            if len(frames) > args.min_stat_frames:
                row = flight_stats(serial, frames, day, args.trim_to_day)
                if row is not None:
                    stats[serial] = row
                    since_save += 1
            if ds is None or ds.sizes["time"] < args.min_frames:
                short += 1
                return 0
            if writer is not None:
                writer.add(ds)
            if args.netcdf:
                write_flight(flight_path(args.out_dir, serial), ds)
        except WriterError:
            # The store session was dropped mid-batch; this is a whole-day failure, not a
            # single bad flight, so let it out to the day loop.
            raise
        except Exception as err:  # one malformed flight must not end a multi-day run
            failed += 1
            print(f"  ! {serial}: {type(err).__name__}: {err}", file=sys.stderr)
            return 0
        written += 1
        return 1

    list_failures = commit_failures = 0
    day_seen = []
    pool = ThreadPoolExecutor(max_workers=args.workers)
    try:
        for day in days:
            try:
                serials = [s for s in list_serials(s3, day) if s not in seen]
                list_failures = 0
            except Exception as err:
                # Leave the day unmarked and move on, so one blip costs a day rather than
                # the run -- but stop entirely if they never let up.
                failed += 1
                list_failures += 1
                print(f"  ! list {day}: {type(err).__name__}: {err}", file=sys.stderr)
                if list_failures >= MAX_LIST_FAILURES:
                    print(
                        f"Giving up: {list_failures} consecutive listing failures. "
                        f"Days from {day} back are untouched; rerun once the cause is fixed.",
                        file=sys.stderr,
                    )
                    break
                continue
            day_written = 0
            day_failed = 0
            day_seen = []
            queue = iter(serials)
            pending = {}

            # Bounded look-ahead. A day holds 500+ flights and the big ones decompress to
            # hundreds of MB of Python objects, so completed-but-unconsumed results are what
            # drives peak memory -- at workers*2 this reached 15 GB RSS and was killed once
            # swap ran out. Parsing is single-threaded anyway, so a shallow queue costs
            # little throughput.
            def submit_more():
                while len(pending) < args.prefetch:
                    try:
                        serial = next(queue)
                    except StopIteration:
                        return
                    pending[pool.submit(fetch_flight, s3, serial)] = serial

            submit_more()
            while pending:
                finished, _ = wait(list(pending), return_when=FIRST_COMPLETED)
                for future in finished:
                    serial = pending.pop(future)
                    try:
                        frames = future.result()
                    except Exception as err:
                        # Nothing a worker raises may reach the day loop; the serial is
                        # left unmarked so a later run retries it.
                        failed += 1
                        day_failed += 1
                        print(f"  ! {serial}: {type(err).__name__}: {err}", file=sys.stderr)
                        continue
                    try:
                        day_written += handle(serial, frames, day)
                    except WriterError as err:
                        failed += 1
                        day_failed += 1
                        print(f"  ! writer {day}: {err}", file=sys.stderr)
                        pending.clear()
                        break
                submit_more()

            if writer is not None:
                try:
                    writer.commit(f"backfill {day} ({day_written} flights)")
                    commit_failures = 0
                except Exception as err:
                    commit_failures += 1
                    day_failed += 1
                    seen.difference_update(day_seen)
                    day_seen.clear()
                    print(f"  ! commit {day}: {type(err).__name__}: {err}", file=sys.stderr)
                    if commit_failures >= MAX_LIST_FAILURES:
                        print(
                            f"Giving up: {commit_failures} consecutive commit failures.",
                            file=sys.stderr,
                        )
                        break
            if day_seen:
                seen_file.write("".join(f"{s}\n" for s in day_seen))
                seen_file.flush()
                day_seen.clear()
            if since_save >= args.save_every:
                save_stats(args.stats, stats)
                since_save = 0
            # A day with unresolved fetches is not finished; leaving it unmarked is what
            # makes those serials get another go on the next run.
            if day_failed == 0:
                done_days.add(day)
                save_done_days(args.state, done_days)
            print(
                f"{day} serials={len(serials):5d} new_flights={day_written:4d} "
                f"total={written} short={short} anomalous={anomalous} "
                f"missing={missing} failed={failed}"
                + (f" INCOMPLETE({day_failed})" if day_failed else ""),
                flush=True,
            )
    except KeyboardInterrupt:
        print("\nInterrupted", file=sys.stderr)
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
        seen_file.close()
        if writer is not None:
            try:
                writer.commit("backfill final flush", force_index=True)
                if day_seen:
                    seen_file.write("".join(f"{s}\n" for s in day_seen))
            except Exception as err:
                print(f"  ! final commit: {type(err).__name__}: {err}", file=sys.stderr)
        save_stats(args.stats, stats)
        save_done_days(args.state, done_days)

    print(
        f"Done. {written} flights written, {short} below threshold, "
        f"{anomalous} dropped for bad timestamps, {missing} unavailable, "
        f"{failed} failed, {len(stats)} in stats."
    )


def plot_flight(args):
    import matplotlib.pyplot as plt

    path = flight_path(args.out_dir, args.plot)
    if os.path.exists(path):
        ds = xr.open_dataset(path)
        source = path
    else:
        from sondehub_icechunk import SondeFlights

        ds = SondeFlights(args.icechunk).sel(args.plot)
        source = f"{args.icechunk}:{args.plot}"
    times = pd.DatetimeIndex(ds.time.values)
    print(f"{source}: {ds.sizes['time']} frames, {times[0]} -> {times[-1]}")
    for key, value in ds.attrs.items():
        print(f"  {key}: {value}")

    plt.figure(figsize=(10, 6))
    plt.subplot(2, 1, 1)
    plt.plot(times, ds.altitude.values, label="Altitude (m)")
    plt.xlabel("Time")
    plt.ylabel("Altitude (m)")
    plt.title(f"Sondehub Flight Altitude Over Time ({args.plot})")
    plt.legend()
    plt.subplot(2, 1, 2)
    if "vertical_velocity" in ds:
        plt.plot(
            times, ds.vertical_velocity.values, label="Vertical Velocity (m/s)", color="orange"
        )
        plt.ylabel("Vertical Velocity (m/s)")
        plt.title("Sondehub Flight Vertical Velocity Over Time")
        plt.legend()
    plt.xlabel("Time")
    plt.tight_layout()
    plt.show()

    plt.figure(figsize=(10, 6))
    plt.plot(ds.longitude.values, ds.latitude.values, label="Flight Path", color="green")
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.title(f"Sondehub Flight Path ({args.plot})")
    plt.legend()
    plt.show()


def parse_args(argv=None):
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--start", type=date.fromisoformat, default=ARCHIVE_START, help="oldest day to walk back to"
    )
    parser.add_argument(
        "--end",
        type=date.fromisoformat,
        default=yesterday,
        help="newest day, processed first (default: yesterday)",
    )
    parser.add_argument(
        "--icechunk",
        default=DEFAULT_STORE,
        help="Icechunk store to append flights to (the primary output)",
    )
    parser.add_argument(
        "--no-icechunk",
        dest="icechunk",
        action="store_const",
        const=None,
        help="skip the Icechunk store",
    )
    parser.add_argument(
        "--netcdf", action="store_true", help="also write one NetCDF per flight into --out-dir"
    )
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    parser.add_argument("--stats", default=DEFAULT_STATS)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--seen", default=DEFAULT_SEEN)
    parser.add_argument("--workers", type=int, default=16, help="concurrent flight downloads")
    parser.add_argument(
        "--read-timeout",
        type=int,
        default=READ_TIMEOUT,
        help="seconds to wait on one S3 read; raise on a slow connection",
    )
    parser.add_argument(
        "--prefetch",
        type=int,
        default=20,
        help="max flights held in memory at once (peak RSS scales with this)",
    )
    parser.add_argument(
        "--min-frames",
        type=int,
        default=1000,
        help="minimum distinct timestamps for a flight to be written",
    )
    parser.add_argument(
        "--min-stat-frames",
        type=int,
        default=100,
        help="minimum raw uploads for a flight to enter the stats file",
    )
    parser.add_argument(
        "--no-require-pressure",
        dest="require_pressure",
        action="store_false",
        help="keep frames without a pressure reading (only some sonde types carry one)",
    )
    parser.add_argument(
        "--keep-anomalies",
        action="store_true",
        help="write flights whose frames span far more than a day instead "
        "of dropping them (they carry a time_anomaly attribute)",
    )
    parser.add_argument(
        "--trim-to-day",
        action="store_true",
        help="discard frames whose clock disagrees with the indexed day "
        "(clean time axis, but drops real telemetry from misdated sondes)",
    )
    parser.add_argument(
        "--no-sites", action="store_true", help="skip the launch-site catalogue lookup"
    )
    parser.add_argument("--save-every", type=int, default=2000, help="flights between stats saves")
    parser.add_argument(
        "--no-resume",
        dest="resume",
        action="store_false",
        help="ignore the state/seen files and start at --end",
    )
    parser.add_argument("--plot", metavar="SERIAL", help="plot an already-written flight")
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_args()
    if args.plot:
        plot_flight(args)
    else:
        run(args)
