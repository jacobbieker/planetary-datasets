"""Archive the WIS2 Global Cache as uncompressed tar files on Source Cooperative.

The WMO WIS2 Global Cache (``s3://wis2globalcache``, us-east-1, anonymous) holds the
GRIB, BUFR and NetCDF that WIS2 nodes publish, a day or so each before it ages out: a few
million objects of ~120 kB on average. At 00Z and 12Z this takes everything that arrived
in the preceding 12 hours and streams it into one tar on Source Cooperative::

    bkr/obs/wis2/wis2_20261029T1200Z.tar            <- arrivals 2026-10-29T00:00Z..12:00Z
    bkr/obs/wis2/wis2_20261029T1200Z.tar.index.csv.gz

named for the end of its window. Consecutive windows meet without overlapping (an object
belongs to the window its arrival time falls in, start inclusive, end exclusive), so each
arrival is in exactly one archive. Members keep their bucket keys
(``data/<centre>/data/core/...``) and arrival times as their mtimes; the index lists each
key, size, arrival time and ETag.

Nothing is staged on disk. The cache is listed and read with ``obstore`` (Rust, so a
single asyncio loop keeps hundreds of requests going without contending for the GIL):
every directory is listed concurrently, objects are fetched a few hundred at a time, and
each is appended to the tar as it arrives, the tar itself being written straight into a
multipart upload whose parts go up in parallel. Memory is bounded by the bytes allowed in
flight and the parts awaiting upload.

Archive targets (:mod:`~planetary_datasets.providers.observations.wis2_targets`) build
the WIS2 store, ``bkr/obs/wis2.icechunk``, from the same stream: each registered target is
handed the objects it wants as they go into the tar and appends what it decoded, to groups
of its own, once the tar is complete. A target's failure never fails the tar. Which targets
have finished a window is kept beside its tar (``<tar>.targets.json``: pending, done,
failed, and the store they were run for), so a run killed part way through the targets
finishes the pending ones from the archived tar when it comes to that window again.

Windows in order
    Appends to a group go forward in time: the TAC groups drop reports the BUFR groups'
    latest rows already hold, and readers expect a group's windows in order. So a target
    still pending over an older archive (a backfill) is not fed a new window: the tar is
    made as always, at once, and the target is recorded as pending over it, to be run from
    the tar, oldest first, between the live windows (:func:`finish_pending`). Archiving the
    cache, which keeps objects only a couple of days, never waits for a backfill.

The store the record is for
    A ``.targets.json`` written for another store (the five ``wis2_<kind>.icechunk`` stores
    this one replaced), or none at all (windows archived before records were kept), means
    every target is pending over that window: the targets' groups, and their ledgers, are
    what say what the store holds, and a target re-run over a window its groups already
    hold writes nothing. Pointing the targets at a new store therefore re-ingests every
    archived window into it, oldest first, by itself.

Run once for a given window end, or keep the 00Z/12Z schedule; run the targets over
windows already archived::

    python -m planetary_datasets.providers.observations.wis2_archive --end 2026-10-29T12:00
    python -m planetary_datasets.providers.observations.wis2_archive --follow
    python -m planetary_datasets.providers.observations.wis2_archive \
        --from-tar bkr/obs/wis2/wis2_20261027T1200Z.tar --targets aircraft
    python -m planetary_datasets.providers.observations.wis2_archive --mark-pending aircraft

``--all-archived`` runs targets over every archive in one go, outside the live loop; with a
``--follow`` process running it would write the same groups at once, out of order, so it is
for a store nothing else writes.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import datetime as dt
import gzip
import io
import json
import queue
import re
import sys
import tarfile
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Callable, Iterator, Sequence

from loguru import logger

from planetary_datasets.config import Config, get_config
from planetary_datasets.providers.observations.wis2_targets import (
    STORE_PREFIX,
    Router,
    build_targets,
    load_targets,
    run_targets,
)

SOURCE_BUCKET = "wis2globalcache"
SOURCE_REGION = "us-east-1"
DEST_PREFIX = "bkr/obs/wis2"
#: How often an archive is made (at 00Z and 12Z), and the arrivals each covers: the same,
#: so the windows tile time without gaps or overlaps.
EVERY = dt.timedelta(hours=12)
LOOKBACK = EVERY
#: How long after a window ends before it is archived, so late listings settle.
SETTLE = dt.timedelta(minutes=10)
#: How often a sleeping follow() logs that it is alive.
HEARTBEAT = dt.timedelta(hours=1)
#: Window ends still worth archiving when a run was missed. The cache keeps objects for
#: about two and a half days (a few stragglers longer), so older windows are mostly gone.
CATCH_UP = dt.timedelta(days=2)
#: A pending target is only started over an older archive with this long left before the
#: next window, so backfilling never holds up the live schedule.
PENDING_BUDGET = dt.timedelta(hours=3)
#: How soon a window whose archive failed is tried again.
RETRY_FAILED = dt.timedelta(minutes=30)

#: Directories listed, and objects fetched, at once.
LIST_WORKERS = 256
FETCH_WORKERS = 512
#: Bytes fetched but not yet in the tar.
MAX_IN_FLIGHT = 4 * 1024**3
#: Multipart part size and parts awaiting upload: 10,000 parts of 256 MB is 2.5 TB.
PART_SIZE = 256 * 1024**2
UPLOAD_WORKERS = 8


@dataclass(frozen=True)
class Entry:
    """One object of the cache."""

    key: str
    size: int
    modified: dt.datetime
    etag: str = ""


def archive_name(end: dt.datetime) -> str:
    """The tar's key for the window ending at ``end``."""
    return f"{DEST_PREFIX}/wis2_{end:%Y%m%dT%H%M}Z.tar"


def archive_key(end: dt.datetime, config: Config) -> str:
    """The tar's key in ``config``'s bucket: :func:`archive_name` under its prefix."""
    return (
        config.prefix.strip("/") + "/" + archive_name(end) if config.prefix else archive_name(end)
    )


def last_boundary(when: dt.datetime) -> dt.datetime:
    """The latest 00Z or 12Z at or before ``when``."""
    floor = when.replace(minute=0, second=0, microsecond=0)
    return floor - dt.timedelta(hours=floor.hour % 12)


def window_ends(now: dt.datetime, back: dt.timedelta = CATCH_UP) -> list[dt.datetime]:
    """The 00Z/12Z window ends that have settled by ``now``, oldest first, within ``back``."""
    ends = []
    end = last_boundary(now - SETTLE)
    while end > now - back:
        ends.append(end)
        end -= EVERY
    return ends[::-1]


# ----------------------------------------------------------------------------- clients


def source_store():
    """Anonymous ``obstore`` store for the cache bucket."""
    from obstore.store import S3Store

    return S3Store(
        SOURCE_BUCKET,
        region=SOURCE_REGION,
        skip_signature=True,
        retry_config={"max_retries": 10, "backoff": {"init_backoff": dt.timedelta(seconds=1)}},
        client_options={"timeout": dt.timedelta(minutes=5)},
    )


def dest_client(config: Config):
    """Client for the Source Cooperative bucket, with the configured credentials."""
    import boto3
    import botocore

    creds = config.credentials
    return boto3.client(
        "s3",
        region_name=config.region,
        endpoint_url=config.endpoint_url,
        aws_access_key_id=creds.aws_access_key_id,
        aws_secret_access_key=creds.aws_secret_access_key,
        config=botocore.config.Config(
            max_pool_connections=UPLOAD_WORKERS * 2,
            retries={"max_attempts": 10, "mode": "adaptive"},
        ),
    )


# ----------------------------------------------------------------------------- listing


def list_window(
    store, start: dt.datetime, end: dt.datetime, workers: int = LIST_WORKERS
) -> list[Entry]:
    """Every object that arrived in ``[start, end)``, every directory listed concurrently."""
    import obstore

    found: list[Entry] = []

    async def run() -> None:
        slots = asyncio.Semaphore(workers)

        async def walk(prefix: str | None) -> None:
            async with slots:
                listed = await obstore.list_with_delimiter_async(store, prefix)
            found.extend(
                Entry(o["path"], o["size"], o["last_modified"], (o.get("e_tag") or "").strip('"'))
                for o in listed["objects"]
                if start <= o["last_modified"] < end
            )
            await asyncio.gather(*(walk(p) for p in listed["common_prefixes"]))

        await walk(None)

    asyncio.run(run())
    found.sort(key=lambda e: e.key)
    return found


# ----------------------------------------------------------------------------- upload


class MultipartWriter(io.RawIOBase):
    """A write-only file whose bytes become an S3 multipart upload.

    Full parts are uploaded by a pool while writing continues; at most
    ``UPLOAD_WORKERS * 2`` parts wait in memory. :meth:`close` completes the upload,
    :meth:`abort` discards it.
    """

    def __init__(self, client, bucket: str, key: str, part_size: int | None = None):
        """A multipart upload to ``bucket``/``key``, started at once."""
        super().__init__()
        self.client, self.bucket, self.key = client, bucket, key
        self.part_size = part_size or PART_SIZE
        self.upload_id = client.create_multipart_upload(Bucket=bucket, Key=key)["UploadId"]
        self.buffer = bytearray()
        self.parts: dict[int, str] = {}
        self.futures: list[Future] = []
        self.pool = ThreadPoolExecutor(UPLOAD_WORKERS)
        self.slots = threading.Semaphore(UPLOAD_WORKERS * 2)
        self.written = 0
        self.finished = False

    def writable(self) -> bool:
        """Always: this is a write-only stream."""
        return True

    def write(self, data) -> int:
        """Buffer ``data``, sending each full part as it fills."""
        self.buffer += data
        self.written += len(data)
        while len(self.buffer) >= self.part_size:
            self._send(bytes(self.buffer[: self.part_size]))
            del self.buffer[: self.part_size]
        return len(data)

    def _send(self, body: bytes) -> None:
        number = len(self.futures) + 1
        self.slots.acquire()

        def upload() -> None:
            try:
                response = self.client.upload_part(
                    Bucket=self.bucket,
                    Key=self.key,
                    UploadId=self.upload_id,
                    PartNumber=number,
                    Body=body,
                )
                self.parts[number] = response["ETag"]
            finally:
                self.slots.release()

        self.futures.append(self.pool.submit(upload))
        # Surface a failed part now rather than after the whole archive is read.
        for future in self.futures:
            if future.done() and future.exception():
                raise future.exception()

    def close(self) -> None:
        """Send the last part and complete the upload."""
        if self.closed or self.finished:
            return super().close()
        if self.buffer or not self.futures:
            self._send(bytes(self.buffer))
            self.buffer.clear()
        for future in self.futures:
            future.result()
        self.pool.shutdown()
        self.client.complete_multipart_upload(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=self.upload_id,
            MultipartUpload={
                "Parts": [{"PartNumber": n, "ETag": self.parts[n]} for n in sorted(self.parts)]
            },
        )
        self.finished = True
        super().close()

    def abort(self) -> None:
        """Discard the upload; nothing appears at the key."""
        self.pool.shutdown(cancel_futures=True)
        self.client.abort_multipart_upload(
            Bucket=self.bucket, Key=self.key, UploadId=self.upload_id
        )
        self.finished = True
        super().close()


# ----------------------------------------------------------------------------- fetching


_DONE = object()


def fetch_all(
    store,
    entries: Sequence[Entry],
    workers: int = FETCH_WORKERS,
    max_in_flight: int = MAX_IN_FLIGHT,
) -> Iterator[tuple[Entry, bytes | None]]:
    """``(entry, body)`` for every entry, in completion order; ``None`` if it has gone.

    An asyncio loop on a background thread fetches up to ``workers`` objects at once,
    holding at most ``max_in_flight`` bytes not yet handed on (an object larger than that
    is still fetched, alone).
    """
    import obstore

    results: queue.Queue = queue.Queue()
    lock = threading.Lock()
    held = 0
    stop = threading.Event()

    async def run() -> None:
        nonlocal held
        slots = asyncio.Semaphore(workers)
        tasks: set[asyncio.Task] = set()

        async def one(entry: Entry) -> None:
            try:
                try:
                    body = await (await obstore.get_async(store, entry.key)).bytes_async()
                except FileNotFoundError:
                    body = None
                results.put((entry, body))
            except BaseException as exc:  # noqa: BLE001 - handed to the consumer to raise
                results.put(exc)
            finally:
                slots.release()

        for entry in entries:
            if stop.is_set():
                break
            await slots.acquire()
            while True:
                with lock:
                    if held == 0 or held + entry.size <= max_in_flight:
                        held += entry.size
                        break
                await asyncio.sleep(0.005)
            task = asyncio.create_task(one(entry))
            tasks.add(task)
            task.add_done_callback(tasks.discard)
        await asyncio.gather(*tasks)

    def loop() -> None:
        try:
            asyncio.run(run())
        except BaseException as exc:  # noqa: BLE001 - as above
            results.put(exc)
        results.put(_DONE)

    threading.Thread(target=loop, name="wis2-fetch", daemon=True).start()
    try:
        while (item := results.get()) is not _DONE:
            if isinstance(item, BaseException):
                raise item
            entry, body = item
            with lock:
                held -= entry.size
            yield entry, body
    finally:
        stop.set()


# ----------------------------------------------------------------------------- archive


@dataclass
class Summary:
    """What one archive run did: the tar, its files and the targets' summaries."""

    key: str
    files: int = 0
    gone: int = 0
    bytes: int = 0
    seconds: float = 0.0
    targets: dict = field(default_factory=dict)


def write_archive(
    entries: Sequence[Entry],
    fetch: Callable[[Sequence[Entry]], Iterator[tuple[Entry, bytes | None]]],
    out: io.RawIOBase,
    observe: Callable[[str, int, bytes, dt.datetime], None] | None = None,
) -> tuple[int, int, int, bytes]:
    """Stream ``entries`` into an uncompressed tar on ``out``.

    ``observe(key, size, body, arrived)`` sees every file written (the archive targets'
    :class:`~planetary_datasets.providers.observations.wis2_targets.Router`).

    Returns:
        Files written, files gone before they could be fetched, payload bytes, and the
        gzipped CSV index of what was written.
    """
    index = io.BytesIO()
    with gzip.GzipFile(fileobj=index, mode="wb") as zipped:
        text = io.TextIOWrapper(zipped, encoding="utf-8", newline="")
        rows = csv.writer(text)
        rows.writerow(["key", "size", "last_modified", "etag"])
        files = gone = payload = 0
        with tarfile.open(fileobj=out, mode="w|", format=tarfile.PAX_FORMAT) as tar:
            for entry, body in fetch(entries):
                if body is None:
                    gone += 1
                    continue
                info = tarfile.TarInfo(entry.key)
                info.size = len(body)
                info.mtime = entry.modified.timestamp()
                info.mode = 0o644
                tar.addfile(info, io.BytesIO(body))
                if observe is not None:
                    observe(entry.key, len(body), body, entry.modified)
                rows.writerow([entry.key, len(body), entry.modified.isoformat(), entry.etag])
                files += 1
                payload += len(body)
        text.flush()
        text.detach()
    return files, gone, payload, index.getvalue()


def exists(client, bucket: str, key: str) -> bool:
    """Whether ``key`` is in ``bucket`` (a 404 is False; other errors raise)."""
    from botocore.exceptions import ClientError

    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def partition_name(tar_key: str) -> str:
    """The ledger entry the archive targets record a window under: the tar's stem."""
    return tar_key.rsplit("/", 1)[-1].removesuffix(".tar")


def archive(
    end: dt.datetime,
    config: Config | None = None,
    force: bool = False,
    targets: Sequence[str] | None = None,
) -> Summary:
    """Archive the arrivals of the 12 hours before ``end`` (UTC).

    The archive targets named in ``targets`` (all registered when None, none when empty)
    are fed every file as it streams and finished once the tar is complete; a target's
    failure is logged and never fails the tar. When the tar exists already, only the
    targets its ``.targets.json`` still lists as pending run, over the archived tar: what
    a run killed part way through the targets left undone (a record for another store, or
    none, lists every target). Targets still pending over an older archive are left
    pending here too, for :func:`finish_pending` to run in time order.
    """
    config = config or get_config()
    end = end.astimezone(dt.timezone.utc) if end.tzinfo else end.replace(tzinfo=dt.timezone.utc)
    start = end - LOOKBACK
    dest = dest_client(config)
    key = archive_key(end, config)
    summary = Summary(key=key)
    if not force and exists(dest, config.bucket, key):
        state = current_state(_load_target_state(dest, config.bucket, key))
        pending = [n for n in state["pending"] if targets is None or n in targets]
        behind = _older_pending(dest, config, key, pending)
        ready = [n for n in pending if n not in behind]
        if ready:
            logger.info(
                f"wis2: {key} already archived, but target(s) {', '.join(ready)} did not "
                "finish; running them over the archived tar"
            )
            summary.targets = targets_from_tar(key, config, targets=ready)
        elif pending:
            logger.info(
                f"wis2: {key} already archived; {', '.join(pending)} wait for older archives"
            )
        else:
            logger.info(f"wis2: {key} already archived")
        return summary
    began = time.time()
    source = source_store()
    entries = list_window(source, start, end)
    total = sum(e.size for e in entries)
    logger.info(
        f"wis2: {len(entries):,} object(s), {total / 1e9:.1f} GB arrived "
        f"{start:%Y-%m-%dT%H:%MZ}..{end:%Y-%m-%dT%H:%MZ} (listed in {time.time() - began:.0f}s)"
    )
    built = _build_targets(config, targets)
    # Targets with older archives still to run over wait for them: recorded as pending
    # here, run from the tar in time order. The tar itself is made now regardless.
    behind = _older_pending(dest, config, key, [t.name for t in built])
    if behind:
        logger.info(
            f"wis2: {', '.join(sorted(behind))} still pending over older archives; recorded "
            f"as pending over {key}, to run after them"
        )
    live = [t for t in built if t.name not in behind]
    _prepare(live, partition_name(key))
    out = MultipartWriter(dest, config.bucket, key)
    try:
        files, gone, payload, index = write_archive(
            entries, lambda es: fetch_all(source, es), out, observe=Router(live) if live else None
        )
        out.close()
    except BaseException:
        out.abort()
        raise
    dest.put_object(Bucket=config.bucket, Key=f"{key}.index.csv.gz", Body=index)
    state = None
    if built:
        state = new_state([t.name for t in built])
        _save_target_state(dest, config.bucket, key, state)
    summary.files, summary.gone, summary.bytes = files, gone, payload
    summary.seconds = time.time() - began
    logger.info(
        f"wis2: wrote {key}: {files:,} file(s), {payload / 1e9:.1f} GB "
        f"({out.written / 1e9:.1f} GB tar), {gone:,} gone before fetching, "
        f"in {summary.seconds / 60:.1f} min"
    )
    if live:
        summary.targets = _finish_targets(dest, config.bucket, key, live, start, end, state)
    return summary


def _target_state_key(tar_key: str) -> str:
    return f"{tar_key}.targets.json"


def registered_targets() -> list[str]:
    """Names of every registered archive target."""
    return sorted(load_targets())


def new_state(pending: Sequence[str]) -> dict:
    """A window's target record for :data:`STORE_PREFIX` with ``pending`` still to run."""
    return {"store": STORE_PREFIX, "pending": list(pending), "done": [], "failed": []}


def current_state(state: dict | None) -> dict:
    """``state`` if it was kept for :data:`STORE_PREFIX`, else every target pending.

    A record from before the store (or none, for windows archived before records were
    kept) says nothing about what this store holds: every registered target is due over
    the window, and the targets' own ledgers decide what is written.
    """
    if state is not None and state.get("store") == STORE_PREFIX:
        return {"pending": [], "done": [], "failed": [], **state}
    return new_state(registered_targets())


def _older_pending(client, config: Config, tar_key: str, names: Sequence[str]) -> set[str]:
    """Of ``names``, the targets still pending over an archive older than ``tar_key``.

    A failure to look is logged and counts as none, so the live window is never held up.
    """
    names = set(names)
    if not names:
        return set()
    found: set[str] = set()
    try:
        for key in archived_tars(config):
            if _window_end(key) is None or key >= tar_key:
                continue
            state = current_state(_load_target_state(client, config.bucket, key))
            found |= names & set(state["pending"])
            if found == names:
                break
    except Exception as exc:  # noqa: BLE001 - the live window must go ahead
        logger.warning(f"wis2: could not check older archives for pending targets: {exc}")
        return set()
    return found


def _load_target_state(client, bucket: str, tar_key: str) -> dict | None:
    """The window's record of pending, done and failed targets; None if it has none."""
    state_key = _target_state_key(tar_key)
    if not exists(client, bucket, state_key):
        return None
    return json.loads(client.get_object(Bucket=bucket, Key=state_key)["Body"].read())


def _save_target_state(client, bucket: str, tar_key: str, state: dict) -> None:
    state["store"] = STORE_PREFIX
    body = json.dumps(state, sort_keys=True).encode()
    client.put_object(Bucket=bucket, Key=_target_state_key(tar_key), Body=body)


def _finish_targets(
    client,
    bucket: str,
    tar_key: str,
    built: list,
    start: dt.datetime | None,
    end: dt.datetime | None,
    state: dict | None,
) -> dict[str, dict]:
    """Finish each target in turn, recording it in ``state`` (when kept) as it does."""
    summaries = {}
    for target in built:
        summaries.update(run_targets([target], start, end, partition_name(tar_key)))
        if state is None:
            continue
        outcome = "failed" if "error" in summaries.get(target.name, {}) else "done"
        for name in ("pending", "done", "failed"):
            state[name] = [n for n in state.get(name, []) if n != target.name]
        state[outcome].append(target.name)
        _save_target_state(client, bucket, tar_key, state)
    return summaries


def _prepare(built: list, partition: str) -> None:
    """Tell each target which window is coming; a failure there never stops the window."""
    for target in built:
        try:
            target.prepare(partition)
        except Exception as exc:  # noqa: BLE001 - targets never stop the archive
            logger.warning(f"wis2 {target.name}: preparing for {partition} failed: {exc}")


def _build_targets(config: Config, names: Sequence[str] | None) -> list:
    """The archive targets to run; none (logged) if they cannot be built."""
    if names is not None and not names:
        return []
    try:
        return build_targets(config, names)
    except Exception as exc:  # noqa: BLE001 - targets never stop the archive
        logger.exception(f"wis2: archive targets not built: {exc}")
        return []


def targets_from_tar(
    tar_key: str, config: Config | None = None, targets: Sequence[str] | None = None
) -> dict[str, dict]:
    """Run the archive targets over a window already archived at ``tar_key``.

    The tar is streamed from Source Cooperative; only the files a target wants are read
    into memory. A window's ``.targets.json``, where it has one, is updated as each target
    finishes.
    """
    config = config or get_config()
    built = _build_targets(config, targets)
    if not built:
        return {}
    _prepare(built, partition_name(tar_key))
    route = Router(built)
    client = dest_client(config)
    body = client.get_object(Bucket=config.bucket, Key=tar_key)["Body"]
    with tarfile.open(fileobj=body, mode="r|") as tar:
        for member in tar:
            if not member.isfile() or not any(t.wants(member.name, member.size) for t in built):
                continue
            arrived = dt.datetime.fromtimestamp(member.mtime, dt.timezone.utc)
            route(member.name, member.size, tar.extractfile(member).read(), arrived)
    end = _window_end(tar_key)
    start = end - LOOKBACK if end else None
    state = current_state(_load_target_state(client, config.bucket, tar_key))
    return _finish_targets(client, config.bucket, tar_key, built, start, end, state)


def _window_end(tar_key: str) -> dt.datetime | None:
    match = re.search(r"wis2_(\d{8}T\d{4})Z\.tar$", tar_key)
    if not match:
        return None
    return dt.datetime.strptime(match[1], "%Y%m%dT%H%M").replace(tzinfo=dt.timezone.utc)


def archived_tars(config: Config | None = None) -> list[str]:
    """Keys of the archives on Source Cooperative, oldest first."""
    config = config or get_config()
    client = dest_client(config)
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=config.bucket, Prefix=f"{DEST_PREFIX}/"
    ):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".tar")]
    return sorted(keys)


def wait(upcoming: dt.datetime, pause: float, sleep: Callable[[float], None] = time.sleep) -> None:
    """Sleep ``pause`` seconds, logging every :data:`HEARTBEAT`.

    The box's guard restarts a lane whose log is silent for over three hours, and the wait
    between windows is longer.
    """
    while pause > 0:
        step = min(pause, HEARTBEAT.total_seconds())
        sleep(step)
        pause -= step
        if pause > 0:
            logger.info(f"wis2: waiting for the {upcoming:%H:%MZ} archive ({pause / 3600:.1f} h)")


def mark_pending(
    names: Sequence[str], config: Config | None = None, keys: Sequence[str] | None = None
) -> int:
    """Record ``names`` as pending in every archived window's target record.

    For a backfill: :func:`follow` then runs them over each archive between the live
    windows, from the one process that writes the stores. Returns the windows marked.
    """
    config = config or get_config()
    client = dest_client(config)
    marked = 0
    for key in keys if keys is not None else archived_tars(config):
        loaded = _load_target_state(client, config.bucket, key)
        state = current_state(loaded)
        # A record for another store (or none) is rewritten for this one: every target
        # pending, which already includes ``names``.
        stale = (loaded or {}).get("store") != STORE_PREFIX
        before = list(state["pending"])
        for name in names:
            for other in ("done", "failed"):
                state[other] = [n for n in state.get(other, []) if n != name]
            if name not in state["pending"]:
                state["pending"].append(name)
        if stale or state["pending"] != before or not before:
            _save_target_state(client, config.bucket, key, state)
            marked += 1
    logger.info(f"wis2: marked {', '.join(names)} pending over {marked} archived window(s)")
    return marked


def finish_pending(
    deadline: dt.datetime,
    config: Config | None = None,
    targets: Sequence[str] | None = None,
    now: Callable[[], dt.datetime] = lambda: dt.datetime.now(dt.timezone.utc),
    limit: int | None = None,
) -> int:
    """Run pending targets over older archives, oldest first, while time allows.

    One is only started with :func:`pending_budget` left before ``deadline`` (the next
    live window): :data:`PENDING_BUDGET`, or more once a run has taken longer, so a run
    never makes the next live archive wait. At most ``limit`` are attempted, failed ones
    included. Returns the archives worked on.
    """
    config = config or get_config()
    client = dest_client(config)
    worked = attempted = 0
    for key in archived_tars(config):
        if now() + pending_budget() > deadline or (limit is not None and attempted >= limit):
            break
        state = current_state(_load_target_state(client, config.bucket, key))
        pending = [n for n in state["pending"] if targets is None or n in targets]
        if not pending:
            continue
        logger.info(f"wis2: finishing {', '.join(pending)} over {key}")
        attempted += 1
        started = now()
        try:
            targets_from_tar(key, config, targets=pending)
        except Exception as exc:  # noqa: BLE001 - the live schedule must carry on
            # Not counted: the caller would otherwise retry it at once, in a loop.
            logger.exception(f"wis2: pending targets over {key} failed: {exc}")
            continue
        finally:
            _PENDING_RUNS.append(now() - started)
            del _PENDING_RUNS[:-RECENT_PENDING_RUNS]
        worked += 1
    return worked


#: Durations of the latest pending runs, the longest of which sizes :func:`pending_budget`.
_PENDING_RUNS: list[dt.timedelta] = []
RECENT_PENDING_RUNS = 8


def pending_budget() -> dt.timedelta:
    """Time to leave before the next live window when starting a pending run.

    :data:`PENDING_BUDGET`, or half as long again as the longest recent run: re-ingesting
    every target over an archive takes longer than the one target a backfill usually
    runs, and must still end before the next window is due. Capped at half the window
    interval, so one very long run cannot leave a budget no gap between windows meets
    (no pending run would start again, and none would record a shorter time).
    """
    longest = max(_PENDING_RUNS, default=dt.timedelta(0))
    return min(max(PENDING_BUDGET, longest * 1.5), EVERY / 2)


def follow(config: Config | None = None, targets: Sequence[str] | None = None) -> None:
    """Archive each settled 00Z/12Z window not yet archived, forever.

    Between windows, targets left pending over older archives (a backfill marked with
    :func:`mark_pending`) are finished, without starting one too close to the next window.
    """
    while True:
        now = dt.datetime.now(dt.timezone.utc)
        failed: set[dt.datetime] = set()
        ends = window_ends(now)
        # Windows with no archive first: an archived one with targets still pending (a
        # backfill reaching the last two days) is finished inside archive() and can take
        # twenty minutes, which a window waiting to be archived should not.
        new = set(_unarchived(ends, config))
        for end in sorted(ends, key=lambda e: (e not in new, e)):
            try:
                archive(end, config, targets=targets)
            except Exception as exc:  # noqa: BLE001 - the next window must still be tried
                failed.add(end)
                logger.exception(f"wis2: archive ending {end:%Y-%m-%dT%H:%MZ} failed: {exc}")
        now = dt.datetime.now(dt.timezone.utc)
        upcoming = last_boundary(now - SETTLE) + EVERY + SETTLE
        try:
            # One archive at a time, then the live windows again: a window that came due
            # while the backfill ran waits for at most one archive, not the whole backfill
            # (the 2026-11-06 00Z window waited eight hours behind nineteen).
            if finish_pending(upcoming, config, targets, limit=1):
                continue
        except Exception as exc:  # noqa: BLE001 - listing the archives must not stop the loop
            logger.exception(f"wis2: checking pending targets failed: {exc}")
        now = dt.datetime.now(dt.timezone.utc)
        upcoming = last_boundary(now - SETTLE) + EVERY + SETTLE
        pause = max(60.0, (upcoming - now).total_seconds())
        unarchived = _unarchived(window_ends(now), config)
        if unarchived:
            # A window still unarchived is tried again soon rather than after the next
            # window: within a minute if it came due while pending targets ran, after
            # RETRY_FAILED if its archive just failed (each try lists the whole cache).
            wait_for = RETRY_FAILED if set(unarchived) <= failed else dt.timedelta(seconds=60)
            upcoming, pause = now + wait_for, wait_for.total_seconds()
            logger.info(f"wis2: {unarchived[0]:%Y-%m-%dT%H:%MZ} is due and not archived")
        logger.info(
            f"wis2: next archive at {upcoming:%Y-%m-%dT%H:%MZ}; sleeping {pause / 3600:.1f} h"
        )
        wait(upcoming, pause)


def _unarchived(ends: Sequence[dt.datetime], config: Config | None) -> list[dt.datetime]:
    """The window ends among ``ends`` with no archive yet.

    All of them if that cannot be checked, so a failed check means trying again soon
    rather than sleeping past them.
    """
    try:
        config = config or get_config()
        client = dest_client(config)
        return [end for end in ends if not exists(client, config.bucket, archive_key(end, config))]
    except Exception as exc:  # noqa: BLE001 - the schedule must not stop on a listing error
        logger.warning(f"wis2: could not check for due windows: {exc}")
        return list(ends)


def main(argv: Sequence[str] | None = None) -> int:
    """Command line: see the module's docstring."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--end", help="window end, UTC ISO 8601 (e.g. 2026-10-29T12:00)")
    group.add_argument("--follow", action="store_true", help="keep the 00Z/12Z schedule")
    group.add_argument("--from-tar", help="run the archive targets over an archived window")
    group.add_argument(
        "--all-archived", action="store_true", help="run the targets over every archived window"
    )
    group.add_argument(
        "--mark-pending",
        help="comma-separated targets to backfill: marked pending over every archived window, "
        "for --follow to finish between live windows",
    )
    parser.add_argument("--force", action="store_true", help="rewrite an existing archive")
    parser.add_argument(
        "--targets",
        help="comma-separated archive targets (default: all registered; 'none' for none)",
    )
    args = parser.parse_args(argv)
    targets = None
    if args.targets is not None:
        targets = [] if args.targets == "none" else [t for t in args.targets.split(",") if t]
    if args.mark_pending:
        mark_pending([t for t in args.mark_pending.split(",") if t])
    elif args.follow:
        follow(targets=targets)
    elif args.from_tar or args.all_archived:
        for key in [args.from_tar] if args.from_tar else archived_tars():
            try:
                targets_from_tar(key, targets=targets)
            except Exception as exc:  # noqa: BLE001 - the next archive must still be tried
                logger.exception(f"wis2: targets over {key} failed: {exc}")
    else:
        archive(dt.datetime.fromisoformat(args.end), force=args.force, targets=targets)
    return 0


if __name__ == "__main__":
    sys.exit(main())
