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
stores from the same stream: each registered target is handed the objects it wants as
they go into the tar and appends what it decoded once the tar is complete (aircraft
reports, ``bkr/obs/wis2_aircraft.icechunk``, for one). A target's failure never fails the
tar.

Run once for a given window end, or keep the 00Z/12Z schedule; run the targets over
windows already archived::

    python -m planetary_datasets.providers.observations.wis2_archive --end 2026-10-29T12:00
    python -m planetary_datasets.providers.observations.wis2_archive --follow
    python -m planetary_datasets.providers.observations.wis2_archive \
        --from-tar bkr/obs/wis2/wis2_20261027T1200Z.tar --targets aircraft
    python -m planetary_datasets.providers.observations.wis2_archive --all-archived
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import datetime as dt
import gzip
import io
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
    Router,
    build_targets,
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
        return True

    def write(self, data) -> int:
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
    failure is logged and never fails the tar. When the tar exists already nothing runs:
    use :func:`targets_from_tar` for that window.
    """
    config = config or get_config()
    end = end.astimezone(dt.timezone.utc) if end.tzinfo else end.replace(tzinfo=dt.timezone.utc)
    start = end - LOOKBACK
    dest = dest_client(config)
    key = config.prefix.strip("/") + "/" + archive_name(end) if config.prefix else archive_name(end)
    summary = Summary(key=key)
    if not force and exists(dest, config.bucket, key):
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
    out = MultipartWriter(dest, config.bucket, key)
    try:
        files, gone, payload, index = write_archive(
            entries, lambda es: fetch_all(source, es), out, observe=Router(built) if built else None
        )
        out.close()
    except BaseException:
        out.abort()
        raise
    dest.put_object(Bucket=config.bucket, Key=f"{key}.index.csv.gz", Body=index)
    summary.files, summary.gone, summary.bytes = files, gone, payload
    summary.seconds = time.time() - began
    logger.info(
        f"wis2: wrote {key}: {files:,} file(s), {payload / 1e9:.1f} GB "
        f"({out.written / 1e9:.1f} GB tar), {gone:,} gone before fetching, "
        f"in {summary.seconds / 60:.1f} min"
    )
    if built:
        summary.targets = run_targets(built, start, end, partition_name(key))
    return summary


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
    into memory.
    """
    config = config or get_config()
    built = _build_targets(config, targets)
    if not built:
        return {}
    route = Router(built)
    body = dest_client(config).get_object(Bucket=config.bucket, Key=tar_key)["Body"]
    with tarfile.open(fileobj=body, mode="r|") as tar:
        for member in tar:
            if not member.isfile() or not any(t.wants(member.name, member.size) for t in built):
                continue
            arrived = dt.datetime.fromtimestamp(member.mtime, dt.timezone.utc)
            route(member.name, member.size, tar.extractfile(member).read(), arrived)
    end = _window_end(tar_key)
    start = end - LOOKBACK if end else None
    return run_targets(built, start, end, partition_name(tar_key))


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


def follow(config: Config | None = None, targets: Sequence[str] | None = None) -> None:
    """Archive each settled 00Z/12Z window not yet archived, forever."""
    while True:
        now = dt.datetime.now(dt.timezone.utc)
        for end in window_ends(now):
            try:
                archive(end, config, targets=targets)
            except Exception as exc:  # noqa: BLE001 - the next window must still be tried
                logger.exception(f"wis2: archive ending {end:%Y-%m-%dT%H:%MZ} failed: {exc}")
        now = dt.datetime.now(dt.timezone.utc)
        upcoming = last_boundary(now - SETTLE) + EVERY + SETTLE
        pause = max(60.0, (upcoming - now).total_seconds())
        logger.info(
            f"wis2: next archive at {upcoming:%Y-%m-%dT%H:%MZ}; sleeping {pause / 3600:.1f} h"
        )
        wait(upcoming, pause)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--end", help="window end, UTC ISO 8601 (e.g. 2026-10-29T12:00)")
    group.add_argument("--follow", action="store_true", help="keep the 00Z/12Z schedule")
    group.add_argument("--from-tar", help="run the archive targets over an archived window")
    group.add_argument(
        "--all-archived", action="store_true", help="run the targets over every archived window"
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
    if args.follow:
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
