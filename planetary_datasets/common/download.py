"""Downloading with retries, atomic writes and skip-if-present.

Consolidates the ``_download_one`` helper that appeared verbatim in the GFS scripts and the
ad hoc "if not os.path.exists(path)" loops in most of the others. Downloads land on a
``.part`` file and are renamed into place only once complete, so an interrupted run leaves
no truncated file that a later skip-if-present check would mistake for good data.
"""

from __future__ import annotations

import hashlib
import os
import pathlib
import threading
import time
import traceback
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Iterable, Sequence, TypeVar

import fsspec
from fsspec.core import split_protocol
from fsspec.spec import AbstractFileSystem
from fsspec.utils import tokenize
from loguru import logger

CHUNK_SIZE = 1024 * 1024

# Failures that will not succeed on a retry. fsspec surfaces HTTP 404 as FileNotFoundError,
# and s3fs maps S3's 403 (AccessDenied) to PermissionError.
PERMANENT_ERRORS = (FileNotFoundError, PermissionError, IsADirectoryError)

# fsspec's HTTP filesystem raises FileNotFoundError only for a 404; any other error status
# arrives as aiohttp's ClientResponseError. These statuses will not change on a retry.
PERMANENT_HTTP_STATUSES = frozenset({401, 403, 404, 410})

_T = TypeVar("_T")

#: Least seconds between requests from one process to a host that limits each client
#: address. GDEX (Apache mod_qos) answers HTTP 500 (and sometimes a false 404) to
#: everything from an address that requests faster than it allows, which the backfill box
#: tripped repeatedly; two seconds apart over three processes was still too fast.
HOST_MIN_INTERVAL: dict[str, float] = {"data.gdex.ucar.edu": 5.0}
#: Least base backoff, in seconds, before retrying a failed request to such a host: its
#: refusal lasts a while, so the default one-second retries only extend it.
HOST_MIN_BACKOFF: dict[str, float] = {"data.gdex.ucar.edu": 60.0}

_HOST_NEXT: dict[str, float] = {}
_HOST_LOCK = threading.Lock()


def _host(url: str) -> str:
    return urllib.parse.urlsplit(url).hostname or ""


def wait_for_host(url: str) -> None:
    """Wait until this process may send another request to ``url``'s host.

    Requests to a host in :data:`HOST_MIN_INTERVAL` are spaced at least that far apart
    across every thread of the process; other hosts are not delayed.
    """
    host = _host(url)
    interval = HOST_MIN_INTERVAL.get(host)
    if not interval:
        return
    with _HOST_LOCK:
        now = time.monotonic()
        turn = max(now, _HOST_NEXT.get(host, 0.0))
        _HOST_NEXT[host] = turn + interval
    if turn > now:
        time.sleep(turn - now)


def url_status(url: str, attempts: int = 3, timeout: float = 60.0) -> int | None:
    """HTTP status of a HEAD request for ``url``, spaced and retried for limited hosts.

    Requests wait their turn with :func:`wait_for_host`. A 5xx, a 429 or a transport error
    is retried after :func:`host_backoff` (doubling) up to ``attempts`` times.

    Returns:
        The last status received, or None if no attempt got an answer.
    """
    import requests

    status: int | None = None
    for attempt in range(attempts):
        if attempt:
            time.sleep(host_backoff(url, 1.0) * 2 ** (attempt - 1))
        wait_for_host(url)
        try:
            status = requests.head(url, allow_redirects=True, timeout=timeout).status_code
        except requests.RequestException as exc:
            logger.debug(f"could not probe {url}: {exc}")
            continue
        if status < 500 and status != 429:
            return status
    return status


def host_backoff(url: str, backoff: float) -> float:
    """``backoff``, raised to the host's :data:`HOST_MIN_BACKOFF` where it has one."""
    return max(backoff, HOST_MIN_BACKOFF.get(_host(url), 0.0))


def _is_permanent_error(exc: BaseException) -> bool:
    """Whether a failure is one that will not succeed on a retry."""
    return isinstance(exc, PERMANENT_ERRORS)


def _is_permanent_read_error(exc: BaseException) -> bool:
    """Like ``_is_permanent_error``, but also recognises permanent HTTP error statuses."""
    if _is_permanent_error(exc):
        return True
    try:
        from aiohttp import ClientResponseError
    except ImportError:  # pragma: no cover - aiohttp comes with fsspec's HTTP support
        return False
    return isinstance(exc, ClientResponseError) and exc.status in PERMANENT_HTTP_STATUSES


def _release_traceback(exc: BaseException) -> None:
    """Drop the frames a handled exception holds, and with them its callers' locals.

    An error re-raised by fsspec's ``sync`` sits in a reference cycle with the frame that
    re-raised it, and a frame keeps every caller up the stack alive through ``f_back``. A
    handled miss therefore pinned whatever its callers held until the cyclic garbage
    collector next ran: reading a run with many missing fields into block buffers kept
    dozens of 150 MB blocks alive and took a 4 GB write to 7 GB.
    """
    seen: set[int] = set()
    pending: list[BaseException | None] = [exc]
    while pending:
        current = pending.pop()
        if current is None or id(current) in seen:
            continue
        seen.add(id(current))
        if current.__traceback__ is not None:
            traceback.clear_frames(current.__traceback__)
            current.__traceback__ = None
        pending += [current.__cause__, current.__context__]


def _with_retries(
    attempt_once: Callable[[], _T],
    uri: str,
    *,
    retries: int,
    backoff: float,
    is_permanent: Callable[[BaseException], bool] = _is_permanent_error,
    on_failure: Callable[[], None] | None = None,
) -> _T | None:
    """Call ``attempt_once`` until it succeeds, retrying transient errors with backoff.

    Args:
        attempt_once: The operation to attempt.
        uri: What is being fetched, for log messages.
        retries: Number of attempts before giving up.
        backoff: Base seconds for exponential backoff between attempts.
        is_permanent: Classifies an exception as not worth retrying.
        on_failure: Called after every failed attempt, e.g. to remove a partial file.

    Returns:
        The operation's result, or None if it failed with a permanent error (or
        ``retries`` is below 1, so nothing was attempted).

    Raises:
        Exception: The last transient error, once every attempt has failed.
    """
    for attempt in range(1, retries + 1):
        try:
            return attempt_once()
        except Exception as exc:  # noqa: BLE001 - classified below; transient ones retry
            if on_failure is not None:
                on_failure()
            if is_permanent(exc):
                # A 404 or a denial will not become a success; retrying only burns the
                # backoff budget. Archives with gaps hit this constantly.
                logger.debug(f"{uri} unavailable ({type(exc).__name__}), not retrying")
                _release_traceback(exc)
                return None
            if attempt == retries:
                raise
            sleep = backoff * (2 ** (attempt - 1))
            logger.debug(
                f"attempt {attempt}/{retries} for {uri} failed ({exc!r}), retrying in {sleep:.1f}s"
            )
            _release_traceback(exc)
            time.sleep(sleep)
    return None


def download_one(
    url: str,
    dest: str | os.PathLike,
    retries: int = 3,
    backoff: float = 1.0,
    chunk_size: int = CHUNK_SIZE,
    overwrite: bool = False,
    timeout: float | None = None,
) -> pathlib.Path | None:
    """Download ``url`` to ``dest``, retrying with exponential backoff.

    The parent directory is created if needed. Returns the destination path, or None if
    every attempt failed.

    Args:
        url: Source URL, anything fsspec can open.
        dest: Destination file path.
        retries: Number of attempts before giving up.
        backoff: Base seconds for exponential backoff between attempts.
        chunk_size: Read/write chunk size in bytes.
        overwrite: Re-download even if the destination already exists.
        timeout: Optional per-attempt timeout, passed to fsspec.
    """
    dest = pathlib.Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)

    if not overwrite and dest.is_file() and dest.stat().st_size > 0:
        logger.debug(f"skipping {dest.name}, already downloaded")
        return dest

    part = dest.with_name(dest.name + ".part")
    open_kwargs = {"timeout": timeout} if timeout is not None else {}

    def _attempt() -> pathlib.Path:
        wait_for_host(url)
        with fsspec.open(url, "rb", **open_kwargs) as src, open(part, "wb") as out:
            while True:
                chunk = src.read(chunk_size)
                if not chunk:
                    break
                out.write(chunk)
        # Rename only after a complete read, so a partial file is never mistaken for a
        # finished download by the skip-if-present check above.
        os.replace(part, dest)
        return dest

    try:
        result = _with_retries(
            _attempt,
            url,
            retries=retries,
            backoff=host_backoff(url, backoff),
            on_failure=lambda: part.unlink(missing_ok=True),
        )
    except Exception as exc:  # noqa: BLE001 - any transport error is worth retrying
        logger.warning(f"failed to download {url} after {retries} attempts: {exc}")
        return None
    if result is not None:
        logger.debug(f"downloaded {url}")
    return result


def download_ranged(
    url: str,
    dest: str | os.PathLike,
    *,
    connections: int = 4,
    piece_size: int = 64 * CHUNK_SIZE,
    retries: int = 5,
    backoff: float = 2.0,
    timeout: float | None = 300.0,
    overwrite: bool = False,
    storage_options: dict[str, Any] | None = None,
) -> pathlib.Path | None:
    """Download one large object as concurrent byte-range requests.

    For servers that throttle each connection but serve ranges, such as thredds.met.no
    (about 5 MB/s per stream). Keep ``connections`` modest: thredds.met.no blocked a host
    that ran 16 at once. The object is split into ``piece_size``
    ranges fetched by ``connections`` threads and written in place into a ``.part`` file,
    which is renamed to ``dest`` only once every piece has landed. Each piece is retried on
    its own, so a dropped connection costs one piece rather than the whole file.

    Args:
        url: Source URL; the server must honour ``Range`` requests.
        dest: Destination file path.
        connections: Concurrent range requests.
        piece_size: Bytes per range request; at most ``connections`` of them are held in
            memory at once.
        retries: Attempts per piece before giving up on the download.
        backoff: Base seconds for exponential backoff between attempts.
        timeout: Wall-clock seconds allowed per piece, or None for no limit.
        overwrite: Re-download even if the destination already exists.
        storage_options: Filesystem options passed to fsspec.

    Returns:
        The destination path, or None if the object is permanently unavailable: missing
        (404), denied (401/403) or gone (410).

    Raises:
        Exception: The last transient error once a piece (or the size request) has failed
            every attempt. The partial file is removed first.
    """
    dest = pathlib.Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    if not overwrite and dest.is_file() and dest.stat().st_size > 0:
        logger.debug(f"skipping {dest.name}, already downloaded")
        return dest

    fs, path = _filesystem_for(url, storage_options)
    cat_kwargs = {"timeout": timeout} if timeout and fs.async_impl else {}
    try:
        size = _with_retries(
            lambda: fs.size(path, **cat_kwargs),
            url,
            retries=retries,
            backoff=backoff,
            is_permanent=_is_permanent_read_error,
        )
    except Exception as exc:  # noqa: BLE001 - logged, then re-raised unchanged
        logger.warning(f"could not size {url}: {exc!r}")
        raise
    if size is None:
        return None

    part = dest.with_name(dest.name + ".part")

    def _piece(fd: int, start: int) -> None:
        end = min(start + piece_size, size)
        data = _with_retries(
            lambda: fs.cat_file(path, start=start, end=end, **cat_kwargs),
            f"{url} [{start}:{end}]",
            retries=retries,
            backoff=backoff,
            is_permanent=_is_permanent_read_error,
        )
        if data is None or len(data) != end - start:
            got = "nothing" if data is None else f"{len(data)} bytes"
            raise OSError(f"bytes {start}-{end} of {url} returned {got}")
        os.pwrite(fd, data, start)

    began = time.monotonic()
    fd = os.open(part, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        os.ftruncate(fd, size)
        with ThreadPoolExecutor(max_workers=connections) as pool:
            # list(): raise the first failed piece here rather than dropping it.
            list(pool.map(lambda start: _piece(fd, start), range(0, size, piece_size)))
    except Exception as exc:  # noqa: BLE001 - logged, then re-raised unchanged
        part.unlink(missing_ok=True)
        logger.warning(f"failed to download {url}: {exc!r}")
        raise
    finally:
        os.close(fd)
    os.replace(part, dest)
    elapsed = max(time.monotonic() - began, 1e-3)
    logger.info(
        f"downloaded {url} ({size / 1e9:.1f} GB in {elapsed:.0f}s, {size / 1e6 / elapsed:.0f} MB/s)"
    )
    return dest


def download_many(
    urls: Sequence[str] | Iterable[str],
    dest_dir: str | os.PathLike,
    workers: int = 4,
    **kwargs,
) -> list[pathlib.Path]:
    """Download many URLs into a directory concurrently.

    Returns the paths that downloaded successfully, in input order. Failures are logged and
    omitted rather than raising, matching how the original scripts behaved when an archive
    had gaps.
    """
    urls = list(urls)
    dest_dir = pathlib.Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)

    # Drop exact duplicates first: two identical URLs map to one destination by
    # definition, and fetching them concurrently would race on the same file.
    seen: set[str] = set()
    deduped = [u for u in urls if not (u in seen or seen.add(u))]
    if len(deduped) != len(urls):
        logger.debug(f"dropped {len(urls) - len(deduped)} duplicate URL(s)")
        urls = deduped

    # Distinct URLs can share a basename; disambiguate only those, so the common case
    # keeps readable filenames.
    names = [u.split("/")[-1] or "download" for u in urls]
    clashing = {n for n in names if names.count(n) > 1}
    targets: list[pathlib.Path] = []
    for url, name in zip(urls, names):
        if name in clashing:
            digest = hashlib.sha256(url.encode()).hexdigest()[:8]
            stem, dot, ext = name.partition(".")
            name = f"{stem}-{digest}{dot}{ext}"
        targets.append(dest_dir / name)

    def _one(pair: tuple[str, pathlib.Path]) -> pathlib.Path | None:
        return download_one(pair[0], pair[1], **kwargs)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(_one, zip(urls, targets)))

    paths = [p for p in results if p is not None]
    if len(paths) != len(urls):
        logger.info(f"downloaded {len(paths)}/{len(urls)} files into {dest_dir}")
    return paths


# Async filesystem instances shared by every thread that calls ``read_bytes``, keyed on the
# protocol and options. See ``read_bytes`` for why sharing them is safe.
_SHARED_FILESYSTEMS: dict[str, AbstractFileSystem] = {}
_SHARED_FILESYSTEMS_LOCK = threading.Lock()


def _reset_shared_filesystems_after_fork() -> None:
    """Give a forked child a fresh lock and no inherited filesystems.

    The parent's instances are bound to its IO loop, which does not survive the fork, and a
    lock held by another parent thread at fork time would never be released in the child.
    """
    global _SHARED_FILESYSTEMS_LOCK
    _SHARED_FILESYSTEMS_LOCK = threading.Lock()
    _SHARED_FILESYSTEMS.clear()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_shared_filesystems_after_fork)


def _filesystem_for(
    uri: str, storage_options: dict[str, Any] | None
) -> tuple[AbstractFileSystem, str]:
    """Return the filesystem that serves ``uri`` and the path within it."""
    protocol = split_protocol(uri)[0] or "file"
    cls = fsspec.get_filesystem_class(protocol)
    options = dict(storage_options or {})
    if protocol in ("http", "https"):
        # Send the URL exactly as given. Otherwise yarl re-quotes the path and turns DWD's
        # ``%3A`` into a literal ``:``, which works only because DWD happens to accept both.
        options.setdefault("encoded", True)
    path = cls._strip_protocol(uri)
    if not cls.async_impl:
        # fsspec caches instances per thread, so this is cheap after a thread's first call.
        return cls(**options), path
    key = tokenize(protocol, options)
    with _SHARED_FILESYSTEMS_LOCK:
        fs = _SHARED_FILESYSTEMS.get(key)
        if fs is None:
            fs = cls(skip_instance_cache=True, **options)
            _SHARED_FILESYSTEMS[key] = fs
    return fs, path


def read_bytes(
    uri: str,
    *,
    retries: int = 3,
    backoff: float = 1.0,
    timeout: float | None = 60.0,
    storage_options: dict[str, Any] | None = None,
) -> bytes | None:
    """Read a whole object into memory, retrying transient errors with exponential backoff.

    Nothing touches disk, which matters on hosts whose ``/tmp`` is RAM-backed. The object
    is fetched with one ``cat_file`` call (a single GET) rather than a buffered file handle.

    Safe to call concurrently from threads. An async filesystem (S3, HTTP(S), GCS, ...) is
    built once per process, protocol and ``storage_options`` and shared by every thread:
    its blocking methods only submit coroutines to fsspec's single IO-loop thread, which
    owns the session and its connection pool. fsspec's own instance cache is keyed on the
    thread id, so relying on it would keep one client alive per thread of every short-lived
    pool. Other filesystems use fsspec's per-thread instance cache.

    HTTP(S) URIs must already be percent-encoded: they are sent verbatim (``encoded=True``
    unless ``storage_options`` sets it), so DWD's ``%3A`` is neither decoded nor re-encoded.

    s3fs also retries some throttling errors internally, so a request can be attempted more
    often than ``retries`` suggests.

    Args:
        uri: Object to read, e.g. ``s3://bucket/key`` or ``https://host/path``.
        retries: Number of attempts before giving up; must be at least 1.
        backoff: Base seconds for exponential backoff between attempts.
        timeout: Wall-clock seconds allowed per attempt, or None for no limit. Enforced for
            async filesystems only; a timed-out attempt counts as a transient failure.
        storage_options: Filesystem options, e.g. ``{"anon": True}`` for a public bucket.

    Returns:
        The object's bytes, or None if it is permanently unavailable: missing (404,
        FileNotFoundError), denied (401/403, PermissionError) or gone (410).

    Raises:
        ValueError: If ``retries`` is less than 1.
        Exception: The last transient error (timeout, 5xx, connection reset, ...) once
            every attempt has failed, unchanged.
    """
    if retries < 1:
        raise ValueError(f"retries must be at least 1, got {retries}")
    fs, path = _filesystem_for(uri, storage_options)
    # The blocking wrappers of an async filesystem take ``timeout`` as a wall-clock limit on
    # the whole coroutine, raising FSTimeoutError (a TimeoutError) when it runs out.
    cat_kwargs = {"timeout": timeout} if timeout and fs.async_impl else {}
    try:
        return _with_retries(
            lambda: fs.cat_file(path, **cat_kwargs),
            uri,
            retries=retries,
            backoff=backoff,
            is_permanent=_is_permanent_read_error,
        )
    except Exception as exc:  # noqa: BLE001 - logged, then re-raised unchanged
        logger.warning(f"failed to read {uri} after {retries} attempts: {exc!r}")
        raise


def cleanup_files(*paths: str | os.PathLike, missing_ok: bool = True) -> int:
    """Delete files after they have been processed. Returns the number removed.

    Index sidecars written next to a GRIB file (``*.idx``) are removed alongside it.
    """
    removed = 0
    for path in paths:
        path = pathlib.Path(path)
        for target in [path, *path.parent.glob(path.name + "*.idx")]:
            try:
                target.unlink()
                removed += 1
                logger.debug(f"deleted {target}")
            except FileNotFoundError:
                if not missing_ok:
                    raise
            except OSError as exc:
                logger.warning(f"could not delete {target}: {exc}")
    return removed
