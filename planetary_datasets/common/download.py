"""Downloading with retries, atomic writes and skip-if-present.

Consolidates the ``_download_one`` helper that appeared verbatim in the GFS scripts and the
ad hoc "if not os.path.exists(path)" loops in most of the others. Downloads land on a
``.part`` file and are renamed into place only once complete, so an interrupted run leaves
no truncated file that a later skip-if-present check would mistake for good data.
"""

from __future__ import annotations

import os
import pathlib
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable, Sequence

import fsspec
from loguru import logger

CHUNK_SIZE = 1024 * 1024


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

    for attempt in range(1, retries + 1):
        try:
            with fsspec.open(url, "rb", **open_kwargs) as src, open(part, "wb") as out:
                while True:
                    chunk = src.read(chunk_size)
                    if not chunk:
                        break
                    out.write(chunk)
            # Rename only after a complete read, so a partial file is never mistaken for a
            # finished download by the skip-if-present check above.
            os.replace(part, dest)
            logger.debug(f"downloaded {url}")
            return dest
        except Exception as exc:  # noqa: BLE001 - any transport error is worth retrying
            part.unlink(missing_ok=True)
            if attempt == retries:
                logger.warning(f"failed to download {url} after {retries} attempts: {exc}")
                return None
            sleep = backoff * (2 ** (attempt - 1))
            logger.debug(f"attempt {attempt}/{retries} for {url} failed ({exc}), retrying in {sleep:.1f}s")
            time.sleep(sleep)
    return None


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

    def _one(url: str) -> pathlib.Path | None:
        return download_one(url, dest_dir / url.split("/")[-1], **kwargs)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(_one, urls))

    paths = [p for p in results if p is not None]
    if len(paths) != len(urls):
        logger.info(f"downloaded {len(paths)}/{len(urls)} files into {dest_dir}")
    return paths


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
