#!/usr/bin/env python
"""
Download selected GFS files using fsspec with retry/backoff and
multithreading. Replaces the old urllib-based serial downloader.

Requires: fsspec
"""
from __future__ import annotations

import logging
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterable, List, Optional

import fsspec
import pandas as pd

# d84001 is the GFS normal forecast; do it every 6 hours (example list)
filelist = [
    "https://osdf-director.osg-htc.org/ncar/gdex/d084003/2016/20160101/gfs.0p25b.2016010100.f000.grib2",
    "https://osdf-director.osg-htc.org/ncar/gdex/d084003/2016/20160101/gfs.0p25b.2016010100.f003.grib2",
    "https://osdf-director.osg-htc.org/ncar/gdex/d084003/2016/20160101/gfs.0p25b.2016010100.f006.grib2",
    "https://osdf-director.osg-htc.org/ncar/gdex/d084003/2016/20160101/gfs.0p25b.2016010100.f009.grib2",
    "https://osdf-director.osg-htc.org/ncar/gdex/d084003/2016/20160101/gfs.0p25b.2016010100.f012.grib2",
    "https://osdf-director.osg-htc.org/ncar/gdex/d084003/2016/20160101/gfs.0p25b.2016010100.f015.grib2",
    "https://osdf-director.osg-htc.org/ncar/gdex/d084003/2016/20160101/gfs.0p25b.2016010100.f018.grib2",
    "https://osdf-director.osg-htc.org/ncar/gdex/d084003/2016/20160101/gfs.0p25b.2016010100.f021.grib2",
    "https://osdf-director.osg-htc.org/ncar/gdex/d084003/2016/20160101/gfs.0p25b.2016010100.f024.grib2",
]


def _download_one(
    url: str,
    out_dir: str = ".",
    retries: int = 3,
    backoff: float = 1.0,
    chunk_size: int = 1024 * 1024,
    overwrite: bool = False,
    timeout: Optional[float] = None,
) -> str:
    """Download a single URL to ``out_dir`` with retries and backoff.

    Writes to a temporary ``.part`` file and renames on success to avoid
    leaving partial files behind.
    """
    name = os.path.basename(url)
    dest = os.path.join(out_dir, name)

    if not overwrite and os.path.exists(dest) and os.path.getsize(dest) > 0:
        logging.info("skipping %s (already exists)", name)
        return dest

    attempt = 0
    last_exc: Optional[BaseException] = None

    while attempt <= retries:
        tmp = dest + ".part"
        try:
            # Ensure output directory exists
            os.makedirs(out_dir, exist_ok=True)

            # Use fsspec to open the remote file and stream it to disk
            fs_kwargs = {"timeout": timeout} if timeout is not None else {}
            with fsspec.open(url, mode="rb", **fs_kwargs) as remote:
                with open(tmp, "wb") as out_f:
                    while True:
                        chunk = remote.read(chunk_size)
                        if not chunk:
                            break
                        out_f.write(chunk)

            # Basic sanity check
            size = os.path.getsize(tmp)
            if size == 0:
                raise IOError(f"downloaded zero bytes from {url}")

            # Atomic replace
            os.replace(tmp, dest)
            logging.info("downloaded %s (%d bytes)", name, size)
            return dest

        except Exception as exc:  # pylint: disable=broad-except
            last_exc = exc
            attempt += 1
            logging.warning(
                "error downloading %s (attempt %d/%d): %s",
                name,
                attempt,
                retries + 1,
                exc,
            )

            # Clean up partial file
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except Exception:
                pass

            if attempt > retries:
                logging.error("giving up on %s after %d attempts", name, retries + 1)
                if last_exc is not None:
                    raise last_exc
                raise RuntimeError(f"failed to download {url}")

            # Exponential backoff with jitter
            sleep = backoff * (2 ** (attempt - 1)) * (0.5 + random.random())
            logging.info("retrying %s in %.1fs", name, sleep)
            time.sleep(sleep)

    # Should not be reachable: if we exit retry loop without returning or
    # raising, raise an explicit error to satisfy static type checkers.
    raise RuntimeError(f"failed to download {url}")


def download_many(
    dates: pd.DatetimeIndex, out_dir: str = ".", threads: Optional[int] = None, **kwargs
) -> List[str]:
    """Download multiple URLs in parallel using a thread pool.

    Returns a list of paths that were successfully downloaded.
    """

    # Create the urls here
    urls = []
    for date in dates:
        date_str = date.strftime("%Y%m%d%H")
        for hour in range(0, 25, 6):
            urls.append(f"https://osdf-director.osg-htc.org/ncar/gdex/d084003/{date.year}/{date_str[:8]}/gfs.0p25b.{date_str}.f{hour:03d}.grib2")
            urls.append(f"https://osdf-director.osg-htc.org/ncar/gdex/d084001/{date.year}/{date_str[:8]}/gfs.0p25.{date_str}.f{hour:03d}.grib2")
    if threads is None:
        threads = min(4, (os.cpu_count() or 1) * 4)

    results: List[str] = []
    with ThreadPoolExecutor(max_workers=threads) as exe:
        futures = {exe.submit(_download_one, url, out_dir, **kwargs): url for url in urls}

        for fut in as_completed(futures):
            url = futures[fut]
            try:
                path = fut.result()
                results.append(path)
            except Exception as exc:
                logging.error("failed to download %s: %s", url, exc)

    return results


def main() -> None:
    date_range = pd.date_range(start="2016-01-01", end="2026-01-01", freq="6h")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    try:
        download_many(date_range, out_dir="/ext_data/GFS_NCAR/", threads=None, retries=20, backoff=1.0)
    except KeyboardInterrupt:
        logging.warning("download interrupted by user")


if __name__ == "__main__":
    main()
