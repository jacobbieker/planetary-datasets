#!/usr/bin/env python3
"""Ingest Himawari AHI full-disk into Icechunk as virtual references.

Built from AHI-L2-FLDK-ISatSS tiles: every scene is 88 files on a 10x10 grid,
each holding exactly one Zarr chunk, so a scene is assembled as a sparse chunk
manifest rather than by combining arrays. See :mod:`himawari_isatss`.

Because one band-day is ~12,500 tile files (against 144 for a GOES channel-day),
the default mode works a **year at a time, newest first**: it ingests the anchor
year, then the previous year, and so on back to the start of the archive. That
way each year's store lands and is usable instead of nothing committing until a
whole-archive probe finishes.

The store location comes from the shared config, so no path or credential is
passed on the command line::

    # Himawari-9, current year then backwards, every band except C03
    python -m planetary_datasets.providers.virtualized.ingest_himawari_isatss \
        --satellite himawari9 --by-year

    # A single band and window, into a local store
    ICECHUNK_LOCAL_PATH=/data/geo python -m \
        planetary_datasets.providers.virtualized.ingest_himawari_isatss \
        --satellite himawari9 --band C13 \
        --start-date 2026-01-01 --end-date 2026-09-26
"""

from __future__ import annotations

import argparse
import datetime
import sys

from loguru import logger

from planetary_datasets.providers.virtualized import himawari_isatss as mod
from planetary_datasets.providers.virtualized import virtual_repo


def _year_windows(
    satellite: str, end_date: datetime.date, start_date: datetime.date | None
) -> list[tuple[datetime.date, datetime.date]]:
    """Year-sized [start, end] windows, newest first.

    The anchor year is clipped to ``end_date``; earlier years run whole, back to
    the archive start (or ``start_date``).
    """
    archive_start = mod.ARCHIVE_START_DATE[satellite]
    floor = max(start_date or archive_start, archive_start)
    windows: list[tuple[datetime.date, datetime.date]] = []
    for year in range(end_date.year, floor.year - 1, -1):
        start = max(datetime.date(year, 1, 1), floor)
        end = min(datetime.date(year, 12, 31), end_date)
        if start <= end:
            windows.append((start, end))
    return windows


def ingest_band(args: argparse.Namespace, band: str) -> list[str | None]:
    """Ingest one band. Top-level so ``ProcessPoolExecutor`` can pickle it.

    Returns the era suffix of every store written, newest first.
    """

    def repo_factory(suffix: str, b: str = band):
        return mod.open_repo(args.satellite, b, suffix or None, base=args.store_base)

    windows = (
        _year_windows(args.satellite, args.end_date, args.start_date)
        if args.by_year
        else [(args.start_date or mod.ARCHIVE_START_DATE[args.satellite], args.end_date)]
    )

    suffixes: list[str | None] = []
    for start, end in windows:
        logger.info(
            f"{mod.SATELLITE_NAMES[args.satellite]} {band}: "
            f"{start.isoformat()} to {end.isoformat()}"
        )
        try:
            suffixes.extend(
                mod.ingest_backwards(
                    args.satellite,
                    band,
                    repo_factory=repo_factory,
                    end_date=end,
                    start_date=start,
                    first_store_suffix=end.isoformat(),
                    branch=args.branch,
                    group="",
                    batch_size=args.batch_size,
                    zarr_async_concurrency=args.zarr_async_concurrency,
                    log_dir=args.log_dir,
                    max_eras=args.max_eras,
                )
            )
        except Exception as exc:  # noqa: BLE001 - one bad year must not lose the rest
            logger.error(f"{band} {start.year}: FAILED — {type(exc).__name__}: {exc}")
            if not args.continue_on_error:
                raise
    return suffixes


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser."""
    p = argparse.ArgumentParser(
        description="Ingest Himawari AHI full-disk (ISatSS tiles) into Icechunk.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--satellite", default="himawari9", choices=sorted(mod.SATELLITE_BUCKET))
    p.add_argument(
        "--band", default=None, choices=mod.BANDS,
        help="Single AHI band. Mutually exclusive with --bands.",
    )
    p.add_argument(
        "--bands", nargs="+", default=None, choices=mod.BANDS,
        help="Several AHI bands, ingested in turn.",
    )
    p.add_argument(
        "--store-base", default=mod.DEFAULT_STORE_BASE,
        help="Logical store name; the satellite, band and era are appended. "
             "Resolved against the configured bucket, or ICECHUNK_LOCAL_PATH.",
    )
    p.add_argument("--branch", default="main")
    p.add_argument("--batch-size", type=int, default=1, help="Days per commit batch.")
    p.add_argument("--zarr-async-concurrency", type=int, default=None)
    p.add_argument(
        "--parallel", action="store_true",
        help="Ingest bands in parallel, one process each.",
    )
    p.add_argument("--max-workers", type=int, default=None)
    p.add_argument("--log-dir", default=None)
    p.add_argument(
        "--check", action="store_true",
        help="Reopen every store written and summarise it.",
    )
    p.add_argument(
        "--end-date", default=None,
        help="Anchor day for the backwards walk, YYYY-MM-DD (default: today).",
    )
    p.add_argument("--start-date", default=None, help="Stop the walk here, YYYY-MM-DD.")
    p.add_argument(
        "--by-year", action="store_true", default=True,
        help="Ingest a year at a time, newest first (the default).",
    )
    p.add_argument(
        "--no-by-year", dest="by_year", action="store_false",
        help="Run one backwards walk over the whole window instead.",
    )
    p.add_argument("--max-eras", type=int, default=None, help="Stop after this many eras.")
    p.add_argument(
        "--continue-on-error", action="store_true",
        help="Keep going when a year window fails.",
    )
    p.add_argument(
        "--smoke-test", action="store_true",
        help="Stitch a single scene and exit.",
    )
    return p


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse and validate arguments."""
    p = build_parser()
    args = p.parse_args(argv)

    for name in ("end_date", "start_date"):
        value = getattr(args, name)
        if value is None:
            continue
        try:
            setattr(args, name, datetime.date.fromisoformat(value))
        except ValueError:
            p.error(f"--{name.replace('_', '-')} must be YYYY-MM-DD, got {value!r}")
    if args.end_date is None:
        args.end_date = datetime.date.today()
    if args.start_date is not None and args.start_date > args.end_date:
        p.error(f"--start-date ({args.start_date}) is after --end-date ({args.end_date})")
    if args.max_eras is not None and args.max_eras < 1:
        p.error(f"--max-eras must be >= 1, got {args.max_eras}")
    if args.band and args.bands:
        p.error("--band and --bands are mutually exclusive")
    return args


def selected_bands(args: argparse.Namespace) -> list[str]:
    """Resolve the band selection, defaulting to everything but the 0.5 km band."""
    if args.band:
        return [args.band]
    if args.bands:
        return list(args.bands)
    # C03 is the half-kilometre band (22000x22000); like GOES C02 it is left
    # out of bulk runs and given its own pass.
    return [b for b in mod.BANDS if b != mod.HIGH_RES_BAND]


def main(argv: list[str] | None = None) -> int:
    """Parse arguments and run the Himawari ISatSS ingest."""
    args = parse_args(argv)

    if args.smoke_test:
        logger.info(mod.smoke_test(satellite=args.satellite, band=args.band or "C13"))
        return 0

    band_list = selected_bands(args)
    logger.info(
        f"{mod.SATELLITE_NAMES[args.satellite]}: bands {band_list}, backwards from "
        f"{args.end_date.isoformat()}"
        + (", a year at a time" if args.by_year else "")
    )

    written: list[str] = []
    if args.parallel and len(band_list) > 1:
        from concurrent.futures import ProcessPoolExecutor, as_completed

        max_workers = args.max_workers or len(band_list)
        logger.info(f"running {len(band_list)} bands in parallel (max_workers={max_workers})")
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(ingest_band, args, band): band for band in band_list}
            for future in as_completed(futures):
                band = futures[future]
                try:
                    written.extend(
                        mod.store_prefix_for(args.satellite, band, s, args.store_base)
                        for s in future.result()
                    )
                    logger.info(f"{band}: completed")
                except Exception as exc:  # noqa: BLE001 - report every band
                    logger.error(f"{band}: FAILED — {type(exc).__name__}: {exc}")
    else:
        for band in band_list:
            logger.info(f"ingesting {mod.SATELLITE_NAMES[args.satellite]} band {band}")
            written.extend(
                mod.store_prefix_for(args.satellite, band, s, args.store_base)
                for s in ingest_band(args, band)
            )

    if args.check:
        failed = virtual_repo.check_stores(
            written,
            virtual_buckets=mod.SATELLITE_BUCKET.values(),
            branch=args.branch,
        )
        if failed:
            return 1

    logger.info("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
