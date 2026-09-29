#!/usr/bin/env python3
"""Ingest GK-2A AMI L1B full-disk radiance into Icechunk as virtual references.

Mirrors ``ingest_goes_radf.py``. By default the ingest runs backwards: it
anchors on today, walks the archive back a day at a time probing whether each
day still combines with the anchor, and closes an era where virtualizarr can no
longer concatenate. Each era gets its own Icechunk store; the newest is
suffixed with the anchor date, older ones with the date their era ends. Within
an era the days are written forwards, since Icechunk appends along ``t``.

GK-2A codecs look stable across the whole archive (BytesCodec + Zlib, 2023
onwards), so a single era spanning everything is the expected outcome. Several
eras would mean something changed and is worth investigating.

The store location comes from the shared config, so no path or credential is
passed on the command line::

    # Local store, one band
    ICECHUNK_LOCAL_PATH=/data/geo python -m \
        planetary_datasets.providers.virtualized.ingest_gk2a_fd --band ir087

    # The configured bucket, every band except the half-kilometre vi006
    python -m planetary_datasets.providers.virtualized.ingest_gk2a_fd --by-year
"""

from __future__ import annotations

import argparse
import datetime
import sys

from loguru import logger

from planetary_datasets.providers.virtualized import gk2a_ami_fd as mod
from planetary_datasets.providers.virtualized import virtual_repo


def _year_windows(
    end_date: datetime.date, start_date: datetime.date | None
) -> list[tuple[datetime.date, datetime.date]]:
    """Year-sized [start, end] windows, newest first.

    The anchor year is clipped to ``end_date``; earlier years run whole, back to
    the archive start (or ``start_date``).
    """
    floor = max(start_date or mod.ARCHIVE_START_DATE, mod.ARCHIVE_START_DATE)
    windows: list[tuple[datetime.date, datetime.date]] = []
    for year in range(end_date.year, floor.year - 1, -1):
        start = max(datetime.date(year, 1, 1), floor)
        end = min(datetime.date(year, 12, 31), end_date)
        if start <= end:
            windows.append((start, end))
    return windows


def ingest_band(args: argparse.Namespace, band: str) -> list[str | None]:
    """Ingest one band. Top-level so ``ProcessPoolExecutor`` can pickle it.

    Returns the era suffix of every store written, newest first. ``None`` means
    a single un-suffixed store, which is what the forward mode produces.
    """

    def repo_factory(suffix: str, b: str = band):
        return mod.open_repo(b, suffix or None, base=args.store_base)

    if args.forward:
        logger.info(f"{mod.SATELLITE_NAME} {band}: full archive, oldest first")
        mod.ingest_all_days(
            mod.open_repo(band, base=args.store_base),
            band,
            branch=args.branch,
            group="",
            batch_size=args.batch_size,
            start_date=args.start_date,
            end_date=args.end_date,
            zarr_async_concurrency=args.zarr_async_concurrency,
            log_dir=args.log_dir,
        )
        return [None]

    # A whole-archive backwards probe is ~1300 days before anything can be
    # committed, which a spot reclaim discards entirely. Year windows cap that
    # at ~365 days so each year lands its own store.
    windows = (
        _year_windows(args.end_date, args.start_date)
        if args.by_year
        else [(args.start_date or mod.ARCHIVE_START_DATE, args.end_date)]
    )

    suffixes: list[str | None] = []
    for start, end in windows:
        logger.info(f"{mod.SATELLITE_NAME} {band}: {start.isoformat()} to {end.isoformat()}")
        try:
            suffixes.extend(
                mod.ingest_backwards(
                    band,
                    repo_factory=repo_factory,
                    end_date=end,
                    start_date=start,
                    # The window that reaches the anchor is the live one, and
                    # its store carries no era suffix: its name would otherwise
                    # move as the archive grows, so a rerun would mint a new
                    # store beside the old rather than resume it. Earlier
                    # windows are closed, so they keep the date they end on.
                    first_store_suffix="" if end == args.end_date else end.isoformat(),
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
        description="Ingest GK-2A AMI L1B full-disk radiance into Icechunk.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--band", default=None, choices=mod.BANDS,
        help="Single AMI band. Mutually exclusive with --bands.",
    )
    p.add_argument(
        "--bands", nargs="+", default=None, choices=mod.BANDS,
        help="Several AMI bands, ingested in turn.",
    )
    p.add_argument(
        "--store-base", default=mod.DEFAULT_STORE_BASE,
        help="Logical store name; the band and era are appended. Resolved "
             "against the configured bucket, or ICECHUNK_LOCAL_PATH.",
    )
    p.add_argument("--branch", default="main")
    p.add_argument(
        "--batch-size", type=int, default=1, help="Days per commit batch.",
    )
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
        "--forward", action="store_true",
        help="Ingest oldest-day-first into one store per band, instead of "
             "walking backwards one store per era.",
    )
    p.add_argument(
        "--end-date", default=None,
        help="Anchor day for the backwards walk, YYYY-MM-DD (default: today). "
             "Also suffixes the newest store.",
    )
    p.add_argument("--start-date", default=None, help="Stop the walk here, YYYY-MM-DD.")
    p.add_argument(
        "--by-year", action="store_true",
        help="Ingest a year at a time, newest first, back to the start of the "
             "archive. Caps each backwards probe at ~365 days so a restart "
             "loses far less work, and each year lands its own store.",
    )
    p.add_argument("--max-eras", type=int, default=None, help="Stop after this many eras.")
    p.add_argument(
        "--continue-on-error", action="store_true",
        help="Keep going when a year window fails.",
    )
    p.add_argument(
        "--smoke-test", action="store_true",
        help="Open a few files through the real preprocess and exit.",
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
    # vi006 is the half-kilometre band (22000x22000); like GOES C02 it is left
    # out of bulk runs and given its own pass.
    return [b for b in mod.BANDS if b != mod.HIGH_RES_BAND]


def main(argv: list[str] | None = None) -> int:
    """Parse arguments and run the GK-2A ingest."""
    args = parse_args(argv)

    if args.smoke_test:
        logger.info(mod.smoke_test(band=args.band or "ir087"))
        return 0

    band_list = selected_bands(args)
    direction = (
        "forwards from the start of the archive"
        if args.forward
        else f"backwards from {args.end_date.isoformat()}, one store per era"
    )
    logger.info(f"{mod.SATELLITE_NAME}: bands {band_list}, {direction}")

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
                        mod.store_prefix_for(band, s, args.store_base) for s in future.result()
                    )
                    logger.info(f"{band}: completed")
                except Exception as exc:  # noqa: BLE001 - report every band
                    logger.error(f"{band}: FAILED — {type(exc).__name__}: {exc}")
    else:
        for band in band_list:
            logger.info(f"ingesting GK-2A band {band}")
            written.extend(
                mod.store_prefix_for(band, s, args.store_base) for s in ingest_band(args, band)
            )

    if args.check:
        failed = virtual_repo.check_stores(
            written, virtual_buckets=mod.BUCKET, branch=args.branch
        )
        if failed:
            return 1

    logger.info("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
