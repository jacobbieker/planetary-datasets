#!/usr/bin/env python3
"""Unified ingest script for GOES ABI-L1b-RadF into Icechunk.

Given an icechunk storage location and satellite, ingests per-channel radiance
data. For GOES-16 and GOES-17, automatically uses Reproc (reprocessed) data
where available, falling back to the original archive outside the Reproc
coverage window.

By default the ingest runs backwards: it anchors on today, walks the archive
back a day at a time probing whether each day still combines with the anchor,
and stops an era where virtualizarr can no longer concatenate — a codec,
dtype, chunking, or schema change. Each era gets its own Icechunk store; the
newest one is suffixed with today's date, older ones with the date their era
ends. Within an era the days are written forwards, since Icechunk appends
along `t`. Pass --forward for the old behaviour: one store per channel,
walking the archive from its start.

Usage:
    # Store location from the shared config (ICECHUNK_BUCKET / ICECHUNK_PREFIX,
    # or ICECHUNK_LOCAL_PATH for a local directory), GOES-17 channel 13
    python -m planetary_datasets.providers.virtualized.ingest_goes_radf \
        --satellite goes17 --channel 13

    # Explicit local icechunk store
    python -m planetary_datasets.providers.virtualized.ingest_goes_radf \
        --satellite goes17 --channel 13 \
        --storage local --path goes17_radf_ch13.icechunk

    # Explicit S3 icechunk store, GOES-16 all channels
    python -m planetary_datasets.providers.virtualized.ingest_goes_radf \
        --satellite goes16 \
        --storage s3 --bucket my-bucket --prefix goes16/radf.icechunk \
        --region us-west-2

    # Only the most recent era, i.e. everything that still combines with today
    python -m planetary_datasets.providers.virtualized.ingest_goes_radf \
        --satellite goes19 --channel 13 --max-eras 1

    # Ingest a specific channel range, oldest data first, one store per channel
    python -m planetary_datasets.providers.virtualized.ingest_goes_radf \
        --satellite goes18 --channels 7 8 9 10 11 12 13 14 15 16 --forward
"""

from __future__ import annotations

import argparse
import datetime
import sys
from typing import TYPE_CHECKING

from planetary_datasets.config import get_config
from planetary_datasets.providers.virtualized import goes_radf_common as common

if TYPE_CHECKING:
    import icechunk


def _date_to_fake_url(d: datetime.date, product: str) -> str:
    """Build a synthetic URL that _parse_url_to_day can extract (year, doy) from.

    The listing functions use _parse_url_to_day which splits on the product
    path component and reads the next two path segments as year and doy.
    """
    doy = d.timetuple().tm_yday
    return f"s3://fake/{product}{d.year}/{doy:03d}/placeholder.nc"


def _storage_for(
    args: argparse.Namespace,
    channel: int | None = None,
    date_suffix: str | None = None,
):
    """Build the icechunk storage for one channel/era store.

    With ``--storage config`` (the default) the location comes entirely from
    :func:`~planetary_datasets.config.get_config`, so ``ICECHUNK_LOCAL_PATH``
    redirects the whole run to a local directory and no bucket name or
    credential is spelled out on the command line. ``local`` and ``s3`` remain
    for ad-hoc runs and for the container, which passes Source Cooperative
    keys explicitly.
    """
    import icechunk

    if args.storage == "config":
        base = args.prefix or common.default_store_prefix(args.satellite)
        prefix = common.suffixed_prefix(base, channel=channel, era=date_suffix)
        return get_config().icechunk_storage(prefix)

    path_suffix = f"_C{channel:02d}" if channel is not None else ""
    if date_suffix:
        path_suffix += f"_{date_suffix}"

    if args.storage == "local":
        # e.g. /data/goes17_radf.icechunk -> /data/goes17_radf_C13.icechunk
        base = args.path
        if base.endswith(".icechunk"):
            store_path = base[:-len(".icechunk")] + path_suffix + ".icechunk"
        else:
            store_path = base + path_suffix
        return icechunk.local_filesystem_storage(store_path)
    if args.storage == "s3":
        base = args.prefix
        if base.endswith(".icechunk"):
            store_prefix = base[:-len(".icechunk")] + path_suffix + ".icechunk"
        else:
            store_prefix = base + path_suffix
        kwargs = {
            "bucket": args.bucket,
            "prefix": store_prefix,
            "region": args.region,
        }
        if args.endpoint_url:
            kwargs["endpoint_url"] = args.endpoint_url
            kwargs["allow_http"] = True
        if args.access_key_id:
            kwargs["access_key_id"] = args.access_key_id
            kwargs["secret_access_key"] = args.secret_access_key
        return icechunk.s3_storage(**kwargs)
    raise ValueError(f"Unknown storage type: {args.storage}")


def _open_repo(
    args: argparse.Namespace,
    channel: int | None = None,
    date_suffix: str | None = None,
) -> "icechunk.Repository":
    """Open or create an icechunk Repository from CLI args.

    When channel is provided and multiple channels are being ingested, the
    storage path/prefix is suffixed with _C{channel:02d} so each channel
    gets its own icechunk store.

    When date_suffix is provided (e.g. "2023-04-19"), it is appended after
    the channel suffix, creating a separate store for a new codec era.
    """
    return common.open_virtual_repository(
        _storage_for(args, channel=channel, date_suffix=date_suffix)
    )


def _module_for(satellite: str):
    """Return the per-satellite RadF module."""
    from planetary_datasets.providers.virtualized import (
        goes_16_radf,
        goes_17_radf,
        goes_18_radf,
        goes_19_radf,
    )

    modules = {
        "goes16": goes_16_radf,
        "goes17": goes_17_radf,
        "goes18": goes_18_radf,
        "goes19": goes_19_radf,
    }
    try:
        return modules[satellite]
    except KeyError:
        raise ValueError(f"Unknown satellite: {satellite}") from None


def ingest_channel_backwards(
    repo_factory,
    satellite: str,
    channel: int,
    *,
    end_date: datetime.date,
    start_date: datetime.date | None = None,
    branch: str = "main",
    batch_size: int = 1,
    zarr_async_concurrency: int | None = None,
    log_dir: str | None = None,
    max_eras: int | None = None,
) -> list[str]:
    """Ingest a single channel backwards from end_date, one store per era.

    Where GOES-16 and GOES-17 have Reproc coverage, days inside that window
    are read from the reprocessed archive — those days land in their own era,
    since Reproc files do not share a codec pipeline with the originals.

    Returns the store suffix of every era ingested, newest first.
    """
    mod = _module_for(satellite)
    common = mod.common

    reproc_start = getattr(mod, "REPROC_START_DATE", None)
    reproc_end = getattr(mod, "REPROC_END_DATE", None)
    reproc_for_date = None
    if reproc_start is not None and reproc_end is not None:
        def reproc_for_date(d: datetime.date) -> bool:
            return reproc_start <= d <= reproc_end

    archive_end = getattr(mod, "ARCHIVE_END_DATE", None)
    walk_end = min(end_date, archive_end) if archive_end else end_date

    print(
        f"\n=== {mod.SATELLITE_NAME} {mod._ch(channel)} — backwards from "
        f"{walk_end.isoformat()} ===\n",
        flush=True,
    )
    return common.ingest_backwards(
        channel,
        repo_factory=repo_factory,
        satellite_name=mod.SATELLITE_NAME,
        satellite=mod.SATELLITE,
        product_label="ABI-L1b-RadF",
        archive_start_date=mod.ARCHIVE_START_DATE,
        store=mod._store(),
        bucket=mod.BUCKET,
        product=mod.PRODUCT,
        min_year=mod.MIN_YEAR,
        product_keys=mod.PRODUCT_KEYS,
        loadable_variables=mod.DEFAULT_LOADABLE_VARIABLES,
        epoch_threshold=mod.EPOCH_THRESHOLD,
        end_date=walk_end,
        start_date=max(start_date, mod.ARCHIVE_START_DATE) if start_date
                   else mod.ARCHIVE_START_DATE,
        # walk_end, not end_date: for a decommissioned satellite the walk is
        # clamped to the end of its archive, and naming the store after an
        # unclamped "today" would mint a new one on every run instead of
        # resuming the previous one.
        first_store_suffix=walk_end.isoformat(),
        branch=branch,
        group="",
        batch_size=batch_size,
        zarr_async_concurrency=zarr_async_concurrency,
        log_dir=log_dir,
        product_reproc=getattr(mod, "PRODUCT_REPROC", None),
        reproc_for_date=reproc_for_date,
        reproc_label="ABI-L1b-RadF-Reproc",
        max_eras=max_eras,
    )


def ingest_channel(
    repo: "icechunk.Repository",
    satellite: str,
    channel: int,
    *,
    branch: str = "main",
    batch_size: int = 1,
    zarr_async_concurrency: int | None = None,
    log_dir: str | None = None,
    repo_factory=None,
) -> None:
    """Ingest a single channel for the given satellite, oldest day first.

    For GOES-16 and GOES-17, splits the ingest into up to three phases:
      1. Pre-Reproc:  original archive data before Reproc coverage starts
      2. Reproc:      reprocessed data for the Reproc coverage window
      3. Post-Reproc: original archive data after Reproc coverage ends

    For GOES-18 and GOES-19, ingests directly from the original archive.
    """
    common_kwargs = dict(
        branch=branch,
        batch_size=batch_size,
        zarr_async_concurrency=zarr_async_concurrency,
        log_dir=log_dir,
        repo_factory=repo_factory,
    )
    mod = _module_for(satellite)

    if satellite in ("goes16", "goes17"):
        _ingest_with_reproc(
            repo=repo,
            mod=mod,
            channel=channel,
            archive_start=mod.ARCHIVE_START_DATE,
            archive_end=getattr(mod, "ARCHIVE_END_DATE", None),
            reproc_start=mod.REPROC_START_DATE,
            reproc_end=mod.REPROC_END_DATE,
            **common_kwargs,
        )
    else:
        print(
            f"\n=== {mod.SATELLITE_NAME} {mod._ch(channel)} — full archive ===\n",
            flush=True,
        )
        mod.ingest_all_days(repo, channel, group="", **common_kwargs)


def _ingest_with_reproc(
    repo,
    mod,
    channel: int,
    archive_start: datetime.date,
    archive_end: datetime.date | None,
    reproc_start: datetime.date,
    reproc_end: datetime.date,
    **kwargs,
) -> None:
    """Three-phase ingest: pre-Reproc, Reproc, post-Reproc.

    All three phases write to the same icechunk store (root group) so the
    data forms a single continuous time series.
    """
    ch_str = mod._ch(channel)
    product = "ABI-L1b-RadF"

    # --- Phase 1: pre-Reproc (original archive before Reproc starts) ---
    pre_reproc_end = reproc_start - datetime.timedelta(days=1)
    if archive_start < reproc_start:
        print(
            f"\n=== Phase 1/3: {ch_str} original archive "
            f"{archive_start} to {pre_reproc_end} ===\n",
            flush=True,
        )
        end_url = _date_to_fake_url(pre_reproc_end, product + "/")
        mod.ingest_all_days(
            repo,
            channel,
            group="",
            end=end_url,
            reproc=False,
            **kwargs,
        )
    else:
        print(
            f"\n=== Phase 1/3: {ch_str} — skipped "
            f"(archive starts {archive_start} >= reproc starts {reproc_start}) ===\n",
            flush=True,
        )

    # --- Phase 2: Reproc data ---
    print(
        f"\n=== Phase 2/3: {ch_str} Reproc "
        f"{reproc_start} to {reproc_end} ===\n",
        flush=True,
    )
    mod.ingest_all_days(
        repo,
        channel,
        group="",
        reproc=True,
        **kwargs,
    )

    # --- Phase 3: post-Reproc (original archive after Reproc ends) ---
    post_reproc_start = reproc_end + datetime.timedelta(days=1)
    has_post = archive_end is None or archive_end > reproc_end
    if has_post:
        print(
            f"\n=== Phase 3/3: {ch_str} original archive "
            f"{post_reproc_start} to {'present' if archive_end is None else archive_end} ===\n",
            flush=True,
        )
        start_url = _date_to_fake_url(post_reproc_start, product + "/")
        mod.ingest_all_days(
            repo,
            channel,
            group="",
            start=start_url,
            reproc=False,
            **kwargs,
        )
    else:
        print(
            f"\n=== Phase 3/3: {ch_str} — skipped "
            f"(archive ends {archive_end} <= reproc ends {reproc_end}) ===\n",
            flush=True,
        )


def _storage_summary(args: argparse.Namespace) -> str:
    """One-line description of where this run will write."""
    if args.storage == "local":
        return f"path={args.path}"
    if args.storage == "s3":
        return f"bucket={args.bucket} prefix={args.prefix}"
    prefix = args.prefix or common.default_store_prefix(args.satellite)
    return get_config().store_path(prefix)


def main() -> None:
    p = argparse.ArgumentParser(
        description="Ingest GOES ABI-L1b-RadF per-channel radiance into Icechunk. "
                    "Automatically uses Reproc data where available for GOES-16/17.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--satellite", required=True,
        choices=["goes16", "goes17", "goes18", "goes19"],
        help="Which GOES satellite to ingest.",
    )
    p.add_argument(
        "--channel", type=int, default=None,
        help="Single ABI channel number 1-16. Mutually exclusive with --channels.",
    )
    p.add_argument(
        "--channels", type=int, nargs="+", default=None,
        help="Multiple ABI channel numbers to ingest sequentially.",
    )

    # Storage options
    storage = p.add_argument_group("icechunk storage")
    storage.add_argument(
        "--storage", default="config", choices=["config", "local", "s3"],
        help="Storage backend. 'config' (the default) takes the location from "
             "the shared config, honouring ICECHUNK_LOCAL_PATH.",
    )
    storage.add_argument("--path", help="Path for local storage.")
    storage.add_argument("--bucket", help="S3 bucket name.")
    storage.add_argument(
        "--prefix",
        help="Store prefix. Required for --storage s3; with --storage config it "
             "defaults to the satellite's configured location.",
    )
    storage.add_argument(
        "--region", default=None,
        help="S3 region (default: the configured AWS_REGION).",
    )
    storage.add_argument("--endpoint-url", default=None, help="S3 endpoint URL (for non-AWS).")
    storage.add_argument("--access-key-id", default=None, help="S3 access key ID.")
    storage.add_argument("--secret-access-key", default=None, help="S3 secret access key.")

    ingest = p.add_argument_group("ingest options")
    ingest.add_argument("--branch", default="main", help="Icechunk branch (default: main).")
    ingest.add_argument("--batch-size", type=int, default=1,
                        help="Days per commit batch (default: 1).")
    ingest.add_argument("--zarr-async-concurrency", type=int, default=None,
                        help="zarr async concurrency for chunk reads/writes.")
    ingest.add_argument("--parallel", action="store_true",
                        help="Ingest all channels in parallel using separate processes.")
    ingest.add_argument("--max-workers", type=int, default=None,
                        help="Max parallel workers (default: number of channels). "
                             "Only used with --parallel.")
    ingest.add_argument("--log-dir", default=None,
                        help="Directory for per-channel ingest log files. "
                             "Skipped and errored days are recorded here. "
                             "Defaults to <data_dir>/logs/virtualized.")
    ingest.add_argument("--check", action="store_true",
                        help="After ingesting (or instead of ingesting if stores "
                             "already exist), open each icechunk store and print "
                             "a summary of its contents.")
    ingest.add_argument("--forward", action="store_true",
                        help="Ingest the whole archive oldest-day-first into a "
                             "single store per channel, instead of walking "
                             "backwards from --end-date one store per era.")
    ingest.add_argument("--end-date", default=None,
                        help="Anchor day for the backwards walk, YYYY-MM-DD "
                             "(default: today). Its date also suffixes the "
                             "newest store.")
    ingest.add_argument("--start-date", default=None,
                        help="Stop the backwards walk here, YYYY-MM-DD "
                             "(default: the start of the archive).")
    ingest.add_argument("--max-eras", type=int, default=None,
                        help="Stop after this many eras. --max-eras 1 ingests "
                             "only the days that still combine with --end-date.")
    ingest.add_argument("--smoke-test", action="store_true",
                        help="Virtualize the first --n-files files of one "
                             "channel, print the dataset and exit. Writes "
                             "nothing, so it needs no store.")
    ingest.add_argument("--n-files", type=int, default=3,
                        help="Files to open for --smoke-test (default: 3).")

    args = p.parse_args()

    if args.smoke_test:
        mod = _module_for(args.satellite)
        print(mod.smoke_test(channel=args.channel or 13, n_files=args.n_files))
        return

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
        p.error(
            f"--start-date ({args.start_date}) is after --end-date ({args.end_date})"
        )
    if args.max_eras is not None and args.max_eras < 1:
        p.error(f"--max-eras must be >= 1, got {args.max_eras}")

    if args.storage == "local" and not args.path:
        p.error("--path is required for local storage")
    if args.storage == "s3" and (not args.bucket or not args.prefix):
        p.error("--bucket and --prefix are required for S3 storage")
    if args.region is None:
        args.region = get_config().region

    # Resolve channels
    if args.channel is not None and args.channels is not None:
        p.error("--channel and --channels are mutually exclusive")
    if args.channel is not None:
        channel_list = [args.channel]
    elif args.channels is not None:
        channel_list = args.channels
    else:
        channel_list = list(range(1, 17))[::-1]
        # Remove channel 2, as its much bigger
        channel_list.remove(2)

    for ch in channel_list:
        if ch < 1 or ch > 16:
            p.error(f"Channel must be 1-16, got {ch}")

    if args.log_dir is None:
        args.log_dir = str(common.default_log_dir())

    # Bound glibc arena growth before any channel worker is spawned; workers
    # inherit the setting, which is where it does its work.
    common.configure_ingest_process()

    print(f"Satellite: {args.satellite}")
    print(f"Channels:  {channel_list}")
    print(f"Storage:   {args.storage} ({_storage_summary(args)})")
    print(f"Branch:    {args.branch}")
    print(f"Batch size: {args.batch_size}")
    direction = (
        "forwards from the start of the archive" if args.forward
        else f"backwards from {args.end_date.isoformat()}, one store per era"
    )
    print(f"Direction: {direction}")
    print(flush=True)

    # (channel, store suffix) pairs actually written, for --check.
    written: list[tuple[int, str | None]] = []

    if args.parallel and len(channel_list) > 1:
        from concurrent.futures import ProcessPoolExecutor, as_completed

        max_workers = args.max_workers or len(channel_list)
        print(f"Running {len(channel_list)} channels in parallel "
              f"(max_workers={max_workers})\n", flush=True)

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    ingest_channel_from_args, args, channel,
                ): channel
                for channel in channel_list
            }
            for future in as_completed(futures):
                ch = futures[future]
                try:
                    written.extend((ch, s) for s in future.result())
                    print(f"  Channel {ch:02d}: completed", flush=True)
                except Exception as e:
                    print(f"  Channel {ch:02d}: FAILED — {type(e).__name__}: {e}",
                          flush=True)
    else:
        for channel in channel_list:
            print(f"\n{'='*60}")
            print(f"  Ingesting {args.satellite} channel {channel:02d}")
            print(f"{'='*60}\n", flush=True)

            written.extend(
                (channel, s) for s in ingest_channel_from_args(args, channel)
            )

    if args.check:
        print(f"\n{'='*60}")
        print("  Checking icechunk stores")
        print(f"{'='*60}\n", flush=True)
        check_stores(args, written)

    print("\nDone.", flush=True)


def build_args(
    satellite: str,
    *,
    storage: str = "config",
    path: str | None = None,
    bucket: str | None = None,
    prefix: str | None = None,
    region: str | None = None,
    endpoint_url: str | None = None,
    access_key_id: str | None = None,
    secret_access_key: str | None = None,
    branch: str = "main",
    batch_size: int = 1,
    zarr_async_concurrency: int | None = None,
    log_dir: str | None = None,
    end_date: datetime.date | None = None,
    start_date: datetime.date | None = None,
    max_eras: int | None = None,
    forward: bool = False,
) -> argparse.Namespace:
    """Build the options :func:`ingest_channel_from_args` takes.

    Callers that are not the command line — the Dagster assets — go through
    this so the store layout rules (per-channel and per-era suffixes, the
    ``config`` storage default) stay in one place.
    """
    return argparse.Namespace(
        satellite=satellite,
        storage=storage,
        path=path,
        bucket=bucket,
        prefix=prefix,
        region=region if region is not None else get_config().region,
        endpoint_url=endpoint_url,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        branch=branch,
        batch_size=batch_size,
        zarr_async_concurrency=zarr_async_concurrency,
        log_dir=log_dir if log_dir is not None else str(common.default_log_dir()),
        end_date=end_date or datetime.date.today(),
        start_date=start_date,
        max_eras=max_eras,
        forward=forward,
    )


def ingest_channel_from_args(
    args: argparse.Namespace, channel: int
) -> list[str | None]:
    """Ingest one channel. Top-level so ProcessPoolExecutor can pickle it.

    Returns the store suffix of every store written, newest first.
    """
    def repo_factory(suffix: str, ch: int = channel):
        return _open_repo(args, channel=ch, date_suffix=suffix)

    if not args.forward:
        return list(ingest_channel_backwards(
            repo_factory,
            args.satellite,
            channel,
            end_date=args.end_date,
            start_date=args.start_date,
            branch=args.branch,
            batch_size=args.batch_size,
            zarr_async_concurrency=args.zarr_async_concurrency,
            log_dir=args.log_dir,
            max_eras=args.max_eras,
        ))

    ingest_channel(
        _open_repo(args, channel=channel),
        args.satellite,
        channel,
        branch=args.branch,
        batch_size=args.batch_size,
        zarr_async_concurrency=args.zarr_async_concurrency,
        log_dir=args.log_dir,
        repo_factory=repo_factory,
    )
    return [None]


def check_stores(
    args: argparse.Namespace, written: list[tuple[int, str | None]]
) -> None:
    """Open each store that was written and print a summary."""
    import xarray as xr

    ok = 0
    failed = 0
    for channel, suffix in written:
        label = f"C{channel:02d}" + (f" [{suffix}]" if suffix else "")
        try:
            repo = _open_repo(args, channel=channel, date_suffix=suffix)
            session = repo.readonly_session(branch=args.branch)
            ds = xr.open_zarr(session.store, consolidated=False)

            n_t = ds.sizes.get("t", 0)
            t_first = str(ds["t"].values[0])[:19] if n_t > 0 else "N/A"
            t_last = str(ds["t"].values[-1])[:19] if n_t > 0 else "N/A"
            data_vars = [v for v in ds.data_vars if v not in ("x_coord", "y_coord")]

            print(f"  {label}: OK — {n_t} timesteps, "
                  f"{t_first} to {t_last}, "
                  f"vars={data_vars}",
                  flush=True)
            ok += 1
        except Exception as e:
            print(f"  {label}: FAILED — {type(e).__name__}: {e}", flush=True)
            failed += 1

    print(f"\n  Summary: {ok} OK, {failed} failed out of {len(written)} store(s)",
          flush=True)
    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
