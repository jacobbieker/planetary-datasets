"""Opening Icechunk repositories that hold virtual chunk references.

A store of virtual references needs three things that a plain
:meth:`~planetary_datasets.config.Config.icechunk_repo` does not set up:

* a **virtual chunk container** per source bucket, so Icechunk knows which
  object store the referenced chunks live in;
* **credentials** for that container. Registering a container is not enough —
  without credentials every read of a virtual chunk fails, even for a public
  bucket, where anonymous credentials must be declared explicitly;
* **manifest splitting** along the append dimension. Appending into a growing
  array holds memory in proportion to the manifest split size, so one split per
  commit batch is a memory setting as much as a layout one.

The store location itself still comes from the shared config, so setting
``ICECHUNK_LOCAL_PATH`` redirects a geostationary ingest to the local
filesystem exactly as it does for every other provider.
"""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Iterable, Mapping, Sequence

import numpy as np
from loguru import logger

from planetary_datasets.config import Config, get_config

if TYPE_CHECKING:
    import icechunk


class OutOfOrderPartition(RuntimeError):
    """Raised when a partition would append behind what the store already holds.

    Icechunk appends along ``t``, so a day older than the store's newest
    timestep cannot be written. The shared ingest engine silently skips such a
    day; raising instead means Dagster marks the partition failed rather than
    materialised-but-empty.
    """


class NothingCommitted(RuntimeError):
    """Raised when an ingest ran but committed no timesteps for the day.

    The shared engine logs and swallows a failed batch, so without this check a
    partition that referenced nothing would still report success and never be
    retried.
    """

#: The NOAA Open Data mirrors of the foreign geostationary archives all live in
#: us-east-1, and are readable without credentials.
DEFAULT_SOURCE_REGION = "us-east-1"


def store_prefix(
    base: str,
    *parts: str | None,
    suffix: str = ".icechunk",
) -> str:
    """Build a store prefix from a base and any number of discriminators.

    ``store_prefix("bkr/geo/gk2a_fd", "ir087", "2026-09-27")`` gives
    ``bkr/geo/gk2a_fd_ir087_2026-09-27.icechunk``. Empty and ``None`` parts are
    dropped, so the same call works whether or not a band or era is in play.

    Args:
        base: Logical store name, without the ``.icechunk`` suffix.
        parts: Discriminators appended with underscores, e.g. band then era.
        suffix: Store extension.
    """
    if base.endswith(suffix):
        base = base[: -len(suffix)]
    tail = "".join(f"_{p}" for p in parts if p)
    return f"{base}{tail}{suffix}"


def live_store_suffix(window_end: datetime.date, anchor: datetime.date) -> str:
    """Era suffix for one window of a by-year walk.

    The window that reaches the anchor is the live one, and its store carries
    no era suffix: its name would otherwise move as the archive grew, so a
    rerun would mint a new store beside the old rather than resume it. Every
    earlier window is closed, so it keeps the date it ends on — without that
    distinction all the windows would collapse into a single store.

    Args:
        window_end: Last day of the window being ingested.
        anchor: The run's end date, which the newest window reaches.
    """
    return "" if window_end == anchor else window_end.isoformat()


def open_virtual_repo(
    prefix: str,
    *,
    virtual_buckets: Iterable[str] | str,
    split_size: int = 144,
    split_dim: str = "t",
    source_region: str = DEFAULT_SOURCE_REGION,
    config: Config | None = None,
    create: bool = True,
    extra_splits: Mapping[str, int] | None = None,
) -> "icechunk.Repository":
    """Open or create the virtual-reference store at ``prefix``.

    Args:
        prefix: Store location relative to the configured bucket, e.g.
            ``bkr/geo/gk2a_fd_ir087.icechunk``. Resolved to S3 or to a local
            directory by the shared config.
        virtual_buckets: Source bucket URIs the references point into, e.g.
            ``s3://noaa-gk2a-pds``. Each gets an anonymous virtual chunk
            container.
        split_size: Manifest rows per split along ``split_dim``. Default 144 is
            one day of ten-minute full-disk slots.
        split_dim: Dimension the manifest is split along.
        source_region: Region of the source buckets.
        config: Override configuration; defaults to the process-wide one.
        create: Create the store when it does not exist, and persist the
            manifest and container config onto it. Pass False to inspect an
            existing store without bringing one into being or rewriting its
            splitting config.
        extra_splits: Further manifest splits as ``{dimension: chunks per
            split}``, applied alongside ``split_dim``. A store whose arrays are
            wide in a second dimension needs this to keep each manifest small,
            e.g. ``{"gid": 3700}`` for millions of sites. Sizes count chunks,
            not elements, as ``split_size`` does.

    Returns:
        An open repository, ready to append to.
    """
    import icechunk

    cfg = config if config is not None else get_config()
    buckets = [virtual_buckets] if isinstance(virtual_buckets, str) else list(virtual_buckets)
    if not buckets:
        raise ValueError("at least one virtual source bucket is required")

    if not create and cfg.use_local_store:
        # icechunk_storage mkdirs the local directory, which would leave an
        # empty store behind for a prefix that was never written.
        import pathlib

        if not pathlib.Path(cfg.store_path(prefix)).is_dir():
            raise FileNotFoundError(f"no store at {cfg.store_path(prefix)}")

    storage = cfg.icechunk_storage(prefix)

    dim_splits = {icechunk.config.ManifestSplitDimCondition.DimensionName(split_dim): split_size}
    for dim, size in (extra_splits or {}).items():
        dim_splits[icechunk.config.ManifestSplitDimCondition.DimensionName(dim)] = size
    split_config = icechunk.config.ManifestSplittingConfig.from_dict(
        {icechunk.config.ManifestSplitCondition.AnyArray(): dim_splits}
    )
    repo_config = icechunk.RepositoryConfig(
        manifest=icechunk.config.ManifestConfig(splitting=split_config)
    )

    url_prefixes = [_as_url_prefix(b) for b in buckets]
    for url_prefix in url_prefixes:
        repo_config.set_virtual_chunk_container(
            icechunk.VirtualChunkContainer(
                url_prefix=url_prefix,
                store=icechunk.s3_store(region=source_region, anonymous=True),
            ),
        )
    virtual_credentials = icechunk.containers_credentials(
        {url_prefix: icechunk.s3_anonymous_credentials() for url_prefix in url_prefixes}
    )

    logger.info(f"opening virtual store {cfg.store_path(prefix)} over {', '.join(url_prefixes)}")
    if not create:
        return icechunk.Repository.open(
            storage, authorize_virtual_chunk_access=virtual_credentials
        )

    repo = icechunk.Repository.open_or_create(
        storage, repo_config, authorize_virtual_chunk_access=virtual_credentials
    )
    repo.save_config()
    return repo


def _as_url_prefix(bucket: str) -> str:
    """Normalise ``noaa-x`` or ``s3://noaa-x`` to the ``s3://noaa-x/`` Icechunk wants."""
    url = bucket if "://" in bucket else f"s3://{bucket}"
    return url if url.endswith("/") else f"{url}/"


def committed_times(
    repo: "icechunk.Repository",
    branch: str = "main",
    dim: str = "t",
    group: str | None = None,
) -> np.ndarray:
    """The values already committed along ``dim``, or an empty array.

    A store with no commits yet reads as empty rather than raising, which is
    the normal state before the first partition runs.

    ``group`` must name the same subgroup the ingest writes into. Reading the
    root of a store whose data lives under, say, ``AMI-L1B-FD/vi006`` finds no
    ``t`` coordinate and reports the day as absent, which turns a successful
    ingest into a ``NothingCommitted`` failure.
    """
    import xarray as xr

    try:
        ds = xr.open_zarr(
            repo.readonly_session(branch).store,
            group=group or None,
            consolidated=False,
            decode_timedelta=True,
        )
    except Exception as exc:  # noqa: BLE001 - any failure here means "nothing written yet"
        logger.debug(f"store not readable ({type(exc).__name__}: {exc})")
        return np.array([], dtype="datetime64[ns]")
    if dim not in ds.coords:
        return np.array([], dtype="datetime64[ns]")
    return ds.coords[dim].values


def day_coverage(
    repo: "icechunk.Repository",
    date: datetime.date,
    branch: str = "main",
    dim: str = "t",
    group: str | None = None,
) -> tuple[int, np.datetime64 | None]:
    """How many committed timesteps fall on ``date``, and the newest one stored.

    Returns ``(steps_on_date, newest_committed)``. ``newest_committed`` is None
    when the store is empty.
    """
    times = committed_times(repo, branch=branch, dim=dim, group=group)
    if times.size == 0:
        return 0, None
    day = np.datetime64(date.isoformat(), "D")
    return int((times.astype("datetime64[D]") == day).sum()), times.max()


def guard_append_order(
    repo: "icechunk.Repository",
    date: datetime.date,
    what: str,
    branch: str = "main",
    group: str | None = None,
) -> int:
    """Check ``date`` can still be appended. Returns steps already stored for it.

    A non-zero return means the day is already in the store and the caller
    should treat the partition as a no-op success.

    ``group`` must be the subgroup the ingest writes into; see
    :func:`committed_times`.

    Raises:
        OutOfOrderPartition: when the store already holds a newer day, so this
            one can never be appended.
    """
    covered, newest = day_coverage(repo, date, branch=branch, group=group)
    if covered:
        return covered
    if newest is not None and newest.astype("datetime64[D]") > np.datetime64(
        date.isoformat(), "D"
    ):
        raise OutOfOrderPartition(
            f"{what}: {date.isoformat()} is older than the store's newest timestep "
            f"({newest}). Icechunk only appends along the time dimension, so this day "
            "must be backfilled into its own store, or the partitions must run oldest "
            "first."
        )
    return 0


def require_committed(
    repo: "icechunk.Repository",
    date: datetime.date,
    what: str,
    branch: str = "main",
    group: str | None = None,
) -> int:
    """Assert the ingest actually committed something for ``date``.

    Returns the number of timesteps now stored for that day. ``group`` must be
    the subgroup the ingest writes into; see :func:`committed_times`.

    Raises:
        NothingCommitted: when the day is still absent from the store.
    """
    covered, _ = day_coverage(repo, date, branch=branch, group=group)
    if covered == 0:
        raise NothingCommitted(
            f"{what}: the ingest of {date.isoformat()} committed no timesteps. The "
            "shared engine logs the underlying batch failure above."
        )
    return covered


def describe_store(repo: "icechunk.Repository", branch: str = "main") -> dict[str, object]:
    """Summarise a written store: timestep count, time span and image shape.

    Returns an empty dict when the store cannot be opened, which is the normal
    state before the first commit.
    """
    import xarray as xr

    try:
        ds = xr.open_zarr(
            repo.readonly_session(branch=branch).store, consolidated=False, decode_timedelta=True
        )
    except Exception as exc:  # noqa: BLE001 - any failure here means "nothing written yet"
        logger.debug(f"store not readable ({type(exc).__name__}: {exc})")
        return {}

    n_t = int(ds.sizes.get("t", 0))
    summary: dict[str, object] = {"timesteps": n_t}
    if n_t:
        summary["first"] = str(ds["t"].values[0])[:19]
        summary["last"] = str(ds["t"].values[-1])[:19]
    for image_var in ("image_pixel_values", "Sectorized_CMI", "Rad"):
        if image_var in ds:
            summary["image_shape"] = tuple(int(n) for n in ds[image_var].shape)
            break
    return summary


def check_stores(
    prefixes: Sequence[str],
    *,
    virtual_buckets: Iterable[str] | str,
    branch: str = "main",
    config: Config | None = None,
) -> int:
    """Open each store and log a summary. Returns the number that failed.

    Opens read-only: a prefix that was never written is reported as failed
    rather than quietly brought into existence.
    """
    failed = 0
    for prefix in prefixes:
        try:
            repo = open_virtual_repo(
                prefix, virtual_buckets=virtual_buckets, config=config, create=False
            )
            summary = describe_store(repo, branch=branch)
            if not summary:
                raise RuntimeError("store is empty or unreadable")
            logger.info(f"{prefix}: OK — {summary}")
        except Exception as exc:  # noqa: BLE001 - report every store, not just the first bad one
            logger.error(f"{prefix}: FAILED — {type(exc).__name__}: {exc}")
            failed += 1
    return failed
