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

from typing import TYPE_CHECKING, Iterable, Sequence

from loguru import logger

from planetary_datasets.config import Config, get_config

if TYPE_CHECKING:
    import icechunk

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


def open_virtual_repo(
    prefix: str,
    *,
    virtual_buckets: Iterable[str] | str,
    split_size: int = 144,
    split_dim: str = "t",
    source_region: str = DEFAULT_SOURCE_REGION,
    config: Config | None = None,
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

    Returns:
        An open repository with its config saved, ready to append to.
    """
    import icechunk

    cfg = config if config is not None else get_config()
    buckets = [virtual_buckets] if isinstance(virtual_buckets, str) else list(virtual_buckets)
    if not buckets:
        raise ValueError("at least one virtual source bucket is required")

    storage = cfg.icechunk_storage(prefix)

    split_config = icechunk.config.ManifestSplittingConfig.from_dict(
        {
            icechunk.config.ManifestSplitCondition.AnyArray(): {
                icechunk.config.ManifestSplitDimCondition.DimensionName(split_dim): split_size
            }
        }
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
    repo = icechunk.Repository.open_or_create(
        storage, repo_config, authorize_virtual_chunk_access=virtual_credentials
    )
    repo.save_config()
    return repo


def _as_url_prefix(bucket: str) -> str:
    """Normalise ``noaa-x`` or ``s3://noaa-x`` to the ``s3://noaa-x/`` Icechunk wants."""
    url = bucket if "://" in bucket else f"s3://{bucket}"
    return url if url.endswith("/") else f"{url}/"


def describe_store(repo: "icechunk.Repository", branch: str = "main") -> dict[str, object]:
    """Summarise a written store: timestep count, time span and image shape.

    Returns an empty dict when the store cannot be opened, which is the normal
    state before the first commit.
    """
    import xarray as xr

    try:
        ds = xr.open_zarr(repo.readonly_session(branch=branch).store, consolidated=False)
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
    """Open each store and log a summary. Returns the number that failed."""
    failed = 0
    for prefix in prefixes:
        try:
            repo = open_virtual_repo(
                prefix, virtual_buckets=virtual_buckets, config=config
            )
            summary = describe_store(repo, branch=branch)
            if not summary:
                raise RuntimeError("store is empty or unreadable")
            logger.info(f"{prefix}: OK — {summary}")
        except Exception as exc:  # noqa: BLE001 - report every store, not just the first bad one
            logger.error(f"{prefix}: FAILED — {type(exc).__name__}: {exc}")
            failed += 1
    return failed
