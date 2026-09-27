"""MTG FCI Level 1c full-disk imagery from the EUMETSAT Data Store.

Meteosat Third Generation Imager-1 sits at 0 degrees and carries the Flexible
Combined Imager. The Data Store publishes L1c full-disk in two collections,
which differ only in resolution:

* ``EO:EUM:DAT:0665`` — High Resolution Fast Imagery (HRFI), 0.5/1.0 km
* ``EO:EUM:DAT:0662`` — Full Disk High Spectral Imagery (FDHSI), 1.0/2.0 km

Both are repeat-cycle products: a ten-minute cycle arrives as ~40 netCDF chunk
files plus trailer and index entries, of which only the ``.nc`` chunks are
wanted.

Access needs an EUMETSAT API key, which is read from the shared config rather
than the environment directly, so the same code runs under Dagster and from a
shell. Get one at https://api.eumetsat.int/api-key/ and set
``EUMETSAT_CONSUMER_KEY`` / ``EUMETSAT_CONSUMER_SECRET``.
"""

from __future__ import annotations

import datetime as dt
import fnmatch
import os
import pathlib
import shutil
from typing import TYPE_CHECKING, Iterator

from loguru import logger

from planetary_datasets.config import Config, get_config

if TYPE_CHECKING:  # pragma: no cover - import cost only paid at runtime
    import eumdac

#: Data Store collection ids, keyed by the short product name used everywhere
#: else in this repo.
COLLECTIONS: dict[str, str] = {
    "fdhi": "EO:EUM:DAT:0665",
    "fdlr": "EO:EUM:DAT:0662",
}

#: Human-readable names, used in logs and Dagster metadata.
PRODUCT_NAMES: dict[str, str] = {
    "fdhi": "FCI L1c High Resolution Fast Imagery",
    "fdlr": "FCI L1c Full Disk High Spectral Imagery",
}

#: Where downloads land under ``cfg.data_dir``.
DATA_SUBDIR = "eumetsat/mtg"

#: First full-disk repeat cycle published to the Data Store.
ARCHIVE_START = dt.datetime(2024, 9, 1, tzinfo=dt.timezone.utc)


def collection_id(product: str) -> str:
    """Resolve a short product name to its Data Store collection id."""
    try:
        return COLLECTIONS[product.lower()]
    except KeyError:
        raise ValueError(
            f"Unknown MTG product {product!r}; expected one of {sorted(COLLECTIONS)}"
        ) from None


def select_netcdf(filenames: list[str]) -> list[str]:
    """Keep only the netCDF chunks of a repeat cycle.

    A product's entry list also holds the trailer and index sidecars, which
    carry no imagery.
    """
    return [name for name in filenames if fnmatch.fnmatch(name, "*.nc")]


def open_datastore(config: Config | None = None) -> "eumdac.DataStore":
    """Authenticate against the Data Store using the configured API key.

    Raises:
        MissingCredential: when the EUMETSAT key or secret is not configured.
    """
    import eumdac

    cfg = config if config is not None else get_config()
    key, secret = cfg.credentials.require(
        "eumetsat_consumer_key", "eumetsat_consumer_secret"
    )
    return eumdac.DataStore(eumdac.AccessToken((key, secret)))


def archive_dir(
    product: str,
    it: dt.datetime,
    config: Config | None = None,
) -> pathlib.Path:
    """Directory one hour of one product is downloaded into."""
    cfg = config if config is not None else get_config()
    return cfg.data_dir / DATA_SUBDIR / it.strftime("%Y%m%d%H") / product.upper()


def search_products(
    product: str,
    start: dt.datetime,
    end: dt.datetime,
    config: Config | None = None,
    datastore: "eumdac.DataStore | None" = None,
) -> Iterator[object]:
    """Yield every Data Store product for ``product`` sensed in [start, end)."""
    datastore = datastore if datastore is not None else open_datastore(config)
    collection = datastore.get_collection(collection_id(product))
    yield from collection.search(dtstart=start, dtend=end)


def download_hour(
    product: str,
    it: dt.datetime,
    dest_dir: pathlib.Path | None = None,
    config: Config | None = None,
    datastore: "eumdac.DataStore | None" = None,
) -> list[pathlib.Path]:
    """Download one hour of one MTG product. Returns the files on disk.

    Each entry is written to a ``.part`` file and renamed into place only once
    complete, so an interrupted run leaves nothing that the skip-if-present
    check would mistake for a finished download.

    Args:
        product: ``fdhi`` or ``fdlr``.
        it: Start of the hour to download, in UTC.
        dest_dir: Override the destination; defaults to :func:`archive_dir`.
        config: Override configuration.
        datastore: Reuse an authenticated Data Store handle.
    """
    product = product.lower()
    cid = collection_id(product)
    datastore = datastore if datastore is not None else open_datastore(config)
    dest_dir = dest_dir if dest_dir is not None else archive_dir(product, it, config)
    dest_dir.mkdir(parents=True, exist_ok=True)

    downloaded: list[pathlib.Path] = []
    for found in search_products(product, it, it + dt.timedelta(hours=1), datastore=datastore):
        # search() yields lightweight handles; get_product() gives one whose
        # entries can be opened.
        item = datastore.get_product(product_id=str(found), collection_id=cid)
        for entry in select_netcdf(list(item.entries)):
            downloaded.append(_download_entry(item, entry, dest_dir))

    logger.info(
        f"MTG {product.upper()} {it:%Y-%m-%d %H}: {len(downloaded)} netCDF chunk(s) in {dest_dir}"
    )
    return downloaded


def _download_entry(item: object, entry: str, dest_dir: pathlib.Path) -> pathlib.Path:
    """Copy one product entry into ``dest_dir``, atomically and once."""
    dest = dest_dir / pathlib.Path(entry).name
    if dest.is_file() and dest.stat().st_size > 0:
        logger.debug(f"skipping {dest.name}, already downloaded")
        return dest

    part = dest.with_name(dest.name + ".part")
    try:
        with item.open(entry=entry) as src, open(part, "wb") as out:
            shutil.copyfileobj(src, out)
        os.replace(part, dest)
    except BaseException:
        part.unlink(missing_ok=True)
        raise
    logger.debug(f"downloaded {dest.name}")
    return dest
