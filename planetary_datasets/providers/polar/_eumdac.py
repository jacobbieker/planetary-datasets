"""Fetching EPS products from the EUMETSAT Data Store.

All of the MetOp instruments come from the same place: search a Data Store collection over
the partition window, download the zipped EPS native products, and hand the extracted
``.nat`` files (or a Data Tailor conversion of them) to the instrument's ``process``.

Credentials come from ``EUMETSAT_CONSUMER_KEY`` / ``EUMETSAT_CONSUMER_SECRET`` via
:class:`~planetary_datasets.config.Config`; the scripts this replaces had them inline.
"""

from __future__ import annotations

import os
import pathlib
import shutil
import zipfile
from typing import Any, Iterator, List

import pandas as pd
from loguru import logger

from planetary_datasets.common.staged import StagedFilesMixin
from planetary_datasets.providers.polar._granule import GranuleProvider
from planetary_datasets.providers.polar.epct_download import stage_dir


class EumdacProvider(StagedFilesMixin, GranuleProvider):
    """Base class for providers backed by a EUMETSAT Data Store collection.

    Attributes:
        collection_id: Data Store collection, e.g. ``EO:EUM:DAT:METOP:IASIL1C-ALL``.
        epct_product: Data Tailor product name, for instruments whose native format has no
            reader. Those are downloaded and tailored to netCDF by the ``docker/epct`` image,
            staged under ``EPCT_ARCHIVE_DIR`` (default ``<data_dir>/epct``), and ``fetch``
            returns the staged files. ``None`` when unused.
        download_retries: Attempts per product before it is skipped.
    """

    collection_id: str
    epct_product: str | None = None
    download_retries: int = 3

    def __init__(self, config=None, product_limit: int | None = None):
        """Build a provider for one Data Store collection.

        Args:
            config: Override the process-wide configuration.
            product_limit: Stop after this many products in a partition. Intended for
                smoke tests against the live archive, not for production backfills.
        """
        super().__init__(config=config)
        self.product_limit = product_limit

    def workdir(self, temp_dir: pathlib.Path | None) -> pathlib.Path:
        """Directory to unpack and convert products in for one partition."""
        base = (
            pathlib.Path(temp_dir)
            if temp_dir is not None
            else self.config.scratch_dir / self.name
        )
        base.mkdir(parents=True, exist_ok=True)
        return base

    # ------------------------------------------------------------------ data store

    def datastore(self) -> Any:
        """Authenticated Data Store client. Raises ``MissingCredential`` when unconfigured."""
        import eumdac

        key, secret = self.config.credentials.require(
            "eumetsat_consumer_key", "eumetsat_consumer_secret"
        )
        return eumdac.DataStore(eumdac.AccessToken((key, secret)))

    def search(self, it: pd.Timestamp) -> List[Any]:
        """Products whose sensing window overlaps the partition starting at ``it``."""
        start, end = self.partition_window(it)
        collection = self.datastore().get_collection(self.collection_id)
        products = list(
            collection.search(dtstart=start.to_pydatetime(), dtend=end.to_pydatetime())
        )
        logger.info(
            f"{self.name}: {len(products)} product(s) in {self.collection_id} "
            f"for {start} - {end}"
        )
        if self.product_limit is not None:
            products = products[: self.product_limit]
        return products

    def _download_product(self, product: Any, dest: pathlib.Path) -> pathlib.Path | None:
        """Download one product into ``dest``, retrying a few times. None if it never worked.

        The body lands on a ``.part`` file and is renamed only once complete, so an
        interrupted run cannot leave a truncated archive that the skip-if-present check
        below would accept.
        """
        for attempt in range(1, self.download_retries + 1):
            try:
                with product.open() as src:
                    target = dest / src.name
                    if target.is_file() and target.stat().st_size > 0:
                        logger.debug(f"{self.name}: {src.name} already downloaded")
                        return target
                    part = target.with_name(target.name + ".part")
                    with open(part, "wb") as out:
                        shutil.copyfileobj(src, out)
                    os.replace(part, target)
                    logger.debug(f"{self.name}: downloaded {src.name}")
                    return target
            except Exception as exc:  # noqa: BLE001 - the Data Store fails transiently
                logger.warning(
                    f"{self.name}: download of {product} failed "
                    f"(attempt {attempt}/{self.download_retries}): {exc}"
                )
        return None

    @property
    def archive_root(self) -> pathlib.Path:
        """Host directory mounted into the Data Tailor image."""
        configured = os.environ.get("EPCT_ARCHIVE_DIR")
        if configured:
            return pathlib.Path(configured).expanduser()
        return self.config.data_dir / "epct"

    def staged_dir(self, it: pd.Timestamp) -> pathlib.Path:
        """Where the Data Tailor image stages this partition."""
        return stage_dir(self.archive_root, self.epct_product, pd.Timestamp(it).to_pydatetime())

    def has_staged(self, it: pd.Timestamp) -> bool:
        """True once the image has run; a partition with no products stages an empty dir."""
        return self.staged_dir(it).is_dir()

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Download every product for the partition, or list the tailored files staged for it."""
        if self.epct_product is not None:
            return [str(p) for p in self.staged_files(it)]
        dest = self.workdir(temp_dir)

        paths: List[str] = []
        for product in self.search(it):
            path = self._download_product(product, dest)
            if path is not None:
                paths.append(str(path))
        return paths

    # ------------------------------------------------------------------ unpacking

    @staticmethod
    def extract_native(archive: str | os.PathLike, dest: str | os.PathLike) -> str | None:
        """Unpack an EPS native product from a downloaded zip.

        Returns the path of the ``.nat`` member, or None when the archive is unreadable or
        holds no native product. A corrupt download is a normal occurrence in these
        archives and must not abort the whole partition.
        """
        archive = pathlib.Path(archive)
        dest = pathlib.Path(dest)
        dest.mkdir(parents=True, exist_ok=True)

        if archive.suffix.lower() == ".nat":
            return str(archive)
        try:
            with zipfile.ZipFile(archive, "r") as zf:
                zf.extractall(dest)
        except (zipfile.BadZipFile, OSError) as exc:
            logger.warning(f"could not unzip {archive.name}: {exc}")
            return None

        natives = sorted(dest.rglob("*.nat"))
        if not natives:
            logger.warning(f"{archive.name} contains no .nat product")
            return None
        # Prefer the member matching the archive name when several are unpacked here.
        expected = archive.name.replace(".zip", ".nat")
        for native in natives:
            if native.name == expected:
                return str(native)
        return str(natives[-1])

    def iter_natives(
        self, archives: List[str], temp_dir: pathlib.Path | None = None
    ) -> Iterator[str]:
        """Yield the extracted native product of each archive, skipping unreadable ones."""
        base = self.workdir(temp_dir) / "native"
        for archive in archives:
            # Named after the archive and emptied first: with no temp dir this directory
            # is shared between partitions, and ``extract_native``'s fallback would
            # otherwise be able to return a product left behind by an earlier run.
            target = base / pathlib.Path(archive).stem
            shutil.rmtree(target, ignore_errors=True)
            native = self.extract_native(archive, target)
            if native is not None:
                yield native
