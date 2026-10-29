"""NASA PACE: SPEXone and HARP2 aerosol and cloud retrievals (and SPEXone L1C), copied.

PACE's two multi-angle polarimeters each have L2 retrievals, one netCDF granule per five
minutes of the orbit, in OB.DAAC's Earthdata Cloud bucket. That bucket needs an Earthdata
login to read (:mod:`planetary_datasets.common.earthdata`), so these products are copied
into ``bkr/polar/pace_l2.icechunk``, where they can be read anonymously. The large HARP2
MAPOL retrievals and the L1 radiances are referenced instead, by
:mod:`planetary_datasets.providers.polar.pace_virtual`.

Store layout, as for :mod:`planetary_datasets.providers.polar.earthcare`: every variable and
dimension is prefixed by its product (``spexone_rtap_land_aot``,
``harp2_cloud_gpc_along_track``). Each granule's groups (``geophysical_data``,
``geolocation_data``, ``diagnostic_data``, ...) are flattened into one namespace, a name
used in two groups taking its group as a further prefix. Rows along the track are
appended granule after granule on ``<p>_along_track`` (with ``<p>_granule_index``); what
does not run along the track (band tables, processing settings) goes on ``<p>_granule``,
with the granule's name and coverage times.

Values are copied exactly: files are read without CF decoding, fill values become the
zarr fill values and scale factors stay as attributes, floats are narrowed only where every
value survives (:mod:`planetary_datasets.common.lossless`), and the compression is
lossless.

The L2 products appear about two months after observation; a day is recorded complete
once every listed granule is stored and it is :data:`SETTLE_DAYS` old.

Environment: ``EARTHDATA_USERNAME``, ``EARTHDATA_PASSWORD``.

Source: https://pace.oceansciences.org/data_table.htm
"""

from __future__ import annotations

import pathlib
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Iterable, List

import h5py
import icechunk
import numpy as np
import pandas as pd
import requests
import xarray as xr
import zarr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common import multidim
from planetary_datasets.common.earthdata import EarthdataS3, granules, s3_url
from planetary_datasets.common.multidim import append_partition
from planetary_datasets.common.store import STORE_READ_ERRORS


@dataclass(frozen=True)
class Product:
    """One PACE collection."""

    prefix: str
    short_name: str
    version: str
    start: str = "2024-02-23"


#: The retrievals copied into the store.
PRODUCTS = {
    p.prefix: p
    for p in (
        Product("spexone_rtap_land", "PACE_SPEXONE_L2_AER_RTAPLAND", "4.0"),
        Product("spexone_rtap_ocean", "PACE_SPEXONE_L2_AER_RTAPOCEAN", "4.0"),
        Product("spexone_mapol_ocean", "PACE_SPEXONE_L2_MAPOL_OCEAN", "4.0"),
        Product("harp2_cloud_gpc", "PACE_HARP2_L2_CLOUD_GPC", "4.0"),
        # SPEXone's radiances are chunked every two rows in the source files, too finely
        # to reference (pace_virtual), so they are copied with the retrievals.
        Product("spexone_l1c", "PACE_SPEXONE_L1C_SCI", "4"),
    )
}

#: Days after a day ends before it is recorded complete and no longer listed. The L2
#: products are published about two months after observation.
SETTLE_DAYS = 120
#: Attribute of ``<p>_granule_name`` listing the product's complete days.
DAYS_ATTR = "days_complete"
#: Uncompressed bytes of granules read into memory before they are written.
BATCH_BYTES = 3 * 2**30
#: Global attributes kept per granule; every granule has them.
GRANULE_ATTRS = ("time_coverage_start", "time_coverage_end", "date_created", "processing_version")


def _decode_strings(values: np.ndarray) -> np.ndarray:
    if values.dtype.kind == "S":
        return np.char.decode(values, "utf-8", errors="replace").astype(object)
    return values


def _groups(path: pathlib.Path) -> list[str]:
    """The root (if it holds variables) and every group, depth first."""
    found: list[str] = []
    with h5py.File(path, "r") as f:
        if any(isinstance(v, h5py.Dataset) and not _is_dimension_scale(v) for v in f.values()):
            found.append("/")
        f.visititems(lambda name, obj: found.append(name) if isinstance(obj, h5py.Group) else None)
    return found


def _is_dimension_scale(dataset: h5py.Dataset) -> bool:
    return dataset.attrs.get("CLASS") == b"DIMENSION_SCALE" and "NAME" in dataset.attrs


def read_granule(path: pathlib.Path, product: Product) -> xr.Dataset:
    """One granule as prefixed ``<p>_along_track`` and ``<p>_granule`` variables.

    Raises:
        ValueError: The granule has no ``geolocation_data/latitude`` to find the track by.
    """
    parts: dict[str, xr.Dataset] = {}
    for group in _groups(path):
        with xr.open_dataset(
            path,
            group=None if group == "/" else group,
            engine="h5netcdf",
            phony_dims="access",
            mask_and_scale=False,
            decode_times=False,
            decode_timedelta=False,
        ) as opened:
            if opened.data_vars:
                parts[group] = opened.load()
    if "geolocation_data" not in parts or "latitude" not in parts["geolocation_data"]:
        raise ValueError(f"{product.prefix}: {path.name} has no geolocation_data/latitude")
    track = parts["geolocation_data"]["latitude"].dims[0]

    seen: dict[str, int] = {}
    for ds in parts.values():
        for name in ds.data_vars:
            seen[name] = seen.get(name, 0) + 1

    p = product.prefix
    variables: dict[str, xr.Variable] = {}
    for group, ds in parts.items():
        for name, variable in ds.data_vars.items():
            label = name if seen[name] == 1 else f"{group.rsplit('/', 1)[-1]}_{name}"
            values = _decode_strings(variable.values)
            attrs = dict(variable.attrs)
            if track in variable.dims:
                order = [track, *[d for d in variable.dims if d != track]]
                moved = xr.Variable(variable.dims, values, attrs).transpose(*order)
                dims = tuple(f"{p}_along_track" if d == track else f"{p}_{d}" for d in moved.dims)
                variables[f"{p}_{label}"] = xr.Variable(dims, moved.values, attrs)
            else:
                dims = (f"{p}_granule", *(f"{p}_{d}" for d in variable.dims))
                variables[f"{p}_{label}"] = xr.Variable(dims, values[np.newaxis], attrs)

    with h5py.File(path, "r") as f:
        for key in GRANULE_ATTRS:
            value = f.attrs.get(key, b"")
            value = value.decode() if isinstance(value, bytes) else str(value)
            variables[f"{p}_{key}"] = xr.Variable(
                (f"{p}_granule",), np.array([value], dtype=object)
            )
    variables[f"{p}_granule_name"] = xr.Variable(
        (f"{p}_granule",), np.array([path.name], dtype=object)
    )
    return xr.Dataset(variables)


class PaceL2Provider(BaseProvider):
    """SPEXone and HARP2 L2 retrievals, one day of granules per partition."""

    name = "pace_l2"
    append_dim = "time"
    store_prefix = "bkr/polar/pace_l2.icechunk"

    def __init__(
        self, config=None, products: Iterable[str] | None = None, download_workers: int = 8
    ):
        """Build the provider.

        Args:
            config: Override the process-wide configuration.
            products: Product prefixes to ingest; all of :data:`PRODUCTS` by default.
            download_workers: Concurrent granule downloads from S3.
        """
        super().__init__(config=config)
        names = list(products) if products is not None else list(PRODUCTS)
        unknown = set(names) - set(PRODUCTS)
        if unknown:
            raise ValueError(f"unknown PACE products: {sorted(unknown)}")
        self.products = [PRODUCTS[n] for n in names]
        self.download_workers = download_workers
        self._s3: EarthdataS3 | None = None
        self._http = requests.Session()

    @property
    def s3(self) -> EarthdataS3:
        """Earthdata S3 credentials, built on first use."""
        if self._s3 is None:
            self._s3 = EarthdataS3("obdaac", self.config)
        return self._s3

    # ------------------------------------------------------------------ state

    def _group(self) -> zarr.Group | None:
        try:
            store = self.get_icechunk_repo().readonly_session("main").store
            return zarr.open_group(store, mode="r")
        except (*STORE_READ_ERRORS, zarr.errors.GroupNotFoundError):
            return None

    @staticmethod
    def _names(group: zarr.Group | None, product: Product) -> zarr.Array | None:
        name = f"{product.prefix}_granule_name"
        return group[name] if group is not None and name in group else None

    def days_complete(self, product: Product) -> set[str]:
        """Days (``YYYY-MM-DD``) recorded complete for ``product``."""
        names = self._names(self._group(), product)
        return set(names.attrs.get(DAYS_ATTR, [])) if names is not None else set()

    def missing_timesteps(self, desired: pd.DatetimeIndex) -> List[pd.Timestamp]:
        """Days some product has not been recorded complete for."""
        group = self._group()
        done = {}
        for p in self.products:
            names = self._names(group, p)
            done[p.prefix] = set(names.attrs.get(DAYS_ATTR, [])) if names is not None else set()
        return [
            day
            for day in desired
            if any(
                f"{day:%Y-%m-%d}" not in done[p.prefix]
                for p in self.products
                if day >= pd.Timestamp(p.start)
            )
        ]

    def stored_granules(self, product: Product) -> tuple[set[str], int]:
        """Granule names already stored for ``product``, and how many that is."""
        names = self._names(self._group(), product)
        if names is None:
            return set(), 0
        values = names[:]
        return {str(v) for v in values}, len(values)

    # ------------------------------------------------------------------ writing

    def _download(self, url: str, directory: pathlib.Path) -> pathlib.Path | None:
        dest = directory / url.rsplit("/", 1)[1]
        for attempt in range(1, 4):
            try:
                self.s3.filesystem().get(url, str(dest))
                return dest
            except Exception as exc:  # noqa: BLE001 - any transport error is worth retrying
                logger.warning(f"{self.name}: {url} attempt {attempt} failed: {exc}")
        return None

    def _write(self, product: Product, paths: list[pathlib.Path], first_index: int) -> int:
        ds = multidim.concat_frames(
            [read_granule(path, product) for path in paths], product.prefix, first_index, "granule"
        )
        p = product.prefix
        append_partition(
            self.get_icechunk_repo(),
            ds,
            None,
            fixed_dims=[d for d in ds.dims if d not in (f"{p}_along_track", f"{p}_granule")],
            label=f"{self.name} {p}",
        )
        return len(paths)

    def run_product_day(self, product: Product, day: pd.Timestamp, pool: ThreadPoolExecutor) -> int:
        """Write the day's granules of one product not yet stored. Returns granules written."""
        listed = {
            s3_url(entry).rsplit("/", 1)[1]: s3_url(entry)
            for entry in granules(
                product.short_name, product.version, day, day + pd.Timedelta(1, "D"), self._http
            )
        }
        stored, count = self.stored_granules(product)
        todo = [listed[n] for n in sorted(listed) if n not in stored]
        written = 0
        if todo:
            scratch = pathlib.Path(
                tempfile.mkdtemp(prefix=f"pace-{product.prefix}-", dir=self.config.scratch_dir)
            )
            try:
                futures = [pool.submit(self._download, url, scratch) for url in todo]
                batch: list[pathlib.Path] = []
                size = 0
                for url, future in zip(todo, futures):
                    path = future.result()
                    if path is None:
                        logger.warning(f"{self.name}: {url} failed to download; next pass")
                        continue
                    batch.append(path)
                    size += path.stat().st_size * 4
                    if size >= BATCH_BYTES:
                        written += self._write(product, batch, count + written)
                        for done in batch:
                            done.unlink(missing_ok=True)
                        batch, size = [], 0
                if batch:
                    written += self._write(product, batch, count + written)
            finally:
                shutil.rmtree(scratch, ignore_errors=True)
            stored, _ = self.stored_granules(product)
        self._settle(product, day, set(listed), stored)
        return written

    def _settle(
        self, product: Product, day: pd.Timestamp, listed: set[str], stored: set[str]
    ) -> None:
        """Record the day complete once it is old enough and every listed granule is in."""
        now = pd.Timestamp.now("UTC").tz_localize(None)
        if now - day <= pd.Timedelta(SETTLE_DAYS + 1, "D") or not stored or not listed <= stored:
            return
        name = f"{product.prefix}_granule_name"
        for attempt in range(1, 6):
            session = self.get_icechunk_repo().writable_session("main")
            array = zarr.open_group(session.store, mode="a")[name]
            days = set(array.attrs.get(DAYS_ATTR, []))
            if f"{day:%Y-%m-%d}" in days:
                return
            array.attrs[DAYS_ATTR] = sorted(days | {f"{day:%Y-%m-%d}"})
            try:
                session.commit(
                    f"{self.name} {product.prefix}: {day:%Y-%m-%d} complete",
                    rebase_with=icechunk.ConflictDetector(),
                )
                return
            except (icechunk.ConflictError, icechunk.RebaseFailedError):
                if attempt == 5:
                    raise

    def run_partition(self, it: pd.Timestamp, check_present: bool = True) -> bool:
        """Every product's granules for the day starting at ``it``. True if any were written."""
        day = pd.Timestamp(it).normalize()
        if day.tzinfo is not None:
            day = day.tz_convert("UTC").tz_localize(None)
        written = 0
        with ThreadPoolExecutor(max_workers=self.download_workers) as pool:
            for product in self.products:
                if day < pd.Timestamp(product.start):
                    continue
                if check_present and f"{day:%Y-%m-%d}" in self.days_complete(product):
                    continue
                n = self.run_product_day(product, day, pool)
                if n:
                    logger.info(f"{self.name}: {product.prefix} {day:%Y-%m-%d}: {n} granule(s)")
                written += n
        return written > 0

    def fetch(self, it: pd.Timestamp, temp_dir=None, **kwargs) -> List[str]:
        """Not used: :meth:`run_partition` downloads and writes product by product."""
        raise NotImplementedError("PaceL2Provider writes through run_partition")

    def process(self, input_files, it, temp_dir=None, **kwargs) -> xr.Dataset:
        """Not used: see :meth:`fetch`."""
        raise NotImplementedError("PaceL2Provider writes through run_partition")


__all__ = ["PRODUCTS", "PaceL2Provider", "Product", "read_granule"]
