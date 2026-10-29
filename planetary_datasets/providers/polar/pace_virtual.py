"""NASA PACE: SPEXone and HARP2 radiances and HARP2 MAPOL retrievals, referenced in place.

These granules are too large to copy (HARP2 L1B alone is ~326 GB a day), so
``bkr/polar/pace_virtual.icechunk`` holds references to their bytes in OB.DAAC's
Earthdata Cloud bucket, ``s3://ob-cumulus-prod-public/``. That bucket cannot be read
anonymously: a reader needs temporary S3 credentials from an Earthdata login, which the
store does not hold. Open it with::

    import icechunk, xarray as xr
    creds = requests.get("https://obdaac-tea.earthdatacloud.nasa.gov/s3credentials",
                         auth=(user, password)).json()
    repo = icechunk.Repository.open(
        storage,
        authorize_virtual_chunk_access=icechunk.containers_credentials({
            "s3://ob-cumulus-prod-public/": icechunk.s3_credentials(
                access_key_id=creds["accessKeyId"],
                secret_access_key=creds["secretAccessKey"],
                session_token=creds["sessionToken"],
            )
        }),
    )
    ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)

Layout. Every product shares the store, each variable and dimension prefixed by its
product (``harp2_l1c_i``, ``harp2_l1c_granule``). Granules are stacked on a leading
``<p>_granule`` dimension, so a variable's other dimensions are the granule's own.

The granules' arrays are stored contiguously and uncompressed, so a reference is a byte
range: one per variable per granule, or one per view where the track is not the first
dimension (HARP2 L1B). That only works for granules of one length along the track, and
the length varies (395 or 396 rows for most L1C granules, ~340 for a few). Each product
therefore has a common length (:attr:`VirtualProduct.length`): a longer granule is
referenced by its first ``length`` rows, dropping the last row or two, and a shorter one
is not referenced and reads as fill. ``<p>_rows`` gives every granule's own length.

Variables that do not run along the track (view angles, band tables) are small, and are
copied rather than referenced, as are any arrays not stored contiguously.

Environment: ``EARTHDATA_USERNAME``, ``EARTHDATA_PASSWORD``.

Source: https://pace.oceansciences.org/data_table.htm
"""

from __future__ import annotations

import posixpath
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
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
from planetary_datasets.common import naming
from planetary_datasets.common.earthdata import EarthdataS3, granules, s3_url
from planetary_datasets.common.store import STORE_READ_ERRORS

BUCKET_PREFIX = "s3://ob-cumulus-prod-public/"
REGION = "us-west-2"
#: Granules per manifest split along each product's granule dimension: about a day.
MANIFEST_GRANULES = 144
#: Granules referenced per commit.
BATCH_GRANULES = 48
#: Attribute of ``<p>_granule_name`` listing the product's complete days.
DAYS_ATTR = "days_complete"
#: Days after a day ends before it is recorded complete and no longer listed.
SETTLE_DAYS = 120
#: HDF5/netCDF bookkeeping attributes not carried into the store.
SKIP_ATTRS = {
    "DIMENSION_LIST",
    "REFERENCE_LIST",
    "CLASS",
    "NAME",
    "_Netcdf4Dimid",
    "_Netcdf4Coordinates",
    "_FillValue",
}


@dataclass(frozen=True)
class VirtualProduct:
    """One PACE collection and how its granules are cut to one length."""

    prefix: str
    short_name: str
    version: str
    track: str
    length: int
    geolocation: str = "geolocation_data"
    start: str = "2024-02-23"


PRODUCTS = {
    p.prefix: p
    for p in (
        VirtualProduct("harp2_l1c", "PACE_HARP2_L1C_SCI", "4", "bins_along_track", 395),
        VirtualProduct("harp2_l1b", "PACE_HARP2_L1B_SCI", "4", "swath_lines", 1294),
        VirtualProduct(
            "spexone_l1b", "PACE_SPEXONE_L1B_SCI", "4", "bins_along_track", 8140, "GEOLOCATION_DATA"
        ),
        VirtualProduct(
            "harp2_mapol_land", "PACE_HARP2_L2_MAPOL_LAND", "4.0", "number_of_lines", 395
        ),
        VirtualProduct(
            "harp2_mapol_ocean", "PACE_HARP2_L2_MAPOL_OCEAN", "4.0", "number_of_lines", 395
        ),
    )
}


@dataclass
class Variable:
    """One source dataset of a granule: where its bytes are, or its values."""

    name: str
    dims: tuple[str, ...]
    shape: tuple[int, ...]
    dtype: np.dtype
    attrs: dict
    fill_value: object = None
    offset: int | None = None
    values: np.ndarray | None = None


@dataclass
class Granule:
    """What a granule contributes: references, copied values and its own length."""

    name: str
    url: str
    rows: int
    variables: dict[str, Variable] = field(default_factory=dict)
    coverage: tuple[str, str] = ("", "")


def _dim_names(dataset: h5py.Dataset) -> tuple[str, ...]:
    names = []
    for axis, size in enumerate(dataset.shape):
        scales = list(dataset.dims[axis].values()) if axis < len(dataset.dims) else []
        name = posixpath.basename(scales[0].name) if scales else f"dim_{size}"
        names.append(name)
    return tuple(names)


def _attrs(dataset: h5py.Dataset) -> dict:
    out = {}
    for key, value in dataset.attrs.items():
        if key in SKIP_ATTRS:
            continue
        if isinstance(value, bytes):
            value = value.decode("utf-8", errors="replace")
        elif isinstance(value, np.ndarray):
            value = [v.decode() if isinstance(v, bytes) else v for v in value.tolist()]
        elif isinstance(value, np.generic):
            value = value.item()
        if isinstance(value, (str, int, float, bool, list)):
            out[key] = value
    return out


def _is_scale(dataset: h5py.Dataset) -> bool:
    return dataset.attrs.get("CLASS") == b"DIMENSION_SCALE"


def describe_granule(f: h5py.File, url: str, product: VirtualProduct) -> Granule:
    """References and copies for one open granule.

    A dataset on the track is referenced when it is contiguous and unfiltered; anything
    else is read and copied.
    """
    rows = f[product.geolocation]["latitude"].shape[
        _dim_names(f[product.geolocation]["latitude"]).index(product.track)
    ]
    granule = Granule(name=posixpath.basename(url), url=url, rows=rows)
    granule.coverage = tuple(
        (v.decode() if isinstance(v, bytes) else str(v))
        for v in (f.attrs.get("time_coverage_start", b""), f.attrs.get("time_coverage_end", b""))
    )
    seen: dict[str, int] = {}
    datasets: list[tuple[str, h5py.Dataset]] = []

    def visit(path: str, obj) -> None:
        if isinstance(obj, h5py.Dataset) and not _is_scale(obj):
            datasets.append((path, obj))
            leaf = posixpath.basename(path)
            seen[leaf] = seen.get(leaf, 0) + 1

    f.visititems(visit)
    for path, dataset in datasets:
        leaf = posixpath.basename(path)
        group = posixpath.basename(posixpath.dirname(path))
        name = leaf if seen[leaf] == 1 or not group else f"{group}_{leaf}"
        dims = _dim_names(dataset)
        fill = dataset.fillvalue if "_FillValue" in dataset.attrs else None
        variable = Variable(
            name=name,
            dims=dims,
            shape=dataset.shape,
            dtype=dataset.dtype,
            attrs=_attrs(dataset),
            fill_value=np.asarray(dataset.attrs["_FillValue"]).item() if fill is not None else None,
        )
        if variable.fill_value is not None:
            # xarray masks zarr v3 arrays by this attribute, not by the array's fill value,
            # and expects a float's encoded the way it writes one.
            from xarray.backends.zarr import FillValueCoder

            variable.attrs["_FillValue"] = (
                FillValueCoder.encode(variable.fill_value, dataset.dtype)
                if dataset.dtype.kind == "f"
                else variable.fill_value
            )
        contiguous = dataset.chunks is None and not dataset.compression and not dataset._filters
        offset = dataset.id.get_offset() if contiguous else None
        if product.track in dims and offset is not None and dataset.dtype.kind in "iuf":
            variable.offset = offset
        else:
            values = np.asarray(dataset[()])
            if values.dtype.kind in "SO":
                values = np.vectorize(
                    lambda v: v.decode("utf-8", "replace") if isinstance(v, bytes) else str(v),
                    otypes=[object],
                )(values)
            if product.track in dims:
                axis = dims.index(product.track)
                keep = min(product.length, values.shape[axis])
                pad = [(0, 0)] * values.ndim
                pad[axis] = (0, product.length - keep)
                values = np.take(values, range(keep), axis=axis)
                if values.dtype.kind in "iuf" and pad[axis][1]:
                    filler = variable.fill_value if variable.fill_value is not None else 0
                    values = np.pad(values, pad, constant_values=filler)
            variable.values = values
        granule.variables[name] = variable
    return granule


def _conversion_attrs(conversion: dict | None) -> dict:
    """A store unit conversion as the attributes that decode it (values stay raw)."""
    if not conversion:
        return {}
    return {
        "scale_factor": float(conversion["scale_factor"]),
        "add_offset": float(conversion["add_offset"]),
        "units": conversion["units"],
    }


class PaceVirtualProvider(BaseProvider):
    """References to PACE L1B/L1C and HARP2 MAPOL granules, a day per partition."""

    name = "pace_virtual"
    append_dim = "time"
    store_prefix = "bkr/polar/pace_virtual.icechunk"

    def __init__(self, config=None, products: Iterable[str] | None = None, workers: int = 16):
        """Build the provider.

        Args:
            config: Override the process-wide configuration.
            products: Product prefixes to reference; all of :data:`PRODUCTS` by default.
            workers: Granules read concurrently (headers and small arrays only).
        """
        super().__init__(config=config)
        names = list(products) if products is not None else list(PRODUCTS)
        unknown = set(names) - set(PRODUCTS)
        if unknown:
            raise ValueError(f"unknown PACE virtual products: {sorted(unknown)}")
        self.products = [PRODUCTS[n] for n in names]
        self.workers = workers
        self._s3: EarthdataS3 | None = None
        self._http = requests.Session()

    @property
    def s3(self) -> EarthdataS3:
        """Earthdata S3 credentials, built on first use."""
        if self._s3 is None:
            self._s3 = EarthdataS3("obdaac", self.config)
        return self._s3

    def _stored(self, name: str) -> str:
        """An array's name in the store (planetary_datasets/naming)."""
        if not hasattr(self, "_store_name_map"):
            self._store_name_map = naming.store_names(self.store_prefix)
        return self._store_name_map.get(name, name)

    def repository_config(self) -> icechunk.RepositoryConfig:
        """The bucket as a virtual chunk container (no credentials), and day-sized manifests."""
        splits = {
            icechunk.config.ManifestSplitDimCondition.DimensionName(
                f"{p}_granule"
            ): MANIFEST_GRANULES
            for p in PRODUCTS
        }
        config = icechunk.RepositoryConfig(
            manifest=icechunk.config.ManifestConfig(
                splitting=icechunk.config.ManifestSplittingConfig.from_dict(
                    {icechunk.config.ManifestSplitCondition.AnyArray(): splits}
                )
            )
        )
        config.set_virtual_chunk_container(self.virtual_container())
        return config

    def virtual_container(self) -> icechunk.VirtualChunkContainer:
        """Where the referenced granules live: the bucket, with no credentials attached."""
        return icechunk.VirtualChunkContainer(
            url_prefix=BUCKET_PREFIX, store=icechunk.s3_store(region=REGION)
        )

    # ------------------------------------------------------------------ state

    def _group(self) -> zarr.Group | None:
        try:
            store = self.get_icechunk_repo().readonly_session("main").store
            return zarr.open_group(store, mode="r")
        except (*STORE_READ_ERRORS, zarr.errors.GroupNotFoundError):
            return None

    def _names(self, group: zarr.Group | None, product: VirtualProduct) -> zarr.Array | None:
        name = f"{product.prefix}_granule_name"
        return group[name] if group is not None and name in group else None

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

    def stored_granules(self, product: VirtualProduct) -> list[str]:
        """Granule names already in the store for ``product``, in granule order."""
        names = self._names(self._group(), product)
        return [str(v) for v in names[:]] if names is not None else []

    # ------------------------------------------------------------------ writing

    def _describe(self, url: str, product: VirtualProduct) -> Granule | None:
        for attempt in range(1, 4):
            try:
                fs = self.s3.filesystem()
                with fs.open(url, "rb", block_size=4 << 20) as handle, h5py.File(handle, "r") as f:
                    return describe_granule(f, url, product)
            except Exception as exc:  # noqa: BLE001 - any read error is worth retrying
                logger.warning(f"{self.name}: {url} attempt {attempt} failed: {exc}")
        return None

    def _create(self, group: zarr.Group, product: VirtualProduct, first: Granule) -> None:
        """The product's arrays, empty along the granule dimension, shaped from ``first``."""
        p = product.prefix
        for variable in first.variables.values():
            shape = list(variable.shape)
            if product.track in variable.dims:
                shape[variable.dims.index(product.track)] = product.length
            if variable.offset is not None:
                axis = variable.dims.index(product.track)
                chunks = [1] * axis + [product.length] + shape[axis + 1 :]
                serializer = zarr.codecs.BytesCodec(
                    endian="big" if variable.dtype.byteorder == ">" else "little"
                )
                compressors = ()
                dtype = variable.dtype.newbyteorder("=")
            else:
                chunks = shape
                serializer = "auto"
                compressors = (zarr.codecs.ZstdCodec(level=5),)
                dtype = str if variable.values.dtype.kind == "O" else variable.values.dtype
            group.create_array(
                self._stored(f"{p}_{variable.name}"),
                shape=(0, *shape),
                chunks=(1, *(max(1, c) for c in chunks)),
                dtype=dtype,
                serializer=serializer,
                compressors=compressors,
                fill_value=variable.fill_value,
                dimension_names=(f"{p}_granule", *(f"{p}_{d}" for d in variable.dims)),
                attributes={
                    **variable.attrs,
                    **_conversion_attrs(
                        naming.store_units(self.store_prefix).get(
                            self._stored(f"{p}_{variable.name}")
                        )
                    ),
                },
            )
        for name in ("granule_name", "time_coverage_start", "time_coverage_end"):
            group.create_array(
                f"{p}_{name}",
                shape=(0,),
                chunks=(MANIFEST_GRANULES,),
                dtype=str,
                dimension_names=(f"{p}_granule",),
            )
        group.create_array(
            f"{p}_rows",
            shape=(0,),
            chunks=(MANIFEST_GRANULES,),
            dtype="int32",
            dimension_names=(f"{p}_granule",),
            attributes={
                "long_name": (
                    f"granule length along {product.track}; "
                    f"rows past {product.length} are not referenced"
                )
            },
        )

    def _write(self, product: VirtualProduct, batch: list[Granule]) -> int:
        """Append ``batch`` to the product's arrays and commit."""
        p = product.prefix
        for attempt in range(1, 6):
            session = self.get_icechunk_repo().writable_session("main")
            store = session.store
            group = zarr.open_group(store, mode="a")
            if f"{p}_granule_name" not in group:
                self._create(group, product, batch[0])
            start = group[f"{p}_granule_name"].shape[0]
            stop = start + len(batch)
            arrays = {
                name: group[name]
                for name in group.array_keys()
                if name.startswith(f"{p}_")
                and (group[name].metadata.dimension_names or ("",))[0] == f"{p}_granule"
            }
            for array in arrays.values():
                array.resize((stop, *array.shape[1:]))
            group[f"{p}_granule_name"][start:stop] = np.array([g.name for g in batch], dtype=object)
            group[f"{p}_time_coverage_start"][start:stop] = np.array(
                [g.coverage[0] for g in batch], dtype=object
            )
            group[f"{p}_time_coverage_end"][start:stop] = np.array(
                [g.coverage[1] for g in batch], dtype=object
            )
            group[f"{p}_rows"][start:stop] = np.array([g.rows for g in batch], dtype="int32")
            refs: dict[str, list[icechunk.VirtualChunkSpec]] = {}
            for index, granule in enumerate(batch, start=start):
                for variable in granule.variables.values():
                    key = self._stored(f"{p}_{variable.name}")
                    if key not in arrays:
                        continue
                    array = arrays[key]
                    if variable.offset is not None:
                        if granule.rows < product.length or array.shape[1:] != tuple(
                            product.length if d == product.track else s
                            for d, s in zip(variable.dims, variable.shape)
                        ):
                            continue
                        axis = variable.dims.index(product.track)
                        inner = (
                            int(np.prod(variable.shape[axis + 1 :], dtype=np.int64))
                            * variable.dtype.itemsize
                        )
                        slab = variable.shape[axis] * inner
                        for lead, idx in enumerate(np.ndindex(*variable.shape[:axis])):
                            refs.setdefault(key, []).append(
                                icechunk.VirtualChunkSpec(
                                    index=[index, *idx, *([0] * (len(variable.shape) - axis))],
                                    location=granule.url,
                                    offset=variable.offset + lead * slab,
                                    length=product.length * inner,
                                )
                            )
                    elif variable.values is not None and array.shape[1:] == variable.values.shape:
                        array[index] = variable.values
            for key, specs in refs.items():
                failed = store.set_virtual_refs(key, specs, validate_containers=True)
                if failed:
                    raise RuntimeError(f"{key}: {len(failed)} reference(s) outside the container")
            try:
                session.commit(
                    f"{self.name} {p}: {len(batch)} granule(s) from {batch[0].name}",
                    rebase_with=icechunk.ConflictDetector(),
                )
                return len(batch)
            except (icechunk.ConflictError, icechunk.RebaseFailedError) as exc:
                if attempt == 5:
                    raise
                logger.warning(f"{self.name}: commit conflicted ({exc}); writing again")
        raise AssertionError("unreachable")

    def run_product_day(
        self, product: VirtualProduct, day: pd.Timestamp, pool: ThreadPoolExecutor
    ) -> int:
        """Reference the day's granules of one product not yet stored. Returns how many."""
        listed = {
            s3_url(e).rsplit("/", 1)[1]: s3_url(e)
            for e in granules(
                product.short_name, product.version, day, day + pd.Timedelta(1, "D"), self._http
            )
        }
        stored = set(self.stored_granules(product))
        todo = [listed[n] for n in sorted(listed) if n not in stored]
        written = 0
        for offset in range(0, len(todo), BATCH_GRANULES):
            urls = todo[offset : offset + BATCH_GRANULES]
            described = [g for g in pool.map(lambda u: self._describe(u, product), urls) if g]
            if described:
                written += self._write(product, described)
        stored = set(self.stored_granules(product))
        self._settle(product, day, set(listed), stored)
        return written

    def _settle(
        self, product: VirtualProduct, day: pd.Timestamp, listed: set[str], stored: set[str]
    ) -> None:
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
        """Every product's granules for the day starting at ``it``. True if any were added."""
        day = pd.Timestamp(it).normalize()
        if day.tzinfo is not None:
            day = day.tz_convert("UTC").tz_localize(None)
        written = 0
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            for product in self.products:
                if day < pd.Timestamp(product.start):
                    continue
                n = self.run_product_day(product, day, pool)
                if n:
                    logger.info(f"{self.name}: {product.prefix} {day:%Y-%m-%d}: {n} granule(s)")
                written += n
        return written > 0

    def fetch(self, it: pd.Timestamp, temp_dir=None, **kwargs) -> List[str]:
        """Not used: :meth:`run_partition` references granules product by product."""
        raise NotImplementedError("PaceVirtualProvider writes through run_partition")

    def process(self, input_files, it, temp_dir=None, **kwargs) -> xr.Dataset:
        """Not used: see :meth:`fetch`."""
        raise NotImplementedError("PaceVirtualProvider writes through run_partition")


__all__ = ["PRODUCTS", "PaceVirtualProvider", "VirtualProduct", "describe_granule"]
