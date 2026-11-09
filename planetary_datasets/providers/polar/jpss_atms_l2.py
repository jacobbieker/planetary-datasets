"""ATMS-based level 2 granules (snowfall rate, MiRS) from the NOAA JPSS open data buckets.

The ATMS SDR, TDR and geolocation are kept on AWS indefinitely and are already archived
by :class:`~planetary_datasets.providers.polar.jpss_atms.JpssAtmsProvider`. The NDE level
2 products retrieved from them are not: since 2026-09-14 they are kept for a rolling 90
days (see :mod:`planetary_datasets.providers.polar._jpss`), so they are copied into
Icechunk here, one group of ``bkr/polar/jpss.icechunk`` per product, each holding all
three spacecraft:

=============  ====================  ====================================
Group          Bucket prefix         Contents
=============  ====================  ====================================
``atms_sfr``   ``ATMS-SFR``          Snowfall rate, ice water path, ...
``mirs_img``   ``NPR_MIRS_IMG``      MiRS surface/column retrievals, BTs
``mirs_snd``   ``NPR_MIRS_SND``      MiRS temperature/moisture profiles
=============  ====================  ====================================

Each granule is one ATMS granule — 12 scans of 96 fields of view, ~32 s — and becomes one
row along ``granule``, with the scan on ``y`` and the field of view on ``x``, its sensing
time on ``time(granule)`` and its spacecraft on ``platform_name(granule)``; see
:class:`~planetary_datasets.providers.polar._jpss.JpssProductProvider`. NOAA-20 and NOAA-21
granules share their boundaries to within 0.1 s, which is why the rows are keyed by
spacecraft and start rather than by a whole-second time.

**Precision.** Variables are stored with the packing the source netCDF uses: MiRS keeps
most fields as ``int16`` with a scale factor, and so does the store; float32 fields stay
float32. Static per-channel or per-level variables (``Freq``, ``Polo``, ``Plevel``,
``Player``) become coordinates. The integer ``ScanTime_*`` components carry units such as
``days``; that attribute is renamed ``time_component_units`` so a reader does not decode
a day-of-month into a duration.

**One packing for three spacecraft.** The group's packing and static coordinates are
fixed by its first write. A granule from any spacecraft that is packed differently, or
carries different static coordinates, is not written (re-encoding it with the store's
scale would quantise it silently): it is logged as an error and that spacecraft's whole
hour is withheld, so the hour stays missing for it and is retried; the other spacecraft
are written as usual.

ATMS ``ATMS_BUFR`` (the same brightness temperatures as the SDR, in BUFR) and
``ATMS-SCIENCE-RDR`` (raw packets) are also rolling but are not archived: the first
duplicates the permanent SDR, the second needs the IDPS processing chain to be of use.
"""

from __future__ import annotations

import pathlib
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.common import download_many, naming
from planetary_datasets.common.dataset import coords_match
from planetary_datasets.common.store import ALIGNMENT_COORDS, STORE_READ_ERRORS, holds_data
from planetary_datasets.providers.polar._granule import serialize_dataset_attrs
from planetary_datasets.providers.polar._jpss import (
    BUCKETS,
    IncompleteDownload,
    JpssProductProvider,
    nde_granule_end,
    nde_granule_key,
    nde_granule_start,
    nde_satellite,
)

#: Nominal ATMS granule shape. Short granules are padded up to this so they concatenate.
GRANULE_SHAPE = {"y": 12, "x": 96}

#: Source dimension names and what they become.
DIM_RENAMES = {"Scanline": "y", "Field_of_view": "x"}

#: Static variables that become coordinates when present.
STATIC_COORDS = ("Freq", "Polo", "Plevel", "Player")

#: Global attributes that do not change between granules and are worth keeping.
KEPT_ATTRS = ("title", "institution", "project", "processing_level", "Conventions")

#: Units xarray would decode an integer into a timedelta for.
DURATION_UNITS = ("days", "hours", "minutes", "seconds")


def source_packing(var: xr.Variable) -> Dict[str, Any] | None:
    """How a decoded variable was packed in its source file, or None if it was not.

    An integer variable with a scale factor or a fill value is decoded by xarray to
    floats; storing those floats would double or quadruple it and gain nothing.
    Returning the source's packing lets it be written back the same way.
    """
    dtype = var.encoding.get("dtype")
    if dtype is None or np.dtype(dtype).kind not in "iu":
        return None
    if not any(key in var.encoding for key in ("scale_factor", "add_offset", "_FillValue")):
        return None
    packing: Dict[str, Any] = {"dtype": np.dtype(dtype).name}
    for key in ("scale_factor", "add_offset"):
        if key in var.encoding:
            packing[key] = var.encoding[key]
    if "_FillValue" in var.encoding:
        packing["_FillValue"] = np.dtype(dtype).type(var.encoding["_FillValue"])
    return packing


def same_packing(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    """True when two :func:`source_packing` results decode a count to the same value."""
    if a.get("dtype") != b.get("dtype"):
        return False
    for key, default in (("scale_factor", 1.0), ("add_offset", 0.0)):
        if not np.isclose(float(a.get(key, default)), float(b.get(key, default)), rtol=1e-6):
            return False
    fills = (a.get("_FillValue"), b.get("_FillValue"))
    if None in fills:
        return fills[0] is fills[1]
    return bool(fills[0] == fills[1])


class JpssNdeL2Provider(JpssProductProvider):
    """An NDE level 2 product on the ATMS granule grid, for the whole constellation.

    Subclasses set :attr:`product`, :attr:`group`, :attr:`name` and
    :attr:`granule_chunk`.

    Attributes:
        product: Bucket prefix, e.g. ``ATMS-SFR``.
        granule_chunk: Granules per chunk along ``granule``. One granule is a few KB per
            field, so chunks span several — but well under the ~340 granules of an hourly
            partition of three spacecraft, because every append rewrites the partly
            filled last chunk and the superseded copy stays in the repository until it is
            garbage collected.
    """

    product: str
    granule_chunk = 32

    key_of = staticmethod(nde_granule_key)
    start_of = staticmethod(nde_granule_start)
    end_of = staticmethod(nde_granule_end)
    satellite_of = staticmethod(nde_satellite)

    def __init__(
        self,
        config=None,
        satellites: Tuple[str, ...] = ("n21", "n20", "snpp"),
        granule_limit: int | None = None,
        download_workers: int = 16,
    ):
        """Build a provider for some or all of the constellation.

        Args:
            config: Override the process-wide configuration.
            satellites: Subset of :data:`~planetary_datasets.providers.polar._jpss.BUCKETS`.
            granule_limit: Stop after this many granules per partition. For smoke tests
                against the live archive, not for production backfills.
            download_workers: Concurrent downloads.
        """
        super().__init__(
            config=config,
            satellites=satellites,
            granule_limit=granule_limit,
            download_workers=download_workers,
        )
        self.source_product = self.product
        renamed = naming.store_names(self.store_prefix, self.group)
        # Checked under both names: the store keeps them renamed when the tables apply.
        self.alignment_coords = (
            *ALIGNMENT_COORDS,
            *STATIC_COORDS,
            *(renamed[c] for c in STATIC_COORDS if c in renamed),
        )
        self._packing: Dict[str, Dict[str, Any]] = {}
        self._granule_packing: Dict[str, Dict[str, Dict[str, Any]]] = {}

    # ------------------------------------------------------------------ fetch

    def granule_keys(self, it: pd.Timestamp) -> List[str]:
        """``bucket/key`` of every unstored granule this partition owns, oldest first."""
        fs = self._s3()
        start, end = self.partition_window(it)
        found: List[str] = []
        for satellite in self.satellites:
            indexed: Dict[tuple, str] = {}
            for day in self._days_to_list(start, end):
                listed = self._list_day(fs, BUCKETS[satellite], self.product, day)
                indexed.update(self._index_keys(listed, window=(start, end), owned=True))
            found.extend(indexed.values())
        found = sorted(self.drop_stored(found), key=self.start_of)
        if self.granule_limit is not None:
            found = found[: self.granule_limit]
        return found

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Download every unstored granule the partition owns, from every spacecraft.

        Raises:
            IncompleteDownload: when any file failed after retries.
        """
        keys = self.granule_keys(it)
        if not keys:
            return []
        dest = pathlib.Path(temp_dir) if temp_dir is not None else self.config.data_dir / self.name
        # ``fs.ls`` returns "bucket/key".
        urls = [self._https_url(*path.split("/", 1)) for path in keys]
        paths = download_many(urls, dest, workers=self.download_workers)
        if len(paths) != len(urls):
            raise IncompleteDownload(
                f"{self.name}: only {len(paths)}/{len(urls)} files downloaded for {it}"
            )
        logger.info(f"{self.name}: fetched {len(paths)} granules for {it}")
        return [str(p) for p in paths]

    # ------------------------------------------------------------------ process

    def process_granule(self, path: str) -> xr.Dataset | None:
        """Read one granule into a one-granule dataset, or None if it is oversized.

        The source packing of its variables is kept in ``_granule_packing`` under the
        granule's id, for :meth:`process` to check against the group's.
        """
        name = pathlib.Path(path).name
        with xr.open_dataset(path, decode_times=False, decode_timedelta=False) as src:
            ds = src.load()

        ds = ds.drop_vars([v for v in ds.data_vars if ds[v].ndim == 0])
        static = [v for v in STATIC_COORDS if v in ds.data_vars]
        ds = ds.set_coords(static)
        moved = [c for c in ("Latitude", "Longitude") if c in ds.coords]
        ds = ds.reset_coords(moved).rename(
            {c: c.lower() for c in ("Latitude", "Longitude") if c in ds.variables}
        )
        ds = ds.rename_dims({k: v for k, v in DIM_RENAMES.items() if k in ds.dims})

        packing: Dict[str, Dict[str, Any]] = {}
        for var in ds.data_vars:
            found = source_packing(ds[var].variable)
            if found is not None:
                packing[str(var)] = found
            if ds[var].attrs.get("units") in DURATION_UNITS:
                ds[var].attrs["time_component_units"] = ds[var].attrs.pop("units")

        ds = self.fit_to_shape(ds, GRANULE_SHAPE)
        if ds is None:
            return None

        satellite = self.satellite_of(name)
        start, end = self.start_of(name), self.end_of(name)
        ds = self.on_granule_axis(ds, satellite, start, start, end)
        self._granule_packing[self.id_of(name)] = packing

        attrs = {key: ds.attrs[key] for key in KEPT_ATTRS if key in ds.attrs}
        ds.attrs = {**attrs, **self.group_attrs("ATMS")}
        for var in ds.variables:
            ds[var].encoding = {}
        return serialize_dataset_attrs(ds)

    def stored_reference(self) -> Tuple[Dict[str, Dict[str, Any]], xr.Dataset | None]:
        """The group's packing by source variable name, and its static coordinates.

        Empty when the group does not exist yet; its first write sets both.
        """
        repo = self.get_icechunk_repo()
        try:
            stored = xr.open_zarr(
                repo.readonly_session("main").store,
                group=self.group,
                consolidated=False,
                decode_timedelta=False,
            )
        except STORE_READ_ERRORS:
            # Only a group that does not exist yet has no reference; an unreadable one
            # must not be mistaken for it, or the first granule would set a new packing.
            if holds_data(repo, self.group):
                raise
            return {}, None
        source = {
            new: old for old, new in naming.store_names(self.store_prefix, self.group).items()
        }
        packing = {}
        for name, var in stored.data_vars.items():
            found = source_packing(var.variable)
            if found is not None:
                packing[source.get(str(name), str(name))] = found
        static = [c for c in stored.coords if source.get(str(c), str(c)) in STATIC_COORDS]
        statics = stored[static].coords.to_dataset() if static else None
        if statics is not None:
            statics = statics.rename({c: source.get(str(c), str(c)) for c in static})
        return packing, statics

    def _mismatch(
        self,
        granule: xr.Dataset,
        reference: Dict[str, Dict[str, Any]],
        statics: xr.Dataset | None,
    ) -> str | None:
        """Why ``granule`` cannot share the group's arrays, or None when it can."""
        packing = self._granule_packing.get(str(granule[self.append_dim].values[0]), {})
        for var, found in packing.items():
            if var in reference and not same_packing(reference[var], found):
                return f"{var} is packed as {found}, the group as {reference[var]}"
        if statics is not None:
            ok, bad = coords_match(statics, granule, STATIC_COORDS)
            if not ok:
                return f"static coordinate {bad} differs from the group's"
        return None

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Read every granule, drop any the group cannot hold, and stack on ``granule``.

        A granule the group cannot hold withholds its spacecraft's whole hour. Writing that
        spacecraft's other granules would make :meth:`missing_timesteps` count the hour as
        done, and the rejected granule would never be tried again; withheld, the hour
        stays missing and is retried (and logged) every pass until it can be stored.
        """
        reference, statics = self.stored_reference()
        self._granule_packing = {}
        datasets = []
        rejected: set[str] = set()
        for path in sorted(input_files, key=self.start_of):
            try:
                granule = self.process_granule(path)
            except (OSError, KeyError, ValueError) as exc:
                logger.warning(f"{self.name}: granule {path} failed: {exc}")
                continue
            if granule is None:
                continue
            reason = self._mismatch(granule, reference, statics)
            if reason is not None:
                logger.error(f"{self.name}: not writing {pathlib.Path(path).name}: {reason}")
                rejected.add(str(granule["platform_name"].values[0]))
                continue
            # The first granule fixes the packing and statics for the rest of a new group.
            for var, found in self._granule_packing[self.id_of(path)].items():
                reference.setdefault(var, found)
            if statics is None:
                present = [c for c in STATIC_COORDS if c in granule.coords]
                statics = granule[present].coords.to_dataset() if present else None
            datasets.append(granule)
        if rejected:
            logger.error(
                f"{self.name}: withholding {sorted(rejected)} for {it} so the hour is retried"
            )
            datasets = [ds for ds in datasets if str(ds["platform_name"].values[0]) not in rejected]
        self._packing = reference
        return self.restrict_to_window(self.concat_granules(datasets), it)

    def encoding_for(self, ds: xr.Dataset) -> Dict[str, Dict[str, Any]]:
        """Chunks and granule times from the base class, plus the source packing."""
        out = super().encoding_for(ds)
        for name, packing in self._packing.items():
            out[name] = {**out.get(name, {}), **packing}
        return out


class JpssAtmsSfrProvider(JpssNdeL2Provider):
    """ATMS snowfall rate (SFR v2) for the JPSS constellation."""

    name = "jpss_atms_sfr"
    group = "atms_sfr"
    product = "ATMS-SFR"
    granule_chunk = 64


class JpssMirsImgProvider(JpssNdeL2Provider):
    """MiRS ATMS surface and column retrievals (``NPR_MIRS_IMG``) for the constellation."""

    name = "jpss_mirs_img"
    group = "mirs_img"
    product = "NPR_MIRS_IMG"
    granule_chunk = 32


class JpssMirsSndProvider(JpssNdeL2Provider):
    """MiRS ATMS temperature, moisture and hydrometeor profiles (``NPR_MIRS_SND``)."""

    name = "jpss_mirs_snd"
    group = "mirs_snd"
    product = "NPR_MIRS_SND"
    granule_chunk = 8
