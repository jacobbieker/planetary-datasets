"""Nordic NWP and radar products published by the Norwegian Meteorological Institute.

Everything here comes off ``thredds.met.no``. Five near-identical scripts that differed
only in which store the tail of the file wrote to are collapsed into four engines plus thin
per-variant subclasses:

``NordicReflectivityProvider``
    Daily radar reflectivity mosaic (``remotesensing/reflectivity-nordic``), one NetCDF per
    day covering the whole Nordic domain.
``MEPSAnalysisProvider``
    MEPS 2.5 km control member (``member_00``) analysis, the first three lead times of each
    cycle, merging the height-level, pressure-level and surface files.
``MEPSPostProcessedProvider``
    Gridded post-processed analysis (``metpparchive``, ``met_analysis_1_0km_nordic``), one
    file per hour, written a day at a time.
``MEPSDetProvider``
    MEPS 2.5 km deterministic model-level fields, read straight over OPeNDAP rather than
    downloaded.

Three of the seven variants crop to a box around Andoya and write to a private bucket rather
than the public source.coop one. That bucket is not hardcoded: set ``MEPS_ANDOYA_BUCKET`` in
the environment to enable them. They fail with a clear message when it is unset. With
``ICECHUNK_LOCAL_PATH`` set every store goes to the local filesystem and the variable is not
needed.

Each Andoya variant fetches its own copy of the source files rather than deriving from the
already-written full-domain store. That doubles the traffic to THREDDS, and is deliberate:
the subsets are the ones that have to keep running when the public archive job is paused,
so they are not allowed to depend on it.
"""

from __future__ import annotations

import dataclasses
import os
import pathlib
from typing import List

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common.download import download_many
from planetary_datasets.config import Config

THREDDS = "https://thredds.met.no/thredds"

#: Environment variable naming the private bucket the Andoya subsets are written to.
ANDOYA_BUCKET_ENV = "MEPS_ANDOYA_BUCKET"

#: Variables excluded from the float16 downcast. ``ap`` and ``b`` define the hybrid levels
#: themselves (``p = ap + b * ps``); at ``ap ~ 20000`` Pa float16 steps in units of 16, so
#: downcasting them would blur the vertical coordinate. The other two span several orders of
#: magnitude and lose real signal.
FLOAT16_SKIP = frozenset({"ap", "b", "specific_humidity_ml", "turbulent_kinetic_energy_ml"})

#: Dimensions that are kept as-is; every other extra dimension is a degenerate level axis
#: that gets squeezed out when merging the MEPS analysis files.
KEEP_DIMS = frozenset({"x", "y", "time", "pressure", "longitude", "latitude"})


class BucketNotConfigured(RuntimeError):
    """Raised when a provider needs a bucket that has not been configured."""


@dataclasses.dataclass(frozen=True)
class Box:
    """A lat/lon box expressed as a centre point and a padding in degrees."""

    latitude: float
    longitude: float
    lat_pad: float = 3.0
    lon_pad: float = 3.0

    def mask(self, ds: xr.Dataset) -> xr.DataArray:
        """Boolean array, True inside the box.

        Latitude and longitude are 2D on the MEPS Lambert grid, so the box cannot be taken
        with ``sel``; it has to be expressed as a mask over the grid indices.
        """
        return (
            (ds.latitude >= self.latitude - self.lat_pad)
            & (ds.latitude <= self.latitude + self.lat_pad)
            & (ds.longitude >= self.longitude - self.lon_pad)
            & (ds.longitude <= self.longitude + self.lon_pad)
        )

    def clip(self, ds: xr.Dataset) -> xr.Dataset:
        """Cut ``ds`` down to the box.

        Equivalent to ``ds.where(mask, drop=True)`` but without materialising the whole
        dataset first: the mask spans only the grid coordinates, so the index bounds can be
        worked out from it and applied with ``isel``. A daily radar mosaic is several GB, and
        loading all of it to keep a corner of it was the most expensive step in the original
        scripts.
        """
        mask = self.mask(ds)
        mask = mask.compute() if hasattr(mask.data, "compute") else mask
        if not bool(mask.any()):
            raise ValueError(f"{self} does not intersect the grid")

        bounds = {}
        for dim in mask.dims:
            others = [d for d in mask.dims if d != dim]
            present = np.flatnonzero((mask.any(dim=others) if others else mask).values)
            bounds[dim] = slice(int(present[0]), int(present[-1]) + 1)

        subset = ds.isel(bounds)
        window = mask.isel(bounds)

        # Mask variable by variable rather than with ``Dataset.where``, which broadcasts
        # every variable against the mask: that turns the scalar ``projection_lambert``
        # grid-mapping sentinel these files carry into a full float grid.
        out = subset.copy()
        for var in subset.data_vars:
            if set(window.dims) <= set(subset[var].dims):
                out[var] = subset[var].where(window)
        return out


#: Andoya Space, northern Norway. The subset stores are cut to this box.
ANDOYA = Box(latitude=69.114, longitude=15.7679)


def nordic_reflectivity_url(date: pd.Timestamp) -> str:
    """URL of the daily Nordic radar reflectivity mosaic covering ``date``."""
    return (
        f"{THREDDS}/fileServer/remotesensing/reflectivity-nordic/"
        f"{date.year}/{date.month:02}/yrwms-nordic.mos.pcappi-0-dbz."
        f"noclass-clfilter-novpr-clcorr-block.nordiclcc-1000.{date.strftime('%Y%m%d')}.nc"
    )


def meps_analysis_urls(date: pd.Timestamp, steps: int = 3) -> list[str]:
    """URLs of the control-member MEPS analysis files for one cycle.

    Three files per lead time: height levels, pressure levels and surface.
    """
    base = (
        f"{THREDDS}/fileServer/meps25epsarchive/"
        f"{date.year}/{date.month:02}/{date.day:02}/{date.hour:02}/member_00"
    )
    stamp = date.strftime("%Y%m%dT%HZ")
    return [
        f"{base}/meps_{kind}_{step:02}_{stamp}.nc"
        for step in range(steps)
        for kind in ("hl", "pl", "sfc")
    ]


def nordic_analysis_url(date: pd.Timestamp) -> str:
    """URL of the 1 km post-processed Nordic analysis for one hour."""
    return (
        f"{THREDDS}/fileServer/metpparchive/{date.year}/{date.month:02}/{date.day:02}/"
        f"met_analysis_1_0km_nordic_{date.strftime('%Y%m%dT%HZ')}.nc"
    )


def meps_det_opendap_url(date: pd.Timestamp) -> str:
    """OPeNDAP endpoint for the MEPS 2.5 km deterministic run at ``date``."""
    return (
        f"{THREDDS}/dodsC/meps25epsarchive/"
        f"{date.year}/{date.month:02}/{date.day:02}/"
        f"meps_det_2_5km_{date.strftime('%Y%m%d')}T{date.strftime('%H')}Z.nc"
    )


class MetNoProvider(BaseProvider):
    """Shared behaviour for the met.no providers.

    Subclasses supply :attr:`store_prefix` and, optionally, a :attr:`crop` box and a
    :attr:`bucket_env` naming an environment variable that overrides the destination bucket.
    """

    #: Environment variable holding the destination bucket, when it is not the default one.
    bucket_env: str | None = None
    #: Region to cut the data down to before writing, or None to keep the full domain.
    crop: Box | None = None
    #: Chunk length along the append dimension. Matches how the live stores were created.
    time_chunk: int = 1
    #: Attempts per file before a download is given up on; the archive is often slow.
    retries: int = 5

    @property
    def config(self) -> Config:
        """Configuration for this provider, with the destination bucket overridden.

        A local store ignores the bucket entirely, so the override is only resolved when
        actually writing to S3.
        """
        cfg = super().config
        if self.bucket_env is None or cfg.use_local_store:
            return cfg
        bucket = (os.environ.get(self.bucket_env) or "").strip()
        if not bucket:
            raise BucketNotConfigured(
                f"{self.name} writes to a private bucket; set {self.bucket_env} in the "
                "environment, or set ICECHUNK_LOCAL_PATH to write locally."
            )
        return dataclasses.replace(cfg, bucket=bucket)

    def download(self, urls: List[str], temp_dir: pathlib.Path | None) -> List[str]:
        """Download ``urls`` into the partition's temporary directory."""
        dest = (
            pathlib.Path(temp_dir) if temp_dir is not None else self.config.scratch_dir / self.name
        )
        return [str(p) for p in download_many(urls, dest, retries=self.retries)]

    def download_all(
        self,
        urls: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None,
    ) -> List[str]:
        """Download every URL of a partition, all-or-nothing.

        Three outcomes, kept distinct on purpose:

        * everything arrived — return the files;
        * nothing arrived — the cycle or day is not in the archive, so return ``[]`` and let
          the caller skip the partition;
        * some arrived — the archive does have this partition but the transfer did not
          finish, so raise. Committing the partial set would mark the partition done and
          leave a gap that no later run looks for, because the skip check only tests the
          partition's first timestep.
        """
        files = self.download(urls, temp_dir)
        if len(files) == len(urls):
            return files
        if not files:
            logger.info(f"{self.name}: nothing published for {it}, skipping")
            return []
        raise RuntimeError(
            f"{self.name}: {it} is only partly available, {len(files)}/{len(urls)} files "
            f"downloaded after {self.retries} attempts each; refusing to write a partial "
            "partition."
        )

    def finalise(self, ds: xr.Dataset) -> xr.Dataset:
        """Crop, chunk and sort a processed dataset ready for writing."""
        if self.crop is not None:
            ds = self.crop.clip(ds)
        chunks = {self.append_dim: self.time_chunk}
        chunks.update({dim: -1 for dim in ds.dims if dim != self.append_dim})
        return ds.chunk(chunks).sortby(self.append_dim)


class NordicReflectivityProvider(MetNoProvider):
    """Daily Nordic radar reflectivity mosaic, one NetCDF file per day."""

    name = "nordic_reflectivity"
    append_dim = "time"
    store_prefix = "bkr/precipradar/norway_radar.icechunk"

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Download the mosaic covering the day ``it`` starts."""
        return self.download_all([nordic_reflectivity_url(it)], it, temp_dir)

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Rename the projection axes to the names the other Nordic stores use."""
        ds = xr.open_dataset(input_files[0])
        ds = ds.rename({"Yc": "y", "Xc": "x", "lon": "longitude", "lat": "latitude"})
        # The source files carry per-variable encoding that conflicts with the store's.
        return self.finalise(ds.drop_encoding())


class AndoyaReflectivityProvider(NordicReflectivityProvider):
    """Nordic radar reflectivity cut to the Andoya box, on the private bucket."""

    name = "andoya_reflectivity"
    store_prefix = "andoya_radar.icechunk"
    bucket_env = ANDOYA_BUCKET_ENV
    crop = dataclasses.replace(ANDOYA, lon_pad=4.0)


class MEPSAnalysisProvider(MetNoProvider):
    """MEPS 2.5 km control member analysis: the first three lead times of one cycle.

    Each lead time is spread over three files (height levels, pressure levels, surface)
    which are merged, then concatenated along time.
    """

    name = "meps_analysis"
    append_dim = "time"
    store_prefix = "bkr/dmi/meps.icechunk"
    #: Number of lead times taken from each cycle; cycles are three hours apart, so three
    #: steps make a gapless hourly series.
    steps = 3

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Download every file of the cycle, or none: a partial cycle cannot be merged."""
        return self.download_all(meps_analysis_urls(it, steps=self.steps), it, temp_dir)

    @staticmethod
    def _merge_step(files: List[str], step: int) -> xr.Dataset:
        """Merge the height, pressure and surface files for one lead time."""
        parts = []
        for kind in ("hl", "pl", "sfc"):
            match = [f for f in files if f"meps_{kind}_{step:02}_" in os.path.basename(f)]
            if not match:
                raise FileNotFoundError(f"no {kind} file for step {step}")
            parts.append(xr.open_dataset(match[0]))
        # The three files repeat the shared coordinates; no_conflicts is the behaviour the
        # original scripts relied on and is pinned here so an xarray default change cannot
        # silently start overriding one file's coordinates with another's.
        ds = xr.merge(parts, compat="no_conflicts", join="outer")

        # Everything but the icing index is published on a single degenerate level axis per
        # variable; squeeze those away and keep the icing levels as a real height dimension.
        icing_dims = set(ds["icing_index"].dims) if "icing_index" in ds else set()
        for dim in list(ds.dims):
            if dim in KEEP_DIMS or dim in icing_dims:
                continue
            if ds.sizes[dim] != 1:
                # Taking index 0 of a multi-valued axis would silently drop levels, and the
                # store would keep accepting the appends because the variable set is
                # unchanged. Say so rather than quietly losing data.
                logger.warning(
                    f"{dim} has {ds.sizes[dim]} levels, not 1; keeping only the first. "
                    "The met.no file layout may have changed."
                )
            ds = ds.isel({dim: 0}).drop_vars(dim, errors="ignore")
        for dim in icing_dims - KEEP_DIMS:
            ds = ds.rename({dim: "height"})
        return ds

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Merge each lead time, stack them along time and sort the vertical axes."""
        steps = [self._merge_step(input_files, step) for step in range(self.steps)]
        ds = xr.concat(steps, dim="time", data_vars="all")
        ds = self.finalise(ds)
        for coord in ("pressure", "height"):
            if coord in ds.coords:
                ds = ds.sortby(coord)
        return ds


class AndoyaMEPSProvider(MEPSAnalysisProvider):
    """MEPS analysis cut to the Andoya box, on the private bucket."""

    name = "andoya_meps"
    store_prefix = "andoya_meps.icechunk"
    bucket_env = ANDOYA_BUCKET_ENV
    crop = ANDOYA


class MEPSPostProcessedProvider(MetNoProvider):
    """Post-processed 1 km Nordic analysis, written a day at a time.

    The archive publishes one file per hour; a partition is a whole day so the store grows
    in useful increments rather than one timestep per commit.
    """

    name = "meps_postprocessed"
    append_dim = "time"
    store_prefix = "bkr/dmi/meps_postprocessed.icechunk"
    #: Hours per partition. A day, ending before the next partition's first hour so no
    #: timestep is written twice.
    hours = 24

    def hourly_range(self, it: pd.Timestamp) -> pd.DatetimeIndex:
        """The hours covered by the partition starting at ``it``."""
        return pd.date_range(start=it, periods=self.hours, freq="1h")

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Download every hourly file of the partition's day."""
        urls = [nordic_analysis_url(d) for d in self.hourly_range(it)]
        return self.download_all(urls, it, temp_dir)

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Stack the hourly files into one dataset."""
        ds = xr.open_mfdataset(
            sorted(input_files),
            concat_dim="time",
            combine="nested",
            data_vars="all",
        )
        return self.finalise(ds.drop_duplicates("time"))


class AndoyaMEPSPostProcessedProvider(MEPSPostProcessedProvider):
    """Post-processed Nordic analysis cut to the Andoya box, on the private bucket."""

    name = "andoya_meps_postprocessed"
    store_prefix = "andoya_meps_postprocessed.icechunk"
    bucket_env = ANDOYA_BUCKET_ENV
    crop = ANDOYA


class MEPSDetProvider(MetNoProvider):
    """MEPS 2.5 km deterministic model-level fields, read over OPeNDAP.

    Only the hybrid-level variables are kept, and only the first three lead times, which is
    what makes a gapless hourly series out of the three-hourly cycles.
    """

    name = "meps_det"
    append_dim = "time"
    store_prefix = "bkr/dmi/meps_model_level.icechunk"
    steps = 3

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Return the OPeNDAP endpoint; it is read in place rather than downloaded."""
        return [meps_det_opendap_url(it)]

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Keep the hybrid-level variables of the first lead times, at reduced precision."""
        ds = xr.open_dataset(input_files[0])
        # ``dims`` as well as ``coords``: some THREDDS aggregations expose hybrid as a bare
        # dimension with no coordinate variable, and keying only off coords drops everything.
        ds = ds[[v for v in ds.data_vars if "hybrid" in ds[v].dims or "hybrid" in ds[v].coords]]
        if not ds.data_vars:
            raise ValueError(f"{input_files[0]} has no hybrid-level variables")

        steps = [s for s in range(self.steps) if s < ds.sizes.get("time", 0)]
        ds = ds.isel(time=steps)
        # One variable at a time: each model-level field is the best part of a gigabyte in
        # float32 over the real grid, and computing the lot before downcasting would hold
        # twice the final footprint at once.
        for var in list(ds.data_vars):
            values = ds[var].compute()
            ds[var] = values if var in FLOAT16_SKIP else values.astype("float16")
        return self.finalise(ds)


#: Every variant, keyed by provider name. The Dagster assets are built from this.
PROVIDERS: dict[str, type[MetNoProvider]] = {
    cls.name: cls
    for cls in (
        NordicReflectivityProvider,
        AndoyaReflectivityProvider,
        MEPSAnalysisProvider,
        AndoyaMEPSProvider,
        MEPSPostProcessedProvider,
        AndoyaMEPSPostProcessedProvider,
        MEPSDetProvider,
    )
}

__all__ = [
    "ANDOYA",
    "ANDOYA_BUCKET_ENV",
    "AndoyaMEPSPostProcessedProvider",
    "AndoyaMEPSProvider",
    "AndoyaReflectivityProvider",
    "Box",
    "BucketNotConfigured",
    "MEPSAnalysisProvider",
    "MEPSDetProvider",
    "MEPSPostProcessedProvider",
    "MetNoProvider",
    "NordicReflectivityProvider",
    "PROVIDERS",
    "meps_analysis_urls",
    "meps_det_opendap_url",
    "nordic_analysis_url",
    "nordic_reflectivity_url",
]
