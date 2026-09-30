"""UK Met Office model output: atmospheric (global and UK), ocean and wave.

The Met Office publishes its deterministic atmospheric models as one NetCDF file per
variable per forecast step, under ``s3://met-office-atmospheric-model-data``. Three stores
are built from it, differing only in which grid and which forecast steps they keep:

===================================== ======================= ==============
store                                 model                   steps kept
===================================== ======================= ==============
``metoffice_global_deterministic_10km`` global-deterministic-10km 0-5 hourly
``..._10km_6hourly_24hr``               global-deterministic-10km 0,6,12,18,24
``metoffice_uk_deterministic_2km``      uk-deterministic-2km      0-5 hourly
===================================== ======================= ==============

The first two are forecast-shaped: the valid time becomes a ``step`` coordinate and the
partition timestamp becomes ``init_time``. The UK store instead keeps the valid times, so
the first six hours of each six-hourly run join up into a continuous hourly analysis.

The ocean and wave products are not on the public S3 bucket; they arrive on a local
archive disk and are read from ``<data_dir>/metoffice-ocean``. The ocean analysis splits
across two stores because its surface fields are hourly and its other fields are on depth
levels; both are forecast-shaped like the atmospheric stores.
"""

from __future__ import annotations

import pathlib
import re
from dataclasses import dataclass
from typing import Iterable, List, Mapping, Sequence

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common.dataset import rename_vars_by_long_name
from planetary_datasets.common.download import download_many
from planetary_datasets.common.store import existing_times

#: Public AWS Open Data bucket holding the Met Office atmospheric models.
ATMOSPHERIC_BUCKET = "met-office-atmospheric-model-data"

#: Anonymous HTTPS endpoint for the bucket, so downloads need no credentials.
ATMOSPHERIC_BASE_URL = f"https://{ATMOSPHERIC_BUCKET}.s3.amazonaws.com"

#: ``<valid time>-PT<hours>H<minutes>M-<variable>.nc``, the naming of every published file.
FILENAME_RE = re.compile(
    r"^(?P<valid>\d{8}T\d{4}Z)-PT(?P<hours>\d+)H(?P<minutes>\d+)M-(?P<variable>.+)\.nc$"
)

#: Trailing ``_max-PT01H`` style markers on a filename's variable part. The Met Office uses
#: the same CF name for an instantaneous field and its hourly aggregate, so the period has
#: to be folded into the variable name or the two collide on merge.
AGGREGATION_RE = re.compile(r"(?:_(?P<agg>max|min|mean))?-PT(?P<hours>\d+)H$")

#: Filename markers that select a subset of the atmosphere. These also share a CF name with
#: the surface field of the same quantity, so they get a suffix too.
VERTICAL_QUALIFIERS = ("_lowest_500m", "_below_500hPa")

#: Bookkeeping variables that describe the grid rather than the weather.
GRID_VARIABLES = (
    "forecast_period",
    "forecast_period_bnds",
    "forecast_reference_time",
    "time_bnds",
    "latitude_bnds",
    "longitude_bnds",
    "latitude_longitude",
    "flag",
    "bnds",
)

#: Fields whose dynamic range or required precision makes float16 lossy enough to matter.
NEVER_FLOAT16 = (
    "air_pressure_at_sea_level",
    "surface_air_pressure",
    "visibility_in_air_1.5m",
)


# --------------------------------------------------------------------------------------
# Atmospheric models
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class AtmosphericVariant:
    """The handful of settings that distinguish the three atmospheric stores.

    Attributes:
        name: Provider name, used in logs and as the Dagster asset name.
        store_prefix: Store location relative to the configured bucket.
        model: Top-level prefix in the source bucket.
        steps: Forecast lead times in whole hours to keep.
        collapse_to_step: Turn the valid time into ``step`` and append along ``init_time``.
            When False the valid times are kept and appended along ``time``.
        to_float16: Downcast floating point fields, except :data:`NEVER_FLOAT16`.
    """

    name: str
    store_prefix: str
    model: str
    steps: tuple[int, ...]
    collapse_to_step: bool
    to_float16: bool

    @property
    def append_dim(self) -> str:
        """Dimension new data is appended along in the store."""
        return "init_time" if self.collapse_to_step else "time"


GLOBAL_10KM = AtmosphericVariant(
    name="metoffice_global_deterministic_10km",
    store_prefix="bkr/metoffice/metoffice_global_deterministic_10km.icechunk",
    model="global-deterministic-10km",
    steps=(0, 1, 2, 3, 4, 5),
    collapse_to_step=True,
    to_float16=True,
)

GLOBAL_10KM_6HOURLY_24HR = AtmosphericVariant(
    name="metoffice_global_deterministic_10km_6hourly_24hr",
    store_prefix="bkr/metoffice/metoffice_global_deterministic_10km_6hourly_24hr.icechunk",
    model="global-deterministic-10km",
    steps=(0, 6, 12, 18, 24),
    collapse_to_step=True,
    to_float16=False,
)

UK_2KM = AtmosphericVariant(
    name="metoffice_uk_deterministic_2km",
    store_prefix="bkr/metoffice/metoffice_uk_deterministic_2km.icechunk",
    model="uk-deterministic-2km",
    steps=(0, 1, 2, 3, 4, 5),
    collapse_to_step=False,
    to_float16=True,
)


def parse_filename(path: str | pathlib.Path) -> dict | None:
    """Split a published filename into valid time, lead time and variable.

    Returns None for anything that does not look like a Met Office data file.
    """
    match = FILENAME_RE.match(pathlib.Path(path).name)
    if match is None:
        return None
    return {
        "valid_time": pd.Timestamp(match.group("valid").replace("Z", ""), tz=None),
        "hours": int(match.group("hours")),
        "minutes": int(match.group("minutes")),
        "variable": match.group("variable"),
    }


def variable_suffix(variable: str) -> str:
    """Suffix distinguishing aggregated or layer-limited fields from the plain field.

    ``wind_gust_at_10m_max-PT01H`` becomes ``_max_1h`` and
    ``precipitation_accumulation-PT06H`` becomes ``_6h``; both share a CF name with the
    instantaneous field they derive from.
    """
    suffix = ""
    for qualifier in VERTICAL_QUALIFIERS:
        if qualifier in variable:
            suffix += qualifier
            break
    match = AGGREGATION_RE.search(variable)
    if match is not None:
        agg = match.group("agg")
        hours = int(match.group("hours"))
        suffix += f"_{agg}_{hours}h" if agg else f"_{hours}h"
    return suffix


def drop_grid_variables(ds: xr.Dataset) -> xr.Dataset:
    """Remove grid-mapping, bounds and forecast bookkeeping variables.

    Grid mappings are found by their ``grid_mapping_name`` attribute rather than by name:
    the global model uses ``latitude_longitude`` and the UK one
    ``lambert_azimuthal_equal_area``, and neither carries any data.
    """
    mappings = [
        str(name) for name, var in ds.variables.items() if "grid_mapping_name" in var.attrs
    ]
    ds = ds.drop_vars([*GRID_VARIABLES, *mappings], errors="ignore")
    ds = ds.drop_vars([v for v in ds.variables if "bnds" in str(v)], errors="ignore")
    return ds.drop_dims("bnds", errors="ignore")


def preprocess(ds: xr.Dataset) -> xr.Dataset:
    """Normalise one published file before it is concatenated with its siblings.

    The ``height`` coordinate is folded into the variable name: a single height becomes a
    suffix such as ``_1.5m`` and the coordinate is dropped, because otherwise merging a
    1.5 m field with a 10 m one conflicts on the shared coordinate. Multi-level files keep
    ``height`` as a dimension and are marked ``_on_height_levels``.
    """
    ds = drop_grid_variables(ds)
    if "height" in ds.coords or "height" in ds.variables:
        height = np.atleast_1d(ds["height"].values)
        if height.size > 1:
            ds = ds.rename({var: f"{var}_on_height_levels" for var in ds.data_vars})
        else:
            ds = ds.rename({var: f"{var}_{height.item()}m" for var in ds.data_vars})
            ds = ds.drop_vars("height")
    return ds


def to_float16(ds: xr.Dataset, skip: Sequence[str] = NEVER_FLOAT16) -> xr.Dataset:
    """Downcast floating point fields to float16.

    Met Office fields are published at far more precision than the models resolve, so this
    roughly halves the store. Fields in ``skip``, and anything that is really a coordinate,
    keep their precision.
    """
    ds = ds.copy()
    for var in ds.data_vars:
        name = str(var)
        if name in skip or "coordinate" in name:
            continue
        if np.issubdtype(ds[var].dtype, np.floating):
            ds[var] = ds[var].astype(np.float16)
    return ds


def group_files_by_variable(files: Iterable[str]) -> dict[str, list[str]]:
    """Group published files by their variable, ignoring the lead time in the name."""
    groups: dict[str, list[str]] = {}
    for file in files:
        parsed = parse_filename(file)
        if parsed is None:
            logger.debug(f"ignoring unrecognised filename {file}")
            continue
        groups.setdefault(parsed["variable"], []).append(str(file))
    return {variable: sorted(paths) for variable, paths in sorted(groups.items())}


def chunking_for(ds: xr.Dataset, append_dim: str) -> dict[str, int]:
    """One chunk per timestep, whole slices in every other dimension."""
    chunks = {str(dim): -1 for dim in ds.dims}
    for dim in (append_dim, "step", "time"):
        if dim in chunks:
            chunks[dim] = 1
    return chunks


class MetOfficeAtmosphericProvider(BaseProvider):
    """Build one of the Met Office atmospheric stores from the public S3 bucket.

    Subclasses set :attr:`variant`; everything else is shared. Files already present under
    ``<data_dir>/metoffice/<model>/<init stamp>/`` are used instead of being re-downloaded,
    which is how a separately-run bulk download is picked up.
    """

    #: Per-store settings. Set by subclasses.
    variant: AtmosphericVariant

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        variant = cls.__dict__.get("variant")
        if variant is not None:
            cls.name = variant.name
            cls.store_prefix = variant.store_prefix
            cls.append_dim = variant.append_dim

    def __init__(self, config=None, archive_dir: pathlib.Path | None = None):
        """Build the provider, optionally overriding the config and archive directory."""
        super().__init__(config=config)
        self._archive_dir = archive_dir

    @property
    def archive_dir(self) -> pathlib.Path:
        """Directory holding any previously downloaded files for this model."""
        if self._archive_dir is not None:
            return self._archive_dir
        return self.config.data_dir / "metoffice" / self.variant.model

    def source_prefix(self, it: pd.Timestamp) -> str:
        """Key prefix in the source bucket for one initialisation time."""
        return f"{self.variant.model}/{pd.Timestamp(it).strftime('%Y%m%dT%H%MZ')}/"

    def wanted(self, path: str) -> bool:
        """True when a published file is one of the whole-hour steps this store keeps."""
        parsed = parse_filename(path)
        if parsed is None:
            return False
        return parsed["minutes"] == 0 and parsed["hours"] in self.variant.steps

    def list_remote(self, it: pd.Timestamp) -> List[str]:
        """List the keys available in the source bucket for one initialisation time."""
        import s3fs

        prefix = self.source_prefix(it)
        fs = s3fs.S3FileSystem(anon=True)
        try:
            keys = fs.ls(f"{ATMOSPHERIC_BUCKET}/{prefix}", detail=False)
        except FileNotFoundError:
            logger.info(f"{self.name}: nothing published under {prefix}")
            return []
        return [key.split(f"{ATMOSPHERIC_BUCKET}/", 1)[-1] for key in keys]

    def missing_steps(self, paths: Sequence[str]) -> List[int]:
        """Configured steps that are not represented in ``paths``.

        A partition is only worth building once every step is there: a store whose ``step``
        axis was built from complete runs cannot take a short one, and a gappy hourly
        series is worse than a gap.
        """
        parsed = [parse_filename(p) for p in paths]
        found = {p["hours"] for p in parsed if p is not None}
        return sorted(set(self.variant.steps) - found)

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Return the files for one initialisation time, downloading them if needed."""
        stamp = pd.Timestamp(it).strftime("%Y%m%dT%H%MZ")
        local = self.archive_dir / stamp
        if local.is_dir():
            cached = sorted(str(p) for p in local.glob("*.nc") if self.wanted(p))
            missing = self.missing_steps(cached)
            if cached and not missing:
                logger.info(f"{self.name}: using {len(cached)} archived file(s) in {local}")
                return cached
            if cached:
                logger.info(
                    f"{self.name}: archive {local} is missing steps {missing}, using the bucket"
                )

        keys = [key for key in self.list_remote(it) if self.wanted(key)]
        if not keys:
            return []
        missing = self.missing_steps(keys)
        if missing:
            logger.info(f"{self.name}: steps {missing} not published for {it}, skipping")
            return []

        destination = pathlib.Path(temp_dir or self.config.scratch_dir) / stamp
        paths = download_many(
            [f"{ATMOSPHERIC_BASE_URL}/{key}" for key in keys], destination, workers=8
        )
        return [str(p) for p in paths]

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Combine one initialisation time's files into a single dataset."""
        per_variable = []
        for variable, files in group_files_by_variable(input_files).items():
            ds = xr.open_mfdataset(
                files,
                combine="nested",
                concat_dim="time",
                preprocess=preprocess,
                decode_timedelta=False,
            ).sortby("time")
            suffix = variable_suffix(variable)
            if suffix:
                ds = ds.rename({var: f"{var}{suffix}" for var in ds.data_vars})
            per_variable.append(ds)

        if not per_variable:
            raise ValueError(f"{self.name}: no recognisable files for {it}")

        # Aggregated fields such as the hourly maximum gust have no step 0, so the merge
        # has to be an outer join and pad them rather than refuse to align.
        ds = drop_grid_variables(xr.merge(per_variable, compat="no_conflicts", join="outer"))
        if self.variant.to_float16:
            ds = to_float16(ds)

        if self.variant.collapse_to_step:
            init_time = pd.Timestamp(it)
            ds = ds.assign_coords(step=("time", (ds["time"] - np.datetime64(init_time)).values))
            ds = ds.swap_dims({"time": "step"}).drop_vars("time")
            ds = ds.assign_coords(init_time=init_time).expand_dims("init_time")
            ds = ds.sortby("step")
            step_dim = "step"
        else:
            ds = ds.sortby("time")
            step_dim = "time"

        if ds.sizes[step_dim] != len(self.variant.steps):
            raise ValueError(
                f"{self.name}: expected {len(self.variant.steps)} steps for {it}, "
                f"got {ds.sizes[step_dim]}"
            )

        return ds.chunk(chunking_for(ds, self.append_dim))


class MetOfficeGlobal10kmProvider(MetOfficeAtmosphericProvider):
    """Global 10 km deterministic model, first six hourly steps of each run."""

    variant = GLOBAL_10KM


class MetOfficeGlobal10km6Hourly24HourProvider(MetOfficeAtmosphericProvider):
    """Global 10 km deterministic model, six-hourly steps out to 24 hours."""

    variant = GLOBAL_10KM_6HOURLY_24HR


class MetOfficeUK2kmProvider(MetOfficeAtmosphericProvider):
    """UK 2 km deterministic model, kept as a continuous hourly analysis."""

    variant = UK_2KM


# --------------------------------------------------------------------------------------
# Ocean and wave
# --------------------------------------------------------------------------------------

#: Ocean product codes in the filenames on the archive disk.
OCEAN_INPUTS = ("BED", "ICE", "TEM", "CUR", "MLD", "SAL", "SSH")

#: Products published both as hourly instantaneous (``hi``) and daily mean (``dm``) files.
OCEAN_SPLIT_INPUTS = ("SSH", "CUR", "TEM")

#: Initialisation times published each day for the global wave model.
WAVE_RUNS = ("T0000Z", "T0600Z", "T1200Z", "T1800Z")

#: Hourly steps kept from each wave run, so four runs tile a day.
WAVE_STEPS_PER_RUN = 6


def slugify_long_names(ds: xr.Dataset) -> xr.Dataset:
    """Rename variables to their ``long_name``, as the ocean and wave stores do.

    The short names in these files are opaque product codes, and ``long_name`` carries the
    only human-readable description. ``A - B`` becomes ``a_to_b`` so ranges stay readable.
    """
    ds = rename_vars_by_long_name(ds)
    renames = {
        var: str(var).replace("-", "_").replace("/", "_").replace("___", "_to_")
        for var in ds.data_vars
    }
    return ds.rename({k: v for k, v in renames.items() if k != v})


class MetOfficeOceanProviderBase(BaseProvider):
    """Shared loading for the two global ocean analysis stores.

    One run of the ORCA025 ocean model writes both hourly surface fields and fields on
    depth levels. They have different shapes, so they go to two stores; both are built from
    the same set of files, which is why the split lives in one place.
    """

    append_dim = "init_time"
    product = "global-ocean-ORCA025"

    def __init__(self, config=None, archive_root: pathlib.Path | None = None):
        """Build the provider, optionally overriding the config and archive root."""
        super().__init__(config=config)
        self._archive_root = archive_root

    @property
    def archive_root(self) -> pathlib.Path:
        """Root of the local Met Office ocean archive."""
        if self._archive_root is not None:
            return self._archive_root
        return self.config.data_dir / "metoffice-ocean"

    def run_dir(self, it: pd.Timestamp) -> pathlib.Path:
        """Directory holding one ocean run."""
        return self.archive_root / self.product / pd.Timestamp(it).strftime("%Y/%m/%d/T%H%MZ")

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Return the archived files for one ocean run."""
        run_dir = self.run_dir(it)
        if not run_dir.is_dir():
            logger.info(f"{self.name}: no ocean archive directory {run_dir}")
            return []
        return sorted(str(p) for p in run_dir.glob("*.nc"))

    @staticmethod
    def split(
        input_files: Sequence[str], it: pd.Timestamp
    ) -> tuple[xr.Dataset | None, xr.Dataset | None]:
        """Return ``(surface, depth)`` datasets built from one run's files.

        A product goes to the depth store when it has a ``depth`` dimension, or when it has
        too few timesteps to be one of the hourly surface series.
        """
        init_time = pd.Timestamp(it)
        by_product: dict[str, list[str]] = {}
        for file in input_files:
            name = pathlib.Path(file).name
            for product in OCEAN_INPUTS:
                if product not in name:
                    continue
                key = product
                if product in OCEAN_SPLIT_INPUTS:
                    key = f"{product}_dm" if "_dm2" in name else f"{product}_hi"
                by_product.setdefault(key, []).append(file)
                break

        surface, depth = [], []
        for key, files in sorted(by_product.items()):
            ds = xr.open_mfdataset(
                sorted(files), combine="nested", concat_dim="time", preprocess=_preprocess_ocean
            ).sortby("time")
            if "depth" in ds.dims or ds.sizes.get("time", 0) < 12:
                depth.append(ds)
            else:
                # Keep only the 24 hours of analysis leading up to the initialisation.
                ds = ds.sel(
                    time=slice(
                        init_time - pd.Timedelta(24, "h"), init_time - pd.Timedelta(1, "min")
                    )
                )
                surface.append(ds)

        surface_ds = xr.merge(surface, compat="no_conflicts") if surface else None
        depth_ds = xr.merge(depth, compat="no_conflicts") if depth else None
        if depth_ds is not None and "depth" in depth_ds.dims:
            depth_ds = depth_ds.sortby("depth")
        return surface_ds, depth_ds

    def finalise(self, ds: xr.Dataset, it: pd.Timestamp) -> xr.Dataset:
        """Give one run's dataset its ``init_time`` dimension and chunking.

        The valid times become a ``step`` offset from the initialisation, exactly as the
        atmospheric stores do. They have to: ``time`` would otherwise be a dimension
        coordinate under ``init_time``, and appending a second run would silently relabel
        every run already in the store with the newest run's hours.
        """
        init_time = pd.Timestamp(it)
        if "time" in ds.dims:
            ds = ds.assign_coords(step=("time", (ds["time"] - np.datetime64(init_time)).values))
            ds = ds.swap_dims({"time": "step"}).drop_vars("time").sortby("step")
        ds = ds.drop_vars("init_time", errors="ignore")
        ds = ds.assign_coords(init_time=init_time).expand_dims("init_time")
        return ds.chunk(chunking_for(ds, self.append_dim))


def _preprocess_ocean(ds: xr.Dataset) -> xr.Dataset:
    """Give one ocean file readable variable names and the shared coordinate names.

    The fixed renames happen before the long names are applied: whether the archive
    declares ``forecast_reference_time`` as a coordinate or as a plain variable differs
    between products, and slugifying first would rename it out from under us.
    """
    ds = ds.drop_vars("forecast_period", errors="ignore")
    renames = {"lat": "latitude", "lon": "longitude", "forecast_reference_time": "init_time"}
    ds = ds.rename({k: v for k, v in renames.items() if k in ds.variables})
    if "init_time" in ds.data_vars:
        ds = ds.set_coords("init_time")
    return slugify_long_names(ds)


class MetOfficeOceanSurfaceProvider(MetOfficeOceanProviderBase):
    """Hourly surface fields from the global ORCA025 ocean analysis."""

    name = "metoffice_global_hourly_ocean_surface_analysis"
    store_prefix = "bkr/metoffice/metoffice_global_hourly_ocean_surface_analysis.icechunk"

    def process(self, input_files, it, temp_dir=None, **kwargs) -> xr.Dataset:
        """Return the hourly surface half of one ocean run."""
        surface, _ = self.split(input_files, it)
        if surface is None:
            raise ValueError(f"{self.name}: no hourly surface fields for {it}")
        if surface.sizes.get("time", 0) > 24:
            logger.warning(
                f"{self.name}: {surface.sizes['time']} hourly steps for {it}, expected at most 24"
            )
        return self.finalise(surface, it)


class MetOfficeOceanDepthProvider(MetOfficeOceanProviderBase):
    """Fields on depth levels from the global ORCA025 ocean analysis."""

    name = "metoffice_global_hourly_ocean_depth_analysis"
    store_prefix = "bkr/metoffice/metoffice_global_hourly_ocean_depth_analysis.icechunk"

    def process(self, input_files, it, temp_dir=None, **kwargs) -> xr.Dataset:
        """Return the depth-level half of one ocean run."""
        _, depth = self.split(input_files, it)
        if depth is None:
            raise ValueError(f"{self.name}: no depth-level fields for {it}")
        return self.finalise(depth, it)


class MetOfficeGlobalWaveProvider(BaseProvider):
    """Global wave model, tiled into a continuous hourly series.

    One partition is a day: the first six hours of each of the four runs are concatenated,
    giving 24 hourly steps appended along ``time``.
    """

    name = "metoffice_global_wave"
    store_prefix = "bkr/metoffice/metoffice_global_wave.icechunk"
    append_dim = "time"
    product = "global-wave"

    def __init__(self, config=None, archive_root: pathlib.Path | None = None):
        """Build the provider, optionally overriding the config and archive root."""
        super().__init__(config=config)
        self._archive_root = archive_root

    @property
    def archive_root(self) -> pathlib.Path:
        """Root of the local Met Office ocean and wave archive."""
        if self._archive_root is not None:
            return self._archive_root
        return self.config.data_dir / "metoffice-ocean"

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Return the archived files for every wave run on one day."""
        day = pd.Timestamp(it).normalize()
        stamp = day.strftime("%Y%m%d")
        root = self.archive_root / self.product / day.strftime("%Y/%m/%d")
        files: List[str] = []
        for run in WAVE_RUNS:
            pattern = f"b{stamp}{run}_hi{stamp}{run}-wave_global_standard_v1*"
            files.extend(sorted(str(p) for p in (root / run).glob(pattern)))
        if not files:
            logger.info(f"{self.name}: no wave files under {root}")
        return files

    def missing_timesteps(self, desired: pd.DatetimeIndex) -> List[pd.Timestamp]:
        """A day counts as stored only when all 24 of its hours are.

        Whether a run's first step is the run hour itself or an hour later differs between
        archive vintages, so both are accepted; but every hour of whichever series is used
        has to be present. Requiring the whole day means a run that was missing from the
        archive disk can still be backfilled later, and accepting only a complete series
        means a day is never mistaken for stored because the neighbouring day overlapped
        its boundary hour.
        """
        stored = existing_times(self.get_icechunk_repo(), append_dim=self.append_dim)
        if stored.size == 0:
            return list(desired)
        present = set(pd.DatetimeIndex(stored))
        missing = []
        for day in desired:
            start = pd.Timestamp(day).normalize()
            hours = pd.date_range(start, periods=25, freq="1h")
            series = (hours[:24], hours[1:])
            if not any(set(hours) <= present for hours in series):
                missing.append(day)
        return missing

    def process(self, input_files, it, temp_dir=None, **kwargs) -> xr.Dataset:
        """Concatenate the kept steps of each run into one day of hourly wave data."""
        by_run: dict[str, list[str]] = {}
        for file in input_files:
            by_run.setdefault(pathlib.Path(file).parent.name, []).append(file)

        per_run = []
        for run in sorted(by_run):
            ds = xr.open_mfdataset(sorted(by_run[run])).sortby("time")
            ds = slugify_long_names(ds)
            ds = ds.isel(time=slice(None, WAVE_STEPS_PER_RUN))
            ds = ds.drop_vars(
                ["forecast_period", "forecast_reference_time", "crs"], errors="ignore"
            ).drop_encoding()
            per_run.append(ds)

        if not per_run:
            raise ValueError(f"{self.name}: no wave files for {it}")

        ds = xr.concat(per_run, dim="time").sortby("time")
        return ds.chunk(chunking_for(ds, self.append_dim))


#: Every Met Office provider, keyed by name, for the Dagster assets and the CLI.
PROVIDERS: Mapping[str, type[BaseProvider]] = {
    cls.name: cls
    for cls in (
        MetOfficeGlobal10kmProvider,
        MetOfficeGlobal10km6Hourly24HourProvider,
        MetOfficeUK2kmProvider,
        MetOfficeOceanSurfaceProvider,
        MetOfficeOceanDepthProvider,
        MetOfficeGlobalWaveProvider,
    )
}
