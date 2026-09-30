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

The ocean and wave products each have a public bucket of their own, laid out one directory
per model run. Five stores are built from them, all *continuous hourly analyses*: from each
run only the steps falling before the next run's analysis time are kept, so consecutive
runs tile the time axis without overlapping.

=========================================== ============================== =============
store                                       bucket                         kept per run
=========================================== ============================== =============
``metoffice_global_wave``                   ``...-global-wave-model-data``  6 of 24 h
``metoffice_nws_wave``                      ``...-nws-wave-model-data``     6 of 24 h
``metoffice_global_ocean_hourly``           ``...-global-ocean-model-data`` 24 h
``metoffice_nws_ocean_surface_hourly``      ``...-nws-ocean-model-data``    24 h
``metoffice_nws_ocean_depth_hourly``        ``...-nws-ocean-model-data``    24 h
=========================================== ============================== =============

The NWS ocean run is split in two because the sizes are nothing alike: a day is 2.3 GB of
surface fields and 58 GB of fields on all 51 depth levels.

Two things are true of every store here and are why they stay affordable:

*Generations.* These are operational models and they get upgraded. Rather than failing
every partition after a resolution change, each provider names a *base* prefix and the
generation actually written to is matched on the incoming data's schema; see
:mod:`planetary_datasets.common.generations`. ``metoffice_global_wave_2`` is what this
looks like when it is done by hand.

*Bitrounding.* Floating point fields are rounded to :data:`METOFFICE_KEEPBITS` mantissa
bits before compression, which is a declared, bounded loss that roughly triples what zstd
manages on its own. See :func:`~planetary_datasets.common.store.bitround`.
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
from planetary_datasets.common.generations import GenerationalStoreMixin
from planetary_datasets.common.store import keepbits_for_tolerance

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

#: Mantissa bits kept in floating point fields across every Met Office store. Twelve bits
#: is one part in 4096 of the value: on a day of AMM15 salinity that is a maximum error of
#: 0.004 PSU, below what the model itself resolves, for roughly a third of the bytes that
#: zstd alone achieves. Measured rather than assumed — see
#: :func:`~planetary_datasets.common.store.bitround` for the mechanism, and the tests for
#: the error bound this is required to hold to.
METOFFICE_KEEPBITS = 12

#: Variables stored exactly, whatever :data:`METOFFICE_KEEPBITS` says. Matched as
#: substrings of the variable name, so they catch the field wherever it appears and at
#: whatever height it is reported.
#:
#: ``pressure``
#:     Every pressure field. These sit on a large offset — around 100000 Pa at the surface
#:     — and bitrounding is *relative*, so one part in 4096 of that is about 25 Pa. The
#:     field's own meaningful variability is finer than that, so the rounding would be
#:     visible in the data rather than below it.
#: ``eastward_wind`` / ``northward_wind``
#:     The wind components. They are not read on their own: they are differenced and
#:     combined to get speed and direction, and near a zero crossing the error that is
#:     negligible against the component is not negligible against the derived direction.
#:
#: Note that the global 10 km store happens to carry ``wind_speed`` and
#: ``wind_from_direction`` rather than components; the component patterns bind on the
#: stores that publish u/v.
KEEPBITS_EXACT: tuple[str, ...] = (
    "pressure",
    "eastward_wind",
    "northward_wind",
)

#: Per-family precision, in the style of the MARS per-parameter keepbits table: rather
#: than one number for a whole dataset, each quantity gets the precision its *units*
#: justify. Each entry is ``(substring, absolute tolerance, magnitude ceiling)``:
#:
#: * the tolerance is the smallest difference that is physically meaningful in that field;
#: * the ceiling is a value the field is not expected to exceed.
#:
#: Keepbits are derived from the pair at import by :func:`keepbits_for_tolerance`, because
#: bitrounding is *relative*: holding 0.01 K costs more bits at 350 K than at 30 K. The
#: ceiling is deliberately generous — it only ever buys extra bits, and a field that
#: overshoots it merely loses a little of its declared tolerance rather than becoming
#: wrong.
#:
#: Derived rather than measured per partition on purpose. Measuring would make a calm day
#: and a stormy one store at different precisions, so the store's accuracy would depend on
#: the weather in it.
#:
#: First match wins, so the list is ordered most specific first.
VARIABLE_TOLERANCES: tuple[tuple[str, float, float], ...] = (
    # Angles. A tenth of a degree is far finer than any wave or wind model resolves.
    ("direction", 0.1, 360.0),
    ("directional_spread", 0.1, 360.0),
    # Wave and sea-surface heights, in metres. Millimetre precision.
    ("significant_wave_height", 0.001, 40.0),
    ("wave_height", 0.001, 40.0),
    ("sea_surface_height", 0.001, 20.0),
    # Periods, in seconds.
    ("period", 0.01, 100.0),
    # Spectral energy density.
    ("energy", 0.01, 1000.0),
    # Temperatures, in kelvin or celsius: a hundredth of a degree.
    ("temperature", 0.01, 350.0),
    ("dew_point", 0.01, 350.0),
    # Salinity, in PSU.
    ("salinity", 0.001, 50.0),
    # Currents and wind speeds, in m/s. Note that the *components* are exempt entirely
    # (see KEEPBITS_EXACT); this covers the speeds and the ocean currents.
    ("current_velocity", 0.001, 15.0),
    ("wind_speed", 0.01, 150.0),
    ("wind_gust", 0.01, 150.0),
    # Depths and thicknesses, in metres.
    ("mixed_layer", 0.01, 12000.0),
    ("depth", 0.01, 12000.0),
    # Fractions and probabilities.
    ("fraction", 0.0001, 1.0),
    ("probability", 0.0001, 1.0),
    # Precipitation and fluxes, where the small values are the interesting ones, so this
    # is set relatively tight.
    ("rainfall", 0.0001, 100.0),
    ("precipitation", 0.0001, 100.0),
    ("radiation", 0.01, 2000.0),
    ("visibility", 1.0, 100000.0),
)


def metoffice_keepbits_table() -> dict[str, int]:
    """Resolve :data:`VARIABLE_TOLERANCES` to keepbits, once, at import."""
    return {
        pattern: keepbits_for_tolerance(ceiling, absolute_tolerance=tolerance)
        for pattern, tolerance, ceiling in VARIABLE_TOLERANCES
    }


#: ``substring -> keepbits``, in the order of :data:`VARIABLE_TOLERANCES`.
KEEPBITS_BY_PATTERN: dict[str, int] = metoffice_keepbits_table()


class MetOfficeKeepbitsMixin:
    """Per-variable bitrounding for every Met Office store.

    Resolution order, most specific first: a variable named in
    :attr:`~planetary_datasets.base.BaseProvider.keepbits_by_variable`; a variable matching
    :data:`KEEPBITS_EXACT`, which is stored bit for bit; a variable matching
    :data:`KEEPBITS_BY_PATTERN`, which gets the precision its units justify; and otherwise
    :data:`METOFFICE_KEEPBITS`.
    """

    #: Fallback for a variable no pattern matches — a new diagnostic, say. Deliberately on
    #: the conservative side of the derived table, which mostly lands between 11 and 16.
    keepbits = METOFFICE_KEEPBITS
    keepbits_exact = KEEPBITS_EXACT

    def keepbits_for(self, variable: str) -> int | None:
        """Mantissa bits for one variable, or None to store it exactly."""
        if variable in self.keepbits_by_variable:
            return self.keepbits_by_variable[variable]
        if any(pattern in variable for pattern in self.keepbits_exact):
            return None
        for pattern, bits in KEEPBITS_BY_PATTERN.items():
            if pattern in variable:
                return bits
        return self.keepbits


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


class MetOfficeAtmosphericProvider(MetOfficeKeepbitsMixin, GenerationalStoreMixin, BaseProvider):
    """Build one of the Met Office atmospheric stores from the public S3 bucket.

    Subclasses set :attr:`variant`; everything else is shared. Files already present under
    ``<data_dir>/metoffice/<model>/<init stamp>/`` are used instead of being re-downloaded,
    which is how a separately-run bulk download is picked up.

    The store is chosen by schema rather than fixed: the Met Office upgrades these models,
    and a run published on a new grid or with a new diagnostic starts a new generation
    instead of failing every partition after the upgrade. See
    :mod:`planetary_datasets.common.generations`.
    """

    #: Per-store settings. Set by subclasses.
    variant: AtmosphericVariant


    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        variant = cls.__dict__.get("variant")
        if variant is not None:
            cls.name = variant.name
            cls.base_store_prefix = variant.store_prefix
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
#
# All four of these products are published the same way: one directory per model run,
# ``<product>/<YYYY>/<MM>/<DD>/T<HHMM>Z/``, holding one NetCDF per variable (wave) or per
# product code (ocean), each covering the whole forecast. They used to be read off a local
# archive disk; they are now downloaded from the Met Office's public buckets, which use
# exactly that layout.
#
# Every store here is a *continuous hourly analysis* rather than a forecast archive: from
# each run only the steps falling before the next run's analysis time are kept, so
# consecutive runs tile the time axis without overlapping. That is six hours from each of
# the four daily wave runs, and twenty-four from the single daily ocean run.

#: Public buckets, one per product family. All read anonymously.
GLOBAL_WAVE_BUCKET = "met-office-global-wave-model-data"
GLOBAL_OCEAN_BUCKET = "met-office-global-ocean-model-data"
NWS_OCEAN_BUCKET = "met-office-nws-ocean-model-data"
NWS_WAVE_BUCKET = "met-office-nws-wave-model-data"

#: Ocean product codes that are published as hourly instantaneous (``hi``) files. The rest
#: of each run is daily means and fields on depth levels, which have no place on an hourly
#: axis; see the module docstring.
GLOBAL_OCEAN_HOURLY_INPUTS = ("CUR", "SSH", "TEM")
NWS_OCEAN_HOURLY_INPUTS = (
    "BED", "CUR", "MAXCURU", "MAXCURV", "MLD", "SAL", "SSC", "SSH",
    "SSS", "SST", "TEM", "TEMPIS", "ZMAXCUR",
)

#: Initialisation times published each day for the wave models.
WAVE_RUNS = ("T0000Z", "T0600Z", "T1200Z", "T1800Z")

#: Hours between consecutive analyses, per family. This is what decides how much of each
#: run is kept, so it is the one number to change if the Met Office adds a run.
WAVE_ANALYSIS_INTERVAL = pd.Timedelta(6, "h")
OCEAN_ANALYSIS_INTERVAL = pd.Timedelta(24, "h")


def anonymous_s3():
    """A filesystem for the public Met Office buckets, which need no credentials."""
    import s3fs

    return s3fs.S3FileSystem(anon=True)


#: Names this module would generate today that an existing store already holds under an
#: older spelling. Applied last, so the store keeps the name it was created with.
#:
#: ``metoffice_global_wave`` was written before :func:`slugify_long_names` learned to turn
#: ``A - B`` into ``a_to_b``, so its peak-period variable is spelled with the bare triple
#: underscore that ``" / "`` used to collapse to. Without this the only difference between
#: today's data and 14418 stored steps would be that one name, and the generation
#: machinery — which cannot tell a model upgrade from a rename in our own code — would
#: fork a new store over it.
LEGACY_VARIABLE_NAMES: Mapping[str, str] = {
    "wave_period_at_spectral_peak_to_peak_period_tp": (
        "wave_period_at_spectral_peak___peak_period_tp"
    ),
}


def slugify_long_names(ds: xr.Dataset) -> xr.Dataset:
    """Rename variables to their ``long_name``, as the ocean and wave stores do.

    The short names in these files are opaque product codes (``VHM0_SW1``, ``thetao``), and
    ``long_name`` carries the only human-readable description. ``A - B`` becomes ``a_to_b``
    so ranges stay readable, and :data:`LEGACY_VARIABLE_NAMES` then puts back the spellings
    the long-lived stores were created with.
    """
    ds = rename_vars_by_long_name(ds)
    renames = {
        var: str(var).replace("-", "_").replace("/", "_").replace("___", "_to_")
        for var in ds.data_vars
    }
    renames = {k: LEGACY_VARIABLE_NAMES.get(v, v) for k, v in renames.items()}
    return ds.rename({k: v for k, v in renames.items() if k != v})


def _preprocess_ocean(ds: xr.Dataset) -> xr.Dataset:
    """Give one ocean file readable variable names and the shared coordinate names.

    The fixed renames happen before the long names are applied: whether a product declares
    ``forecast_reference_time`` as a coordinate or as a plain variable differs between
    them, and slugifying first would rename it out from under us.
    """
    ds = ds.drop_vars(["forecast_period", "time_bounds", "time_bnds"], errors="ignore")
    renames = {"lat": "latitude", "lon": "longitude"}
    ds = ds.rename({k: v for k, v in renames.items() if k in ds.variables})
    ds = ds.drop_vars(["forecast_reference_time", "crs"], errors="ignore")
    return slugify_long_names(ds)


class MetOfficeRunArchiveProvider(MetOfficeKeepbitsMixin, GenerationalStoreMixin, BaseProvider):
    """Shared fetching for the run-directory buckets, and the analysis-window rule.

    Subclasses set :attr:`bucket`, :attr:`product` and :attr:`analysis_interval`, list the
    runs a partition covers with :meth:`runs_for`, and turn the downloaded files into one
    dataset in ``process``.

    The store is chosen by schema, not fixed: these are operational models and they get
    upgraded. See :mod:`planetary_datasets.common.generations`.
    """

    append_dim = "time"
    keepbits = METOFFICE_KEEPBITS
    keepbits_exact = KEEPBITS_EXACT

    #: Public bucket and the prefix within it, e.g. ``global-wave``.
    bucket: str
    product: str

    #: Gap between consecutive analyses; how much of each run is kept.
    analysis_interval: pd.Timedelta

    def wanted_file(self, name: str, run: pd.Timestamp) -> bool:
        """Whether one file in ``run``'s directory belongs to this store.

        Applied at *listing* time, before anything is downloaded: a run directory holds
        several products and several forecast days, and these files are hundreds of
        megabytes each. The default takes the whole run.
        """
        return True

    def __init__(self, config=None, archive_root: pathlib.Path | None = None):
        """Build the provider.

        Args:
            config: Optional configuration override.
            archive_root: Read from this local directory instead of the bucket. The layout
                is identical, so a bulk copy of the bucket works unchanged; this is the
                escape hatch for anyone holding one, and what the tests use.
        """
        super().__init__(config=config)
        self._archive_root = archive_root
        self._filesystem = None

    @property
    def archive_root(self) -> pathlib.Path | None:
        """Local archive to read instead of the bucket, if one is configured."""
        return self._archive_root

    def filesystem(self):
        """Anonymous handle on the bucket, cached across the partitions of one run."""
        if self._filesystem is None:
            self._filesystem = anonymous_s3()
        return self._filesystem

    def runs_for(self, it: pd.Timestamp) -> list[pd.Timestamp]:
        """The initialisation times whose kept steps make up this partition.

        The default is every run that starts inside the partition's day, spaced by
        :attr:`analysis_interval`, so the kept windows tile it exactly.
        """
        day = pd.Timestamp(it).normalize()
        count = max(1, int(pd.Timedelta(1, "D") / self.analysis_interval))
        return [day + i * self.analysis_interval for i in range(count)]

    def run_prefix(self, run: pd.Timestamp) -> str:
        """Key prefix of one run's directory within the bucket."""
        return f"{self.product}/{pd.Timestamp(run).strftime('%Y/%m/%d/T%H%MZ')}"

    def _list_run(self, run: pd.Timestamp) -> list[str]:
        """Object keys, or local paths, for one run's files."""
        prefix = self.run_prefix(run)
        if self.archive_root is not None:
            directory = self.archive_root / prefix
            if not directory.is_dir():
                return []
            found = sorted(str(p) for p in directory.glob("*.nc"))
        else:
            try:
                found = sorted(
                    str(k)
                    for k in self.filesystem().ls(f"{self.bucket}/{prefix}", detail=False)
                    if str(k).endswith(".nc")
                )
            except FileNotFoundError:
                return []
        return [f for f in found if self.wanted_file(pathlib.PurePosixPath(f).name, run)]

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Download every run this partition needs, skipping runs that are not published."""
        directory = pathlib.Path(temp_dir) if temp_dir is not None else self.config.scratch_dir
        paths: List[str] = []

        for run in self.runs_for(it):
            keys = self._list_run(run)
            if not keys:
                logger.info(f"{self.name}: no files for run {run}")
                continue
            if self.archive_root is not None:
                paths.extend(keys)
                continue
            # One directory per run, so `process` can still tell the runs apart by parent.
            into = directory / pd.Timestamp(run).strftime("T%H%MZ")
            into.mkdir(parents=True, exist_ok=True)
            filesystem = self.filesystem()
            for key in keys:
                local = into / pathlib.PurePosixPath(key).name
                try:
                    filesystem.get(key, str(local))
                except FileNotFoundError:
                    logger.debug(f"{self.name}: {key} vanished mid-fetch")
                    continue
                paths.append(str(local))

        if not paths:
            logger.info(f"{self.name}: nothing published for {pd.Timestamp(it)}")
        return paths

    def expected_times(self, it: pd.Timestamp) -> pd.DatetimeIndex:
        """The hours one partition contributes to the store.

        A partition is a day of hourly steps however many runs make it up.
        """
        start = pd.Timestamp(it).normalize()
        return pd.date_range(start, periods=24, freq="1h")

    def missing_timesteps(self, desired) -> List[pd.Timestamp]:
        """Partitions in ``desired`` that the store does not already hold in full.

        Asked about the hours a partition *writes* rather than about its own timestamp,
        because for the ocean models the two are not the same: a run at 00:00 contributes
        01:00 through the following midnight, so its own timestamp is a step the *previous*
        partition wrote. Checking the timestamp would report every second day as already
        stored and silently skip it.

        Both step conventions are accepted — a wave run numbers its steps from the analysis
        hour and an ocean run from an hour later — and a day counts as stored only when all
        24 hours of whichever series it uses are present, so a partition written short is
        revisited rather than retired.
        """
        stored = self.stored_times()
        if not stored:
            return list(pd.DatetimeIndex(desired))

        missing = []
        for partition in pd.DatetimeIndex(desired):
            hours = self.expected_times(partition)
            conventions = (hours, hours + pd.Timedelta(1, "h"))
            if not any(set(series) <= stored for series in conventions):
                missing.append(partition)
        return missing

    def keep_analysis_window(self, ds: xr.Dataset, run: pd.Timestamp) -> xr.Dataset:
        """Drop the steps of ``run`` that the next run supersedes.

        The window is ``[t0, t0 + analysis_interval)`` anchored on the run's *first* step
        rather than on the analysis time itself, because the two families number their
        steps differently: a wave run's first step is the analysis hour (00:00 for T0000Z),
        while an ocean run's is an hour later (01:00, through 00:00 the next day). Anchoring
        on the analysis time would drop an hour from one of them and duplicate one in the
        other. Anchored on the first step, consecutive runs tile either way.
        """
        times = pd.DatetimeIndex(ds["time"].values)
        if times.empty:
            return ds
        start = times.min()
        end = start + self.analysis_interval
        return ds.isel(time=np.flatnonzero((times >= start) & (times < end)))

    def group_by_run(self, input_files: Sequence[str]) -> dict[pd.Timestamp, list[str]]:
        """Group a partition's files by the run they came from.

        Keyed off the initialisation stamp in the filename rather than the directory the
        file was downloaded into, so a local archive laid out differently still groups.
        """
        out: dict[pd.Timestamp, list[str]] = {}
        for path in input_files:
            try:
                run = pd.Timestamp(_run_stamp(path))
            except ValueError:
                logger.debug(f"{self.name}: no initialisation stamp in {path}, skipping")
                continue
            out.setdefault(run, []).append(path)
        return {run: sorted(files) for run, files in out.items()}


def _run_stamp(path: str) -> str:
    """The ``YYYYMMDDTHHMM`` initialisation stamp encoded in a Met Office filename.

    Both naming conventions carry it: ``b20260929T0000Z_hi...`` for the wave products and
    ``..._b20260929_hi20260929.nc`` for the ocean ones, the latter without an hour.
    """
    name = pathlib.Path(path).name
    match = re.search(r"b(\d{8})T(\d{2})(\d{2})Z", name)
    if match:
        return f"{match.group(1)}T{match.group(2)}{match.group(3)}"
    match = re.search(r"_b(\d{8})_", name)
    if match:
        return f"{match.group(1)}T0000"
    raise ValueError(f"no initialisation stamp in {name!r}")


# --------------------------------------------------------------------------------------
# Wave
# --------------------------------------------------------------------------------------


class MetOfficeWaveProvider(MetOfficeRunArchiveProvider):
    """A Met Office wave model, tiled into a continuous hourly series.

    One partition is a day. Each of the four runs contributes the six hours before the
    next one starts, so the day comes out as 24 hourly steps on ``time``.
    """

    analysis_interval = WAVE_ANALYSIS_INTERVAL

    #: ``wave_global_standard_v1`` or ``wave_uk_standard_v1``; also selects the files.
    wave_product: str

    def wanted_file(self, name: str, run: pd.Timestamp) -> bool:
        """One file per variable, all of them for this wave product."""
        return self.wave_product in name

    def process(self, input_files, it, temp_dir=None, **kwargs) -> xr.Dataset:
        """Concatenate each run's kept steps into one day of hourly wave data."""
        per_run = []
        for run, files in sorted(self.group_by_run(input_files).items()):
            ds = xr.open_mfdataset(sorted(files), decode_timedelta=False).sortby("time")
            ds = slugify_long_names(ds)
            ds = ds.drop_vars(
                ["forecast_period", "forecast_reference_time", "crs"], errors="ignore"
            ).drop_encoding()
            kept = self.keep_analysis_window(ds, run)
            if kept.sizes.get("time", 0) == 0:
                logger.warning(f"{self.name}: run {run} contributed no steps")
                continue
            per_run.append(kept)

        if not per_run:
            raise ValueError(f"{self.name}: no wave steps for {it}")

        ds = xr.concat(per_run, dim="time").sortby("time")
        return ds.chunk(chunking_for(ds, self.append_dim))


class MetOfficeGlobalWaveProvider(MetOfficeWaveProvider):
    """Global wave model, hourly."""

    name = "metoffice_global_wave"
    base_store_prefix = "bkr/metoffice/metoffice_global_wave.icechunk"
    bucket = GLOBAL_WAVE_BUCKET
    product = "global-wave"
    wave_product = "wave_global_standard_v1"


class MetOfficeNWSWaveProvider(MetOfficeWaveProvider):
    """North West Shelf (AMM15) wave model, hourly."""

    name = "metoffice_nws_wave"
    base_store_prefix = "bkr/metoffice/metoffice_nws_wave.icechunk"
    bucket = NWS_WAVE_BUCKET
    product = "nws-wave"
    wave_product = "wave_uk_standard_v1"


# --------------------------------------------------------------------------------------
# Ocean
# --------------------------------------------------------------------------------------


def ocean_product_code(path: str, codes: Sequence[str]) -> str:
    """The product code in an ocean filename, e.g. ``CUR`` or ``MAXCURU``.

    Longest match wins: ``MAXCURU`` contains ``CUR``, and matching the shorter one would
    label the maximum-current file as the instantaneous current.
    """
    name = pathlib.Path(path).name
    matches = [code for code in codes if f"_{code}_" in name]
    return max(matches, key=len) if matches else "unknown"


def disambiguate_products(
    parts: Sequence[tuple[str, xr.Dataset]],
) -> list[tuple[str, xr.Dataset]]:
    """Suffix variable names that two products both claim, with the product code.

    Several NWS products share a ``long_name`` while holding different quantities — the
    instantaneous current ``CUR`` and the daily maximum ``MAXCURU`` both describe
    themselves as "eastward current velocity in the water column" — so slugifying alone
    collides them and the merge is refused. Only the names that actually clash are
    suffixed, so the common case keeps its clean name.
    """
    seen: dict[str, int] = {}
    for _, ds in parts:
        for var in ds.data_vars:
            seen[str(var)] = seen.get(str(var), 0) + 1
    clashing = {var for var, count in seen.items() if count > 1}
    if not clashing:
        return list(parts)

    logger.debug(f"disambiguating {sorted(clashing)} by product code")
    out = []
    for code, ds in parts:
        renames = {
            var: f"{var}_{code.lower()}" for var in ds.data_vars if str(var) in clashing
        }
        out.append((code, ds.rename(renames) if renames else ds))
    return out


class MetOfficeOceanHourlyProvider(MetOfficeRunArchiveProvider):
    """A Met Office FOAM ocean analysis, as a continuous hourly series.

    One run a day, so one partition is one run and the whole 24 hours of it are kept. Only
    the hourly instantaneous (``hi``) products are read: the rest of the run is daily means
    and depth-level fields, which cannot share an hourly axis.
    """

    analysis_interval = OCEAN_ANALYSIS_INTERVAL

    #: Product codes to read, e.g. ``("CUR", "SSH", "TEM")``.
    hourly_inputs: tuple[str, ...]

    #: Which half of the run this store keeps. ``"surface"`` takes the fields with no
    #: ``depth`` dimension, ``"depth"`` the ones with it, None takes both. They are split
    #: because the sizes are nothing alike: one NWS day is 2.3 GB of surface fields and
    #: 58 GB of depth-resolved ones, and putting them together would make the cheap half
    #: unusable without paying for the expensive one.
    keep_dimension: str | None = None

    def select_fields(self, ds: xr.Dataset) -> xr.Dataset:
        """Keep only the surface or only the depth-resolved fields, per :attr:`keep_dimension`."""
        if self.keep_dimension is None:
            return ds
        want_depth = self.keep_dimension == "depth"
        keep = [v for v in ds.data_vars if ("depth" in ds[v].dims) == want_depth]
        if not keep:
            return ds[[]]
        out = ds[keep]
        if not want_depth and "depth" in out.dims:
            out = out.drop_dims("depth")
        return out

    def runs_for(self, it: pd.Timestamp) -> list[pd.Timestamp]:
        """The single daily analysis."""
        return [pd.Timestamp(it).normalize()]

    def wanted_file(self, name: str, run: pd.Timestamp) -> bool:
        """The hourly products, for this run's own day only.

        One run directory holds nine forecast days, ``hi`` and ``dm``, for every product:
        around 180 files and tens of gigabytes. What this store wants is one day of hourly
        instantaneous fields, so the day is matched as well as the product — the rest of
        the forecast is what the *next* run's partition covers, from its own analysis.
        """
        if f"_hi{pd.Timestamp(run).strftime('%Y%m%d')}" not in name:
            return False
        return any(f"_{code}_" in name for code in self.hourly_inputs)

    def process(self, input_files, it, temp_dir=None, **kwargs) -> xr.Dataset:
        """Merge the run's hourly products into one day of hourly ocean data."""
        run = pd.Timestamp(it).normalize()
        if not input_files:
            raise ValueError(f"{self.name}: no hourly ocean products for {it}")

        by_code: list[tuple[str, xr.Dataset]] = []
        for path in sorted(input_files):
            ds = _preprocess_ocean(xr.open_dataset(path, decode_timedelta=False))
            if "time" not in ds.dims:
                continue
            by_code.append((ocean_product_code(path, self.hourly_inputs), ds.sortby("time")))

        if not by_code:
            raise ValueError(f"{self.name}: no hourly fields for {it}")

        parts = [ds for _, ds in disambiguate_products(by_code)]
        ds = xr.merge(parts, compat="no_conflicts", join="exact").sortby("time")
        ds = self.select_fields(ds)
        if not ds.data_vars:
            raise ValueError(
                f"{self.name}: the run for {it} has no {self.keep_dimension} fields"
            )
        ds = self.keep_analysis_window(ds, run).drop_encoding()
        if ds.sizes.get("time", 0) == 0:
            raise ValueError(f"{self.name}: no steps inside the analysis window for {it}")
        return ds.chunk(chunking_for(ds, self.append_dim))


class MetOfficeGlobalOceanHourlyProvider(MetOfficeOceanHourlyProvider):
    """Global ORCA025 ocean analysis, hourly surface fields."""

    name = "metoffice_global_ocean_hourly"
    base_store_prefix = "bkr/metoffice/metoffice_global_ocean_hourly.icechunk"
    bucket = GLOBAL_OCEAN_BUCKET
    product = "global-ocean-ORCA025"
    hourly_inputs = GLOBAL_OCEAN_HOURLY_INPUTS


class MetOfficeNWSOceanBase(MetOfficeOceanHourlyProvider):
    """North West Shelf (AMM15) FOAM ocean analysis, hourly.

    The ``nws-ocean`` run directory also carries the AMM15 *wave* files
    (``level1_wave_amm15_NWS_WAV_*``); those belong to the NWS wave store and are excluded
    by :attr:`hourly_inputs` not naming ``WAV``.
    """

    bucket = NWS_OCEAN_BUCKET
    product = "nws-ocean"
    hourly_inputs = NWS_OCEAN_HOURLY_INPUTS


class MetOfficeNWSOceanSurfaceHourlyProvider(MetOfficeNWSOceanBase):
    """AMM15 hourly surface fields: SSH, SST, SSS, mixed layer, surface and max currents."""

    name = "metoffice_nws_ocean_surface_hourly"
    base_store_prefix = "bkr/metoffice/metoffice_nws_ocean_surface_hourly.icechunk"
    keep_dimension = "surface"


class MetOfficeNWSOceanDepthHourlyProvider(MetOfficeNWSOceanBase):
    """AMM15 hourly fields on all 51 depth levels: currents, temperature, salinity.

    The heaviest store here by a wide margin: around 58 GB a day uncompressed. Bitrounding
    and zstd bring that down by roughly an order of magnitude on disk, which is what makes
    keeping the full depth resolution affordable rather than a choice between it and
    nothing.
    """

    name = "metoffice_nws_ocean_depth_hourly"
    base_store_prefix = "bkr/metoffice/metoffice_nws_ocean_depth_hourly.icechunk"
    keep_dimension = "depth"


#: Every Met Office provider, keyed by name, for the Dagster assets and the CLI.
PROVIDERS: Mapping[str, type[BaseProvider]] = {
    cls.name: cls
    for cls in (
        MetOfficeGlobal10kmProvider,
        MetOfficeGlobal10km6Hourly24HourProvider,
        MetOfficeUK2kmProvider,
        MetOfficeGlobalWaveProvider,
        MetOfficeNWSWaveProvider,
        MetOfficeGlobalOceanHourlyProvider,
        MetOfficeNWSOceanSurfaceHourlyProvider,
        MetOfficeNWSOceanDepthHourlyProvider,
    )
}
