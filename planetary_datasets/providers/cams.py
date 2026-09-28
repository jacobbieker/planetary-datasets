"""CAMS atmospheric composition providers.

CAMS is the Copernicus Atmosphere Monitoring Service. Everything here is retrieved from
the Copernicus Atmosphere Data Store (ADS) with the ``cdsapi`` client, which hands back a
zipped NetCDF bundle (``.nc.zip``) rather than a plain file, so the unzip step is part of
processing rather than of downloading.

Three datasets are covered:

``CAMSGlobalCompositionProvider``
    ``cams-global-atmospheric-composition-forecasts`` on the global grid, pressure levels
    plus the surface radiation fields. One partition per valid hour; the 00Z and 12Z runs
    each supply the following twelve hours.
``CAMSGlobalAODProvider``
    The aerosol-optical-depth and radiation subset of the same ADS dataset, retrieved a
    week and a variable at a time out to a 120 hour lead.
``CAMSEuropeAirQualityProvider``
    ``cams-europe-air-quality-forecast``, the regional ensemble, also weekly per variable.

Other CAMS collections on ADS that the same machinery would serve, should they be wanted
later, are ``cams-global-reanalysis-eac4`` and ``cams-global-greenhouse-gas-forecasts``.

Credentials: ``cdsapi`` reads ``~/.cdsapirc`` by default. That still works. When
``CDSAPI_KEY`` (and optionally ``CDSAPI_URL``) are configured they take precedence, which
is how this runs under Dagster and in containers. With neither, a
:class:`~planetary_datasets.config.MissingCredential` is raised before any request is made
rather than an opaque failure from inside the client.
"""

from __future__ import annotations

import os
import pathlib
import shutil
from io import BytesIO
from typing import Any, Iterable, List, Sequence
from zipfile import ZipFile

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common.dataset import (
    make_lat_lon_coords_consistent,
    sort_vertical_coords,
)
from planetary_datasets.config import Config, MissingCredential, get_config

#: Default endpoint for the Atmosphere Data Store. The ``cdsapi`` default points at the
#: Climate Data Store, which does not serve CAMS, so it cannot be relied on here.
DEFAULT_ADS_URL = "https://ads.atmosphere.copernicus.eu/api"

GLOBAL_COMPOSITION_DATASET = "cams-global-atmospheric-composition-forecasts"
EUROPE_AIR_QUALITY_DATASET = "cams-europe-air-quality-forecast"

#: Pressure levels requested for the global composition forecast, in hPa.
CAMS_PRESSURE_LEVELS: tuple[str, ...] = (
    "1", "2", "3", "5", "7", "10", "20", "30", "50", "70", "100", "150", "200", "250",
    "300", "400", "500", "600", "700", "800", "850", "900", "925", "950", "1000",
)

#: Model levels requested for the European air quality ensemble, in metres above ground.
CAMS_EUROPE_LEVELS: tuple[str, ...] = ("0", "50", "250", "500", "1000", "3000", "5000")

CAMS_GLOBAL_COMPOSITION_VARIABLES: tuple[str, ...] = (
    "ammonium_aerosol_optical_depth_550nm",
    "black_carbon_aerosol_optical_depth_550nm",
    "dust_aerosol_optical_depth_550nm",
    "nitrate_aerosol_optical_depth_550nm",
    "sea_salt_aerosol_optical_depth_550nm",
    "sulphate_aerosol_optical_depth_550nm",
    "total_aerosol_optical_depth_469nm",
    "total_aerosol_optical_depth_550nm",
    "total_aerosol_optical_depth_670nm",
    "total_aerosol_optical_depth_865nm",
    "total_aerosol_optical_depth_1240nm",
    "total_column_carbon_monoxide",
    "total_column_chlorine_monoxide",
    "total_column_chlorine_nitrate",
    "total_column_ethane",
    "total_column_formaldehyde",
    "total_column_hydrogen_chloride",
    "total_column_hydrogen_cyanide",
    "total_column_hydrogen_peroxide",
    "total_column_hydroxyl_radical",
    "total_column_isoprene",
    "total_column_methane",
    "total_column_nitric_acid",
    "total_column_nitrogen_dioxide",
    "total_column_nitrogen_monoxide",
    "total_column_ozone",
    "total_column_peroxyacetyl_nitrate",
    "total_column_propane",
    "total_column_sulphur_dioxide",
    "total_column_volcanic_sulphur_dioxide",
    "total_column_water_vapour",
    "uv_biologically_effective_dose_clear_sky",
    "dust_aerosol_0.03-0.55um_mixing_ratio",
    "dust_aerosol_0.55-0.9um_mixing_ratio",
    "dust_aerosol_0.9-20um_mixing_ratio",
    "asymmetry_factor_340nm",
    "asymmetry_factor_355nm",
    "asymmetry_factor_380nm",
    "asymmetry_factor_400nm",
    "asymmetry_factor_440nm",
    "asymmetry_factor_469nm",
    "asymmetry_factor_500nm",
    "asymmetry_factor_532nm",
    "asymmetry_factor_550nm",
    "asymmetry_factor_645nm",
    "asymmetry_factor_670nm",
    "asymmetry_factor_800nm",
    "asymmetry_factor_858nm",
    "asymmetry_factor_865nm",
    "asymmetry_factor_1020nm",
    "asymmetry_factor_1064nm",
    "asymmetry_factor_1240nm",
    "asymmetry_factor_1640nm",
    "asymmetry_factor_2130nm",
    "dust_aerosol_0.03-0.55um_optical_depth_550nm",
    "dust_aerosol_0.55-9um_optical_depth_550nm",
    "dust_aerosol_9-20um_optical_depth_550nm",
    "hydrophilic_black_carbon_aerosol_optical_depth_550nm",
    "hydrophilic_organic_matter_aerosol_optical_depth_550nm",
    "hydrophobic_black_carbon_aerosol_optical_depth_550nm",
    "hydrophobic_organic_matter_aerosol_optical_depth_550nm",
    "nitrate_coarse_mode_aerosol_optical_depth_550nm",
    "nitrate_fine_mode_aerosol_optical_depth_550nm",
    "sea_salt_aerosol_0.03-0.5um_optical_depth_550nm",
    "sea_salt_aerosol_0.5-5um_optical_depth_550nm",
    "sea_salt_aerosol_5-20um_optical_depth_550nm",
    "single_scattering_albedo_340nm",
    "single_scattering_albedo_355nm",
    "single_scattering_albedo_380nm",
    "single_scattering_albedo_400nm",
    "single_scattering_albedo_440nm",
    "single_scattering_albedo_469nm",
    "single_scattering_albedo_500nm",
    "single_scattering_albedo_532nm",
    "single_scattering_albedo_550nm",
    "single_scattering_albedo_645nm",
    "single_scattering_albedo_670nm",
    "single_scattering_albedo_800nm",
    "single_scattering_albedo_858nm",
    "single_scattering_albedo_865nm",
    "single_scattering_albedo_1020nm",
    "single_scattering_albedo_1064nm",
    "single_scattering_albedo_1240nm",
    "single_scattering_albedo_1640nm",
    "single_scattering_albedo_2130nm",
    "total_absorption_aerosol_optical_depth_340nm",
    "total_absorption_aerosol_optical_depth_355nm",
    "total_absorption_aerosol_optical_depth_380nm",
    "total_absorption_aerosol_optical_depth_400nm",
    "total_absorption_aerosol_optical_depth_440nm",
    "total_absorption_aerosol_optical_depth_469nm",
    "total_absorption_aerosol_optical_depth_500nm",
    "total_absorption_aerosol_optical_depth_532nm",
    "total_absorption_aerosol_optical_depth_550nm",
    "total_absorption_aerosol_optical_depth_645nm",
    "total_absorption_aerosol_optical_depth_670nm",
    "total_absorption_aerosol_optical_depth_800nm",
    "total_absorption_aerosol_optical_depth_858nm",
    "total_absorption_aerosol_optical_depth_865nm",
    "total_absorption_aerosol_optical_depth_1020nm",
    "total_absorption_aerosol_optical_depth_1064nm",
    "total_absorption_aerosol_optical_depth_1240nm",
    "total_absorption_aerosol_optical_depth_1640nm",
    "total_absorption_aerosol_optical_depth_2130nm",
    "total_aerosol_optical_depth_340nm",
    "total_aerosol_optical_depth_355nm",
    "total_aerosol_optical_depth_380nm",
    "total_aerosol_optical_depth_400nm",
    "total_aerosol_optical_depth_440nm",
    "total_aerosol_optical_depth_500nm",
    "total_aerosol_optical_depth_532nm",
    "total_aerosol_optical_depth_645nm",
    "total_aerosol_optical_depth_800nm",
    "total_aerosol_optical_depth_858nm",
    "total_aerosol_optical_depth_1020nm",
    "total_aerosol_optical_depth_1064nm",
    "total_aerosol_optical_depth_1640nm",
    "total_aerosol_optical_depth_2130nm",
    "total_fine_mode_aerosol_optical_depth_340nm",
    "total_fine_mode_aerosol_optical_depth_355nm",
    "total_fine_mode_aerosol_optical_depth_380nm",
    "total_fine_mode_aerosol_optical_depth_400nm",
    "total_fine_mode_aerosol_optical_depth_440nm",
    "total_fine_mode_aerosol_optical_depth_469nm",
    "total_fine_mode_aerosol_optical_depth_500nm",
    "total_fine_mode_aerosol_optical_depth_532nm",
    "total_fine_mode_aerosol_optical_depth_550nm",
    "total_fine_mode_aerosol_optical_depth_645nm",
    "total_fine_mode_aerosol_optical_depth_670nm",
    "total_fine_mode_aerosol_optical_depth_800nm",
    "total_fine_mode_aerosol_optical_depth_858nm",
    "total_fine_mode_aerosol_optical_depth_865nm",
    "total_fine_mode_aerosol_optical_depth_1020nm",
    "total_fine_mode_aerosol_optical_depth_1064nm",
    "total_fine_mode_aerosol_optical_depth_1240nm",
    "total_fine_mode_aerosol_optical_depth_1640nm",
    "total_fine_mode_aerosol_optical_depth_2130nm",
    "aerosol_extinction_coefficient_355nm",
    "aerosol_extinction_coefficient_532nm",
    "aerosol_extinction_coefficient_1064nm",
    "attenuated_backscatter_due_to_aerosol_355nm_from_ground",
    "attenuated_backscatter_due_to_aerosol_532nm_from_ground",
    "attenuated_backscatter_due_to_aerosol_1064nm_from_ground",
    "attenuated_backscatter_due_to_aerosol_355nm_from_top_of_atmosphere",
    "attenuated_backscatter_due_to_aerosol_532nm_from_top_of_atmosphere",
    "attenuated_backscatter_due_to_aerosol_1064nm_from_top_of_atmosphere",
    "total_sky_direct_solar_radiation_at_surface",
    "downward_uv_radiation_at_the_surface",
    "direct_solar_radiation",
    "surface_net_solar_radiation",
    "surface_net_thermal_radiation",
)

CAMS_GLOBAL_AOD_VARIABLES: tuple[str, ...] = (
    "total_aerosol_optical_depth_469nm",
    "total_aerosol_optical_depth_550nm",
    "total_aerosol_optical_depth_670nm",
    "total_aerosol_optical_depth_865nm",
    "total_aerosol_optical_depth_1240nm",
    "total_aerosol_optical_depth_340nm",
    "total_aerosol_optical_depth_355nm",
    "total_aerosol_optical_depth_380nm",
    "total_aerosol_optical_depth_400nm",
    "total_aerosol_optical_depth_440nm",
    "total_aerosol_optical_depth_500nm",
    "total_aerosol_optical_depth_532nm",
    "total_aerosol_optical_depth_645nm",
    "total_aerosol_optical_depth_800nm",
    "total_aerosol_optical_depth_858nm",
    "total_aerosol_optical_depth_1020nm",
    "total_aerosol_optical_depth_1064nm",
    "total_aerosol_optical_depth_1640nm",
    "total_aerosol_optical_depth_2130nm",
    "direct_solar_radiation",
    "downward_uv_radiation_at_the_surface",
    "surface_net_solar_radiation",
    "surface_net_thermal_radiation",
    "surface_solar_radiation_downwards",
    "surface_thermal_radiation_downwards",
    "toa_incident_solar_radiation",
    "total_sky_direct_solar_radiation_at_surface",
)

CAMS_EUROPE_VARIABLES: tuple[str, ...] = (
    "alder_pollen",
    "ammonia",
    "birch_pollen",
    "carbon_monoxide",
    "dust",
    "grass_pollen",
    "nitrogen_dioxide",
    "nitrogen_monoxide",
    "non_methane_vocs",
    "olive_pollen",
    "ozone",
    "particulate_matter_10um",
    "particulate_matter_2.5um",
    "peroxyacyl_nitrates",
    "pm10_wildfires",
    "ragweed_pollen",
    "secondary_inorganic_aerosol",
    "sulphur_dioxide",
)


# --------------------------------------------------------------------------------------
# Credentials
# --------------------------------------------------------------------------------------


def cdsapi_credentials(config: Config | None = None) -> tuple[str, str] | None:
    """Return an explicit ``(url, key)`` pair for the ADS, or None to fall back to rc file.

    ``CDSAPI_KEY`` alone is enough; the URL defaults to the Atmosphere Data Store because
    that is where every dataset in this module lives.
    """
    cfg = config if config is not None else get_config()
    key = cfg.credentials.cdsapi_key
    if not key:
        return None
    return cfg.credentials.cdsapi_url or DEFAULT_ADS_URL, key


def _cdsapirc_exists() -> bool:
    """True when the ``cdsapi`` client would find a usable rc file on its own."""
    rc = os.environ.get("CDSAPI_RC")
    if rc:
        return pathlib.Path(rc).expanduser().is_file()
    return (pathlib.Path.home() / ".cdsapirc").is_file()


def cds_client(config: Config | None = None, quiet: bool = False, **kwargs: Any):
    """Build a ``cdsapi.Client`` for the Atmosphere Data Store.

    Prefers ``CDSAPI_URL``/``CDSAPI_KEY`` from the configuration, then ``~/.cdsapirc``.
    Raises :class:`~planetary_datasets.config.MissingCredential` when neither is present,
    so a misconfigured job fails before it queues a multi-hour request.
    """
    import cdsapi

    explicit = cdsapi_credentials(config)
    if explicit is not None:
        url, key = explicit
        return cdsapi.Client(url=url, key=key, quiet=quiet, **kwargs)

    if not _cdsapirc_exists():
        raise MissingCredential(
            "CAMS needs Copernicus ADS credentials. Set CDSAPI_KEY (and optionally "
            "CDSAPI_URL) in .env or the environment, or write a ~/.cdsapirc file. "
            f"The ADS endpoint is {DEFAULT_ADS_URL}."
        )
    return cdsapi.Client(quiet=quiet, **kwargs)


# --------------------------------------------------------------------------------------
# Zip / NetCDF handling
# --------------------------------------------------------------------------------------


def extract_zip(input_zip: str | os.PathLike) -> dict[str, bytes]:
    """Read every member of a zip archive into memory, keyed by member name.

    Kept for the small bundles where reading the bytes is cheaper than touching the disk.
    Use :func:`extract_zip_to_dir` for anything that might be large.
    """
    with ZipFile(input_zip) as archive:
        return {name: archive.read(name) for name in archive.namelist()}


def extract_zip_to_dir(
    input_zip: str | os.PathLike,
    dest_dir: str | os.PathLike,
) -> list[pathlib.Path]:
    """Extract a zip archive flat into ``dest_dir`` and return the written paths.

    Members are written under their base name prefixed with the archive's stem, so the
    ``data_sfc.nc`` of one download never overwrites the ``data_sfc.nc`` of another and no
    member path can escape ``dest_dir``.
    """
    input_zip = pathlib.Path(input_zip)
    dest_dir = pathlib.Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)

    written: list[pathlib.Path] = []
    stem = input_zip.name.split(".")[0]
    with ZipFile(input_zip) as archive:
        for member in archive.namelist():
            name = pathlib.PurePosixPath(member).name
            if not name or member.endswith("/"):
                continue
            target = dest_dir / f"{stem}__{name}"
            with archive.open(member) as src, open(target, "wb") as out:
                shutil.copyfileobj(src, out)
            written.append(target)
    return sorted(written)


def slugify_long_name(long_name: str) -> str:
    """Turn a CAMS ``long_name`` attribute into a variable name.

    Reproduces the naming the existing stores were written with: spaces and hyphens become
    underscores, parentheses are dropped, and the triple underscore left behind by a
    hyphenated range such as ``0.03 - 0.55 um`` becomes ``_to_``.
    """
    slug = long_name.replace(" ", "_").replace("-", "_").lower()
    slug = slug.replace("(", "").replace(")", "")
    return slug.replace("___", "_to_")


def _unique_renames(ds: xr.Dataset, proposed: dict[str, str]) -> dict[str, str]:
    """Drop renames that would collide, keeping the original name for the loser.

    ``Dataset.rename`` raises on duplicate targets. Two CAMS variables occasionally share a
    ``long_name`` across level types, and losing the whole partition over a name clash is
    worse than keeping one of them under its short name.
    """
    taken: set[str] = set()
    final: dict[str, str] = {}
    for var, new in proposed.items():
        if new in taken or (new != var and new in ds.variables):
            logger.warning(f"name {new!r} already taken, keeping {var!r} as-is")
            taken.add(str(var))
            continue
        taken.add(new)
        final[var] = new
    return {k: v for k, v in final.items() if k != v}


def _rename_by_long_name(ds: xr.Dataset) -> xr.Dataset:
    proposed = {
        var: slugify_long_name(str(ds[var].attrs.get("long_name", var))) for var in ds.data_vars
    }
    return ds.rename(_unique_renames(ds, proposed))


def _drop_if_not_dim(ds: xr.Dataset, names: Iterable[str]) -> xr.Dataset:
    """Drop bookkeeping coordinates, but never one that is also a dimension."""
    doomed = [n for n in names if n in ds.variables and n not in ds.dims]
    return ds.drop_vars(doomed) if doomed else ds


def preproc_standard_name(ds: xr.Dataset) -> xr.Dataset:
    """Rename data variables to their ``standard_name`` attribute where they have one."""
    proposed = {var: str(ds[var].attrs.get("standard_name", var)) for var in ds.data_vars}
    return ds.rename(_unique_renames(ds, proposed))


def preproc_cams(ds: xr.Dataset) -> xr.Dataset:
    """Normalise a single-lead CAMS NetCDF onto a ``time`` axis of valid times.

    ADS files carry ``forecast_reference_time`` (the run) and ``forecast_period`` (the
    lead) rather than a valid time. For a request of one lead hour they collapse cleanly
    into ``time = run + lead``, which is what lets hours from the 00Z and 12Z runs sit on
    one axis. Use :func:`preproc_cams_forecast` for bundles holding many lead times.
    """
    ds = _rename_by_long_name(ds)

    if "forecast_reference_time" in ds.variables:
        if "forecast_period" in ds.variables:
            if ds["forecast_period"].size != 1:
                raise ValueError(
                    "preproc_cams needs a single lead time; this file has "
                    f"{ds['forecast_period'].size}. Use preproc_cams_forecast instead."
                )
            lead = ds["forecast_period"].values.reshape(()).astype("timedelta64[h]")
            ds = ds.assign_coords(
                forecast_reference_time=ds["forecast_reference_time"].values + lead
            )
            if "forecast_period" in ds.dims:
                ds = ds.squeeze("forecast_period")
            ds = ds.drop_vars("forecast_period")
        ds = ds.rename({"forecast_reference_time": "time"})

    ds = _drop_if_not_dim(ds, ("valid_time", "expver"))

    if "time" in ds.coords and "time" not in ds.dims:
        ds = ds.expand_dims("time")
    return ds


def preproc_cams_forecast(ds: xr.Dataset) -> xr.Dataset:
    """Normalise a multi-lead CAMS NetCDF onto ``init_time`` and ``step``.

    A week of forecasts has both a run axis and a lead axis. Folding them into one valid
    time would mean stacking two dimensions, which is expensive and produces duplicate
    valid times across overlapping runs. Keeping them separate is the usual NWP archive
    layout and appends cheaply along ``init_time``.
    """
    ds = _rename_by_long_name(ds)

    renames = {}
    if "forecast_reference_time" in ds.variables:
        renames["forecast_reference_time"] = "init_time"
    if "forecast_period" in ds.variables:
        renames["forecast_period"] = "step"
    if renames:
        ds = ds.rename(renames)

    # ``valid_time`` is init_time + step, so it is redundant here, and as a 2-D coordinate
    # it only makes the append harder.
    ds = _drop_if_not_dim(ds, ("valid_time", "expver"))

    if "init_time" in ds.coords and "init_time" not in ds.dims:
        ds = ds.expand_dims("init_time")
    return ds


def open_cams_netcdfs(
    sources: Sequence[Any],
    chunks: dict | None = None,
    preproc=preproc_cams,
) -> xr.Dataset:
    """Open and merge already-extracted CAMS NetCDF files or byte buffers."""
    if not sources:
        raise ValueError("no CAMS NetCDF sources to open")
    datasets = [preproc(xr.open_dataset(src, chunks=chunks)) for src in sources]
    if len(datasets) == 1:
        return datasets[0]
    return xr.merge(datasets, compat="no_conflicts")


def open_cams_zip(
    path: str | os.PathLike,
    temp_dir: str | os.PathLike | None = None,
    chunks: dict | None = None,
    preproc=preproc_cams,
) -> xr.Dataset:
    """Open a ``.nc.zip`` bundle downloaded from the ADS as one dataset.

    A bundle holds one NetCDF per level type — typically ``data_plev.nc`` and
    ``data_sfc.nc`` — which are merged. When ``temp_dir`` is given the members are written
    there first, which keeps memory flat for the multi-gigabyte weekly bundles; otherwise
    they are read into memory.
    """
    path = pathlib.Path(path)
    if temp_dir is not None:
        members = [p for p in extract_zip_to_dir(path, temp_dir) if p.suffix == ".nc"]
        if not members:
            raise ValueError(f"{path} contains no .nc members")
        return open_cams_netcdfs(members, chunks=chunks, preproc=preproc)

    raw = extract_zip(path)
    buffers = [BytesIO(data) for name, data in sorted(raw.items()) if name.endswith(".nc")]
    if not buffers:
        raise ValueError(f"{path} contains no .nc members")
    return open_cams_netcdfs(buffers, chunks=chunks, preproc=preproc)


def finalise(ds: xr.Dataset, append_dim: str = "time", float16: bool = False) -> xr.Dataset:
    """Shape a merged CAMS dataset for writing: precision, coordinate order, chunking.

    ``float16`` is off by default. The script this replaced cast the whole composition
    dataset to float16 to save space, but float16's smallest subnormal is about 6e-8 and
    several CAMS fields — total columns of hydroxyl radical, the dust mixing ratios — live
    around 1e-9, so the cast silently flushed them to zero. zstd with bitshuffle recovers
    most of the space without the loss. Pass ``float16=True`` to reproduce the old dtype.
    """
    if float16:
        ds = ds.astype(np.float16)
    ds = make_lat_lon_coords_consistent(ds)
    # Pressure levels descend towards the surface; the European ``level`` axis is metres
    # above ground and ascends.
    ds = sort_vertical_coords(ds, level_name="pressure_level", height_name="level")
    chunking = {append_dim: 1}
    for dim in ("latitude", "longitude", "pressure_level", "level", "step"):
        chunking[dim] = -1
    return ds.chunk({k: v for k, v in chunking.items() if k in ds.dims})


# --------------------------------------------------------------------------------------
# Providers
# --------------------------------------------------------------------------------------


class _CAMSProvider(BaseProvider):
    """Shared plumbing for the ADS-backed CAMS providers."""

    dataset_id: str
    append_dim = "time"
    #: Subdirectory of ``data_dir`` the raw ``.nc.zip`` downloads are kept in.
    archive_subdir: str
    #: Cast data variables to float16 before writing. See :func:`finalise` for why this is
    #: off despite the original scripts doing it.
    float16: bool = False

    def __init__(
        self,
        config: Config | None = None,
        archive_dir: str | os.PathLike | None = None,
        variables: Iterable[str] | None = None,
    ):
        super().__init__(config=config)
        self._archive_dir = pathlib.Path(archive_dir) if archive_dir is not None else None
        self._variables = tuple(variables) if variables is not None else None

    @property
    def archive_dir(self) -> pathlib.Path:
        """Where raw downloads live. Defaults under the configured data directory."""
        if self._archive_dir is not None:
            return self._archive_dir
        return self.config.data_dir / "cams" / self.archive_subdir

    @property
    def variables(self) -> tuple[str, ...]:
        raise NotImplementedError

    def _retrieve(self, request: dict[str, Any], dst: pathlib.Path) -> pathlib.Path | None:
        """Run one ADS retrieval into ``dst``, atomically and skipping existing files."""
        dst.parent.mkdir(parents=True, exist_ok=True)
        if dst.is_file() and dst.stat().st_size > 0:
            logger.debug(f"{self.name}: {dst.name} already downloaded")
            return dst

        part = dst.with_name(dst.name + ".part")
        part.unlink(missing_ok=True)
        client = cds_client(self.config)
        logger.info(f"{self.name}: requesting {dst.name} from the ADS")
        try:
            client.retrieve(self.dataset_id, request).download(target=str(part))
        except MissingCredential:
            raise
        except Exception as exc:  # noqa: BLE001 - archive gaps and ADS outages are routine
            part.unlink(missing_ok=True)
            logger.warning(f"{self.name}: retrieval of {dst.name} failed: {exc}")
            return None

        if not part.is_file() or part.stat().st_size == 0:
            part.unlink(missing_ok=True)
            logger.warning(f"{self.name}: retrieval of {dst.name} produced no data")
            return None
        # Renamed only once complete, so an interrupted run leaves nothing that the
        # skip-if-present check above would mistake for a finished download.
        os.replace(part, dst)
        return dst


class CAMSGlobalCompositionProvider(_CAMSProvider):
    """Global CAMS composition forecast, one partition per valid hour.

    The ADS publishes a 00Z and a 12Z run. A partition at hour ``H`` is served by the run
    at 00Z when ``H < 12`` and by the 12Z run otherwise, at lead ``H - run_hour``, so
    consecutive hours come from the freshest available run.
    """

    name = "cams-global-composition"
    dataset_id = GLOBAL_COMPOSITION_DATASET
    store_prefix = "bkr/cams/cams_analysis_and_forecast.icechunk"
    archive_subdir = "composition"

    #: Largest lead hour taken from a run before switching to the next one. At the default
    #: of 11 every hour of the day is covered; lower it to stay closer to analysis time and
    #: :meth:`fetch` will skip the hours no longer served.
    max_leadtime_hour: int = 11

    def __init__(
        self,
        config: Config | None = None,
        archive_dir: str | os.PathLike | None = None,
        variables: Iterable[str] | None = None,
        pressure_levels: Iterable[str] | None = None,
    ):
        super().__init__(config=config, archive_dir=archive_dir, variables=variables)
        self._pressure_levels = (
            tuple(pressure_levels) if pressure_levels is not None else CAMS_PRESSURE_LEVELS
        )

    @property
    def variables(self) -> tuple[str, ...]:
        return self._variables or CAMS_GLOBAL_COMPOSITION_VARIABLES

    @property
    def pressure_levels(self) -> tuple[str, ...]:
        return self._pressure_levels

    @staticmethod
    def run_and_leadtime(it: pd.Timestamp) -> tuple[pd.Timestamp, int]:
        """Split a valid time into the run that produces it and the lead hour."""
        it = pd.Timestamp(it)
        run = it.normalize() + pd.Timedelta(hours=0 if it.hour < 12 else 12)
        return run, int((it - run) // pd.Timedelta(hours=1))

    def target_path(self, it: pd.Timestamp) -> pathlib.Path:
        return self.archive_dir / f"cams_composition_{pd.Timestamp(it):%Y%m%d_%H%M}.nc.zip"

    def build_request(self, it: pd.Timestamp) -> dict[str, Any]:
        run, lead = self.run_and_leadtime(it)
        return {
            "variable": list(self.variables),
            "pressure_level": list(self.pressure_levels),
            "date": [f"{run:%Y-%m-%d}/{run:%Y-%m-%d}"],
            "time": [f"{run:%H:%M}"],
            "leadtime_hour": [str(lead)],
            "type": ["forecast"],
            "data_format": "netcdf_zip",
        }

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        _, lead = self.run_and_leadtime(it)
        if lead > self.max_leadtime_hour:
            logger.debug(f"{self.name}: lead {lead}h for {it} is beyond the archive, skipping")
            return []
        dst = self._retrieve(self.build_request(it), self.target_path(it))
        return [str(dst)] if dst is not None else []

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        datasets = [open_cams_zip(f, temp_dir=temp_dir) for f in input_files]
        ds = datasets[0] if len(datasets) == 1 else xr.merge(datasets, compat="no_conflicts")
        return finalise(ds, append_dim=self.append_dim, float16=self.float16)


class _CAMSWeeklyProvider(_CAMSProvider):
    """A CAMS dataset retrieved one week and one variable at a time.

    The ADS rejects a week of every variable in a single request, so each variable is a
    separate retrieval and the bundles are merged at processing time. Partitions are keyed
    by the first day of the week.
    """

    #: A week of global forecasts does not fit in memory, but it is never held there: the
    #: dataset stays dask-backed and ``to_icechunk`` streams it chunk by chunk. The base
    #: class's up-front ``nbytes`` check would reject it on size alone, so it is skipped.
    guard_memory = False

    #: Runs and lead times stay on separate axes, so partitions append along the run.
    append_dim = "init_time"

    #: Number of days covered by one partition.
    window_days: int = 7

    def window(self, it: pd.Timestamp) -> tuple[pd.Timestamp, pd.Timestamp]:
        """Half-open ``[start, end)`` partition window, matching Dagster's time window."""
        start = pd.Timestamp(it)
        return start, start + pd.Timedelta(days=self.window_days)

    def request_window(self, it: pd.Timestamp) -> tuple[pd.Timestamp, pd.Timestamp]:
        """Closed ``[start, end]`` window for the ADS ``date`` range.

        The ADS treats ``"a/b"`` as inclusive of both ends. Passing the exclusive window
        end would fetch an extra day that the next partition fetches again, and both would
        append the same runs to the store.
        """
        start, end = self.window(it)
        return start, end - pd.Timedelta(days=1)

    def target_path(self, it: pd.Timestamp, variable: str) -> pathlib.Path:
        start, end = self.window(it)
        return self.archive_dir / f"{start:%Y%m%d}-{end:%Y%m%d}_{variable}.nc.zip"

    def build_request(self, it: pd.Timestamp, variable: str) -> dict[str, Any]:
        raise NotImplementedError

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        stored: list[str] = []
        for variable in self.variables:
            dst = self._retrieve(self.build_request(it, variable), self.target_path(it, variable))
            if dst is not None:
                stored.append(str(dst))
        if not stored:
            logger.warning(f"{self.name}: no files retrieved for {it}")
        return stored

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        # ``chunks={}`` keeps every variable dask-backed so a week is never materialised.
        datasets = [
            open_cams_zip(f, temp_dir=temp_dir, chunks={}, preproc=preproc_cams_forecast)
            for f in input_files
        ]
        if not datasets:
            raise ValueError(f"{self.name}: nothing to process for {it}")
        ds = datasets[0] if len(datasets) == 1 else xr.merge(datasets, compat="no_conflicts")
        return finalise(ds, append_dim=self.append_dim, float16=self.float16)


class CAMSGlobalAODProvider(_CAMSWeeklyProvider):
    """Global CAMS aerosol optical depth and radiation, a week at a time out to 120 hours."""

    name = "cams-global-aod"
    dataset_id = GLOBAL_COMPOSITION_DATASET
    store_prefix = "bkr/cams/cams_global_aod.icechunk"
    archive_subdir = "global-aod"

    max_leadtime_hour: int = 120

    @property
    def variables(self) -> tuple[str, ...]:
        return self._variables or CAMS_GLOBAL_AOD_VARIABLES

    def build_request(self, it: pd.Timestamp, variable: str) -> dict[str, Any]:
        start, end = self.request_window(it)
        return {
            "date": [f"{start:%Y-%m-%d}/{end:%Y-%m-%d}"],
            "type": ["forecast"],
            "time": ["00:00", "12:00"],
            "leadtime_hour": [str(i) for i in range(self.max_leadtime_hour + 1)],
            "data_format": ["netcdf_zip"],
            "variable": [variable],
        }


class CAMSEuropeAirQualityProvider(_CAMSWeeklyProvider):
    """CAMS European air quality ensemble forecast, a week at a time out to 96 hours."""

    name = "cams-europe-air-quality"
    dataset_id = EUROPE_AIR_QUALITY_DATASET
    store_prefix = "bkr/cams/cams_europe_air_quality.icechunk"
    archive_subdir = "europe-air-quality"

    max_leadtime_hour: int = 96

    def __init__(
        self,
        config: Config | None = None,
        archive_dir: str | os.PathLike | None = None,
        variables: Iterable[str] | None = None,
        levels: Iterable[str] | None = None,
    ):
        super().__init__(config=config, archive_dir=archive_dir, variables=variables)
        self._levels = tuple(levels) if levels is not None else CAMS_EUROPE_LEVELS

    @property
    def variables(self) -> tuple[str, ...]:
        return self._variables or CAMS_EUROPE_VARIABLES

    @property
    def levels(self) -> tuple[str, ...]:
        return self._levels

    def build_request(self, it: pd.Timestamp, variable: str) -> dict[str, Any]:
        start, end = self.request_window(it)
        return {
            "date": [f"{start:%Y-%m-%d}/{end:%Y-%m-%d}"],
            "type": ["forecast"],
            "time": ["00:00"],
            "model": ["ensemble"],
            "leadtime_hour": [str(i) for i in range(self.max_leadtime_hour + 1)],
            "data_format": ["netcdf_zip"],
            "level": list(self.levels),
            "variable": [variable],
        }


__all__ = [
    "CAMSEuropeAirQualityProvider",
    "CAMSGlobalAODProvider",
    "CAMSGlobalCompositionProvider",
    "CAMS_EUROPE_LEVELS",
    "CAMS_EUROPE_VARIABLES",
    "CAMS_GLOBAL_AOD_VARIABLES",
    "CAMS_GLOBAL_COMPOSITION_VARIABLES",
    "CAMS_PRESSURE_LEVELS",
    "DEFAULT_ADS_URL",
    "cds_client",
    "cdsapi_credentials",
    "extract_zip",
    "extract_zip_to_dir",
    "finalise",
    "open_cams_netcdfs",
    "open_cams_zip",
    "preproc_cams",
    "preproc_cams_forecast",
    "preproc_standard_name",
    "slugify_long_name",
]
