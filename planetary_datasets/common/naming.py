"""Human-readable variable names, the same across every store.

The convention is ARCO-ERA5's: a variable is named for what it is, in lower snake case,
with the height of a single-level field leading (``2m_temperature``,
``10m_u_component_of_wind``, ``mean_sea_level_pressure``) and fields on vertical levels
named for the quantity alone (``temperature``, ``specific_humidity``), their levels being
a dimension. ARCO-ERA5's names are ECMWF's parameter names in snake case, so a GRIB short
name is named through ecCodes' own parameter tables (WMO, ECMWF and DWD local), and the
same quantity gets the same name from any model.

:func:`propose` names one variable from what its store knows about it: GRIB attributes,
a CF ``standard_name``, a ``long_name``, or failing those its current name. The names of
every existing store are fixed once, in ``planetary_datasets/naming/store_names.json``
(written by ``scripts/generate_variable_names.py`` and reviewed), and both the renaming
of the stores and every later write go through :func:`store_names`, so a store's names
never drift from the ones it was renamed to.
"""

from __future__ import annotations

import functools
import json
import os
import pathlib
import re
from typing import Mapping

#: Where each store's fixed renaming lives.
STORE_NAMES = pathlib.Path(__file__).resolve().parents[1] / "naming" / "store_names.json"
#: Attribute recording a renamed variable's name in its source.
SOURCE_NAME_ATTR = "source_name"

#: Names that are coordinates or bookkeeping, never renamed.
COORDINATE_NAMES = {
    "time",
    "valid_time",
    "init_time",
    "step",
    "forecast_period",
    "forecast_reference_time",
    "latitude",
    "longitude",
    "lat",
    "lon",
    "x",
    "y",
    "level",
    "levels",
    "pressure_level",
    "model_level",
    "model_level_half",
    "hybrid",
    "hybrid_half",
    "height",
    "depth",
    "station",
    "station_id",
    "station_name",
    "values",
    "number",
    "member",
    "realization",
    "crs",
    "spatial_ref",
    "fixedgrid_projection",
    "goes_imager_projection",
    "projection",
    "t",
    "band",
    "band_id",
    "band_wavelength",
    "clat",
    "clon",
    "clat_bnds",
    "clon_bnds",
}
#: Grid-mapping variables, recognised by name.
GRID_MAPPING = re.compile(
    r"(lambert|latitude_longitude|rotated_pole|polar_stereographic|_projection$|^projection_|^grid_mapping)"
)

#: CF standard names with an ERA5 equivalent of the same meaning and units.
CF_TO_ERA5 = {
    "air_temperature": "temperature",
    "dew_point_temperature": "dewpoint_temperature",
    "x_wind": "u_component_of_wind",
    "eastward_wind": "u_component_of_wind",
    "y_wind": "v_component_of_wind",
    "northward_wind": "v_component_of_wind",
    "upward_air_velocity": "vertical_velocity",
    "relative_humidity": "relative_humidity",
    "specific_humidity": "specific_humidity",
    "air_pressure_at_sea_level": "mean_sea_level_pressure",
    "air_pressure_at_mean_sea_level": "mean_sea_level_pressure",
    "surface_air_pressure": "surface_pressure",
    "geopotential": "geopotential",
    "geopotential_height": "geopotential_height",
    "cloud_area_fraction": "total_cloud_cover",
    "low_type_cloud_area_fraction": "low_cloud_cover",
    "medium_type_cloud_area_fraction": "medium_cloud_cover",
    "high_type_cloud_area_fraction": "high_cloud_cover",
    "sea_surface_temperature": "sea_surface_temperature",
    "sea_ice_area_fraction": "sea_ice_cover",
    "land_binary_mask": "land_sea_mask",
    "atmosphere_boundary_layer_thickness": "boundary_layer_height",
    "visibility_in_air": "visibility",
    "wind_speed": "wind_speed",
    "wind_from_direction": "wind_direction",
    "wind_speed_of_gust": "wind_gust",
    "sea_surface_wave_significant_height": "significant_height_of_combined_wind_waves_and_swell",
    "sea_surface_wave_from_direction": "mean_wave_direction",
}

CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")
#: Long names that say nothing about the variable.
UNINFORMATIVE = {"", "unknown", "none", "nan", "undefined", "missing", "data", "value", "values"}


def snake(text: str) -> str:
    """Lower snake case: "Total Cloud Cover (0-1)" -> "total_cloud_cover_0_1"."""
    text = str(text).strip()
    # Unit spellings the camel-case split would break apart, and DWD's "(m)" for a mean.
    text = re.sub(r"hPa", "hpa", text).replace("(m)", " mean ")
    text = CAMEL.sub("_", text)
    text = re.sub(r"[^0-9A-Za-z]+", "_", text).strip("_").lower()
    return re.sub(r"_+", "_", text)


#: ecCodes wording -> ARCO-ERA5 wording, applied to every proposed name.
ERA5_WORDING = (
    (r"(^|_)u_wind_component($|_)", r"\1u_component_of_wind\2"),
    (r"(^|_)v_wind_component($|_)", r"\1v_component_of_wind\2"),
    (r"(^|_)u_component_of_wind_component($|_)", r"\1u_component_of_wind\2"),
    (r"(^|_)dew_point_temperature($|_)", r"\1dewpoint_temperature\2"),
    (r"^pressure_reduced_to_msl$", "mean_sea_level_pressure"),
    (r"^mean_sea_level$", "mean_sea_level_pressure"),
)


def era5ify(name: str) -> str:
    """Snake case with a leading height written ERA5's way (``2m_``, ``10m_``, ``1_5m_``)."""
    # Bracketed ranges and units ("(0.3 - 5.0 m-6)", "(0 - 1)") are noise in a name.
    name = re.sub(r"\((?!m\))[^)]*\)", " ", str(name))
    s = snake(name)
    s = re.sub(r"_(mean_over_forecast_time|time_average|time_mean)$", "_mean", s)
    s = re.sub(r"^(\d+)_(\d+)_(?:metre|meter|metres|meters|m)_", r"\1_\2m_", s)
    s = re.sub(r"^(\d+)_(?:metre|meter|metres|meters|m)_", r"\1m_", s)
    s = re.sub(r"_(\d+)_(?:metre|meter|metres|meters)_", r"_\1m_", s)
    s = re.sub(r"^time_(maximum|minimum|mean)_", r"\1_", s)
    for pattern, replacement in ERA5_WORDING:
        s = re.sub(pattern, replacement, s)
    return s


def informative(text: object) -> bool:
    """A long name worth naming a variable after."""
    s = snake(str(text)) if text is not None else ""
    return s not in UNINFORMATIVE and len(s) > 3 and not s.isdigit()


@functools.lru_cache(maxsize=1)
def eccodes_names() -> dict[str, str]:
    """GRIB short name -> parameter name, from ecCodes' WMO, ECMWF and DWD tables.

    shortName.def and name.def list the same parameters in the same order, each as a
    quoted key over a block of GRIB keys; blocks are paired by their contents.
    """
    try:
        import eccodes

        root = pathlib.Path(eccodes.codes_definition_path())
    except Exception:  # noqa: BLE001 - names fall back to what the stores carry
        return {}
    pattern = re.compile(r"^'([^']+)'\s*=\s*\{(.*?)\}", re.M | re.S)
    out: dict[str, str] = {}
    # Local tables first, so the WMO name wins where both define a short name.
    for sub in (
        "grib2/localConcepts/edzw",
        "grib1/localConcepts/ecmf",
        "grib2/localConcepts/ecmf",
        "grib2",
    ):
        short = root / sub / "shortName.def"
        name = root / sub / "name.def"
        if not (short.is_file() and name.is_file()):
            continue
        names = {
            re.sub(r"\s+", " ", body): key
            for key, body in pattern.findall(name.read_text(errors="replace"))
        }
        for key, body in pattern.findall(short.read_text(errors="replace")):
            long = names.get(re.sub(r"\s+", " ", body))
            if long:
                # DWD's short names are upper case in the tables, lower case in files.
                out[key] = long
                out.setdefault(key.lower(), long)
    return out


def strip_level_suffix(name: str) -> tuple[str, str]:
    """``t_ml`` -> (``t``, ``model_level``): the level-type suffixes stores append."""
    for suffix, label in (("_ml", "model_level"), ("_pl", "pressure_level"), ("_sfc", "surface")):
        if name.endswith(suffix):
            return name[: -len(suffix)], label
    return name, ""


#: Satellite imager channels: name -> (channel number, quantity, central wavelength in um).
_REF, _BT = "reflectance", "brightness_temperature"
_ABI = (0.47, 0.64, 0.86, 1.37, 1.6, 2.2, 3.9, 6.2, 6.9, 7.3, 8.4, 9.6, 10.3, 11.2, 12.3, 13.3)
_AHI = (0.47, 0.51, 0.64, 0.86, 1.6, 2.3, 3.9, 6.2, 6.9, 7.3, 8.6, 9.6, 10.4, 11.2, 12.4, 13.3)
_AMI = (
    "VI004",
    "VI005",
    "VI006",
    "VI008",
    "NR013",
    "NR016",
    "SW038",
    "WV063",
    "WV069",
    "WV073",
    "IR087",
    "IR096",
    "IR105",
    "IR112",
    "IR123",
    "IR133",
)
_AMI_UM = (0.47, 0.51, 0.64, 0.86, 1.37, 1.6, 3.8, 6.3, 6.9, 7.3, 8.7, 9.6, 10.5, 11.2, 12.3, 13.3)
_SEVIRI = (
    "VIS006",
    "VIS008",
    "IR_016",
    "IR_039",
    "WV_062",
    "WV_073",
    "IR_087",
    "IR_097",
    "IR_108",
    "IR_120",
    "IR_134",
    "HRV",
)
_SEVIRI_UM = (0.6, 0.8, 1.6, 3.9, 6.2, 7.3, 8.7, 9.7, 10.8, 12.0, 13.4, 0.7)
_FCI = (
    "vis_04",
    "vis_05",
    "vis_06",
    "vis_08",
    "vis_09",
    "nir_13",
    "nir_16",
    "nir_22",
    "ir_38",
    "wv_63",
    "wv_73",
    "ir_87",
    "ir_97",
    "ir_105",
    "ir_123",
    "ir_133",
)
_FCI_UM = (
    0.44,
    0.51,
    0.64,
    0.86,
    0.91,
    1.38,
    1.61,
    2.25,
    3.8,
    6.3,
    7.35,
    8.7,
    9.66,
    10.5,
    12.3,
    13.3,
)
CHANNELS: dict[str, tuple[str, str, float]] = {
    **{f"C{n:02d}": (f"{n:02d}", _REF if n <= 6 else _BT, w) for n, w in enumerate(_ABI, 1)},
    **{f"abi{n:02d}c": (f"{n:02d}", _REF if n <= 6 else _BT, w) for n, w in enumerate(_ABI, 1)},
    **{f"B{n:02d}": (f"{n:02d}", _REF if n <= 6 else _BT, w) for n, w in enumerate(_AHI, 1)},
    **{
        b: (f"{n:02d}", _REF if n <= 6 else _BT, w)
        for n, (b, w) in enumerate(zip(_AMI, _AMI_UM), 1)
    },
    **{
        b: (f"{n:02d}", _REF if n in (1, 2, 3, 12) else _BT, w)
        for n, (b, w) in enumerate(zip(_SEVIRI, _SEVIRI_UM), 1)
    },
    **{
        b: (f"{n:02d}", _REF if n <= 8 else _BT, w)
        for n, (b, w) in enumerate(zip(_FCI, _FCI_UM), 1)
    },
    # MetOp AVHRR: 1, 2, 3a, 3b, 4, 5
    "1": ("1", _REF, 0.63),
    "2": ("2", _REF, 0.86),
    "3a": ("3a", _REF, 1.6),
    "3b": ("3b", _BT, 3.7),
    "4": ("4", _BT, 10.8),
    "5": ("5", _BT, 12.0),
}
#: Microwave sounders: channels named by number, frequency kept as an attribute.
MICROWAVE_STORES = ("jpss_atms",)

#: Source names whose generic rules disagree between stores, fixed by hand.
OVERRIDES = {
    "clch": "high_cloud_cover",
    "clcm": "medium_cloud_cover",
    "clcl": "low_cloud_cover",
    "clct": "total_cloud_cover",
    "alb_rad": "surface_albedo",
    "t_2m": "2m_temperature",
    "td_2m": "2m_dewpoint_temperature",
    "relhum_2m": "2m_relative_humidity",
    "u_10m": "10m_u_component_of_wind",
    "v_10m": "10m_v_component_of_wind",
    "vmax_10m": "10m_wind_gust",
    "pmsl": "mean_sea_level_pressure",
    "ps": "surface_pressure",
    "tot_prec": "total_precipitation",
    "tqv": "total_column_water_vapour",
    "tqc": "total_column_cloud_liquid_water",
    "tqi": "total_column_cloud_ice_water",
    "t_g": "skin_temperature",
    "h_snow": "snow_depth",
    "fi": "geopotential",
    "CO": "carbon_monoxide",
    "NO2": "nitrogen_dioxide",
    "O3": "ozone",
    "SO2": "sulphur_dioxide",
    "PM25_RH35_GCC": "particulate_matter_2_5um_rh35_gcc",
    "PM25_RH35_GOCART": "particulate_matter_2_5um_rh35_gocart",
    "Q": "specific_humidity",
    "RH": "relative_humidity",
    "U": "u_component_of_wind",
    "V": "v_component_of_wind",
    "T": "temperature",
    "SLP": "sea_level_pressure",
    # ERA5's mean-rate parameters, which ICON's time-averaged fluxes are.
    "aswdir_s": "mean_surface_direct_short_wave_radiation_flux",
    "aswdifd_s": "mean_surface_diffuse_short_wave_radiation_flux",
    "aswdifu_s": "mean_surface_upward_short_wave_radiation_flux",
    "asob_s": "mean_surface_net_short_wave_radiation_flux",
    "asob_s_cs": "mean_surface_net_short_wave_radiation_flux_clear_sky",
    "athb_s": "mean_surface_net_long_wave_radiation_flux",
    "athb_s_cs": "mean_surface_net_long_wave_radiation_flux_clear_sky",
    "athd_s": "mean_surface_downward_long_wave_radiation_flux",
    "asob_t": "mean_top_net_short_wave_radiation_flux",
    "athb_t": "mean_top_net_long_wave_radiation_flux",
    "alhfl_s": "mean_surface_latent_heat_flux",
    "ashfl_s": "mean_surface_sensible_heat_flux",
    "aumfl_s": "mean_eastward_turbulent_surface_stress",
    "avmfl_s": "mean_northward_turbulent_surface_stress",
    "tcolg": "total_column_graupel",
    "tcolr": "total_column_rain_water",
    "tcols": "total_column_snow_water",
    "hzerocl": "zero_degree_level",
    "z0": "forecast_surface_roughness",
    "sso_stdh": "standard_deviation_of_orography",
    "rain_con": "convective_rainfall",
    "rain_gsp": "large_scale_rainfall",
    "snow_con": "convective_snowfall",
    "snow_gsp": "large_scale_snowfall",
    "qv": "specific_humidity",
    "qc": "specific_cloud_liquid_water_content",
    "qi": "specific_cloud_ice_water_content",
    "qr": "specific_rain_water_content",
    "qs": "specific_snow_water_content",
    "qg": "specific_graupel_content",
    "cape_ml": "mixed_layer_convective_available_potential_energy",
    "cin_ml": "mixed_layer_convective_inhibition",
    "cape_mu": "most_unstable_convective_available_potential_energy",
    "cin_mu": "most_unstable_convective_inhibition",
    "hsurf": "orography",
    "hhl": "height_of_model_half_levels",
    "w_so": "soil_moisture",
    "t_so": "soil_temperature",
    "w_snow": "snow_water_equivalent",
}

#: Station and observation abbreviations.
ABBREVIATIONS = {
    "atmp": "air_temperature",
    "wtmp": "sea_surface_temperature",
    "sst": "sea_surface_temperature",
    "sss": "sea_surface_salinity",
    "ztmp": "sea_water_temperature",
    "zsal": "sea_water_salinity",
    "dewp": "dewpoint_temperature",
    "dewpoint": "dewpoint_temperature",
    "dew_point": "dewpoint_temperature",
    "td": "dewpoint_temperature",
    "hur": "relative_humidity",
    "slp": "sea_level_pressure",
    "msl": "mean_sea_level_pressure",
    "sp": "surface_pressure",
    "pres": "air_pressure",
    "winddir": "wind_direction",
    "wdir": "wind_direction",
    "wd": "wind_direction",
    "windspd": "wind_speed",
    "wspd": "wind_speed",
    "ws": "wind_speed",
    "gst": "wind_gust",
    "wvht": "significant_wave_height",
    "precip": "precipitation",
    "clouds": "cloud_cover",
    "c": "cloud_cover",
    "rlds": "surface_downwelling_longwave_flux",
    "rsds": "surface_downwelling_shortwave_flux",
    "uo": "eastward_sea_water_velocity",
    "vo": "northward_sea_water_velocity",
    "wo": "upward_sea_water_velocity",
    "water_col_ht": "water_column_height",
    "ghi": "global_horizontal_irradiance",
    "dni": "direct_normal_irradiance",
    "dhi": "diffuse_horizontal_irradiance",
    "clearsky_ghi": "clear_sky_global_horizontal_irradiance",
    "clearsky_dni": "clear_sky_direct_normal_irradiance",
    "clearsky_dhi": "clear_sky_diffuse_horizontal_irradiance",
    "aod": "aerosol_optical_depth",
    "ssa": "aerosol_single_scattering_albedo",
    "alpha": "angstrom_exponent",
    "asymmetry": "aerosol_asymmetry_parameter",
    "bangle": "bending_angle",
    "impact": "impact_parameter",
    "refc": "composite_reflectivity",
    "refc_base": "base_reflectivity",
    "t": "temperature",
    "ra1": "precipitation_1h",
    "ra3": "precipitation_3h",
    "ra6": "precipitation_6h",
    "ra12": "precipitation_12h",
    "ra24": "precipitation_24h",
    "lwir": "longwave_infrared",
    "swir": "shortwave_infrared",
    "vis": "visible",
    "wv": "water_vapour",
    "ssr": "shortwave_reflectance",
}
#: Metadata names that already read fine.
KEEP_AS_IS = {
    "country",
    "county",
    "state",
    "eez",
    "offshore",
    "timezone",
    "name",
    "call",
    "elevation",
    "platform_id",
    "platform_code",
    "platform_type",
    "serial",
    "launch_site",
    "sonde_type",
    "flight_id",
    "area",
    "valid",
    "gid",
    "source_gid",
    "satellites",
    "heading",
    "software",
    "subtype",
    "dfmcode",
    "city",
    "units",
    "ap",
    "b",
    "ap_half",
    "b_half",
    "index",
    "storm",
}

_LEVEL = re.compile(
    r"(?:_at|_on)?_(model_levels?|pressure_levels?|height_levels?|surface|sfc|"
    r"atmosphere_single_layer|entire_atmosphere|height|ml|pl)$"
)
_HEIGHT_IN_NAME = re.compile(r"(?:^|_)(?:at_)?(?:height_above_ground_)?(\d+(?:[._]\d+)?)_?m(?:_|$)")


def _height(leaf: str) -> str:
    """A single-level height in a source name (``u_10m``, ``t_at_80.0m``) as ``80m``."""
    m = _HEIGHT_IN_NAME.search(leaf) or re.search(r"height_above_ground_(\d+(?:\.\d+)?)$", leaf)
    if not m:
        return ""
    value = float(m.group(1).replace("_", "."))
    return (f"{value:g}".replace(".", "_")) + "m"


def channel_name(leaf: str) -> str | None:
    """``channel_13_brightness_temperature_10_3um``-style names for imager channels."""
    hit = CHANNELS.get(leaf) or CHANNELS.get(leaf.upper())
    if hit is None:
        return None
    number, quantity, wavelength = hit
    return f"channel_{number}_{quantity}_{f'{wavelength:g}'.replace('.', '_')}um"


#: Coordinates and dimensions, named the same way everywhere.
COORDINATE_RENAMES = {
    "lat": "latitude",
    "lon": "longitude",
    "Latitude": "latitude",
    "Longitude": "longitude",
    "LAT": "latitude",
    "LON": "longitude",
    "nav_lat": "latitude",
    "nav_lon": "longitude",
    "forecast_reference_time": "init_time",
    "reference_time": "init_time",
    "forecast_period": "step",
    "lead_time": "step",
    "isobaricInhPa": "pressure_level",
    "isobaric": "pressure_level",
    "plev": "pressure_level",
    "hybrid": "model_level",
    "generalVerticalLayer": "model_level",
    "hybrid_half": "model_half_level",
    "model_level_half": "model_half_level",
    "heightAboveGround": "height_above_ground",
    "number": "ensemble_member",
    "member": "ensemble_member",
    "realization": "ensemble_member",
    # Projection coordinates are x and y everywhere.
    "x_geostationary": "x",
    "y_geostationary": "y",
    "projection_x_coordinate": "x",
    "projection_y_coordinate": "y",
    "xc": "x",
    "yc": "y",
    # WRF's names.
    "west_east": "x",
    "south_north": "y",
    "west_east_stag": "x_staggered",
    "south_north_stag": "y_staggered",
    "bottom_top": "model_level",
    "bottom_top_stag": "model_half_level",
    "soil_layers_stag": "soil_level",
    "Time": "time",
    # wgrib2/kerchunk level groups (HRRR, NAM, HAFS) and other shorthands.
    "hag": "height_above_ground",
    "isobar": "pressure_level",
    "hybid": "model_level",
    "asl": "height_above_sea_level",
    "depth_bls": "depth_below_land_surface",
    "halflevel": "model_half_level",
    "Channel": "channel",
    "P_Level": "pressure_level",
    "P_Layer": "pressure_layer",
    # ICON's unstructured grid.
    "values": "cell",
}


def coordinate_rename(name: str, attrs: Mapping, dims: list[str]) -> str | None:
    """The standard name of a coordinate or dimension, if it has a different one.

    ``t`` and ``level`` need their units to say what they are: a time axis, pressure levels.
    """
    if name in COORDINATE_RENAMES:
        return COORDINATE_RENAMES[name]
    units = str(attrs.get("units", ""))
    if name == "t" and " since " in units:
        return "time"
    if (
        name in ("level", "levels", "lev", "pressure")
        and name in dims
        and units.strip().lower() in ("hpa", "pa", "millibars", "mb")
    ):
        return "pressure_level"
    if name == "height" and name in dims and units.strip() == "m":
        return "height_above_ground"
    return None


def propose(name: str, attrs: Mapping, dims: list[str], store: str = "") -> tuple[str, str]:
    """A readable name for one variable, and which rule gave it.

    The rule is one of ``coordinate`` (left as it is), ``channel``, ``abbreviation``,
    ``grib``, ``cf``, ``long_name``, ``eccodes``, ``tidied`` or ``kept`` (nothing better
    known; flagged for review).
    """
    leaf = name.rsplit("/", 1)[-1]
    renamed = coordinate_rename(leaf, attrs, dims)
    if renamed:
        return renamed, "coordinate"
    if (
        leaf in COORDINATE_NAMES
        or leaf in dims
        or leaf in KEEP_AS_IS
        or leaf.endswith(("_bnds", "_bounds", "_index"))
        or GRID_MAPPING.search(leaf)
    ):
        return name, "coordinate"
    if any(s in store for s in MICROWAVE_STORES) and leaf.isdigit():
        return f"channel_{int(leaf):02d}_brightness_temperature", "channel"
    channel = channel_name(leaf)
    if (
        channel
        and "geo" in store
        or channel
        and ("avhrr" in store or "conus" in store or "pacus" in store)
    ):
        return channel, "channel"
    if leaf in OVERRIDES:
        return OVERRIDES[leaf], "override"
    if leaf.lower() in ABBREVIATIONS:
        return ABBREVIATIONS[leaf.lower()], "abbreviation"
    height = _height(leaf)
    long_name = attrs.get("long_name") or attrs.get("description")
    standard = attrs.get("standard_name")
    grib_name = attrs.get("GRIB_name")

    def with_height(base: str) -> str:
        stated = bool(height) and (height in base or f"{height[:-1]}_m_" in base)
        if (
            height
            and not stated
            and not re.match(r"^\d", base)
            and not any(
                d in ("height", "pressure_level", "level", "model_level", "isobaricInhPa")
                for d in dims
            )
        ):
            return f"{height}_{base}"
        return base

    base_leaf, level_leaf = strip_level_suffix(leaf)
    if base_leaf in OVERRIDES and level_leaf:
        return OVERRIDES[base_leaf], "override"
    tidied_source = era5ify(leaf)

    def shorter(candidate: str, rule: str) -> tuple[str, str]:
        # A generated name far longer than a source name that already reads well loses.
        if (
            len(candidate) > 45
            and len(tidied_source.split("_")) >= 3
            and len(tidied_source) < len(candidate)
        ):
            return tidied_source, "tidied"
        return re.sub(r"^(diagnostic|time)_", "", candidate), rule

    if grib_name and informative(grib_name):
        return shorter(with_height(era5ify(grib_name)), "grib")
    leaf_words = set(snake(leaf).split("_"))
    if standard in CF_TO_ERA5 and (
        leaf_words & set(standard.split("_"))
        or len(leaf) <= 3
        and leaf_words & set(CF_TO_ERA5[standard].split("_"))
    ):
        return with_height(CF_TO_ERA5[standard] + _qualifier(leaf)), "cf"
    if long_name and informative(long_name):
        candidate = era5ify(long_name)
        if 2 <= len(candidate.split("_")) and len(candidate) <= 60:
            return shorter(with_height(candidate), "long_name")
    table = eccodes_names()
    known = table.get(leaf) or table.get(leaf.lower())
    if known:
        return shorter(with_height(era5ify(known)), "eccodes")
    base, level = strip_level_suffix(leaf)
    known = table.get(base) or table.get(base.lower())
    if known:
        return with_height(era5ify(known)) + (
            f"_{level}" if level and level != "surface" else ""
        ), "eccodes"
    if standard and informative(standard) and leaf_words & set(standard.split("_")):
        return snake(standard), "cf"
    tidy = era5ify(leaf)
    if len(tidy.split("_")) >= 2 or len(tidy) > 8:
        return tidy, "tidied"
    return leaf, "kept"


def level_qualifier(leaf: str) -> str:
    """What a source name says about its level, to tell same-named variables apart."""
    m = _LEVEL.search(leaf)
    if not m:
        return ""
    kind = m.group(1)
    return {
        "ml": "on_model_levels",
        "model_level": "on_model_levels",
        "model_levels": "on_model_levels",
        "pl": "on_pressure_levels",
        "pressure_level": "on_pressure_levels",
        "pressure_levels": "on_pressure_levels",
        "height": "on_height_levels",
        "height_level": "on_height_levels",
        "height_levels": "on_height_levels",
        "surface": "at_surface",
        "sfc": "at_surface",
        "atmosphere_single_layer": "entire_atmosphere",
        "entire_atmosphere": "entire_atmosphere",
    }.get(kind, kind)


def _qualifier(leaf: str, height: str = "") -> str:
    """What a CF-named variable's own name adds: a statistic or window (``_max_1h``)."""
    rest = leaf.replace(height, "_") if height else leaf
    found = re.findall(r"(?:^|_)(min|max|mean|sum|accumulated|\d+h)(?=_|$)", rest)
    return "".join(f"_{q}" for q in found)


#: ERA5's units for the quantities it names, by the end of the standard name.
ERA5_UNITS = (
    (r"(^|_)(temperature|dewpoint_temperature)$", "K"),
    (r"(^|_)(pressure|mean_sea_level_pressure|surface_pressure|sea_level_pressure)$", "Pa"),
    (r"(^|_)total_precipitation(_\d+h)?$", "m"),
    (r"(^|_)(total|low|medium|high)_cloud_cover$", "(0 - 1)"),
    (r"(^|_)geopotential(_at_surface)?$", "m**2 s**-2"),
)
#: (source unit, ERA5 unit) -> (scale, offset): ERA5 value = source * scale + offset.
UNIT_CONVERSIONS = {
    ("degc", "K"): (1.0, 273.15),
    ("deg_c", "K"): (1.0, 273.15),
    ("celsius", "K"): (1.0, 273.15),
    ("c", "K"): (1.0, 273.15),
    ("hpa", "Pa"): (100.0, 0.0),
    ("mbar", "Pa"): (100.0, 0.0),
    ("mb", "Pa"): (100.0, 0.0),
    ("kpa", "Pa"): (1000.0, 0.0),
    ("mm", "m"): (0.001, 0.0),
    ("kg_m_2", "m"): (0.001, 0.0),
    ("kg_m2", "m"): (0.001, 0.0),
    ("pct", "(0 - 1)"): (0.01, 0.0),
    ("%", "(0 - 1)"): (0.01, 0.0),
    ("percent", "(0 - 1)"): (0.01, 0.0),
    ("gpm", "m**2 s**-2"): (9.80665, 0.0),
    ("m", "m**2 s**-2"): (9.80665, 0.0),
}


#: Heights stored where ERA5 stores geopotential: (name pattern, ERA5 name).
GEOPOTENTIAL_RENAMES = (
    (r"^(.*_)?geopotential_height$", r"\1geopotential"),
    (
        r"^(orography|surface_geopotential_height|terrain_height|model_terrain_height|"
        r"geometric_height_of_the_earths_surface_above_sea_level)$",
        "geopotential_at_surface",
    ),
)
STANDARD_GRAVITY = 9.80665


def geopotential_rename(new_name: str, units: object) -> str | None:
    """ERA5's geopotential name for a variable stored as a height in metres, if it is one."""
    if snake(str(units)) not in ("m", "gpm", "geopotential_metre", "geopotential_metres"):
        return None
    for pattern, replacement in GEOPOTENTIAL_RENAMES:
        if re.match(pattern, new_name):
            return re.sub(pattern, replacement, new_name)
    return None


def unit_conversion(new_name: str, units: object) -> tuple[float, float, str] | None:
    """(scale, offset, ERA5 units) turning a variable's units into ERA5's, if they differ.

    Applied as CF ``scale_factor``/``add_offset``: the stored values are untouched and
    xarray decodes them in ERA5's units.
    """
    if units is None:
        return None
    leaf = new_name.rsplit("/", 1)[-1]
    target = next((u for pattern, u in ERA5_UNITS if re.search(pattern, leaf)), None)
    if target is None:
        return None
    source = str(units).strip()
    key = "%" if source == "%" else snake(source)
    if source in (target, "K", "Pa") and source == target:
        return None
    conversion = UNIT_CONVERSIONS.get((key, target))
    if conversion is None:
        return None
    return conversion[0], conversion[1], target


@functools.lru_cache(maxsize=1)
def _all_store_names() -> dict[str, dict[str, str]]:
    if not STORE_NAMES.is_file():
        return {}
    return json.loads(STORE_NAMES.read_text())


def store_names(store_prefix: str) -> dict[str, str]:
    """Old name -> new name for one store (empty for a store not renamed)."""
    key = store_prefix.strip("/")
    names = _all_store_names()
    if key in names:
        return names[key]
    # Generations of a store (``icon_global_25082026``) share the family's names.
    family = re.sub(r"_\d{8}(T\d{4})?(?=\.icechunk$)|_\d+(?=\.icechunk$)", "", key)
    return names.get(family, {})


def rename_for_store(ds, store_prefix: str):
    """``ds`` with its variables named as ``store_prefix`` names them.

    Each renamed variable records its source name in ``source_name``.
    """
    mapping = {old: new for old, new in store_names(store_prefix).items() if old in ds.variables}
    if not mapping or os.environ.get("PLANETARY_DATASETS_KEEP_SOURCE_NAMES"):
        return ds
    ds = ds.rename(mapping)
    for old, new in mapping.items():
        ds[new].attrs.setdefault(SOURCE_NAME_ATTR, old)
    return ds


__all__ = [
    "SOURCE_NAME_ATTR",
    "STORE_NAMES",
    "eccodes_names",
    "era5ify",
    "propose",
    "rename_for_store",
    "snake",
    "store_names",
]
