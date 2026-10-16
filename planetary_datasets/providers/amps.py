"""Antarctic Mesoscale Prediction System (AMPS) forecasts from NSF NCAR GDEX (d473002).

AMPS is NCAR's real-time WRF configuration for Antarctica, run twice a day (00 and 12 UTC)
in support of the US Antarctic Program. GDEX archives GRIB1 subsets of every run, one file
per domain and forecast hour, publicly over HTTPS with no credentials::

    https://data.gdex.ucar.edu/d473002/grib/WRF24/<YYYY>/<MM>/<DD>/<YYYYMMDDHH>_WRF_<domain>_f<FFF>.grb

The same files are mirrored on the Open Science Data Federation
(``https://osdf-director.osg-htc.org/ncar/gdex/d473002/...``), but a cold OSDF cache took
over five minutes for a file GDEX served in ten seconds, so GDEX itself is used.

Only the current grid configuration, ``WRF24`` (24/8/2.67/0.89 km, from 2017-10-10 12 UTC),
is covered. Its domains are all polar stereographic but every one has its own grid, so each
gets its own store, ``bkr/dmi/amps_<domain>.icechunk``. Each partition is one init time,
holding every forecast hour up to 48 h the domain publishes: 3-hourly 0-48 h for the
outer and ocean domains, hourly 0-39 h (the whole run) for the fine nests.

The files are decoded directly with eccodes rather than through cfgrib. AMPS writes GRIB1
under NCAR's centre (60) with a local parameter table, so cfgrib names many fields
``unknown`` and then cannot tell them apart. :data:`FIELDS` maps each GRIB parameter and
level type to a variable explicitly instead, after the AMPS table that ships with
netCDF-Java (``resources/grib1/local/wrf_amps.wrf``). A run is several GB decoded, so the
dataset is built lazily with :func:`~planetary_datasets.common.lazy_fields.lazy_field_dataset`
and streamed into the store one (variable, step) chunk at a time.

Things a reader of the store should know:

* Winds are **grid-relative** (``uvRelativeToGrid = 1``), not earth-relative.
* ``total_precipitation_3h`` is the precipitation of the 3 h ending at each step (of
  the hours since the init time, for the first two hours of the hourly nests).
* The ``_at_10m`` winds and ``_at_2m`` mixing ratios are identical to model level 1 in the
  files sampled; AMPS appears to label its lowest model level that way. Both are kept,
  because model-level output only begins partway through the WRF24 archive.
* ``latitude``/``longitude`` are AMPS's own fields (GRIB parameters 176/177). ``x``/``y``
  are the nominal polar stereographic coordinates rebuilt from the GRIB grid header, which
  is rounded; on the fine nests they sit up to ~0.6 of a cell off the model's own
  positions, so use ``latitude``/``longitude`` for anything that needs to be exact.
"""

from __future__ import annotations

import dataclasses
import pathlib
from typing import Iterable, List, Mapping, Sequence

import numpy as np
import pandas as pd
import requests
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common.download import download_many, download_one
from planetary_datasets.common.generations import GenerationalStoreMixin
from planetary_datasets.common.lazy_fields import (
    FieldVariable,
    bounded_compute,
    lazy_field_dataset,
)
from planetary_datasets.config import Config
from planetary_datasets.providers._timestamps import NaiveUTCPartitions
from planetary_datasets.providers.regional_lam_common import init_time_download_dir

GDEX_BASE_URL = "https://data.gdex.ucar.edu/d473002/grib/WRF24"

#: First init time of the WRF24 grid configuration on GDEX.
WRF24_START = pd.Timestamp("2017-10-10T12:00")

#: AMPS runs twice a day.
INIT_HOURS = (0, 12)

#: Only the first two days of each run are kept.
MAX_LEAD_HOURS = 48

#: Pressure levels (hPa) in the files, surface first.
PRESSURE_LEVELS: tuple[int, ...] = (
    1000, 975, 950, 925, 900, 875, 850, 800, 700, 600, 500, 400, 300, 250, 200, 150, 100,
    70, 50, 30, 20,
)  # fmt: skip

#: Lowest model (eta) levels, counted up from the ground.
MODEL_LEVELS: tuple[int, ...] = (1, 2, 3, 4, 5, 6, 7)

#: Top of each Noah soil layer, in cm (layers 0-10, 10-40, 40-100 and 100-200 cm).
SOIL_DEPTHS: tuple[int, ...] = (0, 10, 40, 100)

#: Sphere GRIB1 assumes for a grid with ``earthIsOblate = 0``, in metres.
GRIB1_EARTH_RADIUS = 6367470.0

#: GRIB1 polar stereographic grid lengths are true at 60 degrees.
GRIB1_TRUE_LATITUDE = 60.0

#: GRIB parameters of the 2-D latitude and longitude fields, used as coordinates.
LATITUDE_PARAM = 176
LONGITUDE_PARAM = 177

#: Parameters deliberately left out: they are not in AMPS's own parameter table and their
#: meaning is undocumented.
IGNORED_PARAMS = frozenset({133, 134, 247})

#: Dimension of each kind of vertical axis, and its levels.
LEVEL_AXES: dict[str, tuple[int, ...]] = {
    "level": PRESSURE_LEVELS,
    "model_level": MODEL_LEVELS,
    "soil_depth": SOIL_DEPTHS,
}


@dataclasses.dataclass(frozen=True)
class AMPSDomain:
    """One AMPS WRF domain.

    Attributes:
        key: The domain's token in GDEX filenames, e.g. ``d1`` or ``DRAKE``.
        description: What the domain covers.
        resolution_km: Nominal grid spacing.
        step_hours: Forecast hours kept for each init time.
        first_init: First init time this domain was archived under WRF24.
        model_levels: Whether the files carry model-level and top-of-atmosphere fields.
            The two ocean domains never have.
    """

    key: str
    description: str
    resolution_km: float
    step_hours: tuple[int, ...]
    first_init: pd.Timestamp = WRF24_START
    model_levels: bool = True

    @property
    def store_name(self) -> str:
        """The domain's part of the store name, e.g. ``amps_d1``."""
        return f"amps_{self.key.lower()}"


_THREE_HOURLY = tuple(range(0, MAX_LEAD_HOURS + 1, 3))
#: The fine nests publish hourly to 39 h; all of it is within the first 48 h.
_HOURLY = tuple(range(0, 40))

DOMAINS: dict[str, AMPSDomain] = {
    domain.key.lower(): domain
    for domain in (
        AMPSDomain("d1", "Antarctica and the Southern Ocean", 24.0, _THREE_HOURLY),
        AMPSDomain("d2", "Antarctic continent", 8.0, _THREE_HOURLY),
        AMPSDomain("d3", "Ross Sea sector, including the South Pole", 2.67, _HOURLY),
        AMPSDomain("d5", "Ross Island", 0.89, _HOURLY),
        AMPSDomain("d6", "Antarctic Peninsula", 2.67, _HOURLY),
        AMPSDomain(
            "DRAKE",
            "Drake Passage",
            6.0,
            _THREE_HOURLY,
            first_init=pd.Timestamp("2020-08-08T00:00"),
            model_levels=False,
        ),
        AMPSDomain(
            "NZ",
            "New Zealand to the Ross Sea",
            6.0,
            _THREE_HOURLY,
            first_init=pd.Timestamp("2020-08-08T00:00"),
            model_levels=False,
        ),
    )
}


@dataclasses.dataclass(frozen=True)
class AMPSField:
    """How one GRIB parameter on one level type becomes a variable.

    Attributes:
        name: Variable name in the store.
        param: GRIB1 ``indicatorOfParameter``.
        level_type: eccodes ``typeOfLevel``.
        long_name: Description, after the AMPS parameter table.
        units: Units.
        axis: Vertical dimension (a key of :data:`LEVEL_AXES`), or None for one level.
        level: For a single-level field, the GRIB ``level`` value to match.
        dtype: Stored dtype. float16 only for range-safe fields (temperature, horizontal
            wind, cloud fraction); everything else full float32.
        model_level_output: Only in domains with model-level output.
        notes: Extra attribute text.
    """

    name: str
    param: int
    level_type: str
    long_name: str
    units: str
    axis: str | None = None
    level: int | None = None
    dtype: str = "float32"
    model_level_output: bool = False
    notes: str = ""


_F16 = "float16"

FIELDS: tuple[AMPSField, ...] = (
    # Pressure levels.
    AMPSField("geopotential_height", 7, "isobaricInhPa", "Geopotential height", "gpm", "level"),
    AMPSField("temperature", 11, "isobaricInhPa", "Temperature", "K", "level", dtype=_F16),
    AMPSField(
        "u_component_of_wind", 33, "isobaricInhPa", "U-component of wind (grid-relative)",
        "m s-1", "level", dtype=_F16,
    ),
    AMPSField(
        "v_component_of_wind", 34, "isobaricInhPa", "V-component of wind (grid-relative)",
        "m s-1", "level", dtype=_F16,
    ),
    AMPSField("vertical_velocity", 40, "isobaricInhPa", "Vertical velocity", "m s-1", "level"),
    AMPSField(
        "relative_humidity", 52, "isobaricInhPa", "Relative humidity (with respect to water)",
        "%", "level", notes="Published for 100-1000 hPa only.",
    ),
    AMPSField(
        "water_vapor_mixing_ratio", 53, "isobaricInhPa", "Water vapor mixing ratio", "kg kg-1",
        "level",
    ),
    AMPSField(
        "cloud_water_mixing_ratio", 153, "isobaricInhPa", "Cloud water mixing ratio",
        "kg kg-1", "level", notes="Published for 100-1000 hPa only.",
    ),
    AMPSField(
        "rain_water_mixing_ratio", 170, "isobaricInhPa", "Rain water mixing ratio", "kg kg-1",
        "level", notes="Published for 100-1000 hPa only.",
    ),
    AMPSField(
        "snow_mixing_ratio", 171, "isobaricInhPa", "Snow mixing ratio", "kg kg-1", "level",
        notes="Published for 100-1000 hPa only.",
    ),
    AMPSField(
        "ice_mixing_ratio", 178, "isobaricInhPa", "Ice mixing ratio", "kg kg-1", "level",
        notes="Published for 100-1000 hPa only.",
    ),
    # Lowest model levels.
    AMPSField(
        "pressure_model_level", 1, "hybrid", "Pressure", "Pa", "model_level",
        model_level_output=True,
    ),
    AMPSField(
        "geopotential_height_model_level", 7, "hybrid", "Geopotential height", "gpm",
        "model_level", model_level_output=True,
    ),
    AMPSField(
        "temperature_model_level", 11, "hybrid", "Temperature", "K", "model_level",
        dtype=_F16, model_level_output=True,
    ),
    AMPSField(
        "u_component_of_wind_model_level", 33, "hybrid", "U-component of wind (grid-relative)",
        "m s-1", "model_level", dtype=_F16, model_level_output=True,
    ),
    AMPSField(
        "v_component_of_wind_model_level", 34, "hybrid", "V-component of wind (grid-relative)",
        "m s-1", "model_level", dtype=_F16, model_level_output=True,
    ),
    AMPSField(
        "water_vapor_mixing_ratio_model_level", 53, "hybrid", "Water vapor mixing ratio",
        "kg kg-1", "model_level", model_level_output=True,
    ),
    # Soil.
    AMPSField(
        "soil_temperature", 11, "depthBelowLandLayer", "Soil temperature", "K", "soil_depth",
        dtype=_F16,
    ),
    # Near-surface and single-level fields.
    AMPSField("surface_pressure", 1, "surface", "Surface pressure", "Pa", level=0),
    AMPSField("mean_sea_level_pressure", 2, "meanSea", "Sea-level pressure", "Pa", level=0),
    AMPSField(
        "cloud_base_height", 7, "cloudBase", "Geopotential height of cloud base", "gpm",
        level=0,
    ),
    AMPSField(
        "surface_temperature", 11, "surface", "Surface (skin) temperature", "K", level=1,
        dtype=_F16,
    ),
    AMPSField(
        "temperature_at_2m", 11, "heightAboveGround", "2 m temperature", "K", level=2,
        dtype=_F16,
    ),
    AMPSField(
        "u_component_of_wind_at_10m", 33, "heightAboveGround",
        "10 m U-component of wind (grid-relative)", "m s-1", level=10, dtype=_F16,
    ),
    AMPSField(
        "v_component_of_wind_at_10m", 34, "heightAboveGround",
        "10 m V-component of wind (grid-relative)", "m s-1", level=10, dtype=_F16,
    ),
    AMPSField(
        "u_component_of_wind_at_150m_above_sea_level", 33, "heightAboveSea",
        "U-component of wind 150 m above sea level (grid-relative)", "m s-1", level=150,
        dtype=_F16,
    ),
    AMPSField(
        "v_component_of_wind_at_150m_above_sea_level", 34, "heightAboveSea",
        "V-component of wind 150 m above sea level (grid-relative)", "m s-1", level=150,
        dtype=_F16,
    ),
    AMPSField(
        "u_component_of_wind_at_300m_above_sea_level", 33, "heightAboveSea",
        "U-component of wind 300 m above sea level (grid-relative)", "m s-1", level=300,
        dtype=_F16,
    ),
    AMPSField(
        "v_component_of_wind_at_300m_above_sea_level", 34, "heightAboveSea",
        "V-component of wind 300 m above sea level (grid-relative)", "m s-1", level=300,
        dtype=_F16,
    ),
    AMPSField(
        "relative_humidity_at_2m", 52, "heightAboveGround",
        "2 m relative humidity (with respect to water)", "%", level=2,
    ),
    AMPSField(
        "water_vapor_mixing_ratio_at_2m", 53, "heightAboveGround",
        "2 m water vapor mixing ratio", "kg kg-1", level=2,
    ),
    AMPSField(
        "cloud_water_mixing_ratio_at_2m", 153, "heightAboveGround",
        "2 m cloud water mixing ratio", "kg kg-1", level=2,
    ),
    AMPSField(
        "rain_water_mixing_ratio_at_2m", 170, "heightAboveGround",
        "2 m rain water mixing ratio", "kg kg-1", level=2,
    ),
    AMPSField(
        "snow_mixing_ratio_at_2m", 171, "heightAboveGround", "2 m snow mixing ratio",
        "kg kg-1", level=2,
    ),
    AMPSField(
        "ice_mixing_ratio_at_2m", 178, "heightAboveGround", "2 m ice mixing ratio", "kg kg-1",
        level=2,
    ),
    AMPSField(
        "precipitable_water", 54, "entireAtmosphere", "Precipitable water (water vapor)",
        "kg m-2", level=1,
    ),
    AMPSField(
        "total_precipitation_3h", 61, "surface",
        "Precipitation over the 3 h ending at the step (since the init time before 3 h)",
        "kg m-2", level=0,
    ),
    AMPSField("albedo", 84, "surface", "Albedo", "%", level=1),
    AMPSField("sea_ice_cover", 91, "surface", "Sea ice", "1", level=1),
    AMPSField("latent_heat_flux", 121, "surface", "Latent heat flux", "W m-2", level=1),
    AMPSField("sensible_heat_flux", 122, "surface", "Sensible heat flux", "W m-2", level=1),
    AMPSField(
        "downward_shortwave_radiation_flux", 204, "surface",
        "Downward shortwave radiation flux", "W m-2", level=1,
    ),
    AMPSField(
        "downward_longwave_radiation_flux", 205, "surface",
        "Downward longwave radiation flux", "W m-2", level=1,
    ),
    AMPSField(
        "outgoing_longwave_radiation", 212, "nominalTop",
        "Upward longwave radiation flux at the top of the atmosphere", "W m-2", level=1,
        model_level_output=True,
        notes="Parameter 212 is not in the AMPS table; named after WMO GRIB1 table 2.",
    ),
    AMPSField(
        "boundary_layer_height", 221, "surface", "Planetary boundary layer height", "m",
        level=1, notes="Parameter 221 is not in the AMPS table; named after WMO GRIB1 table 2.",
    ),
    AMPSField(
        "cloud_fraction", 248, "surface", "Cloud fraction", "1", level=1, dtype=_F16,
        notes="The AMPS table gives units of m; the values are a 0-1 fraction.",
    ),
    AMPSField("inversion_height", 249, "surface", "Inversion height", "m", level=1),
    AMPSField("inversion_strength", 250, "surface", "Inversion strength", "K", level=1),
    AMPSField(
        "column_integrated_snow", 251, "entireAtmosphere", "Column integrated snow", "kg m-2",
        level=1,
    ),
    AMPSField(
        "column_integrated_rain_water", 252, "entireAtmosphere",
        "Column integrated rain water", "kg m-2", level=1,
    ),
    AMPSField(
        "column_integrated_cloud_ice", 253, "entireAtmosphere", "Column integrated cloud ice",
        "kg m-2", level=1,
    ),
    AMPSField(
        "column_integrated_cloud_liquid_water", 254, "entireAtmosphere",
        "Column integrated cloud liquid water", "kg m-2", level=1,
    ),
)  # fmt: skip

#: Fields that do not change between runs, stored once per store as 2-D coordinates
#: rather than per step.
STATIC_COORDS: tuple[AMPSField, ...] = (
    AMPSField("latitude", LATITUDE_PARAM, "surface", "Latitude", "degrees_north", level=0),
    AMPSField("longitude", LONGITUDE_PARAM, "surface", "Longitude", "degrees_east", level=0),
    AMPSField("orography", 7, "surface", "Terrain height", "m", level=0),
    AMPSField("land_sea_mask", 81, "surface", "Land/sea mask (1 = land)", "1", level=1),
)


def fields_for(domain: AMPSDomain) -> tuple[AMPSField, ...]:
    """The variables stored for ``domain``.

    The same set for every init time of a domain, whether or not a given file has all of
    them: a field missing from a file is stored as NaN. That keeps one store per domain
    across the WRF24 archive, which only gained its model-level fields partway through.
    """
    return tuple(f for f in FIELDS if domain.model_levels or not f.model_level_output)


def file_url(domain: AMPSDomain, it: pd.Timestamp, step: int) -> str:
    """GDEX URL of one domain's file for init time ``it`` and forecast hour ``step``."""
    return f"{GDEX_BASE_URL}/{it:%Y/%m/%d}/{it:%Y%m%d%H}_WRF_{domain.key}_f{step:03d}.grb"


@dataclasses.dataclass(frozen=True)
class GribMessage:
    """Where one GRIB1 message lives and what it holds."""

    path: str
    offset: int
    length: int
    param: int
    level_type: str
    level: int
    time_range: int
    p1: int
    end_step: int
    ni: int
    nj: int
    init: pd.Timestamp

    @property
    def uri(self) -> str:
        """A string the lazy reader can find the message from."""
        return f"{self.path}::{self.offset}::{self.length}"


#: GRIB1 ``timeRangeIndicator`` for an accumulation from P1 to P2.
_ACCUMULATION = 4

#: Hours each stored precipitation accumulation covers.
PRECIPITATION_WINDOW_HOURS = 3


def scan_grib_file(path: str | pathlib.Path) -> list[GribMessage]:
    """Read the headers of every message in a GRIB file, without decoding any data.

    Raises:
        ValueError: If the file is truncated or not GRIB.
    """
    import eccodes

    messages: list[GribMessage] = []
    with open(path, "rb") as handle:
        while True:
            try:
                gid = eccodes.codes_grib_new_from_file(handle, headers_only=True)
            except eccodes.GribInternalError as exc:
                raise ValueError(f"{path}: unreadable GRIB after {len(messages)} messages") from exc
            if gid is None:
                break
            try:
                date = eccodes.codes_get(gid, "dataDate")
                hhmm = eccodes.codes_get(gid, "dataTime")
                messages.append(
                    GribMessage(
                        path=str(path),
                        offset=int(eccodes.codes_get(gid, "offset")),
                        length=int(eccodes.codes_get(gid, "totalLength")),
                        param=int(eccodes.codes_get(gid, "indicatorOfParameter")),
                        level_type=str(eccodes.codes_get(gid, "typeOfLevel")),
                        level=int(eccodes.codes_get(gid, "level")),
                        time_range=int(eccodes.codes_get(gid, "timeRangeIndicator")),
                        p1=int(eccodes.codes_get(gid, "P1")),
                        end_step=int(eccodes.codes_get(gid, "endStep")),
                        ni=int(eccodes.codes_get(gid, "Ni")),
                        nj=int(eccodes.codes_get(gid, "Nj")),
                        init=pd.Timestamp(f"{date:08d}T{hhmm:04d}"),
                    )
                )
            finally:
                eccodes.codes_release(gid)
    if not messages:
        raise ValueError(f"{path}: no GRIB messages")
    return messages


def decode_message(uri: str) -> np.ndarray:
    """Decode the message at ``uri`` (from :attr:`GribMessage.uri`) to a flat float64 array.

    Points flagged missing by a bitmap come back as NaN.
    """
    import eccodes

    path, offset, length = uri.rsplit("::", 2)
    with open(path, "rb") as handle:
        handle.seek(int(offset))
        raw = handle.read(int(length))
    gid = eccodes.codes_new_from_message(raw)
    try:
        values = eccodes.codes_get_values(gid).astype("float64", copy=False)
        if eccodes.codes_get(gid, "bitmapPresent"):
            values[values == eccodes.codes_get(gid, "missingValue")] = np.nan
    finally:
        eccodes.codes_release(gid)
    return values


def read_into(uri: str, row: np.ndarray) -> bool:
    """:data:`~planetary_datasets.common.lazy_fields.Reader` for local GRIB messages."""
    values = decode_message(uri)
    if values.size != row.size:
        raise ValueError(f"{uri}: {values.size} points, expected {row.size}")
    row[...] = values
    return True


def _is_precipitation_window(message: GribMessage) -> bool:
    """Whether a precipitation message is the 3 h accumulation ending at its step."""
    start = max(0, message.end_step - PRECIPITATION_WINDOW_HOURS)
    return message.time_range == _ACCUMULATION and message.p1 == start


def match_message(
    message: GribMessage, fields: Iterable[AMPSField]
) -> tuple[AMPSField, int] | None:
    """The field a message belongs to and its index on the field's vertical axis.

    Returns None for a message no field takes. Precipitation is taken as the 3 h
    accumulation every domain publishes in every era; since 2026 the hourly nests also
    publish a 1 h total, which is skipped so that the variable means the same thing in
    every store and every year.
    """
    for field in fields:
        if field.param != message.param or field.level_type != message.level_type:
            continue
        if field.param == 61 and not _is_precipitation_window(message):
            continue
        if field.axis is None:
            if field.level == message.level:
                return field, 0
            continue
        levels = LEVEL_AXES[field.axis]
        if message.level in levels:
            return field, levels.index(message.level)
    return None


def polar_stereographic_axes(
    first_message: str, ni: int, nj: int
) -> tuple[np.ndarray, np.ndarray, dict]:
    """Nominal projected ``x``/``y`` (m) for a GRIB1 polar stereographic grid, and its CRS.

    Built from the grid header with the sphere and true latitude GRIB1 defines. The header
    rounds the first grid point and the spacing, so these are nominal; the model's own
    latitude/longitude fields are authoritative.
    """
    import eccodes
    import pyproj

    path, offset, length = first_message.rsplit("::", 2)
    with open(path, "rb") as handle:
        handle.seek(int(offset))
        raw = handle.read(int(length))
    gid = eccodes.codes_new_from_message(raw)
    try:
        lat1 = eccodes.codes_get(gid, "latitudeOfFirstGridPointInDegrees")
        lon1 = eccodes.codes_get(gid, "longitudeOfFirstGridPointInDegrees")
        lov = eccodes.codes_get(gid, "orientationOfTheGridInDegrees")
        dx = float(eccodes.codes_get(gid, "DxInMetres"))
        dy = float(eccodes.codes_get(gid, "DyInMetres"))
        south = bool(eccodes.codes_get(gid, "projectionCentreFlag") & 128)
        j_positive = bool(eccodes.codes_get(gid, "jScansPositively"))
        i_negative = bool(eccodes.codes_get(gid, "iScansNegatively"))
    finally:
        eccodes.codes_release(gid)

    sign = -1 if south else 1
    proj4 = (
        f"+proj=stere +lat_0={sign * 90} +lat_ts={sign * GRIB1_TRUE_LATITUDE} +lon_0={lov} "
        f"+x_0=0 +y_0=0 +R={GRIB1_EARTH_RADIUS} +units=m +no_defs"
    )
    crs = pyproj.CRS.from_proj4(proj4)
    to_xy = pyproj.Transformer.from_crs(crs.geodetic_crs, crs, always_xy=True)
    x0, y0 = to_xy.transform(lon1, lat1)
    x = x0 + np.arange(ni) * dx * (-1 if i_negative else 1)
    y = y0 + np.arange(nj) * dy * (1 if j_positive else -1)
    grid = {
        "grid_mapping_name": "polar_stereographic",
        "straight_vertical_longitude_from_pole": float(lov),
        "latitude_of_projection_origin": float(sign * 90),
        "standard_parallel": float(sign * GRIB1_TRUE_LATITUDE),
        "earth_radius": GRIB1_EARTH_RADIUS,
        "proj4": proj4,
    }
    return x, y, grid


class AMPSProvider(NaiveUTCPartitions, GenerationalStoreMixin, BaseProvider):
    """One AMPS domain, one partition per init time, forecast hours 0-48.

    Only the newest configuration, WRF24, is archived; init times before it are refused by
    :meth:`is_valid_init`, so the older MM5/WRF grids never reach a store. Within WRF24 the
    grids have not changed (checked in 2017, 2019, 2020, 2023 and 2026), and the store is
    generational only so that a grid change inside WRF24 would start a dated store rather
    than be refused. The grid is identified by the projected ``x``/``y`` axes, which come
    from the GRIB header and are exactly reproducible; the model's latitude/longitude
    fields are packed slightly differently from run to run, so they are stored but not
    compared. Model-level fields, which only some runs carried before October 2020, are
    NaN where a run lacks them rather than starting a generation.

    Args:
        domain: Domain key, one of :data:`DOMAINS` (case-insensitive), e.g. ``"d1"``.
        config: Configuration; defaults to the process-wide one.
        dask_threads: Chunks decoded and written concurrently. Each costs roughly
            3.5x its decoded size (a 21-level field of the 0.89 km nest is 58 MB).
        download_workers: Concurrent downloads from GDEX.
    """

    append_dim = "init_time"
    alignment_coords = ("step", "level", "model_level", "soil_depth", "y", "x")

    def __init__(
        self,
        domain: str = "d1",
        config: Config | None = None,
        dask_threads: int = 4,
        download_workers: int = 4,
    ):
        """Set up the provider for one domain; see the class docstring for the arguments.

        Raises:
            ValueError: On an unknown domain or a worker count below one.
        """
        super().__init__(config=config)
        try:
            self.domain = DOMAINS[domain.lower()]
        except KeyError:
            raise ValueError(
                f"unknown AMPS domain {domain!r}; expected one of {sorted(DOMAINS)}"
            ) from None
        if dask_threads < 1 or download_workers < 1:
            raise ValueError("dask_threads and download_workers must be at least 1")
        self.name = self.domain.store_name
        self.base_store_prefix = f"bkr/dmi/{self.domain.store_name}.icechunk"
        self.fields = fields_for(self.domain)
        self.dask_threads = dask_threads
        self.download_workers = download_workers

    # ------------------------------------------------------------------ fetching

    @property
    def steps(self) -> tuple[int, ...]:
        """Forecast hours stored for each init time."""
        return self.domain.step_hours

    def expected_urls(self, it: pd.Timestamp) -> List[str]:
        """Every file making up init time ``it``, in step order."""
        return [file_url(self.domain, it, step) for step in self.steps]

    def is_valid_init(self, it: pd.Timestamp) -> bool:
        """Whether AMPS ran this domain at ``it`` under the WRF24 configuration."""
        return (
            it.hour in INIT_HOURS
            and it.minute == 0
            and it.second == 0
            and it >= self.domain.first_init
        )

    @staticmethod
    def _published(url: str) -> bool:
        """Whether GDEX has ``url``, without downloading it."""
        try:
            response = requests.head(url, allow_redirects=True, timeout=60)
        except requests.RequestException as exc:
            logger.warning(f"could not check {url}: {exc}")
            return False
        return response.status_code == 200

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Download every file of init time ``it``, or nothing at all.

        A run is several GB, so the first and last forecast hours are checked first: a run
        GDEX has not finished publishing is "nothing to do" without downloading any of it.
        An incomplete set is likewise skipped rather than written with holes, which the
        already-stored check would then never revisit.

        Raises:
            RuntimeError: If a file is still truncated after being downloaded again.
        """
        if not self.is_valid_init(it):
            logger.warning(f"{self.name}: {it} is not an AMPS WRF24 init time for this domain")
            return []

        urls = self.expected_urls(it)
        if not (self._published(urls[-1]) and self._published(urls[0])):
            logger.info(f"{self.name}: {it} is not (fully) on GDEX yet, skipping")
            return []

        target = init_time_download_dir(self.config.scratch_dir, self.name, it, temp_dir)
        paths = download_many(urls, target, workers=self.download_workers)
        if len(paths) != len(urls):
            logger.warning(
                f"{self.name}: only {len(paths)}/{len(urls)} files available for {it}, skipping"
            )
            return []

        for url, path in zip(urls, paths):
            if grib_file_complete(path):
                continue
            logger.warning(f"{self.name}: {path.name} is truncated, downloading it again")
            again = download_one(url, path, overwrite=True)
            if again is None or not grib_file_complete(again):
                raise RuntimeError(f"{self.name}: {url} downloaded truncated twice")
        return [str(p) for p in paths]

    # ---------------------------------------------------------------- processing

    def index_files(self, input_files: Sequence[str], it: pd.Timestamp) -> FileIndex:
        """Locate every field of every forecast hour of init time ``it``.

        Raises:
            ValueError: If a file belongs to another init time or forecast hour, or is on
                a different grid from the first.
        """
        index = FileIndex()
        unmatched: set[tuple[int, str]] = set()
        for step_index, (path, step) in enumerate(zip(input_files, self.steps)):
            messages = scan_grib_file(path)
            if step_index == 0:
                index.first = messages[0]
            shape = (index.first.nj, index.first.ni)
            for message in messages:
                if message.init != it:
                    raise ValueError(f"{path}: init time {message.init}, expected {it}")
                if message.end_step != step:
                    raise ValueError(f"{path}: message for hour {message.end_step}, not {step}")
                if (message.nj, message.ni) != shape:
                    raise ValueError(f"{path}: grid {message.nj}x{message.ni}, expected {shape}")
                static = static_name(message)
                if static is not None:
                    index.statics.setdefault(static, message.uri)
                    continue
                match = match_message(message, self.fields)
                if match is None:
                    if message.param not in IGNORED_PARAMS:
                        unmatched.add((message.param, message.level_type))
                    continue
                field, level_index = match
                index.locations.setdefault((field.name, step_index, level_index), message.uri)
        if unmatched:
            logger.debug(f"{self.name}: {it} has parameters not stored: {sorted(unmatched)}")
        return index

    def static_coords(self, index: FileIndex) -> dict[str, xr.Variable]:
        """Latitude, longitude, orography and land/sea mask as 2-D coordinates.

        Each is taken from the first forecast hour that has it; early WRF24 runs only
        publish some of them at hour 0. One that no file has is stored as NaN.
        """
        shape = (index.first.nj, index.first.ni)
        coords = {}
        for field in STATIC_COORDS:
            uri = index.statics.get(field.name)
            if uri is None:
                logger.warning(f"{self.name}: no {field.name} field in the files; storing NaN")
                values = np.full(shape, np.nan)
            else:
                values = decode_message(uri).reshape(shape)
            coords[field.name] = xr.Variable(
                ("y", "x"),
                values.astype("float32"),
                attrs={"long_name": field.long_name, "units": field.units},
            )
        return coords

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Build the lazy dataset for one init time.

        Nothing but headers, the grid and the static fields is read here; the data is
        decoded chunk by chunk as it is written.

        Raises:
            ValueError: On the wrong number of files, or files that do not belong to ``it``.
        """
        if len(input_files) != len(self.steps):
            raise ValueError(
                f"{self.name}: expected {len(self.steps)} files for {it}, got {len(input_files)}"
            )
        index = self.index_files(input_files, it)
        locations = index.locations
        nj, ni = index.first.nj, index.first.ni
        x, y, grid = polar_stereographic_axes(index.first.uri, ni, nj)

        n_steps = len(self.steps)
        variables = []
        for field in self.fields:
            levels = LEVEL_AXES[field.axis] if field.axis else ()
            dims = ("init_time", "step", *((field.axis,) if field.axis else ()), "values")
            shape = (1, n_steps, *((len(levels),) if field.axis else ()), nj * ni)
            chunks = (1, 1, *shape[2:])

            def uri_for(index: tuple[int, ...], name: str = field.name) -> str | None:
                level_index = index[2] if len(index) > 2 else 0
                return locations.get((name, index[1], level_index))

            attrs = {"long_name": field.long_name, "units": field.units}
            attrs["grib1_parameter"] = field.param
            attrs["grib1_level_type"] = field.level_type
            if field.notes:
                attrs["comment"] = field.notes
            variables.append(
                FieldVariable(
                    name=field.name,
                    dims=dims,
                    shape=shape,
                    chunks=chunks,
                    uri_for=uri_for,
                    dtype=np.dtype(field.dtype),
                    attrs=attrs,
                )
            )

        absent = [f.name for f in self.fields if not any(key[0] == f.name for key in locations)]
        if absent:
            logger.info(f"{self.name}: {it} has no {absent}; stored as NaN")

        flat = lazy_field_dataset(
            variables,
            coords={},
            reader=read_into,
            fetch_workers=1,
            token=f"{self.name}-{it.isoformat()}",
        )
        data_vars = {}
        for name, var in flat.data_vars.items():
            dims = var.dims[:-1] + ("y", "x")
            data_vars[name] = xr.Variable(
                dims, var.data.reshape(var.shape[:-1] + (nj, ni)), attrs=var.attrs
            )

        coords: dict[str, object] = {
            "init_time": ("init_time", [it]),
            "step": ("step", pd.to_timedelta(list(self.steps), unit="h")),
            "y": ("y", y, {"units": "m", "standard_name": "projection_y_coordinate"}),
            "x": ("x", x, {"units": "m", "standard_name": "projection_x_coordinate"}),
            **self.static_coords(index),
        }
        for axis in {f.axis for f in self.fields if f.axis}:
            coords[axis] = (axis, np.asarray(LEVEL_AXES[axis]))
        ds = xr.Dataset(data_vars, coords=coords, attrs=self.dataset_attrs(grid))
        ds["level"].attrs.update(long_name="Pressure level", units="hPa")
        if "model_level" in ds.coords:
            ds["model_level"].attrs.update(long_name="WRF model level, counted from the ground")
        ds["soil_depth"].attrs.update(long_name="Top of the soil layer", units="cm")
        return ds

    def dataset_attrs(self, grid: Mapping[str, object]) -> dict:
        """Global attributes describing the source and the grid."""
        return {
            "title": f"AMPS WRF forecasts, domain {self.domain.key}: {self.domain.description}",
            "source": "Antarctic Mesoscale Prediction System (AMPS), WRF24 configuration",
            "institution": "NSF National Center for Atmospheric Research",
            "references": "https://gdex.ucar.edu/datasets/d473002/ (doi:10.5065/7GG6-7B09)",
            "nominal_resolution_km": self.domain.resolution_km,
            "forecast_hours": f"{self.steps[0]}-{self.steps[-1]}",
            "wind_components": "grid-relative (GRIB uvRelativeToGrid = 1)",
            "projection": dict(grid)["proj4"],
            "comment": (
                "x/y are nominal, rebuilt from the GRIB header; latitude/longitude are the "
                "model's own fields and are authoritative."
            ),
        }

    # ------------------------------------------------------------------ writing

    def write_to_icechunk(self, repo, processed: xr.Dataset) -> bool:
        """Stream the lazy dataset into ``repo`` on at most :attr:`dask_threads` threads."""
        with bounded_compute(self.dask_threads):
            return super().write_to_icechunk(repo, processed)


@dataclasses.dataclass
class FileIndex:
    """Where the fields of one init time live, from :meth:`AMPSProvider.index_files`.

    Attributes:
        locations: ``(variable, step index, level index)`` to the message holding it.
        statics: Static coordinate name to the first message holding it.
        first: The first message of the first file, which defines the grid.
    """

    locations: dict[tuple[str, int, int], str] = dataclasses.field(default_factory=dict)
    statics: dict[str, str] = dataclasses.field(default_factory=dict)
    first: GribMessage | None = None


def static_name(message: GribMessage) -> str | None:
    """The static coordinate a message holds, or None if it is not one."""
    for field in STATIC_COORDS:
        if (message.param, message.level_type, message.level) == (
            field.param,
            field.level_type,
            field.level,
        ):
            return field.name
    return None


#: GRIB messages end with these four bytes.
_GRIB_END = b"7777"


def grib_file_complete(path: str | pathlib.Path) -> bool:
    """Cheap check that a downloaded GRIB file was not cut short.

    The OSDF caches in front of GDEX have been seen to end a transfer early; a cut file no
    longer ends with the ``7777`` end-of-message marker.
    """
    path = pathlib.Path(path)
    try:
        size = path.stat().st_size
        if size < 8:
            return False
        with open(path, "rb") as handle:
            if handle.read(4) != b"GRIB":
                return False
            handle.seek(size - 4)
            return handle.read(4) == _GRIB_END
    except OSError:
        return False


__all__ = [
    "AMPSDomain",
    "AMPSField",
    "AMPSProvider",
    "DOMAINS",
    "FIELDS",
    "GDEX_BASE_URL",
    "STATIC_COORDS",
    "WRF24_START",
    "file_url",
    "fields_for",
    "grib_file_complete",
    "match_message",
    "scan_grib_file",
]
