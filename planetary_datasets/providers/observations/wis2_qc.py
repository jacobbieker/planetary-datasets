"""Per-observation quality flags for the groups of the WIS2 store.

Every observation group of ``bkr/obs/wis2.icechunk`` (see
:mod:`~planetary_datasets.providers.observations.wis2_targets`) is flagged here, just before
each append: :func:`flag` takes the dataset a target is about to write and returns it with
flag variables added. **Values are never altered, masked or dropped**; a flag only says
what a check found, and a reader chooses what to do with it. Quality information a source
sends (BUFR quality fields, instrument flags) is kept as it is; where its meaning is
unambiguous it also sets the :data:`PROVIDER` bit, so one variable answers "did anything
find a problem".

Variables added
    ``quality_flag`` (uint16)
        One per observation, along the group's first dimension (``observation``,
        ``sounding``, ``profile``, ``volume``). The bits below; 0 means no check found
        anything (including when nothing could be checked).
    ``level_quality_flag`` (uint16)
        Profiles only, one per level (``level``, ``profile_level``): the bits of the level's
        own checks. A level's bits are also set in its profile's ``quality_flag``.
    ``<variable>_quality`` (uint8)
        For each variable range- or consistency-checked here, the same shape as the
        variable, except that the repeated-occurrence dimensions of satellite products
        (``<name>_n``, whose width changes between windows) are collapsed (any occurrence).
        Bits 1-8 below. A source variable already named like this keeps its name and the
        flag is called ``<variable>_quality_wis2``.

    Every flag variable has CF ``flag_masks`` and ``flag_meanings``.

Bits of ``quality_flag`` / ``level_quality_flag`` (the first four also of ``<var>_quality``)

    ======  =================================  ==============================================
    value   meaning                            set when
    ======  =================================  ==============================================
    1       ``outside_physical_range``         a value no instrument could measure in the
                                               atmosphere or ocean (e.g. 194 K is possible,
                                               40 K is not; a negative wind speed)
    2       ``outside_plausible_range``        a value beyond the gross-error (climatological)
                                               limits used by NWP centres: possible in
                                               principle, far more likely an error
    4       ``internally_inconsistent``        values of one report contradict each other:
                                               dewpoint above temperature, maximum below
                                               minimum, calm with a direction (or a speed
                                               with direction 0), gust below the mean wind,
                                               a shorter-period precipitation total above a
                                               longer one, radar sweep times outside the
                                               volume or ending before they start
    8       ``vertically_inconsistent``        profiles: a sounding's pressure not falling
                                               along its time since launch, geopotential
                                               height falling as pressure falls (both levels
                                               of the pair), potential temperature dropping
                                               more than :data:`THETA_DROP` between adjacent
                                               levels (superadiabatic), an ocean profile's
                                               pressure (or depth) not strictly monotonic
                                               (Argo test 8), a wind profile's heights or an
                                               occultation's impact parameters out of order
    16      ``position_invalid``               latitude or longitude missing or out of range
    32      ``position_suspect``               exactly 0 N 0 E ("null island")
    64      ``time_invalid``                   the observation time is missing
    128     ``time_in_future``                 observed more than :data:`FUTURE_TOLERANCE`
                                               after it reached the cache
    256     ``time_too_old``                   observed longer before it reached the cache
                                               than the group allows (:class:`Spec`
                                               ``max_age``; 10 days for weather reports)
    512     ``duplicate``                      another row of the same append has the same
                                               platform and time and arrived later or is a
                                               later correction: this row is superseded
                                               (groups whose reports can legitimately repeat
                                               a station and time, sounding parts, satellite
                                               fields of view, are not checked)
    1024    ``provider_flagged``               the source's own quality information marks the
                                               observation suspect or bad (see below)
    ======  =================================  ==============================================

Quality information mapped to bit 1024
    aircraft ``moisture_quality`` (BUFR 0 33 026) 3-8 (wet, contaminated, heater failure,
    invalid input, numeric error) and ``roll_angle_quality`` (0 02 064) 1 (bad); wind
    profiler ``wind_quality`` / ``vertical_wind_quality`` per level: 0 33 002 = 1 (suspect),
    or by their associated-field significance (0 31 021): 1-bit indicator 1, 2-bit
    indicator >= 2, GTSPP class 3 or 4, percent confidence below 50; IASI
    ``gqis_quality_flag`` (0 33 060) 1 (bad); GNSS-RO ``radio_occultation_data_quality_flags``
    (0 33 039) bit 1, non-nominal quality. Kept but not mapped, as their codes do not say
    "bad": hydrometric ``*_qa_qc`` (Water Survey of Canada review state: 1 preliminary,
    2 reviewed, 3 checked, 4 approved), the ATOVS/ATMS/CrIS scan, field-of-view and
    geolocation flag tables, AMV quality indices and the other satellite products' flags.

Ranges
    Explicit tables below (:data:`SURFACE`, :data:`UPPER_AIR`, ...), per group, in the
    stored units, each with its source. Gross-error limits follow WMO-No. 305 (Guide on the
    Global Data-processing System, Attachment II.1, "plausible values" of surface
    variables), WMO-No. 8 (CIMO Guide) and the ECMWF/NCEP blacklist and gross checks for
    upper air; ocean profiles follow the Argo real-time QC global range test (Argo Quality
    Control Manual, test 6). BUFR special values are recognised and never flagged:
    precipitation -0.1 kg m-2 (trace), snow depth -0.01 m (snow cover not continuous) and
    -0.02 m (could not be measured). Satellite products repeat a BUFR element under one
    name (``<name>_n``): only its first occurrence, the value itself, is range-checked, as
    later ones are often its statistics or other channels.

Duplicates are judged within the rows of one append (a window, or a batch of one): rows
written in earlier windows are never changed, so a correction arriving later leaves the
earlier row unflagged.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
from typing import Callable, Mapping, Sequence

import numpy as np
import xarray as xr
from loguru import logger

#: Version of the checks, recorded on every flag variable.
QC_VERSION = "1"

PHYSICAL = 1
PLAUSIBLE = 2
INCONSISTENT = 4
VERTICAL = 8
POSITION_INVALID = 16
POSITION_SUSPECT = 32
TIME_INVALID = 64
TIME_FUTURE = 128
TIME_OLD = 256
DUPLICATE = 512
PROVIDER = 1024

#: Bits of ``quality_flag`` and ``level_quality_flag``, in order.
OBSERVATION_BITS = (
    (PHYSICAL, "outside_physical_range"),
    (PLAUSIBLE, "outside_plausible_range"),
    (INCONSISTENT, "internally_inconsistent"),
    (VERTICAL, "vertically_inconsistent"),
    (POSITION_INVALID, "position_invalid"),
    (POSITION_SUSPECT, "position_suspect"),
    (TIME_INVALID, "time_invalid"),
    (TIME_FUTURE, "time_in_future"),
    (TIME_OLD, "time_too_old"),
    (DUPLICATE, "duplicate"),
    (PROVIDER, "provider_flagged"),
)
#: Bits of each ``<variable>_quality``.
VARIABLE_BITS = OBSERVATION_BITS[:4]

#: How far after its arrival in the cache an observation may be timed (clock error).
FUTURE_TOLERANCE = np.timedelta64(15 * 60, "s")
#: Potential temperature may fall by this much between adjacent levels before the layer
#: counts as (grossly) superadiabatic. Shallow superadiabatic layers over heated ground
#: are real but fall short of this; ECMWF's and NCEP's lapse-rate checks use similar
#: gross limits.
THETA_DROP = 5.0
#: Tolerances of the consistency checks, in the variables' units: rounding of reported
#: values (0.1 K, 1 kt = 0.51 m s-1, 0.1 mm) must not be flagged.
DEWPOINT_TOLERANCE = 0.2
GUST_TOLERANCE = 1.0
PRECIPITATION_TOLERANCE = 0.5
#: Radar sweeps may start this long outside their volume's start and end times.
SWEEP_TOLERANCE = 15 * 60.0


@dataclasses.dataclass(frozen=True)
class Range:
    """Limits of one variable, in its stored units.

    Attributes:
        low, high: What is physically possible; outside sets bit 1.
        plausible_low, plausible_high: Gross-error limits; outside (but possible) sets 2.
        specials: Values with a coded meaning (BUFR trace, say), never flagged.
    """

    low: float
    high: float
    plausible_low: float
    plausible_high: float
    specials: tuple[float, ...] = ()


def R(possible: tuple[float, float], plausible: tuple[float, float], *specials: float) -> Range:
    """A :class:`Range` from (low, high) pairs: physically possible, then plausible."""
    return Range(possible[0], possible[1], plausible[0], plausible[1], tuple(specials))


DIRECTION = R((0, 360), (0, 360))
#: Air temperature near the surface: WMO-No. 305 gross limits -80..+60 C; the world
#: records are -89.2 C (Vostok) and +56.7 C (Death Valley). Physically possible: 150-350 K.
AIR_TEMPERATURE = R((150, 350), (193.15, 333.15))
#: Dewpoint: WMO-No. 305 -80..+35 C, extended to -90 C for the Antarctic plateau.
DEWPOINT = R((100, 350), (183.15, 308.15))
#: Relative humidity: up to 100% (and a few % of sensor supersaturation); over 105% is
#: impossible (the "RH > ~105%" consistency rule).
RELATIVE_HUMIDITY = R((0, 105), (0, 102))
#: Station pressure: 400..1080 hPa in WMO-No. 305; possible from the highest stations
#: (~500 hPa at 5,000 m; Everest ~330 hPa) to 1100 hPa.
STATION_PRESSURE = R((20000, 110000), (40000, 108500))
#: Sea-level pressure: records 870 hPa (Typhoon Tip) and 1084.8 hPa (Tosontsengel).
SEA_LEVEL_PRESSURE = R((80000, 110000), (87000, 108500))
#: Surface wind: WMO-No. 305 0..75 m s-1 for the mean wind; the highest gust measured is
#: 113 m s-1 (Barrow Island, 1996).
SURFACE_WIND = R((0, 120), (0, 75))
GUST = R((0, 150), (0, 100))
#: Precipitation totals, kg m-2 (mm), over their period; plausible up to the world
#: records rounded up (305 mm in 1 h, 1825 mm in 24 h). -0.1 is BUFR's trace.
TRACE = -0.1


def _precipitation(plausible: float) -> Range:
    return R((0, 5000), (0, plausible), TRACE)


PRECIPITATION = {
    "total_precipitation_1h": _precipitation(400),
    "total_precipitation_3h": _precipitation(600),
    "total_precipitation_6h": _precipitation(900),
    "total_precipitation_12h": _precipitation(1200),
    "total_precipitation_24h": _precipitation(1900),
}
#: Snow depth, m: record 11.8 m (Mt Ibuki); -0.01 (not continuous) and -0.02 (could not
#: be measured) are BUFR specials.
SNOW_DEPTH = R((0, 50), (0, 12), -0.01, -0.02)
#: Sea surface temperature, K: Argo global range -2.5..40 C as plausible.
SEA_TEMPERATURE = R((263.15, 323.15), (270.65, 313.15))
#: Salinity, PSU (1e-3): Argo global range 2..41.
SALINITY = R((0, 50), (2, 41))
WAVE_HEIGHT = R((0, 40), (0, 25))
WAVE_PERIOD = R((0, 60), (0, 30))
STATION_HEIGHT = R((-500, 9000), (-450, 5500))

#: Land station reports (BUFR and TAC), in the land groups' names and units.
SURFACE: dict[str, Range] = {
    "2m_temperature": AIR_TEMPERATURE,
    "maximum_2m_temperature": AIR_TEMPERATURE,
    "minimum_2m_temperature": AIR_TEMPERATURE,
    "2m_dewpoint_temperature": DEWPOINT,
    "2m_relative_humidity": RELATIVE_HUMIDITY,
    "surface_pressure": STATION_PRESSURE,
    "mean_sea_level_pressure": SEA_LEVEL_PRESSURE,
    # 3-hour tendency: WMO-No. 305 0..50 hPa; 24-hour: twice that is beyond any storm.
    "pressure_change_3h": R((-10000, 10000), (-5000, 5000)),
    "pressure_change_24h": R((-15000, 15000), (-6000, 6000)),
    "10m_wind_speed": SURFACE_WIND,
    "10m_wind_gust": GUST,
    "10m_wind_direction": DIRECTION,
    "horizontal_visibility": R((0, 150000), (0, 100000)),
    **PRECIPITATION,
    "snow_depth": SNOW_DEPTH,
    "cloud_base_height": R((0, 30000), (0, 21000)),
    # Stored as the reported percentage (decoded to 0-1 through scale_factor).
    "total_cloud_cover": R((0, 100), (0, 100)),
    "total_cloud_cover_oktas": R((0, 9), (0, 9)),
    "low_or_middle_cloud_amount": R((0, 9), (0, 9)),
    "sunshine_duration_1h": R((0, 3600), (0, 3600)),
    "sunshine_duration_24h": R((0, 86400), (0, 86400)),
    "station_elevation": STATION_HEIGHT,
}

#: Ships, buoys and wave buoys (BUFR and TAC), in the ocean groups' names and units.
OCEAN_SURFACE: dict[str, Range] = {
    "surface_air_pressure": SEA_LEVEL_PRESSURE,
    "mean_sea_level_pressure": SEA_LEVEL_PRESSURE,
    "pressure_tendency_3h": R((-10000, 10000), (-5000, 5000)),
    "air_temperature": AIR_TEMPERATURE,
    "dewpoint_temperature": DEWPOINT,
    "relative_humidity": RELATIVE_HUMIDITY,
    "wind_speed": SURFACE_WIND,
    "wind_gust": GUST,
    "wind_direction": DIRECTION,
    "horizontal_visibility": R((0, 150000), (0, 100000)),
    "total_cloud_cover": R((0, 100), (0, 100)),
    "total_cloud_cover_oktas": R((0, 9), (0, 9)),
    "low_or_middle_cloud_amount": R((0, 9), (0, 9)),
    "sea_surface_temperature": SEA_TEMPERATURE,
    "skin_temperature": AIR_TEMPERATURE,
    "sea_surface_salinity": SALINITY,
    # Significant wave height: the highest measured is ~19 m; individual waves ~30 m.
    "significant_height_of_combined_wind_waves_and_swell": WAVE_HEIGHT,
    "significant_height_of_wind_waves": WAVE_HEIGHT,
    "significant_height_of_first_swell_partition": WAVE_HEIGHT,
    "maximum_individual_wave_height": R((0, 50), (0, 35)),
    "mean_wave_period": WAVE_PERIOD,
    "peak_wave_period": WAVE_PERIOD,
    "mean_period_of_wind_waves": WAVE_PERIOD,
    "mean_period_of_first_swell_partition": WAVE_PERIOD,
    "mean_wave_direction": DIRECTION,
    "mean_direction_of_wind_waves": DIRECTION,
    "mean_direction_of_first_swell_partition": DIRECTION,
    "wave_directional_spread": DIRECTION,
    "sea_surface_current_speed": R((0, 10), (0, 5)),
    "sea_surface_current_direction": DIRECTION,
    "sea_floor_depth": R((0, 12000), (0, 11000)),
    "platform_speed": R((0, 100), (0, 30)),
    "platform_direction": DIRECTION,
}

#: Ocean profile levels: the Argo real-time QC global range test (test 6): temperature
#: -2.5..40 C, salinity 2..41, pressure above -5 dbar; oxygen 0..600 umol kg-1 (Argo
#: DOXY). Pressure is in Pa (1 dbar = 1e4 Pa).
OCEAN_PROFILE: dict[str, Range] = {
    "sea_water_pressure": R((-5e4, 1.2e8), (-5e4, 7e7)),
    "depth": R((-5, 12000), (-5, 11000)),
    "sea_water_temperature": SEA_TEMPERATURE,
    "sea_water_salinity": SALINITY,
    "dissolved_oxygen": R((0, 700), (0, 600)),
    "current_speed": R((0, 10), (0, 5)),
    "current_direction": DIRECTION,
}

#: Upper-air temperature: the tropical tropopause and polar stratosphere reach ~180 K; a
#: sounding's surface level ~330 K. ECMWF/NCEP gross limits are of this order.
UPPER_TEMPERATURE = R((130, 350), (160, 333.15))
UPPER_DEWPOINT = R((100, 350), (140, 308.15))
#: Jet-stream winds reach ~110 m s-1.
UPPER_WIND = R((0, 200), (0, 120))

#: Sounding levels (radiosondes, dropsondes, PILOT).
UPPER_AIR: dict[str, Range] = {
    # Sondes burst near 3 hPa (300 Pa); nothing reports below 100 Pa or above 1085 hPa.
    "pressure": R((1, 110000), (100, 108500)),
    "temperature": UPPER_TEMPERATURE,
    "dewpoint_temperature": UPPER_DEWPOINT,
    "geopotential_height": R((-1000, 80000), (-500, 45000)),
    "wind_speed": UPPER_WIND,
    "wind_direction": DIRECTION,
}
SOUNDING: dict[str, Range] = {
    "station_elevation": STATION_HEIGHT,
    "release_height": STATION_HEIGHT,
}

#: Aircraft reports: flight levels to ~16 km (business jets); 50 C on a hot apron.
AIRCRAFT: dict[str, Range] = {
    "temperature": R((150, 350), (183.15, 328.15)),
    "dewpoint_temperature": UPPER_DEWPOINT,
    "relative_humidity": RELATIVE_HUMIDITY,
    "humidity_mixing_ratio": R((0, 0.05), (0, 0.035)),
    "wind_speed": UPPER_WIND,
    "wind_direction": DIRECTION,
    "flight_level": R((-1000, 25000), (-400, 16000)),
    "gnss_altitude": R((-1000, 25000), (-400, 16000)),
    "true_airspeed": R((0, 400), (0, 340)),
    "mean_eddy_dissipation_rate": R((0, 2), (0, 1)),
    "peak_eddy_dissipation_rate": R((0, 2), (0, 1)),
}

#: Wind profiler levels: tropospheric profilers reach ~16 km, MST radars ~20 km; vertical
#: velocity in rain is a fall speed of up to ~10 m s-1.
WIND_PROFILER_LEVELS: dict[str, Range] = {
    "height": R((-500, 30000), (-450, 20000)),
    "height_above_station": R((0, 30000), (0, 20000)),
    "geopotential_height": R((-500, 40000), (-450, 35000)),
    "wind_speed": UPPER_WIND,
    "wind_direction": DIRECTION,
    "eastward_wind": R((-200, 200), (-120, 120)),
    "northward_wind": R((-200, 200), (-120, 120)),
    "upward_air_velocity": R((-50, 50), (-20, 20)),
}
WIND_PROFILER: dict[str, Range] = {"station_elevation": STATION_HEIGHT}

#: Water Survey of Canada: stage is against a gauge datum (sometimes geodetic, hundreds of
#: metres); discharge peaks at ~30,000 m3 s-1 (Mackenzie, St Lawrence). Tidal reaches can
#: report a negative discharge, which is possible but flagged as implausible.
HYDROMETRIC: dict[str, Range] = {
    "water_level": R((-500, 6000), (-100, 5000)),
    "discharge": R((-1e5, 3e5), (0, 40000)),
}

#: Radar volumes: elevations below -2 degrees see the ground; Nyquist velocities are a few
#: tens of m s-1.
RADAR: dict[str, Range] = {
    "elevation": R((-90, 90), (-2, 90)),
    "nyquist_velocity": R((0, 150), (0, 100)),
    "antenna_height": STATION_HEIGHT,
}

#: Brightness temperatures: nothing on Earth is warmer than ~350 K seen from space; deep
#: convection scatters 89-190 GHz radiances down to ~70 K, the cosmic background is 2.7 K.
BRIGHTNESS_TEMPERATURE = R((50, 360), (100, 340))
RADIANCES: dict[str, Range] = {
    "brightness_temperature": BRIGHTNESS_TEMPERATURE,
    "satellite_zenith_angle": R((-90, 90), (-90, 90)),
    "solar_zenith_angle": R((0, 180), (0, 180)),
    "land_fraction": R((0, 1), (0, 1)),
}

#: Satellite products, by BUFR element name in ecCodes' (SI) units.
PRODUCT: dict[str, Range] = {
    # AMVs and SAR/radiometer winds.
    "wind_speed": UPPER_WIND,
    "wind_direction": DIRECTION,
    "pressure": R((100, 110000), (5000, 105000)),
    "air_temperature": UPPER_TEMPERATURE,
    "brightness_temperature": BRIGHTNESS_TEMPERATURE,
    # Scatterometer 10 m winds: ASCAT and HY-2 saturate near 50 m s-1.
    "wind_speed_at10_m": R((0, 100), (0, 50)),
    "wind_direction_at10_m": DIRECTION,
    "wind_speed_from_radiometer": R((0, 100), (0, 50)),
    # GNSS-RO: bending angles from ~1e-8 rad at 60 km to a few 1e-2 near the surface
    # (super-refraction more); noise makes them slightly negative high up. Impact
    # parameters lie between ~6,300 and 6,500 km (Earth's radius plus up to 80 km).
    "bending_angle": R((-0.01, 0.2), (-1e-4, 0.08)),
    "impact_parameter": R((6.2e6, 6.6e6), (6.3e6, 6.5e6)),
    "atmospheric_refractivity": R((0, 600), (0, 500)),
    # Altimeter and SAR wave heights.
    "ku_band_significant_wave_height": WAVE_HEIGHT,
    "c_band_significant_wave_height": WAVE_HEIGHT,
    "height_of_wind_waves": WAVE_HEIGHT,
    "significant_swell_wave_height_of_spectral_partition": WAVE_HEIGHT,
    "satellite_zenith_angle": R((-90, 90), (-90, 90)),
    "solar_zenith_angle": R((0, 180), (0, 180)),
}


# ----------------------------------------------------------------------------- checks


class Flags:
    """The flags of one dataset being built: per observation, per level, per variable."""

    def __init__(self, ds: xr.Dataset, spec: "Spec"):
        """Empty flags for ``ds`` as ``spec`` describes it."""
        self.ds = ds
        self.spec = spec
        self.observation = np.zeros(ds.sizes.get(spec.dim, 0), dtype="uint16")
        self.level = (
            np.zeros(ds.sizes.get(spec.levels.dim, 0), dtype="uint16") if spec.levels else None
        )
        #: name -> uint8 flags, the shape of the variable less its ``_n`` dimensions.
        self.variables: dict[str, np.ndarray] = {}

    def values(self, name: str) -> np.ndarray | None:
        """A variable as float64 with every missing value NaN; None if absent."""
        if name not in self.ds.variables:
            return None
        return as_float(self.ds[name].variable)

    def _kept_axes(self, name: str) -> tuple[int, ...]:
        """The axes of a variable its flag keeps (not the ``<name>_n`` repetitions)."""
        dims = self.ds[name].dims
        return tuple(i for i, d in enumerate(dims) if i == 0 or not str(d).endswith("_n"))

    def mark(self, name: str, mask: np.ndarray, bit: int) -> None:
        """Set ``bit`` on variable ``name`` where ``mask`` holds, and on its observation.

        ``mask`` has the variable's full shape; ``bit`` goes into ``<name>_quality`` (when it
        is one of the variable bits) and into the row's ``quality_flag`` or, for a variable
        on the levels, ``level_quality_flag``.
        """
        mask = np.asarray(mask, dtype=bool)
        if not mask.any():
            return
        variable = self.ds[name]
        kept = self._kept_axes(name)
        collapse = tuple(i for i in range(mask.ndim) if i not in kept)
        reduced = mask.any(axis=collapse) if collapse else mask
        if bit in (PHYSICAL, PLAUSIBLE, INCONSISTENT, VERTICAL):
            flags = self.variables.setdefault(name, np.zeros(reduced.shape, dtype="uint8"))
            flags[reduced] |= np.uint8(bit)
        rows = mask.reshape(mask.shape[0], -1).any(axis=1) if mask.ndim > 1 else mask
        self.row(variable.dims[0], rows, bit)

    def row(self, dim: str, mask: np.ndarray, bit: int) -> None:
        """Set ``bit`` on the observations (or levels, by ``dim``) where ``mask`` holds."""
        if dim == self.spec.dim:
            self.observation[np.asarray(mask, dtype=bool)] |= np.uint16(bit)
        elif self.spec.levels is not None and dim == self.spec.levels.dim:
            self.level[np.asarray(mask, dtype=bool)] |= np.uint16(bit)


def as_float(variable: xr.Variable) -> np.ndarray:
    """A numeric variable's values as float64, its fill and ecCodes' missing markers NaN."""
    values = np.asarray(variable.values)
    if values.dtype.kind == "f":
        return values.astype("float64", copy=False)
    if values.dtype.kind not in "iub":
        return np.full(values.shape, np.nan)
    out = values.astype("float64")
    missing = np.zeros(values.shape, dtype=bool)
    fill = variable.attrs.get("_FillValue", variable.encoding.get("_FillValue"))
    if fill is not None:
        missing |= values == fill
    if values.dtype.kind in "iu":
        info = np.iinfo(values.dtype)
        missing |= (values == info.min) | (values == info.max)
        missing |= values == 2147483647  # ecCodes' missing long, stored in an int64
    out[missing] = np.nan
    return out


def as_times(variable: xr.Variable) -> np.ndarray:
    """A time variable as datetime64[s]: datetimes, or numbers with CF ``units``.

    Integer counts (``seconds since 1970-01-01``, ``milliseconds since ...``) with their
    fill, and float seconds (radar), are decoded; anything else reads as NaT.
    """
    values = np.asarray(variable.values)
    if values.dtype.kind == "M":
        return values.astype("datetime64[s]")
    units = str(variable.attrs.get("units", ""))
    unit, _, since = units.partition(" since ")
    factors = {"seconds": 1.0, "milliseconds": 1e-3, "microseconds": 1e-6, "minutes": 60.0}
    out = np.full(values.shape, np.datetime64("NaT", "s"), dtype="datetime64[s]")
    if not since or unit.strip() not in factors:
        return out
    seconds = as_float(variable) * factors[unit.strip()]
    try:
        reference = np.datetime64(since.strip().replace(" ", "T")[:19], "s")
    except ValueError:
        return out
    valid = np.isfinite(seconds)
    out[valid] = reference + np.floor(seconds[valid]).astype("int64").astype("timedelta64[s]")
    return out


def _check_ranges(flags: Flags, ranges: Mapping[str, Range]) -> None:
    for name, limits in ranges.items():
        values = flags.values(name)
        if values is None:
            continue
        if values.ndim > 1 and any(str(d).endswith("_n") for d in flags.ds[name].dims[1:]):
            # A satellite product's repeated element: the first occurrence is the value,
            # later ones are often its statistics (standard deviation, confidence) or other
            # channels under the same name, so only the first is range-checked.
            values = values.copy()
            values[:, 1:] = np.nan
        valid = np.isfinite(values)
        for special in limits.specials:
            valid &= ~np.isclose(values, special, rtol=0, atol=1e-6)
        physical = valid & ((values < limits.low) | (values > limits.high))
        plausible = (
            valid & ~physical & ((values < limits.plausible_low) | (values > limits.plausible_high))
        )
        flags.mark(name, physical, PHYSICAL)
        flags.mark(name, plausible, PLAUSIBLE)


def _check_position(flags: Flags) -> None:
    spec = flags.spec
    if spec.latitude is None or spec.latitude not in flags.ds or spec.longitude not in flags.ds:
        return
    lat, lon = flags.values(spec.latitude), flags.values(spec.longitude)
    if lat.ndim > 1:
        lat, lon = lat[:, 0], lon[:, 0]
    invalid = ~np.isfinite(lat) | ~np.isfinite(lon)
    with np.errstate(invalid="ignore"):
        invalid |= (np.abs(lat) > 90) | (lon < -180) | (lon > 360)
    flags.row(spec.dim, invalid, POSITION_INVALID)
    flags.row(spec.dim, ~invalid & (lat == 0) & (lon == 0), POSITION_SUSPECT)


def _check_time(flags: Flags, now: np.datetime64) -> None:
    spec = flags.spec
    if spec.time is None or spec.time not in flags.ds:
        return
    times = as_times(flags.ds[spec.time].variable)
    flags.row(spec.dim, np.isnat(times), TIME_INVALID)
    arrival = np.full(times.shape, now, dtype="datetime64[s]")
    for name in reversed(spec.arrival):
        if name in flags.ds:
            arrived = as_times(flags.ds[name].variable)
            arrival = np.where(np.isnat(arrived), arrival, arrived)
    known = ~np.isnat(times)
    flags.row(spec.dim, known & (times > arrival + FUTURE_TOLERANCE), TIME_FUTURE)
    age = np.timedelta64(int(spec.max_age.total_seconds()), "s")
    flags.row(spec.dim, known & (times < arrival - age), TIME_OLD)


def _check_duplicates(flags: Flags) -> None:
    """Every row but the last (by ``duplicate_order``) of each platform and time."""
    import pandas as pd

    spec = flags.spec
    names = [n for n in spec.duplicate_key if n in flags.ds]
    if len(names) < 2 or spec.time not in names:
        return
    table = {}
    for name in names:
        values = flags.ds[name].values
        if name == spec.time:
            values = as_times(flags.ds[name].variable).astype("int64")
        table[name] = np.asarray(values)
    frame = pd.DataFrame(table)
    identifiers = [n for n in names if n != spec.time]
    usable = ~np.isnat(as_times(flags.ds[spec.time].variable))
    usable &= (frame[identifiers].astype(str) != "").any(axis=1).to_numpy()
    order = [n for n in spec.duplicate_order if n in flags.ds]
    for name in order:
        variable = flags.ds[name].variable
        is_time = variable.dtype.kind == "M" or " since " in str(variable.attrs.get("units", ""))
        # NaT is int64's minimum, so a row without an arrival sorts first (superseded).
        frame[name] = as_times(variable).astype("int64") if is_time else as_float(variable)
    frame["_row"] = np.arange(len(frame))
    frame = frame[usable]
    if frame.empty:
        return
    frame = frame.sort_values([*names, *order, "_row"], kind="stable")
    superseded = frame.duplicated(subset=names, keep="last")
    mask = np.zeros(flags.observation.size, dtype=bool)
    mask[frame["_row"].to_numpy()[superseded.to_numpy()]] = True
    flags.row(spec.dim, mask, DUPLICATE)


# ----------------------------------------------------------------- consistency checks


def dewpoint(temperature: str, dew: str) -> Callable[[Flags], None]:
    """Dewpoint above the air temperature (beyond rounding) is impossible."""

    def check(flags: Flags) -> None:
        t, d = flags.values(temperature), flags.values(dew)
        if t is None or d is None:
            return
        with np.errstate(invalid="ignore"):
            bad = d > t + DEWPOINT_TOLERANCE
        flags.mark(temperature, bad, INCONSISTENT)
        flags.mark(dew, bad, INCONSISTENT)

    return check


def maximum_minimum(
    high: str, low: str, high_period: str | None = None, low_period: str | None = None
) -> Callable[[Flags], None]:
    """A maximum below the minimum, where both cover the same period.

    With period variables, only rows whose periods are equal are checked: extremes over
    different periods may legitimately cross.
    """

    def check(flags: Flags) -> None:
        hi, lo = flags.values(high), flags.values(low)
        if hi is None or lo is None:
            return
        same = np.ones(hi.shape, dtype=bool)
        if high_period and low_period:
            hp, lp = flags.values(high_period), flags.values(low_period)
            if hp is None or lp is None:
                return
            same = hp == lp
        with np.errstate(invalid="ignore"):
            bad = same & (hi < lo - DEWPOINT_TOLERANCE)
        flags.mark(high, bad, INCONSISTENT)
        flags.mark(low, bad, INCONSISTENT)

    return check


def calm(speed: str, direction: str) -> Callable[[Flags], None]:
    """BUFR 0 11 001: direction 0 is calm (north is 360), so 0 goes with speed 0 only."""

    def check(flags: Flags) -> None:
        s, d = flags.values(speed), flags.values(direction)
        if s is None or d is None:
            return
        with np.errstate(invalid="ignore"):
            bad = ((s == 0) & (d > 0)) | ((s > 0) & (d == 0))
        flags.mark(speed, bad, INCONSISTENT)
        flags.mark(direction, bad, INCONSISTENT)

    return check


def gust(gust_name: str, speed: str) -> Callable[[Flags], None]:
    """A gust below the mean wind it is the maximum of."""

    def check(flags: Flags) -> None:
        g, s = flags.values(gust_name), flags.values(speed)
        if g is None or s is None:
            return
        with np.errstate(invalid="ignore"):
            bad = g < s - GUST_TOLERANCE
        flags.mark(gust_name, bad, INCONSISTENT)
        flags.mark(speed, bad, INCONSISTENT)

    return check


def precipitation_nesting(names: Sequence[str]) -> Callable[[Flags], None]:
    """A shorter period's precipitation total above a longer one's.

    Every total of a report ends at its time, so they nest. Traces (-0.1) count as 0.
    """

    def check(flags: Flags) -> None:
        present = [(n, flags.values(n)) for n in names if n in flags.ds]
        for i, (short, a) in enumerate(present):
            for long, b in present[i + 1 :]:
                a0, b0 = np.where(a == TRACE, 0.0, a), np.where(b == TRACE, 0.0, b)
                with np.errstate(invalid="ignore"):
                    bad = a0 > b0 + PRECIPITATION_TOLERANCE
                flags.mark(short, bad, INCONSISTENT)
                flags.mark(long, bad, INCONSISTENT)

    return check


def radar_times(flags: Flags) -> None:
    """Radar sweeps outside their volume's times, or ending before they start.

    A sweep must start within its volume (give or take :data:`SWEEP_TOLERANCE`) and end
    after it starts; a volume must end after it starts.
    """
    start, end = flags.values("start_time"), flags.values("end_time")
    sweep_start, sweep_end = flags.values("sweep_start_time"), flags.values("sweep_end_time")
    with np.errstate(invalid="ignore"):
        if start is not None and end is not None:
            bad = end < start
            flags.mark("start_time", bad, INCONSISTENT)
            flags.mark("end_time", bad, INCONSISTENT)
        if sweep_start is not None and sweep_end is not None:
            bad = sweep_end < sweep_start
            flags.mark("sweep_start_time", bad, INCONSISTENT)
            flags.mark("sweep_end_time", bad, INCONSISTENT)
        if sweep_start is not None and start is not None and end is not None:
            outside = (sweep_start < start[:, None] - SWEEP_TOLERANCE) | (
                sweep_start > end[:, None] + SWEEP_TOLERANCE
            )
            flags.mark("sweep_start_time", outside, INCONSISTENT)


# -------------------------------------------------------------------- provider flags


def provider_codes(name: str, bad: Sequence[int]) -> Callable[[Flags], None]:
    """Rows (or levels) whose ``name`` holds one of the ``bad`` codes."""

    def check(flags: Flags) -> None:
        values = flags.values(name)
        if values is None:
            return
        mask = np.isin(values, np.asarray(bad, dtype="float64"))
        if mask.ndim > 1:
            mask = mask.reshape(mask.shape[0], -1).any(axis=1)
        flags.row(flags.ds[name].dims[0], mask, PROVIDER)

    return check


def provider_bit(name: str, bit: int) -> Callable[[Flags], None]:
    """Rows whose flag-table ``name`` has ``bit`` (a value, e.g. 1 << 15) set."""

    def check(flags: Flags) -> None:
        values = flags.values(name)
        if values is None:
            return
        valid = np.isfinite(values)
        mask = np.zeros(values.shape, dtype=bool)
        mask[valid] = (values[valid].astype("int64") & bit) != 0
        if mask.ndim > 1:
            mask = mask.reshape(mask.shape[0], -1).any(axis=1)
        flags.row(flags.ds[name].dims[0], mask, PROVIDER)

    return check


def wind_profiler_quality(quality: str, significance: str) -> Callable[[Flags], None]:
    """Wind profiler quality by its kind (associated field significance, BUFR 0 31 021).

    -1 (0 33 002 quality information): 1 suspect; 1 (1-bit indicator): 1 suspect or bad;
    2 (2-bit indicator): 2 highly suspect, 3 bad; 6 (GTSPP class): 3 probably bad, 4 bad;
    7 (percent confidence): below 50.
    """

    def check(flags: Flags) -> None:
        q, s = flags.values(quality), flags.values(significance)
        if q is None or s is None:
            return
        with np.errstate(invalid="ignore"):
            bad = (
                ((s == -1) & (q == 1))
                | (np.isnan(s) & (q == 1))
                | ((s == 1) & (q == 1))
                | ((s == 2) & (q >= 2))
                | ((s == 6) & np.isin(q, (3, 4)))
                | ((s == 7) & (q < 50))
            )
        flags.row(flags.ds[quality].dims[0], bad, PROVIDER)

    return check


# ------------------------------------------------------------------------ vertical


def _profiles(flags: Flags) -> list[np.ndarray] | None:
    """The level indexes of each profile, from ``level_count`` (CF contiguous ragged)."""
    levels = flags.spec.levels
    counts = flags.values(levels.count)
    if counts is None:
        return None
    counts = np.nan_to_num(counts).astype("int64")
    if counts.sum() != flags.level.size:
        logger.warning(
            f"wis2_qc: {levels.count} sums to {counts.sum()}, not {flags.level.size} levels; "
            "vertical checks skipped"
        )
        return None
    return np.split(np.arange(flags.level.size), np.cumsum(counts)[:-1])


def _reversals(values: np.ndarray, strict: bool, tolerance: float = 0.0) -> np.ndarray:
    """Which of a profile's values break its overall (first to last) monotonic order.

    Missing values are never flagged. Each value is compared with the running extreme of
    the values before it, so a single spike flags itself and not its neighbours.
    """
    out = np.zeros(values.shape, dtype=bool)
    valid = np.flatnonzero(np.isfinite(values))
    if valid.size < 2:
        return out
    v = values[valid]
    direction = np.sign(v[-1] - v[0])
    if direction == 0:
        return out
    v = v * direction  # now expected to increase
    before = np.concatenate([[-np.inf], np.maximum.accumulate(v)[:-1]])
    bad = v <= before + tolerance if strict else v < before - tolerance
    out[valid[bad]] = True
    return out


def monotonic(name: str, strict: bool = False, tolerance: float = 0.0):
    """Levels of ``name`` out of their profile's monotonic order."""

    def check(flags: Flags, profiles: list[np.ndarray]) -> None:
        values = flags.values(name)
        if values is None:
            return
        bad = np.zeros(values.shape, dtype=bool)
        for levels in profiles:
            bad[levels] = _reversals(values[levels], strict, tolerance)
        flags.mark(name, bad, VERTICAL)

    return check


def first_monotonic(names: Sequence[str], strict: bool = False):
    """:func:`monotonic` for the first of ``names`` a profile has any value of."""

    def check(flags: Flags, profiles: list[np.ndarray]) -> None:
        columns = [(n, flags.values(n)) for n in names if n in flags.ds]
        for levels in profiles:
            for name, values in columns:
                if np.isfinite(values[levels]).any():
                    bad = np.zeros(values.shape, dtype=bool)
                    bad[levels] = _reversals(values[levels], strict)
                    flags.mark(name, bad, VERTICAL)
                    break

    return check


def height_with_pressure(pressure: str, height: str, tolerance: float = 50.0):
    """Geopotential height must not fall as pressure falls (levels in pressure order)."""

    def check(flags: Flags, profiles: list[np.ndarray]) -> None:
        p, z = flags.values(pressure), flags.values(height)
        if p is None or z is None:
            return
        bad = np.zeros(p.shape, dtype=bool)
        for levels in profiles:
            both = levels[np.isfinite(p[levels]) & np.isfinite(z[levels])]
            if both.size < 2:
                continue
            ordered = both[np.argsort(-p[both], kind="stable")]
            # Both levels of an inverted pair: either may be the wrong one.
            drop = np.flatnonzero(np.diff(z[ordered]) < -tolerance)
            bad[ordered[drop]] = True
            bad[ordered[drop + 1]] = True
        flags.mark(height, bad, VERTICAL)

    return check


def monotonic_by(name: str, order: str, tolerance: float = 0.0):
    """Levels of ``name`` out of monotonic order along ``order`` (time since launch).

    TEMP messages list standard, significant, tropopause and maximum-wind levels in an
    order of their own, so a sounding's pressure is only checked along its levels' times;
    a sounding without them is not checked.
    """

    def check(flags: Flags, profiles: list[np.ndarray]) -> None:
        values, along = flags.values(name), flags.values(order)
        if values is None or along is None:
            return
        bad = np.zeros(values.shape, dtype=bool)
        for levels in profiles:
            timed = levels[np.isfinite(along[levels])]
            if timed.size < 2:
                continue
            ordered = timed[np.argsort(along[timed], kind="stable")]
            bad[ordered] = _reversals(values[ordered], strict=False, tolerance=tolerance)
        flags.mark(name, bad, VERTICAL)

    return check


def superadiabatic(pressure: str, temperature: str):
    """A superadiabatic jump: potential temperature dropping up a sounding.

    Adjacent levels (in pressure order) whose potential temperature falls by more than
    :data:`THETA_DROP` flag both levels' temperatures.
    """

    def check(flags: Flags, profiles: list[np.ndarray]) -> None:
        p, t = flags.values(pressure), flags.values(temperature)
        if p is None or t is None:
            return
        bad = np.zeros(p.shape, dtype=bool)
        for levels in profiles:
            both = levels[np.isfinite(p[levels]) & np.isfinite(t[levels]) & (p[levels] > 0)]
            if both.size < 2:
                continue
            ordered = both[np.argsort(-p[both], kind="stable")]
            theta = t[ordered] * (1e5 / p[ordered]) ** 0.2857
            drop = np.flatnonzero(np.diff(theta) < -THETA_DROP)
            bad[ordered[drop]] = True
            bad[ordered[drop + 1]] = True
        flags.mark(temperature, bad, VERTICAL)

    return check


def row_monotonic(name: str):
    """A 2-D product variable whose occurrences in a row should be monotonic.

    GNSS-RO impact parameters along the occultation, say.
    """

    def check(flags: Flags) -> None:
        values = flags.values(name)
        if values is None or values.ndim != 2:
            return
        bad = np.zeros(values.shape, dtype=bool)
        for row in np.flatnonzero(np.isfinite(values).sum(axis=1) > 1):
            bad[row] = _reversals(values[row], strict=False)
        flags.mark(name, bad, VERTICAL)

    return check


# ---------------------------------------------------------------------------- specs


@dataclasses.dataclass(frozen=True)
class Levels:
    """The levels of a CF contiguous ragged group."""

    dim: str
    ranges: Mapping[str, Range] = dataclasses.field(default_factory=dict)
    checks: tuple[Callable, ...] = ()
    vertical: tuple[Callable, ...] = ()
    count: str = "level_count"


@dataclasses.dataclass(frozen=True)
class Spec:
    """How a group is flagged.

    Attributes:
        dim: The observation dimension (first dimension of every observation variable).
        ranges: Variables range-checked, with their limits.
        time: The observation time variable (None: no time checks).
        arrival: Arrival-time variables, first preferred; without one, the time of
            flagging stands in.
        latitude, longitude: Position variables (None: no position checks).
        max_age: How long before its arrival an observation may have been made.
        duplicate_key: Platform identifiers and the time variable; with fewer than two
            the group is not checked for duplicates.
        duplicate_order: Variables ranking the copies of a duplicate; the last is kept
            unflagged (a higher correction number, a later arrival).
        checks: Consistency and provider checks, each taking the :class:`Flags`.
        levels: The level dimension of a ragged group, with its own checks.
    """

    dim: str = "observation"
    ranges: Mapping[str, Range] = dataclasses.field(default_factory=dict)
    time: str | None = "time"
    arrival: tuple[str, ...] = ("arrival_time", "first_arrival_time")
    latitude: str | None = "latitude"
    longitude: str | None = "longitude"
    max_age: dt.timedelta = dt.timedelta(days=10)
    duplicate_key: tuple[str, ...] = ()
    duplicate_order: tuple[str, ...] = ()
    checks: tuple[Callable[[Flags], None], ...] = ()
    levels: Levels | None = None


_LAND_CHECKS = (
    dewpoint("2m_temperature", "2m_dewpoint_temperature"),
    calm("10m_wind_speed", "10m_wind_direction"),
    gust("10m_wind_gust", "10m_wind_speed"),
    precipitation_nesting(list(PRECIPITATION)),
)
_OCEAN_CHECKS = (
    dewpoint("air_temperature", "dewpoint_temperature"),
    calm("wind_speed", "wind_direction"),
    gust("wind_gust", "wind_speed"),
)
#: Satellite product groups: ranges, and what else each has.
_PRODUCT_CHECKS: dict[str, tuple] = {
    "gnss_ro": (
        provider_bit("radio_occultation_data_quality_flags", 1 << 15),
        row_monotonic("impact_parameter"),
    ),
}

SPECS: dict[str, Spec] = {
    "aircraft": Spec(
        ranges=AIRCRAFT,
        checks=(
            dewpoint("temperature", "dewpoint_temperature"),
            calm("wind_speed", "wind_direction"),
            provider_codes("moisture_quality", (3, 4, 5, 6, 7, 8)),
            provider_codes("roll_angle_quality", (1,)),
        ),
    ),
    "land/reports": Spec(
        ranges=SURFACE,
        arrival=("arrived", "first_arrival_time"),
        duplicate_key=("station_id", "time"),
        duplicate_order=("update_sequence_number", "arrived"),
        checks=(
            *_LAND_CHECKS,
            maximum_minimum(
                "maximum_2m_temperature",
                "minimum_2m_temperature",
                "maximum_2m_temperature_period",
                "minimum_2m_temperature_period",
            ),
        ),
    ),
    "land/synop_tac": Spec(
        ranges=SURFACE,
        duplicate_key=("station_id", "time"),
        duplicate_order=("arrival_time",),
        checks=_LAND_CHECKS,
    ),
    "land/hydrometric": Spec(
        ranges=HYDROMETRIC,
        latitude=None,
        longitude=None,
        # The daily files repeat the last 30 days.
        max_age=dt.timedelta(days=40),
        duplicate_key=("station_id", "time"),
        duplicate_order=("arrival_time",),
    ),
    "land/radar": Spec(dim="volume", ranges=RADAR, checks=(radar_times,)),
    "ocean/surface": Spec(
        ranges=OCEAN_SURFACE,
        max_age=dt.timedelta(days=31),
        duplicate_key=("wmo_platform_id", "call_sign", "wigos_id", "station_name", "time"),
        duplicate_order=("arrival_time",),
        checks=_OCEAN_CHECKS,
    ),
    "ocean/ship_tac": Spec(
        ranges=OCEAN_SURFACE,
        duplicate_key=("call_sign", "time"),
        duplicate_order=("arrival_time",),
        checks=_OCEAN_CHECKS,
    ),
    "ocean/profiles": Spec(
        dim="profile",
        # Argo profiles can reach the GTS weeks after they were made.
        max_age=dt.timedelta(days=31),
        levels=Levels(
            dim="level",
            ranges=OCEAN_PROFILE,
            # Argo test 8: pressure must increase strictly along the profile.
            vertical=(first_monotonic(("sea_water_pressure", "depth"), strict=True),),
        ),
    ),
    "upper_air/soundings": Spec(
        dim="sounding",
        ranges=SOUNDING,
        time="launch_time",
        levels=Levels(
            dim="level",
            ranges=UPPER_AIR,
            checks=(
                dewpoint("temperature", "dewpoint_temperature"),
                calm("wind_speed", "wind_direction"),
            ),
            vertical=(
                monotonic_by("pressure", "time_since_launch", tolerance=50.0),
                height_with_pressure("pressure", "geopotential_height"),
                superadiabatic("pressure", "temperature"),
            ),
        ),
    ),
    "upper_air/wind_profiler": Spec(
        dim="profile",
        ranges=WIND_PROFILER,
        levels=Levels(
            dim="profile_level",
            ranges=WIND_PROFILER_LEVELS,
            checks=(
                calm("wind_speed", "wind_direction"),
                wind_profiler_quality("wind_quality", "wind_quality_significance"),
                wind_profiler_quality(
                    "vertical_wind_quality", "vertical_wind_quality_significance"
                ),
            ),
            vertical=(first_monotonic(("height", "height_above_station", "geopotential_height")),),
        ),
    ),
    "satellite/radiances": Spec(
        ranges=RADIANCES,
        max_age=dt.timedelta(days=31),
        checks=(provider_codes("gqis_quality_flag", (1,)),),
    ),
    "satellite/product": Spec(ranges=PRODUCT, max_age=dt.timedelta(days=31)),
}


def spec_for(group: str) -> Spec:
    """The :class:`Spec` of a store group (``land/radar/bewid_dbzh``, ``satellite/amv``).

    Raises:
        KeyError: The group is not one this module knows.
    """
    from planetary_datasets.providers.observations import wis2_satellite as sat

    if group in SPECS:
        return SPECS[group]
    if group.startswith("land/radar/"):
        return SPECS["land/radar"]
    if group.startswith("satellite/"):
        name = group.removeprefix("satellite/")
        if name in sat.INSTRUMENTS:
            return SPECS["satellite/radiances"]
        base = SPECS["satellite/product"]
        return dataclasses.replace(base, checks=base.checks + _PRODUCT_CHECKS.get(name, ()))
    raise KeyError(f"wis2_qc: no quality-control spec for group {group!r}")


# ---------------------------------------------------------------------------- flag


def _flag_attrs(bits, long_name: str) -> dict:
    return {
        "long_name": long_name,
        "flag_masks": [int(b) for b, _ in bits],
        "flag_meanings": " ".join(m for _, m in bits),
        "comment": "0: no check found a problem; values are never altered by quality "
        "control. See planetary_datasets.providers.observations.wis2_qc",
        "quality_control_version": QC_VERSION,
    }


def _name(ds: xr.Dataset, wanted: str) -> str:
    """``wanted``, or ``<wanted>_wis2`` when the source already has a variable so named."""
    return wanted if wanted not in ds.variables else f"{wanted}_wis2"


def flag(ds: xr.Dataset, group: str | Spec, now: np.datetime64 | None = None) -> xr.Dataset:
    """``ds`` with quality flags added for every observation (values untouched).

    Args:
        ds: The rows a target is about to append to ``group``.
        group: The store group (see :func:`spec_for`), or a :class:`Spec`.
        now: Stands in for a missing arrival time (default: the current time, UTC).

    Returns:
        A new dataset: ``ds``'s variables and ``quality_flag``, ``level_quality_flag``
        (ragged groups) and a ``<variable>_quality`` per variable checked.
    """
    spec = spec_for(group) if isinstance(group, str) else group
    if spec.dim not in ds.dims and spec.dim not in ds.sizes:
        return ds
    if now is None:
        now = np.datetime64(dt.datetime.now(dt.timezone.utc).replace(tzinfo=None), "s")
    flags = Flags(ds, spec)
    _check_ranges(flags, spec.ranges)
    _check_position(flags)
    _check_time(flags, np.datetime64(now, "s"))
    _check_duplicates(flags)
    for check in spec.checks:
        check(flags)
    if spec.levels is not None and spec.levels.dim in ds.dims:
        _check_ranges(flags, spec.levels.ranges)
        for check in spec.levels.checks:
            check(flags)
        profiles = _profiles(flags)
        if profiles is not None:
            for check in spec.levels.vertical:
                check(flags, profiles)
            # A level's findings are its profile's too.
            owner = np.repeat(np.arange(len(profiles)), [p.size for p in profiles])
            np.bitwise_or.at(flags.observation, owner, flags.level)
    added: dict[str, xr.Variable] = {}
    for name, values in flags.variables.items():
        dims = tuple(ds[name].dims[i] for i in flags._kept_axes(name))
        added[_name(ds, f"{name}_quality")] = xr.Variable(
            dims, values, _flag_attrs(VARIABLE_BITS, f"quality flags of {name}")
        )
    # Variables never flagged still get a flag of zeros, so every window has the same
    # variables and a reader can tell "checked, fine" from "not checked".
    for name in (*spec.ranges, *(spec.levels.ranges if spec.levels else {})):
        if name in ds.variables and _name(ds, f"{name}_quality") not in added:
            dims = tuple(ds[name].dims[i] for i in flags._kept_axes(name))
            shape = tuple(ds[name].shape[i] for i in flags._kept_axes(name))
            added[_name(ds, f"{name}_quality")] = xr.Variable(
                dims,
                np.zeros(shape, dtype="uint8"),
                _flag_attrs(VARIABLE_BITS, f"quality flags of {name}"),
            )
    added[_name(ds, "quality_flag")] = xr.Variable(
        (spec.dim,),
        flags.observation,
        _flag_attrs(OBSERVATION_BITS, "quality flags of the observation"),
    )
    if flags.level is not None:
        added[_name(ds, "level_quality_flag")] = xr.Variable(
            (spec.levels.dim,),
            flags.level,
            _flag_attrs(OBSERVATION_BITS, "quality flags of the level"),
        )
    return ds.assign(added)


__all__ = [
    "OBSERVATION_BITS",
    "Range",
    "SPECS",
    "Spec",
    "VARIABLE_BITS",
    "flag",
    "spec_for",
]
