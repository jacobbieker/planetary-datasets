"""The naming registry: ERA5-style names and unit conversions."""

import pytest

from planetary_datasets.common.naming import (
    era5ify,
    geopotential_rename,
    propose,
    unit_conversion,
)


@pytest.mark.parametrize(
    "text, expected",
    [
        ("2 metre temperature", "2m_temperature"),
        ("10 metre U wind component", "10m_u_component_of_wind"),
        ("Sea ice albedo - diffusive solar (0.3 - 5.0 m-6)", "sea_ice_albedo_diffusive_solar"),
        ("Latent heat net flux (m)", "latent_heat_net_flux_mean"),
        ("Cloud cover (0 - 400 hPa)", "cloud_cover"),
        ("Time-maximum 10 metre northward wind gust", "maximum_10m_northward_wind_gust"),
        (
            "downward direct short wave radiation flux at surface mean over forecast time",
            "downward_direct_short_wave_radiation_flux_at_surface_mean",
        ),
    ],
)
def test_era5ify(text, expected):
    assert era5ify(text) == expected


@pytest.mark.parametrize(
    "name, attrs, dims, store, expected",
    [
        ("t_2m", {}, ["time", "values"], "bkr/icon/icon_global.icechunk", "2m_temperature"),
        ("clch", {}, ["time", "values"], "bkr/icon/icon_global.icechunk", "high_cloud_cover"),
        (
            "C13",
            {"long_name": "brightness_temperature"},
            ["t", "y", "x"],
            "bkr/geo/goes-east_2000m.icechunk",
            "channel_13_brightness_temperature_10_3um",
        ),
        (
            "14",
            {},
            ["time", "y", "x"],
            "bkr/polar/jpss_atms.icechunk",
            "channel_14_brightness_temperature",
        ),
        ("atmp", {"units": "Deg C"}, ["time"], "bkr/aoml/v2/osmc_all.icechunk", "air_temperature"),
        (
            "air_temperature_1.5m",
            {"standard_name": "air_temperature"},
            ["time", "y", "x"],
            "bkr/metoffice/x.icechunk",
            "1_5m_temperature",
        ),
        (
            "NO2",
            {"standard_name": "northward_wind"},
            ["time"],
            "bkr/geos/geos_15min.icechunk",
            "nitrogen_dioxide",
        ),
        ("latitude", {}, ["latitude"], "bkr/x.icechunk", "latitude"),
        (
            "lambert_azimuthal_equal_area",
            {},
            [],
            "bkr/metoffice/x.icechunk",
            "lambert_azimuthal_equal_area",
        ),
    ],
)
def test_propose(name, attrs, dims, store, expected):
    assert propose(name, attrs, dims, store)[0] == expected


def test_unit_conversions_decode_to_era5_units():
    assert unit_conversion("air_temperature", "Deg C") == (1.0, 273.15, "K")
    assert unit_conversion("sea_level_pressure", "hPa") == (100.0, 0.0, "Pa")
    assert unit_conversion("total_cloud_cover", "%") == (0.01, 0.0, "(0 - 1)")
    assert unit_conversion("2m_temperature", "K") is None
    assert unit_conversion("wind_speed", "m/s") is None


def test_heights_become_geopotential():
    assert geopotential_rename("geopotential_height", "gpm") == "geopotential"
    assert geopotential_rename("orography", "m") == "geopotential_at_surface"
    assert geopotential_rename("geopotential_height", "m**2 s**-2") is None
    assert unit_conversion("geopotential", "gpm") == (9.80665, 0.0, "m**2 s**-2")


@pytest.mark.parametrize(
    "name, attrs, dims, store, expected",
    [
        (
            "IR_108",
            {},
            ["time", "y", "x"],
            "bkr/geo/msg_3000m.icechunk",
            "channel_09_brightness_temperature_10_8um",
        ),
        (
            "ir_105",
            {},
            ["time", "y", "x"],
            "bkr/geo/mtg_1000m.icechunk",
            "channel_14_brightness_temperature_10_5um",
        ),
        (
            "3a",
            {},
            ["time", "y", "x"],
            "bkr/polar/metop_avhrr.icechunk",
            "channel_3a_reflectance_1_6um",
        ),
        ("lat", {}, ["lat"], "bkr/x.icechunk", "latitude"),
        ("t", {"units": "minutes since 2025-01-01"}, ["t"], "bkr/geo/x.icechunk", "time"),
        ("hybrid", {}, ["hybrid"], "bkr/x.icechunk", "model_level"),
        ("x_geostationary", {"units": "meter"}, ["x_geostationary"], "bkr/geo/x.icechunk", "x"),
        ("level", {"units": "hPa"}, ["level"], "bkr/x.icechunk", "pressure_level"),
        (
            "aswdir_s",
            {},
            ["time", "values"],
            "bkr/icon/x.icechunk",
            "mean_surface_direct_short_wave_radiation_flux",
        ),
        (
            "valid_pixel_count",
            {"long_name": "number of good and conditionally usable pixels"},
            ["t"],
            "bkr/geo/goes16_radf_C01.icechunk",
            "valid_pixel_count",
        ),
    ],
)
def test_channels_coordinates_and_short_names(name, attrs, dims, store, expected):
    assert propose(name, attrs, dims, store)[0] == expected
