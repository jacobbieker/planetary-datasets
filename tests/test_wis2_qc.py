"""Per-observation quality flags of the WIS2 store (wis2_qc), check family by check family."""

from __future__ import annotations

import numpy as np
import pytest
import xarray as xr

from planetary_datasets.providers.observations import wis2_qc as qc

ARRIVED = np.datetime64("2026-11-01T06:00:00", "s")
NOW = np.datetime64("2026-11-01T12:00:00", "s")


def land(**columns) -> xr.Dataset:
    """Land reports along ``observation``; plausible defaults for what is not given."""
    n = len(next(iter(columns.values())))
    base = {
        "station_id": np.array([f"0-20000-0-{i:05d}" for i in range(n)], dtype=object),
        "time": np.full(n, np.datetime64("2026-11-01T00:00:00", "s")),
        "arrived": np.full(n, ARRIVED),
        "latitude": np.full(n, 50.0),
        "longitude": np.full(n, 5.0),
        "2m_temperature": np.full(n, 280.0),
        "2m_dewpoint_temperature": np.full(n, 275.0),
        "update_sequence_number": np.zeros(n, dtype="int16"),
    }
    base.update(columns)
    ds = xr.Dataset({k: ("observation", np.asarray(v)) for k, v in base.items()})
    ds["2m_temperature"].attrs["units"] = "K"
    return ds


def flagged(ds: xr.Dataset, group: str = "land/reports") -> xr.Dataset:
    return qc.flag(ds, group, now=NOW)


def bits(values) -> list[int]:
    return [int(v) for v in np.asarray(values)]


# ----------------------------------------------------------------------------- general


def test_values_are_never_altered_and_flags_are_described():
    ds = land(**{"2m_temperature": [280.0, 194.0, 40.0, np.nan]})
    out = flagged(ds)
    for name in ds.variables:
        xr.testing.assert_identical(out[name], ds[name])
    flag = out["quality_flag"]
    assert flag.dtype == np.uint16 and flag.dims == ("observation",)
    assert out["2m_temperature_quality"].dtype == np.uint8
    assert flag.attrs["flag_masks"] == [b for b, _ in qc.OBSERVATION_BITS]
    assert flag.attrs["flag_meanings"].split()[0] == "outside_physical_range"
    assert len(flag.attrs["flag_meanings"].split()) == len(flag.attrs["flag_masks"])
    meanings = out["2m_temperature_quality"].attrs["flag_meanings"].split()
    assert meanings == [m for _, m in qc.VARIABLE_BITS]


def test_every_group_of_the_store_has_a_spec():
    for group in (
        "aircraft",
        "land/reports",
        "land/hydrometric",
        "land/radar/bewid_dbzh",
        "land/synop_tac",
        "ocean/surface",
        "ocean/profiles",
        "ocean/ship_tac",
        "upper_air/soundings",
        "upper_air/wind_profiler",
        "satellite/atms",
        "satellite/iasi",
        "satellite/gnss_ro",
        "satellite/olci_wv",
    ):
        assert isinstance(qc.spec_for(group), qc.Spec)
    with pytest.raises(KeyError):
        qc.spec_for("nowhere")


def test_a_source_variable_named_like_a_flag_keeps_its_name():
    ds = land(**{"2m_temperature": [280.0]}).assign(
        quality_flag=("observation", np.array([7], dtype="int32"))
    )
    out = flagged(ds)
    assert out["quality_flag"].dtype == np.int32 and int(out["quality_flag"][0]) == 7
    assert "quality_flag_wis2" in out


# ----------------------------------------------------------------------------- ranges


def test_values_outside_the_possible_and_the_plausible_range():
    temperatures = [280.0, 194.0, 190.0, 40.0, np.nan]
    out = flagged(land(**{"2m_temperature": temperatures, "2m_dewpoint_temperature": [np.nan] * 5}))
    assert bits(out["2m_temperature_quality"]) == [0, 0, qc.PLAUSIBLE, qc.PHYSICAL, 0]
    assert bits(out["quality_flag"]) == [0, 0, qc.PLAUSIBLE, qc.PHYSICAL, 0]


def test_a_315_m_s_wind_is_impossible_and_75_is_the_gross_limit():
    out = flagged(
        land(
            **{
                "10m_wind_speed": [5.0, 80.0, 315.0],
                "10m_wind_direction": [180.0, 180.0, 180.0],
            }
        )
    )
    assert bits(out["10m_wind_speed_quality"]) == [0, qc.PLAUSIBLE, qc.PHYSICAL]


def test_bufr_special_values_are_not_flagged():
    out = flagged(
        land(
            **{
                "total_precipitation_1h": [-0.1, 0.0, -0.5],
                "total_precipitation_24h": [-0.1, 0.0, 1.0],
                "snow_depth": [-0.01, -0.02, -0.5],
            }
        )
    )
    assert bits(out["total_precipitation_1h_quality"]) == [0, 0, qc.PHYSICAL]
    assert bits(out["snow_depth_quality"]) == [0, 0, qc.PHYSICAL]


def test_integer_fill_values_are_missing_not_out_of_range():
    ds = land(**{"2m_temperature": [280.0, 280.0]})
    ds["total_cloud_cover"] = xr.Variable(
        "observation", np.array([-1, 150], dtype="int16"), {"_FillValue": -1}
    )
    assert bits(flagged(ds)["total_cloud_cover_quality"]) == [0, qc.PHYSICAL]


def test_brightness_temperatures_are_flagged_per_channel():
    bt = np.array([[250.0, 251.0, np.nan], [250.0, 20.0, 345.0]], dtype="float32")
    ds = xr.Dataset(
        {
            "brightness_temperature": (("observation", "channel"), bt),
            "time": ("observation", np.full(2, np.datetime64("2026-11-01T00:00", "s"))),
            "latitude": ("observation", [10.0, 20.0]),
            "longitude": ("observation", [10.0, 20.0]),
        }
    )
    out = flagged(ds, "satellite/atms")
    assert out["brightness_temperature_quality"].dims == ("observation", "channel")
    assert out["brightness_temperature_quality"].values.tolist() == [
        [0, 0, 0],
        [0, qc.PHYSICAL, qc.PLAUSIBLE],
    ]
    assert bits(out["quality_flag"]) == [0, qc.PHYSICAL | qc.PLAUSIBLE]


def test_a_products_repeated_element_is_checked_at_its_first_occurrence_only():
    # An AMV: the speed, then (same element name) statistics that are not speeds.
    speed = np.array([[20.0, 400.0], [500.0, 3.0]], dtype="float32")
    ds = xr.Dataset(
        {
            "wind_speed": (("observation", "wind_speed_n"), speed),
            "time": ("observation", np.full(2, np.datetime64("2026-11-01T00:00", "s"))),
            "latitude": ("observation", [10.0, 20.0]),
            "longitude": ("observation", [10.0, 20.0]),
        }
    )
    out = flagged(ds, "satellite/amv")
    assert out["wind_speed_quality"].dims == ("observation",)
    assert bits(out["wind_speed_quality"]) == [0, qc.PHYSICAL]


# ------------------------------------------------------------------------ consistency


def test_dewpoint_above_temperature_flags_both():
    out = flagged(
        land(
            **{
                "2m_temperature": [280.0, 280.0, 280.0],
                "2m_dewpoint_temperature": [279.0, 280.1, 285.0],
            }
        )
    )
    assert bits(out["2m_dewpoint_temperature_quality"]) == [0, 0, qc.INCONSISTENT]
    assert bits(out["2m_temperature_quality"]) == [0, 0, qc.INCONSISTENT]
    assert bits(out["quality_flag"]) == [0, 0, qc.INCONSISTENT]


def test_maximum_below_minimum_over_the_same_period():
    out = flagged(
        land(
            **{
                "maximum_2m_temperature": [285.0, 270.0, 270.0],
                "minimum_2m_temperature": [275.0, 280.0, 280.0],
                "maximum_2m_temperature_period": [-12.0, -12.0, -12.0],
                "minimum_2m_temperature_period": [-12.0, -12.0, -24.0],
            }
        )
    )
    assert bits(out["maximum_2m_temperature_quality"]) == [0, qc.INCONSISTENT, 0]


def test_calm_with_a_direction_and_wind_without_one():
    out = flagged(
        land(
            **{
                "10m_wind_speed": [0.0, 0.0, 5.0, 5.0],
                "10m_wind_direction": [0.0, 90.0, 0.0, 360.0],
            }
        )
    )
    assert bits(out["10m_wind_direction_quality"]) == [0, qc.INCONSISTENT, qc.INCONSISTENT, 0]


def test_a_gust_below_the_mean_wind():
    out = flagged(
        land(
            **{
                "10m_wind_speed": [10.0, 10.0],
                "10m_wind_direction": [90.0, 90.0],
                "10m_wind_gust": [15.0, 5.0],
            }
        )
    )
    assert bits(out["10m_wind_gust_quality"]) == [0, qc.INCONSISTENT]


def test_a_shorter_precipitation_total_above_a_longer_one():
    out = flagged(
        land(
            **{
                "total_precipitation_1h": [2.0, 10.0, -0.1],
                "total_precipitation_6h": [5.0, 3.0, 0.0],
            }
        )
    )
    assert bits(out["total_precipitation_1h_quality"]) == [0, qc.INCONSISTENT, 0]


def test_radar_sweeps_outside_their_volume():
    start = 1793491200.0
    ds = xr.Dataset(
        {
            "time": ("volume", np.full(2, np.datetime64("2026-11-01T00:00", "s"))),
            "arrival_time": ("volume", np.full(2, ARRIVED)),
            "latitude": ("volume", [50.0, 50.0]),
            "longitude": ("volume", [5.0, 5.0]),
            "start_time": ("volume", [start, start]),
            "end_time": ("volume", [start + 300, start + 300]),
            "sweep_start_time": (
                ("volume", "sweep"),
                [[start, start + 60], [start, start + 86400]],
            ),
            "sweep_end_time": (
                ("volume", "sweep"),
                [[start + 50, start + 30], [start + 50, np.nan]],
            ),
            "elevation": (("volume", "sweep"), [[0.5, 1.5], [0.5, -10.0]]),
        }
    )
    out = qc.flag(ds, "land/radar/bewid_dbzh", now=NOW)
    assert out["sweep_start_time_quality"].values.tolist() == [
        [0, qc.INCONSISTENT],
        [0, qc.INCONSISTENT],
    ]
    assert out["elevation_quality"].values.tolist() == [[0, 0], [0, qc.PLAUSIBLE]]
    assert out["quality_flag"].dims == ("volume",)
    assert bits(out["quality_flag"]) == [qc.INCONSISTENT, qc.INCONSISTENT | qc.PLAUSIBLE]


# ----------------------------------------------------------------------- position, time


def test_positions_missing_out_of_range_and_null_island():
    out = flagged(
        land(latitude=[50.0, np.nan, 95.0, 0.0, 0.0], longitude=[5.0, 5.0, 5.0, 0.0, 400.0])
    )
    assert bits(out["quality_flag"]) == [
        0,
        qc.POSITION_INVALID,
        qc.POSITION_INVALID,
        qc.POSITION_SUSPECT,
        qc.POSITION_INVALID,
    ]


def test_times_missing_in_the_future_and_too_old():
    times = np.array(
        [
            "2026-11-01T05:00:00",  # before arrival
            "NaT",
            "2026-11-01T06:10:00",  # within the clock tolerance
            "2026-11-01T07:00:00",  # after it arrived
            "2026-10-01T00:00:00",  # a month before
        ],
        dtype="datetime64[s]",
    )
    out = flagged(land(time=times))
    assert bits(out["quality_flag"]) == [0, qc.TIME_INVALID, 0, qc.TIME_FUTURE, qc.TIME_OLD]


def test_without_an_arrival_the_time_of_flagging_stands_in():
    times = np.array(["2026-11-01T11:00:00", "2026-11-01T13:00:00"], dtype="datetime64[s]")
    arrived = np.array(["NaT", "NaT"], dtype="datetime64[s]")
    out = flagged(land(time=times, arrived=arrived))
    assert bits(out["quality_flag"]) == [0, qc.TIME_FUTURE]


def test_integer_times_with_cf_units_are_decoded():
    seconds = np.datetime64("2026-11-01T07:00:00", "s").astype("int64")
    fill = np.iinfo("int64").min
    ds = xr.Dataset(
        {
            "time": xr.Variable(
                "observation",
                np.array([seconds * 1000, fill], dtype="int64"),
                {"units": "milliseconds since 1970-01-01", "_FillValue": fill},
            ),
            "arrival_time": xr.Variable(
                "observation",
                np.array([ARRIVED.astype("int64")] * 2, dtype="int64"),
                {"units": "seconds since 1970-01-01"},
            ),
            "latitude": ("observation", [1.0, 1.0]),
            "longitude": ("observation", [1.0, 1.0]),
        }
    )
    assert bits(qc.flag(ds, "satellite/gnss_ro", now=NOW)["quality_flag"]) == [
        qc.TIME_FUTURE,
        qc.TIME_INVALID,
    ]


# ------------------------------------------------------------------------- duplicates


def test_a_report_superseded_in_the_same_window_is_a_duplicate():
    ds = land(
        station_id=np.array(["A", "A", "B", "A"], dtype=object),
        update_sequence_number=np.array([0, 1, 0, 0], dtype="int16"),
        time=np.array(
            ["2026-11-01T00:00", "2026-11-01T00:00", "2026-11-01T00:00", "2026-11-01T01:00"],
            dtype="datetime64[s]",
        ),
    )
    # The correction (sequence number 1) is kept unflagged; the original is superseded.
    assert bits(flagged(ds)["quality_flag"]) == [qc.DUPLICATE, 0, 0, 0]


def test_duplicates_are_ranked_by_arrival_where_there_is_no_correction_number():
    ds = xr.Dataset(
        {
            "station_id": ("observation", np.array(["01001", "01001"], dtype=object)),
            "time": ("observation", np.full(2, np.datetime64("2026-11-01T00:00", "s"))),
            "arrival_time": (
                "observation",
                np.array(["2026-11-01T02:00", "2026-11-01T01:00"], dtype="datetime64[s]"),
            ),
            "latitude": ("observation", [70.9, 70.9]),
            "longitude": ("observation", [-8.7, -8.7]),
        }
    )
    assert bits(qc.flag(ds, "land/synop_tac", now=NOW)["quality_flag"]) == [0, qc.DUPLICATE]


def test_groups_whose_reports_may_repeat_a_station_and_time_are_not_checked():
    # TEMP parts A-D share a station and launch time.
    assert qc.spec_for("upper_air/soundings").duplicate_key == ()
    assert qc.spec_for("satellite/atms").duplicate_key == ()


# --------------------------------------------------------------------------- vertical


def sounding(levels: dict, counts=None) -> xr.Dataset:
    n = len(next(iter(levels.values())))
    counts = [n] if counts is None else counts
    data = {
        "launch_time": ("sounding", np.full(len(counts), np.datetime64("2026-11-01T00:00", "s"))),
        "arrival_time": ("sounding", np.full(len(counts), ARRIVED)),
        "latitude": ("sounding", np.full(len(counts), 70.9)),
        "longitude": ("sounding", np.full(len(counts), -8.7)),
        "level_count": ("sounding", np.array(counts, dtype="int32")),
    }
    data.update({k: ("level", np.asarray(v, dtype="float64")) for k, v in levels.items()})
    return xr.Dataset(data)


def test_pressure_rising_along_the_ascent_is_flagged_but_report_order_is_not():
    ds = sounding(
        {
            # A TEMP lists the tropopause after the standard levels: not out of order.
            "pressure": [98500, 100000, 92500, 85000, 23400, 90000],
            "time_since_launch": [0, np.nan, 60, 120, 1800, 1900],
        }
    )
    out = qc.flag(ds, "upper_air/soundings", now=NOW)
    assert bits(out["pressure_quality"]) == [0, 0, 0, 0, 0, qc.VERTICAL]
    assert bits(out["level_quality_flag"]) == [0, 0, 0, 0, 0, qc.VERTICAL]
    assert bits(out["quality_flag"]) == [qc.VERTICAL], "a level's flag is its sounding's"


def test_height_falling_as_pressure_falls_flags_both_levels():
    # Jan Mayen, 27 Oct 2026: the 1000 hPa level below ground kept TEMP's "500 + depth"
    # coding (604 for -104 m) through the conversion to BUFR.
    ds = sounding(
        {
            "pressure": [98500, 100000, 92500, 85000],
            "geopotential_height": [np.nan, 604, 530, 1207],
        }
    )
    out = qc.flag(ds, "upper_air/soundings", now=NOW)
    assert bits(out["geopotential_height_quality"]) == [0, qc.VERTICAL, qc.VERTICAL, 0]


def test_a_superadiabatic_jump_is_flagged():
    ds = sounding({"pressure": [100000, 85000, 70000], "temperature": [280.0, 290.0, 260.0]})
    out = qc.flag(ds, "upper_air/soundings", now=NOW)
    # 280 K at 1000 hPa under 290 K at 850 hPa is fine (an inversion); 260 K at 700 hPa
    # is a potential temperature of ~289 K, 15 K below the 304 K beneath it.
    assert bits(out["temperature_quality"]) == [0, qc.VERTICAL, qc.VERTICAL]


def test_vertical_checks_stay_within_their_profile():
    ds = sounding(
        {"pressure": [90000, 80000, 100000, 90000], "time_since_launch": [0, 60, 0, 60]},
        counts=[2, 2],
    )
    out = qc.flag(ds, "upper_air/soundings", now=NOW)
    assert bits(out["level_quality_flag"]) == [0, 0, 0, 0]


def test_an_ocean_profile_must_deepen_strictly():
    ds = xr.Dataset(
        {
            "time": ("profile", np.full(2, np.datetime64("2026-11-01T00:00", "s"))),
            "arrival_time": ("profile", np.full(2, ARRIVED)),
            "latitude": ("profile", [-40.0, -40.0]),
            "longitude": ("profile", [20.0, 20.0]),
            "level_count": ("profile", np.array([4, 2], dtype="int32")),
            "sea_water_pressure": ("level", [1e4, 2e4, 2e4, 3e4, np.nan, np.nan]),
            "depth": ("level", [np.nan] * 4 + [5.0, 1.0]),
            "sea_water_temperature": ("level", [290.0, 289.0, 330.0, 280.0, 280.0, 280.0]),
        }
    )
    out = qc.flag(ds, "ocean/profiles", now=NOW)
    assert bits(out["sea_water_pressure_quality"]) == [0, 0, qc.VERTICAL, 0, 0, 0]
    assert bits(out["sea_water_temperature_quality"]) == [0, 0, qc.PHYSICAL, 0, 0, 0]
    # The second profile has no pressure: its depths are checked, and they shallow.
    assert bits(out["depth_quality"]) == [0, 0, 0, 0, 0, 0]
    assert bits(out["quality_flag"]) == [qc.VERTICAL | qc.PHYSICAL, 0]


# --------------------------------------------------------------------- provider flags


def test_aircraft_humidity_and_roll_angle_quality_set_the_provider_bit():
    n = 4
    ds = xr.Dataset(
        {
            "time": ("observation", np.full(n, np.datetime64("2026-11-01T00:00", "s"))),
            "arrival_time": ("observation", np.full(n, ARRIVED)),
            "latitude": ("observation", np.full(n, 40.0)),
            "longitude": ("observation", np.full(n, 140.0)),
            "moisture_quality": ("observation", np.array([0, 4, -1, 0], dtype="int16")),
            "roll_angle_quality": ("observation", np.array([0, 0, 0, 1], dtype="int16")),
        }
    )
    ds["moisture_quality"].attrs["_FillValue"] = -1
    out = qc.flag(ds, "aircraft", now=NOW)
    assert bits(out["quality_flag"]) == [0, qc.PROVIDER, 0, qc.PROVIDER]
    assert "moisture_quality_quality" not in out, "mapped into the overall flag only"


def test_wind_profiler_quality_by_its_significance():
    quality = np.array([1, 1, 1, 3, 2, 30, 80, 0], dtype="int32")
    significance = np.array([-1, 1, 21, 2, 6, 7, 7, -1], dtype="int32")
    ds = xr.Dataset(
        {
            "time": ("profile", np.full(1, np.datetime64("2026-11-01T00:00", "s"))),
            "arrival_time": ("profile", np.full(1, ARRIVED)),
            "latitude": ("profile", [52.0]),
            "longitude": ("profile", [-1.0]),
            "level_count": ("profile", np.array([8], dtype="int32")),
            "height": ("profile_level", np.arange(8) * 100.0 + 200),
            "wind_quality": ("profile_level", quality),
            "wind_quality_significance": ("profile_level", significance),
        }
    )
    out = qc.flag(ds, "upper_air/wind_profiler", now=NOW)
    p = qc.PROVIDER
    assert bits(out["level_quality_flag"]) == [p, p, 0, p, 0, p, 0, 0]
    assert bits(out["quality_flag"]) == [p]


def test_iasi_bad_quality_and_non_nominal_occultations():
    ds = xr.Dataset(
        {
            "time": ("observation", np.full(2, np.datetime64("2026-11-01T00:00", "s"))),
            "latitude": ("observation", [1.0, 1.0]),
            "longitude": ("observation", [1.0, 1.0]),
            "gqis_quality_flag": ("observation", np.array([0, 1], dtype="int8")),
        }
    )
    assert bits(qc.flag(ds, "satellite/iasi", now=NOW)["quality_flag"]) == [0, qc.PROVIDER]
    ro = ds.drop_vars("gqis_quality_flag").assign(
        radio_occultation_data_quality_flags=("observation", np.array([0, 1 << 15], "int32"))
    )
    assert bits(qc.flag(ro, "satellite/gnss_ro", now=NOW)["quality_flag"]) == [0, qc.PROVIDER]
