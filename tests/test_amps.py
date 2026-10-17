"""AMPS provider: URL layout, GRIB matching, and a synthetic GRIB1 round trip.

The GRIB files here are built in memory with eccodes from its polar stereographic GRIB1
sample, shaped like the AMPS files (centre 60, local table 2); nothing touches GDEX.
"""

from __future__ import annotations

import dataclasses
import pathlib

import numpy as np
import pandas as pd
import pytest
from helpers import read_store

from planetary_datasets.providers import amps
from planetary_datasets.providers.amps import AMPSProvider, GribMessage

NI, NJ = 4, 3
INIT = pd.Timestamp("2026-09-30T12:00")


def _message(
    param: int,
    level_type: str,
    level: int,
    values: np.ndarray,
    it: pd.Timestamp = INIT,
    step: int = 0,
    p1: int | None = None,
    accumulation: bool = False,
) -> bytes:
    """One AMPS-shaped GRIB1 message on a small south polar stereographic grid."""
    import eccodes

    gid = eccodes.codes_grib_new_from_samples("polar_stereographic_sfc_grib1")
    try:
        settings = {
            "centre": 60,
            "table2Version": 2,
            "Ni": NI,
            "Nj": NJ,
            "DxInMetres": 23019,
            "DyInMetres": 23019,
            "latitudeOfFirstGridPointInDegrees": -24.65,
            "longitudeOfFirstGridPointInDegrees": 38.346,
            "orientationOfTheGridInDegrees": 180.0,
            "projectionCentreFlag": 128,
            "jScansPositively": 1,
            "indicatorOfParameter": param,
            "typeOfLevel": level_type,
            "level": level,
            "dataDate": int(it.strftime("%Y%m%d")),
            "dataTime": int(it.strftime("%H%M")),
            "unitOfTimeRange": 1,
            "bitsPerValue": 24,
        }
        if accumulation:
            settings.update(timeRangeIndicator=4, P1=step if p1 is None else p1, P2=step)
        else:
            settings.update(timeRangeIndicator=0, P1=step, P2=0)
        for key, value in settings.items():
            eccodes.codes_set(gid, key, value)
        eccodes.codes_set_values(gid, np.asarray(values, dtype="float64").ravel())
        return eccodes.codes_get_message(gid)
    finally:
        eccodes.codes_release(gid)


def _field(base: float) -> np.ndarray:
    return base + np.arange(NI * NJ, dtype="float64").reshape(NJ, NI)


def _write_step(path: pathlib.Path, step: int, it: pd.Timestamp = INIT) -> pathlib.Path:
    """A file holding a little of everything the provider has to handle."""
    messages = [
        _message(176, "surface", 0, _field(-80.0), it, step),
        _message(177, "surface", 0, _field(100.0), it, step),
        _message(11, "isobaricInhPa", 500, _field(250.0 + step), it, step),
        _message(11, "isobaricInhPa", 850, _field(270.0 + step), it, step),
        _message(11, "heightAboveGround", 2, _field(280.0 + step), it, step),
        _message(2, "meanSea", 0, _field(100000.0 + step), it, step),
        _message(11, "hybrid", 1, _field(275.0 + step), it, step),
        _message(11, "depthBelowLandLayer", 40, _field(260.0), it, step),
        # The 3 h accumulation every domain publishes, and the 1 h one the nests add.
        _message(61, "surface", 0, _field(step * 1.0), it, step, max(0, step - 3), True),
        _message(61, "surface", 0, _field(-1.0), it, step, max(0, step - 1), True),
        # Undocumented in the AMPS table, so not stored.
        _message(133, "entireAtmosphere", 1, _field(5.0), it, step),
    ]
    if step == 0:
        messages.append(_message(7, "surface", 0, _field(1000.0), it, step))
    path.write_bytes(b"".join(messages))
    return path


def _provider(local_config=None, steps=(0, 3)) -> AMPSProvider:
    provider = AMPSProvider("d1", config=local_config, dask_threads=2)
    provider.domain = dataclasses.replace(provider.domain, step_hours=tuple(steps))
    return provider


def _files(tmp_path: pathlib.Path, it: pd.Timestamp = INIT, steps=(0, 3)) -> list[str]:
    return [str(_write_step(tmp_path / f"{it:%Y%m%d%H}_WRF_d1_f{s:03d}.grb", s, it)) for s in steps]


# ------------------------------------------------------------------ layout


@pytest.mark.parametrize(
    ("domain", "prefix", "n_steps", "last_step"),
    [
        ("d1", "bkr/dmi/amps_d1.icechunk", 17, 48),
        ("d2", "bkr/dmi/amps_d2.icechunk", 17, 48),
        ("d3", "bkr/dmi/amps_d3.icechunk", 40, 39),
        ("d5", "bkr/dmi/amps_d5.icechunk", 40, 39),
        ("d6", "bkr/dmi/amps_d6.icechunk", 40, 39),
        ("DRAKE", "bkr/dmi/amps_drake.icechunk", 17, 48),
        ("nz", "bkr/dmi/amps_nz.icechunk", 17, 48),
    ],
)
def test_each_domain_has_its_own_store_and_keeps_only_48_hours(domain, prefix, n_steps, last_step):
    provider = AMPSProvider(domain)
    assert provider.store_prefix == prefix
    assert provider.append_dim == "init_time"
    assert len(provider.steps) == n_steps
    assert provider.steps[0] == 0 and provider.steps[-1] == last_step
    assert max(provider.steps) <= amps.MAX_LEAD_HOURS


def test_store_path_follows_the_local_override(local_config):
    assert AMPSProvider("d3", config=local_config).store_path.endswith("bkr/dmi/amps_d3.icechunk")


def test_unknown_domain_is_refused():
    with pytest.raises(ValueError, match="unknown AMPS domain"):
        AMPSProvider("d4")


def test_urls_follow_the_gdex_layout():
    urls = AMPSProvider("DRAKE").expected_urls(pd.Timestamp("2026-09-30T12:00"))
    assert urls[0] == (
        "https://data.gdex.ucar.edu/d473002/grib/WRF24/2026/09/30/2026093012_WRF_DRAKE_f000.grb"
    )
    assert urls[-1].endswith("2026093012_WRF_DRAKE_f048.grb")


@pytest.mark.parametrize(
    ("domain", "it", "valid"),
    [
        ("d1", "2017-10-10T12:00", True),
        ("d1", "2017-10-10T00:00", False),  # the previous grid configuration
        ("d1", "2026-01-01T06:00", False),  # AMPS only runs at 00 and 12 UTC
        ("DRAKE", "2020-08-08T00:00", True),
        ("DRAKE", "2019-01-01T00:00", False),  # before the domain existed
    ],
)
def test_init_times_are_checked_against_the_domain(domain, it, valid):
    assert AMPSProvider(domain).is_valid_init(pd.Timestamp(it)) is valid


def test_ocean_domains_do_not_declare_model_level_fields():
    full = {f.name for f in amps.fields_for(amps.DOMAINS["d1"])}
    ocean = {f.name for f in amps.fields_for(amps.DOMAINS["nz"])}
    assert "temperature_model_level" in full and "outgoing_longwave_radiation" in full
    assert full - ocean == {f.name for f in amps.FIELDS if f.model_level_output}


def test_float16_only_for_range_safe_fields():
    halves = {f.name for f in amps.FIELDS if f.dtype == "float16"}
    assert halves, "temperature and wind should be stored as float16"
    # Range-safe fields, checked against real d1 and d5 runs: none overflows float16, and
    # the worst rounding is 0.5 W m-2 on a flux, 4 m on a cloud base, 0.008 on w.
    range_safe = (
        "temperature", "wind", "cloud_fraction", "vertical_velocity", "albedo",
        "sea_ice_cover", "heat_flux", "radiation_flux", "longwave_radiation",
        "boundary_layer_height", "cloud_base_height", "inversion_",
    )  # fmt: skip
    for name in halves:
        assert any(word in name for word in range_safe), name
    full = {f.name for f in amps.FIELDS if f.dtype == "float32"}
    # Moisture, geopotential and pressure never go to 16 bits.
    for name in {f.name for f in amps.FIELDS}:
        if any(w in name for w in ("humidity", "mixing_ratio", "geopotential", "pressure")):
            assert name in full, name
    assert {"precipitable_water", "total_precipitation_3h"} <= full


def test_no_hardcoded_credentials_or_buckets():
    import inspect

    source = inspect.getsource(amps)
    for needle in ("AWS_", "s3://", "source.coop", "secret"):
        assert needle not in source


# --------------------------------------------------------------- matching


def _msg(param, level_type, level, end_step=24, p1=None, time_range=0) -> GribMessage:
    return GribMessage(
        path="f",
        offset=0,
        length=1,
        param=param,
        level_type=level_type,
        level=level,
        time_range=time_range,
        p1=end_step if p1 is None else p1,
        end_step=end_step,
        ni=NI,
        nj=NJ,
        init=INIT,
    )


def test_pressure_levels_map_onto_the_level_axis():
    field, index = amps.match_message(_msg(11, "isobaricInhPa", 850), amps.FIELDS)
    assert field.name == "temperature"
    assert amps.PRESSURE_LEVELS[index] == 850


def test_same_parameter_on_different_levels_goes_to_different_variables():
    names = {
        amps.match_message(_msg(33, level_type, level), amps.FIELDS)[0].name
        for level_type, level in [
            ("isobaricInhPa", 500),
            ("hybrid", 1),
            ("heightAboveGround", 10),
            ("heightAboveSea", 150),
            ("heightAboveSea", 300),
        ]
    }
    assert len(names) == 5


@pytest.mark.parametrize(
    ("end_step", "p1", "taken"),
    [
        (24, 21, True),  # the 3 h accumulation
        (24, 23, False),  # the 1 h accumulation the hourly nests add
        (2, 0, True),  # before 3 h, accumulated from the init time
        (2, 1, False),
        (0, 0, True),
    ],
)
def test_precipitation_is_always_the_3_hour_window(end_step, p1, taken):
    message = _msg(61, "surface", 0, end_step=end_step, p1=p1, time_range=4)
    assert (amps.match_message(message, amps.FIELDS) is not None) is taken


def test_undocumented_and_unknown_parameters_are_not_matched():
    assert amps.match_message(_msg(133, "entireAtmosphere", 1), amps.FIELDS) is None
    assert amps.match_message(_msg(999, "surface", 0), amps.FIELDS) is None
    assert amps.match_message(_msg(11, "isobaricInhPa", 123), amps.FIELDS) is None


def test_grib_file_complete(tmp_path):
    good = _write_step(tmp_path / "good.grb", 0)
    assert amps.grib_file_complete(good)
    cut = tmp_path / "cut.grb"
    cut.write_bytes(good.read_bytes()[:-10])
    assert not amps.grib_file_complete(cut)
    other = tmp_path / "other.grb"
    other.write_bytes(b"<html>not found</html>")
    assert not amps.grib_file_complete(other)
    assert not amps.grib_file_complete(tmp_path / "missing.grb")


# ---------------------------------------------------------------- process


def test_process_builds_a_lazy_init_time(tmp_path):
    provider = _provider()
    ds = provider.process(_files(tmp_path), INIT)

    assert ds.sizes["init_time"] == 1 and ds.sizes["step"] == 2
    assert ds.sizes["y"] == NJ and ds.sizes["x"] == NI
    assert ds["temperature"].chunks is not None, "data must stay lazy until written"
    assert list(ds.step.values / np.timedelta64(1, "h")) == [0, 3]
    assert set(ds.data_vars) == {f.name for f in amps.fields_for(provider.domain)}

    t = ds["temperature"].astype("float32").load()
    np.testing.assert_allclose(t.sel(level=500).isel(init_time=0, step=1), _field(253.0))
    np.testing.assert_allclose(t.sel(level=850).isel(init_time=0, step=0), _field(270.0))
    # Levels and fields the files do not have are NaN, not dropped.
    assert np.isnan(t.sel(level=1000)).all()
    assert np.isnan(ds["albedo"].load()).all()

    msl = ds["mean_sea_level_pressure"].load()
    assert msl.dtype == np.float32
    np.testing.assert_array_equal(msl.isel(init_time=0, step=1), _field(100003.0))
    assert ds["temperature_at_2m"].dtype == np.float16
    np.testing.assert_allclose(
        ds["temperature_model_level"].sel(model_level=1).isel(init_time=0, step=1).astype(float),
        _field(278.0),
    )
    np.testing.assert_allclose(
        ds["soil_temperature"].sel(soil_depth=40).isel(init_time=0, step=0).astype(float),
        _field(260.0),
    )
    # The 3 h window, not the 1 h one (which is -1 everywhere in these files).
    np.testing.assert_allclose(
        ds["total_precipitation_3h"].isel(init_time=0, step=1), _field(3.0), rtol=1e-6
    )


def test_process_takes_static_coordinates_from_the_files(tmp_path):
    ds = _provider().process(_files(tmp_path), INIT)
    np.testing.assert_allclose(ds["latitude"], _field(-80.0), rtol=1e-6)
    np.testing.assert_allclose(ds["longitude"], _field(100.0), rtol=1e-6)
    np.testing.assert_allclose(ds["orography"], _field(1000.0), rtol=1e-6)
    # The land/sea mask is in none of these files.
    assert np.isnan(ds["land_sea_mask"]).all()
    # Projected axes: GRIB spacing, south pole projection.
    np.testing.assert_allclose(np.diff(ds["x"]), 23019.0)
    np.testing.assert_allclose(np.diff(ds["y"]), 23019.0)
    assert "+lat_0=-90" in ds.attrs["projection"]


def test_process_rejects_files_from_another_init_time(tmp_path):
    files = _files(tmp_path)
    _write_step(pathlib.Path(files[1]), 3, it=INIT - pd.Timedelta(12, "h"))
    with pytest.raises(ValueError, match="init time"):
        _provider().process(files, INIT)


def test_process_rejects_a_file_for_the_wrong_hour(tmp_path):
    files = _files(tmp_path)
    _write_step(pathlib.Path(files[1]), 6)
    with pytest.raises(ValueError, match="hour"):
        _provider().process(files, INIT)


def test_process_rejects_an_incomplete_file_set(tmp_path):
    with pytest.raises(ValueError, match="expected 2 files"):
        _provider().process(_files(tmp_path)[:1], INIT)


# ------------------------------------------------------------------ fetch


def test_fetch_skips_a_run_gdex_has_not_published(tmp_path, monkeypatch):
    provider = _provider()
    monkeypatch.setattr(AMPSProvider, "_published", staticmethod(lambda url: False))

    def _no_download(*args, **kwargs):
        raise AssertionError("nothing should be downloaded")

    monkeypatch.setattr(amps, "download_many", _no_download)
    assert provider.fetch(INIT, temp_dir=tmp_path) == []


def test_fetch_skips_an_init_time_with_missing_files(tmp_path, monkeypatch):
    provider = _provider()
    monkeypatch.setattr(AMPSProvider, "_published", staticmethod(lambda url: True))
    files = _files(tmp_path)
    monkeypatch.setattr(amps, "download_many", lambda urls, dest, **kw: [pathlib.Path(files[0])])
    assert provider.fetch(INIT, temp_dir=tmp_path) == []


def test_fetch_skips_times_amps_does_not_run(tmp_path):
    assert _provider().fetch(pd.Timestamp("2026-09-30T06:00"), temp_dir=tmp_path) == []


def test_fetch_downloads_a_truncated_file_again(tmp_path, monkeypatch):
    provider = _provider()
    monkeypatch.setattr(AMPSProvider, "_published", staticmethod(lambda url: True))
    files = [pathlib.Path(f) for f in _files(tmp_path)]
    good = files[1].read_bytes()
    files[1].write_bytes(good[:-10])
    monkeypatch.setattr(amps, "download_many", lambda urls, dest, **kw: list(files))

    def _redownload(url, dest, overwrite=False, **kw):
        assert overwrite
        pathlib.Path(dest).write_bytes(good)
        return pathlib.Path(dest)

    monkeypatch.setattr(amps, "download_one", _redownload)
    assert provider.fetch(INIT, temp_dir=tmp_path) == [str(f) for f in files]
    assert amps.grib_file_complete(files[1])


# ------------------------------------------------------------ store round trip


def test_run_partition_writes_appends_and_then_skips(local_config, tmp_path, monkeypatch):
    provider = _provider(local_config)
    first, second = INIT - pd.Timedelta(12, "h"), INIT

    def _fetch(self, it, temp_dir=None, **kwargs):
        directory = tmp_path / f"{it:%Y%m%d%H}"
        directory.mkdir(exist_ok=True)
        return _files(directory, it)

    monkeypatch.setattr(AMPSProvider, "fetch", _fetch)

    assert provider.run_partition(first) is True
    assert provider.run_partition(second) is True
    assert provider.run_partition(second) is False

    stored = read_store(provider)
    assert [pd.Timestamp(t) for t in stored.init_time.values] == [first, second]
    assert stored.sizes["step"] == 2
    assert stored["temperature"].dtype == np.float16
    np.testing.assert_allclose(
        stored["temperature"].sel(init_time=second, level=500).isel(step=1).astype(float),
        _field(253.0),
    )
    np.testing.assert_allclose(stored["latitude"], _field(-80.0), rtol=1e-6)
    assert provider.generations() == ["bkr/dmi/amps_d1.icechunk"]
    assert provider.missing_timesteps(pd.DatetimeIndex([first, second])) == []


def test_float32_fields_are_bit_rounded_except_moisture_geopotential_and_pressure():
    from planetary_datasets.providers.amps import AMPSProvider

    provider = AMPSProvider("d1")
    assert provider.keepbits_for("vertical_velocity") == 12
    assert provider.keepbits_for("total_precipitation_3h") == 12
    for exact in (
        "relative_humidity",
        "water_vapor_mixing_ratio",
        "geopotential_height",
        "surface_pressure",
        "mean_sea_level_pressure",
        "pressure_model_level",
    ):
        assert provider.keepbits_for(exact) is None, exact
