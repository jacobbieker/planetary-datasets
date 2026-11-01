"""GOES point products (winds, lightning) materialised into one store per satellite."""

from __future__ import annotations

import datetime as dt
import pathlib

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from planetary_datasets.providers import goes_l2_points as gp

DAY = dt.date(2026, 10, 27)
NOW = dt.datetime(2026, 11, 2)


def _folder(root, prefix, when):
    folder = root / prefix / f"{when:%Y}" / f"{when.timetuple().tm_yday:03d}" / f"{when:%H}"
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def _stamp(when):
    return when.strftime("%Y%j%H%M%S") + "0"


def glm_file(root, when, events, groups, flashes, energy_scale=1.9024e-17):
    """A GLM LCFA file: three lists, times as offsets from the file's start."""
    start = pd.Timestamp(when)
    since = f"seconds since {start:%Y-%m-%d %H:%M:%S.000}"
    offset = {"scale_factor": np.float32(0.0003814756), "add_offset": np.float32(-5.0)}
    ds = xr.Dataset(
        {
            "event_energy": (
                ("number_of_events",),
                np.arange(events, dtype="int16"),
                {"scale_factor": np.float32(energy_scale), "add_offset": np.float32(2.85e-16)},
            ),
            "event_lat": (
                ("number_of_events",),
                np.full(events, 1000, "int16"),
                {"scale_factor": np.float32(0.00203128), "add_offset": np.float32(-66.56)},
            ),
            "event_time_offset": (
                ("number_of_events",),
                np.arange(events, dtype="int16") * 100,
                {**offset, "units": since},
            ),
            "event_parent_group_id": (("number_of_events",), np.zeros(events, "int32")),
            "group_lat": (("number_of_groups",), np.linspace(10, 20, groups).astype("float32")),
            "flash_area": (
                ("number_of_flashes",),
                np.ones(flashes, "int16"),
                {"scale_factor": np.float32(152601.86), "add_offset": np.float32(0.0)},
            ),
            "event_count": ((), np.int32(events)),
            "product_time": (
                (),
                np.float64((start - gp.J2000).total_seconds()),
                {"units": "seconds since 2000-01-01 12:00:00"},
            ),
        },
        attrs={"time_coverage_start": f"{start:%Y-%m-%dT%H:%M:%S.0Z}", "orbital_slot": "GOES-East"},
    )
    name = f"OR_GLM-L2-LCFA_G19_s{_stamp(when)}_e{_stamp(when)}_c{_stamp(when)}.nc"
    path = _folder(root, "GLM-L2-LCFA", when) / name
    encoding = {v: {"_FillValue": None} for v in ds.variables}
    ds.to_netcdf(path, engine="h5netcdf", encoding=encoding)
    return path


def dmw_file(root, when, n, band=14, sector="F", extra=False):
    """A derived-motion-wind file: one list of winds plus per-file statistics."""
    measures = ("nMeasures",)
    variables = {
        "wind_speed": (measures, np.linspace(1, 40, n).astype("float32"), {"units": "m s-1"}),
        "lat": (measures, np.linspace(-60, 60, n), {"units": "degrees_north"}),
        "lon": (measures, np.linspace(-120, -30, n), {"units": "degrees_east"}),
        "time": (
            measures,
            np.full(n, (pd.Timestamp(when) - gp.J2000).total_seconds()),
            {"units": "seconds since 2000-01-01 12:00:00"},
        ),
        "DQF": (measures, np.zeros(n, "int8")),
        "mean_wind_speed": ((), np.float32(12.5), {"units": "m s-1"}),
        "atmospheric_layer_pressure": (("atmospheric_layer",), np.array([300, 500, 700], "f4")),
    }
    if extra:
        variables["pressure"] = (measures, np.full(n, 500.0, "float32"), {"units": "hPa"})
    ds = xr.Dataset(variables)
    prefix = "ABI-L2-DMWF" if sector == "F" else f"ABI-L2-DMW{sector[0]}"
    name = f"OR_ABI-L2-DMW{sector}-M6C{band:02d}_G19_s{_stamp(when)}_e{_stamp(when)}_c1.nc"
    path = _folder(root, prefix, when) / name
    ds.to_netcdf(path, engine="h5netcdf", encoding={v: {"_FillValue": None} for v in ds.variables})
    return path


@pytest.fixture
def archive(tmp_path):
    import obstore

    root = tmp_path / "noaa-goes19"
    root.mkdir()
    return root, obstore.store.LocalStore(str(root))


def store(config):
    return config.icechunk_repo(gp.store_prefix(19)).readonly_session("main").store


def test_groups_are_named_by_product_sector_and_band():
    assert gp.group_of("x/OR_GLM-L2-LCFA_G19_s20263001200000_e1_c1.nc") == "glm_lcfa"
    assert gp.group_of("x/OR_ABI-L2-DMWF-M6C14_G19_s2026300120020_e1_c1.nc") == "dmw_f_c14"
    assert gp.group_of("x/OR_ABI-L2-DMWVM1-M6C08_G19_s2026300120020_e1_c1.nc") == "dmwv_m1_c08"
    assert gp.group_of("x/OR_ABI-L2-ACMF-M6_G19_s2026300120020_e1_c1.nc") is None
    assert gp.partitions_of(DAY, "GLM") == [f"{DAY}T00", f"{DAY}T06", f"{DAY}T12", f"{DAY}T18"]
    assert gp.partition_of("x/OR_GLM-L2-LCFA_G19_s20263001400000_e1.nc", DAY, "GLM") == f"{DAY}T12"


def test_lightning_lists_append_with_their_files_and_times(archive, local_config):
    root, src = archive
    first = dt.datetime(2026, 10, 27, 12, 0, 0)
    second = first + dt.timedelta(seconds=20)
    glm_file(root, first, events=5, groups=3, flashes=2)
    glm_file(root, second, events=4, groups=2, flashes=0)
    written = gp.run_prefix(19, "GLM-L2-LCFA", local_config, now=NOW, store=src)
    assert written == 2
    assert gp.run_prefix(19, "GLM-L2-LCFA", local_config, now=NOW, store=src) == 0, "ledgered"

    tree = xr.open_datatree(store(local_config), engine="zarr", consolidated=False)
    assert "glm_lcfa" in tree.children and "layout" in tree.attrs
    ds = xr.open_zarr(store(local_config), group="glm_lcfa", consolidated=False)
    assert {d: ds.sizes[d] for d in ("event", "group", "flash", "file")} == {
        "event": 9,
        "group": 5,
        "flash": 2,
        "file": 2,
    }
    assert ds["event_file_index"].values.tolist() == [0] * 5 + [1] * 4
    assert ds["flash_file_index"].values.tolist() == [0, 0]
    # Packed values decode as the files do.
    np.testing.assert_allclose(ds["event_lat"].values, 1000 * 0.00203128 - 66.56, rtol=1e-6)
    raw = xr.open_zarr(
        store(local_config),
        group="glm_lcfa",
        consolidated=False,
        mask_and_scale=False,
        decode_times=False,
        decode_timedelta=False,
    )
    assert raw["event_energy"].dtype == np.int16
    assert raw["event_energy"].values.tolist() == [0, 1, 2, 3, 4, 0, 1, 2, 3]
    # Each event's time is its own file's start plus its offset.
    refs = pd.DatetimeIndex(ds["time_offset_reference"].values)
    assert list(refs) == [pd.Timestamp(first), pd.Timestamp(second)]
    assert raw["event_time_offset"].attrs["units"] == "s"
    offsets = raw["event_time_offset"].values * np.float32(0.0003814756) + np.float32(-5.0)
    when = refs[ds["event_file_index"].values] + pd.to_timedelta(offsets, unit="s")
    assert when[5] == pd.Timestamp(second) + pd.to_timedelta(np.float32(-5.0), unit="s")
    assert ds["file_name"].values[1].startswith("OR_GLM-L2-LCFA_G19_s2026300120020")


def test_winds_by_band_with_a_field_missing_from_some_files(archive, local_config):
    root, src = archive
    first = dt.datetime(2026, 10, 27, 12)
    dmw_file(root, first, n=3)
    dmw_file(root, first + dt.timedelta(minutes=10), n=2, extra=True)
    dmw_file(root, first, n=4, band=7)
    gp.run_prefix(19, "ABI-L2-DMWF", local_config, now=NOW, store=src)
    tree = xr.open_datatree(store(local_config), engine="zarr", consolidated=False)
    assert {"dmw_f_c14", "dmw_f_c07"} <= set(tree.children)
    ds = xr.open_zarr(store(local_config), group="dmw_f_c14", consolidated=False)
    assert ds.sizes["wind"] == 5 and ds.sizes["file"] == 2
    assert {"latitude", "longitude", "data_quality_flag"} <= set(ds.data_vars)
    assert ds["time"].dtype.kind == "M" and pd.Timestamp(ds["time"].values[0]) == first
    assert np.isnan(ds["pressure"].values[:3]).all() and (ds["pressure"].values[3:] == 500).all()
    assert ds["mean_wind_speed"].dims == ("file",)
    np.testing.assert_array_equal(ds["latitude"].values[:3], np.linspace(-60, 60, 3))


def test_a_changed_packing_is_refused_not_mixed(archive, local_config):
    root, src = archive
    glm_file(root, dt.datetime(2026, 10, 27, 0), events=2, groups=1, flashes=1)
    gp.run_prefix(19, "GLM-L2-LCFA", local_config, now=NOW, store=src)
    later = dt.datetime(2026, 10, 27, 7)
    path = glm_file(root, later, events=2, groups=1, flashes=1, energy_scale=3e-17)
    repo = local_config.icechunk_repo(gp.store_prefix(19))
    with pytest.raises(gp.PackingChanged):
        gp.write_partition(repo, "glm_lcfa", f"{DAY}T06", [(path.name, path.read_bytes())])
    ds = xr.open_zarr(store(local_config), group="glm_lcfa", consolidated=False)
    assert ds.sizes["file"] == 1


def test_mesoscale_and_glm_are_not_confused_with_gridded_products():
    assert pathlib.Path(gp.store_prefix(16)).name == "goes16_l2_points.icechunk"
    assert gp.family_of("dmwv_m2_c08") == "DMWV" and gp.family_of("glm_lcfa") == "GLM"


def test_a_large_partition_is_parsed_in_worker_processes(archive, local_config):
    root, src = archive
    start = dt.datetime(2026, 10, 27, 18)
    for i in range(12):
        glm_file(root, start + dt.timedelta(seconds=20 * i), events=3, groups=2, flashes=1)
    assert gp.run_prefix(19, "GLM-L2-LCFA", local_config, now=NOW, store=src) == 12
    ds = xr.open_zarr(store(local_config), group="glm_lcfa", consolidated=False)
    assert ds.sizes["file"] == 12 and ds.sizes["event"] == 36
    assert ds["event_file_index"].values.tolist() == [i for i in range(12) for _ in range(3)]


def test_a_broken_parser_pool_is_replaced(monkeypatch):
    class Dead:
        def map(self, *args, **kwargs):
            raise gp.BrokenProcessPool("a worker was killed")

        def shutdown(self, **kwargs):
            pass

    class Alive:
        def map(self, fn, items, chunksize=1):
            return map(fn, items)

    pools = iter([Dead(), Alive()])
    monkeypatch.setattr(gp, "_PARSER", None)
    monkeypatch.setattr(gp, "_parser", lambda: next(pools))
    monkeypatch.setattr(gp, "_read_safely", lambda item: item[0])
    bodies = [(f"k{i}", b"") for i in range(12)]
    assert gp._parse(bodies) == [k for k, _ in bodies]


def test_a_pool_that_breaks_twice_fails_the_partition(monkeypatch):
    class Dead:
        def map(self, *args, **kwargs):
            raise gp.BrokenProcessPool("a worker was killed")

        def shutdown(self, **kwargs):
            pass

    monkeypatch.setattr(gp, "_parser", Dead)
    with pytest.raises(gp.BrokenProcessPool):
        gp._parse([(f"k{i}", b"") for i in range(12)])


def _wind_file(units):
    attrs = {} if units is None else {"units": units}
    return {"latitude": (("wind",), np.array([1.0, 2.0]), attrs)}


def test_a_file_omitting_units_still_combines_and_keeps_them():
    """One GOES-18 DMW file on 2024-09-04 had latitude without units; values are unpacked."""
    ds = gp.combine([_wind_file(None), _wind_file("degrees_north")], first_file=0)
    assert ds["latitude"].attrs["units"] == "degrees_north"
    assert ds.sizes["wind"] == 4
    gp.check_packing(ds, {"latitude": {"units": "degrees_north"}}, "dmw_c_c10")


def test_units_that_disagree_are_still_refused():
    files = [_wind_file("degrees_north"), _wind_file("radians")]
    with pytest.raises(gp.PackingChanged):
        gp.combine(files, first_file=0)
