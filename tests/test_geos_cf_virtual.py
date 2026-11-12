"""Virtual GEOS-CF stores, against a local tree laid out like the NCCS portal."""

import datetime as dt
import json
import pathlib

import numpy as np
import pandas as pd
import pytest
import xarray as xr
import zarr

from planetary_datasets.providers.virtualized import geos_cf as g
from planetary_datasets.providers.virtualized.geos_cf_names import variable_name, variable_names

LAT = np.linspace(-90, 90, 5)
LON = np.linspace(-180, 135, 8)
#: Far enough after every test day that all of them have settled.
LATER = dt.datetime(2030, 1, 1)


def write_file(
    path: pathlib.Path,
    when: pd.Timestamp,
    levels: int,
    seed: float,
    extra: bool = False,
    lon_chunk: int = LON.size,
):
    """A small GEOS-CF-like file: one time, ``levels`` levels, one chunk per level field.

    ``lon_chunk`` splits each chunk along longitude.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    shape = (1, levels, LAT.size, LON.size)
    data = {
        "NO2": (("time", "lev", "lat", "lon"), np.full(shape, seed, np.float32)),
        "T": (("time", "lev", "lat", "lon"), np.full(shape, 200 + seed, np.float32)),
    }
    if extra:
        data["EMIS_CO_ANTH"] = (
            ("time", "lat", "lon"),
            np.full(shape[:1] + shape[2:], seed, np.float32),
        )
    ds = xr.Dataset(
        data,
        coords={
            "time": ("time", [0.0], {"units": f"minutes since {when:%Y-%m-%d %H:%M:%S}"}),
            "lev": ("lev", np.arange(levels, 0, -1, dtype="float64") if levels > 1 else [72.0]),
            "lat": ("lat", LAT),
            "lon": ("lon", LON),
        },
    )
    ds["NO2"].attrs.update(
        long_name="Nitrogen dioxide (NO2) mole fraction in dry air", units="mol mol-1"
    )
    ds["T"].attrs.update(long_name="air_temperature", units="K")
    ds["lev"].attrs.update(standard_name="model_level_number", units="layer")
    encoding = {
        name: {
            "zlib": True,
            "shuffle": True,
            "chunksizes": (1, 1, LAT.size, lon_chunk)
            if ds[name].ndim == 4
            else (1, LAT.size, lon_chunk),
            "_FillValue": np.float32(1e15),
        }
        for name in ds.data_vars
    }
    ds.to_netcdf(path, engine="netcdf4", encoding=encoding)
    return path


@pytest.fixture
def portal(tmp_path):
    """A local directory laid out like the portal, and a Portal over it."""
    root = tmp_path / "geos-cf"
    root.mkdir()
    return root, g.Portal(f"file://{root}")


def ana_file(root, stream_path, prefix, collection, when, revision=".R0", **kwargs):
    """Write one analysis file where the portal would list it."""
    day = f"{stream_path}/Y{when:%Y}/M{when:%m}/D{when:%d}"
    name = f"{prefix}.{collection}.{when:%Y%m%d_%H%M}z{revision}.nc4"
    return write_file(root / day / name, when, **kwargs)


def read(collection, config, container, group=None):
    """Open a collection's group of a written store for reading, with the repository."""
    repo = g.open_store(collection.stream, config, create=False, container=container)
    store = repo.readonly_session("main").store
    return xr.open_zarr(store, group=group or collection.group, consolidated=False), repo


# ----------------------------------------------------------------------------- names


def test_file_names_parse_for_every_stream():
    """File names parse for every stream."""
    ana = g.parse_name(
        "GEOS.cf.ana.chm_inst_1hr_glo_L1440x721_v72.20261101_0030z.R1.nc4", "https://x/d"
    )
    assert ana.collection == "chm_inst_1hr_glo_L1440x721_v72"
    assert ana.valid == pd.Timestamp("2026-11-01T00:30") and ana.init is None and ana.revision == 1
    fcst = g.parse_name(
        "GEOS.cf.fcst.htf_inst_15mn_glo_L1440x721_slv.20261105_09z%2B20261105_0915z.R0.nc4",
        "https://x/d",
    )
    assert (fcst.init, fcst.valid) == (
        pd.Timestamp("2026-11-05T09"),
        pd.Timestamp("2026-11-05T09:15"),
    )
    # Decoded: the HTTP store re-encodes a literal "+", and would double-encode "%2B".
    assert fcst.url.endswith("09z+20261105_0915z.R0.nc4")
    v1 = g.parse_name("GEOS-CF.v01.rpl.xgc_tavg_1hr_g1440x721_x1.20250601_0030z.nc4", "https://x/d")
    assert v1.collection == "xgc_tavg_1hr_g1440x721_x1" and v1.revision == -1
    hindcast = g.parse_name(
        "CF2_hindcast.aqc_tavg_1hr_glo_L1440x721_slv.20240601_0030z.R0.nc4", "d"
    )
    assert hindcast.collection == "aqc_tavg_1hr_glo_L1440x721_slv"
    assert g.parse_name("GEOS-CF.v01.rpl.const_g1440x721_x1.000000_0000z.nc4", "d") is None


def test_the_latest_revision_of_a_file_wins():
    """The latest revision of a file wins."""
    files = [
        g.parse_name(f"GEOS.cf.ana.aqc_tavg_1hr_glo_L1440x721_slv.20261101_0030z{r}.nc4", "d")
        for r in ("", ".R0", ".R1")
    ]
    (best,) = g.latest_revisions(files)
    assert best.revision == 1


def test_collections_know_their_levels_steps_and_store():
    """Collections know their levels steps and store."""
    stream = g.STREAMS["v2_fcst"]
    averaged = g.Collection(stream, "aqc_tavg_1hr_glo_L1440x721_slv")
    instant = g.Collection(stream, "htf_inst_15mn_glo_L1440x721_slv")
    three_hourly = g.Collection(stream, "ghg_inst_3hr_glo_L1440x721_v72")
    assert averaged.steps[0] == pd.Timedelta("30min") and len(averaged.steps) == 120
    assert instant.steps[0] == pd.Timedelta("15min") and len(instant.steps) == 480
    assert three_hourly.steps[-1] == pd.Timedelta("120h") and len(three_hourly.steps) == 40
    assert averaged.single_level and averaged.level_dim is None
    assert three_hourly.level_dim == "model_level"
    assert g.Collection(stream, "chm_inst_1hr_glo_L1440x721_p23").level_dim == "pressure_level"
    assert three_hourly.store_prefix == "bkr/geos/geos_cf_v2_fcst.icechunk"
    assert three_hourly.group == "ghg_inst_3hr_v72"
    sat = g.Collection(g.STREAMS["v1_ana"], "sat_inst_1hr_r721x361_v72")
    assert sat.store_prefix == "bkr/geos/geos_cf_v1_ana.icechunk"
    assert sat.group == "sat_inst_1hr_v72_0p5deg"
    assert {s.store_prefix for s in g.STREAMS.values()} == {
        "bkr/geos/geos_cf_v2_ana.icechunk",
        "bkr/geos/geos_cf_v2_fcst.icechunk",
        "bkr/geos/geos_cf_v1_ana.icechunk",
        "bkr/geos/geos_cf_v1_fcst.icechunk",
    }


@pytest.mark.parametrize(
    "code, attrs, expected",
    [
        ("NO2", {"units": "mol mol-1"}, "nitrogen_dioxide_mole_fraction"),
        ("T", {}, "temperature"),
        ("U10M", {}, "10m_u_component_of_wind"),
        ("SLP", {}, "mean_sea_level_pressure"),
        (
            "EMIS_CO_ANTH",
            {"long_name": "carbon monoxide ship emission mass flux"},
            "carbon_monoxide_anthropogenic_emission_flux",
        ),
        ("EMIS_NO", {}, "nitric_oxide_total_emission_flux"),
        ("EMIS_CH4", {}, "methane_net_surface_flux"),
        ("TropCol_NO2", {}, "tropospheric_column_nitrogen_dioxide"),
        ("TotCol_O3", {}, "total_column_ozone"),
        ("AOD550_DUST", {}, "dust_aerosol_optical_depth_550nm"),
        ("PM25_RH35_GCC", {}, "particulate_matter_2_5um_rh35_geos_chem"),
        ("PM25su_RH35", {}, "sulphate_particulate_matter_2_5um_rh35"),
        ("Bry", {"units": "mol mol-1"}, "total_inorganic_bromine_mole_fraction"),
    ],
)
def test_variables_get_explicit_era5_style_names(code, attrs, expected):
    """Variables get explicit era5 style names."""
    assert variable_name(code, attrs)[0] == expected


def test_two_codes_naming_one_variable_is_refused():
    """Two codes naming one variable is refused."""
    with pytest.raises(ValueError, match="both name"):
        variable_names({"HCHO": {"units": "mol mol-1"}, "CH2O": {"units": "mol mol-1"}})


# ----------------------------------------------------------------------------- analysis


def test_analysis_days_are_referenced_renamed_and_resumed(portal, local_config):
    """Analysis days are referenced renamed and resumed."""
    root, p = portal
    stream = g.STREAMS["v2_ana"]
    collection = "chm_inst_1hr_glo_L1440x721_v72"
    for day in ("2026-11-01", "2026-11-02"):
        for hour in range(3):
            when = pd.Timestamp(day) + pd.Timedelta(hours=hour)
            ana_file(
                root,
                "v2/ana",
                "GEOS.cf.ana",
                collection,
                when,
                levels=3,
                seed=hour + 10 * (day[-1] == "2"),
            )
    # Through worker processes, as the lanes open files.
    assert g.run_stream(stream, local_config, p, open_workers=2, now=LATER) == 6

    c = g.Collection(stream, collection)
    ds, repo = read(c, local_config, p.container)
    assert ds["nitrogen_dioxide_mole_fraction"].dims == (
        "time",
        "model_level",
        "latitude",
        "longitude",
    )
    assert ds["nitrogen_dioxide_mole_fraction"].attrs["source_name"] == "NO2"
    assert list(ds["time"].values) == list(
        pd.to_datetime(
            [
                "2026-11-01T00",
                "2026-11-01T01",
                "2026-11-01T02",
                "2026-11-02T00",
                "2026-11-02T01",
                "2026-11-02T02",
            ]
        )
    )
    # Values come through the virtual references.
    np.testing.assert_array_equal(
        ds["nitrogen_dioxide_mole_fraction"].isel(model_level=0).mean(["latitude", "longitude"]),
        [0, 1, 2, 10, 11, 12],
    )
    np.testing.assert_array_equal(ds["model_level"].values, [3, 2, 1])
    attrs = dict(ds.attrs)
    assert attrs[g.LEDGER_ATTR] == ["2026-11-01", "2026-11-02"]
    assert json.loads(attrs["geos_cf_variable_source_names"])["temperature"] == "T"
    # A second pass finds both days written and adds nothing.
    assert g.run_stream(stream, local_config, p, open_workers=0, now=LATER) == 0


def test_a_recent_incomplete_day_waits_until_complete_or_settled(portal, local_config):
    """A recent incomplete day waits until complete or settled."""
    root, p = portal
    stream = g.STREAMS["v2_ana"]
    collection = "met_inst_1hr_glo_L1440x721_p23"
    ana_file(
        root, "v2/ana", "GEOS.cf.ana", collection, pd.Timestamp("2026-11-07T00"), levels=2, seed=1
    )
    assert (
        g.run_stream(stream, local_config, p, open_workers=0, now=dt.datetime(2026, 11, 7, 12)) == 0
    )
    assert g.run_stream(stream, local_config, p, open_workers=0, now=dt.datetime(2026, 11, 12)) == 1
    ds, _ = read(g.Collection(stream, collection), local_config, p.container)
    assert "pressure_level" in ds.dims


def test_single_level_collections_drop_lev_and_keep_it_as_a_coordinate(portal, local_config):
    """Single level collections drop lev and keep it as a coordinate."""
    root, p = portal
    stream = g.STREAMS["v2_ana"]
    collection = "chm_tavg_1hr_glo_L1440x721_slv"
    ana_file(
        root,
        "v2_hindcast/ana",
        "CF2_hindcast",
        collection,
        pd.Timestamp("2024-06-01T00:30"),
        levels=1,
        seed=3,
        extra=True,
    )
    assert g.run_stream(stream, local_config, p, open_workers=0, now=LATER) == 1
    ds, _ = read(g.Collection(stream, collection), local_config, p.container)
    assert ds[
        "ozone_mole_fraction" if "ozone_mole_fraction" in ds else "nitrogen_dioxide_mole_fraction"
    ].dims == (
        "time",
        "latitude",
        "longitude",
    )
    assert float(ds["model_level"]) == 72
    assert ds["carbon_monoxide_anthropogenic_emission_flux"].dims == (
        "time",
        "latitude",
        "longitude",
    )
    assert ds["time"].values[0] == np.datetime64("2024-06-01T00:30")


def test_v1_single_level_only_leaves_the_3d_collections_out(portal, local_config):
    """V1 single level only leaves the 3d collections out."""
    root, p = portal
    when = pd.Timestamp("2025-06-01T00:30")
    ana_file(
        root,
        "v1/ana",
        "GEOS-CF.v01.rpl",
        "xgc_tavg_1hr_g1440x721_x1",
        when,
        revision="",
        levels=1,
        seed=1,
    )
    ana_file(
        root,
        "v1/ana",
        "GEOS-CF.v01.rpl",
        "xgc_tavg_1hr_g1440x721_v36",
        when,
        revision="",
        levels=2,
        seed=1,
    )
    assert g.run_stream(g.STREAMS["v1_ana"], local_config, p, open_workers=0, now=LATER) == 1
    repo = g.open_store(g.STREAMS["v1_ana"], local_config, create=False, container=p.container)
    root = zarr.open_group(repo.readonly_session("main").store, mode="r")
    assert sorted(root.group_keys()) == ["xgc_tavg_1hr_x1"]


# ----------------------------------------------------------------------------- forecasts


def test_a_forecast_run_has_every_step_with_unpublished_ones_missing(portal, local_config):
    """A forecast run has every step with unpublished ones missing."""
    root, p = portal
    stream = g.STREAMS["v2_fcst"]
    collection = "ghg_inst_3hr_glo_L1440x721_v72"
    init = pd.Timestamp("2026-11-05T09")
    steps = g.Collection(stream, collection).steps
    for i, step in enumerate(steps):
        if i == 5:
            continue  # never published
        valid = init + step
        name = f"GEOS.cf.fcst.{collection}.{init:%Y%m%d_%H}z+{valid:%Y%m%d_%H%M}z.R0.nc4"
        write_file(root / "v2/fcst/Y2026/M11/D05" / name, valid, levels=2, seed=i)
    assert g.run_stream(stream, local_config, p, open_workers=0, now=LATER) == 40

    ds, _ = read(g.Collection(stream, collection), local_config, p.container)
    no2 = ds["nitrogen_dioxide_mole_fraction"]
    assert no2.dims == ("init_time", "step", "model_level", "latitude", "longitude")
    assert ds["init_time"].values[0] == np.datetime64("2026-11-05T09")
    assert pd.Timedelta(ds["step"].values[0]) == pd.Timedelta("3h")
    assert ds["valid_time"].values[0, -1] == np.datetime64("2026-11-10T09")
    means = no2.isel(init_time=0, model_level=0).mean(["latitude", "longitude"]).values
    assert np.isnan(means[5])
    np.testing.assert_array_equal(means[[0, 1, 39]], [0, 1, 39])


def test_v1_forecast_runs_are_found_under_their_run_directory(portal, local_config):
    """V1 forecast runs are found under their run directory."""
    root, p = portal
    stream = g.STREAMS["v1_fcst"]
    collection = "aqc_tavg_1hr_g1440x721_v1"
    init = pd.Timestamp("2024-06-01T12")
    for step in g.Collection(stream, collection).steps[:3]:
        valid = init + step
        name = f"GEOS-CF.v01.fcst.{collection}.{init:%Y%m%d_%H}z+{valid:%Y%m%d_%H%M}z.nc4"
        write_file(root / "v1/forecast/Y2024/M06/D01/H12" / name, valid, levels=1, seed=1)
    # The local files are not downloaded: file:// references are read in place.
    assert g.run_stream(stream, local_config, p, open_workers=0, now=LATER) == 120
    ds, _ = read(g.Collection(stream, collection), local_config, p.container)
    assert ds.sizes["step"] == 120 and pd.Timedelta(ds["step"].values[0]) == pd.Timedelta("30min")


def test_a_downloaded_copy_is_referenced_at_its_portal_url(tmp_path, monkeypatch):
    """v1 metadata is read from a local copy; the references must still name the portal."""
    import shutil

    from virtualizarr.manifests import ManifestArray

    source = write_file(tmp_path / "src" / "f.nc4", pd.Timestamp("2025-06-01"), levels=1, seed=1)

    def fake_download(url, dest, retries=3, **kwargs):
        """Fake download."""
        pathlib.Path(dest).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(source, dest)
        return pathlib.Path(dest)

    monkeypatch.setattr("planetary_datasets.common.download_one", fake_download)
    opener = g.Opener(g.STREAMS["v1_ana"], g.Portal(), scratch=tmp_path)
    url = "https://portal.nccs.nasa.gov/datashare/gmao/geos-cf/v1/ana/Y2025/M06/D01/x.nc4"
    ds = opener.open(url)
    data = ds["NO2"].data
    assert isinstance(data, ManifestArray)
    assert set(np.unique(data.manifest._paths)) == {url}
    # Coordinates were read before the copy went away.
    np.testing.assert_array_equal(ds["lat"].values, LAT)
    assert not list((tmp_path).glob("geos-cf-*"))


def test_a_layout_change_continues_in_a_dated_generation(portal, local_config):
    """A day whose variables differ goes to a store named for it; both ledgers count."""
    root, p = portal
    stream = g.STREAMS["v2_ana"]
    collection = "ems_tavg_1hr_glo_L1440x721_slv"
    ana_file(
        root,
        "v2/ana",
        "GEOS.cf.ana",
        collection,
        pd.Timestamp("2026-11-01T00:30"),
        levels=1,
        seed=1,
    )
    ana_file(
        root,
        "v2/ana",
        "GEOS.cf.ana",
        collection,
        pd.Timestamp("2026-11-02T00:30"),
        levels=1,
        seed=2,
        extra=True,
    )
    assert g.run_stream(stream, local_config, p, open_workers=0, now=LATER) == 2
    c = g.Collection(stream, collection)
    _, repo = read(c, local_config, p.container)
    groups = g.generations(repo, c)
    assert groups == ["ems_tavg_1hr_slv", "ems_tavg_1hr_slv_02112026"]
    assert g.written(repo, c) == {"2026-11-01", "2026-11-02"}
    ds, _ = read(c, local_config, p.container, group=groups[1])
    assert "carbon_monoxide_anthropogenic_emission_flux" in ds and ds.sizes["time"] == 1
    # A sibling group, not a new store: the stream keeps its one store.
    assert [s.name for s in pathlib.Path(local_config.store_path("bkr/geos")).iterdir()] == [
        "geos_cf_v2_ana.icechunk"
    ]
    assert g.run_stream(stream, local_config, p, open_workers=0, now=LATER) == 0


def test_later_days_wait_behind_a_day_still_being_published(portal, local_config):
    """Appending a complete later day first would put the store's times out of order."""
    root, p = portal
    stream = g.STREAMS["v2_ana"]
    collection = "met_tavg_1hr_glo_L1440x721_slv"
    ana_file(
        root,
        "v2/ana",
        "GEOS.cf.ana",
        collection,
        pd.Timestamp("2026-11-06T00:30"),
        levels=1,
        seed=1,
    )
    for hour in range(24):
        when = pd.Timestamp("2026-11-07T00:30") + pd.Timedelta(hours=hour)
        ana_file(root, "v2/ana", "GEOS.cf.ana", collection, when, levels=1, seed=hour)
    recent = dt.datetime(2026, 11, 8, 6)
    assert g.run_stream(stream, local_config, p, open_workers=0, now=recent) == 0
    # Once the first day settles, both go in, in order.
    assert g.run_stream(stream, local_config, p, open_workers=0, now=LATER) == 25
    ds, _ = read(g.Collection(stream, collection), local_config, p.container)
    assert (np.diff(ds["time"].values) > np.timedelta64(0)).all()


def test_a_file_that_fails_once_is_retried_and_one_that_keeps_failing_is_recorded(
    portal, local_config, monkeypatch
):
    """A settled unit is written once: a transient failure must not become a silent gap."""
    root, p = portal
    monkeypatch.setattr(g, "OPEN_RETRY_PAUSE", 0.0)
    stream = g.STREAMS["v2_ana"]
    collection = "aqc_tavg_1hr_glo_L1440x721_slv"
    paths = [
        ana_file(
            root,
            "v2_hindcast/ana",
            "CF2_hindcast",
            collection,
            pd.Timestamp("2024-06-01T00:30") + pd.Timedelta(hours=h),
            levels=1,
            seed=h,
        )
        for h in range(3)
    ]
    flaky, broken = f"file://{paths[1]}", f"file://{paths[2]}"
    calls: dict[str, int] = {}
    real = g._open_url

    def open_url(url, opener=None):
        calls[url] = calls.get(url, 0) + 1
        if url == broken or (url == flaky and calls[url] == 1):
            return url, None, "OSError: portal hiccup"
        return real(url, opener)

    monkeypatch.setattr(g, "_open_url", open_url)
    assert g.run_stream(stream, local_config, p, open_workers=0, now=LATER) == 2
    assert calls[flaky] == 2 and calls[broken] == 1 + g.OPEN_RETRIES
    ds, _ = read(g.Collection(stream, collection), local_config, p.container)
    assert ds.attrs[g.LEFT_OUT_ATTR] == {"2024-06-01": [broken]}


def test_a_settled_day_none_of_whose_files_open_holds_back_later_days(
    portal, local_config, monkeypatch
):
    """An outage over a whole day must not let the next day be appended ahead of it."""
    root, p = portal
    monkeypatch.setattr(g, "OPEN_RETRY_PAUSE", 0.0)
    stream = g.STREAMS["v2_ana"]
    collection = "aqc_tavg_1hr_glo_L1440x721_slv"
    first = ana_file(
        root,
        "v2_hindcast/ana",
        "CF2_hindcast",
        collection,
        pd.Timestamp("2024-06-01T00:30"),
        levels=1,
        seed=1,
    )
    ana_file(
        root,
        "v2_hindcast/ana",
        "CF2_hindcast",
        collection,
        pd.Timestamp("2024-06-02T00:30"),
        levels=1,
        seed=2,
    )
    real = g._open_url
    down = {f"file://{first}"}
    monkeypatch.setattr(
        g,
        "_open_url",
        lambda url, opener=None: (url, None, "OSError: down") if url in down else real(url, opener),
    )
    assert g.run_stream(stream, local_config, p, open_workers=0, now=LATER) == 0
    down.clear()
    assert g.run_stream(stream, local_config, p, open_workers=0, now=LATER) == 2
    ds, _ = read(g.Collection(stream, collection), local_config, p.container)
    assert (np.diff(ds["time"].values) > np.timedelta64(0)).all()


# ----------------------------------------------------------------------------- one store


def test_a_streams_collections_are_groups_of_one_store(portal, local_config):
    """Every collection of a stream is a group of the stream's one store, attrs per group."""
    root, p = portal
    stream = g.STREAMS["v2_ana"]
    when = pd.Timestamp("2026-11-01T00:30")
    met, chm = "met_tavg_1hr_glo_L1440x721_slv", "chm_inst_1hr_glo_L1440x721_p23"
    ana_file(root, "v2/ana", "GEOS.cf.ana", met, when, levels=1, seed=1)
    ana_file(root, "v2/ana", "GEOS.cf.ana", chm, when, levels=2, seed=2)
    assert g.run_stream(stream, local_config, p, open_workers=0, now=LATER) == 2
    assert [s.name for s in pathlib.Path(local_config.store_path("bkr/geos")).iterdir()] == [
        "geos_cf_v2_ana.icechunk"
    ]
    repo = g.open_store(stream, local_config, create=False, container=p.container)
    top = zarr.open_group(repo.readonly_session("main").store, mode="r")
    assert sorted(top.group_keys()) == ["chm_inst_1hr_p23", "met_tavg_1hr_slv"]
    # Nothing that tracks a collection is kept at the root.
    assert g.LEDGER_ATTR not in top.attrs and top.attrs["geos_cf_stream"] == "v2_ana"
    met_ds, _ = read(g.Collection(stream, met), local_config, p.container)
    chm_ds, _ = read(g.Collection(stream, chm), local_config, p.container)
    # The same variable name in two collections, each in its own group.
    assert met_ds["nitrogen_dioxide_mole_fraction"].dims == ("time", "latitude", "longitude")
    assert chm_ds["nitrogen_dioxide_mole_fraction"].dims[:2] == ("time", "pressure_level")
    assert met_ds.attrs["collection"] == met
    assert met_ds.attrs[g.LEDGER_ATTR] == chm_ds.attrs[g.LEDGER_ATTR] == ["2026-11-01"]


def test_analysis_groups_split_their_manifests_a_day_at_a_time(portal, local_config):
    """Each group's manifest splits by its own interval; forecasts one run a split."""
    _, p = portal
    ana = g.open_store(g.STREAMS["v2_ana"], local_config, container=p.container)
    sizes = [(str(c), [n for _, n in s]) for c, s in ana.config.manifest.splitting.split_sizes]
    assert [n for _, n in sizes] == [[96], [24], [8], [24]]
    assert "_15mn_" in sizes[0][0] and "any_array" in sizes[-1][0]
    fcst = g.open_store(g.STREAMS["v2_fcst"], local_config, container=p.container)
    ((_, ((dim, n),)),) = fcst.config.manifest.splitting.split_sizes
    assert "init_time" in str(dim) and n == 1


def test_the_hindcast_runs_into_the_live_analysis_without_repeating_a_time(portal, local_config):
    """Hindcast to the day before a collection's first live day, then live only."""
    root, p = portal
    stream = g.STREAMS["v2_ana"]
    met = "met_tavg_1hr_glo_L1440x721_slv"
    aqc = "aqc_tavg_1hr_glo_L1440x721_slv"
    ghg = "ghg_inst_3hr_glo_L1440x721_v72"

    def hindcast(collection, day, seed):
        when = pd.Timestamp(day) + pd.Timedelta(minutes=30)
        ana_file(root, "v2_hindcast/ana", "CF2_hindcast", collection, when, levels=1, seed=seed)

    def live(collection, day, seed, levels=1, minutes=30):
        when = pd.Timestamp(day) + pd.Timedelta(minutes=minutes)
        ana_file(root, "v2/ana", "GEOS.cf.ana", collection, when, levels=levels, seed=seed)

    # met: hindcast 08-02..08-04, live from 08-03 but missing 08-04: a hole the hindcast must
    # not fill, as that would go back to the hindcast after the live analysis began.
    for i, day in enumerate(("2025-08-02", "2025-08-03", "2025-08-04")):
        hindcast(met, day, 100 + i)
    live(met, "2025-08-03", 1)
    live(met, "2025-08-05", 3)
    # aqc: the live analysis has it only from 08-05; the hindcast covers up to then.
    for i, day in enumerate(("2025-08-02", "2025-08-03", "2025-08-04")):
        hindcast(aqc, day, 200 + i)
    live(aqc, "2025-08-05", 5)
    # ghg is in the live analysis only.
    live(ghg, "2025-08-05", 7, levels=2, minutes=0)

    assert g.run_stream(stream, local_config, p, open_workers=0, now=LATER) == 8

    def series(collection):
        ds, _ = read(g.Collection(stream, collection), local_config, p.container)
        no2 = ds["nitrogen_dioxide_mole_fraction"]
        means = no2.mean([d for d in no2.dims if d != "time"]).values
        return ds, [str(t)[:10] for t in ds["time"].values], [float(m) for m in means]

    ds, days, values = series(met)
    assert days == ["2025-08-02", "2025-08-03", "2025-08-05"]
    assert values == [100, 1, 3]
    assert ds.attrs[g.LIVE_FROM_ATTR] == "2025-08-03"
    assert (np.diff(ds["time"].values) > np.timedelta64(0, "ns")).all()
    ds, days, values = series(aqc)
    assert days == ["2025-08-02", "2025-08-03", "2025-08-04", "2025-08-05"]
    assert values == [200, 201, 202, 5]
    assert ds.attrs[g.LIVE_FROM_ATTR] == "2025-08-05"
    ds, days, _ = series(ghg)
    assert days == ["2025-08-05"] and ds.attrs[g.LIVE_FROM_ATTR] == "2025-08-05"


def test_a_resumed_pass_remembers_where_the_live_analysis_began(portal, local_config):
    """Per-group ledgers resume, and the live start is read back from the group."""
    root, p = portal
    stream = g.STREAMS["v2_ana"]
    met = "met_tavg_1hr_glo_L1440x721_slv"
    for i, day in enumerate(("2025-08-02", "2025-08-03", "2025-08-04")):
        when = pd.Timestamp(day) + pd.Timedelta(minutes=30)
        ana_file(root, "v2_hindcast/ana", "CF2_hindcast", met, when, levels=1, seed=100 + i)
    for day in ("2025-08-03", "2025-08-05"):
        when = pd.Timestamp(day) + pd.Timedelta(minutes=30)
        ana_file(root, "v2/ana", "GEOS.cf.ana", met, when, levels=1, seed=1)
    end = dt.date(2025, 8, 3)
    assert g.run_stream(stream, local_config, p, end=end, open_workers=0, now=LATER) == 2
    # The next pass skips 08-02 and 08-03 without listing them, so only the group's
    # attribute says the live analysis began on 08-03: the hindcast's 08-04 stays out.
    assert g.run_stream(stream, local_config, p, open_workers=0, now=LATER) == 1
    c = g.Collection(stream, met)
    ds, repo = read(c, local_config, p.container)
    days = ["2025-08-02", "2025-08-03", "2025-08-05"]
    assert [str(t)[:10] for t in ds["time"].values] == days
    assert g.written(repo, c) == set(days)
    assert g.run_stream(stream, local_config, p, open_workers=0, now=LATER) == 0


def test_a_layout_change_forks_only_its_own_group(portal, local_config):
    """One collection's new layout makes it a dated group; the others carry on as they were."""
    root, p = portal
    stream = g.STREAMS["v2_ana"]
    ems, met = "ems_tavg_1hr_glo_L1440x721_slv", "met_tavg_1hr_glo_L1440x721_slv"
    for day, extra in (("2026-11-01", False), ("2026-11-02", True)):
        when = pd.Timestamp(day) + pd.Timedelta(minutes=30)
        ana_file(root, "v2/ana", "GEOS.cf.ana", ems, when, levels=1, seed=1, extra=extra)
        ana_file(root, "v2/ana", "GEOS.cf.ana", met, when, levels=1, seed=2)
    assert g.run_stream(stream, local_config, p, open_workers=0, now=LATER) == 4
    repo = g.open_store(stream, local_config, create=False, container=p.container)
    top = zarr.open_group(repo.readonly_session("main").store, mode="r")
    assert sorted(top.group_keys()) == [
        "ems_tavg_1hr_slv",
        "ems_tavg_1hr_slv_02112026",
        "met_tavg_1hr_slv",
    ]
    ds, _ = read(g.Collection(stream, met), local_config, p.container)
    assert ds.sizes["time"] == 2


def test_collections_written_at_once_do_not_lose_a_commit(portal, local_config, monkeypatch):
    """Two collections' sessions open on one snapshot; both commits land, by rebasing."""
    import threading

    root, p = portal
    stream = g.STREAMS["v2_ana"]
    names = ["met_tavg_1hr_glo_L1440x721_slv", "aqc_tavg_1hr_glo_L1440x721_slv"]
    for day in ("2026-11-01", "2026-11-02"):
        for seed, name in enumerate(names):
            when = pd.Timestamp(day) + pd.Timedelta(minutes=30)
            ana_file(root, "v2/ana", "GEOS.cf.ana", name, when, levels=1, seed=seed)
    barrier = threading.Barrier(2)
    real_open = g.open_store

    class Together:
        """A repository whose writable sessions open in pairs, on the same snapshot."""

        def __init__(self, repo):
            self.repo = repo

        def __getattr__(self, name):
            return getattr(self.repo, name)

        def writable_session(self, branch):
            session = self.repo.writable_session(branch)
            barrier.wait(timeout=60)
            return session

    monkeypatch.setattr(g, "open_store", lambda *a, **k: Together(real_open(*a, **k)))
    assert g.run_stream(stream, local_config, p, workers=2, open_workers=0, now=LATER) == 4
    monkeypatch.setattr(g, "open_store", real_open)
    for name in names:
        ds, _ = read(g.Collection(stream, name), local_config, p.container)
        assert ds.sizes["time"] == 2 and ds.attrs[g.LEDGER_ATTR] == ["2026-11-01", "2026-11-02"]


def test_the_hindcast_is_no_longer_a_stream_of_its_own():
    """``--streams v2_hindcast`` is refused: the hindcast is the start of ``v2_ana``."""
    with pytest.raises(SystemExit):
        g.main(["--streams", "v2_hindcast"])


def test_a_day_whose_files_are_chunked_differently_is_split_by_layout(portal, local_config):
    """Files re-chunked part way through a day go to their own group; later days follow them."""
    root, p = portal
    stream = g.STREAMS["v1_ana"]
    collection = "chm_tavg_1hr_v1"

    def hour(day, h, **kwargs):
        when = pd.Timestamp(f"2019-03-{day:02d}T{h:02d}:30")
        ana_file(root, "v1/ana", "GEOS-CF.v01.rpl", collection, when, levels=1, seed=h, **kwargs)

    for h in range(4):
        hour(7, h)
    for h in range(4):
        hour(8, h, lon_chunk=LON.size if h < 2 else LON.size // 2)
    for h in range(4):
        hour(9, h, lon_chunk=LON.size // 2)
    stream = g.dataclasses.replace(stream, download=False)
    assert g.run_stream(stream, local_config, p, open_workers=0, now=LATER) == 12
    c = g.Collection(stream, collection)
    _, repo = read(c, local_config, p.container)
    groups = g.generations(repo, c)
    assert groups == ["chm_tavg_1hr_v1", "chm_tavg_1hr_v1_08032019"]
    base, later = (read(c, local_config, p.container, group=name)[0] for name in groups)
    # March 7 and the first two hours of March 8 keep the full-width chunks; the rest of
    # March 8 starts the group of the narrower ones, and March 9 continues it.
    assert base["time"].dt.strftime("%d %H").values.tolist() == [
        f"{d:02d} {h:02d}" for d, h in [(7, 0), (7, 1), (7, 2), (7, 3), (8, 0), (8, 1)]
    ]
    assert later["time"].dt.strftime("%d %H").values.tolist() == [
        f"{d:02d} {h:02d}" for d, h in [(8, 2), (8, 3), (9, 0), (9, 1), (9, 2), (9, 3)]
    ]
    assert g.written(repo, c) == {"2019-03-07", "2019-03-08", "2019-03-09"}
    # Referenced as before, each hour still reads its own values.
    nitrogen = "nitrogen_dioxide_mole_fraction"
    assert base[nitrogen].isel(latitude=0, longitude=0).values.tolist() == [0, 1, 2, 3, 0, 1]
    assert g.run_stream(stream, local_config, p, open_workers=0, now=LATER) == 0


def test_a_split_day_found_after_later_days_joins_the_group_of_its_layout(portal, local_config):
    """The lane's case: later days already sit in a generation when the mixed day is written."""
    root, p = portal
    stream = g.dataclasses.replace(g.STREAMS["v1_ana"], download=False)
    collection = "chm_tavg_1hr_v1"

    def hour(day, h, narrow):
        when = pd.Timestamp(f"2019-03-{day:02d}T{h:02d}:30")
        lon_chunk = LON.size // 2 if narrow else LON.size
        ana_file(
            root,
            "v1/ana",
            "GEOS-CF.v01.rpl",
            collection,
            when,
            levels=1,
            seed=h,
            lon_chunk=lon_chunk,
        )

    for h in range(2):
        hour(7, h, narrow=False)
        hour(9, h, narrow=True)
    assert g.run_stream(stream, local_config, p, open_workers=0, now=LATER) == 4
    for h in range(2):
        hour(8, h, narrow=h == 1)
    assert g.run_stream(stream, local_config, p, open_workers=0, now=LATER) == 2
    c = g.Collection(stream, collection)
    _, repo = read(c, local_config, p.container)
    base, later = (read(c, local_config, p.container, group=n)[0] for n in g.generations(repo, c))
    assert base.sizes["time"] == 3 and later.sizes["time"] == 3
    assert g.written(repo, c) == {"2019-03-07", "2019-03-08", "2019-03-09"}
    assert g.run_stream(stream, local_config, p, open_workers=0, now=LATER) == 0
