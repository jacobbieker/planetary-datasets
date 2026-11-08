"""Virtual GEOS-CF stores, against a local tree laid out like the NCCS portal."""

import datetime as dt
import json
import pathlib

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from planetary_datasets.providers.virtualized import geos_cf as g
from planetary_datasets.providers.virtualized.geos_cf_names import variable_name, variable_names

LAT = np.linspace(-90, 90, 5)
LON = np.linspace(-180, 135, 8)
#: Far enough after every test day that all of them have settled.
LATER = dt.datetime(2030, 1, 1)


def write_file(
    path: pathlib.Path, when: pd.Timestamp, levels: int, seed: float, extra: bool = False
):
    """A small GEOS-CF-like file: one time, ``levels`` levels, one chunk per level field."""
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
            "chunksizes": (1, 1, LAT.size, LON.size)
            if ds[name].ndim == 4
            else (1, LAT.size, LON.size),
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


def read(collection, config, container):
    """Open a written store for reading, with its repository."""
    repo = g.open_store(
        collection.store_prefix, collection, config, create=False, container=container
    )
    return xr.open_zarr(repo.readonly_session("main").store, consolidated=False), repo


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
    assert three_hourly.store_prefix == "bkr/geos/geos_v2_fcst_ghg_inst_3hr_v72.icechunk"
    sat = g.Collection(g.STREAMS["v1_ana"], "sat_inst_1hr_r721x361_v72")
    assert sat.store_prefix == "bkr/geos/geos_v1_ana_sat_inst_1hr_v72_0p5deg.icechunk"


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
    stream = g.STREAMS["v2_hindcast"]
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
    assert not pathlib.Path(
        local_config.store_path(
            g.Collection(g.STREAMS["v1_ana"], "xgc_tavg_1hr_g1440x721_v36").store_prefix
        )
    ).exists()


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
    prefixes = g.generations(local_config, c.store_prefix)
    assert prefixes == [c.store_prefix, c.store_prefix.replace(".icechunk", "_02112026.icechunk")]
    assert g.written(c, local_config, p.container) == {"2026-11-01", "2026-11-02"}
    later = g.open_store(prefixes[1], c, local_config, create=False, container=p.container)
    ds = xr.open_zarr(later.readonly_session("main").store, consolidated=False)
    assert "carbon_monoxide_anthropogenic_emission_flux" in ds and ds.sizes["time"] == 1
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
