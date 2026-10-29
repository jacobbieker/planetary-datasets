"""Renaming existing stores (metadata only) and preparing later writes to match."""

from __future__ import annotations

import importlib.util
import json
import pathlib

import icechunk
import numpy as np
import pandas as pd
import pytest
import xarray as xr
from icechunk.xarray import to_icechunk

from planetary_datasets.common import naming

pytestmark = pytest.mark.store_names

ROOT = pathlib.Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location(
    "rename_tool", ROOT / "scripts" / "rename_store_variables.py"
)
tool = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tool)

PREFIX = "bkr/test/store.icechunk"


@pytest.fixture
def tables(tmp_path, monkeypatch):
    names = {
        PREFIX: {"tc": "air_temperature", "a": "b", "b": "a", "grp/u": "grp/u_component_of_wind"}
    }
    dims = {PREFIX: {"t": "time", "lat": "latitude"}}
    units = {
        PREFIX: {
            "air_temperature": {
                "scale_factor": 1.0,
                "add_offset": 273.15,
                "units": "K",
                "from_units": "Deg C",
            },
            "pressure": {
                "scale_factor": 100.0,
                "add_offset": 0.0,
                "units": "Pa",
                "from_units": "hPa",
            },
        }
    }
    for path, table in (
        ("store_names.json", names),
        ("store_dims.json", dims),
        ("store_units.json", units),
    ):
        (tmp_path / path).write_text(json.dumps(table))
    monkeypatch.setattr(naming, "STORE_NAMES", tmp_path / "store_names.json")
    monkeypatch.setattr(naming, "STORE_DIMS", tmp_path / "store_dims.json")
    monkeypatch.setattr(naming, "STORE_UNITS", tmp_path / "store_units.json")
    monkeypatch.setattr(tool, "STORE_NAMES", tmp_path / "store_names.json")
    monkeypatch.setattr(tool, "STORE_DIMS", tmp_path / "store_dims.json")
    monkeypatch.setattr(tool, "STORE_UNITS", tmp_path / "store_units.json")
    naming._table.cache_clear()
    yield
    naming._table.cache_clear()


@pytest.fixture
def repo(tmp_path, monkeypatch):
    repo = icechunk.Repository.create(icechunk.local_filesystem_storage(str(tmp_path / "store")))

    class Config:
        def icechunk_repo(self, prefix):
            return repo

    monkeypatch.setattr(tool, "get_config", lambda: Config())
    return repo


def write_source(repo):
    ds = xr.Dataset(
        {
            "tc": (
                ("t", "lat"),
                np.array([[25.5, 30.0]], "float32"),
                {"units": "Deg C", "coordinates": "lat"},
            ),
            "a": (("t",), np.array([1], "int16")),
            "b": (("t",), np.array([2], "int16")),
            "pressure": (("t",), np.array([10130], "int32"), {"scale_factor": 0.1, "units": "hPa"}),
        },
        coords={"t": [0], "lat": [10.0, 20.0]},
    )
    session = repo.writable_session("main")
    to_icechunk(ds, session)
    to_icechunk(xr.Dataset({"u": (("t",), np.array([1.5], "float32"))}), session, group="grp")
    session.commit("source")


def test_a_store_is_renamed_without_touching_its_data(tables, repo):
    write_source(repo)
    print(tool.rename_store(PREFIX))
    ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    assert set(ds.dims) == {"time", "latitude"}
    assert ds["air_temperature"].values[0].tolist() == pytest.approx([298.65, 303.15])
    assert ds["air_temperature"].attrs["units"] == "K"
    assert ds["air_temperature"].attrs["source_name"] == "tc"
    # A swap of names lands each array on the other's name.
    assert ds["a"].values.tolist() == [2] and ds["b"].values.tolist() == [1]
    # An existing scale factor composes with the conversion: 10130 * 0.1 hPa = 101300 Pa.
    assert ds["pressure"].values.tolist() == pytest.approx([101300.0])
    raw = xr.open_zarr(
        repo.readonly_session("main").store, consolidated=False, mask_and_scale=False
    )
    assert raw["air_temperature"].values[0].tolist() == pytest.approx([25.5, 30.0])
    grp = xr.open_zarr(repo.readonly_session("main").store, group="grp", consolidated=False)
    assert "u_component_of_wind" in grp
    assert tool.check_store(PREFIX) is None
    assert tool.rename_store(PREFIX).endswith("already renamed")


def test_a_format_1_store_is_skipped_untouched_or_upgraded_and_renamed(
    tables, tmp_path, monkeypatch
):
    storage = icechunk.local_filesystem_storage(str(tmp_path / "v1"))
    repo = icechunk.Repository.create(storage, spec_version=1)

    class Config:
        def icechunk_repo(self, prefix):
            return icechunk.Repository.open(storage)

    monkeypatch.setattr(tool, "get_config", lambda: Config())
    write_source(repo)
    assert "SKIPPED" in tool.rename_store(PREFIX)
    ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    assert "tc" in ds and "units" not in ds.attrs and ds["tc"].attrs["units"] == "Deg C"
    print(tool.rename_store(PREFIX, upgrade_v1=True))
    upgraded = icechunk.Repository.open(storage)
    assert upgraded.spec_version == icechunk.SpecVersion.v2
    ds = xr.open_zarr(upgraded.readonly_session("main").store, consolidated=False)
    assert ds["air_temperature"].values[0].tolist() == pytest.approx([298.65, 303.15])


def test_later_xarray_appends_are_converted_and_round_trip_exactly(tables, repo):
    write_source(repo)
    tool.rename_store(PREFIX)
    incoming = xr.Dataset(
        {
            "tc": (("t", "lat"), np.array([[-3.25, 12.0]], "float32")),
            "a": (("t",), np.array([3], "int16")),
            "b": (("t",), np.array([4], "int16")),
            "pressure": (("t",), np.array([1000.0])),
        },
        coords={"t": [1], "lat": [10.0, 20.0]},
    )
    prepared = naming.prepare_for_store(incoming, PREFIX, naming.stored_arrays(repo))
    assert "air_temperature" in prepared and "time" in prepared.dims
    session = repo.writable_session("main")
    to_icechunk(prepared.drop_vars("latitude"), session, append_dim="time")
    session.commit("append")
    raw = xr.open_zarr(
        repo.readonly_session("main").store, consolidated=False, mask_and_scale=False
    )
    assert raw["air_temperature"].values[1].tolist() == [-3.25, 12.0]


def test_a_new_variable_carries_its_conversion_as_encoding(tables, tmp_path):
    incoming = xr.Dataset({"tc": (("t",), np.array([20.0, 21.5], "float32"))}, coords={"t": [0, 1]})
    prepared = naming.prepare_for_store(incoming, PREFIX, set())
    assert prepared["air_temperature"].encoding["add_offset"] == 273.15
    repo = icechunk.Repository.create(icechunk.local_filesystem_storage(str(tmp_path / "new")))
    session = repo.writable_session("main")
    to_icechunk(prepared, session)
    session.commit("new")
    ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    assert ds["air_temperature"].values.tolist() == pytest.approx([293.15, 294.65])
    raw = xr.open_zarr(
        repo.readonly_session("main").store, consolidated=False, mask_and_scale=False
    )
    assert raw["air_temperature"].values.tolist() == [20.0, 21.5]


def test_raw_writers_get_names_and_attributes_only(tables):
    incoming = xr.Dataset({"tc": (("t",), np.array([20.0], "float32"))})
    prepared = naming.prepare_for_store(incoming, PREFIX, set(), raw=True)
    assert prepared["air_temperature"].values.tolist() == [20.0]
    assert prepared["air_temperature"].attrs["add_offset"] == 273.15


def test_float16_is_never_converted(tables):
    incoming = xr.Dataset({"tc": (("t",), np.array([20.0], "float16"))})
    prepared = naming.prepare_for_store(incoming, PREFIX, set())
    assert prepared["air_temperature"].dtype == np.float16
    assert "add_offset" not in prepared["air_temperature"].encoding


def test_a_renamed_generation_still_matches_its_source_named_partitions(
    tables, local_config, monkeypatch
):
    from planetary_datasets.common.generations import resolve_generation

    base = "bkr/test/gen.icechunk"
    names = json.loads(naming.STORE_NAMES.read_text())
    names[base] = {"tc": "air_temperature"}
    naming.STORE_NAMES.write_text(json.dumps(names))
    naming._table.cache_clear()
    stored = xr.Dataset(
        {"air_temperature": (("time", "x"), np.ones((1, 3), "float32"))},
        coords={"time": pd.to_datetime(["2026-01-01"]), "x": [0, 1, 2]},
    )
    repo = local_config.icechunk_repo(base)
    session = repo.writable_session("main")
    to_icechunk(stored, session)
    session.commit("renamed store")
    incoming = stored.rename_vars({"air_temperature": "tc"}).assign_coords(
        time=pd.to_datetime(["2026-01-02"])
    )
    assert resolve_generation(local_config, base, incoming, "time") == base
