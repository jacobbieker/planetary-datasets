"""Offline tests for the ICON v1 provider.

No network: the schema, the source listings and the GRIB reads are all faked, and every
store is local.
"""

# ruff: noqa: D102, D103, D107 - test names say what they check, as elsewhere in tests/

from __future__ import annotations

import contextlib
import re
import types
import zlib

import dask
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from planetary_datasets.common.lazy_fields import bounded_compute as real_bounded_compute
from planetary_datasets.memory import MemoryLimitExceeded
from planetary_datasets.providers import icon_v1
from planetary_datasets.providers import icon_v1_catalog as cat
from planetary_datasets.providers.icon_v1 import ICONV1Provider, IncompleteRun, plan_variables

N_CELLS = 6
RUN_00 = pd.Timestamp("2026-10-11T00:00")
RUN_06 = pd.Timestamp("2026-10-11T06:00")
LATITUDE = np.linspace(-60, 60, N_CELLS).astype(np.float32)
LONGITUDE = np.linspace(-170, 170, N_CELLS).astype(np.float32)

#: The older, coarser grid ICON-ART and ICON-ART-EU used before 2026-10-06, in miniature.
OLD_CELLS = 4
OLD_RUN = pd.Timestamp("2026-01-07T00:00")
OLD_LATITUDE = np.linspace(-40, 40, OLD_CELLS).astype(np.float32)
OLD_LONGITUDE = np.linspace(-100, 100, OLD_CELLS).astype(np.float32)


def is_current(model: str, era: cat.ICONEra) -> bool:
    return era == cat.current_era(model)


def schema_data(model: str, era: cat.ICONEra | None = None) -> dict:
    """A miniature schema with one variable of every kind the provider handles.

    00/12Z runs go to step 3, 06/18Z runs to step 2. ``T_2M`` is stored as float16, the
    rest as float32 (no ``dtype`` key). ``CEIL_BSC_DUST`` is wavelength
    resolved on a subset (2-3) of the full model levels (1-3); ``W`` and ``HHL`` are on
    the half levels (1-4); ``HHL`` and ``HSURF`` are published at step 0 only. An older
    era has the same fields on a grid of :data:`OLD_CELLS` cells.
    """
    era = era or cat.current_era(model)
    long, short = "s0", "s1"
    every = {"00": long, "06": short, "12": long, "18": short}
    variables = [
        {
            "param": "CEIL_BSC_DUST",
            "level_type": 150,
            "levels": ["2", "3"],
            "steps": every,
            "wavelength": 532,
        },
        {
            "param": "CEIL_BSC_DUST",
            "level_type": 150,
            "levels": ["2", "3"],
            "steps": every,
            "wavelength": 1064,
        },
        {
            "param": "HHL",
            "level_type": 150,
            "levels": ["1", "2", "3", "4"],
            "steps": {h: "s2" for h in every},
        },
        {"param": "HSURF", "level_type": None, "levels": [], "steps": {h: "s2" for h in every}},
        {"param": "T", "level_type": 100, "levels": ["50000", "85000"], "steps": every},
        {"param": "T", "level_type": 150, "levels": ["1", "2", "3"], "steps": every},
        {"param": "TOT_PREC", "level_type": None, "levels": [], "steps": every},
        {"param": "T_2M", "level_type": None, "levels": [], "steps": every, "dtype": "float16"},
        {"param": "W", "level_type": 150, "levels": ["1", "2", "3", "4"], "steps": every},
        {"param": "W_SO", "level_type": 106, "levels": ["0.0", "0.01"], "steps": every},
    ]
    return {
        "format_version": 1,
        "model": model,
        "n_cells": N_CELLS if is_current(model, era) else OLD_CELLS,
        "grid_number": era.grid_number,
        "source": "live",
        "reference_runs": [],
        "generated_at": "2026-10-13T00:00:00Z",
        "coordinate_params": ["CLAT", "CLON"],
        "invariant_params": ["HHL", "HSURF"],
        "step_sets": {"s0": [0, 1, 2, 3], "s1": [0, 1, 2], "s2": [0]},
        "variables": variables,
        "excluded": [],
    }


_URI = re.compile(r"/bkr/icon/(?P<model>[^/]+)/|/v1/m/(?P<live>[^/]+)/")
_RUN = re.compile(r"/r/(\d{4}-\d{2}-\d{2}T\d{2})(?::|%3A)(\d{2})/")


def era_of(uri: str) -> tuple[str, cat.ICONEra]:
    """The model and grid era a field URI belongs to."""
    match = _URI.search(uri)
    model = match.group("model") or match.group("live")
    hour, minute = _RUN.search(uri).groups()
    return model, cat.era_for(model, pd.Timestamp(f"{hour}:{minute}"))


def field_value(uri: str) -> np.ndarray:
    """The deterministic field the fake source holds at ``uri``, on its era's grid."""
    model, era = era_of(uri)
    cells = N_CELLS if is_current(model, era) else OLD_CELLS
    base = zlib.crc32(uri.encode()) % 1000
    return (base + np.arange(cells) / 10).astype(np.float32)


class FakeSource:
    """Stands in for the archive: listings and reads come from the miniature schemas.

    ``read_bytes`` returns the URI itself, which decoding turns back into values.

    Attributes:
        missing: URIs absent from the source: not listed, and never read.
        unreadable: URIs that are listed but read as permanently absent.
        reads: Every URI read, in order.
        listings: Every directory listed.
        latitude: The current era's CLAT field, so a test can move the grid.
    """

    def __init__(self, schemas: dict) -> None:
        self.schemas = schemas
        self.missing: set[str] = set()
        self.unreadable: set[str] = set()
        self.reads: list[str] = []
        self.listings: list[str] = []
        self.latitude = LATITUDE.copy()
        self.num_workers_seen: set = set()

    def load_era_schema(self, model: str, era: cat.ICONEra) -> cat.ICONSchema:
        return self.schemas[(model, era.grid_number)]

    def read_bytes(self, uri: str, **kwargs) -> bytes | None:
        self.reads.append(uri)
        self.num_workers_seen.add(dask.config.get("num_workers", None))
        if uri in self.missing or uri in self.unreadable:
            return None
        return uri.encode()

    def decode(self, data: bytes, dtype=np.float32, out=None) -> np.ndarray:
        uri = data.decode()
        model, era = era_of(uri)
        current = is_current(model, era)
        if "/p/CLAT/" in uri:
            values = self.latitude if current else OLD_LATITUDE
        elif "/p/CLON/" in uri:
            values = LONGITUDE if current else OLD_LONGITUDE
        else:
            values = field_value(uri)
        if out is None:
            return values.astype(dtype)
        out[...] = values
        return out

    def info(self, data: bytes) -> types.SimpleNamespace:
        model, era = era_of(data.decode())
        return types.SimpleNamespace(
            number_of_values=self.schemas[(model, era.grid_number)].n_cells,
            grid_number=era.grid_number,
        )

    def lister(self, model: str, src: str):
        """A lister over every field of the miniature schemas not in :attr:`missing`."""

        def ls(directory: str) -> list[str]:
            self.listings.append(directory)
            hour, minute = _RUN.search(f"/{directory}/").groups()
            run = pd.Timestamp(f"{hour}:{minute}")
            schema = self.schemas[(model, cat.era_for(model, run).grid_number)]
            names = []
            for key in schema.expected_fields(run) + schema.coordinate_fields():
                path = cat.field_path(model, key, run)
                if path.rpartition("/")[0] == directory:
                    if cat.field_uri(model, key, run, src) not in self.missing:
                        names.append(path.rpartition("/")[2])
            if not names:
                raise FileNotFoundError(directory)
            return names

        return ls


@pytest.fixture
def source(monkeypatch, local_config):
    """Fake schemas per era, source choice, listings and GRIB reads; local stores."""
    schemas = {
        (model, era.grid_number): cat.ICONSchema(schema_data(model, era))
        for model in cat.MODELS
        for era in cat.ERAS[model]
    }
    fake = FakeSource(schemas)
    monkeypatch.setattr(icon_v1, "load_era_schema", fake.load_era_schema)
    monkeypatch.setattr(icon_v1, "choose_source", lambda model, it: "archive")
    monkeypatch.setattr(icon_v1, "read_bytes", fake.read_bytes)
    monkeypatch.setattr(icon_v1, "decode_grib_field", fake.decode)
    monkeypatch.setattr(icon_v1, "grib_field_info", fake.info)
    monkeypatch.setattr(icon_v1, "get_lister", fake.lister)
    return fake


def uri(param, level_type, level, step, it, wavelength=None, model="icon-art-eu", src="archive"):
    key = cat.FieldKey(param, level_type, level, step, wavelength)
    return cat.field_uri(model, key, it, src)


def open_store(config, prefix: str) -> xr.Dataset:
    repo = config.icechunk_repo(prefix)
    return xr.open_zarr(repo.readonly_session("main").store, consolidated=False)


# -- layout ------------------------------------------------------------------------------


def test_layout_names_every_level_kind(source):
    layout = plan_variables(cat.ICONSchema(schema_data("icon-art-eu")))
    dims = {v.name: v.dims for v in layout.variables}
    assert dims == {
        "ceil_bsc_dust_ml": ("init_time", "step", "wavelength", "model_level_2_3", "values"),
        "hhl_ml": ("init_time", "model_level_half", "values"),
        "hsurf": ("init_time", "values"),
        "t": ("init_time", "step", "isobaricInhPa", "values"),
        "t_ml": ("init_time", "step", "model_level", "values"),
        "tot_prec": ("init_time", "step", "values"),
        "t_2m": ("init_time", "step", "values"),
        "w_ml": ("init_time", "step", "model_level_half", "values"),
        "w_so": ("init_time", "step", "depth", "values"),
    }
    coords = layout.level_coords
    assert coords["isobaricInhPa"].tolist() == [500.0, 850.0]
    assert coords["model_level"].tolist() == [1, 2, 3]
    assert coords["model_level_half"].tolist() == [1, 2, 3, 4]
    assert coords["model_level_2_3"].tolist() == [2, 3]
    assert coords["depth"].tolist() == [0.0, 0.01]
    assert coords["wavelength"].tolist() == [532, 1064]


@pytest.mark.parametrize("model", sorted(cat.MODELS))
def test_committed_schemas_lay_out_without_collisions(model):
    layout = plan_variables(cat.load_schema(model))
    names = [v.name for v in layout.variables]
    assert len(names) == len(set(names))
    assert {"t_2m", "tot_prec", "hsurf"} <= set(names)
    dims = {v.name: v.dims for v in layout.variables}
    assert dims["hhl_ml"] == ("init_time", "model_level_half", "values")
    assert dims["t_ml"][2] == "model_level"
    assert layout.level_coords["model_level_half"][-1] == layout.level_coords["model_level"][-1] + 1
    if model == "icon":
        # Same names as the original icon_global.icechunk where the field is the same.
        assert dims["t"] == ("init_time", "step", "isobaricInhPa", "values")
        assert layout.level_coords["model_level"].tolist() == list(range(1, 121))
        assert len(layout.level_coords["isobaricInhPa"]) == 18
    else:
        assert dims["ceil_bsc_dust_ml"][2] == "wavelength"
        assert dims["ceil_bsc_dust_ml"][3] == dims["dust_total_mc_ml"][2]
        assert layout.level_coords["wavelength"].tolist() == [532, 1064]


def test_unknown_params_and_variants_are_refused(source):
    with pytest.raises(ValueError, match="variant"):
        ICONV1Provider("eu")
    with pytest.raises(ValueError, match="NOPE"):
        ICONV1Provider("eu_art", params=["T_2M", "NOPE"])


# -- fetch -------------------------------------------------------------------------------


def test_fetch_lists_the_expected_fields_without_reading(source):
    files = ICONV1Provider("eu_art").fetch(RUN_06)
    assert not source.reads
    schema = ICONV1Provider("eu_art").schema
    assert len(files) == len(schema.expected_fields(RUN_06)) + 2
    assert uri("CLAT", None, None, 0, RUN_06) in files
    assert uri("T_2M", None, None, 2, RUN_06) in files
    assert uri("T_2M", None, None, 3, RUN_06) not in files
    assert uri("CEIL_BSC_DUST", 150, "3", 2, RUN_06, wavelength=1064) in files

    subset = ICONV1Provider("eu_art", params=["T_2M", "CLAT", "CLON"]).fetch(RUN_06)
    assert sorted(cat_param(u) for u in subset) == ["CLAT", "CLON", "T_2M", "T_2M", "T_2M"]


def cat_param(u: str) -> str:
    return u.split("/p/")[1].split("/")[0]


def test_fetch_returns_nothing_when_no_source_has_the_run(source, monkeypatch):
    monkeypatch.setattr(icon_v1, "choose_source", lambda model, it: None)
    assert ICONV1Provider("eu_art").fetch(RUN_00) == []
    assert ICONV1Provider("eu_art").run_partition(RUN_00) is False


def young_live_run() -> pd.Timestamp:
    return pd.Timestamp.now("UTC").tz_localize(None).floor("6h")


def every_field(provider: ICONV1Provider, it: pd.Timestamp, src: str = "archive") -> set[str]:
    schema = provider.plan_for(it).schema
    return {
        cat.field_uri(provider.model, key, it, src)
        for key in schema.expected_fields(it) + schema.coordinate_fields()
    }


def test_a_young_live_run_still_arriving_is_refused(source, monkeypatch):
    it = young_live_run()
    monkeypatch.setattr(icon_v1, "choose_source", lambda model, it: "live")
    provider = ICONV1Provider("eu_art")
    source.missing |= every_field(provider, it, "live")
    with pytest.raises(IncompleteRun, match="not fully published"):
        provider.fetch(it)

    # The probed last steps are there but most of the run is not: still too young.
    probes = {cat.field_uri("icon-art-eu", k, it, "live") for k in provider._probe_fields(it)}
    source.missing -= probes
    with pytest.raises(IncompleteRun, match="still being published"):
        provider.fetch(it)

    # Once everything is listed, the run is taken.
    source.missing.clear()
    files = provider.fetch(it)
    assert files and all(f.startswith(cat.LIVE_ROOT) for f in files)


def test_archive_runs_are_never_probed_and_late_live_runs_are(source, monkeypatch):
    provider = ICONV1Provider("eu_art", min_completeness=0.5)
    young = young_live_run()
    probes = {
        cat.field_uri("icon-art-eu", k, young, "archive") for k in provider._probe_fields(young)
    }
    source.missing |= probes
    # An archive run with its probe files missing is not "still publishing": it is written.
    assert provider.fetch(young)
    # A scheduled partition runs once its six hours are up; a run DWD is late with is
    # still refused rather than written with NaN where the late fields belong.
    monkeypatch.setattr(icon_v1, "choose_source", lambda model, it: "live")
    late = young - pd.Timedelta(12, "h")
    source.missing |= every_field(provider, late, "live")
    with pytest.raises(IncompleteRun):
        provider.fetch(late)


def test_a_live_run_below_the_threshold_is_retried_whatever_its_age(source, monkeypatch):
    # A scheduled partition is six hours old when it runs, so age cannot tell a late run
    # from a finished one; a live run short of fields is retried, never skipped.
    monkeypatch.setattr(icon_v1, "choose_source", lambda model, it: "live")
    provider = ICONV1Provider("eu_art")
    settled = young_live_run() - pd.Timedelta(12, "h")
    tot_prec = {f for f in every_field(provider, settled, "live") if "/p/TOT_PREC/" in f}
    source.missing |= tot_prec
    with pytest.raises(IncompleteRun, match="TOT_PREC"):
        provider.fetch(settled)
    assert provider.completeness[settled].missing_by_param == {"TOT_PREC": len(tot_prec)}


# -- process and write -------------------------------------------------------------------


def test_run_partition_writes_every_level_kind(source, local_config):
    provider = ICONV1Provider("eu_art")
    assert provider.run_partition(RUN_00) is True
    assert provider.store_prefix == "bkr/icon/icon_eu_art.icechunk"
    ds = open_store(local_config, provider.store_prefix)

    assert ds.sizes["init_time"] == 1 and ds.sizes["step"] == 4 and ds.sizes["values"] == N_CELLS
    assert list(pd.to_timedelta(ds["step"].values)) == list(pd.to_timedelta([0, 1, 2, 3], unit="h"))
    np.testing.assert_array_equal(ds["latitude"].values, LATITUDE)
    np.testing.assert_array_equal(ds["longitude"].values, LONGITUDE)
    assert ds["latitude"].attrs["units"] == "degrees_north"
    assert ds["isobaricInhPa"].attrs["units"] == "hPa"

    def at(name, **index):
        return ds[name].isel(init_time=0, **index).values

    # T_2M is float16 in this schema, everything else float32.
    assert ds["t_2m"].dtype == np.float16 and ds["t"].dtype == np.float32
    np.testing.assert_array_equal(
        at("t_2m", step=2), field_value(uri("T_2M", None, None, 2, RUN_00)).astype(np.float16)
    )
    np.testing.assert_array_equal(
        at("t", step=1, isobaricInhPa=1), field_value(uri("T", 100, "85000", 1, RUN_00))
    )
    np.testing.assert_array_equal(
        at("t_ml", step=3, model_level=0), field_value(uri("T", 150, "1", 3, RUN_00))
    )
    np.testing.assert_array_equal(
        at("w_ml", step=0, model_level_half=3), field_value(uri("W", 150, "4", 0, RUN_00))
    )
    np.testing.assert_array_equal(
        at("w_so", step=0, depth=1), field_value(uri("W_SO", 106, "0.01", 0, RUN_00))
    )
    np.testing.assert_array_equal(
        at("ceil_bsc_dust_ml", step=3, wavelength=1, model_level_2_3=0),
        field_value(uri("CEIL_BSC_DUST", 150, "2", 3, RUN_00, wavelength=1064)),
    )
    np.testing.assert_array_equal(
        at("hhl_ml", model_level_half=3), field_value(uri("HHL", 150, "4", 0, RUN_00))
    )
    np.testing.assert_array_equal(at("hsurf"), field_value(uri("HSURF", None, None, 0, RUN_00)))
    assert ds["t_2m"].attrs["dwd_param"] == "T_2M"
    # Chunks are one init time, a few steps, one level and the whole field.
    assert ds["t_ml"].encoding["chunks"] == (1, provider.step_chunk, 1, N_CELLS)
    assert ds["hhl_ml"].encoding["chunks"] == (1, 1, N_CELLS)
    assert ds["t_2m"].encoding["compressors"][0].clevel == icon_v1.CREATE_CLEVEL
    # Every field was read exactly once.
    assert len(source.reads) == len(set(source.reads))


def test_06z_steps_beyond_the_horizon_are_nan(source, local_config):
    provider = ICONV1Provider("eu_art")
    provider.run_partition(RUN_06)
    ds = open_store(local_config, provider.store_prefix)
    t2m = ds["t_2m"].isel(init_time=0)
    # NaN survives float16 storage.
    assert t2m.dtype == np.float16
    assert np.isnan(t2m.isel(step=3).values).all()
    assert np.isfinite(t2m.isel(step=slice(0, 3)).values).all()
    assert np.isnan(ds["t_ml"].isel(init_time=0, step=3).values).all()
    assert not any("PT003H00M" in u for u in source.reads)


def test_archive_gaps_are_nan_and_counted(source, local_config):
    gaps = {uri("T_2M", None, None, 1, RUN_00)} | {
        uri("T", 150, level, 2, RUN_00) for level in ("1", "2")
    }
    source.missing |= gaps
    # Listed, but gone by the time it is read.
    source.unreadable.add(uri("T", 150, "3", 2, RUN_00))
    provider = ICONV1Provider("eu_art")
    assert provider.run_partition(RUN_00) is True
    # The unlisted fields were never requested.
    assert not gaps & set(source.reads)
    ds = open_store(local_config, provider.store_prefix)
    assert np.isnan(ds["t_2m"].isel(init_time=0, step=1).values).all()
    assert np.isfinite(ds["t_2m"].isel(init_time=0, step=2).values).all()
    assert np.isnan(ds["t_ml"].isel(init_time=0, step=2).values).all()

    repo = local_config.icechunk_repo(provider.store_prefix)
    commit = next(iter(repo.ancestry(branch="main")))
    assert "4 of" in commit.message and "missing at the source" in commit.message
    assert commit.metadata["fields_missing"] == 4
    assert commit.metadata["fields_unreadable"] == 1
    assert commit.metadata["fields_listed"] == commit.metadata["fields_expected"] - 3
    assert commit.metadata["source"] == "archive"


def test_a_run_without_coordinates_and_no_store_fails_loudly(source):
    source.missing |= {uri("CLAT", None, None, 0, RUN_00), uri("CLON", None, None, 0, RUN_00)}
    with pytest.raises(icon_v1.MissingGridError):
        ICONV1Provider("eu_art").run_partition(RUN_00)


def test_append_carries_the_stored_grid_over(source, local_config):
    ICONV1Provider("eu_art").run_partition(RUN_00)

    # A fresh process whose run lacks CLAT/CLON takes the grid from the store.
    source.missing |= {uri("CLAT", None, None, 0, RUN_06), uri("CLON", None, None, 0, RUN_06)}
    assert ICONV1Provider("eu_art").run_partition(RUN_06) is True

    # One whose CLAT is spelled differently in the last float bits adopts the store's.
    run_12 = pd.Timestamp("2026-10-11T12:00")
    source.latitude = LATITUDE + np.float32(1e-6)
    assert ICONV1Provider("eu_art").run_partition(run_12) is True

    ds = open_store(local_config, "bkr/icon/icon_eu_art.icechunk")
    assert pd.DatetimeIndex(ds["init_time"].values).tolist() == [RUN_00, RUN_06, run_12]
    np.testing.assert_array_equal(ds["latitude"].values, LATITUDE)
    np.testing.assert_array_equal(
        ds["t_2m"].sel(init_time=RUN_06).isel(step=0).values,
        field_value(uri("T_2M", None, None, 0, RUN_06)).astype(np.float16),
    )
    # Already stored: skipped without reading anything.
    before = len(source.reads)
    assert ICONV1Provider("eu_art").run_partition(RUN_06) is False
    assert len(source.reads) == before


def test_a_moved_grid_is_refused_rather_than_appended(source, local_config):
    ICONV1Provider("eu_art").run_partition(RUN_00)
    source.latitude = LATITUDE + np.float32(0.5)
    provider = ICONV1Provider("eu_art")
    files = provider.fetch(RUN_06)
    ds = provider.process(files, RUN_06)
    repo = local_config.icechunk_repo("bkr/icon/icon_eu_art.icechunk")
    with pytest.raises(ValueError, match="latitude"):
        provider.write_to_icechunk(repo, ds)


def write_old_icon_global(config, init_time=pd.Timestamp("2026-01-31T18:00")) -> None:
    """The original ICON-Global store: lower-case names, a few fields, 0-96 h."""
    from icechunk.xarray import to_icechunk

    old = xr.Dataset(
        {
            "t_2m": (("init_time", "step", "values"), np.zeros((1, 2, N_CELLS), np.float32)),
            "t": (
                ("init_time", "step", "values", "isobaricInhPa"),
                np.zeros((1, 2, N_CELLS, 2), np.float32),
            ),
        },
        coords={
            "init_time": [init_time],
            "step": pd.to_timedelta([0, 96], unit="h"),
            "isobaricInhPa": [500.0, 850.0],
            "latitude": ("values", LATITUDE),
            "longitude": ("values", LONGITUDE),
        },
    )
    session = config.icechunk_repo("bkr/icon/icon_global.icechunk").writable_session("main")
    to_icechunk(old, session)
    session.commit("old schema")


def test_global_starts_a_dated_generation_beside_the_old_store(source, monkeypatch, local_config):
    write_old_icon_global(local_config)
    provider = ICONV1Provider("global")
    assert provider.run_partition(RUN_06) is True
    assert provider.store_prefix == "bkr/icon/icon_global_11102026.icechunk"
    # The old store is a generation of the series but not one of this layout.
    assert provider.generations() == ["bkr/icon/icon_global_11102026.icechunk"]
    old = open_store(local_config, "bkr/icon/icon_global.icechunk")
    assert old.sizes["init_time"] == 1  # untouched

    # The next run finds the new generation and appends to it.
    later = ICONV1Provider("global")
    assert later.run_partition(pd.Timestamp("2026-10-11T12:00")) is True
    assert later.store_prefix == provider.store_prefix
    new = open_store(local_config, provider.store_prefix)
    assert new.sizes["init_time"] == 2
    assert np.isnan(new["t_2m"].isel(init_time=0, step=3).values).all()  # 06Z horizon
    # Runs of this layout are found; the old store's are not this store's runs.
    old_run = pd.Timestamp("2026-01-31T18:00")
    assert later.missing_timesteps(pd.DatetimeIndex([RUN_06, old_run])) == [old_run]


def test_runs_in_other_layouts_do_not_count_as_stored(source, monkeypatch, local_config):
    # The original store and a test subset both hold RUN_06; the full store does not.
    write_old_icon_global(local_config, init_time=RUN_06)
    subset = ICONV1Provider("global", params=["T_2M"])
    assert subset.run_partition(RUN_00) is True
    assert subset.store_prefix == "bkr/icon/icon_global_11102026.icechunk"

    full = ICONV1Provider("global")
    assert full.missing_timesteps(pd.DatetimeIndex([RUN_00, RUN_06])) == [RUN_00, RUN_06]
    assert full.run_partition(RUN_06) is True
    assert full.run_partition(RUN_00) is True
    stored = open_store(local_config, full.store_prefix)
    assert full.store_prefix == "bkr/icon/icon_global_11102026_1.icechunk"
    assert sorted(pd.DatetimeIndex(stored["init_time"].values)) == [RUN_00, RUN_06]
    assert len(stored.data_vars) == len(full.layout.variables)


# -- memory bounding ---------------------------------------------------------------------


def test_the_write_runs_under_bounded_compute(source, monkeypatch, local_config):
    used: list[int] = []

    @contextlib.contextmanager
    def spy(num_workers):
        used.append(num_workers)
        with real_bounded_compute(num_workers):
            yield

    monkeypatch.setattr(icon_v1, "bounded_compute", spy)
    provider = ICONV1Provider("eu_art", max_workers=2, fetch_workers=3)
    assert provider.run_partition(RUN_00)
    assert used == [2]
    # Every data read happened inside the bounded scheduler (CLAT/CLON are read before it).
    assert source.num_workers_seen - {None} == {2}


def test_compute_workers_fit_the_ceiling(source, monkeypatch):
    provider = ICONV1Provider("eu_art", max_workers=8, ceiling_gb=4.0)
    files = provider.fetch(RUN_00)
    ds = provider.process(files, RUN_00)
    gb = 1024**3
    monkeypatch.setattr(icon_v1, "process_tree_rss_gb", lambda: 0.5)
    monkeypatch.setattr(icon_v1, "estimate_peak_bytes", lambda ds, n, reader_bytes: n * gb // 2)
    # 4.0 - 0.5 resident - 0.5 headroom leaves 3.0 GB: six half-GB blocks.
    assert provider.compute_workers(ds) == 6
    monkeypatch.setattr(icon_v1, "estimate_peak_bytes", lambda ds, n, reader_bytes: n * 4 * gb)
    with pytest.raises(MemoryLimitExceeded):
        provider.compute_workers(ds)


def test_the_ceiling_never_exceeds_the_configured_budget(source, monkeypatch, local_config):
    assert ICONV1Provider("eu_art").ceiling_gb == icon_v1.DEFAULT_CEILING_GB
    assert ICONV1Provider("eu_art", ceiling_gb=2.5).ceiling_gb == 2.5
    monkeypatch.setenv("MEMORY_CEILING_GB", "3")
    from planetary_datasets import config as config_module

    config_module.reset_config_cache()
    assert ICONV1Provider("eu_art", ceiling_gb=8).ceiling_gb == 3.0


# -- Dagster -----------------------------------------------------------------------------


def test_dagster_assets_have_their_own_pools_and_memory_tags():
    from dags.assets import icon as icon_assets
    from dags.factory import MEMORY_CLASS_TAG, MEMORY_GB_TAG

    by_name = {a.key.to_user_string(): a for a in icon_assets.icon_assets}
    expected = {"icon_global": "icon", "icon_global_art": "icon-art", "icon_eu_art": "icon-art-eu"}
    pools = set()
    for name, model in expected.items():
        asset = by_name[name]
        assert asset.op.pool not in (None, "icon")
        pools.add(asset.op.pool)
        tags = asset.op.tags
        assert tags[MEMORY_CLASS_TAG] == "small"
        assert tags[MEMORY_GB_TAG] == "4"
        spec = next(iter(asset.specs))
        assert spec.tags[MEMORY_CLASS_TAG] == "small"
        partitions = asset.partitions_def
        assert pd.Timestamp(partitions.start).tz_localize(None) == cat.MODELS[model].archive_start
        assert partitions.cron_schedule == "0 0,6,12,18 * * *"
        assert asset.op.tags["dagster/max_runtime"] == str(12 * 3600)
    assert len(pools) == 3
    # The other ICON assets are unchanged.
    assert {"icon_global_model", "icon_eu", "icon_d2_ruc_60min"} <= set(by_name)
    assert "icon_global_art_analysis" in by_name


def test_dagster_assets_stay_off_the_schedules_while_backfilling():
    from dags.assets import icon as icon_assets
    from dags.factory import SCHEDULE_TAG

    by_name = {a.key.to_user_string(): a for a in icon_assets.icon_assets}
    for variant, name in icon_assets.ICON_V1_ASSETS.items():
        spec = next(iter(by_name[name].specs))
        if variant in icon_assets.ICON_V1_BACKFILLING:
            assert spec.tags[SCHEDULE_TAG] == "manual"
        else:
            assert SCHEDULE_TAG not in spec.tags


# -- backfill script ---------------------------------------------------------------------


def load_script(name: str):
    """Import ``scripts/<name>.py``; the scripts are not a package."""
    import importlib.util
    import pathlib

    path = pathlib.Path(__file__).resolve().parent.parent / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_backfill_script():
    return load_script("icon_v1_backfill")


def test_backfill_walks_the_archive_forward_and_resumes(source, monkeypatch, local_config):
    script = load_backfill_script()
    archived = [pd.Timestamp(t) for t in pd.date_range("2026-10-10T18:00", periods=4, freq="6h")]
    monkeypatch.setattr(script, "list_runs", lambda model, src: list(reversed(archived)))

    assert script.main(["eu_art", "--start", "2026-10-11T00:00", "--dry-run"]) == 0
    assert not source.reads

    ICONV1Provider("eu_art").run_partition(archived[2])
    assert script.main(["eu_art", "--start", "2026-10-11T00:00", "--in-process"]) == 0
    ds = open_store(local_config, "bkr/icon/icon_eu_art.icechunk")
    # The already-stored run was skipped and the rest were appended oldest first.
    assert pd.DatetimeIndex(ds["init_time"].values).tolist() == [
        archived[2],
        archived[1],
        archived[3],
    ]


# -- precision ---------------------------------------------------------------------------


def test_float16_variables_are_built_and_estimated_at_two_bytes(source):
    provider = ICONV1Provider("eu_art")
    ds = provider.process(provider.fetch(RUN_00), RUN_00)
    assert ds["t_2m"].dtype == np.float16
    assert ds["tot_prec"].dtype == np.float32
    assert ds["latitude"].dtype == np.float32
    only_half = ICONV1Provider("eu_art", params=["T_2M"])
    only_full = ICONV1Provider("eu_art", params=["TOT_PREC"])
    half = only_half.process(only_half.fetch(RUN_00), RUN_00)
    full = only_full.process(only_full.fetch(RUN_00), RUN_00)
    from planetary_datasets.common.lazy_fields import estimate_peak_bytes

    def per_block(ds):
        return estimate_peak_bytes(ds, 2) - estimate_peak_bytes(ds, 1)

    assert per_block(half) * 2 == per_block(full)


def test_a_float16_field_that_overflows_is_refused(source, local_config):
    real_decode = source.decode
    huge = uri("T_2M", None, None, 1, RUN_00)

    def decode(data, dtype=np.float32, out=None):
        if data.decode() == huge:
            values = np.full(N_CELLS, 1.0e5, np.float32)
            if out is None:
                return values.astype(dtype)
            with np.errstate(over="ignore"):
                out[...] = values
            return out
        return real_decode(data, dtype=dtype, out=out)

    import planetary_datasets.providers.icon_v1 as module

    module_decode = module.decode_grib_field
    try:
        module.decode_grib_field = decode
        with pytest.raises(icon_v1.PrecisionOverflow, match="T_2M.*100000"):
            ICONV1Provider("eu_art").run_partition(RUN_00)
    finally:
        module.decode_grib_field = module_decode
    # Nothing was committed.
    assert ICONV1Provider("eu_art").missing_timesteps(pd.DatetimeIndex([RUN_00])) == [RUN_00]


def test_a_change_of_precision_is_refused_on_append(source, monkeypatch, local_config):
    ICONV1Provider("eu_art").run_partition(RUN_00)
    data = schema_data("icon-art-eu")
    for entry in data["variables"]:
        entry.pop("dtype", None)
    monkeypatch.setattr(icon_v1, "load_era_schema", lambda model, era: cat.ICONSchema(data))
    with pytest.raises(ValueError, match="dtype differs"):
        ICONV1Provider("eu_art").run_partition(RUN_06)


# -- grid eras ---------------------------------------------------------------------------


def test_runs_follow_their_grid_era(source):
    provider = ICONV1Provider("eu_art")
    last_old, first_new = pd.Timestamp("2026-10-05T18:00"), pd.Timestamp("2026-10-06T00:00")
    assert provider.plan_for(last_old).era.grid_number == 51
    assert provider.plan_for(last_old).n_cells == OLD_CELLS
    assert provider.plan_for(first_new).era.grid_number == 61
    assert provider.plan_for(first_new).n_cells == N_CELLS
    old = provider.process(provider.fetch(last_old), last_old)
    assert old.sizes["values"] == OLD_CELLS
    np.testing.assert_array_equal(old["latitude"].values, OLD_LATITUDE)


def test_old_and_new_grid_runs_go_to_different_generations(source, local_config):
    provider = ICONV1Provider("eu_art")
    assert provider.run_partition(OLD_RUN) is True
    assert provider.store_prefix == "bkr/icon/icon_eu_art.icechunk"
    assert provider.run_partition(RUN_00) is True
    assert provider.store_prefix == "bkr/icon/icon_eu_art_11102026.icechunk"
    # Both grids stay cached for runs that lack CLAT/CLON.
    assert set(provider._grids) == {51, 61}

    old = open_store(local_config, "bkr/icon/icon_eu_art.icechunk")
    new = open_store(local_config, "bkr/icon/icon_eu_art_11102026.icechunk")
    assert old.sizes["values"] == OLD_CELLS and new.sizes["values"] == N_CELLS
    assert old.attrs["icon_v1_grid_number"] == 51 and new.attrs["icon_v1_grid_number"] == 61
    assert set(old.data_vars) == set(new.data_vars)

    # Each era's next run finds its own store, and both count as stored.
    later = ICONV1Provider("eu_art")
    assert later.generations() == [
        "bkr/icon/icon_eu_art.icechunk",
        "bkr/icon/icon_eu_art_11102026.icechunk",
    ]
    assert later.missing_timesteps(pd.DatetimeIndex([OLD_RUN, RUN_00])) == []
    assert later.run_partition(OLD_RUN + pd.Timedelta(6, "h")) is True
    assert later.store_prefix == "bkr/icon/icon_eu_art.icechunk"
    assert open_store(local_config, later.store_prefix).sizes["init_time"] == 2


# -- completeness ------------------------------------------------------------------------


def test_a_run_below_the_threshold_is_skipped_without_a_read(source, local_config):
    provider = ICONV1Provider("eu_art")
    t_ml = {f for f in every_field(provider, RUN_00) if "/p/T/lvt1/150/" in f}
    source.missing |= t_ml
    assert provider.run_partition(RUN_00) is False
    assert not source.reads
    listed = provider.completeness[RUN_00]
    assert listed.fraction < icon_v1.DEFAULT_MIN_COMPLETENESS
    assert listed.missing_by_param == {"T": len(t_ml)}
    assert provider.missing_timesteps(pd.DatetimeIndex([RUN_00])) == [RUN_00]

    # At a lower threshold it is written, NaN where the fields are absent, none of which
    # is requested.
    lenient = ICONV1Provider("eu_art", min_completeness=0.5)
    assert lenient.run_partition(RUN_00) is True
    assert not t_ml & set(source.reads)
    ds = open_store(local_config, lenient.store_prefix)
    assert np.isnan(ds["t_ml"].values).all()
    assert np.isfinite(ds["t"].isel(step=0).values).all()


def test_listing_covers_every_directory_once(source):
    provider = ICONV1Provider("eu_art")
    present, listed = provider.list_run(RUN_00, "archive")
    schema = provider.plan_for(RUN_00).schema
    keys = schema.expected_fields(RUN_00) + schema.coordinate_fields()
    directories = {cat.field_path("icon-art-eu", k, RUN_00).rpartition("/")[0] for k in keys}
    assert sorted(source.listings) == sorted(directories)
    assert listed.fraction == 1.0 and len(present) == len(keys)


# -- backfill preflight and skipped runs -------------------------------------------------


def test_backfill_refuses_to_start_beside_an_empty_generation(source, monkeypatch, local_config):
    script = load_backfill_script()
    monkeypatch.setattr(script, "list_runs", lambda model, src: [RUN_00])
    # What an interrupted first write leaves: a dated repository with nothing committed.
    local_config.icechunk_repo("bkr/icon/icon_global_25082026.icechunk")
    provider = ICONV1Provider("global")
    assert provider.empty_generations() == ["bkr/icon/icon_global_25082026.icechunk"]
    assert script.main(["global"]) == 2
    assert not source.reads
    # An empty base store is not a problem: the first run claims it whatever its date.
    local_config.icechunk_repo("bkr/icon/icon_eu_art.icechunk")
    assert ICONV1Provider("eu_art").empty_generations() == []


def test_backfill_records_skipped_runs(source, monkeypatch, local_config, tmp_path):
    script = load_backfill_script()
    monkeypatch.setattr(script, "list_runs", lambda model, src: [RUN_00, RUN_06])
    provider = ICONV1Provider("eu_art")
    source.missing |= {f for f in every_field(provider, RUN_00) if "/p/T/lvt1/150/" in f}
    log = tmp_path / "skipped.tsv"

    assert script.main(["eu_art", "--dry-run", "--skipped-log", str(log)]) == 0
    assert not source.reads and not log.exists()

    assert script.main(["eu_art", "--skipped-log", str(log), "--in-process"]) == 0
    rows = log.read_text().splitlines()
    assert rows[0].split("\t") == list(script.SKIPPED_COLUMNS)
    assert len(rows) == 2 and rows[1].split("\t")[1] == RUN_00.isoformat()
    ds = open_store(local_config, "bkr/icon/icon_eu_art.icechunk")
    assert pd.DatetimeIndex(ds["init_time"].values).tolist() == [RUN_06]


def test_params_must_select_variables_in_every_era(source, monkeypatch):
    old_era = cat.ERAS["icon-art-eu"][0]
    data = schema_data("icon-art-eu", old_era)
    data["variables"] = [v for v in data["variables"] if v["param"] != "TOT_PREC"]
    schemas = dict(source.schemas)
    schemas[("icon-art-eu", old_era.grid_number)] = cat.ICONSchema(data)
    monkeypatch.setattr(
        icon_v1, "load_era_schema", lambda model, era: schemas[(model, era.grid_number)]
    )
    with pytest.raises(ValueError, match="grid 51"):
        ICONV1Provider("eu_art", params=["TOT_PREC"])


def test_dagster_marks_a_skipped_run_and_reports_completeness(source):
    from dags.assets.icon import completeness_metadata, skipped_as_incomplete

    provider = ICONV1Provider("eu_art")
    source.missing |= {f for f in every_field(provider, RUN_00) if "/p/T/lvt1/150/" in f}
    assert provider.run_partition(RUN_00) is False
    skipped = skipped_as_incomplete(provider, RUN_00, written=False)
    assert skipped is not None and skipped.fraction < provider.min_completeness
    metadata = completeness_metadata(provider, RUN_00)
    assert metadata["fields_listed"].value < metadata["fields_expected"].value
    # A run written, or skipped as already stored, is not flagged.
    source.missing.clear()
    assert provider.run_partition(RUN_00) is True
    assert skipped_as_incomplete(provider, RUN_00, written=True) is None
    assert completeness_metadata(provider, RUN_00)["completeness"].value == 1.0


# -- nothing accumulates across runs -----------------------------------------------------


def test_archive_listings_are_not_cached():
    fs = cat.archive_filesystem()
    fs.dircache["bkr/icon/icon-art-eu/p/T_2M/r/2026-10-11T00:00/s"] = [{"name": "x"}]
    assert len(fs.dircache) == 0


def test_a_long_backfill_does_not_accumulate_objects(source):
    """Runs through one provider leave nothing behind that grows with their number.

    With s3fs caching every listing, each ICON-ART-EU run left about 31,000 objects behind
    and a day-long backfill slowed to a quarter of its speed as garbage collection walked
    them. The bound here is far below that.
    """
    import gc

    provider = ICONV1Provider("eu_art")
    runs = list(pd.date_range("2026-10-07T00:00", periods=8, freq="6h"))
    counts = []
    for it in runs:
        assert provider.run_partition(it) is True
        gc.collect()
        counts.append(len(gc.get_objects()))
    assert counts[-1] - counts[2] < 500 * (len(runs) - 3), counts


# Module-level so a spawned child can import them.


def _report_pid(variant, it, provider_kwargs):
    import os

    return os.getpid()


def _a_result(variant, it, provider_kwargs):
    listed = icon_v1.RunCompleteness(it, "archive", 10, 9, {"T": 1}, 0.5)
    return icon_v1.PartitionResult(it, True, "bkr/icon/x.icechunk", listed, 1.0)


def _raise(variant, it, provider_kwargs):
    raise ValueError(f"no grid for {variant} {it}")


class _Unpicklable(Exception):
    def __init__(self, a, b):
        super().__init__(a)
        self.b = b


def _raise_unpicklable(variant, it, provider_kwargs):
    raise _Unpicklable("lost", object())


def _die(variant, it, provider_kwargs):
    import os

    os._exit(9)


def _echo_kwargs(variant, it, provider_kwargs):
    return provider_kwargs


def test_each_partition_runs_in_a_fresh_process():
    import os

    script = load_backfill_script()
    runner = script.PartitionRunner("eu_art", {}, isolated=True, target=_report_pid)
    pids = [runner(RUN_00), runner(RUN_06)]
    assert os.getpid() not in pids and pids[0] != pids[1]
    in_process = script.PartitionRunner("eu_art", {}, isolated=False, target=_report_pid)
    assert in_process(RUN_00) == os.getpid()


def test_a_child_returns_its_result_and_raises_its_errors():
    result = icon_v1.run_in_child(_a_result, "eu_art", RUN_00, {})
    assert result.written and result.completeness.missing_by_param == {"T": 1}
    with pytest.raises(ValueError, match="no grid for eu_art") as raised:
        icon_v1.run_in_child(_raise, "eu_art", RUN_00, {})
    assert any("in child process" in note for note in raised.value.__notes__)
    # An error that cannot be pickled still arrives, as a RuntimeError naming it.
    with pytest.raises(RuntimeError, match="_Unpicklable: lost"):
        icon_v1.run_in_child(_raise_unpicklable, "eu_art", RUN_00, {})
    with pytest.raises(icon_v1.ChildDied, match="exit code 9"):
        icon_v1.run_in_child(_die, "eu_art", RUN_00, {})


def test_a_child_is_given_the_ceiling_less_the_parents_memory(monkeypatch):
    script = load_backfill_script()
    monkeypatch.setattr(script, "process_tree_rss_gb", lambda: 0.75)
    runner = script.PartitionRunner("eu_art", {"ceiling_gb": 4.0}, target=_echo_kwargs)
    assert runner(RUN_00) == {"ceiling_gb": 3.25}
    monkeypatch.setattr(script, "process_tree_rss_gb", lambda: 3.9)
    assert runner.child_kwargs() == {"ceiling_gb": script.MIN_CHILD_CEILING_GB}


def test_a_failed_first_write_stops_the_backfill(source, monkeypatch, local_config):
    script = load_backfill_script()
    monkeypatch.setattr(script, "list_runs", lambda model, src: [RUN_00, RUN_06])
    calls = []

    def fail_and_leave_an_empty_generation(variant, it, kwargs):
        calls.append(it)
        local_config.icechunk_repo("bkr/icon/icon_global_11102026.icechunk")
        raise RuntimeError("killed mid-write")

    monkeypatch.setattr(script, "run_partition_child", fail_and_leave_an_empty_generation)
    assert script.main(["global", "--in-process"]) == 1
    # The second run, of the same day, was never started.
    assert calls == [RUN_00]


# -- uploads and orphaned chunks ---------------------------------------------------------


def test_stores_upload_chunks_as_parallel_multipart(source, local_config):
    provider = ICONV1Provider("eu_art")
    assert provider.run_partition(RUN_00) is True
    # The repository the write went through, and one reopened for the next run, both
    # upload chunks above the threshold in parallel parts.
    storage = provider.get_icechunk_repo().config.storage
    assert storage.minimum_size_for_multipart_upload == icon_v1.MULTIPART_UPLOAD_MIN_BYTES
    # Every multipart upload has at least two parts.
    assert storage.concurrency.ideal_concurrent_request_size * 2 <= (
        storage.minimum_size_for_multipart_upload
    )
    reopened = ICONV1Provider("eu_art")
    reopened.resolve_store(provider.process(provider.fetch(RUN_06), RUN_06))
    repo = reopened.get_icechunk_repo()
    assert (
        repo.config.storage.minimum_size_for_multipart_upload == icon_v1.MULTIPART_UPLOAD_MIN_BYTES
    )


def load_gc_script():
    return load_script("icon_v1_gc")


def test_gc_finds_and_deletes_the_chunks_of_a_run_that_never_committed(
    source, monkeypatch, local_config
):
    from icechunk.xarray import to_icechunk

    # Real chunk objects, not chunks small enough to be inlined into the manifest.
    original = icon_v1.repository_config

    def config_without_inlining():
        config = original()
        config.inline_chunk_threshold_bytes = 0
        return config

    monkeypatch.setattr(icon_v1, "repository_config", config_without_inlining)
    provider = ICONV1Provider("eu_art")
    assert provider.run_partition(RUN_00) is True

    # A run killed after uploading its chunks and before its commit.
    ds = provider.process(provider.fetch(RUN_06), RUN_06)
    session = provider.store_for(ds, None).writable_session("main")
    to_icechunk(ds, session, append_dim="init_time", mode="a-")
    del session

    gc = load_gc_script()
    prefix = "bkr/icon/icon_eu_art.icechunk"
    import datetime as dt

    # Too young to touch at the default age: a run in progress is safe.
    assert gc.collect(prefix, dt.timedelta(hours=24), delete=False)["chunks"] == 0
    found = gc.collect(prefix, dt.timedelta(0), delete=False)
    # The chunks, and the flush snapshots of the forked write sessions; never a commit.
    assert found["chunks"] > 0
    assert gc.main(["eu_art", "--older-than-hours", "0", "--delete"]) == 0
    assert gc.collect(prefix, dt.timedelta(0), delete=False)["chunks"] == 0

    stored = open_store(local_config, prefix)
    assert pd.DatetimeIndex(stored["init_time"].values).tolist() == [RUN_00]
    np.testing.assert_array_equal(
        stored["t"].isel(init_time=0, step=1, isobaricInhPa=1).values,
        field_value(uri("T", 100, "85000", 1, RUN_00)),
    )


def test_gc_dry_run_creates_nothing(source, local_config):
    import os

    gc = load_gc_script()
    root = local_config.local_store_path("bkr/icon")
    # An existing root, so the provider's own generation lookup lists rather than probes.
    root.mkdir(parents=True)
    ICONV1Provider("eu_art").run_partition(RUN_00)
    before = sorted(os.listdir(root))
    assert gc.main(["eu_art"]) == 0
    # No probing of numbered generations, so no new repositories.
    assert sorted(os.listdir(root)) == before
    assert gc.list_generations(local_config, "bkr/icon/icon_eu_art.icechunk") == [
        "bkr/icon/icon_eu_art.icechunk"
    ]


def test_gc_aborts_unfinished_uploads_older_than_the_cutoff(monkeypatch, local_config):
    import datetime as dt

    gc = load_gc_script()
    now = dt.datetime.now(dt.UTC)
    pages = [
        {
            "Uploads": [
                {"Key": "a", "UploadId": "old", "Initiated": now - dt.timedelta(days=2)},
                {"Key": "b", "UploadId": "new", "Initiated": now - dt.timedelta(hours=1)},
            ],
            "IsTruncated": True,
            "NextKeyMarker": "b",
            "NextUploadIdMarker": "new",
        },
        {
            "Uploads": [{"Key": "c", "UploadId": "older", "Initiated": now - dt.timedelta(days=5)}],
            "IsTruncated": False,
        },
    ]
    calls = []

    def call(operation, **kwargs):
        calls.append((operation, kwargs))
        if operation == "list_multipart_uploads":
            return pages[0] if "KeyMarker" not in kwargs else pages[1]
        return {}

    import dataclasses

    s3_config = dataclasses.replace(local_config, icechunk_local_path=None)
    cutoff = now - dt.timedelta(hours=24)
    prefix = "bkr/icon/icon_eu_art.icechunk"
    assert gc.stale_uploads(local_config, prefix, cutoff, delete=False, call=call) == 0
    assert gc.stale_uploads(s3_config, prefix, cutoff, delete=False, call=call) == 2
    assert not [c for c in calls if c[0] == "abort_multipart_upload"]
    assert calls[0][1]["Prefix"] == "bkr/icon/icon_eu_art.icechunk/"
    calls.clear()
    assert gc.stale_uploads(s3_config, prefix, cutoff, delete=True, call=call) == 2
    aborted = sorted(c[1]["UploadId"] for c in calls if c[0] == "abort_multipart_upload")
    assert aborted == ["old", "older"]
