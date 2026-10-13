"""Offline tests for the ICON v1 provider.

No network: the schema, the source listings and the GRIB reads are all faked, and every
store is local.
"""

# ruff: noqa: D102, D103, D107 - test names say what they check, as elsewhere in tests/

from __future__ import annotations

import contextlib
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


def schema_data(model: str) -> dict:
    """A miniature schema with one variable of every kind the provider handles.

    00/12Z runs go to step 3, 06/18Z runs to step 2. ``CEIL_BSC_DUST`` is wavelength
    resolved on a subset (2-3) of the full model levels (1-3); ``W`` and ``HHL`` are on
    the half levels (1-4); ``HHL`` and ``HSURF`` are published at step 0 only.
    """
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
        {"param": "T_2M", "level_type": None, "levels": [], "steps": every},
        {"param": "W", "level_type": 150, "levels": ["1", "2", "3", "4"], "steps": every},
        {"param": "W_SO", "level_type": 106, "levels": ["0.0", "0.01"], "steps": every},
    ]
    return {
        "format_version": 1,
        "model": model,
        "n_cells": N_CELLS,
        "grid_number": cat.MODELS[model].grid_number,
        "source": "live",
        "reference_runs": [],
        "generated_at": "2026-10-13T00:00:00Z",
        "coordinate_params": ["CLAT", "CLON"],
        "invariant_params": ["HHL", "HSURF"],
        "step_sets": {"s0": [0, 1, 2, 3], "s1": [0, 1, 2], "s2": [0]},
        "variables": variables,
        "excluded": [],
    }


def field_value(uri: str) -> np.ndarray:
    """The deterministic field the fake source holds at ``uri``."""
    base = zlib.crc32(uri.encode()) % 1000
    return (base + np.arange(N_CELLS) / 10).astype(np.float32)


class FakeSource:
    """Stands in for the archive: ``read_bytes`` returns the URI, decoding derives values.

    Attributes:
        missing: URIs that read as permanently absent.
        reads: Every URI read, in order.
        latitude: The CLAT field served, so a test can move the grid.
    """

    def __init__(self) -> None:
        self.missing: set[str] = set()
        self.reads: list[str] = []
        self.latitude = LATITUDE.copy()
        self.num_workers_seen: set = set()

    def read_bytes(self, uri: str, **kwargs) -> bytes | None:
        self.reads.append(uri)
        self.num_workers_seen.add(dask.config.get("num_workers", None))
        if uri in self.missing:
            return None
        return uri.encode()

    def decode(self, data: bytes, dtype=np.float32, out=None) -> np.ndarray:
        uri = data.decode()
        if "/p/CLAT/" in uri:
            values = self.latitude
        elif "/p/CLON/" in uri:
            values = LONGITUDE
        else:
            values = field_value(uri)
        if out is None:
            return values.astype(dtype)
        out[...] = values
        return out

    @staticmethod
    def info(data: bytes, model: str = "icon-art-eu") -> types.SimpleNamespace:
        return types.SimpleNamespace(
            number_of_values=N_CELLS, grid_number=cat.MODELS[model].grid_number
        )


@pytest.fixture
def source(monkeypatch, local_config):
    """Fake schema, source choice and GRIB reads for every model; local stores."""
    schemas = {model: cat.ICONSchema(schema_data(model)) for model in cat.MODELS}
    fake = FakeSource()
    monkeypatch.setattr(icon_v1, "load_schema", lambda model: schemas[model])
    monkeypatch.setattr(icon_v1, "choose_source", lambda model, it: "archive")
    monkeypatch.setattr(icon_v1, "read_bytes", fake.read_bytes)
    monkeypatch.setattr(icon_v1, "decode_grib_field", fake.decode)
    monkeypatch.setattr(icon_v1, "grib_field_info", fake.info)
    return fake


def uri(param, level_type, level, step, it, wavelength=None, model="icon-art-eu", src="archive"):
    key = cat.FieldKey(param, level_type, level, step, wavelength)
    return cat.field_uri(model, key, it, src)


def open_store(config, prefix: str) -> xr.Dataset:
    repo = config.icechunk_repo(prefix)
    return xr.open_zarr(repo.readonly_session("main").store, consolidated=False)


# -- layout ------------------------------------------------------------------------------


def test_layout_names_every_level_kind(source):
    layout = plan_variables(icon_v1.load_schema("icon-art-eu"))
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
    schema = icon_v1.load_schema("icon-art-eu")
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


def test_a_young_live_run_still_arriving_is_refused(source, monkeypatch):
    it = young_live_run()
    monkeypatch.setattr(icon_v1, "choose_source", lambda model, it: "live")
    published: set[str] = set()

    def lister(directory):
        return [n.rpartition("/")[2] for n in published if n.startswith(directory + "/")]

    monkeypatch.setattr(icon_v1, "get_lister", lambda model, src: lister)
    provider = ICONV1Provider("eu_art")
    with pytest.raises(IncompleteRun, match="still being published"):
        provider.fetch(it)

    # Once the last step of every probed variable is there, the run is taken.
    for key in provider._probe_fields(it):
        published.add(cat.field_path("icon-art-eu", key, it))
    files = provider.fetch(it)
    assert files and all(f.startswith(cat.LIVE_ROOT) for f in files)


def test_archive_and_settled_live_runs_are_never_probed(source, monkeypatch):
    def lister(directory):
        raise AssertionError("listed")

    monkeypatch.setattr(icon_v1, "get_lister", lambda model, src: lister)
    assert ICONV1Provider("eu_art").fetch(young_live_run())  # archive
    monkeypatch.setattr(icon_v1, "choose_source", lambda model, it: "live")
    assert ICONV1Provider("eu_art").fetch(RUN_00)  # live, but long settled


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

    np.testing.assert_array_equal(
        at("t_2m", step=2), field_value(uri("T_2M", None, None, 2, RUN_00))
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
    assert np.isnan(t2m.isel(step=3).values).all()
    assert np.isfinite(t2m.isel(step=slice(0, 3)).values).all()
    assert np.isnan(ds["t_ml"].isel(init_time=0, step=3).values).all()
    assert not any("PT003H00M" in u for u in source.reads)


def test_archive_gaps_are_nan_and_counted(source, local_config):
    gaps = {uri("T_2M", None, None, 1, RUN_00)} | {
        uri("T", 150, level, 2, RUN_00) for level in ("1", "2", "3")
    }
    source.missing |= gaps
    provider = ICONV1Provider("eu_art")
    assert provider.run_partition(RUN_00) is True
    ds = open_store(local_config, provider.store_prefix)
    assert np.isnan(ds["t_2m"].isel(init_time=0, step=1).values).all()
    assert np.isfinite(ds["t_2m"].isel(init_time=0, step=2).values).all()
    assert np.isnan(ds["t_ml"].isel(init_time=0, step=2).values).all()

    repo = local_config.icechunk_repo(provider.store_prefix)
    commit = next(iter(repo.ancestry(branch="main")))
    assert "4 of" in commit.message and "missing at the source" in commit.message
    assert commit.metadata["fields_missing"] == 4
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
        field_value(uri("T_2M", None, None, 0, RUN_06)),
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


def write_old_icon_global(config) -> None:
    """The original ICON-Global store: lower-case names, 0-96 h, ended 2026-01-31."""
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
            "init_time": [pd.Timestamp("2026-01-31T18:00")],
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
    monkeypatch.setattr(
        icon_v1, "grib_field_info", lambda data: FakeSource.info(data, model="icon")
    )
    write_old_icon_global(local_config)
    provider = ICONV1Provider("global")
    assert provider.run_partition(RUN_06) is True
    assert provider.store_prefix == "bkr/icon/icon_global_11102026.icechunk"
    assert provider.generations() == [
        "bkr/icon/icon_global.icechunk",
        "bkr/icon/icon_global_11102026.icechunk",
    ]
    old = open_store(local_config, "bkr/icon/icon_global.icechunk")
    assert old.sizes["init_time"] == 1  # untouched

    # The next run finds the new generation and appends to it.
    later = ICONV1Provider("global")
    assert later.run_partition(pd.Timestamp("2026-10-11T12:00")) is True
    assert later.store_prefix == provider.store_prefix
    new = open_store(local_config, provider.store_prefix)
    assert new.sizes["init_time"] == 2
    assert np.isnan(new["t_2m"].isel(init_time=0, step=3).values).all()  # 06Z horizon
    # Both runs are found across generations, and neither is fetched again.
    assert (
        later.missing_timesteps(pd.DatetimeIndex([RUN_06, pd.Timestamp("2026-01-31T18:00")])) == []
    )


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
    # 4.0 - 0.5 resident - 0.25 headroom leaves 3.25 GB: six half-GB blocks.
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
        assert asset.op.retry_policy is not None
    assert len(pools) == 3
    # The other ICON assets are unchanged.
    assert {"icon_global_model", "icon_eu", "icon_d2_ruc_60min"} <= set(by_name)
    assert "icon_global_art_analysis" in by_name


# -- backfill script ---------------------------------------------------------------------


def load_backfill_script():
    import importlib.util
    import pathlib

    path = pathlib.Path(__file__).resolve().parent.parent / "scripts" / "icon_v1_backfill.py"
    spec = importlib.util.spec_from_file_location("icon_v1_backfill", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_backfill_walks_the_archive_forward_and_resumes(source, monkeypatch, local_config):
    script = load_backfill_script()
    archived = [pd.Timestamp(t) for t in pd.date_range("2026-10-10T18:00", periods=4, freq="6h")]
    monkeypatch.setattr(script, "list_runs", lambda model, src: list(reversed(archived)))

    assert script.main(["eu_art", "--start", "2026-10-11T00:00", "--dry-run"]) == 0
    assert not source.reads

    ICONV1Provider("eu_art").run_partition(archived[2])
    assert script.main(["eu_art", "--start", "2026-10-11T00:00"]) == 0
    ds = open_store(local_config, "bkr/icon/icon_eu_art.icechunk")
    # The already-stored run was skipped and the rest were appended oldest first.
    assert pd.DatetimeIndex(ds["init_time"].values).tolist() == [
        archived[2],
        archived[1],
        archived[3],
    ]
