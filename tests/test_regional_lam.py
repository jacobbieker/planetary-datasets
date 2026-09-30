"""Offline tests for the regional limited-area model providers.

Nothing here touches the network or the real archives: the GRIB engine is exercised with
hand-built xarray datasets shaped like what cfgrib returns, and the store round-trip runs
against a local icechunk directory.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from helpers import read_store
from planetary_datasets.providers import dmi_harmonie as dmi
from planetary_datasets.providers import hawaii_nam, hrrr_alaska, kenda, regional_lam_common
from planetary_datasets.providers.regional_lam_common import (
    GribMergeSpec,
    chunk_present,
    clean_grib_subset,
    download_with_filesystem,
    init_time_download_dir,
    long_name_slug,
    rename_present,
    resolve_renames,
    soil_level_count,
)

# --------------------------------------------------------------------------- helpers


def _subset(long_names: dict, level: tuple | None = None, **scalars) -> xr.Dataset:
    """A cfgrib-shaped sub-dataset on a 2x3 y/x grid.

    ``long_names`` maps each variable to its ``long_name`` (``None`` for none), ``level``
    is an optional ``(name, values)`` leading dimension, and ``scalars`` become scalar
    coordinates such as a level type.
    """
    dims = ("y", "x") if level is None else (level[0], "y", "x")
    shape = (2, 3) if level is None else (len(level[1]), 2, 3)
    coords = {"y": np.arange(2), "x": np.arange(3), **scalars}
    if level is not None:
        coords[level[0]] = level[1]
    ds = xr.Dataset(
        {name: (dims, np.full(shape, i, dtype="float32")) for i, name in enumerate(long_names)},
        coords=coords,
    )
    for name, long_name in long_names.items():
        if long_name is not None:
            ds[name].attrs["long_name"] = long_name
    return ds


def _surface_subset(**coords) -> xr.Dataset:
    """A cfgrib-shaped sub-dataset with one surface field on a y/x grid."""
    return _subset({"t": "Temperature"}, surface=0.0, time=pd.Timestamp("2026-01-01"), **coords)


def _alaska_step(step_hours: int) -> xr.Dataset:
    return xr.Dataset(
        {"temperature_at_surface": (("isobaricInhPa", "y", "x"), np.ones((2, 2, 3), dtype="float32"))},
        coords={
            "isobaricInhPa": np.array([500.0, 1000.0]),
            "y": np.arange(2),
            "x": np.arange(3),
            "step": pd.Timedelta(step_hours, "h"),
            "time": pd.Timestamp("2026-01-01T00:00"),
        },
    )


def _hawaii_step(valid_time: str) -> xr.Dataset:
    return xr.Dataset(
        {
            "temperature_at_surface": (("isobaricInhPa", "values"), np.ones((2, 3), dtype="float32")),
            "soil_temperature": (("depthBelowLandLayer", "values"), np.ones((2, 3), dtype="float32")),
        },
        coords={
            "isobaricInhPa": np.array([500.0, 1000.0]),
            "depthBelowLandLayer": np.array([0.4, 0.1]),
            "values": np.arange(3),
            "valid_time": pd.Timestamp(valid_time),
        },
    )


# ------------------------------------------------------------------ provider contract


@pytest.mark.parametrize(
    ("provider_cls", "store_prefix"),
    [
        (dmi.DMIHarmonieProvider, "bkr/dmi/harmonie_greenland_iceland_3.icechunk"),
        (dmi.DMIHarmonieModelLevelProvider, "bkr/dmi/harmonie_greenland_iceland_model_level.icechunk"),
        (hrrr_alaska.AlaskaHRRRProvider, "bkr/dmi/alaska_hrrr.icechunk"),
        (hawaii_nam.HawaiiNAMProvider, "bkr/dmi/hawaii_nams.icechunk"),
        # The KENDA prefixes are the ones the original scripts wrote to.
        (kenda.KENDAAnalysisProvider, "bkr/dmi/kenda_switzerland.icechunk"),
        (kenda.KENDAForecastProvider, "bkr/dmi/kenda_forecast_switzerland.icechunk"),
        # The old name still resolves to the analysis store.
        (kenda.KENDAProvider, "bkr/dmi/kenda_switzerland.icechunk"),
    ],
    ids=[
        "harmonie", "harmonie-model-level", "alaska-hrrr", "hawaii-nam",
        "kenda-analysis", "kenda-forecast", "kenda-legacy-name",
    ],
)  # fmt: skip
def test_providers_keep_their_published_store(provider_cls, store_prefix):
    assert provider_cls.name
    assert provider_cls.append_dim == "time"
    assert provider_cls.store_prefix == store_prefix


@pytest.mark.parametrize("module", [dmi, hrrr_alaska, hawaii_nam, kenda])
def test_no_hardcoded_credentials_in_provider_sources(module):
    import inspect

    source = inspect.getsource(module)
    assert "AKIA" not in source
    assert "access_key" not in source


def test_store_path_follows_the_local_override(local_config):
    provider = hrrr_alaska.AlaskaHRRRProvider(config=local_config)
    assert provider.store_path.endswith("bkr/dmi/alaska_hrrr.icechunk")
    assert not provider.store_path.startswith("s3://")


# ------------------------------------------------------------------------- file naming


def test_alaska_urls_cover_every_level_type_and_step():
    urls = hrrr_alaska.AlaskaHRRRProvider().expected_urls(pd.Timestamp("2024-01-10T06:00"))
    assert len(urls) == 9
    assert urls[0] == (
        "https://noaa-hrrr-bdp-pds.s3.amazonaws.com/hrrr.20240110/alaska/"
        "hrrr.t06z.wrfsfcf00.ak.grib2"
    )
    assert urls[-1].endswith("hrrr.t06z.wrfprsf02.ak.grib2")
    assert all(url.startswith("https://") for url in urls)


def test_hawaii_urls_cover_six_forecast_hours():
    urls = hawaii_nam.HawaiiNAMProvider().expected_urls(pd.Timestamp("2025-05-17T06:00"))
    assert len(urls) == 6
    assert urls[0] == (
        "https://noaa-nam-pds.s3.amazonaws.com/nam.20250517/"
        "nam.t06z.hawaiinest.hiresf00.tm00.grib2"
    )
    assert urls[-1].endswith("hiresf05.tm00.grib2")


def test_harmonie_keys_pair_init_time_with_valid_time():
    provider = dmi.DMIHarmonieProvider()
    key = provider.remote_key(pd.Timestamp("2026-03-28T00:00"), 2, "PL")
    assert key == (
        "dmi-opendata/forecastdata/HARMONIE_IG_PL/"
        "HARMONIE_IG_PL_2026-03-28T000000Z_2026-03-28T020000Z.grib"
    )
    ml = dmi.DMIHarmonieModelLevelProvider().remote_key(pd.Timestamp("2026-03-28T00:00"), 0, "ML")
    assert ml.endswith("HARMONIE_IG_ML_2026-03-28T000000Z_2026-03-28T000000Z.grib")


# ------------------------------------------------------------------------ grib engine


def test_long_name_slug_only_strips_parens_when_asked():
    assert long_name_slug("Best (4-layer) Lifted Index") == "best_(4-layer)_lifted_index"
    assert (
        long_name_slug("Best (4-layer) Lifted Index", strip_parens=True)
        == "best_4-layer_lifted_index"
    )


def test_soil_level_count_treats_a_scalar_as_no_profile():
    scalar = xr.DataArray(0.1)
    profile = xr.DataArray(np.linspace(0, 2, 9), dims="depthBelowLandLayer")
    assert soil_level_count(scalar) == 0
    assert soil_level_count(profile) == 9


HAWAII_FLUX_BLOCK = {
    "prate": "Precipitation rate",
    "dswrf": "Surface downward short-wave radiation flux",
    "gflux": "Ground heat flux",
}


@pytest.mark.parametrize(
    ("subset", "spec"),
    [
        pytest.param(_surface_subset(tropopause=0.0), GribMergeSpec(), id="tropopause-level"),
        pytest.param(
            _surface_subset(potentialVorticity=2e-6), GribMergeSpec(), id="pv-level"
        ),
        pytest.param(
            _subset({"t": None}, level=("heightAboveGround", [2.0, 10.0])),
            GribMergeSpec(),
            id="height-above-ground-as-a-dimension",
        ),
        pytest.param(
            _subset({"unknown": None, "SBT124": None, "refc": None}, surface=0.0),
            GribMergeSpec(),
            id="undecodable-and-simulated-imagery",
        ),
        pytest.param(
            _subset({"t": "Something deprecated"}, surface=0.0), GribMergeSpec(), id="deprecated"
        ),
        # It arrives again, with its partner, on a kept level.
        pytest.param(
            _subset({"u": "U component of wind"}, level=("isobaricInhPa", [500.0, 1000.0])),
            GribMergeSpec(),
            id="lone-wind-component",
        ),
        pytest.param(
            _subset({"gh": "Geopotential Height"}), GribMergeSpec(), id="bare-geopotential-height"
        ),
        pytest.param(
            _subset({"pres": "Pressure"}),
            hawaii_nam.HAWAII_MERGE_SPEC,
            id="hawaii-duplicate-pressure",
        ),
        pytest.param(
            _subset(HAWAII_FLUX_BLOCK, surface=0.0),
            hawaii_nam.HAWAII_MERGE_SPEC,
            id="hawaii-duplicate-flux-block",
        ),
    ],
)
def test_unwanted_subsets_are_dropped_whole(subset, spec):
    assert clean_grib_subset(subset, spec) is None


def test_soil_profile_threshold_differs_between_the_two_nests():
    ds = _subset({"st": "Soil Temperature"}, level=("depthBelowLandLayer", np.linspace(0, 2, 4)))
    # HRRR Alaska only wants the full 9-layer profile; the Hawaii nest keeps this one.
    assert clean_grib_subset(ds, hrrr_alaska.ALASKA_MERGE_SPEC) is None
    kept = clean_grib_subset(ds, hawaii_nam.HAWAII_MERGE_SPEC)
    assert kept is not None
    assert "soil_temperature" in kept.data_vars


@pytest.mark.parametrize(
    ("subset", "expected", "level_coord"),
    [
        pytest.param(
            _surface_subset(), ["temperature_at_surface"], "surface", id="level-type-suffix"
        ),
        pytest.param(
            _subset({"t": "Temperature"}, heightAboveGround=2.0),
            ["temperature_at_2.0m"],
            "heightAboveGround",
            id="height-above-ground-suffix",
        ),
        # Two messages sharing a long_name would make Dataset.rename raise; the loser is
        # dropped rather than the whole timestep.
        pytest.param(
            _subset(
                {"t": "Temperature", "t2": "Temperature", "r": "Relative humidity"}, surface=0.0
            ),
            ["relative_humidity_at_surface", "temperature_at_surface"],
            "surface",
            id="duplicated-long-name",
        ),
    ],
)
def test_variables_are_renamed_by_long_name_with_a_level_suffix(subset, expected, level_coord):
    kept = clean_grib_subset(subset, GribMergeSpec())
    assert sorted(kept.data_vars) == expected
    assert level_coord not in kept.coords


# ------------------------------------------------------------------------- combining


def test_alaska_combine_stacks_steps_under_one_init_time():
    provider = hrrr_alaska.AlaskaHRRRProvider()
    it = pd.Timestamp("2026-01-01T00:00")
    ds = provider.combine([_alaska_step(0), _alaska_step(1), _alaska_step(2)], it)

    assert ds.sizes["time"] == 1
    assert ds.sizes["step"] == 3
    assert pd.Timestamp(ds.time.values[0]) == it
    # Pressure levels are stored descending, as the store was created.
    assert list(ds.level.values) == [1000.0, 500.0]
    assert ds["temperature_at_surface"].dtype == np.dtype("float16")


def test_hawaii_combine_turns_forecast_hours_into_a_time_axis():
    provider = hawaii_nam.HawaiiNAMProvider()
    steps = [_hawaii_step(f"2026-01-01T{hour:02d}:00") for hour in range(3)]
    ds = provider.combine(steps, pd.Timestamp("2026-01-01T00:00"))

    assert ds.sizes["time"] == 3
    assert list(pd.DatetimeIndex(ds.time.values).hour) == [0, 1, 2]
    assert list(ds.level.values) == [1000.0, 500.0]
    assert list(ds.depth.values) == [0.1, 0.4]
    assert ds["soil_temperature"].dtype == np.dtype("float16")


@pytest.mark.parametrize(
    ("provider_cls", "files", "expected"),
    [
        (hrrr_alaska.AlaskaHRRRProvider, ["a.grib2"], 9),
        (dmi.DMIHarmonieProvider, ["a.grib", "b.grib"], 6),
    ],
    ids=["nest", "harmonie"],
)
def test_process_rejects_an_incomplete_file_set(provider_cls, files, expected):
    with pytest.raises(ValueError, match=f"expected {expected} files"):
        provider_cls().process(files, pd.Timestamp("2026-01-01T00:00"))


# ------------------------------------------------------------------- dataset helpers


def test_rename_present_ignores_names_that_are_not_there():
    ds = xr.Dataset({"a": ("isobaricInhPa", np.zeros(2))}, coords={"isobaricInhPa": [1.0, 2.0]})
    renamed = rename_present(ds, {"isobaricInhPa": "level", "depthBelowLandLayer": "depth"})
    assert "level" in renamed.dims
    assert "depth" not in renamed.dims


@pytest.mark.parametrize(
    ("names", "renames", "expected"),
    [
        pytest.param(
            "a", {"a": "alpha", "twater": "total_water"}, {"a": "alpha"}, id="absent-source"
        ),
        # GRIB reuses a long_name across level types; renaming both would raise.
        pytest.param("ab", {"a": "shared", "b": "shared"}, {"a": "shared"}, id="first-claim-wins"),
        pytest.param("ak", {"a": "k"}, {}, id="collides-with-a-variable-left-alone"),
        # Dropping b->c leaves b in place, so a->b must be dropped too, not just b->c.
        pytest.param("abc", {"a": "b", "b": "c"}, {}, id="rechecks-after-a-drop"),
        pytest.param("ab", {"a": "b", "b": "a"}, {"a": "b", "b": "a"}, id="simultaneous-swap"),
    ],
)
def test_resolve_renames_yields_a_mapping_rename_accepts(names, renames, expected):
    ds = xr.Dataset({name: ("x", np.full(2, i)) for i, name in enumerate(names)})
    resolved = resolve_renames(ds, renames)
    assert resolved == expected
    renamed = ds.rename(resolved)  # would raise if the mapping still conflicted
    for source, target in resolved.items():
        assert renamed[target].equals(ds[source])


def test_download_dir_separates_init_times_when_no_temp_dir_is_given(tmp_path):
    first = init_time_download_dir(tmp_path, "alaska_hrrr", pd.Timestamp("2026-01-01T06:00"))
    second = init_time_download_dir(tmp_path, "alaska_hrrr", pd.Timestamp("2026-01-02T06:00"))
    assert first != second
    assert first.parent == second.parent == tmp_path / "alaska_hrrr"


def test_download_dir_honours_an_explicit_temp_dir(tmp_path):
    assert init_time_download_dir(
        tmp_path / "scratch", "alaska_hrrr", pd.Timestamp("2026-01-01T06:00"), tmp_path / "given"
    ) == (tmp_path / "given")


def test_chunk_present_ignores_dimensions_that_are_not_there():
    ds = xr.Dataset({"a": (("time", "x"), np.zeros((2, 3)))})
    chunked = chunk_present(ds, {"time": 1, "x": -1, "level": -1})
    assert chunked.chunksizes["time"] == (1, 1)


def test_harmonie_surface_height_split_keeps_the_unsuffixed_pressure_name():
    ds = _subset({"pres": None}, heightAboveGround=0.0)
    split = dmi._split_by_height(ds, "heightAboveGround", "height_above_ground", rename_scalar=False)
    assert list(split.data_vars) == ["pres"]
    assert "heightAboveGround" not in split.coords


def test_harmonie_surface_height_split_fans_out_a_height_stack():
    ds = _subset({"t": None}, level=("heightAboveGround", [2.0, 10.0]))
    split = dmi._split_by_height(ds, "heightAboveGround", "height_above_ground")
    assert sorted(split.data_vars) == [
        "t_at_height_above_ground_10.0",
        "t_at_height_above_ground_2.0",
    ]
    assert "heightAboveGround" not in split.dims


# ------------------------------------------------------------------------------ KENDA


KENDA_IT = pd.Timestamp("2026-06-20T02:00")


def _write_kenda(root, steps=(0,), constants=True):
    """Empty stand-ins for one init time's files: a ``t`` field per step, and the constants."""
    for step in steps:
        (root / f"kenda-ch1-{KENDA_IT:%Y%m%d%H00}-{step}-t-ctrl.grib2").write_bytes(b"")
    if constants:
        (root / kenda.HORIZONTAL_CONSTANTS).write_bytes(b"")
        (root / kenda.VERTICAL_CONSTANTS).write_bytes(b"")


def test_kenda_archive_path_defaults_under_the_data_dir(local_config, tmp_path):
    assert kenda.KENDAAnalysisProvider().archive_path == tmp_path / "data" / "meteoswiss"


def test_kenda_fetch_requires_the_data_and_the_constants(tmp_path):
    provider = kenda.KENDAAnalysisProvider(archive_path=tmp_path)
    assert provider.fetch(KENDA_IT) == [], "an empty feed"

    _write_kenda(tmp_path, constants=False)
    assert provider.fetch(KENDA_IT) == [], "no constants"

    _write_kenda(tmp_path)
    found = provider.fetch(KENDA_IT)
    assert len(found) == 3
    assert sum("constants" in f for f in found) == 2


def test_kenda_analysis_and_forecast_read_different_steps(tmp_path):
    _write_kenda(tmp_path, steps=(0, 1))
    stamp = f"{KENDA_IT:%Y%m%d%H00}"

    analysis = kenda.KENDAAnalysisProvider(archive_path=tmp_path).fetch(KENDA_IT)
    forecast = kenda.KENDAForecastProvider(archive_path=tmp_path).fetch(KENDA_IT)
    assert any(f"-{stamp}-0-" in f for f in analysis)
    assert not any(f"-{stamp}-1-" in f for f in analysis)
    assert any(f"-{stamp}-1-" in f for f in forecast)


def test_kenda_load_constants_needs_both_files():
    with pytest.raises(FileNotFoundError):
        kenda.load_constants(["horizontal_constants_kenda-ch1.grib2"])


# ---------------------------------------------------------------------------- download


class _FlakyFilesystem:
    """Filesystem that fails a fixed number of times before succeeding."""

    def __init__(self, failures: int):
        self.failures = failures
        self.calls = 0

    def get(self, remote, local):
        self.calls += 1
        if self.calls <= self.failures:
            raise OSError("transient")
        with open(local, "wb") as out:
            out.write(b"grib")


def test_download_with_filesystem_retries_then_succeeds(tmp_path):
    fs = _FlakyFilesystem(failures=2)
    dest = download_with_filesystem(fs, "bucket/key.grib", tmp_path / "key.grib", backoff=0)
    assert dest is not None
    assert dest.read_bytes() == b"grib"
    assert fs.calls == 3


def test_download_with_filesystem_leaves_no_partial_file(tmp_path):
    fs = _FlakyFilesystem(failures=99)
    assert download_with_filesystem(fs, "bucket/key.grib", tmp_path / "key.grib", backoff=0) is None
    assert list(tmp_path.iterdir()) == []


def test_download_with_filesystem_skips_an_existing_file(tmp_path):
    dest = tmp_path / "key.grib"
    dest.write_bytes(b"already here")
    fs = _FlakyFilesystem(failures=99)
    assert download_with_filesystem(fs, "bucket/key.grib", dest) == dest
    assert fs.calls == 0


def test_nest_fetch_skips_an_init_time_with_missing_files(tmp_path, monkeypatch):
    provider = hrrr_alaska.AlaskaHRRRProvider()

    def _only_two(urls, dest_dir, **kwargs):
        return [tmp_path / "a", tmp_path / "b"]

    monkeypatch.setattr(regional_lam_common, "download_many", _only_two)
    assert provider.fetch(pd.Timestamp("2026-01-01T00:00"), temp_dir=tmp_path) == []


# --------------------------------------------------------------------- store roundtrip


def test_run_partition_writes_and_then_skips(local_config, monkeypatch):
    """A full pass through BaseProvider.run_partition against a local store."""
    provider = hrrr_alaska.AlaskaHRRRProvider(config=local_config)
    it = pd.Timestamp("2026-01-01T00:00")

    monkeypatch.setattr(type(provider), "fetch", lambda self, it, temp_dir=None, **kw: ["a"] * 9)
    monkeypatch.setattr(
        type(provider),
        "process",
        lambda self, files, it, temp_dir=None, **kw: self.combine(
            [_alaska_step(0), _alaska_step(1), _alaska_step(2)], it
        ),
    )

    assert provider.run_partition(it) is True
    assert provider.run_partition(it) is False

    stored = read_store(provider)
    assert pd.Timestamp(stored.time.values[0]) == it
    assert stored.sizes["step"] == 3
