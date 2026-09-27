"""Offline tests for the Météo-France AROME providers.

The GRIB inputs are too large to ship as fixtures, so these exercise the file naming, the
input selection, the dataset shaping and the fetch/process/write/cleanup contract with
synthetic data. Nothing here touches the network or the public bucket.
"""

from __future__ import annotations

import dataclasses
import pathlib

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from planetary_datasets.providers import arome
from planetary_datasets.providers.arome import (
    OVERSEAS_REGIONS,
    AromeFranceHDProvider,
    AromeFranceProvider,
    AromeOverseasProvider,
    AromeProvider,
)

INIT_TIME = pd.Timestamp("2026-04-15T12:00:00")


def test_overseas_filenames_and_urls_match_the_archive():
    layout = AromeOverseasProvider(region="INDIEN").layout
    assert layout.filename("HP1", "000H", INIT_TIME) == (
        "arome-om-INDIEN__0025__HP1__000H__2026-04-15T120000Z.grib2"
    )
    assert layout.url("HP1", "000H", INIT_TIME) == (
        "https://files.data.gouv.fr/meteofrance-pnt/pnt/2026-04-15T12:00:00Z/"
        "arome-om/INDIEN/0025/HP1/"
        "arome-om-INDIEN__0025__HP1__000H__2026-04-15T12:00:00Z.grib2"
    )
    assert layout.step_tokens == ("000H", "001H", "002H", "003H", "004H", "005H", "006H")
    # A missing analysis step is normal overseas (IP4 starts at 001H).
    assert layout.require_all_steps is False


def test_france_filenames_and_urls_match_the_archive():
    layout = AromeFranceProvider().layout
    assert layout.filename("HP1", "00H06H", INIT_TIME) == (
        "arome__0025__HP1__00H06H__2026-04-15T120000Z.grib2"
    )
    assert layout.url("HP1", "00H06H", INIT_TIME) == (
        "https://files.data.gouv.fr/meteofrance-pnt/pnt/2026-04-15T12:00:00Z/"
        "arome/0025/HP1/arome__0025__HP1__00H06H__2026-04-15T12:00:00Z.grib2"
    )
    assert layout.require_all_steps is True


def test_france_hd_filenames_and_urls_match_the_archive():
    layout = AromeFranceHDProvider().layout
    assert layout.filename("SP2", "01H", INIT_TIME) == (
        "arome__001__SP2__01H__2026-04-15T120000Z.grib2"
    )
    assert layout.url("SP2", "01H", INIT_TIME) == (
        "https://files.data.gouv.fr/meteofrance-pnt/pnt/2026-04-15T12:00:00Z/"
        "arome/001/SP2/arome__001__SP2__01H__2026-04-15T12:00:00Z.grib2"
    )
    assert layout.step_tokens == ("00H", "01H", "02H")


@pytest.mark.parametrize("region", sorted(OVERSEAS_REGIONS))
def test_every_overseas_region_has_its_own_store(region):
    provider = AromeOverseasProvider(region=region)
    assert provider.store_prefix == f"bkr/dmi/arome_{OVERSEAS_REGIONS[region]}.icechunk"
    assert provider.name == f"arome_{OVERSEAS_REGIONS[region]}"


def test_unknown_overseas_region_is_rejected():
    with pytest.raises(ValueError, match="unknown AROME overseas region"):
        AromeOverseasProvider(region="ATLANTIS")


def test_store_prefixes_are_the_ones_already_published():
    assert AromeFranceProvider().store_prefix == "bkr/dmi/arome_france_0025.icechunk"
    assert AromeFranceHDProvider().store_prefix == "bkr/dmi/arome_france.icechunk"


def test_download_dir_is_under_the_configured_data_dir(local_config, tmp_path):
    config = dataclasses.replace(local_config, data_dir=tmp_path / "data")
    provider = AromeFranceHDProvider(config=config)
    assert provider.download_dir == tmp_path / "data" / "meteofrance_france"
    assert provider.download_dir.is_dir()


def test_files_for_selects_one_paquet_in_step_order():
    files = [
        "/tmp/arome-om-INDIEN__0025__IP1__002H__x.grib2",
        "/tmp/arome-om-INDIEN__0025__IP1__000H__x.grib2",
        "/tmp/arome-om-INDIEN__0025__IP10__000H__x.grib2",
        "/tmp/arome-om-INDIEN__0025__HP1__000H__x.grib2",
    ]
    selected = arome._files_for(files, "IP1")
    assert [f.split("__")[3] for f in selected] == ["000H", "002H"]


def test_missing_paquet_is_reported_with_its_name():
    with pytest.raises(FileNotFoundError, match="no HP2 files"):
        arome._require_all(["/tmp/arome__0025__HP1__00H06H__x.grib2"], "HP2", INIT_TIME)


def test_step_becomes_the_valid_time():
    ds = xr.Dataset(
        {"t": (("step", "latitude"), np.zeros((3, 2), dtype="float32"))},
        coords={
            "step": pd.to_timedelta([0, 1, 2], unit="h"),
            "latitude": [0.0, 1.0],
            "time": pd.Timestamp("2026-04-15T12:00"),
            "valid_time": ("step", pd.date_range("2026-04-15T12:00", periods=3, freq="h")),
        },
    )
    out = arome._step_to_time(ds)
    assert "step" not in out.coords
    assert "valid_time" not in out.coords
    assert list(out.time.values) == list(
        pd.date_range("2026-04-15T12:00", periods=3, freq="h").values
    )


def test_finalise_orders_coords_reduces_precision_and_chunks():
    ds = xr.Dataset(
        {
            "temperature_at_height": (
                ("time", "height", "level", "latitude", "longitude"),
                np.ones((2, 2, 3, 2, 2), dtype="float32"),
            ),
            "geopotential": (
                ("time", "height", "level", "latitude", "longitude"),
                np.ones((2, 2, 3, 2, 2), dtype="float32"),
            ),
        },
        coords={
            "time": pd.date_range("2026-04-15T13:00", periods=2, freq="-1h"),
            "height": [10.0, 2.0],
            "level": [500.0, 1000.0, 850.0],
            "latitude": [0.0, 1.0],
            "longitude": [0.0, 1.0],
        },
    )
    out = arome._finalise(ds)

    assert list(out.level.values) == [1000.0, 850.0, 500.0]
    assert list(out.height.values) == [2.0, 10.0]
    assert out.time.values[0] < out.time.values[1]
    assert out["temperature_at_height"].dtype == np.dtype("float16")
    assert out["geopotential"].dtype == np.dtype("float32")
    assert out.chunksizes["time"] == (1, 1)
    assert out.chunksizes["level"] == (3,)


class _StubProvider(AromeProvider):
    """An AROME provider whose inputs are plain files, for testing the write path."""

    name = "arome_stub"
    append_dim = "time"
    store_prefix = "bkr/test/arome_stub.icechunk"
    guard_memory = False

    def __init__(self, files, dataset, config=None):
        self.files = [str(f) for f in files]
        self.dataset = dataset
        super().__init__(config)

    @property
    def layout(self) -> arome.AromeLayout:
        return arome.AromeLayout(
            model_path="arome/0025",
            file_stem="arome",
            resolution="0025",
            paquets=("SP1",),
            step_tokens=("00H06H",),
            download_subdir="stub",
        )

    def fetch(self, it, temp_dir=None, **kwargs):
        return self.files

    def process(self, input_files, it, temp_dir=None, **kwargs):
        return self.dataset


def _grib_stubs(tmp_path: pathlib.Path) -> list[pathlib.Path]:
    paths = []
    for name in ("arome__0025__SP1__00H06H__x.grib2", "arome__0025__SP2__00H06H__x.grib2"):
        path = tmp_path / name
        path.write_bytes(b"GRIB")
        # cfgrib leaves an index sidecar next to each file; it must go too.
        path.with_name(path.name + ".5b7b6.idx").write_bytes(b"idx")
        paths.append(path)
    return paths


def test_run_partition_writes_and_then_deletes_the_gribs(local_config, tmp_path, sample_dataset):
    files = _grib_stubs(tmp_path)
    provider = _StubProvider(files, sample_dataset, config=local_config)

    it = pd.Timestamp(sample_dataset.time.values[0])
    assert provider.run_partition(it) is True

    for path in files:
        assert not path.exists()
        assert not list(path.parent.glob(path.name + "*.idx"))

    store = xr.open_zarr(provider.get_icechunk_repo().readonly_session("main").store, consolidated=False)
    assert pd.Timestamp(store.time.values[0]) == it


def test_run_partition_skips_a_timestep_that_is_already_stored(local_config, tmp_path, sample_dataset):
    it = pd.Timestamp(sample_dataset.time.values[0])
    provider = _StubProvider(_grib_stubs(tmp_path), sample_dataset, config=local_config)
    assert provider.run_partition(it) is True

    files = _grib_stubs(tmp_path)
    again = _StubProvider(files, sample_dataset, config=local_config)
    assert again.run_partition(it) is False
    # Nothing was written, so the second attempt's files stay for a later retry.
    assert all(path.exists() for path in files)


def test_run_partition_keeps_the_gribs_when_the_write_is_skipped(local_config, tmp_path, sample_dataset):
    files = _grib_stubs(tmp_path)
    provider = _StubProvider(files, sample_dataset, config=local_config)
    provider.write_to_icechunk = lambda repo, processed: False

    assert provider.run_partition(pd.Timestamp(sample_dataset.time.values[0])) is False
    assert all(path.exists() for path in files)


def test_fetch_gives_up_when_a_required_paquet_is_unavailable(local_config, tmp_path, monkeypatch):
    provider = AromeFranceProvider(config=dataclasses.replace(local_config, data_dir=tmp_path))
    monkeypatch.setattr(arome, "download_one", lambda url, dest, **kwargs: None)

    assert provider.fetch(INIT_TIME) == []


def test_fetch_returns_every_paquet_when_all_download(local_config, tmp_path, monkeypatch):
    provider = AromeFranceProvider(config=dataclasses.replace(local_config, data_dir=tmp_path))

    def _fake_download(url, dest, **kwargs):
        dest = pathlib.Path(dest)
        dest.write_bytes(b"GRIB")
        return dest

    monkeypatch.setattr(arome, "download_one", _fake_download)

    files = provider.fetch(INIT_TIME)
    assert [pathlib.Path(f).name for f in files] == [
        f"arome__0025__{paquet}__00H06H__2026-04-15T120000Z.grib2"
        for paquet in ("HP1", "HP2", "IP1", "IP3", "SP1", "SP2", "SP3")
    ]


def test_overseas_fetch_tolerates_a_paquet_with_no_analysis_step(local_config, tmp_path, monkeypatch):
    provider = AromeOverseasProvider(region="INDIEN", config=dataclasses.replace(local_config, data_dir=tmp_path))

    def _fake_download(url, dest, **kwargs):
        dest = pathlib.Path(dest)
        if "IP4__000H" in dest.name:
            return None
        dest.write_bytes(b"GRIB")
        return dest

    monkeypatch.setattr(arome, "download_one", _fake_download)

    files = provider.fetch(INIT_TIME)
    assert len(files) == 9 * 7 - 1
    assert len(arome._files_for(files, "IP4")) == 6


def test_overseas_fetch_gives_up_when_a_paquet_is_missing_entirely(local_config, tmp_path, monkeypatch):
    provider = AromeOverseasProvider(region="INDIEN", config=dataclasses.replace(local_config, data_dir=tmp_path))

    def _fake_download(url, dest, **kwargs):
        dest = pathlib.Path(dest)
        if "HP2__" in dest.name:
            return None
        dest.write_bytes(b"GRIB")
        return dest

    monkeypatch.setattr(arome, "download_one", _fake_download)

    assert provider.fetch(INIT_TIME) == []
