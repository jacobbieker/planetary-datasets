"""EarthCARE: frame files into the shared store, offline against synthetic frames."""

from __future__ import annotations

import pathlib

import h5py
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from planetary_datasets.providers.polar import earthcare
from planetary_datasets.providers.polar.earthcare import (
    PRODUCTS,
    EarthCAREProvider,
    _name_phony_dims,
    baseline,
    concat_frames,
    frame_key,
    read_frame,
)

FILL = np.float32(9.96921e36)
CLD = PRODUCTS["CPR_CLD_2A"]


def frame_name(code: str, start: str, key: str, cls: str = "EXBC") -> str:
    return f"ECA_{cls}_{code}_{start}Z_20261020T035358Z_{key}"


def write_frame(
    directory: pathlib.Path, name: str, n: int = 4, offset: float = 0.0
) -> pathlib.Path:
    """A CPR_CLD_2A-shaped frame: a track, a height grid, a fill value and a header."""
    path = directory / f"{name}.h5"
    water = np.arange(n * 3, dtype="float32").reshape(n, 3) * 0.25
    water[0, 0] = FILL
    science = xr.Dataset(
        {
            "time": ("along_track", offset + np.arange(n, dtype="float64")),
            "water_content": (("along_track", "CPR_height"), water),
            # Stored height-first, as some products store their arrays.
            "height": (("CPR_height", "along_track"), np.full((3, n), 1000.0, dtype="float32")),
            "quality": ("along_track", np.arange(n, dtype="int32")),
            "class_name": ("class", np.array([b"ice", b"rain"])),
        }
    )
    science["time"].attrs["units"] = "seconds since 2000-1-1 00:00:00.0 0:00"
    science["water_content"].encoding["_FillValue"] = FILL
    science["quality"].encoding["_FillValue"] = np.int32(-2147483647)
    science.to_netcdf(path, group="ScienceData", engine="h5netcdf")
    with h5py.File(path, "a") as f:
        main = f.require_group("HeaderData/VariableProductHeader/MainProductHeader")
        main["orbitNumber"] = np.int32(13601)
        main["frameID"] = np.bytes_(name[-1])
        main["productName"] = np.bytes_(name)
        f.require_group("HeaderData/FixedProductHeader")["File_Class"] = np.bytes_(name[4:8])
    return path


def test_frame_key_and_baseline():
    name = frame_name("ACM_RT__2B", "20261020T015847", "13601E")
    assert frame_key(name) == "13601E"
    assert baseline(name) == "BC"


def test_prefixes_collapse_underscores():
    assert PRODUCTS["ACM_RT__2B"].prefix == "acm_rt_2b"
    assert PRODUCTS["CPR_CLD_2A"].prefix == "cpr_cld_2a"


def test_read_frame_prefixes_transposes_and_keeps_raw_values(tmp_path):
    path = write_frame(tmp_path, frame_name("CPR_CLD_2A", "20261020T015847", "13601E"))
    ds = read_frame(path, CLD)
    assert ds["cpr_cld_2a_water_content"].dims == (
        "cpr_cld_2a_along_track",
        "cpr_cld_2a_CPR_height",
    )
    assert ds["cpr_cld_2a_height"].dims == ("cpr_cld_2a_along_track", "cpr_cld_2a_CPR_height")
    assert ds["cpr_cld_2a_water_content"].values[0, 0] == FILL
    assert ds["cpr_cld_2a_water_content"].attrs["_FillValue"] == FILL
    assert ds["cpr_cld_2a_class_name"].dims == ("cpr_cld_2a_frame", "cpr_cld_2a_class")
    assert list(ds["cpr_cld_2a_class_name"].values[0]) == ["ice", "rain"]
    assert ds["cpr_cld_2a_header_orbitNumber"].values.tolist() == [13601]
    assert ds["cpr_cld_2a_header_File_Class"].values.tolist() == ["EXBC"]
    assert ds["cpr_cld_2a_frame_key"].values.tolist() == ["13601E"]


def test_concat_frames_indexes_each_row_by_frame(tmp_path):
    a = read_frame(
        write_frame(tmp_path, frame_name("CPR_CLD_2A", "20261020T015847", "13601E")), CLD
    )
    b = read_frame(
        write_frame(tmp_path, frame_name("CPR_CLD_2A", "20261020T021011", "13601F"), n=2), CLD
    )
    ds = concat_frames([a, b], CLD, first_index=5)
    assert ds.sizes["cpr_cld_2a_along_track"] == 6
    assert ds.sizes["cpr_cld_2a_frame"] == 2
    assert ds["cpr_cld_2a_frame_index"].values.tolist() == [5, 5, 5, 5, 6, 6]


def test_phony_dims_take_the_majority_name():
    ds = xr.Dataset(
        {
            "a": (
                ("phony_dim_0", "phony_dim_1"),
                np.zeros((5, 3)),
                {"DimensionNames": "nray,nbin"},
            ),
            "b": (
                ("phony_dim_0", "phony_dim_1"),
                np.zeros((5, 3)),
                {"DimensionNames": "nray,nbin"},
            ),
            "c": (
                ("phony_dim_0", "phony_dim_1", "phony_dim_2"),
                np.zeros((5, 3, 2)),
                {"DimensionNames": "2,nray,nbin"},
            ),
            "d": (("phony_dim_3",), np.zeros(1), {"DimensionNames": "nray"}),
        }
    )
    named = _name_phony_dims(ds)
    assert named["c"].dims == ("nray", "nbin", "size_2")
    assert named["d"].dims == ("size_1",)


class FakeMaap:
    """Catalogue items for a day of frames, and downloads that copy local files."""

    def __init__(self, files: dict[str, pathlib.Path]):
        self.files = files

    def search(self, collection, start, end, filters=None, session=None):
        for name in self.files:
            yield {"id": name, "assets": {"enclosure_h5": {"href": name}}}

    def download(self, href, dest, **kwargs):
        dest = pathlib.Path(dest)
        dest.write_bytes(self.files[href].read_bytes())
        return dest


@pytest.fixture
def provider(local_config, monkeypatch, tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    names = [
        frame_name("CPR_CLD_2A", "20261020T015847", "13601E", "EXBA"),
        frame_name("CPR_CLD_2A", "20261020T015847", "13601E", "EXBC"),
        frame_name("CPR_CLD_2A", "20261020T021011", "13601F"),
        # Started the evening before: belongs to the previous day.
        frame_name("CPR_CLD_2A", "20261019T235011", "13600H"),
    ]
    files = {n: write_frame(source, n, offset=i * 10.0) for i, n in enumerate(names)}
    fake = FakeMaap(files)
    monkeypatch.setattr(earthcare, "search", fake.search)
    monkeypatch.setattr(earthcare, "download_one", fake.download)
    made = EarthCAREProvider(config=local_config, products=["CPR_CLD_2A"], download_workers=2)
    monkeypatch.setattr(
        type(made), "auth", property(lambda self: type("A", (), {"headers": lambda s: {}})())
    )
    return made


def stored(provider) -> xr.Dataset:
    repo = provider.get_icechunk_repo()
    return xr.open_zarr(repo.readonly_session("main").store, consolidated=False)


def test_a_day_is_written_once_with_the_newest_baseline(provider):
    day = pd.Timestamp("2026-10-20")
    assert provider.run_partition(day)
    ds = stored(provider)
    assert sorted(ds["cpr_cld_2a_frame_key"].values.tolist()) == ["13601E", "13601F"]
    assert all(n.split("_")[1] != "EXBA" for n in ds["cpr_cld_2a_product_name"].values)
    assert ds.sizes["cpr_cld_2a_along_track"] == 8
    assert not provider.run_partition(day)
    assert stored(provider).sizes["cpr_cld_2a_along_track"] == 8


def test_the_store_decodes_like_the_source(provider):
    provider.run_partition(pd.Timestamp("2026-10-20"))
    ds = stored(provider)
    assert np.isnan(ds["cpr_cld_2a_water_content"].values[0, 0])
    assert ds["cpr_cld_2a_time"].dtype.kind == "M"
    assert (
        ds["cpr_cld_2a_quality"].dtype == np.dtype("float64")
        or ds["cpr_cld_2a_quality"].dtype.kind in "iuf"
    )
    raw = xr.open_zarr(
        provider.get_icechunk_repo().readonly_session("main").store,
        consolidated=False,
        mask_and_scale=False,
    )
    assert raw["cpr_cld_2a_water_content"].values[0, 0] == FILL
    assert raw["cpr_cld_2a_quality"].dtype == np.int32


def test_a_settled_day_is_recorded_complete(provider, monkeypatch):
    day = pd.Timestamp("2026-10-20")
    monkeypatch.setattr(earthcare, "SETTLE_DAYS", 10_000)
    provider.run_partition(day)
    assert provider.missing_timesteps(pd.DatetimeIndex([day])) == [day]
    monkeypatch.setattr(earthcare, "SETTLE_DAYS", -10)
    provider.run_partition(day)
    assert provider.days_complete(CLD) == {"2026-10-20"}
    assert provider.missing_timesteps(pd.DatetimeIndex([day])) == []
