"""EarthCARE: frame files into the shared store, offline against synthetic frames."""

from __future__ import annotations

import collections
import pathlib

import h5py
import numpy as np
import pandas as pd
import pytest
import xarray as xr
import zarr

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
    """A MAAP product name for a frame of ``code`` starting at ``start``."""
    return f"ECA_{cls}_{code}_{start}Z_20261020T035358Z_{key}"


def write_frame(
    directory: pathlib.Path,
    name: str,
    n: int = 4,
    offset: float = 0.0,
    status: str | None = None,
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
    if status == "2d":
        science["status"] = (("one", "along_track"), np.ones((1, n), dtype="int8"))
    elif status == "1d":
        science["status"] = ("along_track", np.full(n, 2, dtype="int8"))
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
    """The frame key and baseline come out of a product name."""
    name = frame_name("ACM_RT__2B", "20261020T015847", "13601E")
    assert frame_key(name) == "13601E"
    assert baseline(name) == "BC"


def test_prefixes_collapse_underscores():
    """A product's store prefix is its code lower-cased, runs of underscores collapsed."""
    assert PRODUCTS["ACM_RT__2B"].prefix == "acm_rt_2b"
    assert PRODUCTS["CPR_CLD_2A"].prefix == "cpr_cld_2a"


def test_read_frame_prefixes_transposes_and_keeps_raw_values(tmp_path):
    """read_frame prefixes names, puts the track first and keeps the file's raw values."""
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
    """concat_frames points each along-track row at its frame."""
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
    """A phony dimension takes the name most of its variables give it."""
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
        """Serve ``files``, keyed by product name."""
        self.files = files

    def search(self, collection, start, end, filters=None, session=None):
        """Yield a catalogue item for each file."""
        for name in self.files:
            yield {"id": name, "assets": {"enclosure_h5": {"href": name}}}

    def download(self, href, dest, **kwargs):
        """Copy the file behind ``href`` to ``dest``."""
        dest = pathlib.Path(dest)
        dest.write_bytes(self.files[href].read_bytes())
        return dest


@pytest.fixture
def provider(local_config, monkeypatch, tmp_path):
    """A CPR_CLD_2A provider on a local store, reading frames from a fake MAAP."""
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
    made.test_maap = fake  # the tests below wrap its downloads
    monkeypatch.setattr(
        type(made), "auth", property(lambda self: type("A", (), {"headers": lambda s: {}})())
    )
    return made


def stored(provider) -> xr.Dataset:
    """The provider's store as written so far."""
    repo = provider.get_icechunk_repo()
    return xr.open_zarr(repo.readonly_session("main").store, consolidated=False)


def test_a_day_is_written_once_with_the_newest_baseline(provider):
    """A day's frames are written once, each at its newest baseline."""
    day = pd.Timestamp("2026-10-20")
    assert provider.run_partition(day)
    ds = stored(provider)
    assert sorted(ds["cpr_cld_2a_frame_key"].values.tolist()) == ["13601E", "13601F"]
    assert all(n.split("_")[1] != "EXBA" for n in ds["cpr_cld_2a_product_name"].values)
    assert ds.sizes["cpr_cld_2a_along_track"] == 8
    assert not provider.run_partition(day)
    assert stored(provider).sizes["cpr_cld_2a_along_track"] == 8


def test_a_variable_that_loses_a_size_one_dimension_is_written_in_the_stored_layout(provider):
    """Baseline BD's 1-D ``quality_status`` appends to the (n, 1) array baseline BC made."""
    directory = next(iter(provider.test_maap.files.values())).parent / "status"
    directory.mkdir()
    old = frame_name("CPR_CLD_2A", "20261020T015847", "13601E")
    new = frame_name("CPR_CLD_2A", "20261021T015847", "13602E", "EXBD")
    provider.test_maap.files = {
        old: write_frame(directory, old, status="2d"),
        new: write_frame(directory, new, offset=50.0, status="1d"),
    }
    assert provider.run_partition(pd.Timestamp("2026-10-20"))
    assert provider.run_partition(pd.Timestamp("2026-10-21"))
    group = zarr.open_group(provider.get_icechunk_repo().readonly_session("main").store, mode="r")
    status = group["cpr_cld_2a_status"]
    assert status.shape == (8, 1)
    assert tuple(status.metadata.dimension_names) == ("cpr_cld_2a_along_track", "cpr_cld_2a_one")
    assert status[:].ravel().tolist() == [1, 1, 1, 1, 2, 2, 2, 2]


def test_a_rank_change_beyond_size_one_dimensions_raises(provider):
    """Only size-1 dimensions are reconciled; any other mismatch is refused, not reshaped."""
    ds = xr.Dataset({"cpr_cld_2a_status": (("cpr_cld_2a_along_track",), np.zeros(4, "int8"))})
    group = zarr.group()
    group.create_array(
        "cpr_cld_2a_status",
        shape=(4, 3),
        dtype="int8",
        dimension_names=("cpr_cld_2a_along_track", "cpr_cld_2a_one"),
    )
    with pytest.raises(ValueError, match="cpr_cld_2a_status"):
        earthcare.match_stored_rank(ds, group)


def test_equal_sized_dimensions_with_other_names_are_not_relabelled(provider):
    """Matching sizes is not enough: a non-singleton dimension must keep its name."""
    ds = xr.Dataset(
        {"cpr_cld_2a_v": (("cpr_cld_2a_along_track", "cpr_cld_2a_y"), np.zeros((4, 3), "int8"))}
    )
    group = zarr.group()
    group.create_array(
        "cpr_cld_2a_v",
        shape=(4, 3),
        dtype="int8",
        dimension_names=("cpr_cld_2a_along_track", "cpr_cld_2a_x"),
    )
    with pytest.raises(ValueError, match="cpr_cld_2a_v"):
        earthcare.match_stored_rank(ds, group)


def test_the_store_decodes_like_the_source(provider):
    """Fill values and types read back as they do from the source files."""
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
    """A day is recorded complete only once it has settled."""
    day = pd.Timestamp("2026-10-20")
    monkeypatch.setattr(earthcare, "SETTLE_DAYS", 10_000)
    provider.run_partition(day)
    assert provider.missing_timesteps(pd.DatetimeIndex([day])) == [day]
    monkeypatch.setattr(earthcare, "SETTLE_DAYS", -10)
    provider.run_partition(day)
    assert provider.days_complete(CLD) == {"2026-10-20"}
    assert provider.missing_timesteps(pd.DatetimeIndex([day])) == []


def test_a_skipped_day_is_never_fetched_and_counts_as_done(provider, monkeypatch):
    """A day in SKIPPED_DAYS is neither listed nor written, and no lane asks for it again."""
    day = pd.Timestamp("2026-10-20")
    monkeypatch.setitem(earthcare.SKIPPED_DAYS, "CPR_CLD_2A", frozenset({"2026-10-20"}))
    monkeypatch.setattr(earthcare, "search", lambda *a, **k: pytest.fail("searched MAAP"))
    assert not provider.run_partition(day)
    assert not provider.run_partition(day, check_present=False)
    assert provider.missing_timesteps(pd.DatetimeIndex([day])) == []
    assert "2026-10-20" in provider.days_complete(CLD)


def test_the_skipped_atl_days_count_as_done_for_that_product_only(local_config):
    """The real SKIPPED_DAYS: the three ATL_FM__2A days are done, other products still run."""
    atl, cld = PRODUCTS["ATL_FM__2A"], CLD
    days = {"2025-03-13", "2025-03-15", "2025-03-26"}
    assert set(earthcare.SKIPPED_DAYS) <= set(PRODUCTS)
    assert earthcare.SKIPPED_DAYS["ATL_FM__2A"] == days
    assert days <= EarthCAREProvider._days(None, atl)
    assert not days & EarthCAREProvider._days(None, cld)
    made = EarthCAREProvider(config=local_config, products=["ATL_FM__2A", "CPR_CLD_2A"])
    wanted = pd.DatetimeIndex(sorted(days))
    assert made.missing_timesteps(wanted) == list(wanted)
    only_atl = EarthCAREProvider(config=local_config, products=["ATL_FM__2A"])
    assert only_atl.missing_timesteps(wanted) == []


def refusing(fake: FakeMaap, refusals: int):
    """``fake.download``, answering as MAAP does when overloaded ``refusals`` times a frame."""
    calls: collections.Counter = collections.Counter()

    def download(href, dest, **kwargs):
        calls[href] += 1
        if calls[href] <= refusals:
            return None  # download_one's answer to a 404
        return fake.download(href, dest, **kwargs)

    return download, calls


def test_a_catalogued_frame_refused_for_a_while_is_still_written(provider, monkeypatch):
    """A 404 for a listed frame is MAAP overloaded: wait and try again, do not skip it."""
    download, calls = refusing(provider.test_maap, earthcare.MISSING_RETRIES)
    slept = []
    monkeypatch.setattr(earthcare, "download_one", download)
    monkeypatch.setattr(earthcare.time, "sleep", slept.append)
    day = pd.Timestamp("2026-10-20")
    assert provider.run_partition(day)
    ds = stored(provider)
    assert sorted(ds["cpr_cld_2a_frame_key"].values.tolist()) == ["13601E", "13601F"]
    assert set(calls.values()) == {earthcare.MISSING_RETRIES + 1}
    backoff = [earthcare.MISSING_BACKOFF_S * 2**n for n in range(earthcare.MISSING_RETRIES)]
    assert sorted(slept) == sorted(backoff * len(calls))


def test_a_frame_refused_every_time_leaves_its_day_to_a_later_pass(provider, monkeypatch):
    """After the retries the frame is given up for this pass, and the day stays open."""
    download, calls = refusing(provider.test_maap, 10**6)
    monkeypatch.setattr(earthcare, "download_one", download)
    monkeypatch.setattr(earthcare.time, "sleep", lambda s: None)
    monkeypatch.setattr(earthcare, "SETTLE_DAYS", -10)
    day = pd.Timestamp("2026-10-20")
    assert not provider.run_partition(day)
    assert set(calls.values()) == {earthcare.MISSING_RETRIES + 1}
    assert provider.days_complete(CLD) == set()
    assert provider.missing_timesteps(pd.DatetimeIndex([day])) == [day]


def test_a_frame_holding_no_data_is_left_out_and_the_day_stays_open(provider, monkeypatch):
    """The day's other frames are written; the empty one keeps the day from settling."""
    real = earthcare.read_frame

    def read(path, product):
        if "13601F" in path.name:
            raise earthcare.EmptyFrame(f"{product.code}: {path.name} holds no data")
        return real(path, product)

    monkeypatch.setattr(earthcare, "read_frame", read)
    monkeypatch.setattr(earthcare, "SETTLE_DAYS", -10)
    day = pd.Timestamp("2026-10-20")
    assert provider.run_partition(day)
    assert stored(provider)["cpr_cld_2a_frame_key"].values.tolist() == ["13601E"]
    assert provider.days_complete(CLD) == set()
    assert provider.missing_timesteps(pd.DatetimeIndex([day])) == [day]


def test_a_file_with_no_science_data_reads_as_an_empty_frame(tmp_path):
    """Only a file with no dimensions at all is empty; others still fail loudly."""
    import h5py

    path = tmp_path / "ECA_EXBA_ATL_ICE_2A_20250528T000000Z_20250528T001000Z_05600A.h5"
    with h5py.File(path, "w") as f:
        f.create_group("ScienceData")
        f.create_group("HeaderData")
    with pytest.raises(earthcare.EmptyFrame):
        earthcare.read_frame(path, earthcare.PRODUCTS["ATL_ICE_2A"])

    # Scalar measurements are data: an unrecognised layout, not an empty frame.
    scalar = tmp_path / "ECA_EXBA_ATL_ICE_2A_20250528T001000Z_20250528T002000Z_05600B.h5"
    with h5py.File(scalar, "w") as f:
        f.create_group("ScienceData").create_dataset("measurement", data=42.0)
        f.create_group("HeaderData")
    with pytest.raises(ValueError, match="no along-track dimension") as raised:
        earthcare.read_frame(scalar, earthcare.PRODUCTS["ATL_ICE_2A"])
    assert not isinstance(raised.value, earthcare.EmptyFrame)
