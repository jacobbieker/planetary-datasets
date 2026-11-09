"""Copying JPSS history from the old per-product stores into the one-store groups.

The old stores are rebuilt here in the layouts the old code wrote: the combined ATMS store
of the pre-provider script (``time`` at nanoseconds, start/end as xarray chose), and the
per-spacecraft rolling stores of the old providers (``time`` rounded to the second,
start/end in microseconds, the source packing). Every store is local.
"""

from __future__ import annotations

import importlib.util
import pathlib
import sys

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from icechunk.xarray import to_icechunk
from test_jpss_rolling import (  # noqa: F401 - small_granules is a fixture
    MIRS_NAME,
    small_granules,
    write_granule,
    write_mirs,
)

from planetary_datasets.common.store import write_to_icechunk
from planetary_datasets.providers.polar import jpss_viirs
from planetary_datasets.providers.polar._jpss import GRANULE_TIME_ENCODING, NDE_SPACECRAFT
from planetary_datasets.providers.polar.jpss_atms import JpssAtmsProvider
from planetary_datasets.providers.polar.jpss_atms_l2 import JpssMirsImgProvider
from planetary_datasets.providers.polar.jpss_viirs import JpssViirsMbandProvider

spec = importlib.util.spec_from_file_location(
    "copy_jpss_history",
    pathlib.Path(__file__).resolve().parents[1] / "scripts/copy_jpss_history.py",
)
tool = importlib.util.module_from_spec(spec)
# dataclasses look their module up in sys.modules.
sys.modules[spec.name] = tool
spec.loader.exec_module(tool)

HOUR = pd.Timestamp("2026-07-20T12:00")


def mirs_name(satellite: str, start: pd.Timestamp) -> str:
    """An NDE MiRS filename for a 31.6 s granule starting at ``start``."""
    end = start + pd.Timedelta(31.6, "s")

    def stamp(t):
        return f"{t:%Y%m%d%H%M%S}{t.microsecond // 100000}"

    return MIRS_NAME.replace("_n20_", f"_{NDE_SPACECRAFT[satellite]}_").replace(
        "s202610151159550_e202610151200266", f"s{stamp(start)}_e{stamp(end)}"
    )


def to_old_layout(ds: xr.Dataset) -> xr.Dataset:
    """A new-layout dataset as the old providers wrote it: one row per ``time``."""
    times = pd.DatetimeIndex(ds["time"].values).round("s")
    ds = ds.drop_vars(["time", "granule"]).rename_dims({"granule": "time"})
    return ds.assign_coords(time=times)


def old_overrides(provider, ds: xr.Dataset) -> dict:
    """The encoding the old provider gave a store: its packing and chunks, by name."""
    out = provider.encoding_for(ds)
    out.pop("time", None)
    for name in ("start_time", "end_time"):
        out[name] = dict(GRANULE_TIME_ENCODING)
    return out


def write_old_mirs(config, tmp_path, satellite: str, starts, renamed: bool = False) -> None:
    """A per-spacecraft ``jpss_mirs_img_<sat>`` store as the old provider wrote it.

    With ``renamed``, as ``scripts/rename_store_variables.py`` left it: readable names
    carrying ``source_name``, packing unchanged.
    """
    from planetary_datasets.common import naming

    provider = JpssMirsImgProvider(config=config)
    files = [write_mirs(tmp_path, mirs_name(satellite, s)) for s in starts]
    hour = pd.Timestamp(starts[0]).floor("h")
    ds = provider.process(files, hour)
    old = to_old_layout(ds)
    old.attrs["platform"] = str(ds["platform_name"].values[0])
    prefix = f"bkr/polar/jpss_mirs_img_{satellite}.icechunk"
    overrides = old_overrides(provider, ds)
    if renamed:
        names = naming.store_names(prefix)
        old = naming.prepare_for_store(old, prefix)
        overrides = {names.get(k, k): v for k, v in overrides.items()}
    repo = config.icechunk_repo(prefix)
    assert write_to_icechunk(repo, old, encoding_overrides=overrides)


def stored(provider) -> xr.Dataset:
    return xr.open_zarr(
        provider.get_icechunk_repo().readonly_session("main").store,
        group=provider.group,
        consolidated=False,
    )


def snapshots(provider) -> int:
    return len(list(provider.get_icechunk_repo().ancestry(branch="main")))


# --------------------------------------------------------------------- rolling L2


@pytest.fixture
def old_mirs(local_config, tmp_path):
    """Three spacecraft, two hours; NOAA-20 and NOAA-21 start in the same 0.1 s."""
    for satellite, offset in (("n20", 0.0), ("n21", 0.04), ("snpp", 600.0)):
        starts = [
            HOUR + pd.Timedelta(minutes, "min") + pd.Timedelta(offset, "s") for minutes in (10, 70)
        ]
        for start in starts:
            write_old_mirs(local_config, tmp_path / satellite, satellite, [start])
    return local_config


@pytest.fixture(autouse=True)
def _dirs(tmp_path):
    for satellite in ("n20", "n21", "snpp"):
        (tmp_path / satellite).mkdir()


def test_rolling_rows_come_through_with_ids_platforms_and_values(old_mirs):
    copier = tool.Copier("mirs_img", end=HOUR + pd.Timedelta(3, "h"))
    totals = copier.run()
    assert totals.copied == 6

    ds = stored(JpssMirsImgProvider())
    assert ds.sizes["granule"] == 6
    ids = [str(g) for g in ds["granule"].values]
    assert ids[:3] == [
        "2026-07-20T12:10:00.0_n20",
        "2026-07-20T12:10:00.0_n21",
        "2026-07-20T12:20:00.0_snpp",
    ]
    assert [str(p) for p in ds["platform_name"].values[:3]] == [
        "NOAA-20",
        "NOAA-21",
        "Suomi-NPP",
    ]
    # time is the unrounded middle again, as the lane writes it.
    assert ds["time"].values[0] == np.datetime64("2026-07-20T12:10:15.800")
    np.testing.assert_allclose(ds["TPW"].values, 12.3, atol=0.05)
    np.testing.assert_allclose(ds["BT"].values[:, 1:], 250.0, atol=0.0075)
    assert {"Freq", "Polo"} <= set(ds.coords)
    assert "platform" not in ds.attrs


def test_rolling_copy_is_idempotent(old_mirs):
    end = HOUR + pd.Timedelta(3, "h")
    tool.Copier("mirs_img", end=end).run()
    before = snapshots(JpssMirsImgProvider())
    again = tool.Copier("mirs_img", end=end).run()
    assert again.copied == 0 and again.present == 6
    assert snapshots(JpssMirsImgProvider()) == before
    assert stored(JpssMirsImgProvider()).sizes["granule"] == 6


def test_granules_the_lane_wrote_are_skipped(old_mirs, tmp_path):
    lane = JpssMirsImgProvider()
    later = HOUR + pd.Timedelta(1, "h")
    files = [
        write_mirs(tmp_path, mirs_name("n20", later + pd.Timedelta(10, "min"))),
        write_mirs(
            tmp_path, mirs_name("n21", later + pd.Timedelta(10, "min") + pd.Timedelta(0.04, "s"))
        ),
    ]
    assert lane.write_to_icechunk(lane.get_icechunk_repo(), lane.process(files, later))

    # The default end is where the group starts; take everything instead.
    totals = tool.Copier("mirs_img", end=HOUR + pd.Timedelta(3, "h")).run()
    assert totals.present == 2 and totals.copied == 4
    ids = [str(g) for g in stored(lane)["granule"].values]
    assert len(ids) == len(set(ids)) == 6


def test_default_end_is_where_the_group_starts(old_mirs, tmp_path):
    lane = JpssMirsImgProvider()
    later = HOUR + pd.Timedelta(1, "h")
    files = [write_mirs(tmp_path, mirs_name("n20", later + pd.Timedelta(10, "min")))]
    assert lane.write_to_icechunk(lane.get_icechunk_repo(), lane.process(files, later))
    copier = tool.Copier("mirs_img")
    assert copier.end == pd.Timestamp("2026-07-20T13:10:15.8")
    assert copier.run().copied == 3


def test_time_budget_stops_between_commits_and_a_rerun_resumes(old_mirs, tmp_path):
    """Stopped by its budget and re-run with the default end, the copy finishes."""
    lane = JpssMirsImgProvider()
    later = HOUR + pd.Timedelta(3, "h")
    files = [write_mirs(tmp_path, mirs_name("n20", later + pd.Timedelta(10, "min")))]
    assert lane.write_to_icechunk(lane.get_icechunk_repo(), lane.process(files, later))

    class Clock:
        now = 0.0

        def __call__(self):
            return self.now

    clock = Clock()
    copier = tool.Copier("mirs_img", slice_hours=1, deadline=150, clock=clock)
    write = copier.write_slice

    def slow_write(ds):
        write(ds)
        clock.now += 100

    copier.write_slice = slow_write
    before = snapshots(JpssMirsImgProvider())
    totals = copier.run()
    # The 12:00 slice took 100 s; another would end past the 150 s budget.
    assert totals.slices == 1 and totals.copied == 3
    assert snapshots(JpssMirsImgProvider()) == before + 1
    # The next run, with the default end again, carries on where this one stopped: the
    # end is where the lane's rows start, not the earliest row the copy wrote.
    resumed = tool.Copier("mirs_img")
    assert resumed.end == pd.Timestamp("2026-07-20T15:10:15.8")
    assert resumed.run().copied == 3
    assert tool.Copier("mirs_img").run().copied == 0
    assert stored(lane).sizes["granule"] == 7


def test_an_empty_group_needs_an_end(old_mirs):
    """With nothing in the group and no tag, there is no end to default to."""
    with pytest.raises(tool.MigrationError, match="give --end"):
        tool.Copier("mirs_img")


def test_the_boundary_is_recorded_once_and_not_by_a_dry_run(old_mirs, tmp_path):
    """The lane-start tag is written by the first writing run only."""
    lane = JpssMirsImgProvider()
    later = HOUR + pd.Timedelta(3, "h")
    files = [write_mirs(tmp_path, mirs_name("n20", later + pd.Timedelta(10, "min")))]
    assert lane.write_to_icechunk(lane.get_icechunk_repo(), lane.process(files, later))
    tool.Copier("mirs_img").run(dry_run=True)
    assert lane.get_icechunk_repo().list_tags() == set()
    tool.Copier("mirs_img").run()
    tags = lane.get_icechunk_repo().list_tags()
    assert tags == {"copy-jpss-history.mirs_img.end.20260720T151015.800000"}
    tool.Copier("mirs_img").run()
    assert lane.get_icechunk_repo().list_tags() == tags


def test_dry_run_counts_and_writes_nothing(old_mirs):
    totals = tool.Copier("mirs_img", end=HOUR + pd.Timedelta(3, "h")).run(dry_run=True)
    assert totals.copied == 6 and totals.bytes > 0
    assert not (pathlib.Path(old_mirs.store_path("bkr/polar/jpss.icechunk")) / "mirs_img").exists()


def test_a_store_packed_differently_is_refused(local_config, tmp_path):
    write_old_mirs(local_config, tmp_path / "n20", "n20", [HOUR + pd.Timedelta(10, "min")])
    provider = JpssMirsImgProvider(config=local_config)
    later = HOUR + pd.Timedelta(1, "h")
    files = [write_mirs(tmp_path, mirs_name("n21", later), bt_scale=0.02)]
    assert provider.write_to_icechunk(provider.get_icechunk_repo(), provider.process(files, later))
    with pytest.raises(tool.MigrationError, match="re-quantise"):
        tool.Copier("mirs_img", end=later).run()


def test_old_stores_are_not_modified(old_mirs):
    repo = old_mirs.icechunk_repo("bkr/polar/jpss_mirs_img_n20.icechunk")
    before = [s.id for s in repo.ancestry(branch="main")]
    tool.Copier("mirs_img", end=HOUR + pd.Timedelta(3, "h")).run()
    assert [s.id for s in repo.ancestry(branch="main")] == before


@pytest.mark.store_names
def test_renamed_old_stores_map_back_to_source_names(local_config, tmp_path):
    """The old stores were renamed in place; the copy still writes the group's names."""
    from planetary_datasets.common import naming

    write_old_mirs(
        local_config, tmp_path / "n20", "n20", [HOUR + pd.Timedelta(10, "min")], renamed=True
    )
    old = xr.open_zarr(
        local_config.icechunk_repo("bkr/polar/jpss_mirs_img_n20.icechunk")
        .readonly_session("main")
        .store,
        consolidated=False,
    )
    assert "channel_temperature" in old and old["channel_temperature"].attrs["source_name"] == "BT"

    tool.Copier("mirs_img", end=HOUR + pd.Timedelta(1, "h")).run()
    group = stored(JpssMirsImgProvider())
    assert "channel_temperature" in group and "BT" not in group
    assert group["channel_temperature"].encoding["dtype"] == np.int16
    assert "channel" in group.dims
    assert naming.store_names("bkr/polar/jpss.icechunk", "mirs_img")["BT"] == "channel_temperature"


def test_satellite_names():
    assert tool.satellite_of("NOAA-21") == "n21"
    assert tool.satellite_of("Suomi NPP") == "snpp"
    assert tool.satellite_of("", hint="n20") == "n20"
    with pytest.raises(tool.MigrationError):
        tool.satellite_of("MetOp-B")
    with pytest.raises(tool.MigrationError):
        tool.satellite_of("NOAA-20", hint="n21")


# --------------------------------------------------------------------- VIIRS


def test_viirs_rows_keep_their_packing(local_config, small_granules, tmp_path):  # noqa: F811
    provider = JpssViirsMbandProvider(config=local_config)
    start = HOUR + pd.Timedelta(10, "min")
    for sat, code in (("n20", "j01"), ("n21", "j02")):
        files = write_granule(tmp_path / sat, start, sat=code)
        ds = provider.process(files, HOUR)
        repo = local_config.icechunk_repo(f"bkr/polar/jpss_viirs_mband_{sat}.icechunk")
        assert write_to_icechunk(
            repo, to_old_layout(ds), encoding_overrides=old_overrides(provider, ds)
        )

    assert tool.Copier("viirs_mband", end=HOUR + pd.Timedelta(1, "h")).run().copied == 2
    out = stored(JpssViirsMbandProvider())
    assert [str(g) for g in out["granule"].values] == [
        "2026-07-20T12:10:00.0_n20",
        "2026-07-20T12:10:00.0_n21",
    ]
    assert out["M15_brightness_temperature"].encoding["dtype"] == np.uint16
    assert out["M15_brightness_temperature"].encoding["chunks"] == (1, *jpss_viirs.GRANULE_SHAPE)
    # The packed counts are copied exactly.
    import zarr

    old = zarr.open_group(repo.readonly_session("main").store, mode="r")
    new = zarr.open_group(
        JpssViirsMbandProvider().get_icechunk_repo().readonly_session("main").store, mode="r"
    )
    np.testing.assert_array_equal(
        new["viirs_mband/M15_brightness_temperature"][1], old["M15_brightness_temperature"][0]
    )
    assert "bitround_keepbits" not in new["viirs_mband/M15_brightness_temperature"].attrs


# --------------------------------------------------------------------- ATMS


def write_old_atms(config, rows) -> None:
    """The combined ATMS store as the pre-provider script wrote it."""
    starts = [pd.Timestamp(s) for s, _ in rows]
    ends = [s + pd.Timedelta(31.6, "s") for s in starts]
    mids = [s + (e - s) / 2 for s, e in zip(starts, ends)]
    n = len(rows)
    ds = xr.Dataset(
        {
            "1": (("time", "y", "x"), np.arange(n * 6, dtype=np.float16).reshape(n, 2, 3)),
            "latitude": (("time", "y", "x"), np.full((n, 2, 3), 10.5, dtype=np.float32)),
            "start_time": ("time", np.array(starts, dtype="datetime64[ns]")),
            "end_time": ("time", np.array(ends, dtype="datetime64[ns]")),
            "platform_name": ("time", np.array([p for _, p in rows])),
        },
        coords={"time": np.array(mids, dtype="datetime64[ns]")},
        attrs={"platform_name": "NOAA-20", "sensor": "atms"},
    ).sortby("time")
    session = config.icechunk_repo("bkr/polar/jpss_atms.icechunk").writable_session("main")
    encoding = {"time": {"units": "nanoseconds since 2020-01-01", "dtype": "int64"}}
    to_icechunk(ds.chunk({"time": 1}), session, encoding=encoding)
    session.commit("old ATMS")


def test_atms_combined_store_is_split_by_platform(local_config):
    write_old_atms(
        local_config,
        [
            ("2023-03-01T00:00:24.4", "NOAA-20"),
            ("2023-03-01T00:00:24.43", "NOAA-21"),
            ("2023-03-01T00:30:00.0", "Suomi-NPP"),
            ("2025-08-01T00:10:00.0", "NOAA-20"),  # the lane's from here on
        ],
    )
    totals = tool.Copier("atms").run()
    assert totals.copied == 3
    out = stored(JpssAtmsProvider())
    assert [str(g) for g in out["granule"].values] == [
        "2023-03-01T00:00:24.4_n20",
        "2023-03-01T00:00:24.4_n21",
        "2023-03-01T00:30:00.0_snpp",
    ]
    assert out["1"].dtype == np.float16
    np.testing.assert_array_equal(out["1"].values[2], np.arange(12, 18).reshape(2, 3))
    assert out["time"].values[0] == np.datetime64("2023-03-01T00:00:40.2")
    assert "platform_name" not in out.attrs and out.attrs["sensor"] == "atms"
    assert tool.Copier("atms").run().copied == 0


def test_command_line_dry_run_and_budget(old_mirs):
    end = (HOUR + pd.Timedelta(3, "h")).isoformat()
    assert tool.main(["--product", "mirs_img", "--end", end, "--dry-run", "--threads", "2"]) == 0
    # A budget already spent stops before the first slice.
    assert tool.main(["--product", "mirs_img", "--end", end, "--max-hours", "0"]) == 0
    assert not (pathlib.Path(old_mirs.store_path("bkr/polar/jpss.icechunk")) / "mirs_img").exists()
    assert tool.main(["--product", "mirs_img", "--end", end]) == 0
    assert stored(JpssMirsImgProvider()).sizes["granule"] == 6


def test_an_explicit_end_still_records_the_lane_start(old_mirs, tmp_path):
    """A first run given --end must not leave a later default-end run with nothing to do."""
    lane = JpssMirsImgProvider()
    later = HOUR + pd.Timedelta(3, "h")
    files = [write_mirs(tmp_path, mirs_name("n20", later + pd.Timedelta(10, "min")))]
    assert lane.write_to_icechunk(lane.get_icechunk_repo(), lane.process(files, later))
    assert tool.Copier("mirs_img", end=HOUR + pd.Timedelta(1, "h")).run().copied == 3
    resumed = tool.Copier("mirs_img")
    assert resumed.end == pd.Timestamp("2026-07-20T15:10:15.8")
    assert resumed.run().copied == 3


def test_a_group_the_copy_creates_needs_an_end_ever_after(old_mirs):
    """With no lane start known, a later run refuses to guess one from copied rows."""
    first_end = HOUR + pd.Timedelta(1, "h")
    assert tool.Copier("mirs_img", end=first_end).run().copied == 3
    with pytest.raises(tool.MigrationError, match="created this group"):
        tool.Copier("mirs_img")
    assert tool.Copier("mirs_img", end=HOUR + pd.Timedelta(3, "h")).run().copied == 3


def test_a_missing_old_store_is_skipped_but_a_broken_one_raises(old_mirs, monkeypatch):
    """Only a store confirmed absent is skipped."""
    import shutil

    shutil.rmtree(old_mirs.store_path("bkr/polar/jpss_mirs_img_snpp.icechunk"))
    copier = tool.Copier("mirs_img", end=HOUR + pd.Timedelta(3, "h"))
    assert [o.satellite for o in copier.olds] == ["n20", "n21"]

    def broken(storage):
        raise tool.icechunk.IcechunkError("storage unavailable")

    monkeypatch.setattr(tool.icechunk.Repository, "open", broken)
    with pytest.raises(tool.icechunk.IcechunkError):
        tool.Copier("mirs_img", end=HOUR + pd.Timedelta(3, "h"))


def test_a_run_that_writes_nothing_records_nothing(old_mirs):
    """A run stopped before its first slice leaves the group, and its tags, alone."""
    end = HOUR + pd.Timedelta(1, "h")
    assert tool.Copier("mirs_img", end=end, deadline=-1.0).run().copied == 0
    assert JpssMirsImgProvider().get_icechunk_repo().list_tags() == set()
