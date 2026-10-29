"""PACE L2 retrievals into the shared store, offline against synthetic granules."""

from __future__ import annotations

import pathlib

import h5py
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from planetary_datasets.providers.polar import pace
from planetary_datasets.providers.polar.pace import PRODUCTS, PaceL2Provider, read_granule

FILL = np.float32(-32767.0)
LAND = PRODUCTS["spexone_rtap_land"]


def write_granule(
    directory: pathlib.Path, name: str, lines: int = 4, nadir_day: str | None = None
) -> pathlib.Path:
    """A SPEXone L2-shaped granule: geolocation, retrievals, a band table, a name clash."""
    path = directory / name
    geo = xr.Dataset(
        {
            "latitude": (
                ("number_of_lines", "pixels_per_line"),
                np.full((lines, 3), 10.5, "float32"),
            ),
            "quality": (("number_of_lines", "pixels_per_line"), np.ones((lines, 3), "int8")),
        }
    )
    if nadir_day:
        # Seconds of the granule's own day, as PACE stamps them; the last line is fill.
        seconds = np.arange(lines, dtype="float64") + 85000.0
        seconds[-1] = -32767.0
        geo["nadir_view_time"] = ("number_of_lines", seconds)
        geo["nadir_view_time"].attrs.update(
            units=f"seconds since {nadir_day}", valid_min=0.0, valid_max=172800.0
        )
        geo["nadir_view_time"].encoding["_FillValue"] = -32767.0
    aot = np.full((lines, 3, 2), 0.25, dtype="float32")
    aot[0, 0, 0] = FILL
    phys = xr.Dataset(
        {
            # Band-first, as some retrievals are stored.
            "aot": (("wavelength", "number_of_lines", "pixels_per_line"), np.moveaxis(aot, 2, 0)),
            "quality": (("number_of_lines", "pixels_per_line"), np.zeros((lines, 3), "int8")),
        }
    )
    phys["aot"].encoding["_FillValue"] = FILL
    bands = xr.Dataset({"wavelength_value": ("wavelength", np.array([440.0, 550.0], "float32"))})
    geo.to_netcdf(path, group="geolocation_data", engine="h5netcdf")
    phys.to_netcdf(path, group="geophysical_data", engine="h5netcdf", mode="a")
    bands.to_netcdf(path, group="sensor_band_parameters", engine="h5netcdf", mode="a")
    with h5py.File(path, "a") as f:
        f.attrs["time_coverage_start"] = "2026-08-20T00:29:57Z"
        f.attrs["time_coverage_end"] = "2026-08-20T00:34:57Z"
    return path


def test_read_granule_flattens_groups_and_keeps_raw_values(tmp_path):
    ds = read_granule(
        write_granule(tmp_path, "PACE_SPEXONE.20260820T002957.L2.RTAP_LAND.V4_0.nc"), LAND
    )
    p = "spexone_rtap_land"
    # The track moves first; the other dimensions keep their order.
    assert ds[f"{p}_aot"].dims == (f"{p}_along_track", f"{p}_wavelength", f"{p}_pixels_per_line")
    assert ds[f"{p}_aot"].values[0, 0, 0] == FILL
    assert ds[f"{p}_aot"].attrs["_FillValue"] == FILL
    # A name used in two groups takes its group as a further prefix.
    assert f"{p}_geolocation_data_quality" in ds and f"{p}_geophysical_data_quality" in ds
    assert ds[f"{p}_wavelength_value"].dims == (f"{p}_granule", f"{p}_wavelength")
    assert ds[f"{p}_time_coverage_start"].values.tolist() == ["2026-08-20T00:29:57Z"]
    assert ds[f"{p}_granule_name"].values.tolist() == [
        "PACE_SPEXONE.20260820T002957.L2.RTAP_LAND.V4_0.nc"
    ]


@pytest.fixture
def provider(local_config, monkeypatch, tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    names = [
        "PACE_SPEXONE.20260820T002957.L2.RTAP_LAND.V4_0.nc",
        "PACE_SPEXONE.20260820T003457.L2.RTAP_LAND.V4_0.nc",
    ]
    files = {n: write_granule(source, n, lines=3 + i) for i, n in enumerate(names)}

    def fake_granules(short_name, version, start, end, session=None):
        for name in files:
            yield {"links": [{"href": f"s3://ob-cumulus-prod-public/{name}"}]}

    class FakeFS:
        def get(self, url, dest):
            pathlib.Path(dest).write_bytes(files[url.rsplit("/", 1)[1]].read_bytes())

    monkeypatch.setattr(pace, "granules", fake_granules)
    made = PaceL2Provider(config=local_config, products=["spexone_rtap_land"], download_workers=2)
    made._s3 = type("S3", (), {"filesystem": lambda self: FakeFS()})()
    return made


def stored(provider, **kwargs) -> xr.Dataset:
    repo = provider.get_icechunk_repo()
    return xr.open_zarr(repo.readonly_session("main").store, consolidated=False, **kwargs)


def test_a_day_is_written_once(provider):
    day = pd.Timestamp("2026-08-20")
    assert provider.run_partition(day)
    ds = stored(provider)
    assert ds.sizes["spexone_rtap_land_along_track"] == 7
    assert ds["spexone_rtap_land_granule_index"].values.tolist() == [0, 0, 0, 1, 1, 1, 1]
    assert not provider.run_partition(day)
    assert stored(provider).sizes["spexone_rtap_land_along_track"] == 7


def test_fill_values_decode_and_raw_values_survive(provider):
    provider.run_partition(pd.Timestamp("2026-08-20"))
    assert np.isnan(stored(provider)["spexone_rtap_land_aot"].values[0, 0, 0])
    raw = stored(provider, mask_and_scale=False)
    assert raw["spexone_rtap_land_aot"].values[0, 0, 0] == FILL
    assert raw["spexone_rtap_land_geophysical_data_quality"].dtype == np.int8


def test_a_settled_day_is_recorded_complete(provider, monkeypatch):
    day = pd.Timestamp("2026-08-20")
    monkeypatch.setattr(pace, "SETTLE_DAYS", 10_000)
    provider.run_partition(day)
    assert provider.missing_timesteps(pd.DatetimeIndex([day])) == [day]
    monkeypatch.setattr(pace, "SETTLE_DAYS", -10)
    provider.run_partition(day)
    assert provider.missing_timesteps(pd.DatetimeIndex([day])) == []


def test_cmr_5xx_is_retried(monkeypatch):
    from planetary_datasets.common import earthdata

    calls = []

    class Response:
        def __init__(self, status):
            self.status_code = status
            self.headers = {}

        def raise_for_status(self):
            pass

        def json(self):
            return {"feed": {"entry": [{"time_start": "2026-08-20T00:30:00Z", "links": []}]}}

    class Session:
        def get(self, *args, **kwargs):
            calls.append(1)
            return Response(502 if len(calls) == 1 else 200)

    monkeypatch.setattr(earthdata.time, "sleep", lambda s: None)
    found = list(
        earthdata.granules(
            "X", "1", pd.Timestamp("2026-08-20"), pd.Timestamp("2026-08-21"), Session()
        )
    )
    assert len(found) == 1 and len(calls) == 2


def test_day_relative_times_are_rebased_to_a_fixed_epoch(tmp_path):
    path = write_granule(
        tmp_path, "PACE_SPEXONE.20260820T002957.L2.RTAP_LAND.V4_0.nc", 3, nadir_day="2026-08-20"
    )
    ds = read_granule(path, LAND)
    times = ds["spexone_rtap_land_nadir_view_time"]
    assert times.attrs["units"] == "seconds since 1970-01-01"
    assert "valid_max" not in times.attrs
    day = (pd.Timestamp("2026-08-20") - pd.Timestamp("1970-01-01")).total_seconds()
    assert times.values.tolist() == [day + 85000.0, day + 85001.0, -32767.0]
    decoded = xr.decode_cf(ds[["spexone_rtap_land_nadir_view_time"]])
    first = decoded["spexone_rtap_land_nadir_view_time"].values[0]
    assert first == np.datetime64("2026-08-20T23:36:40")


def test_the_repair_rebases_stored_rows_by_their_granules_day(tmp_path, monkeypatch):
    import importlib.util

    import icechunk
    import zarr

    spec = importlib.util.spec_from_file_location(
        "repair_pace_day_seconds",
        pathlib.Path(__file__).resolve().parents[1] / "scripts/repair_pace_day_seconds.py",
    )
    tool = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(tool)
    repo = icechunk.Repository.create(icechunk.local_filesystem_storage(str(tmp_path / "s")))

    class Config:
        def icechunk_repo(self, prefix):
            return repo

    monkeypatch.setattr(tool, "get_config", lambda: Config())
    session = repo.writable_session("main")
    root = zarr.open_group(session.store, mode="a")
    p = "spexone_l1c"
    stale = root.create_array(
        f"{p}_bin_was_viewed_at_nadir_view",
        shape=(5,),
        dtype="float64",
        dimension_names=(f"{p}_along_track",),
        attributes={
            "units": "seconds since 2026-10-29",
            "valid_max": 172800.0,
            "_FillValue": -32767.0,
            "source_name": f"{p}_nadir_view_time",
        },
    )
    day2 = (pd.Timestamp("2026-10-28") - pd.Timestamp("1970-01-01")).total_seconds()
    # Two rows of the 29th, one of the 28th still day-relative, a fill, and a row a fixed
    # writer already rebased.
    stale[:] = [100.0, 200.0, 86500.0, -32767.0, day2 + 300.0]
    index = root.create_array(
        f"{p}_granule_index", shape=(5,), dtype="int32", dimension_names=(f"{p}_along_track",)
    )
    index[:] = [0, 0, 1, 1, 1]
    starts = root.create_array(
        f"{p}_time_coverage_start", shape=(2,), dtype=str, dimension_names=(f"{p}_granule",)
    )
    starts[:] = np.array(["2026-10-29T00:01:52Z", "2026-10-28T23:56:52Z"], dtype=object)
    session.commit("stale")

    tool.repair("bkr/polar/pace_l2.icechunk")
    root = zarr.open_group(repo.readonly_session("main").store, mode="r")
    assert f"{p}_bin_was_viewed_at_nadir_view" not in root
    fixed = root[f"{p}_nadir_view_time"]
    day1 = (pd.Timestamp("2026-10-29") - pd.Timestamp("1970-01-01")).total_seconds()
    assert fixed[:].tolist() == [day1 + 100, day1 + 200, day2 + 86500, -32767.0, day2 + 300]
    assert fixed.attrs["units"] == "seconds since 1970-01-01"
    assert "valid_max" not in fixed.attrs and "source_name" not in fixed.attrs
    assert tool.repair("bkr/polar/pace_l2.icechunk") == []
