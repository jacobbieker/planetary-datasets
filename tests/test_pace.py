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


def write_granule(directory: pathlib.Path, name: str, lines: int = 4) -> pathlib.Path:
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
