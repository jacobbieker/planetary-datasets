"""Offline tests for the C3S seasonal forecast provider."""

from __future__ import annotations

import pathlib
import zipfile

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from helpers import read_store
from planetary_datasets.config import MissingCredential
from planetary_datasets.providers import seasonal as sf


def cds_like_dataset(init: str = "2024-03-01", steps: int = 3, members: int = 0) -> xr.Dataset:
    """A miniature of what the CDS hands back for seasonal-original-single-levels."""
    init_times = pd.DatetimeIndex([init])
    periods = pd.to_timedelta(np.arange(1, steps + 1), unit="D")
    latitude = np.array([89.5, 0.5, -89.5])
    longitude = np.array([0.5, 90.5, 200.5, 359.5])

    shape = (len(init_times), steps, len(latitude), len(longitude))
    dims = ("forecast_reference_time", "forecast_period", "latitude", "longitude")
    coords = {
        "forecast_reference_time": init_times,
        "forecast_period": periods,
        "latitude": latitude,
        "longitude": longitude,
        "valid_time": (
            ("forecast_reference_time", "forecast_period"),
            (init_times.values[:, None] + periods.values[None, :]),
        ),
    }
    if members:
        dims = ("number", *dims)
        shape = (members, *shape)
        coords["number"] = np.arange(members)
    else:
        coords["number"] = 0

    data = np.arange(int(np.prod(shape)), dtype="float64").reshape(shape) * 1e-4
    return xr.Dataset({"tp": (dims, data, {"units": "m"})}, coords=coords)


@pytest.fixture
def provider_for(local_config):
    """Build a provider for ``centre`` against the local store."""
    return lambda centre="ncep", **kwargs: sf.SeasonalForecastProvider(centre=centre, config=local_config, **kwargs)


def _processed(provider, tmp_path, init: str, **kwargs) -> xr.Dataset:
    """Write a CDS-like file for ``init`` and run it through ``process``."""
    path = tmp_path / f"{init}.nc"
    cds_like_dataset(init, **kwargs).to_netcdf(path)
    return provider.process([str(path)], pd.Timestamp(init))


@pytest.mark.parametrize(
    ("centre", "when", "expected"),
    [
        ("ncep", "2020-01-01", "2"),
        ("ecmwf", "2022-10-01", "5"),
        ("ecmwf", "2022-11-01", "51"),
        ("ecmwf", "2025-06-01", "51"),
        ("ukmo", "2024-01-01", "603"),
        ("ukmo", "2026-05-01", "610"),
        ("dwd", "2021-01-01", "21"),
        ("dwd", "2025-06-01", "22"),
        ("ecmwf", "2022-11-01T00:00+00:00", "51"),
    ],
)
def test_system_for_transitions(centre, when, expected):
    assert sf.system_for(centre, pd.Timestamp(when)) == expected


@pytest.mark.parametrize(
    ("centre", "when", "match"),
    [
        ("ukmo", "2019-01-01", "Pass system= explicitly"),
        ("meteofrance", "2024-01-01", "unknown originating centre"),
    ],
    ids=["before-first-system", "unknown-centre"],
)
def test_system_for_refuses_what_it_cannot_look_up(centre, when, match):
    with pytest.raises(ValueError, match=match):
        sf.system_for(centre, pd.Timestamp(when))


def test_provider_rejects_unknown_centre():
    with pytest.raises(ValueError, match="unknown originating centre"):
        sf.SeasonalForecastProvider(centre="nope")


def test_build_request(provider_for):
    request = provider_for(leadtime_hours=(24, 48)).build_request(pd.Timestamp("2024-03-01"))
    assert request["originating_centre"] == "ncep"
    assert request["system"] == "2"
    assert request["year"] == ["2024"]
    assert request["month"] == ["03"]
    assert request["day"] == ["01"]
    assert request["leadtime_hour"] == ["24", "48"]
    assert request["data_format"] == "netcdf"


def test_explicit_system_overrides_the_table(provider_for):
    assert provider_for("ukmo", system="601").build_request(pd.Timestamp("2019-01-01"))["system"] == "601"


def test_store_prefix_is_per_centre(provider_for):
    ecmwf, ncep = provider_for("ecmwf"), provider_for("ncep")
    assert ecmwf.store_prefix != ncep.store_prefix
    assert "ecmwf" in ecmwf.store_prefix


def test_cds_client_raises_a_named_missing_credential(local_config, monkeypatch, tmp_path):
    monkeypatch.setattr(sf, "CDSAPIRC", tmp_path / "absent-cdsapirc")
    with pytest.raises(MissingCredential, match="CDSAPI_URL"):
        sf.cds_client(local_config)


def test_normalise_layout(provider_for):
    ds = provider_for().normalise(cds_like_dataset(), pd.Timestamp("2024-03-01"))

    assert "init_time" in ds.dims
    assert "forecast_reference_time" not in ds.variables
    # valid_time carries its own datetime encoding, which breaks the second append.
    assert "valid_time" not in ds.variables
    assert ds["step"].dtype == np.int32
    assert list(ds["step"].values) == [24, 48, 72]
    # No CF "units": it would make xarray decode step back into a timedelta on read.
    assert "units" not in ds["step"].attrs
    assert ds["step"].attrs["comment"] == "whole hours after init_time"
    assert ds.attrs["originating_centre"] == "ncep"
    # Spatial axes are increasing, with longitude on -180..180.
    longitudes = ds["longitude"].values
    assert longitudes.min() >= -180 and longitudes.max() <= 180
    assert np.all(np.diff(longitudes) > 0)
    assert np.all(np.diff(ds["latitude"].values) > 0)
    # float32, because float16 would flush accumulations around 1e-4 m towards zero.
    assert ds["tp"].dtype == np.float32
    assert np.count_nonzero(ds["tp"].values) == ds["tp"].size - 1  # only the literal zero is zero


def test_normalise_renames_the_ensemble_dimension(provider_for):
    ds = provider_for("ecmwf").normalise(cds_like_dataset(members=4), pd.Timestamp("2024-03-01"))
    assert "ensemble_member" in ds.dims
    assert ds.sizes["ensemble_member"] == 4


def test_normalise_renames_a_dimension_with_no_coordinate(provider_for):
    """A bare dim still has to be renamed, or the ensemble-size guard checks nothing."""
    provider = provider_for("ecmwf")
    bare = cds_like_dataset(members=4).drop_vars("number")
    assert "number" in bare.dims and "number" not in bare.coords
    ds = provider.normalise(bare, pd.Timestamp("2024-03-01"))
    assert "ensemble_member" in ds.dims
    # And it is given a coordinate, because write_to_icechunk indexes it by name.
    assert "ensemble_member" in ds.coords
    assert list(ds.ensemble_member.values) == [0, 1, 2, 3]


def test_append_is_refused_when_the_ensemble_size_changes(provider_for):
    provider = provider_for("ecmwf")
    repo = provider.get_icechunk_repo()
    first = provider.normalise(cds_like_dataset("2024-03-01", members=4), pd.Timestamp("2024-03-01"))
    assert provider.write_to_icechunk(repo, first) is True
    second = provider.normalise(cds_like_dataset("2024-04-01", members=2), pd.Timestamp("2024-04-01"))
    assert provider.write_to_icechunk(repo, second) is False


def test_process_and_append_round_trip(tmp_path, provider_for):
    provider = provider_for(leadtime_hours=(24, 48, 72))
    for init in ("2024-03-01", "2024-04-01"):
        assert provider.write_to_icechunk(provider.get_icechunk_repo(), _processed(provider, tmp_path, init)) is True

    stored = read_store(provider)
    assert list(pd.DatetimeIndex(stored.init_time.values)) == [
        pd.Timestamp("2024-03-01"),
        pd.Timestamp("2024-04-01"),
    ]
    assert list(stored.step.values) == [24, 48, 72]


def test_append_is_refused_when_the_lead_times_change(tmp_path, provider_for):
    provider = provider_for()
    repo = provider.get_icechunk_repo()
    assert provider.write_to_icechunk(repo, _processed(provider, tmp_path, "2024-03-01", steps=3)) is True
    assert provider.write_to_icechunk(repo, _processed(provider, tmp_path, "2024-04-01", steps=2)) is False


def test_zipped_cds_response_is_unpacked(tmp_path, provider_for):
    member = tmp_path / "inner.nc"
    cds_like_dataset().to_netcdf(member)
    archive = tmp_path / "response.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.write(member, arcname="inner.nc")

    ds = provider_for().process([str(archive)], pd.Timestamp("2024-03-01"), temp_dir=tmp_path)
    assert "init_time" in ds.dims


def test_zip_without_netcdf_members_raises(tmp_path, local_config):
    archive = tmp_path / "empty.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("readme.txt", "nothing useful")
    with pytest.raises(RuntimeError, match="no NetCDF members"):
        sf._extract_if_zip(archive, tmp_path)


def _stub_cds(monkeypatch, write: bool = True) -> list[dict]:
    """Stand in for the CDS, remembering each request and writing a byte unless ``write`` is off."""
    seen = []

    class _Client:
        def retrieve(self, dataset, request, target):
            seen.append(request)
            if write:
                pathlib.Path(target).write_bytes(b"x")

    monkeypatch.setattr(sf, "cds_client", lambda config=None: _Client())
    return seen


def test_fetch_reuses_an_already_downloaded_file(tmp_path, provider_for, monkeypatch):
    seen = _stub_cds(monkeypatch)
    provider = provider_for(download_dir=tmp_path)
    first = provider.fetch(pd.Timestamp("2024-03-01"), temp_dir=tmp_path)
    second = provider.fetch(pd.Timestamp("2024-03-01"), temp_dir=tmp_path)
    assert first == second
    assert len(seen) == 1


def test_fetch_does_not_reuse_a_file_from_a_different_request(tmp_path, provider_for, monkeypatch):
    seen = _stub_cds(monkeypatch)
    one_var = provider_for(variables=("total_precipitation",), download_dir=tmp_path)
    other_var = provider_for(variables=("2m_temperature",), download_dir=tmp_path)
    assert one_var.fetch(pd.Timestamp("2024-03-01")) != other_var.fetch(pd.Timestamp("2024-03-01"))
    assert len(seen) == 2


def test_fetch_raises_rather_than_retiring_the_partition(tmp_path, provider_for, monkeypatch):
    """An empty CDS response is transient; returning [] would retire the partition."""
    _stub_cds(monkeypatch, write=False)
    with pytest.raises(RuntimeError, match="returned no data"):
        provider_for(download_dir=tmp_path).fetch(pd.Timestamp("2024-03-01"), temp_dir=tmp_path)
