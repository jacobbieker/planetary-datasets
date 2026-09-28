"""Offline tests for the C3S seasonal forecast provider."""

from __future__ import annotations

import zipfile

import numpy as np
import pandas as pd
import pytest
import xarray as xr

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
    ],
)
def test_system_for_transitions(centre, when, expected):
    assert sf.system_for(centre, pd.Timestamp(when)) == expected


def test_system_for_refuses_dates_before_the_first_known_system():
    with pytest.raises(ValueError, match="Pass system= explicitly"):
        sf.system_for("ukmo", pd.Timestamp("2019-01-01"))


def test_system_for_rejects_unknown_centre():
    with pytest.raises(ValueError, match="unknown originating centre"):
        sf.system_for("meteofrance", pd.Timestamp("2024-01-01"))


def test_system_for_accepts_aware_timestamps():
    aware = pd.Timestamp("2022-11-01T00:00", tz="UTC")
    assert sf.system_for("ecmwf", aware) == "51"


def test_provider_rejects_unknown_centre():
    with pytest.raises(ValueError, match="unknown originating centre"):
        sf.SeasonalForecastProvider(centre="nope")


def test_build_request(local_config):
    provider = sf.SeasonalForecastProvider(
        centre="ncep", leadtime_hours=(24, 48), config=local_config
    )
    request = provider.build_request(pd.Timestamp("2024-03-01"))
    assert request["originating_centre"] == "ncep"
    assert request["system"] == "2"
    assert request["year"] == ["2024"]
    assert request["month"] == ["03"]
    assert request["day"] == ["01"]
    assert request["leadtime_hour"] == ["24", "48"]
    assert request["data_format"] == "netcdf"


def test_explicit_system_overrides_the_table(local_config):
    provider = sf.SeasonalForecastProvider(centre="ukmo", system="601", config=local_config)
    assert provider.build_request(pd.Timestamp("2019-01-01"))["system"] == "601"


def test_store_prefix_is_per_centre(local_config):
    ecmwf = sf.SeasonalForecastProvider(centre="ecmwf", config=local_config)
    ncep = sf.SeasonalForecastProvider(centre="ncep", config=local_config)
    assert ecmwf.store_prefix != ncep.store_prefix
    assert "ecmwf" in ecmwf.store_prefix


def test_cds_client_raises_a_named_missing_credential(local_config, monkeypatch, tmp_path):
    monkeypatch.setattr(sf, "CDSAPIRC", tmp_path / "absent-cdsapirc")
    with pytest.raises(MissingCredential, match="CDSAPI_URL"):
        sf.cds_client(local_config)


def test_normalise_layout(local_config):
    provider = sf.SeasonalForecastProvider(centre="ncep", config=local_config)
    ds = provider.normalise(cds_like_dataset(), pd.Timestamp("2024-03-01"))

    assert "init_time" in ds.dims
    assert "forecast_reference_time" not in ds.variables
    # valid_time carries its own datetime encoding, which breaks the second append.
    assert "valid_time" not in ds.variables
    assert ds["step"].dtype == np.int32
    assert list(ds["step"].values) == [24, 48, 72]
    # No CF "units": it would make xarray decode step back into a timedelta on read.
    assert "units" not in ds["step"].attrs
    assert ds["step"].attrs["comment"] == "whole hours after init_time"
    assert ds["tp"].dtype == np.float32
    assert ds.attrs["originating_centre"] == "ncep"


def test_normalise_puts_longitude_on_minus_180_to_180(local_config):
    provider = sf.SeasonalForecastProvider(centre="ncep", config=local_config)
    ds = provider.normalise(cds_like_dataset(), pd.Timestamp("2024-03-01"))
    longitudes = ds["longitude"].values
    assert longitudes.min() >= -180 and longitudes.max() <= 180
    assert np.all(np.diff(longitudes) > 0)
    assert np.all(np.diff(ds["latitude"].values) > 0)


def test_normalise_keeps_small_precipitation_values(local_config):
    """float16 would flush accumulations around 1e-4 m towards zero."""
    provider = sf.SeasonalForecastProvider(centre="ncep", config=local_config)
    ds = provider.normalise(cds_like_dataset(), pd.Timestamp("2024-03-01"))
    values = ds["tp"].values
    assert np.count_nonzero(values) == values.size - 1  # only the literal zero is zero


def test_normalise_renames_the_ensemble_dimension(local_config):
    provider = sf.SeasonalForecastProvider(centre="ecmwf", config=local_config)
    ds = provider.normalise(cds_like_dataset(members=4), pd.Timestamp("2024-03-01"))
    assert "ensemble_member" in ds.dims
    assert ds.sizes["ensemble_member"] == 4


def test_normalise_renames_a_dimension_with_no_coordinate(local_config):
    """A bare dim still has to be renamed, or the ensemble-size guard checks nothing."""
    provider = sf.SeasonalForecastProvider(centre="ecmwf", config=local_config)
    bare = cds_like_dataset(members=4).drop_vars("number")
    assert "number" in bare.dims and "number" not in bare.coords
    ds = provider.normalise(bare, pd.Timestamp("2024-03-01"))
    assert "ensemble_member" in ds.dims
    # And it is given a coordinate, because write_to_icechunk indexes it by name.
    assert "ensemble_member" in ds.coords
    assert list(ds.ensemble_member.values) == [0, 1, 2, 3]


def test_append_is_refused_when_the_ensemble_size_changes(tmp_path, local_config):
    provider = sf.SeasonalForecastProvider(centre="ecmwf", config=local_config)
    repo = provider.get_icechunk_repo()
    first = provider.normalise(cds_like_dataset("2024-03-01", members=4), pd.Timestamp("2024-03-01"))
    assert provider.write_to_icechunk(repo, first) is True
    second = provider.normalise(cds_like_dataset("2024-04-01", members=2), pd.Timestamp("2024-04-01"))
    assert provider.write_to_icechunk(repo, second) is False


def test_process_and_append_round_trip(tmp_path, local_config):
    provider = sf.SeasonalForecastProvider(
        centre="ncep", leadtime_hours=(24, 48, 72), config=local_config
    )
    for init in ("2024-03-01", "2024-04-01"):
        path = tmp_path / f"ncep_{init}.nc"
        cds_like_dataset(init).to_netcdf(path)
        processed = provider.process([str(path)], pd.Timestamp(init))
        assert provider.write_to_icechunk(provider.get_icechunk_repo(), processed) is True

    stored = xr.open_zarr(
        provider.get_icechunk_repo().readonly_session("main").store, consolidated=False
    )
    assert list(pd.DatetimeIndex(stored.init_time.values)) == [
        pd.Timestamp("2024-03-01"),
        pd.Timestamp("2024-04-01"),
    ]
    assert list(stored.step.values) == [24, 48, 72]


def test_append_is_refused_when_the_lead_times_change(tmp_path, local_config):
    provider = sf.SeasonalForecastProvider(centre="ncep", config=local_config)
    repo = provider.get_icechunk_repo()

    first = tmp_path / "a.nc"
    cds_like_dataset("2024-03-01", steps=3).to_netcdf(first)
    assert provider.write_to_icechunk(repo, provider.process([str(first)], pd.Timestamp("2024-03-01")))

    second = tmp_path / "b.nc"
    cds_like_dataset("2024-04-01", steps=2).to_netcdf(second)
    assert (
        provider.write_to_icechunk(repo, provider.process([str(second)], pd.Timestamp("2024-04-01")))
        is False
    )


def test_zipped_cds_response_is_unpacked(tmp_path, local_config):
    member = tmp_path / "inner.nc"
    cds_like_dataset().to_netcdf(member)
    archive = tmp_path / "response.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.write(member, arcname="inner.nc")

    provider = sf.SeasonalForecastProvider(centre="ncep", config=local_config)
    ds = provider.process([str(archive)], pd.Timestamp("2024-03-01"), temp_dir=tmp_path)
    assert "init_time" in ds.dims


def test_zip_without_netcdf_members_raises(tmp_path, local_config):
    archive = tmp_path / "empty.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("readme.txt", "nothing useful")
    with pytest.raises(RuntimeError, match="no NetCDF members"):
        sf._extract_if_zip(archive, tmp_path)


def _recording_client(monkeypatch, tmp_path):
    """Stand in for the CDS: write a byte to the target and remember the request."""
    seen = []

    class _Client:
        def retrieve(self, dataset, request, target):
            seen.append(request)
            open(target, "wb").write(b"x")

    monkeypatch.setattr(sf, "cds_client", lambda config=None: _Client())
    return seen


def test_fetch_reuses_an_already_downloaded_file(tmp_path, local_config, monkeypatch):
    provider = sf.SeasonalForecastProvider(
        centre="ncep", download_dir=tmp_path, config=local_config
    )
    seen = _recording_client(monkeypatch, tmp_path)
    first = provider.fetch(pd.Timestamp("2024-03-01"), temp_dir=tmp_path)
    second = provider.fetch(pd.Timestamp("2024-03-01"), temp_dir=tmp_path)
    assert first == second
    assert len(seen) == 1


def test_fetch_does_not_reuse_a_file_from_a_different_request(tmp_path, local_config, monkeypatch):
    seen = _recording_client(monkeypatch, tmp_path)
    one_var = sf.SeasonalForecastProvider(
        centre="ncep", variables=("total_precipitation",), download_dir=tmp_path, config=local_config
    )
    other_var = sf.SeasonalForecastProvider(
        centre="ncep", variables=("2m_temperature",), download_dir=tmp_path, config=local_config
    )
    assert one_var.fetch(pd.Timestamp("2024-03-01")) != other_var.fetch(pd.Timestamp("2024-03-01"))
    assert len(seen) == 2


def test_fetch_raises_rather_than_retiring_the_partition(tmp_path, local_config, monkeypatch):
    """An empty CDS response is transient; returning [] would retire the partition."""

    class _Client:
        def retrieve(self, dataset, request, target):
            return None

    monkeypatch.setattr(sf, "cds_client", lambda config=None: _Client())
    provider = sf.SeasonalForecastProvider(
        centre="ncep", download_dir=tmp_path, config=local_config
    )
    with pytest.raises(RuntimeError, match="returned no data"):
        provider.fetch(pd.Timestamp("2024-03-01"), temp_dir=tmp_path)
