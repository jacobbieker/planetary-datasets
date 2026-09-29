"""GloFAS providers, exercised offline against synthetic CDS archives."""

from __future__ import annotations

import zipfile

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from planetary_datasets import config as config_module
from planetary_datasets.config import MissingCredential
from planetary_datasets.providers import glofas


@pytest.fixture
def no_cdsapirc(monkeypatch, tmp_path):
    """Point the cdsapi configuration lookup at a file that does not exist."""
    monkeypatch.setenv("CDSAPI_RC", str(tmp_path / "absent.cdsapirc"))
    return tmp_path / "absent.cdsapirc"


def make_forecast_nc(path, variable="dis24", init="2026-01-01", steps=(24, 48)):
    """Write a NetCDF shaped like the one CDS packs into a GloFAS forecast zip."""
    ds = xr.Dataset(
        {
            variable: (
                ("time", "step", "latitude", "longitude"),
                np.arange(1 * len(steps) * 3 * 4, dtype="float32").reshape(1, len(steps), 3, 4),
            )
        },
        coords={
            "time": pd.DatetimeIndex([init]),
            "step": pd.to_timedelta(list(steps), unit="h"),
            "latitude": np.array([10.0, 0.0, -10.0]),
            "longitude": np.array([0.0, 90.0, 180.0, 270.0]),
        },
    )
    ds.to_netcdf(path, engine="h5netcdf")
    return path


def make_scalar_step_nc(path, step_hours, variable="dis24", init="2026-01-01", members=None):
    """A NetCDF as CDS writes it when a request pinned a single lead time.

    ``step`` comes back as a scalar coordinate, not a size-1 dimension. ``members`` adds an
    ensemble dimension; leaving it None gives the control-forecast shape, which has no
    ensemble coordinate at all.
    """
    shape = (3, 4) if members is None else (len(members), 3, 4)
    dims = ("latitude", "longitude") if members is None else ("number", "latitude", "longitude")
    ds = xr.Dataset(
        {variable: (dims, np.full(shape, float(step_hours), dtype="float32"))},
        coords={
            "time": pd.Timestamp(init),
            "step": pd.Timedelta(step_hours, "h"),
            "latitude": np.array([10.0, 0.0, -10.0]),
            "longitude": np.array([0.0, 90.0, 180.0, 270.0]),
        },
    )
    if members is not None:
        ds = ds.assign_coords(number=list(members))
    ds.to_netcdf(path, engine="h5netcdf")
    return path


def make_zip(zip_path, nc_path, member="data.nc"):
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.write(nc_path, arcname=member)
    return zip_path


# --- request construction -------------------------------------------------------------


def test_forecast_requests_one_archive_per_variable(local_config):
    provider = glofas.GloFASForecastProvider(config=local_config)
    requests = provider.requests_for(pd.Timestamp("2026-03-04"))

    assert [name for name, _ in requests] == [
        "glofas_forecast_river_discharge_in_the_last_24_hours_20260304.zip",
        "glofas_forecast_runoff_water_equivalent_20260304.zip",
    ]
    first = requests[0][1]
    # The original script asked for every year at once, which returned the wrong days.
    assert first["year"] == ["2026"]
    assert first["month"] == ["03"]
    assert first["day"] == ["04"]
    assert first["leadtime_hour"] == ["24", "48", "72", "96", "120", "144", "168"]
    assert provider.dataset == "cems-glofas-forecast"


def test_reforecast_requests_one_archive_per_leadtime(local_config):
    provider = glofas.GloFASReforecastProvider(config=local_config)
    requests = provider.requests_for(pd.Timestamp("2010-07-01"))

    assert len(requests) == len(glofas.DEFAULT_LEADTIME_HOURS)
    token = glofas.variables_token(provider.variables)
    assert requests[0][0] == f"glofas_reforecast_201007_leadtime_024_{token}.zip"
    request = requests[0][1]
    assert request["hyear"] == ["2010"]
    assert request["hmonth"] == ["07"]
    assert request["hday"] == list(glofas.ALL_DAYS)
    assert request["leadtime_hour"] == ["24"]
    assert request["variable"] == ["river_discharge_in_the_last_24_hours"]


def test_historical_requests_one_archive_per_month(local_config):
    provider = glofas.GloFASHistoricalProvider(config=local_config)
    requests = provider.requests_for(pd.Timestamp("1999-12-01"))

    assert len(requests) == 1
    name, request = requests[0]
    token = glofas.variables_token(provider.variables)
    assert name == f"glofas_historical_199912_{token}.zip"
    assert request["product_type"] == ["consolidated"]
    assert request["hyear"] == ["1999"]
    assert "leadtime_hour" not in request


def test_changing_the_variables_changes_the_archive_name(local_config):
    """retrieve_to skips an existing target, so a stale archive must not be reused."""
    default = glofas.GloFASHistoricalProvider(config=local_config)
    narrowed = glofas.GloFASHistoricalProvider(
        config=local_config, variables=("river_discharge_in_the_last_24_hours",)
    )
    it = pd.Timestamp("2020-05-01")

    assert default.requests_for(it)[0][0] != narrowed.requests_for(it)[0][0]


def test_the_variables_token_ignores_ordering():
    assert glofas.variables_token(["a", "b"]) == glofas.variables_token(["b", "a"])
    assert glofas.variables_token(["a"]) != glofas.variables_token(["a", "b"])


def test_providers_write_to_separate_stores(local_config):
    paths = {
        cls(config=local_config).store_path for cls in glofas.PROVIDERS.values()
    }
    assert len(paths) == len(glofas.PROVIDERS)


def test_archive_dir_is_under_the_configured_data_dir(local_config):
    provider = glofas.GloFASForecastProvider(config=local_config)
    assert provider.archive_dir == local_config.data_dir / "glofas" / "glofas-forecast"


# --- credentials ----------------------------------------------------------------------


def test_cds_client_without_any_credentials_raises(local_config, no_cdsapirc):
    with pytest.raises(MissingCredential) as excinfo:
        glofas.cds_client(local_config)
    assert "CDSAPI_KEY" in str(excinfo.value)


def test_cds_client_prefers_configured_credentials(local_config, monkeypatch, no_cdsapirc):
    """Configured credentials are passed through instead of being left to ~/.cdsapirc."""
    import cdsapi

    monkeypatch.setenv("CDSAPI_URL", "https://cds.example/api")
    monkeypatch.setenv("CDSAPI_KEY", "42:abc123")
    cfg = config_module.load_config()

    built: dict[str, str] = {}
    monkeypatch.setattr(cdsapi, "Client", lambda **kwargs: built.update(kwargs) or "client")

    assert glofas.cds_client(cfg) == "client"
    assert built == {"url": "https://cds.example/api", "key": "42:abc123"}


def test_cds_client_falls_back_to_cdsapirc(local_config, tmp_path, monkeypatch):
    """With no configured key, the client is left to find ~/.cdsapirc itself."""
    import cdsapi

    rc = tmp_path / "cdsapirc"
    rc.write_text("url: https://cds.example/api\nkey: 42:abc123\n")
    monkeypatch.setenv("CDSAPI_RC", str(rc))

    monkeypatch.setattr(cdsapi, "Client", lambda **kwargs: ("client", kwargs))
    assert glofas.cds_client(local_config) == ("client", {})


# --- retrieval ------------------------------------------------------------------------


class FakeCDSClient:
    """Stands in for ``cdsapi.Client``, recording what it was asked for."""

    def __init__(self, fail: bool = False):
        self.calls: list[tuple[str, dict, str]] = []
        self.fail = fail

    def retrieve(self, dataset, request, target):
        self.calls.append((dataset, request, target))
        if self.fail:
            raise RuntimeError("CDS said no")
        with open(target, "wb") as handle:
            handle.write(b"payload")


def test_retrieve_to_writes_the_target(tmp_path):
    client = FakeCDSClient()
    out = glofas.retrieve_to(client, "ds", {}, tmp_path / "a" / "x.zip")

    assert out.read_bytes() == b"payload"
    # cdsapi wrote to the .part file, not the final name.
    assert client.calls[0][2].endswith(".part")


def test_retrieve_to_skips_a_completed_download(tmp_path):
    target = tmp_path / "x.zip"
    target.write_bytes(b"already here")
    client = FakeCDSClient()

    glofas.retrieve_to(client, "ds", {}, target)
    assert client.calls == []
    assert target.read_bytes() == b"already here"


def test_a_failed_retrieval_leaves_nothing_behind(tmp_path):
    """A half-written file must not be mistaken for a finished download next run."""
    target = tmp_path / "x.zip"
    with pytest.raises(RuntimeError):
        glofas.retrieve_to(FakeCDSClient(fail=True), "ds", {}, target)

    assert not target.exists()
    assert list(tmp_path.glob("*.part")) == []


# --- processing -----------------------------------------------------------------------


def test_extract_netcdfs_unpacks_each_archive_separately(tmp_path):
    """CDS calls every member "data.nc", so a shared directory would lose all but one."""
    nc = make_forecast_nc(tmp_path / "src.nc")
    archives = [make_zip(tmp_path / f"a{i}.zip", nc) for i in range(3)]

    members = glofas.extract_netcdfs(archives, tmp_path / "out")
    assert len(members) == 3
    assert len(set(members)) == 3


def test_extract_netcdfs_passes_plain_netcdf_through(tmp_path):
    nc = make_forecast_nc(tmp_path / "src.nc")
    assert glofas.extract_netcdfs([nc], tmp_path / "out") == [str(nc)]


def test_open_members_concatenates_files_with_a_scalar_step(tmp_path):
    """The reforecast asks one lead time per request, so step arrives as a scalar."""
    paths = [
        str(make_scalar_step_nc(tmp_path / f"s{h}.nc", h)) for h in (24, 48, 72)
    ]
    ds = glofas.open_members(paths)

    assert ds.sizes["step"] == 3
    assert pd.TimedeltaIndex(ds.step.values).tolist() == pd.to_timedelta(
        [24, 48, 72], unit="h"
    ).tolist()
    assert float(ds.dis24.sel(step=pd.Timedelta(48, "h")).max()) == 48.0


def test_open_members_joins_the_control_forecast_to_the_perturbed_members(tmp_path):
    """CDS ships them as separate files, and only the perturbed one has an ensemble dim."""
    control = str(make_scalar_step_nc(tmp_path / "control.nc", 24))
    perturbed = str(make_scalar_step_nc(tmp_path / "perturbed.nc", 24, members=[1, 2, 3]))

    ds = glofas.open_members([control, perturbed])

    # The control is ECMWF member 0, so the combined ensemble runs 0..3.
    assert ds.sizes["number"] == 4
    assert ds.number.values.tolist() == [0, 1, 2, 3]


def test_open_members_merges_distinct_variables(tmp_path):
    paths = [
        str(make_scalar_step_nc(tmp_path / "dis.nc", 24, variable="dis24")),
        str(make_scalar_step_nc(tmp_path / "rowe.nc", 24, variable="rowe")),
    ]
    assert set(glofas.open_members(paths).data_vars) == {"dis24", "rowe"}


def test_normalise_glofas_renames_and_sorts_coords():
    ds = xr.Dataset(
        {"dis24": (("lat", "lon"), np.zeros((2, 3), dtype="float32"))},
        coords={"lat": [10.0, -10.0], "lon": [0.0, 200.0, 300.0]},
    )
    out = glofas.normalise_glofas(ds)

    assert "latitude" in out.dims and "longitude" in out.dims
    assert out.latitude.values.tolist() == [-10.0, 10.0]
    assert out.longitude.values.tolist() == [-160.0, -60.0, 0.0]


def test_normalise_glofas_drops_valid_time():
    """valid_time is time + step, and differs per initialisation, so it breaks appends."""
    ds = xr.Dataset(
        {"dis24": ("step", np.zeros(2, dtype="float32"))},
        coords={
            "step": pd.to_timedelta([24, 48], unit="h"),
            "valid_time": ("step", pd.to_datetime(["2026-01-02", "2026-01-03"])),
        },
    )
    assert "valid_time" not in glofas.normalise_glofas(ds).coords


def test_normalise_glofas_keeps_valid_time_when_it_is_the_time_axis():
    """Newer CDS files use valid_time as the time dimension; dropping it loses the data."""
    ds = xr.Dataset(
        {"dis24": ("valid_time", np.zeros(2, dtype="float32"))},
        coords={"valid_time": pd.to_datetime(["2026-01-02", "2026-01-03"])},
    )
    assert "valid_time" in glofas.normalise_glofas(ds).dims


def test_historical_renames_a_valid_time_axis_to_time(local_config):
    provider = glofas.GloFASHistoricalProvider(config=local_config)
    ds = xr.Dataset(
        {"dis24": ("valid_time", np.zeros(2, dtype="float32"))},
        coords={"valid_time": pd.to_datetime(["2026-01-03", "2026-01-02"])},
    )
    out = provider.finalise(ds, pd.Timestamp("2026-01-01"))

    assert "time" in out.dims and "valid_time" not in out.dims
    assert pd.DatetimeIndex(out.time.values).is_monotonic_increasing


class OfflineFetch:
    """Mixin replacing the CDS call with prebuilt archives, recording each partition fetched."""

    def __init__(self, archives, **kwargs):
        super().__init__(**kwargs)
        self.archives = [str(p) for p in archives]
        self.fetch_calls: list[pd.Timestamp] = []

    def fetch(self, it, temp_dir=None, **kwargs):
        self.fetch_calls.append(pd.Timestamp(it))
        return list(self.archives)


class OfflineForecastProvider(OfflineFetch, glofas.GloFASForecastProvider):
    pass


class OfflineReforecastProvider(OfflineFetch, glofas.GloFASReforecastProvider):
    pass


@pytest.fixture
def forecast_archives(tmp_path):
    discharge = make_forecast_nc(tmp_path / "dis.nc", variable="dis24")
    runoff = make_forecast_nc(tmp_path / "rowe.nc", variable="rowe")
    return [
        make_zip(tmp_path / "dis.zip", discharge),
        make_zip(tmp_path / "rowe.zip", runoff),
    ]


def test_process_merges_the_variables_and_names_the_init_time(
    local_config, forecast_archives, tmp_path
):
    provider = OfflineForecastProvider(forecast_archives, config=local_config)
    ds = provider.process(
        provider.archives, pd.Timestamp("2026-01-01"), temp_dir=tmp_path / "scratch"
    )

    assert set(ds.data_vars) == {"dis24", "rowe"}
    assert ds.sizes["init_time"] == 1
    assert ds["dis24"].dims[0] == "init_time"
    assert pd.Timestamp(ds.init_time.values[0]) == pd.Timestamp("2026-01-01")
    assert "time" not in ds.dims
    # Dask-backed, so the memory guard sizes the partition by the chunks in flight rather
    # than by the whole global grid.
    assert ds["dis24"].chunks is not None


def test_process_without_any_netcdf_members_raises(local_config, tmp_path):
    empty = tmp_path / "empty.zip"
    with zipfile.ZipFile(empty, "w") as zf:
        zf.writestr("readme.txt", "no data here")
    provider = glofas.GloFASForecastProvider(config=local_config)

    with pytest.raises(ValueError, match="no NetCDF members"):
        provider.process([str(empty)], pd.Timestamp("2026-01-01"), temp_dir=tmp_path / "scratch")


def test_run_partition_writes_and_then_skips(local_config, forecast_archives):
    provider = OfflineForecastProvider(forecast_archives, config=local_config)
    it = pd.Timestamp("2026-01-01")

    assert provider.run_partition(it) is True
    assert provider.run_partition(it) is False

    session = provider.get_icechunk_repo().readonly_session("main")
    stored = xr.open_zarr(session.store, consolidated=False)
    assert pd.Timestamp(stored.init_time.values[0]) == it
    assert set(stored.data_vars) == {"dis24", "rowe"}


def test_a_stored_reforecast_month_is_skipped(local_config, tmp_path):
    """Hindcasts are initialised Mon/Thu, so the month start is never a stored init_time."""
    # 2003-01-02 is the first Thursday of the month; the partition key is 2003-01-01.
    nc = make_forecast_nc(tmp_path / "rf.nc", variable="dis24", init="2003-01-02")
    archives = [make_zip(tmp_path / "rf.zip", nc)]
    provider = OfflineReforecastProvider(archives, config=local_config)
    month = pd.Timestamp("2003-01-01")

    assert provider.run_partition(month) is True
    assert provider.run_partition(month) is False
    assert provider.fetch_calls == [month]

    # A different month is still considered missing.
    assert provider.missing_timesteps(pd.DatetimeIndex(["2003-02-01"])) == [
        pd.Timestamp("2003-02-01")
    ]


def test_reforecast_accepts_variables_positionally(local_config):
    """The old *args/**kwargs signature raised TypeError on a positional variables list."""
    provider = glofas.GloFASReforecastProvider(local_config, None, ["dis24"])
    assert provider.variables == ["dis24"]
    assert glofas.GloFASReforecastProvider(local_config).variables == [
        "river_discharge_in_the_last_24_hours"
    ]
