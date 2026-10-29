"""TEMPO L3 cloud and aerosol scans into one grouped store, offline."""

from __future__ import annotations

import pathlib

import h5py
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from planetary_datasets.providers import tempo
from planetary_datasets.providers.tempo import PRODUCTS, TempoL3Provider, append_scan, read_scan

NLAT, NLON = 6, 10
FILL = np.float32(-1e30)
EPOCH = pd.Timestamp("1980-01-06")


def write_scan(
    directory: pathlib.Path, short: str, start: str, scan: int = 8, lat0: float = 14.01
) -> pathlib.Path:
    """A small L3 file shaped like TEMPO's: groups, a fixed grid, one time step."""
    stamp = pd.Timestamp(start)
    path = directory / f"{short}_V04_{stamp:%Y%m%dT%H%M%S}Z_S{scan:03d}.nc"
    seconds = (stamp - EPOCH).total_seconds()
    rng = np.random.default_rng(scan)
    with h5py.File(path, "w") as f:
        f.attrs["scan_num"] = np.array([scan], "int32")
        f.attrs["time_coverage_start"] = f"{stamp:%Y-%m-%dT%H:%M:%SZ}"
        f.attrs["time_coverage_end"] = f"{stamp + pd.Timedelta(minutes=59):%Y-%m-%dT%H:%M:%SZ}"
        f.attrs["processing_version"] = np.array([4], "int32")
        f.attrs["production_date_time"] = "2026-10-20T19:59:37Z"
        f.attrs["input_files"] = np.array([f"{short[:-3]}_L2_G0{i}.nc" for i in range(1, 4)], "S")
        f.attrs["coremetadata"] = "GROUP = INVENTORYMETADATA"
        f["latitude"] = (lat0 + 0.02 * np.arange(NLAT)).astype("float32")
        f["longitude"] = (-167.99 + 0.02 * np.arange(NLON)).astype("float32")
        t = f.create_dataset("time", data=np.array([seconds]))
        t.attrs["units"] = b"seconds since 1980-01-06T00:00:00Z"
        weight = f.create_dataset("weight", data=rng.random((NLAT, NLON), dtype="float32"))
        weight.attrs["units"] = b"km^2"

        def field(name, dtype="float32", fill=FILL, units=None):
            values = rng.random((1, NLAT, NLON)).astype(dtype)
            values[0, 0, 0] = fill
            d = f.create_dataset(name, data=values, compression="gzip")
            d.attrs["_FillValue"] = np.array([fill], dtype)
            if units:
                d.attrs["units"] = units
            return d

        field("geolocation/solar_zenith_angle", units=b"degrees")
        if "CLDO4" in short:
            field("product/cloud_fraction", units=b"no unit")
            field("product/cloud_pressure", units=b"hPa")
            field("product/CloudRadianceFraction440", units=b"no unit")
            field("support_data/GLER440", units=b"no unit")
            field("support_data/surface_pressure", units=b"hPa")
        else:
            field("product/uv_aerosol_index")
            field("product/so2_index")
            field("product/column_amount_o3", units=b"DU")
            field("product/fc")
            field("support_data/cloud_pressure", units=b"hPA")
            field("support_data/terrain_height", dtype="int16", fill=np.int16(-32767))
            field("qa_statistics/num_column_samples", dtype="int32", fill=np.int32(-1))
    return path


def test_a_scan_reads_onto_the_grid_with_readable_names(tmp_path):
    ds = read_scan(write_scan(tmp_path, "TEMPO_CLDO4_L3", "2026-10-20T17:23:09"))
    assert dict(ds.sizes) == {"time": 1, "latitude": NLAT, "longitude": NLON}
    assert ds["effective_cloud_fraction"].dims == ("time", "latitude", "longitude")
    assert ds["effective_cloud_fraction"].attrs["source_name"] == "product/cloud_fraction"
    assert ds["cloud_pressure"].attrs["units"] == "hPa"
    assert "cloud_radiance_fraction_440nm" in ds
    assert "surface_lambertian_equivalent_reflectivity_440nm" in ds
    assert ds["area_weight"].dims == ("time", "latitude", "longitude")
    assert "solar_zenith_angle" not in ds, "geometry follows from time and grid"
    assert ds["time"].values[0] == np.datetime64("2026-10-20T17:23:09")
    assert ds["scan_number"].values.tolist() == [8]
    assert ds["l2_granule_count"].values.tolist() == [3]


def test_the_aerosol_scan_keeps_integer_fields_raw(tmp_path):
    ds = read_scan(write_scan(tmp_path, "TEMPO_O3TOT_L3", "2026-10-20T17:23:09"))
    assert "uv_aerosol_index" in ds and "total_column_ozone" in ds
    assert ds["terrain_height"].dtype == np.int16
    assert ds["terrain_height"].values[0, 0, 0] == -32767
    assert ds["total_column_ozone_sample_count"].dtype == np.int32


@pytest.fixture
def repo(local_config):
    return TempoL3Provider(config=local_config).get_icechunk_repo()


def test_scans_append_along_time_in_their_groups_and_decode(tmp_path, repo):
    tempo.ensure_root(repo)
    cloud = PRODUCTS["cloud_l3"]
    aerosol = PRODUCTS["aerosol_l3"]
    first = write_scan(tmp_path, "TEMPO_CLDO4_L3", "2026-10-20T17:23:09", scan=8)
    second = write_scan(tmp_path, "TEMPO_CLDO4_L3", "2026-10-20T16:23:09", scan=7)
    assert append_scan(repo, cloud, read_scan(first))
    assert append_scan(repo, cloud, read_scan(second))
    assert not append_scan(repo, cloud, read_scan(first)), "a stored scan is not written twice"
    assert append_scan(
        repo, aerosol, read_scan(write_scan(tmp_path, "TEMPO_O3TOT_L3", "2026-10-20T17:23:09"))
    )

    store = repo.readonly_session("main").store
    tree = xr.open_datatree(store, engine="zarr", consolidated=False)
    assert set(tree.children) == {"cloud_l3", "aerosol_l3"}
    assert "layout" in tree.attrs
    ds = xr.open_zarr(store, group="cloud_l3", consolidated=False)
    assert ds.sizes["time"] == 2
    assert list(ds["time"].values) == [
        np.datetime64("2026-10-20T17:23:09", "ns"),
        np.datetime64("2026-10-20T16:23:09", "ns"),
    ], "scans keep the order they were written in"
    # Fill decodes to NaN, every other value is the source's exactly.
    source = read_scan(first)
    stored = ds["effective_cloud_fraction"].isel(time=0).values
    raw = source["effective_cloud_fraction"].values[0]
    assert np.isnan(stored[0, 0])
    np.testing.assert_array_equal(stored.ravel()[1:], raw.ravel()[1:])
    assert ds["effective_cloud_fraction"].dtype == np.float32
    assert ds["effective_cloud_fraction"].encoding["chunks"] == (1, NLAT, NLON)
    assert ds["scan_name"].values.tolist() == [first.name, second.name]
    aer = xr.open_zarr(store, group="aerosol_l3", consolidated=False, mask_and_scale=False)
    assert (
        aer["terrain_height"].dtype == np.int16 and aer["terrain_height"].values[0, 0, 0] == -32767
    )


def test_a_scan_on_another_grid_is_refused(tmp_path, repo):
    cloud = PRODUCTS["cloud_l3"]
    append_scan(
        repo, cloud, read_scan(write_scan(tmp_path, "TEMPO_CLDO4_L3", "2026-10-20T17:23:09"))
    )
    moved = write_scan(tmp_path, "TEMPO_CLDO4_L3", "2026-10-20T18:23:09", scan=9, lat0=14.03)
    with pytest.raises(tempo.GridChanged):
        append_scan(repo, cloud, read_scan(moved))


def test_two_groups_written_concurrently_rebase(tmp_path, repo):
    import zarr

    tempo.ensure_root(repo)
    cloud, aerosol = PRODUCTS["cloud_l3"], PRODUCTS["aerosol_l3"]
    append_scan(
        repo, cloud, read_scan(write_scan(tmp_path, "TEMPO_CLDO4_L3", "2026-10-20T15:00:00", 6))
    )
    append_scan(
        repo, aerosol, read_scan(write_scan(tmp_path, "TEMPO_O3TOT_L3", "2026-10-20T15:00:00", 6))
    )
    # Two sessions opened before either commits, each appending to its own group.
    real = repo.writable_session
    sessions = [real("main"), real("main")]
    opened = iter(sessions)
    from unittest import mock

    with mock.patch.object(type(repo), "writable_session", lambda self, branch: next(opened)):
        a = tempo.read_scan(write_scan(tmp_path, "TEMPO_CLDO4_L3", "2026-10-20T16:00:00", 7))
        b = tempo.read_scan(write_scan(tmp_path, "TEMPO_O3TOT_L3", "2026-10-20T16:00:00", 7))
        assert append_scan(repo, cloud, a)
        assert append_scan(repo, aerosol, b)
    store = repo.readonly_session("main").store
    for group in ("cloud_l3", "aerosol_l3"):
        assert zarr.open_group(store, path=group, mode="r")["scan_name"].shape == (2,)


def test_a_day_is_written_once_and_settles(tmp_path, local_config, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    day = pd.Timestamp("2026-10-10")
    files = {
        p.name: p
        for p in (
            write_scan(source, "TEMPO_CLDO4_L3", "2026-10-10T15:00:00", 6),
            write_scan(source, "TEMPO_CLDO4_L3", "2026-10-10T16:00:00", 7),
        )
    }

    def fake_granules(short_name, version, start, end, session=None):
        assert short_name == "TEMPO_CLDO4_L3" and version == "V04"
        for name in files:
            yield {"links": [{"href": f"s3://asdc-prod-protected/TEMPO/{name}"}]}

    class FakeFS:
        def get(self, url, dest):
            pathlib.Path(dest).write_bytes(files[url.rsplit("/", 1)[1]].read_bytes())

    monkeypatch.setattr(tempo, "granules", fake_granules)
    provider = TempoL3Provider(config=local_config, products=["cloud_l3"])
    provider._s3 = type("S3", (), {"filesystem": lambda self: FakeFS()})()
    assert provider.missing_timesteps(pd.DatetimeIndex([day])) == [day]
    assert provider.run_partition(day)
    assert provider.stored_scans(PRODUCTS["cloud_l3"]) == set(files)
    assert provider.missing_timesteps(pd.DatetimeIndex([day])) == [], "settled once old enough"
    assert not provider.run_partition(day)
    assert provider.missing_timesteps(pd.DatetimeIndex(["2023-07-01"])) == []


def test_unknown_products_are_refused(local_config):
    with pytest.raises(ValueError, match="unknown TEMPO"):
        TempoL3Provider(config=local_config, products=["cloud_l2"])
