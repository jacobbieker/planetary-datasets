"""PACE references: synthetic contiguous granules referenced from a local container."""

from __future__ import annotations

import pathlib

import icechunk
import numpy as np
import pytest
import xarray as xr

from planetary_datasets.providers.polar.pace_virtual import (
    PaceVirtualProvider,
    VirtualProduct,
    describe_granule,
)

FILL = np.int16(-32767)
PRODUCT = VirtualProduct("harp2_l1b", "PACE_HARP2_L1B_SCI", "4", "swath_lines", 4)


def write_granule(directory: pathlib.Path, name: str, lines: int) -> pathlib.Path:
    """HARP2 L1B-shaped: views first, then the track, all contiguous and unfiltered."""
    path = directory / name
    geo = xr.Dataset(
        {
            "latitude": (
                ("views", "swath_lines", "swath_pixels"),
                np.arange(2 * lines * 3, dtype="float32").reshape(2, lines, 3),
            )
        }
    )
    obs = xr.Dataset(
        {
            "i": (
                ("views", "swath_lines", "swath_pixels"),
                np.arange(2 * lines * 3, dtype="int16").reshape(2, lines, 3) + 100,
            ),
            "view_angle": ("views", np.array([-50.0, 50.0], dtype="float32")),
        }
    )
    obs["i"].values[0, 0, 0] = FILL
    obs["i"].encoding.update({"_FillValue": FILL, "contiguous": True})
    geo["latitude"].encoding["contiguous"] = True
    geo.to_netcdf(path, group="geolocation_data", engine="h5netcdf")
    obs.to_netcdf(path, group="observation_data", engine="h5netcdf", mode="a")
    import h5py

    with h5py.File(path, "a") as f:
        # Set after writing: xarray would otherwise apply it to the values on the way out.
        f["observation_data/i"].attrs["scale_factor"] = np.float32(0.5)
    return path


@pytest.fixture
def provider(local_config, monkeypatch, tmp_path):
    source = tmp_path / "granules"
    source.mkdir()
    made = PaceVirtualProvider(config=local_config, products=[])
    prefix = f"file://{source}/"
    monkeypatch.setattr(
        type(made),
        "virtual_container",
        lambda self: icechunk.VirtualChunkContainer(
            url_prefix=prefix, store=icechunk.local_filesystem_store(str(source))
        ),
    )
    made.source = source
    made.prefix = prefix
    return made


def reference(provider, name: str, lines: int):
    import h5py

    path = write_granule(provider.source, name, lines)
    with h5py.File(path, "r") as f:
        return describe_granule(f, provider.prefix + name, PRODUCT)


def open_store(provider, **kwargs) -> xr.Dataset:
    repo = icechunk.Repository.open(
        provider.config.icechunk_storage(provider.store_prefix),
        authorize_virtual_chunk_access=icechunk.containers_credentials({provider.prefix: None}),
    )
    return xr.open_zarr(repo.readonly_session("main").store, consolidated=False, **kwargs)


def test_granules_are_referenced_cut_to_the_common_length(provider):
    batch = [
        reference(provider, "a.nc", 5),
        reference(provider, "b.nc", 4),
        reference(provider, "c.nc", 3),
    ]
    assert provider._write(PRODUCT, batch) == 3
    raw = open_store(provider, mask_and_scale=False)
    i = raw["harp2_l1b_i"]
    assert i.dims == (
        "harp2_l1b_granule",
        "harp2_l1b_views",
        "harp2_l1b_swath_lines",
        "harp2_l1b_swath_pixels",
    )
    assert i.shape == (3, 2, 4, 3)
    full = np.arange(2 * 5 * 3, dtype="int16").reshape(2, 5, 3) + 100
    full[0, 0, 0] = FILL
    np.testing.assert_array_equal(i.values[0], full[:, :4])
    assert raw["harp2_l1b_rows"].values.tolist() == [5, 4, 3]
    assert raw["harp2_l1b_granule_name"].values.tolist() == ["a.nc", "b.nc", "c.nc"]


def test_a_short_granule_reads_as_fill_and_scale_factors_decode(provider):
    provider._write(PRODUCT, [reference(provider, "b.nc", 4), reference(provider, "c.nc", 3)])
    ds = open_store(provider)
    assert np.isnan(ds["harp2_l1b_i"].values[0, 0, 0, 0])
    assert ds["harp2_l1b_i"].values[0, 0, 0, 1] == pytest.approx(101 * 0.5)
    assert np.isnan(ds["harp2_l1b_i"].values[1]).all()
    np.testing.assert_array_equal(ds["harp2_l1b_view_angle"].values, [[-50.0, 50.0], [-50.0, 50.0]])


def test_only_track_arrays_are_references(provider):
    granule = reference(provider, "a.nc", 4)
    assert granule.variables["i"].offset is not None
    assert granule.variables["latitude"].offset is not None
    assert granule.variables["view_angle"].values is not None


def test_the_store_holds_no_credentials(provider):
    provider._write(PRODUCT, [reference(provider, "a.nc", 4)])
    config = icechunk.Repository.fetch_config(
        provider.config.icechunk_storage(provider.store_prefix)
    )
    containers = config.virtual_chunk_containers
    assert list(containers) == [provider.prefix]
