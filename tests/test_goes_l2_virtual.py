"""GOES ABI L2 virtual stores, against tiny synthetic granules on the local disk."""

from __future__ import annotations

import datetime as dt
import pathlib

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from planetary_datasets.providers.virtualized import goes_l2 as g

DAY = dt.date(2026, 10, 29)


def granule(
    root: pathlib.Path,
    product: str,
    when: dt.datetime,
    *,
    rows: int = 8,
    chunk_rows: int = 2,
    seed: int = 0,
    name: str = "TPW",
    channel: int | None = None,
) -> pathlib.Path:
    """One file laid out like NOAA's: a scalar t, a chunked zlib grid, small metadata."""
    code = f"M6C{channel:02d}" if channel else "M6"
    stamp = when.strftime("%Y%j%H%M%S") + "0"
    folder = (
        root / f"ABI-L2-{product}" / f"{when:%Y}" / f"{when.timetuple().tm_yday:03d}" / f"{when:%H}"
    )
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"OR_ABI-L2-{product}-{code}_G19_s{stamp}_e{stamp}_c{stamp}.nc"
    rng = np.random.default_rng(seed)
    ds = xr.Dataset(
        {
            name: (("y", "x"), rng.integers(0, 60000, (rows, 6)).astype("uint16")),
            "time_bounds": (("number_of_time_bounds",), np.array([0.0, 600.0])),
        },
        coords={
            "t": np.datetime64(when, "ns"),
            "y": np.linspace(0.1, -0.1, rows).astype("float32"),
            "x": np.linspace(-0.1, 0.1, 6).astype("float32"),
        },
    )
    ds[name].attrs.update(long_name="Total Precipitable Water", units="mm")
    encoding = {
        name: {"chunksizes": (chunk_rows, 6), "zlib": True, "shuffle": True, "_FillValue": 65535},
        "t": {"units": "seconds since 2000-01-01 12:00:00"},
    }
    ds.to_netcdf(path, engine="netcdf4", encoding=encoding)
    return path


@pytest.fixture
def archive(tmp_path, local_config, monkeypatch):
    """A local stand-in for s3://noaa-goes19, and stores that can reference it."""
    import icechunk
    import obstore

    root = tmp_path / "noaa-goes19"
    root.mkdir()
    url = f"file://{root}"
    monkeypatch.setattr(g, "source", lambda product: (url, obstore.store.LocalStore(str(root))))

    def open_store(prefix, product, config, create=True):
        storage_path = pathlib.Path(config.store_path(prefix))
        if not create and not storage_path.is_dir():
            raise FileNotFoundError(prefix)
        cfg = icechunk.RepositoryConfig.default()
        cfg.set_virtual_chunk_container(
            icechunk.VirtualChunkContainer(f"{url}/", icechunk.local_filesystem_store(str(root)))
        )
        creds = icechunk.containers_credentials(
            {f"{url}/": icechunk.credentials.LocalFileSystemAccess}
        )
        storage = config.icechunk_storage(prefix)
        if not create:
            return icechunk.Repository.open(storage, authorize_virtual_chunk_access=creds)
        return icechunk.Repository.open_or_create(
            storage, cfg, authorize_virtual_chunk_access=creds
        )

    monkeypatch.setattr(g, "open_store", open_store)
    return root


def read(product, config, prefix=None):
    repo = g.open_store(prefix or product.store_prefix, product, config, create=False)
    return xr.open_zarr(repo.readonly_session("main").store, consolidated=False)


def test_products_cover_gridded_full_disk_and_conus_with_cmip_by_channel():
    listing = [
        "ABI-L1b-RadF",
        "ABI-L2-ACMF",
        "ABI-L2-ACMM",
        "ABI-L2-CMIPC",
        "ABI-L2-DMWF",
        "ABI-L2-ACHA2KMF",
        "GLM-L2-LCFA",
    ]
    products = g.products_from_listing(19, listing)
    names = {p.name for p in products}
    assert {
        "goes19_l2_acm_f",
        "goes19_l2_acha2km_f",
        "goes19_l2_cmip_c_c01",
        "goes19_l2_cmip_c_c16",
    } <= names
    assert not any("dmw" in n or "_m" in n.rsplit("_", 1)[-1] for n in names)
    assert len(products) == 2 + 16
    cmip = g.Product(19, "CMIP", "F", 13)
    assert cmip.store_prefix == "bkr/geo/virtualized/goes19_l2/cmip_f_c13.icechunk"
    assert cmip.wants("ABI-L2-CMIPF/2026/302/00/OR_ABI-L2-CMIPF-M6C13_G19_s2026302000020_e.nc")
    assert not cmip.wants("ABI-L2-CMIPF/2026/302/00/OR_ABI-L2-CMIPF-M6C01_G19_s2026302000020_e.nc")


def test_days_go_recent_first_then_oldest_first():
    days = [DAY - dt.timedelta(days=n) for n in range(12)]
    ordered = g.order_days(days, DAY, recent=3)
    assert ordered[:4] == [DAY, DAY - dt.timedelta(1), DAY - dt.timedelta(2), DAY - dt.timedelta(3)]
    assert ordered[4:] == sorted(ordered[4:])


def test_a_day_is_referenced_once_and_reads_back_bit_for_bit(archive, local_config):
    start = dt.datetime.combine(DAY, dt.time())
    paths = [
        granule(archive, "TPWF", start + dt.timedelta(minutes=10 * i), seed=i) for i in range(3)
    ]
    product = g.Product(19, "TPW", "F")
    now = dt.datetime.combine(DAY + dt.timedelta(days=2), dt.time())
    assert g.run_product(product, local_config, now=now) == 3
    assert g.run_product(product, local_config, now=now) == 0, "the ledger stops a rewrite"
    ds = read(product, local_config)
    assert ds.sizes["t"] == 3 and ds.attrs[g.LEDGER_ATTR] == [DAY.isoformat()]
    raw = xr.open_zarr(
        g.open_store(product.store_prefix, product, local_config, create=False)
        .readonly_session("main")
        .store,
        consolidated=False,
        mask_and_scale=False,
    )
    for i, path in enumerate(paths):
        with xr.open_dataset(path, mask_and_scale=False) as src:
            np.testing.assert_array_equal(raw["TPW"].isel(t=i).values, src["TPW"].values)
    assert ds["TPW"].attrs["long_name"] == "Total Precipitable Water"
    assert pd.Timestamp(ds["t"].values[0]) == pd.Timestamp(start)


def test_a_changed_layout_starts_a_dated_generation_and_the_old_one_is_found_again(
    archive, local_config
):
    product = g.Product(19, "TPW", "F")
    first = dt.datetime.combine(DAY, dt.time())
    later_day = DAY + dt.timedelta(days=1)
    granule(archive, "TPWF", first)
    granule(archive, "TPWF", dt.datetime.combine(later_day, dt.time()), chunk_rows=4)
    now = dt.datetime.combine(DAY + dt.timedelta(days=5), dt.time())
    assert g.run_product(product, local_config, now=now) == 2
    stores = g.generations(local_config, product.store_prefix)
    assert len(stores) == 2 and stores[1].startswith("bkr/geo/virtualized/goes19_l2/tpw_f_")
    assert [read(product, local_config, p).sizes["t"] for p in stores] == [1, 1]
    chunks = {p: read(product, local_config, p)["TPW"].encoding["chunks"][1] for p in stores}
    assert sorted(chunks.values()) == [2, 4], "one layout per store"
    # Another day in the 2-row layout finds that store again; no third store appears.
    granule(archive, "TPWF", dt.datetime.combine(DAY - dt.timedelta(days=1), dt.time()))
    assert g.run_product(product, local_config, now=now) == 1
    two_row = next(p for p, c in chunks.items() if c == 2)
    assert read(product, local_config, two_row).sizes["t"] == 2
    assert len(g.generations(local_config, product.store_prefix)) == 2


def test_a_file_missing_the_grid_variable_is_left_out(archive, local_config):
    start = dt.datetime.combine(DAY, dt.time())
    granule(archive, "TPWF", start)
    granule(archive, "TPWF", start + dt.timedelta(minutes=10))
    granule(archive, "TPWF", start + dt.timedelta(minutes=20), name="OTHER")
    product = g.Product(19, "TPW", "F")
    now = dt.datetime.combine(DAY + dt.timedelta(days=2), dt.time())
    assert g.run_product(product, local_config, now=now) == 2
