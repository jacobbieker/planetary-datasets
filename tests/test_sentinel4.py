"""Sentinel-4 L2 archiving, offline: synthetic TROPOMI-layout chunks behind a fake Data Store."""

from __future__ import annotations

import io
import pathlib
import zipfile

import netCDF4
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from planetary_datasets.providers.eumetsat import sentinel4
from planetary_datasets.providers.eumetsat.sentinel4 import (
    Sentinel4Provider,
    classify,
    concat_granules,
    read_granule,
)

DAY = pd.Timestamp("2026-10-20")


def write_chunk(path: pathlib.Path, start: pd.Timestamp, scanlines: int, pixels: int = 4):
    """A body chunk shaped like the S4 L2 (TROPOMI-style) layout."""
    with netCDF4.Dataset(path, "w") as nc:
        nc.time_coverage_start = f"{start:%Y-%m-%dT%H:%M:%SZ}"
        nc.time_coverage_end = f"{start + pd.Timedelta(5, 'min'):%Y-%m-%dT%H:%M:%SZ}"
        nc.processor_version = "1.0.0"
        product = nc.createGroup("PRODUCT")
        product.createDimension("time", 1)
        product.createDimension("scanline", scanlines)
        product.createDimension("ground_pixel", pixels)
        for name in ("time", "scanline", "ground_pixel"):
            v = product.createVariable(name, "i4", (name,))
            v[:] = np.arange(len(product.dimensions[name]))
        lat = product.createVariable("latitude", "f4", ("time", "scanline", "ground_pixel"))
        lat[:] = np.linspace(30, 60, scanlines * pixels).reshape(1, scanlines, pixels)
        lon = product.createVariable("longitude", "f4", ("time", "scanline", "ground_pixel"))
        lon[:] = np.full((1, scanlines, pixels), 10.0)
        no2 = product.createVariable(
            "nitrogendioxide_tropospheric_column",
            "f4",
            ("time", "scanline", "ground_pixel"),
            fill_value=np.float32(9.96921e36),
        )
        no2.units = "mol m-2"
        values = np.full((1, scanlines, pixels), 1.25e-5, "f4")
        values[0, 0, 0] = 9.96921e36
        no2[:] = values
        qa = product.createVariable("qa_value", "u1", ("time", "scanline", "ground_pixel"))
        qa[:] = 100
        # Milliseconds since the chunk's own day: the epoch differs file to file.
        delta = product.createVariable("delta_time", "i4", ("time", "scanline"))
        delta.units = f"milliseconds since {start:%Y-%m-%d} 00:00:00"
        offset = int((start - start.normalize()).total_seconds() * 1000)
        delta[:] = offset + np.arange(scanlines) * 1000
        support = product.createGroup("SUPPORT_DATA").createGroup("DETAILED_RESULTS")
        support.createDimension("layer", 3)
        layers = support.createVariable(
            "averaging_kernel", "f4", ("scanline", "ground_pixel", "layer")
        )
        layers[:] = np.ones((scanlines, pixels, 3), "f4")
        clash = support.createVariable("qa_value", "u1", ("scanline", "ground_pixel"))
        clash[:] = 7
        meta = nc.createGroup("METADATA")
        meta.algorithm = "attributes only"


def zipped_product(tmp: pathlib.Path, name: str, start: pd.Timestamp, sizes: list[int]) -> bytes:
    """A Data Store product: its body chunks and a trailer, zipped."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        for i, n in enumerate(sizes):
            chunk = tmp / f"{name}-CHK-BODY-{i:03d}.nc"
            write_chunk(chunk, start + pd.Timedelta(5 * i, "min"), n)
            z.write(chunk, chunk.name)
        trailer = tmp / f"{name}-CHK-TRAIL.nc"
        with netCDF4.Dataset(trailer, "w") as nc:
            nc.list_of_chunks = "..."
        z.write(trailer, trailer.name)
    return buffer.getvalue()


class FakeProduct:
    def __init__(self, product_id: str, body: bytes):
        self.id, self.body = product_id, body

    def __str__(self):
        return self.id

    def open(self):
        product = self

        class Opened(io.BytesIO):
            name = f"{product.id}.zip"

            def __enter__(self):
                return self

        return Opened(product.body)


class FakeCollection:
    def __init__(self, cid: str, title: str, products: list[FakeProduct]):
        self.id, self.title, self.products = cid, title, products

    def __str__(self):
        return self.id

    def search(self, dtstart, dtend):
        return list(self.products)


class FakeDataStore:
    def __init__(self, collections: list[FakeCollection]):
        self.collections = collections

    def get_collection(self, cid):
        return next(c for c in self.collections if c.id == cid)


@pytest.mark.parametrize(
    ("title", "key"),
    [
        (
            "Sentinel-4 UVN Level 2 Nitrogen Dioxide (NO2) Total and Tropospheric Columns - MTG",
            "no2",
        ),
        ("Sentinel-4 UVN Level 2 Ozone (O3) Total Column - MTG", "o3"),
        ("Sentinel-4 UVN Level 2 Tropospheric Ozone - MTG", "o3_tropospheric"),
        ("UVN Level 2 Glyoxal (CHOCHO) Background - Sentinel-4", "chocho_background"),
        ("Sentinel-4 Level 2 Gapless Surface Reflectance", "gapless_surface_reflectance"),
        ("Sentinel-4 Level 2 Absorbing Aerosol Index", "aerosol_index"),
        ("Sentinel-4 UVN Level 2 Cloud Properties", "cloud"),
        ("Sentinel-4 UVN Level 1B Earth Radiance UVVIS", None),
        ("TROPOMI Level 2 Nitrogen Dioxide (NO2) - Sentinel-5P", None),
        ("Cloud Mask (netCDF) - MTG - 0 degree", None),
    ],
)
def test_collections_are_recognised_by_title(title, key):
    assert classify(title) == key


def test_a_chunk_reads_flat_on_its_scanlines(tmp_path):
    path = tmp_path / "chunk.nc"
    write_chunk(path, DAY + pd.Timedelta(10, "h"), 5)
    ds, track = read_granule(path)
    assert track == "scanline"
    assert ds["nitrogendioxide_tropospheric_column"].dims == ("scanline", "ground_pixel")
    assert ds["averaging_kernel"].dims == ("scanline", "ground_pixel", "layer")
    # Two groups' qa_value are told apart by group.
    assert "product_qa_value" in ds and "detailed_results_qa_value" in ds
    times = ds["delta_time"].values
    assert times.dtype.kind == "M" and times[0] == np.datetime64("2026-10-20T10:00:00")
    assert ds["granule_name"].values.tolist() == ["chunk.nc"]
    assert ds["time_coverage_start"].values.tolist() == ["2026-10-20T10:00:00Z"]
    assert "scanline" not in ds.data_vars and "time" not in ds.dims


def test_chunks_of_different_lengths_concatenate(tmp_path):
    frames = []
    for i, n in enumerate((3, 5)):
        path = tmp_path / f"c{i}.nc"
        write_chunk(path, DAY + pd.Timedelta(i, "h"), n)
        frames.append(read_granule(path))
    ds, track = concat_granules(frames, 10)
    assert track == "scanline" and ds.sizes["scanline"] == 8 and ds.sizes["granule"] == 2
    assert ds["granule_index"].values.tolist() == [10] * 3 + [11] * 5


@pytest.fixture
def provider(local_config, tmp_path, monkeypatch):
    src = tmp_path / "src"
    src.mkdir()
    noon = DAY + pd.Timedelta(12, "h")
    store = FakeDataStore(
        [
            FakeCollection(
                "EO:EUM:DAT:9001",
                "Sentinel-4 UVN Level 2 Nitrogen Dioxide (NO2) - MTG",
                [
                    FakeProduct("S4-NO2-1200", zipped_product(src, "NO2-1200", noon, [3, 4])),
                    FakeProduct(
                        "S4-NO2-1300",
                        zipped_product(src, "NO2-1300", noon + pd.Timedelta(1, "h"), [2]),
                    ),
                ],
            ),
            FakeCollection(
                "EO:EUM:DAT:9002",
                "Sentinel-4 UVN Level 2 Ozone (O3) Total Column - MTG",
                [FakeProduct("S4-O3-1200", zipped_product(src, "O3-1200", noon, [3]))],
            ),
            FakeCollection("EO:EUM:DAT:0662", "FCI Level 1c Normal Resolution - MTG", []),
        ]
    )
    made = Sentinel4Provider(config=local_config, download_workers=2)
    monkeypatch.setattr(made, "datastore", lambda: store)
    return made


def test_products_land_in_their_own_groups_and_open_as_a_tree(provider):
    assert provider.collections() == {"no2": "EO:EUM:DAT:9001", "o3": "EO:EUM:DAT:9002"}
    assert provider.missing_timesteps(pd.DatetimeIndex([pd.Timestamp("2025-01-01"), DAY])) == [DAY]
    assert provider.run_partition(DAY)
    store = provider.get_icechunk_repo().readonly_session("main").store
    tree = xr.open_datatree(store, engine="zarr", consolidated=False)
    assert set(tree.children) == {"no2", "o3"}
    no2 = xr.open_zarr(store, group="no2", consolidated=False)
    assert no2.sizes["scanline"] == 9 and no2.sizes["granule"] == 3
    assert no2["data_store_product"].values.tolist() == [
        "S4-NO2-1200",
        "S4-NO2-1200",
        "S4-NO2-1300",
    ]
    # Fill decodes to NaN; the rest is exact.
    column = no2["nitrogendioxide_tropospheric_column"].values
    assert np.isnan(column[0, 0]) and column[0, 1] == np.float32(1.25e-5)
    # Times from chunks with different epochs land on one axis.
    times = no2["delta_time"].values
    assert times[0] == np.datetime64("2026-10-20T12:00:00")
    assert times[-1] == np.datetime64("2026-10-20T13:00:01")
    assert "averaging_kernel" in no2 and no2["averaging_kernel"].dims[-1] == "layer"
    assert xr.open_zarr(store, group="o3", consolidated=False).sizes["scanline"] == 3
    # Products already stored are not fetched again.
    assert not provider.run_partition(DAY)


def test_nothing_published_means_unavailable(local_config, monkeypatch):
    made = Sentinel4Provider(config=local_config)
    monkeypatch.setattr(made, "datastore", lambda: FakeDataStore([]))
    assert not made.available()
    monkeypatch.setenv(sentinel4.COLLECTIONS_ENV, '{"no2": "EO:EUM:DAT:1234"}')
    made._collections = None
    assert made.available() and made.collections() == {"no2": "EO:EUM:DAT:1234"}


def test_the_lane_exits_unavailable_until_published(monkeypatch):
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "_backfill_lanes", pathlib.Path(__file__).resolve().parents[1] / "_backfill_lanes.py"
    )
    lanes = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(lanes)

    class Unpublished:
        def available(self):
            return False

    lane = lanes.LANES["sentinel4_l2"]
    monkeypatch.setitem(
        lanes.LANES, "sentinel4_l2", lanes.Lane(lambda: Unpublished(), lane.start, lane.freq)
    )
    assert lanes.main(["sentinel4_l2", "--once"]) == lanes.EXIT_UNAVAILABLE
