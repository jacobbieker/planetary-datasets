"""The virtual GEM-15 store, against small GeoTIFFs laid out like Google's bucket."""

import os
import pathlib

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

from planetary_datasets.providers.virtualized import gem15 as g

LATTICE = g.LATTICE
SIZE = 1024  # pixels: a 2 x 2 tile cell, the real ones being 16 x 16


def write_tif(path: pathlib.Path, data: np.ndarray, epsg: int, x0: float, y0: float, **extra):
    """A tiled, DEFLATE-compressed, pixel-interleaved float32 COG-like file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    options = {
        "driver": "GTiff",
        "width": data.shape[2],
        "height": data.shape[1],
        "count": data.shape[0],
        "dtype": "float32",
        "crs": f"EPSG:{epsg}",
        "transform": from_origin(x0, y0, g.PIXEL, g.PIXEL),
        "tiled": True,
        "blockxsize": 512,
        "blockysize": 512,
        "compress": "deflate",
        "interleave": "pixel",
        "nodata": float("nan"),
        **extra,
    }
    with rasterio.open(path, "w", **options) as out:
        out.write(data)
        out.build_overviews([2, 4], rasterio.enums.Resampling.average)


def make_data(bands: int, seed: int, rows: int = SIZE, cols: int = SIZE) -> np.ndarray:
    """Random heights with a band of NaN (masked ground) and a NaN tile corner."""
    rng = np.random.default_rng(seed)
    data = (rng.standard_normal((bands, rows, cols)) * 100 + 500).astype(np.float32)
    data[:, 100:140, :] = np.nan
    data[:, 600:, 700:] = np.nan  # a masked corner
    return data


def add_cell(root: pathlib.Path, block: str, cell: str, epsg: int, x0, y0, seed, dtm=True):
    """Write a cell's DSM (five bands) and, unless ``dtm`` is False, its DTM."""
    files = {"dsm": make_data(5, seed)}
    if dtm:
        files["dtm"] = make_data(1, seed + 1000)
    for kind, data in files.items():
        write_tif(root / block / cell / kind / "zall" / "mosaic.tif", data, epsg, x0, y0)
    # The bucket holds other files beside the GeoTIFFs.
    (root / block / cell / "manifest_gem-15.json").write_text("{}")
    return files


# Cell corners on the 7680 m lattice of each zone's grid.
X_A = 64 * LATTICE
Y_N = g.GRID["N"][3] - 500 * LATTICE
Y_S = g.GRID["S"][3] - 30 * LATTICE


@pytest.fixture
def bucket(tmp_path):
    """A source with three cells in zone 31N (one apart from the other two), one in 31S."""
    root = tmp_path / "gem-15"
    truth = {
        "a": add_cell(root, "m10_p0", "cella", 32631, X_A, Y_N, 1),
        # Right beside a, with no DTM.
        "b": add_cell(root, "m10_p0", "cellb", 32631, X_A + SIZE * g.PIXEL, Y_N, 2, dtm=False),
        # Four chunks to the right of b, leaving two chunks of no cell between them.
        "c": add_cell(root, "m20_p0", "cellc", 32631, X_A + 4 * SIZE * g.PIXEL, Y_N, 3),
        "s": add_cell(root, "p10_p0", "cells", 32731, 10 * LATTICE, Y_S, 4),
    }
    return g.Source.local(str(root)), root, truth


@pytest.fixture
def repo(bucket, local_config):
    """An empty local store opened for writing."""
    source, *_ = bucket
    return g.open_writer("bkr/terrain/test.icechunk", source)


def window(ds, x0, y0, data):
    """The part of ``ds`` over a cell whose top-left corner is ``(x0, y0)``."""
    return ds.sel(
        x=slice(x0, x0 + SIZE * g.PIXEL),
        y=slice(y0, y0 - SIZE * g.PIXEL),
    )


def test_listing_finds_cells_and_their_files(bucket):
    """Listing finds cells and their files."""
    source, *_ = bucket
    cells = g.list_cells(source)
    assert sorted(cells) == ["cella", "cellb", "cellc", "cells"]
    assert cells["cella"].block == "m10_p0"
    assert cells["cella"].dsm and cells["cella"].dtm
    assert cells["cellb"].dtm is None


def test_cells_are_read_back_exactly_in_their_zone_groups(bucket, repo):
    """Cells are read back exactly in their zone groups."""
    source, _, truth = bucket
    stats = g.run_pass(repo, source, batch_cells=2)
    assert (stats.listed, stats.written, stats.failed, stats.commits) == (4, 4, 0, 2)

    north = g.open_zone(repo, "epsg_32631")
    assert set(g.VARIABLES) <= set(north.data_vars)
    assert north.dsm.dtype == np.float32
    for key, x0 in (("a", X_A), ("b", X_A + SIZE * g.PIXEL), ("c", X_A + 4 * SIZE * g.PIXEL)):
        cell = window(north, x0, Y_N, None)
        assert cell.sizes == {"y": SIZE, "x": SIZE}
        dsm = truth[key]["dsm"]
        for i, name in enumerate(g.DSM_BANDS):
            np.testing.assert_array_equal(cell[name].values, dsm[i])
        if "dtm" in truth[key]:
            np.testing.assert_array_equal(cell.dtm.values, truth[key]["dtm"][0])
        else:
            assert np.isnan(cell.dtm.values).all()

    south = g.open_zone(repo, "epsg_32731")
    cell = window(south, 10 * LATTICE, Y_S, None)
    np.testing.assert_array_equal(cell.dsm_std.values, truth["s"]["dsm"][4])
    np.testing.assert_array_equal(cell.dtm.values, truth["s"]["dtm"][0])


def test_chunks_with_no_cell_read_as_nan(bucket, repo):
    """Chunks with no cell read as nan."""
    source, *_ = bucket
    g.run_pass(repo, source)
    north = g.open_zone(repo, "epsg_32631")
    gap = north.sel(
        x=slice(X_A + 2 * SIZE * g.PIXEL, X_A + 4 * SIZE * g.PIXEL - 1),
        y=slice(Y_N, Y_N - SIZE * g.PIXEL),
    )
    assert gap.sizes["x"] == 2 * SIZE
    for name in g.VARIABLES:
        assert np.isnan(gap[name].values).all()
    # Beyond every cell of the zone.
    far = north.isel(y=slice(0, 600), x=slice(0, 600))
    assert np.isnan(far.dsm.values).all()


def test_the_arrays_are_plain_zarr_on_the_zones_native_grid(bucket, repo):
    """The arrays are plain zarr on the zones native grid."""
    import zarr

    source, *_ = bucket
    g.run_pass(repo, source)
    root = zarr.open_group(repo.readonly_session("main").store, mode="r")
    group = root["epsg_32631"]
    stack = group[g.STACK]
    assert stack.chunks == (512, 512, 5)
    assert stack.shape == (*g.Grid(32631).shape, 5)
    assert stack.metadata.dimension_names == ("y", "x", "dsm_band")
    assert [type(c).__name__ for c in stack.metadata.codecs] == ["BytesCodec", "Zlib"]
    assert group["dtm"].chunks == (512, 512)
    crs = group["spatial_ref"].attrs
    assert crs["epsg_code"] == "EPSG:32631"
    assert "UTM zone 31N" in crs["crs_wkt"]
    assert [float(v) for v in crs["GeoTransform"].split()] == [
        0.0,
        15.0,
        0.0,
        Y_N + 500 * LATTICE,
        0.0,
        -15.0,
    ]
    attrs = dict(root.attrs)
    assert attrs["license"] == "CC-BY-4.0"
    assert attrs["attribution"] == "This dataset is produced by Google"
    assert "10.31223/X55N75" in attrs["citation"]
    assert dict(group.attrs)["vertical_datum"].startswith("EGM96")


def test_a_second_pass_writes_nothing_and_a_changed_file_is_rewritten(bucket, repo):
    """A second pass writes nothing and a changed file is rewritten."""
    source, root, truth = bucket
    g.run_pass(repo, source)
    commits = len(list(repo.ancestry(branch="main")))
    again = g.run_pass(repo, source)
    assert (again.pending, again.written, again.commits) == (0, 0, 0)
    assert len(list(repo.ancestry(branch="main"))) == commits

    new = make_data(5, 99)
    write_tif(root / "m10_p0" / "cella" / "dsm" / "zall" / "mosaic.tif", new, 32631, X_A, Y_N)
    path = root / "m10_p0" / "cella" / "dsm" / "zall" / "mosaic.tif"
    os.utime(path, (path.stat().st_atime, path.stat().st_mtime + 5))
    changed = g.run_pass(repo, source)
    assert (changed.pending, changed.written) == (1, 1)
    north = g.open_zone(repo, "epsg_32631")
    np.testing.assert_array_equal(window(north, X_A, Y_N, None).dsm.values, new[0])
    # Its DTM, untouched, is still there.
    np.testing.assert_array_equal(window(north, X_A, Y_N, None).dtm.values, truth["a"]["dtm"][0])


def test_a_new_cell_is_added_on_a_later_pass(bucket, repo):
    """A new cell is added on a later pass."""
    source, root, _ = bucket
    g.run_pass(repo, source, only=["cella"])
    new = add_cell(root, "m10_p0", "celld", 32631, X_A, Y_N - SIZE * g.PIXEL, 7)
    stats = g.run_pass(repo, source)
    assert (stats.pending, stats.written) == (4, 4)
    north = g.open_zone(repo, "epsg_32631")
    below = window(north, X_A, Y_N - SIZE * g.PIXEL, None)
    np.testing.assert_array_equal(below.dsm.values, new["dsm"][0])


def test_a_cell_off_the_lattice_goes_to_a_shifted_grid(bucket, repo):
    """A cell off the lattice goes to a shifted grid."""
    source, root, _ = bucket
    # 15 m east of the lattice: no whole number of chunks from the grid's origin.
    truth = add_cell(root, "m10_p0", "celle", 32631, X_A + 15, Y_N - 4 * SIZE * g.PIXEL, 8)
    stats = g.run_pass(repo, source)
    assert (stats.written, stats.failed) == (5, 0)
    ledger = g.read_ledgers(repo)
    assert ledger["celle"]["dsm"]["zone"] == "epsg_32631_dx15_dy0"
    shifted = g.open_zone(repo, "epsg_32631_dx15_dy0")
    cell = window(shifted, X_A + 15, Y_N - 4 * SIZE * g.PIXEL, None)
    np.testing.assert_array_equal(cell.dsm.values, truth["dsm"][0])
    np.testing.assert_array_equal(cell.dtm.values, truth["dtm"][0])


def test_a_cell_outside_the_zones_grid_is_skipped_and_retried(bucket, repo):
    """A cell outside the zones grid is skipped and retried."""
    source, root, _ = bucket
    add_cell(root, "m10_p0", "celle", 32631, 200 * LATTICE, Y_N, 8)
    stats = g.run_pass(repo, source)
    assert (stats.written, stats.failed) == (4, 1)
    assert "celle" not in g.read_ledgers(repo)
    assert g.run_pass(repo, source).failed == 1


def test_cells_need_not_be_square_nor_dsm_and_dtm_alike(bucket, repo):
    """Cells need not be square nor dsm and dtm alike."""
    source, root, _ = bucket
    y0 = Y_N - 6 * SIZE * g.PIXEL
    dsm = make_data(5, 11, rows=SIZE, cols=3 * 512)
    dtm = make_data(1, 12, rows=3 * 512, cols=SIZE)
    write_tif(root / "m10_p0" / "cellg" / "dsm" / "zall" / "mosaic.tif", dsm, 32631, X_A, y0)
    write_tif(root / "m10_p0" / "cellg" / "dtm" / "zall" / "mosaic.tif", dtm, 32631, X_A, y0)
    assert g.run_pass(repo, source).failed == 0
    north = g.open_zone(repo, "epsg_32631")
    got = north.sel(x=slice(X_A, X_A + 3 * 512 * g.PIXEL), y=slice(y0, y0 - 3 * 512 * g.PIXEL))
    np.testing.assert_array_equal(got.dsm.values[:SIZE], dsm[0])
    assert np.isnan(got.dsm.values[SIZE:]).all()
    np.testing.assert_array_equal(got.dtm.values[:, :SIZE], dtm[0])
    assert np.isnan(got.dtm.values[:, SIZE:]).all()


def test_a_geotiff_with_another_layout_is_refused(bucket, repo, tmp_path):
    """A geotiff with another layout is refused."""
    source, root, _ = bucket
    write_tif(
        root / "m10_p0" / "cellf" / "dsm" / "zall" / "mosaic.tif",
        make_data(5, 5),
        32631,
        X_A,
        Y_N - 2 * SIZE * g.PIXEL,
        interleave="band",
    )
    stats = g.run_pass(repo, source)
    assert stats.failed == 1
    assert "cellf" not in g.read_ledgers(repo)


def test_the_ledger_names_each_cells_zone_and_signatures(bucket, repo):
    """The ledger names each cells zone and signatures."""
    source, *_ = bucket
    g.run_pass(repo, source)
    ledger = g.read_ledgers(repo)
    zones = {cell: {e["zone"] for e in entry.values()} for cell, entry in ledger.items()}
    assert zones == {
        "cella": {"epsg_32631"},
        "cellb": {"epsg_32631"},
        "cellc": {"epsg_32631"},
        "cells": {"epsg_32731"},
    }
    assert ledger["cella"]["dsm"]["sig"] == g.list_cells(source)["cella"].dsm
    assert (ledger["cella"]["dsm"]["row"], ledger["cella"]["dsm"]["col"]) == (500, 64)
    assert "dtm" not in ledger["cellb"]


def test_a_limited_pass_resumes(bucket, repo):
    """A limited pass resumes."""
    source, *_ = bucket
    assert g.run_pass(repo, source, limit=1).written == 1
    assert g.run_pass(repo, source).written == 3
    assert g.run_pass(repo, source).written == 0


@pytest.mark.parametrize(
    "lon, lat, group",
    [
        (-0.12, 51.5, "epsg_32630"),
        (2.0, 48.8, "epsg_32631"),
        (3.0, 0.0, "epsg_32631"),
        (-179.9, -45.0, "epsg_32701"),
        (179.9, 10.0, "epsg_32660"),
        (151.2, -33.9, "epsg_32756"),
    ],
)
def test_zone_group_of_a_point(lon, lat, group):
    """Zone group of a point."""
    assert g.zone_group(lon, lat) == group


def test_open_around_a_point(bucket, repo):
    """Open around a point."""
    source, *_ = bucket
    g.run_pass(repo, source)
    # A point inside cell a: UTM 31N, x = X_A + 3000, y = Y_N - 3000 .
    from rasterio.warp import transform

    (lon,), (lat,) = transform("EPSG:32631", "EPSG:4326", [X_A + 3000.0], [Y_N - 3000.0])
    ds = g.open_around(repo, lon, lat, half_width_m=300)
    assert ds.sizes["x"] == pytest.approx(40, abs=2)
    assert np.isfinite(ds.dsm.values).any()


def test_hemisphere_of_a_crs():
    """Hemisphere of a crs."""
    assert g.hemisphere(32631) == "N"
    assert g.hemisphere(32731) == "S"
    with pytest.raises(g.UnsupportedTiff):
        g.hemisphere(3857)


def test_a_replacement_that_shrank_leaves_none_of_the_old_footprint(bucket, repo):
    """A replacement that shrank leaves none of the old footprint."""
    source, root, _ = bucket
    g.run_pass(repo, source)
    path = root / "m10_p0" / "cella" / "dsm" / "zall" / "mosaic.tif"
    small = make_data(5, 77, rows=512, cols=SIZE)
    write_tif(path, small, 32631, X_A, Y_N)
    os.utime(path, (path.stat().st_atime, path.stat().st_mtime + 9))
    assert g.run_pass(repo, source).written == 1
    cell = window(g.open_zone(repo, "epsg_32631"), X_A, Y_N, None)
    np.testing.assert_array_equal(cell.dsm.values[:512], small[0])
    assert np.isnan(cell.dsm.values[512:]).all()
    assert np.isnan(cell.dsm_std.values[512:]).all()
    # The DTM, not replaced, is as it was.
    assert not np.isnan(cell.dtm.values[512:]).all()
