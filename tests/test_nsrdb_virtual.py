"""Offline tests for the NSRDB virtual-store ingest.

Tiny NSRDB-shaped HDF5 files are written under ``tmp_path`` and served through
a local virtual chunk container, so the whole path — listing, h5py metadata,
chunk indexing, virtual references, layout groups, schema drift, the reader —
runs without the network. Values read back through the store must equal the
h5py data divided by its scale factor exactly.
"""

from __future__ import annotations

import numpy as np
import pytest

h5py = pytest.importorskip("h5py")
pytest.importorskip("icechunk")
pytest.importorskip("virtualizarr")
pytest.importorskip("obstore")

import zarr  # noqa: E402

from planetary_datasets.providers.virtualized import nsrdb  # noqa: E402
from planetary_datasets.providers.virtualized.virtual_repo import (  # noqa: E402
    OutOfOrderPartition,
)

KEY = "testset"
DIRECTORY = "test/v1/"
STEM = "nsrdb_test"
N_GID = 50
GROUPS = ["clouds", "irradiance", "pv"]

#: Variables per group: name -> (dtype, scale_factor, (lo, hi) stored range).
VARIABLES = {
    "irradiance": {
        "ghi": ("<u2", 1.0, (0, 1300)),
        "dni": ("<u2", 1.0, (0, 1300)),
        "fill_flag": ("u1", 1.0, (0, 7)),
    },
    "clouds": {
        "cld_opd_dcomp": ("<u2", 100.0, (0, 8000)),
        "cloud_type": ("i1", 1.0, (-15, 12)),
    },
    "pv": {
        "air_temperature": ("<i2", 10.0, (-400, 500)),
        "wind_speed": ("<u2", 10.0, (0, 400)),
    },
}

#: Normal years: (time chunk, 16-bit gid chunk); 8-bit variables get twice the gid chunk.
NORMAL_CHUNKS = (7, 8)
#: A "leap year" layout, as Himawari-7 has, that cannot share arrays with the others.
LEAP_CHUNKS = (8, 9)


def _times(year: int, *, suffix: bool) -> np.ndarray:
    start = np.datetime64(f"{year}-01-01T00:00")
    stop = np.datetime64(f"{year + 1}-01-01T00:00")
    days = np.arange(start, stop, np.timedelta64(1, "D"))
    text = [str(t).replace("T", " ") + ":00" + ("+00:00" if suffix else "") for t in days]
    return np.array(text, dtype="S25")


def _lat_lon(n: int = N_GID, *, shuffled: bool = False) -> tuple[np.ndarray, np.ndarray]:
    lat = np.linspace(-60, 60, n, dtype=np.float32)
    lon = np.linspace(-170, 170, n, dtype=np.float32)
    if shuffled:
        lat = lat[::-1].copy()
    return lat, lon


def _values(year: int, name: str, shape, dtype: str, lo: int, hi: int) -> np.ndarray:
    seed = year * 1000 + sum(map(ord, name))
    return np.random.default_rng(seed).integers(lo, hi + 1, size=shape).astype(dtype)


def write_group_file(
    bucket,
    year: int,
    group: str,
    *,
    chunks=NORMAL_CHUNKS,
    extra_vars=(),
    drop_vars=(),
    extra_meta_field: bool = False,
    suffix: bool = True,
    shuffled_meta: bool = False,
    timezone_dtype: str = "<i2",
    version: str | None = None,
    days: int | None = None,
):
    """One synthetic NSRDB group file, laid out the way NREL's are."""
    path = bucket / DIRECTORY / f"{STEM}_{group}_{year}.h5"
    path.parent.mkdir(parents=True, exist_ok=True)
    times = _times(year, suffix=suffix)[:days]
    n_time = times.size
    fields = [
        ("latitude", "<f4"),
        ("longitude", "<f4"),
        ("elevation", "<i2"),
        ("timezone", timezone_dtype),
        ("country", "S12"),
        ("state", "S12"),
        ("county", "S12"),
    ]
    if extra_meta_field:
        fields.append(("offshore", "i1"))
    meta = np.zeros(N_GID, dtype=fields)
    lat, lon = _lat_lon(shuffled=shuffled_meta)
    meta["latitude"], meta["longitude"] = lat, lon
    meta["elevation"] = np.arange(N_GID) * 10
    meta["timezone"] = np.arange(N_GID) % 12
    meta["country"] = [f"Côte{i}".encode() for i in range(N_GID)]
    meta["state"] = b"None"
    meta["county"] = [f"c{i}".encode() for i in range(N_GID)]
    if extra_meta_field:
        meta["offshore"] = np.arange(N_GID) % 2

    variables = dict(VARIABLES[group])
    for name in extra_vars:
        variables[name] = ("<u2", 10.0, (0, 400))
    for name in drop_vars:
        variables.pop(name)

    with h5py.File(path, "w", libver="earliest") as f:
        f.attrs["version"] = version or f"4.{year % 10}.0"
        f.create_dataset("time_index", data=times)
        f.create_dataset("meta", data=meta, chunks=(16,))
        f.create_dataset("coordinates", data=np.stack([lat, lon], axis=1))
        t_chunk, g_chunk = chunks
        for name, (dtype, sf, (lo, hi)) in variables.items():
            width = g_chunk * (2 if np.dtype(dtype).itemsize == 1 else 1)
            data = _values(year, name, (n_time, N_GID), dtype, lo, hi)
            dset = f.create_dataset(name, data=data, chunks=(t_chunk, width))
            dset.attrs["scale_factor"] = np.float64(sf)
            dset.attrs["psm_scale_factor"] = np.float64(sf)
            dset.attrs["units"] = "unitless"
            dset.attrs["physical_min"] = np.float64(lo / sf)
            dset.attrs["physical_max"] = np.float64(hi / sf)
            dset.attrs["chunks"] = np.array([2000, 500])
    return path


def write_year(bucket, year: int, groups=GROUPS, **kwargs):
    per_group = kwargs.pop("per_group", {})
    for group in groups:
        write_group_file(bucket, year, group, **{**kwargs, **per_group.get(group, {})})


@pytest.fixture
def bucket(tmp_path, monkeypatch):
    """A local bucket, a test dataset in the catalog, and the source to read it through."""
    root = tmp_path / "bucket"
    root.mkdir()
    monkeypatch.setitem(
        nsrdb.DATASETS,
        KEY,
        nsrdb.NSRDBDataset(KEY, DIRECTORY, STEM, 1440, NORMAL_CHUNKS[0], "synthetic"),
    )
    return root


@pytest.fixture
def source(bucket):
    return nsrdb.Source(url=f"file://{bucket}")


def run(source, **kwargs):
    kwargs.setdefault("groups", GROUPS)
    kwargs.setdefault("workers", 0)
    return nsrdb.ingest(KEY, source=source, **kwargs)


def h5_physical(bucket, year: int, group: str, name: str) -> np.ndarray:
    with h5py.File(bucket / DIRECTORY / f"{STEM}_{group}_{year}.h5", "r") as f:
        dset = f[name]
        # What the scale codec computes: stored / scale in float64, then float32.
        return (dset[:] / np.float64(dset.attrs["scale_factor"])).astype(np.float32)


def year_slice(ds, year: int):
    years = ds["time"].values.astype("datetime64[Y]").astype(int) + 1970
    return np.flatnonzero(years == year)


def time_dim_lengths(source, config, path=""):
    repo = nsrdb.open_repo(KEY, source=source, config=config, create=False)
    group = zarr.open_group(repo.readonly_session("main").store, path=path, mode="r")
    return {n: a.shape[0] for n, a in nsrdb._time_arrays(group).items()}


# ---------------------------------------------------------------------------
# Catalog and listing
# ---------------------------------------------------------------------------
def test_file_pattern_matches_group_years_and_nothing_else():
    pattern = nsrdb.DATASETS["goes_full_disc_v4"].file_pattern
    assert pattern.match("nsrdb_full_disc_ancillary_a_2018.h5")["group"] == "ancillary_a"
    assert pattern.match("nsrdb_full_disc_pv_2025.h5")["year"] == "2025"
    assert pattern.match("nsrdb_full_disc_tmy_2022.h5") is None
    assert pattern.match("nsrdb_full_disc_pv_2025.h5.bak") is None


def test_himawari_datasets_read_the_right_folders():
    assert nsrdb.DATASETS["himawari8"].filename("ghi", 2016) == "himawari/himawari8_ghi_2016.h5"
    assert nsrdb.DATASETS["himawari7"].directory == "himawari/himawari7/"


def test_store_prefix_and_split_sizes():
    assert nsrdb.store_prefix_for("himawari7") == "bkr/nsrdb/himawari7.icechunk"
    # 17520 half-hour steps in 1344-row chunks: 14 chunks in a padded year.
    assert nsrdb.DATASETS["himawari7"].time_chunks_per_year == 14


def test_discover_files_groups_by_year_and_ignores_other_files(bucket, source):
    write_year(bucket, 2001, groups=["irradiance", "pv"])
    (bucket / DIRECTORY / f"{STEM}_tmy_2001.h5").write_bytes(b"")
    (bucket / DIRECTORY / "sub").mkdir()
    (bucket / DIRECTORY / "sub" / f"{STEM}_clouds_2001.h5").write_bytes(b"")
    listing = nsrdb.discover_files(KEY, source=source)
    assert list(listing) == [2001]
    assert sorted(listing[2001]) == ["irradiance", "pv"]
    assert listing[2001]["pv"].url == f"file://{bucket}/{DIRECTORY}{STEM}_pv_2001.h5"


# ---------------------------------------------------------------------------
# Round trip
# ---------------------------------------------------------------------------
@pytest.fixture
def four_years(bucket):
    """Two normal years, a leap year with another chunk layout, and one more normal year.

    2002 adds ``wind_speed_10m``, drops ``cld_opd_dcomp`` and adds an ``offshore``
    meta field; 2002 and 2005 have ``time_index`` without the ``+00:00`` suffix.
    """
    write_year(bucket, 2001, timezone_dtype="<i2")
    write_year(
        bucket,
        2002,
        suffix=False,
        extra_meta_field=True,
        timezone_dtype="<f4",
        per_group={
            "pv": {"extra_vars": ["wind_speed_10m"]},
            "clouds": {"drop_vars": ["cld_opd_dcomp"]},
        },
    )
    write_year(bucket, 2004, chunks=LEAP_CHUNKS)
    write_year(bucket, 2005, suffix=False)
    return [2001, 2002, 2004, 2005]


def test_ingest_round_trips_every_year_exactly(bucket, source, local_config, four_years):
    report = run(source)
    assert report.written == four_years
    assert report.layouts == {2001: "", 2002: "", 2004: "layout_8x9", 2005: ""}

    ds = nsrdb.open_nsrdb(KEY, source=source)
    index = ds.indexes["time"]
    assert index.is_monotonic_increasing and index.is_unique
    assert ds.sizes["time"] == 365 + 365 + 366 + 365
    assert ds["time"].values[0] == np.datetime64("2001-01-01T00:00")
    assert ds["time"].values[-1] == np.datetime64("2005-12-31T00:00")
    assert "valid" not in ds

    for year in four_years:
        rows = year_slice(ds, year)
        for group, names in VARIABLES.items():
            for name in names:
                if year == 2002 and name == "cld_opd_dcomp":
                    continue
                expected = h5_physical(bucket, year, group, name)
                got = ds[name].isel(time=rows).values
                assert got.dtype == np.float32
                np.testing.assert_array_equal(got, expected, err_msg=f"{name} {year}")

    # Schema drift: a variable new in 2002 reads NaN before it, a dropped one NaN in 2002.
    assert np.isnan(ds["wind_speed_10m"].isel(time=year_slice(ds, 2001)).values).all()
    np.testing.assert_array_equal(
        ds["wind_speed_10m"].isel(time=year_slice(ds, 2002)).values,
        h5_physical(bucket, 2002, "pv", "wind_speed_10m"),
    )
    assert np.isnan(ds["wind_speed_10m"].isel(time=year_slice(ds, 2005)).values).all()
    assert np.isnan(ds["cld_opd_dcomp"].isel(time=year_slice(ds, 2002)).values).all()

    # Sites: coordinates on gid, strings decoded, the meta union kept.
    lat, lon = _lat_lon()
    np.testing.assert_array_equal(ds["latitude"].values, lat)
    np.testing.assert_array_equal(ds["longitude"].values, lon)
    assert ds["timezone"].dtype == np.float32
    assert ds["country"].values[3] == "Côte3"
    np.testing.assert_array_equal(ds["offshore"].values, np.arange(N_GID) % 2)
    assert "latitude" in ds.coords and "coordinates" not in ds
    assert ds["ghi"].attrs["nsrdb_group"] == "irradiance"
    assert ds["cld_opd_dcomp"].attrs["nsrdb_scale_factor"] == 100.0

    attrs = ds.attrs
    assert attrs[nsrdb.ATTR_VERSIONS] == {
        "2001": "4.1.0",
        "2002": "4.2.0",
        "2004": "4.4.0",
        "2005": "4.5.0",
    }
    assert attrs[nsrdb.ATTR_META_FIELDS]["offshore"] == 2002
    assert attrs[nsrdb.ATTR_YEAR_LAYOUT]["2004"] == "layout_8x9"
    assert sorted(attrs[nsrdb.ATTR_FILES]["2001"]) == GROUPS


def test_every_time_dim_array_has_one_length_per_group(source, local_config, four_years):
    run(source)
    root = time_dim_lengths(source, local_config)
    layout = time_dim_lengths(source, local_config, "layout_8x9")
    # Three normal years of 365 days padded to 53 seven-day chunks.
    assert set(root.values()) == {3 * 371}
    # One leap year of 366 days padded to 46 eight-day chunks.
    assert set(layout.values()) == {368}
    assert {"time", "valid", "ghi", "wind_speed_10m", "cld_opd_dcomp"} <= set(root)


def test_virtual_arrays_carry_no_compressor_and_reference_the_source(
    bucket, source, local_config, four_years
):
    run(source, years=[2001])
    repo = nsrdb.open_repo(KEY, source=source, config=local_config, create=False)
    root = zarr.open_group(repo.readonly_session("main").store, mode="r")
    ghi = root["ghi"]
    assert ghi.compressors == ()
    assert ghi.dtype == np.float32 and np.isnan(ghi.fill_value)
    (scale,) = ghi.filters
    assert {k: v for k, v in scale.codec_config.items() if k != "id"} == {
        "offset": 0,
        "scale": 1.0,
        "dtype": "<f4",
        "astype": "<u2",
    }
    assert ghi.chunks == NORMAL_CHUNKS
    assert "scale_factor" not in ghi.attrs and "_FillValue" not in ghi.attrs
    assert ghi.attrs["nsrdb_stored_dtype"] == "<u2" and ghi.attrs["nsrdb_scale_factor"] == 1.0
    assert "chunks" not in ghi.attrs
    assert "coordinates" not in root


def test_plain_open_zarr_decodes_to_float32(bucket, source, local_config, four_years):
    import xarray as xr

    run(source, years=[2001])
    repo = nsrdb.open_repo(KEY, source=source, config=local_config, create=False)
    store = repo.readonly_session("main").store
    ds = xr.open_zarr(store, consolidated=False, decode_times=False)
    expected = h5_physical(bucket, 2001, "clouds", "cld_opd_dcomp")
    assert ds["cld_opd_dcomp"].dtype == np.float32
    np.testing.assert_array_equal(ds["cld_opd_dcomp"].values[:365], expected)
    assert ds["valid"].values[:365].all() and not ds["valid"].values[365:].any()
    raw = ds["time"].values
    assert raw.dtype == np.float64
    assert raw[0] == 978307200 and np.isnan(raw[365:]).all()
    assert ds["time"].attrs["units"] == "seconds since 1970-01-01"
    assert "latitude" in ds.coords


def test_plain_open_zarr_decodes_the_padded_time_axis(source, local_config, four_years):
    """xarray decodes NaN seconds as NaT; it cannot decode a masked integer time axis."""
    import xarray as xr

    run(source, years=[2001, 2002])
    repo = nsrdb.open_repo(KEY, source=source, config=local_config, create=False)
    ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    times, valid = ds["time"].values, ds["valid"].values
    assert times.dtype.kind == "M"
    assert np.isnat(times[~valid]).all() and not np.isnat(times[valid]).any()
    assert times[valid][0] == np.datetime64("2001-01-01T00:00")
    assert times[valid][-1] == np.datetime64("2002-12-31T00:00")


def test_migrate_time_float64_converts_an_int64_time_axis(source, local_config, four_years):
    run(source)
    repo = nsrdb.open_repo(KEY, source=source, config=local_config, create=False)
    session = repo.writable_session("main")
    root = zarr.open_group(session.store, mode="a")
    expected = {}
    for path in ["", "layout_8x9"]:
        group = root if not path else root[path]
        old = group["time"]
        times = nsrdb._decode_time(old[:])
        expected[path] = times
        attrs, chunks = dict(old.attrs), old.chunks
        del group["time"]
        legacy = group.create_array(
            "time",
            shape=times.shape,
            chunks=chunks,
            dtype=np.int64,
            fill_value=np.iinfo(np.int64).min,
            dimension_names=("time",),
            attributes=attrs,
        )
        legacy[:] = nsrdb._encode_time(times, np.int64)
    session.commit("legacy int64 time")
    before = nsrdb.open_nsrdb(KEY, source=source, config=local_config)

    assert nsrdb.migrate_time_float64(KEY, source=source, config=local_config) == 2
    assert nsrdb.migrate_time_float64(KEY, source=source, config=local_config) == 0

    root = zarr.open_group(repo.readonly_session("main").store, mode="r")
    for path, times in expected.items():
        group = root if not path else root[path]
        assert group["time"].dtype == np.float64
        np.testing.assert_array_equal(nsrdb._decode_time(group["time"][:]), times)
    after = nsrdb.open_nsrdb(KEY, source=source, config=local_config)
    np.testing.assert_array_equal(after["time"].values, before["time"].values)
    assert run(source).written == []


def _write_legacy_encoding(store) -> None:
    """Rewrite every data variable as the earlier integer + CF-attribute encoding."""
    import json

    from zarr.core.buffer import default_buffer_prototype
    from zarr.core.sync import sync

    root = zarr.open_group(store, mode="r")
    for name, arr in root.arrays():
        if arr.ndim != 2:
            continue
        stored = np.dtype(arr.attrs["nsrdb_stored_dtype"])
        sf = arr.attrs["nsrdb_scale_factor"]
        info = np.iinfo(stored)
        meta = arr.metadata.to_dict()
        attrs = {k: v for k, v in arr.attrs.items() if k != "nsrdb_stored_dtype"}
        attrs.update(
            scale_factor=1.0 / sf, _FillValue=int(info.max if stored.kind == "u" else info.min)
        )
        meta.update(
            data_type=stored.name,
            fill_value=attrs["_FillValue"],
            codecs=[{"name": "bytes", "configuration": {"endian": "little"}}],
            attributes=attrs,
        )
        buf = default_buffer_prototype().buffer.from_bytes(json.dumps(meta).encode())
        sync(store.set(f"{name}/zarr.json", buf))


def test_migrate_float32_converts_an_integer_encoded_store(
    bucket, source, local_config, four_years
):
    import xarray as xr

    run(source, years=[2001])
    repo = nsrdb.open_repo(KEY, source=source, config=local_config, create=False)
    session = repo.writable_session("main")
    _write_legacy_encoding(session.store)
    session.commit("legacy encoding")

    def plain():
        store = repo.readonly_session("main").store
        return xr.open_zarr(store, consolidated=False)

    assert plain()["ghi"].dtype == np.float64
    legacy = nsrdb.open_nsrdb(KEY, source=source, config=local_config)
    refs_before = sorted(repo.readonly_session("main").all_virtual_chunk_locations())

    assert nsrdb.migrate_float32(KEY, source=source, config=local_config) > 0
    assert nsrdb.migrate_float32(KEY, source=source, config=local_config) == 0

    ds = plain()
    assert all(ds[n].dtype == np.float32 for n in ds.data_vars if ds[n].ndim == 2)
    for group, names in VARIABLES.items():
        for name in names:
            np.testing.assert_array_equal(
                ds[name].values[:365], h5_physical(bucket, 2001, group, name), err_msg=name
            )
    migrated = nsrdb.open_nsrdb(KEY, source=source, config=local_config)
    np.testing.assert_allclose(migrated["ghi"].values, legacy["ghi"].values, rtol=1e-6)
    assert sorted(repo.readonly_session("main").all_virtual_chunk_locations()) == refs_before


# ---------------------------------------------------------------------------
# Resume and ordering
# ---------------------------------------------------------------------------
def test_rerun_is_a_no_op(source, local_config, four_years):
    run(source)
    repo = nsrdb.open_repo(KEY, source=source, config=local_config, create=False)
    before = repo.lookup_branch("main")
    report = run(source)
    assert report.written == [] and report.backfilled == []
    assert report.already_present == four_years
    assert repo.lookup_branch("main") == before


def test_out_of_order_year_raises(source, local_config, four_years):
    run(source, years=[2002])
    with pytest.raises(OutOfOrderPartition, match="2001"):
        run(source, years=[2001])


def test_unrequested_stranded_year_is_reported_after_the_rest(source, local_config, four_years):
    run(source, years=[2002])
    with pytest.raises(OutOfOrderPartition, match=r"\[2001\]"):
        run(source)
    ds = nsrdb.open_nsrdb(KEY, source=source)
    assert sorted(set(ds["time"].values.astype("datetime64[Y]").astype(int) + 1970)) == [
        2002,
        2004,
        2005,
    ]


def test_incomplete_year_is_skipped_unless_requested(bucket, source, local_config):
    write_year(bucket, 2001)
    write_year(bucket, 2002, groups=["irradiance"])
    report = run(source)
    assert report.written == [2001] and report.incomplete == [2002]
    with pytest.raises(nsrdb.IncompleteYear, match="clouds, pv"):
        run(source, years=[2002])


def test_years_after_an_incomplete_one_are_held_back_until_it_fills(bucket, source, local_config):
    write_year(bucket, 2001)
    write_year(bucket, 2002, groups=["irradiance"])
    write_year(bucket, 2003)
    with pytest.raises(nsrdb.IncompleteYear, match=r"\[2003\] held back"):
        run(source)
    write_year(bucket, 2002, groups=["clouds", "pv"])
    report = run(source)
    assert report.written == [2002, 2003] and report.already_present == [2001]


def test_backfill_refuses_groups_whose_time_index_differs(bucket, source, local_config):
    write_year(bucket, 2001, groups=["irradiance"])
    write_year(bucket, 2001, groups=["clouds", "pv"], days=360)
    run(source, groups=["irradiance"])
    with pytest.raises(nsrdb.LayoutConflict, match="time_index"):
        run(source)


def test_backfill_keeps_the_version_of_the_groups_already_stored(bucket, source, local_config):
    write_year(bucket, 2001, groups=["irradiance"], version="4.0.0")
    write_year(bucket, 2001, groups=["clouds", "pv"], version="4.0.1")
    run(source, groups=["irradiance"])
    assert run(source).backfilled == [2001]
    ds = nsrdb.open_nsrdb(KEY, source=source)
    assert ds.attrs[nsrdb.ATTR_VERSIONS]["2001"] == {
        "irradiance": "4.0.0",
        "clouds": "4.0.1",
        "pv": "4.0.1",
    }


def test_big_endian_variables_are_refused(bucket):
    path = bucket / "be.h5"
    with h5py.File(path, "w") as f:
        f.create_dataset("time_index", data=_times(2001, suffix=True))
        meta = np.zeros(N_GID, dtype=[("latitude", "<f4"), ("longitude", "<f4")])
        f.create_dataset("meta", data=meta)
        f.create_dataset("ghi", data=np.zeros((365, N_GID), ">u2"), chunks=(7, 8))
    with pytest.raises(nsrdb.UnsupportedLayout, match="big-endian"):
        nsrdb._read_file_info(f"file://{path}", "x", "irradiance")


def test_meta_change_is_refused(bucket, source, local_config):
    write_year(bucket, 2001)
    write_year(bucket, 2002, shuffled_meta=True)
    run(source, years=[2001])
    with pytest.raises(nsrdb.MetaMismatch, match="fingerprint"):
        run(source, years=[2002])


# ---------------------------------------------------------------------------
# Group subsets, backfill and large years
# ---------------------------------------------------------------------------
def test_a_group_subset_store_takes_the_other_groups_later(
    bucket, source, local_config, four_years
):
    run(source, years=[2001, 2002], groups=["irradiance"])
    ds = nsrdb.open_nsrdb(KEY, source=source)
    assert "cloud_type" not in ds

    report = run(source)
    assert report.backfilled == [2001, 2002]
    assert report.written == [2004, 2005]
    assert set(time_dim_lengths(source, local_config).values()) == {3 * 371}

    ds = nsrdb.open_nsrdb(KEY, source=source)
    for year in four_years:
        np.testing.assert_array_equal(
            ds["cloud_type"].isel(time=year_slice(ds, year)).values,
            h5_physical(bucket, year, "clouds", "cloud_type"),
        )
    assert ds.attrs[nsrdb.ATTR_YEAR_GROUPS]["2001"] == GROUPS
    assert run(source).backfilled == []


def test_a_year_too_large_for_one_change_set_lands_through_a_scratch_branch(
    bucket, source, local_config, four_years
):
    run(source, years=[2001], max_refs_per_commit=1)
    run(source, years=[2002], max_refs_per_commit=1)
    repo = nsrdb.open_repo(KEY, source=source, config=local_config, create=False)
    assert repo.list_branches() == {"main"}
    ds = nsrdb.open_nsrdb(KEY, source=source)
    np.testing.assert_array_equal(
        ds["air_temperature"].isel(time=year_slice(ds, 2002)).values,
        h5_physical(bucket, 2002, "pv", "air_temperature"),
    )
    # One commit per batch on the scratch branch, then main reset to its tip.
    messages = [s.message for s in repo.ancestry(branch="main")]
    assert sum("2002" in m for m in messages) == 3


# ---------------------------------------------------------------------------
# Indexing
# ---------------------------------------------------------------------------
def test_h5py_index_matches_chunk_info(bucket):
    path = write_group_file(bucket, 2001, "irradiance")
    index = nsrdb._h5py_index_task(f"file://{path}", "x", "ghi")
    assert index.shape == (365, N_GID) and index.chunk_shape == NORMAL_CHUNKS
    assert index.offsets.shape == (53, 7)
    with h5py.File(path, "r") as f:
        info = f["ghi"].id.get_chunk_info_by_coord((14, 16))
    assert index.offsets[2, 2] == info.byte_offset and index.lengths[2, 2] == info.size
    assert (index.lengths == 7 * 8 * 2).all()


def test_injected_indexer_is_used_and_cache_is_reused(bucket, source, local_config, tmp_path):
    write_year(bucket, 2001)
    calls = []

    def indexer(url, dataset):
        calls.append(dataset)
        return nsrdb._h5py_index_task(url, "x", dataset)

    run(source, indexer=indexer, cache_dir=tmp_path / "cache")
    assert sorted(calls) == sorted(n for g in VARIABLES.values() for n in g)

    listing = nsrdb.discover_files(KEY, source=source)
    plan = nsrdb._load_plan(2001, listing[2001], source, workers=0)
    calls.clear()
    nsrdb.index_variables(
        plan,
        list(plan.variables),
        source=source,
        indexer=indexer,
        cache_dir=tmp_path / "cache" / KEY,
    )
    assert calls == []


def test_process_pool_fallback_indexes_like_in_process(bucket, source, monkeypatch):
    write_year(bucket, 2001, groups=["irradiance"])
    monkeypatch.setattr(nsrdb, "_shared_indexer", lambda: None)
    plan = nsrdb._load_plan(2001, nsrdb.discover_files(KEY, source=source)[2001], source, workers=2)
    pooled = nsrdb.index_variables(plan, ["ghi", "fill_flag"], source=source, workers=2)
    serial = nsrdb.index_variables(plan, ["ghi", "fill_flag"], source=source, workers=0)
    for name in ("ghi", "fill_flag"):
        np.testing.assert_array_equal(pooled[name].offsets, serial[name].offsets)
        np.testing.assert_array_equal(pooled[name].lengths, serial[name].lengths)


def test_filtered_variables_are_refused(bucket):
    path = bucket / "f.h5"
    with h5py.File(path, "w") as f:
        f.create_dataset("time_index", data=_times(2001, suffix=True))
        meta = np.zeros(N_GID, dtype=[("latitude", "<f4"), ("longitude", "<f4")])
        f.create_dataset("meta", data=meta)
        f.create_dataset(
            "ghi", data=np.zeros((365, N_GID), "u2"), chunks=(7, 8), compression="gzip"
        )
    with pytest.raises(nsrdb.UnsupportedLayout, match="filters"):
        nsrdb._read_file_info(f"file://{path}", "x", "irradiance")


def test_time_index_with_and_without_offset_parse_alike():
    with_offset = nsrdb._parse_time_index(np.array([b"2018-01-01 00:30:00+00:00"]))
    without = nsrdb._parse_time_index(np.array([b"2018-01-01 00:30:00"]))
    assert with_offset[0] == without[0] == np.datetime64("2018-01-01T00:30", "ns")


def test_padded_year_marks_pad_rows():
    plan = nsrdb.YearPlan(
        year=2001,
        files={},
        infos={},
        times=np.arange(
            np.datetime64("2001-01-01", "ns"),
            np.datetime64("2001-01-11", "ns"),
            np.timedelta64(1, "D"),
        ),
        n_gid=1,
        t_chunk=4,
        variables={},
    )
    times, valid = plan.padded_times()
    assert plan.n_pad == 12
    assert valid.sum() == 10 and not valid[10:].any()
    assert np.isnat(times[10:]).all()
    assert plan.last_modified() is None
