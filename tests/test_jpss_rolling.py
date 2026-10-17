"""Offline tests for the rolling-retention JPSS stores: VIIRS M-band SDRs and ATMS level 2.

Every source file is synthesised in ``tmp_path`` with the layout of the operational
product, and every store is local. Nothing touches the network.
"""

from __future__ import annotations

import pathlib

import numpy as np
import pandas as pd
import pytest
import xarray as xr
import zarr

from planetary_datasets.providers.polar import _jpss, jpss_viirs
from planetary_datasets.providers.polar.jpss_atms_l2 import (
    JpssAtmsSfrProvider,
    JpssMirsImgProvider,
    JpssMirsSndProvider,
    source_packing,
)
from planetary_datasets.providers.polar.jpss_viirs import (
    BANDS,
    GEO_PRODUCT,
    JpssViirsMbandProvider,
    PackingChanged,
    band_file_prefix,
    band_product,
)

#: A small stand-in for the 768 x 3200 granule, so synthetic files stay tiny.
SHAPE = (32, 48)

#: IDPS Epoch Time runs ahead of UTC by the leap seconds; 37 s in 2026.
LEAP_SECONDS = 37


@pytest.fixture
def small_granules(monkeypatch):
    """Shrink the M-band granule shape for the duration of a test."""
    monkeypatch.setattr(jpss_viirs, "GRANULE_SHAPE", SHAPE)


def iet(stamp: pd.Timestamp) -> int:
    """IDPS Epoch Time (microseconds since 1958, leap seconds included) of a UTC time."""
    return int((stamp - pd.Timestamp("1958-01-01")) / pd.Timedelta(1, "us")) + LEAP_SECONDS * 10**6


def idps_name(prefix: str, start: pd.Timestamp, sat: str = "j01", orbit: int = 46155) -> str:
    """An IDPS filename for a granule starting at ``start`` and lasting 84.5 s."""
    end = start + pd.Timedelta(84.5, "s")
    return (
        f"{prefix}_{sat}_d{start:%Y%m%d}_t{start:%H%M%S}{start.microsecond // 100000}"
        f"_e{end:%H%M%S}{end.microsecond // 100000}_b{orbit:05d}"
        "_c20261015131336831000_oebc_ops.h5"
    )


def counts(seed: int, fill_rows: int = 2) -> np.ndarray:
    """Valid uint16 counts with the first ``fill_rows`` rows set to the bow-tie fill."""
    rng = np.random.default_rng(seed)
    data = rng.integers(0, 60000, size=SHAPE, dtype=np.uint16)
    data[:fill_rows] = 65533
    return data


def write_band(directory: pathlib.Path, band: str, start: pd.Timestamp, **overrides) -> str:
    """Write one synthetic M-band SDR file and return its path."""
    import h5py

    path = directory / idps_name(band_file_prefix(band), start)
    group = f"All_Data/{band_product(band)}_All"
    with h5py.File(path, "w") as h5:
        for quantity, field in BANDS[band].items():
            name = f"{group}/{field.dataset}"
            if field.packing is None:
                data = np.linspace(0.5, 300.0, SHAPE[0] * SHAPE[1], dtype=np.float32)
                data = data.reshape(SHAPE)
                data[:2] = -999.7
                h5[name] = overrides.get(field.dataset, data)
            else:
                h5[name] = overrides.get(field.dataset, counts(hash((band, quantity)) % 1000))
                factors = np.array([*field.packing, *field.packing], dtype=np.float32)
                h5[f"{name}Factors"] = overrides.get(f"{field.dataset}Factors", factors)
        h5[f"{group}/QF1_VIIRSMBANDSDR"] = np.full(SHAPE, 4, dtype=np.uint8)
    return str(path)


def write_geo(directory: pathlib.Path, start: pd.Timestamp) -> str:
    """Write one synthetic MOD-GEO-TC file and return its path."""
    import h5py

    path = directory / idps_name("GMTCO", start)
    group = f"All_Data/{GEO_PRODUCT}_All"
    with h5py.File(path, "w") as h5:
        for dataset, value in (
            ("Latitude", 45.123456),
            ("Longitude", -120.654321),
            ("SolarZenithAngle", 30.0),
            ("SolarAzimuthAngle", 150.0),
            ("SatelliteZenithAngle", 10.0),
            ("SatelliteAzimuthAngle", -80.0),
            ("Height", 1234.5),
        ):
            data = np.full(SHAPE, value, dtype=np.float32)
            data[:2] = -999.3
            h5[f"{group}/{dataset}"] = data
        scans = np.array(
            [iet(start + pd.Timedelta(1.786 * i, "s")) for i in range(jpss_viirs.SCANS)],
            dtype=np.int64,
        )
        scans[-1] = -993
        h5[f"{group}/StartTime"] = scans
        product = h5.create_dataset(f"Data_Products/{GEO_PRODUCT}/{GEO_PRODUCT}_Gran_0", (1,), "i")
        product.attrs["N_Beginning_Time_IET"] = np.array([[iet(start)]], dtype=np.uint64)
        product.attrs["Beginning_Date"] = np.array([[f"{start:%Y%m%d}".encode()]])
        tod = f"{start:%H%M%S}.{start.microsecond:06d}Z"
        product.attrs["Beginning_Time"] = np.array([[tod.encode()]])
    return str(path)


def write_granule(directory: pathlib.Path, start: pd.Timestamp, skip=()) -> list[str]:
    """All seventeen files of one synthetic granule, less the prefixes in ``skip``."""
    files = [write_band(directory, band, start) for band in BANDS if band not in skip]
    if "GMTCO" not in skip:
        files.append(write_geo(directory, start))
    return files


# --------------------------------------------------------------------- shared plumbing


def test_nde_filenames_parse_to_tenths_of_a_second():
    name = "SFR_v2r0_n20_s202610151159550_e202610151200266_c202610151221350.nc"
    assert _jpss.nde_granule_key(name) == ("n20", "202610151159550", "202610151200266")
    assert _jpss.nde_granule_start(name) == pd.Timestamp("2026-10-15T11:59:55.0")
    assert _jpss.nde_granule_end(name) == pd.Timestamp("2026-10-15T12:00:26.6")
    mirs = "NPR-MIRS-IMG_v11r10_npp_s202610152359510_e202610160000226_c202610160022370.nc"
    assert _jpss.nde_granule_end(mirs) == pd.Timestamp("2026-10-16T00:00:22.6")
    with pytest.raises(ValueError):
        _jpss.nde_granule_key("index.html")


def test_oldest_rolling_partition_leaves_a_day_of_margin():
    now = pd.Timestamp("2026-10-16T23:08", tz="UTC")
    assert _jpss.oldest_rolling_partition(now) == pd.Timestamp("2026-07-19")


def test_owned_by_matches_the_stored_timestamp(local_config):
    provider = JpssViirsMbandProvider("n20", config=local_config)
    start, end = provider.partition_window(pd.Timestamp("2026-10-15T12:00"))
    # Starts before the hour, but its middle (12:00:00.6) is inside it.
    straddling = idps_name("SVM01", pd.Timestamp("2026-10-15T11:59:18.3"))
    assert provider.overlaps(straddling, start, end)
    assert provider.owned_by(straddling, start, end)
    # Starts before the hour with its middle before it too: the previous partition's.
    earlier = idps_name("SVM01", pd.Timestamp("2026-10-15T11:59:00"))
    assert provider.overlaps(earlier, start, end)
    assert not provider.owned_by(earlier, start, end)


def test_index_keys_ignores_checksums_and_html(local_config):
    provider = JpssViirsMbandProvider("n20", config=local_config)
    name = idps_name("SVM01", pd.Timestamp("2026-10-15T12:10"))
    indexed = provider._index_keys(
        [f"b/VIIRS-M1-SDR/{name}", f"b/VIIRS-M1-SDR/{name}.sha384", "b/VIIRS-M1-SDR/index.html"]
    )
    assert list(indexed.values()) == [f"b/VIIRS-M1-SDR/{name}"]


# --------------------------------------------------------------------- VIIRS M-band


def test_viirs_stores_are_per_spacecraft(local_config):
    provider = JpssViirsMbandProvider("n21", config=local_config)
    assert provider.store_prefix == "bkr/polar/jpss_viirs_mband_n21.icechunk"
    assert provider.bucket == "noaa-nesdis-n21-pds"
    with pytest.raises(ValueError):
        JpssViirsMbandProvider("metop-b", config=local_config)


def test_viirs_packing_table_covers_the_source_layout():
    packed = jpss_viirs.packed_variables()
    assert packed["M05_reflectance"] == (np.float32(2.4417415e-05), np.float32(0.0))
    assert packed["M15_brightness_temperature"] == (np.float32(0.004120439), np.float32(111.0))
    # IDPS stores these as float32, so the store does too.
    for name in ("M03_radiance", "M04_radiance", "M05_radiance", "M07_radiance"):
        assert name not in packed
    assert "M13_brightness_temperature" not in packed and "M13_radiance" not in packed


def test_viirs_round_trips_packed_counts_exactly(local_config, small_granules, tmp_path):
    provider = JpssViirsMbandProvider("n20", config=local_config)
    start = pd.Timestamp("2026-10-15T12:10:01.5")
    files = write_granule(tmp_path, start)
    ds = provider.process(files, pd.Timestamp("2026-10-15T12:00"))
    assert ds.sizes == {"time": 1, "y": SHAPE[0], "x": SHAPE[1], "scan": jpss_viirs.SCANS}
    assert provider.write_to_icechunk(provider.get_icechunk_repo(), ds)

    store = provider.get_icechunk_repo().readonly_session("main").store
    group = zarr.open_group(store, mode="r")
    stored = group["M15_brightness_temperature"][0]
    source = counts(hash(("M15", "brightness_temperature")) % 1000)
    assert group["M15_brightness_temperature"].dtype == np.uint16
    assert group["M15_brightness_temperature"].chunks == (1, *SHAPE)
    np.testing.assert_array_equal(stored[2:], source[2:])
    assert (stored[:2] == jpss_viirs.PACKED_FILL).all()

    decoded = xr.open_zarr(store, consolidated=False)
    assert np.isnan(decoded["M15_brightness_temperature"][0, :2]).all()
    assert decoded["M01_quality_flags"].dtype == np.uint8
    # Latitude is exact; the angles are rounded to 16 mantissa bits.
    np.testing.assert_array_equal(decoded["latitude"][0, 2:], np.float32(45.123456))
    assert decoded["solar_zenith_angle"].attrs["bitround_keepbits"] == 16
    assert "bitround_keepbits" not in decoded["latitude"].attrs
    assert "bitround_keepbits" not in decoded["M05_reflectance"].attrs
    assert str(decoded["platform_name"].values[0]) == "NOAA-20"
    # A missing scan survives the round trip as NaN.
    offsets = decoded["scan_start_offset"].values[0]
    assert offsets[0] == pytest.approx(0.0) and offsets[1] == pytest.approx(1.786)
    assert np.isnan(offsets[-1])
    # Granule times are pinned to microseconds since the epoch, not the first value seen.
    assert group["start_time"].attrs["units"] == "microseconds since 1970-01-01"


def test_viirs_scan_times_are_utc_not_iet(small_granules, tmp_path):
    start = pd.Timestamp("2026-10-15T12:10:01.5")
    reference = pd.Timestamp("2026-10-15T12:10:01.4")
    offsets = jpss_viirs.read_scan_offsets(write_geo(tmp_path, start), reference)
    assert offsets.shape == (jpss_viirs.SCANS,)
    np.testing.assert_allclose(offsets[:2], [0.1, 1.886], atol=1e-6)
    assert np.isnan(offsets[-1])


def test_viirs_night_band_is_all_nan(tmp_path, small_granules):
    path = write_band(
        tmp_path,
        "M05",
        pd.Timestamp("2026-10-15T12:10"),
        Reflectance=np.full(SHAPE, 65535, dtype=np.uint16),
        ReflectanceFactors=np.full(2, -999.3, dtype=np.float32),
    )
    assert np.isnan(jpss_viirs.read_band_field(path, "M05", "reflectance")).all()


@pytest.mark.parametrize(
    "factors",
    [np.array([3e-05, 0.0], dtype=np.float32), np.full(2, -999.3, dtype=np.float32)],
    ids=["repacked", "fill-factors-with-valid-counts"],
)
def test_viirs_refuses_to_requantise(tmp_path, small_granules, factors):
    path = write_band(tmp_path, "M05", pd.Timestamp("2026-10-15T12:10"), ReflectanceFactors=factors)
    with pytest.raises(PackingChanged):
        jpss_viirs.read_band_field(path, "M05", "reflectance")


def test_viirs_skips_incomplete_granules(local_config, small_granules, tmp_path):
    provider = JpssViirsMbandProvider("n20", config=local_config)
    complete = write_granule(tmp_path, pd.Timestamp("2026-10-15T12:10:01.5"))
    partial = write_granule(tmp_path, pd.Timestamp("2026-10-15T12:11:26.0"), skip=("M07",))
    ds = provider.process(complete + partial, pd.Timestamp("2026-10-15T12:00"))
    assert ds.sizes["time"] == 1


def test_viirs_two_partitions_append_and_dedupe(
    local_config, small_granules, tmp_path, monkeypatch
):
    provider = JpssViirsMbandProvider("n20", config=local_config)
    by_hour = {
        pd.Timestamp("2026-10-15T12:00"): write_granule(tmp_path, pd.Timestamp("2026-10-15T12:30")),
        pd.Timestamp("2026-10-15T13:00"): write_granule(tmp_path, pd.Timestamp("2026-10-15T13:30")),
    }
    monkeypatch.setattr(provider, "fetch", lambda it, temp_dir=None, **kw: by_hour[it])
    hours = pd.DatetimeIndex(list(by_hour))
    assert provider.run_range(hours) == 2
    assert provider.missing_timesteps(hours) == []
    assert provider.run_partition(hours[0]) is False
    ds = xr.open_zarr(
        provider.get_icechunk_repo().readonly_session("main").store, consolidated=False
    )
    assert ds.sizes["time"] == 2


def test_viirs_granule_sets_require_every_product(local_config, monkeypatch):
    provider = JpssViirsMbandProvider("n20", config=local_config)
    full = pd.Timestamp("2026-10-15T12:10:01.5")
    partial = pd.Timestamp("2026-10-15T12:11:26.0")
    outside = pd.Timestamp("2026-10-15T13:20:00.0")

    def fake_list(fs, bucket, product, day):
        prefix = provider.products()[product]
        stamps = [full, outside] if product == "VIIRS-M7-SDR" else [full, partial, outside]
        return [f"{bucket}/{product}/{idps_name(prefix, s)}" for s in stamps]

    monkeypatch.setattr(provider, "_s3", lambda: None)
    monkeypatch.setattr(provider, "_list_day", fake_list)
    sets = provider.granule_sets(pd.Timestamp("2026-10-15T12:00"))
    assert len(sets) == 1
    assert set(sets[0]) == set(provider.products())
    assert all("t1210015" in key for key in sets[0].values())


def test_viirs_fetch_fails_rather_than_writing_holes(local_config, monkeypatch, tmp_path):
    provider = JpssViirsMbandProvider("n20", config=local_config)
    monkeypatch.setattr(
        provider, "granule_sets", lambda it: [{"VIIRS-M1-SDR": "b/k1", "GMTCO": "b/k2"}]
    )
    monkeypatch.setattr(
        jpss_viirs, "download_many", lambda urls, dest, workers: [tmp_path / "only-one"]
    )
    with pytest.raises(_jpss.IncompleteDownload):
        provider.fetch(pd.Timestamp("2026-10-15T12:00"), temp_dir=tmp_path)


# --------------------------------------------------------------------- ATMS level 2

MIRS_NAME = "NPR-MIRS-IMG_v11r10_n20_s202610151159550_e202610151200266_c202610151222370.nc"


def write_mirs(directory: pathlib.Path, name: str = MIRS_NAME, scans: int = 12) -> str:
    """A small MiRS-like granule with packed, float, time-component and static fields."""
    shape = (scans, 96)
    bt = np.full((*shape, 22), 250.0)
    bt[0, 0, 0] = np.nan
    ds = xr.Dataset(
        {
            "BT": (("Scanline", "Field_of_view", "Channel"), bt),
            "TPW": (("Scanline", "Field_of_view"), np.full(shape, 12.3)),
            "RR": (("Scanline", "Field_of_view"), np.full(shape, 0.5, dtype=np.float32)),
            "Qc": (("Scanline", "Field_of_view"), np.zeros(shape, dtype=np.int32)),
            "ScanTime_dom": (("Scanline",), np.full(scans, 15.0), {"units": "days"}),
            "Polo": (("Channel",), np.arange(22, dtype=np.int16)),
            "quality_information": ((), b"x"),
        },
        coords={
            "Freq": (("Channel",), np.linspace(23.8, 183.31, 22, dtype=np.float32)),
            "Latitude": (("Scanline", "Field_of_view"), np.full(shape, 10.0, dtype=np.float32)),
            "Longitude": (("Scanline", "Field_of_view"), np.full(shape, 20.0, dtype=np.float32)),
        },
        attrs={"title": "MiRS", "start_orbit_number": 46154},
    )
    encoding = {
        "BT": {"dtype": "int16", "scale_factor": 0.015, "_FillValue": -999},
        "TPW": {"dtype": "int16", "scale_factor": 0.1, "_FillValue": -999},
        "RR": {"_FillValue": np.float32(-999.0)},
        "ScanTime_dom": {"dtype": "int16", "_FillValue": -999},
    }
    path = directory / name
    ds.to_netcdf(path, encoding=encoding)
    return str(path)


def test_source_packing_only_reports_packed_integers(tmp_path):
    with xr.open_dataset(write_mirs(tmp_path), decode_timedelta=False) as ds:
        assert source_packing(ds["BT"].variable) == {
            "dtype": "int16",
            "scale_factor": 0.015,
            "_FillValue": np.int16(-999),
        }
        assert source_packing(ds["RR"].variable) is None
        assert source_packing(ds["Qc"].variable) is None


def test_l2_stores_are_per_spacecraft(local_config):
    assert JpssAtmsSfrProvider("n21", config=local_config).store_prefix == (
        "bkr/polar/jpss_atms_sfr_n21.icechunk"
    )
    assert JpssMirsSndProvider("snpp", config=local_config).bucket == "noaa-nesdis-snpp-pds"
    with pytest.raises(ValueError):
        JpssMirsImgProvider("n19", config=local_config)


def test_l2_granule_is_shaped_like_the_other_polar_stores(local_config, tmp_path):
    provider = JpssMirsImgProvider("n20", config=local_config)
    ds = provider.process_granule(write_mirs(tmp_path))
    assert ds.sizes["time"] == 1 and ds.sizes["y"] == 12 and ds.sizes["x"] == 96
    assert {"Freq", "Polo"} <= set(ds.coords) and "time" not in ds["Freq"].dims
    assert {"latitude", "longitude"} <= set(ds.data_vars)
    assert "quality_information" not in ds
    assert ds["ScanTime_dom"].attrs == {"time_component_units": "days"}
    assert ds.time.values[0] == np.datetime64("2026-10-15T12:00:11")
    assert str(ds["platform_name"].values[0]) == "NOAA-20"
    assert ds.attrs["title"] == "MiRS" and "start_orbit_number" not in ds.attrs


def test_l2_short_granule_is_padded(local_config, tmp_path):
    provider = JpssMirsImgProvider("n20", config=local_config)
    ds = provider.process_granule(write_mirs(tmp_path, scans=9))
    assert ds.sizes["y"] == 12
    assert np.isnan(ds["TPW"].values[0, 9:]).all()


def test_l2_store_keeps_the_source_packing(local_config, tmp_path):
    provider = JpssMirsImgProvider("n20", config=local_config)
    ds = provider.process([write_mirs(tmp_path)], pd.Timestamp("2026-10-15T12:00"))
    assert provider.write_to_icechunk(provider.get_icechunk_repo(), ds)

    store = provider.get_icechunk_repo().readonly_session("main").store
    group = zarr.open_group(store, mode="r")
    assert group["BT"].dtype == np.int16
    assert group["BT"].attrs["scale_factor"] == 0.015
    assert group["BT"].chunks == (provider.time_chunk, 12, 96, 22)
    assert group["RR"].dtype == np.float32
    assert group["ScanTime_dom"].dtype == np.int16
    assert group["end_time"].attrs["units"] == "microseconds since 1970-01-01"
    out = xr.open_zarr(store, consolidated=False)
    assert out["end_time"].values[0] == np.datetime64("2026-10-15T12:00:26.600")
    np.testing.assert_allclose(out["BT"].values[0, 1:, :, :], 250.0, atol=0.0075)
    assert np.isnan(out["BT"].values[0, 0, 0, 0])
    np.testing.assert_allclose(out["TPW"].values, 12.3, atol=0.05)


def test_l2_owns_granules_by_mid_time(local_config, monkeypatch):
    provider = JpssAtmsSfrProvider("n20", config=local_config)
    names = [
        "SFR_v2r0_n20_s202610151159230_e202610151159546_c202610151221350.nc",  # 11:59:39
        "SFR_v2r0_n20_s202610151159550_e202610151200266_c202610151221350.nc",  # 12:00:11
        "SFR_v2r0_n20_s202610151259550_e202610151300266_c202610151321350.nc",  # 13:00:11
    ]
    monkeypatch.setattr(provider, "_s3", lambda: None)
    monkeypatch.setattr(
        provider,
        "_list_day",
        lambda fs, bucket, product, day: [f"{bucket}/{product}/{n}" for n in names],
    )
    keys = provider.granule_keys(pd.Timestamp("2026-10-15T12:00"))
    assert [k.rsplit("/", 1)[1] for k in keys] == [names[1]]


def test_l2_fetch_fails_rather_than_writing_holes(local_config, monkeypatch, tmp_path):
    from planetary_datasets.providers.polar import jpss_atms_l2

    provider = JpssAtmsSfrProvider("n20", config=local_config)
    monkeypatch.setattr(provider, "granule_keys", lambda it: ["b/k1", "b/k2"])
    monkeypatch.setattr(jpss_atms_l2, "download_many", lambda urls, dest, workers: [])
    with pytest.raises(_jpss.IncompleteDownload):
        provider.fetch(pd.Timestamp("2026-10-15T12:00"), temp_dir=tmp_path)
