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


def write_band(
    directory: pathlib.Path, band: str, start: pd.Timestamp, sat: str = "j01", **overrides
) -> str:
    """Write one synthetic M-band SDR file and return its path."""
    import h5py

    path = directory / idps_name(band_file_prefix(band), start, sat=sat)
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


def write_geo(directory: pathlib.Path, start: pd.Timestamp, sat: str = "j01") -> str:
    """Write one synthetic MOD-GEO-TC file and return its path."""
    import h5py

    path = directory / idps_name("GMTCO", start, sat=sat)
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


def write_granule(
    directory: pathlib.Path, start: pd.Timestamp, skip=(), sat: str = "j01"
) -> list[str]:
    """All seventeen files of one synthetic granule, less the prefixes in ``skip``."""
    files = [write_band(directory, band, start, sat=sat) for band in BANDS if band not in skip]
    if "GMTCO" not in skip:
        files.append(write_geo(directory, start, sat=sat))
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
    provider = JpssViirsMbandProvider(config=local_config)
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
    provider = JpssViirsMbandProvider(config=local_config)
    name = idps_name("SVM01", pd.Timestamp("2026-10-15T12:10"))
    indexed = provider._index_keys(
        [f"b/VIIRS-M1-SDR/{name}", f"b/VIIRS-M1-SDR/{name}.sha384", "b/VIIRS-M1-SDR/index.html"]
    )
    assert list(indexed.values()) == [f"b/VIIRS-M1-SDR/{name}"]


# --------------------------------------------------------------------- VIIRS M-band


def test_viirs_group_holds_every_spacecraft(local_config):
    provider = JpssViirsMbandProvider(config=local_config)
    assert provider.store_prefix == "bkr/polar/jpss.icechunk"
    assert provider.group == "viirs_mband"
    assert provider.satellites == ("n21", "n20", "snpp")
    with pytest.raises(ValueError):
        JpssViirsMbandProvider(config=local_config, satellites=("metop-b",))


def test_viirs_packing_table_covers_the_source_layout():
    packed = jpss_viirs.packed_variables()
    assert packed["M05_reflectance"] == (np.float32(2.4417415e-05), np.float32(0.0))
    assert packed["M15_brightness_temperature"] == (np.float32(0.004120439), np.float32(111.0))
    # IDPS stores these as float32, so the store does too.
    for name in ("M03_radiance", "M04_radiance", "M05_radiance", "M07_radiance"):
        assert name not in packed
    assert "M13_brightness_temperature" not in packed and "M13_radiance" not in packed


def test_viirs_round_trips_packed_counts_exactly(local_config, small_granules, tmp_path):
    provider = JpssViirsMbandProvider(config=local_config)
    start = pd.Timestamp("2026-10-15T12:10:01.5")
    files = write_granule(tmp_path, start)
    ds = provider.process(files, pd.Timestamp("2026-10-15T12:00"))
    assert ds.sizes == {"granule": 1, "y": SHAPE[0], "x": SHAPE[1], "scan": jpss_viirs.SCANS}
    assert provider.write_to_icechunk(provider.get_icechunk_repo(), ds)

    store = provider.get_icechunk_repo().readonly_session("main").store
    group = zarr.open_group(store, mode="r")["viirs_mband"]
    stored = group["M15_brightness_temperature"][0]
    source = counts(hash(("M15", "brightness_temperature")) % 1000)
    assert group["M15_brightness_temperature"].dtype == np.uint16
    assert group["M15_brightness_temperature"].chunks == (1, *SHAPE)
    np.testing.assert_array_equal(stored[2:], source[2:])
    assert (stored[:2] == jpss_viirs.PACKED_FILL).all()

    decoded = xr.open_zarr(store, group="viirs_mband", consolidated=False)
    assert str(decoded["granule"].values[0]) == "2026-10-15T12:10:01.5_n20"
    assert decoded["time"].values[0] == np.datetime64("2026-10-15T12:10:43.75")
    assert int(decoded["orbit_number"].values[0]) == 46155
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
    provider = JpssViirsMbandProvider(config=local_config)
    complete = write_granule(tmp_path, pd.Timestamp("2026-10-15T12:10:01.5"))
    partial = write_granule(tmp_path, pd.Timestamp("2026-10-15T12:11:26.0"), skip=("M07",))
    ds = provider.process(complete + partial, pd.Timestamp("2026-10-15T12:00"))
    assert ds.sizes["granule"] == 1


def test_viirs_two_partitions_of_three_spacecraft_append_and_dedupe(
    local_config, small_granules, tmp_path, monkeypatch
):
    """NOAA-20 and NOAA-21 start in the same tenth of a second; both are kept."""
    provider = JpssViirsMbandProvider(config=local_config)
    by_hour = {}
    for hour in (pd.Timestamp("2026-10-15T12:00"), pd.Timestamp("2026-10-15T13:00")):
        start = hour + pd.Timedelta(30, "min")
        by_hour[hour] = [
            path
            for sat, offset in (("j01", 0.0), ("j02", 0.0), ("npp", 600.0))
            for path in write_granule(tmp_path, start + pd.Timedelta(offset, "s"), sat=sat)
        ]
    monkeypatch.setattr(provider, "fetch", lambda it, temp_dir=None, **kw: by_hour[it])
    hours = pd.DatetimeIndex(list(by_hour))
    assert provider.run_range(hours) == 2
    assert provider.missing_timesteps(hours) == []
    assert provider.run_partition(hours[0]) is False
    # Even past the "done" check, the writer stores nothing twice.
    assert provider.run_partition(hours[0], check_present=False) is False
    ds = xr.open_zarr(
        provider.get_icechunk_repo().readonly_session("main").store,
        group="viirs_mband",
        consolidated=False,
    )
    assert ds.sizes["granule"] == 6
    assert list(ds["granule"].values[:3]) == [
        "2026-10-15T12:30:00.0_n20",
        "2026-10-15T12:30:00.0_n21",
        "2026-10-15T12:40:00.0_snpp",
    ]
    assert [str(p) for p in ds["platform_name"].values[:3]] == ["NOAA-20", "NOAA-21", "Suomi-NPP"]
    # Two spacecraft, one sensing time: the old whole-second time axis kept only one.
    assert ds["time"].values[0] == ds["time"].values[1]
    assert pd.Index(ds["time"].values).is_monotonic_increasing


def test_viirs_granule_sets_require_every_product(local_config, monkeypatch):
    provider = JpssViirsMbandProvider(config=local_config, satellites=("n20",))
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
    provider = JpssViirsMbandProvider(config=local_config)
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


def write_mirs(
    directory: pathlib.Path, name: str = MIRS_NAME, scans: int = 12, bt_scale: float = 0.015
) -> str:
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
        "BT": {"dtype": "int16", "scale_factor": bt_scale, "_FillValue": -999},
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


def test_l2_groups_hold_every_spacecraft(local_config):
    sfr = JpssAtmsSfrProvider(config=local_config)
    assert (sfr.store_prefix, sfr.group) == ("bkr/polar/jpss.icechunk", "atms_sfr")
    assert JpssMirsSndProvider(config=local_config).group == "mirs_snd"
    assert JpssMirsImgProvider(config=local_config).satellites == ("n21", "n20", "snpp")
    with pytest.raises(ValueError):
        JpssMirsImgProvider(config=local_config, satellites=("n19",))


def test_l2_granule_is_shaped_like_the_other_polar_stores(local_config, tmp_path):
    provider = JpssMirsImgProvider(config=local_config)
    ds = provider.process_granule(write_mirs(tmp_path))
    assert ds.sizes["granule"] == 1 and ds.sizes["y"] == 12 and ds.sizes["x"] == 96
    assert {"Freq", "Polo"} <= set(ds.coords) and "granule" not in ds["Freq"].dims
    assert {"latitude", "longitude"} <= set(ds.data_vars)
    assert "quality_information" not in ds
    assert ds["ScanTime_dom"].attrs == {"time_component_units": "days"}
    # The middle of 11:59:55.0 - 12:00:26.6, not rounded to the second.
    assert ds.time.values[0] == np.datetime64("2026-10-15T12:00:10.8")
    assert str(ds["platform_name"].values[0]) == "NOAA-20"
    assert str(ds["granule"].values[0]) == "2026-10-15T11:59:55.0_n20"
    assert ds.attrs["title"] == "MiRS" and "start_orbit_number" not in ds.attrs
    assert "platform" not in ds.attrs and "NOAA-21" in ds.attrs["platforms"]


def test_l2_short_granule_is_padded(local_config, tmp_path):
    provider = JpssMirsImgProvider(config=local_config)
    ds = provider.process_granule(write_mirs(tmp_path, scans=9))
    assert ds.sizes["y"] == 12
    assert np.isnan(ds["TPW"].values[0, 9:]).all()


def test_l2_store_keeps_the_source_packing(local_config, tmp_path):
    provider = JpssMirsImgProvider(config=local_config)
    ds = provider.process([write_mirs(tmp_path)], pd.Timestamp("2026-10-15T12:00"))
    assert provider.write_to_icechunk(provider.get_icechunk_repo(), ds)

    store = provider.get_icechunk_repo().readonly_session("main").store
    group = zarr.open_group(store, mode="r")["mirs_img"]
    assert group["BT"].dtype == np.int16
    assert group["BT"].attrs["scale_factor"] == 0.015
    assert group["BT"].chunks == (provider.granule_chunk, 12, 96, 22)
    assert group["RR"].dtype == np.float32
    assert group["ScanTime_dom"].dtype == np.int16
    assert group["end_time"].attrs["units"] == "microseconds since 1970-01-01"
    assert group["time"].attrs["units"] == "microseconds since 1970-01-01"
    out = xr.open_zarr(store, group="mirs_img", consolidated=False)
    assert out["end_time"].values[0] == np.datetime64("2026-10-15T12:00:26.600")
    np.testing.assert_allclose(out["BT"].values[0, 1:, :, :], 250.0, atol=0.0075)
    assert np.isnan(out["BT"].values[0, 0, 0, 0])
    np.testing.assert_allclose(out["TPW"].values, 12.3, atol=0.05)


SFR_NAMES = [
    "SFR_v2r0_n20_s202610151159230_e202610151159546_c202610151221350.nc",  # 11:59:39
    "SFR_v2r0_n20_s202610151159550_e202610151200266_c202610151221350.nc",  # 12:00:11
    "SFR_v2r0_n20_s202610151259550_e202610151300266_c202610151321350.nc",  # 13:00:11
]


def fake_buckets(provider, monkeypatch, names_by_sat):
    """List ``names_by_sat[sat]`` under each spacecraft's bucket."""
    buckets = {bucket: sat for sat, bucket in _jpss.BUCKETS.items()}
    monkeypatch.setattr(provider, "_s3", lambda: None)
    monkeypatch.setattr(
        provider,
        "_list_day",
        lambda fs, bucket, product, day: [
            f"{bucket}/{product}/{n}" for n in names_by_sat.get(buckets[bucket], [])
        ],
    )


def test_l2_owns_granules_by_mid_time(local_config, monkeypatch):
    provider = JpssAtmsSfrProvider(config=local_config)
    fake_buckets(provider, monkeypatch, {"n20": SFR_NAMES})
    keys = provider.granule_keys(pd.Timestamp("2026-10-15T12:00"))
    assert keys == [f"noaa-nesdis-n20-pds/ATMS-SFR/{SFR_NAMES[1]}"]


def test_l2_lists_every_spacecraft_and_skips_stored_granules(local_config, monkeypatch, tmp_path):
    provider = JpssMirsImgProvider(config=local_config)
    names = {
        sat: [MIRS_NAME.replace("_n20_", f"_{code}_")] for sat, code in _jpss.NDE_SPACECRAFT.items()
    }
    fake_buckets(provider, monkeypatch, names)
    hour = pd.Timestamp("2026-10-15T12:00")
    keys = provider.granule_keys(hour)
    assert sorted(k.split("/", 1)[0] for k in keys) == sorted(_jpss.BUCKETS.values())

    files = [write_mirs(tmp_path, names["n20"][0]), write_mirs(tmp_path, names["n21"][0])]
    assert provider.write_to_icechunk(provider.get_icechunk_repo(), provider.process(files, hour))
    assert [k.split("/", 1)[0] for k in provider.granule_keys(hour)] == ["noaa-nesdis-snpp-pds"]


def test_l2_fetch_fails_rather_than_writing_holes(local_config, monkeypatch, tmp_path):
    from planetary_datasets.providers.polar import jpss_atms_l2

    provider = JpssAtmsSfrProvider(config=local_config)
    monkeypatch.setattr(provider, "granule_keys", lambda it: ["b/k1", "b/k2"])
    monkeypatch.setattr(jpss_atms_l2, "download_many", lambda urls, dest, workers: [])
    with pytest.raises(_jpss.IncompleteDownload):
        provider.fetch(pd.Timestamp("2026-10-15T12:00"), temp_dir=tmp_path)


def test_a_day_is_listed_afresh_each_time(local_config):
    """s3fs caches listings per process; a long-running lane must still see new granules."""

    class GrowingBucket:
        def __init__(self):
            self.keys = ["b/p/2026/10/18/a.nc"]

        def ls(self, path, detail=False, refresh=False):
            assert refresh, "a cached listing hides granules that land later in the day"
            return list(self.keys)

    provider = JpssAtmsSfrProvider(config=local_config)
    fs = GrowingBucket()
    day = pd.Timestamp("2026-10-18")
    assert provider._list_day(fs, "b", "p", day) == ["b/p/2026/10/18/a.nc"]
    fs.keys.append("b/p/2026/10/18/b.nc")
    assert len(provider._list_day(fs, "b", "p", day)) == 2


# --------------------------------------------------------------------- one store, groups


def mirs_for(satellite: str, name: str = MIRS_NAME) -> str:
    """``name`` as the given spacecraft's granule."""
    return name.replace("_n20_", f"_{_jpss.NDE_SPACECRAFT[satellite]}_")


def stored(provider) -> xr.Dataset:
    return xr.open_zarr(
        provider.get_icechunk_repo().readonly_session("main").store,
        group=provider.group,
        consolidated=False,
    )


def test_coinciding_granules_of_two_spacecraft_are_both_kept(local_config, tmp_path):
    """NOAA-20 and NOAA-21 granules with identical sensing times are two rows."""
    provider = JpssMirsImgProvider(config=local_config)
    hour = pd.Timestamp("2026-10-15T12:00")
    files = [write_mirs(tmp_path, mirs_for(sat)) for sat in ("n21", "n20")]
    assert provider.write_to_icechunk(provider.get_icechunk_repo(), provider.process(files, hour))

    ds = stored(provider)
    assert ds.sizes["granule"] == 2
    assert [str(p) for p in ds["platform_name"].values] == ["NOAA-20", "NOAA-21"]
    assert ds["time"].values[0] == ds["time"].values[1]


def test_rerunning_an_hour_writes_nothing(local_config, tmp_path, monkeypatch):
    provider = JpssMirsImgProvider(config=local_config)
    hour = pd.Timestamp("2026-10-15T12:00")
    files = [write_mirs(tmp_path, mirs_for(sat)) for sat in ("n21", "n20", "snpp")]
    monkeypatch.setattr(provider, "fetch", lambda it, temp_dir=None, **kw: files)
    assert provider.run_partition(hour) is True
    snapshots = len(list(provider.get_icechunk_repo().ancestry(branch="main")))

    assert provider.missing_timesteps(pd.DatetimeIndex([hour])) == []
    assert provider.run_partition(hour) is False
    assert provider.run_partition(hour, check_present=False) is False
    assert len(list(provider.get_icechunk_repo().ancestry(branch="main"))) == snapshots
    assert stored(provider).sizes["granule"] == 3


def test_an_hour_is_done_only_when_every_spacecraft_is_stored(local_config, tmp_path):
    provider = JpssMirsImgProvider(config=local_config)
    hour = pd.Timestamp("2026-10-15T12:00")
    hours = pd.DatetimeIndex([hour])
    repo = provider.get_icechunk_repo()

    n20 = provider.process([write_mirs(tmp_path, mirs_for("n20"))], hour)
    assert provider.write_to_icechunk(repo, n20)
    # NOAA-20 alone does not make the hour done for the others...
    assert provider.missing_timesteps(hours) == [hour]
    # ...though it is for a lane that only ingests NOAA-20.
    only_n20 = JpssMirsImgProvider(config=local_config, satellites=("n20",))
    assert only_n20.missing_timesteps(hours) == []

    rest = [write_mirs(tmp_path, mirs_for(sat)) for sat in ("n21", "snpp")]
    assert provider.write_to_icechunk(repo, provider.process(rest, hour))
    assert provider.missing_timesteps(hours) == []
    # Appended in a second commit, yet still sorted: it is ordered by id, time first.
    assert stored(provider).sizes["granule"] == 3


def test_l2_skips_a_granule_packed_differently_from_the_group(local_config, tmp_path):
    provider = JpssMirsImgProvider(config=local_config)
    hour = pd.Timestamp("2026-10-15T12:00")
    repo = provider.get_icechunk_repo()
    first = provider.process([write_mirs(tmp_path, mirs_for("n20"))], hour)
    assert provider.write_to_icechunk(repo, first)

    repacked = write_mirs(tmp_path, mirs_for("n21"), bt_scale=0.02)
    # A compatible NOAA-21 granule in the same hour is withheld too: written, it would
    # make the hour look done for NOAA-21 and the repacked one would never be retried.
    later = MIRS_NAME.replace(
        "s202610151159550_e202610151200266", "s202610151210000_e202610151210316"
    )
    n21_ok = write_mirs(tmp_path, mirs_for("n21", later))
    same = write_mirs(tmp_path, mirs_for("snpp"))
    ds = provider.process([repacked, n21_ok, same], hour)
    assert [str(p) for p in ds["platform_name"].values] == ["Suomi-NPP"]
    assert provider.write_to_icechunk(repo, ds)
    # NOAA-21 is still missing, so the hour is retried rather than counted as done.
    assert provider.missing_timesteps(pd.DatetimeIndex([hour])) == [hour]


def test_two_groups_written_at_once_both_commit(local_config, tmp_path, monkeypatch):
    """Lanes of different products share the repository; neither loses its commit."""
    import threading

    from planetary_datasets.common import store as store_module

    hour = pd.Timestamp("2026-10-15T12:00")
    providers = [JpssMirsImgProvider(config=local_config), JpssMirsSndProvider(config=local_config)]
    datasets = [
        provider.process([write_mirs(tmp_path, mirs_for(sat))], hour)
        for provider, sat in zip(providers, ("n20", "n21"))
    ]
    for provider, ds in zip(providers, datasets):
        assert provider.write_to_icechunk(provider.get_icechunk_repo(), ds)

    # Both write their next hour into a session opened on the same snapshot; the first
    # commits, then the second, which has to rebase onto it. The commits are put in order
    # because the local filesystem storage, unlike S3, cannot arbitrate two at once.
    later = hour + pd.Timedelta(1, "h")
    an_hour_on = MIRS_NAME.replace(
        "s202610151159550_e202610151200266", "s202610151259550_e202610151300266"
    )
    nxt = [
        provider.process([write_mirs(tmp_path, mirs_for(sat, an_hour_on))], later)
        for provider, sat in zip(providers, ("n20", "n21"))
    ]
    barrier = threading.Barrier(2)
    first_committed = threading.Event()
    original = store_module.to_icechunk
    waited = set()

    def racing(ds, session, **kwargs):
        original(ds, session, **kwargs)
        name = threading.current_thread().name
        if name not in waited:
            waited.add(name)
            barrier.wait(timeout=30)
            if name == "w1":
                assert first_committed.wait(timeout=60)

    monkeypatch.setattr(store_module, "to_icechunk", racing)
    results = {}

    def write(index):
        provider = providers[index]
        try:
            results[index] = provider.write_to_icechunk(provider.get_icechunk_repo(), nxt[index])
        finally:
            if index == 0:
                first_committed.set()

    threads = [threading.Thread(target=write, args=(i,), name=f"w{i}") for i in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=120)
    assert results == {0: True, 1: True}

    for provider in providers:
        fresh = type(provider)(config=local_config)
        assert stored(fresh).sizes["granule"] == 2
    root = zarr.open_group(
        providers[0].get_icechunk_repo().readonly_session("main").store, mode="r"
    )
    assert sorted(root.group_keys()) == ["mirs_img", "mirs_snd"]
    assert dict(root.attrs) == {}


@pytest.mark.store_names
def test_readable_names_keep_the_packing(local_config, small_granules, tmp_path, monkeypatch):
    """The store's renames must not cost a packed variable its packing or exactness."""
    viirs = JpssViirsMbandProvider(config=local_config)
    hour = pd.Timestamp("2026-10-15T12:00")
    files = write_granule(tmp_path, pd.Timestamp("2026-10-15T12:10:01.5"))
    monkeypatch.setattr(viirs, "fetch", lambda it, temp_dir=None, **kw: files)
    assert viirs.run_partition(hour)

    mirs = JpssMirsImgProvider(config=local_config)
    monkeypatch.setattr(
        mirs, "fetch", lambda it, temp_dir=None, **kw: [write_mirs(tmp_path, mirs_for("n20"))]
    )
    assert mirs.run_partition(hour)

    root = zarr.open_group(viirs.get_icechunk_repo().readonly_session("main").store, mode="r")
    bt = root["viirs_mband/brightness_temperature_viirs_band_m15"]
    assert bt.dtype == np.uint16
    assert "bitround_keepbits" not in bt.attrs
    source = counts(hash(("M15", "brightness_temperature")) % 1000)
    np.testing.assert_array_equal(bt[0][2:], source[2:])
    assert root["viirs_mband/solar_zenith_angle"].attrs["bitround_keepbits"] == 16
    assert root["mirs_img/channel_temperature"].dtype == np.int16
    assert root["mirs_img/channel_temperature"].chunks[0] == mirs.granule_chunk


def test_holds_data_distinguishes_absent_from_unreadable(local_config, tmp_path, monkeypatch):
    """An absent group is empty; a failure to read an existing one is not."""
    from planetary_datasets.common import store as store_module

    provider = JpssMirsImgProvider(config=local_config)
    repo = provider.get_icechunk_repo()
    ds = provider.process([write_mirs(tmp_path)], pd.Timestamp("2026-10-15T12:00"))
    assert provider.write_to_icechunk(repo, ds)
    assert store_module.holds_data(repo, "mirs_img")
    assert not store_module.holds_data(repo, "mirs_snd")

    def broken(*args, **kwargs):
        raise ValueError("unreadable metadata")

    monkeypatch.setattr(store_module.zarr, "open_group", broken)
    with pytest.raises(ValueError):
        store_module.holds_data(repo, "mirs_snd")
