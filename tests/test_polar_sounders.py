"""Offline tests for the polar-orbiting sounder providers.

None of these touch the network: filename parsing, dataset shaping and the window-based
dedupe are all exercised against synthetic inputs and a local store.
"""

from __future__ import annotations

import datetime as dt

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from planetary_datasets.config import MissingCredential
from planetary_datasets.providers.polar import (
    JpssAtmsProvider,
    MetopAmsuaProvider,
    MetopAscatProvider,
    MetopAvhrrProvider,
    MetopGomeProvider,
    MetopIasiProvider,
    mid_time,
    pad_dim,
    process_eps_netcdf,
    serialize_attrs,
)
from planetary_datasets.providers.polar.jpss_atms import (
    granule_end,
    granule_key,
    granule_overlaps,
    granule_start,
)
from planetary_datasets.providers.polar.metop_iasi import platform_from_filename

ALL_PROVIDERS = [
    JpssAtmsProvider,
    MetopAmsuaProvider,
    MetopAscatProvider,
    MetopAvhrrProvider,
    MetopGomeProvider,
    MetopIasiProvider,
]

EUMDAC_PROVIDERS = [
    MetopAmsuaProvider,
    MetopAscatProvider,
    MetopAvhrrProvider,
    MetopGomeProvider,
    MetopIasiProvider,
]

SDR_NAME = "SATMS_j02_d20250601_t0000244_e0000560_b13247_c20250601002450674000_oeac_ops.h5"
GEO_NAME = "GATMO_j02_d20250601_t0000244_e0000560_b13247_c20250601002451076000_oeac_ops.h5"


def eps_orbit(along_track: int = 10, across_track: int = 4) -> xr.Dataset:
    """A stand-in for a Data Tailor ``netcdf4_satellite`` EPS product."""
    shape = (along_track, across_track)
    return xr.Dataset(
        {
            "channel_1": (("along_track", "across_track"), np.ones(shape, dtype="float32")),
            "degraded_ins_MDR": ("along_track", np.zeros(along_track, dtype="int8")),
            "degraded_proc_MDR": ("along_track", np.zeros(along_track, dtype="int8")),
            "record_start_time": (
                "along_track",
                pd.date_range("2025-07-18T00:37", periods=along_track, freq="8s").values,
            ),
            "record_stop_time": (
                "along_track",
                pd.date_range("2025-07-18T00:38", periods=along_track, freq="8s").values,
            ),
        },
        coords={
            "lat": (("along_track", "across_track"), np.zeros(shape, dtype="float32")),
            "lon": (("along_track", "across_track"), np.zeros(shape, dtype="float32")),
        },
        attrs={
            "source": "MetOp-C AMSUA",
            "start_sensing_data_time": "20250718003719Z",
            "end_sensing_data_time": "20250718022223Z",
        },
    )


def write_orbit(tmp_path, along_track: int) -> str:
    """Write :func:`eps_orbit` to ``tmp_path`` and return the path."""
    path = tmp_path / "orbit.nc"
    eps_orbit(along_track=along_track).to_netcdf(path)
    return str(path)


def one_row_per_time(*stamps: str) -> xr.Dataset:
    """A dataset with one value of ``a`` per timestamp along ``time``."""
    times = pd.DatetimeIndex(stamps)
    return xr.Dataset({"a": ("time", np.arange(1.0, len(times) + 1))}, coords={"time": times})


@pytest.fixture
def jpss(local_config):
    return JpssAtmsProvider(config=local_config)


@pytest.fixture
def avhrr(local_config):
    return MetopAvhrrProvider(config=local_config)


@pytest.fixture
def ascat(local_config):
    return MetopAscatProvider(config=local_config)


@pytest.fixture
def iasi(local_config):
    return MetopIasiProvider(config=local_config)


@pytest.fixture
def gome(local_config):
    return MetopGomeProvider(config=local_config)


@pytest.fixture
def amsua(local_config):
    """An AMSU-A provider padding to a test-sized 16 rows instead of the archive's 1100."""
    provider = MetopAmsuaProvider(config=local_config)
    provider.along_track_length = 16
    return provider


# --------------------------------------------------------------------- store wiring


@pytest.mark.parametrize("provider_cls", ALL_PROVIDERS)
def test_store_prefix_resolves_locally(provider_cls, local_config, tmp_path):
    provider = provider_cls(config=local_config)
    assert provider.store_prefix.startswith("bkr/polar/")
    assert provider.store_prefix.endswith(".icechunk")
    assert provider.store_path.startswith(str(tmp_path))


def test_store_prefixes_are_unique():
    assert len({cls.store_prefix for cls in ALL_PROVIDERS}) == len(ALL_PROVIDERS)


@pytest.mark.parametrize("provider_cls", EUMDAC_PROVIDERS)
def test_eumdac_providers_require_credentials(provider_cls, local_config):
    provider = provider_cls(config=local_config)
    with pytest.raises(MissingCredential, match="EUMETSAT_CONSUMER"):
        provider.datastore()


def test_eumdac_fetch_surfaces_missing_credentials(iasi):
    with pytest.raises(MissingCredential):
        iasi.fetch(pd.Timestamp("2025-01-01T00:00"))


# --------------------------------------------------------------------- window dedupe


def test_partition_window_matches_provider_width(iasi):
    start, end = iasi.partition_window(pd.Timestamp("2025-01-01T04:00"))
    assert end - start == pd.Timedelta("2h")


def test_partition_window_drops_timezone(jpss):
    start, _ = jpss.partition_window(pd.Timestamp("2025-01-01T04:00", tz="UTC"))
    assert start.tzinfo is None


def test_missing_timesteps_uses_the_window_not_the_exact_stamp(jpss, sample_dataset):
    """A granule at 04:17 must mark the 04:00 partition as done."""
    partitions = pd.DatetimeIndex(["2025-01-01T04:00", "2025-01-01T05:00"])
    assert jpss.missing_timesteps(partitions) == list(partitions)

    stored = sample_dataset.assign_coords(time=pd.DatetimeIndex(["2025-01-01T04:17:33"]))
    jpss.write_to_icechunk(jpss.get_icechunk_repo(), stored)
    assert jpss.missing_timesteps(partitions) == [pd.Timestamp("2025-01-01T05:00")]


def test_restrict_to_window_drops_rows_owned_by_the_neighbour(avhrr):
    """A boundary-crossing orbit belongs to exactly one partition (AVHRR: 4 hour window)."""
    ds = one_row_per_time("2025-01-01T03:30", "2025-01-01T04:30")
    first = avhrr.restrict_to_window(ds, pd.Timestamp("2025-01-01T00:00"))
    second = avhrr.restrict_to_window(ds, pd.Timestamp("2025-01-01T04:00"))
    assert list(first["time"].values) == [np.datetime64("2025-01-01T03:30")]
    assert list(second["time"].values) == [np.datetime64("2025-01-01T04:30")]
    # A partition none of the rows fall in is left empty.
    later = one_row_per_time("2025-01-01T09:00")
    assert avhrr.restrict_to_window(later, pd.Timestamp("2025-01-01T00:00")).sizes["time"] == 0


def test_restricted_writes_leave_the_neighbouring_partition_missing(avhrr):
    """The bug the restriction exists to prevent: one write marking two partitions done."""
    ds = one_row_per_time("2025-01-01T03:30", "2025-01-01T04:30")
    first = pd.Timestamp("2025-01-01T00:00")
    second = pd.Timestamp("2025-01-01T04:00")
    avhrr.write_to_icechunk(avhrr.get_icechunk_repo(), avhrr.restrict_to_window(ds, first))

    assert avhrr.missing_timesteps(pd.DatetimeIndex([first])) == []
    assert avhrr.missing_timesteps(pd.DatetimeIndex([second])) == [second]


# --------------------------------------------------------------------- JPSS ATMS


def test_granule_key_pairs_sdr_with_geolocation():
    assert granule_key(SDR_NAME) == granule_key(GEO_NAME)
    assert granule_key(SDR_NAME) == ("j02", "d20250601", "t0000244", "e0000560", "b13247")
    # Leading directories are ignored.
    assert granule_key(f"noaa-nesdis-n21-pds/ATMS-SDR/2025/06/01/{SDR_NAME}") == (
        granule_key(SDR_NAME)
    )


def test_granule_key_separates_spacecraft():
    """All three spacecraft share one store, so the key has to tell them apart."""
    other = SDR_NAME.replace("_j02_", "_j01_")
    assert granule_key(SDR_NAME) != granule_key(other)


def test_granule_key_rejects_other_filenames():
    with pytest.raises(ValueError):
        granule_key("something_else.h5")


@pytest.mark.parametrize(
    "name,start,end",
    [
        (SDR_NAME, "2025-06-01T00:00:24.400", "2025-06-01T00:00:56.000"),
        # Only the start day is in the filename, so an end before the start is the next day.
        (
            SDR_NAME.replace("t0000244", "t2359344").replace("e0000560", "e0000060"),
            "2025-06-01T23:59:34.400",
            "2025-06-02T00:00:06.000",
        ),
    ],
    ids=["same-day", "over-midnight"],
)
def test_granule_start_and_end_decode_tenths_of_a_second(name, start, end):
    assert granule_start(name) == pd.Timestamp(start)
    assert granule_end(name) == pd.Timestamp(end)


def test_granule_overlap_selects_a_granule_the_next_partition_owns():
    """The boundary case: selected by this hour, stored under the next one."""
    name = SDR_NAME.replace("t0000244", "t0059504").replace("e0000560", "e0100266")
    hour = pd.Timestamp("2025-06-01T00:00")
    assert granule_overlaps(name, hour, hour + pd.Timedelta("1h"))
    assert granule_overlaps(name, hour + pd.Timedelta("1h"), hour + pd.Timedelta("2h"))


def test_granule_overlap_excludes_a_granule_in_another_hour():
    hour = pd.Timestamp("2025-06-01T05:00")
    assert not granule_overlaps(SDR_NAME, hour, hour + pd.Timedelta("1h"))


def test_days_to_list_only_reaches_back_at_midnight(jpss):
    """Listing the previous day costs a full extra listing, so only do it when it can help."""
    midnight = jpss._days_to_list(*jpss.partition_window(pd.Timestamp("2025-06-01T00:00")))
    later = jpss._days_to_list(*jpss.partition_window(pd.Timestamp("2025-06-01T05:00")))
    assert list(midnight) == [pd.Timestamp("2025-05-31"), pd.Timestamp("2025-06-01")]
    assert list(later) == [pd.Timestamp("2025-06-01")]


def test_index_keys_skips_objects_that_are_not_granules(jpss):
    """One stray object in a day prefix must not take the partition down."""
    indexed = jpss._index_keys([f"bucket/ATMS-SDR/{SDR_NAME}", "bucket/ATMS-SDR/index.html"])
    assert list(indexed.values()) == [f"bucket/ATMS-SDR/{SDR_NAME}"]


def test_jpss_rejects_unknown_satellites(local_config):
    with pytest.raises(ValueError, match="unknown JPSS satellites"):
        JpssAtmsProvider(config=local_config, satellites=("n19",))


def test_jpss_process_pairs_files_and_skips_unmatched(jpss, monkeypatch):
    """Downloads come back as a flat list; process must re-pair them by granule key."""
    seen = []

    def fake_granule(sdr, geo):
        seen.append((sdr.split("/")[-1], geo.split("/")[-1]))
        return xr.Dataset(
            {"brightness": ("time", [1.0])},
            coords={"time": pd.DatetimeIndex([granule_start(sdr)])},
        )

    monkeypatch.setattr(jpss, "process_granule", fake_granule)
    lonely = SDR_NAME.replace("b13247", "b13248")
    ds = jpss.process(
        [f"/tmp/{GEO_NAME}", f"/tmp/{SDR_NAME}", f"/tmp/{lonely}"], pd.Timestamp("2025-06-01")
    )

    assert seen == [(SDR_NAME, GEO_NAME)]
    assert ds.sizes["time"] == 1


def test_jpss_https_url_is_anonymous_and_public():
    url = JpssAtmsProvider._https_url("noaa-nesdis-n21-pds", "ATMS-SDR/2025/06/01/x.h5")
    assert url == "https://noaa-nesdis-n21-pds.s3.amazonaws.com/ATMS-SDR/2025/06/01/x.h5"


# --------------------------------------------------------------------- EPS shaping


def test_process_eps_netcdf_lifts_the_sensing_window_onto_time():
    ds = process_eps_netcdf(eps_orbit())
    assert ds.sizes["time"] == 1
    assert ds["time"].values[0] == np.datetime64("2025-07-18T01:29:51")
    assert ds["platform_name"].values[0] == "MetOp-C"
    assert ds["start_time"].values[0] == np.datetime64("2025-07-18T00:37:19")
    # The swath dims are renamed and lat/lon promoted to data variables.
    assert set(ds.dims) == {"time", "y", "x"}
    assert "latitude" in ds.data_vars and "longitude" in ds.data_vars


def test_process_eps_netcdf_pads_without_breaking_dtypes():
    ds = process_eps_netcdf(eps_orbit(along_track=10), pad_along_track=16)
    assert ds.sizes["y"] == 16
    assert ds["degraded_ins_MDR"].dtype == np.int8
    assert ds["degraded_ins_MDR"].values[0, -1] == -1
    assert np.isnan(ds["channel_1"].values[0, -1, 0])
    assert ds["record_start_time"].values[0, -1] == np.datetime64("2000-01-01T00:00:00")


def test_process_eps_netcdf_leaves_long_orbits_alone():
    ds = process_eps_netcdf(eps_orbit(along_track=20), pad_along_track=16)
    assert ds.sizes["y"] == 20


def test_amsua_pads_to_the_archive_maximum():
    """The padded length is the ``y`` size of the published store, fixed by its first write."""
    assert MetopAmsuaProvider.along_track_length == 1100


def test_amsua_open_tailored_reads_a_netcdf_and_downcasts_channels(amsua, tmp_path):
    """The whole AMSU-A process path bar the Data Tailor call itself."""
    ds = amsua.open_tailored(write_orbit(tmp_path, 10))

    assert ds.sizes == {"time": 1, "y": 16, "x": 4}
    assert ds["channel_1"].dtype == np.float16
    assert ds["platform_name"].values[0] == "MetOp-C"


def test_amsua_drops_an_over_long_orbit(amsua, tmp_path):
    """An orbit longer than the padding target would otherwise break the concat."""
    assert amsua.open_tailored(write_orbit(tmp_path, 20)) is None


def test_ascat_reads_an_orbit_at_its_natural_length(ascat, tmp_path):
    assert ascat.open_tailored(write_orbit(tmp_path, 10)).sizes["y"] == 10


def test_ascat_target_length_is_the_longest_orbit_when_the_store_is_empty(ascat):
    orbits = [process_eps_netcdf(eps_orbit(along_track=n)) for n in (10, 14, 12)]
    assert ascat.target_length(orbits) == 14


def test_ascat_target_length_follows_the_store_once_it_exists(ascat):
    """A store's dimensions are fixed by its first write, so later orbits must match it."""
    first = process_eps_netcdf(eps_orbit(along_track=14))
    ascat.write_to_icechunk(ascat.get_icechunk_repo(), first)

    shorter = [process_eps_netcdf(eps_orbit(along_track=9))]
    assert ascat.target_length(shorter) == 14


def test_ascat_pads_differing_orbits_so_they_concatenate(ascat, tmp_path, monkeypatch):
    """Orbits of different lengths used to raise an AlignmentError out of the concat."""
    paths = []
    for index, length in enumerate((10, 14)):
        path = tmp_path / f"orbit{index}.nc"
        orbit = eps_orbit(along_track=length)
        orbit.attrs["start_sensing_data_time"] = f"2025071800{index}719Z"
        orbit.attrs["end_sensing_data_time"] = f"2025071801{index}223Z"
        orbit.to_netcdf(path)
        paths.append(str(path))

    monkeypatch.setattr(ascat, "tailor_to_netcdf", lambda *a, **k: paths)
    ds = ascat.process([], pd.Timestamp("2025-07-18T00:00"))
    assert ds.sizes == {"time": 2, "y": 14, "x": 4}


# --------------------------------------------------------------------- IASI / AVHRR


def iasi_product(periods: int = 5) -> xr.Dataset:
    """A stand-in for what harp returns for an IASI native product."""
    return xr.Dataset(
        {
            "datetime": ("time", pd.date_range("2025-01-01", periods=periods, freq="1s").values),
            "radiance": (("time", "spectral"), np.ones((periods, 3), dtype="float32")),
            "index": ("time", np.arange(periods)),
            "orbit_index": ("time", np.zeros(periods, dtype="int32")),
        }
    )


@pytest.fixture
def fake_harp(monkeypatch):
    """Make harp hand back :func:`iasi_product` for any native file."""
    monkeypatch.setattr(
        "planetary_datasets.providers.polar.metop_iasi.harp_to_dataset",
        lambda _: iasi_product(),
    )


def test_iasi_reads_soundings_without_inventing_rows(iasi, fake_harp):
    """No chunk padding: every stored row is a real sounding at its real time."""
    ds = iasi.process_granule("IASI_xxx_1C_M01_2025.nat")
    assert ds.sizes["time"] == 5
    assert "datetime" not in ds.variables and "orbit_index" not in ds.variables
    assert list(ds["platform_name"].values) == ["Metop-B"] * 5
    assert ds["index"].dtype.kind in "iu"
    assert ds["time"].values[-1] == np.datetime64("2025-01-01T00:00:04")
    assert (ds["time"].values == np.sort(ds["time"].values)).all()


def test_iasi_process_restricts_to_the_window_and_chunks(iasi, fake_harp, monkeypatch):
    iasi.chunk_soundings = 2
    monkeypatch.setattr(iasi, "iter_natives", lambda *a, **k: iter(["one.nat"]))

    inside = iasi.process([], pd.Timestamp("2025-01-01T00:00"))
    assert inside.sizes["time"] == 5
    assert inside.chunksizes["time"][0] == 2

    outside = iasi.process([], pd.Timestamp("2025-01-01T06:00"))
    assert outside.sizes.get("time", 0) == 0


@pytest.mark.parametrize("rows,expected", [(5, {"y": 8, "x": 4}), (9, None)], ids=["pads", "drops"])
def test_avhrr_align_pads_short_orbits_and_drops_oversized_ones(avhrr, rows, expected):
    avhrr.swath_shape = {"y": 8, "x": 4}
    aligned = avhrr.align(xr.Dataset({"a": (("y", "x"), np.ones((rows, 4), dtype="float32"))}))
    assert (aligned if expected is None else dict(aligned.sizes)) == expected


def test_fit_to_shape_keeps_integer_dtypes(jpss):
    out = jpss.fit_to_shape(xr.Dataset({"flag": ("y", np.ones(3, dtype="int8"))}), {"y": 5})
    assert out["flag"].dtype == np.int8
    assert out["flag"].values[-1] == -1


# --------------------------------------------------------------------- downloads


class _FakeProduct:
    """Minimal stand-in for an eumdac product."""

    def __init__(self, name, body=b"payload", failures=0):
        self.name = name
        self.body = body
        self.failures = failures
        self.opens = 0

    def open(self):
        self.opens += 1
        if self.opens <= self.failures:
            raise OSError("data store hiccup")
        import io

        stream = io.BytesIO(self.body)
        stream.name = self.name

        class _Ctx:
            def __enter__(inner):
                return stream

            def __exit__(inner, *exc):
                return False

        return _Ctx()


def test_download_product_retries_then_succeeds(gome, tmp_path):
    product = _FakeProduct("p.zip", failures=2)
    path = gome._download_product(product, tmp_path)
    assert path is not None and path.read_bytes() == b"payload"
    assert product.opens == 3


def test_download_product_gives_up_and_leaves_no_partial(gome, tmp_path):
    product = _FakeProduct("p.zip", failures=99)
    assert gome._download_product(product, tmp_path) is None
    assert list(tmp_path.iterdir()) == []


def test_download_product_skips_an_existing_file(gome, tmp_path):
    (tmp_path / "p.zip").write_bytes(b"already here")
    path = gome._download_product(_FakeProduct("p.zip"), tmp_path)
    assert path.read_bytes() == b"already here"


# --------------------------------------------------------------------- helpers


def test_mid_time_rounds_to_whole_seconds():
    got = mid_time("2025-01-01T00:00:00.100", "2025-01-01T00:00:01.100")
    assert got == pd.Timestamp("2025-01-01T00:00:01")


def test_pad_dim_is_a_no_op_for_missing_dims():
    ds = xr.Dataset({"a": ("y", np.zeros(3))})
    assert pad_dim(ds, "z", 10).sizes == ds.sizes


def test_serialize_attrs_flattens_everything_zarr_cannot_hold():
    out = serialize_attrs(
        {
            "when": dt.datetime(2025, 1, 1, 12),
            "flag": np.bool_(True),
            "nested": {"n": 3},
            "plain": "text",
        }
    )
    assert out == {
        "when": "2025-01-01T12:00:00",
        "flag": "True",
        "nested": {"n": "3"},
        "plain": "text",
    }


def test_platform_from_filename():
    assert platform_from_filename("IASI_xxx_1C_M01_2025.nat") == "Metop-B"
    assert platform_from_filename("IASI_xxx_1C_M03_2025.nat") == "Metop-C"
    assert platform_from_filename("IASI_xxx_1C_MZZ_2025.nat") == "unknown"


def test_extract_native_passes_through_a_bare_native(tmp_path):
    native = tmp_path / "product.nat"
    native.write_bytes(b"x")
    assert MetopGomeProvider.extract_native(native, tmp_path / "out") == str(native)


def test_extract_native_survives_a_corrupt_archive(tmp_path):
    archive = tmp_path / "product.zip"
    archive.write_bytes(b"definitely not a zip")
    assert MetopGomeProvider.extract_native(archive, tmp_path / "out") is None


def test_extract_native_finds_the_member_matching_the_archive(tmp_path):
    import zipfile

    archive = tmp_path / "product.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("other.nat", "no")
        zf.writestr("product.nat", "yes")
    found = MetopGomeProvider.extract_native(archive, tmp_path / "out")
    assert found.endswith("product.nat")


def test_tailor_requires_a_configured_product(ascat, monkeypatch):
    monkeypatch.setattr(type(ascat), "epct_product", None)
    with pytest.raises((ValueError, RuntimeError)):
        ascat.tailor_to_netcdf([], temp_dir=None)


def test_concat_granules_sorts_by_time(jpss):
    late = one_row_per_time("2025-01-01T01:00")
    early = one_row_per_time("2025-01-01T00:00").assign(a=("time", [0.0]))
    assert list(jpss.concat_granules([late, early])["a"].values) == [0.0, 1.0]


def test_an_empty_partition_concatenates_to_nothing_and_is_not_written(jpss):
    assert jpss.concat_granules([]).sizes == {}
    assert jpss.write_to_icechunk(jpss.get_icechunk_repo(), xr.Dataset()) is False


# --------------------------------------------------------------------- dagster wiring


def test_dagster_assets_load():
    import dagster as dg

    from dags.assets import polar_sounders

    defs = dg.Definitions(assets=polar_sounders.polar_sounder_assets)
    keys = {spec.key.to_user_string() for spec in defs.resolve_all_asset_specs()}
    assert keys == {
        "jpss-atms",
        "metop-amsua",
        "metop-ascat",
        "metop-avhrr",
        "metop-gome",
        "metop-iasi",
    }


def test_dagster_partition_widths_match_the_providers():
    from dags.assets import polar_sounders

    pairs = [
        (polar_sounders.metop_iasi_partitions, MetopIasiProvider),
        (polar_sounders.metop_avhrr_partitions, MetopAvhrrProvider),
        (polar_sounders.jpss_atms_partitions, JpssAtmsProvider),
        (polar_sounders.metop_gome_partitions, MetopGomeProvider),
        (polar_sounders.metop_daily_partitions, MetopAmsuaProvider),
    ]
    for partitions, provider_cls in pairs:
        keys = partitions.get_partition_keys(
            current_time=dt.datetime(2025, 1, 5, tzinfo=dt.timezone.utc)
        )
        window = partitions.time_window_for_partition_key(keys[-1])
        assert pd.Timestamp(window.end) - pd.Timestamp(window.start) == provider_cls.window
