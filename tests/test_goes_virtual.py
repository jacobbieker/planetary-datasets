"""Offline tests for the GOES virtualized ingest.

Everything here is a pure function or a config lookup: no network, no store.
The filename parsing and codec validation are where a silent regression would
be most expensive — a bad scan-start parse writes data under the wrong `t`,
and a codec check that accepts everything lets two eras into one store.
"""

from __future__ import annotations

import datetime
import os
import types

import numpy as np
import pytest

from planetary_datasets import config as config_module

# The virtual-reference stack is newer than the pins in pixi.toml: this ingest
# needs virtualizarr 2.x (open_virtual_mfdataset, the HDF parser, the `vz`
# accessor) with obstore and obspec_utils, and icechunk 2.x for manifest
# splitting. Skip rather than fail where the environment has not caught up.
pytest.importorskip("obstore", reason="virtualizarr 2.x stack not installed")
pytest.importorskip("obspec_utils", reason="virtualizarr 2.x stack not installed")

from planetary_datasets.providers.virtualized import goes_radf_common as common  # noqa: E402


RADF_URL = (
    "s3://noaa-goes16/ABI-L1b-RadF/2023/109/12/"
    "OR_ABI-L1b-RadF-M6C13_G16_s20231091200205_e20231091209525_c20231091209594.nc"
)


# ---------------------------------------------------------------------------
# Filename / URL parsing
# ---------------------------------------------------------------------------
def test_scan_start_token_is_read_from_the_basename():
    assert common._scan_start_token(RADF_URL) == "20231091200205"


def test_channel_is_read_from_the_mode_channel_token():
    assert common._channel_from_filename(RADF_URL) == 13
    assert common._channel_from_filename(RADF_URL.replace("M6C13", "M6C02")) == 2


def test_parse_scan_start_resolves_day_of_year_and_tenths():
    t = common.parse_scan_start_to_datetime(RADF_URL)
    # 2023 day 109 is 19 April; the trailing digit is tenths of a second.
    assert t == np.datetime64("2023-04-19T12:00:20.500000000", "ns")
    assert t.dtype == common.DATETIME_NS


def test_parse_scan_start_rejects_a_truncated_token():
    with pytest.raises(ValueError, match="token length"):
        common.parse_scan_start_to_datetime("OR_ABI-L1b-RadF-M6C13_G16_s2023109_e1_c1.nc")


@pytest.mark.parametrize(
    "url,aborted",
    [
        (RADF_URL.replace("e20231091209525", "e20231091200205"), True),
        (RADF_URL, False),
        # A filename without the tokens must not crash the check.
        ("s3://noaa-goes16/index.html", False),
    ],
)
def test_aborted_scans_are_those_whose_start_and_end_tokens_match(url, aborted):
    assert common._is_aborted_scan(url) is aborted


def test_parse_url_to_day_prefers_the_first_matching_product_key():
    reproc = RADF_URL.replace("ABI-L1b-RadF/", "ABI-L1b-RadF-Reproc/")
    keys = ["ABI-L1b-RadF-Reproc", "ABI-L1b-RadF"]
    assert common.parse_url_to_day(RADF_URL, keys) == (2023, 109)
    assert common.parse_url_to_day(reproc, keys) == (2023, 109)


def test_parse_url_to_day_rejects_an_unrelated_url():
    with pytest.raises(ValueError, match="doesn't contain"):
        common.parse_url_to_day("s3://noaa-goes16/ABI-L2-CMIPF/2023/109/x.nc", ["ABI-L1b-RadF"])


def test_channel_label_and_grid_size():
    assert common._ch(2) == "C02"
    assert common._ch(13) == "C13"
    # C02 is the half-kilometre band, four times the linear size of a 2km one.
    assert common._grid_size(2) == 21696
    assert common._grid_size(13) == 5424


def test_date_from_doy_round_trips():
    assert common._date_from_doy(2023, 109) == datetime.date(2023, 4, 19)
    assert common._date_from_doy(2024, 60) == datetime.date(2024, 2, 29)


def test_archive_day_index_counts_from_the_archive_start():
    start = datetime.date(2017, 2, 28)
    assert common._archive_day_index(2017, 59, start) == 0
    assert common._archive_day_index(2017, 60, start) == 1


# ---------------------------------------------------------------------------
# Codec validation
# ---------------------------------------------------------------------------
class _FakeBytesCodec:
    """Stands in for zarr's BytesCodec; only the attributes the check reads."""

    def __init__(self, endian="little"):
        self.endian = endian

    def __repr__(self):
        return f"BytesCodec(endian={self.endian!r})"


_FakeBytesCodec.__name__ = "BytesCodec"


class _FakeNumcodecBase:
    """Stands in for a numcodecs-backed zarr codec wrapper."""


def _numcodec(class_name, codec_name, **config):
    """A codec object whose class name and config the check will read."""
    cls = types.new_class(class_name, (_FakeNumcodecBase,))
    obj = cls()
    obj.codec_name = codec_name
    obj.codec_config = config
    return obj


def _zlib(level=1):
    return _numcodec("Zlib", "numcodecs.zlib", level=level)


def _shuffle(elementsize):
    return _numcodec("Shuffle", "numcodecs.shuffle", elementsize=elementsize)


PRE = {
    "Rad": [
        {"class": "BytesCodec", "endian": "little"},
        {"class": "Zlib", "codec_name": "numcodecs.zlib", "codec_config": {"level": 1}},
    ]
}
POST = {
    "Rad": [
        {"class": "BytesCodec", "endian": "little"},
        {
            "class": "Shuffle",
            "codec_name": "numcodecs.shuffle",
            "codec_config": {"elementsize": 2},
        },
        {"class": "Zlib", "codec_name": "numcodecs.zlib", "codec_config": {"level": 1}},
    ]
}


def test_codec_matches_compares_class_and_config():
    assert common._codec_matches(_FakeBytesCodec(), PRE["Rad"][0])
    assert not common._codec_matches(_FakeBytesCodec("big"), PRE["Rad"][0])
    assert common._codec_matches(_zlib(), PRE["Rad"][1])
    assert not common._codec_matches(_zlib(level=6), PRE["Rad"][1])


def test_codec_config_comparison_ignores_extra_keys_only():
    """Only the listed config keys must match; a missing one is a mismatch."""
    codec = _numcodec("Zlib", "numcodecs.zlib", level=1, extra="ignored")
    assert common._codec_matches(codec, PRE["Rad"][1])
    assert not common._codec_matches(
        _numcodec("Zlib", "numcodecs.zlib"), PRE["Rad"][1]
    )


def test_pipeline_matches_treats_an_absent_era_table_as_no_match():
    """A variable listed in only one era's table must not crash the check."""
    assert common._pipeline_matches([_FakeBytesCodec()], None) is False


def test_pipeline_matches_requires_the_same_length_and_order():
    pre = [_FakeBytesCodec(), _zlib()]
    post = [_FakeBytesCodec(), _shuffle(2), _zlib()]
    assert common._pipeline_matches(pre, PRE["Rad"])
    assert not common._pipeline_matches(pre, POST["Rad"])
    assert common._pipeline_matches(post, POST["Rad"])
    assert not common._pipeline_matches(post[::-1], POST["Rad"])


def test_is_codec_error_sees_through_the_validation_wrapper():
    inner = ValueError("Codec validation failed:\n  Rad: ...")
    assert common._is_codec_error(inner)
    assert common._is_codec_error(common.RadFValidationError("file.nc", inner))
    assert not common._is_codec_error(ValueError("no files found"))
    assert not common._is_codec_error(
        common.RadFValidationError("file.nc", KeyError("Rad"))
    )


def test_codec_change_detected_carries_the_boundary_date():
    exc = common.CodecChangeDetected(datetime.date(2023, 4, 19), "Rad: mismatch")
    assert exc.codec_change_date == datetime.date(2023, 4, 19)
    assert "2023-04-19" in str(exc)


# ---------------------------------------------------------------------------
# Unreadable source files
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "message",
    [
        "Unable to synchronously open file (truncated file: eof = 1234)",
        "Unable to open file (file signature not found)",
        "Unable to read data (bad object header version number)",
    ],
)
def test_h5py_corruption_is_recognised_as_an_unreadable_source(message):
    """h5py reports every one of these as a bare OSError, so the text is all there is."""
    assert common._is_unreadable_source_error(OSError(message))
    assert common._is_unreadable_source_error(
        common.RadFValidationError("file.nc", OSError(message))
    )


def test_our_own_failures_are_not_blamed_on_the_source():
    """The two are bounded separately, so the classifier must not over-claim."""
    assert not common._is_unreadable_source_error(ValueError("Codec validation failed"))
    assert not common._is_unreadable_source_error(KeyError("Rad"))
    # An OSError that is not corruption: a network blip is ours to retry, not a dead file.
    assert not common._is_unreadable_source_error(OSError("Connection reset by peer"))


def test_unreadable_days_get_a_far_higher_threshold_than_other_failures():
    """A corrupt file in NOAA's bucket will never succeed, but the good days past it
    still should.

    GK-2A ir105 hit a run of truncated files and abandoned the rest of two year-windows.
    """
    assert common.MAX_CONSECUTIVE_UNREADABLE_DAYS > common.MAX_CONSECUTIVE_FAILED_DAYS


# ---------------------------------------------------------------------------
# Malformed RadF files
# ---------------------------------------------------------------------------
def test_metadata_only_files_are_dropped_from_the_listing():
    """NOAA publishes the odd RadF file with every ancillary variable but no imagery.

    One is enough to make xarray fill ``Rad`` across the whole day, which indexes
    positionally into a ManifestArray and costs the entire channel-day. They are 1-2% of
    the median size, so the listing alone separates them.
    """
    urls = [f"s3://b/good{i}.nc" for i in range(10)] + ["s3://b/metadata_only.nc"]
    sizes = {u: 9_000_000 for u in urls}
    sizes["s3://b/metadata_only.nc"] = 81_000

    kept = common._drop_metadata_only_files(urls, sizes, 13)

    assert "s3://b/metadata_only.nc" not in kept
    assert len(kept) == 10


def test_a_normal_days_spread_of_sizes_is_left_alone():
    """Real files vary 6.9-14.8MB; nothing in that range may be mistaken for metadata."""
    sizes = {f"s3://b/f{i}.nc": s for i, s in enumerate([6_900_000, 9_000_000, 14_800_000])}
    urls = list(sizes)
    assert common._drop_metadata_only_files(urls, sizes, 13) == urls


def test_metadata_only_filter_needs_a_population_to_judge_against():
    """With one or two files there is no meaningful median, so nothing is dropped."""
    urls = ["s3://b/a.nc", "s3://b/b.nc"]
    sizes = {"s3://b/a.nc": 9_000_000, "s3://b/b.nc": 81_000}
    assert common._drop_metadata_only_files(urls, sizes, 13) == urls


def test_a_file_missing_a_required_variable_is_dropped(monkeypatch):
    """The full-size case size cannot catch: an observed file had ``Rad`` at normal shape
    and codecs but no ``DQF``, at 119% of the day's median.
    """
    import xarray as xr

    def fake_open(url, **kwargs):
        if url == "s3://b/b.nc":
            return xr.Dataset({"Rad": ("t", [1.0])})
        return xr.Dataset({"Rad": ("t", [1.0]), "DQF": ("t", [0.0])})

    monkeypatch.setattr(common.vz, "open_virtual_dataset", fake_open)
    kept = common.drop_files_missing_required_vars(
        ["s3://b/a.nc", "s3://b/b.nc", "s3://b/c.nc"], registry=None, parser=None
    )
    assert kept == ["s3://b/a.nc", "s3://b/c.nc"]


def test_a_file_that_cannot_be_opened_is_dropped_too(monkeypatch):
    """Unreadable here means unusable in the combine as well."""

    def fake_open(url, **kwargs):
        raise OSError("truncated file")

    monkeypatch.setattr(common.vz, "open_virtual_dataset", fake_open)
    kept = common.drop_files_missing_required_vars(["s3://b/a.nc"], registry=None, parser=None)
    assert kept == []


# ---------------------------------------------------------------------------
# Batch schema check
# ---------------------------------------------------------------------------
def test_validate_batch_data_vars_reports_missing_and_extra():
    import xarray as xr

    ds = xr.Dataset(
        {"Rad": ("t", [1.0]), "surprise": ("t", [2.0])},
        coords={"t": [np.datetime64("2023-04-19T12:00", "ns")]},
    )
    missing, extra = common.validate_batch_data_vars(ds, frozenset({"Rad", "DQF"}))
    assert missing == frozenset({"DQF"})
    assert extra == frozenset({"surprise"})


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
def test_source_bucket_defaults_to_the_public_archive():
    assert common.source_bucket("goes16") == "s3://noaa-goes16"
    assert common.source_bucket("goes19") == "s3://noaa-goes19"


def test_source_bucket_can_be_pointed_at_a_mirror(monkeypatch):
    monkeypatch.setenv("GOES16_SOURCE_BUCKET", "s3://my-mirror/")
    assert common.source_bucket("goes16") == "s3://my-mirror"


def test_source_bucket_rejects_an_unknown_satellite():
    with pytest.raises(ValueError, match="No source bucket known"):
        common.source_bucket("goes99")


def test_default_store_prefix_uses_the_configured_root(monkeypatch):
    assert common.default_store_prefix("goes18").endswith("goes18_radf.icechunk")
    monkeypatch.setenv("GOES_STORE_ROOT", "someone/else")
    assert common.default_store_prefix("goes18") == "someone/else/goes18_radf.icechunk"


def test_suffixed_prefix_inserts_channel_and_era_before_the_extension():
    base = "bkr/geo/virtualized/goes16_radf.icechunk"
    assert common.suffixed_prefix(base) == base
    assert (
        common.suffixed_prefix(base, channel=13)
        == "bkr/geo/virtualized/goes16_radf_C13.icechunk"
    )
    assert (
        common.suffixed_prefix(base, channel=2, era="2023-04-19")
        == "bkr/geo/virtualized/goes16_radf_C02_2023-04-19.icechunk"
    )


def test_suffixed_prefix_handles_a_prefix_without_the_extension():
    assert common.suffixed_prefix("goes16_radf", channel=1) == "goes16_radf_C01"


def test_store_prefix_resolves_through_the_shared_config(local_config):
    prefix = common.suffixed_prefix(
        common.default_store_prefix("goes16"), channel=13, era="2024-01-01"
    )
    resolved = local_config.store_path(prefix)
    assert resolved.endswith("goes16_radf_C13_2024-01-01.icechunk")
    assert str(local_config.icechunk_local_path) in resolved


def test_virtual_chunk_buckets_are_bare_names_and_deduplicated():
    buckets = common.virtual_chunk_buckets(["s3://noaa-goes16/", "s3://my-mirror"])
    assert set(buckets[:4]) == {
        "noaa-goes16",
        "noaa-goes17",
        "noaa-goes18",
        "noaa-goes19",
    }
    assert buckets[-1] == "my-mirror"
    assert len(buckets) == len(set(buckets))


def test_virtual_chunk_buckets_include_a_configured_mirror(monkeypatch):
    """A mirror must get a chunk container, or every chunk read of the store
    it produced would be unauthorized."""
    monkeypatch.setenv("GOES18_SOURCE_BUCKET", "s3://my-mirror-goes18")
    buckets = common.virtual_chunk_buckets()
    assert "my-mirror-goes18" in buckets
    # The defaults stay: an existing store's manifests still point at them.
    assert "noaa-goes18" in buckets


def test_default_log_dir_is_under_the_configured_data_dir(local_config):
    assert common.default_log_dir() == local_config.data_dir / "logs" / "virtualized"


def test_configure_ingest_process_bounds_glibc_arenas(monkeypatch):
    monkeypatch.delenv("MALLOC_ARENA_MAX", raising=False)
    common.configure_ingest_process()
    assert os.environ["MALLOC_ARENA_MAX"] == "2"


def test_rss_gb_reports_a_current_positive_figure():
    # Must be current, not a peak counter: the retention measurements the
    # ingest's tuning rests on are meaningless if it can never go down.
    assert common.rss_gb() > 0


# ---------------------------------------------------------------------------
# Codec-era splitting in the forward ingest
# ---------------------------------------------------------------------------
class _FakeIndex(list):
    """A `t` index just complete enough for the strictly-increasing check."""

    @property
    def is_monotonic_increasing(self):
        return all(a < b for a, b in zip(self, self[1:]))

    @property
    def is_unique(self):
        return len(set(self)) == len(self)


class _FakeVirtualDataset:
    """Stands in for the virtual dataset a batch opens.

    Only the surface `ingest_all_days` touches: the schema check, the `t`
    ordering check, the `vz.to_icechunk` write and `close`. A real
    `xr.Dataset` cannot be used because `.vz.to_icechunk` would need a live
    Icechunk store.
    """

    def __init__(self, day, sink):
        self.day = day
        self._sink = sink
        self.data_vars = {"Rad": None}
        self.coords = {"t": None}
        self.dims = ("t",)
        self.indexes = {"t": _FakeIndex([day])}
        self.closed = False
        self.vz = self._Accessor(self)

    class _Accessor:
        def __init__(self, ds):
            self._ds = ds

        def to_icechunk(self, store, group=None, append_dim=None):
            self._ds._sink.append((self._ds.day, append_dim))

    def close(self):
        self.closed = True


class _FakeSession:
    def __init__(self):
        self.store = object()

    def commit(self, msg):
        return "commit"


class _FakeRepo:
    """An Icechunk repository stand-in recording what was written to it."""

    def __init__(self, name, schema_exists=False):
        self.name = name
        self.schema_exists = schema_exists
        self.writes: list[tuple[int, str | None]] = []

    def writable_session(self, branch):
        return _FakeSession()


def _run_forward_ingest(
    monkeypatch,
    *,
    repos,
    codec_bad_from,
    last_committed=lambda r, b, g=None: None,
    broken_in_every_store=False,
):
    """Drive ingest_all_days offline over days 1-6, with a codec change part-way through.

    Days from ``codec_bad_from`` raise a codec error while the first store is
    current, which is how a real era boundary presents itself; with
    ``broken_in_every_store`` they fail against the new era's store too.
    """
    state = {"repo": repos[0]}

    monkeypatch.setattr(common, "MAX_CONSECUTIVE_FAILED_DAYS", 2)
    monkeypatch.setattr(common, "_schema_exists", lambda r, b, g=None: r.schema_exists)
    monkeypatch.setattr(common, "_last_committed_day", last_committed)

    def open_batch_fn(urls, **kwargs):
        day = int(urls[0])
        if day >= codec_bad_from and (broken_in_every_store or state["repo"] is repos[0]):
            raise ValueError("Codec validation failed:\n  Rad: mismatch")
        return _FakeVirtualDataset(day, state["repo"].writes)

    def repo_factory(suffix):
        state["repo"] = repos[1]
        return repos[1]

    common.ingest_all_days(
        repos[0],
        13,
        satellite_name="TEST",
        satellite="test",
        product_label="RadF",
        archive_start_date=datetime.date(2023, 1, 1),
        all_days=[((2023, d), [str(d)]) for d in range(1, 7)],
        preprocess_fn=lambda ds: ds,
        bucket="s3://noaa-goes16",
        open_batch_fn=open_batch_fn,
        repo_factory=repo_factory,
        epoch_threshold=np.datetime64("2017-01-01", "ns"),
        keep_data_vars=frozenset({"Rad"}),
        loadable_variables=(),
    )


def test_codec_change_retries_the_days_that_proved_the_boundary(monkeypatch):
    """The days that trip the threshold belong to the new era.

    They failed against the old store, so they are not in it; before the
    rewind they were never re-attempted either, leaving a
    MAX_CONSECUTIVE_FAILED_DAYS gap at every era boundary.
    """
    repos = [_FakeRepo("old"), _FakeRepo("new")]
    _run_forward_ingest(monkeypatch, repos=repos, codec_bad_from=4)

    assert [day for day, _ in repos[0].writes] == [1, 2, 3]
    assert [day for day, _ in repos[1].writes] == [4, 5, 6]
    # The new store was empty, so its first write creates the schema.
    assert repos[1].writes[0][1] is None
    assert [append for _, append in repos[1].writes[1:]] == ["t", "t"]


def test_codec_change_appends_to_an_era_store_that_already_exists(monkeypatch):
    """A re-run must append to the era store, not recreate it.

    Setting is_first_write unconditionally made the second run write with no
    append_dim, which recreates the arrays and drops what was committed. The
    old store's resume point must not leak across either.
    """
    repos = [_FakeRepo("old"), _FakeRepo("new", schema_exists=True)]
    _run_forward_ingest(
        monkeypatch,
        repos=repos,
        codec_bad_from=4,
        last_committed=lambda r, b, g=None: (
            ((2023, 4), np.datetime64("2023-01-04", "ns")) if r.schema_exists else None
        ),
    )

    # Day 4 is already committed to the era store, so only 5 and 6 are added,
    # and every write appends rather than recreating.
    assert [day for day, _ in repos[1].writes] == [5, 6]
    assert {append for _, append in repos[1].writes} == {"t"}


def test_a_day_that_keeps_failing_does_not_loop_forever(monkeypatch):
    """The rewind must not bounce between "new era" and "still failing"."""
    repos = [_FakeRepo("old"), _FakeRepo("new")]
    _run_forward_ingest(monkeypatch, repos=repos, codec_bad_from=4, broken_in_every_store=True)

    assert [day for day, _ in repos[0].writes] == [1, 2, 3]
    assert repos[1].writes == []


# ---------------------------------------------------------------------------
# Per-satellite config modules
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "module_name,satellite",
    [
        ("goes_16_radf", "goes16"),
        ("goes_17_radf", "goes17"),
        ("goes_18_radf", "goes18"),
        ("goes_19_radf", "goes19"),
    ],
)
def test_satellite_modules_agree_with_the_shared_config(module_name, satellite):
    import importlib

    mod = importlib.import_module(
        f"planetary_datasets.providers.virtualized.{module_name}"
    )
    assert mod.SATELLITE == satellite
    assert mod.BUCKET == common.DEFAULT_SOURCE_BUCKETS[satellite]
    assert mod.STORE_PREFIX.endswith(f"{satellite}_radf.icechunk")
    assert mod.EPOCH_THRESHOLD.dtype == common.DATETIME_NS


def test_unified_cli_resolves_the_store_from_the_config(local_config):
    from planetary_datasets.providers.virtualized import ingest_goes_radf

    args = ingest_goes_radf.build_args("goes17", end_date=datetime.date(2024, 5, 1))
    assert args.storage == "config"
    assert args.max_eras is None
    assert args.region == local_config.region
    summary = ingest_goes_radf._storage_summary(args)
    assert str(local_config.icechunk_local_path) in summary
    assert "goes17_radf.icechunk" in summary


def test_reported_store_path_matches_the_one_written_under_a_prefix(
    local_config, monkeypatch
):
    """The path the run prints must be the path it writes to.

    ICECHUNK_PREFIX is how a staging run is separated from production, so a
    summary that omits it would report staging while writing to production.
    """
    from planetary_datasets.providers.virtualized import ingest_goes_radf

    monkeypatch.setenv("ICECHUNK_PREFIX", "staging")
    config_module.reset_config_cache()
    cfg = config_module.get_config()

    args = ingest_goes_radf.build_args("goes16")
    assert "staging" in ingest_goes_radf._storage_summary(args)

    storage = ingest_goes_radf._storage_for(args, channel=13, date_suffix="2024-01-02")
    reported = cfg.store_path(
        common.suffixed_prefix(
            common.default_store_prefix("goes16"), channel=13, era="2024-01-02"
        )
    )
    assert str(storage).count("staging") == 1
    assert reported.endswith("goes16_radf_C13_2024-01-02.icechunk")
    assert "/staging/" in reported


def test_unified_cli_rejects_an_unknown_satellite():
    from planetary_datasets.providers.virtualized import ingest_goes_radf

    with pytest.raises(ValueError, match="Unknown satellite"):
        ingest_goes_radf._module_for("goes99")


def test_backwards_walk_gives_the_live_store_an_anchor_independent_name(monkeypatch):
    """A decommissioned satellite's newest store must have a stable name.

    The walk is clamped to the archive end. Naming the newest store after the
    anchor renamed it whenever the anchor moved, so no run resumed the last
    one. The newest era is now the live store and carries no era suffix at
    all, which is stable by construction — asserted here across two different
    anchors. The clamp still applies to the walk itself.
    """
    from planetary_datasets.providers.virtualized import ingest_goes_radf

    captured = {}

    def fake_ingest_backwards(channel, **kwargs):
        captured.update(kwargs)
        return []

    mod = ingest_goes_radf._module_for("goes17")
    monkeypatch.setattr(mod, "ARCHIVE_END_DATE", datetime.date(2023, 1, 10), raising=False)
    monkeypatch.setattr(common, "ingest_backwards", fake_ingest_backwards)

    for anchor in (datetime.date(2026, 9, 27), datetime.date(2027, 3, 1)):
        captured.clear()
        ingest_goes_radf.ingest_channel_backwards(
            lambda suffix: None,
            "goes17",
            13,
            end_date=anchor,
        )
        # Same store whichever anchor the run used.
        assert captured["first_store_suffix"] == ""
        # ...while the walk itself is still clamped to the archive end.
        assert captured["end_date"] == datetime.date(2023, 1, 10)


def test_date_to_fake_url_round_trips_through_parse_url_to_day():
    from planetary_datasets.providers.virtualized import ingest_goes_radf

    url = ingest_goes_radf._date_to_fake_url(
        datetime.date(2023, 4, 19), "ABI-L1b-RadF/"
    )
    assert common.parse_url_to_day(url, ["ABI-L1b-RadF"]) == (2023, 109)


# ---------------------------------------------------------------------------
# Dagster assets
# ---------------------------------------------------------------------------
@pytest.fixture
def gv():
    """The GOES virtual Dagster asset module."""
    pytest.importorskip("dagster")
    from dags.assets import goes_virtual

    return goes_virtual


def test_assets_carry_their_own_concurrency_key_and_a_long_runtime(gv):
    assert len(gv.all_assets) == len(gv.SATELLITES)
    for asset in gv.all_assets:
        tags = asset.node_def.tags
        assert tags["dagster/concurrency_key"] == gv.CONCURRENCY_KEY
        # A single channel-era is hours of full-disk scans.
        assert float(tags["dagster/max_runtime"]) >= 6 * 60 * 60


def test_assets_are_partitioned_by_every_abi_channel(gv):
    assert gv.channel_partitions.get_partition_keys() == [
        f"C{c:02d}" for c in range(1, 17)
    ]
    assert gv._channel_number("C02") == 2
    with pytest.raises(ValueError, match="Channel must be 1-16"):
        gv._channel_number("C99")
    with pytest.raises(ValueError, match="Bad channel partition key"):
        gv._channel_number("banana")


def test_anchor_is_unpinned_until_configured(gv):
    """An unpinned anchor must be visible to the caller, not silently today.

    The anchor names the newest era's store: defaulting it to today writes a
    new store every run and re-ingests the era from nothing.
    """
    end_date, max_eras, batch_size = gv._run_options({})
    assert end_date is None
    assert (max_eras, batch_size) == (1, 1)


def test_anchor_comes_from_the_environment_then_the_run_tag(gv, monkeypatch):
    monkeypatch.setenv("GOES_VIRTUAL_END_DATE", "2026-09-01")
    assert gv._run_options({})[0] == datetime.date(2026, 9, 1)
    # A run tag overrides the deployment-wide pin.
    assert gv._run_options({"goes_virtual/end_date": "2025-01-05"})[0] == datetime.date(
        2025, 1, 5
    )


def test_max_eras_all_means_every_era(gv):
    assert gv._run_options({"goes_virtual/max_eras": "all"})[1] is None
    assert gv._run_options({"goes_virtual/max_eras": "3"})[1] == 3
    assert gv._run_options({"goes_virtual/batch_size": "5"})[2] == 5


def _truncated_oserror(eof: int = 34603008, stored: int = 35675309) -> OSError:
    """The exact error a short read produces, indistinguishable from corruption."""
    return OSError(
        f"Unable to synchronously open file (truncated file: eof = {eof}, "
        f"sblock->base_addr = 0, stored_eof = {stored})"
    )


def test_a_short_read_is_retried_rather_than_treated_as_corruption():
    """A file that reads short once and fully next time must not be lost.

    Object storage surfaces a short read as a byte-identical HDF5 error to real
    corruption, and the store only appends along `t`, so believing the first
    attempt discards the day permanently. GK-2A ir105 lost 581 of 909 days this
    way while the files were intact in the bucket.
    """
    attempts = []

    def flaky(urls):
        attempts.append(urls)
        if len(attempts) < 3:
            raise _truncated_oserror()
        return "dataset"

    slept = []
    result = common.open_with_unreadable_retry(
        flaky, ["a.nc"], sleep=slept.append
    )

    assert result == "dataset"
    assert len(attempts) == 3
    # Backs off further each time, since short reads cluster under concurrency.
    assert slept == [5.0, 10.0]


def test_a_genuinely_corrupt_file_still_gives_up():
    """Retrying must not turn a permanent failure into an unbounded loop."""
    attempts = []

    def always_bad(urls):
        attempts.append(urls)
        raise _truncated_oserror()

    with pytest.raises(OSError, match="truncated file"):
        common.open_with_unreadable_retry(
            always_bad, ["a.nc"], sleep=lambda _: None
        )
    assert len(attempts) == common.UNREADABLE_RETRY_ATTEMPTS
    # Still classified as unreadable, so the caller keeps counting it against
    # MAX_CONSECUTIVE_UNREADABLE_DAYS rather than the much tighter failure cap.
    assert common._is_unreadable_source_error(_truncated_oserror())


def test_a_codec_boundary_is_not_retried():
    """Only unreadable-source errors retry; a codec change must raise at once.

    An era boundary is detected by the open failing, so retrying it would delay
    every era split by the full backoff for no benefit.
    """
    attempts = []

    def codec_error(urls):
        attempts.append(urls)
        raise NotImplementedError(
            "The ManifestArray class cannot concatenate arrays which were "
            "stored using different codecs"
        )

    with pytest.raises(NotImplementedError, match="different codecs"):
        common.open_with_unreadable_retry(
            codec_error, ["a.nc"], sleep=lambda _: None
        )
    assert len(attempts) == 1


def test_a_short_read_during_probing_does_not_split_an_era():
    """The probe must retry too, or a transient read closes an era by mistake.

    A failed probe open is not a skipped day: it reads as "does not combine",
    which is how era boundaries are found. A short read there strands every day
    beyond it in a separate store. This is what GK-2A ir105 was doing -- its
    losses were logged as PROBE_MISMATCH, not as ingest failures.
    """
    calls = []

    def flaky_probe(urls):
        calls.append(urls)
        if len(calls) == 1:
            raise _truncated_oserror()
        return "probe-dataset"

    result = common.open_with_unreadable_retry(
        flaky_probe, ["a.nc", "b.nc"], sleep=lambda _: None
    )

    # Second attempt succeeded, so the caller sees a combinable day and the era
    # stays open rather than being split at a file that was never really bad.
    assert result == "probe-dataset"
    assert len(calls) == 2


def test_a_mission_with_its_own_repair_can_use_it_for_an_unreadable_file():
    """GK-2A's repair drops files it cannot read, but could never be reached.

    The batch handler only caught NotImplementedError and demanded "codec" in
    the message, while a truncated file raises OSError. So one bad file in
    NOAA's bucket cost the whole band-day, permanently -- the store appends
    along `t`, so a re-run skips the day rather than filling it.
    """
    truncated = _truncated_oserror()
    codec = NotImplementedError("cannot concatenate ... different codecs")
    unrelated = NotImplementedError("something else entirely")

    def gate(exc):
        # Mirrors the condition in ingest_all_days.
        return "codec" in str(exc).lower() or common._is_unreadable_source_error(exc)

    assert gate(truncated), "a truncated file must now reach the repair"
    assert gate(codec), "codec outliers must still reach the repair"
    assert not gate(unrelated), "an unrelated error must still propagate"


def test_himawari_retries_a_scene_that_would_not_stitch():
    """A tile-based mission cannot drop a file, so its transient case retries.

    build_batch deliberately refuses to commit a partial day: dropping a scene
    would leave a gap the append-only store could never fill. Its documented
    common cause -- a slot still uploading when the day was listed -- is a
    retry case, so IncompleteBatch is declared retryable.
    """
    from planetary_datasets.providers.virtualized import himawari_isatss

    is_retryable = lambda e: isinstance(e, himawari_isatss.IncompleteBatch)
    calls = []

    def flaky(urls):
        calls.append(urls)
        if len(calls) == 1:
            raise himawari_isatss.IncompleteBatch("stitched 143 of 144 scenes")
        return "batch"

    result = common.open_with_unreadable_retry(
        flaky, ["t.nc"], is_retryable=is_retryable, sleep=lambda _: None
    )
    assert result == "batch"
    assert len(calls) == 2

    # An unreadable file is still retried as well, not replaced by this.
    assert common._is_unreadable_source_error(_truncated_oserror())
    # ...and an unrelated error is still not retried.
    other = []
    with pytest.raises(ValueError):
        common.open_with_unreadable_retry(
            lambda u: other.append(u) or (_ for _ in ()).throw(ValueError("nope")),
            ["t.nc"], is_retryable=is_retryable, sleep=lambda _: None,
        )
    assert len(other) == 1
