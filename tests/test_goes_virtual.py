"""Offline tests for the GOES virtualized ingest.

Everything here is a pure function or a config lookup: no network, no store.
The filename parsing and codec validation are where a silent regression would
be most expensive — a bad scan-start parse writes data under the wrong `t`,
and a codec check that accepts everything lets two eras into one store.
"""

from __future__ import annotations

import datetime
import types

import numpy as np
import pytest

from planetary_datasets import config as config_module
from planetary_datasets.providers.virtualized import goes_radf_common as common


@pytest.fixture
def local_store_config(tmp_path, monkeypatch):
    """Make ``get_config()`` itself resolve to a store under ``tmp_path``.

    The ingest reads the process-wide config rather than being handed one, so
    the shared ``local_config`` fixture is not enough here: this also points
    ``REPO_ROOT`` at an empty directory so a developer's real ``.env`` cannot
    leak back in through ``load_dotenv``.
    """
    monkeypatch.setattr(config_module, "REPO_ROOT", tmp_path)
    monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "stores"))
    monkeypatch.setenv("PLANETARY_DATASETS_DATA_DIR", str(tmp_path / "data"))
    config_module.reset_config_cache()
    yield config_module.get_config()
    config_module.reset_config_cache()


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


def test_aborted_scans_are_those_whose_start_and_end_tokens_match():
    aborted = RADF_URL.replace("e20231091209525", "e20231091200205")
    assert common._is_aborted_scan(aborted)
    assert not common._is_aborted_scan(RADF_URL)


def test_aborted_scan_check_tolerates_a_filename_without_tokens():
    assert not common._is_aborted_scan("s3://noaa-goes16/index.html")


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


def test_store_prefix_resolves_through_the_shared_config(local_store_config):
    prefix = common.suffixed_prefix(
        common.default_store_prefix("goes16"), channel=13, era="2024-01-01"
    )
    resolved = local_store_config.store_path(prefix)
    assert resolved.endswith("goes16_radf_C13_2024-01-01.icechunk")
    assert str(local_store_config.icechunk_local_path) in resolved


def test_virtual_chunk_buckets_are_bare_names_and_deduplicated():
    buckets = common.virtual_chunk_buckets(["s3://noaa-goes16/", "s3://my-mirror"])
    assert buckets[:4] == ["noaa-goes16", "noaa-goes17", "noaa-goes18", "noaa-goes19"]
    assert buckets[-1] == "my-mirror"
    assert len(buckets) == len(set(buckets))


def test_default_log_dir_is_under_the_configured_data_dir(local_store_config):
    assert common.default_log_dir() == local_store_config.data_dir / "logs" / "virtualized"


def test_configure_ingest_process_bounds_glibc_arenas(monkeypatch):
    monkeypatch.delenv("MALLOC_ARENA_MAX", raising=False)
    common.configure_ingest_process()
    assert __import__("os").environ["MALLOC_ARENA_MAX"] == "2"


def test_rss_gb_reports_a_current_positive_figure():
    # Must be current, not a peak counter: the retention measurements the
    # ingest's tuning rests on are meaningless if it can never go down.
    assert common.rss_gb() > 0


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


def test_unified_cli_resolves_the_store_from_the_config(local_store_config):
    from planetary_datasets.providers.virtualized import ingest_goes_radf

    args = ingest_goes_radf.build_args("goes17", end_date=datetime.date(2024, 5, 1))
    assert args.storage == "config"
    assert args.max_eras is None
    assert args.region == local_store_config.region
    summary = ingest_goes_radf._storage_summary(args)
    assert str(local_store_config.icechunk_local_path) in summary
    assert "goes17_radf.icechunk" in summary


def test_unified_cli_rejects_an_unknown_satellite():
    from planetary_datasets.providers.virtualized import ingest_goes_radf

    with pytest.raises(ValueError, match="Unknown satellite"):
        ingest_goes_radf._module_for("goes99")


def test_date_to_fake_url_round_trips_through_parse_url_to_day():
    from planetary_datasets.providers.virtualized import ingest_goes_radf

    url = ingest_goes_radf._date_to_fake_url(
        datetime.date(2023, 4, 19), "ABI-L1b-RadF/"
    )
    assert common.parse_url_to_day(url, ["ABI-L1b-RadF"]) == (2023, 109)
