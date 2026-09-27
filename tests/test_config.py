"""Configuration loading and store-path resolution."""

from __future__ import annotations

import pytest

from planetary_datasets.config import (
    DEFAULT_BUCKET,
    Credentials,
    MissingCredential,
    load_config,
)


def test_defaults_when_nothing_is_set(tmp_path):
    cfg = load_config(env_file=tmp_path / "absent.env")
    assert cfg.bucket == DEFAULT_BUCKET
    assert cfg.region == "us-west-2"
    assert cfg.use_local_store is False
    assert cfg.memory_fraction == 0.8


def test_env_file_is_read(tmp_path):
    env = tmp_path / ".env"
    env.write_text("ICECHUNK_BUCKET=my-bucket\nAWS_REGION=eu-west-1\n")
    cfg = load_config(env_file=env)
    assert cfg.bucket == "my-bucket"
    assert cfg.region == "eu-west-1"


def test_real_environment_beats_env_file(tmp_path, monkeypatch):
    env = tmp_path / ".env"
    env.write_text("ICECHUNK_BUCKET=from-file\n")
    monkeypatch.setenv("ICECHUNK_BUCKET", "from-environment")
    cfg = load_config(env_file=env)
    assert cfg.bucket == "from-environment"


def test_store_path_is_an_s3_uri_by_default(tmp_path):
    cfg = load_config(env_file=tmp_path / "absent.env")
    assert cfg.store_path("bkr/dmi/hawaii.icechunk") == f"s3://{DEFAULT_BUCKET}/bkr/dmi/hawaii.icechunk"


def test_local_path_redirects_every_store(tmp_path, monkeypatch):
    monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "stores"))
    cfg = load_config(env_file=tmp_path / "absent.env")
    assert cfg.use_local_store is True
    assert cfg.store_path("bkr/x.icechunk") == str(tmp_path / "stores" / "bkr/x.icechunk")
    assert "s3://" not in cfg.store_path("bkr/x.icechunk")


def test_prefix_is_prepended(tmp_path, monkeypatch):
    monkeypatch.setenv("ICECHUNK_PREFIX", "staging")
    cfg = load_config(env_file=tmp_path / "absent.env")
    assert cfg.store_path("bkr/x.icechunk") == f"s3://{DEFAULT_BUCKET}/staging/bkr/x.icechunk"


def test_blank_values_are_treated_as_unset(tmp_path, monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "   ")
    cfg = load_config(env_file=tmp_path / "absent.env")
    assert cfg.credentials.aws_access_key_id is None


def test_require_returns_present_credentials():
    creds = Credentials(earthdata_username="u", earthdata_password="p")
    assert creds.require("earthdata_username", "earthdata_password") == ("u", "p")


def test_require_names_what_is_missing():
    creds = Credentials(earthdata_username="u")
    with pytest.raises(MissingCredential, match="EARTHDATA_PASSWORD"):
        creds.require("earthdata_username", "earthdata_password")


def test_storage_builds_for_each_credential_style(tmp_path, monkeypatch):
    """icechunk.s3_storage takes no `profile` argument, so a profile must go via the env."""
    monkeypatch.setenv("AWS_PROFILE", "sc")
    cfg = load_config(env_file=tmp_path / "absent.env")
    assert cfg.icechunk_storage("bkr/x.icechunk") is not None

    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    cfg = load_config(env_file=tmp_path / "absent.env")
    assert cfg.icechunk_storage("bkr/x.icechunk") is not None


def test_custom_endpoint_is_used(tmp_path, monkeypatch):
    """Most stores live at bucket `bkr` behind source.coop's own endpoint."""
    monkeypatch.setenv("ICECHUNK_BUCKET", "bkr")
    monkeypatch.setenv("ICECHUNK_ENDPOINT_URL", "https://data.source.coop")
    cfg = load_config(env_file=tmp_path / "absent.env")
    assert cfg.endpoint_url == "https://data.source.coop"
    assert cfg.icechunk_storage("geo/himawari_1km.icechunk") is not None


def test_path_style_defaults_on_with_a_custom_endpoint(tmp_path, monkeypatch):
    monkeypatch.setenv("ICECHUNK_ENDPOINT_URL", "https://data.source.coop")
    cfg = load_config(env_file=tmp_path / "absent.env")
    # Not set explicitly, but must be applied: a dotted bucket cannot use virtual-host
    # addressing over TLS.
    assert cfg.force_path_style is None
    assert cfg.icechunk_storage("x.icechunk") is not None


def test_path_style_can_be_forced_off(tmp_path, monkeypatch):
    monkeypatch.setenv("ICECHUNK_ENDPOINT_URL", "https://data.source.coop")
    monkeypatch.setenv("ICECHUNK_FORCE_PATH_STYLE", "false")
    cfg = load_config(env_file=tmp_path / "absent.env")
    assert cfg.force_path_style is False


def test_endpoint_is_absent_by_default(tmp_path):
    cfg = load_config(env_file=tmp_path / "absent.env")
    assert cfg.endpoint_url is None
    assert cfg.allow_http is False


def test_bool_env_parsing(tmp_path, monkeypatch):
    for raw, expected in [("1", True), ("true", True), ("YES", True), ("on", True),
                          ("0", False), ("false", False), ("no", False)]:
        monkeypatch.setenv("ICECHUNK_ALLOW_HTTP", raw)
        assert load_config(env_file=tmp_path / "absent.env").allow_http is expected


def test_local_storage_creates_the_directory(tmp_path, monkeypatch):
    monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "stores"))
    cfg = load_config(env_file=tmp_path / "absent.env")
    cfg.icechunk_storage("bkr/x.icechunk")
    assert (tmp_path / "stores" / "bkr/x.icechunk").is_dir()


def test_bad_memory_fraction_is_rejected(tmp_path, monkeypatch):
    monkeypatch.setenv("MEMORY_FRACTION", "not-a-number")
    with pytest.raises(ValueError, match="MEMORY_FRACTION"):
        load_config(env_file=tmp_path / "absent.env")
