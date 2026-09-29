"""Configuration loading and store-path resolution."""

from __future__ import annotations

import pytest

from planetary_datasets.config import (
    DEFAULT_BUCKET,
    Credentials,
    MissingCredential,
    load_config,
)




def test_defaults_when_nothing_is_set(load_env_config):
    cfg = load_env_config()
    assert cfg.bucket == DEFAULT_BUCKET
    assert cfg.region == "us-west-2"
    assert cfg.use_local_store is False
    assert cfg.memory_fraction == 0.8
    assert cfg.endpoint_url is None
    assert cfg.allow_http is False
    assert cfg.store_path("bkr/dmi/hawaii.icechunk") == f"s3://{DEFAULT_BUCKET}/bkr/dmi/hawaii.icechunk"


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
    assert load_config(env_file=env).bucket == "from-environment"


def test_local_path_redirects_every_store(load_env_config, tmp_path):
    cfg = load_env_config(ICECHUNK_LOCAL_PATH=str(tmp_path / "stores"))
    assert cfg.use_local_store is True
    assert cfg.store_path("bkr/x.icechunk") == str(tmp_path / "stores" / "bkr/x.icechunk")


def test_local_storage_creates_the_directory(load_env_config, tmp_path):
    load_env_config(ICECHUNK_LOCAL_PATH=str(tmp_path / "stores")).icechunk_storage("bkr/x.icechunk")
    assert (tmp_path / "stores" / "bkr/x.icechunk").is_dir()


def test_prefix_is_prepended(load_env_config):
    cfg = load_env_config(ICECHUNK_PREFIX="staging")
    assert cfg.store_path("bkr/x.icechunk") == f"s3://{DEFAULT_BUCKET}/staging/bkr/x.icechunk"


def test_blank_values_are_treated_as_unset(load_env_config):
    assert load_env_config(AWS_ACCESS_KEY_ID="   ").credentials.aws_access_key_id is None


def test_require_returns_present_credentials():
    creds = Credentials(earthdata_username="u", earthdata_password="p")
    assert creds.require("earthdata_username", "earthdata_password") == ("u", "p")


def test_require_names_what_is_missing():
    creds = Credentials(earthdata_username="u")
    with pytest.raises(MissingCredential, match="EARTHDATA_PASSWORD"):
        creds.require("earthdata_username", "earthdata_password")


@pytest.mark.parametrize(
    "env",
    [
        # icechunk.s3_storage takes no `profile` argument, so a profile must go via the env.
        {"AWS_PROFILE": "sc"},
        {"AWS_ACCESS_KEY_ID": "key", "AWS_SECRET_ACCESS_KEY": "secret"},
    ],
    ids=["profile", "static-keys"],
)
def test_storage_builds_for_each_credential_style(load_env_config, env):
    assert load_env_config(**env).icechunk_storage("bkr/x.icechunk") is not None


def test_custom_endpoint_is_used(load_env_config):
    """Most stores live at bucket `bkr` behind source.coop's own endpoint."""
    cfg = load_env_config(ICECHUNK_BUCKET="bkr", ICECHUNK_ENDPOINT_URL="https://data.source.coop")
    assert cfg.endpoint_url == "https://data.source.coop"
    # Not set explicitly, but must be applied: a dotted bucket cannot use virtual-host
    # addressing over TLS.
    assert cfg.force_path_style is None
    assert cfg.icechunk_storage("geo/himawari_1km.icechunk") is not None


def test_path_style_can_be_forced_off(load_env_config):
    cfg = load_env_config(ICECHUNK_ENDPOINT_URL="https://data.source.coop", ICECHUNK_FORCE_PATH_STYLE="false")
    assert cfg.force_path_style is False


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("1", True), ("true", True), ("YES", True), ("on", True), ("0", False), ("false", False), ("no", False)],
)
def test_bool_env_parsing(load_env_config, raw, expected):
    assert load_env_config(ICECHUNK_ALLOW_HTTP=raw).allow_http is expected


def test_bad_memory_fraction_is_rejected(load_env_config):
    with pytest.raises(ValueError, match="MEMORY_FRACTION"):
        load_env_config(MEMORY_FRACTION="not-a-number")
