"""ECMWF MARS configuration wiring.

Offline only: a real retrieval takes hours and needs ECMWF credentials, so
these tests check that every path, store and credential the MARS modules use
is resolved from :mod:`planetary_datasets.config`, and that the CLI
entrypoints still parse.
"""

from __future__ import annotations

import contextlib
import dataclasses
import os
import pathlib
import subprocess
import sys

import pandas as pd
import pytest

from planetary_datasets.config import DEFAULT_BUCKET, MissingCredential, load_config
from planetary_datasets.memory import MemoryLimitExceeded
from planetary_datasets.providers import mars, mars_icechunk as mi, mars_pipeline as mp

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent


@contextlib.contextmanager
def loaded_env(path):
    """Load a dotenv into a ``Config`` and take its keys back out of ``os.environ``.

    ``load_config`` goes through ``load_dotenv``, which copies every key into
    the real environment and leaves it there. Without this, a fixture's values
    would leak into every test that ran afterwards.
    """
    before = dict(os.environ)
    try:
        yield load_config(env_file=path)
    finally:
        for key in set(os.environ) - set(before):
            del os.environ[key]
        os.environ.update(before)


@pytest.fixture
def mars_config(tmp_path):
    """A config whose data, scratch and store directories are all under tmp_path.

    Written as a real ``.env`` rather than patched in, so the test exercises
    the same path a deployment takes.
    """
    env = tmp_path / ".env"
    env.write_text(
        f"PLANETARY_DATASETS_DATA_DIR={tmp_path / 'data'}\n"
        f"PLANETARY_DATASETS_SCRATCH_DIR={tmp_path / 'scratch'}\n"
        f"ICECHUNK_LOCAL_PATH={tmp_path / 'stores'}\n"
        "AWS_PROFILE=mars-writer\n"
        # A fixed budget, so the tests that assert on memory sizing do not
        # depend on how much of the machine happens to be free.
        "MEMORY_CEILING_GB=48\n"
    )
    with loaded_env(env) as cfg:
        yield cfg


def _pipeline(config, **kwargs) -> mp.MarsPipeline:
    return mp.MarsPipeline(
        start=pd.Timestamp("2026-01-01"), end=pd.Timestamp("2026-01-02"), config=config, **kwargs
    )


# -- directories -------------------------------------------------------


def test_source_and_staging_directories_come_from_the_config(mars_config, tmp_path):
    assert mi.default_source_dir(mars_config) == tmp_path / "data" / "mars"
    assert mi.default_staging_dir(mars_config) == tmp_path / "scratch" / "mars_staging"


def test_no_machine_specific_paths_or_buckets_are_hardcoded():
    # The three machine-specific literals the modules used to carry, plus the
    # published bucket, which now only exists as a `config.py` default. The
    # old module-level names are checked too, since a reintroduced constant
    # would most likely come back under its old spelling.
    forbidden = [
        "/ext_data",
        "/nvme",
        DEFAULT_BUCKET,
        "DEFAULT_SOURCE_DIR",
        "DEFAULT_STAGING_DIR",
        "DEFAULT_AWS_PROFILE",
        "SOURCE_COOP_PATH",
    ]
    for name in ("mars.py", "mars_icechunk.py", "mars_pipeline.py"):
        source = (REPO_ROOT / "planetary_datasets" / "providers" / name).read_text()
        for literal in forbidden:
            assert literal not in source, f"{name} still mentions {literal}"


# -- stores ------------------------------------------------------------


def test_the_store_prefix_resolves_through_the_config(mars_config, tmp_path):
    provider = mi.build_provider(config=mars_config)
    assert provider.store_prefix == mi.STORE_PREFIX
    assert provider.icechunk_path == str(tmp_path / "stores" / mi.STORE_PREFIX)


def test_the_store_is_an_s3_uri_without_a_local_path(tmp_path):
    cfg = load_config(env_file=tmp_path / "absent.env")
    provider = mi.build_provider(config=cfg)
    assert provider.icechunk_path == f"s3://{cfg.bucket}/{mi.STORE_PREFIX}"


def test_regrid_stores_sit_beside_the_native_one(mars_config):
    provider = mi.build_provider(config=mars_config)
    quarter = mi.MARSRegridProvider(provider, resolution=0.25)
    degree = mi.MARSRegridProvider(provider, resolution=1.0)
    assert quarter.store_prefix == "bkr/ifs/ifs_native_025.icechunk"
    assert degree.store_prefix == "bkr/ifs/ifs_native_100.icechunk"
    # The regrid inherits the native provider's configuration, so a test run
    # cannot have one store local and the other on S3.
    assert quarter.config is provider.config


def test_a_regrid_prefix_must_name_an_icechunk_store():
    with pytest.raises(ValueError, match="does not end in .icechunk"):
        mi.regrid_store_prefix("bkr/ifs/ifs_native", 0.25)


def test_the_repository_opens_against_the_local_store(mars_config):
    provider = mi.build_provider(config=mars_config)
    repo = provider.get_icechunk_repo()
    assert repo.readonly_session("main") is not None
    assert pathlib.Path(provider.icechunk_path).is_dir()


def test_an_explicit_profile_overrides_the_configured_one(mars_config):
    assert mars_config.credentials.aws_profile == "mars-writer"
    provider = mi.build_provider(aws_profile="source-coop", config=mars_config)
    assert provider.aws_profile == "source-coop"
    # The override must not disturb the rest of the configuration.
    assert provider.icechunk_path == mars_config.store_path(mi.STORE_PREFIX)
    # Without one, whatever the configuration resolved stands.
    assert mi.build_provider(config=mars_config).aws_profile == "mars-writer"
    # And a regrid built from the provider keeps the override.
    assert mi.MARSRegridProvider(provider, 0.25).aws_profile == "source-coop"


# -- credentials -------------------------------------------------------


def test_mars_service_requires_ecmwf_credentials(tmp_path, monkeypatch):
    for var in ("ECMWF_API_KEY", "ECMWF_API_EMAIL", "ECMWF_API_URL"):
        monkeypatch.delenv(var, raising=False)
    # No ~/.ecmwfapirc to fall back to, so the missing key has to be reported
    # rather than quietly downgraded to anonymous access.
    monkeypatch.setattr(pathlib.Path, "home", classmethod(lambda cls: tmp_path))
    monkeypatch.setattr(mars, "get_config", lambda: load_config(env_file=tmp_path / "absent.env"))
    with pytest.raises(MissingCredential, match="ECMWF_API_EMAIL"):
        mars.mars_service()


def test_mars_service_passes_the_configured_credentials(tmp_path, monkeypatch):
    env = tmp_path / ".env"
    env.write_text("ECMWF_API_KEY=abc123\nECMWF_API_EMAIL=someone@example.com\n")
    cfg = load_config(env_file=env)
    monkeypatch.setattr(mars, "get_config", lambda: cfg)

    captured = {}

    class FakeService:
        def __init__(self, service, url=None, key=None, email=None):
            captured.update(service=service, url=url, key=key, email=email)

    monkeypatch.setattr(mars, "_ecmwf_service_class", lambda: FakeService)
    mars.mars_service()
    # All three have to be passed together: ecmwfapi throws away what it was
    # given and re-reads the environment as soon as any one of them is None.
    assert captured == {
        "service": "mars",
        "url": mars.DEFAULT_ECMWF_API_URL,
        "key": "abc123",
        "email": "someone@example.com",
    }


# -- partial retrievals ------------------------------------------------


@pytest.mark.parametrize(
    "error",
    [
        RuntimeError("MARS gave up"),
        # The likeliest way this leaks, and not an Exception: nothing else catches it
        # part way through a multi-hour retrieval.
        KeyboardInterrupt(),
    ],
    ids=["failed", "interrupted"],
)
def test_a_failed_retrieval_leaves_no_partial_file(tmp_path, monkeypatch, error):
    """Regression: the .tmp orphan was invisible to every cleanup path.

    A few of those (up to 75GB each) filled the data array, and the free-space check then parked
    downloads forever with nothing it could delete.
    """

    class FailingService:
        def execute(self, request, target):
            pathlib.Path(target).write_bytes(b"half a grib")
            raise error

    monkeypatch.setattr(mars, "mars_service", FailingService)

    with pytest.raises(type(error)):
        mars.retrieve_mars({"class": "od"}, tmp_path / "output_20240101_20240103.grib")

    assert list(tmp_path.iterdir()) == []


def test_the_pipeline_sweeps_partial_downloads_left_by_a_killed_run(tmp_path):
    """`retrieve` cleans up after itself on every exit path; a killed process cannot."""
    pipeline = mp.MarsPipeline.__new__(mp.MarsPipeline)
    pipeline.source_dir = tmp_path
    pipeline.native = type("Native", (), {"pattern": "output_*.grib"})()

    orphan = tmp_path / "output_20240101_20240103.grib.tmp"
    orphan.write_bytes(b"partial")
    keep = tmp_path / "output_20240104_20240106.grib"
    keep.write_bytes(b"complete")

    pipeline._sweep_partial_downloads()

    assert not orphan.exists()
    assert keep.exists()


# -- retrieval planning ------------------------------------------------


def test_retrievals_default_to_the_configured_data_directory(mars_config, monkeypatch, tmp_path):
    monkeypatch.setattr(mars, "get_config", lambda: mars_config)
    assert mars.default_target_dir() == tmp_path / "data" / "mars"


def test_planned_jobs_land_in_the_source_directory(tmp_path):
    jobs = mp.plan_jobs(pd.Timestamp("2026-01-01"), pd.Timestamp("2026-01-02"), tmp_path)
    assert jobs
    assert all(job.target.parent == tmp_path for job in jobs)
    assert all(job.group in mi.GROUPS for job in jobs)


# -- memory ------------------------------------------------------------


def test_a_block_larger_than_the_budget_is_refused(monkeypatch):
    monkeypatch.setenv("MEMORY_CEILING_GB", "0.001")
    with pytest.raises(MemoryLimitExceeded, match="GRIB block"):
        mi._load_fields([], (mi.N_MODEL_LEVELS, mi.N_VALUES), "level", False, 24)


def test_each_ingest_worker_gets_a_share_of_the_injected_budget(mars_config):
    # 48 GB comes from the fixture's .env and nothing else: the process-wide
    # config is untouched here, so this fails if the budget is resolved from
    # ambient state rather than from the Config the pipeline was handed.
    assert mars_config.memory_ceiling_gb == 48.0
    pipeline = _pipeline(mars_config, ingest_workers=3)
    assert pipeline.memory_ceiling_gb == pytest.approx(16.0)
    assert pipeline.cfg["memory_ceiling_gb"] == pytest.approx(16.0)
    # The worker's config carries it too, so `_worker_state` can publish it.
    assert pipeline.cfg["config"].memory_ceiling_gb == 48.0


def test_slab_size_follows_the_injected_budget(mars_config):
    # A quarter of the injected 48 GB is over the tuned default, so the
    # default stands; the regrid provider must not consult the ambient config.
    assert mi.default_slab_bytes(mars_config) == mi.SLAB_BYTES
    provider = mi.build_provider(config=mars_config)
    assert mi.MARSRegridProvider(provider, 0.25).slab_bytes == mi.SLAB_BYTES

    tight = dataclasses.replace(mars_config, memory_ceiling_gb=2.0)
    assert mi.default_slab_bytes(tight) == 512 << 20
    assert mi.MARSRegridProvider(mi.build_provider(config=tight), 0.25).slab_bytes == 512 << 20


# -- the pipeline as a whole -------------------------------------------


def test_the_pipeline_resolves_every_path_from_the_config(mars_config, tmp_path):
    pipeline = _pipeline(mars_config)
    assert pipeline.source_dir == (tmp_path / "data" / "mars").resolve()
    assert pipeline.staging_dir == tmp_path / "scratch" / "mars_staging"
    assert pipeline.native.index_path == pipeline.source_dir / "mars_grib_index.parquet"
    assert pipeline.native.icechunk_path == str(tmp_path / "stores" / mi.STORE_PREFIX)
    assert [t.store_prefix for t in pipeline.targets] == [
        "bkr/ifs/ifs_native_025.icechunk",
        "bkr/ifs/ifs_native_100.icechunk",
    ]


def test_describe_reports_the_resolved_configuration(mars_config, tmp_path):
    described = _pipeline(mars_config).describe()
    assert str(tmp_path / "scratch" / "mars_staging") in described
    assert str(tmp_path / "stores" / mi.STORE_PREFIX) in described
    assert "retrievals:" in described


def test_a_range_is_required_when_there_is_no_grib_on_disk(mars_config):
    with pytest.raises(ValueError, match="no output_..grib files"):
        mp.MarsPipeline(config=mars_config)


# -- entrypoints -------------------------------------------------------


@pytest.mark.parametrize(
    "module",
    [
        "planetary_datasets.providers.mars",
        "planetary_datasets.providers.mars_icechunk",
        "planetary_datasets.providers.mars_pipeline",
    ],
)
def test_the_command_line_entrypoints_still_parse(module):
    result = subprocess.run(
        [sys.executable, "-m", module, "--help"],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        timeout=180,
    )
    assert result.returncode == 0, result.stderr
    assert "usage:" in result.stdout
