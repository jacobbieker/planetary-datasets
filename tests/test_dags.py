"""Tests for the Dagster code location: the provider asset factory and auto-discovery."""

from __future__ import annotations

import pathlib
import sys
import textwrap

import dagster as dg
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from helpers import read_store
from dags import loader as loader_module
from dags.factory import (
    DEFAULT_MEMORY_GB,
    MEMORY_CLASS_TAG,
    MEMORY_GB_TAG,
    concurrency_limit_for,
    daily_partitions,
    executor_tag_concurrency_limits,
    make_provider_asset,
    make_provider_assets,
    memory_class_for,
)
from planetary_datasets import config as config_module
from planetary_datasets.base import BaseProvider

PARTITION = "2024-06-01"


class TinyProvider(BaseProvider):
    """A provider that invents one tiny dataset per timestep."""

    name = "tiny-provider"
    append_dim = "time"
    store_prefix = "test/tiny.icechunk"

    def fetch(self, it, temp_dir=None, **kwargs):
        return ["synthetic"]

    def process(self, input_files, it, temp_dir=None, **kwargs):
        return xr.Dataset(
            {"value": (("time",), np.array([float(it.hour)], dtype="float32"))},
            coords={"time": pd.DatetimeIndex([it])},
        )


class AlwaysFailsProvider(TinyProvider):
    name = "always-fails-provider"
    store_prefix = "test/always_fails.icechunk"

    def fetch(self, it, temp_dir=None, **kwargs):
        raise RuntimeError("upstream is down")


def materialisation_metadata(result) -> dict:
    """The metadata of the single materialisation in ``result``."""
    (event,) = result.get_asset_materialization_events()
    return event.materialization.metadata


# --- memory classes ----------------------------------------------------------------


@pytest.mark.parametrize(
    ("memory_gb", "expected"),
    [
        (0.5, "small"),
        (4.0, "small"),
        (4.1, "medium"),
        (16.0, "medium"),
        (16.1, "large"),
        (48.0, "large"),
        (64.0, "xlarge"),
        (1024.0, "xlarge"),
    ],
)
def test_memory_class_boundaries(memory_gb, expected):
    assert memory_class_for(memory_gb) == expected


def test_concurrency_limits_shrink_as_memory_need_grows_but_never_below_one():
    limits = {name: concurrency_limit_for(name, budget_gb=64.0) for name in
              ("small", "medium", "large", "xlarge")}
    assert limits == {"small": 16, "medium": 4, "large": 1, "xlarge": 1}
    assert concurrency_limit_for("large", budget_gb=2.0) == 1, "on a small host"


def test_executor_limits_cover_every_class():
    limits = executor_tag_concurrency_limits(budget_gb=64.0)
    assert {entry["value"] for entry in limits} == {"small", "medium", "large", "xlarge"}
    assert all(entry["key"] == MEMORY_CLASS_TAG for entry in limits)
    assert all(entry["limit"] >= 1 for entry in limits)


# --- the factory -------------------------------------------------------------------


def test_factory_declares_memory_partitions_and_pool():
    asset = make_provider_asset(TinyProvider, name="tiny", memory_gb=32, freq="6h")

    (spec,) = asset.specs
    assert spec.key == dg.AssetKey("tiny")
    assert spec.tags[MEMORY_CLASS_TAG] == "large"
    assert spec.tags[MEMORY_GB_TAG] == "32"
    assert spec.tags["dagster/max_runtime"] == str(12 * 3600)
    assert asset.partitions_def is daily_partitions


def test_factory_keeps_hyphenated_asset_names_but_pools_under_a_legal_one():
    asset = make_provider_asset(TinyProvider, name="arome-france-0025", memory_gb=2)
    # Asset keys may contain hyphens; Dagster pool names may not.
    assert next(iter(asset.specs)).key == dg.AssetKey("arome-france-0025")
    assert asset.op.pool == "arome_france_0025"


def test_factory_defaults_the_name_to_the_provider_name():
    asset = make_provider_asset(TinyProvider)
    assert next(iter(asset.specs)).key == dg.AssetKey("tiny-provider")


@pytest.mark.parametrize(
    ("provider", "kwargs", "error"),
    [(object, {}, TypeError), (TinyProvider, {"memory_gb": 0}, ValueError)],
    ids=["not-a-provider", "nonsense-memory-declaration"],
)
def test_factory_rejects_bad_arguments(provider, kwargs, error):
    with pytest.raises(error):
        make_provider_asset(provider, **kwargs)


def test_make_provider_assets_builds_one_asset_per_variant():
    assets = make_provider_assets(
        TinyProvider,
        {"tiny-a": {"provider_kwargs": {"store_prefix": "test/a.icechunk"}},
         "tiny-b": {"provider_kwargs": {"store_prefix": "test/b.icechunk"}}},
        freq="1D",
        memory_gb=2,
    )
    assert [next(iter(a.specs)).key.to_user_string() for a in assets] == ["tiny-a", "tiny-b"]
    assert all(next(iter(a.specs)).tags[MEMORY_CLASS_TAG] == "small" for a in assets)


# --- materialisation ---------------------------------------------------------------


def test_materialising_a_partition_writes_every_init_time(local_config, tmp_path):
    import icechunk

    asset = make_provider_asset(TinyProvider, name="tiny", freq="6h", memory_gb=2)

    result = dg.materialize([asset], partition_key=PARTITION)
    assert result.success

    store_dir = tmp_path / "stores" / "test" / "tiny.icechunk"
    repo = icechunk.Repository.open(icechunk.local_filesystem_storage(str(store_dir)))
    ds = read_store(repo)

    assert list(pd.DatetimeIndex(ds.time.values)) == list(
        pd.date_range(PARTITION, periods=4, freq="6h")
    )
    assert ds.value.values.tolist() == [0.0, 6.0, 12.0, 18.0]


def test_materialising_twice_is_a_no_op(local_config):
    asset = make_provider_asset(TinyProvider, name="tiny", freq="12h", memory_gb=2)

    dg.materialize([asset], partition_key=PARTITION)
    metadata = materialisation_metadata(dg.materialize([asset], partition_key=PARTITION))
    assert metadata["written"].value == 0
    assert metadata["skipped"].value == 2


def test_materialisation_reports_memory_and_store_metadata(local_config):
    asset = make_provider_asset(TinyProvider, name="tiny", freq="1D", memory_gb=2)

    metadata = materialisation_metadata(dg.materialize([asset], partition_key=PARTITION))

    assert metadata["written"].value == 1
    assert metadata["declared_memory_gb"].value == 2
    assert metadata["memory_class"].text == "small"
    assert "tiny.icechunk" in metadata["store"].text
    assert metadata["peak_memory_gb"].value >= 0


def test_a_partition_where_everything_fails_fails_the_asset(local_config):
    asset = make_provider_asset(AlwaysFailsProvider, name="broken", freq="12h", memory_gb=2)

    result = dg.materialize([asset], partition_key=PARTITION, raise_on_error=False)
    assert not result.success


def test_a_partly_failed_day_is_red_so_dagster_retries_the_holes(local_config):
    class HalfBrokenProvider(TinyProvider):
        name = "half-broken-provider"
        store_prefix = "test/half_broken.icechunk"

        def fetch(self, it, temp_dir=None, **kwargs):
            if it.hour != 0:
                raise RuntimeError("upstream is down")
            return ["synthetic"]

    asset = make_provider_asset(HalfBrokenProvider, name="half", freq="6h", memory_gb=2)

    # Three of four init times raise. One success must not retire the partition with
    # holes in it.
    assert not dg.materialize([asset], partition_key=PARTITION, raise_on_error=False).success
    # And a rerun, where hour 00 is now already stored, is still red.
    assert not dg.materialize([asset], partition_key=PARTITION, raise_on_error=False).success


def test_an_unavailable_timestep_is_skipped_rather_than_failed(local_config):
    class NothingAvailableProvider(TinyProvider):
        name = "nothing-available-provider"
        store_prefix = "test/nothing_available.icechunk"

        def fetch(self, it, temp_dir=None, **kwargs):
            return []

    asset = make_provider_asset(
        NothingAvailableProvider, name="nothing", freq="12h", memory_gb=2
    )

    result = dg.materialize([asset], partition_key=PARTITION)
    assert result.success

    metadata = materialisation_metadata(result)
    assert metadata["skipped"].value == 2
    assert metadata["failed"].value == 0


def test_the_memory_guard_refuses_work_that_cannot_fit(local_config, tmp_path, monkeypatch):
    monkeypatch.setenv("MEMORY_CEILING_GB", "1")
    config_module.reset_config_cache()

    asset = make_provider_asset(TinyProvider, name="tiny", freq="1D", memory_gb=256)
    result = dg.materialize([asset], partition_key=PARTITION, raise_on_error=False)

    assert not result.success
    store_dir = tmp_path / "stores" / "test" / "tiny.icechunk"
    assert not any(store_dir.glob("**/*.json")), "no work should have started"


# --- discovery ---------------------------------------------------------------------


ASSET_MODULE = "import dagster as dg\n\n@dg.asset\ndef {}():\n    return 1\n"

# Swallows Exception in a loop, the way cdsapi's retry handling does.
HANGS_ON_IMPORT = """
    import time

    while True:
        try:
            time.sleep(0.05)
        except Exception:
            pass
    """


def _write_package(root: pathlib.Path, name: str, modules: dict[str, str]):
    """Create an importable package on disk and return it.

    A module name may contain ``/`` to place it in a subpackage.
    """
    import importlib

    package_dir = root / name
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text("")
    for module_name, source in modules.items():
        path = package_dir / f"{module_name}.py"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(source))
    sys.path.insert(0, str(root))
    try:
        return importlib.import_module(name)
    finally:
        sys.path.remove(str(root))


def _loaded_keys(package) -> list[str]:
    """Discover and load ``package``'s assets, and return their keys."""
    modules, _ = loader_module.discover_asset_modules(package)
    assets, failures = loader_module.load_assets(modules)
    assert failures == {}
    return sorted(k.to_user_string() for a in assets for k in a.keys)


def test_discovery_skips_a_broken_module_and_keeps_the_rest(tmp_path):
    package = _write_package(
        tmp_path,
        "pd_discovery_ok",
        {
            "good": ASSET_MODULE.format("good_asset"),
            "broken": "raise RuntimeError('this module is deliberately broken')\n",
            "quitter": "import sys\nsys.exit(2)\n",
            "_private": "raise RuntimeError('never imported')\n",
        },
    )

    modules, failures = loader_module.discover_asset_modules(package)

    assert [m.__name__ for m in modules] == ["pd_discovery_ok.good"]
    assert "deliberately broken" in failures["pd_discovery_ok.broken"]
    assert "pd_discovery_ok.quitter" in failures, "sys.exit must not end discovery"
    assert "pd_discovery_ok._private" not in failures


def test_discovery_survives_a_subpackage_whose_init_is_broken(tmp_path):
    package = _write_package(
        tmp_path,
        "pd_discovery_subpkg",
        {
            "good": ASSET_MODULE.format("fine"),
            # Not an ImportError, so walk_packages re-raises it out of the generator.
            "broken_pkg/__init__": "raise RuntimeError('bad subpackage')\n",
            "broken_pkg/child": ASSET_MODULE.format("child"),
        },
    )

    modules, failures = loader_module.discover_asset_modules(package)

    assert [m.__name__ for m in modules] == ["pd_discovery_subpkg.good"]
    assert any("broken_pkg" in name for name in failures)


def test_discovery_stops_importing_once_the_budget_is_gone(tmp_path, monkeypatch):
    monkeypatch.setattr(loader_module, "IMPORT_TIMEOUT_SECONDS", 1.0)
    monkeypatch.setattr(loader_module, "FIRST_IMPORT_TIMEOUT_SECONDS", 1.0)
    monkeypatch.setattr(loader_module, "DISCOVERY_BUDGET_SECONDS", 1.0)
    package = _write_package(
        tmp_path,
        "pd_discovery_budget",
        {"a_hangs": HANGS_ON_IMPORT, "z_never_reached": ASSET_MODULE.format("late")},
    )

    modules, failures = loader_module.discover_asset_modules(package)

    assert modules == []
    assert "budget" in failures["pd_discovery_budget.z_never_reached"]


def test_discovery_times_out_a_module_that_hangs_on_import(tmp_path, monkeypatch):
    monkeypatch.setattr(loader_module, "IMPORT_TIMEOUT_SECONDS", 1.0)
    monkeypatch.setattr(loader_module, "FIRST_IMPORT_TIMEOUT_SECONDS", 1.0)
    package = _write_package(
        tmp_path,
        "pd_discovery_hang",
        {"good": ASSET_MODULE.format("fine_asset"), "hangs": HANGS_ON_IMPORT},
    )

    modules, failures = loader_module.discover_asset_modules(package)

    assert [m.__name__ for m in modules] == ["pd_discovery_hang.good"]
    assert "did not import within" in failures["pd_discovery_hang.hangs"]


def test_loaded_assets_are_namespaced_by_family_and_deduplicated(tmp_path):
    package = _write_package(
        tmp_path,
        "dags_fake_assets",
        {
            "alpha": ASSET_MODULE.format("shared"),
            "beta": "from dags_fake_assets.alpha import shared\n",
        },
    )
    assert _loaded_keys(package) == ["alpha/shared"]


@pytest.mark.parametrize(
    ("module_name", "expected_family"),
    [
        # A module in a family subpackage, and one placed directly under dags/assets/.
        ("dags.assets.nwp.gfs", "nwp"),
        ("dags.assets.observation.asos", "observation"),
        ("dags.assets.arome", "arome"),
        ("dags.assets.polar_sounders", "polar_sounders"),
        ("dags.assets.nwp.regional.harmonie", "nwp"),
    ],
)
def test_family_covers_both_module_layouts(module_name, expected_family):
    assert loader_module._family(module_name) == expected_family


def test_an_asset_that_chose_its_own_key_prefix_keeps_it(tmp_path):
    package = _write_package(
        tmp_path,
        "pd_prefix_pkg",
        {
            "cmems": (
                "import dagster as dg\n\n"
                "@dg.asset(key_prefix='ocean')\n"
                "def cmems_analysis():\n    return 1\n\n"
                "@dg.asset\n"
                "def cmems_forecast():\n    return 1\n"
            ),
        },
    )
    # The explicit prefix survives; the bare asset is namespaced under its family.
    assert _loaded_keys(package) == ["cmems/cmems_forecast", "ocean/cmems_analysis"]


def _resolved(assets, jobs, schedules):
    """Resolve the schedules ``build_memory_class_jobs`` returns.

    ``build_schedule_from_partitioned_job`` hands back an unresolved definition; the cron
    it derives from the partitioning is only known once the code location assembles it.
    """
    defs = dg.Definitions(assets=assets, jobs=jobs, schedules=schedules)
    return [defs.resolve_schedule_def(s.name) for s in schedules]


def test_jobs_are_grouped_by_memory_class():
    assets = [
        make_provider_asset(TinyProvider, name="small-one", memory_gb=2),
        make_provider_asset(TinyProvider, name="small-two", memory_gb=4),
        make_provider_asset(TinyProvider, name="big-one", memory_gb=64),
    ]

    jobs, schedules = loader_module.build_memory_class_jobs(assets)

    assert len(jobs) == 2
    classes = {j.tags[MEMORY_CLASS_TAG] for j in jobs}
    assert classes == {"small", "xlarge"}
    assert all(j.name.endswith(j.tags[MEMORY_CLASS_TAG]) for j in jobs)

    resolved = _resolved(assets, jobs, schedules)
    assert all(s.cron_schedule == "0 6 * * *" for s in resolved)
    assert all(s.execution_timezone == "UTC" for s in resolved)
    assert all(s.default_status == dg.DefaultScheduleStatus.RUNNING for s in resolved)


def test_job_names_are_stable_across_rebuilds():
    """The schedule and its run history are addressed by name, so it must not move."""
    first, _ = loader_module.build_memory_class_jobs(
        [make_provider_asset(TinyProvider, name="tiny", memory_gb=2)]
    )
    second, _ = loader_module.build_memory_class_jobs(
        [make_provider_asset(TinyProvider, name="tiny", memory_gb=2)]
    )
    assert [j.name for j in first] == [j.name for j in second]


def test_an_asset_that_declares_no_memory_class_still_gets_one():
    """Regression: the selection required a tag that no hand-rolled asset sets.

    It therefore matched nothing, and the code location shipped zero jobs, zero schedules,
    and a set of ``tag_concurrency_limits`` that applied to no run.
    """

    @dg.asset(name="hand_rolled", partitions_def=daily_partitions)
    def hand_rolled(context):
        return 1

    jobs, schedules = loader_module.build_memory_class_jobs([hand_rolled])

    assert len(jobs) == 1
    assert jobs[0].tags[MEMORY_CLASS_TAG] == memory_class_for(DEFAULT_MEMORY_GB)
    assert len(schedules) == 1


def test_assets_with_different_partitionings_get_their_own_job_and_cadence():
    """A partitioned asset job needs one partitions_def, and each wants its own cadence."""
    hourly = dg.HourlyPartitionsDefinition(start_date="2024-01-01-00:00")

    @dg.asset(name="daily_one", partitions_def=daily_partitions)
    def daily_one(context):
        return 1

    @dg.asset(name="hourly_one", partitions_def=hourly)
    def hourly_one(context):
        return 1

    jobs, schedules = loader_module.build_memory_class_jobs([daily_one, hourly_one])

    assert len(jobs) == 2
    crons = sorted(s.cron_schedule for s in _resolved([daily_one, hourly_one], jobs, schedules))
    # Not two daily ticks: the hourly asset would only ever get one partition a day.
    assert crons == ["0 * * * *", "0 6 * * *"]


def test_a_schedule_tick_carries_a_partition_key():
    """Regression: the schedule emitted ``RunRequest(partition_key=None)``.

    A plain ``ScheduleDefinition`` over a partitioned job does that, and executing a
    partitioned job without a partition key fails outright, so every tick was a failed run.
    """

    @dg.asset(name="tick_me", partitions_def=daily_partitions)
    def tick_me(context):
        return 1

    _, (schedule,) = loader_module.build_memory_class_jobs([tick_me])
    defs = dg.Definitions(assets=[tick_me], schedules=[schedule])
    resolved = defs.resolve_schedule_def(schedule.name)

    context = dg.build_schedule_context(scheduled_execution_time=pd.Timestamp("2026-06-02T06:00"))
    run_requests = list(resolved.evaluate_tick(context).run_requests or [])

    assert run_requests, "the schedule produced no run request"
    assert all(request.partition_key for request in run_requests)


def test_an_asset_with_its_own_automation_condition_is_not_also_scheduled():
    """Both would fire, launching every partition twice."""

    @dg.asset(
        name="self_driving",
        partitions_def=daily_partitions,
        automation_condition=dg.AutomationCondition.on_cron("0 7 * * *"),
    )
    def self_driving(context):
        return 1

    jobs, schedules = loader_module.build_memory_class_jobs([self_driving])

    assert jobs == []
    assert schedules == []


def test_a_spec_only_asset_does_not_take_down_the_code_location(tmp_path):
    """Regression: ``node_def`` raises CheckError, not AttributeError.

    A spec-only ``AssetsDefinition`` signals "no op" that way, the ``getattr`` default
    never caught it, and the access sat outside the per-module try.
    """
    package = _write_package(
        tmp_path,
        "pd_spec_only_pkg",
        {
            "external": (
                "import dagster as dg\n\n"
                "upstream = dg.AssetsDefinition(specs=[dg.AssetSpec('upstream_thing')])\n"
            ),
            "normal": ASSET_MODULE.format("normal_asset"),
        },
    )
    assert _loaded_keys(package) == ["external/upstream_thing", "normal/normal_asset"]


def test_skip_prefixes_match_at_package_boundaries():
    """`dags.assets.virtual_goes` merely shares a prefix with the skipped `virt` package."""
    assert loader_module._is_skipped("dags.assets.virt.virtualize_goes_mcmpf")
    assert loader_module._is_skipped("dags.assets.virt")
    assert not loader_module._is_skipped("dags.assets.virtual_goes")
    assert not loader_module._is_skipped("dags.assets.virtual_gk2a")


@pytest.mark.parametrize("budget_gb", [4.0, 8.0])
def test_executor_parallelism_is_bounded_by_the_memory_budget(budget_gb):
    executor = loader_module.build_executor(budget_gb=budget_gb)
    config = executor.config_schema.resolve_config({}).value["config"]
    # One default-sized step fits in 8 GB, and a smaller host still gets one slot.
    assert config["max_concurrent"] == 1
    assert config["tag_concurrency_limits"] == executor_tag_concurrency_limits(budget_gb)


def test_dagster_yaml_is_valid_instance_config(tmp_path, monkeypatch):
    import shutil

    from dagster import DagsterInstance

    dagster_home = tmp_path / "dagster_home"
    dagster_home.mkdir()
    dagster_yaml = pathlib.Path(loader_module.__file__).parent / "dagster.yaml"
    shutil.copy(dagster_yaml, dagster_home / "dagster.yaml")
    monkeypatch.setenv("DAGSTER_HOME", str(dagster_home))

    with DagsterInstance.get() as instance:
        concurrency = instance.get_concurrency_config()
        run_queue = concurrency.run_queue_config
        assert run_queue is not None
        limits = {
            (entry["key"], entry.get("value")): entry["limit"]
            for entry in run_queue.tag_concurrency_limits
        }
        # Larger memory classes must never be allowed more concurrency than smaller ones.
        ordered = [
            limits[(MEMORY_CLASS_TAG, name)]
            for name in ("small", "medium", "large", "xlarge")
        ]
        assert ordered == sorted(ordered, reverse=True)
        assert instance.run_launcher is not None
