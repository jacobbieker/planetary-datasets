"""Path construction from configuration and command-line input."""

from __future__ import annotations

import os
import pathlib
import stat

import pytest

from planetary_datasets.common.paths import (
    UnsafePath,
    default_scratch_dir,
    private_dir,
    safe_component,
    safe_join,
)
from planetary_datasets.config import load_config


class TestSafeComponent:
    def test_ordinary_labels_are_untouched(self):
        assert safe_component("goes16") == "goes16"
        assert safe_component("C13") == "C13"
        assert safe_component("ir087") == "ir087"

    def test_separators_and_traversal_are_replaced(self):
        assert "/" not in safe_component("../../etc/passwd")
        assert ".." not in safe_component("..")

    def test_an_empty_result_falls_back(self):
        assert safe_component("...") == "_"
        assert safe_component("") == "_"

    def test_whitespace_is_trimmed(self):
        assert safe_component("  goes16  ") == "goes16"


class TestSafeJoin:
    def test_a_normal_join_resolves_under_the_root(self, tmp_path):
        assert safe_join(tmp_path, "bkr", "x.icechunk") == (tmp_path / "bkr/x.icechunk").resolve()

    def test_traversal_is_refused(self, tmp_path):
        with pytest.raises(UnsafePath, match="outside"):
            safe_join(tmp_path, "../escaped")

    def test_traversal_hidden_mid_path_is_refused(self, tmp_path):
        with pytest.raises(UnsafePath, match="outside"):
            safe_join(tmp_path, "a/../../escaped")

    def test_an_absolute_component_is_refused(self, tmp_path):
        with pytest.raises(UnsafePath, match="absolute"):
            safe_join(tmp_path, "/etc/passwd")

    def test_a_symlink_out_of_the_root_is_refused(self, tmp_path):
        outside = tmp_path.parent / "outside_root"
        outside.mkdir(exist_ok=True)
        root = tmp_path / "root"
        root.mkdir()
        (root / "link").symlink_to(outside, target_is_directory=True)
        with pytest.raises(UnsafePath, match="outside"):
            safe_join(root, "link", "file.txt")

    def test_the_root_itself_is_allowed(self, tmp_path):
        assert safe_join(tmp_path) == tmp_path.resolve()


class TestPrivateDir:
    def test_it_is_created_owner_only(self, tmp_path):
        d = private_dir(tmp_path, "proj")
        assert d.is_dir()
        assert stat.S_IMODE(d.stat().st_mode) & 0o077 == 0

    def test_an_existing_loose_directory_is_tightened(self, tmp_path):
        loose = tmp_path / "proj"
        loose.mkdir(mode=0o777)
        os.chmod(loose, 0o777)
        private_dir(tmp_path, "proj")
        assert stat.S_IMODE(loose.stat().st_mode) & 0o077 == 0

    def test_the_name_is_reduced_to_one_component(self, tmp_path):
        d = private_dir(tmp_path, "../escape")
        assert d.parent == tmp_path.resolve()


class TestConfigUsesThem:
    def test_scratch_defaults_to_a_private_directory_not_the_temp_root(self, tmp_path):
        cfg = load_config(env_file=tmp_path / "absent.env")
        assert cfg.scratch_dir != pathlib.Path("/tmp")
        assert cfg.scratch_dir == default_scratch_dir()
        assert stat.S_IMODE(cfg.scratch_dir.stat().st_mode) & 0o077 == 0

    def test_an_explicit_scratch_dir_is_honoured(self, tmp_path, monkeypatch):
        monkeypatch.setenv("PLANETARY_DATASETS_SCRATCH_DIR", str(tmp_path / "mine"))
        cfg = load_config(env_file=tmp_path / "absent.env")
        assert cfg.scratch_dir == tmp_path / "mine"

    def test_a_traversing_store_prefix_is_refused(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "stores"))
        cfg = load_config(env_file=tmp_path / "absent.env")
        with pytest.raises(UnsafePath):
            cfg.store_path("../../escaped.icechunk")

    def test_a_traversing_icechunk_prefix_is_refused(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "stores"))
        monkeypatch.setenv("ICECHUNK_PREFIX", "../..")
        cfg = load_config(env_file=tmp_path / "absent.env")
        with pytest.raises(UnsafePath):
            cfg.icechunk_storage("x.icechunk")

    def test_an_ordinary_local_store_still_resolves(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "stores"))
        cfg = load_config(env_file=tmp_path / "absent.env")
        assert cfg.store_path("bkr/dmi/x.icechunk").startswith(str((tmp_path / "stores").resolve()))

    def test_s3_paths_are_unaffected(self, tmp_path):
        cfg = load_config(env_file=tmp_path / "absent.env")
        assert cfg.store_path("bkr/x.icechunk").startswith("s3://")


def test_log_event_keeps_the_file_inside_its_directory(tmp_path):
    """A CLI-supplied channel name must not place the log outside the log directory."""
    common = pytest.importorskip("planetary_datasets.providers.virtualized.goes_radf_common")
    log_dir = tmp_path / "logs"
    common.log_event(str(log_dir), "../../goes16", "../C13", "2024-01-01", "skip", "because")
    written = list(log_dir.glob("*.log"))
    assert written, "expected a log file inside the log directory"
    assert all(p.parent == log_dir for p in written)
    assert not list(tmp_path.glob("*.log"))
