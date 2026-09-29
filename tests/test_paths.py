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


def _is_owner_only(path: pathlib.Path) -> bool:
    return stat.S_IMODE(path.stat().st_mode) & 0o077 == 0


class TestSafeComponent:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("goes16", "goes16"),
            ("C13", "C13"),
            ("  goes16  ", "goes16"),
            # A name that reduces to nothing falls back rather than vanishing.
            ("...", "_"),
            ("", "_"),
        ],
    )
    def test_labels_are_normalised(self, raw, expected):
        assert safe_component(raw) == expected

    def test_separators_and_traversal_are_replaced(self):
        assert "/" not in safe_component("../../etc/passwd")
        assert ".." not in safe_component("..")


class TestSafeJoin:
    def test_a_normal_join_resolves_under_the_root(self, tmp_path):
        assert safe_join(tmp_path, "bkr", "x.icechunk") == (tmp_path / "bkr/x.icechunk").resolve()

    def test_the_root_itself_is_allowed(self, tmp_path):
        assert safe_join(tmp_path) == tmp_path.resolve()

    @pytest.mark.parametrize(
        ("part", "match"),
        [
            ("../escaped", "outside"),
            ("a/../../escaped", "outside"),
            ("/etc/passwd", "absolute"),
        ],
    )
    def test_escaping_components_are_refused(self, tmp_path, part, match):
        with pytest.raises(UnsafePath, match=match):
            safe_join(tmp_path, part)

    def test_a_symlink_out_of_the_root_is_refused(self, tmp_path):
        outside = tmp_path.parent / "outside_root"
        outside.mkdir(exist_ok=True)
        root = tmp_path / "root"
        root.mkdir()
        (root / "link").symlink_to(outside, target_is_directory=True)
        with pytest.raises(UnsafePath, match="outside"):
            safe_join(root, "link", "file.txt")


class TestPrivateDir:
    def test_it_is_created_owner_only(self, tmp_path):
        d = private_dir(tmp_path, "proj")
        assert d.is_dir()
        assert _is_owner_only(d)

    def test_an_existing_loose_directory_is_tightened(self, tmp_path):
        loose = tmp_path / "proj"
        loose.mkdir()
        os.chmod(loose, 0o777)
        private_dir(tmp_path, "proj")
        assert _is_owner_only(loose)

    def test_the_name_is_reduced_to_one_component(self, tmp_path):
        assert private_dir(tmp_path, "../escape").parent == tmp_path.resolve()


class TestConfigUsesThem:
    def test_scratch_defaults_to_a_private_directory_not_the_temp_root(self, load_env_config):
        cfg = load_env_config()
        assert cfg.scratch_dir != pathlib.Path("/tmp")
        assert cfg.scratch_dir == default_scratch_dir()
        assert _is_owner_only(cfg.scratch_dir)

    def test_an_explicit_scratch_dir_is_honoured(self, load_env_config, tmp_path):
        cfg = load_env_config(PLANETARY_DATASETS_SCRATCH_DIR=str(tmp_path / "mine"))
        assert cfg.scratch_dir == tmp_path / "mine"

    def test_a_traversing_store_prefix_is_refused(self, load_env_config, tmp_path):
        cfg = load_env_config(ICECHUNK_LOCAL_PATH=str(tmp_path / "stores"))
        with pytest.raises(UnsafePath):
            cfg.store_path("../../escaped.icechunk")

    def test_a_traversing_icechunk_prefix_is_refused(self, load_env_config, tmp_path):
        cfg = load_env_config(ICECHUNK_LOCAL_PATH=str(tmp_path / "stores"), ICECHUNK_PREFIX="../..")
        with pytest.raises(UnsafePath):
            cfg.icechunk_storage("x.icechunk")


def test_log_event_keeps_the_file_inside_its_directory(tmp_path):
    """A CLI-supplied channel name must not place the log outside the log directory."""
    common = pytest.importorskip("planetary_datasets.providers.virtualized.goes_radf_common")
    log_dir = tmp_path / "logs"
    common.log_event(str(log_dir), "../../goes16", "../C13", "2024-01-01", "skip", "because")
    written = list(log_dir.glob("*.log"))
    assert written, "expected a log file inside the log directory"
    assert all(p.parent == log_dir for p in written)
    assert not list(tmp_path.glob("*.log"))
