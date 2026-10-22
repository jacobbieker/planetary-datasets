"""The DWD-to-archive ICON mirror, against a fake server and bucket, without the network."""

# ruff: noqa: D102, D103, D107 - test names say what each test checks

from __future__ import annotations

import pandas as pd
import pytest

from planetary_datasets.providers import icon_v1_mirror as mod
from planetary_datasets.providers.icon_d2_ruc_mirror import Field

RUN = pd.Timestamp("2026-10-22T06:00")
STEPS = ("PT000H00M.grib2", "PT001H00M.grib2", "PT120H00M.grib2")


class FakeBucket:
    """The few s3fs calls the mirror makes, over a dict of key -> bytes."""

    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def ls(self, prefix, detail=False):
        keys = [k for k in self.objects if k.startswith(prefix.rstrip("/") + "/")]
        if not keys:
            raise FileNotFoundError(prefix)
        return keys

    def pipe_file(self, key, data):
        self.objects[key] = data


@pytest.fixture
def mirror(monkeypatch):
    unpublished: set[str] = set()

    def read_bytes(url, **kwargs):
        return None if url in unpublished else url.encode()

    monkeypatch.setattr(mod, "read_bytes", read_bytes)
    m = mod.ArchiveMirror("icon-art", FakeBucket(), archive_root="s3://bucket/bkr/icon")
    m.fields = [Field("T_2M", STEPS), Field("T/lvt1/150/lv1/40", STEPS[:2])]
    m.unpublished = unpublished
    return m


def test_every_step_is_mirrored_not_only_the_first_hour():
    assert mod.ALL_STEPS > pd.Timedelta(180, "h")


def test_a_run_is_copied_into_the_archive_layout(mirror):
    assert mirror.mirror_run(RUN) == (5, 0)
    key = "bucket/bkr/icon/icon-art/p/T_2M/r/2026-10-22T06:00/s/PT120H00M.grib2"
    assert mirror.fs.objects[key].endswith(b"/T_2M/r/2026-10-22T06%3A00/s/PT120H00M.grib2")
    assert RUN in mirror.complete


def test_only_what_the_archive_lacks_is_copied(mirror):
    have = "bucket/bkr/icon/icon-art/p/T_2M/r/2026-10-22T06:00/s/PT000H00M.grib2"
    mirror.fs.objects[have] = b"already"
    assert mirror.mirror_run(RUN) == (4, 0)
    assert mirror.fs.objects[have] == b"already"


def test_a_file_not_yet_published_is_left_for_the_next_pass(mirror):
    late = mirror.url(mirror.fields[0], RUN, STEPS[-1])
    mirror.unpublished.add(late)
    assert mirror.mirror_run(RUN) == (4, 1)
    assert RUN not in mirror.complete

    mirror.unpublished.clear()
    assert mirror.mirror_run(RUN) == (1, 0)
    assert RUN in mirror.complete
