"""``_backfill_mtg_2000m_archive.sh`` stops at the month before the current one."""

import datetime as dt
import pathlib
import shutil
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[1]


def test_archive_lane_stops_before_the_current_month(tmp_path):
    """The satcons wrapper is called for 202409 up to last month, never the live month."""
    shutil.copy(ROOT / "_backfill_mtg_2000m_archive.sh", tmp_path)
    (tmp_path / "_backfill_satcons.sh").write_text('#!/bin/bash\necho "$@"\n')
    (tmp_path / "_backfill_satcons.sh").chmod(0o755)
    args = subprocess.run(
        ["bash", str(tmp_path / "_backfill_mtg_2000m_archive.sh")],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    ).stdout.split()
    first = dt.datetime.now(dt.timezone.utc).date().replace(day=1)
    last_month = (first - dt.timedelta(days=1)).strftime("%Y%m")
    assert args == ["odegree-12", "2000", "mtg_2000m", "202409", last_month]
