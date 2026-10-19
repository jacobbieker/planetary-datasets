"""The ICON-D2-RUC first-hour mirror, against a fake DWD listing and no network."""

# ruff: noqa: D102, D103, D107 - test names say what each test checks

from __future__ import annotations

import pathlib

import pandas as pd
import pytest

from planetary_datasets.providers import icon_d2_ruc_mirror as mod
from planetary_datasets.providers.icon import ICOND2RUCProvider

RUNS = [pd.Timestamp("2026-10-19T09:00") + pd.Timedelta(h, "h") for h in range(4)]
HOURLY = ["PT000H00M.grib2", "PT001H00M.grib2", "PT002H00M.grib2"]
QUARTER = [f"PT00{m // 60}H{m % 60:02d}M.grib2" for m in range(0, 135, 15)]
FIVE = [f"PT00{m // 60}H{m % 60:02d}M.grib2" for m in range(0, 125, 5)]


def run_dir(run: pd.Timestamp) -> str:
    return run.strftime("%Y-%m-%dT%H:%M")


class FakeServer:
    """A DWD ``p/`` tree: T_2M hourly, LPI 15-minute, TOT_PREC 5-minute, T on two levels."""

    def __init__(self):
        runs = [run_dir(r) for r in RUNS]
        self.tree: dict[str, list[str]] = {
            "": ["T_2M", "LPI", "TOT_PREC", "T"],
            "T_2M": ["r"],
            "LPI": ["r"],
            "TOT_PREC": ["r"],
            "T": ["lvt1"],
            "T/lvt1": ["150"],
            "T/lvt1/150": ["lv1"],
            "T/lvt1/150/lv1": ["40", "41"],
            "T/lvt1/150/lv1/40": ["r"],
            "T/lvt1/150/lv1/41": ["r"],
        }
        steps = {"T_2M": HOURLY, "LPI": QUARTER, "TOT_PREC": FIVE}
        steps.update({f"T/lvt1/150/lv1/{lv}": HOURLY for lv in (40, 41)})
        for field, names in steps.items():
            self.tree[f"{field}/r"] = runs
            for run in RUNS:
                self.tree[f"{field}/r/{mod.run_url_name(run)}/s"] = names
        self.downloads: list[tuple[str, pathlib.Path]] = []
        self.unavailable: set[str] = set()

    def list_dir(self, relative: str) -> list[str]:
        return list(self.tree.get(relative.strip("/"), []))

    def download(self, url: str, dest, retries: int = 3):
        if url in self.unavailable:
            return None
        dest = pathlib.Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"GRIB")
        self.downloads.append((url, dest))
        return dest


@pytest.fixture
def server(monkeypatch) -> FakeServer:
    fake = FakeServer()
    monkeypatch.setattr(mod.Mirror, "list_dir", lambda self, relative: fake.list_dir(relative))
    monkeypatch.setattr(mod, "download_one", fake.download)
    return fake


def test_first_hour_runs_from_the_analysis_to_one_hour_inclusive():
    assert mod.first_hour_steps(HOURLY) == ["PT000H00M.grib2", "PT001H00M.grib2"]
    assert len(mod.first_hour_steps(QUARTER)) == 5
    assert mod.first_hour_steps(FIVE)[-1] == "PT001H00M.grib2"
    assert len(mod.first_hour_steps(FIVE)) == 13
    assert mod.first_hour_steps(["index.html", "PT001H05M.grib2"]) == []


def test_run_names_are_encoded_for_the_server_only():
    assert mod.run_url_name(pd.Timestamp("2026-10-19T12:00")) == "2026-10-19T12%3A00"


def test_discover_finds_every_field_and_its_first_hour_steps(server, tmp_path):
    fields = {f.path: f.steps for f in mod.Mirror(tmp_path).discover()}
    assert set(fields) == {"T_2M", "LPI", "TOT_PREC", "T/lvt1/150/lv1/40", "T/lvt1/150/lv1/41"}
    assert len(fields["T_2M"]) == 2
    assert len(fields["LPI"]) == 5
    assert len(fields["TOT_PREC"]) == 13


def test_steps_are_the_union_of_the_oldest_runs(server, tmp_path):
    """The oldest run may be part-deleted; its neighbours fill in what it lost."""
    server.tree[f"LPI/r/{mod.run_url_name(RUNS[0])}/s"] = QUARTER[:2]
    fields = {f.path: f.steps for f in mod.Mirror(tmp_path).discover()}
    assert len(fields["LPI"]) == 5


def test_a_run_is_mirrored_in_the_providers_layout(server, tmp_path):
    mirror = mod.Mirror(tmp_path)
    mirror.discover()
    assert mirror.mirror_run(RUNS[1]) == (2 + 5 + 13 + 2 + 2, 0)
    url, dest = server.downloads[0]
    assert "%3A" in url and ":" not in url.split("://", 1)[1]
    assert run_dir(RUNS[1]) in str(dest)

    files = ICOND2RUCProvider(grib_dir=tmp_path).fetch(RUNS[1])
    # The provider reads the analysis hour; PT001H00M belongs to the mirror only.
    assert len(files) == 1 + 4 + 12 + 1 + 1
    assert all("/PT000H" in f for f in files)


def test_a_complete_run_is_not_fetched_again(server, tmp_path):
    mirror = mod.Mirror(tmp_path)
    mirror.discover()
    mirror.mirror_run(RUNS[1])
    count = len(server.downloads)
    assert mirror.mirror_run(RUNS[1]) == (0, 0)
    assert len(server.downloads) == count


def test_a_file_not_yet_published_is_retried_next_pass(server, tmp_path):
    mirror = mod.Mirror(tmp_path)
    mirror.discover()
    late = mirror.url(mirror.fields[0], RUNS[3], mirror.fields[0].steps[-1])
    server.unavailable.add(late)
    results = mirror.run_pass()
    assert results[RUNS[3]][1] == 1
    assert RUNS[3] not in mirror.complete

    server.unavailable.clear()
    assert mirror.run_pass() == {RUNS[3]: (1, 0)}


def test_prune_drops_only_runs_older_than_the_window(server, tmp_path):
    mirror = mod.Mirror(tmp_path)
    mirror.discover()
    for run in RUNS:
        mirror.mirror_run(run)
    removed = mirror.prune(pd.Timedelta(2, "h"), now=RUNS[-1] + pd.Timedelta(30, "min"))
    assert removed == 2 * len(mirror.fields)
    left = sorted(p.name for p in (tmp_path / "T_2M" / "r").iterdir())
    assert left == [run_dir(r) for r in RUNS[2:]]
