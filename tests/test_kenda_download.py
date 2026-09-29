"""Tests for the KENDA-CH1 downloader and the Dagster asset that launches its image.

``meteodatalab`` is only installed in the image, so the OGD API is replaced by a small
fake that serves a fixed set of published files.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
import pathlib
import sys

import dagster as dg
import pandas as pd
import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from planetary_datasets.providers import kenda, kenda_download  # noqa: E402
from planetary_datasets.providers.kenda_download import (  # noqa: E402
    ANALYSIS_VARIABLES,
    CONSTANTS,
    FORECAST_VARIABLES,
    IncompleteDownload,
    data_filename,
    download_kenda,
)

REF_TIME = dt.datetime(2026, 9, 29, 6, tzinfo=dt.timezone.utc)
BASE_URL = "https://data.geo.admin.ch/ch.meteoschweiz.ogd-analysis-kenda-ch1"

#: Every variable the two original download scripts asked for, at either step.
ORIGINAL_SCRIPT_VARIABLES = {
    "ASOB_S", "ASOB_S_OS", "ASWDIFD_S", "ASWDIFU_S", "ASWDIFU_S_OS", "ASWDIR_S",
    "ASWDIR_S_OS", "ATHB_S", "ATHD_S", "ATHU_S", "AUMFL_S", "AVMFL_S", "CAPE_ML",
    "CAPE_MU", "CIN_ML", "CIN_MU", "TQC", "TQI", "TQV", "DEN", "VMAX_10M", "CLC",
    "GRAU_GSP", "H_SNOW", "P", "PMSL", "QC", "QV", "RAIN_GSP", "SNOW_GSP", "T", "TKE",
    "TD_2M", "TWATER", "TOT_PREC", "U", "V", "W",
}  # fmt: skip


@dataclasses.dataclass(frozen=True)
class FakeRequest:
    collection: str
    variable: str
    ref_time: str
    perturbed: bool
    lead_time: str


class FakeResponse:
    def __init__(self, body: bytes, sha256: str | None):
        self._body = body
        self.headers = {} if sha256 is None else {"X-Amz-Meta-Sha256": sha256}

    def raise_for_status(self) -> None:
        pass

    def close(self) -> None:
        pass

    def iter_content(self, size: int):
        for i in range(0, len(self._body), size):
            yield self._body[i : i + size]


class FakeOGD:
    """Serves the files in ``published`` (step -> variables), plus the constants."""

    Request = FakeRequest

    def __init__(self, published: dict[int, set[str]] | None = None, corrupt: set[str] = ()):
        self.published = (
            published
            if published is not None
            else {
                0: set(ANALYSIS_VARIABLES),
                1: set(FORECAST_VARIABLES),
            }
        )
        self.corrupt = set(corrupt)
        self.requests: list[FakeRequest] = []
        self.fetched: list[str] = []
        self.revision = b""
        self.session = self

    def get_asset_urls(self, request: FakeRequest) -> list[str]:
        self.requests.append(request)
        step = {"P0DT0H": 0, "P0DT1H": 1}[request.lead_time]
        ref_time = dt.datetime.strptime(request.ref_time, "%Y-%m-%dT%H:%M:%SZ")
        if request.variable not in self.published.get(step, set()):
            return []
        return [f"{BASE_URL}/{data_filename(ref_time, step, request.variable)}"]

    def get_collection_asset_url(self, collection_id: str, asset_id: str) -> str:
        assert collection_id == kenda_download.COLLECTION_ID
        return f"{BASE_URL}/{asset_id}"

    def get(self, url: str, stream: bool = False, timeout: float | None = None):
        name = url.rsplit("/", 1)[-1]
        body = name.encode() * 100 + self.revision
        sha = hashlib.sha256(body).hexdigest()
        response = FakeResponse(body, "0" * 64 if name in self.corrupt else sha)
        original = response.iter_content

        def iter_content(size):
            # Only a body that is actually read counts as a download.
            self.fetched.append(name)
            return original(size)

        response.iter_content = iter_content
        return response


class ExplodingOGD(FakeOGD):
    def get_asset_urls(self, request):
        raise AssertionError("the API must not be queried for files already on disk")


def test_step_variable_sets_are_disjoint_and_cover_the_original_scripts():
    assert not set(ANALYSIS_VARIABLES) & set(FORECAST_VARIABLES)
    assert set(ANALYSIS_VARIABLES) | set(FORECAST_VARIABLES) == ORIGINAL_SCRIPT_VARIABLES


def test_constants_match_the_names_the_provider_reads():
    assert set(CONSTANTS) == {kenda.HORIZONTAL_CONSTANTS, kenda.VERTICAL_CONSTANTS}


def test_filenames_follow_the_published_convention():
    assert data_filename(REF_TIME, 1, "ASWDIR_S_OS") == (
        "kenda-ch1-202609290600-1-aswdir_s_os-ctrl.grib2"
    )


def test_each_variable_is_only_requested_at_the_step_that_publishes_it(tmp_path):
    api = FakeOGD()
    download_kenda(REF_TIME, tmp_path, ogd_api=api)

    by_step = {
        lead: {r.variable for r in api.requests if r.lead_time == lead}
        for lead in ("P0DT0H", "P0DT1H")
    }
    assert by_step["P0DT0H"] == set(ANALYSIS_VARIABLES)
    assert by_step["P0DT1H"] == set(FORECAST_VARIABLES)
    assert {r.ref_time for r in api.requests} == {"2026-09-29T06:00:00Z"}
    assert all(r.perturbed is False for r in api.requests)


def test_a_complete_hour_is_downloaded_with_checksums_and_constants(tmp_path):
    report = download_kenda(REF_TIME, tmp_path, ogd_api=FakeOGD())

    assert report.complete
    assert len(report.downloaded) == len(ANALYSIS_VARIABLES) + len(FORECAST_VARIABLES) + 2
    for name in report.downloaded:
        path = tmp_path / name
        assert path.is_file()
        assert (
            path.with_suffix(".sha256").read_text() == hashlib.sha256(path.read_bytes()).hexdigest()
        )
    assert not list(tmp_path.glob("*.part"))


def test_a_rerun_never_queries_the_api_for_files_already_on_disk(tmp_path):
    """Once an hour has aged out of the API, re-running it must still succeed."""
    download_kenda(REF_TIME, tmp_path, ogd_api=FakeOGD())

    report = download_kenda(REF_TIME, tmp_path, ogd_api=ExplodingOGD())

    assert report.complete
    assert report.downloaded == []
    assert len(report.present) == len(ANALYSIS_VARIABLES) + len(FORECAST_VARIABLES)


def test_constants_are_only_fetched_again_when_republished(tmp_path):
    api = FakeOGD()
    download_kenda(REF_TIME, tmp_path, ogd_api=api)
    api.fetched.clear()

    download_kenda(REF_TIME, tmp_path, ogd_api=api)
    assert api.fetched == []

    api.revision = b"republished"
    report = download_kenda(REF_TIME, tmp_path, ogd_api=api)
    assert sorted(api.fetched) == sorted(CONSTANTS)
    assert sorted(report.downloaded) == sorted(CONSTANTS)
    horizontal = tmp_path / CONSTANTS[0]
    assert horizontal.read_bytes().endswith(b"republished")


def test_a_file_without_a_matching_checksum_is_fetched_again(tmp_path):
    download_kenda(REF_TIME, tmp_path, ogd_api=FakeOGD())
    damaged = tmp_path / data_filename(REF_TIME, 0, "T")
    damaged.write_bytes(b"truncated")

    api = FakeOGD()
    report = download_kenda(REF_TIME, tmp_path, ogd_api=api)

    assert api.fetched == [damaged.name]
    assert report.downloaded == [damaged.name]


def test_unpublished_variables_are_reported_missing(tmp_path):
    api = FakeOGD(published={0: set(ANALYSIS_VARIABLES), 1: {"TOT_PREC"}})

    report = download_kenda(REF_TIME, tmp_path, ogd_api=api)

    assert not report.complete
    assert report.missing[0] == []
    assert set(report.missing[1]) == set(FORECAST_VARIABLES) - {"TOT_PREC"}


def test_a_checksum_mismatch_leaves_nothing_behind(tmp_path):
    bad = data_filename(REF_TIME, 0, "QV")
    report = download_kenda(REF_TIME, tmp_path, ogd_api=FakeOGD(corrupt={bad}))

    assert report.missing[0] == ["QV"]
    assert not (tmp_path / bad).exists()
    assert not list(tmp_path.glob("*.part"))


def test_nothing_published_downloads_no_constants(tmp_path):
    api = FakeOGD(published={})

    report = download_kenda(REF_TIME, tmp_path, ogd_api=api)

    assert report.downloaded == []
    assert not any(tmp_path.iterdir())


def test_variables_can_be_restricted(tmp_path):
    api = FakeOGD()
    report = download_kenda(REF_TIME, tmp_path, variables=["t", "tot_prec"], ogd_api=api)

    assert {r.variable for r in api.requests} == {"T", "TOT_PREC"}
    assert report.complete


def test_an_unknown_step_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="steps"):
        download_kenda(REF_TIME, tmp_path, steps=[2], ogd_api=FakeOGD())


def test_naive_reference_times_are_utc():
    assert kenda_download.to_utc("2026-09-29T06:00") == REF_TIME
    assert kenda_download.to_utc("2026-09-29T08:00+02:00") == REF_TIME


def test_run_fails_on_an_incomplete_hour_unless_allowed(tmp_path, monkeypatch):
    api = FakeOGD(published={0: set(ANALYSIS_VARIABLES)})
    monkeypatch.setattr(kenda_download, "_ogd_api", lambda: api)
    args = kenda_download._parse_args(["--ref-time", "2026-09-29T06:00", "--target", str(tmp_path)])

    with pytest.raises(IncompleteDownload, match="TOT_PREC"):
        kenda_download.run(args)

    args.allow_partial = True
    reported = []

    class Pipes:
        def report_asset_materialization(self, metadata):
            reported.append(metadata)

    kenda_download.run(args, Pipes())
    assert reported[0]["already_present"] == len(ANALYSIS_VARIABLES)
    assert reported[0]["missing"]["type"] == "json"
    assert set(reported[0]["missing"]["raw_value"]["1"]) == set(FORECAST_VARIABLES)


def test_pipes_accepts_the_metadata_of_a_complete_hour():
    """A complete hour has an empty ``missing`` dict, which Pipes rejects untagged."""
    from dagster_pipes import _normalize_param_metadata

    summary = kenda_download.DownloadReport(ref_time=REF_TIME).summary()
    assert summary["missing"] == {}
    _normalize_param_metadata(
        kenda_download.pipes_metadata(summary), "report_asset_materialization", "metadata"
    )


def test_the_provider_reads_what_the_downloader_writes(tmp_path):
    download_kenda(REF_TIME, tmp_path, ogd_api=FakeOGD())
    it = pd.Timestamp("2026-09-29T06:00")

    analysis = kenda.KENDAAnalysisProvider(archive_path=tmp_path).fetch(it)
    forecast = kenda.KENDAForecastProvider(archive_path=tmp_path).fetch(it)

    assert len(analysis) == len(ANALYSIS_VARIABLES) + 2
    assert len(forecast) == len(FORECAST_VARIABLES) + 2
    assert {pathlib.Path(f).name for f in analysis} >= set(CONSTANTS)


# --- Dagster asset --------------------------------------------------------------------


class FakeInvocation:
    def get_materialize_result(self):
        return dg.MaterializeResult(metadata={"fake": True})


class FakeDockerClient:
    def __init__(self):
        self.calls: list[dict] = []

    def run(self, **kwargs):
        self.calls.append(kwargs)
        return FakeInvocation()


def test_the_download_asset_mounts_the_archive_and_passes_the_hour(tmp_path, monkeypatch):
    from dags.assets.nwp import regional_lam
    from planetary_datasets import config as config_module

    monkeypatch.setenv("PLANETARY_DATASETS_DATA_DIR", str(tmp_path))
    monkeypatch.setenv(regional_lam.KENDA_IMAGE_ENV, "example/kenda:test")
    config_module.reset_config_cache()
    client = FakeDockerClient()
    try:
        result = dg.materialize(
            [regional_lam.kenda_download_asset],
            partition_key="2026-09-29-06:00",
            resources={"pipes_docker_client": client},
        )
    finally:
        config_module.reset_config_cache()

    assert result.success
    (call,) = client.calls
    assert call["image"] == "example/kenda:test"
    assert call["command"] == [
        "--ref-time", "2026-09-29T06:00:00Z", "--target", regional_lam.KENDA_CONTAINER_ARCHIVE,
    ]  # fmt: skip
    archive = str((tmp_path / "meteoswiss").resolve())
    assert call["container_kwargs"]["volumes"] == {
        archive: {"bind": regional_lam.KENDA_CONTAINER_ARCHIVE, "mode": "rw"}
    }
    assert (tmp_path / "meteoswiss").is_dir()


def test_the_kenda_stores_depend_on_the_download_after_key_prefixing():
    from dags import loader
    from dags.assets.nwp import regional_lam

    assets, failures = loader.load_assets([regional_lam])
    assert not failures
    graph = {key: set(a.asset_deps[key]) for a in assets for key in a.keys}

    download = dg.AssetKey(["nwp", "kenda_download"])
    assert graph[dg.AssetKey(["nwp", "kenda_analysis"])] == {download}
    assert graph[dg.AssetKey(["nwp", "kenda_forecast"])] == {download}
    assert graph[download] == set()
