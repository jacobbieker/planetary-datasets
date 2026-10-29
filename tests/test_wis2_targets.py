"""The WIS2 archive-target framework: registry, queued decoding, routing, BUFR rows."""

from __future__ import annotations

import datetime as dt
import io
import tarfile

import pytest
from wis2_bufr import bulletin, encode_amdar, report

from planetary_datasets.providers.observations import wis2_archive as wis2
from planetary_datasets.providers.observations import wis2_targets as targets
from planetary_datasets.providers.observations.wis2_targets import (
    QueuedTarget,
    Router,
    rows_from_bufr,
    run_targets,
    split_bufr,
)

UTC = dt.timezone.utc


class Recorder(QueuedTarget):
    """Keeps the keys it decodes; fails on any key containing ``bad``."""

    name = "recorder"
    store_prefix = "bkr/test/recorder.icechunk"

    def __init__(self, config=None):
        super().__init__(config)
        self.keys: list[str] = []
        self.finished: list[tuple] = []

    def wants(self, key, size):
        return key.startswith("data/x/")

    def decode(self, key, body, arrived):
        if "bad" in key:
            raise ValueError("cannot decode")
        self.keys.append(key)

    def write(self, window_start, window_end, partition):
        self.finished.append((window_start, window_end, partition))
        return {"rows": len(self.keys)}


class Broken(Recorder):
    name = "broken"

    def write(self, window_start, window_end, partition):
        raise RuntimeError("store unreachable")


def test_a_queued_target_decodes_off_thread_and_counts_failures():
    target = Recorder()
    route = Router([target])
    for key in ("data/x/1", "data/y/2", "data/x/bad", "data/x/3"):
        route(key, 1, b"body", None)
    summary = target.finish(None, None, "wis2_20261029T1200Z")
    assert target.keys == ["data/x/1", "data/x/3"]
    assert summary == {"objects": 2, "undecodable_objects": 1, "rows": 2}
    assert target.finished == [(None, None, "wis2_20261029T1200Z")]


def test_a_failing_target_is_reported_not_raised():
    good, bad = Recorder(), Broken()
    summaries = run_targets([bad, good], None, None, "p")
    assert "error" in summaries["broken"] and summaries["recorder"]["rows"] == 0


def test_registered_targets_are_discovered_and_built(monkeypatch):
    monkeypatch.setattr(targets, "TARGETS", {})
    targets.register("recorder")(Recorder)
    assert [t.name for t in targets.build_targets(None, ["recorder"])] == ["recorder"]
    assert "recorder" in [t.name for t in targets.build_targets(None)]
    with pytest.raises(ValueError, match="unknown"):
        targets.build_targets(None, ["nope"])


def test_wis2_modules_register_themselves():
    loaded = targets.load_targets()
    assert "aircraft" in loaded


def test_bufr_rows_are_cut_per_subset_with_repeated_elements_collected():
    message = encode_amdar(
        [
            report("HK0013", edr=[(0.1, 0.2), (0.15, 0.3)]),
            report("HK0014", minute=37, temperature=250.15),
        ]
    )
    data = bulletin(message, message)
    messages = list(split_bufr(data))
    assert len(messages) == 2 and messages[0] == message
    rows = rows_from_bufr(
        message,
        ["aircraftRegistrationNumberOrOtherIdentification", "airTemperature", "minute",
         "meanTurbulenceIntensityEddyDissipationRate"],
        repeated=["meanTurbulenceIntensityEddyDissipationRate"],
    )  # fmt: skip
    assert [r["aircraftRegistrationNumberOrOtherIdentification"] for r in rows] == [
        "HK0013",
        "HK0014",
    ]
    assert rows[1]["airTemperature"] == pytest.approx(250.15) and rows[1]["minute"] == 37
    assert rows[0]["meanTurbulenceIntensityEddyDissipationRate"] == pytest.approx([0.1, 0.15])
    assert "meanTurbulenceIntensityEddyDissipationRate" not in rows[1]


def test_compressed_messages_give_the_same_rows():
    reports = [report("HK0013"), report("HK0014", minute=37, temperature=250.15)]
    keys = ["aircraftRegistrationNumberOrOtherIdentification", "airTemperature", "minute"]
    plain = rows_from_bufr(encode_amdar(reports), keys)
    packed = rows_from_bufr(encode_amdar(reports, compressed=True), keys)
    assert packed == plain


def test_missing_values_are_none():
    rows = rows_from_bufr(encode_amdar([report()]), ["dewpointTemperature", "mixingRatio"])
    assert rows == [{"dewpointTemperature": None, "mixingRatio": None}]


# ----------------------------------------------------------------------------- archive hook


class FakeDest:
    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def create_multipart_upload(self, Bucket, Key):
        self.objects[Key + ".parts"] = b""
        return {"UploadId": Key}

    def upload_part(self, Bucket, Key, UploadId, PartNumber, Body):
        self.objects[UploadId + ".parts"] += bytes(Body)
        return {"ETag": str(PartNumber)}

    def complete_multipart_upload(self, Bucket, Key, UploadId, MultipartUpload):
        self.objects[Key] = self.objects.pop(UploadId + ".parts")

    def abort_multipart_upload(self, Bucket, Key, UploadId):
        self.objects.pop(UploadId + ".parts", None)

    def put_object(self, Bucket, Key, Body):
        self.objects[Key] = bytes(Body)

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[Key])}


def _cache():
    import obstore
    from obstore.store import MemoryStore

    store = MemoryStore()
    before = dt.datetime.now(UTC) - dt.timedelta(seconds=1)
    for key in ("data/x/one", "data/x/bad", "data/y/two"):
        obstore.put(store, key, key.encode())
    return store, before


def test_archive_feeds_its_targets_and_a_broken_one_never_fails_the_tar(monkeypatch):
    store, _ = _cache()
    dest = FakeDest()
    built = [Recorder(), Broken()]
    monkeypatch.setattr(wis2, "source_store", lambda: store)
    monkeypatch.setattr(wis2, "dest_client", lambda config: dest)
    monkeypatch.setattr(wis2, "exists", lambda *a: False)
    monkeypatch.setattr(wis2, "build_targets", lambda config, names: built)
    end = dt.datetime.now(UTC) + dt.timedelta(minutes=1)
    summary = wis2.archive(end, config=_config())
    assert summary.files == 3 and summary.key in dest.objects
    assert built[0].keys == ["data/x/one"]
    partition = wis2.partition_name(summary.key)
    assert built[0].finished == [(end - wis2.LOOKBACK, end, partition)]
    assert summary.targets["recorder"]["rows"] == 1 and "error" in summary.targets["broken"]


def test_targets_run_over_an_archived_tar(monkeypatch):
    dest = FakeDest()
    tar_bytes = io.BytesIO()
    with tarfile.open(fileobj=tar_bytes, mode="w") as tar:
        for key in ("data/x/one", "data/y/two"):
            info = tarfile.TarInfo(key)
            info.size = len(key)
            info.mtime = 1761552000
            tar.addfile(info, io.BytesIO(key.encode()))
    key = "bkr/obs/wis2/wis2_20261027T1200Z.tar"
    dest.objects[key] = tar_bytes.getvalue()
    target = Recorder()
    monkeypatch.setattr(wis2, "dest_client", lambda config: dest)
    monkeypatch.setattr(wis2, "build_targets", lambda config, names: [target])
    summaries = wis2.targets_from_tar(key, config=_config())
    assert target.keys == ["data/x/one"] and summaries["recorder"]["rows"] == 1
    end = dt.datetime(2026, 10, 27, 12, tzinfo=UTC)
    assert target.finished == [(end - wis2.LOOKBACK, end, "wis2_20261027T1200Z")]


def test_no_targets_when_asked_for_none(monkeypatch):
    called = []
    monkeypatch.setattr(wis2, "build_targets", lambda config, names: called.append(names) or [])
    assert wis2._build_targets(_config(), []) == [] and called == []


def _config():
    from planetary_datasets.config import Config, Credentials

    return Config(
        bucket="bucket",
        prefix="",
        region="us-west-2",
        endpoint_url=None,
        force_path_style=None,
        allow_http=False,
        data_dir=None,
        scratch_dir=None,
        icechunk_local_path=None,
        hf_repo_id=None,
        memory_fraction=0.8,
        memory_ceiling_gb=None,
        credentials=Credentials(),
    )
