"""The WIS2 archive-target framework: registry, queued decoding, routing, BUFR rows."""

from __future__ import annotations

import datetime as dt
import io
import json
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

    def head_object(self, Bucket, Key):
        from botocore.exceptions import ClientError

        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {}


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


def _archived_tar(dest, key="bkr/obs/wis2/wis2_20261027T1200Z.tar"):
    tar_bytes = io.BytesIO()
    with tarfile.open(fileobj=tar_bytes, mode="w") as tar:
        for name in ("data/x/one", "data/y/two"):
            info = tarfile.TarInfo(name)
            info.size = len(name)
            info.mtime = 1761552000
            tar.addfile(info, io.BytesIO(name.encode()))
    dest.objects[key] = tar_bytes.getvalue()
    return key


def _state(dest, key):
    return json.loads(dest.objects[f"{key}.targets.json"])


def test_an_archive_records_which_targets_finished(monkeypatch):
    store, _ = _cache()
    dest = FakeDest()
    monkeypatch.setattr(wis2, "source_store", lambda: store)
    monkeypatch.setattr(wis2, "dest_client", lambda config: dest)
    monkeypatch.setattr(wis2, "build_targets", lambda config, names: [Recorder(), Broken()])
    summary = wis2.archive(dt.datetime.now(UTC) + dt.timedelta(minutes=1), config=_config())
    assert _state(dest, summary.key) == {
        "pending": [],
        "done": ["recorder"],
        "failed": ["broken"],
    }


def test_targets_an_interrupted_run_left_pending_are_finished_from_the_tar(monkeypatch):
    dest = FakeDest()
    key = _archived_tar(dest)
    dest.objects[f"{key}.targets.json"] = json.dumps(
        {"pending": ["recorder"], "done": ["broken"], "failed": []}
    ).encode()
    built_for = []

    def build(config, names):
        built_for.append(names)
        return [Recorder()]

    monkeypatch.setattr(wis2, "dest_client", lambda config: dest)
    monkeypatch.setattr(wis2, "build_targets", build)
    end = dt.datetime(2026, 10, 27, 12, tzinfo=UTC)
    summary = wis2.archive(end, config=_config())
    assert built_for == [["recorder"]], "only the pending target, not the finished one"
    assert summary.files == 0 and summary.targets["recorder"]["rows"] == 1
    assert _state(dest, key) == {"pending": [], "done": ["broken", "recorder"], "failed": []}
    built_for.clear()
    wis2.archive(end, config=_config())
    assert built_for == [], "nothing left to finish"


def test_an_archived_tar_without_a_target_record_is_left_alone(monkeypatch):
    dest = FakeDest()
    _archived_tar(dest)
    monkeypatch.setattr(wis2, "dest_client", lambda config: dest)
    monkeypatch.setattr(
        wis2, "build_targets", lambda config, names: pytest.fail("no targets should run")
    )
    wis2.archive(dt.datetime(2026, 10, 27, 12, tzinfo=UTC), config=_config())


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


def test_targets_get_plain_bytes_from_the_archives_obstore_buffers():
    """The archive streams obstore ``Bytes``; BUFR splitting needs ``bytes`` methods."""
    import obstore
    from obstore.store import MemoryStore

    from planetary_datasets.providers.observations.wis2_targets import Router

    store = MemoryStore()
    obstore.put(store, "k", b"xxBUFR")
    body = obstore.get(store, "k").bytes()
    assert not isinstance(body, bytes)
    seen = []

    class Target:
        name = "t"

        def wants(self, key, size):
            return True

        def feed(self, key, data, arrived):
            seen.append((type(data), data.find(b"BUFR")))

    Router([Target(), Target()])("k", 6, body, None)
    assert seen == [(bytes, 2), (bytes, 2)]


def test_marked_targets_are_finished_between_windows_while_time_allows(monkeypatch):
    dest = FakeDest()
    old, new = "bkr/obs/wis2/wis2_20261027T1200Z.tar", "bkr/obs/wis2/wis2_20261028T0000Z.tar"
    for key in (old, new):
        _archived_tar(dest, key)
    dest.objects[f"{new}.targets.json"] = json.dumps(
        {"pending": [], "done": ["recorder", "broken"], "failed": []}
    ).encode()
    built: list[Recorder] = []
    monkeypatch.setattr(wis2, "dest_client", lambda config: dest)
    monkeypatch.setattr(wis2, "archived_tars", lambda config=None: [old, new])
    monkeypatch.setattr(
        wis2, "build_targets", lambda config, names: built.append(Recorder()) or [built[-1]]
    )
    assert wis2.mark_pending(["recorder"], config=_config()) == 2
    assert _state(dest, new) == {"pending": ["recorder"], "done": ["broken"], "failed": []}

    now = dt.datetime(2026, 11, 1, 15, tzinfo=UTC)
    soon = now + wis2.PENDING_BUDGET - dt.timedelta(minutes=1)
    assert wis2.finish_pending(soon, _config(), now=lambda: now) == 0, "too close to a window"
    assert built == []
    later = now + dt.timedelta(hours=9)
    assert wis2.finish_pending(later, _config(), now=lambda: now) == 2
    assert [t.finished[0][2] for t in built] == ["wis2_20261027T1200Z", "wis2_20261028T0000Z"]
    assert _state(dest, old)["done"] == ["recorder"] and _state(dest, new)["pending"] == []
    assert wis2.finish_pending(later, _config(), now=lambda: now) == 0, "nothing left"


def test_a_pending_target_that_raises_is_not_retried_in_a_loop(monkeypatch):
    dest = FakeDest()
    key = _archived_tar(dest)
    dest.objects[f"{key}.targets.json"] = json.dumps(
        {"pending": ["recorder"], "done": [], "failed": []}
    ).encode()
    monkeypatch.setattr(wis2, "dest_client", lambda config: dest)
    monkeypatch.setattr(wis2, "archived_tars", lambda config=None: [key])

    def boom(*args, **kwargs):
        raise ConnectionError("source unreachable")

    monkeypatch.setattr(wis2, "targets_from_tar", boom)
    now = dt.datetime(2026, 11, 1, 15, tzinfo=UTC)
    assert wis2.finish_pending(now + dt.timedelta(hours=9), _config(), now=lambda: now) == 0


def test_targets_are_told_the_window_before_it_streams(monkeypatch):
    dest = FakeDest()
    key = _archived_tar(dest)
    seen = []

    class Declining(Recorder):
        def prepare(self, partition):
            seen.append(partition)
            self.declined = True

        def wants(self, key, size):
            return not getattr(self, "declined", False) and super().wants(key, size)

    target = Declining()
    monkeypatch.setattr(wis2, "dest_client", lambda config: dest)
    monkeypatch.setattr(wis2, "build_targets", lambda config, names: [target])
    wis2.targets_from_tar(key, config=_config())
    assert seen == ["wis2_20261027T1200Z"] and target.keys == []
