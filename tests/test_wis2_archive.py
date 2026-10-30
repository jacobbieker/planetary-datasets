"""WIS2 Global Cache archiver, against in-memory stand-ins for both buckets."""

from __future__ import annotations

import csv
import datetime as dt
import gzip
import io
import tarfile

import pytest

from planetary_datasets.providers.observations import wis2_archive as wis2

UTC = dt.timezone.utc
END = dt.datetime(2026, 10, 29, 12, tzinfo=UTC)


class FakeDest:
    """Multipart uploads and plain puts into a dict."""

    def __init__(self, fail_part: int | None = None):
        self.objects: dict[str, bytes] = {}
        self.uploads: dict[str, dict[int, bytes]] = {}
        self.aborted: list[str] = []
        self.fail_part = fail_part

    def create_multipart_upload(self, Bucket, Key):
        self.uploads[Key] = {}
        return {"UploadId": Key}

    def upload_part(self, Bucket, Key, UploadId, PartNumber, Body):
        if PartNumber == self.fail_part:
            raise ConnectionError("part failed")
        self.uploads[UploadId][PartNumber] = bytes(Body)
        return {"ETag": f"etag{PartNumber}"}

    def complete_multipart_upload(self, Bucket, Key, UploadId, MultipartUpload):
        parts = self.uploads.pop(UploadId)
        numbers = [p["PartNumber"] for p in MultipartUpload["Parts"]]
        assert numbers == sorted(parts)
        self.objects[Key] = b"".join(parts[n] for n in numbers)

    def abort_multipart_upload(self, Bucket, Key, UploadId):
        self.uploads.pop(UploadId, None)
        self.aborted.append(Key)

    def put_object(self, Bucket, Key, Body):
        self.objects[Key] = bytes(Body)


LONG = "data/cn-cma/data/core/weather/x/" + "long" * 40 + ".nc"
FILES = {
    "data/de-dwd/data/core/A/O/C/I/21/BABJ/A_1": b"bufr one",
    "data/de-dwd/data/core/A/O/C/I/21/BABJ/A_2": b"bufr two",
    LONG: b"n" * 5000,
    "index.html": b"<html>",
    **{f"data/ca-eccc/data/core/climate/experimental/+.{n:05d}": b"c" for n in range(1500)},
}


def cache() -> tuple[object, dt.datetime, dt.datetime]:
    """An in-memory cache bucket, and a window around when its objects arrived."""
    import obstore
    from obstore.store import MemoryStore

    store = MemoryStore()
    before = dt.datetime.now(UTC) - dt.timedelta(seconds=1)
    for key, body in FILES.items():
        obstore.put(store, key, body)
    return store, before, dt.datetime.now(UTC) + dt.timedelta(seconds=1)


def test_the_listing_takes_every_directory_and_only_the_window():
    store, start, end = cache()
    found = wis2.list_window(store, start, end, workers=4)
    assert [e.key for e in found] == sorted(FILES)
    assert {e.size for e in found if e.key == LONG} == {5000}
    assert wis2.list_window(store, start - dt.timedelta(hours=1), start) == []


def test_an_archive_streams_every_file_into_a_tar_with_an_index(monkeypatch):
    import obstore

    monkeypatch.setattr(wis2, "PART_SIZE", 4096)
    store, start, end = cache()
    dest = FakeDest()
    entries = wis2.list_window(store, start, end)
    gone = "data/de-dwd/data/core/A/O/C/I/21/BABJ/A_2"
    obstore.delete(store, gone)
    out = wis2.MultipartWriter(dest, "bucket", "bkr/obs/wis2/x.tar")
    files, missing, payload, index = wis2.write_archive(
        entries, lambda es: wis2.fetch_all(store, es, workers=7, max_in_flight=100), out
    )
    out.close()
    assert (files, missing) == (len(FILES) - 1, 1)
    assert payload == sum(len(b) for k, b in FILES.items() if k != gone)
    assert len(dest.objects["bkr/obs/wis2/x.tar"]) > 4096, "spread over several parts"

    with tarfile.open(fileobj=io.BytesIO(dest.objects["bkr/obs/wis2/x.tar"])) as tar:
        members = {m.name: m for m in tar.getmembers()}
        assert len(LONG) > 100 and tar.extractfile(LONG).read() == b"n" * 5000
        assert gone not in members and len(members) == len(FILES) - 1
        assert start.timestamp() <= members["index.html"].mtime <= end.timestamp()
    rows = list(csv.DictReader(io.StringIO(gzip.decompress(index).decode())))
    assert sorted(r["key"] for r in rows) == sorted(members)


def test_a_failed_part_aborts_the_upload_and_leaves_nothing(monkeypatch):
    monkeypatch.setattr(wis2, "PART_SIZE", 1024)
    monkeypatch.setattr(wis2, "source_store", lambda: cache()[0])
    dest = FakeDest(fail_part=2)
    monkeypatch.setattr(wis2, "dest_client", lambda config: dest)
    monkeypatch.setattr(wis2, "exists", lambda *a: False)
    with pytest.raises(ConnectionError):
        wis2.archive(_soon(), config=_config())
    assert len(dest.aborted) == 1
    assert not dest.objects


def test_an_archive_is_written_once(monkeypatch):
    dest = FakeDest()
    monkeypatch.setattr(wis2, "source_store", lambda: cache()[0])
    monkeypatch.setattr(wis2, "dest_client", lambda config: dest)
    monkeypatch.setattr(wis2, "exists", lambda client, bucket, key: key in dest.objects)
    first = wis2.archive(_soon(), config=_config())
    assert first.files == len(FILES) and first.key.startswith("bkr/obs/wis2/wis2_")
    assert f"{first.key}.index.csv.gz" in dest.objects
    assert wis2.archive(_soon(), config=_config()).files == 0


def test_windows_end_at_00z_and_12z_once_settled():
    now = dt.datetime(2026, 10, 29, 12, 5, tzinfo=UTC)
    assert wis2.window_ends(now)[-1] == dt.datetime(2026, 10, 29, 0, tzinfo=UTC)
    later = dt.datetime(2026, 10, 29, 12, 11, tzinfo=UTC)
    ends = wis2.window_ends(later)
    assert ends[-1] == END and len(ends) == 4
    assert all(b - a == wis2.EVERY for a, b in zip(ends, ends[1:]))


def test_windows_tile_time_without_gaps_or_overlap():
    assert wis2.LOOKBACK == wis2.EVERY == dt.timedelta(hours=12)
    ends = wis2.window_ends(dt.datetime(2026, 10, 29, 12, 11, tzinfo=UTC))
    for previous, end in zip(ends, ends[1:]):
        assert end - wis2.LOOKBACK == previous, "each window starts where the last ended"


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


def _soon() -> dt.datetime:
    return dt.datetime.now(UTC) + dt.timedelta(minutes=1)


def test_archive_names_carry_the_window_end():
    assert wis2.archive_name(END) == "bkr/obs/wis2/wis2_20261029T1200Z.tar"


def test_the_wait_between_windows_logs_every_hour(caplog):
    from loguru import logger

    slept = []
    handler = logger.add(lambda m: slept.append(("log", m)), level="INFO")
    try:
        wis2.wait(END, 11.5 * 3600, sleep=lambda s: slept.append(("sleep", s)))
    finally:
        logger.remove(handler)
    sleeps = [v for k, v in slept if k == "sleep"]
    assert max(sleeps) <= 3600 and sum(sleeps) == 11.5 * 3600
    assert sum(1 for k, _ in slept if k == "log") == len(sleeps) - 1
