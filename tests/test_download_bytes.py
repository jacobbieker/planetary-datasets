"""``read_bytes``: whole-object reads into memory with retries, without the network."""

# ruff: noqa: D102, D103, D107 - test names say what each test checks

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from aiohttp import ClientResponseError

from planetary_datasets.common import download as dl

S3_URI = "s3://us-west-2.opendata.source.coop/bkr/icon/icon-art-eu/p/T_2M/r/2026-10-11T00:00/s/PT000H00M.grib2"
DWD_URI = "https://opendata.dwd.de/weather/nwp/v1/m/icon-art-eu/p/T_2M/r/2026-10-13T06%3A00/s/PT000H00M.grib2"


def _http_error(status: int) -> ClientResponseError:
    return ClientResponseError(request_info=None, history=(), status=status)


class FakeFS:
    """Stands in for an async fsspec filesystem; ``cat_file`` plays a script of outcomes."""

    async_impl = True
    script: list = []
    calls: list = []
    instances: list = []

    def __init__(self, **options):
        self.options = options
        FakeFS.instances.append(self)

    @staticmethod
    def _strip_protocol(uri: str) -> str:
        return uri.split("://", 1)[1] if uri.startswith("s3://") else uri

    def cat_file(self, path, **kwargs):
        FakeFS.calls.append((path, kwargs))
        outcome = FakeFS.script.pop(0) if FakeFS.script else b"GRIBpayload"
        if isinstance(outcome, BaseException):
            raise outcome
        if callable(outcome):
            return outcome()
        return outcome


@pytest.fixture
def fake_fs(monkeypatch):
    FakeFS.script, FakeFS.calls, FakeFS.instances = [], [], []
    monkeypatch.setattr(dl.fsspec, "get_filesystem_class", lambda protocol: FakeFS)
    monkeypatch.setattr(dl, "_SHARED_FILESYSTEMS", {})
    return FakeFS


@pytest.fixture
def sleeps(monkeypatch):
    slept: list[float] = []
    monkeypatch.setattr(dl.time, "sleep", slept.append)
    return slept


def test_success_returns_the_bytes_in_one_call(fake_fs, sleeps):
    assert dl.read_bytes(S3_URI, storage_options={"anon": True}) == b"GRIBpayload"
    assert fake_fs.calls == [(S3_URI.removeprefix("s3://"), {"timeout": 60.0})]
    assert fake_fs.instances[0].options == {"anon": True, "skip_instance_cache": True}
    assert sleeps == []


def test_https_url_is_sent_verbatim(fake_fs, sleeps):
    """DWD's ``%3A`` must reach the server as is, not decoded or double-encoded."""
    dl.read_bytes(DWD_URI)
    assert fake_fs.calls[0][0] == DWD_URI
    assert fake_fs.instances[0].options == {"encoded": True, "skip_instance_cache": True}


def test_caller_can_override_url_encoding(fake_fs, sleeps):
    dl.read_bytes(DWD_URI, storage_options={"encoded": False})
    assert fake_fs.instances[0].options == {"encoded": False, "skip_instance_cache": True}


def test_transient_failures_are_retried_with_exponential_backoff(fake_fs, sleeps):
    fake_fs.script = [TimeoutError("slow"), ConnectionResetError("reset"), b"GRIBok"]
    assert dl.read_bytes(S3_URI, retries=3, backoff=1.0) == b"GRIBok"
    assert len(fake_fs.calls) == 3
    assert sleeps == [1.0, 2.0]


@pytest.mark.parametrize(
    "error",
    [
        FileNotFoundError("404"),
        PermissionError("403"),
        _http_error(403),
        _http_error(404),
        _http_error(410),
    ],
    ids=["not-found", "permission", "http-403", "http-404", "http-410"],
)
def test_permanent_errors_return_none_without_retrying(fake_fs, sleeps, error):
    fake_fs.script = [error]
    assert dl.read_bytes(S3_URI, retries=5) is None
    assert len(fake_fs.calls) == 1
    assert sleeps == []


def _raise_like_fsspec_sync(error: BaseException):
    """Re-raise ``error`` the way ``fsspec.asyn.sync`` does, from a frame holding it."""

    def sync():
        return_result = error
        raise return_result

    return sync


@pytest.mark.parametrize("first", [FileNotFoundError("gone"), TimeoutError("slow")])
def test_a_handled_error_does_not_keep_the_callers_locals_alive(fake_fs, sleeps, first):
    # A re-raised error is in a cycle with the frame that raised it, and frames reach
    # their callers through f_back: unbroken, a miss pinned a reader's 150 MB block buffer
    # until the cyclic collector happened to run.
    import gc
    import weakref

    class Held:
        pass

    def caller():
        held = Held()
        dl.read_bytes(S3_URI, retries=2)
        return weakref.ref(held)

    fake_fs.script = [_raise_like_fsspec_sync(first), b"GRIB"]
    gc.disable()
    try:
        assert caller()() is None
    finally:
        gc.enable()


@pytest.mark.parametrize(
    "error", [TimeoutError("slow"), _http_error(503)], ids=["timeout", "http-503"]
)
def test_exhausted_retries_raise_the_last_error(fake_fs, sleeps, error):
    fake_fs.script = [error] * 3
    with pytest.raises(type(error)):
        dl.read_bytes(S3_URI, retries=3, backoff=0.5)
    assert len(fake_fs.calls) == 3
    assert sleeps == [0.5, 1.0]


def test_no_timeout_is_passed_when_disabled(fake_fs, sleeps):
    dl.read_bytes(S3_URI, timeout=None)
    assert fake_fs.calls[0][1] == {}


def test_retries_below_one_is_rejected(fake_fs):
    with pytest.raises(ValueError):
        dl.read_bytes(S3_URI, retries=0)
    assert fake_fs.calls == []


def test_one_filesystem_is_shared_across_threads(fake_fs, sleeps):
    uris = [S3_URI.replace("PT000H", f"PT{i:03d}H") for i in range(32)]
    seen_threads: set[int] = set()

    def read(uri: str) -> bytes | None:
        seen_threads.add(threading.get_ident())
        return dl.read_bytes(uri, storage_options={"anon": True})

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(read, uris))

    assert results == [b"GRIBpayload"] * len(uris)
    assert len(seen_threads) > 1
    assert len(fake_fs.instances) == 1
    # Different options are a different filesystem.
    dl.read_bytes(S3_URI, storage_options={"anon": False})
    assert len(fake_fs.instances) == 2


def test_a_forked_child_starts_with_a_free_lock_and_no_filesystems(fake_fs, monkeypatch):
    """A lock held by a parent thread at fork time must not hang the child."""
    held = threading.Lock()
    held.acquire()
    monkeypatch.setattr(dl, "_SHARED_FILESYSTEMS_LOCK", held)
    dl._SHARED_FILESYSTEMS["stale"] = object()

    dl._reset_shared_filesystems_after_fork()

    assert dl._SHARED_FILESYSTEMS == {}
    assert dl._SHARED_FILESYSTEMS_LOCK is not held
    assert dl.read_bytes(S3_URI, timeout=None) == b"GRIBpayload"


class TestLocalFiles:
    """The synchronous fallback, against real fsspec on the local filesystem."""

    def test_reads_a_local_file(self, tmp_path):
        path = tmp_path / "field.grib2"
        path.write_bytes(b"GRIB" + bytes(100))
        assert dl.read_bytes(str(path)) == b"GRIB" + bytes(100)
        assert dl.read_bytes(path.as_uri()) == b"GRIB" + bytes(100)

    def test_a_missing_local_file_is_none(self, tmp_path, sleeps):
        assert dl.read_bytes(str(tmp_path / "missing.grib2"), retries=5) is None
        assert sleeps == []
