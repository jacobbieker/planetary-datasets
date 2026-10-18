"""``download_ranged``: parallel byte-range downloads, without the network."""

# ruff: noqa: D102, D103, D107 - test names say what each test checks

from __future__ import annotations

import threading

import pytest
from aiohttp import ClientResponseError

from planetary_datasets.common import download as dl

URL = "https://thredds.met.no/thredds/fileServer/meps25epsarchive/2026/10/15/run.nc"
BLOB = bytes(range(256)) * 41  # 10,496 bytes: not a whole number of pieces


class RangeFS:
    """Serves ``BLOB`` by range; ``flaky`` ranges fail once before succeeding."""

    async_impl = True

    def __init__(self, blob: bytes = BLOB, flaky: tuple[int, ...] = (), size_error=None):
        self.blob = blob
        self.flaky = set(flaky)
        self.size_error = size_error
        self.calls: list[tuple[int, int]] = []
        self.lock = threading.Lock()

    def size(self, path, **kwargs):
        if self.size_error is not None:
            raise self.size_error
        return len(self.blob)

    def cat_file(self, path, start=None, end=None, **kwargs):
        with self.lock:
            self.calls.append((start, end))
            if start in self.flaky:
                self.flaky.discard(start)
                raise ConnectionResetError("dropped")
        return self.blob[start:end]


@pytest.fixture
def serve(monkeypatch):
    def _serve(fs: RangeFS) -> RangeFS:
        monkeypatch.setattr(dl, "_filesystem_for", lambda uri, options: (fs, uri))
        monkeypatch.setattr(dl.time, "sleep", lambda s: None)
        return fs

    return _serve


def test_pieces_reassemble_into_the_whole_object(serve, tmp_path):
    fs = serve(RangeFS())
    path = dl.download_ranged(URL, tmp_path / "run.nc", connections=4, piece_size=1000)
    assert path.read_bytes() == BLOB
    assert len(fs.calls) == 11
    assert not (tmp_path / "run.nc.part").exists()


def test_a_dropped_piece_is_retried_on_its_own(serve, tmp_path):
    fs = serve(RangeFS(flaky=(3000,)))
    path = dl.download_ranged(URL, tmp_path / "run.nc", connections=4, piece_size=1000)
    assert path.read_bytes() == BLOB
    assert [c for c in fs.calls if c[0] == 3000] == [(3000, 4000), (3000, 4000)]
    assert len(fs.calls) == 12


def test_a_short_piece_fails_the_download_and_leaves_no_file(serve, tmp_path):
    class Short(RangeFS):
        def cat_file(self, path, start=None, end=None, **kwargs):
            return (
                super().cat_file(path, start, end)[:-1]
                if start == 2000
                else super().cat_file(path, start, end)
            )

    serve(Short())
    with pytest.raises(OSError, match="bytes 2000-3000"):
        dl.download_ranged(URL, tmp_path / "run.nc", piece_size=1000, retries=2)
    assert list(tmp_path.iterdir()) == []


def test_a_missing_object_is_none_not_an_error(serve, tmp_path):
    serve(RangeFS(size_error=FileNotFoundError(URL)))
    assert dl.download_ranged(URL, tmp_path / "run.nc") is None
    assert list(tmp_path.iterdir()) == []


def test_a_server_error_on_sizing_is_raised(serve, tmp_path):
    error = ClientResponseError(request_info=None, history=(), status=503)
    serve(RangeFS(size_error=error))
    with pytest.raises(ClientResponseError):
        dl.download_ranged(URL, tmp_path / "run.nc", retries=2)


def test_an_existing_download_is_kept(serve, tmp_path):
    fs = serve(RangeFS())
    (tmp_path / "run.nc").write_bytes(b"done")
    assert dl.download_ranged(URL, tmp_path / "run.nc").read_bytes() == b"done"
    assert fs.calls == []
