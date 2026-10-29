"""A stalled EUMETSAT download gives up instead of blocking its lane."""

from __future__ import annotations

import socket
import threading
import time

import pytest
import requests

from planetary_datasets.providers.polar._eumdac import _set_read_timeout


def _stalling_server() -> tuple[int, threading.Event]:
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    done = threading.Event()

    def serve():
        conn, _ = listener.accept()
        conn.recv(65536)
        conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 1000000\r\n\r\n" + b"x" * 1000)
        done.wait(10)  # then nothing: the connection stalls mid-body
        conn.close()
        listener.close()

    threading.Thread(target=serve, daemon=True).start()
    return listener.getsockname()[1], done


def test_a_silent_connection_times_out_mid_body():
    port, done = _stalling_server()
    try:
        with requests.get(f"http://127.0.0.1:{port}/", stream=True) as response:
            raw = response.raw
            _set_read_timeout(raw, 0.5)
            began = time.monotonic()
            with pytest.raises(Exception, match="(?i)timed out"):
                while raw.read(65536):
                    pass
            assert time.monotonic() - began < 5
    finally:
        done.set()
