"""SondeHub archive access through the history.sondehub.org front end, with HTTP stubbed.

Direct anonymous S3 access to ``sondehub-history`` returns AccessDenied since November
2026, so the provider and the backfill driver now go through SondeHub's S3-compatible front
end. These tests serve a fake archive two ways and never touch the network:

* a loopback HTTP server speaking enough of the S3 REST API (ListObjectsV2 with
  continuation tokens, GetObject, HeadObject) for s3fs, path-style; and
* a botocore ``before-send`` hook for the driver's boto3 client, which also checks that the
  documented endpoint plus virtual-host addressing really lands on history.sondehub.org.
"""

from __future__ import annotations

import gzip
import http.server
import io
import json
import threading
import urllib.parse
from datetime import date
from xml.sax.saxutils import escape

import pandas as pd
import pytest
from botocore.awsrequest import AWSResponse
from helpers import read_store

import sondehub_t
from planetary_datasets.providers.observations import sondehub as sondehub_module
from planetary_datasets.providers.observations.sondehub import (
    SONDEHUB_ENDPOINT,
    SondehubProvider,
    archive_storage_options,
    list_day_keys,
    read_frames,
)

#: Keys per listing page, small so a five-sonde day needs three pages.
PAGE_SIZE = 2

DAY = "2026-11-20"
SERIALS = ["S0005", "S0001", "S0003", "S0002", "S0004"]


def _frame(serial: str, minute: int) -> dict:
    return {
        "datetime": f"{DAY}T11:{minute:02d}:00Z",
        "serial": serial,
        "lat": "50.1",
        "lon": "4.8",
        "alt": str(500 + minute),
        "type": "RS41",
    }


def _objects() -> dict[str, tuple[bytes, dict[str, str]]]:
    """The fake bucket: key -> (body, extra response headers)."""
    objects: dict[str, tuple[bytes, dict[str, str]]] = {}
    for serial in SERIALS:
        summary = [_frame(serial, 0), _frame(serial, 30), _frame(serial, 59)]
        objects[f"date/2026/11/20/{serial}.json"] = (json.dumps(summary).encode(), {})
        full = [_frame(serial, m) for m in range(60)]
        # The front end labels these Content-Encoding: gzip, as the real one does.
        objects[f"serial/{serial}.json.gz"] = (
            gzip.compress(json.dumps(full).encode()),
            {"Content-Encoding": "gzip", "Content-Type": "application/json"},
        )
    objects["date/2026/11/20/README.txt"] = (b"not a sonde", {})
    return objects


class FakeArchive:
    """Answers S3 REST requests for one bucket, recording what was asked."""

    def __init__(self) -> None:
        self.objects = _objects()
        self.requests: list[tuple[str, str, dict]] = []

    def respond(self, method: str, key: str, query: dict) -> tuple[int, dict, bytes]:
        """Return ``(status, headers, body)`` for a request on ``key`` (no bucket)."""
        self.requests.append((method, key, query))
        if key == "" and method == "GET":
            return self._list(query)
        if key in self.objects:
            body, headers = self.objects[key]
            headers = {"Content-Length": str(len(body)), "ETag": '"etag"', **headers}
            return 200, headers, b"" if method == "HEAD" else body
        error = (
            b'<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchKey</Code>'
            b"<Message>The specified key does not exist.</Message></Error>"
        )
        return 404, {"Content-Type": "application/xml"}, b"" if method == "HEAD" else error

    def _list(self, query: dict) -> tuple[int, dict, bytes]:
        prefix = query.get("prefix", "")
        keys = sorted(k for k in self.objects if k.startswith(prefix))
        start = int(query.get("continuation-token", "0"))
        page = keys[start : start + PAGE_SIZE]
        truncated = start + PAGE_SIZE < len(keys)
        contents = "".join(
            f"<Contents><Key>{escape(k)}</Key><LastModified>2026-11-20T20:00:00.000Z"
            f'</LastModified><ETag>"etag"</ETag><Size>{len(self.objects[k][0])}</Size>'
            "<StorageClass>STANDARD</StorageClass></Contents>"
            for k in page
        )
        token = f"<NextContinuationToken>{start + PAGE_SIZE}</NextContinuationToken>"
        body = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
            f"<Name>history</Name><Prefix>{escape(prefix)}</Prefix>"
            f"<KeyCount>{len(page)}</KeyCount><MaxKeys>{PAGE_SIZE}</MaxKeys>"
            f"<IsTruncated>{str(truncated).lower()}</IsTruncated>"
            f"{token if truncated else ''}{contents}</ListBucketResult>"
        )
        return 200, {"Content-Type": "application/xml"}, body.encode()

    def list_pages(self) -> int:
        """How many listing requests were served."""
        return sum(1 for method, key, _ in self.requests if method == "GET" and key == "")


@pytest.fixture
def archive_server():
    """A loopback S3 endpoint serving bucket ``history`` path-style.

    Yields the archive and the storage options that point s3fs at it.
    """
    archive = FakeArchive()

    class Handler(http.server.BaseHTTPRequestHandler):
        def _serve(self, method: str) -> None:
            url = urllib.parse.urlsplit(self.path)
            bucket, _, key = url.path.lstrip("/").partition("/")
            query = dict(urllib.parse.parse_qsl(url.query, keep_blank_values=True))
            if bucket != "history":
                status, headers, body = 404, {}, b""
            else:
                status, headers, body = archive.respond(method, urllib.parse.unquote(key), query)
            self.send_response(status)
            for name, value in headers.items():
                self.send_header(name, value)
            if "Content-Length" not in headers:
                self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if method != "HEAD":
                self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802 - http.server's naming
            self._serve("GET")

        def do_HEAD(self) -> None:  # noqa: N802 - http.server's naming
            self._serve("HEAD")

        def log_message(self, *args) -> None:
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    options = {
        "client_kwargs": {"endpoint_url": f"http://127.0.0.1:{server.server_port}"},
        "config_kwargs": {"s3": {"addressing_style": "path"}},
    }
    try:
        yield archive, options
    finally:
        server.shutdown()
        server.server_close()


# --- provider (s3fs) ----------------------------------------------------------------------


def test_default_options_are_the_documented_front_end():
    """Endpoint sondehub.org + bucket ``history`` + virtual addressing = history.sondehub.org."""
    options = archive_storage_options()
    assert options["anon"] is True
    assert options["client_kwargs"]["endpoint_url"] == SONDEHUB_ENDPOINT == "https://sondehub.org"
    assert options["config_kwargs"] == {"s3": {"addressing_style": "virtual"}}
    assert sondehub_module.SONDEHUB_BUCKET == "history"


def test_storage_options_override_without_touching_the_defaults():
    options = archive_storage_options({"client_kwargs": {"endpoint_url": "http://mirror"}})
    assert options["client_kwargs"] == {"endpoint_url": "http://mirror"}
    options["config_kwargs"]["s3"]["addressing_style"] = "path"
    assert archive_storage_options()["config_kwargs"]["s3"]["addressing_style"] == "virtual"


def test_day_listing_follows_every_page(archive_server):
    archive, options = archive_server
    keys = list_day_keys(DAY, storage_options=options)
    assert keys == [f"history/date/2026/11/20/{s}.json" for s in sorted(SERIALS)]
    # Six keys under the prefix at two a page: the listing had to page to see them all.
    assert archive.list_pages() == 3
    tokens = [q.get("continuation-token") for m, k, q in archive.requests if k == ""]
    assert tokens == [None, "2", "4"]


def test_day_listing_of_an_uncovered_day_is_empty(archive_server):
    _, options = archive_server
    assert list_day_keys("1999-01-01", storage_options=options) == []


def test_read_frames_reads_a_day_object(archive_server):
    _, options = archive_server
    frames = read_frames("history/date/2026/11/20/S0001.json", options)
    assert [f["datetime"] for f in frames][0] == f"{DAY}T11:00:00Z"
    assert len(frames) == 3


def test_read_frames_reads_a_gzip_encoded_serial_object(archive_server):
    _, options = archive_server
    frames = sondehub_module.download_serial("S0002", options)
    assert len(frames) == 60
    assert {f["serial"] for f in frames} == {"S0002"}


def test_read_frames_on_a_missing_object(archive_server):
    _, options = archive_server
    with pytest.raises(FileNotFoundError):
        read_frames("history/serial/NOPE.json.gz", options)


def test_provider_writes_a_day_through_the_front_end(local_config, archive_server):
    _, options = archive_server
    provider = SondehubProvider(config=local_config, storage_options=options, workers=2)
    assert provider.run_partition(pd.Timestamp(DAY)) is True
    stored = read_store(provider)
    # Five sondes, three summary frames each; the README is not a sonde.
    assert stored.sizes["time"] == 15
    assert sorted(set(stored["serial"].values.tolist())) == sorted(SERIALS)
    assert stored.attrs["source"] == "https://history.sondehub.org/date"


# --- backfill driver (boto3) --------------------------------------------------------------


class _Raw(io.BytesIO):
    """The bits of a urllib3 response that botocore reads.

    ``stream`` serves parsed bodies such as listings; ``read`` a streamed ``GetObject`` body.
    """

    def stream(self, *args, **kwargs):
        yield self.getvalue()


@pytest.fixture
def stubbed_client():
    """The driver's real client, answered by the fake archive instead of the network."""
    archive = FakeArchive()
    hosts: list[str] = []
    client = sondehub_t.make_client(workers=2)

    def _send(request, **kwargs):
        url = urllib.parse.urlsplit(request.url)
        hosts.append(url.hostname)
        query = dict(urllib.parse.parse_qsl(url.query, keep_blank_values=True))
        key = urllib.parse.unquote(url.path.lstrip("/"))
        status, headers, body = archive.respond(request.method, key, query)
        return AWSResponse(request.url, status, headers, _Raw(body))

    client.meta.events.register("before-send.s3", _send)
    return client, archive, hosts


def test_driver_requests_go_to_history_sondehub_org(stubbed_client):
    client, _, hosts = stubbed_client
    sondehub_t.list_serials(client, date(2026, 11, 20))
    assert hosts and set(hosts) == {"history.sondehub.org"}


def test_driver_lists_every_page(stubbed_client):
    client, archive, _ = stubbed_client
    serials = sondehub_t.list_serials(client, date(2026, 11, 20))
    assert sorted(serials) == sorted(SERIALS)
    assert archive.list_pages() == 3


def test_driver_fetches_a_gzip_encoded_flight(stubbed_client):
    client, _, _ = stubbed_client
    frames = sondehub_t.fetch_flight(client, "S0003")
    assert isinstance(frames, list) and len(frames) == 60


def test_driver_reports_a_missing_flight_as_permanent(stubbed_client):
    """A missing object stays a permanent miss, exactly as before the endpoint change."""
    client, _, _ = stubbed_client
    code = sondehub_t.fetch_flight(client, "NOPE")
    assert code == "NoSuchKey"
    assert code in sondehub_t.PERMANENT_ERRORS
