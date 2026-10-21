"""Per-host request spacing and backoff in ``common.download``."""

from __future__ import annotations

from planetary_datasets.common import download as dl

GDEX = "https://data.gdex.ucar.edu/d473002/grib/WRF24/2025/01/10/x.grb"


def test_requests_to_a_limited_host_are_spaced(monkeypatch):
    clock = [100.0]
    slept = []
    monkeypatch.setattr(dl.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(dl.time, "sleep", slept.append)
    monkeypatch.setattr(dl, "_HOST_NEXT", {})
    for _ in range(3):
        dl.wait_for_host(GDEX)
    assert slept == [5.0, 10.0]


def test_other_hosts_are_not_delayed(monkeypatch):
    slept = []
    monkeypatch.setattr(dl.time, "sleep", slept.append)
    for _ in range(3):
        dl.wait_for_host("https://opendata.dwd.de/weather/x.grib2")
    assert slept == []


def test_a_limited_host_backs_off_for_longer():
    assert dl.host_backoff(GDEX, 1.0) == 60.0
    assert dl.host_backoff("https://example.com/x", 1.0) == 1.0
