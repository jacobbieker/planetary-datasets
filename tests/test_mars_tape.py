"""A MARS request refused for an unavailable tape is not retried every few minutes."""

import os
import time

import pandas as pd

from planetary_datasets.providers import mars_pipeline as mp

TAPE_LOG = [
    "mars - ERROR  - 20261101.140533 - Mars server task finished in error",
    "mars - ERROR  - 20261101.140533 - AccessError: The tape on which the data reside is "
    "unavailable. Please visit ECMWF Support Portal and raise a ticket: "
    "http://support.ecmwf.int [marsod]",
    "mars - ERROR  - 20261101.140533 - Expected 19190, got 0.",
]


def _tape_refused(request, target):
    os._exit(mp.TAPE_UNAVAILABLE_EXIT)


def _job(tmp_path):
    return mp.MarsJob(
        target=tmp_path / "output_fc_18z_20260725_20260726.grib",
        request={"class": "od"},
        group="ml",
        valid_times=pd.DatetimeIndex(["2026-07-25 19:00"]),
    )


def test_the_tape_error_is_recognised_in_the_server_log():
    assert mp.tape_unavailable(TAPE_LOG)
    assert not mp.tape_unavailable(TAPE_LOG[2:])
    assert not mp.tape_unavailable(["ERROR 102 (USER_QUEUED_LIMIT_EXCEEDED)"])


def test_an_unavailable_tape_gives_up_at_once_instead_of_retrying(tmp_path):
    began = time.monotonic()
    ok = mp.retrieve(
        _job(tmp_path), attempts=5, retry_seconds=600, poll_seconds=0.05, execute=_tape_refused
    )
    assert ok is False
    assert time.monotonic() - began < 60, "no 600 s retry sleeps"
    assert list(tmp_path.iterdir()) == []
