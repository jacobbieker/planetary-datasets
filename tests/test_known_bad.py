"""The skip list of partitions broken at the source, ``planetary_datasets/known_bad.json``."""

import importlib.util
import json
import pathlib
import re

import pandas as pd

from planetary_datasets.common.known_bad import KNOWN_BAD, known_bad, load
from planetary_datasets.providers.virtualized.geos_cf import STREAMS

ROOT = pathlib.Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location("_backfill_lanes", ROOT / "_backfill_lanes.py")
lanes = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(lanes)

#: ``Collection.short``: ``<group>_<inst|tavg>_<interval>_<levels>``, optionally ``_0p5deg``.
GEOS_CF_SHORT = re.compile(r"^[a-z]+_(inst|tavg)_[0-9a-z]+_[a-z0-9]+(_0p5deg)?$")


def test_entries_are_timestamps_with_reasons(tmp_path):
    """Keys become naive timestamps; an unlisted name has nothing to skip."""
    path = tmp_path / "known_bad.json"
    path.write_text(json.dumps({"lane": {"2020-11-12T06:00:00": "broken GRIB"}}))
    assert known_bad("lane", path) == {pd.Timestamp("2020-11-12 06:00"): "broken GRIB"}
    assert known_bad("other", path) == {}


def test_every_entry_names_a_lane_or_a_geos_cf_collection_and_a_reason():
    """A typo anywhere in a name would skip nothing, silently."""
    table = json.loads(KNOWN_BAD.read_text())
    for name, entries in table.items():
        if "/" in name:
            # geos_cf_<stream>/<collection short name>, as Collection.label has it.
            stream, short = name.split("/", 1)
            assert stream.removeprefix("geos_cf_") in STREAMS, name
            assert stream.startswith("geos_cf_"), name
            assert GEOS_CF_SHORT.match(short), name
        else:
            assert name in lanes.LANES, name
        for when, why in entries.items():
            pd.Timestamp(when)
            assert why.strip(), f"{name} {when} has no reason"


def test_the_list_is_read_afresh(tmp_path):
    """An entry removed from the file is gone at the next read, without a restart."""
    path = tmp_path / "known_bad.json"
    path.write_text(json.dumps({"lane": {"2020-11-12T06:00:00": "broken GRIB"}}))
    assert known_bad("lane", path)
    path.write_text(json.dumps({}))
    assert not known_bad("lane", path)
    assert load(tmp_path / "absent.json") == {}


def test_lane_entries_fall_on_the_lanes_partitions():
    """A skipped time that is not one of the lane's partitions would never match."""
    table = json.loads(KNOWN_BAD.read_text())
    for name, entries in table.items():
        if name not in lanes.LANES:
            continue
        lane = lanes.LANES[name]
        for when in entries:
            stamp = pd.Timestamp(when)
            assert stamp >= pd.Timestamp(lane.start)
            assert (stamp - pd.Timestamp(lane.start)) % pd.Timedelta(lane.freq) == pd.Timedelta(0)
