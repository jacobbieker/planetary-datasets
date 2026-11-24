"""The skip list of partitions broken at the source, ``planetary_datasets/known_bad.json``."""

import importlib.util
import json
import pathlib

import pandas as pd

from planetary_datasets.common.known_bad import KNOWN_BAD, known_bad

ROOT = pathlib.Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location("_backfill_lanes", ROOT / "_backfill_lanes.py")
lanes = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(lanes)


def test_entries_are_timestamps_with_reasons(tmp_path):
    """Keys become naive timestamps; an unlisted name has nothing to skip."""
    path = tmp_path / "known_bad.json"
    path.write_text(json.dumps({"lane": {"2020-11-12T06:00:00": "broken GRIB"}}))
    assert known_bad("lane", path) == {pd.Timestamp("2020-11-12 06:00"): "broken GRIB"}
    assert known_bad("other", path) == {}


def test_every_entry_names_a_lane_or_a_geos_cf_collection_and_a_reason():
    """A typo in a name would skip nothing, silently."""
    table = json.loads(KNOWN_BAD.read_text())
    for name, entries in table.items():
        lane = name.split("/")[0]
        assert lane in lanes.LANES or lane.startswith("geos_cf_"), name
        for when, why in entries.items():
            pd.Timestamp(when)
            assert why.strip(), f"{name} {when} has no reason"


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
