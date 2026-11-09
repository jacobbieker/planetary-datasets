"""``scripts/split_ami_csr.py`` against a local store built from real GK-2A objects."""

from __future__ import annotations

import dataclasses
import datetime as dt
import importlib.util
import pathlib
from collections import Counter

import numpy as np
import pytest
import xarray as xr

from planetary_datasets.providers.observations import wis2_satellite as sat
from planetary_datasets.providers.observations import wis2_satellite_products as sp

HERE = pathlib.Path(__file__).parent
DATA = HERE / "data" / "wis2_satellite_products"
ARRIVED = dt.datetime(2026, 11, 1, 6, tzinfo=dt.timezone.utc)
spec = importlib.util.spec_from_file_location(
    "split_ami_csr", HERE.parent / "scripts" / "split_ami_csr.py"
)
tool = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tool)
#: ami_csr as it was before it filtered by category: every object under the ami topic.
OLD_AMI_CSR = sp.Product("ami_csr", "GK-2A AMI clear-sky radiances", ("ami",))
#: amv as it was before it took the ami topic's vectors (no ``<window>:ami`` entry).
OLD_AMV = dataclasses.replace(sp.PRODUCTS["amv"], later=())


def key(name: str) -> str:
    """Cache key of a fixture."""
    return (DATA / f"{name}.key").read_text().strip()


def body(name: str) -> bytes:
    """Bytes of a fixture."""
    return (DATA / f"{name}.bufr").read_bytes()


def spool(tmp_path, name: str, product: sp.Product = OLD_AMI_CSR) -> str:
    """Decode a fixture for ``product`` (by default the old ami_csr) and spool it."""
    arrays, _ = sp.decode_object(key(name), body(name), product)
    sat._provenance(arrays, key(name), ARRIVED)
    arrays = sat._compact(arrays)
    path = tmp_path / f"{name}.npz"
    np.savez(path, **{k.replace(".", "__"): v for k, v in arrays.items()}, allow_pickle=True)
    return str(path)


@pytest.fixture
def mixed(local_config, tmp_path):
    """A store whose ami_csr holds radiances and vectors, and an amv without the vectors."""
    repo = local_config.icechunk_repo(sat.STORE_PREFIX)
    sat.ensure_root(repo)
    paths = [spool(tmp_path, "ami_amv"), spool(tmp_path, "ami_csr")]
    assert sat.write_product_group(repo, OLD_AMI_CSR, paths, "w", Counter()) == 80
    avhrr = [spool(tmp_path, "amv_avhrr", OLD_AMV)]
    assert sat.write_product_group(repo, OLD_AMV, avhrr, "w", Counter()) > 0
    return repo


def open_group(repo, group: str) -> xr.Dataset:
    """A group of the store as xarray reads it."""
    return xr.open_zarr(
        repo.readonly_session("main").store, group=sat.store_group(group), consolidated=False
    )


def test_it_refuses_while_amv_lacks_a_vector(mixed):
    """Nothing is written while any AMV row of ami_csr is missing from amv."""
    before = open_group(mixed, "ami_csr")
    with pytest.raises(tool.NotVerified, match="40 AMV rows"):
        tool.split(mixed, apply=True)
    after = open_group(mixed, "ami_csr")
    assert after.sizes["observation"] == before.sizes["observation"] == 80
    assert tool.main([]) == 1


def test_it_keeps_only_the_radiances_once_amv_holds_every_vector(mixed, local_config):
    """After the backfill adds the vectors to amv, ami_csr keeps its radiance rows exactly."""
    backfill = sat.SatelliteTarget(local_config, workers=1)
    backfill.prepare("w")
    assert backfill.wants(key("ami_amv"), 1)
    backfill.feed(key("ami_amv"), body("ami_amv"), ARRIVED)
    assert backfill.finish(None, None, "w")["rows"] == {"amv": 40}

    before = open_group(mixed, "ami_csr").load()
    checked = tool.split(mixed, apply=False)
    assert checked["applied"] is False and checked["amv_rows_missing_from_amv"] == 0
    assert open_group(mixed, "ami_csr").sizes["observation"] == 80

    summary = tool.split(mixed, apply=True)
    assert summary["amv_rows"] == 40 and summary["csr_rows"] == 40 and summary["applied"]
    after = open_group(mixed, "ami_csr").load()
    radiances = before.isel(observation=before["bufr_template"].values != "310077")
    assert after.sizes["observation"] == 40
    assert set(after["bufr_template"].values.tolist()) == set(
        radiances["bufr_template"].values.tolist()
    )
    for name in after.data_vars:
        expected = radiances[name]
        if after[name].ndim == 2:
            expected = expected[:, : after[name].shape[1]]
        np.testing.assert_array_equal(after[name].values, expected.values, err_msg=name)
    assert "wind_speed" in summary["removed_arrays"] and "wind_speed" not in after
    # Quality flags are kept even where every kept row's flag is 0.
    assert "quality_flag" in after and after["quality_flag"].sizes["observation"] == 40
    assert not any(name.endswith("_quality") for name in summary["removed_arrays"])
    assert after.attrs["partitions_written"] == ["w"]
    # Nothing left to move: a second run changes nothing.
    assert tool.split(mixed, apply=True)["amv_rows"] == 0


def test_a_later_window_appends_to_the_split_group(mixed, local_config, tmp_path):
    """Radiances of a window after the split append; elements removed come back as fill."""
    backfill = sat.SatelliteTarget(local_config, workers=1)
    backfill.prepare("w")
    backfill.feed(key("ami_amv"), body("ami_amv"), ARRIVED)
    backfill.finish(None, None, "w")
    tool.split(mixed, apply=True)
    split = open_group(mixed, "ami_csr")
    assert "pressure" not in split, "KMA sends it missing in every radiance subset"

    # The next window's radiances: the same subsets moved a degree north, so new rows.
    arrays, _ = sp.decode_object(key("ami_csr"), body("ami_csr"), sp.PRODUCTS["ami_csr"])
    arrays["latitude"] = arrays["latitude"] + 1.0
    arrays["observation_identity"] = sp.identities(arrays)
    sat._provenance(arrays, key("ami_csr"), ARRIVED)
    path = tmp_path / "later.npz"
    np.savez(path, **{k.replace(".", "__"): v for k, v in sat._compact(arrays).items()})
    rows = sat.write_product_group(mixed, sp.PRODUCTS["ami_csr"], [str(path)], "w2", Counter())
    assert rows == 40
    after = open_group(mixed, "ami_csr")
    assert after.sizes["observation"] == 80
    assert after["satellite_channel_centre_frequency"].shape[1] == 24
    assert after["pressure"].isnull().all()
    np.testing.assert_array_equal(
        after["brightness_temperature"].values[:40], split["brightness_temperature"].values
    )
    assert set(after.attrs["partitions_written"]) == {"w", "w2"}
