"""HAFS virtual stores, exercised offline: the parser and the bucket listing are faked.

The fakes mirror what ``GribberishParser`` returns for one HAFS ``.atm`` file: per group, a
dataset on ``(time, [level], latitude, longitude)`` whose data variables are
``ManifestArray`` references (one GRIB message per chunk) and whose 1-D ``latitude`` and
``longitude`` are plain values. Nothing is decoded, so no network is needed.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import xarray as xr
import zarr

from planetary_datasets.providers.virtualized import hafs
from planetary_datasets.providers.virtualized.hafs import (
    STEPS,
    HafsVirtualProvider,
    _smallest_messages,
    _storm_runs,
)

NY, NX = 4, 5
CYCLE = pd.Timestamp("2026-10-15T00:00")


def _manifest(name: str, url: str, offsets, lengths, dims, shape):
    from gribberish.virtualizarr.parser import _gribberish_codecs
    from virtualizarr.manifests import ChunkManifest, ManifestArray
    from virtualizarr.manifests.utils import create_v3_array_metadata

    grid = (*shape[:-2], 1, 1)
    paths = np.full(grid, url, dtype=np.dtypes.StringDType())
    manifest = ChunkManifest.from_arrays(
        paths=paths,
        offsets=np.asarray(offsets, dtype=np.uint64).reshape(grid),
        lengths=np.asarray(lengths, dtype=np.uint64).reshape(grid),
    )
    metadata = create_v3_array_metadata(
        shape=shape,
        data_type=np.dtype("float64"),
        chunk_shape=(*[1] * (len(shape) - 2), *shape[-2:]),
        fill_value=float("nan"),
        codecs=_gribberish_codecs(name, north_up=True),
        attributes={"long_name": name, "forecast_date": "x"},
        dimension_names=dims,
    )
    return ManifestArray(metadata=metadata, chunkmanifest=manifest)


def fake_file(url: str, lat0: float, lon0: float, extra: bool = False):
    """One parsed ``.atm`` file: ``/msl/instant`` and ``/isobar/instant`` groups."""
    lat = lat0 + 0.02 * np.arange(NY)[::-1]
    lon = lon0 + 0.02 * np.arange(NX)
    coords = {"time": [np.datetime64("2026-10-15T00", "ns")], "latitude": lat, "longitude": lon}
    msl_vars = {
        "prmsl": _manifest(
            "prmsl", url, [0], [500], ("time", "latitude", "longitude"), (1, NY, NX)
        ),
        "mslet": _manifest(
            "mslet", url, [500], [90], ("time", "latitude", "longitude"), (1, NY, NX)
        ),
    }
    if extra:
        msl_vars["gust"] = _manifest(
            "gust", url, [590], [300], ("time", "latitude", "longitude"), (1, NY, NX)
        )
    msl = xr.Dataset(
        {k: (("time", "latitude", "longitude"), v) for k, v in msl_vars.items()}, coords=coords
    )
    tmp = _manifest(
        "tmp",
        url,
        [1000, 2000],
        [400, 450],
        ("time", "isobar", "latitude", "longitude"),
        (1, 2, NY, NX),
    )
    iso = xr.Dataset(
        {"tmp": (("time", "isobar", "latitude", "longitude"), tmp)},
        coords={**coords, "isobar": [50000.0, 85000.0]},
    )
    return {"/msl/instant": msl, "/isobar/instant": iso}


def listing(it: pd.Timestamp, storms, steps=STEPS, model="hfsa", domain="storm", idx=True):
    names = []
    for storm in storms:
        for s in steps:
            stem = f"{storm}.{it:%Y%m%d%H}.{model}.{domain}.atm.f{s:03d}.grb2"
            names.append(stem)
            if idx:
                names.append(stem + ".idx")
        names.append(f"{storm}.{it:%Y%m%d%H}.{model}.storm_info")
    return names


@pytest.fixture
def fake_bucket(monkeypatch):
    """Fake listings and parses; ``state`` says what the bucket holds."""
    state = {"listings": {}, "extra": set()}

    def list_folder(bucket, folder):
        return state["listings"].get(folder, [])

    def parse(product, url, registry):
        name = url.rsplit("/", 1)[-1]
        storm, cycle, *_ = name.split(".")
        step = int(name.split(".f")[-1][:3])
        # The storm nest drifts north 0.1 deg per step; each cycle starts further west.
        lat0 = 15.0 + 0.1 * step / 3
        lon0 = 250.0 - (int(cycle[-2:]) / 6) + (10.0 if storm == "21e" else 0.0)
        return fake_file(url, lat0, lon0, extra=(storm, cycle) in state["extra"])

    monkeypatch.setattr(hafs, "_list_folder", list_folder)
    monkeypatch.setattr(hafs, "_parse", parse)
    monkeypatch.setattr(hafs, "_storm_name", lambda product, it, storm: f"name{storm}")
    monkeypatch.setattr(hafs, "_registry", lambda bucket: None)
    # Every faked run is settled, whatever today's date.
    monkeypatch.setattr(hafs, "SETTLE", pd.Timedelta(0))
    return state


def _publish(state, it, storms, **kwargs):
    product = hafs.PRODUCTS["hafs_a_storm"]
    state["listings"][product.folder(it)] = listing(it, storms, **kwargs)


def _open(provider, prefix=None):
    repo = provider.repo(prefix or provider.product.store_prefix)
    return repo.readonly_session("main").store


# ---------------------------------------------------------------------------- naming


def test_products_cover_both_variants_and_domains():
    assert sorted(hafs.PRODUCTS) == [
        "hafs_a_parent",
        "hafs_a_storm",
        "hafs_b_parent",
        "hafs_b_storm",
    ]
    product = hafs.PRODUCTS["hafs_b_parent"]
    assert product.store_prefix == "bkr/dmi/virtualized/hafs_b_parent.icechunk"
    assert product.url(CYCLE, "20e", 3) == (
        "s3://noaa-nws-hafs-pds/hfsb/20261015/00/20e.2026101500.hfsb.parent.atm.f003.grb2"
    )
    assert STEPS == tuple(range(0, 127, 3))


def test_cycles_are_six_hourly_and_clipped_to_the_archive():
    got = hafs.cycles("2023-06-19", "2023-06-20T06")
    assert list(got) == list(pd.date_range("2023-06-19T18", "2023-06-20T06", freq="6h"))


def test_storm_runs_keep_only_this_variant_domain_and_cycle():
    names = (
        listing(CYCLE, ["20e"], steps=(0, 3), idx=True)
        + listing(CYCLE, ["21e"], steps=(0,), idx=False)
        + listing(CYCLE, ["20e"], domain="parent")
        + listing(CYCLE, ["20e"], model="hfsb")
        + ["20e.2026101500.hfsa.storm.sat.f003.grb2", "20e.2026101500.hfsa.parent.swath.grb2"]
    )
    runs = _storm_runs(hafs.PRODUCTS["hafs_a_storm"], CYCLE, names)
    assert [(r.storm, r.steps, r.indexed) for r in runs] == [
        ("20e", [0, 3], [0, 3]),
        ("21e", [0], []),
    ]
    assert not runs[0].complete
    full = _storm_runs(hafs.PRODUCTS["hafs_a_storm"], CYCLE, listing(CYCLE, ["20e"]))
    assert full[0].complete


def test_pending_waits_for_young_partial_runs_but_takes_settled_ones(local_config):
    provider = HafsVirtualProvider("hafs_a_storm", config=local_config)
    names = listing(CYCLE, ["20e"], steps=STEPS[:10]) + listing(CYCLE, ["21e"])
    young = provider.pending(CYCLE, listing=names, now=CYCLE + pd.Timedelta(hours=8))
    assert [r.storm for r in young] == ["21e"]
    old = provider.pending(CYCLE, listing=names, now=CYCLE + pd.Timedelta(days=3))
    assert [r.storm for r in old] == ["20e", "21e"]


def test_smallest_message_per_step_is_chosen_for_the_grid_coordinates():
    variables, _ = hafs._collect({0: fake_file("s3://noaa-nws-hafs-pds/a", 10.0, 200.0)})
    assert _smallest_messages(variables) == {0: ("s3://noaa-nws-hafs-pds/a", 500, 90)}


# ---------------------------------------------------------------------------- writing


def test_partition_appends_one_run_per_storm_with_moving_grids(local_config, fake_bucket):
    provider = HafsVirtualProvider("hafs_a_storm", config=local_config)
    second = CYCLE + pd.Timedelta(hours=6)
    _publish(fake_bucket, CYCLE, ["20e", "21e"])
    # The later cycle lost its last step: settled, so it is referenced with a gap.
    _publish(fake_bucket, second, ["20e"], steps=STEPS[:-1])

    assert provider.run_partition(CYCLE)
    assert provider.run_partition(second)
    assert not provider.run_partition(CYCLE)

    root = zarr.open_group(_open(provider), mode="r")
    assert sorted(root.attrs[hafs.LEDGER_ATTR]) == [
        "2026-10-15T00:00:00/20e",
        "2026-10-15T00:00:00/21e",
        "2026-10-15T06:00:00/20e",
    ]
    ds = xr.open_zarr(_open(provider), group="msl/instant", consolidated=False)
    assert ds.sizes == {"run": 3, "step": len(STEPS), "y": NY, "x": NX}
    assert ds.prmsl.dims == ("run", "step", "y", "x")
    assert list(ds.storm.values) == ["20e", "21e", "20e"]
    assert list(ds.storm_name.values) == ["name20e", "name21e", "name20e"]
    assert list(ds.init_time.values) == [
        np.datetime64(CYCLE),
        np.datetime64(CYCLE),
        np.datetime64(second),
    ]
    assert set(ds.coords) >= {"latitude", "longitude", "center_latitude", "center_longitude"}
    assert ds.latitude.dims == ("run", "step", "y")
    assert ds.longitude.dims == ("run", "step", "x")
    assert "forecast_date" not in ds.prmsl.attrs

    centre_lat = ds.center_latitude.values
    centre_lon = ds.center_longitude.values
    # The nest follows the storm within a run ...
    assert centre_lat[0, 1] - centre_lat[0, 0] == pytest.approx(0.1)
    # ... a second storm in the same cycle has its own grid ...
    assert centre_lon[1, 0] - centre_lon[0, 0] == pytest.approx(10.0)
    # ... and the next cycle starts somewhere else.
    assert centre_lon[2, 0] == pytest.approx(centre_lon[0, 0] - 1.0)
    assert np.isnan(centre_lat[2, -1]) and not np.isnan(centre_lat[2, -2])

    iso = xr.open_zarr(_open(provider), group="isobar/instant", consolidated=False)
    assert iso.tmp.dims == ("run", "step", "isobar", "y", "x")
    assert list(iso.isobar.values) == [50000.0, 85000.0]


def test_references_point_at_the_source_files(local_config, fake_bucket):
    provider = HafsVirtualProvider("hafs_a_storm", config=local_config)
    _publish(fake_bucket, CYCLE, ["20e"], steps=(0, 3))
    assert provider.run_partition(CYCLE)
    session = provider.repo(provider.product.store_prefix).readonly_session("main")
    locations = set(session.all_virtual_chunk_locations())
    assert locations == {hafs.PRODUCTS["hafs_a_storm"].url(CYCLE, "20e", s) for s in (0, 3)}


def test_a_run_with_new_fields_starts_a_generation_and_older_runs_fit_it(local_config, fake_bucket):
    provider = HafsVirtualProvider("hafs_a_storm", config=local_config)
    later = CYCLE + pd.Timedelta(days=1)
    _publish(fake_bucket, CYCLE, ["20e"])
    _publish(fake_bucket, later, ["20e"])
    fake_bucket["extra"].add(("20e", f"{later:%Y%m%d%H}"))

    assert provider.run_partition(CYCLE)
    assert provider.run_partition(later)
    base, newer = provider.generations()
    assert newer.endswith("hafs_a_storm_16102026.icechunk")
    assert provider.ledger(newer) == {"2026-10-16T00:00:00/20e"}

    # A backfilled run without the new field lands in the newest generation that holds it.
    earlier = CYCLE - pd.Timedelta(days=1)
    _publish(fake_bucket, earlier, ["20e"])
    assert provider.run_partition(earlier)
    assert provider.ledger(newer) == {"2026-10-14T00:00:00/20e", "2026-10-16T00:00:00/20e"}
    ds = xr.open_zarr(_open(provider, newer), group="msl/instant", consolidated=False)
    assert list(ds.storm.values) == ["20e", "20e"]
    assert "gust" in ds


def test_missing_timesteps_lists_cycles_with_unstored_runs(local_config, fake_bucket):
    provider = HafsVirtualProvider("hafs_a_storm", config=local_config)
    second = CYCLE + pd.Timedelta(hours=6)
    _publish(fake_bucket, CYCLE, ["20e"])
    _publish(fake_bucket, second, ["20e"])
    cycles = [CYCLE, second, second + pd.Timedelta(hours=6)]
    assert provider.missing_timesteps(cycles) == [CYCLE, second]
    provider.run_partition(CYCLE)
    assert provider.missing_timesteps(cycles) == [second]
