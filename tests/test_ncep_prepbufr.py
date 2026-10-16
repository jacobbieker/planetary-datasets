"""Offline tests for the NCEP PREPBUFR upper-air stores (aircraft and radiosondes).

pb2nc is never run: the tests write small files in MET's point-observation layout and
stub out the download and decode, then exercise the real ``process`` and the ledgered
writes against a local icechunk store.
"""

from __future__ import annotations

import pathlib
import stat

import numpy as np
import pandas as pd
import pytest
import xarray as xr
import zarr
from helpers import read_store

from planetary_datasets.providers.observations import ncep_prepbufr as ncep
from planetary_datasets.providers.observations.amdar import (
    MissingToolError,
    render_pb2nc_config,
    run_pb2nc_docker,
)
from planetary_datasets.providers.observations.ncep_prepbufr import (
    ADPUPA_STORE,
    AMDAR_STORE,
    FALLBACK_ATTR,
    LEDGER_ATTR,
    NCEPUpperAirProvider,
    PrepbufrUnavailable,
    as_cycle,
    cycle_label,
    ledger_add,
    ledger_contains,
    ledger_runs,
    merge_mass_and_wind,
    pb2nc_config_text,
    pb2nc_levels,
    source_urls,
)

CYCLE = pd.Timestamp("2026-10-10T06")
MET_VARS = ["PRES", "SPFH", "TMP", "HGT", "UGRD", "VGRD"]


def met_file(
    path: pathlib.Path,
    headers: list[dict],
    observations: list[tuple],
    report_types: bool = True,
) -> pathlib.Path:
    """Write a file in MET's point-observation layout.

    Args:
        path: Where to write the file.
        headers: ``typ``, ``sid``, ``vld`` (``YYYYMMDD_HHMMSS``), ``lat``, ``lon``,
            ``elv`` and optionally ``prpt`` (PREPBUFR report type) per report.
        observations: ``(header, variable, level_hpa, height, value, quality)`` per value.
        report_types: Write ``hdr_prpt_typ``; the oldest files lack it.
    """

    def table(values):
        unique = list(dict.fromkeys(values))
        return np.array(unique, dtype="S40"), np.array([unique.index(v) for v in values])

    typ_table, typ = table([h["typ"] for h in headers])
    sid_table, sid = table([h["sid"] for h in headers])
    vld_table, vld = table([h["vld"] for h in headers])
    qty_table, qty = table([o[5] for o in observations])
    fill = {"_FillValue": -9999}
    data = {
        "hdr_typ": ("nhdr", typ.astype("int32")),
        "hdr_sid": ("nhdr", sid.astype("int32")),
        "hdr_vld": ("nhdr", vld.astype("int32")),
        "hdr_lat": ("nhdr", np.array([h["lat"] for h in headers], dtype="float32")),
        "hdr_lon": ("nhdr", np.array([h["lon"] for h in headers], dtype="float32")),
        "hdr_elv": ("nhdr", np.array([h["elv"] for h in headers], dtype="float32")),
        "obs_qty": ("nobs", qty.astype("int32")),
        "obs_hid": ("nobs", np.array([o[0] for o in observations], dtype="int32")),
        "obs_vid": ("nobs", np.array([MET_VARS.index(o[1]) for o in observations], "int32")),
        "obs_lvl": ("nobs", np.array([o[2] for o in observations], dtype="float32")),
        "obs_hgt": ("nobs", np.array([o[3] for o in observations], dtype="float32")),
        "obs_val": ("nobs", np.array([o[4] for o in observations], dtype="float32")),
        "hdr_typ_table": ("nhdr_typ", typ_table),
        "hdr_sid_table": ("nhdr_sid", sid_table),
        "hdr_vld_table": ("nhdr_vld", vld_table.astype("S16")),
        "obs_qty_table": ("nobs_qty", qty_table.astype("S16")),
        "obs_var": ("obs_var_num", np.array(MET_VARS, dtype="S40")),
    }
    if report_types:
        prpt = np.array([h.get("prpt", np.nan) for h in headers], dtype="float64")
        data["hdr_prpt_typ"] = ("npbhdr", np.where(np.isnan(prpt), -9999, prpt).astype("int32"))
        data["hdr_inst_typ"] = ("npbhdr", np.full(len(headers), -9999, dtype="int32"))
    ds = xr.Dataset(data)
    encoding = {name: dict(fill) for name in ("hdr_prpt_typ", "hdr_inst_typ") if name in ds}
    encoding.update({name: {"_FillValue": -9999.0} for name in ("obs_hgt", "hdr_elv")})
    ds.to_netcdf(path, encoding=encoding)
    return path


def aircraft_cycle(path: pathlib.Path, cycle: pd.Timestamp = CYCLE) -> pathlib.Path:
    """One AMDAR report split into mass and wind halves, one sounding, and stragglers.

    The sounding has two levels. One aircraft report sits exactly on the window's end and
    belongs to the next cycle.
    """
    stamp = lambda hours: (cycle + pd.Timedelta(hours=hours)).strftime("%Y%m%d_%H%M%S")  # noqa: E731
    headers = [
        # 0, 1: the mass and wind halves of one AMDAR report.
        dict(typ="AIRCFT", sid="EU1234", vld=stamp(1), lat=52.0, lon=350.0, elv=10000, prpt=131),
        dict(typ="AIRCFT", sid="EU1234", vld=stamp(1), lat=52.0, lon=350.0, elv=10000, prpt=231),
        # 2: an ACARS report with mass only.
        dict(typ="AIRCAR", sid="US0001", vld=stamp(-2), lat=40.0, lon=-100.0, elv=3000, prpt=133),
        # 3, 4: a radiosonde, mass and wind.
        dict(typ="ADPUPA", sid="03354", vld=stamp(-1), lat=53.0, lon=-1.25, elv=117, prpt=120),
        dict(typ="ADPUPA", sid="03354", vld=stamp(-1), lat=53.0, lon=-1.25, elv=117, prpt=220),
        # 5: on the window's end, so the next cycle's.
        dict(typ="AIRCAR", sid="US0002", vld=stamp(3), lat=41.0, lon=-90.0, elv=9000, prpt=133),
    ]
    obs = [
        (0, "PRES", 264.0, 10000, 26400.0, "2"),
        (0, "TMP", 264.0, 10000, 223.15, "1"),
        (0, "HGT", 264.0, 10000, 10000.0, "2"),
        (1, "PRES", 264.0, 10000, 26400.0, "2"),
        (1, "UGRD", 264.0, 10000, 30.0, "1"),
        (1, "VGRD", 264.0, 10000, -5.0, "1"),
        (2, "PRES", 700.0, 3000, 70000.0, "2"),
        (2, "TMP", 700.0, 3000, 270.0, "13"),
        (2, "SPFH", 700.0, 3000, 0.002, "2"),
        (3, "PRES", 1000.0, np.nan, 100000.0, "2"),
        (3, "TMP", 1000.0, np.nan, 285.0, "2"),
        (3, "PRES", 500.0, 5500, 50000.0, "2"),
        (3, "TMP", 500.0, 5500, 250.0, "2"),
        (4, "PRES", 500.0, 5500, 50000.0, "2"),
        (4, "UGRD", 500.0, 5500, 20.0, "2"),
        (4, "VGRD", 500.0, 5500, 2.0, "2"),
        (5, "PRES", 300.0, 9000, 30000.0, "2"),
        (5, "TMP", 300.0, 9000, 230.0, "2"),
    ]
    return met_file(path, headers, obs)


def read_nc(path) -> xr.Dataset:
    with xr.open_dataset(path, decode_times=False) as handle:
        return handle.load()


# --- cycles and the ledger --------------------------------------------------------------


def test_as_cycle_rejects_times_between_cycles():
    assert as_cycle(pd.Timestamp("2026-10-10T06:00", tz="UTC")) == CYCLE
    with pytest.raises(ValueError, match="GDAS cycle"):
        as_cycle(pd.Timestamp("2026-10-10T03:00"))


def test_ledger_merges_consecutive_cycles_into_runs():
    raw = []
    for hour in ("00", "06", "18"):
        raw = ledger_add(raw, pd.Timestamp(f"2026-10-10T{hour}"))
    assert raw == [["2026-10-10T00", "2026-10-10T06"], ["2026-10-10T18", "2026-10-10T18"]]
    raw = ledger_add(raw, pd.Timestamp("2026-10-10T12"))
    assert raw == [["2026-10-10T00", "2026-10-10T18"]]
    runs = ledger_runs(raw)
    assert ledger_contains(runs, pd.Timestamp("2026-10-10T12"))
    assert not ledger_contains(runs, pd.Timestamp("2026-10-11T00"))
    # Adding a cycle twice changes nothing.
    assert ledger_add(raw, pd.Timestamp("2026-10-10T06")) == raw


def test_cycle_label_round_trips_through_the_ledger():
    assert cycle_label(CYCLE) == "2026-10-10T06"
    assert ledger_runs([["2026-10-10T06", "2026-10-10T06"]]) == ledger_runs(ledger_add([], CYCLE))


# --- sources ----------------------------------------------------------------------------


def test_source_urls_follow_the_archive_generations():
    assert source_urls(pd.Timestamp("1997-04-30T18")) == []
    old = source_urls(pd.Timestamp("2000-01-01T00"))
    assert old[0].endswith("/prepnr/2000/prepbufr.gdas.2000010100.wo40.nr")
    assert old[1].endswith("/prepnr/2000/prepbufr.gdas.2000010100.nr")
    assert source_urls(pd.Timestamp("2010-06-01T12")) == [
        "https://data.gdex.ucar.edu/d337000/prepnr/2010/prepbufr.gdas.2010060112.nr"
    ]
    recent = source_urls(CYCLE, now=pd.Timestamp("2026-10-16"))
    assert recent == [
        "https://data.gdex.ucar.edu/d337000/prep48h/2026/prepbufr.gdas.20261010.t06z.nr.48h"
    ]
    overdue = source_urls(CYCLE, now=pd.Timestamp("2027-01-01"))
    assert overdue[1].endswith("/prepnr/2026/prepbufr.gdas.2026101006.nr")


def test_pb2nc_config_keeps_every_quality_mark_and_both_kinds():
    text = pb2nc_config_text("/scratch")
    assert 'message_type = [ "AIRCAR", "AIRCFT", "ADPUPA" ];' in text
    assert "quality_mark_thresh = 15;" in text
    assert 'obs_bufr_var = [ "POB", "QOB", "TOB", "ZOB", "UOB", "VOB" ];' in text
    assert 'tmp_dir = "/scratch";' in text


def test_default_pb2nc_config_is_unchanged():
    text = render_pb2nc_config("/scratch")
    assert 'message_type = [ "AIRCAR", "AIRCFT" ];' in text
    assert "obs_bufr_var = [];" in text
    assert "quality_mark_thresh = 5;" in text


# --- decoding ---------------------------------------------------------------------------


def test_levels_pivot_variables_and_merge_mass_with_wind(tmp_path):
    levels = pb2nc_levels(read_nc(aircraft_cycle(tmp_path / "c.nc")))
    # One row per (report, level): 2 + 1 + 2 + 1 + 1 aircraft/sonde levels.
    assert len(levels) == 7
    merged = merge_mass_and_wind(levels)
    assert len(merged) == 5
    amdar = merged[merged.station_id == "EU1234"].iloc[0]
    assert amdar.report_type == 131
    assert amdar.temperature == pytest.approx(223.15)
    assert (amdar.u_wind, amdar.v_wind) == pytest.approx((30.0, -5.0))
    assert amdar.pressure == pytest.approx(26400.0)
    assert amdar.temperature_qm == 1 and amdar.wind_qm == 1
    # 350E is stored as -10.
    assert amdar.longitude == pytest.approx(-10.0)
    acars = merged[merged.station_id == "US0001"].iloc[0]
    assert acars.temperature_qm == 13
    assert np.isnan(acars.u_wind)
    sonde = merged[merged.station_id == "03354"].sort_values("pressure")
    assert sonde.pressure.tolist() == [50000.0, 100000.0]
    assert sonde.iloc[0].u_wind == pytest.approx(20.0)
    assert np.isnan(sonde.iloc[1].u_wind)
    # A level with no observed height takes none rather than a fill value.
    assert np.isnan(sonde.iloc[1].height)


def test_marked_copy_wins_over_the_unmarked_duplicate(tmp_path):
    """The oldest files carry every value twice, once marked and once with NA."""
    headers = [dict(typ="ADPUPA", sid="72520", vld="20050101_000000", lat=40.5, lon=-80.2, elv=360)]
    obs = [
        (0, "PRES", 850.0, 1500, 85000.0, "2"),
        (0, "TMP", 850.0, 1500, 280.0, "2"),
        (0, "PRES", 850.0, 1500, 85000.0, "NA"),
        (0, "TMP", 850.0, 1500, 281.5, "NA"),
    ]
    path = met_file(tmp_path / "old.nc", headers, obs, report_types=False)
    levels = pb2nc_levels(read_nc(path))
    assert len(levels) == 1
    assert levels.temperature.iloc[0] == pytest.approx(280.0)
    assert levels.temperature_qm.iloc[0] == 2
    assert np.isnan(levels.report_type.iloc[0])


def test_pb2nc_levels_rejects_other_files():
    with pytest.raises(ValueError, match="MET point-observation"):
        pb2nc_levels(xr.Dataset({"x": ("y", [1.0])}))


# --- the provider -----------------------------------------------------------------------


@pytest.fixture
def provider(local_config):
    return NCEPUpperAirProvider(config=local_config)


@pytest.fixture
def staged_fetch(provider, tmp_path, monkeypatch):
    """Make ``fetch`` hand back a synthetic decoded file per cycle."""
    calls = []

    def fetch(it, temp_dir=None, **kwargs):
        cycle = as_cycle(it)
        calls.append(cycle)
        name = f"prepbufr.gdas.{cycle:%Y%m%d}.t{cycle:%H}z.nr.48h.nc"
        return [str(aircraft_cycle(tmp_path / name, cycle))]

    monkeypatch.setattr(provider, "fetch", fetch)
    return calls


def stored(config, prefix) -> tuple[xr.Dataset | None, dict]:
    repo = config.icechunk_repo(prefix)
    group = zarr.open_group(repo.readonly_session("main").store, mode="r")
    attrs = dict(group.attrs)
    return (read_store(repo) if "time" in group else None), attrs


def test_run_partition_writes_both_stores_with_ledgers(provider, local_config, staged_fetch):
    assert provider.run_partition(CYCLE) is True
    amdar, amdar_attrs = stored(local_config, AMDAR_STORE)
    sondes, sonde_attrs = stored(local_config, ADPUPA_STORE)
    assert set(amdar["message_type"].values) == {"AIRCAR", "AIRCFT"}
    # The report on the window's end is left for the next cycle.
    assert sorted(amdar["station_id"].values) == ["EU1234", "US0001"]
    assert set(sondes["station_id"].values) == {"03354"}
    assert sondes.sizes["time"] == 2
    for attrs in (amdar_attrs, sonde_attrs):
        assert attrs[LEDGER_ATTR] == [["2026-10-10T06", "2026-10-10T06"]]
        assert "aircraft_coverage" in attrs
    assert amdar["temperature"].attrs["units"] == "K"
    assert amdar["temperature"].dtype == np.float32
    assert bool((np.diff(amdar.time.values) >= np.timedelta64(0, "s")).all())


def test_stored_cycles_are_skipped_without_fetching(provider, staged_fetch):
    assert provider.run_partition(CYCLE) is True
    assert provider.run_partition(CYCLE) is False
    assert staged_fetch == [CYCLE]
    later = CYCLE + pd.Timedelta(6, "h")
    assert provider.missing_timesteps(pd.DatetimeIndex([CYCLE, later])) == [later]


def test_cycles_append_in_any_order(provider, local_config, staged_fetch):
    for hour in (12, 0, 6):
        assert provider.run_partition(pd.Timestamp(f"2026-10-10T{hour:02d}")) is True
    amdar, attrs = stored(local_config, AMDAR_STORE)
    assert amdar.sizes["time"] == 6
    assert attrs[LEDGER_ATTR] == [["2026-10-10T00", "2026-10-10T12"]]


def test_an_interrupted_cycle_does_not_duplicate_the_store_written_first(
    provider, local_config, staged_fetch
):
    ds = provider.process(provider.fetch(CYCLE), CYCLE)
    sondes = ds.isel(time=np.flatnonzero(ds["message_type"].values == "ADPUPA"))
    ncep.append_with_ledger(provider.repo_for(ADPUPA_STORE), sondes, CYCLE, "ncep_adpupa")
    assert provider.missing_timesteps([CYCLE]) == [CYCLE]

    assert provider.run_partition(CYCLE) is True
    sonde_store, _ = stored(local_config, ADPUPA_STORE)
    assert sonde_store.sizes["time"] == 2


def test_an_empty_cycle_is_recorded_and_a_later_one_creates_the_arrays(
    provider, local_config, tmp_path, monkeypatch
):
    empty = met_file(tmp_path / "empty.nc", [], [])
    monkeypatch.setattr(provider, "fetch", lambda it, temp_dir=None, **kw: [str(empty)])
    assert provider.run_partition(CYCLE) is True
    data, attrs = stored(local_config, AMDAR_STORE)
    assert data is None
    assert attrs[LEDGER_ATTR] == [["2026-10-10T06", "2026-10-10T06"]]

    later = CYCLE + pd.Timedelta(6, "h")
    monkeypatch.setattr(
        provider,
        "fetch",
        lambda it, temp_dir=None, **kw: [str(aircraft_cycle(tmp_path / "later.nc", later))],
    )
    assert provider.run_partition(later) is True
    data, attrs = stored(local_config, AMDAR_STORE)
    assert data.sizes["time"] == 2
    assert attrs[LEDGER_ATTR] == [["2026-10-10T06", "2026-10-10T12"]]


def test_a_non_restricted_stand_in_is_listed(provider, local_config, tmp_path, monkeypatch):
    path = aircraft_cycle(tmp_path / "prepbufr.gdas.2026101006.nr.nc")
    monkeypatch.setattr(provider, "fetch", lambda it, temp_dir=None, **kw: [str(path)])
    provider.run_partition(CYCLE)
    _, attrs = stored(local_config, AMDAR_STORE)
    assert attrs[FALLBACK_ATTR] == ["2026-10-10T06"]


def test_fetch_returns_nothing_when_the_archive_has_no_file(provider, monkeypatch):
    monkeypatch.setattr(ncep, "download_one", lambda url, dest, **kw: None)
    monkeypatch.setattr(ncep, "url_is_absent", lambda url: True)
    assert provider.fetch(CYCLE) == []
    assert not provider.staged_dir(CYCLE).exists()


def test_fetch_raises_when_a_download_fails_for_another_reason(provider, monkeypatch):
    monkeypatch.setattr(ncep, "download_one", lambda url, dest, **kw: None)
    monkeypatch.setattr(ncep, "url_is_absent", lambda url: False)
    with pytest.raises(PrepbufrUnavailable):
        provider.fetch(CYCLE)


def test_fetch_downloads_decodes_and_reuses_the_staged_file(provider, monkeypatch, tmp_path):
    downloads, decodes = [], []

    def download(url, dest, **kw):
        downloads.append(url)
        pathlib.Path(dest).write_bytes(b"BUFR")
        return pathlib.Path(dest)

    def convert(raw, output):
        decodes.append(raw.name)
        return aircraft_cycle(output)

    monkeypatch.setattr(ncep, "download_one", download)
    monkeypatch.setattr(provider, "convert", convert)
    first = provider.fetch(CYCLE)
    assert [pathlib.Path(p).name for p in first] == ["prepbufr.gdas.20261010.t06z.nr.48h.nc"]
    # The raw PREPBUFR is removed once decoded.
    assert sorted(p.name for p in provider.staged_dir(CYCLE).iterdir()) == [
        "prepbufr.gdas.20261010.t06z.nr.48h.nc"
    ]
    assert provider.fetch(CYCLE) == first
    assert len(downloads) == len(decodes) == 1


def test_prefetch_decodes_the_following_cycles(local_config, monkeypatch):
    provider = NCEPUpperAirProvider(config=local_config, prefetch_workers=3)
    staged = []
    monkeypatch.setattr(provider, "stage", lambda it: staged.append(it) or [])
    provider.fetch(pd.Timestamp("2020-01-01T00"))
    provider.close()
    assert sorted(staged) == list(pd.date_range("2020-01-01T00", periods=3, freq="6h"))


def test_write_clears_the_staged_conversion(provider, monkeypatch):
    target = provider.staged_dir(CYCLE)
    target.mkdir(parents=True)
    path = aircraft_cycle(target / "prepbufr.gdas.20261010.t06z.nr.48h.nc")
    assert provider.fetch(CYCLE) == [str(path)]
    assert provider.run_partition(CYCLE) is True
    assert not target.exists()


# --- pb2nc in Docker --------------------------------------------------------------------


@pytest.fixture
def stub_docker(tmp_path, monkeypatch):
    """Put a ``docker`` on PATH that maps the output mount back to the host and writes."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    script = bin_dir / "docker"
    script.write_text(
        "#!/bin/sh\n"
        'echo "$@" > "$(dirname "$0")/args"\n'
        "out=''\n"
        'for a in "$@"; do case "$a" in *:/pb2nc/out) out="${a%:/pb2nc/out}";; esac; done\n'
        "for last; do :; done\n"
        'eval "dest=\\${$(($#-1))}"\n'
        'printf data > "$out/$(basename "$dest")"\n'
    )
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("PATH", f"{bin_dir}:/usr/bin:/bin")
    return bin_dir


def test_run_pb2nc_docker_mounts_and_renames_atomically(tmp_path, stub_docker, monkeypatch):
    monkeypatch.setenv("MET_IMAGE", "example/met:1")
    raw = tmp_path / "in" / "prepbufr.gdas.2026101006.nr"
    raw.parent.mkdir()
    raw.write_bytes(b"BUFR")
    config = tmp_path / "pb2nc.cfg"
    config.write_text("cfg")
    out = run_pb2nc_docker(raw, tmp_path / "out" / "x.nc", config, memory_gb=3)
    assert out.read_text() == "data"
    assert not (tmp_path / "out" / "x.nc.part").exists()
    args = (stub_docker / "args").read_text()
    assert "-m 3g" in args
    assert "example/met:1 pb2nc /pb2nc/in/prepbufr.gdas.2026101006.nr" in args


def test_run_pb2nc_docker_needs_docker(tmp_path, monkeypatch):
    monkeypatch.setenv("PATH", str(tmp_path))
    with pytest.raises(MissingToolError, match="docker"):
        run_pb2nc_docker(tmp_path / "a", tmp_path / "b.nc", tmp_path / "c")
