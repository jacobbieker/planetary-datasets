"""Offline tests for the ICON v1 catalog and its frozen schemas. No network access."""

# ruff: noqa: D103 - test names say what they check, as elsewhere in tests/

from __future__ import annotations

import json
import posixpath

import numpy as np
import pandas as pd
import pytest

from planetary_datasets.providers import icon_v1_catalog as cat
from planetary_datasets.providers.icon_v1_catalog import FieldKey

RUN_00 = pd.Timestamp("2026-10-11T00:00")
RUN_06 = pd.Timestamp("2026-10-11T06:00")


# -- fakes -------------------------------------------------------------------------------


def make_lister(files: list[str], noise: bool = True):
    """Build a Lister over model-relative file paths, mimicking the mirrored tree.

    Every directory also contains DWD's ``index.html`` and, when ``noise`` is set, a macOS
    ``._`` resource fork, as the Source Cooperative mirror does.
    """
    tree: dict[str, set[str]] = {}
    for path in files:
        parts = path.split("/")
        for i in range(len(parts)):
            parent = "/".join(parts[:i])
            tree.setdefault(parent, set()).add(parts[i])
    calls: list[str] = []

    def ls(relative: str) -> list[str]:
        relative = relative.strip("/")
        calls.append(relative)
        if relative not in tree:
            raise FileNotFoundError(relative)
        names = sorted(tree[relative]) + ["index.html"]
        if noise:
            names += [f"._{n}" for n in sorted(tree[relative])[:1]]
        # Emulate s3fs, which returns full keys rather than bare names.
        return [posixpath.join("bucket/bkr/icon/icon", relative, n) for n in names]

    ls.calls = calls  # type: ignore[attr-defined]
    return ls


def grib_header(points: int, grid: int = 26, template: int = 101) -> bytes:
    """A GRIB2 message head: sections 0, 1 and an unstructured-grid section 3."""
    sec0 = b"GRIB" + b"\x00\x00" + bytes([0, 2]) + (1000).to_bytes(8, "big")
    sec1 = (21).to_bytes(4, "big") + bytes([1]) + bytes(16)
    sec3 = (
        (35).to_bytes(4, "big")
        + bytes([3, 0])
        + points.to_bytes(4, "big")
        + bytes([0, 0])
        + template.to_bytes(2, "big")
        + bytes([6])
        + grid.to_bytes(3, "big")
        + bytes([1])
        + bytes(16)
    )
    return sec0 + sec1 + sec3 + b"7777"


def run_files(prefix: str, run: pd.Timestamp, steps) -> list[str]:
    return [f"{prefix}/r/{run:%Y-%m-%dT%H:%M}/s/PT{s:03d}H00M.grib2" for s in steps]


#: Two days of runs; the tests pick their reference runs from the second.
TREE_RUNS = list(pd.date_range("2026-10-10T00:00", "2026-10-11T18:00", freq="6h"))
RUNS_4 = [pd.Timestamp(t) for t in pd.date_range("2026-10-11T00:00", periods=4, freq="6h")]


@pytest.fixture
def fake_tree() -> list[str]:
    """A miniature ``icon`` tree: one of each field kind, plus the things to exclude.

    00/12Z runs go to step 3, 06/18Z runs to step 2. ``RSTOM`` is published at 00Z only,
    as in ICON-ART.
    """
    files: list[str] = []
    for run in TREE_RUNS:
        steps = range(0, 4) if run.hour in (0, 12) else range(0, 3)
        if run.hour == 0:
            files += run_files("p/RSTOM", run, steps)
        files += run_files("p/T_2M", run, steps)
        files += run_files("p/TMAX_2M", run, [s for s in steps if s > 0])
        for level in ("50000", "85000"):
            files += run_files(f"p/T/lvt1/100/lv1/{level}", run, steps)
        for level in ("1", "2", "10"):
            files += run_files(f"p/T/lvt1/150/lv1/{level}", run, steps)
        for level in ("0.0", "0.01"):
            files += run_files(f"p/W_SO/lvt1/106/lv1/{level}", run, steps)
        for param in ("HSURF", "CLAT", "CLON", "ELAT", "OFFGRID"):
            files += run_files(f"p/{param}", run, [0])
        files += run_files("p/HHL/lvt1/150/lv1/1", run, [0])
        for nm in ("532", "1064"):
            for level in ("119", "120"):
                files += run_files(f"p/CEIL_BSC_DUST/wvl1/{nm}/lvt1/150/lv1/{level}", run, steps)
        # A layout FieldKey cannot address.
        files += run_files("p/ODD/ens1/1", run, steps)
    # A level directory with no files in either reference run.
    files += run_files("p/T/lvt1/150/lv1/3", pd.Timestamp("2026-09-01T00:00"), [0])
    return files


def header_reader(path: str) -> bytes:
    if path.startswith("p/OFFGRID/"):
        return grib_header(4_423_680)
    return grib_header(cat.MODELS["icon"].n_cells)


# -- addressing --------------------------------------------------------------------------


def test_field_path_single_level():
    key = FieldKey("T_2M", None, None, 3)
    assert cat.field_path("icon", key, RUN_00) == "p/T_2M/r/2026-10-11T00:00/s/PT003H00M.grib2"


def test_field_path_pressure_and_soil_levels():
    pressure = FieldKey("T", 100, "85000", 120)
    soil = FieldKey("W_SO", 106, "0.01", 0)
    assert (
        cat.field_path("icon", pressure, RUN_06)
        == "p/T/lvt1/100/lv1/85000/r/2026-10-11T06:00/s/PT120H00M.grib2"
    )
    assert (
        cat.field_path("icon-art", soil, RUN_00)
        == "p/W_SO/lvt1/106/lv1/0.01/r/2026-10-11T00:00/s/PT000H00M.grib2"
    )


def test_field_uri_archive_keeps_colon_and_live_encodes_it():
    key = FieldKey("T", 100, "85000", 3)
    assert cat.field_uri("icon-art-eu", key, RUN_00, "archive") == (
        "s3://us-west-2.opendata.source.coop/bkr/icon/icon-art-eu/"
        "p/T/lvt1/100/lv1/85000/r/2026-10-11T00:00/s/PT003H00M.grib2"
    )
    assert cat.field_uri("icon-art-eu", key, RUN_00, "live") == (
        "https://opendata.dwd.de/weather/nwp/v1/m/icon-art-eu/"
        "p/T/lvt1/100/lv1/85000/r/2026-10-11T00%3A00/s/PT003H00M.grib2"
    )


def test_field_path_accepts_tz_aware_and_rejects_off_cycle_times():
    key = FieldKey("T_2M", None, None, 0)
    aware = pd.Timestamp("2026-10-11T02:00", tz="Europe/Berlin")  # 00Z
    assert "/r/2026-10-11T00:00/" in cat.field_path("icon", key, aware)
    with pytest.raises(ValueError, match="not an ICON cycle"):
        cat.field_path("icon", key, pd.Timestamp("2026-10-11T03:00"))
    with pytest.raises(ValueError, match="unknown ICON model"):
        cat.field_path("icon-d2", key, RUN_00)


def test_field_key_rejects_half_levels():
    with pytest.raises(ValueError):
        FieldKey("T", 100, None, 0)
    with pytest.raises(ValueError):
        FieldKey("T", None, "85000", 0)


def test_storage_options():
    assert cat.storage_options("archive") == {"anon": True}
    assert cat.storage_options("live") == {}
    with pytest.raises(ValueError):
        cat.storage_options("ftp")  # type: ignore[arg-type]


def test_models_match_the_published_grids():
    assert set(cat.MODELS) == {"icon", "icon-art", "icon-art-eu"}
    assert cat.MODELS["icon"].n_cells == 2_949_120
    assert cat.MODELS["icon-art"].grid_number == 39
    assert cat.MODELS["icon-art-eu"].archive_start == pd.Timestamp("2025-09-30T18:00")


# -- listings ----------------------------------------------------------------------------


def test_clean_names_drops_index_pages_and_resource_forks():
    raw = [
        "bucket/bkr/icon/icon/p/T_2M",
        "bucket/bkr/icon/icon/p/index.html",
        "bucket/bkr/icon/icon/p/._T_2M",
        ".DS_Store",
        "2026-10-12T18%3A00/",
        "PT000H00M.grib2",
        "T_2M",
    ]
    assert cat.clean_names(raw) == ["2026-10-12T18:00", "PT000H00M.grib2", "T_2M"]


def test_parse_index_html_reads_dwd_autoindex():
    html = """<html><head><title>Index of /weather/nwp/v1/m/icon/p/T_2M/r/</title></head>
<body><h1>Index of /weather/nwp/v1/m/icon/p/T_2M/r/</h1><hr><pre><a href="../">../</a>
<a href="2026-10-12T18%3A00/">2026-10-12T18:00/</a>   12-Oct-2026 20:39:26   -
<a href="2026-10-13T00%3A00/">2026-10-13T00:00/</a>   13-Oct-2026 02:36:26   -
<a href="https://www.dwd.de/">elsewhere</a>
</pre><hr></body></html>"""
    names = cat.parse_index_html(html)
    assert names == ["2026-10-12T18%3A00/", "2026-10-13T00%3A00/"]
    assert cat.clean_names(names) == ["2026-10-12T18:00", "2026-10-13T00:00"]


def test_list_runs_sorts_dedupes_and_ignores_noise():
    def lister(relative: str) -> list[str]:
        assert relative == "p/T_2M/r"
        return [
            "2026-10-13T06:00",
            "2026-10-12T18%3A00/",
            "2026-10-12T18:00",
            "index.html",
            "._2026-10-13T06:00",
            "not-a-run",
            "2026-10-13T07:00",  # not a cycle
        ]

    runs = cat.list_runs("icon", "live", lister=lister)
    assert runs == [pd.Timestamp("2026-10-12T18:00"), pd.Timestamp("2026-10-13T06:00")]


def test_list_runs_of_a_missing_param_is_empty():
    assert cat.list_runs("icon", "archive", probe_param="NOPE", lister=make_lister([])) == []


# -- choose_source -----------------------------------------------------------------------

NOW = pd.Timestamp("2026-10-13T17:00")
LISTINGS = {
    "live": [pd.Timestamp(t) for t in ("2026-10-12T18:00", "2026-10-13T00:00", "2026-10-13T12:00")],
    "archive": [pd.Timestamp(t) for t in ("2026-10-11T00:00", "2026-10-12T18:00")],
}


@pytest.mark.parametrize(
    ("init_time", "expected"),
    [
        ("2026-10-13T12:00", "live"),
        ("2026-10-12T18:00", "live"),  # in both: live wins
        ("2026-10-11T00:00", "archive"),
        ("2026-10-13T06:00", None),  # in neither
        ("2026-10-14T00:00", None),  # in the future
    ],
)
def test_choose_source(init_time, expected):
    assert (
        cat.choose_source("icon", pd.Timestamp(init_time), now=NOW, listings=LISTINGS) == expected
    )


def test_choose_source_prefers_the_archive_once_live_files_start_expiring():
    run = pd.Timestamp("2026-10-12T12:00")  # 29 h old: listed live, early files gone
    both = {"live": [run], "archive": [run]}
    assert cat.choose_source("icon", run, now=NOW, listings=both) == "archive"
    assert cat.choose_source("icon", run, now=NOW, listings={"live": [run]}) == "live"


def test_choose_source_does_not_ask_live_about_expired_runs(monkeypatch):
    asked: list[str] = []

    def fake_cached_runs(model, source, *, probe_param="T_2M"):
        asked.append(source)
        return (pd.Timestamp("2026-10-01T00:00"),)

    monkeypatch.setattr(cat, "cached_runs", fake_cached_runs)
    assert cat.choose_source("icon", pd.Timestamp("2026-10-01T00:00"), now=NOW) == "archive"
    assert asked == ["archive"]


def test_choose_source_falls_back_to_archive_when_live_listing_fails(monkeypatch):
    def fake_cached_runs(model, source, *, probe_param="T_2M"):
        if source == "live":
            raise cat.ListingError("down")
        return (pd.Timestamp("2026-10-13T00:00"),)

    monkeypatch.setattr(cat, "cached_runs", fake_cached_runs)
    assert cat.choose_source("icon", pd.Timestamp("2026-10-13T00:00"), now=NOW) == "archive"
    with pytest.raises(cat.ListingError):
        cat.choose_source("icon", pd.Timestamp("2026-10-13T06:00"), now=NOW)


def test_cached_runs_reuses_listings_until_the_ttl(monkeypatch):
    cat.clear_listing_cache()
    calls: list[tuple[str, str]] = []
    clock = [1000.0]

    def fake_list_runs(model, source, *, probe_param="T_2M", lister=None):
        calls.append((model, source))
        return [RUN_00]

    monkeypatch.setattr(cat, "list_runs", fake_list_runs)
    monkeypatch.setattr(cat, "_clock", lambda: clock[0])
    try:
        for _ in range(5):
            assert cat.cached_runs("icon", "archive") == (RUN_00,)
        assert calls == [("icon", "archive")]
        clock[0] += cat.LISTING_TTL["archive"].total_seconds() + 1
        cat.cached_runs("icon", "archive")
        assert len(calls) == 2
        cat.clear_listing_cache()
        cat.cached_runs("icon", "archive")
        assert len(calls) == 3
    finally:
        cat.clear_listing_cache()


# -- GRIB header -------------------------------------------------------------------------


def test_parse_grib2_grid():
    assert cat.parse_grib2_grid(grib_header(1_013_404, grid=61)) == (1_013_404, 61)
    assert cat.parse_grib2_grid(grib_header(100, template=0)) == (100, None)
    with pytest.raises(ValueError):
        cat.parse_grib2_grid(b"GRIB" + bytes(12))
    with pytest.raises(ValueError):
        cat.parse_grib2_grid(b"not grib at all")
    with pytest.raises(ValueError):
        cat.parse_grib2_grid(grib_header(100)[:40])


# -- discover_schema ---------------------------------------------------------------------


def discover(files, runs=(RUN_00, RUN_06), **kwargs):
    kwargs.setdefault("header_reader", header_reader)
    return cat.discover_schema("icon", list(runs), "archive", lister=make_lister(files), **kwargs)


def test_discover_schema_builds_variables_per_level_type(fake_tree):
    data = discover(fake_tree)
    json.dumps(data)  # serialisable
    keys = [(v["param"], v["level_type"], v.get("wavelength")) for v in data["variables"]]
    assert keys == [
        ("CEIL_BSC_DUST", 150, 532),
        ("CEIL_BSC_DUST", 150, 1064),
        ("HHL", 150, None),
        ("HSURF", None, None),
        ("RSTOM", None, None),
        ("T", 100, None),
        ("T", 150, None),
        ("TMAX_2M", None, None),
        ("T_2M", None, None),
        ("W_SO", 106, None),
    ]
    # Only wavelength-resolved entries carry the key, so other entries are unchanged.
    assert ["wavelength" in v for v in data["variables"]].count(True) == 2
    by_key = {(v["param"], v["level_type"]): v for v in data["variables"]}
    # Levels sort numerically and keep their path spelling; the empty level 3 is dropped.
    assert by_key[("T", 150)]["levels"] == ["1", "2", "10"]
    assert by_key[("W_SO", 106)]["levels"] == ["0.0", "0.01"]
    assert by_key[("T_2M", None)]["levels"] == []
    assert data["reference_runs"] == ["2026-10-11T00:00", "2026-10-11T06:00"]
    assert data["derived_cycles"] == {"12": "00", "18": "06"}
    assert data["invariant_params"] == ["HHL", "HSURF"]
    assert data["coordinate_params"] == ["CLAT", "CLON"]


def test_field_path_wavelength_levels():
    key = FieldKey("CEIL_BSC_DUST", 150, "77", 6, wavelength=1064)
    assert cat.field_path("icon-art", key, RUN_00) == (
        "p/CEIL_BSC_DUST/wvl1/1064/lvt1/150/lv1/77/r/2026-10-11T00:00/s/PT006H00M.grib2"
    )
    # Positional construction without a wavelength still works and means "none".
    assert FieldKey("T", 150, "77", 6).wavelength is None
    with pytest.raises(ValueError, match="wavelength"):
        FieldKey("CEIL_BSC_DUST", 150, "77", 6, wavelength=0)


def test_discover_schema_descends_into_wavelengths(fake_tree):
    schema = cat.ICONSchema(discover(fake_tree, runs=RUNS_4))
    ceil = [v for v in schema.variables if v.param == "CEIL_BSC_DUST"]
    assert [(v.level_type, v.wavelength, v.levels) for v in ceil] == [
        (150, 532, ("119", "120")),
        (150, 1064, ("119", "120")),
    ]
    assert schema.variable("CEIL_BSC_DUST", 150, 1064).steps == (0, 1, 2, 3)
    assert schema.steps_for_cycle("CEIL_BSC_DUST", 150, 6, 532) == (0, 1, 2)
    with pytest.raises(KeyError):
        schema.variable("CEIL_BSC_DUST", 150)
    assert FieldKey("CEIL_BSC_DUST", 150, "119", 3, 1064) in schema.expected_fields(RUN_00)


def test_discover_schema_excludes_coordinates_off_grid_and_unknown_layouts(fake_tree):
    data = discover(fake_tree)
    params = {v["param"] for v in data["variables"]}
    assert not params & {"CLAT", "CLON", "ELAT", "OFFGRID", "ODD"}
    excluded = {e["param"]: e["reason"] for e in data["excluded"]}
    assert set(excluded) == {"ELAT", "OFFGRID", "ODD"}
    assert "ens1" in excluded["ODD"]
    assert "4423680 points" in excluded["OFFGRID"]


def test_discover_schema_strict_refuses_unreadable_grid_headers(fake_tree):
    def flaky_reader(path: str) -> bytes:
        if path.startswith("p/OFFGRID/"):
            raise OSError("connection reset")
        return header_reader(path)

    data = discover(fake_tree, header_reader=flaky_reader)  # lenient keeps it
    assert "OFFGRID" in {v["param"] for v in data["variables"]}
    with pytest.raises(cat.IncompleteReferenceRun, match="could not read the grid"):
        discover(fake_tree, header_reader=flaky_reader, strict=True)


def test_discover_schema_without_grid_check_keeps_unknown_grids(fake_tree):
    data = discover(fake_tree, check_grid=False, header_reader=None)
    assert "OFFGRID" in {v["param"] for v in data["variables"]}


def test_discover_schema_only_descends_into_reference_runs(fake_tree):
    lister = make_lister(fake_tree)
    cat.discover_schema("icon", [RUN_00, RUN_06], "archive", lister=lister, check_grid=False)
    below_r = [c for c in lister.calls if "/r/" in c]
    assert below_r
    for call in below_r:
        assert call.endswith("/s")
        assert "/r/2026-10-11T00:00/s" in call or "/r/2026-10-11T06:00/s" in call


def test_discover_schema_needs_both_cycle_classes(fake_tree):
    with pytest.raises(ValueError, match="cover neither"):
        discover(fake_tree, runs=[RUN_00])


def test_discover_schema_records_hour_specific_params(fake_tree):
    data = discover(fake_tree, runs=RUNS_4, strict=True)
    assert data["derived_cycles"] == {}
    schema = cat.ICONSchema(data)
    assert schema.steps_for_cycle("RSTOM", None, 0) == (0, 1, 2, 3)
    for hour in (6, 12, 18):
        assert schema.steps_for_cycle("RSTOM", None, hour) == ()
    assert schema.steps_for_cycle("T_2M", None, 12) == (0, 1, 2, 3)
    assert schema.steps_for_cycle("T_2M", None, 18) == (0, 1, 2)


def test_discover_schema_derived_hours_skip_params_never_published_then(fake_tree):
    schema = cat.ICONSchema(discover(fake_tree, strict=True))
    # 12Z copies 00Z, except RSTOM: the source holds 12Z runs, but none of RSTOM.
    assert schema.steps_for_cycle("T_2M", None, 12) == (0, 1, 2, 3)
    assert schema.steps_for_cycle("RSTOM", None, 0) == (0, 1, 2, 3)
    assert schema.steps_for_cycle("RSTOM", None, 12) == ()


def test_discover_schema_strict_rejects_missing_levels(fake_tree):
    partial = [f for f in fake_tree if not f.startswith("p/T/lvt1/150/lv1/10/r/2026-10-11T06")]
    data = discover(partial)  # lenient: levels are still the union over runs
    by_key = {(v["param"], v["level_type"]): v for v in data["variables"]}
    assert by_key[("T", 150)]["levels"] == ["1", "2", "10"]
    with pytest.raises(cat.IncompleteReferenceRun, match="lacks levels"):
        discover(partial, strict=True)


def test_discover_schema_strict_rejects_a_variable_missing_from_a_run(fake_tree):
    # The mirror lacks W_SO for the 06Z reference run, though it has other 06Z runs of it.
    partial = [f for f in fake_tree if not (f.startswith("p/W_SO/") and "2026-10-11T06" in f)]
    with pytest.raises(cat.IncompleteReferenceRun, match="W_SO@106: no files in 2026-10-11T06"):
        discover(partial, strict=True)
    # Lenient discovery records what it saw.
    schema = cat.ICONSchema(discover(partial))
    assert schema.steps_for_cycle("W_SO", 106, 6) == ()


def test_discover_schema_strict_rejects_same_hour_runs_that_disagree(fake_tree):
    runs = [RUN_00, RUN_06, pd.Timestamp("2026-10-10T00:00")]
    partial = [f for f in fake_tree if f != "p/T_2M/r/2026-10-10T00:00/s/PT003H00M.grib2"]
    discover(fake_tree, runs=runs, strict=True)
    with pytest.raises(cat.IncompleteReferenceRun, match="disagree"):
        discover(partial, runs=runs, strict=True)


def test_discovered_schema_loads_and_is_cycle_aware(fake_tree):
    schema = cat.ICONSchema(discover(fake_tree, runs=RUNS_4))
    assert schema.step_axis == (0, 1, 2, 3)
    assert schema.steps_for_cycle("T_2M", None, 0) == (0, 1, 2, 3)
    assert schema.steps_for_cycle("T_2M", None, 18) == (0, 1, 2)
    assert schema.steps_for_cycle("TMAX_2M", None, 12) == (1, 2, 3)
    assert schema.variable("T", 150).steps == (0, 1, 2, 3)
    assert schema.invariant_params == ("HHL", "HSURF")
    assert schema.variable("HSURF", None).steps == (0,)
    with pytest.raises(ValueError):
        schema.steps_for_cycle("T_2M", None, 3)
    with pytest.raises(KeyError):
        schema.steps_for_cycle("T_2M", 100, 0)

    fields = {run: schema.expected_fields(run) for run in RUNS_4}
    f00, f06 = fields[RUN_00], fields[RUN_06]
    assert FieldKey("T_2M", None, None, 3) in f00
    assert FieldKey("T_2M", None, None, 3) not in f06
    assert FieldKey("RSTOM", None, None, 0) in f00
    assert FieldKey("RSTOM", None, None, 0) not in fields[RUNS_4[2]]
    assert FieldKey("T", 150, "10", 2) in f06
    assert FieldKey("HSURF", None, None, 0) in f06
    # One step more per level of each forecast variable, plus RSTOM's four files.
    # CEIL_BSC_DUST adds one step at each of its two levels and two wavelengths.
    assert len(f00) - len(f06) == (2 + 3 + 2 + 1 + 1 + 4) + 4
    assert len(set(f00)) == len(f00)
    # Every expected field of a run exists in the tree, and nothing on the grid is missed.
    off_schema = ("p/CLAT", "p/CLON", "p/ELAT", "p/OFFGRID", "p/ODD")
    tree = set(fake_tree)
    for run, keys in fields.items():
        paths = {cat.field_path("icon", k, run) for k in keys}
        stem = f"/r/{run:%Y-%m-%dT%H:%M}/"
        assert paths == {f for f in tree if stem in f and not f.startswith(off_schema)}
    assert schema.coordinate_fields() == [
        FieldKey("CLAT", None, None, 0),
        FieldKey("CLON", None, None, 0),
    ]


def test_dump_schema_round_trips(fake_tree):
    data = discover(fake_tree)
    text = cat.dump_schema(data)
    assert json.loads(text) == data
    assert text.count("\n") > len(data["variables"])  # one variable per line


def test_icon_schema_rejects_bad_step_set_references(fake_tree):
    data = discover(fake_tree)
    data["variables"][0]["steps"]["00"] = "missing"
    with pytest.raises(ValueError, match="unknown step set"):
        cat.ICONSchema(data)


def test_default_reference_runs_picks_the_newest_old_enough_run_per_hour():
    runs = [pd.Timestamp(t) for t in pd.date_range("2026-10-10", "2026-10-13T12:00", freq="6h")]
    chosen = cat.default_reference_runs(runs, NOW, pd.Timedelta(36, "h"))
    assert chosen == [
        pd.Timestamp("2026-10-12T00:00"),
        pd.Timestamp("2026-10-11T06:00"),
        pd.Timestamp("2026-10-11T12:00"),
        pd.Timestamp("2026-10-11T18:00"),
    ]
    with pytest.raises(ValueError):
        cat.default_reference_runs(runs[:1], NOW, pd.Timedelta(36, "h"))


# -- committed schemas -------------------------------------------------------------------


#: Per model: variables, length of T_2M's 00Z and 06Z schedules, and the step axis. The
#: axis is the union over all variables, longer than T_2M's 113/75 steps because VMAX_10M
#: stays hourly to the end (global) and the ART-EU dust fields are hourly to 72 h.
COMMITTED = {
    "icon": (103, 113, 93, 181),
    "icon-art": (137, 113, 93, 181),
    "icon-art-eu": (130, 75, 75, 89),
}


@pytest.mark.parametrize("model", sorted(COMMITTED))
def test_committed_schemas_load(model):
    n_variables, n_t2m_00, n_t2m_06, n_steps = COMMITTED[model]
    schema = cat.load_schema(model)
    spec = cat.MODELS[model]
    assert schema.model == model
    assert schema.n_cells == spec.n_cells
    assert schema.grid_number == spec.grid_number
    assert len(schema.variables) == n_variables
    assert len(schema.step_axis) == n_steps
    assert schema.step_axis == tuple(sorted(set(schema.step_axis)))
    assert schema.step_axis[0] == 0
    assert schema.coordinate_params == ("CLAT", "CLON")
    assert len(schema.reference_runs) == 4
    assert {r.hour for r in schema.reference_runs} == set(cat.CYCLE_HOURS)
    params = {v.param for v in schema.variables}
    assert not params & {"CLAT", "CLON", "ELAT", "ELON"}
    assert {"T_2M", "HSURF"} <= params
    assert "HSURF" in schema.invariant_params
    assert set(schema.invariant_params) <= params
    assert schema.variable("HSURF", None).steps == (0,)
    t2m = [schema.steps_for_cycle("T_2M", None, h) for h in cat.CYCLE_HOURS]
    assert [len(s) for s in t2m] == [n_t2m_00, n_t2m_06, n_t2m_00, n_t2m_06]
    keys = [(v.param, v.level_type, v.wavelength) for v in schema.variables]
    assert len(keys) == len(set(keys))
    for v in schema.variables:
        assert set(v.steps) <= set(schema.step_axis)
        assert list(v.levels) == sorted(v.levels, key=float)


def test_committed_icon_schema_is_cycle_aware():
    schema = cat.load_schema("icon")
    t2m_00 = schema.steps_for_cycle("T_2M", None, 0)
    assert t2m_00[:79] == tuple(range(79))
    assert t2m_00[79:] == tuple(range(81, 181, 3))
    assert schema.steps_for_cycle("T_2M", None, 6) == t2m_00[:93]
    assert schema.step_axis == tuple(range(181))
    f00 = schema.expected_fields(pd.Timestamp("2026-10-11T00:00"))
    f06 = schema.expected_fields(pd.Timestamp("2026-10-11T06:00"))
    assert len(f00) > len(f06)
    assert max(k.step for k in f06) == 120
    assert FieldKey("T_2M", None, None, 180) in f00
    assert FieldKey("T_2M", None, None, 180) not in f06
    assert FieldKey("T", 100, "85000", 0) in f00
    assert FieldKey("W_SO", 106, "0.01", 0) in f00
    assert len(schema.variable("T", 100).levels) == 18
    assert len(schema.variable("T", 150).levels) == 120
    assert len(schema.variable("HHL", 150).levels) == 121
    assert {v.level_type for v in schema.variables if v.param == "T"} == {100, 150}


@pytest.mark.parametrize("model", ["icon-art", "icon-art-eu"])
def test_committed_art_schemas_know_the_00z_only_fields(model):
    schema = cat.load_schema(model)
    for param in ("RSTOM", "ALHFL_BS"):
        assert schema.steps_for_cycle(param, None, 0)
        for hour in (6, 12, 18):
            assert schema.steps_for_cycle(param, None, hour) == ()
    f00 = schema.expected_fields(pd.Timestamp("2026-10-11T00:00"))
    f12 = schema.expected_fields(pd.Timestamp("2026-10-11T12:00"))
    assert any(k.param == "RSTOM" for k in f00)
    assert not any(k.param == "RSTOM" for k in f12)
    excluded = {e["param"] for e in schema.excluded}
    assert {"ELAT", "ELON"} <= excluded
    assert not {"CEIL_BSC_DUST", "SAT_BSC_DUST"} & excluded


#: The wavelength-resolved backscatter fields: their levels, a subset of the model levels.
WAVELENGTH_LEVELS = {"icon-art": (77, 120), "icon-art-eu": (31, 74)}


@pytest.mark.parametrize("model", sorted(WAVELENGTH_LEVELS))
def test_committed_art_schemas_carry_the_wavelength_fields(model):
    schema = cat.load_schema(model)
    first, last = WAVELENGTH_LEVELS[model]
    for param in ("CEIL_BSC_DUST", "SAT_BSC_DUST"):
        specs = [v for v in schema.variables if v.param == param]
        assert sorted(v.wavelength for v in specs) == [532, 1064]
        for v in specs:
            assert v.level_type == 150
            assert v.levels == tuple(str(lv) for lv in range(first, last + 1))
            assert v.steps == specs[0].steps
    assert all(v.wavelength is None for v in schema.variables if "BSC" not in v.param)
    key = FieldKey("SAT_BSC_DUST", 150, str(last), 0, 532)
    assert key in schema.expected_fields(pd.Timestamp("2026-10-11T00:00"))


# -- storage precision -------------------------------------------------------------------


def stats_of(*fields) -> cat.PrecisionStats:
    stats = cat.PrecisionStats()
    for values in fields:
        stats.add(np.asarray(values, dtype=np.float32))
    return stats


def test_precision_stats_measure_range_underflow_and_fp16_error():
    stats = stats_of([0.0, 1.0, 250.123, -3.0], [1e-6, 2.0, np.nan])
    assert stats.samples == 2
    assert stats.max_abs == pytest.approx(250.123, rel=1e-6)
    assert stats.nonzero == 5 and stats.below_normal == 1
    assert stats.subnormal_fraction == pytest.approx(0.2)
    assert 0 < stats.fp16_max_abs_error <= 0.125
    # Merging is the same as adding everything to one.
    merged = stats_of([0.0, 1.0, 250.123, -3.0]).merge(stats_of([1e-6, 2.0, np.nan]))
    assert merged == stats


@pytest.mark.parametrize(
    "param, fields, expected",
    [
        ("T_2M", [[230.0, 310.5]], ("float16", "fits")),
        ("U_10M", [[-30.0, 0.0, 0.0, 12.5]], ("float16", "fits")),
        # Above 65504 / 4, even under a name not on the list.
        ("VIS", [[0.0, 79770.0]], ("float32", "range")),
        # No evidence of the range at all.
        ("FR_ICE", [[0.0, 0.0, np.nan]], ("float32", "no non-zero values sampled")),
        # More than 0.1% of the non-zero values below float16's smallest normal.
        ("W", [[0.5] * 998 + [1e-6, 2e-6]], ("float32", "underflow")),
        # One in a thousand is allowed.
        ("W", [[0.5] * 999 + [1e-6]], ("float16", "fits")),
        # The list beats statistics that would pass.
        ("QV", [[0.5, 1.0]], ("float32", "full-precision list")),
        ("tqv", [[0.5, 1.0]], ("float32", "full-precision list")),
        ("FI", [[10.0]], ("float32", "full-precision list")),
        ("PS", [[10.0]], ("float32", "full-precision list")),
        ("ACCDRYDEPO_DUSTA", [[1.0]], ("float32", "full-precision list")),
        ("CEIL_BSC_DUST", [[1.0]], ("float32", "full-precision list")),
        ("CLAT", [[1.0]], ("float32", "full-precision list")),
        # Accumulated or averaged since the start of the run: differenced downstream.
        ("TOT_PREC", [[0.0, 3.5]], ("float32", "full-precision list")),
        ("ASOB_S", [[0.0, 300.0]], ("float32", "full-precision list")),
        ("RUNOFF_G", [[0.0, 300.0]], ("float32", "full-precision list")),
    ],
)
def test_classify_precision(param, fields, expected):
    assert cat.classify_precision(param, stats_of(*fields)) == expected


def test_unsampled_variables_stay_float32():
    assert cat.classify_precision("T_2M", None) == ("float32", "not sampled")
    assert cat.classify_precision("T_2M", cat.PrecisionStats()) == ("float32", "not sampled")


def test_full_precision_list_covers_the_committed_families():
    params = {v.param for m in cat.MODELS for v in cat.load_schema(m).variables}
    full = {p for p in params if cat.is_full_precision(p)}
    assert {"QV", "QC", "QI", "QV_S", "TQV", "TQC_DIA", "FI", "HHL", "HSURF", "P", "PS"} <= full
    assert {p for p in params if "DUST" in p} <= full
    assert {"TOT_PREC", "RAIN_GSP", "ASOB_S", "ATHB_T", "ALHFL_BS", "AVMFL_S"} <= full
    assert not {"T_2M", "U_10M", "CLCT", "T", "U", "V", "ALB_RAD", "SOILTYP"} & full


def test_a_missing_dtype_means_float32(fake_tree):
    data = discover(fake_tree)
    assert all("dtype" not in v for v in data["variables"])
    schema = cat.ICONSchema(data)
    assert {v.dtype for v in schema.variables} == {"float32"}
    data["variables"][0]["dtype"] = "float64"
    with pytest.raises(ValueError, match="dtype"):
        cat.ICONSchema(data)


def test_measure_and_apply_precision_sample_each_variable(fake_tree):
    data = discover(fake_tree, runs=RUNS_4)
    schema = cat.ICONSchema(data)
    read: list[str] = []

    def reader(uri: str):
        read.append(uri)
        if "/p/HSURF/" in uri:
            return None  # missing at the source
        if "/p/W_SO/" in uri:
            return np.array([1e-7, 1e-7, 0.3], np.float32)
        return np.array([280.0, 0.0, 1.5], np.float32)

    runs = [RUN_00, pd.Timestamp("2026-10-11T12:00")]
    results = cat.measure_precision("icon", runs, "archive", schema=schema, reader=reader)
    # T on model levels 1, 2, 10: top, middle and bottom level, by first, middle and last
    # step, in each of the two runs.
    assert results[("T", 150, None)].samples == 3 * 3 * 2
    assert results[("HSURF", None, None)].samples == 0
    assert results[("HSURF", None, None)].missing == 2
    assert not any("/r/2026-10-11T06:00/" in u for u in read)

    updated = cat.apply_precision(data, results)
    entries = {(v["param"], v["level_type"], v.get("wavelength")): v for v in updated["variables"]}
    assert entries[("T_2M", None, None)]["dtype"] == "float16"
    assert entries[("W_SO", 106, None)]["precision"]["reason"] == "underflow"
    assert entries[("HSURF", None, None)]["dtype"] == "float32"
    assert entries[("T_2M", None, None)]["precision"]["samples"] == 6
    reloaded = cat.ICONSchema(json.loads(cat.dump_schema(updated)))
    assert reloaded.variable("T_2M", None).dtype == "float16"
    assert "dtype" not in data["variables"][0]  # the input is not modified

    # Rediscovering keeps the recorded precision of the variables it still has.
    carried = cat.carry_precision(discover(fake_tree, runs=RUNS_4), updated)
    assert carried == updated | {"generated_at": carried["generated_at"]}


@pytest.mark.parametrize("model", sorted(cat.MODELS))
def test_committed_schemas_carry_a_dtype_for_every_variable(model):
    schema = cat.load_schema(model)
    assert {v.dtype for v in schema.variables} == {"float16", "float32"}
    for v in schema.variables:
        if cat.is_full_precision(v.param):
            assert v.dtype == "float32", v
    assert schema.variable("T_2M", None).dtype == "float16"
    assert schema.variable("QV", 150).dtype == "float32"


def test_one_parameter_gets_one_dtype_across_wavelengths_and_models():
    fits = stats_of([0.5, 1.0])
    tiny = stats_of([1e-7, 1e-7, 1.0])
    # Pooled over the wavelengths of one model...
    pooled = cat.pool_precision({("BSC", 150, 532): fits, ("BSC", 150, 1064): tiny})
    assert cat.classify_precision("BSC", pooled[("BSC", 150)])[0] == "float32"
    # ... and over models: float16 in one sample does not win over float32 in another.
    pooled = cat.pool_precision({("W", 150, None): fits}, {("W", 150, None): tiny})
    data = {"variables": [{"param": "W", "level_type": 150, "levels": ["1"]}]}
    entry = cat.apply_precision(data, {("W", 150, None): fits}, pooled)["variables"][0]
    assert entry["dtype"] == "float32" and entry["precision"]["reason"] == "underflow"
    # The entry keeps its own model's statistics.
    assert entry["precision"]["subnormal_fraction"] == 0.0


def test_precision_runs_are_the_newest_settled_00z_and_12z():
    runs = [pd.Timestamp(t) for t in pd.date_range("2026-10-12T18:00", periods=5, freq="6h")]
    now = pd.Timestamp("2026-10-13T19:00")
    assert cat.precision_runs(runs, now, pd.Timedelta(6, "h")) == [
        pd.Timestamp("2026-10-13T00:00"),
        pd.Timestamp("2026-10-13T12:00"),
    ]
    with pytest.raises(ValueError, match="12Z"):
        cat.precision_runs(runs, now, pd.Timedelta(8, "h"))


# -- grid eras ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "model, old_grid, old_cells, new_grid, new_cells",
    [
        ("icon-art", 36, 737_280, 39, 1_310_720),
        ("icon-art-eu", 51, 571_088, 61, 1_013_404),
    ],
)
def test_art_models_changed_grid_with_the_2026_10_06_run(
    model, old_grid, old_cells, new_grid, new_cells
):
    last_old, first_new = pd.Timestamp("2026-10-05T18:00"), pd.Timestamp("2026-10-06T00:00")
    assert cat.era_for(model, last_old).grid_number == old_grid
    assert cat.era_for(model, first_new).grid_number == new_grid
    assert cat.era_for(model, pd.Timestamp("2025-09-30T18:00")).grid_number == old_grid
    assert cat.current_era(model).grid_number == new_grid == cat.MODELS[model].grid_number
    old = cat.load_schema(model, last_old)
    new = cat.load_schema(model, first_new)
    assert (old.n_cells, old.grid_number) == (old_cells, old_grid)
    assert (new.n_cells, new.grid_number) == (new_cells, new_grid)
    assert cat.load_schema(model) is new
    assert cat.era_by_grid(model, old_grid).schema == f"{model}_grid{old_grid}"
    with pytest.raises(ValueError, match="no era on grid"):
        cat.era_by_grid(model, 26)
    # The old grid's schema has the same fields; only the grid differs.
    assert [(v.param, v.level_type, v.wavelength, v.levels, v.dtype) for v in old.variables] == [
        (v.param, v.level_type, v.wavelength, v.levels, v.dtype) for v in new.variables
    ]
    assert len(old.reference_runs) >= 4


def test_icon_global_has_one_era():
    assert cat.era_for("icon", pd.Timestamp("2026-08-25T12:00")) == cat.current_era("icon")
    assert cat.load_schema("icon", pd.Timestamp("2026-08-25T12:00")).n_cells == 2_949_120


def test_a_schema_must_match_its_eras_grid():
    wrong = cat.ICONEra(None, 51, 571_088, "icon-art-eu")  # the current era's schema
    with pytest.raises(ValueError, match="grid 61"):
        cat.load_era_schema("icon-art-eu", wrong)


def test_discover_schema_takes_the_grid_of_its_runs_era(fake_tree):
    old_runs = [pd.Timestamp("2026-01-07T00:00"), pd.Timestamp("2026-01-07T06:00")]
    files = [f.replace("2026-10-11T", "2026-01-07T") for f in fake_tree]
    data = cat.discover_schema(
        "icon-art", old_runs, "archive", lister=make_lister(files), check_grid=False
    )
    assert (data["n_cells"], data["grid_number"]) == (737_280, 36)
    with pytest.raises(ValueError, match="span 2"):
        cat.discover_schema(
            "icon-art", [old_runs[0], pd.Timestamp("2026-10-11T00:00")], "archive", lister=None
        )


def test_reference_runs_are_ranked_by_how_much_of_them_the_source_holds(fake_tree):
    # Drop most parameters from the newest 00Z run: the older, whole one wins its hour.
    newest = pd.Timestamp("2026-10-11T00:00")
    stem = f"/r/{newest:%Y-%m-%dT%H:%M}/"
    files = [f for f in fake_tree if stem not in f or f.startswith("p/T_2M/")]
    chosen = cat.rank_reference_runs(
        "icon", "archive", TREE_RUNS, per_hour=1, lister=make_lister(files)
    )
    assert pd.Timestamp("2026-10-10T00:00") in chosen and newest not in chosen
    assert sorted(r.hour for r in chosen) == [0, 6, 12, 18]
    two = cat.rank_reference_runs(
        "icon", "archive", TREE_RUNS, per_hour=2, lister=make_lister(files)
    )
    assert len(two) == 8


def test_an_older_era_inherits_the_current_dtypes():
    reference = {
        "variables": [
            {"param": "T_2M", "level_type": None, "dtype": "float16"},
            {"param": "T", "level_type": 150, "dtype": "float16"},
            {"param": "T", "level_type": 100, "dtype": "float16"},
            {"param": "QV", "level_type": 150, "dtype": "float32"},
        ]
    }
    data = {
        "variables": [
            {"param": "T_2M", "level_type": None},
            {"param": "T", "level_type": 106},  # a level type the references lack
            {"param": "QV", "level_type": 150},
            {"param": "NEW_FIELD", "level_type": None},
        ]
    }
    updated, unmatched = cat.inherit_precision(data, [reference])
    assert [v.get("dtype") for v in updated["variables"]] == ["float16", None, "float32", None]
    assert unmatched == [("T", 106, None), ("NEW_FIELD", None, None)]


def test_the_schema_command_refuses_runs_of_another_era():
    # Runs of the old grid cannot be written into the current era's schema; nothing is
    # listed before the refusal.
    with pytest.raises(SystemExit):
        cat.main(["schema", "icon-art", "--runs", "2026-10-01T00:00", "2026-10-01T06:00"])
