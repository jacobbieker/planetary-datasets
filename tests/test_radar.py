"""Offline tests for the UK and Finnish radar providers.

Every fixture is synthesised here, so the suite needs neither the network nor the sample
radar files that sit untracked at the repository root. The two tests that do use those
samples skip themselves when they are absent.
"""

from __future__ import annotations

import pathlib

import numpy as np
import pandas as pd
import pytest

from helpers import read_store as stored
from planetary_datasets.providers import radar as radar_module
from planetary_datasets.providers.radar import (
    FMIRadarProvider,
    IncompletePartition,
    RadarArchiveNotConfigured,
    UKRadarProvider,
    find_by_pattern,
    gdal_metadata,
    odim_grid,
    open_fmi_radar,
    open_uk_radar,
    provider_by_name,
    stamp_of,
    to_naive_utc,
)

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent

UK_SAMPLE = REPO_ROOT / "202505292000_ODIM_ng_radar_rainrate_composite_1km_UK.h5"
FMI_SAMPLE = REPO_ROOT / "202102020100_FIN-ACRR1H-3067-1KM.tif"

PROJDEF = "+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.999601 +x_0=400000 +y_0=-100000 +ellps=airy +units=m"

#: Small enough to stay fast, big enough that an axis-orientation bug shows up.
NX, NY = 4, 3
SCALE = 1000.0
#: Projected extent of the synthetic grid, in the CRS above.
X0, Y0 = -5000.0, 8000.0


def _corner_lonlats() -> dict:
    """Corner lat/lons of the synthetic grid, the way an ODIM file states them."""
    import pyproj

    back = pyproj.Transformer.from_crs(pyproj.CRS.from_proj4(PROJDEF), "EPSG:4326", always_xy=True)
    x1, y1 = X0 + NX * SCALE, Y0 - NY * SCALE
    corners = {"UL": (X0, Y0), "UR": (x1, Y0), "LL": (X0, y1), "LR": (x1, y1)}
    out = {}
    for label, (x, y) in corners.items():
        lon, lat = back.transform(x, y)
        out[f"{label}_lon"], out[f"{label}_lat"] = lon, lat
    return out


def write_odim(
    path: pathlib.Path,
    when: pd.Timestamp,
    values: np.ndarray | None = None,
    gain: float = 1.0,
    offset: float = 0.0,
    nodata: float = -1.0,
) -> pathlib.Path:
    """Write a minimal ODIM HDF5 rain-rate composite mirroring the Met Office layout."""
    import h5py

    if values is None:
        values = np.arange(NY * NX, dtype="float32").reshape(NY, NX)
    path.parent.mkdir(parents=True, exist_ok=True)
    with h5py.File(path, "w") as handle:
        what = handle.create_group("what")
        what.attrs["date"] = when.strftime("%Y%m%d")
        what.attrs["time"] = when.strftime("%H%M%S")
        what.attrs["object"] = "COMP"
        what.attrs["source"] = "WMO:03523"
        what.attrs["version"] = "H5rad 2.2"

        where = handle.create_group("where")
        where.attrs["projdef"] = PROJDEF
        where.attrs["xsize"] = NX
        where.attrs["ysize"] = NY
        where.attrs["xscale"] = SCALE
        where.attrs["yscale"] = SCALE
        for key, value in _corner_lonlats().items():
            where.attrs[key] = value

        data1 = handle.create_group("dataset1/data1")
        data1.create_dataset("data", data=values)
        data_what = data1.create_group("what")
        data_what.attrs["gain"] = gain
        data_what.attrs["offset"] = offset
        data_what.attrs["nodata"] = nodata
        data_what.attrs["undetect"] = 0.0
        data_what.attrs["quantity"] = "RATE"
    return path


def uk_name(when: pd.Timestamp) -> str:
    return f"{when:%Y%m%d%H%M}_ODIM_ng_radar_rainrate_composite_1km_UK.h5"


def write_uk_hour(root: pathlib.Path, hour: pd.Timestamp, steps: int = 12) -> list[pathlib.Path]:
    """Write ``steps`` five-minute frames of a synthetic UK hour."""
    written = []
    for index in range(steps):
        when = hour + pd.Timedelta(5 * index, "min")
        written.append(write_odim(root / uk_name(when), when))
    return written


FMI_METADATA = (
    "<GDALMetadata>\n"
    '<Item name="Observation time" format="YYYYMMDDhhmm">{stamp}</Item>\n'
    '<Item name="Quantity" unit="mm">Precipitation accumulation</Item>\n'
    '<Item name="Gain">{gain:f}</Item>\n'
    '<Item name="Offset">{offset:f}</Item>\n'
    '<Item name="Nodata">65535</Item>\n'
    '<Item name="Undetect">0</Item>\n'
    '<Item name="Accumulation time" unit="h">{hours}</Item>\n'
    "</GDALMetadata>\n"
)


def write_fmi(
    path: pathlib.Path,
    when: pd.Timestamp,
    hours: int,
    values: np.ndarray | None = None,
    gain: float = 0.01,
    offset: float = 0.0,
) -> pathlib.Path:
    """Write a minimal FMI accumulation GeoTIFF."""
    import rasterio
    from rasterio.transform import from_origin

    if values is None:
        values = np.arange(NY * NX, dtype="uint16").reshape(NY, NX)
    path.parent.mkdir(parents=True, exist_ok=True)
    metadata = FMI_METADATA.format(
        stamp=when.strftime("%Y%m%d%H%M"), gain=gain, offset=offset, hours=hours
    )
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=values.shape[0],
        width=values.shape[1],
        count=1,
        dtype="uint16",
        crs="EPSG:3067",
        transform=from_origin(0.0, 1000.0 * values.shape[0], 1000.0, 1000.0),
    ) as dst:
        dst.write(values, 1)
        dst.update_tags(GDAL_METADATA=metadata)
    return path


def fmi_name(when: pd.Timestamp, hours: int) -> str:
    return f"{when:%Y%m%d%H%M}_FIN-ACRR{hours}H-3067-1KM.tif"


def write_fmi_hour(
    root: pathlib.Path, hour: pd.Timestamp, accumulations: tuple[int, ...] = (1, 12, 24)
) -> list[pathlib.Path]:
    return [write_fmi(root / fmi_name(hour, hours), hour, hours) for hours in accumulations]


FMI_VARIABLES = [f"rainfall_rate_accumulation_{hours}h" for hours in (1, 12, 24)]




# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        (uk_name(pd.Timestamp("2025-05-29 20:00")), "2025-05-29 20:00"),
        (fmi_name(pd.Timestamp("2021-02-02 01:00"), 24), "2021-02-02 01:00"),
        # The stamp comes from the filename, not a stamped directory above it.
        ("/archive/202102020100/202505292000_x.h5", "2025-05-29 20:00"),
    ],
    ids=["uk", "fmi", "ignores-the-directory"],
)
def test_stamp_of_parses_the_filename(name, expected):
    assert stamp_of(name) == pd.Timestamp(expected)


@pytest.mark.parametrize(
    "name",
    [
        "no-timestamp-here.h5",
        # A 14-digit run is not a YYYYMMDDhhmm stamp; taking its first twelve digits would
        # silently file the data under the wrong minute.
        "20250529200000_composite.h5",
    ],
    ids=["no-digits", "longer-digit-run"],
)
def test_stamp_of_rejects_a_name_without_a_stamp(name):
    with pytest.raises(ValueError):
        stamp_of(name)


def test_to_naive_utc_normalises_tz_aware_timestamps():
    aware = pd.Timestamp("2025-05-29 21:00", tz="Europe/Berlin")
    assert to_naive_utc(aware) == pd.Timestamp("2025-05-29 19:00")
    assert to_naive_utc(aware).tzinfo is None
    naive = pd.Timestamp("2025-05-29 20:00")
    assert to_naive_utc(naive) == naive


@pytest.mark.parametrize(
    ("files", "hint", "expected_layout"),
    [
        # Listed first is the file expected back.
        (["a.h5", "deep/a.h5"], None, ""),
        (["2025/05/29/b.h5"], None, "%Y/%m/%d"),
        (["20250529/b.h5"], "%Y%m%d", "%Y%m%d"),
        (["odd/layout/b.h5"], None, None),
    ],
    ids=["prefers-the-flat-layout", "probes-a-known-date-layout", "hinted-layout", "walk-fallback"],
)
def test_find_by_pattern(tmp_path, monkeypatch, files, hint, expected_layout):
    for name in files:
        (tmp_path / name).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / name).touch()
    if expected_layout is not None:

        def explode(*args, **kwargs):
            raise AssertionError("a known date layout must not trigger a recursive walk")

        monkeypatch.setattr(pathlib.Path, "rglob", explode)

    found = find_by_pattern(tmp_path, "*.h5", when=pd.Timestamp("2025-05-29 20:00"), hint=hint)
    assert found == ([tmp_path / files[0]], expected_layout)


def test_gdal_metadata_reads_items_by_name():
    parsed = gdal_metadata(
        {"GDAL_METADATA": FMI_METADATA.format(stamp="202102020100", gain=0.01, offset=0.0, hours=12)}
    )
    assert parsed["Observation time"] == "202102020100"
    assert parsed["Accumulation time"] == "12"
    assert float(parsed["Gain"]) == pytest.approx(0.01)


def test_gdal_metadata_rejects_a_raster_without_it():
    with pytest.raises(ValueError, match="GDAL_METADATA"):
        gdal_metadata({"AREA_OR_POINT": "Area"})


def test_odim_grid_builds_descending_cell_centres():
    where = {
        "projdef": PROJDEF,
        "xsize": NX,
        "ysize": NY,
        "xscale": SCALE,
        "yscale": SCALE,
        **_corner_lonlats(),
    }
    grid = odim_grid(where)
    assert grid["x"].shape == (NX,)
    assert grid["y"].shape == (NY,)
    # Cell centres, half a cell in from the stated corner.
    assert grid["x"][0] == pytest.approx(X0 + SCALE / 2, abs=0.1)
    assert grid["y"][0] == pytest.approx(Y0 - SCALE / 2, abs=0.1)
    assert np.all(np.diff(grid["x"]) > 0)
    # ODIM row zero is the northern edge.
    assert np.all(np.diff(grid["y"]) < 0)


def test_odim_grid_reports_missing_attributes():
    with pytest.raises(ValueError, match="missing"):
        odim_grid({"projdef": PROJDEF})


# --------------------------------------------------------------------------------------
# Readers
# --------------------------------------------------------------------------------------


def test_open_uk_radar_shapes_and_times(tmp_path):
    when = pd.Timestamp("2025-05-29 20:05")
    ds = open_uk_radar(str(write_odim(tmp_path / uk_name(when), when)))
    assert ds.sizes == {"time": 1, "y": NY, "x": NX}
    assert pd.Timestamp(ds["time"].values[0]) == when
    assert ds["rainfall_rate"].dtype == np.float16
    assert ds.attrs["crs"] == PROJDEF


def test_open_uk_radar_applies_gain_offset_and_nodata(tmp_path):
    when = pd.Timestamp("2025-05-29 20:00")
    values = np.array([[-1.0, 0.0, 10.0, 20.0]] * NY, dtype="float32")
    path = write_odim(tmp_path / uk_name(when), when, values=values, gain=0.5, offset=1.0)
    rate = open_uk_radar(str(path))["rainfall_rate"].isel(time=0).values

    assert np.isnan(rate[0, 0])  # nodata
    # undetect (0) is a measured zero, not a gap, so it survives the gain/offset.
    assert rate[0, 1] == pytest.approx(1.0)
    assert rate[0, 2] == pytest.approx(6.0)
    assert rate[0, 3] == pytest.approx(11.0)


def test_uk_reader_rejects_a_non_rate_product(tmp_path):
    """RADARNET also ships reflectivity composites; they must not land in the mm/h store."""
    import h5py

    when = pd.Timestamp("2025-05-29 20:00")
    path = write_odim(tmp_path / uk_name(when), when)
    with h5py.File(path, "r+") as handle:
        handle["/dataset1/data1/what"].attrs["quantity"] = "DBZH"
    with pytest.raises(ValueError, match="DBZH"):
        open_uk_radar(str(path))


def test_open_fmi_radar_names_the_variable_from_the_metadata(tmp_path):
    when = pd.Timestamp("2021-02-02 01:00")
    # Deliberately mismatch the filename and the declared window: the metadata wins.
    path = write_fmi(tmp_path / fmi_name(when, 1), when, hours=24)
    ds = open_fmi_radar(str(path))
    assert list(ds.data_vars) == ["rainfall_rate_accumulation_24h"]
    assert pd.Timestamp(ds["time"].values[0]) == when


def test_open_fmi_radar_applies_gain_and_nodata(tmp_path):
    when = pd.Timestamp("2021-02-02 01:00")
    values = np.array([[65535, 0, 100, 250]] * NY, dtype="uint16")
    path = write_fmi(tmp_path / fmi_name(when, 1), when, hours=1, values=values, gain=0.01)
    acc = open_fmi_radar(str(path))["rainfall_rate_accumulation_1h"].isel(time=0).values

    assert np.isnan(acc[0, 0])
    assert acc[0, 1] == pytest.approx(0.0)
    assert acc[0, 2] == pytest.approx(1.0, abs=1e-2)
    assert acc[0, 3] == pytest.approx(2.5, abs=1e-2)


# --------------------------------------------------------------------------------------
# Archive discovery and completeness
# --------------------------------------------------------------------------------------


@pytest.fixture
def make_provider(tmp_path, monkeypatch, local_config):
    """Build a radar provider over its own empty archive directory under ``tmp_path``."""

    def make(cls, **kwargs):
        archive = tmp_path / cls.name
        archive.mkdir()
        monkeypatch.setenv(cls.archive_env, str(archive))
        return cls(config=local_config, **kwargs)

    return make


@pytest.fixture
def uk_provider(make_provider):
    return make_provider(UKRadarProvider)


@pytest.fixture
def fmi_provider(make_provider):
    return make_provider(FMIRadarProvider)


def test_archive_dir_defaults_under_the_data_dir(tmp_path, local_config):
    assert UKRadarProvider().archive_dir == tmp_path / "data" / "uk_radar"
    assert FMIRadarProvider().archive_dir == tmp_path / "data" / "fmi_radar"


def test_archive_dir_honours_the_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("FMI_RADAR_ARCHIVE_DIR", str(tmp_path / "elsewhere"))
    assert FMIRadarProvider().archive_dir == tmp_path / "elsewhere"


def test_missing_archive_directory_is_a_configuration_error(tmp_path, monkeypatch, local_config):
    monkeypatch.setenv("UK_RADAR_ARCHIVE_DIR", str(tmp_path / "absent"))
    with pytest.raises(RadarArchiveNotConfigured, match="UK_RADAR_ARCHIVE_DIR"):
        UKRadarProvider(config=local_config).fetch(pd.Timestamp("2025-05-29 20:00"))


def test_a_genuinely_absent_hour_is_nothing_to_do(uk_provider):
    hour = pd.Timestamp("2025-05-29 20:00")
    assert uk_provider.fetch(hour) == []
    assert uk_provider.run_partition(hour) is False


def test_fetch_raises_on_a_partly_filled_hour(uk_provider):
    hour = pd.Timestamp("2025-05-29 20:00")
    write_uk_hour(uk_provider.archive_dir, hour, steps=11)
    with pytest.raises(IncompletePartition, match="20:55"):
        uk_provider.fetch(hour)


def test_allow_partial_fetches_what_is_there(make_provider):
    provider = make_provider(UKRadarProvider, allow_partial=True)
    hour = pd.Timestamp("2025-05-29 20:00")
    write_uk_hour(provider.archive_dir, hour, steps=3)
    assert len(provider.fetch(hour)) == 3


def test_fetch_ignores_frames_from_a_neighbouring_hour(uk_provider):
    hour = pd.Timestamp("2025-05-29 20:00")
    write_uk_hour(uk_provider.archive_dir, hour)
    write_uk_hour(uk_provider.archive_dir, hour + pd.Timedelta(1, "h"), steps=2)
    found = uk_provider.fetch(hour)
    assert len(found) == 12
    assert all(stamp_of(p).hour == 20 for p in found)


def test_fetch_finds_a_date_nested_archive(uk_provider):
    hour = pd.Timestamp("2025-05-29 20:00")
    write_uk_hour(uk_provider.archive_dir / "2025" / "05" / "29", hour)
    assert len(uk_provider.fetch(hour)) == 12


def test_fmi_fetch_requires_every_accumulation_window(fmi_provider):
    hour = pd.Timestamp("2021-02-02 01:00")
    write_fmi_hour(fmi_provider.archive_dir, hour, accumulations=(1, 24))
    with pytest.raises(IncompletePartition, match="12h"):
        fmi_provider.fetch(hour)

    write_fmi_hour(fmi_provider.archive_dir, hour, accumulations=(12,))
    assert len(fmi_provider.fetch(hour)) == 3


def test_fmi_accumulations_can_be_narrowed(fmi_provider):
    fmi_provider.accumulations = (1,)
    hour = pd.Timestamp("2021-02-02 01:00")
    write_fmi_hour(fmi_provider.archive_dir, hour, accumulations=(1,))
    assert len(fmi_provider.fetch(hour)) == 1


# --------------------------------------------------------------------------------------
# Processing and writing
# --------------------------------------------------------------------------------------


def test_uk_process_concatenates_the_hour_in_time_order(uk_provider):
    hour = pd.Timestamp("2025-05-29 20:00")
    paths = write_uk_hour(uk_provider.archive_dir, hour)
    ds = uk_provider.process([str(p) for p in reversed(paths)], hour)
    assert ds.sizes["time"] == 12
    times = pd.DatetimeIndex(ds["time"].values)
    assert times[0] == hour
    assert times.is_monotonic_increasing


def test_uk_process_refuses_to_outer_join_a_changed_grid(uk_provider, monkeypatch):
    hour = pd.Timestamp("2025-05-29 20:00")
    paths = write_uk_hour(uk_provider.archive_dir, hour, steps=2)

    real = radar_module.open_uk_radar

    def shifted(path):
        ds = real(path)
        if path.endswith(uk_name(hour + pd.Timedelta(5, "min"))):
            ds = ds.assign_coords(x=ds["x"] + 500.0)
        return ds

    monkeypatch.setattr(radar_module, "open_uk_radar", shifted)
    with pytest.raises(ValueError):
        uk_provider.process([str(p) for p in paths], hour)


def test_fmi_process_merges_the_three_windows(fmi_provider):
    hour = pd.Timestamp("2021-02-02 01:00")
    paths = write_fmi_hour(fmi_provider.archive_dir, hour)
    ds = fmi_provider.process([str(p) for p in paths], hour)
    assert set(ds.data_vars) == set(FMI_VARIABLES)
    assert ds.sizes["time"] == 1


def test_fmi_process_rejects_products_from_different_times(fmi_provider):
    hour = pd.Timestamp("2021-02-02 01:00")
    good = write_fmi(fmi_provider.archive_dir / fmi_name(hour, 1), hour, hours=1)
    later = hour + pd.Timedelta(1, "h")
    # Same filename stamp, different observation time in the metadata.
    odd = write_fmi(fmi_provider.archive_dir / fmi_name(hour, 24), later, hours=24)
    with pytest.raises(ValueError, match="disagree"):
        fmi_provider.process([str(good), str(odd)], hour)


@pytest.mark.parametrize("tz", [None, "UTC"], ids=["naive", "tz-aware"])
def test_run_partition_writes_and_is_idempotent(uk_provider, tz):
    """Dagster hands over tz-aware starts; they must land as the same naive UTC time."""
    hour = pd.Timestamp("2025-05-29 20:00")
    write_uk_hour(uk_provider.archive_dir, hour)

    assert uk_provider.partition_stored(hour) is False
    assert uk_provider.run_partition(hour.tz_localize(tz)) is True
    # A strict provider skips an hour by its start alone, under the *naive* timestamp too.
    assert uk_provider.partition_stored(hour) is True
    assert uk_provider.run_partition(hour) is False

    ds = stored(uk_provider)
    assert ds.sizes["time"] == 12
    assert pd.Timestamp(ds["time"].values[0]) == hour
    assert ds["rainfall_rate"].dtype == np.float16


def test_run_partition_appends_the_next_hour(uk_provider):
    first = pd.Timestamp("2025-05-29 20:00")
    second = pd.Timestamp("2025-05-29 21:00")
    write_uk_hour(uk_provider.archive_dir, first)
    write_uk_hour(uk_provider.archive_dir, second)

    assert uk_provider.run_partition(first) is True
    assert uk_provider.run_partition(second) is True

    ds = stored(uk_provider)
    assert ds.sizes["time"] == 24
    assert pd.DatetimeIndex(ds["time"].values).is_monotonic_increasing


def test_fmi_run_partition_round_trips(fmi_provider):
    hour = pd.Timestamp("2021-02-02 01:00")
    write_fmi_hour(fmi_provider.archive_dir, hour)

    assert fmi_provider.run_partition(hour.tz_localize("UTC")) is True

    ds = stored(fmi_provider)
    assert pd.Timestamp(ds["time"].values[0]) == hour
    assert "rainfall_rate_accumulation_12h" in ds.data_vars


@pytest.mark.parametrize("tz", [None, "UTC"], ids=["naive", "tz-aware"])
def test_run_range_skips_hours_already_stored(uk_provider, tz):
    hours = pd.date_range("2025-05-29 20:00", periods=2, freq="1h", tz=tz)
    for hour in hours:
        write_uk_hour(uk_provider.archive_dir, hour.tz_localize(None))

    assert uk_provider.run_range(hours) == 2
    assert uk_provider.run_range(hours) == 0
    assert pd.Timestamp(stored(uk_provider)["time"].values[0]) == pd.Timestamp("2025-05-29 20:00")


def test_write_rejects_a_grid_that_does_not_match_the_store(uk_provider):
    hour = pd.Timestamp("2025-05-29 20:00")
    paths = write_uk_hour(uk_provider.archive_dir, hour)
    assert uk_provider.run_partition(hour) is True

    processed = uk_provider.process([str(p) for p in paths], hour)
    shifted = processed.assign_coords(
        time=processed["time"] + pd.Timedelta(1, "h"),
        x=processed["x"] + 500.0,
    )
    assert uk_provider.write_to_icechunk(uk_provider.get_icechunk_repo(), shifted) is False


def test_fmi_partial_hour_still_carries_every_variable(make_provider):
    """A partial write must not fix the store's schema to a subset of the products."""
    provider = make_provider(FMIRadarProvider, allow_partial=True)

    first = pd.Timestamp("2021-02-02 01:00")
    write_fmi_hour(provider.archive_dir, first, accumulations=(1, 24))
    assert provider.run_partition(first) is True

    ds = stored(provider)
    assert set(ds.data_vars) == set(FMI_VARIABLES)
    assert bool(np.isnan(ds["rainfall_rate_accumulation_12h"].values).all())

    # The next, complete hour must append rather than be rejected for a variable mismatch.
    second = first + pd.Timedelta(1, "h")
    write_fmi_hour(provider.archive_dir, second)
    assert provider.run_partition(second) is True
    ds = stored(provider)
    assert ds.sizes["time"] == 2
    assert not bool(np.isnan(ds["rainfall_rate_accumulation_12h"].isel(time=1).values).all())


def test_allow_partial_revisits_an_hour_that_later_fills_in(make_provider):
    """A short commit must not make the whole hour look done for ever."""
    provider = make_provider(UKRadarProvider, allow_partial=True)
    hour = pd.Timestamp("2025-05-29 20:00")

    write_uk_hour(provider.archive_dir, hour, steps=4)
    assert provider.run_partition(hour) is True
    assert provider.partition_stored(hour) is False

    write_uk_hour(provider.archive_dir, hour)
    assert provider.run_partition(hour) is True

    assert stored(provider).sizes["time"] == 12
    assert provider.partition_stored(hour) is True
    assert provider.run_partition(hour) is False


def test_provider_by_name():
    assert provider_by_name("uk_radar") is UKRadarProvider
    assert provider_by_name("fmi_radar") is FMIRadarProvider
    with pytest.raises(KeyError):
        provider_by_name("nope")


def test_store_prefixes_are_distinct():
    assert UKRadarProvider.store_prefix != FMIRadarProvider.store_prefix


# --------------------------------------------------------------------------------------
# The untracked sample files at the repository root, when they are present
# --------------------------------------------------------------------------------------


@pytest.mark.skipif(not UK_SAMPLE.is_file(), reason="UK ODIM sample not present")
def test_real_uk_sample():
    ds = open_uk_radar(str(UK_SAMPLE))
    assert ds.sizes == {"time": 1, "y": 2175, "x": 1725}
    assert pd.Timestamp(ds["time"].values[0]) == pd.Timestamp("2025-05-29 20:00")
    # The corner lat/lons round-trip to a clean 1 km grid.
    assert np.diff(ds["x"].values).min() == pytest.approx(1000.0, abs=1e-6)
    assert np.diff(ds["y"].values).max() == pytest.approx(-1000.0, abs=1e-6)


@pytest.mark.skipif(not FMI_SAMPLE.is_file(), reason="FMI accumulation sample not present")
def test_real_fmi_sample():
    ds = open_fmi_radar(str(FMI_SAMPLE))
    assert list(ds.data_vars) == ["rainfall_rate_accumulation_1h"]
    assert pd.Timestamp(ds["time"].values[0]) == pd.Timestamp("2021-02-02 01:00")
    assert ds.sizes["y"] == 1345
    assert ds.sizes["x"] == 850
