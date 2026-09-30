"""Shared helpers: downloads, dataset shaping, store writes."""

from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
import pytest
import xarray as xr
import zarr

from planetary_datasets.common import dataset as ds_helpers
from planetary_datasets.common import download as dl
from planetary_datasets.common import store as store_helpers
from planetary_datasets.common.grib import quiet_combine_defaults
from planetary_datasets.common.time import freq_to_timedelta


class TestReducePrecision:
    def test_matching_variables_are_downcast(self):
        ds = xr.Dataset(
            {
                "wind_speed": ("x", np.ones(3, dtype="float32")),
                "pressure": ("x", np.ones(3, dtype="float32")),
            }
        )
        out = ds_helpers.reduce_precision(ds)
        assert out["wind_speed"].dtype == np.float16
        assert out["pressure"].dtype == np.float32

    def test_input_is_not_modified(self):
        ds = xr.Dataset({"wind_speed": ("x", np.ones(3, dtype="float32"))})
        ds_helpers.reduce_precision(ds)
        assert ds["wind_speed"].dtype == np.float32


class TestCoordinateNormalisation:
    def test_longitude_is_converted_to_minus_180(self):
        ds = xr.Dataset({"v": ("longitude", np.arange(4))}, coords={"longitude": [0, 90, 180, 270]})
        out = ds_helpers.lon_to_m180(ds)
        assert out.longitude.min() >= -180
        assert out.longitude.max() < 180
        assert list(out.longitude.values) == sorted(out.longitude.values)

    def test_descending_latitude_is_sorted(self):
        ds = xr.Dataset({"v": ("latitude", np.arange(3))}, coords={"latitude": [10, 0, -10]})
        out = ds_helpers.make_spatial_coords_increasing(ds)
        assert list(out.latitude.values) == [-10, 0, 10]

    def test_missing_coords_are_tolerated(self):
        ds = xr.Dataset({"v": ("x", np.arange(3))})
        assert ds_helpers.make_lat_lon_coords_consistent(ds).equals(ds)


def _on_latitude(values) -> xr.Dataset:
    return xr.Dataset({"v": ("latitude", np.arange(len(values)))}, coords={"latitude": values})


class TestCoordsMatch:
    @pytest.mark.parametrize(
        ("other", "expected"),
        [
            ([1, 2, 3], (True, None)),
            ([1, 2, 4], (False, "latitude")),
            # A length change is reported, not raised.
            ([1, 2], (False, "latitude")),
        ],
        ids=["identical", "differing-values", "differing-lengths"],
    )
    def test_one_dimensional_coords_are_compared(self, other, expected):
        assert ds_helpers.coords_match(_on_latitude([1, 2, 3]), _on_latitude(other), ("latitude",)) == expected

    def test_two_dimensional_non_dim_coords_are_checked(self):
        """Regression: keying on dims meant geostationary 2-D lat/lon was never compared."""
        base = xr.Dataset(
            {"v": (("y", "x"), np.zeros((2, 3)))},
            coords={"latitude": (("y", "x"), np.zeros((2, 3)))},
        )
        shifted = xr.Dataset(
            {"v": (("y", "x"), np.zeros((2, 3)))},
            coords={"latitude": (("y", "x"), np.ones((2, 3)))},
        )
        assert ds_helpers.coords_match(base, base, ("latitude",)) == (True, None)
        assert ds_helpers.coords_match(base, shifted, ("latitude",)) == (False, "latitude")

    def test_a_nan_station_position_still_matches_itself(self):
        """Regression: a NaN coordinate made the store fail to match itself.

        Station rosters carry NaN lat/lon for stations with no known position (ISD leaves
        LAT/LON blank for hundreds of them), and NaN != NaN, so every append after the
        first was silently skipped while the Dagster asset still went green.
        """
        roster = xr.Dataset(
            {"temperature": ("station", np.zeros(3))},
            coords={
                "station": np.array(["a", "b", "c"], dtype=object),
                "latitude": ("station", np.array([51.5, np.nan, -33.9])),
            },
        )
        assert ds_helpers.coords_match(roster, roster, ("latitude",)) == (True, None)

    def test_a_moved_station_is_still_reported(self):
        """The NaN tolerance must not make the guard blind to a real change."""
        a = xr.Dataset(
            {"v": ("station", np.zeros(2))},
            coords={"latitude": ("station", np.array([51.5, np.nan]))},
        )
        b = xr.Dataset(
            {"v": ("station", np.zeros(2))},
            coords={"latitude": ("station", np.array([52.5, np.nan]))},
        )
        assert ds_helpers.coords_match(a, b, ("latitude",)) == (False, "latitude")

    def test_non_float_coords_are_still_compared(self):
        """equal_nan is only valid for floats; a string station axis must not raise."""
        a = xr.Dataset(coords={"station": np.array(["a", "b"], dtype=object)})
        b = xr.Dataset(coords={"station": np.array(["a", "c"], dtype=object)})
        assert ds_helpers.coords_match(a, a, ("station",)) == (True, None)
        assert ds_helpers.coords_match(a, b, ("station",)) == (False, "station")


class TestRenameVarsByLongName:
    def test_variables_are_renamed_to_their_long_name(self):
        ds = xr.Dataset({"t2m": ("x", np.zeros(2))})
        ds["t2m"].attrs["long_name"] = "2 metre temperature"
        out = ds_helpers.rename_vars_by_long_name(ds, suffix="_at_surface")
        assert list(out.data_vars) == ["2_metre_temperature_at_surface"]

    def test_a_long_name_that_collides_with_another_variables_name_does_not_raise(self):
        """Regression: the fallback name could collide with an already-renamed variable.

        Two variables then pointed at one name, and ``ds.rename`` rejects that outright.
        """
        ds = xr.Dataset({"t": ("x", np.zeros(2)), "temperature": ("x", np.zeros(2))})
        ds["t"].attrs["long_name"] = "Temperature"

        out = ds_helpers.rename_vars_by_long_name(ds)

        assert len(out.data_vars) == 2
        assert "temperature" in out.data_vars


class TestDownloadOne:
    def test_file_is_downloaded(self, tmp_path):
        src = tmp_path / "src.bin"
        src.write_bytes(b"payload")
        dest = tmp_path / "out" / "dest.bin"
        assert dl.download_one(str(src), dest) == dest
        assert dest.read_bytes() == b"payload"

    @pytest.mark.parametrize(
        ("existing", "overwrite", "expected"),
        [
            (b"old", False, b"old"),
            (b"old", True, b"new"),
            # A zero-byte file is a failed earlier attempt, not a download.
            (b"", False, b"new"),
        ],
        ids=["skipped", "overwritten", "zero-byte-refetched"],
    )
    def test_an_existing_file_is_reused_unless_empty_or_overwritten(
        self, tmp_path, existing, overwrite, expected
    ):
        src = tmp_path / "src.bin"
        src.write_bytes(b"new")
        dest = tmp_path / "dest.bin"
        dest.write_bytes(existing)
        dl.download_one(str(src), dest, overwrite=overwrite)
        assert dest.read_bytes() == expected

    def test_failure_returns_none_and_leaves_no_partial(self, tmp_path):
        dest = tmp_path / "dest.bin"
        assert dl.download_one(str(tmp_path / "missing.bin"), dest, retries=1, backoff=0) is None
        assert not dest.exists()
        assert not dest.with_name("dest.bin.part").exists()

    def test_missing_source_is_not_retried(self, tmp_path, monkeypatch):
        """A 404 will not become a success; retrying only burns the backoff budget."""
        calls = []
        real_open = dl.fsspec.open

        def counting_open(url, *args, **kwargs):
            calls.append(url)
            return real_open(url, *args, **kwargs)

        monkeypatch.setattr(dl.fsspec, "open", counting_open)
        assert dl.download_one(str(tmp_path / "gone.bin"), tmp_path / "d.bin", retries=5, backoff=0) is None
        assert len(calls) == 1

    def test_transient_errors_are_still_retried(self, tmp_path, monkeypatch):
        calls = []

        def flaky_open(url, *args, **kwargs):
            calls.append(url)
            raise TimeoutError("transient")

        monkeypatch.setattr(dl.fsspec, "open", flaky_open)
        assert dl.download_one("http://x/y.bin", tmp_path / "d.bin", retries=3, backoff=0) is None
        assert len(calls) == 3


class TestDownloadMany:
    def test_same_basename_in_different_directories_does_not_collide(self, tmp_path):
        (tmp_path / "a").mkdir()
        (tmp_path / "b").mkdir()
        (tmp_path / "a" / "f.bin").write_bytes(b"AAA")
        (tmp_path / "b" / "f.bin").write_bytes(b"BBB")
        out = tmp_path / "out"
        got = dl.download_many([str(tmp_path / "a" / "f.bin"), str(tmp_path / "b" / "f.bin")], out)
        assert len(got) == 2
        assert {p.read_bytes() for p in got} == {b"AAA", b"BBB"}

    def test_duplicate_urls_are_fetched_once(self, tmp_path):
        """Regression: identical URLs mapped to one path and raced on it."""
        src = tmp_path / "f.bin"
        src.write_bytes(b"payload")
        got = dl.download_many([str(src), str(src), str(src)], tmp_path / "out")
        assert len(got) == 1
        assert got[0].read_bytes() == b"payload"

    def test_unique_names_keep_their_basename(self, tmp_path):
        (tmp_path / "one.bin").write_bytes(b"1")
        (tmp_path / "two.bin").write_bytes(b"2")
        got = dl.download_many([str(tmp_path / "one.bin"), str(tmp_path / "two.bin")], tmp_path / "out")
        assert sorted(p.name for p in got) == ["one.bin", "two.bin"]


class TestCleanupFiles:
    def test_files_and_index_sidecars_are_removed(self, tmp_path):
        grib = tmp_path / "a.grib2"
        grib.write_bytes(b"x")
        idx = tmp_path / "a.grib2.5b7b6.idx"
        idx.write_bytes(b"y")
        assert dl.cleanup_files(grib) == 2
        assert not grib.exists() and not idx.exists()

    def test_missing_file_is_tolerated(self, tmp_path):
        assert dl.cleanup_files(tmp_path / "nope.grib2") == 0


class TestBuildEncoding:
    def test_every_variable_gets_a_compressor_and_time_an_integer_encoding(self, sample_dataset):
        enc = store_helpers.build_encoding(sample_dataset)
        assert set(enc) == {"temperature", "pressure", "time"}
        assert "compressors" in enc["temperature"]
        assert enc["time"]["dtype"] == "int64"

    def test_a_non_temporal_append_dim_gets_no_time_encoding(self):
        """Regression: CF time encoding on a string coord dies with 'rint' not supported."""
        ds = xr.Dataset(
            {"v": ("station", np.arange(3, dtype="float32"))},
            coords={"station": np.array(["AAA", "BBB", "CCC"], dtype=object)},
        )
        enc = store_helpers.build_encoding(ds, append_dim="station")
        assert "station" not in enc
        assert "compressors" in enc["v"]


class TestFreqToTimedelta:
    @pytest.mark.parametrize(
        ("freq", "expected"),
        [("1h", 3600), ("2min", 120), ("1D", 86400), ("0s", 0), ("20s", 20)],
    )
    def test_a_frequency_string_becomes_its_duration(self, freq, expected):
        assert freq_to_timedelta(freq).total_seconds() == expected

    def test_it_does_not_build_a_generic_unit_timedelta(self):
        """``pd.Timedelta("1h")`` warns on the pinned pandas; the helper must not."""
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            assert freq_to_timedelta("1h") == pd.Timedelta(1, "h")


class TestQuietCombineDefaults:
    """cfgrib merges the hypercubes it reads without passing ``compat``; we cannot fix
    that call, so :func:`quiet_combine_defaults` covers it."""

    @staticmethod
    def _overlapping_merge():
        """Merge two datasets sharing a variable name, which is when ``compat`` matters."""

        def build():
            return xr.Dataset({"v": ("x", np.ones(3))}, coords={"x": [1, 2, 3]})

        return lambda: xr.merge([build(), build()])

    def test_the_warning_this_suppresses_is_still_worded_the_way_we_match_it(self):
        """If xarray rewords it the filter stops matching, so assert it fires unwrapped."""
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self._overlapping_merge()()
        assert [w for w in caught if "default value for compat" in str(w.message)]

    def test_inside_the_context_manager_it_is_silent(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with quiet_combine_defaults():
                self._overlapping_merge()()
        assert [w for w in caught if "default value for compat" in str(w.message)] == []


def _at(ds: xr.Dataset, *stamps: str) -> xr.Dataset:
    """``ds`` moved to the given time steps."""
    return ds.assign_coords(time=pd.DatetimeIndex(list(stamps)))


def _break_reads(monkeypatch):
    def boom(*args, **kwargs):
        raise ValueError("transient S3 read failure")

    monkeypatch.setattr(store_helpers.xr, "open_zarr", boom)


class TestStoreRoundTrip:
    @pytest.fixture
    def repo(self, local_config):
        return local_config.icechunk_repo("test/roundtrip.icechunk")

    @pytest.fixture
    def written(self, repo, sample_dataset):
        """A store holding ``sample_dataset``'s single step, 2026-01-01T00:00."""
        assert store_helpers.write_to_icechunk(repo, sample_dataset) is True
        return repo

    def test_a_fresh_store_is_empty_until_the_first_write(self, repo, sample_dataset):
        assert store_helpers.has_committed_data(repo) is False
        assert store_helpers.existing_times(repo).size == 0
        store_helpers.write_to_icechunk(repo, sample_dataset)
        assert store_helpers.has_committed_data(repo) is True
        assert store_helpers.existing_times(repo).size == 1

    def test_second_write_of_the_same_time_is_skipped(self, written, sample_dataset):
        assert store_helpers.write_to_icechunk(written, sample_dataset) is False
        assert store_helpers.existing_times(written).size == 1

    def test_a_new_time_is_appended(self, written, sample_dataset):
        assert store_helpers.write_to_icechunk(written, _at(sample_dataset, "2026-01-01T01:00")) is True
        assert store_helpers.existing_times(written).size == 2

    def test_a_step_older_than_the_store_is_appended_out_of_order(self, written, sample_dataset):
        """A late-arriving earlier timestep is kept, on the end, leaving `time` unsorted.

        This is the trade: refusing it lost the step for good, which is how a backfill
        running its partitions out of order lost most of them. `sort_append_axis` puts the
        axis back in order afterwards.
        """
        store_helpers.write_to_icechunk(written, _at(sample_dataset, "2026-01-01T02:00"))
        assert store_helpers.write_to_icechunk(written, _at(sample_dataset, "2026-01-01T01:00")) is True

        times = pd.DatetimeIndex(store_helpers.existing_times(written))
        assert list(times) == list(
            pd.DatetimeIndex(["2026-01-01T00:00", "2026-01-01T02:00", "2026-01-01T01:00"])
        )
        assert not times.is_monotonic_increasing
        assert store_helpers.axis_is_sorted(written) is False

    def test_a_batch_straddling_the_store_end_keeps_every_new_step(self, written, sample_dataset):
        store_helpers.write_to_icechunk(written, _at(sample_dataset, "2026-01-01T03:00"))
        batch = xr.concat(
            [_at(sample_dataset, "2026-01-01T02:00"), _at(sample_dataset, "2026-01-01T04:00")], dim="time"
        )
        assert store_helpers.write_to_icechunk(written, batch) is True

        times = pd.DatetimeIndex(store_helpers.existing_times(written))
        assert pd.Timestamp("2026-01-01T02:00") in times
        assert pd.Timestamp("2026-01-01T04:00") in times

    def test_require_monotonic_still_drops_earlier_steps(self, written, sample_dataset):
        """The old, lossy behaviour is still reachable for a store that must stay sorted."""
        store_helpers.write_to_icechunk(written, _at(sample_dataset, "2026-01-01T02:00"))
        stale = _at(sample_dataset, "2026-01-01T01:00")
        assert store_helpers.write_to_icechunk(written, stale, require_monotonic=True) is False
        assert store_helpers.existing_times(written).size == 2
        assert store_helpers.axis_is_sorted(written) is True

    def test_sorting_puts_an_out_of_order_axis_back_in_order(self, written, sample_dataset):
        """The whole point: the data stays paired with its timestamp through the remap."""
        for hour, value in (("02:00", 2.0), ("01:00", 1.0), ("03:00", 3.0)):
            step = _at(sample_dataset, f"2026-01-01T{hour}")
            step["temperature"] = step["temperature"] * 0 + value
            store_helpers.write_to_icechunk(written, step)

        before = xr.open_zarr(
            written.readonly_session("main").store, consolidated=False, decode_timedelta=True
        ).load()
        assert not pd.DatetimeIndex(before.time.values).is_monotonic_increasing

        assert store_helpers.sort_append_axis(written) is True
        assert store_helpers.axis_is_sorted(written) is True

        after = xr.open_zarr(
            written.readonly_session("main").store, consolidated=False, decode_timedelta=True
        ).load()
        assert list(pd.DatetimeIndex(after.time.values)) == sorted(
            pd.DatetimeIndex(before.time.values)
        )
        # Every step still carries the values it was written with, at its new index.
        xr.testing.assert_identical(after, before.sortby("time"))

    def test_sorting_an_ordered_axis_does_nothing(self, written, sample_dataset):
        store_helpers.write_to_icechunk(written, _at(sample_dataset, "2026-01-01T01:00"))
        assert store_helpers.sort_append_axis(written) is False

    def test_mismatched_variables_are_refused(self, written, sample_dataset):
        different = _at(sample_dataset.drop_vars("pressure"), "2026-01-01T01:00")
        assert store_helpers.write_to_icechunk(written, different) is False
        assert store_helpers.existing_times(written).size == 1

    def test_mismatched_coords_are_refused(self, written, sample_dataset):
        shifted = _at(sample_dataset, "2026-01-01T01:00").assign_coords(
            latitude=sample_dataset.latitude.values + 1
        )
        assert store_helpers.write_to_icechunk(written, shifted) is False

    def test_has_timestep_reflects_what_was_written(self, repo, sample_dataset):
        stamp = pd.Timestamp("2026-01-01T00:00")
        assert store_helpers.has_timestep(repo, stamp) is False
        store_helpers.write_to_icechunk(repo, sample_dataset)
        assert store_helpers.has_timestep(repo, stamp) is True

    def test_missing_timesteps_filters_what_is_stored(self, written):
        wanted = pd.DatetimeIndex(["2026-01-01T00:00", "2026-01-01T01:00"])
        assert store_helpers.missing_timesteps(written, list(wanted)) == [pd.Timestamp("2026-01-01T01:00")]

    def test_read_failure_never_overwrites_a_populated_store(self, written, sample_dataset, monkeypatch):
        """A transient read error must not be mistaken for an empty store.

        Regression: the create-fresh path would otherwise replace a multi-year archive
        with a single timestep.
        """
        _break_reads(monkeypatch)
        with pytest.raises(store_helpers.StoreReadError, match="Refusing to overwrite"):
            store_helpers.write_to_icechunk(written, _at(sample_dataset, "2026-01-01T01:00"))

    def test_read_failure_is_not_reported_as_no_times(self, written, monkeypatch):
        _break_reads(monkeypatch)
        with pytest.raises(store_helpers.StoreReadError, match="Refusing to report it as empty"):
            store_helpers.existing_times(written)

    def test_dataset_without_append_dim_is_rejected(self, repo):
        with pytest.raises(ValueError, match="no 'time' coordinate"):
            store_helpers.write_to_icechunk(repo, xr.Dataset({"v": ("x", np.arange(3))}))


def _forecast(stamp: str) -> xr.Dataset:
    """One init time of a three step forecast, the shape the ``step`` stores have."""
    return xr.Dataset(
        {"x": (("init_time", "step"), np.zeros((1, 3), dtype="float32"))},
        coords={
            "init_time": pd.DatetimeIndex([stamp]),
            "step": pd.to_timedelta(np.arange(1, 4), unit="h"),
        },
    )


def test_reordering_a_forecast_store_on_a_non_default_append_dim(local_config):
    """A store with a second dimension, reordered along ``init_time`` rather than ``time``.

    Exercises the parts a single-dimension store does not: finding the append axis by name
    rather than assuming axis 0, and leaving ``step`` — which spans no append axis —
    untouched.
    """
    repo = local_config.icechunk_repo("test/reorder_forecast.icechunk")
    for stamp, value in (("2026-01-01", 0.0), ("2026-01-03", 3.0), ("2026-01-02", 2.0)):
        run = _forecast(stamp)
        run["x"] = run["x"] + value
        assert store_helpers.write_to_icechunk(repo, run, append_dim="init_time") is True

    assert store_helpers.axis_is_sorted(repo, append_dim="init_time") is False
    assert store_helpers.sort_append_axis(repo, append_dim="init_time") is True

    stored = xr.open_zarr(
        repo.readonly_session("main").store, consolidated=False, decode_timedelta=True
    ).load()
    assert list(pd.DatetimeIndex(stored.init_time.values)) == list(
        pd.DatetimeIndex(["2026-01-01", "2026-01-02", "2026-01-03"])
    )
    # Each run kept its own values, and the step axis came through unchanged.
    assert list(stored.x[:, 0].values) == [0.0, 2.0, 3.0]
    assert list(stored.step.values) == list(pd.to_timedelta(np.arange(1, 4), unit="h"))


class TestLegacyStepStores:
    """Appending to a store whose ``step`` predates xarray's ``dtype`` attribute.

    Every forecast store in the archive was written before xarray started tagging
    timedelta variables with a ``dtype`` attribute, so xarray decodes their ``step`` from
    its ``units`` alone and warns that it will stop doing so. Reads pin the behaviour
    down with ``decode_timedelta=True``; the append path cannot pass it through icechunk
    and silences the warning instead.
    """

    @pytest.fixture
    def legacy_store(self, local_config):
        repo = local_config.icechunk_repo("test/legacy_step.icechunk")
        store_helpers.write_to_icechunk(repo, _forecast("2026-01-01T00:00"), append_dim="init_time")
        session = repo.writable_session("main")
        step = zarr.open_group(session.store, mode="a")["step"]
        attrs = {k: v for k, v in step.attrs.items() if k != "dtype"}
        step.attrs.clear()
        step.attrs.update(attrs)
        session.commit("drop the dtype attribute xarray now writes")
        return repo

    def test_appending_emits_no_timedelta_decoding_warning(self, legacy_store):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            assert (
                store_helpers.write_to_icechunk(
                    legacy_store, _forecast("2026-01-02T00:00"), append_dim="init_time"
                )
                is True
            )
        assert [w for w in caught if "will not decode the variable" in str(w.message)] == []

    def test_step_still_reads_back_as_a_timedelta(self, legacy_store):
        stored = xr.open_zarr(
            legacy_store.readonly_session("main").store,
            consolidated=False,
            decode_timedelta=True,
        )
        assert stored["step"].dtype == np.dtype("timedelta64[ns]")
        assert stored["step"].values[0] == pd.Timedelta(1, "h")
