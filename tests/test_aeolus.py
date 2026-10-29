"""Aeolus via VirES: products into the shared store, offline against fake responses."""

from __future__ import annotations

import json

import netCDF4
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from planetary_datasets.providers.polar import aeolus
from planetary_datasets.providers.polar.aeolus import (
    LEVELS,
    AeolusProvider,
    _read_raw,
    product_span,
    sources,
)

L2B_A = "AE_OPER_ALD_U_N_2B_20201002T000000_20201002T013000_0005"
L2B_B = "AE_OPER_ALD_U_N_2B_20201002T012930_20201002T030000_0005"
L2B_C = "AE_OPER_ALD_U_N_2B_20201003T000000_20201003T013000_0005"


def test_product_span_reads_both_name_styles():
    start, stop = product_span("AE_OPER_ALD_U_N_1B_20201002T105035026_005399993_012235_0005")
    assert start == pd.Timestamp("2020-10-02T10:50:35.026")
    assert stop == start + pd.Timedelta(5399993, "ms")
    assert product_span(L2B_A) == (
        pd.Timestamp("2020-10-02T00:00"),
        pd.Timestamp("2020-10-02T01:30"),
    )


def test_read_raw_takes_sources_units_and_the_undeclared_fill(tmp_path):
    path = tmp_path / "v.nc"
    with netCDF4.Dataset(path, "w") as nc:
        nc.history = json.dumps(
            {"inputFiles": [L2B_A], "baselines": ["2B16"], "software_vers": ["03.90"]}
        )
        group = nc.createGroup("sca_dim")
        group.createDimension("sca_dim", 3)
        t = group.createVariable("SCA_time_obs", "f8", ("sca_dim",))
        t[:] = [6.5e8, 9.969209968386869e36, 6.5e8 + 12]
        q = group.createVariable("SCA_QC_flag", "u1", ("sca_dim",))
        q[:] = [1, 2, 3]
    (ds,) = _read_raw(path)
    assert sources(ds) == [(L2B_A, "2B16", "03.90")]
    assert ds["SCA_time_obs"].attrs["_FillValue"] == 9.969209968386869e36
    assert ds["SCA_time_obs"].values[1] == 9.969209968386869e36
    assert ds["SCA_QC_flag"].dtype == np.uint8
    assert "_FillValue" not in ds["SCA_QC_flag"].attrs
    assert xr.decode_cf(ds.assign_attrs(Sources=""))["SCA_time_obs"].dtype.kind == "M"


class FakeVirES:
    """Answers ``_get`` from per-product rows, filtering by time as VirES does."""

    def __init__(self):
        self.calls = []

    def __call__(self, level, group, fields, start, end):
        start, end = pd.Timestamp(start), pd.Timestamp(end)
        self.calls.append((group, start, end))
        epoch = pd.Timestamp("2000-01-01")
        if group in level.whole:
            # Whole products: every one overlapping the window, three groupings each.
            names = [
                n
                for n in (L2B_A, L2B_B, L2B_C)
                if product_span(n)[0] < end and product_span(n)[1] > start
            ]
            ids = np.tile(np.arange(1, 4, dtype="uint16"), len(names))
            ds = xr.Dataset({"mie_grouping_id": ("mie_grouping_data", ids)})
            return ds.assign_attrs(Sources=[(n, "2B16", "03.90") for n in names])
        rows, names = [], []
        for name in (L2B_A, L2B_B, L2B_C):
            s, e = product_span(name)
            times = pd.date_range(s, e, freq="10min", inclusive="left")
            keep = times[(times >= start) & (times < end)]
            if len(keep):
                names.append(name)
                rows += [(t - epoch).total_seconds() for t in keep]
        ds = xr.Dataset({"mie_wind_result_COG_time": ("mie_wind_data", np.array(rows, dtype="f8"))})
        ds["mie_wind_result_COG_time"].attrs["units"] = "seconds since 2000-01-01"
        return ds.assign_attrs(Sources=[(n, "2B16", "03.90") for n in names])


@pytest.fixture
def provider(local_config, monkeypatch):
    made = AeolusProvider(config=local_config, levels=["l2b"], workers=2)
    made._fields = {
        "l2b": {
            "mie_grouping_fields": ["mie_grouping_id"],
            "mie_wind_fields": ["mie_wind_result_COG_time"],
        }
    }
    made._shared = {"l2b": set()}
    fake = FakeVirES()
    monkeypatch.setattr(made, "_get", fake)
    made.fake = fake
    return made


def test_windows_tile_from_product_to_product(provider):
    listed = provider.products(LEVELS["l2b"], pd.Timestamp("2020-10-02"))
    assert listed == [
        (L2B_A, product_span(L2B_B)[0]),
        (L2B_B, product_span(L2B_C)[0]),
    ]


def test_a_day_is_written_once_and_decodes(provider):
    day = pd.Timestamp("2020-10-02")
    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(2) as pool:
        assert provider.run_level_day(LEVELS["l2b"], day, pool) == 2
        assert provider.run_level_day(LEVELS["l2b"], day, pool) == 0
    ds = xr.open_zarr(
        provider.get_icechunk_repo().readonly_session("main").store, consolidated=False
    )
    assert ds["l2b_product_name"].values.tolist() == [L2B_A, L2B_B]
    assert ds["l2b_product_baseline"].values.tolist() == ["2B16", "2B16"]
    times = ds["l2b_mie_wind_result_COG_time"].values
    assert times.dtype.kind == "M" and len(times) == len(set(times.tolist()))
    index = ds["l2b_mie_wind_data_product_index"].values
    assert index[0] == 0 and index[-1] == 1
    assert ds["l2b_mie_grouping_data_product_index"].values.tolist() == [0, 0, 0, 1, 1, 1]
    assert provider.missing_timesteps(pd.DatetimeIndex([day])) == []


def test_a_whole_product_answer_from_another_product_is_refused(provider):
    real = provider._get

    def wrong(level, group, fields, start, end):
        ds = real(level, group, fields, start, end)
        if group in level.whole:
            ds.attrs["Sources"] = [(L2B_B, "2B16", "03.90")]
        return ds

    provider._get = wrong
    with pytest.raises(ValueError, match="came from"):
        provider.fetch_product(LEVELS["l2b"], L2B_A, product_span(L2B_B)[0])


def test_fields_shared_between_groups_take_the_group_prefix(local_config):
    made = AeolusProvider(config=local_config, levels=["l1b"])
    assert "time" in made._shared["l1b"]
    assert "mie_HLOS_wind_speed" in made._shared["l1b"]


def test_days_outside_the_mission_are_never_missing(provider):
    days = pd.DatetimeIndex(["2018-08-01", "2024-01-01"])
    assert provider.missing_timesteps(days) == []


def test_fields_a_product_lacks_are_dropped_and_remembered(local_config, monkeypatch):
    import dataclasses

    import viresclient

    config = dataclasses.replace(
        local_config,
        credentials=dataclasses.replace(local_config.credentials, vires_token="token"),
    )
    made = AeolusProvider(config=config, levels=["l2c"])
    asked = []

    class Request:
        def __init__(self, **kwargs):
            pass

        def set_collection(self, collection):
            pass

        def set_fields(self, **groups):
            self.fields = list(next(iter(groups.values())))

        def get_between(self, *args, **kwargs):
            asked.append(list(self.fields))
            if "mie_tc_23" in self.fields:
                try:
                    raise KeyError("No such field 'mie_tc_23'")
                except KeyError as inner:
                    raise RuntimeError("Server error.") from inner
            raise RuntimeError("stop here")

    monkeypatch.setattr(viresclient, "AeolusRequest", Request)
    monkeypatch.setattr(aeolus, "VIRES_URL", "x")
    level = LEVELS["l2c"]
    with pytest.raises(RuntimeError, match="failed"):
        made._get(level, "mie_wind_fields", ["a", "mie_tc_23"], "2018-09-27", "2018-09-28")
    assert asked[0] == ["a", "mie_tc_23"] and asked[1] == ["a"]
    asked.clear()
    with pytest.raises(RuntimeError):
        made._get(level, "mie_wind_fields", ["a", "mie_tc_23"], "2018-09-28", "2018-09-29")
    assert asked[0] == ["a"]
