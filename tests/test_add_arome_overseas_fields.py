"""The new AROME overseas fields are added to a store and then accept appends."""

from __future__ import annotations

import importlib.util
import pathlib

import icechunk
import numpy as np
import pandas as pd
import xarray as xr
from icechunk.xarray import to_icechunk

spec = importlib.util.spec_from_file_location(
    "add_arome_overseas_fields",
    pathlib.Path(__file__).resolve().parents[1] / "scripts/add_arome_overseas_fields.py",
)
tool = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tool)


def frame(times, fields):
    coords = {
        "time": pd.to_datetime(times),
        "pressure_level": [850.0, 700.0],
        "latitude": [10.0, 11.0],
        "longitude": [20.0],
    }
    shape = (len(times), 2, 2, 1)
    return xr.Dataset(
        {name: (tuple(coords), np.full(shape, 1.5, dtype)) for name, dtype in fields.items()},
        coords=coords,
    )


def test_fields_are_added_empty_and_later_partitions_fill_them(tmp_path):
    repo = icechunk.Repository.create(icechunk.local_filesystem_storage(str(tmp_path)))
    session = repo.writable_session("main")
    to_icechunk(frame(["2026-10-29T00"], {"vorticity": "float32"}), session)
    session.commit("existing store")

    assert tool.add_fields(repo) == list(tool.FIELDS)
    assert tool.add_fields(repo) == []

    fields = {"vorticity": "float32", **{n: d for n, (d, _) in tool.FIELDS.items()}}
    session = repo.writable_session("main")
    to_icechunk(frame(["2026-10-29T01"], fields), session, append_dim="time")
    session.commit("new partition")
    ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    assert np.isnan(ds["divergence"].isel(time=0)).all()
    assert (ds["divergence"].isel(time=1) == 1.5).all()
    assert ds["pseudo_adiabatic_potential_temperature"].dtype == np.float16
    assert ds["divergence"].attrs["long_name"] == "Divergence"
