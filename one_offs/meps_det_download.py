import datetime as dt
import os

import icechunk
import pandas as pd
import xarray as xr
import zarr.codecs
from icechunk.xarray import to_icechunk

STORE_PATH = "meps_det_ml.icechunk"

FLOAT16_SKIP = {"b", "specific_humidity_ml", "turbulent_kinetic_energy_ml"}


def opendap_url(date: dt.datetime) -> str:
    """https://thredds.met.no/thredds/catalog/meps25epsarchive/2026/04/01/catalog.html?dataset=meps25epsarchive/2026/04/01/meps_det_2_5km_20260401T00Z.nc"""
    return (
        f"https://thredds.met.no/thredds/dodsC/meps25epsarchive/{date.strftime('%Y')}/{date.strftime('%m')}/{date.strftime('%d')}/"
        f"meps_det_2_5km_{date.strftime('%Y%m%d')}T{date.strftime('%H')}Z.nc"
    )


def open_ml(date: dt.datetime) -> xr.Dataset:
    url = opendap_url(date)
    ds = xr.open_dataset(url, engine="netcdf4")
    ds = ds[[v for v in ds.data_vars if "hybrid" in ds[v].coords]]
    ds = ds.isel(time=[0, 1, 2]).compute()
    for var in ds.data_vars:
        if var not in FLOAT16_SKIP:
            ds[var] = ds[var].astype("float16")
    return ds


def get_repo() -> icechunk.Repository:
    storage = icechunk.s3_storage(
        bucket="us-west-2.opendata.source.coop",
        prefix="bkr/dmi/meps_model_level.icechunk",
        access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region="us-west-2",
    )
    # storage = icechunk.local_filesystem_storage(store_path)
    repo = icechunk.Repository.open_or_create(storage, config=icechunk.RepositoryConfig.default())
    return repo


def existing_times(repo: icechunk.Repository) -> set:
    try:
        return set(
            xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
            .coords["time"]
            .values
        )
    except Exception:
        return set()


def write(ds: xr.Dataset, repo: icechunk.Repository, first_write: bool):
    session = repo.writable_session("main")
    ds = ds.chunk({"time": 1, "hybrid": -1, "x": -1, "y": -1})
    if first_write:
        encoding = {
            v: {
                "compressors": zarr.codecs.BloscCodec(
                    cname="zstd", clevel=9, shuffle=zarr.codecs.BloscShuffle.bitshuffle
                )
            }
            for v in ds.data_vars
        }
        encoding["time"] = {
            "units": "seconds since 1970-01-01",
            "calendar": "standard",
            "dtype": "int64",
        }
        to_icechunk(ds, session, encoding=encoding)
    else:
        to_icechunk(ds, session, append_dim="time")
    session.commit(f"MEPS det ML: {ds.time.values[0]}")


if __name__ == "__main__":
    repo = get_repo()
    times = existing_times(repo)
    first_write = len(times) == 0

    date_range = pd.date_range(start="2026-04-01", end=dt.datetime.utcnow(), freq="3h")

    for date in date_range:
        if pd.Timestamp(date).to_datetime64() in times:
            print(f"Already have {date}, skipping")
            continue

        print(f"Processing {date} ...")
        try:
            ds = open_ml(date)
            ds = ds.compute()
            write(ds, repo, first_write)
            first_write = False
            print(f"  Written {date}")
        except Exception as e:
            print(f"  Failed {date}: {e}")
            continue
