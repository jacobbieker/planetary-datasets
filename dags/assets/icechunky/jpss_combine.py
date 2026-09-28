"""Print the combined JPSS ATMS zarr stores, for checking what is on disk.

A one-off script, not a Dagster asset module. The body used to run at module scope, so
`dags/loader.py` -- which imports every module under `dags/assets/` to discover assets --
opened every store each time the code location loaded.
"""

import xarray as xr


def main() -> None:
    ds = xr.open_mfdataset(
        "/home/jacob/Elements/jpss_atms*.zarr",
        engine="zarr",
        concat_dim="time",
        combine="nested",
    )
    print(ds)


if __name__ == "__main__":
    main()
