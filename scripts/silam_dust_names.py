"""Readable names for the SILAM dust store, built from a source file's own metadata.

    python scripts/silam_dust_names.py SOURCE.nc4 [--apply]

The store (bkr/silam-dust/silam_global_dust.icechunk) was created without the source's
variable attributes, so the naming pass had nothing to read and kept SILAM's codes
(``cnc_dust_m1_5``). The files do describe every variable: the quantity in its long name
("Concentration in air", "Cocktail dry deposition", "optical column depth"), the size bin in
``fix_diam_mode_min_diameter``/``..._max_diameter`` and the wavelength in
``optical_wavelength``. Names follow the CAMS convention (``dust_aerosol_optical_depth_550nm``,
``dust_aerosol_1_2_5um_concentration``).

Without ``--apply`` this prints the names and attributes. With it, the store's arrays get
the source's attributes back (one metadata commit) and the names are written to
``planetary_datasets/naming/store_names.json``; run ``scripts/rename_store_variables.py``
for the store afterwards to move the arrays.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import sys

PREFIX = "bkr/silam-dust/silam_global_dust.icechunk"
#: Variables whose meaning is in their long name alone.
FIXED = {
    "air_dens": "air_density",
    "dust_column": "total_column_dust_aerosol",
    "vrt_dep_srf_pr": "surface_pressure",
}
QUANTITIES = {
    "Concentration in air": "concentration",
    "Cocktail dry deposition": "dry_deposition_rate",
    "Dry deposition rate": "dry_deposition_rate",
    "Cocktail wet deposition": "wet_deposition_rate",
    "Wet deposition rate": "wet_deposition_rate",
    "optical column depth": "optical_depth",
}
#: Source attributes worth keeping on the stored arrays.
KEEP = (
    "long_name",
    "units",
    "cell_methods",
    "substance_name",
    "mode_distribution_type",
    "mode_nominal_diameter",
    "fix_diam_mode_min_diameter",
    "fix_diam_mode_max_diameter",
    "fix_diam_mode_mean_diameter",
    "optical_wavelength",
    "components",
)


def _micrometres(text: str) -> float:
    return float(str(text).split()[0])


def _number(value: float) -> str:
    """1.0 -> '1', 2.5 -> '2_5', 0.01 -> '0_01'."""
    return f"{value:g}".replace(".", "_")


def name_for(source: str, attrs: dict) -> str | None:
    """The readable name of a SILAM dust variable, or None if its metadata says too little."""
    if source in FIXED:
        return FIXED[source]
    long_name = str(attrs.get("long_name", ""))
    quantity = next((q for text, q in QUANTITIES.items() if long_name.startswith(text)), None)
    substance = attrs.get("substance_name") or ("dust" if "dust" in long_name else None)
    if quantity is None or substance != "dust":
        return None
    parts = ["dust_aerosol"]
    if "fix_diam_mode_min_diameter" in attrs and "fix_diam_mode_max_diameter" in attrs:
        low = _micrometres(attrs["fix_diam_mode_min_diameter"])
        high = _micrometres(attrs["fix_diam_mode_max_diameter"])
        parts.append(f"{_number(low)}_{_number(high)}um")
    parts.append(quantity)
    if quantity == "optical_depth":
        if "optical_wavelength" in attrs:
            nm = round(_micrometres(attrs["optical_wavelength"]) * 1000)
        else:
            match = re.search(r"(\d+)\s*nm", long_name)
            if not match:
                return None
            nm = int(match[1])
        parts.append(f"{nm}nm")
    return "_".join(parts)


def plan(source_file: str) -> tuple[dict[str, str], dict[str, dict]]:
    """Names and attributes for every data variable in a source file."""
    import xarray as xr

    with xr.open_dataset(source_file) as ds:
        names, attrs = {}, {}
        for variable in ds.data_vars:
            kept = {k: v for k, v in ds[variable].attrs.items() if k in KEEP and v != ""}
            attrs[variable] = kept
            name = name_for(variable, ds[variable].attrs)
            if name:
                names[variable] = name
    if len(set(names.values())) != len(names):
        raise ValueError(f"names collide: {names}")
    return names, attrs


def restore_attributes(attrs: dict[str, dict]) -> int:
    """Give the stored arrays their source attributes back, in one commit."""
    import zarr

    from planetary_datasets.config import get_config

    repo = get_config().icechunk_repo(PREFIX)
    session = repo.writable_session("main")
    group = zarr.open_group(session.store, mode="a")
    touched = 0
    for name, kept in attrs.items():
        if name in group and kept:
            group[name].attrs.update({k: v for k, v in kept.items()})
            touched += 1
    if touched:
        session.commit(f"silam dust: source attributes restored on {touched} array(s)")
    return touched


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("source")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    names, attrs = plan(args.source)
    print(json.dumps(names, indent=1, sort_keys=True))
    if not args.apply:
        return 0
    print(f"restored attributes on {restore_attributes(attrs)} array(s)")
    table = (
        pathlib.Path(__file__).resolve().parents[1] / "planetary_datasets/naming/store_names.json"
    )
    stored = json.loads(table.read_text())
    stored[PREFIX] = dict(sorted(names.items()))
    table.write_text(json.dumps(stored, indent=1, sort_keys=True) + "\n")
    print(f"wrote {len(names)} name(s) to {table}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
