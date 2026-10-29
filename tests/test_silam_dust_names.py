"""SILAM dust names come from each variable's own source metadata."""

from __future__ import annotations

import importlib.util
import pathlib

spec = importlib.util.spec_from_file_location(
    "silam_dust_names",
    pathlib.Path(__file__).resolve().parents[1] / "scripts/silam_dust_names.py",
)
names = importlib.util.module_from_spec(spec)
spec.loader.exec_module(names)


def bin_attrs(low: str, high: str, **extra) -> dict:
    return {
        "substance_name": "dust",
        "fix_diam_mode_min_diameter": f"{low}            um",
        "fix_diam_mode_max_diameter": f"{high}           um",
        **extra,
    }


def test_binned_quantities_name_their_diameter_range_and_wavelength():
    conc = bin_attrs("2.5000000", "10.0000000", long_name="Concentration in air dust_m6_0")
    assert names.name_for("cnc_dust_m6_0", conc) == "dust_aerosol_2_5_10um_concentration"
    dry = bin_attrs("0.0100000", "1.0000000", long_name="Cocktail dry deposition dust_m_30")
    assert names.name_for("dd_dust_m_30", dry) == "dust_aerosol_0_01_1um_dry_deposition_rate"
    aod = bin_attrs(
        "10.0000000",
        "30.0000000",
        long_name="optical column depth dust_m20_w550",
        optical_wavelength="0.5500000            um",
    )
    assert names.name_for("ocd_dust_m20_w550", aod) == "dust_aerosol_10_30um_optical_depth_550nm"


def test_totals_and_fixed_names():
    total = {"long_name": "optical column depth dust @ 550nm", "mode_distribution_type": "NO_MODE"}
    assert names.name_for("ocd_dust_w550", total) == "dust_aerosol_optical_depth_550nm"
    assert names.name_for("wd_dust", {"long_name": "Wet deposition rate dust"}) == (
        "dust_aerosol_wet_deposition_rate"
    )
    assert names.name_for("air_dens", {}) == "air_density"
    assert names.name_for("mystery", {"long_name": "something else"}) is None
