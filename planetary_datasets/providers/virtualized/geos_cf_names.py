"""Explicit, ERA5-style variable names for the GEOS-CF collections.

GEOS-CF names its fields with GEOS-Chem species codes (``NO2``, ``PM25su_RH35``,
``EMIS_CO_ANTH``) and GEOS meteorology codes (``T``, ``U``, ``ZPBL``). The virtual stores
name them the way ARCO-ERA5 does (see :mod:`planetary_datasets.common.naming`): lower snake
case, the quantity spelled out, meteorology under ERA5's own names, and every chemical
named for the species *and* the quantity, so ``NO2`` is ``nitrogen_dioxide_mole_fraction``
and ``TropCol_NO2`` is ``tropospheric_column_nitrogen_dioxide``.

Names are built from the code, not from the long names: the long names are inconsistent
(``EMIS_CO_ANTH`` is described as a *ship* emission) and truncated in places. The code's
parts are each looked up: :data:`SPECIES` for the GEOS-Chem species, :data:`EMISSION_SECTORS`
for an emission's source, :data:`AEROSOLS` for the aerosol optical depth and PM components,
and :data:`METEOROLOGY` for everything that is not chemistry. A code none of them covers
falls back to its long name through :func:`planetary_datasets.common.naming.era5ify`, and
:func:`variable_name` says which rule named it, so unknown codes can be reviewed.
"""

from __future__ import annotations

import re
from typing import Mapping

from planetary_datasets.common.naming import era5ify

#: GEOS-Chem species code -> what the species is.
SPECIES: dict[str, str] = {
    "ACET": "acetone",
    "ACTA": "acetic_acid",
    "ALD2": "acetaldehyde",
    "ALK4": "lumped_c4_and_higher_alkanes",
    "BCPI": "hydrophilic_black_carbon",
    "BCPO": "hydrophobic_black_carbon",
    "BENZ": "benzene",
    "Br": "atomic_bromine",
    "Br2": "molecular_bromine",
    "BrCl": "bromine_chloride",
    "BrNO2": "nitryl_bromide",
    "BrNO3": "bromine_nitrate",
    "BrO": "bromine_monoxide",
    "Bry": "total_inorganic_bromine",
    "C2H6": "ethane",
    "C3H8": "propane",
    "CCl4": "carbon_tetrachloride",
    "CFC11": "cfc_11",
    "CFC113": "cfc_113",
    "CFC114": "cfc_114",
    "CFC115": "cfc_115",
    "CFC12": "cfc_12",
    "CH2Br2": "dibromomethane",
    "CH2I2": "diiodomethane",
    "CH2IBr": "bromoiodomethane",
    "CH2ICl": "chloroiodomethane",
    "CH2O": "formaldehyde",
    "CH3Br": "bromomethane",
    "CH3CCl3": "methyl_chloroform",
    "CH3Cl": "chloromethane",
    "CH3I": "methyl_iodide",
    "CH4": "methane",
    "CHBr3": "bromoform",
    "CO": "carbon_monoxide",
    "CO2": "carbon_dioxide",
    "Cl": "atomic_chlorine",
    "Cl2": "molecular_chlorine",
    "Cl2O2": "dichlorine_dioxide",
    "ClNO2": "nitryl_chloride",
    "ClNO3": "chlorine_nitrate",
    "ClO": "chlorine_monoxide",
    "ClONO2": "chlorine_nitrate",
    "ClOO": "chlorine_peroxide_radical",
    "Cly": "total_inorganic_chlorine",
    "DMS": "dimethyl_sulfide",
    "DST1": "dust_0_7um",
    "DST2": "dust_1_4um",
    "DST3": "dust_2_4um",
    "DST4": "dust_4_5um",
    "EOH": "ethanol",
    "GLYX": "glyoxal",
    "H1211": "halon_1211",
    "H1301": "halon_1301",
    "H2402": "halon_2402",
    "H2O": "water_vapour",
    "H2O2": "hydrogen_peroxide",
    "HBr": "hydrogen_bromide",
    "HCFC123": "hcfc_123",
    "HCFC141b": "hcfc_141b",
    "HCFC142b": "hcfc_142b",
    "HCFC22": "hcfc_22",
    "HCHO": "formaldehyde",
    "HCOOH": "formic_acid",
    "HCl": "hydrochloric_acid",
    "HI": "hydrogen_iodide",
    "HNO2": "nitrous_acid",
    "HNO3": "nitric_acid",
    "HNO4": "peroxynitric_acid",
    "HO2": "hydroperoxyl_radical",
    "HOBr": "hypobromous_acid",
    "HOCl": "hypochlorous_acid",
    "HOI": "hypoiodous_acid",
    "I": "atomic_iodine",
    "I2": "molecular_iodine",
    "IO": "iodine_monoxide",
    "ISOP": "isoprene",
    "LIMO": "limonene",
    "MACR": "methacrolein",
    "MEK": "methyl_ethyl_ketone",
    "MOH": "methanol",
    "MTPA": "lumped_monoterpenes_pinenes_sabinene_carene",
    "MTPO": "other_monoterpenes",
    "MVK": "methyl_vinyl_ketone",
    "N2O": "nitrous_oxide",
    "N2O5": "dinitrogen_pentoxide",
    "NH3": "ammonia",
    "NH4": "ammonium",
    "NIT": "inorganic_nitrates",
    "NO": "nitric_oxide",
    "NO2": "nitrogen_dioxide",
    "NO3": "nitrate_radical",
    "NOx": "nitrogen_oxides",
    "NOy": "total_reactive_oxidized_nitrogen",
    "O1D": "excited_atomic_oxygen_1d",
    "O3": "ozone",
    "O3P": "ground_state_atomic_oxygen_3p",
    "OCPI": "hydrophilic_organic_carbon",
    "OCPO": "hydrophobic_organic_carbon",
    "OClO": "chlorine_dioxide",
    "OCS": "carbonyl_sulfide",
    "OH": "hydroxyl_radical",
    "PAN": "peroxyacetyl_nitrate",
    "PRPE": "lumped_c3_and_higher_alkenes",
    "RCHO": "lumped_c3_and_higher_aldehydes",
    "SALA": "fine_sea_salt_0_01_0_5um",
    "SALC": "coarse_sea_salt_0_5_8um",
    "SESQ": "sesquiterpenes",
    "SO2": "sulphur_dioxide",
    "SO4": "sulphate",
    "SOAP": "secondary_organic_aerosol_precursor",
    "SOAS": "simple_secondary_organic_aerosol",
    "TOLU": "toluene",
    "XYLE": "xylene",
    "pFe": "anthropogenic_iron",
}

#: Emission sector suffix -> source, for ``EMIS_<species>_<sector>``. No suffix is the total.
EMISSION_SECTORS: dict[str, str] = {
    "": "total",
    "ANTH": "anthropogenic",
    "AGR": "agricultural",
    "AIR": "aircraft",
    "BB": "biomass_burning",
    "BG": "biogenic",
    "LGHT": "lightning",
    "OC": "ocean",
    "SHIP": "ship",
    "SOIL": "soil",
    "VOLC": "volcanic",
}

#: Aerosol code (``AOD550_<code>``, ``PM25<code>_RH35``) -> aerosol, in CAMS's words.
AEROSOLS: dict[str, str] = {
    "": "total",
    "BC": "black_carbon",
    "bc": "black_carbon",
    "DUST": "dust",
    "DST": "dust",
    "du": "dust",
    "OC": "organic_matter",
    "oc": "primary_organic",
    "soa": "secondary_organic",
    "PSC": "polar_stratospheric_cloud",
    "SLA": "stratospheric_liquid",
    "SNA": "sulphate_nitrate_ammonium",
    "nh4": "ammonium",
    "nit": "nitrate",
    "SS": "sea_salt",
    "ss": "sea_salt",
    "SU": "sulphate",
    "su": "sulphate",
    "CLOUD": "cloud",
}

#: Meteorology and other non-species codes -> ARCO-ERA5 name, or an explicit one where ERA5
#: has no equivalent. Upper case as GEOS writes them.
METEOROLOGY: dict[str, str] = {
    "T": "temperature",
    "U": "u_component_of_wind",
    "V": "v_component_of_wind",
    "Q": "specific_humidity",
    "RH": "relative_humidity",
    "OMEGA": "vertical_velocity",
    "EPV": "potential_vorticity",
    "TH": "potential_temperature",
    "H": "layer_edge_height",
    "ZL": "layer_mid_height",
    "PL": "layer_mid_pressure",
    "DELP": "layer_pressure_thickness",
    "AIRDENS": "air_density",
    "AIRDENS_DRYP": "dry_air_partial_density",
    "QCTOT": "total_cloud_water_mass_fraction",
    "QW": "total_water_mass_mixing_ratio",
    "CLOUD": "cloud_area_fraction_in_layer",
    "PS": "surface_pressure",
    "SLP": "mean_sea_level_pressure",
    "PHIS": "surface_geopotential",
    "TS": "skin_temperature",
    "T2M": "2m_temperature",
    "T10M": "10m_temperature",
    "Q2M": "2m_specific_humidity",
    "Q10M": "10m_specific_humidity",
    "U2M": "2m_u_component_of_wind",
    "V2M": "2m_v_component_of_wind",
    "U10M": "10m_u_component_of_wind",
    "V10M": "10m_v_component_of_wind",
    "ZPBL": "boundary_layer_height",
    "PPBL": "boundary_layer_top_pressure",
    "CLDTT": "total_cloud_cover",
    "TPREC": "total_precipitation_rate",
    "PRECTOT": "total_precipitation_rate",
    "PRECCON": "convective_precipitation_rate",
    "PRECLSC": "large_scale_precipitation_rate",
    "PRECSNO": "snowfall_rate",
    "COSZ": "cosine_of_solar_zenith_angle",
    "FRSEAICE": "sea_ice_cover",
    "FRSNO": "snow_cover_fraction",
    "FRLAND": "land_fraction",
    "FROCEAN": "ocean_fraction",
    "FRLANDICE": "land_ice_fraction",
    "SNODP": "snow_depth",
    "SNOMAS": "snow_mass",
    "TROPPB": "tropopause_pressure_blended",
    "TROPPT": "tropopause_pressure_thermal",
    "TROPPV": "tropopause_pressure_potential_vorticity",
    "TROPT": "tropopause_temperature",
    "FLASHRATE": "lightning_flash_rate",
    "SWGDN": "surface_downward_shortwave_radiation_flux",
    "LWGNT": "surface_net_longwave_radiation_flux",
    "HFLUX": "surface_sensible_heat_flux",
    "EFLUX": "surface_latent_heat_flux",
    "USTAR": "friction_velocity",
    "Z0H": "surface_roughness_for_heat",
    "Z0M": "surface_roughness_for_momentum",
    "OH_reactivity": "hydroxyl_radical_reactivity",
    "AerSurfAreaPolarStratCloud": "polar_stratospheric_cloud_surface_area_density",
    "AerSurfAreaStratLiquid": "stratospheric_liquid_aerosol_surface_area_density",
    # Net surface-atmosphere fluxes, which the ghg collection files under EMIS_ codes.
    "EMIS_CH4": "methane_net_surface_flux",
    "EMIS_CO2": "carbon_dioxide_net_surface_flux",
    "XCH4": "column_averaged_methane_mole_fraction",
    "XCO2": "column_averaged_carbon_dioxide_mole_fraction",
}

_EMISSION = re.compile(r"^EMIS_(?P<species>[A-Za-z0-9]+?)(?:_(?P<sector>[A-Z]+))?$")
_COLUMN = re.compile(r"^(?P<kind>TotCol|TropCol|PBLCol)_(?P<species>[A-Za-z0-9]+)$")
_AOD = re.compile(r"^AOD550(?:_(?P<aerosol>[A-Za-z]+))?$")
_PM = re.compile(r"^PM(?P<size>25|10)(?P<aerosol>[a-z0-9]*)_RH35(?:_(?P<scheme>GCC|GOCART))?$")
_COLUMN_KINDS = {
    "TotCol": "total_column",
    "TropCol": "tropospheric_column",
    "PBLCol": "boundary_layer_column",
}
_PM_SIZES = {"25": "2_5um", "10": "10um"}
_SCHEMES = {"GCC": "geos_chem", "GOCART": "gocart"}


def _species(code: str) -> str | None:
    return SPECIES.get(code) or SPECIES.get(code.upper())


def variable_name(code: str, attrs: Mapping | None = None) -> tuple[str, str]:
    """The store name of a GEOS-CF variable, and the rule that named it.

    The rule is ``meteorology``, ``species``, ``emission``, ``column``, ``aod``, ``pm`` or,
    for a code nothing here covers, ``long_name``/``kept``.

    Args:
        code: The variable's name in the GEOS-CF file.
        attrs: Its attributes; only the long name and units are used, for the fallback and
            to tell a mole fraction from another quantity.
    """
    attrs = attrs or {}
    units = str(attrs.get("units", "")).strip()
    if code in METEOROLOGY:
        return METEOROLOGY[code], "meteorology"
    if m := _EMISSION.match(code):
        species, sector = m["species"], m["sector"] or ""
        if sector not in EMISSION_SECTORS:
            # A species code with an underscore of its own, e.g. none known yet.
            species, sector = f"{species}_{sector}", ""
        name = _species(species)
        if name:
            return f"{name}_{EMISSION_SECTORS[sector]}_emission_flux", "emission"
    if m := _COLUMN.match(code):
        name = _species(m["species"])
        if name:
            return f"{_COLUMN_KINDS[m['kind']]}_{name}", "column"
    if m := _AOD.match(code):
        aerosol = AEROSOLS.get(m["aerosol"] or "")
        if aerosol:
            return f"{aerosol}_aerosol_optical_depth_550nm", "aod"
    if m := _PM.match(code):
        aerosol = AEROSOLS.get(m["aerosol"]) if m["aerosol"] else ""
        if aerosol is not None:
            parts = [aerosol, "particulate_matter", _PM_SIZES[m["size"]], "rh35"]
            if m["scheme"]:
                parts.append(_SCHEMES[m["scheme"]])
            return "_".join(p for p in parts if p), "pm"
    name = _species(code)
    if name:
        if units.startswith("mol mol-1") or "mole fraction" in str(attrs.get("long_name", "")):
            return f"{name}_mole_fraction", "species"
        if units.startswith("kg kg-1"):
            return f"{name}_mass_mixing_ratio", "species"
        if units.startswith(("ug m-3", "kg m-3")):
            return f"{name}_mass_concentration", "species"
        return name, "species"
    long_name = attrs.get("long_name")
    if long_name:
        return era5ify(str(long_name)), "long_name"
    return code, "kept"


def variable_names(variables: Mapping[str, Mapping]) -> dict[str, str]:
    """GEOS-CF code -> store name for every data variable of one collection.

    Raises:
        ValueError: when two codes would get the same name, which would silently drop one.
    """
    names = {code: variable_name(code, attrs)[0] for code, attrs in variables.items()}
    seen: dict[str, str] = {}
    for code, name in names.items():
        if name in seen:
            raise ValueError(f"GEOS-CF codes {seen[name]!r} and {code!r} both name {name!r}")
        seen[name] = code
    return names
