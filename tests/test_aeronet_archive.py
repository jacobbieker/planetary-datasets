"""AERONET archive stores: parsing, naming, the ragged layout and its de-duplication.

No network: the files and web responses are small fakes in AERONET's formats.
"""

from __future__ import annotations

import io
import re
import tarfile

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from planetary_datasets.providers.observations import aeronet_archive as aeronet
from planetary_datasets.providers.observations.aeronet_archive import (
    PRODUCTS,
    AeronetStore,
    column_name,
    parse_table,
)

HEAD = [
    "AERONET Version 3",
    "{site}",
    "Version 3: AOD Level 2.0",
    "The following data are automatically cloud cleared and quality assured.",
    "Contact: PI=Jane Doe; PI Email=jane@example.org",
    "All Points,UNITS can be found at,,, https://aeronet.gsfc.nasa.gov/new_web/units.html",
]
COLUMNS = (
    "Date(dd:mm:yyyy),Time(hh:mm:ss),Day_of_Year,Day_of_Year(Fraction),AOD_500nm,AOD_Empty,"
    "AOD_Empty,Precipitable_Water(cm),440-870_Angstrom_Exponent,N[AOD_500nm],Data_Quality_Level"
)


def aod_file(site: str, rows: list[str]) -> str:
    return "\n".join([*(h.format(site=site) for h in HEAD), COLUMNS, *rows]) + "\n"


ROWS = [
    "21:06:1993,13:31:51,172,172.563785,0.065048,-999.,-999.,1.989356,0.727492,12,lev20",
    "21:06:1993,13:46:51,172,172.574201,0.070000,-999.,-999.,2.001000,-999.000000,0,lev20",
]


@pytest.fixture
def sites() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "New_Site_ID": ["SAM0001BRA", "NAM0002USA"],
            "Latitude(decimal_degrees)": ["-15.555244", "32.233002"],
            "Longitude(decimal_degrees)": ["-56.070214", "-110.953003"],
            "Altitude(Meters)": ["234.000000", "779.000000"],
            "Land_Use_type": ["Urban_and_Built-Up", "Open_Shrublands"],
        },
        index=pd.Index(["Cuiaba", "Tucson"], name="Name"),
    )


# --- names --------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("column", "name", "units"),
    [
        ("AOD_1640nm", "aerosol_optical_depth_1640nm", None),
        ("440-870_Angstrom_Exponent", "angstrom_exponent_440_870nm", None),
        ("440-675_Angstrom_Exponent[Polar]", "angstrom_exponent_440_675nm_polar", None),
        ("Precipitable_Water(cm)", "total_column_water_vapour", "cm"),
        ("NUM_DAYS[AOD_500nm]", "number_of_days_aerosol_optical_depth_500nm", None),
        (
            "N[Single_Scattering_Albedo[440nm]]",
            "number_of_points_single_scattering_albedo_440nm",
            None,
        ),
        ("Total_AOD_500nm[tau_a]", "total_aerosol_optical_depth_500nm", None),
        ("FineModeFraction_500nm[eta]", "fine_mode_fraction_500nm", None),
        ("VolC-C", "volume_concentration_coarse", None),
        ("Lwn_f/Q[412nm]", "normalized_water_leaving_radiance_f_over_q_412nm", None),
        ("0.050000", "volume_size_distribution_at_0_05um", "um3 um-2"),
        ("3.000000[440nm]-Total", "phase_function_total_440nm_at_3_degrees", None),
        (
            "Exact_Wavelengths_of_AOD(um)_1640nm",
            "exact_wavelengths_of_aerosol_optical_depth_1640nm",
            "um",
        ),
        ("Site_Latitude(Degrees)", "latitude", "degrees"),
    ],
)
def test_columns_get_readable_names_and_units(column, name, units):
    assert column_name(column) == (name, units)


def test_only_all_points_products_each_a_group():
    assert len(PRODUCTS) == 27
    assert aeronet.STORE_PREFIX == "bkr/obs/aeronet.icechunk"
    assert not [k for k in PRODUCTS if re.search("daily|monthly|all_points", k)]
    assert all("All_Points" in p.bulk for p in PRODUCTS.values() if p.bulk)
    assert all(q.count("AVG=10") for p in PRODUCTS.values() for q in p.web)
    assert PRODUCTS["aod_level20"].bulk == "AOD/AOD_Level20_All_Points_V3.tar.gz"
    assert PRODUCTS["aod_level10"].bulk is None
    assert PRODUCTS["tot_level15"].web == ("TOT15=1&AVG=10",)
    assert "inversion_hybrid_phase_functions_polarized_level15" in PRODUCTS
    assert PRODUCTS["ocean_colour_level20"].bulk == "LWN/LWN_Level20_All_Points_V3.tar.gz"
    pfn = PRODUCTS["inversion_hybrid_phase_functions_level20"]
    assert pfn.inversion and len(pfn.web) == 3
    assert PRODUCTS["inversion_almucantar_level20"].bulk == ("INV/INV_Level2_All_Points_V3.tar.gz")


# --- parsing ------------------------------------------------------------------------------


def test_a_bulk_file_parses_into_typed_columns():
    table = parse_table(aod_file("Cuiaba", ROWS))
    rows = table.rows
    assert table.site == "Cuiaba"
    assert table.principal_investigators == "PI=Jane Doe; PI Email=jane@example.org"
    assert rows["time"].tolist() == [
        pd.Timestamp("1993-06-21 13:31:51"),
        pd.Timestamp("1993-06-21 13:46:51"),
    ]
    # Six published decimals survive float32; the fraction of a day needs float64.
    assert rows["aerosol_optical_depth_500nm"].dtype == np.float32
    assert rows["day_of_year_fraction"].dtype == np.float64
    assert rows["number_of_points_aerosol_optical_depth_500nm"].dtype == np.int32
    assert rows["data_quality_level"].tolist() == [20, 20]
    assert {"aerosol_optical_depth_empty", "aerosol_optical_depth_empty_2"} <= set(rows)
    attrs = table.attrs["total_column_water_vapour"]
    assert attrs["scale_factor"] == 10.0 and attrs["units"] == "kg m-2"
    assert attrs["source_units"] == "cm" and attrs["_FillValue"] == -999.0
    assert table.attrs["data_quality_level"]["flag_meanings"] == "lev10 lev15 lev20"


def test_float32_only_when_it_reprints_the_published_text():
    rows = ROWS[:1] + ["21:06:1993,13:46:51,172,172.574201,123.456789,-999.,-999.,2.0,1.0,1,lev20"]
    table = parse_table(aod_file("Cuiaba", rows))
    assert table.rows["aerosol_optical_depth_500nm"].dtype == np.float64
    assert table.rows["aerosol_optical_depth_500nm"].iloc[1] == 123.456789


def test_ocean_colour_dates_and_site_column():
    ocean = "\n".join(
        [
            *(h.format(site="Venise") for h in HEAD),
            "Date(dd-mm-yyyy),Time(hh:mm:ss),Lw[412nm],AERONET_Site_Name,Last_Date_Processed",
            "20:04:2002,12:00:00,0.614370,Venise,10:05:2024,",
        ]
    )
    table = parse_table(ocean)
    assert table.rows["time"].iloc[0] == pd.Timestamp("2002-04-20 12:00")
    assert table.rows["last_processing_time"].iloc[0] == pd.Timestamp("2024-05-10")
    assert table.rows["site_name"].tolist() == ["Venise"]
    assert table.site is None


def test_a_value_outside_a_vocabulary_is_refused():
    with pytest.raises(ValueError, match="vocabulary"):
        parse_table(aod_file("Cuiaba", [ROWS[0].replace("lev20", "lev99")]))


# --- stores -------------------------------------------------------------------------------


def stored(store: AeronetStore) -> xr.Dataset:
    return xr.open_zarr(
        store.repo.readonly_session("main").store, group=store.path, consolidated=False
    )


def test_rows_append_once_and_decode(local_config, sites):
    product = PRODUCTS["aod_level20"]
    store = AeronetStore(product, local_config, sites)
    assert store.append([parse_table(aod_file("Cuiaba", ROWS))]) == 2
    # The same rows again, and one new row from a new site, through a fresh store object.
    tucson = ["01:07:1994,10:00:00,182,182.416667,0.2,-999.,-999.,1.5,1.2,4,lev20"]
    again = AeronetStore(product, local_config, sites)
    tables = [parse_table(aod_file("Cuiaba", ROWS)), parse_table(aod_file("Tucson", tucson))]
    assert again.append(tables) == 1

    ds = stored(store)
    assert ds.sizes == {"observation": 3, "site": 2}
    assert ds["site_name"].values.tolist() == ["Cuiaba", "Tucson"]
    assert ds["site_index"].values.tolist() == [0, 0, 1]
    assert ds["site_latitude"].values.tolist() == [-15.555244, 32.233002]
    assert ds["principal_investigators"].values[0].startswith("PI=Jane Doe")
    assert ds["time"].values[2] == np.datetime64("1994-07-01T10:00:00")
    # -999 reads as missing; precipitable water decodes in ERA5's kg m-2.
    assert np.isnan(ds["angstrom_exponent_440_870nm"].values[1])
    assert ds["total_column_water_vapour"].values[0] == pytest.approx(19.89356, rel=1e-6)
    raw = xr.open_zarr(
        store.repo.readonly_session("main").store,
        group=store.path,
        consolidated=False,
        mask_and_scale=False,
    )
    assert raw["total_column_water_vapour"].values[0] == pytest.approx(1.989356)
    assert raw["aerosol_optical_depth_500nm"].values[0] == np.float32(0.065048)


def test_a_web_response_shares_the_bulk_store(local_config, sites):
    product = PRODUCTS["aod_level20"]
    AeronetStore(product, local_config, sites).append([parse_table(aod_file("Cuiaba", ROWS))])
    web = "\n".join(
        [
            "AERONET Data Download (Version 3 Direct Sun)",
            *HEAD[0:1],
            *HEAD[2:4],
            "Contact: Please visit https://aeronet.gsfc.nasa.gov/new_web/data_usage.html",
            "AERONET_Site," + COLUMNS,
            "Cuiaba," + ROWS[1],
            "Tucson,02:07:1994,10:00:00,183,183.416667,0.3,-999.,-999.,1.4,1.1,5,lev20",
        ]
    )
    store = AeronetStore(product, local_config, sites)
    assert store.append([parse_table(web)]) == 1
    assert stored(store)["site_index"].values.tolist() == [0, 0, 1]


def test_phase_function_modes_are_distinct_rows(local_config, sites):
    columns = "Site,Date(dd:mm:yyyy),Time(hh:mm:ss),3.000000[440nm]-Total,Phase_Function_Mode"
    text = "\n".join(
        [
            *(h.format(site="Cuiaba") for h in HEAD),
            columns,
            "Cuiaba,20:06:1993,18:58:26,0.806450,Total",
            "Cuiaba,20:06:1993,18:58:26,0.400000,Fine",
        ]
    )
    product = PRODUCTS["inversion_almucantar_phase_functions_level20"]
    store = AeronetStore(product, local_config, sites)
    assert store.append([parse_table(text)]) == 2
    assert AeronetStore(product, local_config, sites).append([parse_table(text)]) == 0
    assert stored(store)["phase_function_mode"].values.tolist() == [0, 1]


# --- lanes --------------------------------------------------------------------------------


def test_a_tarball_is_read_when_new_or_stale():
    now = pd.Timestamp("2026-10-29")
    assert aeronet.due(set(), "2026-10-24", now)
    assert not aeronet.due({"bulk:2026-10-24"}, "2026-10-24", now)
    assert not aeronet.due({"bulk:2026-10-17"}, "2026-10-24", now)
    assert aeronet.due({"bulk:2026-07-01"}, "2026-10-24", now)


def test_the_bulk_lane_reads_a_tarball_into_its_store(local_config, sites, monkeypatch):
    downloaded = []

    def fake_download(url, dest, **kwargs):
        dest.parent.mkdir(parents=True, exist_ok=True)
        downloaded.append(dest)
        with tarfile.open(dest, "w:gz") as archive:
            for name, rows in (("Cuiaba", ROWS), ("Tucson", ROWS[:1])):
                data = aod_file(name, rows).encode("latin-1")
                info = tarfile.TarInfo(f"AOD/AOD20/ALL_POINTS/19930101_20261024_{name}.lev20")
                info.size = len(data)
                archive.addfile(info, io.BytesIO(data))
        return dest

    monkeypatch.setattr(aeronet, "download_one", fake_download)
    monkeypatch.setattr(aeronet, "published", lambda url, session=None: ("2026-10-24", 1))
    product = PRODUCTS["aod_level20"]
    assert aeronet.run_bulk([product], local_config, sites) == 3
    store = AeronetStore(product, local_config, sites)
    assert "bulk:2026-10-24" in store.ledger()
    assert aeronet.run_bulk([product], local_config, sites) == 0
    assert downloaded and not downloaded[0].exists(), "the tarball is removed once read"


class FakeSession:
    def __init__(self, text: str):
        self.text, self.urls = text, []

    def get(self, url, **kwargs):
        self.urls.append(url)
        response = type("Response", (), {})()
        response.text = self.text if "day=1&" in url else "AERONET Data Download\n"
        response.raise_for_status = lambda: None
        return response


def test_the_history_walk_ledgers_only_whole_months(local_config, sites, monkeypatch):
    text = "\n".join(
        [
            "AERONET Data Download (Version 3 Direct Sun)",
            "AERONET Version 3",
            "Version 3: AOD Level 1.0",
            "unscreened",
            "Contact: see the web site",
            "AERONET_Site," + COLUMNS,
            "Cuiaba,01:06:1993,13:31:51,152,152.563785,0.1,-999.,-999.,1.9,0.7,3,lev10",
        ]
    )
    session = FakeSession(text)
    monkeypatch.setattr(aeronet.requests, "Session", lambda: session)
    product = PRODUCTS["aod_level10"]
    june = list(pd.date_range("1993-06-01", "1993-06-30"))
    assert aeronet.run_web([product], june, local_config, sites, ledger_months=True) == 1
    assert "web:1993-06" in AeronetStore(product, local_config, sites).ledger()
    asked = len(session.urls)
    aeronet.run_web([product], june, local_config, sites, ledger_months=True)
    assert len(session.urls) == asked, "a ledgered month is not asked for again"
    july = list(pd.date_range("1993-07-01", "1993-07-10"))
    aeronet.run_web([product], july, local_config, sites, ledger_months=True)
    assert "web:1993-07" not in AeronetStore(product, local_config, sites).ledger()


def test_a_lane_rereads_a_store_another_lane_wrote(local_config, sites):
    product = PRODUCTS["aod_level20"]
    first = AeronetStore(product, local_config, sites)
    second = AeronetStore(product, local_config, sites)
    assert first.append([parse_table(aod_file("Cuiaba", ROWS[:1]))]) == 1
    tucson = ["01:07:1994,10:00:00,182,182.416667,0.2,-999.,-999.,1.5,1.2,4,lev20"]
    assert second.append([parse_table(aod_file("Tucson", tucson))]) == 1
    # first's view is stale: Tucson already took index 1 and Cuiaba's row is stored.
    assert (
        first.append(
            [parse_table(aod_file("Cuiaba", ROWS)), parse_table(aod_file("Tucson", tucson))]
        )
        == 1
    )
    ds = stored(first)
    assert ds["site_name"].values.tolist() == ["Cuiaba", "Tucson"]
    assert ds["site_index"].values.tolist() == [0, 1, 0]


@pytest.mark.parametrize("today", ["2026-10-29", "2026-03-01", "2026-03-08", "2024-02-29"])
def test_no_day_of_a_web_product_falls_between_history_and_recent(today):
    today = pd.Timestamp(today)
    recent = set(aeronet.recent_window(today))
    tarball = "2026-01-15"
    for product in PRODUCTS.values():
        if not product.web:
            continue
        ledger = {f"bulk:{tarball}"} if product.bulk else set()
        history = set(aeronet.history_days(product, ledger, today))
        # Before the tarball's day the tarball holds a product; a web-only one, history.
        start = pd.Timestamp(tarball) if product.bulk else aeronet.ARCHIVE_START
        wanted = set(pd.date_range(min(start, today), today))
        missing = sorted(wanted - history - recent)
        assert not missing, (product.key, missing[:3])


def test_a_product_with_a_tarball_waits_for_it_before_walking():
    product = PRODUCTS["aod_level15"]
    assert len(aeronet.history_days(product, set(), pd.Timestamp("2026-10-29"))) == 0


def test_a_running_month_is_read_but_not_ledgered_until_it_ends():
    running = pd.Series(pd.date_range("2026-10-01", "2026-10-21"))
    assert not aeronet._whole_month("2026-10", running)
    assert aeronet._whole_month("2026-09", pd.Series(pd.date_range("2026-09-12", "2026-09-30")))


def test_a_header_with_a_trailing_separator_and_shorter_rows_parses():
    """The header ends in a comma (a last name that is empty), rows have one field less."""
    from planetary_datasets.providers.observations.aeronet_archive import parse_table

    header = (
        "AERONET_Site,Date_(dd:mm:yyyy),Time_(hh:mm:ss),Day_of_Year,"
        "Total_AOD_500nm[tau_a],Data_Quality_Level,AERONET_Instrument_Number,"
        "AERONET_Site_Name,Site_Latitude(Degrees),Site_Longitude(Degrees),Site_Elevation(m),"
    )
    row = "Lulin,22:10:2026,00:00:00,295,0.123456,lev10,1564,Lulin,23.468611,120.873611,2868.000000"
    table = parse_table("\n".join(["Version 3: SDA", "Level 1.0", header, row, row]))
    assert len(table.rows) == 2


def test_products_share_one_store_as_groups_that_open_as_a_datatree(local_config, sites):
    aod = AeronetStore(PRODUCTS["aod_level20"], local_config, sites)
    aod.append([parse_table(aod_file("Cuiaba", ROWS))])
    inversion = AeronetStore(PRODUCTS["inversion_almucantar_level20"], local_config, sites)
    columns = "Site,Date(dd:mm:yyyy),Time(hh:mm:ss),Single_Scattering_Albedo[440nm]"
    text = "\n".join(
        [*(h.format(site="Tucson") for h in HEAD), columns, "Tucson,20:06:1993,18:58:26,0.9"]
    )
    inversion.append([parse_table(text)])
    aod.mark("bulk:2026-10-24")

    assert aod.repo is not None and local_config.store_path(aeronet.STORE_PREFIX)
    store = aod.repo.readonly_session("main").store
    tree = xr.open_datatree(store, engine="zarr", consolidated=False)
    assert set(tree.children) == {"aod_level20", "inversion_almucantar_level20"}
    assert "layout" in tree.attrs and "open_datatree" in tree.attrs["layout"]
    assert tree["aod_level20"].sizes == {"observation": 2, "site": 1}
    assert tree["inversion_almucantar_level20"].sizes == {"observation": 1, "site": 1}
    one = xr.open_zarr(store, group="inversion_almucantar_level20", consolidated=False)
    assert one["single_scattering_albedo_440nm"].values.tolist() == [pytest.approx(0.9)]
    assert one["site_name"].values.tolist() == ["Tucson"]
    # Ledgers are per group.
    assert aod.ledger() == {"bulk:2026-10-24"}
    assert inversion.ledger() == set()


def test_writers_of_different_groups_rebase_over_each_other(local_config, sites):
    from planetary_datasets.common import multidim

    aod = AeronetStore(PRODUCTS["aod_level20"], local_config, sites)
    sda = AeronetStore(PRODUCTS["sda_level20"], local_config, sites)
    aeronet.ensure_root(aod.repo)
    first = aod._dataset(
        parse_table(aod_file("Cuiaba", ROWS)).rows.assign(site_index=np.uint16(0)),
        {},
        aod._site_rows(["Cuiaba"], {}),
    )
    second = sda._dataset(
        parse_table(aod_file("Tucson", ROWS[:1])).rows.assign(site_index=np.uint16(0)),
        {},
        sda._site_rows(["Tucson"], {}),
    )
    repo = aod.repo
    # Both sessions open before either commits; each commit rebases over the other's.
    a = repo.writable_session("main")
    b = repo.writable_session("main")
    from icechunk.xarray import to_icechunk

    to_icechunk(first, a, group="aod_level20", mode="w-")
    to_icechunk(second, b, group="sda_level20", mode="w-")
    a.commit("aod", rebase_with=aeronet.icechunk.ConflictDetector())
    b.commit("sda", rebase_with=aeronet.icechunk.ConflictDetector())
    # And through the archiver: ledger entries for both groups, interleaved.
    multidim.mark_written(repo, "bulk:1", group="aod_level20")
    multidim.mark_written(repo, "bulk:2", group="sda_level20")
    tree = xr.open_datatree(repo.readonly_session("main").store, engine="zarr", consolidated=False)
    assert set(tree.children) == {"aod_level20", "sda_level20"}
    assert tree["aod_level20"].sizes["observation"] == 2
    assert tree["sda_level20"].sizes["observation"] == 1
    assert aod.ledger() == {"bulk:1"} and sda.ledger() == {"bulk:2"}
    assert tree.attrs["title"] == aeronet.ROOT_ATTRS["title"]


def test_appends_to_different_groups_from_stale_sessions_both_land(local_config, sites):
    """Two appends whose sessions were opened before either committed."""
    from planetary_datasets.common import multidim

    aod = AeronetStore(PRODUCTS["aod_level20"], local_config, sites)
    sda = AeronetStore(PRODUCTS["sda_level20"], local_config, sites)
    aeronet.ensure_root(aod.repo)
    repo = aod.repo
    real = repo.writable_session
    opened = [real("main"), real("main")]
    sessions = iter(opened)
    repo.writable_session = lambda branch: next(sessions)
    ds_a = aod._dataset(
        parse_table(aod_file("Cuiaba", ROWS)).rows.assign(site_index=np.uint16(0)),
        {},
        aod._site_rows(["Cuiaba"], {}),
    )
    ds_b = sda._dataset(
        parse_table(aod_file("Tucson", ROWS[:1])).rows.assign(site_index=np.uint16(0)),
        {},
        sda._site_rows(["Tucson"], {}),
    )
    multidim.append_partition(repo, ds_a, None, label="aod", group="aod_level20")
    multidim.append_partition(repo, ds_b, None, label="sda", group="sda_level20")
    repo.writable_session = real
    tree = xr.open_datatree(repo.readonly_session("main").store, engine="zarr", consolidated=False)
    assert tree["aod_level20"].sizes["observation"] == 2
    assert tree["sda_level20"].sizes["observation"] == 1
