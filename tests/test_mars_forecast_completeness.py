"""Model-level completeness at forecast steps, where MARS archives no geopotential."""

import pandas as pd

from planetary_datasets.providers import mars_icechunk as mi

ANALYSIS = pd.Timestamp("2026-07-25 06:00")
FORECAST = pd.Timestamp("2026-07-25 07:00")


def _rows(it, data_type, names):
    rows = []
    for name in names:
        levels = [1] if name in mi.ML_SURFACE_SHORT_NAMES else range(1, mi.N_MODEL_LEVELS + 1)
        rows += [
            {
                "group": mi.GROUP_ML,
                "valid_time": it,
                "short_name": name,
                "level": level,
                "data_type": data_type,
            }
            for level in levels
        ]
    return rows


def _provider(index_rows):
    provider = object.__new__(mi.MARSIcechunkProvider)
    provider._index = pd.DataFrame(index_rows)
    provider._expected_names = {
        mi.GROUP_ML: {"t", "q", "lnsp", "z"},
        mi.GROUP_WAVE: set(),
        mi.GROUP_SPECTRA: set(),
    }
    return provider


def test_a_forecast_step_without_geopotential_is_complete():
    provider = _provider(_rows(FORECAST, "fc", ["t", "q", "lnsp"]))
    assert provider.required_names(mi.GROUP_ML, FORECAST) == {"t", "q", "lnsp"}
    assert provider.ml_is_complete(FORECAST)


def test_an_analysis_still_needs_geopotential():
    provider = _provider(_rows(ANALYSIS, "an", ["t", "q", "lnsp"]))
    assert "z" in provider.required_names(mi.GROUP_ML, ANALYSIS)
    assert not provider.ml_is_complete(ANALYSIS)


def test_a_forecast_step_missing_another_variable_is_still_incomplete():
    provider = _provider(_rows(FORECAST, "fc", ["t", "lnsp"]))
    assert not provider.ml_is_complete(FORECAST)
