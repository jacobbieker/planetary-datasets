"""Turning pandas frequency strings into durations.

Providers describe their cadence with pandas offset aliases (``"1h"``, ``"10min"``,
``"1D"``) and need the matching duration to step through init times or bound a
window. ``pd.Timedelta("1h")`` parses the alias as a generic unit string, which some
pandas versions deprecate; going through :func:`~pandas.tseries.frequencies.to_offset`
reads it as the frequency it is.
"""

from __future__ import annotations

import pandas as pd
from pandas.tseries.frequencies import to_offset


def freq_to_timedelta(freq: str | pd.DateOffset) -> pd.Timedelta:
    """The fixed duration of a pandas frequency.

    Args:
        freq: An offset alias such as ``"1h"`` or ``"15min"``, or an offset object.

    Returns:
        The duration of one step, e.g. ``Timedelta(hours=1)`` for ``"1h"``.

    Raises:
        ValueError: If ``freq`` is not a valid alias, or is a calendar frequency such
            as ``"MS"`` whose length varies from step to step.
    """
    offset = to_offset(freq)
    try:
        nanos = offset.nanos
    except ValueError as exc:
        raise ValueError(f"{freq!r} has no fixed duration") from exc
    return pd.Timedelta(nanos, "ns")
