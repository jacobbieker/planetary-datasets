"""Synthetic WIS2 BUFR bulletins for the archive-target tests (encoded with ecCodes)."""

from __future__ import annotations

import datetime as dt

#: Hong Kong's aircraft sequence: identity, position, time, flight level, wind, temperature,
#: dewpoint, phase of flight, mixing ratio, then a replicated eddy-dissipation-rate block.
AMDAR_LEGACY = [
    1008, 1023, 301021, 301011, 301013, 311006, 33025, 8004, 13002,
    104000, 31001, 11075, 11076, 11037, 11077,
]  # fmt: skip


def encode_amdar(reports: list[dict], compressed: bool = False) -> bytes:
    """One BUFR message, one subset per report.

    Each report: ``identifier``, ``time`` (datetime), ``latitude``, ``longitude``,
    ``flight_level`` (m), ``temperature`` (K), ``wind_direction``, ``wind_speed`` and
    optionally ``edr``, a list of ``(mean, peak)`` eddy dissipation rates (replicated).
    """
    import eccodes

    handle = eccodes.codes_bufr_new_from_samples("BUFR4")
    try:
        eccodes.codes_set(handle, "masterTablesVersionNumber", 33)
        eccodes.codes_set(handle, "dataCategory", 4)
        eccodes.codes_set(handle, "numberOfSubsets", len(reports))
        eccodes.codes_set(handle, "compressedData", 1 if compressed else 0)
        eccodes.codes_set_array(
            handle,
            "inputDelayedDescriptorReplicationFactor",
            [len(r.get("edr", [])) for r in reports],
        )
        eccodes.codes_set_array(handle, "unexpandedDescriptors", AMDAR_LEGACY)
        edr = 0
        columns: dict[str, list] = {}
        for n, report in enumerate(reports, start=1):
            when: dt.datetime = report["time"]
            values = {
                "aircraftRegistrationNumberOrOtherIdentification": report["identifier"],
                "observationSequenceNumber": n,
                "latitude": report["latitude"],
                "longitude": report["longitude"],
                "year": when.year,
                "month": when.month,
                "day": when.day,
                "hour": when.hour,
                "minute": when.minute,
                "second": when.second,
                "flightLevel": report["flight_level"],
                "windDirection": report["wind_direction"],
                "windSpeed": report["wind_speed"],
                "airTemperature": report["temperature"],
                "phaseOfAircraftFlight": report.get("phase", 5),
            }
            for key, value in values.items():
                if compressed:
                    columns.setdefault(key, []).append(value)
                else:
                    eccodes.codes_set(handle, f"#{n}#{key}", value)
            for mean, peak in report.get("edr", []):
                edr += 1
                eccodes.codes_set(
                    handle, f"#{edr}#meanTurbulenceIntensityEddyDissipationRate", mean
                )
                eccodes.codes_set(
                    handle, f"#{edr}#peakTurbulenceIntensityEddyDissipationRate", peak
                )
        # Compressed data takes each element for every subset at once.
        for key, column in columns.items():
            if isinstance(column[0], str):
                eccodes.codes_set_string_array(handle, f"#1#{key}", column)
            else:
                eccodes.codes_set_array(handle, f"#1#{key}", column)
        eccodes.codes_set(handle, "pack", 1)
        return eccodes.codes_get_message(handle)
    finally:
        eccodes.codes_release(handle)


def bulletin(*messages: bytes, header: str = "IUAG04 VHHH 270642") -> bytes:
    """Messages wrapped as a GTS bulletin: a heading line, then the messages back to back."""
    return f"\r\r\n{header}\r\r\n".encode() + b"".join(messages)


def report(identifier="HK0013", minute=36, **overrides) -> dict:
    """One plausible report."""
    base = {
        "identifier": identifier,
        "time": dt.datetime(2026, 10, 27, 6, minute, 0),
        "latitude": 22.13333,
        "longitude": 113.765,
        "flight_level": 1058,
        "temperature": 291.85,
        "wind_direction": 64,
        "wind_speed": 2.6,
    }
    return {**base, **overrides}
