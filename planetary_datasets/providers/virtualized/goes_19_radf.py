"""GOES-19 ABI L1b full-disk radiance archive.

operational from 2024; launched after the ABI Shuffle change, so it
has a single codec era and no reprocessed archive.

The configuration lives in :data:`~planetary_datasets.providers.virtualized.goes_radf_common.SATELLITES`
and the behaviour in that module; this binds the two together. Import the names from here
as before, or use ``goes_radf_common.SATELLITES["goes19"]`` directly.

    python -m planetary_datasets.providers.virtualized.goes_19_radf --smoke-test --channel 13
"""

from __future__ import annotations

from planetary_datasets.providers.virtualized.goes_radf_common import bind

globals().update(bind("goes19"))

if __name__ == "__main__":
    from planetary_datasets.providers.virtualized.goes_radf_common import satellite_cli

    satellite_cli("goes19")
