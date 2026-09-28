"""GOES-17 ABI L1b full-disk radiance archive.

GOES-West from 2018 until GOES-18 replaced it; reprocessed archive
covering 2018-08-28 to 2023-01-01.

The configuration lives in :data:`~planetary_datasets.providers.virtualized.goes_radf_common.SATELLITES`
and the behaviour in that module; this binds the two together. Import the names from here
as before, or use ``goes_radf_common.SATELLITES["goes17"]`` directly.

    python -m planetary_datasets.providers.virtualized.goes_17_radf --smoke-test --channel 13
"""

from __future__ import annotations

from planetary_datasets.providers.virtualized.goes_radf_common import bind

globals().update(bind("goes17"))

if __name__ == "__main__":
    from planetary_datasets.providers.virtualized.goes_radf_common import satellite_cli

    satellite_cli("goes17")
