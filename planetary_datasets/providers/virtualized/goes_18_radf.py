"""GOES-18 ABI L1b full-disk radiance archive.

replaced GOES-17 as GOES-West in 2022; no reprocessed archive.

The configuration lives in :data:`~planetary_datasets.providers.virtualized.goes_radf_common.SATELLITES`
and the behaviour in that module; this binds the two together. Import the names from here
as before, or use ``goes_radf_common.SATELLITES["goes18"]`` directly.

    python -m planetary_datasets.providers.virtualized.goes_18_radf --smoke-test --channel 13
"""

from __future__ import annotations

from planetary_datasets.providers.virtualized.goes_radf_common import bind

globals().update(bind("goes18"))

if __name__ == "__main__":
    from planetary_datasets.providers.virtualized.goes_radf_common import satellite_cli

    satellite_cli("goes18")
