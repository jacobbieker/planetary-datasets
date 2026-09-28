"""GOES-16 ABI L1b full-disk radiance archive.

operational GOES-East from 2017; archive from 2017-02-28, with a
reprocessed archive covering 2018-01-04 to 2024-12-28.

The configuration lives in :data:`~planetary_datasets.providers.virtualized.goes_radf_common.SATELLITES`
and the behaviour in that module; this binds the two together. Import the names from here
as before, or use ``goes_radf_common.SATELLITES["goes16"]`` directly.

    python -m planetary_datasets.providers.virtualized.goes_16_radf --smoke-test --channel 13
"""

from __future__ import annotations

from planetary_datasets.providers.virtualized.goes_radf_common import bind

globals().update(bind("goes16"))

if __name__ == "__main__":
    from planetary_datasets.providers.virtualized.goes_radf_common import satellite_cli

    satellite_cli("goes16")
