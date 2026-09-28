"""Surface observation networks.

One module per network, all sharing :mod:`planetary_datasets.providers.observations.base`:
hit an endpoint per station or per partition, assemble dataframes, reshape into a dense
``(time, station)`` cube and append it to an Icechunk store.

============================  ==========================  ==========  ==============
Module                        Provider                    Partition   Credentials
============================  ==========================  ==========  ==============
:mod:`~.aeronet`              ``AeronetProvider``         day         none
:mod:`~.asos`                 ``ASOSOneMinuteProvider``   day         none
:mod:`~.ghcn`                 ``GHCNHourlyProvider``      quarter     none
:mod:`~.gnss`                 ``GNSSProvider``            month       CDS
:mod:`~.isd`                  ``ISDProvider``             month       none
:mod:`~.meteostat`            ``MeteostatHourlyProvider`` month       none
:mod:`~.midc`                 ``MIDCProvider``            day         none
:mod:`~.pvlive`               ``PVLiveProvider``          month       none
:mod:`~.rahm`                 ``RAHMProvider``            month       CDS
:mod:`~.solrad`               ``SolradProvider``          day         none
:mod:`~.surfrad`              ``SurfradProvider``         day         none
============================  ==========================  ==========  ==============

The shared pieces are re-exported here; the providers themselves are not, so importing
one network does not drag in another's optional dependencies.
"""

from planetary_datasets.providers.observations.base import (
    NoStationDataError,
    Station,
    StationObservationProvider,
    align_to_grid,
    download_or_none,
    frames_to_dataset,
    partition_time_index,
)

__all__ = [
    "NoStationDataError",
    "Station",
    "StationObservationProvider",
    "align_to_grid",
    "download_or_none",
    "frames_to_dataset",
    "partition_time_index",
]
