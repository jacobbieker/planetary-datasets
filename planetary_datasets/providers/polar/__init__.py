"""Polar-orbiting sounder and imager providers.

One provider per instrument, all writing into ``bkr/polar/*.icechunk``:

============================  =======================================================
:class:`JpssAtmsProvider`     ATMS microwave sounder, NOAA-21 / NOAA-20 / Suomi-NPP
:class:`MetopAmsuaProvider`   AMSU-A microwave sounder, MetOp
:class:`MetopAscatProvider`   ASCAT scatterometer, MetOp
:class:`MetopAvhrrProvider`   AVHRR imager, MetOp
:class:`MetopGomeProvider`    GOME-2 spectrometer, MetOp
:class:`MetopIasiProvider`    IASI infrared sounder, MetOp
:class:`MetopMhsProvider`     MHS microwave humidity sounder, MetOp
:class:`MetopSgMwsProvider`   MWS microwave sounder, MetOp-SG
:class:`MetopSgMetimageProvider`  METimage imager, MetOp-SG
:class:`MetopSgRoProvider`    GRAS-2 radio occultation, MetOp-SG
============================  =======================================================

ATMS comes from the public NOAA open data buckets and needs no credentials. The MetOp
instruments come from the EUMETSAT Data Store and need ``EUMETSAT_CONSUMER_KEY`` and
``EUMETSAT_CONSUMER_SECRET``; AMSU-A, ASCAT and MHS are downloaded and converted to netCDF by
the EUMETSAT Data Tailor in the ``docker/epct`` image.
"""

from planetary_datasets.providers.polar._eumdac import EumdacProvider
from planetary_datasets.providers.polar._granule import (
    GranuleProvider,
    mid_time,
    pad_dim,
    process_eps_netcdf,
    serialize_attrs,
    serialize_dataset_attrs,
)
from planetary_datasets.providers.polar.jpss_atms import JpssAtmsProvider
from planetary_datasets.providers.polar.metop_amsua import MetopAmsuaProvider
from planetary_datasets.providers.polar.metop_ascat import MetopAscatProvider
from planetary_datasets.providers.polar.metop_avhrr import MetopAvhrrProvider
from planetary_datasets.providers.polar.metop_gome import MetopGomeProvider
from planetary_datasets.providers.polar.metop_iasi import MetopIasiProvider
from planetary_datasets.providers.polar.metop_mhs import MetopMhsProvider
from planetary_datasets.providers.polar.metop_sg import (
    MetopSgMetimageProvider,
    MetopSgMwsProvider,
    MetopSgRoProvider,
)

__all__ = [
    "EumdacProvider",
    "GranuleProvider",
    "JpssAtmsProvider",
    "MetopAmsuaProvider",
    "MetopAscatProvider",
    "MetopAvhrrProvider",
    "MetopGomeProvider",
    "MetopIasiProvider",
    "MetopMhsProvider",
    "MetopSgMetimageProvider",
    "MetopSgMwsProvider",
    "MetopSgRoProvider",
    "mid_time",
    "pad_dim",
    "process_eps_netcdf",
    "serialize_attrs",
    "serialize_dataset_attrs",
]
