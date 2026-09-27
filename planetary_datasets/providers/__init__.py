"""Dataset providers.

Each module holds one :class:`~planetary_datasets.base.BaseProvider` subclass per
dataset, responsible only for fetching the inputs for a partition and turning them into
an :class:`xarray.Dataset`. Store locations, credentials and memory limits come from
:mod:`planetary_datasets.config` and :mod:`planetary_datasets.base`.
"""
