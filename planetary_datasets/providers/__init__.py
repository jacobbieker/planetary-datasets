"""Concrete dataset providers.

Each module here defines one :class:`~planetary_datasets.base.BaseProvider` subclass that
knows how to fetch and shape a single dataset. Import the provider you need directly from
its module, e.g. ``from planetary_datasets.providers.geos import GEOSProvider``, so that
loading one provider does not pull in every other provider's dependencies.
"""
