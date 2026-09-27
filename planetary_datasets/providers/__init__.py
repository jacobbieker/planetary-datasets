"""Concrete dataset providers.

Each module here holds one :class:`~planetary_datasets.base.BaseProvider` subclass that
knows how to fetch and shape a single dataset. Import the provider you need directly, e.g.
``from planetary_datasets.providers.gfs import GFSProvider``, so that pulling in one
provider does not drag in every optional dependency of the others.
"""
