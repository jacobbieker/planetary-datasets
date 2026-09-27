"""Dataset providers.

Each module here holds one :class:`~planetary_datasets.base.BaseProvider` subclass per
dataset. Import the provider you need directly from its module, e.g.::

    from planetary_datasets.providers.mrms import MRMSProvider

Providers are intentionally not imported eagerly here: several pull in heavy optional
dependencies, and a Dagster code location should only pay for the ones it uses.
"""
