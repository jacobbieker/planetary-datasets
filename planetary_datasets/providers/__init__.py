"""Dataset providers.

Each module here defines one or more :class:`~planetary_datasets.base.BaseProvider`
subclasses for a single data source. Import the provider you need directly::

    from planetary_datasets.providers.mrms import MRMSProvider

Nothing is re-exported from this module on purpose: importing every provider eagerly would
pull in the whole optional dependency surface (cfgrib, satpy, harp, copernicusmarine, ...)
for anyone importing any single one of them.
"""
