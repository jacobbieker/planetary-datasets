"""Concrete dataset providers.

Each module here holds one or more :class:`~planetary_datasets.base.BaseProvider`
subclasses. Import the provider you need directly from its module, e.g.::

    from planetary_datasets.providers.cams import CAMSGlobalCompositionProvider

Nothing is re-exported from this package so that importing one provider never drags in the
optional dependencies of every other one.
"""
