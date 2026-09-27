"""Dataset providers.

Each module here holds one or more :class:`~planetary_datasets.base.BaseProvider`
subclasses. Import the provider you need directly from its module rather than from this
package, so that pulling in one dataset does not import every optional dependency::

    from planetary_datasets.providers.hawaii_nam import HawaiiNAMProvider
"""
