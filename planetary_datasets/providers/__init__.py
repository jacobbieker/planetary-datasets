"""Concrete data providers, one subpackage or module per source family.

Each provider subclasses :class:`planetary_datasets.base.BaseProvider` and is driven by the
Dagster assets in ``dags/assets``. Importing this package is deliberately cheap: heavy
optional dependencies (``satpy``, ``harp``, ``eumdac`` …) are imported inside the methods
that need them, not at module scope.
"""
