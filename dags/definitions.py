"""The single Dagster code location for planetary-datasets.

Launch it with::

    dagster dev -m dags.definitions

Every module under ``dags/assets/`` is discovered and loaded automatically. Nothing has
to be registered here by hand, which matters because asset modules are added
continuously and by many people at once. Three consequences follow from that:

* **A broken module is skipped, not fatal.** One module that fails to import used to
  take the whole code location down with it, hiding every other asset. Import errors are
  logged and collected into the ``asset_module_import_failures`` metadata on the
  definitions instead.
* **A module that hangs is skipped too.** Several of these files still do network I/O at
  import; one blocking forever would leave the location permanently "loading".
* **Group and key prefix come from the file's location.** ``dags/assets/nwp/gfs.py``
  lands in group ``nwp`` under key prefix ``nwp``; ``dags/assets/dmi.py`` lands in group
  ``dmi``.

Concurrency is sized from the host's memory. Each factory-built asset declares what it
needs (see :mod:`dags.factory`); assets are grouped into one scheduled job per memory
class, the run queue in ``dags/dagster.yaml`` limits how many runs of each class are
dequeued, and the executor limits how many steps of each class run inside a run.

The machinery lives in :mod:`dags.loader` so that it can be imported and tested without
the side effect of importing every asset module. This file does one thing: run it.
"""

from dags.loader import build_definitions

defs = build_definitions()
