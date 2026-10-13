"""Spreading Dagster runs over several machines.

* :mod:`dags.fleet.hosts` — the inventory in ``dags/fleet.yaml``, and probing hosts.
* :mod:`dags.fleet.placement` — which host a run fits on (pure; no Dagster, no SSH).
* :mod:`dags.fleet.sync` — keeping each host's copy of the code current.
* :mod:`dags.fleet.launcher` — the ``FleetRunLauncher`` that ``dags/dagster.yaml`` uses.
* ``python -m dags.fleet`` — status, setup and logs; see :mod:`dags.fleet.__main__`.
"""
