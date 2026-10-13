"""Fleet health check: materialise ``fleet_check/fleet_host_report`` to see where it ran.

Defined in :mod:`dags.fleet.smoke`, which can also be loaded on its own to test the fleet
without the rest of the code location's schedules. Pin a run to one machine with the run
tag ``planetary/host``.
"""

from dags.fleet.smoke import fleet_host_report

__all__ = ["fleet_host_report"]
