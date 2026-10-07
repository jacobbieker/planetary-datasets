"""Opening GRIB files with cfgrib.

cfgrib splits a GRIB file into one dataset per level type (``cfgrib.open_datasets``),
merging the hypercubes it finds for each with ``xr.merge`` and no explicit ``compat``.
Current xarray warns on every such call that the default is changing; the call is
inside cfgrib, so the warning cannot be fixed at its source and would otherwise repeat
for every file a provider opens.
"""

from __future__ import annotations

import contextlib
import warnings
from typing import Any, Iterator

import xarray as xr

#: The xarray FutureWarnings about ``combine``/``merge`` defaults changing.
_COMBINE_DEFAULT_WARNINGS = (
    r".*default value for compat will change.*",
    r".*default value for join will change.*",
)


@contextlib.contextmanager
def quiet_combine_defaults() -> Iterator[None]:
    """Silence xarray's warnings about upcoming ``compat``/``join`` defaults.

    Only those warnings are suppressed, and only inside the block. Code that calls
    ``xr.merge`` itself should pass ``compat`` and ``join`` explicitly instead.
    """
    with warnings.catch_warnings():
        for message in _COMBINE_DEFAULT_WARNINGS:
            warnings.filterwarnings("ignore", message=message, category=FutureWarning)
        yield


def open_grib_datasets(path: str, **kwargs: Any) -> list[xr.Dataset]:
    """Open every hypercube in a GRIB file, one dataset per level type.

    Args:
        path: Local path of the GRIB file.
        **kwargs: Passed through to ``cfgrib.open_datasets``, e.g. ``backend_kwargs``.

    Returns:
        The datasets cfgrib finds in the file, in its order.
    """
    import cfgrib

    with quiet_combine_defaults():
        return cfgrib.open_datasets(str(path), **kwargs)
