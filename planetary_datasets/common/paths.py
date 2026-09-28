"""Building filesystem paths from configuration and command-line input safely.

Store prefixes, archive directories, satellite and channel names all arrive from a ``.env``
file, an environment variable or a CLI argument and are then joined onto a root directory.
A component containing ``..`` or a leading ``/`` escapes that root, so a misconfigured
``ICECHUNK_PREFIX`` or a mistyped ``--channel`` can write outside the directory the caller
intended.

Joining through :func:`safe_join` keeps the result under its root, and
:func:`safe_component` turns an arbitrary label into something usable as a single path
segment.
"""

from __future__ import annotations

import os
import pathlib
import re
import stat
import tempfile

# Anything outside this set is replaced when a label becomes a path segment.
_UNSAFE_IN_COMPONENT = re.compile(r"[^A-Za-z0-9._-]")


class UnsafePath(ValueError):
    """A path component would escape the root it was being joined to."""


def safe_component(value: str, *, replacement: str = "_") -> str:
    """Reduce an arbitrary string to one safe path segment.

    Separators and traversal sequences are replaced rather than rejected, because the
    inputs are labels such as a satellite or channel name, not paths::

        >>> safe_component("goes16")
        'goes16'
        >>> safe_component("../../etc/passwd")
        '.._.._etc_passwd'
    """
    cleaned = _UNSAFE_IN_COMPONENT.sub(replacement, value.strip())
    cleaned = cleaned.strip(".") or replacement
    return cleaned


def safe_join(root: str | os.PathLike, *parts: str | os.PathLike) -> pathlib.Path:
    """Join ``parts`` onto ``root``, refusing anything that escapes it.

    Raises :class:`UnsafePath` if a part is absolute or the resolved result falls outside
    ``root``. Symlinks are resolved before the check, so a symlinked component cannot be
    used to step outside either.
    """
    root_path = pathlib.Path(root).expanduser()
    resolved_root = root_path.resolve()

    candidate = root_path
    for part in parts:
        part_path = pathlib.Path(part)
        if part_path.is_absolute():
            raise UnsafePath(f"refusing absolute path component {str(part)!r} under {root}")
        candidate = candidate / part_path

    resolved = candidate.expanduser().resolve()
    if resolved != resolved_root and resolved_root not in resolved.parents:
        raise UnsafePath(
            f"path {'/'.join(str(p) for p in parts)!r} resolves to {resolved}, outside {resolved_root}"
        )
    return resolved


def private_dir(parent: str | os.PathLike, name: str) -> pathlib.Path:
    """Return ``parent/name``, creating it owner-only if it does not exist.

    Scratch space defaults under the system temporary directory, which is world-writable
    and has predictable paths. Giving the project its own ``0o700`` directory there means
    another user cannot pre-create it or plant a symlink for a provider to follow.

    If the directory already exists and is not owner-only, its mode is tightened.
    """
    path = safe_join(parent, safe_component(name))
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        mode = stat.S_IMODE(path.stat().st_mode)
        if mode & 0o077:
            path.chmod(0o700)
    except OSError:
        # A directory we cannot stat or chmod (a mounted share, say) is the caller's to
        # get right; refusing to run over it would be worse than proceeding.
        pass
    return path


def default_scratch_dir(name: str = "planetary-datasets") -> pathlib.Path:
    """A private scratch directory under the system temporary directory."""
    return private_dir(tempfile.gettempdir(), name)
