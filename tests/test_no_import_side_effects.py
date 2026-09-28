"""Importing a module must not touch the network, write to disk, or exit the process.

Dagster imports every asset module on each code-location load. A module that downloads
at import turns loading the definitions into a data transfer — one observation asset used
to pull several hundred MB and litter NetCDF files into the repository root every time the
code location was read, including on every ``dagster dev`` reload.

Destructive filesystem calls are checked as well as the network, and for a worse reason.
Several modules here began life as scripts whose body ran at module scope; one of them
opened with ``shutil.rmtree("/data/gmgsi/gmgsi_v3.icechunk")``. Catching that needs a
positive check: the probe below deliberately swallows every exception a module raises
(a missing optional dependency is not this test's concern), so on CI, where ``/data``
does not exist, such a module simply fails its first line and looks clean.
"""

from __future__ import annotations

import pathlib
import subprocess
import sys
import textwrap

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent

# Packages whose modules are imported by the Dagster code location or by a provider.
SCANNED = ("dags/assets", "planetary_datasets")

PROBE = textwrap.dedent(
    '''
    import json, os, shutil, socket, sys, importlib, pkgutil, signal
    sys.path.insert(0, os.getcwd())

    class SideEffectAtImport(Exception):
        pass

    # Record every attempt, not just the ones whose exception escapes. Libraries such as
    # cdsapi catch connection errors and retry internally, so a module can perform real
    # I/O at import and still appear to import cleanly. The same applies to a module that
    # wraps its own destructive call in a try/except.
    ATTEMPTS = []

    def _record(what):
        ATTEMPTS.append(what)
        raise SideEffectAtImport(what)

    def _connect(self, addr):
        _record(f"socket.connect({addr!r})")

    def _create_connection(addr=None, *a, **k):
        _record(f"socket.create_connection({addr!r})")

    def _getaddrinfo(host=None, *a, **k):
        _record(f"socket.getaddrinfo({host!r})")

    socket.socket.connect = _connect
    socket.create_connection = _create_connection
    socket.getaddrinfo = _getaddrinfo

    # Destructive filesystem calls. A module doing any of these at import deletes or
    # overwrites real data every time the Dagster code location loads. Scratch paths are
    # let through: a dependency that uses a TemporaryDirectory at import is not the
    # problem, and flagging it would make this check something people learn to ignore.
    import tempfile
    _SCRATCH = os.path.realpath(tempfile.gettempdir())
    _real_rmtree, _real_remove = shutil.rmtree, os.remove
    _real_rmdir, _real_replace = os.rmdir, os.replace

    def _is_scratch(path):
        try:
            real = os.path.realpath(os.fspath(path))
        except Exception:
            return False
        return real.startswith(_SCRATCH + os.sep) or "__pycache__" in real.split(os.sep)

    def _guarded(name, real):
        def wrapper(path, *a, **k):
            # A dir_fd-relative call comes from inside shutil.rmtree's own directory walk,
            # which is already intercepted at the top by the wrapper below it. The path is
            # a bare entry name there and cannot be resolved, so passing it through is both
            # necessary and safe.
            if k.get("dir_fd") is not None or _is_scratch(path):
                return real(path, *a, **k)
            _record(f"{name}({os.fspath(path)!r})")
        return wrapper

    def _guarded_replace(src, dst, *a, **k):
        if _is_scratch(src) and _is_scratch(dst):
            return _real_replace(src, dst, *a, **k)
        _record(f"os.replace({os.fspath(src)!r}, {os.fspath(dst)!r})")

    shutil.rmtree = _guarded("shutil.rmtree", _real_rmtree)
    os.remove = os.unlink = _guarded("os.remove", _real_remove)
    os.rmdir = _guarded("os.rmdir", _real_rmdir)
    os.replace = os.rename = _guarded_replace

    def _timeout(signum, frame):
        print(json.dumps({"hung": os.environ.get("CURRENT_MODULE", "?")}))
        os._exit(0)

    signal.signal(signal.SIGALRM, _timeout)

    offenders = []
    modules = json.loads(sys.argv[1])
    for name in modules:
        os.environ["CURRENT_MODULE"] = name
        before = len(ATTEMPTS)
        signal.alarm(60)
        try:
            importlib.import_module(name)
        except SystemExit:
            offenders.append({"module": name, "reason": "called sys.exit() at import"})
        except BaseException:
            # A missing optional dependency or a syntax error is not this test's concern;
            # other tests cover importability. This is why every side effect has to be
            # recorded as it is attempted rather than inferred from what escapes: a module
            # whose first statement deletes a store raises here on a machine where the
            # store is absent, and would otherwise look perfectly clean.
            pass
        finally:
            signal.alarm(0)
        attempted = ATTEMPTS[before:]
        if attempted and not any(o["module"] == name for o in offenders):
            offenders.append({"module": name, "reason": attempted[0]})

    print(json.dumps(offenders))
    '''
)


def _module_names() -> list[str]:
    names: list[str] = []
    for package in SCANNED:
        root = REPO_ROOT / package
        if not root.is_dir():
            continue
        for path in sorted(root.rglob("*.py")):
            if ".pixi" in path.parts or "__pycache__" in path.parts:
                continue
            rel = path.relative_to(REPO_ROOT).with_suffix("")
            parts = list(rel.parts)
            if parts[-1] == "__init__":
                parts.pop()
            if parts:
                names.append(".".join(parts))
    return names


def test_no_module_performs_network_io_at_import():
    import json

    modules = _module_names()
    assert modules, "found no modules to scan; the layout must have moved"

    result = subprocess.run(
        [sys.executable, "-c", PROBE, json.dumps(modules)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=900,
    )
    assert result.returncode == 0, f"probe failed:\n{result.stderr[-2000:]}"

    lines = [line for line in result.stdout.strip().splitlines() if line.startswith(("[", "{"))]
    assert lines, f"probe produced no result:\n{result.stdout[-2000:]}\n{result.stderr[-2000:]}"

    hung = [json.loads(line) for line in lines if line.startswith("{")]
    assert not hung, f"module(s) blocked at import: {hung}"

    offenders = json.loads(lines[-1])
    assert not offenders, "modules with side effects at import:\n" + "\n".join(
        f"  {o['module']}: {o['reason']}" for o in offenders
    )


def _run_canary(source: str) -> list[dict]:
    """Import a throwaway module under the probe and return what it flagged."""
    import json

    canary = REPO_ROOT / "_canary_side_effect_at_import.py"
    canary.write_text(source)
    try:
        result = subprocess.run(
            [sys.executable, "-c", PROBE, json.dumps(["_canary_side_effect_at_import"])],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=120,
        )
        lines = [line for line in result.stdout.strip().splitlines() if line.startswith(("[", "{"))]
        return json.loads(lines[-1])
    finally:
        canary.unlink(missing_ok=True)


def test_the_probe_detects_a_module_that_does_reach_the_network():
    """Guard the guard: a check that cannot fail is worse than none."""
    offenders = _run_canary(
        "import urllib.request\nurllib.request.urlopen('http://example.com', timeout=5)\n"
    )
    assert [o["module"] for o in offenders] == ["_canary_side_effect_at_import"]


def test_the_probe_detects_a_module_that_deletes_a_store_at_import():
    """Regression: an asset module opened with ``rmtree`` of a production icechunk store.

    It survived this file because the probe only watched the network, and because the
    ``except BaseException`` above swallows the ``FileNotFoundError`` the same statement
    raises on a machine that has no ``/data``.
    """
    offenders = _run_canary(
        "import shutil\n"
        "try:\n"
        "    shutil.rmtree('/data/gmgsi/gmgsi_v3.icechunk')\n"
        "except Exception:\n"
        "    pass\n"
    )
    assert [o["module"] for o in offenders] == ["_canary_side_effect_at_import"]
    assert "rmtree" in offenders[0]["reason"]


def test_the_probe_allows_scratch_writes_at_import():
    """A dependency using a temporary directory at import is not an offender."""
    offenders = _run_canary(
        "import tempfile, pathlib\n"
        "with tempfile.TemporaryDirectory() as d:\n"
        "    pathlib.Path(d, 'x').write_text('hello')\n"
    )
    assert offenders == []


@pytest.mark.parametrize("package", SCANNED)
def test_scanned_packages_exist(package):
    """If a package is renamed, the scan must not silently cover nothing."""
    if package == "dags/assets" and not (REPO_ROOT / package).is_dir():
        pytest.skip("asset package not present on this branch")
    assert (REPO_ROOT / package).is_dir()
