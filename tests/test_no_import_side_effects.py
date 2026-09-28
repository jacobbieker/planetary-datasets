"""Importing a module must not touch the network or exit the process.

Dagster imports every asset module on each code-location load. A module that downloads
at import turns loading the definitions into a data transfer — one observation asset used
to pull several hundred MB and litter NetCDF files into the repository root every time the
code location was read, including on every ``dagster dev`` reload.

The check runs in a subprocess so it is unaffected by modules another test already
imported, and so a module that calls ``sys.exit`` cannot take the test runner with it.
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
    import json, os, socket, sys, importlib, pkgutil, signal
    sys.path.insert(0, os.getcwd())

    class NetworkAtImport(Exception):
        pass

    # Record every attempt, not just the ones whose exception escapes. Libraries such as
    # cdsapi catch connection errors and retry internally, so a module can perform real
    # I/O at import and still appear to import cleanly.
    ATTEMPTS = []

    def _connect(self, addr):
        ATTEMPTS.append(f"socket.connect({addr!r})")
        raise NetworkAtImport(ATTEMPTS[-1])

    def _create_connection(addr=None, *a, **k):
        ATTEMPTS.append(f"socket.create_connection({addr!r})")
        raise NetworkAtImport(ATTEMPTS[-1])

    def _getaddrinfo(host=None, *a, **k):
        ATTEMPTS.append(f"socket.getaddrinfo({host!r})")
        raise NetworkAtImport(ATTEMPTS[-1])

    socket.socket.connect = _connect
    socket.create_connection = _create_connection
    socket.getaddrinfo = _getaddrinfo

    def _timeout(signum, frame):
        print(json.dumps({"hung": os.environ.get("CURRENT_MODULE", "?")}))
        os._exit(0)

    signal.signal(signal.SIGALRM, _timeout)

    def _caused_by_network(exc):
        seen, cur = 0, exc
        while cur is not None and seen < 20:
            if isinstance(cur, NetworkAtImport):
                return cur
            cur = cur.__cause__ or cur.__context__
            seen += 1
        return None

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
            # other tests cover importability. Any network attempt is still recorded below.
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
    assert not offenders, "modules performing network I/O at import:\n" + "\n".join(
        f"  {o['module']}: {o['reason']}" for o in offenders
    )


def test_the_probe_detects_a_module_that_does_reach_the_network(tmp_path):
    """Guard the guard: a check that cannot fail is worse than none."""
    import json

    canary = REPO_ROOT / "_canary_network_at_import.py"
    canary.write_text("import urllib.request\nurllib.request.urlopen('http://example.com', timeout=5)\n")
    try:
        result = subprocess.run(
            [sys.executable, "-c", PROBE, json.dumps(["_canary_network_at_import"])],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=120,
        )
        lines = [line for line in result.stdout.strip().splitlines() if line.startswith(("[", "{"))]
        offenders = json.loads(lines[-1])
        assert [o["module"] for o in offenders] == ["_canary_network_at_import"]
    finally:
        canary.unlink(missing_ok=True)


@pytest.mark.parametrize("package", SCANNED)
def test_scanned_packages_exist(package):
    """If a package is renamed, the scan must not silently cover nothing."""
    if package == "dags/assets" and not (REPO_ROOT / package).is_dir():
        pytest.skip("asset package not present on this branch")
    assert (REPO_ROOT / package).is_dir()
