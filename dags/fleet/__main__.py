"""Operate the fleet: ``pixi run fleet <command>``.

Commands:
    dev                 Start the webserver and daemon, loading code from code_dir.
    status              Probe every host and show what each can take.
    sync [HOST...]      Copy the code to remote hosts (launches do this themselves).
    setup HOST...       Sync, install the pixi env, write the env file, check Postgres.
    postgres            Start the shared Postgres container on this machine.
    logs RUN_ID         Print the worker log of a run, wherever it ran.
    exec -- CMD...      Run CMD with this host's fleet env file loaded.
"""

from __future__ import annotations

import argparse
import os
import secrets
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

from dags.fleet.hosts import (
    HOST_ENV_FILE,
    REPO_ROOT,
    SSH_OPTIONS,
    Fleet,
    Host,
    load_fleet,
    probe,
    run_on,
    shell_path,
)
from dags.fleet.placement import FLEET_HOST_TAG, FLEET_MEMORY_TAG, ledger
from dags.fleet.sync import sync_host

POSTGRES_CONTAINER = "planetary-dagster-postgres"
POSTGRES_IMAGE = "postgres:17"
#: Tailscale's address range. Postgres listens on every interface but only accepts
#: password logins from here and from this machine.
TAILNET_CIDR = "100.64.0.0/10"


def read_env_file(path: str = HOST_ENV_FILE) -> dict[str, str]:
    """Parse a ``KEY=value`` env file; missing file means empty."""
    env: dict[str, str] = {}
    file = Path(path).expanduser()
    if not file.exists():
        return env
    for line in file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            env[key.strip()] = value.strip().strip("'\"")
    return env


def render_env_file(env: dict[str, str]) -> str:
    """Format ``env`` as a shell-sourceable env file."""
    lines = ["# Written by `pixi run fleet setup`. Sourced by every Dagster run on this host."]
    lines += [f"{key}={shlex.quote(value)}" for key, value in sorted(env.items())]
    return "\n".join(lines) + "\n"


def write_local_env(env: dict[str, str]) -> None:
    """Write this host's env file, readable only by its owner."""
    file = Path(HOST_ENV_FILE).expanduser()
    file.parent.mkdir(parents=True, exist_ok=True)
    file.touch(mode=0o600, exist_ok=True)
    file.chmod(0o600)
    file.write_text(render_env_file(env))


def host_env(fleet: Fleet, host: Host, password: str) -> dict[str, str]:
    """The env file for ``host``: Postgres details plus Dagster's storage directories.

    The storage directories are here as well as on every launch because the webserver
    and daemon on the local host read the same ``dagster.yaml``.
    """
    env = {**fleet.postgres.env(), "DAGSTER_PG_PASSWORD": password}
    run_env = host.run_env()
    for key in ("DAGSTER_STORAGE_DIR", "DAGSTER_COMPUTE_LOG_DIR"):
        env[key] = run_env[key]
    return env


def _instance():
    from dagster import DagsterInstance

    os.environ.update({k: v for k, v in read_env_file().items() if k not in os.environ})
    return DagsterInstance.get()


def cmd_status(fleet: Fleet, args: argparse.Namespace) -> int:
    """Probe every host and print its capacity and reservations."""
    loads = {}
    try:
        from dagster._core.storage.dagster_run import RunsFilter

        from dags.fleet.launcher import IN_FLIGHT

        runs = _instance().get_runs(filters=RunsFilter(statuses=IN_FLIGHT))
        loads = ledger(run.tags for run in runs)
    except Exception as exc:  # noqa: BLE001 - status still useful without the instance
        print(f"(no run ledger: {type(exc).__name__}: {exc})\n")

    header = (
        f"{'host':8} {'state':12} {'platform':16} {'cpu':>4} {'mem free/total':>15} "
        f"{'commit':>7} {'reserved':>9} {'runs':>5} {'disk free':>10}"
    )
    print(header)
    print("-" * len(header))
    for host in fleet.hosts:
        p = probe(host)
        load = loads.get(host.name)
        runs = f"{load.runs if load else 0}/{host.max_runs}"
        reserved = f"{load.memory_gb if load else 0:.0f} GB"
        if not p.reachable:
            state = "disabled" if not host.enabled else "unreachable"
            blank = f"{'':16} {'':>4} {'':>15} {'':>7}"
            print(f"{host.name:8} {state:12} {blank} {reserved:>9} {runs:>5}  {p.error[:60]}")
            continue
        state = "ok" if host.enabled else "disabled"
        print(
            f"{host.name:8} {state:12} {p.platform:16} {p.cpus:>4} "
            f"{p.available_gb:>6.0f}/{p.total_gb:<5.0f} GB {host.capacity_gb(p.total_gb):>4.0f} GB "
            f"{reserved:>9} {runs:>5} {p.disk_free_gb:>7.0f} GB"
        )
    return 0


def _selected(fleet: Fleet, names: list[str]) -> list[Host]:
    return [fleet.host(n) for n in names] if names else [h for h in fleet.hosts if not h.is_local]


def cmd_sync(fleet: Fleet, args: argparse.Namespace) -> int:
    """Rsync the code to the named remote hosts, or all of them."""
    for host in _selected(fleet, args.hosts):
        sync_host(host)
        print(f"{host.name}: synced to {host.repo_dir}")
    return 0


def ensure_pixi(host: Host) -> bool:
    """Make sure ``host.pixi`` exists and matches this machine's pixi version.

    A pixi older than the one that wrote ``pixi.lock`` cannot read it. Rather than
    upgrading the pixi someone uses for other projects on that machine, the fleet keeps its
    own binary at ``host.pixi``: this machine's binary when the platform matches, otherwise
    the same release from pixi's installer.
    """
    local_pixi = shutil.which("pixi") or os.path.expanduser("~/.pixi/bin/pixi")
    local_version = subprocess.run(
        [local_pixi, "--version"], capture_output=True, text=True
    ).stdout.strip()
    remote = run_on(host, f"{shell_path(host.pixi)} --version; uname -sm", timeout=30)
    lines = remote.stdout.strip().splitlines()
    if lines and lines[0] == local_version:
        return True
    if host.is_local:
        print(f"   {host.pixi} is not {local_version}; fix the pixi path in dags/fleet.yaml")
        return False
    platform = lines[-1] if lines else ""
    here = " ".join(os.uname()[i] for i in (0, 4))
    if platform != here:
        # A different OS or architecture (macOS on arm64): this machine's binary will not
        # run there, so fetch the same release with pixi's own installer, into the fleet's
        # directory and without touching the host's shell profile.
        version = local_version.split()[-1]
        install = run_on(
            host,
            f"curl -fsSL https://pixi.sh/install.sh | PIXI_VERSION={shlex.quote(version)} "
            f"PIXI_BIN_DIR={shell_path(str(Path(host.pixi).parent))} PIXI_NO_PATH_UPDATE=1 bash",
            timeout=600,
        )
        if install.returncode != 0:
            print(f"   pixi install on {host.name} failed: {install.stderr.strip()[-300:]}")
            return False
        print(f"   installed {local_version} at {host.name}:{host.pixi} ({platform})")
        return True
    directory = shell_path(str(Path(host.pixi).parent))
    run_on(host, f"mkdir -p {directory}", timeout=30, check=True)
    target = host.pixi[2:] if host.pixi.startswith("~/") else host.pixi
    subprocess.run(
        ["scp", "-q", *SSH_OPTIONS, local_pixi, f"{host.ssh}:{target}"], check=True, timeout=300
    )
    print(f"   copied {local_version} to {host.name}:{host.pixi}")
    return True


def cmd_setup(fleet: Fleet, args: argparse.Namespace) -> int:
    """Prepare hosts to take runs: code, env file, pixi env, Postgres check."""
    password = read_env_file().get("DAGSTER_PG_PASSWORD")
    if not password:
        print("No DAGSTER_PG_PASSWORD in the local env file; run `pixi run fleet postgres` first.")
        return 1
    failed = 0
    for host in _selected(fleet, args.hosts):
        print(f"== {host.name}")
        env_text = render_env_file(host_env(fleet, host, password))
        if host.is_local:
            print(f"   code_dir: {ensure_code_dir(fleet)}")
            write_local_env(host_env(fleet, host, password))
        else:
            if host.repo_dir != fleet.code_dir:
                print(
                    f"   WARNING: repo_dir {host.repo_dir} is not code_dir {fleet.code_dir}; "
                    "runs there will be refused unless code_dir does not exist on it"
                )
            sync_host(host)
            write = (
                f"umask 077; mkdir -p {shell_path(str(Path(HOST_ENV_FILE).parent))}; "
                f"cat > {shell_path(HOST_ENV_FILE)}"
            )
            run_on(host, write, timeout=30, check=True, input=env_text)
        print("   env file written")
        if not ensure_pixi(host):
            failed += 1
            continue
        print("   installing the pixi environment (first time can take a while)")
        install = run_on(
            host,
            f"{shell_path(host.pixi)} install --frozen --manifest-path "
            f"{shell_path(host.repo_dir + '/pixi.toml')} 2>&1 | tail -n 5",
            timeout=3600,
        )
        print("   " + install.stdout.strip().replace("\n", "\n   "))
        check = run_on(
            host,
            f"set -a; . {shell_path(HOST_ENV_FILE)}; set +a; {shell_path(host.python)} -c "
            + shlex.quote(
                "import os, psycopg2; psycopg2.connect(host=os.environ['DAGSTER_PG_HOST'], "
                "port=os.environ['DAGSTER_PG_PORT'], dbname=os.environ['DAGSTER_PG_DB'], "
                "user=os.environ['DAGSTER_PG_USER'], password=os.environ['DAGSTER_PG_PASSWORD'], "
                "connect_timeout=10).close(); print('postgres ok')"
            ),
            timeout=60,
        )
        ok = check.returncode == 0
        failed += not ok
        print(
            "   "
            + (check.stdout.strip() if ok else f"postgres FAILED: {check.stderr.strip()[-300:]}")
        )
        p = probe(host)
        print(f"   probe: {'ok' if p.reachable else 'FAILED ' + p.error}")
        failed += not p.reachable
    return 1 if failed else 0


def cmd_postgres(fleet: Fleet, args: argparse.Namespace) -> int:
    """Start the shared Postgres container on the local host."""
    host = fleet.local
    if host is None:
        print("No local host (ssh: null) in the fleet inventory.")
        return 1
    env = read_env_file()
    if not env.get("DAGSTER_PG_PASSWORD"):
        env["DAGSTER_PG_PASSWORD"] = secrets.token_urlsafe(32)
    env.update(host_env(fleet, host, env["DAGSTER_PG_PASSWORD"]))
    write_local_env(env)

    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    hba = data_dir.parent / "pg_hba.conf"
    hba.write_text(
        "local all all trust\n"
        "host all all 127.0.0.1/32 scram-sha-256\n"
        f"host all all {TAILNET_CIDR} scram-sha-256\n"
    )
    exists = (
        subprocess.run(
            ["docker", "container", "inspect", POSTGRES_CONTAINER], capture_output=True
        ).returncode
        == 0
    )
    if exists:
        subprocess.run(["docker", "start", POSTGRES_CONTAINER], check=True)
        print(f"{POSTGRES_CONTAINER} started (already existed)")
        return 0
    pg = fleet.postgres
    subprocess.run(
        [
            "docker", "run", "-d", "--name", POSTGRES_CONTAINER, "--restart", "unless-stopped",
            # Host networking so the server sees real client addresses for pg_hba, and so
            # it listens on the tailnet address whenever tailscale comes up, before or
            # after the container.
            "--network", "host",
            "-e", f"POSTGRES_USER={pg.user}",
            "-e", f"POSTGRES_PASSWORD={env['DAGSTER_PG_PASSWORD']}",
            "-e", f"POSTGRES_DB={pg.db}",
            "-v", f"{data_dir}:/var/lib/postgresql/data",
            "-v", f"{hba}:/etc/postgresql/pg_hba.conf:ro",
            POSTGRES_IMAGE,
            "-c", "listen_addresses=*",
            "-c", f"port={pg.port}",
            "-c", "hba_file=/etc/postgresql/pg_hba.conf",
            "-c", "max_connections=300",
        ],
        check=True,
    )  # fmt: skip
    print(f"{POSTGRES_CONTAINER} running; data in {data_dir}; password in {HOST_ENV_FILE}")
    return 0


def cmd_logs(fleet: Fleet, args: argparse.Namespace) -> int:
    """Print the tail of a run's worker log from whichever host ran it."""
    run = _instance().get_run_by_id(args.run_id)
    if run is None:
        print(f"no run {args.run_id}")
        return 1
    name = run.tags.get(FLEET_HOST_TAG)
    if not name:
        print(f"run {args.run_id} has not been placed on a host")
        return 1
    host = fleet.host(name)
    reserved = run.tags.get(FLEET_MEMORY_TAG)
    print(f"# {host.name}:{host.log_dir}/{run.run_id}.log ({reserved} GB reserved)")
    result = run_on(
        host, f"tail -n {args.lines} {shell_path(host.log_dir)}/{run.run_id}.log", timeout=60
    )
    sys.stdout.write(result.stdout or result.stderr)
    return result.returncode


def ensure_code_dir(fleet: Fleet) -> Path:
    """Make ``fleet.code_dir`` on this machine point at this checkout, and return it.

    See :attr:`dags.fleet.hosts.Fleet.code_dir` for why every host must load the code
    location from the same path.
    """
    code_dir = Path(os.path.expanduser(fleet.code_dir))
    if code_dir.exists() or code_dir.is_symlink():
        if code_dir.resolve() != REPO_ROOT.resolve():
            raise SystemExit(
                f"{code_dir} exists but is not this checkout ({REPO_ROOT}); move it aside "
                "or set code_dir in dags/fleet.yaml"
            )
        return code_dir
    code_dir.parent.mkdir(parents=True, exist_ok=True)
    code_dir.symlink_to(REPO_ROOT, target_is_directory=True)
    print(f"linked {code_dir} -> {REPO_ROOT}")
    return code_dir


def _load_env(fleet: Fleet) -> None:
    for key, value in read_env_file().items():
        os.environ.setdefault(key, value)
    # dagster.yaml names dags.fleet.launcher; console scripts do not put the working
    # directory on sys.path, so the daemon and webserver could not import it otherwise.
    paths = [str(REPO_ROOT), *filter(None, os.environ.get("PYTHONPATH", "").split(os.pathsep))]
    os.environ["PYTHONPATH"] = os.pathsep.join(dict.fromkeys(paths))
    local = fleet.local
    if local is not None:
        for key, value in local.run_env().items():
            if key not in ("DAGSTER_HOME", "PLANETARY_FLEET_HOST", "PYTHONPATH"):
                os.environ.setdefault(key, os.path.expanduser(value))


def cmd_dev(fleet: Fleet, args: argparse.Namespace) -> int:
    """Start Dagster with the code location loaded from ``code_dir``."""
    code_dir = ensure_code_dir(fleet)
    _load_env(fleet)
    os.chdir(code_dir)
    if args.daemon_only:
        command = ["dagster-daemon", "run"]
    else:
        command = ["dagster", "dev"]
    command += ["-m", args.module, "-d", str(code_dir), *args.extra]
    os.execvp(command[0], command)
    return 127  # unreachable


def cmd_exec(fleet: Fleet, args: argparse.Namespace) -> int:
    """Exec a command with the local fleet env file loaded."""
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        print("usage: fleet exec -- COMMAND [ARGS...]")
        return 2
    _load_env(fleet)
    os.execvp(command[0], command)
    return 127  # unreachable


def main(argv: list[str] | None = None) -> int:
    """Entry point for ``python -m dags.fleet``."""
    parser = argparse.ArgumentParser(prog="fleet", description=__doc__.split("\n")[0])
    parser.add_argument("--config", help="fleet inventory (default: dags/fleet.yaml)")
    sub = parser.add_subparsers(dest="subcommand", required=True)
    dev = sub.add_parser("dev")
    dev.add_argument("--module", "-m", default="dags.definitions")
    dev.add_argument("--daemon-only", action="store_true", help="dagster-daemon run only")
    dev.add_argument("extra", nargs=argparse.REMAINDER)
    sub.add_parser("status")
    sub.add_parser("sync").add_argument("hosts", nargs="*")
    sub.add_parser("setup").add_argument("hosts", nargs="*")
    pg = sub.add_parser("postgres")
    pg.add_argument("--data-dir", default="/mnt/NVME2/dagster/postgres")
    logs = sub.add_parser("logs")
    logs.add_argument("run_id")
    logs.add_argument("-n", "--lines", type=int, default=200)
    sub.add_parser("exec").add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)

    fleet = load_fleet(args.config)
    handler = {
        "dev": cmd_dev,
        "status": cmd_status,
        "sync": cmd_sync,
        "setup": cmd_setup,
        "postgres": cmd_postgres,
        "logs": cmd_logs,
        "exec": cmd_exec,
    }[args.subcommand]
    return handler(fleet, args)


if __name__ == "__main__":
    sys.exit(main())
