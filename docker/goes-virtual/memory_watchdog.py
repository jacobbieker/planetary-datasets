#!/usr/bin/env python3
"""Keep the GOES virtual ingest under a fixed memory budget.

Samples the aggregate RSS of the whole ingest process tree every 30s. The
channel workers are spawned by ProcessPoolExecutor and so carry a different
command line from their parent, which is why this walks ppid links rather than
grepping for the module name.

If the job exceeds the budget for 3 consecutive samples it sheds load, starting
with the C02 runs: their 21696^2 grid carries ~16x the chunk entries of a 2km
channel, and every ingest resumes from its last commit, so giving one up is
cheap.
"""
import collections
import os
import re
import signal
import subprocess
import sys
import time


def _config_defaults():
    """Budget and log directory from the shared config, if it is importable.

    The image installs planetary_datasets, so MEMORY_CEILING_GB / the
    configured data_dir are the single place to set these. But this script is
    deliberately runnable on its own — it only needs `ps` — so an import
    failure just falls back to the environment.
    """
    try:
        from planetary_datasets.config import get_config
    except Exception:
        return None, None
    try:
        cfg = get_config()
        return cfg.memory_ceiling_gb, str(cfg.data_dir)
    except Exception:
        return None, None


_CFG_BUDGET_GB, _CFG_DATA_DIR = _config_defaults()

BUDGET_GB = float(os.environ.get("BUDGET_GB") or _CFG_BUDGET_GB or 64)
INTERVAL = 30
STRIKES_BEFORE_SHED = 3
LOG = os.environ.get("WATCHDOG_LOG") or os.path.join(
    os.environ.get("GOES_OUT") or _CFG_DATA_DIR or "/data",
    "logs",
    "watchdog.log",
)
# Both mission CLIs. Matching only the GOES one left a GK-2A-only
# container entirely unmonitored: the watchdog found no processes and
# exited immediately.
PATTERNS = ("ingest_goes_radf", "ingest_gk2a_fd", "ingest_himawari_isatss")


def snapshot():
    """Return {pid: (ppid, rss_kb, command)} for every process."""
    out = subprocess.run(
        ["ps", "-Ao", "pid=,ppid=,rss=,command="],
        capture_output=True, text=True,
    ).stdout
    procs = {}
    for line in out.splitlines():
        parts = line.split(None, 3)
        if len(parts) < 4:
            continue
        pid, ppid, rss, command = parts
        try:
            procs[int(pid)] = (int(ppid), int(rss), command)
        except ValueError:
            continue
    return procs


def job_tree(procs, return_roots=False):
    """PIDs of the ingest parents and every descendant of theirs.

    With return_roots, also returns just the parents. Shedding must target a
    parent: killing a ProcessPoolExecutor worker raises BrokenProcessPool in
    its parent and fails every remaining channel for that mission, which is
    how an earlier build silently lost a whole satellite.
    """
    # A real mission parent is the `-m ...ingest_*` invocation. multiprocessing
    # embeds main_path=.../ingest_goes_radf.py in its forkserver/spawn command
    # lines, so a bare substring match also catches pool workers -- and killing
    # one of those raises BrokenProcessPool and loses the whole satellite.
    roots = {
        pid for pid, (_, _, cmd) in procs.items()
        if any(pat in cmd for pat in PATTERNS)
        and "watchdog" not in cmd
        and not any(
            marker in cmd
            for marker in ("multiprocessing", "spawn_main", "forkserver", "-c ")
        )
    }
    children = collections.defaultdict(list)
    for pid, (ppid, _, _) in procs.items():
        children[ppid].append(pid)

    seen, stack = set(), list(roots)
    while stack:
        pid = stack.pop()
        if pid in seen:
            continue
        seen.add(pid)
        stack.extend(children.get(pid, ()))
    if return_roots:
        return seen, roots, children
    return seen


def log(msg):
    with open(LOG, "a") as f:
        f.write(f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} {msg}\n")


def main():
    os.makedirs(os.path.dirname(LOG) or ".", exist_ok=True)
    log(f"watchdog start, budget {BUDGET_GB:.0f}GB")
    strikes = 0
    while True:
        time.sleep(INTERVAL)
        procs = snapshot()
        tree = job_tree(procs)
        if not tree:
            log("no ingest processes left, watchdog exiting")
            return

        used_gb = sum(procs[p][1] for p in tree) / 1048576
        if used_gb <= BUDGET_GB:
            strikes = 0
            log(f"{used_gb:.1f}GB / {BUDGET_GB:.0f}GB ok ({len(tree)} procs)")
            continue

        strikes += 1
        log(f"{used_gb:.1f}GB / {BUDGET_GB:.0f}GB OVER "
            f"(strike {strikes}/{STRIKES_BEFORE_SHED}, {len(tree)} procs)")
        if strikes < STRIKES_BEFORE_SHED:
            continue

        # The half-kilometre passes (ABI C02, AMI vi006) carry roughly 16x
        # the chunk-manifest entries of a 2km channel, so they go first.
        heavy = [
            p for p in tree
            if re.search(r"--channel\s+2(\s|$)", procs[p][2])
            or re.search(r"--band\s+vi006(\s|$)", procs[p][2])
            or re.search(r"--band\s+C03(\s|$)", procs[p][2])
        ]
        if heavy:
            log(f"shedding high-resolution runs: {sorted(heavy)}")
            victims = heavy
        else:
            _, roots, children = job_tree(procs, return_roots=True)
            if not roots:
                log("nothing left to shed")
                strikes = 0
                continue

            def subtree_rss(root):
                total, stack = 0, [root]
                while stack:
                    pid = stack.pop()
                    if pid in procs:
                        total += procs[pid][1]
                        stack.extend(children.get(pid, ()))
                return total

            biggest = max(roots, key=subtree_rss)
            log(f"no high-resolution runs left, stopping heaviest mission "
                f"parent pid={biggest} ({subtree_rss(biggest) / 1048576:.1f}GB): "
                f"{procs[biggest][2][:90]}")
            victims = [biggest]
        for pid in victims:
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        strikes = 0


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
