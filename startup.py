#!/usr/bin/env python3

import subprocess
import sys
import time

# ─── Configuration ───────────────────────────────────────────────────────────
SSH_USER = "lgd226"
DOMAIN = "cse.lehigh.edu"
PROGRAM_PATH = "~/CSE376/Replicated-Hash-Table/build/Replicated_Hash_Table"  # <── CHANGE THIS
NUM_NODES = 6
TMUX_SESSION = "rht"

MACHINES = [
    "ariel", "caliban", "callisto", "ceres", "chiron", "cupid",
    "eris", "europa", "hydra", "iapetus", "io", "ixion",
    "mars", "mercury", "neptune", "nereid", "nix", "orcus",
    "phobos", "puck", "saturn", "triton", "varda", "vesta", "xena",
]
# ─────────────────────────────────────────────────────────────────────────────


def ssh_host(machine: str) -> str:
    return f"{SSH_USER}@{machine}.{DOMAIN}"


def run_ssh(machine: str, cmd: str, timeout: int = 10) -> str | None:
    """Run a command on a remote machine via SSH. Returns stdout or None on failure."""
    try:
        result = subprocess.run(
            ["ssh", "-o", "ConnectTimeout=5", "-o", "StrictHostKeyChecking=no",
             ssh_host(machine), cmd],
            capture_output=True, text=True, timeout=timeout,
        )
        if result.returncode == 0:
            return result.stdout.strip()
        return None
    except (subprocess.TimeoutExpired, Exception):
        return None


def get_load(machine: str) -> float | None:
    """Get 1-minute load average from a machine."""
    out = run_ssh(machine, "cat /proc/loadavg")
    if out:
        try:
            return float(out.split()[0])
        except (ValueError, IndexError):
            return None
    return None


def get_ip(machine: str) -> str | None:
    """Resolve the IP address of a machine."""
    out = run_ssh(machine, "hostname -I")
    if out:
        return out.split()[0]
    return None


def pick_best_machines(n: int) -> list[str]:
    """Survey all machines and return the n with the lowest load."""
    print(f"Surveying {len(MACHINES)} machines for load averages...")
    loads: list[tuple[str, float]] = []

    for m in MACHINES:
        load = get_load(m)
        if load is not None:
            loads.append((m, load))
            print(f"  {m:12s}  load: {load:.2f}")
        else:
            print(f"  {m:12s}  unreachable")

    loads.sort(key=lambda x: x[1])
    return loads


def select_and_resolve(n: int) -> tuple[list[str], list[str]]:
    """Pick n machines with lowest load that also resolve an IP."""
    ranked = pick_best_machines(n * 2)  # get extra candidates in case some fail

    print("Resolving IP addresses...")
    machines: list[str] = []
    ips: list[str] = []
    for name, load in ranked:
        if len(machines) >= n:
            break
        ip = get_ip(name)
        if ip is None:
            print(f"  {name:12s}  SKIPPED (could not resolve IP)")
            continue
        machines.append(name)
        ips.append(ip)
        print(f"  {name:12s}  ->  {ip}")

    if len(machines) < n:
        print(f"  ERROR: only found {len(machines)} usable machines (need {n})", file=sys.stderr)
        sys.exit(1)

    print(f"\nSelected machines: {machines}")
    return machines, ips


def launch_tmux(machines: list[str], ips: list[str]):
    """Create a tmux session with one window per node, each running the program."""
    ip_args = " ".join(ips)

    # Kill any existing session with the same name
    subprocess.run(["tmux", "kill-session", "-t", TMUX_SESSION],
                   capture_output=True)
    time.sleep(0.5)

    # Wrap commands so the window stays open on exit (shows errors)
    def wrap_cmd(machine, idx):
        remote_cmd = f"{PROGRAM_PATH} {idx} {ip_args}"
        return f"ssh {ssh_host(machine)} '{remote_cmd}' 2>&1; echo \"--- exited with code $? ---\"; read -p 'Press enter to close...'"

    # Create a detached tmux session with the first node
    subprocess.run(
        ["tmux", "new-session", "-d", "-s", TMUX_SESSION, "-n", machines[0],
         "bash", "-c", wrap_cmd(machines[0], 0)],
        check=True,
    )
    print(f"  Window 0: {machines[0]} (index=0)")

    # Create additional windows for remaining nodes
    for idx in range(1, len(machines)):
        machine = machines[idx]
        subprocess.run(
            ["tmux", "new-window", "-t", TMUX_SESSION, "-n", machine,
             "bash", "-c", wrap_cmd(machine, idx)],
            check=True,
        )
        print(f"  Window {idx}: {machine} (index={idx})")

    print(f"\nAll {len(machines)} nodes launched in tmux session '{TMUX_SESSION}'.")
    print(f"Attach with:  tmux attach -t {TMUX_SESSION}")


def main():
    chosen, ips = select_and_resolve(NUM_NODES)
    launch_tmux(chosen, ips)


if __name__ == "__main__":
    main()
