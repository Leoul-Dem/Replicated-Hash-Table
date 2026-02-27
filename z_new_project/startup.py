#!/usr/bin/env python3

import re
import subprocess
import sys
import time

# ─── Configuration ───────────────────────────────────────────────────────────
SSH_USER = "lgd226"
DOMAIN = "cse.lehigh.edu"
PROGRAM_PATH = "~/z_new_project/build/Replicated_Hash_Table"  # <── CHANGE THIS
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
    ranked = pick_best_machines(n * 2)

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

    subprocess.run(["tmux", "kill-session", "-t", TMUX_SESSION],
                   capture_output=True)
    time.sleep(0.5)

    def wrap_cmd(machine, idx):
        remote_cmd = f"{PROGRAM_PATH} {idx} {ip_args}"
        return f"ssh {ssh_host(machine)} '{remote_cmd}' 2>&1; echo \"--- exited with code $? ---\"; read -p 'Press enter to close...'"

    subprocess.run(
        ["tmux", "new-session", "-d", "-s", TMUX_SESSION, "-n", machines[0],
         "bash", "-c", wrap_cmd(machines[0], 0)],
        check=True,
    )
    print(f"  Window 0: {machines[0]} (index=0)")

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


def stop_and_collect():
    """Send SIGINT to all nodes via SSH, wait for shutdown, capture and aggregate stats."""
    result = subprocess.run(["tmux", "has-session", "-t", TMUX_SESSION],
                            capture_output=True)
    if result.returncode != 0:
        print(f"No tmux session '{TMUX_SESSION}' found.")
        sys.exit(1)

    result = subprocess.run(
        ["tmux", "list-windows", "-t", TMUX_SESSION, "-F", "#{window_index} #{window_name}"],
        capture_output=True, text=True,
    )
    windows = []
    machines = []
    for line in result.stdout.strip().split("\n"):
        parts = line.split()
        windows.append(parts[0])
        machines.append(parts[1].rstrip("-*"))

    print(f"Sending SIGINT to {len(machines)} nodes simultaneously...")

    procs = []
    for m in machines:
        p = subprocess.Popen(
            ["ssh", "-o", "ConnectTimeout=5", "-o", "StrictHostKeyChecking=no",
             ssh_host(m), "pkill -INT -f Replicated_Hash_Table"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        procs.append(p)

    for p in procs:
        p.wait()

    print("Waiting for nodes to shut down...")
    time.sleep(15)

    total_successful = 0
    total_throughput = 0.0
    total_ops = 0
    all_p50 = []
    all_p90 = []
    all_p99 = []
    all_avg = []
    nodes_collected = 0

    for w in windows:
        result = subprocess.run(
            ["tmux", "capture-pane", "-t", f"{TMUX_SESSION}:{w}", "-p", "-S", "-200"],
            capture_output=True, text=True,
        )
        output = result.stdout

        raw = output.strip()
        if raw:
            for line in raw.split('\n'):
                if line.strip():
                    print(f"    | {line}")

        stats = parse_stats(output)
        if stats:
            nodes_collected += 1
            print(f"\n--- Node {w} ---")
            print(f"  Successful ops: {stats['successful_ops']}")
            print(f"  Throughput:     {stats['throughput']:.2f} ops/s")
            print(f"  Total ops:      {stats['total_ops']}")
            print(f"  Avg latency:    {stats['avg_latency']:.2f} us")
            print(f"  P50 latency:    {stats['p50']:.2f} us")
            print(f"  P90 latency:    {stats['p90']:.2f} us")
            print(f"  P99 latency:    {stats['p99']:.2f} us")

            total_successful += stats['successful_ops']
            total_throughput += stats['throughput']
            total_ops += stats['total_ops']
            all_avg.append((stats['avg_latency'], stats['total_ops']))
            all_p50.append(stats['p50'])
            all_p90.append(stats['p90'])
            all_p99.append(stats['p99'])
        else:
            print(f"\n--- Node {w} ---")
            print(f"  Could not parse stats")

    if nodes_collected > 0:
        weighted_avg = sum(a * n for a, n in all_avg) / sum(n for _, n in all_avg) if all_avg else 0

        print(f"\n{'=' * 42}")
        print(f"  Aggregated Stats ({nodes_collected} nodes)")
        print(f"{'=' * 42}")
        print(f"  Total successful ops:  {total_successful:,}")
        print(f"  Combined throughput:   {total_throughput:,.2f} ops/s")
        print(f"  Total operations:      {total_ops:,}")
        print(f"  Weighted avg latency:  {weighted_avg:,.2f} us")
        print(f"  Avg P50 latency:       {sum(all_p50) / len(all_p50):,.2f} us")
        print(f"  Avg P90 latency:       {sum(all_p90) / len(all_p90):,.2f} us")
        print(f"  Avg P99 latency:       {sum(all_p99) / len(all_p99):,.2f} us")
        print(f"{'=' * 42}")
    else:
        print("\nNo stats could be collected from any node.")

    subprocess.run(["tmux", "kill-session", "-t", TMUX_SESSION], capture_output=True)
    print(f"\nSession '{TMUX_SESSION}' terminated.")


def parse_stats(output: str) -> dict | None:
    """Parse performance stats from a node's stdout output."""
    patterns = {
        'successful_ops': r'Successful ops:\s+(\d+)',
        'throughput':     r'Throughput:\s+([\d.]+)',
        'total_ops':      r'Total operations:\s+(\d+)',
        'avg_latency':    r'Avg latency:\s+([\d.]+)',
        'p50':            r'P50 latency:\s+([\d.]+)',
        'p90':            r'P90 latency:\s+([\d.]+)',
        'p99':            r'P99 latency:\s+([\d.]+)',
    }

    stats = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, output)
        if not match:
            return None
        val = match.group(1)
        stats[key] = int(val) if key in ('successful_ops', 'total_ops') else float(val)

    return stats


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "stop":
        stop_and_collect()
    else:
        chosen, ips = select_and_resolve(NUM_NODES)
        launch_tmux(chosen, ips)


if __name__ == "__main__":
    main()
