#!/usr/bin/env python3

import os
import re
import signal
import subprocess
import sys
import time

# ─── Configuration ───────────────────────────────────────────────────────────
PROGRAM_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "build", "Replicated_Hash_Table")
NUM_NODES = 6
TMUX_SESSION = "rht"
BASE_PORT = 7000          # Each node gets BASE_PORT + index
# ─────────────────────────────────────────────────────────────────────────────


def launch_tmux():
    """Launch NUM_NODES local processes in a tmux session, each on a different port."""
    ports = [BASE_PORT + i for i in range(NUM_NODES)]
    addr_args = " ".join(f"127.0.0.1:{p}" for p in ports)

    subprocess.run(["tmux", "kill-session", "-t", TMUX_SESSION],
                   capture_output=True)
    time.sleep(0.3)

    for idx in range(NUM_NODES):
        cmd = f"{PROGRAM_PATH} {idx} {ports[idx]} {addr_args}"
        bash_cmd = f"{cmd} 2>&1; echo '--- exited with code '$?' ---'; read -p 'Press enter to close...'"

        if idx == 0:
            subprocess.run(
                ["tmux", "new-session", "-d", "-s", TMUX_SESSION, "-n", f"node{idx}",
                 "bash", "-c", bash_cmd],
                check=True,
            )
        else:
            subprocess.run(
                ["tmux", "new-window", "-t", TMUX_SESSION, "-n", f"node{idx}",
                 "bash", "-c", bash_cmd],
                check=True,
            )
        print(f"  Window {idx}: node{idx} (port={ports[idx]})")

    print(f"\nAll {NUM_NODES} nodes launched in tmux session '{TMUX_SESSION}'.")
    print(f"Attach with:  tmux attach -t {TMUX_SESSION}")


def stop_and_collect():
    """Send SIGINT to all node processes, wait for shutdown, capture and aggregate stats."""
    result = subprocess.run(["tmux", "has-session", "-t", TMUX_SESSION],
                            capture_output=True)
    if result.returncode != 0:
        print(f"No tmux session '{TMUX_SESSION}' found.")
        sys.exit(1)

    # Get window list
    result = subprocess.run(
        ["tmux", "list-windows", "-t", TMUX_SESSION, "-F", "#{window_index} #{window_name}"],
        capture_output=True, text=True,
    )
    windows = []
    for line in result.stdout.strip().split("\n"):
        parts = line.split()
        windows.append(parts[0])

    # Find the actual program PIDs (exclude bash wrappers, grep, pgrep)
    print(f"Sending SIGINT to {len(windows)} nodes...")
    result = subprocess.run(
        ["pgrep", "-x", "Replicated_Has"],  # pgrep matches up to 15 chars of comm
        capture_output=True, text=True,
    )
    pids = []
    if result.stdout.strip():
        for line in result.stdout.strip().split("\n"):
            line = line.strip()
            if line:
                pids.append(int(line))

    if not pids:
        # Fallback: use pidof
        result = subprocess.run(["pidof", "Replicated_Hash_Table"],
                                capture_output=True, text=True)
        if result.stdout.strip():
            pids = [int(p) for p in result.stdout.strip().split()]

    for pid in pids:
        try:
            os.kill(pid, signal.SIGINT)
        except ProcessLookupError:
            pass

    print(f"  Sent SIGINT to PIDs: {pids}")
    print("Waiting for nodes to shut down...")
    time.sleep(5)

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
        launch_tmux()


if __name__ == "__main__":
    main()
