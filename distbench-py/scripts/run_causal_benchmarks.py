#!/usr/bin/env python3
"""
Benchmark Runner for Causal Broadcast (RCO-broadcast)

Runs performance benchmarks for the Reliable Causal Order broadcast algorithm.
Collects metrics: latency, message counts, delivery rates

Based on run_bracha_benchmarks.py
"""

import argparse
import subprocess
import sys
from pathlib import Path
import json
import re
import time
from typing import Dict, List
import yaml


class CausalBenchmarkRunner:
    def __init__(self, config_dir: Path, output_dir: Path, trials: int, timeout: int):
        self.config_dir = config_dir
        self.output_dir = output_dir
        self.trials = trials
        self.timeout = timeout
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def run_benchmark(self, config_file: Path, trial: int) -> dict:
        """Run a single benchmark trial."""
        print(f"\n  Trial {trial + 1}/{self.trials}")
        print(f"    Config: {config_file.name}")

        # Clean up old log files before each run
        import glob
        for pattern in ["n*.txt", "bracha-n*.txt", "dolev-n*.txt"]:
            for f in glob.glob(pattern):
                try:
                    Path(f).unlink()
                except Exception:
                    pass

        # Run distbench
        cmd = [
            "uv", "run", "distbench",
            "-c", str(config_file),
            "-a", "causal",
            "--mode", "local",
            "--latency", "20-50",
            "--timeout", str(self.timeout),
        ]

        try:
            print(f"    Executing...")
            start_time = time.time()
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self.timeout + 10
            )
            elapsed = time.time() - start_time

            print(f"    Completed in {elapsed:.1f}s")

            # Parse metrics from output
            metrics = self.parse_metrics(result.stdout, result.stderr)
            metrics.update({
                "config_file": config_file.name,
                "trial": trial,
                "elapsed_time": elapsed,
                "exit_code": result.returncode
            })

            return metrics

        except subprocess.TimeoutExpired:
            print(f"    Timeout after {self.timeout}s")
            return None
        except Exception as e:
            print(f"    Error: {e}")
            return None

    def parse_metrics(self, stdout: str, stderr: str) -> dict:
        """Parse metrics from distbench output and log files."""
        from datetime import datetime
        import glob

        metrics = {
            "nodes_total": 0,
            "nodes_delivered": 0,
            "total_messages_sent": 0,
            "total_messages_received": 0,
            "avg_latency_ms": 0.0,
            "min_latency_ms": 0.0,
            "max_latency_ms": 0.0,
            "rcb_broadcast_total": 0,
            "rcb_deliver_total": 0,
        }

        # Read from log files (n*.txt) where metrics are actually written
        log_files = sorted(glob.glob("n*.txt"))
        metrics["nodes_total"] = len(log_files)

        all_deliveries = 0
        all_broadcasts = 0
        latency_values = []

        for log_file in log_files:
            try:
                with open(log_file, 'r') as f:
                    content = f.read()

                # Count deliveries
                deliveries = len(re.findall(r">>> RCB DELIVER", content))
                all_deliveries += deliveries

                # Count broadcasts
                broadcasts = len(re.findall(r"rcbBroadcast", content))
                all_broadcasts += broadcasts

                # Calculate latency from timestamps
                # Find first line timestamp (start time) and delivery timestamps
                lines = content.strip().split('\n')
                start_time = None

                for line in lines:
                    # Parse timestamp: "2026-01-08 15:40:50,843"
                    ts_match = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})", line)
                    if ts_match:
                        ts_str = ts_match.group(1)
                        ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S,%f")
                        if start_time is None:
                            start_time = ts

                        # If this is a delivery line, calculate latency
                        if ">>> RCB DELIVER" in line and start_time:
                            latency_ms = (ts - start_time).total_seconds() * 1000
                            latency_values.append(latency_ms)

            except Exception as e:
                print(f"    Warning: Error reading {log_file}: {e}")

        # Also count messages from bracha/dolev logs
        bracha_files = sorted(glob.glob("bracha-n*.txt"))
        dolev_files = sorted(glob.glob("dolev-n*.txt"))

        for bracha_file in bracha_files:
            try:
                with open(bracha_file, 'r') as f:
                    content = f.read()
                    # Count outgoing BRACHA messages (send, echo, ready)
                    bracha_sends = len(re.findall(r"BRACHA send", content))
                    metrics["total_messages_sent"] += bracha_sends
                    # Count received messages
                    recv_echo = len(re.findall(r"RECV ECHO", content))
                    recv_ready = len(re.findall(r"RECV READY", content))
                    recv_send = len(re.findall(r"RECV SEND", content))
                    metrics["total_messages_received"] += recv_echo + recv_ready + recv_send
            except Exception:
                pass

        for dolev_file in dolev_files:
            try:
                with open(dolev_file, 'r') as f:
                    content = f.read()
                    # Count all dolev message exchanges (recv entries represent actual network messages)
                    dolev_recv = len(re.findall(r"\] recv ", content))
                    # Add to total_messages_sent as measure of network message complexity
                    metrics["total_messages_sent"] += dolev_recv
                    metrics["total_messages_received"] += dolev_recv
            except Exception:
                pass

        # Set aggregated metrics
        metrics["rcb_deliver_total"] = all_deliveries
        metrics["rcb_broadcast_total"] = all_broadcasts
        metrics["nodes_delivered"] = all_deliveries

        if latency_values:
            metrics["avg_latency_ms"] = sum(latency_values) / len(latency_values)
            metrics["min_latency_ms"] = min(latency_values)
            metrics["max_latency_ms"] = max(latency_values)

        return metrics

    def run_benchmark_suite(self, config_files: List[Path]):
        """Run full benchmark suite across all configs."""
        print("\n" + "="*70)
        print("Causal Broadcast (RCO) Benchmark Suite")
        print("="*70)
        print(f"Configs: {len(config_files)}")
        print(f"Trials per config: {self.trials}")
        print(f"Total runs: {len(config_files) * self.trials}")
        print(f"Output directory: {self.output_dir}")
        print()

        all_results = []
        total_runs = len(config_files) * self.trials
        completed = 0

        for config_file in config_files:
            print(f"\n[{completed}/{total_runs}] {config_file.name}")
            print("-" * 70)

            for trial in range(self.trials):
                metrics = self.run_benchmark(config_file, trial)
                if metrics:
                    all_results.append(metrics)
                completed += 1

                # Small delay between trials
                time.sleep(1)

        # Save aggregated results
        self.save_results(all_results)
        print("\n" + "="*70)
        print(f"Benchmark suite completed: {len(all_results)}/{total_runs} successful runs")
        print(f"Results saved to: {self.output_dir}")
        print("="*70 + "\n")

        return all_results

    def save_results(self, results: List[dict]):
        """Save benchmark results to JSON."""
        # Save raw results
        results_file = self.output_dir / "raw_results.json"
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"\n Saved raw results: {results_file}")

        # Aggregate by config
        aggregated = {}
        for result in results:
            key = result['config_file']
            if key not in aggregated:
                aggregated[key] = {
                    "config_file": result["config_file"],
                    "trials": [],
                }
            aggregated[key]["trials"].append(result)

        # Compute statistics
        summary = []
        for key, data in aggregated.items():
            trials = data["trials"]
            if not trials:
                continue

            summary.append({
                "config_file": data["config_file"],
                "num_trials": len(trials),
                "avg_latency_ms": {
                    "mean": sum(t["avg_latency_ms"] for t in trials) / len(trials),
                    "min": min(t["avg_latency_ms"] for t in trials),
                    "max": max(t["avg_latency_ms"] for t in trials),
                },
                "rcb_deliver_total": {
                    "mean": sum(t["rcb_deliver_total"] for t in trials) / len(trials),
                },
                "rcb_broadcast_total": {
                    "mean": sum(t["rcb_broadcast_total"] for t in trials) / len(trials),
                },
                "nodes_delivered": {
                    "mean": sum(t["nodes_delivered"] for t in trials) / len(trials),
                },
                "total_messages_sent": {
                    "mean": sum(t["total_messages_sent"] for t in trials) / len(trials),
                    "min": min(t["total_messages_sent"] for t in trials),
                    "max": max(t["total_messages_sent"] for t in trials),
                },
            })

        # Save summary
        summary_file = self.output_dir / "summary.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        print(f" Saved summary: {summary_file}")


def main():
    parser = argparse.ArgumentParser(description='Run Causal Broadcast (RCO) performance benchmarks')
    parser.add_argument('--config-dir', '-c', type=str, default='configs/causal/benchmark',
                        help='Directory containing benchmark configs')
    parser.add_argument('--output-dir', '-o', type=str, default='results/causal/metrics',
                        help='Output directory for results')
    parser.add_argument('--trials', '-t', type=int, default=3,
                        help='Number of trials per configuration')
    parser.add_argument('--timeout', type=int, default=60,
                        help='Timeout per trial in seconds')

    args = parser.parse_args()

    config_dir = Path(args.config_dir)
    output_dir = Path(args.output_dir)

    if not config_dir.exists():
        print(f"ERROR: Config directory not found: {config_dir}")
        sys.exit(1)

    # Find all benchmark configs
    config_files = sorted(config_dir.glob("*.yaml"))
    if not config_files:
        print(f"ERROR: No config files found in {config_dir}")
        sys.exit(1)

    print(f"Found {len(config_files)} config files:")
    for cf in config_files:
        print(f"  - {cf.name}")

    # Run benchmarks
    runner = CausalBenchmarkRunner(config_dir, output_dir, args.trials, args.timeout)
    results = runner.run_benchmark_suite(config_files)

    if not results:
        print("No successful benchmark runs")
        sys.exit(1)

    print(f"\nBenchmark complete! Results in: {output_dir}")
    print("\nNext steps:")
    print(f"  1. Analyze results: uv run python scripts/analyze_causal_results.py -r {output_dir} -o results/causal/plots")
    print("  2. Review plots in results/causal/plots/")


if __name__ == '__main__':
    main()
