#!/usr/bin/env python3
"""
Benchmark Runner for Bracha Byzantine Reliable Broadcast

Runs performance benchmarks comparing optimization combinations:
1. Baseline (standard bracha)
2. Echo Amplification only
3. Echo Amplification + Single-hop Send
4. Full optimization (all three)

Collects metrics: latency, message counts, delivery rates
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


# Optimization configurations to test
OPTIMIZATION_CONFIGS = {
    "baseline": {
        "algorithm": "bracha",
        "flags": {},
        "description": "Standard Bracha (no optimizations)"
    },
    "echo_amp": {
        "algorithm": "bracha_optimized",
        "flags": {
            "enable_echo_amplification": True,
            "enable_single_hop_send": False,
            "enable_reduced_messages": False
        },
        "description": "Echo Amplification only"
    },
    "echo_amp_single_hop": {
        "algorithm": "bracha_optimized",
        "flags": {
            "enable_echo_amplification": True,
            "enable_single_hop_send": True,
            "enable_reduced_messages": False
        },
        "description": "Echo Amp + Single-hop Send"
    },
    "reduced_messages": {
        "algorithm": "bracha_optimized",
        "flags": {
            "enable_echo_amplification": False,
            "enable_single_hop_send": False,
            "enable_reduced_messages": True
        },
        "description": "Reduced Messages only (MBD.11)"
    },
    "full_optimized": {
        "algorithm": "bracha_optimized",
        "flags": {
            "enable_echo_amplification": True,
            "enable_single_hop_send": True,
            "enable_reduced_messages": True
        },
        "description": "All optimizations"
    }
}


class BenchmarkRunner:
    def __init__(self, config_dir: Path, output_dir: Path, trials: int, timeout: int):
        self.config_dir = config_dir
        self.output_dir = output_dir
        self.trials = trials
        self.timeout = timeout
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def modify_config_for_optimization(self, config_path: Path, opt_config: dict, trial: int) -> Path:
        """Create a modified config file with optimization flags."""
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        # Add optimization flags to all nodes
        for node_id, node_config in config.items():
            if opt_config["flags"]:
                node_config.update(opt_config["flags"])

        # Save modified config
        config_name = config_path.stem
        opt_name = list(OPTIMIZATION_CONFIGS.keys())[
            list(OPTIMIZATION_CONFIGS.values()).index(opt_config)
        ]
        modified_path = self.output_dir / f"temp_{config_name}_{opt_name}_trial{trial}.yaml"

        with open(modified_path, 'w') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)

        return modified_path

    def run_benchmark(self, config_file: Path, opt_name: str, opt_config: dict, trial: int):
        """Run a single benchmark trial."""
        print(f"\n  Trial {trial + 1}/{self.trials}")
        print(f"    Config: {config_file.name}")
        print(f"    Optimization: {opt_config['description']}")

        # Modify config with optimization flags
        modified_config = self.modify_config_for_optimization(config_file, opt_config, trial)

        # Run distbench
        cmd = [
            "uv", "run", "distbench",
            "-c", str(modified_config),
            "-a", opt_config["algorithm"],
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

            print(f"    ✓ Completed in {elapsed:.1f}s")

            # Parse metrics from output
            metrics = self.parse_metrics(result.stdout, result.stderr)
            metrics.update({
                "config_file": config_file.name,
                "optimization": opt_name,
                "trial": trial,
                "elapsed_time": elapsed,
                "exit_code": result.returncode
            })

            # Clean up temp config
            modified_config.unlink(missing_ok=True)

            return metrics

        except subprocess.TimeoutExpired:
            print(f"    ❌ Timeout after {self.timeout}s")
            modified_config.unlink(missing_ok=True)
            return None
        except Exception as e:
            print(f"    ❌ Error: {e}")
            modified_config.unlink(missing_ok=True)
            return None

    def parse_metrics(self, stdout: str, stderr: str) -> dict:
        """Parse metrics from distbench output."""
        output = stdout + stderr

        # Extract metrics from algorithm report
        metrics = {
            "nodes_total": 0,
            "nodes_delivered": 0,
            "total_messages_sent": 0,
            "total_messages_received": 0,
            "avg_latency_ms": 0.0,
            "min_latency_ms": 0.0,
            "max_latency_ms": 0.0,
        }

        # Parse node-level metrics
        messages_sent_values = []
        messages_received_values = []
        latency_values = []

        # Look for METRICS_JSON or individual metric lines
        metrics_pattern = r"'messages_sent': '(\d+)'"
        messages_sent_matches = re.findall(metrics_pattern, output)
        if messages_sent_matches:
            messages_sent_values = [int(x) for x in messages_sent_matches]

        metrics_pattern = r"'messages_received': '(\d+)'"
        messages_received_matches = re.findall(metrics_pattern, output)
        if messages_received_matches:
            messages_received_values = [int(x) for x in messages_received_matches]

        metrics_pattern = r"'avg_latency_ms': '([\d.]+)'"
        latency_matches = re.findall(metrics_pattern, output)
        if latency_matches:
            latency_values = [float(x) for x in latency_matches if float(x) > 0]

        # Count deliveries
        delivered_pattern = r">>> BRACHA DELIVERED"
        deliveries = len(re.findall(delivered_pattern, output))

        # Aggregate metrics
        if messages_sent_values:
            metrics["total_messages_sent"] = sum(messages_sent_values)
        if messages_received_values:
            metrics["total_messages_received"] = sum(messages_received_values)
        if latency_values:
            metrics["avg_latency_ms"] = sum(latency_values) / len(latency_values)
            metrics["min_latency_ms"] = min(latency_values)
            metrics["max_latency_ms"] = max(latency_values)

        metrics["nodes_delivered"] = deliveries

        return metrics

    def run_benchmark_suite(self, config_files: List[Path]):
        """Run full benchmark suite across all configs and optimizations."""
        print("\n" + "="*70)
        print("Bracha BRB Benchmark Suite - Part C Performance Testing")
        print("="*70)
        print(f"Configs: {len(config_files)}")
        print(f"Optimizations: {len(OPTIMIZATION_CONFIGS)}")
        print(f"Trials per config: {self.trials}")
        print(f"Total runs: {len(config_files) * len(OPTIMIZATION_CONFIGS) * self.trials}")
        print(f"Output directory: {self.output_dir}")
        print()

        all_results = []
        total_runs = len(config_files) * len(OPTIMIZATION_CONFIGS) * self.trials
        completed = 0

        for config_file in config_files:
            for opt_name, opt_config in OPTIMIZATION_CONFIGS.items():
                print(f"\n[{completed}/{total_runs}] {config_file.name} - {opt_config['description']}")
                print("-" * 70)

                for trial in range(self.trials):
                    metrics = self.run_benchmark(config_file, opt_name, opt_config, trial)
                    if metrics:
                        all_results.append(metrics)
                    completed += 1

                    # Small delay between trials
                    time.sleep(1)

        # Save aggregated results
        self.save_results(all_results)
        print("\n" + "="*70)
        print(f"✅ Benchmark suite completed: {len(all_results)}/{total_runs} successful runs")
        print(f"Results saved to: {self.output_dir}")
        print("="*70 + "\n")

        return all_results

    def save_results(self, results: List[dict]):
        """Save benchmark results to JSON."""
        # Save raw results
        results_file = self.output_dir / "raw_results.json"
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"\n✓ Saved raw results: {results_file}")

        # Aggregate by config and optimization
        aggregated = {}
        for result in results:
            key = f"{result['config_file']}_{result['optimization']}"
            if key not in aggregated:
                aggregated[key] = {
                    "config_file": result["config_file"],
                    "optimization": result["optimization"],
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
                "optimization": data["optimization"],
                "num_trials": len(trials),
                "avg_latency_ms": {
                    "mean": sum(t["avg_latency_ms"] for t in trials) / len(trials),
                    "min": min(t["avg_latency_ms"] for t in trials),
                    "max": max(t["avg_latency_ms"] for t in trials),
                },
                "total_messages_sent": {
                    "mean": sum(t["total_messages_sent"] for t in trials) / len(trials),
                    "min": min(t["total_messages_sent"] for t in trials),
                    "max": max(t["total_messages_sent"] for t in trials),
                },
                "total_messages_received": {
                    "mean": sum(t["total_messages_received"] for t in trials) / len(trials),
                },
                "nodes_delivered": {
                    "mean": sum(t["nodes_delivered"] for t in trials) / len(trials),
                },
            })

        # Save summary
        summary_file = self.output_dir / "summary.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        print(f"✓ Saved summary: {summary_file}")


def main():
    parser = argparse.ArgumentParser(description='Run Bracha BRB performance benchmarks')
    parser.add_argument('--config-dir', '-c', type=str, default='configs/bracha/benchmark',
                        help='Directory containing benchmark configs')
    parser.add_argument('--output-dir', '-o', type=str, default='results/bracha/metrics',
                        help='Output directory for results')
    parser.add_argument('--trials', '-t', type=int, default=5,
                        help='Number of trials per configuration')
    parser.add_argument('--timeout', type=int, default=30,
                        help='Timeout per trial in seconds')

    args = parser.parse_args()

    config_dir = Path(args.config_dir)
    output_dir = Path(args.output_dir)

    if not config_dir.exists():
        print(f"ERROR: Config directory not found: {config_dir}")
        print("Run: uv run python scripts/generate_configs.py --algorithm bracha --mode benchmark")
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
    runner = BenchmarkRunner(config_dir, output_dir, args.trials, args.timeout)
    results = runner.run_benchmark_suite(config_files)

    if not results:
        print("❌ No successful benchmark runs")
        sys.exit(1)

    print(f"\n✅ Benchmark complete! Results in: {output_dir}")
    print("\nNext steps:")
    print(f"  1. Analyze results: uv run python scripts/analyze_bracha_results.py -r {output_dir} -o results/bracha/plots")
    print("  2. Review plots in results/bracha/plots/")


if __name__ == '__main__':
    main()
