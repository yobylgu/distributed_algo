#!/usr/bin/env python3
"""
Automated benchmarking suite for Dolev's Reliable Broadcast Algorithm.

This script runs the algorithm with varying parameters (N, f, Byzantine nodes, etc.),
collects metrics, and outputs results for analysis and plotting.
"""

import asyncio
import json
import os
import random
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Any
import yaml

# Benchmark configurations
BENCHMARKS = {
    "scalability": [
        {"name": "N4_f1", "N": 4, "f": 1, "senders": 1, "byzantine": 0},
        {"name": "N7_f2", "N": 7, "f": 2, "senders": 1, "byzantine": 0},
        {"name": "N10_f3", "N": 10, "f": 3, "senders": 1, "byzantine": 0},
        {"name": "N13_f4", "N": 13, "f": 4, "senders": 1, "byzantine": 0},
    ],
    "fault_impact": [
        {"name": "f3_byz0", "N": 10, "f": 3, "senders": 1, "byzantine": 0},
        {"name": "f3_byz1", "N": 10, "f": 3, "senders": 1, "byzantine": 1},
        {"name": "f3_byz2", "N": 10, "f": 3, "senders": 1, "byzantine": 2},
        {"name": "f3_byz3", "N": 10, "f": 3, "senders": 1, "byzantine": 3},
    ],
    "concurrency": [
        {"name": "concurrent_3", "N": 7, "f": 2, "senders": 3, "byzantine": 0},
        {"name": "concurrent_5", "N": 10, "f": 3, "senders": 5, "byzantine": 0},
    ],
}

def generate_random_topology(n, f):
    """
    Generates a random (2f+1)-connected topology with bidirectional edges.
    """
    min_deg = 2 * f + 1
    if min_deg >= n:
        raise ValueError("2f+1 must be < n for Dolev connectivity")

    neighbours = {f"n{i}": set() for i in range(n)}

    for i in range(n):
        node = f"n{i}"
        candidates = [f"n{j}" for j in range(n) if j != i]
        chosen = random.sample(candidates, min_deg)
        neighbours[node].update(chosen)

    for a, neighs in neighbours.items():
        for b in neighs:
            neighbours[b].add(a)

    return neighbours

def generate_config(N: int, f: int, senders: int, byzantine: int) -> Dict[str, Any]:
    """
    Generate a YAML configuration for N nodes with specified parameters.

    Args:
        N: Total number of nodes
        f: Maximum Byzantine nodes tolerated
        senders: Number of sender nodes (first 'senders' nodes will send)
        byzantine: Number of actual Byzantine SILENT nodes (last 'byzantine' nodes)

    Returns:
        Configuration dictionary
    """
    config = {}

    topology = generate_random_topology(N, f)

    for i in range(N):
        node_id = f"n{i}"
        is_sender = i < senders
        is_byzantine = i >= (N - byzantine)

        config[node_id] = {
            "neighbours": [],
            "f": f,
            "is_sender": is_sender,
            "payload": f"msg_from_{node_id}" if is_sender else "hello",
            "neighbours": sorted(list(topology[node_id])),
            "delay_min": 0.01,
            "delay_max": 0.05,
            "behavior_mode": "BYZANTINE_SILENT" if is_byzantine else "HONEST",
            "num_messages": 1,
        }

    return config


def generate_docker_compose(N: int, config_name: str) -> str:
    """
    Generate a docker-compose.yaml content for N nodes.

    Args:
        N: Number of nodes
        config_name: Name of the config file (without extension)

    Returns:
        Docker compose YAML content as string
    """
    compose = {
        "services": {},
        "networks": {
            "dolev_network": {
                "driver": "bridge"
            }
        }
    }

    for i in range(N):
        node_id = f"n{i}"
        compose["services"][node_id] = {
            "build": ".",
            "container_name": node_id,
            "hostname": node_id,
            "command": f"python -m distbench.main -c /app/configs/benchmark/{config_name}.yaml -a dolev --mode network --id {node_id} -v",
            "volumes": [
                "./configs:/app/configs",
                "./logs:/app/logs"
            ],
            "networks": ["dolev_network"],
            "environment": ["PYTHONUNBUFFERED=1"]
        }

    return yaml.dump(compose, default_flow_style=False)


def generate_docker_config(config: Dict[str, Any], N: int) -> Dict[str, Any]:
    """
    Add host and port information for Docker network mode.

    Args:
        config: Base configuration
        N: Number of nodes

    Returns:
        Docker-compatible configuration
    """
    docker_config = {}
    for i in range(N):
        node_id = f"n{i}"
        docker_config[node_id] = {
            **config[node_id],
            "host": node_id,
            "port": 10000
        }
    return docker_config


def run_docker_benchmark(config_name: str, N: int, timeout: int = 60) -> List[str]:
    """
    Run a benchmark using Docker Compose and capture logs.

    Args:
        config_name: Name of the configuration
        N: Number of nodes
        timeout: Timeout in seconds

    Returns:
        List of log lines
    """
    print(f"  Starting Docker containers for {config_name}...")

    # Generate docker-compose file
    compose_content = generate_docker_compose(N, config_name)
    compose_path = Path("docker-compose-benchmark.yaml")
    compose_path.write_text(compose_content)

    try:
        # Run docker compose
        result = subprocess.run(
            ["docker", "compose", "-f", "docker-compose-benchmark.yaml", "up", "--abort-on-container-exit"],
            capture_output=True,
            text=True,
            timeout=timeout
        )

        logs = result.stdout + result.stderr

        return logs.split("\n")

    except subprocess.TimeoutExpired:
        print(f"  WARNING: Benchmark {config_name} timed out")
        return []

    finally:
        # Clean up compose file
        if compose_path.exists():
            compose_path.unlink()

def save_node_logs(config_name: str, N: int):
    """
    Fetch logs from each node container and save them to:
      logs/benchmark/<experiment>/nX.txt
    """
    outdir = Path(f"logs/benchmark/{config_name}")
    outdir.mkdir(parents=True, exist_ok=True)

    for i in range(N):
        node_id = f"n{i}"
        log_path = outdir / f"{node_id}.txt"

        # fetch logs
        result = subprocess.run(
            ["docker", "logs", node_id],
            capture_output=True,
            text=True
        )

        with open(log_path, "w") as f:
            f.write(result.stdout)
            f.write(result.stderr)



def parse_metrics_from_logs(logs: List[str]) -> List[Dict[str, Any]]:
    """
    Extract METRICS_JSON lines from logs.

    Args:
        logs: List of log lines

    Returns:
        List of metrics dictionaries
    """
    metrics = []
    pattern = re.compile(r'METRICS_JSON: ({.*})')

    for line in logs:
        match = pattern.search(line)
        if match:
            try:
                metric_data = json.loads(match.group(1))
                metrics.append(metric_data)
            except json.JSONDecodeError as e:
                print(f"  WARNING: Failed to parse metrics JSON: {e}")

    return metrics


def aggregate_metrics(metrics_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Aggregate metrics from all nodes.

    Args:
        metrics_list: List of per-node metrics

    Returns:
        Aggregated metrics
    """
    if not metrics_list:
        return {
            "avg_latency_ms": 0,
            "total_messages": 0,
            "delivered_count": 0,
            "honest_nodes": 0,
            "byzantine_nodes": 0
        }

    # Filter honest nodes for latency calculation
    honest_metrics = [m for m in metrics_list if m.get("behavior_mode") == "HONEST"]

    latencies = [m["avg_latency_ms"] for m in honest_metrics if m.get("avg_latency_ms", 0) > 0]
    total_messages = sum(m["total_messages_sent"] for m in metrics_list)
    delivered = [m["delivered_count"] for m in honest_metrics]

    return {
        "avg_latency_ms": sum(latencies) / len(latencies) if latencies else 0,
        "total_messages": total_messages,
        "avg_delivered": sum(delivered) / len(delivered) if delivered else 0,
        "honest_nodes": len(honest_metrics),
        "byzantine_nodes": len([m for m in metrics_list if m.get("behavior_mode") != "HONEST"])
    }


def run_benchmark_suite():
    """Run all benchmarks and save results to CSV."""
    print("=" * 60)
    print("Dolev's Reliable Broadcast - Benchmark Suite")
    print("=" * 60)

    # Ensure output directories exist
    Path("configs/benchmark").mkdir(parents=True, exist_ok=True)
    Path("logs/benchmark").mkdir(parents=True, exist_ok=True)

    results = []

    for category, configs in BENCHMARKS.items():
        print(f"\n### Running {category} benchmarks ###\n")

        for config_spec in configs:
            name = config_spec["name"]
            N = config_spec["N"]
            f = config_spec["f"]
            senders = config_spec["senders"]
            byzantine = config_spec["byzantine"]

            print(f"[{name}] N={N}, f={f}, senders={senders}, byzantine={byzantine}")

            # Generate configuration
            config = generate_config(N, f, senders, byzantine)
            docker_config = generate_docker_config(config, N)

            # Save configuration
            config_path = Path(f"configs/benchmark/{name}.yaml")
            config_path.write_text(yaml.dump(docker_config))

            # Run benchmark
            logs = run_docker_benchmark(name, N, timeout=30)
            save_node_logs(name, N)
            # Parse metrics
            metrics = parse_metrics_from_logs(logs)

            if not metrics:
                print(f"  WARNING: No metrics collected for {name}")
                continue

            # Aggregate results
            agg = aggregate_metrics(metrics)

            result_row = {
                "experiment": category,
                "name": name,
                "N": N,
                "f": f,
                "senders": senders,
                "byzantine_count": byzantine,
                "honest_nodes": agg["honest_nodes"],
                "avg_latency_ms": round(agg["avg_latency_ms"], 3),
                "total_messages": agg["total_messages"],
                "avg_delivered": round(agg["avg_delivered"], 2),
            }

            results.append(result_row)

            print(f"  ✓ Latency: {result_row['avg_latency_ms']:.2f}ms, "
                  f"Messages: {result_row['total_messages']}, "
                  f"Delivered: {result_row['avg_delivered']:.1f}")

            # Save raw metrics
            with open(f"logs/benchmark/{name}_metrics.json", "w") as f:
                json.dump(metrics, f, indent=2)

            # Small delay between benchmarks
            time.sleep(2)

    # Save results to CSV
    if results:
        import csv
        csv_path = Path("results.csv")
        fieldnames = results[0].keys()

        with open(csv_path, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)

        print(f"\n✓ Results saved to {csv_path}")
        print(f"  Total benchmarks: {len(results)}")
    else:
        print("\n✗ No results collected")
        sys.exit(1)


if __name__ == "__main__":
    run_benchmark_suite()
