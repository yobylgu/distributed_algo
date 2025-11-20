#!/usr/bin/env python3
"""
Quick test of the benchmarking system with a single small configuration.
"""

import json
import subprocess
import sys
from pathlib import Path
import yaml

# Import functions from benchmark.py
from benchmark import (
    generate_config,
    generate_docker_config,
    generate_docker_compose,
    parse_metrics_from_logs,
    aggregate_metrics
)

def test_config_generation():
    """Test configuration generation."""
    print("Testing configuration generation...")
    config = generate_config(N=4, f=1, senders=1, byzantine=0)
    assert len(config) == 4
    assert config["n0"]["is_sender"] == True
    assert config["n1"]["is_sender"] == False
    print("✓ Configuration generation works")

def test_docker_compose_generation():
    """Test docker-compose generation."""
    print("Testing docker-compose generation...")
    compose_yaml = generate_docker_compose(N=4, config_name="test")
    assert "n0:" in compose_yaml
    assert "n3:" in compose_yaml
    assert "dolev_network" in compose_yaml
    print("✓ Docker-compose generation works")

def test_metrics_parsing():
    """Test metrics parsing."""
    print("Testing metrics parsing...")
    sample_logs = [
        '[n0] INFO: METRICS_JSON: {"node_id": "n0", "total_messages_sent": 10, "avg_latency_ms": 5.2}',
        '[n1] INFO: METRICS_JSON: {"node_id": "n1", "total_messages_sent": 12, "avg_latency_ms": 6.1}',
        'Some other log line',
    ]
    metrics = parse_metrics_from_logs(sample_logs)
    assert len(metrics) == 2
    assert metrics[0]["node_id"] == "n0"
    print("✓ Metrics parsing works")

def test_aggregation():
    """Test metrics aggregation."""
    print("Testing metrics aggregation...")
    metrics = [
        {"node_id": "n0", "total_messages_sent": 10, "avg_latency_ms": 5.0, "delivered_count": 1, "behavior_mode": "HONEST"},
        {"node_id": "n1", "total_messages_sent": 12, "avg_latency_ms": 6.0, "delivered_count": 1, "behavior_mode": "HONEST"},
    ]
    agg = aggregate_metrics(metrics)
    assert agg["total_messages"] == 22
    assert agg["avg_latency_ms"] == 5.5
    print("✓ Metrics aggregation works")

def main():
    """Run all tests."""
    print("="*60)
    print("Testing Benchmark System Components")
    print("="*60 + "\n")

    try:
        test_config_generation()
        test_docker_compose_generation()
        test_metrics_parsing()
        test_aggregation()

        print("\n" + "="*60)
        print("✓ All tests passed!")
        print("="*60)
        print("\nYou can now run the full benchmark suite with:")
        print("  python benchmark.py")
        return 0

    except AssertionError as e:
        print(f"\n✗ Test failed: {e}")
        return 1
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
