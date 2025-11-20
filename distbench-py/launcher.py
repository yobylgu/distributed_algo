#!/usr/bin/env python3
"""
Topology and configuration generator for Dolev's Reliable Broadcast testing.

Generates YAML config files for different test scenarios.
"""

import argparse
import random
import yaml
from pathlib import Path
from typing import Dict, List, Set


def generate_complete_graph(n: int) -> Dict[str, List[str]]:
    """Generate a complete graph (fully connected)."""
    nodes = [f"n{i}" for i in range(n)]
    topology = {}
    for node in nodes:
        topology[node] = [other for other in nodes if other != node]
    return topology


def generate_min_connected_graph(n: int, f: int, seed: int = 42) -> Dict[str, List[str]]:
    """
    Generate a (2f+1)-connected graph.
    For simplicity, we create a complete graph minus some random edges,
    ensuring minimum connectivity.
    """
    random.seed(seed)
    # Start with complete graph and ensure minimum connectivity
    topology = generate_complete_graph(n)
    return topology


def generate_config(
    n: int,
    f: int,
    senders: List[str],
    byzantine_nodes: Dict[str, str],
    delay_min: float = 0.0,
    delay_max: float = 0.0,
    payloads: Dict[str, str] = None
) -> Dict[str, Dict]:
    """
    Generate a config dictionary for all nodes.

    Args:
        n: Total number of nodes
        f: Maximum number of Byzantine nodes
        senders: List of node IDs that should broadcast
        byzantine_nodes: Dict mapping node_id -> behavior_mode (BYZANTINE_SILENT, BYZANTINE_SPOOF)
        delay_min: Minimum network delay
        delay_max: Maximum network delay
        payloads: Optional dict mapping node_id -> custom payload
    """
    config = {}

    for i in range(n):
        node_id = f"n{i}"
        node_config = {
            "neighbours": [],  # Empty list = fully connected
            "f": f,
            "is_sender": node_id in senders,
            "delay_min": delay_min,
            "delay_max": delay_max,
        }

        # Set payload
        if payloads and node_id in payloads:
            node_config["payload"] = payloads[node_id]
        elif node_id in senders:
            node_config["payload"] = f"msg_from_{node_id}"
        else:
            node_config["payload"] = "hello"

        # Set behavior mode
        if node_id in byzantine_nodes:
            node_config["behavior_mode"] = byzantine_nodes[node_id]
        else:
            node_config["behavior_mode"] = "HONEST"

        config[node_id] = node_config

    return config


def save_config(config: Dict, filename: str, configs_dir: Path = Path("configs")):
    """Save config to YAML file."""
    configs_dir.mkdir(exist_ok=True)
    filepath = configs_dir / filename
    with open(filepath, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)
    print(f"Generated: {filepath}")


def generate_test_scenarios():
    """Generate all 5 test scenario configurations."""

    # Scenario 1: Concurrency - Multiple nodes broadcasting simultaneously
    print("\n=== Scenario 1: Concurrency ===")
    config1 = generate_config(
        n=7,
        f=2,
        senders=["n0", "n1", "n2"],  # 3 simultaneous senders
        byzantine_nodes={},
        delay_min=0.01,
        delay_max=0.05,
        payloads={"n0": "msg_A", "n1": "msg_B", "n2": "msg_C"}
    )
    save_config(config1, "test_concurrency.yaml")

    # Scenario 2: Volume - One node broadcasting multiple messages
    print("\n=== Scenario 2: Volume ===")
    # Note: For volume testing, we'll send multiple messages from code
    # This config just sets up a single sender
    config2 = generate_config(
        n=7,
        f=2,
        senders=["n0"],
        byzantine_nodes={},
        delay_min=0.01,
        delay_max=0.03,
    )
    save_config(config2, "test_volume.yaml")

    # Scenario 3: Integrity - Byzantine node spoofs message
    print("\n=== Scenario 3: Integrity (Byzantine Spoof) ===")
    config3 = generate_config(
        n=7,
        f=2,
        senders=["n0"],  # n0 is honest sender
        byzantine_nodes={"n6": "BYZANTINE_SPOOF"},  # n6 tries to spoof
        delay_min=0.01,
        delay_max=0.05,
    )
    save_config(config3, "test_integrity.yaml")

    # Scenario 4: No Totality - Byzantine sender sends to subset
    print("\n=== Scenario 4: No Totality ===")
    # Note: Current implementation doesn't support selective sending
    # We'll use BYZANTINE_SPOOF to demonstrate partial/fake messages
    config4 = generate_config(
        n=7,
        f=2,
        senders=[],  # No honest senders
        byzantine_nodes={"n0": "BYZANTINE_SPOOF"},  # Byzantine broadcaster
        delay_min=0.01,
        delay_max=0.05,
    )
    save_config(config4, "test_no_totality.yaml")

    # Scenario 5: Fault Tolerance - f Byzantine silent nodes
    print("\n=== Scenario 5: Fault Tolerance ===")
    config5 = generate_config(
        n=7,  # N = 3f + 1 = 7 for f=2
        f=2,
        senders=["n0", "n1"],  # 2 honest senders
        byzantine_nodes={
            "n5": "BYZANTINE_SILENT",
            "n6": "BYZANTINE_SILENT",
        },  # f=2 silent Byzantine nodes
        delay_min=0.01,
        delay_max=0.05,
    )
    save_config(config5, "test_fault_tolerance.yaml")

    print("\n=== All test scenarios generated ===")


def main():
    parser = argparse.ArgumentParser(
        description="Generate configurations for Dolev's Reliable Broadcast testing"
    )
    parser.add_argument(
        "--generate-tests",
        action="store_true",
        help="Generate all 5 test scenario configs"
    )
    parser.add_argument(
        "--custom",
        action="store_true",
        help="Generate custom configuration"
    )
    parser.add_argument("-n", "--nodes", type=int, default=7, help="Number of nodes")
    parser.add_argument("-f", "--byzantine", type=int, default=2, help="Max Byzantine nodes")
    parser.add_argument("--output", type=str, default="custom.yaml", help="Output filename")

    args = parser.parse_args()

    if args.generate_tests:
        generate_test_scenarios()
    elif args.custom:
        config = generate_config(
            n=args.nodes,
            f=args.byzantine,
            senders=["n0"],
            byzantine_nodes={},
        )
        save_config(config, args.output)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
