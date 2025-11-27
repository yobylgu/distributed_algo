#!/usr/bin/env python3
"""
Configuration Generator for Dolev Byzantine Reliable Broadcast Benchmarks

Generates YAML configuration files for varying parameters: n, f, k.
"""

import argparse
import yaml
from pathlib import Path
from typing import List


def generate_ring_topology(n: int, k: int) -> dict:
    """Generate a ring topology with k-connectivity."""
    neighbours = {}
    for i in range(n):
        node_neighbours = []
        for j in range(1, k + 1):
            # Add neighbors on both sides of the ring
            if j <= k // 2:
                node_neighbours.append(f"n{(i + j) % n}")
                node_neighbours.append(f"n{(i - j) % n}")
            elif k % 2 == 1 and j == k // 2 + 1:
                node_neighbours.append(f"n{(i + j) % n}")
        neighbours[f"n{i}"] = list(set(node_neighbours))[:k]
    return neighbours


def generate_config(n: int, f: int, k: int, num_byzantine: int = 0, 
                   num_senders: int = 1, output_path: Path = None):
    """Generate a Dolev configuration file."""
    config = {}
    topology = generate_ring_topology(n, k)
    
    # Determine which nodes are Byzantine
    byzantine_nodes = [f"n{i}" for i in range(n - num_byzantine, n)]
    
    # Determine which nodes are senders
    sender_nodes = [f"n{i}" for i in range(num_senders)]
    
    for i in range(n):
        node_id = f"n{i}"
        config[node_id] = {
            'host': '127.0.0.1',
            'port': 10000 + i,
            'neighbours': topology[node_id],
            'is_sender': node_id in sender_nodes,
            'payload': 'hello',
            'behavior_mode': 'BYZANTINE_SILENT' if node_id in byzantine_nodes else 'HONEST',
            'f': f
        }
    
    if output_path:
        with open(output_path, 'w') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
        print(f"Generated: {output_path}")
    
    return config


def main():
    parser = argparse.ArgumentParser(description='Generate Dolev benchmark configurations')
    parser.add_argument('--output-dir', '-o', type=str, default='configs/benchmark',
                        help='Output directory for generated configs')
    parser.add_argument('--mode', '-m', choices=['n', 'byzantine', 'connectivity', 'all'], required=True,
                        help='Generation mode: vary n, byzantine nodes, connectivity, or all')
    
    args = parser.parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    if args.mode == 'n' or args.mode == 'all':
        print("Generating configs with varying n (number of processes)...")
        f = 1
        k = 2 * f + 1  # k = 3
        for n in [4, 7, 10, 13, 16]:
            output_path = output_dir / f'n_{n}_f_{f}_k_{k}.yaml'
            generate_config(n, f, k, num_byzantine=0, output_path=output_path)
    
    if args.mode == 'byzantine' or args.mode == 'all':
        print("Generating configs with varying Byzantine nodes...")
        n = 10
        f = 1
        k = 2 * f + 1
        for num_byz in [0, 1]:
            output_path = output_dir / f'n_{n}_f_{f}_k_{k}_byz_{num_byz}.yaml'
            generate_config(n, f, k, num_byzantine=num_byz, output_path=output_path)
    
    if args.mode == 'connectivity' or args.mode == 'all':
        print("Generating configs with varying connectivity...")
        n = 10
        f = 1
        for k in [3, 4, 5, 6]:
            output_path = output_dir / f'n_{n}_f_{f}_k_{k}.yaml'
            generate_config(n, f, k, num_byzantine=0, output_path=output_path)
    
    print(f"\nAll configs generated in: {output_dir}")


if __name__ == '__main__':
    main()
