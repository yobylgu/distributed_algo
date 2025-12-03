#!/usr/bin/env python3
"""
Configuration Generator for Byzantine Reliable Broadcast Benchmarks

Generates YAML configuration files for varying parameters: n, f, k.
Supports both Dolev and Bracha algorithms with ring or random regular graph topologies.
"""

import argparse
import yaml
from pathlib import Path
from typing import List

try:
    import networkx as nx
except ImportError:
    print("Warning: networkx not installed. Run: uv add networkx")
    nx = None


def generate_topology(n: int, f: int, topology_type: str = 'random_regular') -> dict:
    """
    Generate network topology with degree d ≥ 2f+1 for Byzantine fault tolerance.

    Args:
        n: Number of nodes
        f: Number of Byzantine faults to tolerate
        topology_type: 'random_regular' or 'ring'

    Returns:
        Dictionary mapping node IDs to list of neighbor IDs
    """
    if topology_type == 'ring':
        # Legacy ring topology
        k = (2 * f) + 1
        return generate_ring_topology(n, k)

    elif topology_type == 'random_regular':
        if nx is None:
            print("ERROR: networkx required for random_regular topology")
            print("Run: uv add networkx")
            raise ImportError("networkx not installed")

        # Dolev requires 2f+1 node-disjoint paths
        # Use degree = 2f+2 for safety margin
        degree = (2 * f) + 2

        if degree >= n:
            # Fallback to complete graph if N is too small
            print(f"Warning: degree {degree} >= n {n}, using complete graph")
            G = nx.complete_graph(n)
        else:
            # Generate random regular graph, ensure it's connected
            max_attempts = 10
            for attempt in range(max_attempts):
                try:
                    G = nx.random_regular_graph(degree, n)
                    if nx.is_connected(G):
                        break
                except nx.NetworkXError as e:
                    if attempt == max_attempts - 1:
                        print(f"Warning: Failed to generate connected graph, using complete graph")
                        G = nx.complete_graph(n)
                    continue
            else:
                # Fallback if all attempts failed
                print(f"Warning: Using complete graph as fallback")
                G = nx.complete_graph(n)

        # Convert to adjacency list for YAML config
        topology = {f"n{i}": [] for i in range(n)}
        for u, v in G.edges():
            topology[f"n{u}"].append(f"n{v}")
            topology[f"n{v}"].append(f"n{u}")

        # Verify connectivity
        actual_degree = len(topology["n0"]) if topology else 0
        print(f"  Generated graph: N={n}, f={f}, degree={actual_degree}, connected={nx.is_connected(G)}")

        return topology

    else:
        raise ValueError(f"Unknown topology type: {topology_type}")


def generate_ring_topology(n: int, k: int) -> dict:
    """Generate a ring topology with k-connectivity (legacy for compatibility)."""
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


def generate_config(n: int, f: int, num_byzantine: int = 0,
                   num_senders: int = 1, output_path: Path = None,
                   algorithm: str = 'dolev', topology_type: str = 'random_regular',
                   byzantine_mode: str = 'BYZANTINE_SILENT',
                   optimization_flags: dict = None):
    """
    Generate a configuration file for Byzantine broadcast algorithms.

    Args:
        n: Number of nodes
        f: Byzantine fault tolerance parameter
        num_byzantine: Number of Byzantine nodes
        num_senders: Number of sender nodes
        output_path: Path to save YAML file
        algorithm: 'dolev', 'bracha', or 'bracha_optimized'
        topology_type: 'ring' or 'random_regular'
        byzantine_mode: 'BYZANTINE_SILENT', 'BYZANTINE_SELECTIVE', 'BYZANTINE_SPOOF'
        optimization_flags: Dict of Bracha optimization flags (for bracha_optimized)
    """
    config = {}

    # Generate topology
    if topology_type == 'ring':
        k = (2 * f) + 1
        topology = generate_ring_topology(n, k)
    else:
        topology = generate_topology(n, f, topology_type)

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
            'f': f
        }

        # Add algorithm-specific fields
        if algorithm in ['dolev']:
            config[node_id]['payload'] = 'hello'
            config[node_id]['behavior_mode'] = byzantine_mode if node_id in byzantine_nodes else 'HONEST'

        elif algorithm in ['bracha', 'bracha_optimized']:
            config[node_id]['behavior_mode'] = byzantine_mode if node_id in byzantine_nodes else 'HONEST'

            # Add Bracha optimization flags for bracha_optimized
            if algorithm == 'bracha_optimized' and optimization_flags:
                config[node_id].update(optimization_flags)

    if output_path:
        with open(output_path, 'w') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
        print(f"Generated: {output_path}")

    return config


def main():
    parser = argparse.ArgumentParser(description='Generate Byzantine broadcast benchmark configurations')
    parser.add_argument('--output-dir', '-o', type=str, default='configs/benchmark',
                        help='Output directory for generated configs')
    parser.add_argument('--mode', '-m', choices=['n', 'byzantine', 'connectivity', 'all', 'demos', 'benchmark'], required=True,
                        help='Generation mode: vary n, byzantine nodes, connectivity, demos (for Bracha), benchmark (for Bracha), or all')
    parser.add_argument('--algorithm', '-a', type=str, default='dolev', choices=['dolev', 'bracha', 'bracha_optimized'],
                        help='Algorithm to generate configs for')
    parser.add_argument('--topology', '-t', type=str, default='random_regular', choices=['ring', 'random_regular'],
                        help='Topology type: ring or random_regular')

    args = parser.parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate demo configs for Bracha
    if args.mode == 'demos':
        if args.algorithm not in ['bracha', 'bracha_optimized']:
            print(f"ERROR: --mode demos only supported for bracha/bracha_optimized")
            return

        print(f"Generating Bracha demo configurations...")

        # Demo 1: Success - Multiple message delivery
        print("  [1/3] Success demo (N=10, f=1, all HONEST)")
        output_path = output_dir / 'success.yaml'
        generate_config(n=10, f=1, num_byzantine=0, num_senders=1,
                       output_path=output_path, algorithm=args.algorithm,
                       topology_type=args.topology)

        # Demo 2: Agreement - Faulty selective sender
        print("  [2/3] Agreement demo (N=10, f=1, BYZANTINE_SELECTIVE sender)")
        output_path = output_dir / 'agreement.yaml'
        generate_config(n=10, f=1, num_byzantine=0, num_senders=1,
                       output_path=output_path, algorithm=args.algorithm,
                       topology_type=args.topology, byzantine_mode='BYZANTINE_SELECTIVE')
        # Manually mark sender as Byzantine after generation
        with open(output_path, 'r') as f:
            config = yaml.safe_load(f)
        config['n0']['behavior_mode'] = 'BYZANTINE_SELECTIVE'  # Sender is n0
        with open(output_path, 'w') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
        print(f"    Modified: Set n0 (sender) to BYZANTINE_SELECTIVE")

        # Demo 3: Integrity - Forgery prevention with colluding Byzantine nodes
        print("  [3/3] Integrity demo (N=10, f=2, 2 BYZANTINE_SPOOF nodes)")
        output_path = output_dir / 'integrity.yaml'
        generate_config(n=10, f=2, num_byzantine=2, num_senders=1,
                       output_path=output_path, algorithm=args.algorithm,
                       topology_type=args.topology, byzantine_mode='BYZANTINE_SPOOF')

        print(f"\n✓ Demo configs generated in: {output_dir}")
        return

    # Generate benchmark configs for Bracha
    if args.mode == 'benchmark':
        if args.algorithm not in ['bracha', 'bracha_optimized']:
            print(f"ERROR: --mode benchmark only supported for bracha/bracha_optimized")
            return

        print(f"Generating Bracha benchmark configurations...")

        # N values: 7, 10, 13, 16 with appropriate f values
        benchmark_params = [
            (7, 1),   # f = floor((7-1)/3) = 1
            (10, 2),  # f = floor((10-1)/3) = 2
            (13, 3),  # f = floor((13-1)/3) = 3
            (16, 5),  # f = floor((16-1)/3) = 5
        ]

        for n, f in benchmark_params:
            d = (2 * f) + 2  # Degree for random regular graph
            output_path = output_dir / f'n{n}_f{f}_d{d}.yaml'
            print(f"  Generating: N={n}, f={f}, d={d}")
            generate_config(n=n, f=f, num_byzantine=0, num_senders=1,
                           output_path=output_path, algorithm=args.algorithm,
                           topology_type=args.topology)

        print(f"\n✓ Benchmark configs generated in: {output_dir}")
        return

    # Legacy modes for Dolev (backward compatibility)
    if args.mode == 'n' or args.mode == 'all':
        print("Generating configs with varying n (number of processes)...")
        f = 1
        for n in [4, 7, 10, 13, 16]:
            output_path = output_dir / f'n_{n}_f_{f}.yaml'
            generate_config(n, f, num_byzantine=0, output_path=output_path,
                           algorithm=args.algorithm, topology_type=args.topology)

    if args.mode == 'byzantine' or args.mode == 'all':
        print("Generating configs with varying Byzantine nodes...")
        n = 16
        f = 2
        for num_byz in [0, 1, 2]:
            output_path = output_dir / f'n_{n}_f_{f}_byz_{num_byz}.yaml'
            generate_config(n, f, num_byzantine=num_byz, output_path=output_path,
                           algorithm=args.algorithm, topology_type=args.topology)

    if args.mode == 'connectivity' or args.mode == 'all':
        print("Generating configs with varying connectivity (only for ring topology)...")
        if args.topology != 'ring':
            print("  Skipping: connectivity mode only applicable to ring topology")
        else:
            n = 10
            f = 1
            for k in [3, 4, 5, 6]:
                output_path = output_dir / f'n_{n}_f_{f}_k_{k}.yaml'
                # Use ring topology with specific k for this mode
                topology = generate_ring_topology(n, k)
                generate_config(n, f, num_byzantine=0, output_path=output_path,
                               algorithm=args.algorithm, topology_type='ring')

    print(f"\n✓ All configs generated in: {output_dir}")


if __name__ == '__main__':
    main()
