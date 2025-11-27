#!/usr/bin/env python3
"""
Metrics Parser for Dolev Byzantine Reliable Broadcast
"""

import json
import re
import glob
import argparse
from pathlib import Path
from typing import Dict, List, Any
import csv


def parse_node_log(filepath: Path) -> Dict[str, Any]:
    metrics = None
    with open(filepath, 'r') as f:
        for line in f:
            if 'METRICS_JSON:' in line:
                match = re.search(r'METRICS_JSON:\s*(\{.*\})', line)
                if match:
                    try:
                        metrics = json.loads(match.group(1))
                    except json.JSONDecodeError:
                        print(f"Warning: Failed to parse JSON in {filepath}")
    return metrics


def parse_all_logs(log_dir: Path) -> List[Dict[str, Any]]:
    results = []
    log_files = sorted(glob.glob(str(log_dir / "n*.txt")))
    for log_file in log_files:
        metrics = parse_node_log(Path(log_file))
        if metrics:
            results.append(metrics)
    return results


def aggregate_metrics(node_metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not node_metrics:
        return {}
    total_messages_sent = sum(m.get('total_messages_sent', 0) for m in node_metrics)
    total_forwarded = sum(m.get('messages_forwarded', 0) for m in node_metrics)
    total_delivered = sum(m.get('delivered_count', 0) for m in node_metrics)
    latencies = [m.get('avg_latency_ms', 0) for m in node_metrics if m.get('avg_latency_ms', 0) > 0]
    return {
        'num_nodes': len(node_metrics),
        'total_messages_sent': total_messages_sent,
        'total_messages_forwarded': total_forwarded,
        'total_delivered': total_delivered,
        'avg_latency_ms': sum(latencies) / len(latencies) if latencies else 0,
        'min_latency_ms': min(latencies) if latencies else 0,
        'max_latency_ms': max(latencies) if latencies else 0,
        'per_node': node_metrics
    }


def export_to_csv(aggregated: Dict[str, Any], output_path: Path):
    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Metric', 'Value'])
        for key in ['num_nodes', 'total_messages_sent', 'total_messages_forwarded',
                    'total_delivered', 'avg_latency_ms', 'min_latency_ms', 'max_latency_ms']:
            writer.writerow([key, aggregated.get(key, 0)])


def export_to_json(aggregated: Dict[str, Any], output_path: Path):
    with open(output_path, 'w') as f:
        json.dump(aggregated, f, indent=2)


def main():
    parser = argparse.ArgumentParser(description='Parse Dolev metrics from node logs')
    parser.add_argument('--log-dir', '-l', type=str, default='.', help='Directory containing node log files')
    parser.add_argument('--output', '-o', type=str, default=None, help='Output file path')
    parser.add_argument('--format', '-f', choices=['json', 'csv', 'both'], default='json', help='Output format')
    parser.add_argument('--verbose', '-v', action='store_true', help='Print detailed per-node metrics')
    args = parser.parse_args()
    log_dir = Path(args.log_dir)
    print(f"Parsing logs from: {log_dir}")
    node_metrics = parse_all_logs(log_dir)
    if not node_metrics:
        print("No metrics found in log files")
        return
    print(f"Found metrics from {len(node_metrics)} nodes")
    aggregated = aggregate_metrics(node_metrics)
    print("\n=== Aggregated Metrics ===")
    print(f"Number of nodes: {aggregated['num_nodes']}")
    print(f"Total messages sent: {aggregated['total_messages_sent']}")
    print(f"Total messages forwarded: {aggregated['total_messages_forwarded']}")
    print(f"Total deliveries: {aggregated['total_delivered']}")
    print(f"Average latency: {aggregated['avg_latency_ms']:.2f} ms")
    if args.verbose:
        print("\n=== Per-Node Metrics ===")
        for m in node_metrics:
            print(f"  {m['node_id']}: sent={m['total_messages_sent']}, delivered={m['delivered_count']}")
    if args.output:
        output_path = Path(args.output)
        if args.format in ('json', 'both'):
            export_to_json(aggregated, output_path.with_suffix('.json'))
        if args.format in ('csv', 'both'):
            export_to_csv(aggregated, output_path.with_suffix('.csv'))


if __name__ == '__main__':
    main()
