#!/usr/bin/env python3
"""
Plotting Module for Dolev Byzantine Reliable Broadcast Benchmarks

Generates plots for latency and message complexity vs various parameters.
"""

import json
import argparse
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple


def load_benchmark_results(results_dir: Path) -> Dict[str, List[Dict]]:
    """Load all benchmark result JSON files from a directory."""
    results_by_param = {}
    
    for json_file in results_dir.glob('*.json'):
        with open(json_file, 'r') as f:
            data = json.load(f)
            
        # Extract parameter from filename (e.g., n_10_trial_0.json)
        filename = json_file.stem
        param_key = '_'.join(filename.split('_')[:-2])  # Remove trial number
        
        if param_key not in results_by_param:
            results_by_param[param_key] = []
        results_by_param[param_key].append(data)
    
    return results_by_param


def aggregate_trials(trials: List[Dict]) -> Tuple[float, float, float, float]:
    """Aggregate metrics across multiple trials (mean and std)."""
    latencies = [t.get('avg_latency_ms', 0) for t in trials]
    messages = [t.get('total_messages_sent', 0) for t in trials]
    
    return (
        np.mean(latencies),
        np.std(latencies),
        np.mean(messages),
        np.std(messages)
    )


def plot_vs_n(results_by_n: Dict[int, List[Dict]], output_path: Path):
    """Plot latency and message complexity vs number of nodes."""
    n_values = sorted(results_by_n.keys())
    latencies_mean = []
    latencies_std = []
    messages_mean = []
    messages_std = []
    
    for n in n_values:
        lat_mean, lat_std, msg_mean, msg_std = aggregate_trials(results_by_n[n])
        latencies_mean.append(lat_mean)
        latencies_std.append(lat_std)
        messages_mean.append(msg_mean)
        messages_std.append(msg_std)
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    
    # Latency plot
    ax1.errorbar(n_values, latencies_mean, yerr=latencies_std, marker='o', capsize=5, linestyle='-')
    ax1.set_xlabel('Number of Processes (n)', fontsize=12)
    ax1.set_ylabel('Average Latency (ms)', fontsize=12)
    ax1.set_title('Latency vs Number of Processes', fontsize=14)
    ax1.grid(True, alpha=0.3)
    
    # Message complexity plot
    ax2.errorbar(n_values, messages_mean, yerr=messages_std, marker='s', capsize=5, linestyle='-', color='orange')
    ax2.set_xlabel('Number of Processes (n)', fontsize=12)
    ax2.set_ylabel('Total Messages Sent', fontsize=12)
    ax2.set_title('Message Complexity vs Number of Processes', fontsize=14)
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Saved plot: {output_path}")
    plt.close()


def plot_vs_byzantine(results_by_byz: Dict[int, List[Dict]], output_path: Path):
    """Plot latency and message complexity vs number of Byzantine nodes."""
    byz_values = sorted(results_by_byz.keys())
    latencies_mean = []
    latencies_std = []
    messages_mean = []
    messages_std = []
    
    for byz in byz_values:
        lat_mean, lat_std, msg_mean, msg_std = aggregate_trials(results_by_byz[byz])
        latencies_mean.append(lat_mean)
        latencies_std.append(lat_std)
        messages_mean.append(msg_mean)
        messages_std.append(msg_std)
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    
    # Latency plot
    ax1.errorbar(byz_values, latencies_mean, yerr=latencies_std, marker='o', capsize=5, linestyle='-', color='red')
    ax1.set_xlabel('Number of Byzantine Nodes', fontsize=12)
    ax1.set_ylabel('Average Latency (ms)', fontsize=12)
    ax1.set_title('Latency vs Number of Byzantine Nodes', fontsize=14)
    ax1.grid(True, alpha=0.3)
    
    # Message complexity plot
    ax2.errorbar(byz_values, messages_mean, yerr=messages_std, marker='s', capsize=5, linestyle='-', color='darkred')
    ax2.set_xlabel('Number of Byzantine Nodes', fontsize=12)
    ax2.set_ylabel('Total Messages Sent', fontsize=12)
    ax2.set_title('Message Complexity vs Number of Byzantine Nodes', fontsize=14)
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Saved plot: {output_path}")
    plt.close()


def plot_vs_connectivity(results_by_k: Dict[int, List[Dict]], output_path: Path):
    """Plot latency and message complexity vs network connectivity."""
    k_values = sorted(results_by_k.keys())
    latencies_mean = []
    latencies_std = []
    messages_mean = []
    messages_std = []
    
    for k in k_values:
        lat_mean, lat_std, msg_mean, msg_std = aggregate_trials(results_by_k[k])
        latencies_mean.append(lat_mean)
        latencies_std.append(lat_std)
        messages_mean.append(msg_mean)
        messages_std.append(msg_std)
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    
    # Latency plot
    ax1.errorbar(k_values, latencies_mean, yerr=latencies_std, marker='o', capsize=5, linestyle='-', color='green')
    ax1.set_xlabel('Network Connectivity (k)', fontsize=12)
    ax1.set_ylabel('Average Latency (ms)', fontsize=12)
    ax1.set_title('Latency vs Network Connectivity', fontsize=14)
    ax1.grid(True, alpha=0.3)
    
    # Message complexity plot
    ax2.errorbar(k_values, messages_mean, yerr=messages_std, marker='s', capsize=5, linestyle='-', color='darkgreen')
    ax2.set_xlabel('Network Connectivity (k)', fontsize=12)
    ax2.set_ylabel('Total Messages Sent', fontsize=12)
    ax2.set_title('Message Complexity vs Network Connectivity', fontsize=14)
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Saved plot: {output_path}")
    plt.close()


def main():
    parser = argparse.ArgumentParser(description='Plot Dolev benchmark results')
    parser.add_argument('--results-dir', '-r', type=str, required=True, help='Directory containing result JSON files')
    parser.add_argument('--output-dir', '-o', type=str, default='results/plots', help='Output directory for plots')
    parser.add_argument('--param', '-p', choices=['n', 'byzantine', 'connectivity'], required=True,
                        help='Parameter to plot against')
    
    args = parser.parse_args()
    results_dir = Path(args.results_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Loading results from: {results_dir}")
    
    # Load and organize results by parameter value
    results_by_param = {}
    for json_file in results_dir.glob('*.json'):
        with open(json_file, 'r') as f:
            data = json.load(f)
        
        # Extract parameter value from filename
        filename = json_file.stem
        if args.param == 'n':
            param_val = int(filename.split('_')[1])
        elif args.param == 'byzantine':
            param_val = int(filename.split('_')[3])
        elif args.param == 'connectivity':
            param_val = int(filename.split('_')[5])
        
        if param_val not in results_by_param:
            results_by_param[param_val] = []
        results_by_param[param_val].append(data)
    
    # Generate plots
    if args.param == 'n':
        output_path = output_dir / 'latency_msgcomplexity_vs_n.png'
        plot_vs_n(results_by_param, output_path)
    elif args.param == 'byzantine':
        output_path = output_dir / 'latency_msgcomplexity_vs_byzantine.png'
        plot_vs_byzantine(results_by_param, output_path)
    elif args.param == 'connectivity':
        output_path = output_dir / 'latency_msgcomplexity_vs_connectivity.png'
        plot_vs_connectivity(results_by_param, output_path)


if __name__ == '__main__':
    main()
