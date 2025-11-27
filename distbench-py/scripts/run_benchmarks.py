#!/usr/bin/env python3
"""
Benchmark Runner for Dolev Byzantine Reliable Broadcast

Runs multiple trials of the Dolev algorithm with different configurations.
"""

import subprocess
import argparse
import time
import shutil
import json
from pathlib import Path
from typing import List
import sys


def run_single_trial(config_path: Path, latency: str, timeout: int = 30) -> dict:
    """Run a single trial of the Dolev algorithm."""
    cmd = [
        'uv', 'run', 'distbench',
        '-c', str(config_path),
        '-a', 'dolev',
        '--mode', 'local',
        '--latency', latency,
        '--timeout', str(timeout)
    ]
    
    print(f"Running: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 10)
        
        # Parse metrics from log files
        metrics = parse_metrics_from_logs()
        return metrics
        
    except subprocess.TimeoutExpired:
        print(f"Warning: Trial timed out for {config_path}")
        return None
    except Exception as e:
        print(f"Error running trial: {e}")
        return None


def parse_metrics_from_logs() -> dict:
    """Parse METRICS_JSON from node log files."""
    import glob
    import re
    
    metrics_list = []
    for log_file in glob.glob("n*.txt"):
        with open(log_file, 'r') as f:
            for line in f:
                if 'METRICS_JSON:' in line:
                    match = re.search(r'METRICS_JSON:\s*(\{.*\})', line)
                    if match:
                        try:
                            metrics = json.loads(match.group(1))
                            metrics_list.append(metrics)
                        except json.JSONDecodeError:
                            pass
    
    # Aggregate metrics
    if not metrics_list:
        return {}
    
    total_messages_sent = sum(m.get('total_messages_sent', 0) for m in metrics_list)
    total_forwarded = sum(m.get('messages_forwarded', 0) for m in metrics_list)
    total_delivered = sum(m.get('delivered_count', 0) for m in metrics_list)
    latencies = [m.get('avg_latency_ms', 0) for m in metrics_list if m.get('avg_latency_ms', 0) > 0]
    
    return {
        'num_nodes': len(metrics_list),
        'total_messages_sent': total_messages_sent,
        'total_messages_forwarded': total_forwarded,
        'total_delivered': total_delivered,
        'avg_latency_ms': sum(latencies) / len(latencies) if latencies else 0,
        'min_latency_ms': min(latencies) if latencies else 0,
        'max_latency_ms': max(latencies) if latencies else 0,
    }


def cleanup_logs():
    """Remove node log files."""
    import glob
    for log_file in glob.glob("n*.txt"):
        try:
            Path(log_file).unlink()
        except Exception:
            pass


def run_benchmarks(config_dir: Path, num_trials: int, latency_modes: List[str],
                  output_dir: Path, timeout: int = 30):
    """Run benchmarks for all configs in a directory."""
    output_dir.mkdir(parents=True, exist_ok=True)
    
    config_files = sorted(config_dir.glob('*.yaml'))
    
    if not config_files:
        print(f"No config files found in {config_dir}")
        return
    
    print(f"Found {len(config_files)} configuration files")
    print(f"Running {num_trials} trials per config with latency modes: {latency_modes}")
    
    for config_file in config_files:
        config_name = config_file.stem
        print(f"\n{'='*60}")
        print(f"Benchmarking: {config_name}")
        print(f"{'='*60}")
        
        for latency_mode in latency_modes:
            latency_name = latency_mode.replace('-', '_')
            
            for trial in range(num_trials):
                print(f"\nTrial {trial + 1}/{num_trials} (latency: {latency_mode})")
                
                # Clean up previous logs
                cleanup_logs()
                
                # Run trial
                metrics = run_single_trial(config_file, latency_mode, timeout)
                
                if metrics:
                    # Save results
                    result_file = output_dir / f'{config_name}_lat_{latency_name}_trial_{trial}.json'
                    with open(result_file, 'w') as f:
                        json.dump(metrics, f, indent=2)
                    print(f"Saved results to: {result_file}")
                
                time.sleep(1)  # Brief pause between trials
    
    print(f"\n{'='*60}")
    print(f"Benchmarking complete! Results saved to: {output_dir}")
    print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(description='Run Dolev benchmarks')
    parser.add_argument('--config-dir', '-c', type=str, required=True,
                        help='Directory containing config files')
    parser.add_argument('--output-dir', '-o', type=str, default='results/metrics',
                        help='Output directory for results')
    parser.add_argument('--trials', '-t', type=int, default=5,
                        help='Number of trials per configuration')
    parser.add_argument('--latency', '-l', type=str, default='0-0,20-50',
                        help='Comma-separated latency modes (e.g., "0-0,20-50")')
    parser.add_argument('--timeout', type=int, default=30,
                        help='Timeout per trial in seconds')
    
    args = parser.parse_args()
    
    config_dir = Path(args.config_dir)
    output_dir = Path(args.output_dir)
    latency_modes = args.latency.split(',')
    
    if not config_dir.exists():
        print(f"Error: Config directory not found: {config_dir}")
        sys.exit(1)
    
    run_benchmarks(config_dir, args.trials, latency_modes, output_dir, args.timeout)


if __name__ == '__main__':
    main()
