#!/usr/bin/env python3
"""
Analysis and Plotting for Bracha BRB Benchmark Results

Generates plots comparing optimization combinations:
1. Latency vs N (linear Y-axis)
2. Message Count vs N (LOG SCALE Y-axis)
3. Reduction percentages

CRITICAL: Uses log scale for message counts to show optimizations clearly
"""

import argparse
import json
from pathlib import Path
import re
from typing import Dict, List
import sys

try:
    import matplotlib.pyplot as plt
    import numpy as np
except ImportError:
    print("ERROR: matplotlib not installed. Run: uv add matplotlib")
    sys.exit(1)


# Style configuration
plt.style.use('default')
COLORS = {
    "baseline": "#e74c3c",  # Red
    "echo_amp": "#3498db",  # Blue
    "echo_amp_single_hop": "#2ecc71",  # Green
    "reduced_messages": "#f39c12",  # Orange
    "full_optimized": "#9b59b6",  # Purple
}

LABELS = {
    "baseline": "Baseline (No Opt)",
    "echo_amp": "Echo Amplification",
    "echo_amp_single_hop": "Echo Amp + Single-hop",
    "reduced_messages": "Reduced Messages (MBD.11)",
    "full_optimized": "Full Optimization",
}


class ResultAnalyzer:
    def __init__(self, results_dir: Path, output_dir: Path):
        self.results_dir = results_dir
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.summary_file = results_dir / "summary.json"
        if not self.summary_file.exists():
            raise FileNotFoundError(f"Summary file not found: {self.summary_file}")

        with open(self.summary_file, 'r') as f:
            self.summary = json.load(f)

    def extract_n_value(self, config_file: str) -> int:
        """Extract N value from config filename (e.g., n10_f2_d6.yaml -> 10)."""
        match = re.search(r'n(\d+)', config_file)
        if match:
            return int(match.group(1))
        return 0

    def organize_data(self) -> Dict[str, Dict[int, dict]]:
        """Organize summary data by optimization and N value."""
        data = {}

        for entry in self.summary:
            opt = entry["optimization"]
            n = self.extract_n_value(entry["config_file"])

            if n == 0:
                continue

            if opt not in data:
                data[opt] = {}

            data[opt][n] = entry

        return data

    def plot_latency_vs_n(self, data: Dict[str, Dict[int, dict]], comparison: str):
        """Plot latency vs N (linear Y-axis)."""
        fig, ax = plt.subplots(figsize=(10, 6))

        for opt, n_data in data.items():
            if not n_data:
                continue

            n_values = sorted(n_data.keys())
            latencies = [n_data[n]["avg_latency_ms"]["mean"] for n in n_values]

            ax.plot(n_values, latencies, marker='o', linewidth=2,
                   label=LABELS.get(opt, opt), color=COLORS.get(opt))

        ax.set_xlabel('Number of Nodes (N)', fontsize=12)
        ax.set_ylabel('Average Latency (ms)', fontsize=12)
        ax.set_title(f'Latency vs Network Size - {comparison}', fontsize=14, fontweight='bold')
        ax.legend(loc='best', fontsize=10)
        ax.grid(True, alpha=0.3)

        output_file = self.output_dir / f"latency_vs_n_{comparison.lower().replace(' ', '_')}.png"
        plt.tight_layout()
        plt.savefig(output_file, dpi=300)
        print(f"  ✓ Saved: {output_file.name}")
        plt.close()

    def plot_messages_vs_n(self, data: Dict[str, Dict[int, dict]], comparison: str):
        """Plot message count vs N (LOG SCALE Y-axis) - CRITICAL for visibility."""
        fig, ax = plt.subplots(figsize=(10, 6))

        for opt, n_data in data.items():
            if not n_data:
                continue

            n_values = sorted(n_data.keys())
            messages = [n_data[n]["total_messages_sent"]["mean"] for n in n_values]

            ax.plot(n_values, messages, marker='o', linewidth=2,
                   label=LABELS.get(opt, opt), color=COLORS.get(opt))

        ax.set_xlabel('Number of Nodes (N)', fontsize=12)
        ax.set_ylabel('Total Messages Sent (log scale)', fontsize=12)
        ax.set_title(f'Message Complexity vs Network Size - {comparison}', fontsize=14, fontweight='bold')

        # CRITICAL: Use log scale for Y-axis
        ax.set_yscale('log')

        ax.legend(loc='best', fontsize=10)
        ax.grid(True, alpha=0.3, which='both')  # Show grid for both major and minor ticks

        output_file = self.output_dir / f"messages_vs_n_{comparison.lower().replace(' ', '_')}.png"
        plt.tight_layout()
        plt.savefig(output_file, dpi=300)
        print(f"  ✓ Saved: {output_file.name} (LOG SCALE)")
        plt.close()

    def generate_comparison_plots(self):
        """Generate all comparison plots for Part C report."""
        print("\nGenerating comparison plots...")

        data = self.organize_data()

        if not data:
            print("❌ No data to plot")
            return

        # Comparison 1: Echo Amplification only
        print("\n1. Echo Amplification Impact:")
        data_echo_amp = {
            "baseline": data.get("baseline", {}),
            "echo_amp": data.get("echo_amp", {}),
        }
        self.plot_latency_vs_n(data_echo_amp, "Echo Amplification")
        self.plot_messages_vs_n(data_echo_amp, "Echo Amplification")

        # Comparison 2: Single-hop Send Impact
        print("\n2. Single-hop Send Impact:")
        data_single_hop = {
            "echo_amp": data.get("echo_amp", {}),
            "echo_amp_single_hop": data.get("echo_amp_single_hop", {}),
        }
        self.plot_latency_vs_n(data_single_hop, "Single-hop Send")
        self.plot_messages_vs_n(data_single_hop, "Single-hop Send")

        # Comparison 3: Reduced Messages (MBD.11) Impact
        print("\n3. Reduced Messages Impact:")
        data_reduced = {
            "echo_amp_single_hop": data.get("echo_amp_single_hop", {}),
            "full_optimized": data.get("full_optimized", {}),
        }
        self.plot_latency_vs_n(data_reduced, "Reduced Messages")
        self.plot_messages_vs_n(data_reduced, "Reduced Messages")

        # All-in-one comparison
        print("\n4. All Optimizations Comparison:")
        self.plot_latency_vs_n(data, "All Optimizations")
        self.plot_messages_vs_n(data, "All Optimizations")

    def generate_summary_table(self):
        """Generate summary statistics table."""
        print("\nGenerating summary table...")

        data = self.organize_data()

        table_file = self.output_dir / "summary_table.txt"
        with open(table_file, 'w') as f:
            f.write("Bracha BRB Performance Summary\n")
            f.write("="*80 + "\n\n")

            for n in sorted({self.extract_n_value(e["config_file"]) for e in self.summary if self.extract_n_value(e["config_file"]) > 0}):
                f.write(f"\nN = {n} nodes\n")
                f.write("-"*80 + "\n")
                f.write(f"{'Optimization':<30} {'Latency (ms)':<20} {'Messages Sent':<20}\n")
                f.write("-"*80 + "\n")

                for opt in ["baseline", "echo_amp", "echo_amp_single_hop", "reduced_messages", "full_optimized"]:
                    if opt in data and n in data[opt]:
                        entry = data[opt][n]
                        latency = entry["avg_latency_ms"]["mean"]
                        messages = entry["total_messages_sent"]["mean"]
                        f.write(f"{LABELS[opt]:<30} {latency:<20.2f} {messages:<20.0f}\n")

        print(f"  ✓ Saved: {table_file.name}")

    def calculate_reductions(self):
        """Calculate percentage reductions for report."""
        print("\nCalculating reduction percentages...")

        data = self.organize_data()
        reductions_file = self.output_dir / "reductions.txt"

        with open(reductions_file, 'w') as f:
            f.write("Performance Improvement Analysis\n")
            f.write("="*80 + "\n\n")

            for n in sorted({self.extract_n_value(e["config_file"]) for e in self.summary if self.extract_n_value(e["config_file"]) > 0}):
                f.write(f"\nN = {n} nodes\n")
                f.write("-"*80 + "\n")

                if "baseline" not in data or n not in data["baseline"]:
                    continue

                baseline_latency = data["baseline"][n]["avg_latency_ms"]["mean"]
                baseline_messages = data["baseline"][n]["total_messages_sent"]["mean"]

                for opt in ["echo_amp", "echo_amp_single_hop", "reduced_messages", "full_optimized"]:
                    if opt not in data or n not in data[opt]:
                        continue

                    opt_latency = data[opt][n]["avg_latency_ms"]["mean"]
                    opt_messages = data[opt][n]["total_messages_sent"]["mean"]

                    latency_reduction = ((baseline_latency - opt_latency) / baseline_latency) * 100
                    message_reduction = ((baseline_messages - opt_messages) / baseline_messages) * 100

                    f.write(f"\n{LABELS[opt]}:\n")
                    f.write(f"  Latency reduction: {latency_reduction:+.1f}%\n")
                    f.write(f"  Message reduction: {message_reduction:+.1f}%\n")

        print(f"  ✓ Saved: {reductions_file.name}")


def main():
    parser = argparse.ArgumentParser(description='Analyze Bracha BRB benchmark results and generate plots')
    parser.add_argument('--results-dir', '-r', type=str, required=True,
                        help='Directory containing benchmark results (summary.json)')
    parser.add_argument('--output-dir', '-o', type=str, required=True,
                        help='Output directory for plots and tables')

    args = parser.parse_args()

    results_dir = Path(args.results_dir)
    output_dir = Path(args.output_dir)

    if not results_dir.exists():
        print(f"ERROR: Results directory not found: {results_dir}")
        sys.exit(1)

    print("="*70)
    print("Bracha BRB Results Analysis - Part C Report Generation")
    print("="*70)
    print(f"Results directory: {results_dir}")
    print(f"Output directory: {output_dir}")

    try:
        analyzer = ResultAnalyzer(results_dir, output_dir)
        analyzer.generate_comparison_plots()
        analyzer.generate_summary_table()
        analyzer.calculate_reductions()

        print("\n" + "="*70)
        print("✅ Analysis complete!")
        print(f"Plots and tables saved to: {output_dir}")
        print("\nGenerated files:")
        for f in sorted(output_dir.glob("*")):
            print(f"  - {f.name}")
        print("="*70 + "\n")

    except Exception as e:
        print(f"\n❌ Error during analysis: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
