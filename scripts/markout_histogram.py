#!/usr/bin/env python3

"""Plot histogram of markouts from markouts.txt"""

import argparse
import re
from pathlib import Path

import matplotlib.pyplot as plt


def parse_markouts(path: Path) -> list[float]:
    """Parse bps values from markouts.txt file."""
    bps_values = []
    
    with open(path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or not line.startswith('[markout]'):
                continue
            
            # Extract bps value using regex
            match = re.search(r'bps=([-\d.]+)', line)
            if match:
                try:
                    bps = float(match.group(1))
                    bps_values.append(bps)
                except ValueError:
                    continue
    
    return bps_values


def plot_histogram(bps_values: list[float], bins: int = 100) -> None:
    """Plot histogram of markout bps values."""
    if not bps_values:
        raise SystemExit("No markout data found to plot.")
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    ax.hist(bps_values, bins=bins, color='tab:blue', alpha=0.75, edgecolor='black')
    ax.set_xlabel('Markout (bps)')
    ax.set_ylabel('Count')
    ax.set_title('Markout Distribution')
    ax.grid(True, alpha=0.2)
    
    # Add statistics
    import numpy as np
    mean_bps = np.mean(bps_values)
    median_bps = np.median(bps_values)
    std_bps = np.std(bps_values)
    
    ax.axvline(mean_bps, color='red', linestyle='--', linewidth=1.5, alpha=0.7, label=f'Mean: {mean_bps:.2f} bps')
    ax.axvline(median_bps, color='green', linestyle='--', linewidth=1.5, alpha=0.7, label=f'Median: {median_bps:.2f} bps')
    
    ax.legend()
    
    stats_text = f'Mean: {mean_bps:.2f} bps\nMedian: {median_bps:.2f} bps\nStd: {std_bps:.2f} bps\nN: {len(bps_values)}'
    ax.text(
        0.98,
        0.98,
        stats_text,
        transform=ax.transAxes,
        ha='right',
        va='top',
        fontsize=10,
        bbox={'facecolor': 'white', 'edgecolor': 'black', 'alpha': 0.8, 'boxstyle': 'round,pad=0.5'},
    )
    
    fig.tight_layout()
    plt.show()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot histogram of markouts from markouts.txt"
    )
    parser.add_argument(
        "markouts_file",
        nargs="?",
        default="markouts.txt",
        help="Path to markouts.txt file",
    )
    parser.add_argument(
        "--bins",
        type=int,
        default=100,
        help="Number of bins for the histogram (default: 50)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    markouts_path = Path(args.markouts_file).expanduser()
    
    if not markouts_path.exists():
        raise SystemExit(f"Markouts file not found: {markouts_path}")
    
    bps_values = parse_markouts(markouts_path)
    plot_histogram(bps_values, args.bins)


if __name__ == "__main__":
    main()
