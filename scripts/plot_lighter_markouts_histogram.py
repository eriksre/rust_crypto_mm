#!/usr/bin/env python3

"""Plot histograms of pnl_markout_bps from lighter markouts CSV, one per horizon (100/500/1000 ms)."""

import argparse
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

HORIZONS_MS = (100, 500, 1000)


def load_markouts(path: Path) -> pd.DataFrame:
    """Load CSV with pnl_markout_bps and horizon_ms; drop invalid rows with a warning."""
    if not path.exists():
        raise SystemExit(f"File not found: {path}")

    df = pd.read_csv(path, low_memory=False)
    for col in ("pnl_markout_bps", "horizon_ms"):
        if col not in df.columns:
            raise SystemExit(
                f"CSV has no column '{col}'. Columns: {list(df.columns)}"
            )

    df["pnl_markout_bps"] = pd.to_numeric(df["pnl_markout_bps"], errors="coerce")
    df["horizon_ms"] = pd.to_numeric(df["horizon_ms"], errors="coerce")
    before = len(df)
    df = df.dropna(subset=["pnl_markout_bps", "horizon_ms"])
    n_dropped = before - len(df)
    if n_dropped > 0:
        print(
            f"Warning: dropped {n_dropped} row(s) with non-numeric pnl_markout_bps or horizon_ms",
            file=sys.stderr,
        )
    if len(df) == 0:
        raise SystemExit("No valid rows to plot.")
    return df


def plot_one_histogram(ax: plt.Axes, values: np.ndarray, horizon_ms: int, bins: int) -> None:
    """Draw a single histogram with mean/median and stats box."""
    ax.hist(values, bins=bins, color="tab:blue", alpha=0.75, edgecolor="black")
    ax.set_xlabel("PnL markout (bps)")
    ax.set_ylabel("Count")
    ax.set_title(f"Horizon {horizon_ms} ms")
    ax.grid(True, alpha=0.2)

    mean_bps = float(np.mean(values))
    median_bps = float(np.median(values))
    std_bps = float(np.std(values))

    ax.axvline(
        mean_bps,
        color="red",
        linestyle="--",
        linewidth=1.5,
        alpha=0.7,
        label=f"Mean: {mean_bps:.2f} bps",
    )
    ax.axvline(
        median_bps,
        color="green",
        linestyle="--",
        linewidth=1.5,
        alpha=0.7,
        label=f"Median: {median_bps:.2f} bps",
    )
    ax.legend()

    stats_text = (
        f"Mean: {mean_bps:.2f} bps\n"
        f"Median: {median_bps:.2f} bps\n"
        f"Std: {std_bps:.2f} bps\n"
        f"N: {len(values)}"
    )
    ax.text(
        0.98,
        0.98,
        stats_text,
        transform=ax.transAxes,
        ha="right",
        va="top",
        fontsize=10,
        bbox={
            "facecolor": "white",
            "edgecolor": "black",
            "alpha": 0.8,
            "boxstyle": "round,pad=0.5",
        },
    )


def plot_histograms(df: pd.DataFrame, bins: int = 30, out_path: Path | None = None) -> None:
    """Plot three histograms: one per horizon (100, 500, 1000 ms)."""
    fig, axes = plt.subplots(1, 3, figsize=(14, 5))
    fig.suptitle("Lighter markouts: PnL (bps) by horizon", fontsize=12)

    for ax, horizon_ms in zip(axes, HORIZONS_MS, strict=True):
        subset = df[df["horizon_ms"] == horizon_ms]["pnl_markout_bps"].to_numpy()
        if len(subset) == 0:
            ax.set_title(f"Horizon {horizon_ms} ms (no data)")
            ax.set_xlabel("PnL markout (bps)")
            ax.set_ylabel("Count")
        else:
            plot_one_histogram(ax, subset, horizon_ms, bins)

    fig.tight_layout()
    if out_path is not None:
        fig.savefig(out_path, dpi=150)
        print(f"Saved: {out_path}")
    else:
        plt.show()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot histograms of pnl_markout_bps by horizon (100/500/1000 ms) from lighter markouts CSV"
    )
    parser.add_argument(
        "csv",
        nargs="?",
        default="logs/lighter_markouts.csv",
        help="Path to lighter markouts CSV (default: logs/lighter_markouts.csv)",
    )
    parser.add_argument(
        "--bins",
        type=int,
        default=30,
        help="Number of histogram bins (default: 30)",
    )
    parser.add_argument(
        "-o",
        "--out",
        type=Path,
        default=None,
        help="Save figure to path instead of showing",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    path = Path(args.csv).expanduser()
    df = load_markouts(path)
    plot_histograms(df, bins=args.bins, out_path=args.out)


if __name__ == "__main__":
    main()
