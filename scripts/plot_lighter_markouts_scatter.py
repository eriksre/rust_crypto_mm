#!/usr/bin/env python3

"""Plot scatter of pnl_markout_bps vs entry_move_bps from lighter markouts CSV, one panel per horizon."""

import argparse
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

HORIZONS_MS = (100, 500, 1000)


def load_markouts(path: Path) -> pd.DataFrame:
    """Load CSV with pnl_markout_bps, entry_move_bps, horizon_ms; drop invalid rows with a warning."""
    if not path.exists():
        raise SystemExit(f"File not found: {path}")

    df = pd.read_csv(path, low_memory=False)
    for col in ("pnl_markout_bps", "entry_move_bps", "horizon_ms"):
        if col not in df.columns:
            raise SystemExit(
                f"CSV has no column '{col}'. Columns: {list(df.columns)}"
            )

    df["pnl_markout_bps"] = pd.to_numeric(df["pnl_markout_bps"], errors="coerce")
    df["entry_move_bps"] = pd.to_numeric(df["entry_move_bps"], errors="coerce")
    df["horizon_ms"] = pd.to_numeric(df["horizon_ms"], errors="coerce")
    before = len(df)
    df = df.dropna(subset=["pnl_markout_bps", "entry_move_bps", "horizon_ms"])
    n_dropped = before - len(df)
    if n_dropped > 0:
        print(
            f"Warning: dropped {n_dropped} row(s) with non-numeric pnl_markout_bps, entry_move_bps or horizon_ms",
            file=sys.stderr,
        )
    if len(df) == 0:
        raise SystemExit("No valid rows to plot.")
    return df


def plot_one_scatter(
    ax: plt.Axes,
    x: np.ndarray,
    y: np.ndarray,
    horizon_ms: int,
) -> None:
    """Draw scatter and optional correlation line."""
    ax.scatter(x, y, alpha=0.5, s=20, color="tab:blue", edgecolors="none")
    ax.set_xlabel("Entry move (bps)")
    ax.set_ylabel("PnL markout (bps)")
    ax.set_title(f"Horizon {horizon_ms} ms")
    ax.grid(True, alpha=0.2)
    ax.axhline(0, color="gray", linestyle="-", linewidth=0.5)
    ax.axvline(0, color="gray", linestyle="-", linewidth=0.5)

    if len(x) >= 2:
        r = np.corrcoef(x, y)[0, 1]
        ax.text(
            0.05,
            0.95,
            f"r = {r:.3f}\nn = {len(x)}",
            transform=ax.transAxes,
            ha="left",
            va="top",
            fontsize=10,
            bbox={
                "facecolor": "white",
                "edgecolor": "black",
                "alpha": 0.8,
                "boxstyle": "round,pad=0.5",
            },
        )


def plot_scatters(df: pd.DataFrame, out_path: Path | None = None) -> None:
    """Plot three scatter panels: pnl_markout_bps vs entry_move_bps, one per horizon."""
    fig, axes = plt.subplots(1, 3, figsize=(14, 5))
    fig.suptitle("Lighter markouts: PnL (bps) vs entry move (bps) by horizon", fontsize=12)

    for ax, horizon_ms in zip(axes, HORIZONS_MS, strict=True):
        sub = df[df["horizon_ms"] == horizon_ms]
        if len(sub) == 0:
            ax.set_title(f"Horizon {horizon_ms} ms (no data)")
            ax.set_xlabel("Entry move (bps)")
            ax.set_ylabel("PnL markout (bps)")
        else:
            x = sub["entry_move_bps"].to_numpy()
            y = sub["pnl_markout_bps"].to_numpy()
            plot_one_scatter(ax, x, y, horizon_ms)

    fig.tight_layout()
    if out_path is not None:
        fig.savefig(out_path, dpi=150)
        print(f"Saved: {out_path}")
    else:
        plt.show()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot scatter of pnl_markout_bps vs entry_move_bps by horizon (100/500/1000 ms)"
    )
    parser.add_argument(
        "csv",
        nargs="?",
        default="logs/lighter_markouts.csv",
        help="Path to lighter markouts CSV (default: logs/lighter_markouts.csv)",
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
    plot_scatters(df, out_path=args.out)


if __name__ == "__main__":
    main()
