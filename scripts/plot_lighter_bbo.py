#!/usr/bin/env python3

"""Plot best bid and best offer as step graph for the Lighter exchange."""

import argparse
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot best bid/offer step graph for Lighter exchange"
    )
    parser.add_argument(
        "csv",
        nargs="?",
        default="logs/lighter_activity.csv",
        help="Path to CSV produced by the activity logger",
    )
    return parser.parse_args()


def load_dataframe(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)
    
    # Convert numeric columns
    numeric_cols = ["ts_ns", "bid_px_1", "ask_px_1", "bid_sz_1", "ask_sz_1"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Normalize string columns
    for col in ("exchange", "feed", "event_type"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower().str.strip()

    # Convert timestamp to datetime
    df["dt"] = pd.to_datetime(df["ts_ns"], unit="ns", errors="coerce")
    return df.dropna(subset=["dt"])


def filter_lighter_bbo(df: pd.DataFrame) -> pd.DataFrame:
    """Filter for Lighter exchange market data with BBO info."""
    mask = (
        (df["exchange"] == "lighter")
        & (df["event_type"] == "market")
        & (df["feed"].isin(["orderbook", "bbo"]))
        & df["bid_px_1"].notna()
        & df["ask_px_1"].notna()
    )
    return df[mask].copy().sort_values("dt").reset_index(drop=True)


def plot_bbo_step(ax: plt.Axes, data: pd.DataFrame) -> None:
    """Plot best bid and offer as step graphs."""
    if data.empty:
        ax.text(0.5, 0.5, "No Lighter BBO data found", transform=ax.transAxes,
                ha="center", va="center", fontsize=14)
        return

    # Step graph for best bid
    ax.step(
        data["dt"],
        data["bid_px_1"],
        where="post",
        label="Best Bid",
        color="#2E86DE",
        linewidth=1.5,
        alpha=0.9,
    )

    # Step graph for best offer
    ax.step(
        data["dt"],
        data["ask_px_1"],
        where="post",
        label="Best Ask",
        color="#EE5A6F",
        linewidth=1.5,
        alpha=0.9,
    )

    # Fill between bid and ask to show spread
    ax.fill_between(
        data["dt"],
        data["bid_px_1"],
        data["ask_px_1"],
        step="post",
        alpha=0.15,
        color="#9B59B6",
        label="Spread",
    )


def main() -> None:
    args = parse_args()
    csv_path = Path(args.csv).expanduser()
    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")

    df = load_dataframe(csv_path)
    lighter_data = filter_lighter_bbo(df)

    print(f"Loaded {len(lighter_data)} Lighter BBO records")

    fig, ax = plt.subplots(figsize=(14, 6))
    
    plot_bbo_step(ax, lighter_data)

    ax.set_title("Lighter Exchange - Best Bid / Best Offer", fontsize=14, fontweight="bold")
    ax.set_xlabel("Time")
    ax.set_ylabel("Price")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="upper left")

    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    fig.autofmt_xdate()
    fig.tight_layout()

    plt.show()


if __name__ == "__main__":
    main()

