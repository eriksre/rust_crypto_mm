#!/usr/bin/env python3

"""Plot Lighter top-of-book (bid/ask) as step functions over time with trades.

Reads the activity logger CSV (default: logs/lighter_activity.csv) and filters to:
  - exchange == "lighter"
  - feed == "orderbook" (for BBO) or "trade" (for trades)
  - event_type == "market"

Creates two charts:
  1. Top chart: `bid_px_1` and `ask_px_1` as step lines, with trades overlaid as:
     - Up arrows (^) for buy trades
     - Down arrows (v) for sell trades
  2. Bottom chart: Time-decaying cumulative trade impact
     - Each trade has initial impact = signed_size (positive for buys, negative for sells)
     - Impact decays linearly to zero over 10 seconds
     - At each point, we sum the decayed impacts of all recent trades
     - Green fill for net buying pressure, red fill for net selling pressure
"""

from __future__ import annotations

import argparse
from pathlib import Path

try:
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
except ModuleNotFoundError as e:  # pragma: no cover
    raise SystemExit(
        "matplotlib is required. Run with the repo venv:\n"
        "  .venv/bin/python scripts/plot_lighter_tob_steps.py ..."
    ) from e

try:
    import pandas as pd
except ModuleNotFoundError as e:  # pragma: no cover
    raise SystemExit(
        "pandas is required. Run with the repo venv:\n"
        "  .venv/bin/python scripts/plot_lighter_tob_steps.py ..."
    ) from e

try:
    import numpy as np
except ModuleNotFoundError as e:  # pragma: no cover
    raise SystemExit(
        "numpy is required. Run with the repo venv:\n"
        "  .venv/bin/python scripts/plot_lighter_tob_steps.py ..."
    ) from e

# Decay window in seconds: each trade's impact decays linearly to zero over this period
DECAY_WINDOW_SEC = 10.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Step-plot Lighter top-of-book bid/ask over time",
    )
    parser.add_argument(
        "csv",
        nargs="?",
        default="logs/lighter_activity.csv",
        help="Path to CSV produced by the gate activity logger",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Keep only the last N Lighter orderbook rows (after filtering)",
    )
    parser.add_argument(
        "--out",
        type=str,
        help="Optional output image path (e.g. plots/lighter_tob.png)",
    )
    parser.add_argument(
        "--no-show",
        action="store_true",
        help="Do not open an interactive window (useful with --out)",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=500_000,
        help="Rows per chunk when reading the CSV (default: 500000)",
    )
    return parser.parse_args()


def _load_lighter_orderbook(path: Path, *, chunksize: int) -> pd.DataFrame:
    usecols = ["ts_ns", "exchange", "feed", "event_type", "bid_px_1", "ask_px_1"]
    frames: list[pd.DataFrame] = []

    for chunk in pd.read_csv(path, usecols=usecols, chunksize=chunksize, low_memory=False):
        for col in ("exchange", "feed", "event_type"):
            chunk[col] = chunk[col].astype(str).str.lower().str.strip()

        chunk = chunk[
            (chunk["exchange"] == "lighter")
            & (chunk["feed"] == "orderbook")
            & (chunk["event_type"] == "market")
        ]
        if chunk.empty:
            continue

        chunk["ts_ns"] = pd.to_numeric(chunk["ts_ns"], errors="coerce")
        chunk["bid_px_1"] = pd.to_numeric(chunk["bid_px_1"], errors="coerce")
        chunk["ask_px_1"] = pd.to_numeric(chunk["ask_px_1"], errors="coerce")
        chunk = chunk.dropna(subset=["ts_ns"])

        frames.append(chunk[["ts_ns", "bid_px_1", "ask_px_1"]])

    if not frames:
        return pd.DataFrame(columns=["ts_ns", "bid_px_1", "ask_px_1"])

    df = pd.concat(frames, ignore_index=True)
    df = df.sort_values("ts_ns", kind="mergesort").drop_duplicates("ts_ns", keep="last")
    return df.reset_index(drop=True)


def _compute_decayed_impact(trades_df: pd.DataFrame, decay_sec: float = DECAY_WINDOW_SEC) -> pd.Series:
    """Compute cumulative trade impact with linear decay over time.

    Each trade has an initial impact equal to its signed size (positive for buys,
    negative for sells). This impact decays linearly to zero over `decay_sec` seconds.

    At each trade timestamp, we sum the decayed impacts of all trades within the
    decay window.

    Args:
        trades_df: DataFrame with 'ts_ns' and 'signed_size' columns, sorted by ts_ns.
        decay_sec: Time window in seconds for impact decay (default: 10s).

    Returns:
        Series of cumulative decayed impact at each trade timestamp.
    """
    if trades_df.empty:
        return pd.Series(dtype=float)

    ts_ns = trades_df["ts_ns"].values
    signed_sizes = trades_df["signed_size"].values
    n = len(ts_ns)

    decay_ns = decay_sec * 1e9  # Convert to nanoseconds
    cumulative_impact = np.zeros(n, dtype=float)

    for i in range(n):
        current_ts = ts_ns[i]
        # Look back at all trades within the decay window
        total_impact = 0.0
        for j in range(i, -1, -1):
            age_ns = current_ts - ts_ns[j]
            if age_ns > decay_ns:
                break  # Trades are sorted, so all earlier trades are also too old
            # Linear decay: impact = signed_size * (1 - age/decay_window)
            decay_factor = 1.0 - (age_ns / decay_ns)
            total_impact += signed_sizes[j] * decay_factor
        cumulative_impact[i] = total_impact

    return pd.Series(cumulative_impact, index=trades_df.index)


def _load_lighter_trades(path: Path, *, chunksize: int) -> pd.DataFrame:
    usecols = ["ts_ns", "exchange", "feed", "event_type", "price", "direction", "size"]
    frames: list[pd.DataFrame] = []

    for chunk in pd.read_csv(path, usecols=usecols, chunksize=chunksize, low_memory=False):
        for col in ("exchange", "feed", "event_type", "direction"):
            if col in chunk.columns:
                chunk[col] = chunk[col].astype(str).str.lower().str.strip()

        chunk = chunk[
            (chunk["exchange"] == "lighter")
            & (chunk["feed"] == "trade")
            & (chunk["event_type"] == "market")
        ]
        if chunk.empty:
            continue

        chunk["ts_ns"] = pd.to_numeric(chunk["ts_ns"], errors="coerce")
        chunk["price"] = pd.to_numeric(chunk["price"], errors="coerce")
        chunk["size"] = pd.to_numeric(chunk["size"], errors="coerce")
        chunk = chunk.dropna(subset=["ts_ns", "price"])

        frames.append(chunk[["ts_ns", "price", "direction", "size"]])

    if not frames:
        return pd.DataFrame(columns=["ts_ns", "price", "direction", "size"])

    df = pd.concat(frames, ignore_index=True)
    df = df.sort_values("ts_ns", kind="mergesort")
    return df.reset_index(drop=True)


def plot_top_of_book(
    df: pd.DataFrame,
    trades: pd.DataFrame,
    *,
    title: str,
) -> None:
    if df.empty:
        raise SystemExit("No Lighter orderbook market rows found; nothing to plot.")

    df["dt"] = pd.to_datetime(df["ts_ns"], unit="ns", errors="coerce")
    df = df.dropna(subset=["dt"]).copy()
    if df.empty:
        raise SystemExit("No valid timestamps after parsing ts_ns; nothing to plot.")

    # Forward-fill to keep a continuous top-of-book series (common when one side is missing).
    df[["bid_px_1", "ask_px_1"]] = df[["bid_px_1", "ask_px_1"]].ffill()
    df = df.dropna(subset=["bid_px_1", "ask_px_1"])
    if df.empty:
        raise SystemExit("No valid bid/ask values after cleaning; nothing to plot.")

    # Prepare trades data
    trades_with_dt = pd.DataFrame()
    if not trades.empty:
        trades_with_dt = trades.copy()
        trades_with_dt["dt"] = pd.to_datetime(trades_with_dt["ts_ns"], unit="ns", errors="coerce")
        trades_with_dt = trades_with_dt.dropna(subset=["dt", "price", "size"])
        # Compute signed size: positive for buys, negative for sells
        trades_with_dt["signed_size"] = trades_with_dt.apply(
            lambda row: row["size"] if row["direction"] == "buy" else -row["size"],
            axis=1,
        )
        # Compute cumulative impact with linear decay over 10 seconds
        trades_with_dt["decayed_impact"] = _compute_decayed_impact(trades_with_dt)

    # Create subplots: price chart on top, trade flow below
    fig, (ax_price, ax_flow) = plt.subplots(
        2, 1,
        figsize=(14, 9),
        sharex=True,
        gridspec_kw={"height_ratios": [2, 1]},
    )

    # --- Top chart: Price with trades ---
    ax_price.step(
        df["dt"],
        df["bid_px_1"],
        where="post",
        color="#2E86DE",
        linewidth=1.4,
        label="Lighter bid (px_1)",
    )
    ax_price.step(
        df["dt"],
        df["ask_px_1"],
        where="post",
        color="#EE5A6F",
        linewidth=1.4,
        label="Lighter ask (px_1)",
    )

    # Plot trades as arrows
    if not trades_with_dt.empty:
        buys = trades_with_dt[trades_with_dt["direction"] == "buy"]
        sells = trades_with_dt[trades_with_dt["direction"] == "sell"]

        if not buys.empty:
            ax_price.scatter(
                buys["dt"],
                buys["price"],
                marker="^",
                color="#27AE60",
                s=60,
                alpha=0.85,
                label="Buy trade",
                edgecolors="white",
                linewidths=0.5,
                zorder=5,
            )

        if not sells.empty:
            ax_price.scatter(
                sells["dt"],
                sells["price"],
                marker="v",
                color="#E74C3C",
                s=60,
                alpha=0.85,
                label="Sell trade",
                edgecolors="white",
                linewidths=0.5,
                zorder=5,
            )

    ax_price.set_title(title)
    ax_price.set_ylabel("Price")
    ax_price.grid(True, alpha=0.3)
    ax_price.legend(loc="upper left")

    # --- Bottom chart: Time-decaying cumulative trade impact ---
    if not trades_with_dt.empty:
        ax_flow.step(
            trades_with_dt["dt"],
            trades_with_dt["decayed_impact"],
            where="post",
            color="#8E44AD",
            linewidth=1.4,
            label=f"Decayed impact ({DECAY_WINDOW_SEC:.0f}s window)",
        )
        # Fill positive (green) and negative (red) regions
        ax_flow.fill_between(
            trades_with_dt["dt"],
            trades_with_dt["decayed_impact"],
            0,
            where=trades_with_dt["decayed_impact"] >= 0,
            step="post",
            alpha=0.3,
            color="#27AE60",
            label="Net buying pressure",
        )
        ax_flow.fill_between(
            trades_with_dt["dt"],
            trades_with_dt["decayed_impact"],
            0,
            where=trades_with_dt["decayed_impact"] < 0,
            step="post",
            alpha=0.3,
            color="#E74C3C",
            label="Net selling pressure",
        )
    else:
        ax_flow.text(
            0.5, 0.5, "No trade data available",
            transform=ax_flow.transAxes, ha="center", va="center", fontsize=12,
        )

    ax_flow.axhline(0, color="grey", linewidth=0.8, linestyle="--", alpha=0.7)
    ax_flow.set_xlabel("Time")
    ax_flow.set_ylabel("Decayed Impact")
    ax_flow.grid(True, alpha=0.3)
    ax_flow.legend(loc="upper left")

    ax_flow.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S.%f"))
    fig.autofmt_xdate()
    fig.tight_layout()


def main() -> None:
    args = parse_args()
    csv_path = Path(args.csv).expanduser()
    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")

    df = _load_lighter_orderbook(csv_path, chunksize=args.chunksize)
    trades = _load_lighter_trades(csv_path, chunksize=args.chunksize)

    if args.limit is not None and args.limit > 0:
        df = df.tail(args.limit).reset_index(drop=True)
        # Filter trades to match the time range of the orderbook data
        if not df.empty and not trades.empty:
            min_ts = df["ts_ns"].min()
            max_ts = df["ts_ns"].max()
            trades = trades[(trades["ts_ns"] >= min_ts) & (trades["ts_ns"] <= max_ts)]

    plot_top_of_book(df, trades, title="Lighter top-of-book (orderbook feed)")

    if args.out:
        out_path = Path(args.out).expanduser()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(out_path, dpi=150)

    if not args.no_show:
        plt.show()


if __name__ == "__main__":
    main()
