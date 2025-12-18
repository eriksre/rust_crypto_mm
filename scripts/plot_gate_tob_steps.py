#!/usr/bin/env python3

"""Plot Gate top-of-book (bid/ask) as step functions over time.

Reads the activity logger CSV (default: logs/lighter_activity.csv) and filters to:
  - exchange == "gate"
  - feed == "orderbook" or "bbo"
  - event_type == "market"

Then plots `bid_px_1` and `ask_px_1` as step lines.
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
        "  .venv/bin/python scripts/plot_gate_tob_steps.py ..."
    ) from e

try:
    import pandas as pd
except ModuleNotFoundError as e:  # pragma: no cover
    raise SystemExit(
        "pandas is required. Run with the repo venv:\n"
        "  .venv/bin/python scripts/plot_gate_tob_steps.py ..."
    ) from e


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Step-plot Gate top-of-book bid/ask over time",
    )
    parser.add_argument(
        "csv",
        nargs="?",
        default="logs/lighter_activity.csv",
        help="Path to CSV produced by the activity logger",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Keep only the last N Gate orderbook rows (after filtering)",
    )
    parser.add_argument(
        "--out",
        type=str,
        help="Optional output image path (e.g. plots/gate_tob.png)",
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


def _load_gate_orderbook(path: Path, *, chunksize: int) -> pd.DataFrame:
    usecols = ["ts_ns", "exchange", "feed", "event_type", "bid_px_1", "ask_px_1"]
    frames: list[pd.DataFrame] = []

    for chunk in pd.read_csv(path, usecols=usecols, chunksize=chunksize, low_memory=False):
        for col in ("exchange", "feed", "event_type"):
            chunk[col] = chunk[col].astype(str).str.lower().str.strip()

        chunk = chunk[
            (chunk["exchange"] == "gate")
            & (chunk["feed"].isin(["orderbook", "bbo"]))
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


def plot_top_of_book(df: pd.DataFrame, *, title: str) -> None:
    if df.empty:
        raise SystemExit("No Gate orderbook market rows found; nothing to plot.")

    df["dt"] = pd.to_datetime(df["ts_ns"], unit="ns", errors="coerce")
    df = df.dropna(subset=["dt"]).copy()
    if df.empty:
        raise SystemExit("No valid timestamps after parsing ts_ns; nothing to plot.")

    # Forward-fill to keep a continuous top-of-book series (common when one side is missing).
    df[["bid_px_1", "ask_px_1"]] = df[["bid_px_1", "ask_px_1"]].ffill()
    df = df.dropna(subset=["bid_px_1", "ask_px_1"])
    if df.empty:
        raise SystemExit("No valid bid/ask values after cleaning; nothing to plot.")

    fig, ax = plt.subplots(figsize=(14, 6))

    ax.step(
        df["dt"],
        df["bid_px_1"],
        where="post",
        color="#2E86DE",
        linewidth=1.4,
        label="Gate bid (px_1)",
    )
    ax.step(
        df["dt"],
        df["ask_px_1"],
        where="post",
        color="#EE5A6F",
        linewidth=1.4,
        label="Gate ask (px_1)",
    )

    ax.set_title(title)
    ax.set_xlabel("Time")
    ax.set_ylabel("Price")
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S.%f"))
    fig.autofmt_xdate()
    ax.legend(loc="upper left")
    fig.tight_layout()


def main() -> None:
    args = parse_args()
    csv_path = Path(args.csv).expanduser()
    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")

    df = _load_gate_orderbook(csv_path, chunksize=args.chunksize)
    if args.limit is not None and args.limit > 0:
        df = df.tail(args.limit).reset_index(drop=True)

    plot_top_of_book(df, title="Gate top-of-book (orderbook feed)")

    if args.out:
        out_path = Path(args.out).expanduser()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(out_path, dpi=150)

    if not args.no_show:
        plt.show()


if __name__ == "__main__":
    main()

