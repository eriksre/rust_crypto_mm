#!/usr/bin/env python3
"""Visualise per-exchange price updates stored in all_exchanges.csv."""

import argparse
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

COLORS = {
    "gate": "#1f77b4",
    "binance": "#ff7f0e",
    "bybit": "#2ca02c",
    "bitget": "#d62728",
}

TRADE_MARKERS = {
    "buy": "^",
    "sell": "v",
    "unknown": "d",
}

FEED_MARKERS = {
    "bbo": "o",
    "orderbook": "s",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot BBO/orderbook/trade prices from collect_all_csv output",
    )
    parser.add_argument(
        "csv",
        nargs="?",
        default="all_exchanges.csv",
        help="Path to CSV produced by collect_all_csv",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Plot only the last N rows",
    )
    parser.add_argument(
        "--exchanges",
        type=str,
        help="Comma separated exchange filter (e.g. gate,binance)",
    )
    return parser.parse_args()


def load_data(path: Path, limit: int | None, exchanges: set[str] | None) -> pd.DataFrame:
    df = pd.read_csv(
        path,
        usecols=["ts_ns", "exchange", "feed", "price", "direction"],
        low_memory=False,
    )
    df["ts_ns"] = pd.to_numeric(df["ts_ns"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["ts_ns", "price"])
    df.sort_values("ts_ns", inplace=True)
    if limit is not None:
        df = df.tail(limit)

    df["exchange"] = df["exchange"].astype(str).str.lower().str.strip()
    df["feed"] = df["feed"].astype(str).str.lower().str.strip()
    df["direction"] = (
        df["direction"].astype(str).str.lower().str.strip().replace({"": "unknown"})
    )
    df.loc[df["direction"].isin({"nan", "none"}), "direction"] = "unknown"

    if exchanges:
        df = df[df["exchange"].isin(exchanges)]

    df["dt"] = pd.to_datetime(df["ts_ns"], unit="ns", errors="coerce")
    df = df.dropna(subset=["dt"])
    return df


def plot(df: pd.DataFrame, title: str) -> None:
    if df.empty:
        raise SystemExit("No rows left after filtering; nothing to plot.")

    fig, ax = plt.subplots(figsize=(12, 8))
    seen_labels: set[str] = set()

    for exchange in sorted(df["exchange"].unique()):
        ex_df = df[df["exchange"] == exchange]
        color = COLORS.get(exchange, "#888888")

        for feed, marker in FEED_MARKERS.items():
            feed_df = ex_df[ex_df["feed"] == feed]
            if feed_df.empty:
                continue
            label = f"{exchange.title()} {feed.upper()}"
            plot_label = label if label not in seen_labels else None
            ax.scatter(
                feed_df["dt"],
                feed_df["price"],
                s=14,
                c=color,
                marker=marker,
                alpha=0.55,
                label=plot_label,
            )
            seen_labels.add(label)

        trade_df = ex_df[ex_df["feed"] == "trade"]
        if trade_df.empty:
            continue
        for direction, marker in TRADE_MARKERS.items():
            dir_df = trade_df[trade_df["direction"].fillna("unknown") == direction]
            if dir_df.empty:
                continue
            label = f"{exchange.title()} {direction.upper()}"
            plot_label = label if label not in seen_labels else None
            ax.scatter(
                dir_df["dt"],
                dir_df["price"],
                s=18,
                c=color,
                marker=marker,
                alpha=0.75,
                label=plot_label,
            )
            seen_labels.add(label)

    ax.set_title(title, fontweight="bold")
    ax.set_xlabel("Time")
    ax.set_ylabel("Price")
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S.%f"))
    fig.autofmt_xdate()
    ax.legend(loc="upper left", bbox_to_anchor=(1, 1))
    fig.tight_layout()
    plt.show()


def main() -> None:
    args = parse_args()
    exchanges = (
        {ex.strip().lower() for ex in args.exchanges.split(",") if ex.strip()}
        if args.exchanges
        else None
    )
    path = Path(args.csv).expanduser()
    if not path.exists():
        raise SystemExit(f"CSV not found: {path}")

    df = load_data(path, args.limit, exchanges)
    window = f" last {args.limit} rows" if args.limit else ""
    title = f"Exchange price updates from {path.name}{window}"
    plot(df, title)


if __name__ == "__main__":
    main()
