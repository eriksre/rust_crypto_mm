#!/usr/bin/env python3

"""Visualise market data together with quote/cancel activity."""

import argparse
from collections import defaultdict
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd


MARKET_FEEDS = ("orderbook", "bbo", "trade")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot market snapshots alongside quote/cancel lifecycle"
    )
    parser.add_argument(
        "csv",
        nargs="?",
        default="logs/gate_activity.csv",
        help="Path to CSV produced by the gate activity logger",
    )
    parser.add_argument(
        "--exchange",
        help="Optional exchange filter (e.g. gate, bybit)",
    )
    return parser.parse_args()


def load_dataframe(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)
    if "event_type" not in df.columns:
        raise SystemExit(
            "CSV is missing 'event_type'. Did you pass the activity logger output?"
        )

    numeric_cols = [
        "ts_ns",
        "price",
        "reference_ts_ns",
        "reference_price",
        "size",
        "reprice_latency_us",
        "cancel_latency_us",
        "quote_latency_us",
        "ack_latency_us",
        "sent_ts_ns",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ("exchange", "feed", "event_type", "side", "reference_source"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower().str.strip()

    df["dt"] = pd.to_datetime(df["ts_ns"], unit="ns", errors="coerce")
    if "reference_ts_ns" in df.columns:
        df["ref_dt"] = pd.to_datetime(df["reference_ts_ns"], unit="ns", errors="coerce")
    else:
        df["ref_dt"] = pd.NaT
    return df.dropna(subset=["dt"])


def split_frames(df: pd.DataFrame, exchange_filter: str | None):
    if exchange_filter:
        df = df[df["exchange"] == exchange_filter.lower()]

    market = df[df["event_type"] == "market"].copy()
    quotes = df[df["event_type"] == "quote"].copy()
    cancels = df[df["event_type"] == "cancel"].copy()
    fills = df[df["event_type"] == "fill"].copy()
    reports = df[df["event_type"] == "report"].copy()
    return market, quotes, cancels, fills, reports


def build_quote_lifecycles(
    quotes: pd.DataFrame,
    cancels: pd.DataFrame,
    fills: pd.DataFrame,
    reports: pd.DataFrame,
) -> pd.DataFrame:
    if quotes.empty:
        return pd.DataFrame()

    cancel_subset = cancels[
        [
            "client_order_id",
            "dt",
            "price",
            "ref_dt",
            "reference_price",
            "reference_source",
            "cancel_internal_us",
        ]
    ].rename(
        columns={
            "dt": "cancel_dt",
            "price": "cancel_price",
            "ref_dt": "cancel_ref_dt",
            "reference_price": "cancel_reference_price",
            "reference_source": "cancel_reference_source",
        }
    )

    cancel_subset["cancel_external_us"] = pd.NA
    cancel_subset["cancel_ack_dt"] = pd.NaT

    merged = quotes.merge(cancel_subset, on="client_order_id", how="left")
    merged = merged.rename(
        columns={
            "dt": "quote_dt",
            "price": "quote_price",
            "size": "quote_size",
            "ref_dt": "quote_ref_dt",
            "reference_price": "quote_reference_price",
            "reference_source": "quote_reference_source",
            "quote_internal_us": "quote_internal_us",
        }
    )
    merged["side"] = merged["side"].fillna("unknown")

    if not fills.empty:
        fills_subset = (
            fills[
                ["client_order_id", "dt", "price", "size"]
            ]
            .copy()
            .sort_values("dt")
            .rename(
                columns={
                    "dt": "fill_dt",
                    "price": "fill_price",
                    "size": "fill_qty",
                }
            )
        )
        fills_grouped = fills_subset.groupby("client_order_id").agg(
            {
                "fill_dt": "last",
                "fill_price": "last",
                "fill_qty": "sum",
            }
        )
        merged = merged.merge(
            fills_grouped,
            on="client_order_id",
            how="left",
        )
    else:
        merged["fill_dt"] = pd.NaT
        merged["fill_price"] = pd.NA
        merged["fill_qty"] = pd.NA

    if not reports.empty:
        cancel_acks = (
            reports[reports["feed"] == "cancel_ack"]
            [["client_order_id", "cancel_external_us", "dt"]]
            .rename(
                columns={
                    "cancel_external_us": "cancel_external_us_report",
                    "dt": "cancel_ack_dt_report",
                }
            )
        )
        cancel_acks = (
            cancel_acks.sort_values("cancel_ack_dt_report")
            .drop_duplicates("client_order_id", keep="last")
        )
        merged = merged.merge(cancel_acks, on="client_order_id", how="left")
        existing_cancel_ext = merged.get(
            "cancel_external_us",
            pd.Series(pd.NA, index=merged.index)
        )
        merged["cancel_external_us"] = existing_cancel_ext.fillna(
            merged.pop("cancel_external_us_report")
        )
        merged["cancel_ack_dt"] = merged.get("cancel_ack_dt", pd.NaT).combine_first(
            merged.pop("cancel_ack_dt_report")
        )

        quote_acks = (
            reports[reports["feed"] == "quote_ack"]
            [["client_order_id", "quote_external_us", "dt"]]
            .rename(
                columns={
                    "quote_external_us": "quote_external_us_report",
                    "dt": "quote_ack_dt",
                }
            )
        )
        quote_acks = (
            quote_acks.sort_values("quote_ack_dt")
            .drop_duplicates("client_order_id", keep="last")
        )
        merged = merged.merge(quote_acks, on="client_order_id", how="left")
        existing_quote_ext = merged.get(
            "quote_external_us",
            pd.Series(pd.NA, index=merged.index)
        )
        merged["quote_external_us"] = existing_quote_ext.fillna(
            merged.pop("quote_external_us_report")
        )
    else:
        merged["quote_ack_dt"] = pd.NaT

    def _merge_columns(prefix: str) -> None:
        cols = [c for c in merged.columns if c.startswith(prefix) and c != prefix]
        if not cols:
            return
        base = merged.get(prefix, pd.Series(pd.NA, index=merged.index))
        for col in cols:
            base = base.fillna(merged.pop(col))
        merged[prefix] = base

    _merge_columns("cancel_internal_us")
    _merge_columns("cancel_external_us")

    return merged.sort_values("quote_dt")


def plot_market(ax: plt.Axes, market: pd.DataFrame) -> None:
    if market.empty:
        return

    color_map = {
        "bybit": "tab:blue",
        "binance": "tab:orange",
        "gate": "tab:green",
        "bitget": "tab:red",
    }
    marker_map = {
        "bbo": "o",
        "orderbook": "s",
        "trade": "^",
    }

    labels_used = set()
    for (exchange, feed), group in market.groupby(["exchange", "feed"]):
        if feed not in MARKET_FEEDS:
            continue
        color = color_map.get(exchange, "grey")
        marker = marker_map.get(feed, ".")
        label = f"{exchange}:{feed}"
        label = None if label in labels_used else label

        ax.scatter(
            group["dt"],
            group["price"],
            s=10,
            c=color,
            marker=marker,
            alpha=0.5,
            label=label,
        )
        labels_used.add(f"{exchange}:{feed}")


def plot_quote_bars(ax: plt.Axes, lifecycles: pd.DataFrame) -> None:
    if lifecycles.empty:
        return

    legend_added = defaultdict(bool)

    for _, row in lifecycles.iterrows():
        start = row["quote_dt"]
        if pd.isna(start):
            continue
        end = row["cancel_dt"] if pd.notna(row.get("cancel_dt")) else row.get("fill_dt")
        if pd.isna(end):
            end = start
        if start == end:
            end = start + pd.Timedelta(milliseconds=5)

        price = row["quote_price"]
        side = row["side"]
        color = "tab:blue" if side == "bid" else ("tab:red" if side == "ask" else "grey")

        ax.hlines(
            y=price,
            xmin=start,
            xmax=end,
            colors=color,
            linewidth=2.0,
            alpha=0.9,
        )
        label = f"{side} quote"
        if not legend_added[label]:
            ax.scatter(start, price, marker="|", c=color, s=120, label=label)
            legend_added[label] = True
        else:
            ax.scatter(start, price, marker="|", c=color, s=120)

        if pd.notna(row.get("cancel_dt")):
            ax.scatter(row["cancel_dt"], price, marker="x", c=color, s=60, alpha=0.9)

        if pd.notna(row.get("fill_dt")) and pd.notna(row.get("fill_price")):
            label_key = f"{side} fill"
            ax.scatter(
                row["fill_dt"],
                row["fill_price"],
                marker="X",
                c=color,
                s=70,
                alpha=0.9,
                label=None if legend_added[label_key] else f"{side} fill",
            )
            legend_added[label_key] = True

        # Reference markers
        if pd.notna(row.get("quote_ref_dt")) and pd.notna(row.get("quote_reference_price")):
            ax.scatter(
                row["quote_ref_dt"],
                row["quote_reference_price"],
                marker="*",
                c=color,
                s=80,
                alpha=0.8,
                label=None if legend_added[f"{side} quote ref"] else f"{side} quote ref",
                edgecolors="k",
            )
            legend_added[f"{side} quote ref"] = True

        if pd.notna(row.get("cancel_ref_dt")) and pd.notna(row.get("cancel_reference_price")):
            ax.scatter(
                row["cancel_ref_dt"],
                row["cancel_reference_price"],
                marker="D",
                c=color,
                s=50,
                alpha=0.7,
                label=None if legend_added[f"{side} cancel ref"] else f"{side} cancel ref",
            )
            legend_added[f"{side} cancel ref"] = True


def plot_fills(ax: plt.Axes, fills: pd.DataFrame) -> None:
    if fills.empty:
        return

    fills = fills.dropna(subset=["dt", "price"])
    if fills.empty:
        return

    if "exchange" in fills.columns:
        fills = fills[fills["exchange"] == "gate"]
    if fills.empty:
        return

    for side, group in fills.groupby("side"):
        color = "tab:blue" if side == "bid" else ("tab:red" if side == "ask" else "grey")
        label = f"our {side} fills"
        if "size" in group.columns:
            sizes = group["size"].abs().fillna(0.0)
        else:
            sizes = pd.Series(0.0, index=group.index)

        scaled = sizes.clip(lower=0.0).pow(0.5)
        marker_sizes = (scaled * 60.0 + 50.0).clip(upper=220.0)
        ax.scatter(
            group["dt"],
            group["price"],
            marker="X",
            c=color,
            s=marker_sizes,
            alpha=0.85,
            label=label,
            edgecolors="k",
            linewidths=0.6,
            zorder=5,
        )


def plot_latencies(ax: plt.Axes, lifecycles: pd.DataFrame) -> None:
    if lifecycles.empty:
        ax.set_ylabel("Latency (ms)")
        return

    internals = [
        ("quote_internal_us", "quote_dt", "quote internal", "tab:blue", "o"),
        ("cancel_internal_us", "cancel_dt", "cancel internal", "tab:purple", "s"),
    ]
    externals = [
        ("quote_external_us", "quote_ack_dt", "quote ack", "tab:green", "d"),
        ("cancel_external_us", "cancel_ack_dt", "cancel ack", "tab:red", "^"),
    ]

    for column, time_column, label, color, marker in internals:
        if column not in lifecycles.columns:
            continue
        subset = lifecycles.dropna(subset=[column]).copy()
        if subset.empty:
            continue
        time_series = subset.get(time_column, subset["quote_dt"])
        ax.scatter(
            time_series,
            subset[column],
            c=color,
            marker=marker,
            s=40,
            alpha=0.8,
            label=label,
        )

    twin = ax.twinx()
    for column, time_column, label, color, marker in externals:
        if column not in lifecycles.columns:
            continue
        subset = lifecycles.dropna(subset=[column]).copy()
        if subset.empty:
            continue
        time_series = subset.get(time_column, subset["quote_dt"])
        twin.scatter(
            time_series,
            subset[column] / 1000.0,
            c=color,
            marker=marker,
            s=40,
            alpha=0.8,
            label=label,
        )

    ax.set_ylabel("Internal Latency (μs)")
    twin.set_ylabel("Ack Latency (ms)")
    handles, labels = ax.get_legend_handles_labels()
    handles2, labels2 = twin.get_legend_handles_labels()
    twin.legend(handles + handles2, labels + labels2, loc="upper right")
    ax.grid(True, alpha=0.3)


def main() -> None:
    args = parse_args()
    csv_path = Path(args.csv).expanduser()
    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")

    df = load_dataframe(csv_path)
    market_df, quotes_df, cancels_df, fills_df, reports_df = split_frames(df, args.exchange)
    lifecycles = build_quote_lifecycles(quotes_df, cancels_df, fills_df, reports_df)

    fig, (ax_price, ax_latency) = plt.subplots(
        2,
        1,
        figsize=(14, 10),
        sharex=True,
        gridspec_kw={"height_ratios": [3, 1]},
    )

    plot_market(ax_price, market_df)
    plot_quote_bars(ax_price, lifecycles)
    plot_fills(ax_price, fills_df)

    ax_price.set_title("Market feeds with quote/cancel lifecycle")
    ax_price.set_ylabel("Price")
    ax_price.grid(True, alpha=0.3)
    ax_price.legend(loc="upper left", bbox_to_anchor=(1.02, 1), borderaxespad=0.0)

    plot_latencies(ax_latency, lifecycles)
    ax_latency.set_xlabel("Time")

    ax_price.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S.%f"))
    fig.autofmt_xdate()
    fig.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
