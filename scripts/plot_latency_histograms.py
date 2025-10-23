#!/usr/bin/env python3

"""Visualise quote and cancel latency distributions."""

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot histograms for quote and cancel latencies"
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
    parser.add_argument(
        "--bins",
        type=int,
        default=200,
        help="Number of bins to use for the histograms",
    )
    return parser.parse_args()


def plot_histograms(
    lifecycles: pd.DataFrame,
    bins: int,
) -> None:
    if lifecycles.empty:
        raise SystemExit("No quote lifecycle data found for histogram plot.")

    specs = [
        (
            "quote_internal_us",
            "Quote Internal Latency (μs)",
            "tab:blue",
            1.0,
            "Latency (μs)",
        ),
        (
            "cancel_internal_us",
            "Cancel Internal Latency (μs)",
            "tab:purple",
            1.0,
            "Latency (μs)",
        ),
        (
            "quote_external_us",
            "Quote Ack Latency (ms)",
            "tab:green",
            1000.0,
            "Latency (ms)",
        ),
        (
            "cancel_external_us",
            "Cancel Ack Latency (ms)",
            "tab:red",
            1000.0,
            "Latency (ms)",
        ),
    ]

    fig, axes = plt.subplots(2, 2, figsize=(12, 8))
    axes_flat = axes.flatten()
    for ax, (column, title, color, scale, xlabel) in zip(axes_flat, specs):
        if column not in lifecycles.columns:
            ax.set_title(f"{title}\n(no data)")
            ax.axis("off")
            continue

        series = lifecycles[column].dropna()
        if series.empty:
            ax.set_title(f"{title}\n(no data)")
            ax.axis("off")
            continue

        values = series / scale
        ax.hist(values, bins=bins, color=color, alpha=0.75, edgecolor="black")
        ax.set_title(title)
        ax.set_xlabel(xlabel)
        ax.set_ylabel("Count")
        ax.grid(True, alpha=0.2)

        p90 = values.quantile(0.9)
        p99 = values.quantile(0.99)
        ax.axvline(p90, color="black", linestyle="--", linewidth=1.2, alpha=0.7, label="p90")
        ax.axvline(p99, color="black", linestyle=":", linewidth=1.2, alpha=0.7, label="p99")
        ax.text(
            0.98,
            0.92,
            f"p90: {p90:.3f}\np99: {p99:.3f}",
            transform=ax.transAxes,
            ha="right",
            va="top",
            fontsize=9,
            bbox={"facecolor": "white", "edgecolor": "black", "alpha": 0.6, "boxstyle": "round,pad=0.25"},
        )

    fig.suptitle("Quote and Cancel Latency Distributions")
    fig.tight_layout()
    plt.show()


def main() -> None:
    args = parse_args()
    csv_path = Path(args.csv).expanduser()
    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")

    df = load_dataframe(csv_path)
    _, quotes_df, cancels_df, fills_df, reports_df = split_frames(df, args.exchange)
    lifecycles = build_quote_lifecycles(quotes_df, cancels_df, fills_df, reports_df)
    plot_histograms(lifecycles, args.bins)


if __name__ == "__main__":
    main()
