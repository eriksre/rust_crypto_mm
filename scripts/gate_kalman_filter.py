#!/usr/bin/env python3
"""
Anchor-biased pricing helper.

Uses a single anchor venue to define the fair price F_t, learns a slow-moving
structural bias b[v] for every other venue (aligned via the anchor slope), and
exposes the same `run_analysis` API that the plotting scripts expect.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Dict, Optional

import numpy as np
import pandas as pd


# --------------------------------------------------------------------------- #
# Configuration                                                               #
# --------------------------------------------------------------------------- #


@dataclass
class Settings:
    input_path: Path = Path("logs/gate_activity.csv")
    row_limit: Optional[int] = None

    anchor_exchange: str = "gate"
    target_aliases: tuple[str, ...] = ("gate", "gateio")

    bias_alpha: float = 0.004
    rejection_threshold_bp: float = 40.0
    max_alignment_gap_ns: int = 100_000_000  # 250 ms
    slope_cap_abs: float = 100.0  # $/s

    volatility_alpha: float = 0.25
    min_state_variance: float = 1e-4


DEFAULT_SETTINGS = Settings()
DEFAULT_SIGMA = 2.0


@dataclass
class RegressionSummary:
    coefficients: pd.Series
    rmse: float
    mae: float
    sample_count: int


EMPTY_SUMMARY = RegressionSummary(
    coefficients=pd.Series(dtype=float),
    rmse=0.0,
    mae=0.0,
    sample_count=0,
)


# --------------------------------------------------------------------------- #
# Loading & bias learning                                                     #
# --------------------------------------------------------------------------- #


def load_ticks(settings: Settings = DEFAULT_SETTINGS) -> pd.DataFrame:
    """Load CSV, keep market rows, derive mid, normalise venues."""

    path = settings.input_path.expanduser()
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}")

    df = pd.read_csv(path, nrows=settings.row_limit, low_memory=False)
    if "ts_ns" not in df.columns:
        raise ValueError("CSV missing 'ts_ns'")

    df["ts_ns"] = pd.to_numeric(df["ts_ns"], errors="coerce")
    df = df.dropna(subset=["ts_ns"]).copy()

    for col in ("exchange", "feed", "event_type"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower().str.strip()

    market = df[df["event_type"] == "market"].copy()
    if market.empty:
        return market

    if "bid_px_1" in market.columns and "ask_px_1" in market.columns:
        market["mid_price"] = np.where(
            market["bid_px_1"].notna() & market["ask_px_1"].notna(),
            (market["bid_px_1"] + market["ask_px_1"]) / 2.0,
            market.get("price", np.nan),
        )
    else:
        market["mid_price"] = market.get("price", np.nan)

    market = market.dropna(subset=["mid_price"]).copy()
    market = market.sort_values("ts_ns").reset_index(drop=True)

    anchor_target = settings.anchor_exchange.lower()
    alias_map = {alias.lower(): anchor_target for alias in settings.target_aliases}
    alias_map[anchor_target] = anchor_target

    market["venue"] = market["exchange"].apply(
        lambda x: alias_map.get(str(x).lower().strip(), str(x).lower().strip())
    )
    market["timestamp"] = pd.to_datetime(market["ts_ns"], unit="ns", utc=True)
    return market


def learn_bias_series(
    market: pd.DataFrame,
    settings: Settings,
) -> tuple[pd.Series, pd.Series, pd.DataFrame]:
    """Align venues to anchor timeline and learn slow-moving biases."""

    if market.empty:
        empty = pd.Series(dtype=float)
        return empty, empty, pd.DataFrame()

    anchor = settings.anchor_exchange.lower()
    venues = sorted(market["venue"].unique())
    biases: Dict[str, float] = {v: 0.0 for v in venues}

    anchor_prices = []
    anchor_times = []
    state_vars = []
    bias_snapshots = []

    last_anchor_price: Optional[float] = None
    last_anchor_ts: Optional[int] = None
    anchor_slope = 0.0
    volatility_state = settings.min_state_variance

    rejection_scale = max(settings.rejection_threshold_bp, 1e-3) / 10_000.0

    for row in market.itertuples():
        venue = row.venue
        price = float(row.mid_price)
        ts_ns = int(row.ts_ns)

        if venue == anchor:
            if last_anchor_price is not None and last_anchor_ts is not None:
                dt_s = (ts_ns - last_anchor_ts) / 1e9
                if dt_s > 0:
                    candidate = (price - last_anchor_price) / dt_s
                    if abs(candidate) <= settings.slope_cap_abs:
                        anchor_slope = candidate
                price_delta = price - last_anchor_price
                volatility_state = (
                    (1.0 - settings.volatility_alpha) * volatility_state
                    + settings.volatility_alpha * (price_delta ** 2)
                )
            last_anchor_price = price
            last_anchor_ts = ts_ns

            anchor_prices.append(price)
            anchor_times.append(ts_ns)
            state_vars.append(max(volatility_state, settings.min_state_variance))
            bias_snapshots.append({v: biases.get(v, 0.0) for v in venues})
            continue

        if last_anchor_price is None or last_anchor_ts is None:
            continue

        gap_ns = ts_ns - last_anchor_ts
        if gap_ns < 0 or gap_ns > settings.max_alignment_gap_ns:
            continue

        dt_s = gap_ns / 1e9
        aligned_anchor = last_anchor_price + anchor_slope * dt_s
        diff = price - aligned_anchor
        current_bias = biases.get(venue, 0.0)
        threshold = rejection_scale * max(abs(aligned_anchor), 1.0)

        if abs(diff - current_bias) <= threshold:
            new_bias = (1.0 - settings.bias_alpha) * current_bias + settings.bias_alpha * diff
            biases[venue] = new_bias

    if not anchor_times:
        empty = pd.Series(dtype=float)
        return empty, empty, pd.DataFrame()

    index = pd.to_datetime(anchor_times, unit="ns", utc=True).tz_convert(None)
    anchor_series = pd.Series(anchor_prices, index=index, dtype=float)
    state_var_series = pd.Series(state_vars, index=index, dtype=float)
    bias_df = pd.DataFrame(bias_snapshots, index=index, columns=venues)
    bias_df = bias_df.ffill().fillna(0.0)

    return anchor_series, state_var_series, bias_df


# --------------------------------------------------------------------------- #
# Plot helpers                                                                #
# --------------------------------------------------------------------------- #


def _lazy_import_matplotlib():
    try:
        import matplotlib.dates as mdates  # type: ignore
        import matplotlib.pyplot as plt  # type: ignore
    except ImportError as exc:  # pragma: no cover
        raise SystemExit(
            "matplotlib is required for plotting; install it or use plot_mid_with_quotes.py."
        ) from exc
    return mdates, plt


def _plot_market_scatter(ax, market: pd.DataFrame) -> None:
    if market.empty:
        return

    feeds = {"orderbook", "bbo", "trade"}
    subset = market[market["feed"].isin(feeds)].copy()
    if subset.empty:
        return

    color_map = {
        "bybit": "tab:blue",
        "binance": "tab:orange",
        "gate": "tab:green",
        "gateio": "tab:green",
        "bitget": "tab:red",
        "okx": "tab:purple",
    }
    marker_map = {"bbo": "o", "orderbook": "s", "trade": "^"}

    seen = set()
    for (exchange, feed), group in subset.groupby(["exchange", "feed"]):
        label = f"{exchange}:{feed}"
        if label in seen:
            label = None
        seen.add(f"{exchange}:{feed}")

        timestamps = group["timestamp"].dt.tz_convert("UTC").dt.tz_localize(None)
        ax.scatter(
            timestamps,
            group["mid_price"],
            s=12,
            c=color_map.get(exchange, "grey"),
            marker=marker_map.get(feed, "."),
            alpha=0.55,
            label=label,
        )


def _plot_fair_band(ax, price_frame: pd.DataFrame, sigma: float) -> None:
    if price_frame.empty:
        return

    required = {"timestamp", "efficient_price", "state_variance"}
    missing = required - set(price_frame.columns)
    if missing:
        raise ValueError(f"Price frame missing columns: {missing}")

    clean = price_frame.dropna(subset=["efficient_price"]).copy()
    if clean.empty:
        return

    timestamps = pd.to_datetime(clean["timestamp"])
    mean_series = clean["efficient_price"]
    std_series = np.sqrt(clean["state_variance"].clip(lower=0.0))
    delta = sigma * std_series

    ax.plot(
        timestamps,
        mean_series,
        color="black",
        linewidth=1.8,
        label="Anchor fair (F_t)",
        zorder=5,
    )
    ax.fill_between(
        timestamps,
        mean_series - delta,
        mean_series + delta,
        color="black",
        alpha=0.15,
        label=f"{sigma:.1f}σ envelope",
        zorder=4,
    )


def _plot_market_with_overlay(
    market: pd.DataFrame,
    price_frame: pd.DataFrame,
    sigma: float = DEFAULT_SIGMA,
) -> None:
    mdates, plt = _lazy_import_matplotlib()
    fig, ax = plt.subplots(figsize=(14, 6))
    _plot_market_scatter(ax, market)
    _plot_fair_band(ax, price_frame, sigma)

    ax.set_title("Market feeds with anchor fair overlay")
    ax.set_ylabel("Price")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="upper left", bbox_to_anchor=(1.02, 1), borderaxespad=0.0)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S.%f"))
    fig.autofmt_xdate()
    fig.tight_layout()
    plt.show()


# --------------------------------------------------------------------------- #
# Public API                                                                  #
# --------------------------------------------------------------------------- #


def run_analysis(
    settings: Settings = DEFAULT_SETTINGS,
    preloaded: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, RegressionSummary]:
    market = preloaded if preloaded is not None else load_ticks(settings)
    if market.empty:
        return pd.DataFrame(), pd.DataFrame(), EMPTY_SUMMARY

    anchor_series, state_var_series, bias_df = learn_bias_series(market, settings)
    if anchor_series.empty:
        return pd.DataFrame(), pd.DataFrame(), EMPTY_SUMMARY

    index = anchor_series.index
    state_var_series = state_var_series.reindex(index, method="ffill").fillna(settings.min_state_variance)
    price_frame = pd.DataFrame(
        {
            "timestamp": index,
            "efficient_price": anchor_series.values,
            "consensus_price": anchor_series.values,
            "state_variance": state_var_series.values,
        }
    )

    for venue in bias_df.columns:
        price_frame[f"bias_{venue}"] = bias_df[venue].reindex(index).values
        price_frame[f"local_fair_{venue}"] = anchor_series.add(bias_df[venue], axis=0).reindex(index).values

    bias_long = bias_df.stack().reset_index()
    bias_long.columns = ["timestamp", "exchange", "bias"]
    bias_long["local_fair"] = bias_long["timestamp"].map(anchor_series).add(bias_long["bias"], fill_value=np.nan)

    return price_frame, bias_long, EMPTY_SUMMARY


def main() -> None:
    parser = argparse.ArgumentParser(description="Anchor-based fair overlay")
    parser.add_argument(
        "csv",
        nargs="?",
        default=str(DEFAULT_SETTINGS.input_path),
        help="Path to gate activity CSV",
    )
    parser.add_argument("--exchange", help="Anchor override (e.g. gate, bybit)")
    parser.add_argument("--row-limit", type=int, help="Optional row limit")
    parser.add_argument("--sigma", type=float, default=DEFAULT_SIGMA, help="Envelope width (σ)")
    args = parser.parse_args()

    csv_path = Path(args.csv).expanduser()
    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")

    settings = replace(
        DEFAULT_SETTINGS,
        input_path=csv_path,
        row_limit=args.row_limit if args.row_limit is not None else DEFAULT_SETTINGS.row_limit,
        anchor_exchange=args.exchange.lower() if args.exchange else DEFAULT_SETTINGS.anchor_exchange,
        target_aliases=DEFAULT_SETTINGS.target_aliases,
    )

    market = load_ticks(settings)
    price_frame, _, _ = run_analysis(settings, preloaded=market)
    if price_frame.empty:
        print("No anchor data available.")
        return

    _plot_market_with_overlay(market, price_frame, sigma=args.sigma)


if __name__ == "__main__":
    main()
