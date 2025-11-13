#!/usr/bin/env python3
"""
Anchor-biased pricing helper, with optional multi-venue Kalman fusion.

Two modes are supported:

- "anchor" (default):
  Use a single anchor venue to define the fair price F_t, learn a
  slow-moving structural bias b[v] for every other venue (aligned via the
  anchor slope), and expose a frame with `efficient_price` taken directly
  from the anchor series.

- "kalman":
  Fuse all venues asynchronously in a de-biased space relative to the anchor.
  Each tick (Gate or non-Gate) is treated as a measurement of the latent
  Gate price, after subtracting the learned per-venue bias; an online
  constant-velocity Kalman filter updates the state so the estimate moves
  even when the latest tick is not from Gate.

The plotting API remains the same: `run_analysis` returns a `price_frame`
with `efficient_price` and `state_variance` columns suitable for overlay.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, replace, field
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
    max_alignment_gap_ns: int = 100_000_0  # 1 ms
    slope_cap_abs: float = 100.0  # $/s

    volatility_alpha: float = 0.25
    min_state_variance: float = 1e-4

    # Kalman fusion settings
    # Process noise is modeled as constant-acceleration white noise with
    # variance q = accel_var_scale * state_var, where state_var is an EW
    # variance of anchor price deltas. Q(dt) = q * [[dt^3/3, dt^2/2], [dt^2/2, dt]].
    accel_var_scale: float = 1.0
    initial_price_var: float = 1e-2
    initial_velocity_var: float = 10.0

    # Measurement noise (basis points) by feed; falls back to 'default'.
    feed_noise_bp: Dict[str, float] = field(
        default_factory=lambda: {"default": 6.0, "bbo": 4.0, "orderbook": 6.0, "trade": 10.0}
    )

    # Demean mode settings
    demean_window_s: float = 3.0
    cross_section_recency_s: float = 1.5


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

    ax.step(
        timestamps,
        mean_series,
        where="post",
        color="black",
        linewidth=1.8,
        label="Fair estimate (F_t)",
        zorder=5,
    )
    ax.fill_between(
        timestamps,
        mean_series - delta,
        mean_series + delta,
        step="post",
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


def _kalman_fuse_prices(
    market: pd.DataFrame,
    bias_df: pd.DataFrame,
    state_var_series: pd.Series,
    settings: Settings,
) -> pd.DataFrame:
    """Fuse de-biased venue measurements to estimate latent Gate price.

    - State: x = [price, velocity]
    - Transition: x' = F(dt) x + noise, with constant-velocity model
    - Measurement: z_v = price_v - bias_v = H x + eps, H = [1, 0]
    - Process noise depends on EW anchor volatility to adapt to regimes.
    """
    if market.empty:
        return pd.DataFrame()

    # Event timeline in nanoseconds and a tz-naive datetime index compatible
    # with the bias/state series constructed on anchor timestamps.
    ts_ns = market["ts_ns"].astype(np.int64).values
    event_index = pd.to_datetime(ts_ns, unit="ns", utc=True).tz_convert(None)

    # Bias snapshots aligned to event times; forward-fill between anchor points.
    if not bias_df.empty:
        bias_at_events = bias_df.reindex(event_index).ffill()
    else:
        bias_at_events = pd.DataFrame(index=pd.Index(event_index), columns=[])

    # State variance driver aligned to event times; fallback to minimum.
    if state_var_series is not None and not state_var_series.empty:
        state_var_events = (
            state_var_series.reindex(event_index).ffill().fillna(settings.min_state_variance).values
        )
    else:
        state_var_events = np.full(len(event_index), settings.min_state_variance, dtype=float)

    # Kalman filter init
    x = None  # [price, velocity]
    P = None  # 2x2 covariance
    last_ts = None

    mu_list: list[float] = []
    var_list: list[float] = []

    # Fast column index for bias lookup
    bias_cols = {c: i for i, c in enumerate(bias_at_events.columns)}
    bias_values = bias_at_events.values  # shape (N, V) or (N, 0)

    feeds = market.get("feed") if "feed" in market.columns else pd.Series(["default"] * len(market))

    for i, row in enumerate(market.itertuples()):
        price = float(row.mid_price)
        venue = getattr(row, "venue", getattr(row, "exchange", ""))
        feed = str(feeds.iat[i])
        t = int(ts_ns[i])

        # Prediction step
        if last_ts is not None:
            dt = max(0.0, (t - last_ts) / 1e9)
            F = np.array([[1.0, dt], [0.0, 1.0]], dtype=float)
            q = settings.accel_var_scale * float(state_var_events[i])
            Q = q * np.array([[dt ** 3 / 3.0, dt ** 2 / 2.0], [dt ** 2 / 2.0, dt]], dtype=float)
            if x is not None and P is not None:
                x = F @ x
                P = F @ P @ F.T + Q
        else:
            # First timestamp; will initialize after forming measurement.
            dt = 0.0

        # De-biased measurement
        if venue in bias_cols and bias_values.size:
            bias_val = float(bias_values[i, bias_cols[venue]])
        else:
            bias_val = 0.0
        z = price - bias_val

        # Initialize filter state from first measurement
        if x is None:
            x = np.array([z, 0.0], dtype=float)
            P = np.diag([settings.initial_price_var, settings.initial_velocity_var]).astype(float)
            last_ts = t
            mu_list.append(float(x[0]))
            var_list.append(float(P[0, 0]))
            continue

        # Measurement noise (basis points scaled by price level)
        std_bp = settings.feed_noise_bp.get(feed, settings.feed_noise_bp.get("default", 6.0))
        meas_std = max(1e-6, abs(z) * (std_bp / 10_000.0))
        R = meas_std ** 2

        # Update step (H = [1, 0])
        H = np.array([[1.0, 0.0]], dtype=float)
        y = z - x[0]
        S = float(P[0, 0] + R)
        K = np.array([P[0, 0] / S, P[1, 0] / S], dtype=float)
        x = x + K * y
        # Joseph form could be used; simple form suffices here for H=[1,0]
        I_KH = np.array([[1.0 - K[0], 0.0 - K[1] * 0.0], [-K[1] * 1.0, 1.0 - 0.0]], dtype=float)
        # But simplify explicitly: (I-KH)P with H=[1,0]
        P00 = (1.0 - K[0]) * P[0, 0]
        P01 = (1.0 - K[0]) * P[0, 1]
        P10 = P[1, 0] - K[1] * P[0, 0]
        P11 = P[1, 1] - K[1] * P[0, 1]
        P = np.array([[P00, P01], [P10, P11]], dtype=float)

        last_ts = t
        mu_list.append(float(x[0]))
        var_list.append(float(P[0, 0]))

    out = pd.DataFrame(
        {
            "timestamp": event_index,
            "ts_ns": ts_ns,
            "efficient_price": mu_list,
            "consensus_price": mu_list,
            "state_variance": var_list,
        }
    )
    return out


def _demean_fuse_prices(
    market: pd.DataFrame,
    settings: Settings,
) -> pd.DataFrame:
    """Rolling de-mean vs anchor and plot around most recent de-biased value.

    - Compute rolling mean bias for each venue relative to the anchor over a
      trailing time window of `demean_window_s` seconds, using asof alignment
      (non-anchor tick vs last-known anchor price).
    - At each event, the fair estimate is the current event's de-biased price.
      Between events, the line is stepped (most recent value).
    - The envelope uses cross-sectional variance of the most recent de-biased
      quotes from venues whose last tick is within `cross_section_recency_s`.
    """
    if market.empty:
        return pd.DataFrame()

    anchor = settings.anchor_exchange.lower()
    df = market.sort_values("ts_ns").copy()

    # Split anchor and non-anchor streams
    anchor_df = df[df["venue"] == anchor][["timestamp", "ts_ns", "mid_price"]].rename(
        columns={"mid_price": "anchor_price"}
    )
    others = df[df["venue"] != anchor].copy()

    if anchor_df.empty:
        # Without anchor, cannot de-mean; return a pass-through of last seen price
        return pd.DataFrame(
            {
                "timestamp": df["timestamp"],
                "ts_ns": df["ts_ns"],
                "efficient_price": df["mid_price"].values,
                "consensus_price": df["mid_price"].values,
                "state_variance": np.full(len(df), settings.min_state_variance),
            }
        )

    # Align each non-anchor tick with the most recent anchor price (asof join)
    anchor_sorted = anchor_df.sort_values("timestamp")
    others_sorted = others.sort_values("timestamp")
    merged = pd.merge_asof(
        others_sorted,
        anchor_sorted,
        on="timestamp",
        direction="backward",
        tolerance=pd.Timedelta(seconds=max(settings.demean_window_s, 0.0) * 10),
    )

    # Compute instantaneous diff where anchor is available
    merged["diff"] = merged["mid_price"] - merged["anchor_price"]
    merged = merged.dropna(subset=["diff"]).copy()
    if merged.empty:
        # If no overlaps, fallback to pass-through
        return pd.DataFrame(
            {
                "timestamp": df["timestamp"],
                "ts_ns": df["ts_ns"],
                "efficient_price": df["mid_price"].values,
                "consensus_price": df["mid_price"].values,
                "state_variance": np.full(len(df), settings.min_state_variance),
            }
        )

    # Rolling time-window mean of diff per venue
    window = f"{max(settings.demean_window_s, 0.0)}s"
    merged = merged.sort_values(["venue", "timestamp"])  # group-rolling needs sorted index
    bias_roll = (
        merged.set_index("timestamp").groupby("venue")["diff"].rolling(window).mean().rename("bias")
    )
    bias_roll = bias_roll.reset_index()

    # Bias series in wide form to allow quick lookup by venue at arbitrary times
    bias_wide = bias_roll.pivot(index="timestamp", columns="venue", values="bias").sort_index()

    # Build output on the full event timeline; at each event use the event's
    # venue bias (ffill) to de-bias the event price and step the line.
    ts_ns = df["ts_ns"].astype(np.int64).values
    event_index = pd.to_datetime(ts_ns, unit="ns", utc=True).tz_convert(None)
    venues = df["venue"].astype(str).values
    prices = df["mid_price"].astype(float).values

    bias_events = bias_wide.reindex(event_index).ffill().reindex(event_index)  # ensure exact index
    # For venues without any computed bias, use 0.0
    bias_events = bias_events.fillna(0.0)

    # Vectorized lookup of bias per event venue
    bias_cols = {c: i for i, c in enumerate(bias_events.columns)}
    bias_mat = bias_events.values  # shape (N, V)
    event_bias = np.zeros(len(df), dtype=float)
    for i, v in enumerate(venues):
        j = bias_cols.get(v)
        event_bias[i] = bias_mat[i, j] if j is not None else 0.0

    debiased = prices - event_bias

    # Cross-sectional variance band: collect most recent de-biased per venue
    recency = pd.Timedelta(seconds=max(settings.cross_section_recency_s, 0.0))
    last_by_venue: Dict[str, tuple[pd.Timestamp, float]] = {}
    variances: list[float] = []
    for i, ts in enumerate(event_index):
        v = venues[i]
        last_by_venue[v] = (ts, debiased[i])
        # collect values within recency window
        values = [val for (t0, val) in last_by_venue.values() if ts - t0 <= recency]
        if len(values) >= 2:
            variances.append(float(np.var(values, ddof=1)))
        else:
            variances.append(settings.min_state_variance)

    out = pd.DataFrame(
        {
            "timestamp": event_index,
            "ts_ns": ts_ns,
            "efficient_price": debiased,
            "consensus_price": debiased,
            "state_variance": variances,
        }
    )
    return out


def run_analysis(
    settings: Settings = DEFAULT_SETTINGS,
    preloaded: pd.DataFrame | None = None,
    mode: str = "anchor",
) -> tuple[pd.DataFrame, pd.DataFrame, RegressionSummary]:
    market = preloaded if preloaded is not None else load_ticks(settings)
    if market.empty:
        return pd.DataFrame(), pd.DataFrame(), EMPTY_SUMMARY

    anchor_series, state_var_series, bias_df = learn_bias_series(market, settings)

    if mode == "anchor":
        if anchor_series.empty:
            return pd.DataFrame(), pd.DataFrame(), EMPTY_SUMMARY

        index = anchor_series.index
        state_var_series = (
            state_var_series.reindex(index, method="ffill").fillna(settings.min_state_variance)
        )
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
            price_frame[f"local_fair_{venue}"] = (
                anchor_series.add(bias_df[venue], axis=0).reindex(index).values
            )

        bias_long = bias_df.stack().reset_index()
        bias_long.columns = ["timestamp", "exchange", "bias"]
        bias_long["local_fair"] = (
            bias_long["timestamp"].map(anchor_series).add(bias_long["bias"], fill_value=np.nan)
        )

        return price_frame, bias_long, EMPTY_SUMMARY

    if mode == "kalman":
        fused_frame = _kalman_fuse_prices(market, bias_df, state_var_series, settings)
        # Provide bias_long (aligned to anchor timeline) for optional inspection
        if not bias_df.empty and not anchor_series.empty:
            bias_long = bias_df.stack().reset_index()
            bias_long.columns = ["timestamp", "exchange", "bias"]
            bias_long["local_fair"] = (
                bias_long["timestamp"].map(anchor_series).add(bias_long["bias"], fill_value=np.nan)
            )
        else:
            bias_long = pd.DataFrame(columns=["timestamp", "exchange", "bias", "local_fair"])  # empty
        return fused_frame, bias_long, EMPTY_SUMMARY

    if mode == "demean":
        fused_frame = _demean_fuse_prices(market, settings)
        # Derive a simple bias_long based on rolling de-mean snapshots
        # (using the internal pivot's index as timestamps)
        try:
            # Recompute a lightweight rolling bias table for export
            anchor = settings.anchor_exchange.lower()
            df = market.sort_values("ts_ns").copy()
            anchor_df = df[df["venue"] == anchor][["timestamp", "mid_price"]].rename(
                columns={"mid_price": "anchor_price"}
            )
            others = df[df["venue"] != anchor].copy()
            anchor_sorted = anchor_df.sort_values("timestamp")
            others_sorted = others.sort_values("timestamp")
            merged = pd.merge_asof(
                others_sorted,
                anchor_sorted,
                on="timestamp",
                direction="backward",
                tolerance=pd.Timedelta(seconds=max(settings.demean_window_s, 0.0) * 10),
            )
            merged["diff"] = merged["mid_price"] - merged["anchor_price"]
            merged = merged.dropna(subset=["diff"]).copy()
            window = f"{max(settings.demean_window_s, 0.0)}s"
            bias_roll = (
                merged.set_index("timestamp").groupby("venue")["diff"].rolling(window).mean().rename("bias")
            ).reset_index()
            bias_roll.columns = ["timestamp", "exchange", "bias"]
            bias_roll["local_fair"] = np.nan  # not defined on anchor timeline
            bias_long = bias_roll
        except Exception:
            bias_long = pd.DataFrame(columns=["timestamp", "exchange", "bias", "local_fair"])  # empty
        return fused_frame, bias_long, EMPTY_SUMMARY
    # Should not reach here
    return pd.DataFrame(), pd.DataFrame(), EMPTY_SUMMARY


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
    parser.add_argument(
        "--mode",
        choices=["anchor", "kalman", "demean"],
        default="anchor",
        help="Pricing mode: anchor overlay, Kalman fusion, or rolling de-mean",
    )
    parser.add_argument(
        "--demean-window-s",
        type=float,
        default=DEFAULT_SETTINGS.demean_window_s,
        help="Rolling window (seconds) for de-meaning vs anchor in 'demean' mode",
    )
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
        demean_window_s=args.demean_window_s if hasattr(args, "demean_window_s") else DEFAULT_SETTINGS.demean_window_s,
    )

    market = load_ticks(settings)
    price_frame, _, _ = run_analysis(settings, preloaded=market, mode=args.mode)
    if price_frame.empty:
        print("No data available for selected mode.")
        return

    _plot_market_with_overlay(market, price_frame, sigma=args.sigma)


if __name__ == "__main__":
    main()
