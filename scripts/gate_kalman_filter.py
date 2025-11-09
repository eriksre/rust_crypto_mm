#!/usr/bin/env python3
"""
Multi-venue pricing framework for Gate quoting overlays.

The previous version of this module only exposed CSV loading helpers.  It now
builds a latency-aware, depth-weighted consensus from every venue in the activity
log, blends it with the live Gate mid (only when Gate is trustworthy), applies a
mean-reversion guardrail to damp flash-move thrash, and finally runs a 1-D
Kalman filter so plotting utilities can visualise an efficient price together
with its confidence band.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd


# --------------------------------------------------------------------------- #
# Configuration                                                               #
# --------------------------------------------------------------------------- #


@dataclass
class BasicSettings:
    """Configuration driving the consensus + Kalman pipeline."""

    input_path: Path = Path("logs/gate_activity.csv")
    row_limit: Optional[int] = None
    exchange_filter: Optional[str] = None  # When provided, becomes the target venue.

    # Market data selection / normalisation.
    target_exchange: str = "gate"
    target_aliases: Tuple[str, ...] = ("gate", "gateio")
    market_feeds: Tuple[str, ...] = ("orderbook", "bbo", "trade")

    # Resampling parameters.
    resample_ms: int = 50
    max_staleness_ms: int = 600

    # Weighting heuristics.
    consensus_half_life_ms: int = 350
    latency_half_life_ms: int = 150
    outlier_threshold_bp: float = 35.0
    gate_outlier_bp: float = 80.0
    gate_base_weight: float = 0.78
    lead_time_ms: float = 80.0
    lead_bonus: float = 0.2

    # Mean reversion guardrail.
    mean_reversion_threshold_pct: float = 0.004  # 40 bps
    mean_reversion_shrink: float = 0.15
    mean_reversion_span: int = 80

    # Kalman tuning.
    process_noise: float = 2e-5
    measurement_noise: float = 8e-4
    dispersion_ref: float = 0.0015


DEFAULT_SETTINGS = BasicSettings()


@dataclass
class RegressionSummary:
    """Regression diagnostics for consensus -> Gate prediction."""

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
# Loading & preparation                                                       #
# --------------------------------------------------------------------------- #


def load_events(settings: BasicSettings = DEFAULT_SETTINGS) -> pd.DataFrame:
    """Load the activity CSV and apply very light normalisation."""

    path = settings.input_path.expanduser()
    if not path.exists():
        raise FileNotFoundError(f"Input file does not exist: {path}")

    df = pd.read_csv(path, nrows=settings.row_limit, low_memory=False)
    if "ts_ns" not in df.columns:
        raise ValueError("CSV is missing 'ts_ns'; cannot build a timeline.")

    df["ts_ns"] = pd.to_numeric(df["ts_ns"], errors="coerce")
    df = df.dropna(subset=["ts_ns"]).copy()

    categorical = ("exchange", "feed", "event_type", "direction")
    for col in categorical:
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower().str.strip()

    return df.reset_index(drop=True)


def _normalise_exchange(value: str, alias_map: Dict[str, str]) -> str:
    key = str(value).lower().strip()
    return alias_map.get(key, key)


def prepare_market_events(df: pd.DataFrame, settings: BasicSettings) -> pd.DataFrame:
    """Filter to market data rows and derive per-feed metrics."""

    if df.empty:
        return df

    target_override = (settings.exchange_filter or settings.target_exchange).lower()
    alias_map = {alias.lower(): target_override for alias in settings.target_aliases}
    alias_map[target_override] = target_override

    numeric_cols = [
        "price",
        "bid_px_1",
        "ask_px_1",
        "bid_sz_1",
        "ask_sz_1",
        "bid_depth",
        "ask_depth",
        "source_engine_ts_ns",
        "source_system_ts_ns",
        "size",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    market = df[df["event_type"] == "market"].copy()
    market = market[market["feed"].isin(settings.market_feeds)]
    if market.empty:
        return market

    bid = market.get("bid_px_1")
    ask = market.get("ask_px_1")
    has_book = bid.notna() & ask.notna() if bid is not None and ask is not None else pd.Series(False, index=market.index)
    market["mid_price"] = np.where(
        has_book,
        (bid + ask) / 2.0,
        market["price"],
    )
    market["mid_price"] = market["mid_price"].astype(float)

    market["liquidity_score"] = (
        market.get("bid_depth", 0.0).fillna(0.0)
        + market.get("ask_depth", 0.0).fillna(0.0)
    )
    if "bid_sz_1" in market.columns and "ask_sz_1" in market.columns:
        top_depth = market["bid_sz_1"].fillna(0.0) + market["ask_sz_1"].fillna(0.0)
        market["liquidity_score"] = market["liquidity_score"].where(
            market["liquidity_score"] > 0, top_depth
        )

    if "size" in market.columns:
        market.loc[market["feed"] == "trade", "liquidity_score"] = market.loc[
            market["feed"] == "trade", "size"
        ].abs()

    market["liquidity_score"] = np.log1p(market["liquidity_score"].clip(lower=0.0))

    if has_book.any():
        spread = (ask - bid).where(has_book)
        mid = ((ask + bid) / 2.0).where(has_book)
        market["spread_pct"] = (spread / mid).replace([np.inf, -np.inf], np.nan)
    else:
        market["spread_pct"] = np.nan

    if "source_engine_ts_ns" in market.columns:
        latency_ns = market["ts_ns"] - market["source_engine_ts_ns"]
        market["latency_ms"] = latency_ns / 1e6
    else:
        market["latency_ms"] = np.nan

    market = market.dropna(subset=["mid_price"]).copy()
    market["timestamp"] = pd.to_datetime(market["ts_ns"], unit="ns", utc=True)
    market["venue"] = market["exchange"].apply(lambda x: _normalise_exchange(x, alias_map))

    feed_priority = {"orderbook": 0, "bbo": 1, "trade": 2}
    market["feed_priority"] = market["feed"].map(feed_priority).fillna(5)
    market = market.sort_values(["ts_ns", "feed_priority"])
    market = market.drop_duplicates(subset=["ts_ns", "venue"], keep="first")
    return market.reset_index(drop=True)


# --------------------------------------------------------------------------- #
# Consensus building                                                          #
# --------------------------------------------------------------------------- #


def _build_timeline_index(market: pd.DataFrame, settings: BasicSettings) -> pd.DatetimeIndex:
    start = pd.to_datetime(market["ts_ns"].min(), unit="ns", utc=True)
    end = pd.to_datetime(market["ts_ns"].max(), unit="ns", utc=True)
    if start == end:
        return pd.DatetimeIndex([start])

    freq_value = max(1, settings.resample_ms)
    freq = f"{freq_value}ms"
    aligned_start = start.floor(freq)
    aligned_end = end.ceil(freq)
    return pd.date_range(aligned_start, aligned_end, freq=freq)


def _pivot_feature(
    market: pd.DataFrame,
    column: str,
    timeline: pd.DatetimeIndex,
    limit: int,
) -> pd.DataFrame:
    if column not in market.columns:
        return pd.DataFrame(index=timeline)

    pivot = (
        market.pivot_table(index="timestamp", columns="venue", values=column, aggfunc="last")
        .sort_index()
    )
    pivot = pivot.reindex(timeline)
    pivot = pivot.ffill(limit=limit)
    return pivot


def _compute_staleness(
    market: pd.DataFrame,
    timeline: pd.DatetimeIndex,
) -> pd.DataFrame:
    ts_pivot = (
        market.pivot_table(index="timestamp", columns="venue", values="ts_ns", aggfunc="last")
        .sort_index()
    )
    ts_pivot = ts_pivot.reindex(timeline).ffill()
    timeline_ns = timeline.view("int64")
    values = (timeline_ns.reshape(-1, 1) - ts_pivot.to_numpy()) / 1e6
    staleness = pd.DataFrame(values, index=timeline, columns=ts_pivot.columns)
    return staleness


def _compute_consensus(
    prices: pd.DataFrame,
    liquidity: pd.DataFrame,
    latency: pd.DataFrame,
    staleness: pd.DataFrame,
    settings: BasicSettings,
) -> tuple[pd.Series, pd.DataFrame, pd.Series, pd.Series]:
    if prices.empty:
        index = liquidity.index if not liquidity.empty else pd.Index([])
        return (
            pd.Series(dtype=float, index=index),
            pd.DataFrame(index=index),
            pd.Series(dtype=float, index=index),
            pd.Series(dtype=float, index=index),
        )

    limit_mask = prices.notna()
    freshness = np.exp(
        -staleness.clip(lower=0.0).divide(settings.consensus_half_life_ms)
    )
    freshness = freshness.where(limit_mask, 0.0)

    latency_penalty = np.exp(
        -latency.clip(lower=0.0).divide(settings.latency_half_life_ms)
    ).where(limit_mask, 0.0)

    base_weight = liquidity.where(limit_mask, 0.0).fillna(0.0) + 1.0

    median = prices.median(axis=1, skipna=True)
    median = median.replace(0.0, np.nan)
    diff_pct = prices.subtract(median, axis=0).abs().divide(median.abs(), axis=0)
    threshold = max(settings.outlier_threshold_bp, 1e-3) / 10000.0
    outlier_penalty = np.exp(-np.square(diff_pct / threshold)).where(limit_mask, 0.0)

    weights = base_weight * freshness * latency_penalty * outlier_penalty
    weight_sum = weights.sum(axis=1)
    consensus = weights.mul(prices).sum(axis=1).divide(weight_sum)

    abs_dev = prices.subtract(median, axis=0).abs()
    mad = abs_dev.median(axis=1, skipna=True)
    dispersion = (mad / median.abs()).replace([np.inf, -np.inf], np.nan)
    obs_count = limit_mask.sum(axis=1)

    return consensus, weights, dispersion, obs_count


# --------------------------------------------------------------------------- #
# Regression + blending                                                       #
# --------------------------------------------------------------------------- #


def _fit_gate_regression(consensus: pd.Series, gate_mid: pd.Series) -> RegressionSummary:
    reg_df = pd.DataFrame(
        {
            "gate_mid": gate_mid,
            "consensus": consensus,
            "gate_lag": gate_mid.shift(1),
        }
    ).dropna()

    if reg_df.empty or len(reg_df) < 50:
        return EMPTY_SUMMARY

    X = np.column_stack(
        [
            np.ones(len(reg_df)),
            reg_df["consensus"].to_numpy(),
            reg_df["gate_lag"].to_numpy(),
        ]
    )
    y = reg_df["gate_mid"].to_numpy()
    coeffs, *_ = np.linalg.lstsq(X, y, rcond=None)
    preds = X @ coeffs
    residuals = y - preds

    summary = RegressionSummary(
        coefficients=pd.Series(
            coeffs,
            index=["intercept", "consensus", "gate_lag"],
            dtype=float,
        ),
        rmse=float(np.sqrt(np.mean(np.square(residuals)))),
        mae=float(np.mean(np.abs(residuals))),
        sample_count=len(reg_df),
    )
    return summary


def _apply_mean_reversion(series: pd.Series, settings: BasicSettings) -> pd.Series:
    if series.empty:
        return series

    ema = series.ewm(span=settings.mean_reversion_span, adjust=False, ignore_na=True).mean()
    diff = series - ema
    rel = diff.divide(ema).replace([np.inf, -np.inf], np.nan)
    excess = rel.abs() - settings.mean_reversion_threshold_pct
    mask = excess > 0
    adjusted = ema + np.sign(diff) * ema * (
        settings.mean_reversion_threshold_pct + excess * (1.0 - settings.mean_reversion_shrink)
    )
    result = series.copy()
    result[mask] = adjusted[mask]
    return result


def _blend_gate_with_consensus(
    consensus: pd.Series,
    gate_mid: pd.Series,
    staleness: pd.Series,
    latency: pd.Series,
    settings: BasicSettings,
) -> tuple[pd.Series, pd.Series]:
    if consensus.empty and gate_mid.empty:
        return consensus, pd.Series(dtype=float, index=consensus.index)

    gate_weight = pd.Series(0.0, index=consensus.index)
    if gate_mid.empty:
        blended = consensus
        return blended, gate_weight

    freshness = np.exp(-staleness.clip(lower=0.0) / settings.consensus_half_life_ms)
    latency_penalty = np.exp(-latency.clip(lower=0.0) / settings.latency_half_life_ms)
    scale = max(settings.gate_outlier_bp, 1e-3) / 10000.0
    diff_pct = (gate_mid - consensus).abs().divide(consensus.abs())
    outlier_penalty = np.exp(-np.square(diff_pct / scale))

    gate_weight = (
        settings.gate_base_weight
        * freshness.fillna(0.0)
        * latency_penalty.fillna(1.0)
        * outlier_penalty.fillna(0.0)
    )

    gate_weight = gate_weight.clip(lower=0.0, upper=1.0)

    blended = consensus.copy()
    mask = gate_mid.notna() & consensus.notna()
    blended[mask] = (
        (1.0 - gate_weight[mask]) * consensus[mask] + gate_weight[mask] * gate_mid[mask]
    )
    blended = blended.where(consensus.notna(), gate_mid)
    blended = blended.where(blended.notna(), gate_mid)
    return blended, gate_weight


# --------------------------------------------------------------------------- #
# Kalman filtering                                                            #
# --------------------------------------------------------------------------- #


def _run_kalman_filter(
    observations: pd.Series,
    measurement_var: pd.Series,
    settings: BasicSettings,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    if observations.empty:
        return observations, measurement_var, measurement_var

    non_nan = observations.dropna()
    if non_nan.empty:
        empty = pd.Series(np.nan, index=observations.index, dtype=float)
        gains = pd.Series(0.0, index=observations.index, dtype=float)
        return empty, empty.copy(), gains

    state_est = pd.Series(index=observations.index, dtype=float)
    state_var = pd.Series(index=observations.index, dtype=float)
    gains = pd.Series(index=observations.index, dtype=float)

    estimate = non_nan.iloc[0]
    variance = measurement_var.dropna().iloc[0] if measurement_var.dropna().any() else settings.measurement_noise

    for idx in observations.index:
        pred_est = estimate
        pred_var = variance + settings.process_noise
        obs = observations.loc[idx]
        meas_var = measurement_var.loc[idx] if not pd.isna(measurement_var.loc[idx]) else settings.measurement_noise

        if pd.isna(obs):
            estimate = pred_est
            variance = pred_var
            gain = 0.0
        else:
            kalman_gain = pred_var / (pred_var + meas_var)
            estimate = pred_est + kalman_gain * (obs - pred_est)
            variance = (1.0 - kalman_gain) * pred_var
            gain = kalman_gain

        state_est.loc[idx] = estimate
        state_var.loc[idx] = variance
        gains.loc[idx] = gain

    return state_est, state_var, gains


# --------------------------------------------------------------------------- #
# Public API                                                                  #
# --------------------------------------------------------------------------- #


def run_analysis(
    settings: BasicSettings = DEFAULT_SETTINGS,
) -> tuple[pd.DataFrame, pd.DataFrame, RegressionSummary]:
    """Entry point consumed by plotting scripts."""

    df = load_events(settings)
    market = prepare_market_events(df, settings)
    if market.empty:
        return pd.DataFrame(), pd.DataFrame(), EMPTY_SUMMARY

    target = (settings.exchange_filter or settings.target_exchange).lower()
    timeline = _build_timeline_index(market, settings)
    if timeline.empty:
        return pd.DataFrame(), pd.DataFrame(), EMPTY_SUMMARY

    ffill_limit = max(1, int(np.floor(settings.max_staleness_ms / settings.resample_ms)))
    prices = _pivot_feature(market, "mid_price", timeline, ffill_limit)
    liquidity = _pivot_feature(market, "liquidity_score", timeline, ffill_limit)
    latency = _pivot_feature(market, "latency_ms", timeline, ffill_limit)
    staleness = _compute_staleness(market, timeline)

    # Align matrices.
    common_cols = prices.columns
    liquidity = liquidity.reindex(columns=common_cols)
    latency = latency.reindex(columns=common_cols)
    staleness = staleness.reindex(columns=common_cols)

    external_cols = [col for col in prices.columns if col != target]
    if not external_cols:
        external_cols = list(prices.columns)

    consensus, weights, dispersion, obs_count = _compute_consensus(
        prices[external_cols],
        liquidity[external_cols],
        latency[external_cols],
        staleness[external_cols],
        settings,
    )

    gate_mid = prices[target] if target in prices.columns else pd.Series(dtype=float, index=timeline)
    gate_latency = latency[target] if target in latency.columns else pd.Series(dtype=float, index=timeline)
    gate_staleness = staleness[target] if target in staleness.columns else pd.Series(dtype=float, index=timeline)

    blended, gate_weight = _blend_gate_with_consensus(
        consensus,
        gate_mid,
        gate_staleness,
        gate_latency,
        settings,
    )

    regression_summary = _fit_gate_regression(consensus, gate_mid)
    reg_inputs = pd.DataFrame(
        {
            "consensus": consensus,
            "gate_lag": gate_mid.shift(1),
        }
    )
    if regression_summary.sample_count > 0:
        coeffs = regression_summary.coefficients.reindex(["intercept", "consensus", "gate_lag"]).fillna(0.0)
        reg_preds = coeffs["intercept"] + coeffs["consensus"] * reg_inputs["consensus"] + coeffs["gate_lag"] * reg_inputs["gate_lag"]
    else:
        reg_preds = pd.Series(dtype=float, index=timeline)

    blended = blended.combine_first(reg_preds)
    blended = blended.combine_first(consensus)

    mean_reverted = _apply_mean_reversion(blended, settings)

    dispersion = dispersion.reindex(timeline).ffill()
    dispersion = dispersion.fillna(0.0)
    effective_meas = settings.measurement_noise * (1.0 + np.square(dispersion / max(settings.dispersion_ref, 1e-6)))
    effective_meas = effective_meas.divide(obs_count.clip(lower=1))

    efficient_price, state_var, kalman_gain = _run_kalman_filter(
        mean_reverted,
        effective_meas,
        settings,
    )

    price_frame = pd.DataFrame(
        {
            "timestamp": timeline,
            "consensus_price": consensus.reindex(timeline),
            "consensus_dispersion": dispersion.reindex(timeline),
            "external_weight_sum": weights.sum(axis=1).reindex(timeline),
            "obs_count": obs_count.reindex(timeline),
            "gate_mid": gate_mid.reindex(timeline),
            "gate_weight": gate_weight.reindex(timeline),
            "blended_price": blended.reindex(timeline),
            "mean_reverted_price": mean_reverted.reindex(timeline),
            "efficient_price": efficient_price.reindex(timeline),
            "state_variance": state_var.reindex(timeline),
            "measurement_var": effective_meas.reindex(timeline),
            "kalman_gain": kalman_gain.reindex(timeline),
        }
    ).dropna(subset=["timestamp"]).reset_index(drop=True)

    diag_frames = []
    for col in prices.columns:
        diag = pd.DataFrame(
            {
                "timestamp": timeline,
                "exchange": col,
                "mid_price": prices[col],
                "liquidity_score": liquidity[col],
                "latency_ms": latency[col],
                "staleness_ms": staleness[col],
                "weight": weights[col] if col in weights.columns else (gate_weight if col == target else np.nan),
            }
        )
        diag_frames.append(diag)

    diagnostics = (
        pd.concat(diag_frames, ignore_index=True)
        .dropna(subset=["mid_price"], how="all")
        .reset_index(drop=True)
    )

    return price_frame, diagnostics, regression_summary


def main() -> None:
    prices, _, summary = run_analysis(DEFAULT_SETTINGS)
    print(f"Generated {len(prices)} price points. Regression samples: {summary.sample_count}.")


if __name__ == "__main__":
    main()
