"""
Predict Lighter mid-price using a (scalar) state-space Kalman filter.

Data source: logs/lighter_activity.csv (event stream across venues).

Model (in log-price space, centered by a constant mu):
  x_t = exp(k * dt) * x_{t-1} + w_t,         w_t ~ N(0, q_per_sec * dt)
  z_t = (log(obs_price_t) - mu) = x_t + b_s - gamma * imb_t + v, v ~ N(0, r_s)  for stream s

Notes:
- Trades are treated differently from books:
  - trade direction adds a small signed offset to the effective trade observation
  - trades are down-weighted (larger measurement variance; optionally scaled by size)
- Orderbook-derived features (mid/micro/vwap5) are combined into a single observation per update
  to avoid violating conditional-independence with multiple "virtual observations".
- Measurement variance is inflated when a stream looks latent, stale, or in high-vol regimes;
  and robustly gated for outliers (Huber-like).

We fit {k, q_per_sec, b_s, r_s} on the first 80% of time-ordered events (market-only),
then run a causal filter over the full sample and plot predictions vs Lighter orderbook mid.

Run:
  .venv/bin/python scripts/lighter_pricing_model.py --csv logs/lighter_activity.csv
"""

from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd

try:
    import matplotlib.pyplot as plt
    import matplotlib
except ModuleNotFoundError as e:  # pragma: no cover
    raise SystemExit(
        "matplotlib is required. Run with the repo venv:\n"
        "  .venv/bin/python scripts/lighter_pricing_model.py ..."
    ) from e

try:
    from scipy.optimize import minimize_scalar
except ModuleNotFoundError as e:  # pragma: no cover
    raise SystemExit(
        "scipy is required. Run with the repo venv:\n"
        "  .venv/bin/python scripts/lighter_pricing_model.py ..."
    ) from e


MARKET_EVENT_TYPE = "market"
TARGET_EXCHANGE = "lighter"
TARGET_FEED = "orderbook"
DEFAULT_TRAIN_FRAC = 0.8


@dataclass(frozen=True)
class KalmanParams:
    mu_log: float
    k_per_sec: float  # k <= 0; phi(dt) = exp(k * dt)
    q_per_sec: float
    gamma_imb: float
    bias_by_stream: Dict[str, float]
    r_by_stream: Dict[str, float]
    ref_stream: str
    latency_median_us: Dict[str, float]
    latency_mad_us: Dict[str, float]
    spread_median_bps: Dict[str, float]
    spread_mad_bps: Dict[str, float]
    top_ratio_median: Dict[str, float]
    top_ratio_mad: Dict[str, float]


@dataclass(frozen=True)
class FilterTuning:
    trade_dir_bps: float
    trade_r_mult: float
    trade_size_beta: float
    book_w_mid: float
    book_w_micro: float
    book_w_vwap5: float
    lighter_r_mult: float
    latency_alpha: float
    stale_alpha: float
    vol_alpha: float
    vol_halflife_s: float
    robust_z: float
    jump_z: float
    jump_beta: float
    bias_ewma_halflife_s: float
    horizon_ms: float
    q_floor_mult: float
    q_dt_floor_ms: float
    eval_move_bps: float


def _ns_to_datetime(ns: np.ndarray) -> np.ndarray:
    return pd.to_datetime(ns, unit="ns", utc=True).to_numpy()


def _safe_log_prices(prices: np.ndarray) -> np.ndarray:
    prices = np.asarray(prices, dtype=np.float64)
    prices = np.where(prices > 0, prices, np.nan)
    return np.log(prices)


def _engine_time_ns(df: pd.DataFrame) -> pd.Series:
    # Prefer matching-engine timestamp; fall back to system timestamp; then wire timestamp.
    t = df["source_engine_ts_ns"].copy()
    t = t.where(~t.isna(), df["source_system_ts_ns"])
    t = t.where(~t.isna(), df["ts_ns"])
    return t.astype("int64")


def _mid_from_top_of_book(df: pd.DataFrame) -> pd.Series:
    bid = pd.to_numeric(df["bid_px_1"], errors="coerce")
    ask = pd.to_numeric(df["ask_px_1"], errors="coerce")
    return (bid + ask) / 2.0


def _microprice(df: pd.DataFrame) -> pd.Series:
    bid = pd.to_numeric(df["bid_px_1"], errors="coerce")
    ask = pd.to_numeric(df["ask_px_1"], errors="coerce")
    bid_sz = pd.to_numeric(df["bid_sz_1"], errors="coerce")
    ask_sz = pd.to_numeric(df["ask_sz_1"], errors="coerce")
    denom = bid_sz + ask_sz
    out = (bid * ask_sz + ask * bid_sz) / denom
    return out.where(denom > 0)


def _vwap_mid_top5(df: pd.DataFrame) -> pd.Series:
    bid_prices = []
    bid_sizes = []
    ask_prices = []
    ask_sizes = []
    for level in range(1, 6):
        bid_prices.append(pd.to_numeric(df.get(f"bid_px_{level}"), errors="coerce"))
        bid_sizes.append(pd.to_numeric(df.get(f"bid_sz_{level}"), errors="coerce"))
        ask_prices.append(pd.to_numeric(df.get(f"ask_px_{level}"), errors="coerce"))
        ask_sizes.append(pd.to_numeric(df.get(f"ask_sz_{level}"), errors="coerce"))

    bid_prices = np.vstack([s.to_numpy(dtype=np.float64) for s in bid_prices])
    bid_sizes = np.vstack([s.to_numpy(dtype=np.float64) for s in bid_sizes])
    ask_prices = np.vstack([s.to_numpy(dtype=np.float64) for s in ask_prices])
    ask_sizes = np.vstack([s.to_numpy(dtype=np.float64) for s in ask_sizes])

    bid_sz_sum = np.nansum(bid_sizes, axis=0)
    ask_sz_sum = np.nansum(ask_sizes, axis=0)
    with np.errstate(divide="ignore", invalid="ignore"):
        bid_vwap = np.nansum(bid_prices * bid_sizes, axis=0) / bid_sz_sum
        ask_vwap = np.nansum(ask_prices * ask_sizes, axis=0) / ask_sz_sum

    bid_vwap = np.where(bid_sz_sum > 0, bid_vwap, np.nan)
    ask_vwap = np.where(ask_sz_sum > 0, ask_vwap, np.nan)
    return pd.Series((bid_vwap + ask_vwap) / 2.0, index=df.index)

def _median_mad(x: np.ndarray) -> Tuple[float, float]:
    x = np.asarray(x, dtype=np.float64)
    x = x[np.isfinite(x)]
    if len(x) == 0:
        return 0.0, 1.0
    med = float(np.median(x))
    mad = float(np.median(np.abs(x - med)))
    mad = max(mad, 1e-6)
    return med, mad


def _combine_book_prices(
    mid: np.ndarray, micro: np.ndarray, vwap5: np.ndarray, w_mid: float, w_micro: float, w_vwap5: float
) -> np.ndarray:
    mid = np.asarray(mid, dtype=np.float64)
    micro = np.asarray(micro, dtype=np.float64)
    vwap5 = np.asarray(vwap5, dtype=np.float64)
    weights = np.array([w_mid, w_micro, w_vwap5], dtype=np.float64)
    weights = np.where(weights >= 0, weights, 0.0)
    if weights.sum() <= 0:
        weights = np.array([1.0, 0.0, 0.0], dtype=np.float64)
    p = np.vstack([mid, micro, vwap5])
    w = np.broadcast_to(weights[:, None], p.shape).copy()
    w[~np.isfinite(p)] = 0.0
    wsum = np.sum(w, axis=0)
    out = np.sum(w * np.where(np.isfinite(p), p, 0.0), axis=0) / np.where(wsum > 0, wsum, np.nan)
    return out


def load_observations(
    csv_path: str | Path,
    *,
    max_rows: int | None = None,
    tuning: FilterTuning,
    time_basis: str = "wire",
) -> pd.DataFrame:
    usecols = [
        "ts_ns",
        "exchange",
        "feed",
        "event_type",
        "source_engine_ts_ns",
        "source_system_ts_ns",
        "price",
        "direction",
        "size",
        "bid_px_1",
        "bid_sz_1",
        "ask_px_1",
        "ask_sz_1",
        "bid_px_2",
        "bid_sz_2",
        "bid_px_3",
        "bid_sz_3",
        "bid_px_4",
        "bid_sz_4",
        "bid_px_5",
        "bid_sz_5",
        "ask_px_2",
        "ask_sz_2",
        "ask_px_3",
        "ask_sz_3",
        "ask_px_4",
        "ask_sz_4",
        "ask_px_5",
        "ask_sz_5",
        "bid_depth",
        "ask_depth",
    ]
    dtypes = {
        "ts_ns": "int64",
        "exchange": "category",
        "feed": "category",
        "event_type": "category",
        "source_engine_ts_ns": "float64",
        "source_system_ts_ns": "float64",
        "price": "float64",
        "direction": "category",
        "size": "float64",
        "bid_px_1": "float64",
        "bid_sz_1": "float64",
        "ask_px_1": "float64",
        "ask_sz_1": "float64",
        "bid_px_2": "float64",
        "bid_sz_2": "float64",
        "bid_px_3": "float64",
        "bid_sz_3": "float64",
        "bid_px_4": "float64",
        "bid_sz_4": "float64",
        "bid_px_5": "float64",
        "bid_sz_5": "float64",
        "ask_px_2": "float64",
        "ask_sz_2": "float64",
        "ask_px_3": "float64",
        "ask_sz_3": "float64",
        "ask_px_4": "float64",
        "ask_sz_4": "float64",
        "ask_px_5": "float64",
        "ask_sz_5": "float64",
        "bid_depth": "float64",
        "ask_depth": "float64",
    }

    df = pd.read_csv(
        csv_path,
        usecols=usecols,
        dtype=dtypes,
        nrows=max_rows,
        low_memory=False,
    )
    df = df[df["event_type"] == MARKET_EVENT_TYPE].copy()

    time_basis = str(time_basis).lower().strip()
    if time_basis not in {"wire", "engine"}:
        raise ValueError("--time-basis must be 'wire' or 'engine'")

    df["engine_ts_ns"] = _engine_time_ns(df)
    df["t_ns"] = df["ts_ns"].astype("int64") if time_basis == "wire" else df["engine_ts_ns"]
    df.sort_values("t_ns", inplace=True, kind="mergesort")

    df["mid"] = df["price"]
    df["mid"] = df["mid"].where(~df["mid"].isna(), _mid_from_top_of_book(df))
    df["micro"] = _microprice(df)
    df["vwap5"] = _vwap_mid_top5(df)

    feed_s = df["feed"].astype(str)
    is_trade = feed_s == "trade"
    is_booky = feed_s.isin(["orderbook", "bbo"])

    # Combine mid/micro/vwap5 into a single observation per book update.
    book_obs = _combine_book_prices(
        df["mid"].to_numpy(dtype=np.float64),
        df["micro"].to_numpy(dtype=np.float64),
        df["vwap5"].to_numpy(dtype=np.float64),
        tuning.book_w_mid,
        tuning.book_w_micro,
        tuning.book_w_vwap5,
    )

    # Trades: adjust the effective observation using direction (buy -> fair value above trade px).
    dir_s = df["direction"].astype(str).str.lower()
    dir_sign = np.where(dir_s == "buy", 1.0, np.where(dir_s == "sell", -1.0, 0.0))
    trade_offset_log = dir_sign * (tuning.trade_dir_bps * 1e-4)
    trade_obs = df["price"].to_numpy(dtype=np.float64) * np.exp(trade_offset_log)

    df["obs_price"] = np.where(is_trade.to_numpy(), trade_obs, book_obs)

    # Feature engineering for down-weighting.
    engine_ts = df["engine_ts_ns"].to_numpy(dtype=np.float64)
    wire_ts = df["ts_ns"].to_numpy(dtype=np.float64)
    df["latency_us"] = (wire_ts - engine_ts) / 1e3
    bid = df["bid_px_1"].to_numpy(dtype=np.float64)
    ask = df["ask_px_1"].to_numpy(dtype=np.float64)
    mid = df["mid"].to_numpy(dtype=np.float64)
    with np.errstate(divide="ignore", invalid="ignore"):
        df["spread_bps"] = ((ask - bid) / mid) * 1e4

    bid_depth = df["bid_depth"].to_numpy(dtype=np.float64)
    ask_depth = df["ask_depth"].to_numpy(dtype=np.float64)
    depth = bid_depth + ask_depth
    with np.errstate(divide="ignore", invalid="ignore"):
        df["imbalance"] = (bid_depth - ask_depth) / depth

    top_sz = df["bid_sz_1"].to_numpy(dtype=np.float64) + df["ask_sz_1"].to_numpy(dtype=np.float64)
    with np.errstate(divide="ignore", invalid="ignore"):
        df["top_ratio"] = top_sz / depth

    out = df[
        [
            "t_ns",
            "exchange",
            "feed",
            "obs_price",
            "mid",
            "latency_us",
            "spread_bps",
            "imbalance",
            "top_ratio",
            "size",
        ]
    ].copy()
    out["stream"] = out["exchange"].astype(str) + ":" + out["feed"].astype(str)
    out["is_target"] = (out["exchange"].astype(str) == TARGET_EXCHANGE) & (
        out["feed"].astype(str) == TARGET_FEED
    )
    out["is_trade"] = out["feed"].astype(str) == "trade"
    out["is_book"] = out["feed"].astype(str).isin(["orderbook", "bbo"])

    out = out[np.isfinite(out["obs_price"]) & (out["obs_price"] > 0)].copy()
    return out


def _kalman_filter(
    t_sec: np.ndarray,
    z_obs: np.ndarray,
    stream_idx: np.ndarray,
    params: KalmanParams,
    stream_names: List[str],
    *,
    exclude_obs_mask: np.ndarray | None = None,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    n = len(z_obs)
    x_pred = np.zeros(n, dtype=np.float64)
    p_pred = np.zeros(n, dtype=np.float64)
    x_filt = np.zeros(n, dtype=np.float64)
    p_filt = np.zeros(n, dtype=np.float64)
    phi_dt = np.ones(n, dtype=np.float64)

    x = 0.0
    p = 1.0
    last_t = t_sec[0]

    biases = params.bias_by_stream
    r_map = params.r_by_stream
    k = min(params.k_per_sec, 0.0)
    q_per_sec = max(params.q_per_sec, 0.0)

    for i in range(n):
        dt = float(t_sec[i] - last_t) if i > 0 else 0.0
        dt = max(dt, 0.0)
        phi = math.exp(k * dt) if dt > 0 else 1.0
        q = q_per_sec * (dt if dt > 0 else 0.0)

        # Time update.
        x = phi * x
        p = (phi * phi) * p + q

        x_pred[i] = x
        p_pred[i] = p
        phi_dt[i] = phi

        if exclude_obs_mask is not None and bool(exclude_obs_mask[i]):
            x_filt[i] = x
            p_filt[i] = p
            last_t = t_sec[i]
            continue

        s_name = stream_names[int(stream_idx[i])]
        b = float(biases.get(s_name, 0.0))
        r = float(r_map.get(s_name, 1e-4))
        r = max(r, 1e-12)

        y = float(z_obs[i] - b)
        s = p + r
        k_gain = p / s
        x = x + k_gain * (y - x)
        p = (1.0 - k_gain) * p

        x_filt[i] = x
        p_filt[i] = p
        last_t = t_sec[i]

    return x_pred, p_pred, x_filt, p_filt, phi_dt


def _rts_smoother(
    x_pred: np.ndarray,
    p_pred: np.ndarray,
    x_filt: np.ndarray,
    p_filt: np.ndarray,
    phi_dt: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray]:
    n = len(x_filt)
    x_smooth = np.zeros(n, dtype=np.float64)
    p_smooth = np.zeros(n, dtype=np.float64)
    x_smooth[-1] = x_filt[-1]
    p_smooth[-1] = p_filt[-1]

    for i in range(n - 2, -1, -1):
        denom = p_pred[i + 1]
        if denom <= 0:
            x_smooth[i] = x_filt[i]
            p_smooth[i] = p_filt[i]
            continue
        c = (p_filt[i] * phi_dt[i + 1]) / denom
        x_smooth[i] = x_filt[i] + c * (x_smooth[i + 1] - x_pred[i + 1])
        p_smooth[i] = p_filt[i] + (c * c) * (p_smooth[i + 1] - p_pred[i + 1])

    return x_smooth, p_smooth


def _estimate_k_from_smoothed(x: np.ndarray, dt: np.ndarray) -> float:
    x0 = x[:-1]
    x1 = x[1:]
    dt = np.asarray(dt, dtype=np.float64)
    mask = dt > 1e-6
    if mask.sum() < 100:
        return 0.0
    x0 = x0[mask]
    x1 = x1[mask]
    dt = dt[mask]

    def obj(k: float) -> float:
        phi = np.exp(k * dt)
        resid = x1 - phi * x0
        return float(np.mean(resid * resid))

    res = minimize_scalar(obj, bounds=(-5.0, 0.0), method="bounded")
    k_hat = float(res.x)
    if not np.isfinite(k_hat):
        return 0.0
    return min(k_hat, 0.0)


def _estimate_q_from_smoothed(x: np.ndarray, dt: np.ndarray, k: float) -> float:
    x0 = x[:-1]
    x1 = x[1:]
    dt = np.asarray(dt, dtype=np.float64)
    mask = dt > 1e-6
    if mask.sum() < 100:
        return 1e-6
    x0 = x0[mask]
    x1 = x1[mask]
    dt = dt[mask]
    phi = np.exp(k * dt)
    innov = x1 - phi * x0
    q = np.mean((innov * innov) / dt)
    if not np.isfinite(q) or q <= 0:
        return 1e-6
    return float(q)


def fit_kalman_params(
    obs: pd.DataFrame,
    *,
    train_frac: float = DEFAULT_TRAIN_FRAC,
    em_iters: int = 6,
    min_stream_obs: int = 200,
    gamma_clip: float = 0.05,
    q_floor_mult: float = 0.0,
    q_dt_floor_ms: float = 1.0,
) -> Tuple[KalmanParams, int]:
    obs = obs.sort_values("t_ns", kind="mergesort").reset_index(drop=True)
    # Time-based split (avoid skew if we added multiple virtual obs per base event time).
    unique_times = np.asarray(pd.unique(obs["t_ns"]), dtype=np.int64)
    unique_times.sort()
    split_ns = int(np.quantile(unique_times, train_frac))
    train = obs[obs["t_ns"] <= split_ns].copy()

    # Ensure the target stream exists; if not, we still fit but reference another stream.
    stream_counts = train["stream"].value_counts()
    keep_streams = stream_counts[stream_counts >= min_stream_obs].index.tolist()
    train = train[train["stream"].isin(keep_streams)].copy()

    stream_counts = train["stream"].value_counts()
    if len(stream_counts) == 0:
        raise ValueError("No streams left after filtering; try lowering --min-stream-obs.")

    ref_stream = f"{TARGET_EXCHANGE}:{TARGET_FEED}"
    if ref_stream not in stream_counts.index:
        ref_stream = str(stream_counts.index[0])

    stream_names = sorted(stream_counts.index.astype(str).tolist())
    stream_to_idx = {s: i for i, s in enumerate(stream_names)}
    stream_idx = train["stream"].map(stream_to_idx).to_numpy(dtype=np.int32)

    t_ns = train["t_ns"].to_numpy(dtype=np.int64)
    t_sec = (t_ns - t_ns[0]) / 1e9
    dt = np.diff(t_sec, prepend=t_sec[0])

    y_log = _safe_log_prices(train["obs_price"].to_numpy(dtype=np.float64))
    mu_log = float(np.nanmedian(y_log))
    z_obs = y_log - mu_log
    imb = train["imbalance"].to_numpy(dtype=np.float64)
    imb = np.where(np.isfinite(imb), imb, 0.0)

    global_r = float(np.nanvar(z_obs[np.isfinite(z_obs)], ddof=1))
    global_r = max(global_r, 1e-6)

    bias_by_stream = {s: 0.0 for s in stream_names}
    r_by_stream = {s: global_r for s in stream_names}
    gamma_imb = 0.0
    k_per_sec = 0.0  # start as random-walk-ish; EM will pull k negative if there is mean reversion
    q_per_sec = 1e-6

    exclude_none = np.zeros(len(z_obs), dtype=bool)

    for _ in range(em_iters):
        z_adj = z_obs + gamma_imb * imb
        params = KalmanParams(
            mu_log=mu_log,
            k_per_sec=k_per_sec,
            q_per_sec=q_per_sec,
            gamma_imb=gamma_imb,
            bias_by_stream=dict(bias_by_stream),
            r_by_stream=dict(r_by_stream),
            ref_stream=ref_stream,
            latency_median_us={},
            latency_mad_us={},
            spread_median_bps={},
            spread_mad_bps={},
            top_ratio_median={},
            top_ratio_mad={},
        )

        x_pred, p_pred, x_filt, p_filt, phi_dt = _kalman_filter(
            t_sec=t_sec,
            z_obs=z_adj,
            stream_idx=stream_idx,
            params=params,
            stream_names=stream_names,
            exclude_obs_mask=exclude_none,
        )
        x_smooth, _p_smooth = _rts_smoother(x_pred, p_pred, x_filt, p_filt, phi_dt)

        # Update biases (anchored by ref stream = 0).
        bias_updates = {}
        for s in stream_names:
            m = train["stream"].to_numpy() == s
            if m.sum() < 5:
                continue
            resid = z_adj[m] - x_smooth[m]
            bias_updates[s] = float(np.nanmean(resid))
        ref_b = float(bias_updates.get(ref_stream, 0.0))
        for s in stream_names:
            bias_by_stream[s] = float(bias_updates.get(s, bias_by_stream[s]) - ref_b)
        bias_by_stream[ref_stream] = 0.0

        # Update measurement variances per stream.
        for s in stream_names:
            m = train["stream"].to_numpy() == s
            if m.sum() < 20:
                r_by_stream[s] = global_r
                continue
            resid = z_adj[m] - x_smooth[m] - bias_by_stream[s]
            r = float(np.nanvar(resid, ddof=1))
            r_by_stream[s] = float(np.clip(r, 1e-10, 1e-1))

        # Update imbalance loading gamma using (book) residuals after bias.
        is_book = train["is_book"].to_numpy(dtype=bool)
        bias_arr = train["stream"].astype(str).map(bias_by_stream).to_numpy(dtype=np.float64)
        m = is_book & np.isfinite(train["imbalance"].to_numpy(dtype=np.float64)) & np.isfinite(bias_arr)
        if m.sum() > 200:
            resid_no_gamma = z_obs[m] - x_smooth[m] - bias_arr[m]
            imb_m = train["imbalance"].to_numpy(dtype=np.float64)[m]
            denom = float(np.mean(imb_m * imb_m))
            if denom > 1e-8:
                gamma_imb = float(-np.mean(resid_no_gamma * imb_m) / denom)
                gamma_imb = float(np.clip(gamma_imb, -gamma_clip, gamma_clip))

        # Update AR(1) + process noise from smoothed state.
        k_per_sec = _estimate_k_from_smoothed(x_smooth, dt[1:])
        q_per_sec = _estimate_q_from_smoothed(x_smooth, dt[1:], k_per_sec)

    # Floor q_per_sec using realized variance of Lighter orderbook log-mid (keeps filter responsive).
    if q_floor_mult and q_floor_mult > 0:
        ref_mask = (train["exchange"].astype(str) == TARGET_EXCHANGE) & (train["feed"].astype(str) == TARGET_FEED)
        ref = train.loc[ref_mask, ["t_ns", "obs_price"]].copy()
        if len(ref) >= 10:
            ref_t = ref["t_ns"].to_numpy(dtype=np.int64)
            ref_sec = (ref_t - ref_t[0]) / 1e9
            ref_log = _safe_log_prices(ref["obs_price"].to_numpy(dtype=np.float64)) - mu_log
            dlog = np.diff(ref_log)
            dsec = np.diff(ref_sec)
            dt_floor = max(float(q_dt_floor_ms) / 1000.0, 1e-6)
            dsec = np.where(dsec > dt_floor, dsec, dt_floor)
            q_i = (dlog * dlog) / dsec
            q_ref = float(np.nanmedian(q_i[np.isfinite(q_i)])) if np.isfinite(q_i).any() else 0.0
            if np.isfinite(q_ref) and q_ref > 0:
                q_per_sec = max(float(q_per_sec), float(q_floor_mult) * q_ref)

    # Stream-level stats used for latency/staleness downweighting (trained on in-sample only).
    latency_median_us: Dict[str, float] = {}
    latency_mad_us: Dict[str, float] = {}
    spread_median_bps: Dict[str, float] = {}
    spread_mad_bps: Dict[str, float] = {}
    top_ratio_median: Dict[str, float] = {}
    top_ratio_mad: Dict[str, float] = {}
    for s in stream_names:
        m = (train["stream"].astype(str) == s).to_numpy()
        med, mad = _median_mad(train.loc[m, "latency_us"].to_numpy(dtype=np.float64))
        latency_median_us[s] = med
        latency_mad_us[s] = mad
        m_book = m & train["is_book"].to_numpy(dtype=bool)
        med, mad = _median_mad(train.loc[m_book, "spread_bps"].to_numpy(dtype=np.float64))
        spread_median_bps[s] = med
        spread_mad_bps[s] = mad
        med, mad = _median_mad(train.loc[m_book, "top_ratio"].to_numpy(dtype=np.float64))
        top_ratio_median[s] = med
        top_ratio_mad[s] = mad

    fitted = KalmanParams(
        mu_log=mu_log,
        k_per_sec=k_per_sec,
        q_per_sec=q_per_sec,
        gamma_imb=gamma_imb,
        bias_by_stream=bias_by_stream,
        r_by_stream=r_by_stream,
        ref_stream=ref_stream,
        latency_median_us=latency_median_us,
        latency_mad_us=latency_mad_us,
        spread_median_bps=spread_median_bps,
        spread_mad_bps=spread_mad_bps,
        top_ratio_median=top_ratio_median,
        top_ratio_mad=top_ratio_mad,
    )
    return fitted, split_ns


def run_predictions(obs: pd.DataFrame, params: KalmanParams, *, tuning: FilterTuning) -> pd.DataFrame:
    # Ensure that (within a timestamp) we process non-Lighter events first, then Lighter orderbook.
    obs = obs.copy()
    obs["_is_target_ob"] = (obs["exchange"].astype(str) == TARGET_EXCHANGE) & (
        obs["feed"].astype(str) == TARGET_FEED
    )
    obs.sort_values(["t_ns", "_is_target_ob"], inplace=True, kind="mergesort")
    obs.reset_index(drop=True, inplace=True)

    # Build stream index for all streams seen in obs; unseen streams get default r and bias=0.
    t_ns = obs["t_ns"].to_numpy(dtype=np.int64)
    t_sec = (t_ns - t_ns[0]) / 1e9

    y_log = _safe_log_prices(obs["obs_price"].to_numpy(dtype=np.float64))
    z_obs = y_log - params.mu_log

    streams = obs["stream"].astype(str).to_numpy()
    is_target_obs = obs["_is_target_ob"].to_numpy(dtype=bool)
    is_trade = obs["is_trade"].to_numpy(dtype=bool)
    is_book = obs["is_book"].to_numpy(dtype=bool)

    biases = dict(params.bias_by_stream)
    r_map = params.r_by_stream
    # If we didn't learn a stream's measurement noise, treat it as much noisier than typical.
    med_r = float(np.median(list(r_map.values()))) if r_map else 1e-4
    default_r = max(10.0 * med_r, 1e-10)

    k = min(params.k_per_sec, 0.0)
    q_per_sec = max(params.q_per_sec, 0.0)
    gamma_imb = float(params.gamma_imb)
    horizon_s = max(float(tuning.horizon_ms) / 1000.0, 0.0)

    # Online, single-pass filter: at target instants record pre-update prediction, then update state.
    n = len(obs)
    x_post = np.zeros(n, dtype=np.float64)
    p_post = np.zeros(n, dtype=np.float64)
    pred_mid_post = np.zeros(n, dtype=np.float64)
    pred_mid_post_lo = np.zeros(n, dtype=np.float64)
    pred_mid_post_hi = np.zeros(n, dtype=np.float64)
    pred_mid_post_h = np.zeros(n, dtype=np.float64)

    pred_mid_pre = np.full(n, np.nan, dtype=np.float64)
    pred_mid_pre_lo = np.full(n, np.nan, dtype=np.float64)
    pred_mid_pre_hi = np.full(n, np.nan, dtype=np.float64)
    pred_mid_pre_h = np.full(n, np.nan, dtype=np.float64)

    r_eff_used = np.zeros(n, dtype=np.float64)
    nu_used = np.zeros(n, dtype=np.float64)

    latency_us = obs["latency_us"].to_numpy(dtype=np.float64)
    spread_bps = obs["spread_bps"].to_numpy(dtype=np.float64)
    top_ratio = obs["top_ratio"].to_numpy(dtype=np.float64)
    imb = obs["imbalance"].to_numpy(dtype=np.float64)
    imb = np.where(np.isfinite(imb), imb, 0.0)
    trade_size = obs["size"].to_numpy(dtype=np.float64)

    x = 0.0
    p = 1.0
    last_t = float(t_sec[0])
    last_z = float(z_obs[0]) if np.isfinite(z_obs[0]) else 0.0
    vol2 = 1e-8
    for i in range(n):
        dt = float(t_sec[i] - last_t) if i > 0 else 0.0
        dt = max(dt, 0.0)
        phi = math.exp(k * dt) if dt > 0 else 1.0
        q = q_per_sec * (dt if dt > 0 else 0.0)

        # Time update.
        x = phi * x
        p = (phi * phi) * p + q

        # Update a simple EWMA volatility proxy from observation log-diffs (used for variance scaling).
        if i > 0 and np.isfinite(z_obs[i]):
            dz = float(z_obs[i] - last_z)
            if tuning.vol_halflife_s > 0 and dt > 0:
                lam = 1.0 - math.exp(-dt * math.log(2.0) / tuning.vol_halflife_s)
            else:
                lam = 0.0
            vol2 = (1.0 - lam) * vol2 + lam * (dz * dz)
            last_z = float(z_obs[i])

        # Prediction recorded right before consuming the contemporaneous Lighter mid update.
        if bool(is_target_obs[i]):
            pred_log_pre = params.mu_log + x
            pred_mid_pre[i] = math.exp(pred_log_pre)
            s = math.sqrt(max(p, 0.0))
            pred_mid_pre_lo[i] = math.exp(pred_log_pre - 2.0 * s)
            pred_mid_pre_hi[i] = math.exp(pred_log_pre + 2.0 * s)
            if horizon_s > 0:
                phi_h = math.exp(k * horizon_s)
                x_h = phi_h * x
                p_h = (phi_h * phi_h) * p + q_per_sec * horizon_s
                pred_mid_pre_h[i] = math.exp(params.mu_log + x_h)

        # Measurement update (always).
        s_name = streams[i]
        b = float(biases.get(s_name, 0.0))
        r_base = float(r_map.get(s_name, default_r))
        r_base = max(r_base, 1e-12)

        # Apply imbalance term only for book updates.
        z_i = float(z_obs[i])
        if not np.isfinite(z_i):
            z_i = last_z
        imb_term = gamma_imb * float(imb[i]) if bool(is_book[i]) else 0.0
        y_raw = z_i + imb_term

        # Optional adaptive bias (EWMA) for non-reference streams.
        if tuning.bias_ewma_halflife_s > 0 and s_name != params.ref_stream:
            lam_b = 1.0 - math.exp(-max(dt, 0.0) * math.log(2.0) / tuning.bias_ewma_halflife_s) if dt > 0 else 0.0
            b = b + lam_b * ((y_raw - x) - b)
            biases[s_name] = b

        # Base variance, then inflate based on trade/book characteristics and staleness/latency/vol.
        r_eff = r_base
        is_ref = s_name == params.ref_stream
        if is_ref:
            r_eff = max(r_eff * max(tuning.lighter_r_mult, 1e-6), 1e-12)

        if bool(is_trade[i]):
            r_eff *= max(tuning.trade_r_mult, 1.0)
            sz = float(trade_size[i]) if np.isfinite(trade_size[i]) else 0.0
            if sz > 0 and tuning.trade_size_beta > 0:
                r_eff /= (1.0 + tuning.trade_size_beta * sz)

        z_lat = 0.0
        if (not is_ref) and tuning.latency_alpha > 0 and np.isfinite(latency_us[i]):
            med = float(params.latency_median_us.get(s_name, 0.0))
            mad = float(params.latency_mad_us.get(s_name, 1.0))
            z_lat = max(0.0, (float(latency_us[i]) - med) / mad)
            z_lat = min(z_lat, 10.0)
            r_eff *= 1.0 + tuning.latency_alpha * z_lat

        stale_score = 0.0
        if (not is_ref) and tuning.stale_alpha > 0 and bool(is_book[i]):
            if np.isfinite(spread_bps[i]):
                med = float(params.spread_median_bps.get(s_name, 0.0))
                mad = float(params.spread_mad_bps.get(s_name, 1.0))
                z_sp = max(0.0, (float(spread_bps[i]) - med) / mad)
                z_sp = min(z_sp, 10.0)
            else:
                z_sp = 0.0
            if np.isfinite(top_ratio[i]):
                med = float(params.top_ratio_median.get(s_name, 0.0))
                mad = float(params.top_ratio_mad.get(s_name, 1.0))
                z_top = max(0.0, (med - float(top_ratio[i])) / mad)
                z_top = min(z_top, 10.0)
            else:
                z_top = 0.0
            stale_score = z_sp + z_top
            r_eff *= 1.0 + tuning.stale_alpha * stale_score

        if (not is_ref) and tuning.vol_alpha > 0:
            vol_scale = 1.0 + tuning.vol_alpha * math.sqrt(max(vol2, 0.0))
            vol_scale = min(vol_scale, 10.0)
            r_eff *= vol_scale * vol_scale

        y = y_raw - b
        s_var = p + r_eff
        std = math.sqrt(max(s_var, 1e-12))
        nu = (y - x) / std

        suspicious = (not is_ref) and (bool(is_trade[i]) or (z_lat >= 2.0) or (stale_score >= 2.0))
        if suspicious and tuning.robust_z > 0 and abs(nu) > tuning.robust_z:
            # If a stream looks latent/stale (or it's a trade), treat huge innovations as outliers.
            r_eff *= (abs(nu) / tuning.robust_z) ** 2
            s_var = p + r_eff
            std = math.sqrt(max(s_var, 1e-12))
            nu = (y - x) / std
        elif (not suspicious) and tuning.jump_z > 0 and abs(nu) > tuning.jump_z and tuning.jump_beta > 0:
            # If a trusted stream reports a big move, temporarily increase state uncertainty so the
            # filter can jump (avoid lagging real price jumps).
            jump = float(tuning.jump_beta) * (y - x) * (y - x)
            p = p + jump
            s_var = p + r_eff
            std = math.sqrt(max(s_var, 1e-12))
            nu = (y - x) / std

        k_gain = p / (p + r_eff)
        x = x + k_gain * (y - x)
        p = (1.0 - k_gain) * p

        r_eff_used[i] = r_eff
        nu_used[i] = nu

        x_post[i] = x
        p_post[i] = p
        pred_log_post = params.mu_log + x
        pred_mid_post[i] = math.exp(pred_log_post)
        s2 = math.sqrt(max(p, 0.0))
        pred_mid_post_lo[i] = math.exp(pred_log_post - 2.0 * s2)
        pred_mid_post_hi[i] = math.exp(pred_log_post + 2.0 * s2)
        if horizon_s > 0:
            phi_h = math.exp(k * horizon_s)
            x_h = phi_h * x
            p_h = (phi_h * phi_h) * p + q_per_sec * horizon_s
            pred_mid_post_h[i] = math.exp(params.mu_log + x_h)
        else:
            pred_mid_post_h[i] = pred_mid_post[i]
        last_t = float(t_sec[i])

    # Actual Lighter mid (only meaningful on Lighter orderbook events), forward-filled for plotting.
    lighter_mid = np.where(
        (obs["exchange"].astype(str) == TARGET_EXCHANGE).to_numpy()
        & (obs["feed"].astype(str) == TARGET_FEED).to_numpy(),
        obs["mid"].to_numpy(dtype=np.float64),
        np.nan,
    )
    lighter_mid_ffill = pd.Series(lighter_mid).ffill().to_numpy(dtype=np.float64)

    out = obs[["t_ns", "exchange", "feed", "stream", "is_target", "obs_price", "mid", "is_trade", "is_book"]].copy()
    out["pred_mid"] = pred_mid_post
    out["pred_mid_lo_2s"] = pred_mid_post_lo
    out["pred_mid_hi_2s"] = pred_mid_post_hi
    out["pred_mid_h"] = pred_mid_post_h
    out["pred_mid_pre_target"] = pred_mid_pre
    out["pred_mid_pre_target_lo_2s"] = pred_mid_pre_lo
    out["pred_mid_pre_target_hi_2s"] = pred_mid_pre_hi
    out["pred_mid_pre_target_h"] = pred_mid_pre_h
    out["x_filt_online"] = x_post
    out["p_filt_online"] = p_post
    out["r_eff"] = r_eff_used
    out["nu"] = nu_used
    out["lighter_mid"] = lighter_mid
    out["lighter_mid_ffill"] = lighter_mid_ffill
    out["t_dt"] = _ns_to_datetime(out["t_ns"].to_numpy(dtype=np.int64))
    out.drop(columns=["_is_target_ob"], inplace=True, errors="ignore")
    return out


def _rmse(a: np.ndarray, b: np.ndarray) -> float:
    m = np.isfinite(a) & np.isfinite(b)
    if m.sum() == 0:
        return float("nan")
    return float(np.sqrt(np.mean((a[m] - b[m]) ** 2)))

def _mae(a: np.ndarray, b: np.ndarray) -> float:
    m = np.isfinite(a) & np.isfinite(b)
    if m.sum() == 0:
        return float("nan")
    return float(np.mean(np.abs(a[m] - b[m])))


def _pctl_abs_err(a: np.ndarray, b: np.ndarray, p: float) -> float:
    m = np.isfinite(a) & np.isfinite(b)
    if m.sum() == 0:
        return float("nan")
    return float(np.percentile(np.abs(a[m] - b[m]), p))


def _directional_accuracy(pred_level: np.ndarray, actual_level: np.ndarray, ref_level: np.ndarray) -> float:
    pred_move = pred_level - ref_level
    actual_move = actual_level - ref_level
    m = np.isfinite(pred_move) & np.isfinite(actual_move) & (actual_move != 0)
    if m.sum() == 0:
        return float("nan")
    return float(np.mean(np.sign(pred_move[m]) == np.sign(actual_move[m])))


def _future_value_at_or_after(t_ns: np.ndarray, values: np.ndarray, query_ns: np.ndarray) -> np.ndarray:
    t_ns = np.asarray(t_ns, dtype=np.int64)
    values = np.asarray(values, dtype=np.float64)
    query_ns = np.asarray(query_ns, dtype=np.int64)
    idx = np.searchsorted(t_ns, query_ns, side="left")
    out = np.full(len(query_ns), np.nan, dtype=np.float64)
    ok = idx < len(t_ns)
    out[ok] = values[idx[ok]]
    return out


def compute_naive_baselines(preds: pd.DataFrame) -> pd.DataFrame:
    preds = preds.copy()
    is_target_ob = (preds["exchange"].astype(str) == TARGET_EXCHANGE) & (preds["feed"].astype(str) == TARGET_FEED)
    is_non_lighter_book = (preds["exchange"].astype(str) != TARGET_EXCHANGE) & preds["is_book"].to_numpy(dtype=bool)
    obs_price = preds["obs_price"].to_numpy(dtype=np.float64)
    lighter_mid = preds["lighter_mid"].to_numpy(dtype=np.float64)

    naive_prev_lighter = np.full(len(preds), np.nan, dtype=np.float64)
    naive_last_ref = np.full(len(preds), np.nan, dtype=np.float64)

    last_lighter = np.nan
    last_ref = np.nan
    for i in range(len(preds)):
        if bool(is_target_ob.iloc[i]):
            naive_prev_lighter[i] = last_lighter
            naive_last_ref[i] = last_ref
            if np.isfinite(lighter_mid[i]):
                last_lighter = float(lighter_mid[i])
            continue
        if bool(is_non_lighter_book.iloc[i]) and np.isfinite(obs_price[i]):
            last_ref = float(obs_price[i])

    preds["naive_prev_lighter"] = naive_prev_lighter
    preds["naive_last_ref_book"] = naive_last_ref
    return preds


def print_eval_comparison(
    preds: pd.DataFrame,
    *,
    split_ns: int,
    horizon_ms: float = 0.0,
    eval_min_gap_ms: float = 0.0,
    eval_move_bps: float = 0.0,
) -> None:
    eval_rows = preds[preds["is_target"] & preds["lighter_mid"].notna()].copy()
    if len(eval_rows) < 5:
        print("Not enough target observations for evaluation.")
        return
    eval_rows["is_test"] = eval_rows["t_ns"] > split_ns
    t_target = eval_rows["t_ns"].to_numpy(dtype=np.int64)
    eval_rows["gap_ms"] = np.r_[np.nan, np.diff(t_target) / 1e6]

    if eval_min_gap_ms and eval_min_gap_ms > 0:
        before = len(eval_rows)
        eval_rows = eval_rows[eval_rows["gap_ms"] >= eval_min_gap_ms].copy()
        after = len(eval_rows)
        print(f"Applying eval_min_gap_ms={eval_min_gap_ms:g}: kept {after}/{before} target updates")
        if after < 5:
            print("Not enough target observations after gap filter for evaluation.")
            return

    def summarize(name: str, pred_col: str) -> Tuple[float, float, float, float, float, float]:
        in_mask = ~eval_rows["is_test"].to_numpy(dtype=bool)
        out_mask = eval_rows["is_test"].to_numpy(dtype=bool)
        pred = eval_rows[pred_col].to_numpy(dtype=np.float64)
        actual = eval_rows["lighter_mid"].to_numpy(dtype=np.float64)
        ref = np.r_[np.nan, actual[:-1]]
        in_rmse = _rmse(pred[in_mask], actual[in_mask])
        out_rmse = _rmse(pred[out_mask], actual[out_mask])
        in_mae = _mae(pred[in_mask], actual[in_mask])
        out_mae = _mae(pred[out_mask], actual[out_mask])
        in_p95 = _pctl_abs_err(pred[in_mask], actual[in_mask], 95.0)
        out_p95 = _pctl_abs_err(pred[out_mask], actual[out_mask], 95.0)
        in_dir = _directional_accuracy(pred[in_mask], actual[in_mask], ref[in_mask])
        out_dir = _directional_accuracy(pred[out_mask], actual[out_mask], ref[out_mask])
        print(
            f"{name}: "
            f"in RMSE={in_rmse:.2f} MAE={in_mae:.2f} p95={in_p95:.2f} dir={in_dir:.3f} | "
            f"out RMSE={out_rmse:.2f} MAE={out_mae:.2f} p95={out_p95:.2f} dir={out_dir:.3f}"
        )
        return in_rmse, out_rmse, in_mae, out_mae, in_p95, out_p95

    print("Evaluation @ lighter:orderbook updates (predict current mid from prior info)")
    model_in_rmse, model_out_rmse, *_ = summarize("Kalman", "pred_mid_pre_target")
    b1_in_rmse, b1_out_rmse, *_ = summarize("Naive(prev lighter mid)", "naive_prev_lighter")
    b2_in_rmse, b2_out_rmse, *_ = summarize("Naive(last non-lighter book mid)", "naive_last_ref_book")
    print(
        "RMSE improvement vs naive(prev lighter): "
        f"in {(b1_in_rmse - model_in_rmse):.2f}, out {(b1_out_rmse - model_out_rmse):.2f}"
    )
    print(
        "RMSE improvement vs naive(last non-lighter): "
        f"in {(b2_in_rmse - model_in_rmse):.2f}, out {(b2_out_rmse - model_out_rmse):.2f}"
    )

    if eval_move_bps and eval_move_bps > 0:
        actual = eval_rows["lighter_mid"].to_numpy(dtype=np.float64)
        prev_actual = np.r_[np.nan, actual[:-1]]
        with np.errstate(divide="ignore", invalid="ignore"):
            move_bps = np.abs((actual - prev_actual) / prev_actual) * 1e4
        big = np.isfinite(move_bps) & (move_bps >= eval_move_bps)
        kept = int(big.sum())
        print(f"Large-move subset: |Δ mid| >= {eval_move_bps:g} bps -> {kept}/{len(eval_rows)} points")
        if kept >= 10:
            tmp = eval_rows.loc[big].copy()
            tmp["is_test"] = tmp["t_ns"] > split_ns
            # Reuse summarize logic on the subset.
            def summarize_subset(name: str, pred_col: str) -> Tuple[float, float]:
                in_mask = ~tmp["is_test"].to_numpy(dtype=bool)
                out_mask = tmp["is_test"].to_numpy(dtype=bool)
                pred = tmp[pred_col].to_numpy(dtype=np.float64)
                actual2 = tmp["lighter_mid"].to_numpy(dtype=np.float64)
                return _rmse(pred[in_mask], actual2[in_mask]), _rmse(pred[out_mask], actual2[out_mask])

            km_in, km_out = summarize_subset("Kalman", "pred_mid_pre_target")
            n1_in, n1_out = summarize_subset("Naive(prev lighter)", "naive_prev_lighter")
            n2_in, n2_out = summarize_subset("Naive(last non-lighter)", "naive_last_ref_book")
            print(
                f"Large-move RMSE: Kalman in={km_in:.2f} out={km_out:.2f} | "
                f"naive(prev) in={n1_in:.2f} out={n1_out:.2f} | "
                f"naive(ref) in={n2_in:.2f} out={n2_out:.2f}"
            )

    if horizon_ms and horizon_ms > 0 and "pred_mid_pre_target_h" in eval_rows.columns:
        t_target = eval_rows["t_ns"].to_numpy(dtype=np.int64)
        mid_target = eval_rows["lighter_mid"].to_numpy(dtype=np.float64)
        future_mid = _future_value_at_or_after(t_target, mid_target, t_target + int(horizon_ms * 1e6))

        def summarize_h(name: str, pred_col: str) -> Tuple[float, float]:
            in_mask = ~eval_rows["is_test"].to_numpy(dtype=bool)
            out_mask = eval_rows["is_test"].to_numpy(dtype=bool)
            pred = eval_rows[pred_col].to_numpy(dtype=np.float64)
            in_rmse = _rmse(pred[in_mask], future_mid[in_mask])
            out_rmse = _rmse(pred[out_mask], future_mid[out_mask])
            print(f"{name} (horizon {horizon_ms:g}ms): in RMSE={in_rmse:.2f} | out RMSE={out_rmse:.2f}")
            return in_rmse, out_rmse

        print(f"Evaluation @ lighter:orderbook updates (predict mid at +{horizon_ms:g}ms, best-effort)")
        model_h_in, model_h_out = summarize_h("Kalman", "pred_mid_pre_target_h")
        b1_h_in, b1_h_out = summarize_h("Naive(prev lighter mid)", "naive_prev_lighter")
        b2_h_in, b2_h_out = summarize_h("Naive(last non-lighter book mid)", "naive_last_ref_book")
        print(
            "Horizon RMSE improvement vs naive(prev lighter): "
            f"in {(b1_h_in - model_h_in):.2f}, out {(b1_h_out - model_h_out):.2f}"
        )
        print(
            "Horizon RMSE improvement vs naive(last non-lighter): "
            f"in {(b2_h_in - model_h_in):.2f}, out {(b2_h_out - model_h_out):.2f}"
        )

    # Streaming horizon eval: from any time t, predict Lighter mid at t+h using only info <= t.
    if horizon_ms and horizon_ms > 0 and "pred_mid_h" in preds.columns:
        horizon_ns = int(horizon_ms * 1e6)
        lighter_updates = preds[preds["lighter_mid"].notna()].copy()
        if len(lighter_updates) >= 10:
            t_l = lighter_updates["t_ns"].to_numpy(dtype=np.int64)
            m_l = lighter_updates["lighter_mid"].to_numpy(dtype=np.float64)
            q_ns = preds["t_ns"].to_numpy(dtype=np.int64) + horizon_ns
            future_mid = _future_value_at_or_after(t_l, m_l, q_ns)
            model = preds["pred_mid_h"].to_numpy(dtype=np.float64)
            naive = preds["lighter_mid_ffill"].to_numpy(dtype=np.float64)

            ok = np.isfinite(future_mid) & np.isfinite(model) & np.isfinite(naive)
            # Downsample a bit to keep this evaluation cheap and avoid overweighting bursts.
            idx = np.arange(len(preds))[ok]
            if len(idx) > 200_000:
                idx = idx[:: int(math.ceil(len(idx) / 200_000))]

            is_test = q_ns[idx] > split_ns
            in_mask = ~is_test
            out_mask = is_test

            def sse(a: np.ndarray, b: np.ndarray) -> float:
                return float(np.sqrt(np.mean((a - b) ** 2))) if len(a) else float("nan")

            m_in = sse(model[idx][in_mask], future_mid[idx][in_mask])
            m_out = sse(model[idx][out_mask], future_mid[idx][out_mask])
            n_in = sse(naive[idx][in_mask], future_mid[idx][in_mask])
            n_out = sse(naive[idx][out_mask], future_mid[idx][out_mask])
            print(f"Streaming horizon (+{horizon_ms:g}ms) RMSE: Kalman in={m_in:.2f} out={m_out:.2f} | naive(ffill) in={n_in:.2f} out={n_out:.2f}")
            print(f"Streaming horizon RMSE improvement vs naive(ffill): in {(n_in - m_in):.2f}, out {(n_out - m_out):.2f}")

            if eval_move_bps and eval_move_bps > 0:
                with np.errstate(divide="ignore", invalid="ignore"):
                    move_bps = np.abs((future_mid[idx] - naive[idx]) / naive[idx]) * 1e4
                big = np.isfinite(move_bps) & (move_bps >= eval_move_bps)
                if int(big.sum()) >= 50:
                    mb_in = sse(model[idx][big & in_mask], future_mid[idx][big & in_mask])
                    mb_out = sse(model[idx][big & out_mask], future_mid[idx][big & out_mask])
                    nb_in = sse(naive[idx][big & in_mask], future_mid[idx][big & in_mask])
                    nb_out = sse(naive[idx][big & out_mask], future_mid[idx][big & out_mask])
                    print(f"Streaming big-move subset (|Δ|>={eval_move_bps:g}bps): Kalman in={mb_in:.2f} out={mb_out:.2f} | naive in={nb_in:.2f} out={nb_out:.2f}")


def plot_results(
    preds: pd.DataFrame,
    *,
    split_ns: int,
    out_path: str | Path,
    title: str,
    horizon_ms: float = 0.0,
    show: bool = True,
    max_points: int = 80_000,
) -> None:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # For plotting, downsample to keep render times reasonable.
    if len(preds) > max_points:
        step = int(math.ceil(len(preds) / max_points))
        p = preds.iloc[::step].copy()
    else:
        p = preds.copy()

    fig, (ax0, ax1) = plt.subplots(2, 1, figsize=(14, 8), sharex=True, gridspec_kw={"height_ratios": [3, 1]})

    if "pred_mid_h" in p.columns and np.isfinite(p["pred_mid_h"]).any() and not np.allclose(p["pred_mid_h"], p["pred_mid"]):
        ax0.step(p["t_dt"], p["pred_mid"], where="post", label="Filtered (now)", linewidth=1.0, alpha=0.7)
        ax0.step(p["t_dt"], p["pred_mid_h"], where="post", label="Filtered → horizon", linewidth=1.3)
    else:
        ax0.step(p["t_dt"], p["pred_mid"], where="post", label="Kalman predicted Lighter mid (filtered)", linewidth=1.2)
    ax0.fill_between(
        p["t_dt"],
        p["pred_mid_lo_2s"],
        p["pred_mid_hi_2s"],
        color="C0",
        alpha=0.12,
        linewidth=0,
        label="±2σ (log-space)",
    )
    ax0.step(p["t_dt"], p["lighter_mid_ffill"], where="post", label="Lighter mid (ffill)", linewidth=1.0, alpha=0.8)

    split_dt = pd.to_datetime(split_ns, unit="ns", utc=True)
    ax0.axvline(split_dt, color="k", linestyle="--", linewidth=1.0, alpha=0.7, label="train/test split")
    ax0.set_title(title)
    ax0.set_ylabel("Price")
    ax0.grid(True, alpha=0.25)
    ax0.legend(loc="best")

    # Error evaluated only at the instants when Lighter orderbook updates arrive (no-leak prediction).
    eval_rows = preds[preds["is_target"] & preds["lighter_mid"].notna()].copy()
    eval_rows["is_test"] = eval_rows["t_ns"] > split_ns
    horizon_pred = eval_rows["pred_mid_pre_target_h"].to_numpy(dtype=np.float64)
    has_horizon = (horizon_ms and horizon_ms > 0) and np.isfinite(horizon_pred).any()
    if has_horizon:
        t_target = eval_rows["t_ns"].to_numpy(dtype=np.int64)
        mid_target = eval_rows["lighter_mid"].to_numpy(dtype=np.float64)
        horizon_ns = t_target + int(horizon_ms * 1e6)
        future_mid = _future_value_at_or_after(t_target, mid_target, horizon_ns)
        pred = horizon_pred
        actual = future_mid
        ref = mid_target
        label_suffix = " (horizon)"
    else:
        pred = eval_rows["pred_mid_pre_target"].to_numpy(dtype=np.float64)
        actual = eval_rows["lighter_mid"].to_numpy(dtype=np.float64)
        ref = np.r_[np.nan, eval_rows["lighter_mid"].to_numpy(dtype=np.float64)[:-1]]
        label_suffix = ""

    in_mask = ~eval_rows["is_test"].to_numpy(dtype=bool)
    out_mask = eval_rows["is_test"].to_numpy(dtype=bool)
    in_rmse = _rmse(pred[in_mask], actual[in_mask])
    out_rmse = _rmse(pred[out_mask], actual[out_mask])
    in_mae = _mae(pred[in_mask], actual[in_mask])
    out_mae = _mae(pred[out_mask], actual[out_mask])
    in_p95 = _pctl_abs_err(pred[in_mask], actual[in_mask], 95.0)
    out_p95 = _pctl_abs_err(pred[out_mask], actual[out_mask], 95.0)
    in_dir = _directional_accuracy(pred[in_mask], actual[in_mask], ref[in_mask])
    out_dir = _directional_accuracy(pred[out_mask], actual[out_mask], ref[out_mask])

    ax1.step(eval_rows["t_dt"], pred - actual, where="post", linewidth=0.8)
    ax1.axhline(0.0, color="k", linewidth=1.0, alpha=0.5)
    ax1.axvline(split_dt, color="k", linestyle="--", linewidth=1.0, alpha=0.7)
    ax1.set_ylabel("Pred - Actual")
    ax1.grid(True, alpha=0.25)
    ax1.set_title(
        f"Errors @ lighter orderbook updates{label_suffix}: "
        f"in RMSE={in_rmse:.2f} MAE={in_mae:.2f} p95={in_p95:.2f} dir={in_dir:.3f} | "
        f"out RMSE={out_rmse:.2f} MAE={out_mae:.2f} p95={out_p95:.2f} dir={out_dir:.3f}"
    )

    fig.tight_layout()
    fig.savefig(out_path, dpi=180)
    if show:
        try:
            plt.show()
        except Exception as e:  # pragma: no cover
            print(f"Plot display failed (backend={matplotlib.get_backend()}): {e}")
    plt.close(fig)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", type=str, default="logs/lighter_activity.csv")
    ap.add_argument("--out", type=str, default="plots/lighter_mid_kalman.png")
    ap.add_argument("--time-basis", type=str, default="wire", help="Event-time basis for filtering: wire (ts_ns) or engine (source_engine_ts_ns).")
    ap.add_argument("--train-frac", type=float, default=DEFAULT_TRAIN_FRAC)
    ap.add_argument("--max-rows", type=int, default=None)
    ap.add_argument("--em-iters", type=int, default=6)
    ap.add_argument("--min-stream-obs", type=int, default=200)
    ap.add_argument("--trade-dir-bps", type=float, default=0.5, help="Signed log-offset (in bps) applied to trades by direction.")
    ap.add_argument("--trade-r-mult", type=float, default=5.0, help="Multiplier on measurement variance for trades.")
    ap.add_argument("--trade-size-beta", type=float, default=0.0, help="Downweight trades by size: r /= (1 + beta*size).")
    ap.add_argument("--book-w-mid", type=float, default=0.6)
    ap.add_argument("--book-w-micro", type=float, default=0.25)
    ap.add_argument("--book-w-vwap5", type=float, default=0.15)
    ap.add_argument("--lighter-r-mult", type=float, default=0.05, help="Scale measurement variance for lighter:orderbook (smaller => tracks Lighter tighter).")
    ap.add_argument("--latency-alpha", type=float, default=0.5, help="Inflate r with (latency - median)/MAD.")
    ap.add_argument("--stale-alpha", type=float, default=0.5, help="Inflate r with stale score from spread/top_ratio.")
    ap.add_argument("--vol-alpha", type=float, default=0.0, help="Inflate r with EWMA volatility proxy.")
    ap.add_argument("--vol-halflife-s", type=float, default=1.0)
    ap.add_argument("--robust-z", type=float, default=6.0, help="Outlier gating threshold for suspicious obs (0 disables).")
    ap.add_argument("--jump-z", type=float, default=4.0, help="If |innovation|>z on trusted obs, increase state uncertainty to jump.")
    ap.add_argument("--jump-beta", type=float, default=0.3, help="Jump strength: p += beta*(innovation^2) when jump triggers.")
    ap.add_argument("--bias-ewma-halflife-s", type=float, default=2.0, help="Adaptive per-stream bias half-life in seconds (0 disables).")
    ap.add_argument("--horizon-ms", type=float, default=0.0, help="Predict forward by this many ms (quote-latency horizon).")
    ap.add_argument("--q-floor-mult", type=float, default=20.0, help="Floor q_per_sec using Lighter realized variance * this.")
    ap.add_argument("--q-dt-floor-ms", type=float, default=1.0, help="dt floor for q floor estimation.")
    ap.add_argument("--eval-min-gap-ms", type=float, default=0.0, help="Evaluate only on target updates with >= this gap from the previous target update.")
    ap.add_argument("--eval-move-bps", type=float, default=1.0, help="Also evaluate on points with |Δ mid| >= this (bps).")
    ap.add_argument("--show", action=argparse.BooleanOptionalAction, default=True, help="Display plot interactively.")
    ap.add_argument("--no-virtual-ob", action="store_true", help="Alias: use mid only for book observations.")
    args = ap.parse_args()

    if args.no_virtual_ob:
        args.book_w_mid, args.book_w_micro, args.book_w_vwap5 = 1.0, 0.0, 0.0

    tuning = FilterTuning(
        trade_dir_bps=float(args.trade_dir_bps),
        trade_r_mult=float(args.trade_r_mult),
        trade_size_beta=float(args.trade_size_beta),
        book_w_mid=float(args.book_w_mid),
        book_w_micro=float(args.book_w_micro),
        book_w_vwap5=float(args.book_w_vwap5),
        lighter_r_mult=float(args.lighter_r_mult),
        latency_alpha=float(args.latency_alpha),
        stale_alpha=float(args.stale_alpha),
        vol_alpha=float(args.vol_alpha),
        vol_halflife_s=float(args.vol_halflife_s),
        robust_z=float(args.robust_z),
        jump_z=float(args.jump_z),
        jump_beta=float(args.jump_beta),
        bias_ewma_halflife_s=float(args.bias_ewma_halflife_s),
        horizon_ms=float(args.horizon_ms),
        q_floor_mult=float(args.q_floor_mult),
        q_dt_floor_ms=float(args.q_dt_floor_ms),
        eval_move_bps=float(args.eval_move_bps),
    )

    obs = load_observations(args.csv, max_rows=args.max_rows, tuning=tuning, time_basis=str(args.time_basis))
    params, split_ns = fit_kalman_params(
        obs,
        train_frac=args.train_frac,
        em_iters=args.em_iters,
        min_stream_obs=args.min_stream_obs,
        q_floor_mult=tuning.q_floor_mult,
        q_dt_floor_ms=tuning.q_dt_floor_ms,
    )
    preds = run_predictions(obs, params, tuning=tuning)
    preds = compute_naive_baselines(preds)

    # Human-readable summary.
    half_life = float("inf")
    if params.k_per_sec < 0:
        half_life = math.log(2.0) / (-params.k_per_sec)
    print(
        "Fitted params: "
        f"ref_stream={params.ref_stream}  "
        f"k_per_sec={params.k_per_sec:.6g}  half_life_s={half_life:.3g}  "
        f"q_per_sec={params.q_per_sec:.6g}  gamma_imb={params.gamma_imb:.6g}"
    )
    print(f"Streams: {len(params.r_by_stream)} (bias/var learned on train)  horizon_ms={tuning.horizon_ms:g}")

    print_eval_comparison(
        preds,
        split_ns=split_ns,
        horizon_ms=tuning.horizon_ms,
        eval_min_gap_ms=float(args.eval_min_gap_ms),
        eval_move_bps=tuning.eval_move_bps,
    )

    plot_results(
        preds,
        split_ns=split_ns,
        out_path=args.out,
        title="Lighter mid prediction (Kalman state-space)",
        horizon_ms=tuning.horizon_ms,
        show=bool(args.show),
    )
    print(f"Wrote plot: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
