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
  .venv/bin/python scripts/lighter_pricing_model.py --csv logs/lighter_activity.csv --horizon-ms 500
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
    import matplotlib  # type: ignore
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
    blend_age0_ms: float
    blend_age_scale_ms: float
    blend_diff0_bps: float
    blend_diff_scale_bps: float
    blend_max_w: float
    vel_halflife_s: float
    vel_cap_bps_per_s: float
    ecm_tau_ms: float
    common_source: str
    common_disp0_bps: float
    snap_diff0_bps: float
    snap_diff_scale_bps: float
    snap_disp_max_bps: float
    snap_disp_scale_bps: float
    snap_max_w: float
    snap_age0_ms: float
    snap_age_scale_ms: float
    snap_min_n: int
    lighter_bias_halflife_s: float
    lighter_bias_cap_bps: float


def _ns_to_datetime(ns: np.ndarray) -> np.ndarray:
    return pd.to_datetime(ns, unit="ns", utc=True).to_numpy()

def _import_pyplot(show: bool, *, backend: str | None = None):
    # If pyplot is imported before setting a backend, switching backends won't work.
    # So we import it lazily after optionally selecting an interactive backend.
    import matplotlib as mpl  # type: ignore

    def _is_noninteractive_backend(name: str) -> bool:
        b = str(name).lower().strip()
        noninteractive = {
            "agg",
            "pdf",
            "ps",
            "svg",
            "cairo",
            "template",
        }
        if b in noninteractive:
            return True
        # Common IDE / notebook backends that won't pop up a native window.
        if "inline" in b or "matplotlib_inline" in b or "backend_inline" in b:
            return True
        if "nbagg" in b or "notebook" in b:
            return True
        return False

    if backend:
        try:
            mpl.use(str(backend), force=True)
        except Exception as e:
            print(f"Failed to set matplotlib backend to {backend!r}: {e}")

    if show:
        # Try to ensure an interactive backend. If none works, we'll fall back to opening the PNG.
        cur = str(mpl.get_backend())
        if _is_noninteractive_backend(cur):
            for cand in ("MacOSX", "TkAgg", "QtAgg", "Qt5Agg"):
                try:
                    mpl.use(cand, force=True)
                    break
                except Exception:
                    continue

    import matplotlib.pyplot as plt  # type: ignore

    return mpl, plt


def _open_file(path: Path) -> None:
    import os
    import subprocess
    import sys

    try:
        if sys.platform == "darwin":
            subprocess.run(["open", str(path)], check=False)
        elif sys.platform.startswith("linux"):
            subprocess.run(["xdg-open", str(path)], check=False)
        elif sys.platform.startswith("win"):
            os.startfile(str(path))  # type: ignore[attr-defined]
    except Exception:
        pass


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
    blend_w_used = np.zeros(n, dtype=np.float64)
    common_mid_used = np.full(n, np.nan, dtype=np.float64)
    common_vel_used = np.full(n, np.nan, dtype=np.float64)
    lighter_age_ms_used = np.full(n, np.nan, dtype=np.float64)
    common_mid_median_used = np.full(n, np.nan, dtype=np.float64)
    common_mad_bps_used = np.full(n, np.nan, dtype=np.float64)
    common_n_used = np.zeros(n, dtype=np.int16)
    nowcast_mid_used = np.full(n, np.nan, dtype=np.float64)
    nowcast_mid_pre_target_used = np.full(n, np.nan, dtype=np.float64)
    snap_w_used = np.zeros(n, dtype=np.float64)

    latency_us = obs["latency_us"].to_numpy(dtype=np.float64)
    spread_bps = obs["spread_bps"].to_numpy(dtype=np.float64)
    top_ratio = obs["top_ratio"].to_numpy(dtype=np.float64)
    imb = obs["imbalance"].to_numpy(dtype=np.float64)
    imb = np.where(np.isfinite(imb), imb, 0.0)
    trade_size = obs["size"].to_numpy(dtype=np.float64)
    mid_arr = obs["mid"].to_numpy(dtype=np.float64)
    obs_px_arr = obs["obs_price"].to_numpy(dtype=np.float64)
    exch_arr = obs["exchange"].astype(str).to_numpy()

    # "All-streams" state.
    x = 0.0
    p = 1.0

    # "Common" state updated only by non-Lighter observations.
    x_c = 0.0
    p_c = 1.0
    v_c = 0.0
    last_common_t = float(t_sec[0])
    last_common_x = 0.0

    last_lighter_px = float("nan")
    last_lighter_t = float("nan")

    other_exchanges = sorted(set(exch_arr) - {TARGET_EXCHANGE})
    last_by_exch = {ex: float("nan") for ex in other_exchanges if ex}

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
        x_c = phi * x_c
        p_c = (phi * phi) * p_c + q

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
                common_now_pre = math.exp(params.mu_log + x_c)
                # Add a tiny momentum term from cross-venue common velocity.
                cap_log_per_s = float(tuning.vel_cap_bps_per_s) * 1e-4
                v_eff = float(np.clip(v_c, -cap_log_per_s, cap_log_per_s))
                common_h_pre = math.exp(params.mu_log + (phi_h * x_c) + (v_eff * horizon_s))
                base_pre = last_lighter_px
                if not np.isfinite(base_pre) or base_pre <= 0:
                    base_pre = common_now_pre

                age_ms = float("inf") if not np.isfinite(last_lighter_t) else max(0.0, (t_sec[i] - last_lighter_t) * 1000.0)
                diff_bps = abs(common_now_pre - base_pre) / base_pre * 1e4 if base_pre > 0 else 0.0
                a0 = max(tuning.blend_age0_ms, 0.0)
                as_ = max(tuning.blend_age_scale_ms, 1e-6)
                d0 = max(tuning.blend_diff0_bps, 0.0)
                ds = max(tuning.blend_diff_scale_bps, 1e-6)
                w_age = float(_sigmoid((age_ms - a0) / as_))
                w_diff = float(_sigmoid((diff_bps - d0) / ds))
                w = float(np.clip(tuning.blend_max_w * w_age * w_diff, 0.0, 1.0))
                blend_w_used[i] = w
                log_base = math.log(base_pre)
                log_common = math.log(common_h_pre)
                pred_mid_pre_h[i] = math.exp(log_base + w * (log_common - log_base))

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

        # Update "common" state with non-Lighter observations only.
        if s_name != params.ref_stream:
            k_gain_c = p_c / (p_c + r_eff)
            x_c = x_c + k_gain_c * (y - x_c)
            p_c = (1.0 - k_gain_c) * p_c
            # EWMA velocity estimate from the common state increments.
            dt_c = max(0.0, float(t_sec[i] - last_common_t))
            if dt_c > 1e-6 and tuning.vel_halflife_s and tuning.vel_halflife_s > 0:
                inst_v = (x_c - last_common_x) / dt_c
                lam_v = 1.0 - math.exp(-dt_c * math.log(2.0) / tuning.vel_halflife_s)
                v_c = (1.0 - lam_v) * v_c + lam_v * inst_v
                last_common_x = x_c
                last_common_t = float(t_sec[i])

        # Robust cross-venue reference: median of last-seen book mids across non-Lighter venues.
        if exch_arr[i] != TARGET_EXCHANGE and bool(is_book[i]) and np.isfinite(obs_px_arr[i]) and obs_px_arr[i] > 0:
            if exch_arr[i] in last_by_exch:
                last_by_exch[exch_arr[i]] = float(obs_px_arr[i])

        vals = np.fromiter((v for v in last_by_exch.values() if np.isfinite(v) and v > 0), dtype=np.float64)
        common_n_used[i] = int(vals.size)
        if vals.size:
            med = float(np.median(vals))
            common_mid_median_used[i] = med
            if vals.size >= 3:
                mad = float(np.median(np.abs(vals - med)))
                if med > 0:
                    common_mad_bps_used[i] = (mad / med) * 1e4

        x_post[i] = x
        p_post[i] = p
        pred_log_post = params.mu_log + x
        pred_mid_post[i] = math.exp(pred_log_post)
        s2 = math.sqrt(max(p, 0.0))
        pred_mid_post_lo[i] = math.exp(pred_log_post - 2.0 * s2)
        pred_mid_post_hi[i] = math.exp(pred_log_post + 2.0 * s2)

        # Fast nowcast:
        # - base = most recent Lighter mid if we have it (and for the Lighter event itself, use the
        #   current mid because we've just consumed that observation)
        # - snap toward robust cross-venue consensus only when Lighter is stale and venues agree
        base_now = float(pred_mid_post[i])
        if bool(is_target_obs[i]) and np.isfinite(mid_arr[i]) and mid_arr[i] > 0:
            base_now = float(mid_arr[i])
        elif np.isfinite(last_lighter_px) and last_lighter_px > 0:
            base_now = float(last_lighter_px)
        raw_now = float(base_now)
        consensus = float(common_mid_median_used[i]) if np.isfinite(common_mid_median_used[i]) else float("nan")
        disp = float(common_mad_bps_used[i]) if np.isfinite(common_mad_bps_used[i]) else float("inf")
        age_ms = float("inf") if not np.isfinite(last_lighter_t) else max(0.0, (t_sec[i] - last_lighter_t) * 1000.0)
        w = 0.0
        if (
            np.isfinite(consensus)
            and consensus > 0
            and raw_now > 0
            and int(common_n_used[i]) >= int(max(tuning.snap_min_n, 1))
            and (not bool(is_target_obs[i]))
        ):
            diff_bps = abs(consensus - raw_now) / raw_now * 1e4
            w_diff = float(_sigmoid((diff_bps - tuning.snap_diff0_bps) / max(tuning.snap_diff_scale_bps, 1e-6)))
            w_disp = float(_sigmoid((tuning.snap_disp_max_bps - disp) / max(tuning.snap_disp_scale_bps, 1e-6)))
            w_age = float(_sigmoid((age_ms - tuning.snap_age0_ms) / max(tuning.snap_age_scale_ms, 1e-6)))
            w = float(np.clip(tuning.snap_max_w * w_diff * w_disp * w_age, 0.0, 1.0))
            raw_now = math.exp(math.log(raw_now) + w * (math.log(consensus) - math.log(raw_now)))
        nowcast_mid_used[i] = raw_now
        snap_w_used[i] = w

        if bool(is_target_obs[i]) and np.isfinite(pred_mid_pre[i]) and pred_mid_pre[i] > 0:
            raw_pre = float(last_lighter_px) if np.isfinite(last_lighter_px) and last_lighter_px > 0 else float(pred_mid_pre[i])
            if (
                np.isfinite(consensus)
                and consensus > 0
                and raw_pre > 0
                and int(common_n_used[i]) >= int(max(tuning.snap_min_n, 1))
            ):
                diff_bps = abs(consensus - raw_pre) / raw_pre * 1e4
                w_diff = float(_sigmoid((diff_bps - tuning.snap_diff0_bps) / max(tuning.snap_diff_scale_bps, 1e-6)))
                w_disp = float(_sigmoid((tuning.snap_disp_max_bps - disp) / max(tuning.snap_disp_scale_bps, 1e-6)))
                w_age = float(_sigmoid((age_ms - tuning.snap_age0_ms) / max(tuning.snap_age_scale_ms, 1e-6)))
                w_pre = float(np.clip(tuning.snap_max_w * w_diff * w_disp * w_age, 0.0, 1.0))
                raw_pre = math.exp(math.log(raw_pre) + w_pre * (math.log(consensus) - math.log(raw_pre)))
            nowcast_mid_pre_target_used[i] = raw_pre
        # Record common state for downstream (horizon forecasting is applied as a post-process ECM step).
        if horizon_s > 0:
            common_now = math.exp(params.mu_log + x_c)
            common_mid_used[i] = common_now
            cap_log_per_s = float(tuning.vel_cap_bps_per_s) * 1e-4
            v_eff = float(np.clip(v_c, -cap_log_per_s, cap_log_per_s))
            common_vel_used[i] = v_eff
            age_ms = float("nan") if not np.isfinite(last_lighter_t) else max(0.0, (t_sec[i] - last_lighter_t) * 1000.0)
            lighter_age_ms_used[i] = age_ms
            pred_mid_post_h[i] = pred_mid_post[i]
        else:
            pred_mid_post_h[i] = pred_mid_post[i]

        # Track last Lighter mid for future events (after we've computed pre-target quantities).
        if bool(is_target_obs[i]) and np.isfinite(mid_arr[i]) and mid_arr[i] > 0:
            last_lighter_px = float(mid_arr[i])
            last_lighter_t = float(t_sec[i])
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
    out["pred_mid_raw"] = pred_mid_post
    out["pred_mid"] = nowcast_mid_used
    out["pred_mid_lo_2s"] = pred_mid_post_lo
    out["pred_mid_hi_2s"] = pred_mid_post_hi
    out["pred_mid_h"] = pred_mid_post_h
    out["pred_mid_pre_target_raw"] = pred_mid_pre
    out["pred_mid_pre_target"] = nowcast_mid_pre_target_used
    out["pred_mid_pre_target_lo_2s"] = pred_mid_pre_lo
    out["pred_mid_pre_target_hi_2s"] = pred_mid_pre_hi
    out["pred_mid_pre_target_h"] = pred_mid_pre_h
    out["x_filt_online"] = x_post
    out["p_filt_online"] = p_post
    out["r_eff"] = r_eff_used
    out["nu"] = nu_used
    out["blend_w"] = blend_w_used
    out["common_mid"] = common_mid_used
    out["common_vel"] = common_vel_used
    out["common_mid_median"] = common_mid_median_used
    out["common_mad_bps"] = common_mad_bps_used
    out["common_n"] = common_n_used.astype(np.int16)
    out["snap_w"] = snap_w_used
    out["lighter_age_ms"] = lighter_age_ms_used
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


def _past_value_at_or_before(t_ns: np.ndarray, values: np.ndarray, query_ns: np.ndarray) -> np.ndarray:
    t_ns = np.asarray(t_ns, dtype=np.int64)
    values = np.asarray(values, dtype=np.float64)
    query_ns = np.asarray(query_ns, dtype=np.int64)
    idx = np.searchsorted(t_ns, query_ns, side="right") - 1
    out = np.full(len(query_ns), np.nan, dtype=np.float64)
    ok = idx >= 0
    out[ok] = values[idx[ok]]
    return out


def _sigmoid(x: np.ndarray | float) -> np.ndarray | float:
    x = np.asarray(x, dtype=np.float64) if not np.isscalar(x) else float(x)
    # Numerically stable sigmoid.
    if np.isscalar(x):
        if x >= 0:
            z = math.exp(-x)
            return 1.0 / (1.0 + z)
        z = math.exp(x)
        return z / (1.0 + z)
    out = np.empty_like(x, dtype=np.float64)
    pos = x >= 0
    out[pos] = 1.0 / (1.0 + np.exp(-x[pos]))
    z = np.exp(x[~pos])
    out[~pos] = z / (1.0 + z)
    return out


def _autocorr_lag1(err: np.ndarray) -> float:
    err = np.asarray(err, dtype=np.float64)
    m = np.isfinite(err)
    err = err[m]
    if len(err) < 3:
        return float("nan")
    x = err[:-1]
    y = err[1:]
    x = x - float(np.mean(x))
    y = y - float(np.mean(y))
    denom = float(np.sqrt(np.mean(x * x) * np.mean(y * y)))
    if denom <= 0:
        return float("nan")
    return float(np.mean(x * y) / denom)


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


def apply_ecm_horizon_forecast(
    preds: pd.DataFrame,
    *,
    split_ns: int,
    horizon_ms: float,
    diff0_bps: float,
    diff_scale_bps: float,
    max_w: float,
    tau_ms: float,
    common_source: str,
    fit_tau: bool = True,
    max_fit_points: int = 250_000,
) -> Tuple[pd.DataFrame, float]:
    if horizon_ms <= 0:
        return preds, float("nan")

    preds = preds.copy()
    horizon_ns = int(horizon_ms * 1e6)

    # Build future Lighter mid at t+h (first Lighter update at/after t+h).
    lighter_updates = preds[preds["lighter_mid"].notna()][["t_ns", "lighter_mid"]].copy()
    if len(lighter_updates) < 10:
        return preds, float("nan")
    t_l = lighter_updates["t_ns"].to_numpy(dtype=np.int64)
    m_l = lighter_updates["lighter_mid"].to_numpy(dtype=np.float64)

    t = preds["t_ns"].to_numpy(dtype=np.int64)
    future_mid = _future_value_at_or_after(t_l, m_l, t + horizon_ns)

    base = preds["lighter_mid_ffill"].to_numpy(dtype=np.float64)
    common_source = str(common_source).lower().strip()
    if common_source == "median" and "common_mid_median" in preds.columns:
        common_raw = preds["common_mid_median"].to_numpy(dtype=np.float64)
    else:
        common_raw = preds["common_mid"].to_numpy(dtype=np.float64)
    common = pd.Series(common_raw).ffill().to_numpy(dtype=np.float64)
    age_ms = preds["lighter_age_ms"].to_numpy(dtype=np.float64) if "lighter_age_ms" in preds.columns else np.full(len(preds), np.nan)

    ok = np.isfinite(future_mid) & np.isfinite(base) & np.isfinite(common) & (base > 0) & (common > 0)
    if not ok.any():
        return preds, float("nan")

    with np.errstate(divide="ignore", invalid="ignore"):
        diff_bps = np.abs((common - base) / base) * 1e4
    w_diff = _sigmoid((diff_bps - float(diff0_bps)) / max(float(diff_scale_bps), 1e-6))
    w_diff = np.where(np.isfinite(w_diff), w_diff, 0.0)
    # Only correct when Lighter is stale-ish (optional gate).
    # If age_ms is missing, w_age=1. If age_ms is NaN (before first Lighter update), w_age=1.
    w_age = np.ones(len(preds), dtype=np.float64)
    if np.isfinite(age_ms).any():
        # Thresholds come from CLI args (stashed into dataframe attrs by main).
        age0 = float(preds.attrs.get("blend_age0_ms", 50.0))
        age_scale = max(float(preds.attrs.get("blend_age_scale_ms", 25.0)), 1e-6)
        w_age = _sigmoid((np.where(np.isfinite(age_ms), age_ms, 1e9) - age0) / age_scale)
        w_age = np.where(np.isfinite(w_age), w_age, 1.0)

    # Fit tau on in-sample points if requested.
    if fit_tau and (not tau_ms or tau_ms <= 0):
        # Fit on the stale subset where corrections are meant to matter.
        idx = np.where(ok & (t <= split_ns) & (w_age >= 0.5))[0]
        if len(idx) > max_fit_points:
            step = int(math.ceil(len(idx) / max_fit_points))
            idx = idx[::step]
        if len(idx) < 500:
            idx = np.where(ok & (t <= split_ns))[0]
            if len(idx) > max_fit_points:
                step = int(math.ceil(len(idx) / max_fit_points))
                idx = idx[::step]

        log_base = np.log(base[idx])
        log_common = np.log(common[idx])
        y = np.log(future_mid[idx])
        w_d = w_diff[idx]
        w_a = w_age[idx]

        def obj(log_tau: float) -> float:
            tau = math.exp(log_tau)
            beta = (1.0 - math.exp(-horizon_ms / tau)) * float(max_w)
            w = np.clip(beta * w_d * w_a, 0.0, 1.0)
            pred = log_base + w * (log_common - log_base)
            e = np.exp(pred) - np.exp(y)
            return float(np.sqrt(np.mean(e * e)))

        res = minimize_scalar(obj, bounds=(math.log(5.0), math.log(20_000.0)), method="bounded")
        tau_ms = float(math.exp(res.x))

    tau_ms = float(tau_ms) if tau_ms and tau_ms > 0 else 1e9
    beta = (1.0 - math.exp(-horizon_ms / tau_ms)) * float(max_w)

    log_base_all = np.log(np.where(base > 0, base, np.nan))
    log_common_all = np.log(np.where(common > 0, common, np.nan))
    w = np.clip(beta * w_diff * w_age, 0.0, 1.0)
    log_pred_h = log_base_all + w * (log_common_all - log_base_all)
    preds["blend_w"] = w
    preds["pred_mid_h"] = np.exp(log_pred_h)

    # Also fill the pre-target horizon prediction using the previous Lighter mid as base.
    is_target = preds["is_target"].to_numpy(dtype=bool)
    base_prev = preds["naive_prev_lighter"].to_numpy(dtype=np.float64)
    ok_prev = is_target & np.isfinite(base_prev) & (base_prev > 0) & np.isfinite(common) & (common > 0)
    with np.errstate(divide="ignore", invalid="ignore"):
        diff_prev_bps = np.abs((common - base_prev) / base_prev) * 1e4
    w_diff_prev = _sigmoid((diff_prev_bps - float(diff0_bps)) / max(float(diff_scale_bps), 1e-6))
    w_prev = np.clip(beta * np.where(np.isfinite(w_diff_prev), w_diff_prev, 0.0) * w_age, 0.0, 1.0)
    log_base_prev = np.log(np.where(base_prev > 0, base_prev, np.nan))
    log_pred_pre_h = log_base_prev + w_prev * (log_common_all - log_base_prev)
    preds["pred_mid_pre_target_h"] = np.where(ok_prev, np.exp(log_pred_pre_h), np.nan)

    return preds, tau_ms


def fit_horizon_linear_model(
    preds: pd.DataFrame,
    *,
    split_ns: int,
    horizon_ms: float,
    mom_lag_ms: float,
    mom_lookback: int,
    common_source: str,
    common_disp0_bps: float,
    base_source: str,
    ridge_l2: float = 0.0,
    max_fit_points: int = 250_000,
) -> Tuple[np.ndarray, float]:
    horizon_ns = int(horizon_ms * 1e6)
    lag_ns = int(mom_lag_ms * 1e6)

    lighter_updates = preds[preds["lighter_mid"].notna()][["t_ns", "lighter_mid"]].copy()
    if len(lighter_updates) < 10:
        return np.array([0.0, 0.0, 0.0], dtype=np.float64), float("nan")
    t_l = lighter_updates["t_ns"].to_numpy(dtype=np.int64)
    m_l = lighter_updates["lighter_mid"].to_numpy(dtype=np.float64)

    t = preds["t_ns"].to_numpy(dtype=np.int64)
    future_mid = _future_value_at_or_after(t_l, m_l, t + horizon_ns)

    base_source = str(base_source).lower().strip()
    if base_source == "nowcast":
        if "pred_mid" in preds.columns:
            base = preds["pred_mid"].to_numpy(dtype=np.float64)
        else:
            base = preds["lighter_mid_ffill"].to_numpy(dtype=np.float64)
    else:
        base = preds["lighter_mid_ffill"].to_numpy(dtype=np.float64)
    common_source = str(common_source).lower().strip()
    if common_source == "median" and "common_mid_median" in preds.columns:
        common_raw = preds["common_mid_median"].to_numpy(dtype=np.float64)
        mad_bps = preds["common_mad_bps"].to_numpy(dtype=np.float64) if "common_mad_bps" in preds.columns else np.full(len(preds), np.nan)
    else:
        common_raw = preds["common_mid"].to_numpy(dtype=np.float64)
        mad_bps = np.full(len(preds), 0.0, dtype=np.float64)
    common = pd.Series(common_raw).ffill().to_numpy(dtype=np.float64)
    mad_bps = pd.Series(mad_bps).ffill().to_numpy(dtype=np.float64)
    base_lag = _past_value_at_or_before(t, base, t - lag_ns)
    lookback = int(max(mom_lookback, 1))
    common_lags = [
        _past_value_at_or_before(t, common, t - (k * lag_ns)) for k in range(1, lookback + 1)
    ]
    age_ms = preds["lighter_age_ms"].to_numpy(dtype=np.float64) if "lighter_age_ms" in preds.columns else np.full(len(preds), np.nan)

    ok = (
        np.isfinite(future_mid)
        & np.isfinite(base)
        & np.isfinite(common)
        & np.isfinite(base_lag)
        & np.isfinite(np.vstack(common_lags)).all(axis=0)
        & (future_mid > 0)
        & (base > 0)
        & (common > 0)
        & (base_lag > 0)
        & (np.vstack(common_lags) > 0).all(axis=0)
        & (t <= split_ns)
    )
    idx = np.where(ok)[0]
    if len(idx) > max_fit_points:
        step = int(math.ceil(len(idx) / max_fit_points))
        idx = idx[::step]

    lb = np.log(base[idx])
    lc = np.log(common[idx])
    y = np.log(future_mid[idx]) - lb
    # Preemptive momentum lookback: use non-Lighter consensus momentum over multiple lags.
    # mom_k = log(common_{t-(k-1)}) - log(common_{t-k}) where each step is mom_lag_ms.
    log_common_lags = [np.log(l[idx]) for l in common_lags]
    moms: List[np.ndarray] = [lc - log_common_lags[0]]
    for k in range(1, lookback):
        moms.append(log_common_lags[k - 1] - log_common_lags[k])
    div = lc - lb
    disp0 = max(float(common_disp0_bps), 1e-6)
    disp_w = 1.0 / (1.0 + np.clip(mad_bps[idx], 0.0, 1e6) / disp0)
    div = div * disp_w
    if np.isfinite(age_ms).any():
        a0 = float(preds.attrs.get("blend_age0_ms", 50.0))
        as_ = max(float(preds.attrs.get("blend_age_scale_ms", 25.0)), 1e-6)
        age = np.where(np.isfinite(age_ms[idx]), age_ms[idx], 1e9)
        w_age = _sigmoid((age - a0) / as_)
    else:
        w_age = np.ones(len(idx), dtype=np.float64)
    m2 = np.isfinite(y) & np.isfinite(div)
    for m in moms:
        m2 = m2 & np.isfinite(m)
    y = y[m2]
    moms = [m[m2] for m in moms]
    div = div[m2]
    w_age = w_age[m2]

    # Robustness: clip extreme values to avoid numerical issues during least-squares.
    moms = [np.clip(m, -0.02, 0.02) for m in moms]  # ±200 bps
    div = np.clip(div, -0.02, 0.02)
    y = np.clip(y, -0.05, 0.05)  # ±500 bps

    # Weighted ridge regression with intercept:
    # y = Σ b_k * mom_k + b_div * div + b0
    X = np.column_stack([*moms, div, np.ones(len(y))]).astype(np.float64, copy=False)
    y = y.astype(np.float64, copy=False)
    w = np.sqrt(np.clip(w_age, 0.0, 1.0)).astype(np.float64, copy=False)
    Xw = X * w[:, None]
    yw = y * w
    l2 = float(max(ridge_l2, 0.0))
    if l2 > 0:
        # Ridge via augmented least-squares (more numerically stable than forming X'X).
        # Penalize all coefficients except the intercept (last term).
        p = X.shape[1]
        P = np.eye(p, dtype=np.float64)
        P[-1, -1] = 0.0
        A = np.vstack([Xw, math.sqrt(l2) * P])
        b_aug = np.concatenate([yw, np.zeros(p, dtype=np.float64)])
        beta, *_ = np.linalg.lstsq(A, b_aug, rcond=None)
    else:
        beta, *_ = np.linalg.lstsq(Xw, yw, rcond=None)
    with np.errstate(over="ignore", divide="ignore", invalid="ignore"):
        pred = X @ beta
    rmse = float(np.sqrt(np.mean((pred - y) ** 2))) if len(y) else float("nan")
    return beta.astype(np.float64), rmse


def apply_horizon_linear_forecast(
    preds: pd.DataFrame,
    *,
    beta: np.ndarray,
    horizon_ms: float,
    mom_lag_ms: float,
    mom_lookback: int,
    move_cap_bps: float,
    common_source: str,
    common_disp0_bps: float,
    base_source: str,
) -> pd.DataFrame:
    preds = preds.copy()
    horizon_ns = int(horizon_ms * 1e6)
    lag_ns = int(mom_lag_ms * 1e6)

    t = preds["t_ns"].to_numpy(dtype=np.int64)
    base_source = str(base_source).lower().strip()
    if base_source == "nowcast":
        if "pred_mid" in preds.columns:
            base = preds["pred_mid"].to_numpy(dtype=np.float64)
        else:
            base = preds["lighter_mid_ffill"].to_numpy(dtype=np.float64)
    else:
        base = preds["lighter_mid_ffill"].to_numpy(dtype=np.float64)
    common_source = str(common_source).lower().strip()
    if common_source == "median" and "common_mid_median" in preds.columns:
        common_raw = preds["common_mid_median"].to_numpy(dtype=np.float64)
        mad_bps = preds["common_mad_bps"].to_numpy(dtype=np.float64) if "common_mad_bps" in preds.columns else np.full(len(preds), np.nan)
    else:
        common_raw = preds["common_mid"].to_numpy(dtype=np.float64)
        mad_bps = np.full(len(preds), 0.0, dtype=np.float64)
    common = pd.Series(common_raw).ffill().to_numpy(dtype=np.float64)
    mad_bps = pd.Series(mad_bps).ffill().to_numpy(dtype=np.float64)
    base_lag = _past_value_at_or_before(t, base, t - lag_ns)
    lookback = int(max(mom_lookback, 1))
    common_lags = [
        _past_value_at_or_before(t, common, t - (k * lag_ns)) for k in range(1, lookback + 1)
    ]

    preds["pred_mid_h"] = base
    ok = np.isfinite(base) & np.isfinite(common) & (base > 0) & (common > 0)
    ok = ok & np.isfinite(np.vstack(common_lags)).all(axis=0) & (np.vstack(common_lags) > 0).all(axis=0)
    lb = np.log(np.where(ok, base, np.nan))
    lc = np.log(np.where(ok, common, np.nan))
    log_common_lags = [np.log(np.where(ok, l, np.nan)) for l in common_lags]
    moms: List[np.ndarray] = [lc - log_common_lags[0]]
    for k in range(1, lookback):
        moms.append(log_common_lags[k - 1] - log_common_lags[k])
    div = lc - lb
    disp0 = max(float(common_disp0_bps), 1e-6)
    disp_w = 1.0 / (1.0 + np.clip(mad_bps, 0.0, 1e6) / disp0)
    div = div * disp_w

    moms = [np.clip(m, -0.02, 0.02) for m in moms]
    div = np.clip(div, -0.02, 0.02)
    # y_hat = Σ b_k*mom_k + b_div*div + b0
    y_hat = np.zeros(len(preds), dtype=np.float64)
    for k, m in enumerate(moms):
        y_hat = y_hat + float(beta[k]) * m
    y_hat = y_hat + float(beta[lookback]) * div + float(beta[lookback + 1])
    cap = float(move_cap_bps) * 1e-4
    y_hat = np.clip(y_hat, -cap, cap)
    preds.loc[ok, "pred_mid_h"] = np.exp(lb[ok] + y_hat[ok])

    # For target-update evaluation, use the prediction available immediately before the Lighter update.
    # run_predictions sorts non-Lighter events before Lighter at the same timestamp, so shift(1) is causal.
    is_target = preds["is_target"].to_numpy(dtype=bool)
    pred_shift = pd.Series(preds["pred_mid_h"]).shift(1).to_numpy(dtype=np.float64)
    preds["pred_mid_pre_target_h"] = np.where(is_target, pred_shift, np.nan)
    return preds


def apply_horizon_blend_forecast(
    preds: pd.DataFrame,
    *,
    horizon_ms: float,
    diff0_bps: float,
    diff_scale_bps: float,
    age0_ms: float,
    age_scale_ms: float,
    max_w: float,
    vel_halflife_s: float,
    vel_cap_bps_per_s: float,
    common_source: str,
    common_disp0_bps: float,
    base_source: str,
    min_common_n: int,
) -> pd.DataFrame:
    preds = preds.copy()
    if horizon_ms <= 0:
        return preds

    horizon_s = float(horizon_ms) / 1000.0
    t = preds["t_ns"].to_numpy(dtype=np.int64)

    base_source = str(base_source).lower().strip()
    if base_source == "nowcast" and "pred_mid" in preds.columns:
        base = preds["pred_mid"].to_numpy(dtype=np.float64)
    else:
        base = preds["lighter_mid_ffill"].to_numpy(dtype=np.float64)

    common_source = str(common_source).lower().strip()
    if common_source == "median" and "common_mid_median" in preds.columns:
        common_raw = preds["common_mid_median"].to_numpy(dtype=np.float64)
        mad_bps = preds["common_mad_bps"].to_numpy(dtype=np.float64) if "common_mad_bps" in preds.columns else np.full(len(preds), np.nan)
        common_n = preds["common_n"].to_numpy(dtype=np.float64) if "common_n" in preds.columns else np.full(len(preds), np.nan)
    else:
        common_raw = preds["common_mid"].to_numpy(dtype=np.float64)
        mad_bps = np.full(len(preds), 0.0, dtype=np.float64)
        common_n = np.full(len(preds), np.nan, dtype=np.float64)

    common = pd.Series(common_raw).ffill().to_numpy(dtype=np.float64)
    mad_bps = pd.Series(mad_bps).ffill().to_numpy(dtype=np.float64)
    common_n = pd.Series(common_n).ffill().to_numpy(dtype=np.float64)

    # Consensus velocity from the (ffilled) common series.
    log_common = np.log(np.where(common > 0, common, np.nan))
    v = 0.0
    v_arr = np.zeros(len(preds), dtype=np.float64)
    last_log = float("nan")
    last_t = int(t[0]) if len(t) else 0
    cap_log_per_s = float(vel_cap_bps_per_s) * 1e-4
    for i in range(len(preds)):
        if i == 0:
            last_log = float(log_common[0]) if np.isfinite(log_common[0]) else float("nan")
            v_arr[0] = 0.0
            continue
        dt = float(t[i] - last_t) / 1e9
        dt = max(dt, 0.0)
        if dt > 1e-9 and np.isfinite(log_common[i]) and np.isfinite(last_log):
            inst_v = float((log_common[i] - last_log) / dt)
            if vel_halflife_s and vel_halflife_s > 0:
                lam = 1.0 - math.exp(-dt * math.log(2.0) / float(vel_halflife_s))
            else:
                lam = 1.0
            v = (1.0 - lam) * v + lam * inst_v
            v = float(np.clip(v, -cap_log_per_s, cap_log_per_s))
        if np.isfinite(log_common[i]):
            last_log = float(log_common[i])
            last_t = int(t[i])
        v_arr[i] = v

    common_h = common * np.exp(v_arr * horizon_s)

    # Weighting: only adjust toward consensus when Lighter is stale and venues agree.
    age = preds["lighter_age_ms"].to_numpy(dtype=np.float64) if "lighter_age_ms" in preds.columns else np.full(len(preds), np.nan)
    age_eff = np.where(np.isfinite(age), age, 1e9)
    w_age = _sigmoid((age_eff - float(age0_ms)) / max(float(age_scale_ms), 1e-6))
    disp0 = max(float(common_disp0_bps), 1e-6)
    w_disp = 1.0 / (1.0 + np.clip(mad_bps, 0.0, 1e6) / disp0)
    min_n = int(max(min_common_n, 1))
    w_n = np.where(np.isfinite(common_n) & (common_n >= min_n), 1.0, 0.0)

    ok = np.isfinite(base) & np.isfinite(common_h) & (base > 0) & (common_h > 0)
    with np.errstate(divide="ignore", invalid="ignore"):
        diff_bps = np.abs((common_h - base) / base) * 1e4
    w_diff = _sigmoid((np.where(np.isfinite(diff_bps), diff_bps, 0.0) - float(diff0_bps)) / max(float(diff_scale_bps), 1e-6))
    w = np.clip(float(max_w) * w_age * w_diff * w_disp * w_n, 0.0, 1.0)
    preds["blend_w"] = w

    lb = np.log(np.where(ok, base, np.nan))
    lc = np.log(np.where(ok, common_h, np.nan))
    log_pred = lb + w * (lc - lb)
    preds["pred_mid_h"] = base
    preds.loc[ok, "pred_mid_h"] = np.exp(log_pred[ok])

    is_target = preds["is_target"].to_numpy(dtype=bool)
    pred_shift = pd.Series(preds["pred_mid_h"]).shift(1).to_numpy(dtype=np.float64)
    preds["pred_mid_pre_target_h"] = np.where(is_target, pred_shift, np.nan)
    return preds


def apply_horizon_preemptive_forecast(
    preds: pd.DataFrame,
    *,
    horizon_ms: float,
    common_source: str,
    vel_halflife_s: float,
    vel_cap_bps_per_s: float,
    bias_halflife_s: float,
    bias_cap_bps: float,
    min_common_n: int,
) -> pd.DataFrame:
    """
    Preemptive forecast of Lighter(t+h) from non-Lighter consensus:
      log(L_h) = log(common_h) + bias_t

    - common_h is the non-Lighter consensus projected forward by an EWMA velocity.
    - bias_t is an EWMA estimate of (log Lighter - log common), updated only when Lighter updates arrive.

    This will move immediately when other venues move (preemptive), rather than hugging the latest Lighter tick.
    """
    preds = preds.copy()
    if horizon_ms <= 0:
        return preds

    horizon_s = float(horizon_ms) / 1000.0
    t = preds["t_ns"].to_numpy(dtype=np.int64)

    common_source = str(common_source).lower().strip()
    if common_source == "median" and "common_mid_median" in preds.columns:
        common_raw = preds["common_mid_median"].to_numpy(dtype=np.float64)
        common_n = preds["common_n"].to_numpy(dtype=np.float64) if "common_n" in preds.columns else np.full(len(preds), np.nan)
    else:
        common_raw = preds["common_mid"].to_numpy(dtype=np.float64)
        common_n = np.full(len(preds), np.nan, dtype=np.float64)

    common = pd.Series(common_raw).ffill().to_numpy(dtype=np.float64)
    common_n = pd.Series(common_n).ffill().to_numpy(dtype=np.float64)
    log_common = np.log(np.where(common > 0, common, np.nan))

    # EWMA velocity from the (ffilled) common series in log-space.
    v = 0.0
    v_arr = np.zeros(len(preds), dtype=np.float64)
    last_log = float("nan")
    last_t = int(t[0]) if len(t) else 0
    cap_log_per_s = float(vel_cap_bps_per_s) * 1e-4
    for i in range(len(preds)):
        if i == 0:
            last_log = float(log_common[0]) if np.isfinite(log_common[0]) else float("nan")
            v_arr[0] = 0.0
            continue
        dt = float(t[i] - last_t) / 1e9
        dt = max(dt, 0.0)
        if dt > 1e-9 and np.isfinite(log_common[i]) and np.isfinite(last_log):
            inst_v = float((log_common[i] - last_log) / dt)
            if vel_halflife_s and vel_halflife_s > 0:
                lam = 1.0 - math.exp(-dt * math.log(2.0) / float(vel_halflife_s))
            else:
                lam = 1.0
            v = (1.0 - lam) * v + lam * inst_v
            v = float(np.clip(v, -cap_log_per_s, cap_log_per_s))
        if np.isfinite(log_common[i]):
            last_log = float(log_common[i])
            last_t = int(t[i])
        v_arr[i] = v

    common_h = np.exp(log_common + v_arr * horizon_s)

    # Bias EWMA: update only at Lighter orderbook updates.
    bias = 0.0
    bias_arr = np.zeros(len(preds), dtype=np.float64)
    cap_bias = float(bias_cap_bps) * 1e-4
    is_target = preds["is_target"].to_numpy(dtype=bool)
    lighter_mid = preds["lighter_mid"].to_numpy(dtype=np.float64)
    last_bias_t = int(t[0]) if len(t) else 0
    for i in range(len(preds)):
        dt = float(t[i] - last_bias_t) / 1e9
        dt = max(dt, 0.0)
        last_bias_t = int(t[i])

        if bool(is_target[i]) and np.isfinite(lighter_mid[i]) and lighter_mid[i] > 0 and np.isfinite(log_common[i]):
            resid = float(math.log(float(lighter_mid[i])) - float(log_common[i]))
            if bias_halflife_s and bias_halflife_s > 0 and dt > 0:
                lam = 1.0 - math.exp(-dt * math.log(2.0) / float(bias_halflife_s))
            else:
                lam = 1.0
            bias = (1.0 - lam) * bias + lam * resid
            bias = float(np.clip(bias, -cap_bias, cap_bias))
        bias_arr[i] = bias

    ok = np.isfinite(common_h) & (common_h > 0) & np.isfinite(bias_arr)
    min_n = int(max(min_common_n, 1))
    if "common_n" in preds.columns:
        ok = ok & (np.where(np.isfinite(common_n), common_n, 0.0) >= float(min_n))

    preds["pred_mid_h"] = np.nan
    preds.loc[ok, "pred_mid_h"] = np.exp(np.log(common_h[ok]) + bias_arr[ok])
    preds["lighter_bias_log"] = bias_arr

    pred_shift = pd.Series(preds["pred_mid_h"]).shift(1).to_numpy(dtype=np.float64)
    preds["pred_mid_pre_target_h"] = np.where(is_target, pred_shift, np.nan)
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
            e_k = model[idx] - future_mid[idx]
            e_n = naive[idx] - future_mid[idx]
            print(f"Streaming horizon error lag-1 autocorr: Kalman={_autocorr_lag1(e_k):.3f} naive={_autocorr_lag1(e_n):.3f}")
            if "lighter_age_ms" in preds.columns:
                age = preds["lighter_age_ms"].to_numpy(dtype=np.float64)[idx]
                for thr in (50.0, 100.0, 200.0):
                    stale = np.isfinite(age) & (age >= thr)
                    if int(stale.sum()) < 500:
                        continue
                    mk_in = sse(model[idx][stale & in_mask], future_mid[idx][stale & in_mask])
                    mk_out = sse(model[idx][stale & out_mask], future_mid[idx][stale & out_mask])
                    nk_in = sse(naive[idx][stale & in_mask], future_mid[idx][stale & in_mask])
                    nk_out = sse(naive[idx][stale & out_mask], future_mid[idx][stale & out_mask])
                    print(
                        f"Streaming stale(age>={thr:g}ms) RMSE: "
                        f"Kalman in={mk_in:.2f} out={mk_out:.2f} | naive in={nk_in:.2f} out={nk_out:.2f} | "
                        f"impr in={(nk_in-mk_in):.2f} out={(nk_out-mk_out):.2f}"
                    )

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
    plot_other_exchanges: bool = True,
    mpl_backend: str | None = None,
    show: bool = True,
    max_points: int = 80_000,
) -> None:
    mpl, plt = _import_pyplot(show, backend=mpl_backend)
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    def _is_noninteractive_backend(name: str) -> bool:
        b = str(name).lower().strip()
        if b in {"agg", "pdf", "ps", "svg", "cairo", "template"}:
            return True
        if "inline" in b or "matplotlib_inline" in b or "backend_inline" in b:
            return True
        if "nbagg" in b or "notebook" in b:
            return True
        return False

    # For plotting, downsample to keep render times reasonable.
    if len(preds) > max_points:
        step = int(math.ceil(len(preds) / max_points))
        p = preds.iloc[::step].copy()
    else:
        p = preds.copy()

    fig, (ax0, ax1) = plt.subplots(2, 1, figsize=(14, 8), sharex=True, gridspec_kw={"height_ratios": [3, 1]})

    has_h = bool(horizon_ms and horizon_ms > 0) and ("pred_mid_h" in p.columns) and np.isfinite(p["pred_mid_h"]).any()
    is_target_plot = p["is_target"].to_numpy(dtype=bool) if "is_target" in p.columns else np.zeros(len(p), dtype=bool)
    pred_h_plot = p["pred_mid_h"].to_numpy(dtype=np.float64) if has_h else None
    if pred_h_plot is not None and np.isfinite(pred_h_plot).any():
        # For visualization, avoid any "looks like leakage" at Lighter update instants by plotting the
        # prediction that existed immediately before the Lighter update.
        pred_shift = pd.Series(pred_h_plot).shift(1).to_numpy(dtype=np.float64)
        pred_h_plot = np.where(is_target_plot, pred_shift, pred_h_plot)

    # Optional: plot other exchanges' book mids (faint) for context.
    if plot_other_exchanges:
        other_exchanges = sorted(set(preds["exchange"].astype(str).unique()) - {TARGET_EXCHANGE})
        per_exch_cap = max(2_000, int(max_points // max(len(other_exchanges), 1) // 2))
        for exch in other_exchanges:
            s = preds[(preds["exchange"].astype(str) == exch) & (preds["is_book"].to_numpy(dtype=bool))][
                ["t_ns", "t_dt", "obs_price"]
            ].copy()
            s = s[np.isfinite(s["obs_price"])]
            if s.empty:
                continue
            if len(s) > per_exch_cap:
                step = int(math.ceil(len(s) / per_exch_cap))
                s = s.iloc[::step]
            ax0.step(
                s["t_dt"],
                s["obs_price"],
                where="post",
                linewidth=0.9,
                alpha=0.22,
                label=f"{exch} mid",
            )

    if has_h and pred_h_plot is not None and not np.allclose(pred_h_plot, p["pred_mid"].to_numpy(dtype=np.float64), equal_nan=True):
        ax0.step(
            p["t_dt"],
            pred_h_plot,
            where="post",
            label=f"Predicted Lighter mid (+{horizon_ms:g}ms)",
            linewidth=1.4,
            color="red",
        )
        ax0.step(p["t_dt"], p["pred_mid"], where="post", label="Nowcast (t)", linewidth=0.9, alpha=0.5)
    else:
        ax0.step(p["t_dt"], p["pred_mid"], where="post", label="Nowcast (t)", linewidth=1.2, color="red")
    ax0.fill_between(
        p["t_dt"],
        p["pred_mid_lo_2s"],
        p["pred_mid_hi_2s"],
        color="C0",
        alpha=0.12,
        linewidth=0,
        label="±2σ (log-space)",
    )
    ax0.step(
        p["t_dt"],
        p["lighter_mid_ffill"],
        where="post",
        label="Lighter mid (ffill)",
        linewidth=1.0,
        alpha=0.8,
        color="blue",
    )

    # If horizon is enabled, overlay the realized Lighter mid at t+h on the same time axis (t).
    if has_h:
        horizon_ns = int(horizon_ms * 1e6)
        lighter_updates = preds[preds["lighter_mid"].notna()][["t_ns", "lighter_mid"]].copy()
        if len(lighter_updates) > 2:
            t_l = lighter_updates["t_ns"].to_numpy(dtype=np.int64)
            m_l = lighter_updates["lighter_mid"].to_numpy(dtype=np.float64)
            realized_h = _future_value_at_or_after(t_l, m_l, p["t_ns"].to_numpy(dtype=np.int64) + horizon_ns)
            ax0.step(
                p["t_dt"],
                realized_h,
                where="post",
                label=f"Realized Lighter mid at t+{horizon_ms:g}ms (shifted to t)",
                linewidth=1.2,
                alpha=0.85,
                color="black",
            )

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
            print(f"matplotlib backend: {mpl.get_backend()}")
            if _is_noninteractive_backend(mpl.get_backend()):
                print(f"Non-interactive matplotlib backend ({mpl.get_backend()}); opening saved plot instead: {out_path}")
                _open_file(out_path)
            else:
                plt.show(block=True)
        except Exception as e:  # pragma: no cover
            print(f"Plot display failed (backend={mpl.get_backend()}): {e}")
            print(f"Opening saved plot instead: {out_path}")
            _open_file(out_path)
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
    ap.add_argument("--horizon-ms", type=float, default=500.0, help="Predict forward by this many ms (quote-latency horizon).")
    ap.add_argument("--q-floor-mult", type=float, default=20.0, help="Floor q_per_sec using Lighter realized variance * this.")
    ap.add_argument("--q-dt-floor-ms", type=float, default=1.0, help="dt floor for q floor estimation.")
    ap.add_argument("--eval-min-gap-ms", type=float, default=0.0, help="Evaluate only on target updates with >= this gap from the previous target update.")
    ap.add_argument("--eval-move-bps", type=float, default=1.0, help="Also evaluate on points with |Δ mid| >= this (bps).")
    ap.add_argument("--blend-age0-ms", type=float, default=50.0, help="Blending: age (ms) where we start trusting cross-venue more.")
    ap.add_argument("--blend-age-scale-ms", type=float, default=25.0, help="Blending: age sigmoid scale (ms).")
    ap.add_argument("--blend-diff0-bps", type=float, default=5.0, help="Blending: diff (bps) where we start correcting toward cross-venue.")
    ap.add_argument("--blend-diff-scale-bps", type=float, default=2.0, help="Blending: diff sigmoid scale (bps).")
    ap.add_argument("--blend-max-w", type=float, default=0.8, help="Blending: maximum weight on cross-venue common forecast.")
    ap.add_argument("--vel-halflife-s", type=float, default=0.2, help="Cross-venue common momentum EWMA half-life (s).")
    ap.add_argument("--vel-cap-bps-per-s", type=float, default=200.0, help="Cap common momentum magnitude (bps/s).")
    ap.add_argument("--ecm-tau-ms", type=float, default=0.0, help="ECM mean-reversion time constant in ms (0 => fit on in-sample).")
    ap.add_argument(
        "--horizon-model",
        type=str,
        default="linear",
        choices=["preemptive", "blend", "linear", "ecm"],
        help="How to build the t+h forecast from filtered signals.",
    )
    ap.add_argument("--mom-lag-ms", type=float, default=100.0, help="Momentum feature lag for horizon model (ms).")
    ap.add_argument("--mom-lookback", type=int, default=10, help="Number of lagged momentum time periods to use in the horizon linear model.")
    ap.add_argument("--linear-ridge-l2", type=float, default=1e-5, help="Linear horizon model L2 ridge penalty (0 disables).")
    ap.add_argument("--move-cap-bps", type=float, default=30.0, help="Cap the predicted log-move magnitude (bps) for stability.")
    ap.add_argument("--common-source", type=str, default="median", choices=["median", "kalman"], help="Cross-venue common mid source.")
    ap.add_argument("--common-disp0-bps", type=float, default=3.0, help="Downweight divergence when cross-venue MAD is high (bps).")
    ap.add_argument("--base-source", type=str, default="lighter_ffill", choices=["nowcast", "lighter_ffill"], help="Base 'current Lighter price' used by the horizon model.")
    ap.add_argument("--snap-diff0-bps", type=float, default=5.0, help="Nowcast snap: start snapping when |consensus-nowcast| exceeds this (bps).")
    ap.add_argument("--snap-diff-scale-bps", type=float, default=1.0, help="Nowcast snap: sigmoid scale for diff (bps).")
    ap.add_argument("--snap-disp-max-bps", type=float, default=1.5, help="Nowcast snap: only snap strongly when cross-venue dispersion (MAD) is below this (bps).")
    ap.add_argument("--snap-disp-scale-bps", type=float, default=1.0, help="Nowcast snap: sigmoid scale for dispersion (bps).")
    ap.add_argument("--snap-max-w", type=float, default=0.9, help="Nowcast snap: maximum snap weight toward consensus.")
    ap.add_argument("--snap-age0-ms", type=float, default=20.0, help="Nowcast snap: start snapping when Lighter age exceeds this (ms).")
    ap.add_argument("--snap-age-scale-ms", type=float, default=10.0, help="Nowcast snap: sigmoid scale for Lighter age (ms).")
    ap.add_argument("--snap-min-n", type=int, default=3, help="Nowcast snap: require at least this many non-Lighter venues in the median.")
    ap.add_argument("--lighter-bias-halflife-s", type=float, default=1.0, help="Preemptive: EWMA half-life for (log lighter - log common) bias.")
    ap.add_argument("--lighter-bias-cap-bps", type=float, default=10.0, help="Preemptive: clamp bias magnitude (bps) for robustness.")
    ap.add_argument("--plot-other-exchanges", action=argparse.BooleanOptionalAction, default=True, help="Overlay other exchanges' book mids on the plot.")
    ap.add_argument("--mpl-backend", type=str, default=None, help="Force a matplotlib backend (e.g. MacOSX, TkAgg, QtAgg).")
    ap.add_argument("--open-out", action=argparse.BooleanOptionalAction, default=False, help="Open the saved plot file after writing it.")
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
        blend_age0_ms=float(args.blend_age0_ms),
        blend_age_scale_ms=float(args.blend_age_scale_ms),
        blend_diff0_bps=float(args.blend_diff0_bps),
        blend_diff_scale_bps=float(args.blend_diff_scale_bps),
        blend_max_w=float(args.blend_max_w),
        vel_halflife_s=float(args.vel_halflife_s),
        vel_cap_bps_per_s=float(args.vel_cap_bps_per_s),
        ecm_tau_ms=float(args.ecm_tau_ms),
        common_source=str(args.common_source),
        common_disp0_bps=float(args.common_disp0_bps),
        snap_diff0_bps=float(args.snap_diff0_bps),
        snap_diff_scale_bps=float(args.snap_diff_scale_bps),
        snap_disp_max_bps=float(args.snap_disp_max_bps),
        snap_disp_scale_bps=float(args.snap_disp_scale_bps),
        snap_max_w=float(args.snap_max_w),
        snap_age0_ms=float(args.snap_age0_ms),
        snap_age_scale_ms=float(args.snap_age_scale_ms),
        snap_min_n=int(args.snap_min_n),
        lighter_bias_halflife_s=float(args.lighter_bias_halflife_s),
        lighter_bias_cap_bps=float(args.lighter_bias_cap_bps),
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
    if tuning.horizon_ms and tuning.horizon_ms > 0:
        if args.horizon_model == "preemptive":
            preds = apply_horizon_preemptive_forecast(
                preds,
                horizon_ms=tuning.horizon_ms,
                common_source=tuning.common_source,
                vel_halflife_s=tuning.vel_halflife_s,
                vel_cap_bps_per_s=tuning.vel_cap_bps_per_s,
                bias_halflife_s=tuning.lighter_bias_halflife_s,
                bias_cap_bps=tuning.lighter_bias_cap_bps,
                min_common_n=int(tuning.snap_min_n),
            )
            print(
                f"Horizon preemptive: common={tuning.common_source}  "
                f"bias_hl={tuning.lighter_bias_halflife_s:g}s  vel_hl={tuning.vel_halflife_s:g}s"
            )
        elif args.horizon_model == "ecm":
            preds.attrs["blend_age0_ms"] = float(tuning.blend_age0_ms)
            preds.attrs["blend_age_scale_ms"] = float(tuning.blend_age_scale_ms)
            preds, tau_ms = apply_ecm_horizon_forecast(
                preds,
                split_ns=split_ns,
                horizon_ms=tuning.horizon_ms,
                diff0_bps=tuning.blend_diff0_bps,
                diff_scale_bps=tuning.blend_diff_scale_bps,
                max_w=tuning.blend_max_w,
                tau_ms=tuning.ecm_tau_ms,
                common_source=tuning.common_source,
                fit_tau=True,
            )
            print(
                f"ECM tau_ms={tau_ms:.1f}  max_w={tuning.blend_max_w:g}  "
                f"diff0_bps={tuning.blend_diff0_bps:g}  diff_scale_bps={tuning.blend_diff_scale_bps:g}"
            )
        elif args.horizon_model == "linear":
            beta, rmse_log = fit_horizon_linear_model(
                preds,
                split_ns=split_ns,
                horizon_ms=tuning.horizon_ms,
                mom_lag_ms=float(args.mom_lag_ms),
                mom_lookback=int(args.mom_lookback),
                common_source=tuning.common_source,
                common_disp0_bps=tuning.common_disp0_bps,
                base_source=str(args.base_source),
                ridge_l2=float(args.linear_ridge_l2),
            )
            preds = apply_horizon_linear_forecast(
                preds,
                beta=beta,
                horizon_ms=tuning.horizon_ms,
                mom_lag_ms=float(args.mom_lag_ms),
                mom_lookback=int(args.mom_lookback),
                move_cap_bps=float(args.move_cap_bps),
                common_source=tuning.common_source,
                common_disp0_bps=tuning.common_disp0_bps,
                base_source=str(args.base_source),
            )
            lookback = int(max(int(args.mom_lookback), 1))
            mom_betas = " ".join([f"b_mom{k+1}={beta[k]:.3f}" for k in range(lookback)])
            b_div = float(beta[lookback]) if len(beta) > lookback else float("nan")
            b0 = float(beta[lookback + 1]) if len(beta) > lookback + 1 else float("nan")
            print(f"Horizon linear model: {mom_betas} b_div={b_div:.3f} intercept={b0:.3g}  train_rmse_log={rmse_log:.4g}")
        else:
            preds = apply_horizon_blend_forecast(
                preds,
                horizon_ms=tuning.horizon_ms,
                diff0_bps=tuning.blend_diff0_bps,
                diff_scale_bps=tuning.blend_diff_scale_bps,
                age0_ms=tuning.blend_age0_ms,
                age_scale_ms=tuning.blend_age_scale_ms,
                max_w=tuning.blend_max_w,
                vel_halflife_s=tuning.vel_halflife_s,
                vel_cap_bps_per_s=tuning.vel_cap_bps_per_s,
                common_source=tuning.common_source,
                common_disp0_bps=tuning.common_disp0_bps,
                base_source=str(args.base_source),
                min_common_n=int(tuning.snap_min_n),
            )
            print(
                f"Horizon blend: max_w={tuning.blend_max_w:g}  "
                f"age0={tuning.blend_age0_ms:g}ms  diff0={tuning.blend_diff0_bps:g}bps  "
                f"common_disp0={tuning.common_disp0_bps:g}bps"
            )

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
        title=f"Lighter mid prediction (Kalman state-space, +{tuning.horizon_ms:g}ms)",
        horizon_ms=tuning.horizon_ms,
        plot_other_exchanges=bool(args.plot_other_exchanges),
        mpl_backend=args.mpl_backend,
        show=bool(args.show),
    )
    print(f"Wrote plot: {args.out}")
    if bool(args.open_out):
        _open_file(Path(args.out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
