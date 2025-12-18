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

def _minimize_scalar_bounded(
    f,
    a: float,
    b: float,
    *,
    max_iter: int = 80,
    tol: float = 1e-8,
) -> float:
    # Golden-section search for bounded, unimodal-ish objectives.
    # This avoids a SciPy dependency for a simple 1D calibration.
    a = float(a)
    b = float(b)
    if not (np.isfinite(a) and np.isfinite(b) and b > a):
        raise ValueError("Invalid bounds for scalar minimization.")
    gr = (math.sqrt(5.0) - 1.0) / 2.0  # 0.618...
    c = b - gr * (b - a)
    d = a + gr * (b - a)
    fc = float(f(c))
    fd = float(f(d))
    for _ in range(int(max_iter)):
        if abs(b - a) <= tol * (abs(a) + abs(b) + 1.0):
            break
        if not (np.isfinite(fc) and np.isfinite(fd)):
            break
        if fc < fd:
            b, d, fd = d, c, fc
            c = b - gr * (b - a)
            fc = float(f(c))
        else:
            a, c, fc = c, d, fd
            d = a + gr * (b - a)
            fd = float(f(d))
    return float((a + b) / 2.0)


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
    common_max_age_ms: float
    snap_diff0_bps: float
    snap_diff_scale_bps: float
    snap_disp_max_bps: float
    snap_disp_scale_bps: float
    snap_max_w: float
    snap_age0_ms: float
    snap_age_scale_ms: float
    snap_min_n: int
    side_age_tau_ms: float
    dir_vscale_bps_per_s: float
    quote_half_spread_floor_bps: float
    quote_half_spread_cap_bps: float
    quote_disp0_bps: float
    quote_disp_mult: float
    quote_age0_ms: float
    quote_age_bps_per_100ms: float
    quote_unc_mult: float
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
            "bid_px_1",
            "ask_px_1",
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

    k_hat = float(_minimize_scalar_bounded(obj, -5.0, 0.0))
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
    # Default: random walk (no global mean reversion). Mean-reversion (k<0) tends to be harmful for
    # intraday drift/regimes, and we mainly care about short-horizon forecasting.
    k_per_sec = 0.0
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

        # Update process noise from smoothed state (random-walk: k=0).
        k_per_sec = 0.0
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
    bid_arr = obs["bid_px_1"].to_numpy(dtype=np.float64) if "bid_px_1" in obs.columns else np.full(n, np.nan, dtype=np.float64)
    ask_arr = obs["ask_px_1"].to_numpy(dtype=np.float64) if "ask_px_1" in obs.columns else np.full(n, np.nan, dtype=np.float64)
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
    last_lighter_bid = float("nan")
    last_lighter_ask = float("nan")
    last_lighter_spread_bps = float("nan")
    last_lighter_bid_change_t = float("nan")
    last_lighter_ask_change_t = float("nan")

    # Bias of cross-venue common price to Lighter price level (log space).
    lighter_basis_log = 0.0
    last_basis_t = float(t_sec[0])

    # Microstructure diagnostics for the nowcast.
    lighter_bid_age_ms_used = np.full(n, np.nan, dtype=np.float64)
    lighter_ask_age_ms_used = np.full(n, np.nan, dtype=np.float64)
    lighter_w_bid_used = np.full(n, np.nan, dtype=np.float64)
    lighter_w_ask_used = np.full(n, np.nan, dtype=np.float64)
    lighter_w_lighter_used = np.full(n, np.nan, dtype=np.float64)
    lighter_basis_log_used = np.full(n, np.nan, dtype=np.float64)
    dir_p_up_used = np.full(n, np.nan, dtype=np.float64)

    other_exchanges = sorted(set(exch_arr) - {TARGET_EXCHANGE})
    last_px_by_exch = {ex: float("nan") for ex in other_exchanges if ex}
    last_t_by_exch = {ex: float("nan") for ex in other_exchanges if ex}

    last_t = float(t_sec[0])
    last_z = float(z_obs[0]) if np.isfinite(z_obs[0]) else 0.0
    vol2 = 1e-8

    lighter_stream = str(params.ref_stream)
    spread_med_l = float(params.spread_median_bps.get(lighter_stream, 0.0))
    spread_mad_l = float(params.spread_mad_bps.get(lighter_stream, 1.0))
    spread_mad_l = max(spread_mad_l, 1e-6)
    half_spread_bps_l = 0.5 * max(spread_med_l, 0.0)
    side_tau_ms = max(float(tuning.side_age_tau_ms), 1e-6)
    vscale_bps_per_s = max(float(tuning.dir_vscale_bps_per_s), 1e-6)

    def _side_fresh(age_ms: float) -> float:
        if not np.isfinite(age_ms) or age_ms <= 0:
            return 1.0 if (np.isfinite(age_ms) and age_ms <= 0) else 0.0
        return float(math.exp(-age_ms / side_tau_ms))

    def _compute_lighter_fair(
        *,
        bid_px: float,
        ask_px: float,
        age_bid_ms: float,
        age_ask_ms: float,
        v_log_per_s: float,
    ) -> Tuple[float, float, float, float]:
        # Returns (fair_px, w_bid, w_ask, p_up).
        if not (np.isfinite(bid_px) and bid_px > 0) and not (np.isfinite(ask_px) and ask_px > 0):
            return float("nan"), float("nan"), float("nan"), 0.5

        v_bps_per_s = float(v_log_per_s) * 1e4
        p_up = float(_sigmoid(v_bps_per_s / vscale_bps_per_s))

        f_bid = _side_fresh(age_bid_ms) if (np.isfinite(bid_px) and bid_px > 0) else 0.0
        f_ask = _side_fresh(age_ask_ms) if (np.isfinite(ask_px) and ask_px > 0) else 0.0

        w_bid_raw = (1.0 - p_up) * f_bid
        w_ask_raw = p_up * f_ask
        w_sum = w_bid_raw + w_ask_raw
        if w_sum <= 0:
            w_bid = 1.0 if (np.isfinite(bid_px) and bid_px > 0) else 0.0
            w_ask = 1.0 if (np.isfinite(ask_px) and ask_px > 0) else 0.0
            w_sum2 = w_bid + w_ask
            if w_sum2 <= 0:
                return float("nan"), float("nan"), float("nan"), p_up
            w_bid /= w_sum2
            w_ask /= w_sum2
        else:
            w_bid = w_bid_raw / w_sum
            w_ask = w_ask_raw / w_sum

        delta = float(half_spread_bps_l) * 1e-4
        log_bid_fair = math.log(bid_px) + delta if (np.isfinite(bid_px) and bid_px > 0) else float("nan")
        log_ask_fair = math.log(ask_px) - delta if (np.isfinite(ask_px) and ask_px > 0) else float("nan")
        if np.isfinite(log_bid_fair) and np.isfinite(log_ask_fair):
            return float(math.exp(w_bid * log_bid_fair + w_ask * log_ask_fair)), float(w_bid), float(w_ask), p_up
        if np.isfinite(log_bid_fair):
            return float(math.exp(log_bid_fair)), 1.0, 0.0, p_up
        if np.isfinite(log_ask_fair):
            return float(math.exp(log_ask_fair)), 0.0, 1.0, p_up
        return float("nan"), float("nan"), float("nan"), p_up

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

        # Update a simple EWMA volatility proxy from (dz/dt)^2 (variance-per-second proxy).
        if i > 0 and np.isfinite(z_obs[i]):
            dz = float(z_obs[i] - last_z)
            dt_floor = max(float(tuning.q_dt_floor_ms) / 1000.0, 1e-6)
            dt_eff = max(dt, dt_floor)
            dz_dt = dz / dt_eff
            if tuning.vol_halflife_s > 0 and dt > 0:
                lam = 1.0 - math.exp(-dt * math.log(2.0) / tuning.vol_halflife_s)
            else:
                lam = 0.0
            vol2 = (1.0 - lam) * vol2 + lam * (dz_dt * dz_dt)
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
            # vol2 is (log-price per second)^2; convert to bps/s for a stable scale.
            vol_bps_per_s = 1e4 * math.sqrt(max(vol2, 0.0))
            vol_scale = 1.0 + tuning.vol_alpha * (vol_bps_per_s / 100.0)
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

        # Robust cross-venue reference: median of last-seen (fresh) book mids across non-Lighter venues.
        if exch_arr[i] != TARGET_EXCHANGE and bool(is_book[i]) and np.isfinite(obs_px_arr[i]) and obs_px_arr[i] > 0:
            ex = exch_arr[i]
            if ex in last_px_by_exch:
                last_px_by_exch[ex] = float(obs_px_arr[i])
                last_t_by_exch[ex] = float(t_sec[i])

        max_age_ms = float(tuning.common_max_age_ms)
        vals_list: List[float] = []
        for ex, px in last_px_by_exch.items():
            if not (np.isfinite(px) and px > 0):
                continue
            t_last = float(last_t_by_exch.get(ex, float("nan")))
            if not np.isfinite(t_last):
                continue
            if max_age_ms > 0:
                age_ms_ex = (float(t_sec[i]) - t_last) * 1000.0
                if age_ms_ex > max_age_ms:
                    continue
            vals_list.append(float(px))
        vals = np.asarray(vals_list, dtype=np.float64)
        common_n_used[i] = int(vals.size)
        if vals.size:
            med = float(np.median(vals))
            common_mid_median_used[i] = med
            if vals.size >= 3:
                mad = float(np.median(np.abs(vals - med)))
                if med > 0:
                    common_mad_bps_used[i] = (mad / med) * 1e4

        # Record common (non-Lighter) state + velocity for downstream nowcast/horizon models.
        common_now_kalman = math.exp(params.mu_log + x_c)
        common_mid_used[i] = common_now_kalman
        cap_log_per_s = float(tuning.vel_cap_bps_per_s) * 1e-4
        v_eff = float(np.clip(v_c, -cap_log_per_s, cap_log_per_s))
        common_vel_used[i] = v_eff

        # How stale is the last Lighter quote update in time?
        quote_age_ms = float("inf") if not np.isfinite(last_lighter_t) else max(0.0, (t_sec[i] - last_lighter_t) * 1000.0)
        lighter_age_ms_used[i] = quote_age_ms if np.isfinite(quote_age_ms) else float("nan")

        x_post[i] = x
        p_post[i] = p
        pred_log_post = params.mu_log + x
        pred_mid_post[i] = math.exp(pred_log_post)
        s2 = math.sqrt(max(p, 0.0))
        pred_mid_post_lo[i] = math.exp(pred_log_post - 2.0 * s2)
        pred_mid_post_hi[i] = math.exp(pred_log_post + 2.0 * s2)

        # Microstructure-aware nowcast:
        # - infer a Lighter fair using one-sided information (bid/ask) + cross-venue direction
        # - anchor to cross-venue common price (with an adaptive basis) to avoid blowups on huge spreads
        consensus = float(common_mid_median_used[i]) if np.isfinite(common_mid_median_used[i]) else float("nan")
        disp = float(common_mad_bps_used[i]) if np.isfinite(common_mad_bps_used[i]) else float("inf")
        min_n = int(max(tuning.snap_min_n, 1))
        n_ok = int(common_n_used[i]) >= min_n

        # Blend median consensus into the kalman-common anchor only when venues agree.
        anchor_common = float(common_now_kalman)
        if np.isfinite(consensus) and consensus > 0 and n_ok and np.isfinite(disp):
            w_disp = float(_sigmoid((tuning.snap_disp_max_bps - disp) / max(tuning.snap_disp_scale_bps, 1e-6)))
            anchor_common = float(math.exp(math.log(anchor_common) + w_disp * (math.log(consensus) - math.log(anchor_common))))

        # Compute a "pre-target" nowcast available immediately before a Lighter update arrives.
        if bool(is_target_obs[i]):
            age_bid_pre = float("inf") if not np.isfinite(last_lighter_bid_change_t) else max(0.0, (t_sec[i] - last_lighter_bid_change_t) * 1000.0)
            age_ask_pre = float("inf") if not np.isfinite(last_lighter_ask_change_t) else max(0.0, (t_sec[i] - last_lighter_ask_change_t) * 1000.0)
            fair_pre, w_bid_pre, w_ask_pre, p_up = _compute_lighter_fair(
                bid_px=last_lighter_bid,
                ask_px=last_lighter_ask,
                age_bid_ms=age_bid_pre,
                age_ask_ms=age_ask_pre,
                v_log_per_s=v_eff,
            )
            dir_p_up_used[i] = p_up
            if np.isfinite(fair_pre) and fair_pre > 0 and np.isfinite(anchor_common) and anchor_common > 0:
                # Update basis only on "healthy" quotes: both sides fresh-ish and spread not extreme.
                spread_now = float(last_lighter_spread_bps) if np.isfinite(last_lighter_spread_bps) else float("nan")
                z_sp = 0.0
                if np.isfinite(spread_now):
                    z_sp = max(0.0, (spread_now - spread_med_l) / spread_mad_l)
                ok_basis = n_ok and (z_sp <= 2.0) and (age_bid_pre <= 2.0 * side_tau_ms) and (age_ask_pre <= 2.0 * side_tau_ms)
                dt_b = max(0.0, float(t_sec[i] - last_basis_t))
                if ok_basis and tuning.lighter_bias_halflife_s and tuning.lighter_bias_halflife_s > 0 and dt_b > 0:
                    lam = 1.0 - math.exp(-dt_b * math.log(2.0) / tuning.lighter_bias_halflife_s)
                    target = math.log(fair_pre) - math.log(anchor_common)
                    lighter_basis_log = (1.0 - lam) * lighter_basis_log + lam * target
                    cap_bias = float(tuning.lighter_bias_cap_bps) * 1e-4
                    lighter_basis_log = float(np.clip(lighter_basis_log, -cap_bias, cap_bias))
                    last_basis_t = float(t_sec[i])

                anchor_on_lighter = float(math.exp(math.log(anchor_common) + lighter_basis_log))
                # Reliability of Lighter pre-quote: if quote is stale in time, or spread is huge, trust anchor.
                w_q = float(_sigmoid((tuning.snap_age0_ms - quote_age_ms) / max(tuning.snap_age_scale_ms, 1e-6)))
                g_sp = float(math.exp(-0.5 * max(0.0, z_sp)))
                f_side = max(_side_fresh(age_bid_pre), _side_fresh(age_ask_pre))
                w_l = float(np.clip(tuning.snap_max_w * w_q * g_sp * f_side, 0.0, 1.0))
                nowcast_mid_pre_target_used[i] = float(math.exp(math.log(anchor_on_lighter) + w_l * (math.log(fair_pre) - math.log(anchor_on_lighter))))
                lighter_bid_age_ms_used[i] = age_bid_pre
                lighter_ask_age_ms_used[i] = age_ask_pre
                lighter_w_bid_used[i] = w_bid_pre
                lighter_w_ask_used[i] = w_ask_pre
                lighter_w_lighter_used[i] = w_l
                snap_w_used[i] = w_l
                lighter_basis_log_used[i] = lighter_basis_log

        # Post-update nowcast (for plotting / continuous-time features): use latest known Lighter bid/ask,
        # but never let the stale side (huge spread) dominate.
        bid_px_now = float(last_lighter_bid)
        ask_px_now = float(last_lighter_ask)
        age_bid_now = float("inf") if not np.isfinite(last_lighter_bid_change_t) else max(0.0, (t_sec[i] - last_lighter_bid_change_t) * 1000.0)
        age_ask_now = float("inf") if not np.isfinite(last_lighter_ask_change_t) else max(0.0, (t_sec[i] - last_lighter_ask_change_t) * 1000.0)
        if bool(is_target_obs[i]):
            # Use the freshly-arrived top-of-book, but keep the per-side ages causal.
            b_new = float(bid_arr[i]) if np.isfinite(bid_arr[i]) and bid_arr[i] > 0 else float("nan")
            a_new = float(ask_arr[i]) if np.isfinite(ask_arr[i]) and ask_arr[i] > 0 else float("nan")
            if np.isfinite(b_new):
                bid_px_now = b_new
                if (not np.isfinite(last_lighter_bid)) or (b_new != last_lighter_bid):
                    age_bid_now = 0.0
            if np.isfinite(a_new):
                ask_px_now = a_new
                if (not np.isfinite(last_lighter_ask)) or (a_new != last_lighter_ask):
                    age_ask_now = 0.0

        fair_now, w_bid_now, w_ask_now, p_up_now = _compute_lighter_fair(
            bid_px=bid_px_now,
            ask_px=ask_px_now,
            age_bid_ms=age_bid_now,
            age_ask_ms=age_ask_now,
            v_log_per_s=v_eff,
        )
        if not np.isfinite(dir_p_up_used[i]):
            dir_p_up_used[i] = p_up_now
        anchor_on_lighter = float(math.exp(math.log(anchor_common) + lighter_basis_log)) if (np.isfinite(anchor_common) and anchor_common > 0) else float("nan")
        if np.isfinite(fair_now) and fair_now > 0 and np.isfinite(anchor_on_lighter) and anchor_on_lighter > 0:
            spread_now = float(last_lighter_spread_bps) if np.isfinite(last_lighter_spread_bps) else float("nan")
            if bool(is_target_obs[i]) and np.isfinite(spread_bps[i]):
                spread_now = float(spread_bps[i])
            z_sp = 0.0
            if np.isfinite(spread_now):
                z_sp = max(0.0, (spread_now - spread_med_l) / spread_mad_l)
            w_q = float(_sigmoid((tuning.snap_age0_ms - quote_age_ms) / max(tuning.snap_age_scale_ms, 1e-6)))
            g_sp = float(math.exp(-0.5 * max(0.0, z_sp)))
            f_side = max(_side_fresh(age_bid_now), _side_fresh(age_ask_now))
            w_l = float(np.clip(tuning.snap_max_w * w_q * g_sp * f_side, 0.0, 1.0))
            nowcast_mid_used[i] = float(math.exp(math.log(anchor_on_lighter) + w_l * (math.log(fair_now) - math.log(anchor_on_lighter))))
            if not bool(is_target_obs[i]):
                snap_w_used[i] = w_l
            if not np.isfinite(lighter_bid_age_ms_used[i]):
                lighter_bid_age_ms_used[i] = age_bid_now
            if not np.isfinite(lighter_ask_age_ms_used[i]):
                lighter_ask_age_ms_used[i] = age_ask_now
            if not np.isfinite(lighter_w_bid_used[i]):
                lighter_w_bid_used[i] = w_bid_now
            if not np.isfinite(lighter_w_ask_used[i]):
                lighter_w_ask_used[i] = w_ask_now
            if not np.isfinite(lighter_w_lighter_used[i]):
                lighter_w_lighter_used[i] = w_l
            if not np.isfinite(lighter_basis_log_used[i]):
                lighter_basis_log_used[i] = lighter_basis_log
        else:
            # Early in the sample we might not have a Lighter book yet; fall back to the all-stream state.
            nowcast_mid_used[i] = float(pred_mid_post[i])

        pred_mid_post_h[i] = pred_mid_post[i]

        # Track last Lighter mid for future events (after we've computed pre-target quantities).
        if bool(is_target_obs[i]) and np.isfinite(mid_arr[i]) and mid_arr[i] > 0:
            last_lighter_px = float(mid_arr[i])
            last_lighter_t = float(t_sec[i])
            if np.isfinite(bid_arr[i]) and bid_arr[i] > 0:
                b = float(bid_arr[i])
                if (not np.isfinite(last_lighter_bid)) or (b != last_lighter_bid):
                    last_lighter_bid_change_t = float(t_sec[i])
                last_lighter_bid = b
            if np.isfinite(ask_arr[i]) and ask_arr[i] > 0:
                a = float(ask_arr[i])
                if (not np.isfinite(last_lighter_ask)) or (a != last_lighter_ask):
                    last_lighter_ask_change_t = float(t_sec[i])
                last_lighter_ask = a
            if np.isfinite(spread_bps[i]):
                last_lighter_spread_bps = float(spread_bps[i])
        last_t = float(t_sec[i])

    # Actual Lighter mid (only meaningful on Lighter orderbook events), forward-filled for plotting.
    lighter_mid = np.where(
        (obs["exchange"].astype(str) == TARGET_EXCHANGE).to_numpy()
        & (obs["feed"].astype(str) == TARGET_FEED).to_numpy(),
        obs["mid"].to_numpy(dtype=np.float64),
        np.nan,
    )
    lighter_mid_ffill = pd.Series(lighter_mid).ffill().to_numpy(dtype=np.float64)

    out = obs[
        [
            "t_ns",
            "exchange",
            "feed",
            "stream",
            "is_target",
            "obs_price",
            "mid",
            "bid_px_1",
            "ask_px_1",
            "is_trade",
            "is_book",
        ]
    ].copy()
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
    out["lighter_bid_age_ms"] = lighter_bid_age_ms_used
    out["lighter_ask_age_ms"] = lighter_ask_age_ms_used
    out["lighter_w_bid"] = lighter_w_bid_used
    out["lighter_w_ask"] = lighter_w_ask_used
    out["lighter_w_lighter"] = lighter_w_lighter_used
    out["lighter_basis_log"] = lighter_basis_log_used
    out["dir_p_up"] = dir_p_up_used
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


def _rmse_bps(pred: np.ndarray, actual: np.ndarray) -> float:
    m = np.isfinite(pred) & np.isfinite(actual) & (actual > 0)
    if m.sum() == 0:
        return float("nan")
    with np.errstate(divide="ignore", invalid="ignore"):
        e_bps = (pred[m] - actual[m]) / actual[m] * 1e4
    return float(np.sqrt(np.mean(e_bps * e_bps)))


def _mae_bps(pred: np.ndarray, actual: np.ndarray) -> float:
    m = np.isfinite(pred) & np.isfinite(actual) & (actual > 0)
    if m.sum() == 0:
        return float("nan")
    with np.errstate(divide="ignore", invalid="ignore"):
        e_bps = np.abs((pred[m] - actual[m]) / actual[m]) * 1e4
    return float(np.mean(e_bps))


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

        log_tau = float(_minimize_scalar_bounded(obj, math.log(5.0), math.log(20_000.0)))
        tau_ms = float(math.exp(log_tau))

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


def add_quote_bbo(
    preds: pd.DataFrame,
    *,
    fair_col: str,
    out_bid_col: str,
    out_ask_col: str,
    half_spread_floor_bps: float,
    half_spread_cap_bps: float,
    disp0_bps: float,
    disp_mult: float,
    age0_ms: float,
    age_bps_per_100ms: float,
    unc_mult: float,
    w_col: str = "lighter_w_lighter",
    disp_col: str = "common_mad_bps",
    age_col: str = "lighter_age_ms",
    bid_px_col: str = "bid_px_1",
    ask_px_col: str = "ask_px_1",
    mid_col: str = "mid",
    bid_age_col: str = "lighter_bid_age_ms",
    ask_age_col: str = "lighter_ask_age_ms",
) -> pd.DataFrame:
    if fair_col not in preds.columns:
        preds[out_bid_col] = np.nan
        preds[out_ask_col] = np.nan
        return preds

    # Robust baseline spread: use median Lighter spread when both sides look fresh.
    base_hs = float(preds.attrs.get("base_half_spread_bps", float("nan")))
    if bid_px_col in preds.columns and ask_px_col in preds.columns and mid_col in preds.columns:
        # Only recompute if we haven't stashed it already.
        if not (np.isfinite(base_hs) and base_hs > 0):
            bid = preds[bid_px_col].to_numpy(dtype=np.float64)
            ask = preds[ask_px_col].to_numpy(dtype=np.float64)
            mid = preds[mid_col].to_numpy(dtype=np.float64)
            with np.errstate(divide="ignore", invalid="ignore"):
                sp_bps = ((ask - bid) / mid) * 1e4
            fresh = np.isfinite(sp_bps) & (sp_bps > 0) & np.isfinite(bid) & np.isfinite(ask) & (bid > 0) & (ask > 0)
            if bid_age_col in preds.columns:
                b_age = preds[bid_age_col].to_numpy(dtype=np.float64)
                fresh &= np.isfinite(b_age) & (b_age <= 50.0)
            if ask_age_col in preds.columns:
                a_age = preds[ask_age_col].to_numpy(dtype=np.float64)
                fresh &= np.isfinite(a_age) & (a_age <= 50.0)
            if int(fresh.sum()) >= 50:
                base_hs = 0.5 * float(np.nanmedian(sp_bps[fresh]))
                if np.isfinite(base_hs) and base_hs > 0:
                    preds.attrs["base_half_spread_bps"] = float(base_hs)

    if not np.isfinite(base_hs) or base_hs <= 0:
        base_hs = 0.5

    floor = max(float(half_spread_floor_bps), 0.0)
    cap = max(float(half_spread_cap_bps), floor)
    base_hs = max(base_hs, floor)

    fair = preds[fair_col].to_numpy(dtype=np.float64)
    w = preds[w_col].to_numpy(dtype=np.float64) if w_col in preds.columns else np.full(len(preds), np.nan, dtype=np.float64)
    w = np.where(np.isfinite(w), w, 0.0)
    w = np.clip(w, 0.0, 1.0)
    disp = preds[disp_col].to_numpy(dtype=np.float64) if disp_col in preds.columns else np.full(len(preds), np.nan, dtype=np.float64)
    disp = pd.Series(disp).ffill().to_numpy(dtype=np.float64)
    disp = np.where(np.isfinite(disp), disp, float("inf"))
    age_ms = preds[age_col].to_numpy(dtype=np.float64) if age_col in preds.columns else np.full(len(preds), np.nan, dtype=np.float64)
    age_ms = pd.Series(age_ms).ffill().to_numpy(dtype=np.float64)
    age_ms = np.where(np.isfinite(age_ms), age_ms, 1e9)

    # Half-spread construction (bps):
    # - start from a robust baseline
    # - widen with cross-venue dispersion (agreement proxy)
    # - widen with Lighter staleness (time since last Lighter update)
    # - widen when we don't trust the Lighter quote-side inference (1-w)
    disp0 = max(float(disp0_bps), 0.0)
    disp_mult = max(float(disp_mult), 0.0)
    age0 = max(float(age0_ms), 0.0)
    age_mult = max(float(age_bps_per_100ms), 0.0)
    unc_mult = max(float(unc_mult), 0.0)

    widen_disp = disp_mult * np.maximum(0.0, disp - disp0)
    widen_age = age_mult * (np.maximum(0.0, age_ms - age0) / 100.0)
    hs = base_hs * (1.0 + unc_mult * (1.0 - w)) + widen_disp + widen_age
    hs = np.clip(hs, floor, cap)

    ok = np.isfinite(fair) & (fair > 0)
    preds[out_bid_col] = np.nan
    preds[out_ask_col] = np.nan
    with np.errstate(over="ignore", divide="ignore", invalid="ignore"):
        log_f = np.log(np.where(ok, fair, np.nan))
        d = hs * 1e-4
        preds.loc[ok, out_bid_col] = np.exp(log_f[ok] - d[ok])
        preds.loc[ok, out_ask_col] = np.exp(log_f[ok] + d[ok])
    return preds


def print_eval_comparison(
    preds: pd.DataFrame,
    *,
    split_ns: int,
    horizon_ms: float = 0.0,
    horizon_pred_col: str = "pred_mid_pre_target_h",
    horizon_stream_col: str = "pred_mid_h",
    include_nowcast: bool = True,
    eval_min_gap_ms: float = 0.0,
    eval_move_bps: float = 0.0,
) -> None:
    def fmt(x: float, digits: int = 2) -> str:
        if not np.isfinite(x):
            return "nan"
        # Avoid "-0.00" noise.
        if abs(x) < 0.5 * (10 ** (-digits)):
            x = 0.0
        return f"{x:.{digits}f}"

    def fmt_pct(x: float, digits: int = 1) -> str:
        return "nan" if not np.isfinite(x) else f"{100.0 * x:.{digits}f}%"

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

    def summarize(name: str, pred_col: str) -> Dict[str, float]:
        in_mask = ~eval_rows["is_test"].to_numpy(dtype=bool)
        out_mask = eval_rows["is_test"].to_numpy(dtype=bool)
        pred = eval_rows[pred_col].to_numpy(dtype=np.float64)
        actual = eval_rows["lighter_mid"].to_numpy(dtype=np.float64)
        ref = np.r_[np.nan, actual[:-1]]
        return {
            "in_rmse": _rmse(pred[in_mask], actual[in_mask]),
            "out_rmse": _rmse(pred[out_mask], actual[out_mask]),
            "in_rmse_bps": _rmse_bps(pred[in_mask], actual[in_mask]),
            "out_rmse_bps": _rmse_bps(pred[out_mask], actual[out_mask]),
            "in_mae": _mae(pred[in_mask], actual[in_mask]),
            "out_mae": _mae(pred[out_mask], actual[out_mask]),
            "in_mae_bps": _mae_bps(pred[in_mask], actual[in_mask]),
            "out_mae_bps": _mae_bps(pred[out_mask], actual[out_mask]),
            "in_p95": _pctl_abs_err(pred[in_mask], actual[in_mask], 95.0),
            "out_p95": _pctl_abs_err(pred[out_mask], actual[out_mask], 95.0),
            "in_dir": _directional_accuracy(pred[in_mask], actual[in_mask], ref[in_mask]),
            "out_dir": _directional_accuracy(pred[out_mask], actual[out_mask], ref[out_mask]),
        }

    def print_block(title: str, rows: List[Tuple[str, Dict[str, float]]]) -> None:
        print(title)
        for name, m in rows:
            print(
                f"  {name}: "
                f"in RMSE={fmt(m['in_rmse'])} ({fmt(m['in_rmse_bps'], 3)}bps) "
                f"MAE={fmt(m['in_mae'])} ({fmt(m['in_mae_bps'], 3)}bps) "
                f"p95={fmt(m['in_p95'])} dir={fmt(m['in_dir'], 3)} | "
                f"out RMSE={fmt(m['out_rmse'])} ({fmt(m['out_rmse_bps'], 3)}bps) "
                f"MAE={fmt(m['out_mae'])} ({fmt(m['out_mae_bps'], 3)}bps) "
                f"p95={fmt(m['out_p95'])} dir={fmt(m['out_dir'], 3)}"
            )

    def print_improvement(label: str, baseline: Dict[str, float], model: Dict[str, float]) -> None:
        d_in = baseline["in_rmse"] - model["in_rmse"]
        d_out = baseline["out_rmse"] - model["out_rmse"]
        d_in_bps = baseline["in_rmse_bps"] - model["in_rmse_bps"]
        d_out_bps = baseline["out_rmse_bps"] - model["out_rmse_bps"]
        p_in = d_in / baseline["in_rmse"] if np.isfinite(baseline["in_rmse"]) and baseline["in_rmse"] > 0 else float("nan")
        p_out = d_out / baseline["out_rmse"] if np.isfinite(baseline["out_rmse"]) and baseline["out_rmse"] > 0 else float("nan")
        print(
            f"  Δ vs {label}: "
            f"in {fmt(d_in)} ({fmt(d_in_bps, 3)}bps, {fmt_pct(p_in)}) | "
            f"out {fmt(d_out)} ({fmt(d_out_bps, 3)}bps, {fmt_pct(p_out)})"
        )

    if include_nowcast:
        # Filtering: predict "now" at Lighter updates, using only info strictly before that update.
        model = summarize("Kalman", "pred_mid_pre_target")
        naive_prev = summarize("Naive(prev lighter mid)", "naive_prev_lighter")
        naive_ref = summarize("Naive(last non-lighter book mid)", "naive_last_ref_book")
        print_block(
            "=== Filtering @ lighter:orderbook (predict mid at t, pre-update) ===",
            [("Kalman", model), ("Naive(prev lighter mid)", naive_prev), ("Naive(last non-lighter book mid)", naive_ref)],
        )
        print_improvement("naive(prev lighter)", naive_prev, model)
        print_improvement("naive(last non-lighter)", naive_ref, model)
        print("")

    if include_nowcast and eval_move_bps and eval_move_bps > 0:
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

    if horizon_ms and horizon_ms > 0 and horizon_pred_col in eval_rows.columns:
        t_target = eval_rows["t_ns"].to_numpy(dtype=np.int64)
        mid_target = eval_rows["lighter_mid"].to_numpy(dtype=np.float64)
        future_mid = _future_value_at_or_after(t_target, mid_target, t_target + int(horizon_ms * 1e6))

        def summarize_h(pred_col: str) -> Dict[str, float]:
            in_mask = ~eval_rows["is_test"].to_numpy(dtype=bool)
            out_mask = eval_rows["is_test"].to_numpy(dtype=bool)
            pred = eval_rows[pred_col].to_numpy(dtype=np.float64)
            return {
                "in_rmse": _rmse(pred[in_mask], future_mid[in_mask]),
                "out_rmse": _rmse(pred[out_mask], future_mid[out_mask]),
                "in_rmse_bps": _rmse_bps(pred[in_mask], future_mid[in_mask]),
                "out_rmse_bps": _rmse_bps(pred[out_mask], future_mid[out_mask]),
            }

        model_h = summarize_h(horizon_pred_col)
        naive_prev_h = summarize_h("naive_prev_lighter")
        naive_ref_h = summarize_h("naive_last_ref_book")
        print(f"=== Horizon @ lighter:orderbook (predict mid at t+{horizon_ms:g}ms, pre-update) ===")
        print(
            f"  Kalman: in RMSE={fmt(model_h['in_rmse'])} ({fmt(model_h['in_rmse_bps'], 3)}bps) | "
            f"out RMSE={fmt(model_h['out_rmse'])} ({fmt(model_h['out_rmse_bps'], 3)}bps)"
        )
        print(
            f"  Naive(prev lighter mid): in RMSE={fmt(naive_prev_h['in_rmse'])} ({fmt(naive_prev_h['in_rmse_bps'], 3)}bps) | "
            f"out RMSE={fmt(naive_prev_h['out_rmse'])} ({fmt(naive_prev_h['out_rmse_bps'], 3)}bps)"
        )
        print(
            f"  Naive(last non-lighter book mid): in RMSE={fmt(naive_ref_h['in_rmse'])} ({fmt(naive_ref_h['in_rmse_bps'], 3)}bps) | "
            f"out RMSE={fmt(naive_ref_h['out_rmse'])} ({fmt(naive_ref_h['out_rmse_bps'], 3)}bps)"
        )
        print_improvement("naive(prev lighter mid)", naive_prev_h, model_h)
        print_improvement("naive(last non-lighter book mid)", naive_ref_h, model_h)
        print("")

    # Streaming horizon eval: from any time t, predict Lighter mid at t+h using only info <= t.
    if horizon_ms and horizon_ms > 0 and horizon_stream_col in preds.columns:
        horizon_ns = int(horizon_ms * 1e6)
        lighter_updates = preds[preds["lighter_mid"].notna()].copy()
        if len(lighter_updates) >= 10:
            t_l = lighter_updates["t_ns"].to_numpy(dtype=np.int64)
            m_l = lighter_updates["lighter_mid"].to_numpy(dtype=np.float64)
            q_ns = preds["t_ns"].to_numpy(dtype=np.int64) + horizon_ns
            future_mid = _future_value_at_or_after(t_l, m_l, q_ns)
            model = preds[horizon_stream_col].to_numpy(dtype=np.float64)
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
            mb_in = _rmse_bps(model[idx][in_mask], future_mid[idx][in_mask])
            mb_out = _rmse_bps(model[idx][out_mask], future_mid[idx][out_mask])
            nb_in = _rmse_bps(naive[idx][in_mask], future_mid[idx][in_mask])
            nb_out = _rmse_bps(naive[idx][out_mask], future_mid[idx][out_mask])
            d_in = n_in - m_in
            d_out = n_out - m_out
            d_in_bps = nb_in - mb_in
            d_out_bps = nb_out - mb_out
            p_in = d_in / n_in if np.isfinite(n_in) and n_in > 0 else float("nan")
            p_out = d_out / n_out if np.isfinite(n_out) and n_out > 0 else float("nan")
            print(f"=== Streaming horizon (predict mid at t+{horizon_ms:g}ms) ===")
            print(f"  Kalman: in RMSE={fmt(m_in)} ({fmt(mb_in, 3)}bps) | out RMSE={fmt(m_out)} ({fmt(mb_out, 3)}bps)")
            print(f"  Naive(ffill): in RMSE={fmt(n_in)} ({fmt(nb_in, 3)}bps) | out RMSE={fmt(n_out)} ({fmt(nb_out, 3)}bps)")
            print(f"  Δ vs naive(ffill): in {fmt(d_in)} ({fmt(d_in_bps, 3)}bps, {fmt_pct(p_in)}) | out {fmt(d_out)} ({fmt(d_out_bps, 3)}bps, {fmt_pct(p_out)})")
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
                    mkb_in = _rmse_bps(model[idx][stale & in_mask], future_mid[idx][stale & in_mask])
                    mkb_out = _rmse_bps(model[idx][stale & out_mask], future_mid[idx][stale & out_mask])
                    nkb_in = _rmse_bps(naive[idx][stale & in_mask], future_mid[idx][stale & in_mask])
                    nkb_out = _rmse_bps(naive[idx][stale & out_mask], future_mid[idx][stale & out_mask])
                    print(
                        f"  Stale(age>={thr:g}ms): "
                        f"Kalman in={fmt(mk_in)} ({fmt(mkb_in, 3)}bps) out={fmt(mk_out)} ({fmt(mkb_out, 3)}bps) | "
                        f"naive in={fmt(nk_in)} ({fmt(nkb_in, 3)}bps) out={fmt(nk_out)} ({fmt(nkb_out, 3)}bps)"
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
                    mbb_in = _rmse_bps(model[idx][big & in_mask], future_mid[idx][big & in_mask])
                    mbb_out = _rmse_bps(model[idx][big & out_mask], future_mid[idx][big & out_mask])
                    nbb_in = _rmse_bps(naive[idx][big & in_mask], future_mid[idx][big & in_mask])
                    nbb_out = _rmse_bps(naive[idx][big & out_mask], future_mid[idx][big & out_mask])
                    print(
                        f"  Big-move(|Δ|>={eval_move_bps:g}bps): "
                        f"Kalman in={fmt(mb_in)} ({fmt(mbb_in, 3)}bps) out={fmt(mb_out)} ({fmt(mbb_out, 3)}bps) | "
                        f"naive in={fmt(nb_in)} ({fmt(nbb_in, 3)}bps) out={fmt(nb_out)} ({fmt(nbb_out, 3)}bps)"
                    )
            print("")


def plot_results(
    preds: pd.DataFrame,
    *,
    split_ns: int,
    out_path: str | Path,
    title: str,
    horizon_ms: float = 0.0,
    horizon_pred_col: str = "pred_mid_pre_target_h",
    horizon_stream_col: str = "pred_mid_h",
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

    has_h = bool(horizon_ms and horizon_ms > 0) and (horizon_stream_col in p.columns) and np.isfinite(p[horizon_stream_col]).any()
    is_target_plot = p["is_target"].to_numpy(dtype=bool) if "is_target" in p.columns else np.zeros(len(p), dtype=bool)
    t_ns_plot = p["t_ns"].to_numpy(dtype=np.int64)

    def _causal_at_target(series: np.ndarray, pre_target: np.ndarray | None) -> np.ndarray:
        if pre_target is not None and np.isfinite(pre_target).any():
            out = series.copy()
            out[is_target_plot] = pre_target[is_target_plot]
            return out
        # Fall back to a 1-step causal shift.
        s = pd.Series(series).shift(1).to_numpy(dtype=np.float64)
        return np.where(is_target_plot, s, series)

    # Determine horizon tag (e.g., pred_mid_h_150 -> "150") to locate bid/ask columns.
    horizon_tag: str | None = None
    parts = str(horizon_stream_col).split("_")
    if parts and parts[-1].isdigit():
        horizon_tag = parts[-1]

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

    # Lighter actual top-of-book (step, ffilled).
    lighter_bid = np.where(is_target_plot, p["bid_px_1"].to_numpy(dtype=np.float64), np.nan) if "bid_px_1" in p.columns else np.full(len(p), np.nan, dtype=np.float64)
    lighter_ask = np.where(is_target_plot, p["ask_px_1"].to_numpy(dtype=np.float64), np.nan) if "ask_px_1" in p.columns else np.full(len(p), np.nan, dtype=np.float64)
    lighter_bid = pd.Series(lighter_bid).ffill().to_numpy(dtype=np.float64)
    lighter_ask = pd.Series(lighter_ask).ffill().to_numpy(dtype=np.float64)
    ax0.step(p["t_dt"], lighter_bid, where="post", label="Lighter bid (ffill)", linewidth=1.0, alpha=0.95, color="C0")
    ax0.step(p["t_dt"], lighter_ask, where="post", label="Lighter ask (ffill)", linewidth=1.0, alpha=0.35, color="C0")

    # Model nowcast quotes (t), plotted causally at Lighter update instants.
    if "pred_bid" in p.columns and "pred_ask" in p.columns:
        pred_bid = p["pred_bid"].to_numpy(dtype=np.float64)
        pred_ask = p["pred_ask"].to_numpy(dtype=np.float64)
        pre_bid = p["pred_bid_pre_target"].to_numpy(dtype=np.float64) if "pred_bid_pre_target" in p.columns else None
        pre_ask = p["pred_ask_pre_target"].to_numpy(dtype=np.float64) if "pred_ask_pre_target" in p.columns else None
        pred_bid_plot = _causal_at_target(pred_bid, pre_bid)
        pred_ask_plot = _causal_at_target(pred_ask, pre_ask)
        ax0.step(p["t_dt"], pred_bid_plot, where="post", label="Model bid (t)", linewidth=1.4, alpha=0.9, color="C3")
        ax0.step(p["t_dt"], pred_ask_plot, where="post", label="Model ask (t)", linewidth=1.4, alpha=0.9, color="C4")

    # Model horizon quotes (t+h) if available.
    if has_h and horizon_tag is not None:
        bid_h_col = f"pred_bid_h_{horizon_tag}"
        ask_h_col = f"pred_ask_h_{horizon_tag}"
        bid_pre_h_col = f"pred_bid_pre_target_h_{horizon_tag}"
        ask_pre_h_col = f"pred_ask_pre_target_h_{horizon_tag}"
        if bid_h_col in p.columns and ask_h_col in p.columns:
            bid_h = p[bid_h_col].to_numpy(dtype=np.float64)
            ask_h = p[ask_h_col].to_numpy(dtype=np.float64)
            bid_pre_h = p[bid_pre_h_col].to_numpy(dtype=np.float64) if bid_pre_h_col in p.columns else None
            ask_pre_h = p[ask_pre_h_col].to_numpy(dtype=np.float64) if ask_pre_h_col in p.columns else None
            bid_h_plot = _causal_at_target(bid_h, bid_pre_h)
            ask_h_plot = _causal_at_target(ask_h, ask_pre_h)
            ax0.step(p["t_dt"], bid_h_plot, where="post", label=f"Model bid (t+{horizon_ms:g}ms)", linewidth=1.3, alpha=0.9, color="C2")
            ax0.step(p["t_dt"], ask_h_plot, where="post", label=f"Model ask (t+{horizon_ms:g}ms)", linewidth=1.3, alpha=0.9, color="C8")

    # If horizon is enabled, overlay the realized Lighter bid/ask at t+h on the same time axis (t).
    if has_h:
        horizon_ns = int(horizon_ms * 1e6)
        lighter_updates = preds[preds["is_target"]][["t_ns", "bid_px_1", "ask_px_1"]].copy() if "is_target" in preds.columns else preds[preds["lighter_mid"].notna()][["t_ns", "bid_px_1", "ask_px_1"]].copy()
        if len(lighter_updates) > 2 and "bid_px_1" in lighter_updates.columns and "ask_px_1" in lighter_updates.columns:
            t_l = lighter_updates["t_ns"].to_numpy(dtype=np.int64)
            b_l = lighter_updates["bid_px_1"].to_numpy(dtype=np.float64)
            a_l = lighter_updates["ask_px_1"].to_numpy(dtype=np.float64)
            realized_bid_h = _future_value_at_or_after(t_l, b_l, t_ns_plot + horizon_ns)
            realized_ask_h = _future_value_at_or_after(t_l, a_l, t_ns_plot + horizon_ns)
            ax0.step(p["t_dt"], realized_bid_h, where="post", label=f"Realized bid at t+{horizon_ms:g}ms", linewidth=1.2, alpha=0.85, color="C7")
            ax0.step(p["t_dt"], realized_ask_h, where="post", label=f"Realized ask at t+{horizon_ms:g}ms", linewidth=1.2, alpha=0.55, color="C7")

    split_dt = pd.to_datetime(split_ns, unit="ns", utc=True)
    ax0.axvline(split_dt, color="k", linestyle="--", linewidth=1.0, alpha=0.7, label="train/test split")
    ax0.set_title(title)
    ax0.set_ylabel("Price")
    ax0.grid(True, alpha=0.25)
    ax0.legend(loc="best")

    # Error evaluated only at the instants when Lighter orderbook updates arrive (no-leak prediction).
    eval_rows = preds[preds["is_target"] & preds["lighter_mid"].notna()].copy()
    eval_rows["is_test"] = eval_rows["t_ns"] > split_ns
    horizon_pred = eval_rows[horizon_pred_col].to_numpy(dtype=np.float64) if horizon_pred_col in eval_rows.columns else np.full(len(eval_rows), np.nan, dtype=np.float64)
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
    ap.add_argument("--horizon-ms-short", type=float, default=150.0, help="Also compute a second horizon forecast at this many ms (0 disables).")
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
    ap.add_argument("--mom-lookback", type=int, default=20, help="Number of lagged momentum time periods to use in the horizon linear model.")
    ap.add_argument("--linear-ridge-l2", type=float, default=2e-5, help="Linear horizon model L2 ridge penalty (0 disables).")
    ap.add_argument("--move-cap-bps", type=float, default=30.0, help="Cap the predicted log-move magnitude (bps) for stability.")
    ap.add_argument("--common-source", type=str, default="median", choices=["median", "kalman"], help="Cross-venue common mid source.")
    ap.add_argument("--common-disp0-bps", type=float, default=3.0, help="Downweight divergence when cross-venue MAD is high (bps).")
    ap.add_argument("--common-max-age-ms", type=float, default=150.0, help="Only include non-Lighter venues with age <= this in the cross-venue median (0 disables).")
    ap.add_argument("--base-source", type=str, default="lighter_ffill", choices=["nowcast", "lighter_ffill"], help="Base 'current Lighter price' used by the horizon model.")
    ap.add_argument("--snap-diff0-bps", type=float, default=5.0, help="Nowcast snap: start snapping when |consensus-nowcast| exceeds this (bps).")
    ap.add_argument("--snap-diff-scale-bps", type=float, default=1.0, help="Nowcast snap: sigmoid scale for diff (bps).")
    ap.add_argument("--snap-disp-max-bps", type=float, default=1.5, help="Nowcast snap: only snap strongly when cross-venue dispersion (MAD) is below this (bps).")
    ap.add_argument("--snap-disp-scale-bps", type=float, default=1.0, help="Nowcast snap: sigmoid scale for dispersion (bps).")
    ap.add_argument("--snap-max-w", type=float, default=0.9, help="Nowcast snap: maximum snap weight toward consensus.")
    ap.add_argument("--snap-age0-ms", type=float, default=20.0, help="Nowcast snap: start snapping when Lighter age exceeds this (ms).")
    ap.add_argument("--snap-age-scale-ms", type=float, default=10.0, help="Nowcast snap: sigmoid scale for Lighter age (ms).")
    ap.add_argument("--snap-min-n", type=int, default=3, help="Nowcast snap: require at least this many non-Lighter venues in the median.")
    ap.add_argument("--side-age-tau-ms", type=float, default=75.0, help="Microstructure nowcast: per-side freshness time constant (ms) for bid/ask updates.")
    ap.add_argument("--dir-vscale-bps-per-s", type=float, default=50.0, help="Microstructure nowcast: direction sigmoid scale (bps/s) from cross-venue velocity.")
    ap.add_argument("--quote-half-spread-floor-bps", type=float, default=0.5, help="Quoting: minimum half-spread (bps) around the model fair.")
    ap.add_argument("--quote-half-spread-cap-bps", type=float, default=50.0, help="Quoting: cap on half-spread (bps).")
    ap.add_argument("--quote-disp0-bps", type=float, default=1.0, help="Quoting: dispersion (bps) below which we don't widen for disagreement.")
    ap.add_argument("--quote-disp-mult", type=float, default=1.0, help="Quoting: widen half-spread by disp_mult*max(0, MAD-disp0).")
    ap.add_argument("--quote-age0-ms", type=float, default=20.0, help="Quoting: Lighter age (ms) below which we don't widen for staleness.")
    ap.add_argument("--quote-age-bps-per-100ms", type=float, default=0.5, help="Quoting: widen half-spread by this many bps per extra 100ms of Lighter staleness.")
    ap.add_argument("--quote-unc-mult", type=float, default=1.0, help="Quoting: widen baseline half-spread by (1 + unc_mult*(1-w_lighter)).")
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
        common_max_age_ms=float(args.common_max_age_ms),
        snap_diff0_bps=float(args.snap_diff0_bps),
        snap_diff_scale_bps=float(args.snap_diff_scale_bps),
        snap_disp_max_bps=float(args.snap_disp_max_bps),
        snap_disp_scale_bps=float(args.snap_disp_scale_bps),
        snap_max_w=float(args.snap_max_w),
        snap_age0_ms=float(args.snap_age0_ms),
        snap_age_scale_ms=float(args.snap_age_scale_ms),
        snap_min_n=int(args.snap_min_n),
        side_age_tau_ms=float(args.side_age_tau_ms),
        dir_vscale_bps_per_s=float(args.dir_vscale_bps_per_s),
        quote_half_spread_floor_bps=float(args.quote_half_spread_floor_bps),
        quote_half_spread_cap_bps=float(args.quote_half_spread_cap_bps),
        quote_disp0_bps=float(args.quote_disp0_bps),
        quote_disp_mult=float(args.quote_disp_mult),
        quote_age0_ms=float(args.quote_age0_ms),
        quote_age_bps_per_100ms=float(args.quote_age_bps_per_100ms),
        quote_unc_mult=float(args.quote_unc_mult),
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

    # Derive "model bid/ask" quotes around the fair nowcast and horizons.
    preds = add_quote_bbo(
        preds,
        fair_col="pred_mid",
        out_bid_col="pred_bid",
        out_ask_col="pred_ask",
        half_spread_floor_bps=tuning.quote_half_spread_floor_bps,
        half_spread_cap_bps=tuning.quote_half_spread_cap_bps,
        disp0_bps=tuning.quote_disp0_bps,
        disp_mult=tuning.quote_disp_mult,
        age0_ms=tuning.quote_age0_ms,
        age_bps_per_100ms=tuning.quote_age_bps_per_100ms,
        unc_mult=tuning.quote_unc_mult,
    )
    preds = add_quote_bbo(
        preds,
        fair_col="pred_mid_pre_target",
        out_bid_col="pred_bid_pre_target",
        out_ask_col="pred_ask_pre_target",
        half_spread_floor_bps=tuning.quote_half_spread_floor_bps,
        half_spread_cap_bps=tuning.quote_half_spread_cap_bps,
        disp0_bps=tuning.quote_disp0_bps,
        disp_mult=tuning.quote_disp_mult,
        age0_ms=tuning.quote_age0_ms,
        age_bps_per_100ms=tuning.quote_age_bps_per_100ms,
        unc_mult=tuning.quote_unc_mult,
    )
    horizons_ms: List[float] = []
    for h in (float(args.horizon_ms_short), float(tuning.horizon_ms)):
        if h > 0 and not any(abs(h - h2) < 1e-9 for h2 in horizons_ms):
            horizons_ms.append(h)

    for h_ms in horizons_ms:
        tag = int(round(h_ms))
        tmp = preds
        if args.horizon_model == "preemptive":
            tmp = apply_horizon_preemptive_forecast(
                preds,
                horizon_ms=h_ms,
                common_source=tuning.common_source,
                vel_halflife_s=tuning.vel_halflife_s,
                vel_cap_bps_per_s=tuning.vel_cap_bps_per_s,
                bias_halflife_s=tuning.lighter_bias_halflife_s,
                bias_cap_bps=tuning.lighter_bias_cap_bps,
                min_common_n=int(tuning.snap_min_n),
            )
            print(
                f"Horizon preemptive (+{h_ms:g}ms): common={tuning.common_source}  "
                f"bias_hl={tuning.lighter_bias_halflife_s:g}s  vel_hl={tuning.vel_halflife_s:g}s"
            )
        elif args.horizon_model == "ecm":
            preds.attrs["blend_age0_ms"] = float(tuning.blend_age0_ms)
            preds.attrs["blend_age_scale_ms"] = float(tuning.blend_age_scale_ms)
            tmp, tau_ms = apply_ecm_horizon_forecast(
                preds,
                split_ns=split_ns,
                horizon_ms=h_ms,
                diff0_bps=tuning.blend_diff0_bps,
                diff_scale_bps=tuning.blend_diff_scale_bps,
                max_w=tuning.blend_max_w,
                tau_ms=tuning.ecm_tau_ms,
                common_source=tuning.common_source,
                fit_tau=True,
            )
            print(
                f"ECM (+{h_ms:g}ms): tau_ms={tau_ms:.1f}  max_w={tuning.blend_max_w:g}  "
                f"diff0_bps={tuning.blend_diff0_bps:g}  diff_scale_bps={tuning.blend_diff_scale_bps:g}"
            )
        elif args.horizon_model == "linear":
            beta, rmse_log = fit_horizon_linear_model(
                preds,
                split_ns=split_ns,
                horizon_ms=h_ms,
                mom_lag_ms=float(args.mom_lag_ms),
                mom_lookback=int(args.mom_lookback),
                common_source=tuning.common_source,
                common_disp0_bps=tuning.common_disp0_bps,
                base_source=str(args.base_source),
                ridge_l2=float(args.linear_ridge_l2),
            )
            tmp = apply_horizon_linear_forecast(
                preds,
                beta=beta,
                horizon_ms=h_ms,
                mom_lag_ms=float(args.mom_lag_ms),
                mom_lookback=int(args.mom_lookback),
                move_cap_bps=float(args.move_cap_bps),
                common_source=tuning.common_source,
                common_disp0_bps=tuning.common_disp0_bps,
                base_source=str(args.base_source),
            )
            lookback = int(max(int(args.mom_lookback), 1))
            mom_betas = " ".join([f"b_mom{k+1}={beta[k]:.3f}" for k in range(min(lookback, 5))])
            b_div = float(beta[lookback]) if len(beta) > lookback else float("nan")
            b0 = float(beta[lookback + 1]) if len(beta) > lookback + 1 else float("nan")
            print(
                f"Horizon linear (+{h_ms:g}ms): {mom_betas} "
                f"b_div={b_div:.3f} intercept={b0:.3g}  train_rmse_log={rmse_log:.4g}"
            )
        else:
            tmp = apply_horizon_blend_forecast(
                preds,
                horizon_ms=h_ms,
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
                f"Horizon blend (+{h_ms:g}ms): max_w={tuning.blend_max_w:g}  "
                f"age0={tuning.blend_age0_ms:g}ms  diff0={tuning.blend_diff0_bps:g}bps  "
                f"common_disp0={tuning.common_disp0_bps:g}bps"
            )

        if "pred_mid_h" in tmp.columns:
            preds[f"pred_mid_h_{tag}"] = tmp["pred_mid_h"].to_numpy(dtype=np.float64, copy=False)
        if "pred_mid_pre_target_h" in tmp.columns:
            preds[f"pred_mid_pre_target_h_{tag}"] = tmp["pred_mid_pre_target_h"].to_numpy(dtype=np.float64, copy=False)
        if "lighter_bias_log" in tmp.columns:
            preds[f"lighter_bias_log_{tag}"] = tmp["lighter_bias_log"].to_numpy(dtype=np.float64, copy=False)

        # Bid/ask around the horizon fair.
        preds = add_quote_bbo(
            preds,
            fair_col=f"pred_mid_h_{tag}",
            out_bid_col=f"pred_bid_h_{tag}",
            out_ask_col=f"pred_ask_h_{tag}",
            half_spread_floor_bps=tuning.quote_half_spread_floor_bps,
            half_spread_cap_bps=tuning.quote_half_spread_cap_bps,
            disp0_bps=tuning.quote_disp0_bps,
            disp_mult=tuning.quote_disp_mult,
            age0_ms=tuning.quote_age0_ms,
            age_bps_per_100ms=tuning.quote_age_bps_per_100ms,
            unc_mult=tuning.quote_unc_mult,
        )
        preds = add_quote_bbo(
            preds,
            fair_col=f"pred_mid_pre_target_h_{tag}",
            out_bid_col=f"pred_bid_pre_target_h_{tag}",
            out_ask_col=f"pred_ask_pre_target_h_{tag}",
            half_spread_floor_bps=tuning.quote_half_spread_floor_bps,
            half_spread_cap_bps=tuning.quote_half_spread_cap_bps,
            disp0_bps=tuning.quote_disp0_bps,
            disp_mult=tuning.quote_disp_mult,
            age0_ms=tuning.quote_age0_ms,
            age_bps_per_100ms=tuning.quote_age_bps_per_100ms,
            unc_mult=tuning.quote_unc_mult,
        )

    # Backward-compatible columns: use the "long" horizon if available.
    long_tag = int(round(float(tuning.horizon_ms)))
    if long_tag and f"pred_mid_h_{long_tag}" in preds.columns:
        preds["pred_mid_h"] = preds[f"pred_mid_h_{long_tag}"]
    if long_tag and f"pred_mid_pre_target_h_{long_tag}" in preds.columns:
        preds["pred_mid_pre_target_h"] = preds[f"pred_mid_pre_target_h_{long_tag}"]

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
    hz_str = ",".join([f"{h:g}" for h in horizons_ms]) if horizons_ms else "0"
    print(f"Streams: {len(params.r_by_stream)} (bias/var learned on train)  horizons_ms={hz_str}")

    print_eval_comparison(
        preds,
        split_ns=split_ns,
        horizon_ms=0.0,
        include_nowcast=True,
        eval_min_gap_ms=float(args.eval_min_gap_ms),
        eval_move_bps=tuning.eval_move_bps,
    )
    for h_ms in horizons_ms:
        tag = int(round(h_ms))
        pred_col = f"pred_mid_pre_target_h_{tag}"
        stream_col = f"pred_mid_h_{tag}"
        if pred_col in preds.columns and stream_col in preds.columns:
            print_eval_comparison(
                preds,
                split_ns=split_ns,
                horizon_ms=h_ms,
                horizon_pred_col=pred_col,
                horizon_stream_col=stream_col,
                include_nowcast=False,
                eval_min_gap_ms=float(args.eval_min_gap_ms),
                eval_move_bps=tuning.eval_move_bps,
            )

    def _out_with_tag(path_s: str, tag_s: str) -> str:
        p = Path(path_s)
        return str(p.with_name(f"{p.stem}_{tag_s}{p.suffix}"))

    if horizons_ms:
        for j, h_ms in enumerate(horizons_ms):
            tag = int(round(h_ms))
            pred_col = f"pred_mid_pre_target_h_{tag}"
            stream_col = f"pred_mid_h_{tag}"
            out_path = _out_with_tag(str(args.out), f"h{tag}")
            plot_results(
                preds,
                split_ns=split_ns,
                out_path=out_path,
                title=f"Lighter mid prediction (microstructure nowcast, +{h_ms:g}ms)",
                horizon_ms=h_ms,
                horizon_pred_col=pred_col,
                horizon_stream_col=stream_col,
                plot_other_exchanges=bool(args.plot_other_exchanges),
                mpl_backend=args.mpl_backend,
                show=bool(args.show) and (j == len(horizons_ms) - 1),
            )
            print(f"Wrote plot: {out_path}")
            if bool(args.open_out) and (j == len(horizons_ms) - 1):
                _open_file(Path(out_path))
    else:
        plot_results(
            preds,
            split_ns=split_ns,
            out_path=args.out,
            title="Lighter mid prediction (microstructure nowcast)",
            horizon_ms=0.0,
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
