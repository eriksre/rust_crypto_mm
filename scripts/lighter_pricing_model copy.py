"""
Predict Lighter mid-price using a (scalar) state-space Kalman filter.

Data source: logs/lighter_activity.csv (event stream across venues).

Model (in log-price space, centered by a constant mu):
  x_t = exp(k * dt) * x_{t-1} + w_t,         w_t ~ N(0, q_per_sec * dt)
  z_t = (log(price_t) - mu) = x_t + b_s + v, v ~ N(0, r_s)  for stream s

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
    bias_by_stream: Dict[str, float]
    r_by_stream: Dict[str, float]
    ref_stream: str


def _ns_to_datetime(ns: np.ndarray) -> np.ndarray:
    return pd.to_datetime(ns, unit="ns", utc=True).to_numpy()


def _safe_log_prices(prices: np.ndarray) -> np.ndarray:
    prices = np.asarray(prices, dtype=np.float64)
    prices = np.where(prices > 0, prices, np.nan)
    return np.log(prices)


def _event_time_ns(df: pd.DataFrame) -> pd.Series:
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


def load_observations(
    csv_path: str | Path,
    *,
    max_rows: int | None = None,
    include_virtual_ob_prices: bool = True,
) -> pd.DataFrame:
    usecols = [
        "ts_ns",
        "exchange",
        "feed",
        "event_type",
        "source_engine_ts_ns",
        "source_system_ts_ns",
        "price",
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
    ]
    dtypes = {
        "ts_ns": "int64",
        "exchange": "category",
        "feed": "category",
        "event_type": "category",
        "source_engine_ts_ns": "float64",
        "source_system_ts_ns": "float64",
        "price": "float64",
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
    }

    df = pd.read_csv(
        csv_path,
        usecols=usecols,
        dtype=dtypes,
        nrows=max_rows,
        low_memory=False,
    )
    df = df[df["event_type"] == MARKET_EVENT_TYPE].copy()

    df["t_ns"] = _event_time_ns(df)
    df.sort_values("t_ns", inplace=True, kind="mergesort")

    df["mid"] = df["price"]
    df["mid"] = df["mid"].where(~df["mid"].isna(), _mid_from_top_of_book(df))
    df["micro"] = _microprice(df)
    df["vwap5"] = _vwap_mid_top5(df)

    # Primary observation: mid for bbo/orderbook; trade price for trades (already in df["price"]).
    is_trade = df["feed"].astype(str) == "trade"
    df["obs_price"] = np.where(is_trade.to_numpy(), df["price"].to_numpy(), df["mid"].to_numpy())

    out = df[["t_ns", "exchange", "feed", "obs_price", "mid", "micro", "vwap5"]].copy()
    out["stream"] = out["exchange"].astype(str) + ":" + out["feed"].astype(str)
    out["is_target"] = (out["exchange"].astype(str) == TARGET_EXCHANGE) & (
        out["feed"].astype(str) == TARGET_FEED
    )

    out = out[np.isfinite(out["obs_price"]) & (out["obs_price"] > 0)].copy()

    if not include_virtual_ob_prices:
        return out[["t_ns", "stream", "obs_price", "mid", "exchange", "feed", "is_target"]]

    # Add "virtual" observations derived from top-of-book / top-5 levels to use orderbook depth info.
    # Avoid adding virtual obs for Lighter itself to prevent leaking the mid from the same message.
    not_lighter = out["exchange"].astype(str) != TARGET_EXCHANGE
    is_booky = out["feed"].astype(str).isin(["orderbook", "bbo"])

    virtual_frames: List[pd.DataFrame] = []
    for name, col in [("micro", "micro"), ("vwap5", "vwap5")]:
        v = out.loc[not_lighter & is_booky, ["t_ns", "exchange", "feed", "mid", "is_target", col]].copy()
        v.rename(columns={col: "obs_price"}, inplace=True)
        v = v[np.isfinite(v["obs_price"]) & (v["obs_price"] > 0)]
        v["stream"] = v["exchange"].astype(str) + f":{name}"
        virtual_frames.append(v[["t_ns", "stream", "obs_price", "mid", "exchange", "feed", "is_target"]])

    if virtual_frames:
        out = pd.concat(
            [out[["t_ns", "stream", "obs_price", "mid", "exchange", "feed", "is_target"]]]
            + virtual_frames,
            ignore_index=True,
        )
        out.sort_values("t_ns", inplace=True, kind="mergesort")

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

    global_r = float(np.nanvar(z_obs[np.isfinite(z_obs)], ddof=1))
    global_r = max(global_r, 1e-6)

    bias_by_stream = {s: 0.0 for s in stream_names}
    r_by_stream = {s: global_r for s in stream_names}
    k_per_sec = 0.0  # start as random-walk-ish; EM will pull k negative if there is mean reversion
    q_per_sec = 1e-6

    exclude_none = np.zeros(len(z_obs), dtype=bool)

    for _ in range(em_iters):
        params = KalmanParams(
            mu_log=mu_log,
            k_per_sec=k_per_sec,
            q_per_sec=q_per_sec,
            bias_by_stream=dict(bias_by_stream),
            r_by_stream=dict(r_by_stream),
            ref_stream=ref_stream,
        )

        x_pred, p_pred, x_filt, p_filt, phi_dt = _kalman_filter(
            t_sec=t_sec,
            z_obs=z_obs,
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
            resid = z_obs[m] - x_smooth[m]
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
            resid = z_obs[m] - x_smooth[m] - bias_by_stream[s]
            r = float(np.nanvar(resid, ddof=1))
            r_by_stream[s] = float(np.clip(r, 1e-10, 1e-1))

        # Update AR(1) + process noise from smoothed state.
        k_per_sec = _estimate_k_from_smoothed(x_smooth, dt[1:])
        q_per_sec = _estimate_q_from_smoothed(x_smooth, dt[1:], k_per_sec)

    fitted = KalmanParams(
        mu_log=mu_log,
        k_per_sec=k_per_sec,
        q_per_sec=q_per_sec,
        bias_by_stream=bias_by_stream,
        r_by_stream=r_by_stream,
        ref_stream=ref_stream,
    )
    return fitted, split_ns


def run_predictions(obs: pd.DataFrame, params: KalmanParams) -> pd.DataFrame:
    obs = obs.sort_values("t_ns", kind="mergesort").reset_index(drop=True)

    # Build stream index for all streams seen in obs; unseen streams get default r and bias=0.
    t_ns = obs["t_ns"].to_numpy(dtype=np.int64)
    t_sec = (t_ns - t_ns[0]) / 1e9

    y_log = _safe_log_prices(obs["obs_price"].to_numpy(dtype=np.float64))
    z_obs = y_log - params.mu_log

    streams = obs["stream"].astype(str).to_numpy()
    is_target = obs["is_target"].to_numpy(dtype=bool)
    is_target_stream = streams == f"{TARGET_EXCHANGE}:{TARGET_FEED}"
    is_target_obs = is_target & is_target_stream

    biases = params.bias_by_stream
    r_map = params.r_by_stream
    # If we didn't learn a stream's measurement noise, treat it as much noisier than typical.
    med_r = float(np.median(list(r_map.values()))) if r_map else 1e-4
    default_r = max(10.0 * med_r, 1e-10)

    k = min(params.k_per_sec, 0.0)
    q_per_sec = max(params.q_per_sec, 0.0)

    # Online, single-pass filter: at target instants record pre-update prediction, then update state.
    n = len(obs)
    x_post = np.zeros(n, dtype=np.float64)
    p_post = np.zeros(n, dtype=np.float64)
    pred_mid_post = np.zeros(n, dtype=np.float64)
    pred_mid_post_lo = np.zeros(n, dtype=np.float64)
    pred_mid_post_hi = np.zeros(n, dtype=np.float64)

    pred_mid_pre = np.full(n, np.nan, dtype=np.float64)
    pred_mid_pre_lo = np.full(n, np.nan, dtype=np.float64)
    pred_mid_pre_hi = np.full(n, np.nan, dtype=np.float64)

    x = 0.0
    p = 1.0
    last_t = float(t_sec[0])
    for i in range(n):
        dt = float(t_sec[i] - last_t) if i > 0 else 0.0
        dt = max(dt, 0.0)
        phi = math.exp(k * dt) if dt > 0 else 1.0
        q = q_per_sec * (dt if dt > 0 else 0.0)

        # Time update.
        x = phi * x
        p = (phi * phi) * p + q

        # "No-leak" prediction recorded right before consuming the contemporaneous lighter mid update.
        if bool(is_target_obs[i]):
            pred_log_pre = params.mu_log + x
            pred_mid_pre[i] = math.exp(pred_log_pre)
            s = math.sqrt(max(p, 0.0))
            pred_mid_pre_lo[i] = math.exp(pred_log_pre - 2.0 * s)
            pred_mid_pre_hi[i] = math.exp(pred_log_pre + 2.0 * s)

        # Measurement update (always).
        s_name = streams[i]
        b = float(biases.get(s_name, 0.0))
        r = float(r_map.get(s_name, default_r))
        r = max(r, 1e-12)
        y = float(z_obs[i] - b)
        s = p + r
        k_gain = p / s
        x = x + k_gain * (y - x)
        p = (1.0 - k_gain) * p

        x_post[i] = x
        p_post[i] = p
        pred_log_post = params.mu_log + x
        pred_mid_post[i] = math.exp(pred_log_post)
        s2 = math.sqrt(max(p, 0.0))
        pred_mid_post_lo[i] = math.exp(pred_log_post - 2.0 * s2)
        pred_mid_post_hi[i] = math.exp(pred_log_post + 2.0 * s2)
        last_t = float(t_sec[i])

    # Actual Lighter mid (only meaningful on Lighter orderbook events), forward-filled for plotting.
    lighter_mid = np.where(
        (obs["exchange"].astype(str) == TARGET_EXCHANGE).to_numpy()
        & (obs["feed"].astype(str) == TARGET_FEED).to_numpy(),
        obs["mid"].to_numpy(dtype=np.float64),
        np.nan,
    )
    lighter_mid_ffill = pd.Series(lighter_mid).ffill().to_numpy(dtype=np.float64)

    out = obs[["t_ns", "exchange", "feed", "stream", "is_target"]].copy()
    out["pred_mid"] = pred_mid_post
    out["pred_mid_lo_2s"] = pred_mid_post_lo
    out["pred_mid_hi_2s"] = pred_mid_post_hi
    out["pred_mid_pre_target"] = pred_mid_pre
    out["pred_mid_pre_target_lo_2s"] = pred_mid_pre_lo
    out["pred_mid_pre_target_hi_2s"] = pred_mid_pre_hi
    out["x_filt_online"] = x_post
    out["p_filt_online"] = p_post
    out["lighter_mid"] = lighter_mid
    out["lighter_mid_ffill"] = lighter_mid_ffill
    out["t_dt"] = _ns_to_datetime(out["t_ns"].to_numpy(dtype=np.int64))
    return out


def _rmse(a: np.ndarray, b: np.ndarray) -> float:
    m = np.isfinite(a) & np.isfinite(b)
    if m.sum() == 0:
        return float("nan")
    return float(np.sqrt(np.mean((a[m] - b[m]) ** 2)))


def plot_results(
    preds: pd.DataFrame,
    *,
    split_ns: int,
    out_path: str | Path,
    title: str,
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

    ax0.plot(p["t_dt"], p["pred_mid"], label="Kalman predicted Lighter mid (filtered)", linewidth=1.2)
    ax0.fill_between(
        p["t_dt"],
        p["pred_mid_lo_2s"],
        p["pred_mid_hi_2s"],
        color="C0",
        alpha=0.12,
        linewidth=0,
        label="±2σ (log-space)",
    )
    ax0.plot(p["t_dt"], p["lighter_mid_ffill"], label="Lighter mid (ffill)", linewidth=1.0, alpha=0.8)

    split_dt = pd.to_datetime(split_ns, unit="ns", utc=True)
    ax0.axvline(split_dt, color="k", linestyle="--", linewidth=1.0, alpha=0.7, label="train/test split")
    ax0.set_title(title)
    ax0.set_ylabel("Price")
    ax0.grid(True, alpha=0.25)
    ax0.legend(loc="best")

    # Error evaluated only at the instants when Lighter orderbook updates arrive (no-leak prediction).
    eval_rows = preds[preds["is_target"] & preds["lighter_mid"].notna()].copy()
    eval_rows["is_test"] = eval_rows["t_ns"] > split_ns
    in_rmse = _rmse(
        eval_rows.loc[~eval_rows["is_test"], "pred_mid_pre_target"].to_numpy(),
        eval_rows.loc[~eval_rows["is_test"], "lighter_mid"].to_numpy(),
    )
    out_rmse = _rmse(
        eval_rows.loc[eval_rows["is_test"], "pred_mid_pre_target"].to_numpy(),
        eval_rows.loc[eval_rows["is_test"], "lighter_mid"].to_numpy(),
    )

    ax1.plot(eval_rows["t_dt"], eval_rows["pred_mid_pre_target"] - eval_rows["lighter_mid"], linewidth=0.8)
    ax1.axhline(0.0, color="k", linewidth=1.0, alpha=0.5)
    ax1.axvline(split_dt, color="k", linestyle="--", linewidth=1.0, alpha=0.7)
    ax1.set_ylabel("Pred - Actual")
    ax1.grid(True, alpha=0.25)
    ax1.set_title(f"RMSE @ lighter orderbook updates: in-sample={in_rmse:.4f}, out-of-sample={out_rmse:.4f}")

    fig.tight_layout()
    fig.savefig(out_path, dpi=180)
    plt.close(fig)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", type=str, default="logs/lighter_activity.csv")
    ap.add_argument("--out", type=str, default="plots/lighter_mid_kalman.png")
    ap.add_argument("--train-frac", type=float, default=DEFAULT_TRAIN_FRAC)
    ap.add_argument("--max-rows", type=int, default=None)
    ap.add_argument("--em-iters", type=int, default=6)
    ap.add_argument("--min-stream-obs", type=int, default=200)
    ap.add_argument("--no-virtual-ob", action="store_true", help="Do not add micro/vwap5 virtual observations.")
    args = ap.parse_args()

    obs = load_observations(
        args.csv,
        max_rows=args.max_rows,
        include_virtual_ob_prices=not args.no_virtual_ob,
    )
    params, split_ns = fit_kalman_params(
        obs,
        train_frac=args.train_frac,
        em_iters=args.em_iters,
        min_stream_obs=args.min_stream_obs,
    )
    preds = run_predictions(obs, params)

    # Human-readable summary.
    half_life = float("inf")
    if params.k_per_sec < 0:
        half_life = math.log(2.0) / (-params.k_per_sec)
    print(f"Fitted params: ref_stream={params.ref_stream}  k_per_sec={params.k_per_sec:.6g}  half_life_s={half_life:.3g}  q_per_sec={params.q_per_sec:.6g}")
    print(f"Streams: {len(params.r_by_stream)} (bias/var learned on train)")

    plot_results(
        preds,
        split_ns=split_ns,
        out_path=args.out,
        title="Lighter mid prediction (Kalman state-space)",
    )
    print(f"Wrote plot: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
