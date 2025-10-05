#!/usr/bin/env python3
"""Evaluate fair value strategies against future Gate.io mids."""
import argparse
from pathlib import Path
from typing import Dict, Iterable, List

import numpy as np
import pandas as pd
import statsmodels.api as sm

DEFAULT_CSV = Path("/Users/eriksreinfelds/Documents/GitHub/rust_test/output.csv")
DEFAULT_LOOKAHEADS = (0.5, 1.0, 3.0)  # seconds
PLOT_RESULTS = True  # Toggle to True to always show the plot when running the script directly
EXTERNAL_EXCHANGES = ("binance", "bybit", "bitget")
EXCLUDED_REGRESSION_STRATEGIES = {"gate_trades_only", "inverse_recency_combo"}

# Toggle to reintroduce Gate.io orderbook feeds if needed.
DROP_GATE_ORDERBOOK = True
GATE_ORDERBOOK_EXCHANGES = {"gate", "gateio"}
GATE_ORDERBOOK_FEEDS = {"orderbook"}

# ---------------------------------------------------------------------------
# Helpers for derived metrics
# ---------------------------------------------------------------------------


def calc_microprice(row: pd.Series) -> float:
    bid_px = row.get("bid_px_1")
    ask_px = row.get("ask_px_1")
    bid_qty = row.get("bid_qty_1")
    ask_qty = row.get("ask_qty_1")
    if pd.isna(bid_px) or pd.isna(ask_px) or pd.isna(bid_qty) or pd.isna(ask_qty):
        return np.nan
    total_qty = bid_qty + ask_qty
    if total_qty == 0:
        return np.nan
    return (ask_px * bid_qty + bid_px * ask_qty) / total_qty


def calc_imbalance(row: pd.Series) -> float:
    bid_qty = row.get("bid_qty_1")
    ask_qty = row.get("ask_qty_1")
    if pd.isna(bid_qty) or pd.isna(ask_qty):
        return np.nan
    total = bid_qty + ask_qty
    if total == 0:
        return np.nan
    return (bid_qty - ask_qty) / total


def pivot_metric(df: pd.DataFrame, metric: str, suffix: str) -> pd.DataFrame:
    pivot = df.pivot(index="ts_ns", columns=["exchange", "feed"], values=metric)
    pivot.columns = [f"{ex}_{feed}_{suffix}" for ex, feed in pivot.columns]
    return pivot.sort_index()

# ---------------------------------------------------------------------------
# Loading & preprocessing
# ---------------------------------------------------------------------------

def load_depth_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)
    needed = {"ts_ns", "exchange", "feed", "price"}
    if not needed.issubset(df.columns):
        raise ValueError(f"CSV missing required columns: {needed - set(df.columns)}")

    numeric_cols = [c for c in df.columns if c.startswith(("bid_", "ask_", "price"))]
    for col in numeric_cols + ["ts_ns"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["ts_ns"]).copy()
    df["ts_ns"] = df["ts_ns"].astype(np.int64)
 
    df["exchange"] = df["exchange"].astype(str).str.lower().str.strip()
    df["feed"] = df["feed"].astype(str).str.lower().str.strip()
    df["col"] = df["exchange"] + "_" + df["feed"]

    mid_from_book = (df["bid_px_1"] + df["ask_px_1"]) / 2.0
    df["mid"] = np.where(mid_from_book.notna(), mid_from_book, df["price"])
    bid_px = df["bid_px_1"]
    ask_px = df["ask_px_1"]
    bid_qty = df["bid_qty_1"]
    ask_qty = df["ask_qty_1"]
    total_qty = bid_qty + ask_qty
    with np.errstate(divide="ignore", invalid="ignore"):
        df["microprice"] = (ask_px * bid_qty + bid_px * ask_qty) / total_qty
        df.loc[total_qty <= 0, "microprice"] = np.nan
        df["imbalance"] = (bid_qty - ask_qty) / total_qty
        df.loc[total_qty <= 0, "imbalance"] = np.nan
    if DROP_GATE_ORDERBOOK:
        mask = ~(
            df["exchange"].isin(GATE_ORDERBOOK_EXCHANGES)
            & df["feed"].isin(GATE_ORDERBOOK_FEEDS)
        )
        df = df.loc[mask]

    df = df.dropna(subset=["mid"]).copy()

    df = df.sort_values("ts_ns")
    df = df.drop_duplicates(subset=["ts_ns", "col"], keep="last")
    return df


def pivot_wide(df: pd.DataFrame) -> pd.DataFrame:
    wide = df.pivot(index="ts_ns", columns=["exchange", "feed"], values="mid")
    wide.columns = [f"{exchange}_{feed}" for exchange, feed in wide.columns]
    wide = wide.sort_index()
    wide["dt"] = pd.to_datetime(wide.index, unit="ns", utc=True)
    return wide


def compute_recency(raw: pd.DataFrame, index: pd.Index, columns: Iterable[str]) -> pd.DataFrame:
    recency = pd.DataFrame(index=index, columns=list(columns), dtype=float)
    index_values = index.to_numpy(dtype=np.int64)
    grouped = raw.groupby("col")["ts_ns"]

    for col in recency.columns:
        if col not in grouped.groups:
            continue
        updates = grouped.get_group(col).drop_duplicates().to_numpy()
        if updates.size == 0:
            continue
        series = pd.Series(updates, index=updates)
        last = series.reindex(index, method="ffill")
        recency[col] = (index_values - last.to_numpy()) / 1e9
    return recency


def bias_correct_series(series: pd.Series, base: pd.Series, span: int = 600) -> pd.Series:
    if base is None:
        return series
    mask = series.notna() & base.notna()
    if not mask.any():
        return series
    diff = (series - base).where(mask)
    bias = diff.ewm(span=span, adjust=False, ignore_na=True).mean()
    return series - bias


def freshness_weight(
    recency_series: pd.Series | None,
    index: pd.Index,
    max_age: float,
    exponent: float = 1.0,
) -> pd.Series:
    if recency_series is None:
        return pd.Series(0.0, index=index)
    weights = max_age - recency_series.reindex(index)
    weights = weights.clip(lower=0.0)
    weights = (weights / max_age).clip(0.0, 1.0)
    if exponent != 1.0:
        weights = weights.pow(exponent)
    return weights.fillna(0.0)


def limit_by_latency(
    weights: pd.Series,
    feed_recency: pd.Series | None,
    base_recency: pd.Series | None,
    tolerance: float,
    hard_clip: bool = True,
) -> pd.Series:
    if feed_recency is None or base_recency is None:
        return weights

    base = base_recency.reindex(weights.index)
    feed = feed_recency.reindex(weights.index)
    gap = (feed - base).clip(lower=0.0)
    gate = 1.0 - gap / tolerance
    if hard_clip:
        gate = gate.clip(lower=0.0, upper=1.0)
    gate = gate.where(gate.notna(), 1.0)
    return weights.mul(gate, fill_value=0.0)


def _accumulate_component(
    store: Dict[str, pd.Series] | None,
    name: str | None,
    index: pd.Index,
    delta: pd.Series | np.ndarray | float,
) -> None:
    """Accumulate adjustment contributions for strategy decomposition."""

    if store is None or name is None:
        return

    if isinstance(delta, pd.Series):
        contrib = delta.reindex(index).fillna(0.0).astype(float)
    else:
        contrib = pd.Series(delta, index=index, dtype=float)

    if name not in store:
        store[name] = pd.Series(0.0, index=index, dtype=float)

    store[name] = store[name].add(contrib, fill_value=0.0)


def pull_toward(
    current: pd.Series,
    signal: pd.Series | None,
    weight: pd.Series,
    *,
    component_name: str | None = None,
    components: Dict[str, pd.Series] | None = None,
) -> pd.Series:
    if signal is None:
        return current
    weight = weight.where(signal.notna(), 0.0)
    if weight.abs().sum() == 0:
        return current
    adjustment = (signal - current).mul(weight, fill_value=0.0)
    _accumulate_component(components, component_name, current.index, adjustment)
    return current.add(adjustment, fill_value=0.0)


def rolling_bias(
    series: pd.Series,
    base: pd.Series,
    time_index: pd.DatetimeIndex,
    window: str = "10s",
    min_periods: int = 10,
) -> pd.Series:
    if series.isna().all():
        return pd.Series(0.0, index=series.index)
    diff = (series - base).where(series.notna() & base.notna())
    diff_time = diff.copy()
    diff_time.index = time_index
    bias = diff_time.rolling(window=window, min_periods=min_periods).mean()
    bias.index = series.index
    return bias.fillna(0.0)


def demean_time_window(
    series: pd.Series,
    dt_index: pd.DatetimeIndex,
    window: str = "10s",
    min_periods: int = 10,
) -> pd.Series:
    if series.isna().all():
        return pd.Series(np.nan, index=series.index, dtype=float)

    time_series = pd.Series(series.to_numpy(dtype=float), index=dt_index)
    rolling_mean = time_series.rolling(window=window, min_periods=min_periods).mean()
    demeaned = time_series - rolling_mean
    return pd.Series(demeaned.to_numpy(), index=series.index, dtype=float)


def compute_demeaned_feed_strategies(
    wide: pd.DataFrame,
    window: str = "10s",
    min_periods: int = 10,
) -> Dict[str, pd.Series]:
    dt_series = wide.get("dt")
    if dt_series is None:
        dt_index = pd.to_datetime(wide.index, unit="ns", utc=True)
    else:
        dt_index = pd.DatetimeIndex(dt_series)

    skip_cols = {"dt", "gate_mid_ref"}
    skip_suffixes = ("_microprice", "_imbalance")

    demeaned_map: Dict[str, pd.Series] = {}
    for column in wide.columns:
        if column in skip_cols or column.endswith(skip_suffixes):
            continue

        series = pd.to_numeric(wide[column], errors="coerce")
        if series.notna().sum() == 0:
            continue

        demeaned = demean_time_window(series, dt_index, window=window, min_periods=min_periods)
        if demeaned.notna().sum() == 0:
            continue
        std = float(demeaned.std(skipna=True))
        if not np.isfinite(std) or std < 1e-12:
            continue

        demeaned_map[f"demeaned_{column}"] = demeaned

    return demeaned_map


def demean_series_map(
    series_map: Dict[str, pd.Series],
    dt_index: pd.DatetimeIndex,
    window: str = "10s",
    min_periods: int = 10,
) -> Dict[str, pd.Series]:
    demeaned: Dict[str, pd.Series] = {}
    for name, series in series_map.items():
        if series is None:
            continue
        numeric = pd.to_numeric(series, errors="coerce")
        if numeric.notna().sum() == 0:
            continue
        adjusted = demean_time_window(numeric, dt_index, window=window, min_periods=min_periods)
        if adjusted.notna().sum() == 0:
            continue
        std = float(adjusted.std(skipna=True))
        if not np.isfinite(std) or std < 1e-12:
            continue
        demeaned[name] = adjusted
    return demeaned


def penalize_volatility(
    series: pd.Series,
    alpha: float = 0.08,
    cap_multiplier: float = 1.4,
) -> pd.Series:
    if series is None or series.dropna().empty:
        return series

    filled = series.ffill().fillna(0.0)
    kalman_series = kalman_smooth(filled, process_var=5e-5, measurement_var=8e-4)
    blended = 0.25 * filled + 0.75 * kalman_series

    baseline = blended.ewm(alpha=alpha * 0.7, adjust=False).mean()
    deviation = (blended - baseline).abs()
    cap = deviation.ewm(span=120, adjust=False).mean() * cap_multiplier
    cap = cap.fillna(cap.median())
    positive_cap = cap[cap > 0]
    floor = positive_cap.min() if not positive_cap.empty else 1e-6
    cap = cap.replace(0.0, floor)

    limited = baseline + (blended - baseline).clip(lower=-cap, upper=cap)
    limited = kalman_smooth(limited, process_var=2e-5, measurement_var=6e-4)
    limited = limited.ewm(alpha=alpha * 0.6, adjust=False).mean()
    return limited.reindex(series.index)


def kalman_smooth(
    series: pd.Series,
    process_var: float = 1e-4,
    measurement_var: float = 1e-3,
) -> pd.Series:
    values = series.to_numpy(dtype=float)
    filtered = np.full(values.shape, np.nan, dtype=float)
    estimate = None
    error = 1.0

    for idx, value in enumerate(values):
        if np.isnan(value):
            continue

        if estimate is None:
            estimate = value
            filtered[idx] = estimate
            error = measurement_var
            continue

        error += process_var
        kalman_gain = error / (error + measurement_var)
        estimate = estimate + kalman_gain * (value - estimate)
        error = (1.0 - kalman_gain) * error
        filtered[idx] = estimate

    return pd.Series(filtered, index=series.index)


# ---------------------------------------------------------------------------
# Weighted imbalance helpers
# ---------------------------------------------------------------------------

WEIGHT_MAP: Dict[str, float] = {
    "binance": 0.4,
    "bybit": 0.3,
    "gate": 0.15,
    "bitget": 0.15,
}

BBO_ALIASES: Dict[str, List[str]] = {
    "gate": ["gate_bbo_imbalance", "gateio_bbo_imbalance"],
}

ORDER_ALIASES: Dict[str, List[str]] = {
    "gate": ["gate_orderbook_imbalance", "gateio_orderbook_imbalance"],
}

TRADE_ALIASES: Dict[str, List[str]] = {
    "gate": ["gate", "gateio"],
}


def _gather_series(
    wide: pd.DataFrame,
    column_template: str,
    aliases: Dict[str, List[str]],
) -> Dict[str, pd.Series]:
    series_map: Dict[str, pd.Series] = {}
    for exchange in WEIGHT_MAP:
        candidates = aliases.get(exchange, [column_template.format(exchange=exchange)])
        for column in candidates:
            series = wide.get(column)
            if series is None or series.dropna().empty:
                continue
            series_map[exchange] = series
            break
    return series_map


def _compute_trade_series(raw: pd.DataFrame, wide: pd.DataFrame) -> Dict[str, pd.Series]:
    if raw.empty:
        return {}
    trades = raw[raw["feed"] == "trade"].copy()
    if trades.empty:
        return {}

    trades = trades.sort_values("ts_ns")
    trades["direction"] = trades.get("direction", "").astype(str).str.lower()

    trade_series: Dict[str, pd.Series] = {}
    for exchange in WEIGHT_MAP:
        candidates = TRADE_ALIASES.get(exchange, [exchange])
        matched = trades[trades["exchange"].isin(candidates)]
        if matched.empty:
            continue
        buys = (matched["direction"] == "buy").astype(float)
        sells = (matched["direction"] == "sell").astype(float)
        buy_ewm = buys.ewm(span=50, adjust=False, ignore_na=True).mean()
        sell_ewm = sells.ewm(span=50, adjust=False, ignore_na=True).mean()
        total = buy_ewm + sell_ewm
        imbalance = (buy_ewm - sell_ewm) / (total + 1e-9)
        imbalance = imbalance.clip(-1.0, 1.0)
        series = pd.Series(imbalance.to_numpy(), index=matched["ts_ns"].to_numpy())
        series = series.reindex(wide.index, method="ffill")
        trade_series[exchange] = series
    return trade_series


def compute_weighted_imbalances(
    wide: pd.DataFrame,
    raw: pd.DataFrame,
    apply_kalman: bool = False,
) -> Dict[str, pd.Series]:
    results: Dict[str, pd.Series] = {}

    bbo_series = _gather_series(wide, "{exchange}_bbo_imbalance", BBO_ALIASES)
    if bbo_series:
        weights = pd.Series({ex: WEIGHT_MAP[ex] for ex in bbo_series})
        weighted = (
            pd.DataFrame(bbo_series).mul(weights / weights.sum(), axis=1).sum(axis=1, min_count=1)
        )
        if apply_kalman:
            weighted = kalman_smooth(weighted).combine_first(weighted)
        results["weighted_bbo_imbalance"] = weighted
    else:
        results["weighted_bbo_imbalance"] = pd.Series(np.nan, index=wide.index)

    order_series = _gather_series(wide, "{exchange}_orderbook_imbalance", ORDER_ALIASES)
    if order_series:
        weights = pd.Series({ex: WEIGHT_MAP[ex] for ex in order_series})
        weighted = (
            pd.DataFrame(order_series).mul(weights / weights.sum(), axis=1).sum(axis=1, min_count=1)
        )
        if apply_kalman:
            weighted = kalman_smooth(weighted).combine_first(weighted)
        results["weighted_orderbook_imbalance"] = weighted
    else:
        results["weighted_orderbook_imbalance"] = pd.Series(np.nan, index=wide.index)

    trade_series = _compute_trade_series(raw, wide)
    if trade_series:
        weights = pd.Series({ex: WEIGHT_MAP[ex] for ex in trade_series})
        weighted_trade = (
            pd.DataFrame(trade_series).mul(weights / weights.sum(), axis=1).sum(axis=1, min_count=1)
        )
        results["weighted_trade_imbalance"] = weighted_trade
    else:
        results["weighted_trade_imbalance"] = pd.Series(np.nan, index=wide.index)

    return results


def compute_latency_lead_features(
    wide: pd.DataFrame,
    recency: pd.DataFrame,
    gate_series: pd.Series,
) -> Dict[str, pd.Series]:
    base_rec = recency.get("gate_bbo")
    if base_rec is None:
        base_rec = recency.get("gate_orderbook")
    if base_rec is None:
        return {}

    base_rec = base_rec.reindex(gate_series.index)
    features: Dict[str, pd.Series] = {}

    for exchange in EXTERNAL_EXCHANGES:
        bbo_col = f"{exchange}_bbo"
        if bbo_col not in wide.columns:
            continue
        ext_bbo = wide[bbo_col]
        rec = recency.get(bbo_col)
        if rec is None:
            continue
        rec = rec.reindex(gate_series.index)
        lead = (base_rec - rec).clip(lower=0.0)
        lead_weight = (lead / (lead + 0.03)).clip(lower=0.0, upper=1.0)

        diff = (ext_bbo - gate_series).fillna(0.0)
        features[f"latency_lead_{exchange}_bbo_diff"] = diff.mul(lead_weight, fill_value=0.0)
        features[f"latency_lead_{exchange}_bbo_weight"] = lead_weight

    return features


def fit_multi_horizon_ensemble(
    strategies: Dict[str, pd.Series],
    targets: Dict[float, pd.Series],
    base_names: List[str],
    max_samples: int = 1_000_000,
) -> tuple[np.ndarray, float, int] | None:
    base_names = [name for name in base_names if name in strategies]
    if len(base_names) < 2:
        return None

    k = len(base_names)
    xtx = np.zeros((k + 1, k + 1), dtype=np.float64)
    xty = np.zeros(k + 1, dtype=np.float64)
    total = 0

    base_frame = pd.DataFrame({name: strategies[name] for name in base_names})

    for target in targets.values():
        df = base_frame.copy()
        df["target"] = target
        df = df.dropna()
        if df.empty:
            continue
        if len(df) > max_samples:
            step = max(len(df) // max_samples, 1)
            df = df.iloc[::step]

        X = df[base_names].to_numpy(dtype=np.float64, copy=False)
        y = df["target"].to_numpy(dtype=np.float64, copy=False)
        ones = np.ones((len(df), 1), dtype=np.float64)
        design = np.hstack([X, ones])
        xtx += design.T @ design
        xty += design.T @ y
        total += len(df)

    if total == 0:
        return None

    try:
        params = np.linalg.solve(xtx, xty)
    except np.linalg.LinAlgError:
        params = np.linalg.pinv(xtx) @ xty

    weights = params[:-1]
    intercept = float(params[-1])
    return weights, intercept, total


# ---------------------------------------------------------------------------
# Regression helpers
# ---------------------------------------------------------------------------


def run_markout_regression(
    indicators: Dict[str, pd.Series],
    current_price: pd.Series,
    future_price: pd.Series | None,
) -> tuple[str | None, pd.Series | None]:
    if future_price is None:
        return None, None

    data = pd.DataFrame({name: series for name, series in indicators.items() if series is not None})
    if data.empty:
        return None, None

    data["markout"] = (future_price - current_price).astype(float)
    data = data.dropna()
    if len(data) < max(200, len(data.columns) * 8):
        return None, None

    y = data.pop("markout").to_numpy(dtype=np.float64, copy=False)
    X = data.to_numpy(dtype=np.float64, copy=False)
    feature_names = data.columns.to_list()

    if X.size == 0:
        return None, None

    X_mean = np.nanmean(X, axis=0)
    X_std = np.nanstd(X, axis=0)
    valid_mask = np.isfinite(X_std) & (X_std > 1e-9)
    if not np.any(valid_mask):
        return None, None

    X = X[:, valid_mask]
    feature_names = [feature_names[i] for i in range(len(feature_names)) if valid_mask[i]]
    X_mean = X_mean[valid_mask]
    X_std = X_std[valid_mask]

    X_norm = (X - X_mean) / X_std
    X_norm = np.nan_to_num(X_norm, nan=0.0, posinf=0.0, neginf=0.0)

    y_mean = float(np.nanmean(y))
    y_centered = y - y_mean
    y_std = float(np.std(y_centered, ddof=1))
    if not np.isfinite(y_std) or y_std < 1e-9:
        return None, None

    n_samples, n_features = X_norm.shape
    if n_samples <= n_features + 5:
        return None, None

    y_norm = y_centered / y_std
    corr = np.abs((X_norm.T @ y_norm) / max(n_samples - 1, 1))
    feature_order = np.argsort(corr)[::-1]
    max_features = min(80, max(25, n_features))
    selected = feature_order[:max_features]
    X_norm = X_norm[:, selected]
    X_mean = X_mean[selected]
    X_std = X_std[selected]
    feature_names = [feature_names[i] for i in selected]
    corr = corr[selected]
    n_features = X_norm.shape[1]

    rng = np.random.default_rng(42)
    indices = np.arange(n_samples)
    rng.shuffle(indices)
    val_size = max(int(n_samples * 0.12), n_features * 5)
    val_size = min(val_size, n_samples // 2)
    train_idx = indices[val_size:]
    val_idx = indices[:val_size]

    if train_idx.size <= n_features:
        return None, None

    X_train = X_norm[train_idx]
    y_train = y_centered[train_idx]
    X_val = X_norm[val_idx]
    y_val = y_centered[val_idx]

    XtX = X_train.T @ X_train
    Xty = X_train.T @ y_train
    identity = np.eye(n_features, dtype=np.float64)

    alphas = np.array([
        0.0,
        1e-6,
        5e-6,
        1e-5,
        5e-5,
        1e-4,
        5e-4,
        1e-3,
        5e-3,
        1e-2,
        5e-2,
        1e-1,
    ])

    best_alpha = None
    best_coef = None
    best_val_rmse = float("inf")
    train_rmse = float("nan")
    train_r2 = float("nan")
    val_r2 = float("nan")

    for alpha in alphas:
        try:
            coef = np.linalg.solve(XtX + identity * alpha, Xty)
        except np.linalg.LinAlgError:
            coef = np.linalg.pinv(XtX + identity * alpha) @ Xty

        train_pred = X_train @ coef
        val_pred = X_val @ coef

        train_resid = y_train - train_pred
        val_resid = y_val - val_pred
        current_train_rmse = float(np.sqrt(np.mean(train_resid**2)))
        current_val_rmse = float(np.sqrt(np.mean(val_resid**2)))

        train_ss_tot = float(np.sum((y_train - y_train.mean()) ** 2))
        val_ss_tot = float(np.sum((y_val - y_val.mean()) ** 2))
        current_train_r2 = 1.0 - float(np.sum(train_resid**2)) / train_ss_tot if train_ss_tot > 1e-12 else float("nan")
        current_val_r2 = 1.0 - float(np.sum(val_resid**2)) / val_ss_tot if val_ss_tot > 1e-12 else float("nan")

        if current_val_rmse < best_val_rmse - 1e-8:
            best_val_rmse = current_val_rmse
            best_alpha = alpha
            best_coef = coef
            train_rmse = current_train_rmse
            train_r2 = current_train_r2
            val_r2 = current_val_r2

    if best_coef is None:
        return None, None

    coef_std = best_coef
    coef_orig = coef_std / X_std
    intercept = y_mean - float(np.dot(X_mean, coef_orig))

    preds_centered = X_norm @ coef_std
    preds = preds_centered + y_mean

    preds = pd.Series(preds, index=data.index)
    preds = preds.reindex(current_price.index)

    order = np.argsort(np.abs(coef_orig))[::-1]
    top_k = min(15, len(order))
    lines = [
        "Ridge-like Regression Summary",
        f"samples={n_samples:,} features={n_features} val_size={val_idx.size:,}",
        (
            f"alpha={best_alpha:.1e} train_rmse={train_rmse:.6f} val_rmse={best_val_rmse:.6f}"
            f" train_r2={train_r2:.4f} val_r2={val_r2:.4f}"
        ),
        f"intercept={intercept:+.6f}",
        "Top coefficients (original scale):",
    ]
    for idx in order[:top_k]:
        name = feature_names[idx]
        coeff = coef_orig[idx]
        corr_val = corr[idx]
        lines.append(f"  {name:40s} {coeff:+.6f}  corr={corr_val:.3f}")

    summary_text = "\n".join(lines)

    return summary_text, preds


# ---------------------------------------------------------------------------
# Strategy definitions
# ---------------------------------------------------------------------------

def get_column(df: pd.DataFrame, name: str) -> pd.Series:
    if name in df.columns:
        return df[name]
    return pd.Series(np.nan, index=df.index)


def gate_trades_only(features: pd.DataFrame, recency: pd.DataFrame) -> pd.Series:
    """Use Gate trade prints as-is, forward filling to keep a continuous view."""
    trades = get_column(features, "gate_trade")
    if trades.isna().all():
        return trades

    pred = trades.ffill()
    base = get_column(features, "gate_orderbook")
    if base.isna().all():
        base = get_column(features, "gate_bbo")
    pred = pred.combine_first(base)
    return pred


def latency_blend(
    features: pd.DataFrame,
    recency: pd.DataFrame,
    *,
    return_components: bool = False,
) -> pd.Series | tuple[pd.Series, Dict[str, pd.Series]]:
    gate_ob = get_column(features, "gate_orderbook")
    if gate_ob.isna().all():
        gate_ob = get_column(features, "gate_bbo")
    pred = gate_ob.copy()
    components: Dict[str, pd.Series] | None = {} if return_components else None

    gate_trade = get_column(features, "gate_trade")
    trade_recent = (get_column(recency, "gate_trade") <= 0.3)
    trade_mask = trade_recent & gate_trade.notna()
    trade_mix = 0.6 * gate_ob + 0.4 * gate_trade
    trade_adjustment = (trade_mix - pred).where(trade_mask, 0.0)
    pred = pred.add(trade_adjustment.fillna(0.0), fill_value=0.0)
    _accumulate_component(components, "latency_blend_trade_adjustment", pred.index, trade_adjustment)
    if components is not None:
        components["latency_blend_trade_activation"] = trade_mask.astype(float).reindex(pred.index).fillna(0.0)

    gate_bbo = get_column(features, "gate_bbo")
    bbo_recent = (get_column(recency, "gate_bbo") <= 0.4)
    bbo_mask = (~trade_mask) & bbo_recent & gate_bbo.notna()
    bbo_mix = 0.7 * gate_bbo + 0.3 * gate_ob
    bbo_adjustment = (bbo_mix - pred).where(bbo_mask, 0.0)
    pred = pred.add(bbo_adjustment.fillna(0.0), fill_value=0.0)
    _accumulate_component(components, "latency_blend_bbo_adjustment", pred.index, bbo_adjustment)
    if components is not None:
        components["latency_blend_bbo_activation"] = bbo_mask.astype(float).reindex(pred.index).fillna(0.0)

    if components is None:
        return pred
    return pred, components


def gate_trade_bbo_orderbook_combo(
    features: pd.DataFrame,
    recency: pd.DataFrame,
    *,
    return_components: bool = False,
) -> pd.Series | tuple[pd.Series, Dict[str, pd.Series]]:
    index = features.index
    gate_ob = get_column(features, "gate_orderbook")
    if gate_ob.isna().all():
        gate_ob = get_column(features, "gate_bbo")
    gate_bbo = get_column(features, "gate_bbo")
    gate_trade = get_column(features, "gate_trade").ffill()

    base = gate_ob.combine_first(gate_bbo).combine_first(gate_trade).ffill()
    components: Dict[str, pd.Series] | None = {} if return_components else None

    ob_weight = freshness_weight(recency.get("gate_orderbook"), index, max_age=0.45, exponent=1.0)
    bbo_weight = freshness_weight(recency.get("gate_bbo"), index, max_age=0.35, exponent=1.15)
    trade_weight = freshness_weight(recency.get("gate_trade"), index, max_age=0.22, exponent=1.4)

    weights = pd.DataFrame(
        {
            "orderbook": ob_weight.clip(upper=1.0),
            "bbo": bbo_weight.clip(upper=1.0),
            "trade": (trade_weight * 1.2).clip(upper=1.0),
        },
        index=index,
    )
    values = pd.DataFrame(
        {
            "orderbook": gate_ob,
            "bbo": gate_bbo,
            "trade": gate_trade,
        },
        index=index,
    )

    weight_sum = weights.sum(axis=1)
    safe_weight_sum = weight_sum.replace(0.0, np.nan)
    normalized = weights.div(safe_weight_sum, axis=0)
    normalized = normalized.where(weight_sum > 0.0, 0.0).fillna(0.0)

    order_delta = (values["orderbook"] - base) * normalized["orderbook"]
    bbo_delta = (values["bbo"] - base) * normalized["bbo"]
    trade_delta = (values["trade"] - base) * normalized["trade"]

    blended = base.add(order_delta, fill_value=0.0)
    blended = blended.add(bbo_delta, fill_value=0.0)
    blended = blended.add(trade_delta, fill_value=0.0)
    blended = blended.where(weight_sum > 0.0, base)
    blended = blended.combine_first(base).ffill()

    if components is None:
        return blended

    _accumulate_component(components, "gate_combo_orderbook_delta", index, order_delta)
    _accumulate_component(components, "gate_combo_bbo_delta", index, bbo_delta)
    _accumulate_component(components, "gate_combo_trade_delta", index, trade_delta)
    components["gate_combo_orderbook_weight"] = normalized["orderbook"].reindex(index).fillna(0.0)
    components["gate_combo_bbo_weight"] = normalized["bbo"].reindex(index).fillna(0.0)
    components["gate_combo_trade_weight"] = normalized["trade"].reindex(index).fillna(0.0)

    return blended, components


def inverse_recency_combo(
    features: pd.DataFrame,
    recency: pd.DataFrame,
    *,
    return_components: bool = False,
) -> pd.Series | tuple[pd.Series, Dict[str, pd.Series]]:
    columns = [
        "gate_orderbook",
        "gate_bbo",
        "gate_trade",
        "binance_orderbook",
        "binance_bbo",
        "bybit_orderbook",
        "bybit_bbo",
        "bitget_orderbook",
        "bitget_bbo",
    ]

    values = pd.DataFrame({col: get_column(features, col) for col in columns})
    recs = pd.DataFrame({col: get_column(recency, col) for col in columns})

    gate_ob = get_column(features, "gate_orderbook")
    if gate_ob.isna().all():
        gate_ob = get_column(features, "gate_bbo")
    for col in columns:
        if col.startswith("gate_"):
            continue
        values[col] = bias_correct_series(values[col], gate_ob)

    recs = recs.where(recs <= 3.0)
    weights = 1.0 / (recs + 0.3)
    weights = weights.where(~weights.isna(), 0.0)

    weighted_sum = (values * weights).sum(axis=1)
    weight_total = weights.sum(axis=1)
    combo = weighted_sum / weight_total

    combo = combo.where(weight_total > 0.0, gate_ob)
    combo = combo.fillna(gate_ob)

    if not return_components:
        return combo

    index = features.index
    components: Dict[str, pd.Series] = {}
    safe_total = weight_total.replace(0.0, np.nan)
    normalized = weights.div(safe_total, axis=0).where(weight_total > 0.0, 0.0).fillna(0.0)

    for col in columns:
        if col not in normalized.columns:
            continue
        delta = (values[col] - gate_ob) * normalized[col]
        comp_name = f"inverse_recency_{col}_delta"
        _accumulate_component(components, comp_name, index, delta)
        weight_name = f"inverse_recency_{col}_weight"
        components[weight_name] = normalized[col].reindex(index).fillna(0.0)

    components["inverse_recency_weight_total"] = weight_total.reindex(index).fillna(0.0)
    return combo, components


def trade_momentum(
    features: pd.DataFrame,
    recency: pd.DataFrame,
    *,
    return_components: bool = False,
) -> pd.Series | tuple[pd.Series, Dict[str, pd.Series]]:
    gate_ob = get_column(features, "gate_orderbook")
    if gate_ob.isna().all():
        gate_ob = get_column(features, "gate_bbo")
    gate_trade = get_column(features, "gate_trade")
    gate_trade_recent = get_column(recency, "gate_trade")

    trade_cols = [c for c in features.columns if c.endswith("_trade") and not c.startswith("gate_")]
    if not trade_cols:
        trade_cols = []

    trade_values = (
        pd.DataFrame({col: get_column(features, col) for col in trade_cols})
        if trade_cols
        else pd.DataFrame(index=features.index)
    )
    trade_rec = pd.DataFrame({col: get_column(recency, col) for col in trade_cols}) if trade_cols else pd.DataFrame(index=features.index)

    if not trade_cols:
        cross_trade_avg = pd.Series(np.nan, index=features.index)
        weights_sum = pd.Series(0.0, index=features.index)
    else:
        for col in trade_cols:
            trade_values[col] = bias_correct_series(trade_values[col], gate_ob)
        trade_rec = trade_rec.where(trade_rec <= 1.5)
        weights = 1.0 / (trade_rec + 0.2)
        weights = weights.where(~weights.isna(), 0.0)
        weights_sum = weights.sum(axis=1)
        cross_trade_avg = (trade_values * weights).sum(axis=1) / weights_sum
        cross_trade_avg = cross_trade_avg.where(weights_sum > 0.0)

    pred = gate_ob.copy()

    gate_trade_mask = (gate_trade_recent <= 0.2) & gate_trade.notna()
    trade_adjustment = gate_trade_mask.fillna(False) * 0.25 * (gate_trade - gate_ob)
    pred = pred.add(trade_adjustment.fillna(0.0), fill_value=0.0)

    cross_adjustment = 0.35 * (cross_trade_avg - gate_ob)
    cross_adjustment = cross_adjustment.where(cross_trade_avg.notna(), 0.0)
    pred = pred.add(cross_adjustment.fillna(0.0), fill_value=0.0)

    pred = pred.fillna(gate_ob)
    if not return_components:
        return pred

    components: Dict[str, pd.Series] = {}
    index = features.index
    _accumulate_component(components, "trade_momentum_gate_trade_delta", index, trade_adjustment)
    _accumulate_component(components, "trade_momentum_cross_trade_delta", index, cross_adjustment)
    components["trade_momentum_gate_trade_active"] = gate_trade_mask.astype(float).reindex(index).fillna(0.0)
    components["trade_momentum_cross_trade_weight_sum"] = weights_sum.reindex(index).fillna(0.0)
    return pred, components


def fusion_alpha(
    features: pd.DataFrame,
    recency: pd.DataFrame,
    *,
    return_components: bool = False,
) -> pd.Series | tuple[pd.Series, Dict[str, pd.Series]]:
    index = features.index
    base = get_column(features, "gate_mid_ref")
    if base.isna().all():
        base = get_column(features, "gate_bbo").combine_first(get_column(features, "gate_orderbook"))

    pred = base.copy()
    components: Dict[str, pd.Series] | None = {} if return_components else None

    rec_gate_bbo = recency.get("gate_bbo")
    rec_gate_ob = recency.get("gate_orderbook")
    base_rec = rec_gate_bbo if rec_gate_bbo is not None else rec_gate_ob
    base_rec = base_rec.reindex(index) if base_rec is not None else pd.Series(np.nan, index=index)

    gate_trade = get_column(features, "gate_trade")
    trade_weight = freshness_weight(recency.get("gate_trade"), index, max_age=0.20, exponent=1.4)
    trade_gain = trade_weight * 0.85
    trade_gain = limit_by_latency(trade_gain, recency.get("gate_trade"), base_rec, tolerance=0.04)
    pred = pull_toward(
        pred,
        gate_trade,
        trade_gain,
        component_name="fusion_alpha_gate_trade_pull",
        components=components,
    )
    if components is not None:
        components["fusion_alpha_gate_trade_weight"] = trade_gain.reindex(index).fillna(0.0)

    gate_micro = get_column(features, "gate_orderbook_microprice")
    if gate_micro.isna().all():
        gate_micro = get_column(features, "gate_bbo_microprice")
    micro_weight = freshness_weight(base_rec, index, max_age=0.35, exponent=1.2) * 0.55
    micro_weight = limit_by_latency(micro_weight, base_rec, base_rec, tolerance=0.06)
    pred = pull_toward(
        pred,
        gate_micro,
        micro_weight,
        component_name="fusion_alpha_gate_micro_pull",
        components=components,
    )
    if components is not None:
        components["fusion_alpha_gate_micro_weight"] = micro_weight.reindex(index).fillna(0.0)

    for exchange, coeff in (("binance", 0.70), ("bybit", 0.55), ("bitget", 0.45)):
        ext_bbo = get_column(features, f"{exchange}_bbo")
        ext_rec = recency.get(f"{exchange}_bbo")
        weights = freshness_weight(ext_rec, index, max_age=0.40, exponent=1.1)
        lead_bonus = pd.Series(0.0, index=index)
        if base_rec is not None:
            lead_bonus = ((base_rec - ext_rec.reindex(index)) if ext_rec is not None else 0.0)
            lead_bonus = lead_bonus.clip(lower=0.0) / 0.40
        weights = weights * (1.0 + 0.65 * lead_bonus.fillna(0.0))
        weights = limit_by_latency(weights, ext_rec, base_rec, tolerance=0.05)
        ext_adj = bias_correct_series(ext_bbo, base)
        bbo_gain = weights * coeff
        pred = pull_toward(
            pred,
            ext_adj,
            bbo_gain,
            component_name=f"fusion_alpha_{exchange}_bbo_pull",
            components=components,
        )
        if components is not None:
            components[f"fusion_alpha_{exchange}_bbo_weight"] = bbo_gain.reindex(index).fillna(0.0)

        ext_trade = get_column(features, f"{exchange}_trade")
        trade_rec = recency.get(f"{exchange}_trade")
        trade_weights = freshness_weight(trade_rec, index, max_age=0.25, exponent=1.3) * (coeff * 0.6)
        trade_weights = limit_by_latency(trade_weights, trade_rec, recency.get("gate_trade"), tolerance=0.045)
        ext_trade_adj = bias_correct_series(ext_trade, base)
        pred = pull_toward(
            pred,
            ext_trade_adj,
            trade_weights,
            component_name=f"fusion_alpha_{exchange}_trade_pull",
            components=components,
        )
        if components is not None:
            components[f"fusion_alpha_{exchange}_trade_weight"] = trade_weights.reindex(index).fillna(0.0)

    gate_imb = get_column(features, "gate_orderbook_imbalance")
    if gate_imb.isna().all():
        gate_imb = get_column(features, "gate_bbo_imbalance")
    imbalance_signal = gate_imb.clip(-1.0, 1.0)
    imbalance_weight = freshness_weight(base_rec, index, max_age=0.30, exponent=1.1)
    imbalance_weight = limit_by_latency(imbalance_weight, base_rec, base_rec, tolerance=0.05)
    imb_delta = imbalance_signal.fillna(0.0) * 0.0008 * imbalance_weight
    pred = pred.add(imb_delta, fill_value=0.0)
    if components is not None:
        _accumulate_component(components, "fusion_alpha_gate_imbalance_adjustment", index, imb_delta)
        components["fusion_alpha_gate_imbalance_weight"] = imbalance_weight.reindex(index).fillna(0.0)

    pred = pred.ffill().fillna(base)
    pre_smooth = pred.copy()
    pred = pred.ewm(alpha=0.25, adjust=False).mean()
    if components is not None:
        smoothing_delta = pred - pre_smooth
        _accumulate_component(components, "fusion_alpha_ewm_smoothing", index, smoothing_delta)

    pred = pred.combine_first(base).ffill()
    if components is None:
        return pred
    return pred, components


def lead_lag_edge(
    features: pd.DataFrame,
    recency: pd.DataFrame,
    *,
    return_components: bool = False,
) -> pd.Series | tuple[pd.Series, Dict[str, pd.Series]]:
    index = features.index
    base = get_column(features, "gate_mid_ref")
    if base.isna().all():
        base = get_column(features, "gate_bbo").combine_first(get_column(features, "gate_orderbook"))
    pred = base.copy()
    components: Dict[str, pd.Series] | None = {} if return_components else None

    base_rec = recency.get("gate_bbo")
    if base_rec is None:
        base_rec = recency.get("gate_orderbook")
    base_rec = base_rec.reindex(index) if base_rec is not None else pd.Series(np.nan, index=index)

    gate_trade = get_column(features, "gate_trade")
    trade_w = freshness_weight(recency.get("gate_trade"), index, max_age=0.18, exponent=1.6)
    trade_w = limit_by_latency(trade_w, recency.get("gate_trade"), base_rec, tolerance=0.04)
    trade_delta = (gate_trade - pred).where(trade_w > 0, 0.0) * trade_w * 0.95
    pred = pred.add(trade_delta.fillna(0.0), fill_value=0.0)
    if components is not None:
        _accumulate_component(components, "lead_lag_gate_trade_delta", index, trade_delta)
        components["lead_lag_gate_trade_weight"] = (trade_w * 0.95).reindex(index).fillna(0.0)

    gate_micro = get_column(features, "gate_orderbook_microprice")
    if gate_micro.isna().all():
        gate_micro = get_column(features, "gate_bbo_microprice")
    micro_w = freshness_weight(base_rec, index, max_age=0.30, exponent=1.1)
    micro_w = limit_by_latency(micro_w, base_rec, base_rec, tolerance=0.05)
    micro_delta = (gate_micro - pred).where(micro_w > 0, 0.0) * micro_w * 0.55
    pred = pred.add(micro_delta.fillna(0.0), fill_value=0.0)
    if components is not None:
        _accumulate_component(components, "lead_lag_gate_micro_delta", index, micro_delta)
        components["lead_lag_gate_micro_weight"] = (micro_w * 0.55).reindex(index).fillna(0.0)

    def apply_external(exchange: str, bbo_gain: float, trade_gain: float, max_age: float):
        nonlocal pred
        bbo_col = f"{exchange}_bbo"
        trade_col = f"{exchange}_trade"

        ext_bbo = get_column(features, bbo_col)
        if not ext_bbo.isna().all():
            ext_rec = recency.get(bbo_col)
            w = freshness_weight(ext_rec, index, max_age=max_age, exponent=1.2)
            if ext_rec is not None and base_rec is not None:
                lead_bonus = (base_rec - ext_rec.reindex(index)).clip(lower=0.0) / max_age
                w = w * (1.0 + 0.7 * lead_bonus.fillna(0.0))
            w = limit_by_latency(w, ext_rec, base_rec, tolerance=max_age * 0.18 + 0.02)
            ext_adj = bias_correct_series(ext_bbo, base)
            bbo_weights = w * bbo_gain
            bbo_delta = (ext_adj - pred) * bbo_weights
            pred = pred.add(bbo_delta.fillna(0.0), fill_value=0.0)
            if components is not None:
                _accumulate_component(components, f"lead_lag_{exchange}_bbo_delta", index, bbo_delta)
                components[f"lead_lag_{exchange}_bbo_weight"] = bbo_weights.reindex(index).fillna(0.0)

        ext_trade = get_column(features, trade_col)
        if not ext_trade.isna().all():
            ext_rec = recency.get(trade_col)
            w_trade = freshness_weight(ext_rec, index, max_age=max_age * 0.7, exponent=1.4)
            w_trade = limit_by_latency(w_trade, ext_rec, recency.get("gate_trade"), tolerance=max_age * 0.12 + 0.02)
            ext_adj = bias_correct_series(ext_trade, base)
            trade_weights = w_trade * trade_gain
            trade_delta = (ext_adj - pred) * trade_weights
            pred = pred.add(trade_delta.fillna(0.0), fill_value=0.0)
            if components is not None:
                _accumulate_component(components, f"lead_lag_{exchange}_trade_delta", index, trade_delta)
                components[f"lead_lag_{exchange}_trade_weight"] = trade_weights.reindex(index).fillna(0.0)

    apply_external("binance", bbo_gain=0.8, trade_gain=0.5, max_age=0.28)
    apply_external("bybit", bbo_gain=0.6, trade_gain=0.45, max_age=0.32)
    apply_external("bitget", bbo_gain=0.45, trade_gain=0.35, max_age=0.35)

    gate_imb = get_column(features, "gate_orderbook_imbalance")
    if gate_imb.isna().all():
        gate_imb = get_column(features, "gate_bbo_imbalance")
    imb_effect = gate_imb.clip(-1.0, 1.0).fillna(0.0) * 0.0009
    imb_weight = freshness_weight(base_rec, index, max_age=0.25, exponent=1.0)
    imb_weight = limit_by_latency(imb_weight, base_rec, base_rec, tolerance=0.05)
    imb_delta = imb_effect * imb_weight
    pred = pred + imb_delta
    if components is not None:
        _accumulate_component(components, "lead_lag_gate_imbalance_delta", index, imb_delta)
        components["lead_lag_gate_imbalance_weight"] = imb_weight.reindex(index).fillna(0.0)

    pred = pred.ffill().fillna(base)
    if components is None:
        return pred
    return pred, components


def external_latency_lead(
    features: pd.DataFrame,
    recency: pd.DataFrame,
    *,
    return_components: bool = False,
) -> pd.Series | tuple[pd.Series, Dict[str, pd.Series]]:
    index = features.index
    base = get_column(features, "gate_mid_ref")
    if base.isna().all():
        base = get_column(features, "gate_bbo").combine_first(get_column(features, "gate_orderbook"))

    pred = base.copy()
    components: Dict[str, pd.Series] | None = {} if return_components else None
    base_rec = recency.get("gate_bbo")
    if base_rec is None:
        base_rec = recency.get("gate_orderbook")
    if base_rec is not None:
        base_rec = base_rec.reindex(index)

    for exchange in EXTERNAL_EXCHANGES:
        bbo_col = f"{exchange}_bbo"
        ext_bbo = get_column(features, bbo_col)
        if not ext_bbo.isna().all():
            ext_adj = bias_correct_series(ext_bbo, base, span=420)
            rec = recency.get(bbo_col)
            weight = freshness_weight(rec, index, max_age=0.35, exponent=1.15)
            weight = limit_by_latency(weight, rec, base_rec, tolerance=0.045)
            pred = pull_toward(
                pred,
                ext_adj,
                weight * 0.55,
                component_name=f"external_latency_{exchange}_bbo_pull",
                components=components,
            )
            if components is not None:
                components[f"external_latency_{exchange}_bbo_weight"] = (weight * 0.55).reindex(index).fillna(0.0)

        trade_col = f"{exchange}_trade"
        ext_trade = get_column(features, trade_col)
        if not ext_trade.isna().all():
            trade_base = bias_correct_series(ext_trade, base, span=280)
            rec_trade = recency.get(trade_col)
            trade_w = freshness_weight(rec_trade, index, max_age=0.22, exponent=1.25)
            trade_w = limit_by_latency(trade_w, rec_trade, recency.get("gate_trade"), tolerance=0.04)
            pred = pull_toward(
                pred,
                trade_base,
                trade_w * 0.35,
                component_name=f"external_latency_{exchange}_trade_pull",
                components=components,
            )
            if components is not None:
                components[f"external_latency_{exchange}_trade_weight"] = (trade_w * 0.35).reindex(index).fillna(0.0)

    pred = pred.combine_first(base).ffill()
    if components is None:
        return pred
    return pred, components


def demeaned_lead(
    features: pd.DataFrame,
    recency: pd.DataFrame,
    *,
    return_components: bool = False,
) -> pd.Series | tuple[pd.Series, Dict[str, pd.Series]]:
    index = features.index
    time_index = pd.to_datetime(index, unit="ns")
    base = get_column(features, "gate_mid_ref")
    if base.isna().all():
        base = get_column(features, "gate_bbo").combine_first(get_column(features, "gate_orderbook"))

    pred = base.copy()
    components: Dict[str, pd.Series] | None = {} if return_components else None

    gate_bbo_rec = recency.get("gate_bbo")
    if gate_bbo_rec is None:
        gate_bbo_rec = recency.get("gate_orderbook")
    gate_trade_rec = recency.get("gate_trade")

    gate_trade = get_column(features, "gate_trade")
    if not gate_trade.isna().all():
        trade_w = freshness_weight(recency.get("gate_trade"), index, max_age=0.18, exponent=1.7)
        trade_w = trade_w.clip(upper=0.5)
        trade_w = limit_by_latency(trade_w, gate_trade_rec, gate_bbo_rec, tolerance=0.04)
        trade_delta = (gate_trade - base).where(gate_trade.notna(), 0.0) * trade_w * 0.4
        pred = pred.add(trade_delta.fillna(0.0), fill_value=0.0)
        if components is not None:
            _accumulate_component(components, "demeaned_lead_gate_trade_delta", index, trade_delta)
            components["demeaned_lead_gate_trade_weight"] = (trade_w * 0.4).reindex(index).fillna(0.0)

    gate_micro = get_column(features, "gate_orderbook_microprice")
    if gate_micro.isna().all():
        gate_micro = get_column(features, "gate_bbo_microprice")
    if not gate_micro.isna().all():
        micro_w = freshness_weight(recency.get("gate_bbo"), index, max_age=0.28, exponent=1.2)
        micro_w = micro_w.clip(upper=0.4)
        micro_w = limit_by_latency(micro_w, recency.get("gate_bbo"), gate_bbo_rec, tolerance=0.05)
        micro_delta = (gate_micro - base).where(gate_micro.notna(), 0.0) * micro_w * 0.2
        pred = pred.add(micro_delta.fillna(0.0), fill_value=0.0)
        if components is not None:
            _accumulate_component(components, "demeaned_lead_gate_micro_delta", index, micro_delta)
            components["demeaned_lead_gate_micro_weight"] = (micro_w * 0.2).reindex(index).fillna(0.0)

    exchanges = ["binance", "bybit", "bitget"]
    for exch in exchanges:
        bbo_col = f"{exch}_bbo"
        trade_col = f"{exch}_trade"

        ext_bbo = get_column(features, bbo_col)
        if not ext_bbo.isna().all():
            bias = rolling_bias(ext_bbo, base, time_index, window="10s", min_periods=15)
            ext_adj = ext_bbo - bias
            rec = recency.get(bbo_col)
            age_weight = freshness_weight(rec, index, max_age=0.32, exponent=1.2)
            if rec is not None:
                gate_rec = gate_bbo_rec
                if gate_rec is not None:
                    lead_bonus = (gate_rec.reindex(index) - rec.reindex(index)).clip(lower=0.0) / 0.32
                    age_weight = age_weight * (1.0 + 0.6 * lead_bonus.fillna(0.0))
            age_weight = age_weight.clip(upper=0.4)
            age_weight = limit_by_latency(age_weight, rec, gate_bbo_rec, tolerance=0.05)
            gain = {"binance": 0.25, "bybit": 0.2, "bitget": 0.15}.get(exch, 0.15)
            bbo_delta = (ext_adj - base).where(ext_adj.notna(), 0.0) * (age_weight * gain)
            pred = pred.add(bbo_delta.fillna(0.0), fill_value=0.0)
            if components is not None:
                _accumulate_component(components, f"demeaned_lead_{exch}_bbo_delta", index, bbo_delta)
                components[f"demeaned_lead_{exch}_bbo_weight"] = (age_weight * gain).reindex(index).fillna(0.0)

        ext_trade = get_column(features, trade_col)
        if not ext_trade.isna().all():
            bias = rolling_bias(ext_trade, base, time_index, window="12s", min_periods=10)
            ext_adj = ext_trade - bias
            rec = recency.get(trade_col)
            age_weight = freshness_weight(rec, index, max_age=0.22, exponent=1.3)
            age_weight = age_weight.clip(upper=0.35)
            age_weight = limit_by_latency(age_weight, rec, gate_trade_rec, tolerance=0.04)
            gain = {"binance": 0.2, "bybit": 0.15, "bitget": 0.1}.get(exch, 0.1)
            trade_delta = (ext_adj - base).where(ext_adj.notna(), 0.0) * (age_weight * gain)
            pred = pred.add(trade_delta.fillna(0.0), fill_value=0.0)
            if components is not None:
                _accumulate_component(components, f"demeaned_lead_{exch}_trade_delta", index, trade_delta)
                components[f"demeaned_lead_{exch}_trade_weight"] = (age_weight * gain).reindex(index).fillna(0.0)

    gate_imb = get_column(features, "gate_orderbook_imbalance")
    if gate_imb.isna().all():
        gate_imb = get_column(features, "gate_bbo_imbalance")
    if not gate_imb.isna().all():
        imb = gate_imb.clip(-1.0, 1.0).fillna(0.0)
        imb_weight = freshness_weight(recency.get("gate_bbo"), index, max_age=0.20, exponent=1.1)
        imb_weight = imb_weight.clip(upper=0.3)
        imb_weight = limit_by_latency(imb_weight, recency.get("gate_bbo"), gate_bbo_rec, tolerance=0.05)
        imb_delta = imb * 0.0004 * imb_weight
        pred = pred.add(imb_delta.fillna(0.0), fill_value=0.0)
        if components is not None:
            _accumulate_component(components, "demeaned_lead_gate_imbalance_delta", index, imb_delta)
            components["demeaned_lead_gate_imbalance_weight"] = imb_weight.reindex(index).fillna(0.0)

    pred = pred.combine_first(base).ffill()
    if components is None:
        return pred
    return pred, components

# ---------------------------------------------------------------------------
# Targets & scoring
# ---------------------------------------------------------------------------

def compute_future_targets(gate_series: pd.Series, index: pd.Index, lookaheads: Iterable[float]) -> Dict[float, pd.Series]:
    gate_df = gate_series.dropna().rename("value").reset_index()
    gate_df = gate_df.rename(columns={"ts_ns": "gate_ts"})
    targets: Dict[float, pd.Series] = {}

    base = pd.DataFrame({"ts_ns": index})
    base = base.sort_values("ts_ns")

    for horizon in lookaheads:
        offset = int(horizon * 1e9)
        shifted = base.copy()
        shifted["lookup_ts"] = shifted["ts_ns"] + offset
        merged = pd.merge_asof(
            shifted,
            gate_df,
            left_on="lookup_ts",
            right_on="gate_ts",
            direction="forward",
            allow_exact_matches=True,
        )
        targets[horizon] = pd.Series(merged["value"].to_numpy(), index=index)
    return targets


def rmse(pred: pd.Series, target: pd.Series) -> float:
    mask = pred.notna() & target.notna()
    if mask.sum() == 0:
        return float("nan")
    return float(np.sqrt(np.mean((pred[mask] - target[mask]) ** 2)))

# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def evaluate_strategies(
    path: Path,
    lookaheads: Iterable[float],
    target_strategy: str = "demeaned_lead",
):
    raw = load_depth_csv(path)
    wide_base = pivot_wide(raw)
    wide_micro = pivot_metric(raw, "microprice", "microprice")
    wide_imbalance = pivot_metric(raw, "imbalance", "imbalance")

    wide = pd.concat([wide_base, wide_micro, wide_imbalance], axis=1)
    wide = wide.sort_index()
    wide = wide.ffill()
    required_col = "gate_bbo" if "gate_orderbook" not in wide.columns else "gate_orderbook"
    if required_col not in wide.columns:
        raise ValueError("Gate BBO required to compute targets once orderbook is dropped.")
    wide = wide.dropna(subset=[required_col], how="any")

    if "dt" in wide.columns:
        dt_index = pd.DatetimeIndex(wide["dt"])
    else:
        dt_index = pd.to_datetime(wide.index, unit="ns", utc=True)

    recency = compute_recency(raw, wide.index, [c for c in wide.columns if c != "dt"])

    gate_ob = wide.get("gate_orderbook")
    gate_bbo = wide.get("gate_bbo")
    if gate_ob is None and gate_bbo is None:
        raise ValueError("Gate orderbook or BBO feed required to compute targets.")

    if gate_ob is None:
        gate_mid_ref = gate_bbo.copy()
    else:
        gate_mid_ref = gate_ob.copy()

    if gate_bbo is not None:
        rec_ob = recency.get("gate_orderbook") if gate_ob is not None else None
        rec_bbo = recency.get("gate_bbo") if "gate_bbo" in recency.columns else None
        use_bbo = gate_bbo.notna()
        if gate_ob is not None:
            missing_ob = gate_ob.isna()
        else:
            missing_ob = pd.Series(True, index=gate_bbo.index)

        if rec_ob is not None and rec_bbo is not None:
            use_bbo = use_bbo & ((rec_bbo < rec_ob) | missing_ob)
        else:
            use_bbo = use_bbo & missing_ob
        gate_mid_ref = gate_mid_ref.where(~use_bbo, gate_bbo)
        gate_mid_ref = gate_mid_ref.fillna(gate_bbo)

    gate_series = gate_mid_ref
    wide["gate_mid_ref"] = gate_series
    strategies: Dict[str, pd.Series] = {}
    strategy_components: Dict[str, pd.Series] = {}

    strategies["gate_orderbook"] = gate_ob
    strategies["gate_trades_only"] = gate_trades_only(wide, recency)
    if gate_bbo is not None:
        strategies["gate_bbo"] = gate_bbo

    combo_series, combo_components = gate_trade_bbo_orderbook_combo(
        wide, recency, return_components=True
    )
    strategies["gate_trade_bbo_orderbook_combo"] = combo_series
    strategy_components.update(combo_components)

    latency_series, latency_components = latency_blend(wide, recency, return_components=True)
    strategies["latency_blend"] = latency_series
    strategy_components.update(latency_components)

    inv_series, inv_components = inverse_recency_combo(
        wide, recency, return_components=True
    )
    strategies["inverse_recency_combo"] = inv_series
    strategy_components.update(inv_components)

    momentum_series, momentum_components = trade_momentum(
        wide, recency, return_components=True
    )
    strategies["trade_momentum"] = momentum_series
    strategy_components.update(momentum_components)

    fusion_series, fusion_components = fusion_alpha(wide, recency, return_components=True)
    strategies["fusion_alpha"] = fusion_series
    strategy_components.update(fusion_components)

    lead_series, lead_components = lead_lag_edge(wide, recency, return_components=True)
    strategies["lead_lag_edge"] = lead_series
    strategy_components.update(lead_components)

    external_series, external_components = external_latency_lead(
        wide, recency, return_components=True
    )
    strategies["external_latency_lead"] = external_series
    strategy_components.update(external_components)

    demeaned_series, demeaned_components = demeaned_lead(
        wide, recency, return_components=True
    )
    strategies["demeaned_lead"] = demeaned_series
    strategy_components.update(demeaned_components)

    strategies = {name: series for name, series in strategies.items() if series is not None}

    demeaned_feed_strategies = compute_demeaned_feed_strategies(wide, window="10s", min_periods=10)

    target_series: pd.Series | None
    if target_strategy == "gate_mid_ref":
        target_series = gate_series
    else:
        target_series = strategies.get(target_strategy)
        if target_series is None and target_strategy in wide.columns:
            target_series = pd.to_numeric(wide[target_strategy], errors="coerce")
        if target_series is None:
            available = ", ".join(sorted(strategies.keys()))
            raise ValueError(
                f"Target strategy '{target_strategy}' not found. Available strategies: {available}"
            )

    if target_strategy not in strategies and target_series is not None:
        strategies[target_strategy] = target_series

    targets = compute_future_targets(target_series, wide.index, lookaheads)

    weighted_sum = None
    weight_total = 0.0
    markouts: List[pd.Series] = []
    for horizon, target in targets.items():
        diff = (pd.to_numeric(target, errors="coerce") - gate_series).astype(float)
        markouts.append(diff)
        weight = 1.0 / max(float(horizon), 0.25)
        if weighted_sum is None:
            weighted_sum = diff * weight
        else:
            weighted_sum = weighted_sum.add(diff * weight, fill_value=0.0)
        weight_total += weight
    if weighted_sum is not None and weight_total > 0.0:
        combined_markout = weighted_sum / weight_total
        combined_future = gate_series.add(combined_markout, fill_value=np.nan)
    else:
        combined_future = None

    imbalance_series = compute_weighted_imbalances(wide, raw)
    latency_features = compute_latency_lead_features(wide, recency, gate_series)
    horizon_key = next((h for h in targets if abs(h - 1.0) < 1e-9), None)
    future_price = combined_future
    if future_price is None and horizon_key is not None:
        future_price = targets.get(horizon_key)

    indicator_inputs: Dict[str, pd.Series] = {
        "weighted_orderbook_imbalance": imbalance_series.get("weighted_orderbook_imbalance"),
        "weighted_bbo_imbalance": imbalance_series.get("weighted_bbo_imbalance"),
        "weighted_trade_imbalance": imbalance_series.get("weighted_trade_imbalance"),
    }
    indicator_inputs.update(latency_features)
    indicator_inputs = {k: v for k, v in indicator_inputs.items() if v is not None}
    for name, series in strategies.items():
        if name == target_strategy or name == "gate_orderbook" or name in EXCLUDED_REGRESSION_STRATEGIES:
            continue
        indicator_inputs[f"strategy_{name}"] = series
    for comp_name, series in strategy_components.items():
        if series is None:
            continue
        if comp_name.startswith(f"{target_strategy}_"):
            continue
        if comp_name.startswith("inverse_recency_") or comp_name.startswith("gate_trades_only"):
            continue
        indicator_inputs[f"component_{comp_name}"] = series

    demeaned_indicator_inputs = demean_series_map(
        indicator_inputs, dt_index, window="10s", min_periods=10
    )
    regression_inputs = {**demeaned_indicator_inputs, **demeaned_feed_strategies}
    regression_inputs.pop("demeaned_gate_orderbook", None)

    regression_summary, regression_markout = run_markout_regression(
        regression_inputs, target_series, future_price
    )

    if regression_summary and markouts:
        regression_summary = (
            "Aggregate lookahead regression (weights proportional to 1/h)\n"
            + regression_summary
        )

    arima_info: str | None = None
    if regression_markout is not None:
        regression_markout = penalize_volatility(regression_markout, alpha=0.08, cap_multiplier=1.3)
        regression_prediction = target_series.add(regression_markout, fill_value=np.nan)
        if regression_prediction.notna().any():
            strategies["regression_prediction"] = regression_prediction

        markout_series = regression_markout.dropna()
        lagged = markout_series.shift(1).dropna()
        if len(lagged) > 30:
            aligned = markout_series.loc[lagged.index]
            lag_mean = float(lagged.mean())
            curr_mean = float(aligned.mean())
            denom = float(((lagged - lag_mean) ** 2).sum())
            if denom > 1e-12:
                phi = float(((lagged - lag_mean) * (aligned - curr_mean)).sum() / denom)
                intercept = curr_mean - phi * lag_mean
                arima_markout = intercept + regression_markout.shift(1) * phi
                arima_prediction = target_series.add(arima_markout, fill_value=np.nan)
                arima_prediction = penalize_volatility(arima_prediction, alpha=0.06, cap_multiplier=1.05)
                if arima_prediction.notna().any():
                    strategies["regression_arima"] = arima_prediction.reindex(gate_series.index)
                    arima_info = (
                        f"regression_arima AR(1) on regression markout: phi={phi:+.3f}, intercept={intercept:+.6f}"
                    )
    ensemble_info = None

    records = []
    for name, pred in strategies.items():
        row = {"strategy": name}
        for horizon, target in targets.items():
            row[f"rmse_{horizon:.1f}s"] = rmse(pred, target)
        rmse_vals = [row[k] for k in row if k.startswith("rmse_") and not np.isnan(row[k])]
        row["mean_rmse"] = float(np.mean(rmse_vals)) if rmse_vals else float("nan")
        records.append(row)

    results = pd.DataFrame(records).set_index("strategy").sort_values("mean_rmse")
    results = results.round(6)

    extra_lines: List[str] = []
    if ensemble_info:
        extra_lines.append(ensemble_info)
    if arima_info:
        extra_lines.append(arima_info)

    if regression_summary:
        if extra_lines:
            regression_summary = regression_summary + "\n" + "\n".join(extra_lines)
    elif extra_lines:
        regression_summary = "\n".join(extra_lines)

    return results, raw, wide, strategies, regression_summary


def plot_with_strategies(
    raw: pd.DataFrame,
    wide: pd.DataFrame,
    strategies: Dict[str, pd.Series],
    chosen: List[str],
    target_strategy: str,
    plot_demeaned: bool = False,
):
    import matplotlib.pyplot as plt

    df = raw.copy()
    df["dt"] = pd.to_datetime(df["ts_ns"], unit="ns", utc=True).dt.tz_localize(None)

    if plot_demeaned:
        df["mid_plot"] = np.nan
        for col_name, sub in df.groupby("col"):
            series = pd.Series(sub["mid"].to_numpy(dtype=float), index=sub.index)
            dt_index = pd.to_datetime(sub["ts_ns"], unit="ns", utc=True)
            demeaned = demean_time_window(series, dt_index, window="10s", min_periods=10)
            df.loc[sub.index, "mid_plot"] = demeaned.to_numpy()
        df["mid_plot"] = df["mid_plot"].astype(float)
    else:
        df["mid_plot"] = df["mid"]

    fig, (ax_main, ax_imb) = plt.subplots(
        2,
        1,
        sharex=True,
        figsize=(12, 8.5),
        gridspec_kw={"height_ratios": [3, 1], "hspace": 0.05},
    )

    color_map = {
        "binance": "tab:orange",
        "bitget": "tab:red",
        "bybit": "tab:green",
        "gateio": "tab:blue",
        "gate": "tab:blue",
    }
    marker_map = {
        "bbo": "o",
        "orderbook": "s",
        "trade": "^",
    }

    feed_order = ["bbo", "orderbook", "trade"]
    exchanges = sorted(df["exchange"].unique())
    for ex in exchanges:
        ex_df = df[df["exchange"] == ex]
        for feed in feed_order:
            sub = ex_df[ex_df["feed"] == feed]
            if sub.empty:
                continue
            c = color_map.get(ex, "black")
            m = marker_map.get(feed, ".")
            display_ex = {"gate": "Gateio", "gateio": "Gateio"}.get(ex, ex.title())
            label = f"{display_ex} {feed.upper()}"
            if feed == "trade" and "direction" in sub.columns:
                buy = sub[sub.get("direction") == "buy"]
                sell = sub[sub.get("direction") == "sell"]
                rest = sub[~sub.get("direction").isin(["buy", "sell"])]
                if not buy.empty:
                    ax_main.scatter(
                        buy["dt"],
                        buy["mid_plot"],
                        s=12,
                        c=c,
                        marker="^",
                        label=label,
                        alpha=0.7,
                    )
                if not sell.empty:
                    ax_main.scatter(
                        sell["dt"],
                        sell["mid_plot"],
                        s=12,
                        c=c,
                        marker="v",
                        label="_nolegend_",
                        alpha=0.7,
                    )
                if not rest.empty:
                    ax_main.scatter(
                        rest["dt"],
                        rest["mid_plot"],
                        s=12,
                        c=c,
                        marker=m,
                        label="_nolegend_",
                        alpha=0.7,
                    )
            else:
                ax_main.scatter(
                    sub["dt"],
                    sub["mid_plot"],
                    s=12,
                    c=c,
                    marker=m,
                    label=label,
                    alpha=0.7,
                )

    strategy_dt = wide["dt"].dt.tz_localize(None)
    if plot_demeaned:
        dt_index = pd.DatetimeIndex(wide["dt"])
        demeaned_strats = demean_series_map(strategies, dt_index, window="10s", min_periods=10)
        plot_series: Dict[str, pd.Series] = {}
        for name, series in strategies.items():
            plot_series[name] = demeaned_strats.get(name, series)
    else:
        plot_series = strategies

    palette = {
        "gate_orderbook": "black",
        "gate_trades_only": "tab:blue",
        "gate_trade_bbo_orderbook_combo": "tab:orange",
        "latency_blend": "tab:purple",
        "inverse_recency_combo": "tab:brown",
        "trade_momentum": "tab:pink",
        "fusion_alpha": "tab:cyan",
        "lead_lag_edge": "tab:olive",
        "external_latency_lead": "tab:purple",
        "demeaned_lead": "tab:gray",
        "regression_prediction": "tab:red",
    }
    palette.setdefault(target_strategy, "tab:green")

    chosen = chosen or list(strategies.keys())
    if target_strategy in strategies and target_strategy not in chosen:
        chosen = list(chosen) + [target_strategy]
    highlight = {target_strategy, "regression_prediction"}

    for name in chosen:
        if name not in plot_series:
            continue
        series = plot_series[name]
        linewidth = 2.2 if name in highlight else 1.6
        alpha = 0.98 if name in highlight else 0.78
        ax_main.plot(
            strategy_dt,
            series.to_numpy(dtype=float),
            label=f"{name}",
            linewidth=linewidth,
            color=palette.get(name),
            alpha=alpha,
            zorder=5 if name in highlight else 2,
        )

    ax_main.set_ylabel("De-meaned Mid" if plot_demeaned else "Mid Price")
    title_suffix = " (demeaned)" if plot_demeaned else ""
    ax_main.set_title(f"Exchange Mids and Fair Value Strategies vs {target_strategy}{title_suffix}")
    if plot_demeaned:
        ax_main.axhline(0.0, color="black", linewidth=0.8, linestyle="--", alpha=0.6)
    ax_main.legend(bbox_to_anchor=(1.02, 1), loc="upper left", fontsize=10)
    ax_main.grid(True, alpha=0.3)

    imbalance_series = compute_weighted_imbalances(wide, raw)

    bbo_series = imbalance_series.get("weighted_bbo_imbalance")
    order_series = imbalance_series.get("weighted_orderbook_imbalance")
    trade_series = imbalance_series.get("weighted_trade_imbalance")

    plotted_primary = False
    legend_handles: List = []
    legend_labels: List[str] = []

    if bbo_series is not None and not bbo_series.dropna().empty:
        line_bbo = ax_imb.plot(
            strategy_dt,
            bbo_series.to_numpy(),
            label="Weighted BBO Imbalance",
            linewidth=1.4,
            color="tab:red",
        )[0]
        legend_handles.append(line_bbo)
        legend_labels.append("Weighted BBO Imbalance")
        plotted_primary = True

    if order_series is not None and not order_series.dropna().empty:
        line_order = ax_imb.plot(
            strategy_dt,
            order_series.to_numpy(),
            label="Weighted OB Imbalance",
            linewidth=1.4,
            color="tab:green",
        )[0]
        legend_handles.append(line_order)
        legend_labels.append("Weighted OB Imbalance")
        plotted_primary = True

    trade_axis = None
    if trade_series is not None and not trade_series.dropna().empty:
        trade_axis = ax_imb.twinx()
        line_trades = trade_axis.plot(
            strategy_dt,
            trade_series.to_numpy(),
            label="Weighted Trade Imbalance",
            linewidth=1.2,
            color="tab:blue",
        )[0]
        trade_axis.set_ylabel("Trade Imbalance (Buy-Sell)", color="tab:blue")
        trade_axis.set_ylim(-1.05, 1.05)
        trade_axis.tick_params(axis="y", labelcolor="tab:blue")
        legend_handles.append(line_trades)
        legend_labels.append("Weighted Trade Imbalance")

    if plotted_primary:
        ax_imb.axhline(0.0, color="black", linewidth=0.8, linestyle="--", alpha=0.6)
        ax_imb.set_ylabel("Book Imbalance")
        ax_imb.set_ylim(-1.05, 1.05)
        ax_imb.grid(True, alpha=0.3)
    else:
        if trade_axis is None:
            ax_imb.set_visible(False)

    if plotted_primary or trade_axis is not None:
        ax_imb.set_xlabel("Time")
        for label in ax_imb.get_xticklabels():
            label.set_rotation(45)
        if trade_axis is not None:
            for label in trade_axis.get_xticklabels():
                label.set_rotation(45)
        if legend_handles:
            ax_imb.legend(legend_handles, legend_labels, loc="upper left", fontsize=9)

    fig.tight_layout()
    plt.show()


def main():
    parser = argparse.ArgumentParser(description="Evaluate fair value strategies.")
    parser.add_argument(
        "--csv",
        type=Path,
        default=DEFAULT_CSV,
        help=f"Path to depth CSV (default: {DEFAULT_CSV})",
    )
    parser.add_argument(
        "--lookaheads",
        type=float,
        nargs="*",
        default=list(DEFAULT_LOOKAHEADS),
        help="Lookahead horizons in seconds (default: 0.5 1.0 3.0)",
    )
    parser.add_argument(
        "--plot",
        action="store_true",
        help="Plot exchange mids with selected fair value strategies",
    )
    parser.add_argument(
        "--plot-strategies",
        nargs="*",
        help="Subset of strategies to overlay on the plot",
    )
    parser.add_argument(
        "--plot-demeaned",
        action="store_true",
        help="Show scatter and strategy series after 10s de-meaning",
    )
    parser.add_argument(
        "--target-strategy",
        type=str,
        default="demeaned_lead",
        help="Series to use as the regression/score target (e.g. gate_mid_ref, demeaned_lead)",
    )
    args = parser.parse_args()

    results, raw, wide, strategies, regression = evaluate_strategies(
        args.csv, args.lookaheads, target_strategy=args.target_strategy
    )
    print(f"Fair Value Strategy RMSEs vs future {args.target_strategy}")
    print(results)

    print("\nIndicator vs 1s Markout Regression")
    if regression:
        print(regression)
    elif sm is None:
        print("statsmodels not available; regression skipped.")
    else:
        print("No sufficient data to compute regressions.")

    should_plot = args.plot or PLOT_RESULTS
    if should_plot:
        chosen = args.plot_strategies or list(results.index)
        plot_with_strategies(
            raw,
            wide,
            strategies,
            chosen,
            target_strategy=args.target_strategy,
            plot_demeaned=args.plot_demeaned,
        )


if __name__ == "__main__":
    main()
