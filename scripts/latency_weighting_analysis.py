#!/usr/bin/env python3
"""
Latency analysis utilities for gate_activity.csv.

This script evaluates how update latency affects short-horizon price accuracy
and derives exchange-specific discount factors that can be used to weight or
skip stale updates.
"""

from __future__ import annotations

import argparse
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import numpy as np

try:
    import pandas as pd
except ImportError as exc:  # pragma: no cover - dependency guard
    raise SystemExit(
        "pandas is required for this analysis. Install it via "
        "`python3 -m pip install --user pandas` and retry."
    ) from exc


NS_TO_MS = 1e6
NS_TO_SEC = 1e9
DEFAULT_FEATURES = [
    "latency_engine_ms",
    "latency_engine_ms_rel",
    "latency_system_ms",
    "latency_system_ms_rel",
    "source_pipeline_ms",
    "future_dt_ms",
]
TARGET_COLUMN = "abs_error_bps"


@dataclass
class RegressionResult:
    coefficients: pd.DataFrame
    r2: float
    sample_count: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Quantify latency effects and derive weighting heuristics."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("logs/gate_activity.csv"),
        help="CSV file with gate_activity data (default: logs/gate_activity.csv)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional row limit for faster iteration.",
    )
    parser.add_argument(
        "--horizon-ms",
        type=float,
        default=1000.0,
        help="Max horizon (in ms) for the next update used as ground truth.",
    )
    parser.add_argument(
        "--min-samples",
        type=int,
        default=2000,
        help="Minimum samples per exchange required for heuristics.",
    )
    parser.add_argument(
        "--skip-ratio",
        type=float,
        default=1.25,
        help=(
            "Error ratio (slow/fast) above which we recommend skipping updates "
            "whose relative latency exceeds the derived threshold."
        ),
    )
    parser.add_argument(
        "--output-summary",
        type=Path,
        default=None,
        help="Optional path to write the per-exchange summary CSV.",
    )
    parser.add_argument(
        "--output-regression",
        type=Path,
        default=None,
        help="Optional path to write the regression coefficients CSV.",
    )
    return parser.parse_args()


def load_data(path: Path, limit: Optional[int]) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    df = pd.read_csv(path, na_values=[""], nrows=limit)
    time_cols = [
        "ts_ns",
        "source_engine_ts_ns",
        "source_system_ts_ns",
        "reference_ts_ns",
        "sent_ts_ns",
    ]
    for col in time_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
    if "feed" in df.columns:
        df["feed"] = df["feed"].fillna("unknown").astype(str)

    if "exchange" not in df.columns or "ts_ns" not in df.columns:
        raise ValueError("Expected columns 'exchange' and 'ts_ns' not present.")

    df = df.sort_values(["exchange", "ts_ns"]).reset_index(drop=True)
    return df


def add_future_targets(df: pd.DataFrame, horizon_ms: float) -> pd.DataFrame:
    df = df.copy()
    df["next_price"] = df.groupby("exchange")["price"].shift(-1)
    df["next_ts_ns"] = df.groupby("exchange")["ts_ns"].shift(-1)
    df["future_dt_ms"] = (df["next_ts_ns"] - df["ts_ns"]) / NS_TO_MS
    mask = (df["future_dt_ms"] > 0) & (df["future_dt_ms"] <= horizon_ms)
    df = df.loc[mask].copy()
    price_shift = df["next_price"] - df["price"]
    df["signed_return_bps"] = price_shift / df["price"] * 1e4
    df[TARGET_COLUMN] = price_shift.abs() / df["price"] * 1e4
    return df


def add_latency_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["latency_engine_ms"] = (df["ts_ns"] - df["source_engine_ts_ns"]) / NS_TO_MS
    df["latency_system_ms"] = (df["ts_ns"] - df["source_system_ts_ns"]) / NS_TO_MS
    df["source_pipeline_ms"] = (
        df["source_system_ts_ns"] - df["source_engine_ts_ns"]
    ) / NS_TO_MS
    df["latency_sent_ms"] = (df["ts_ns"] - df["sent_ts_ns"]) / NS_TO_MS

    for col in ["latency_engine_ms", "latency_system_ms", "latency_sent_ms"]:
        if col in df.columns:
            median_per_exchange = df.groupby("exchange")[col].transform("median")
            df[f"{col}_rel"] = df[col] - median_per_exchange

    return df


def run_regression(
    df: pd.DataFrame,
    feature_cols: Iterable[str],
    cat_cols: Iterable[str],
    target_col: str,
) -> RegressionResult:
    required_cols = list(feature_cols) + [target_col]
    data = df.dropna(subset=required_cols).copy()
    if data.empty:
        raise ValueError("No data remaining after dropping missing regression rows.")

    dummies = pd.get_dummies(data.loc[:, cat_cols].astype("category"), drop_first=True)
    X = np.column_stack([np.ones(len(data)), data.loc[:, feature_cols].values, dummies.values])
    y = data[target_col].values

    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    y_hat = X @ beta
    ss_res = np.square(y - y_hat).sum()
    ss_tot = np.square(y - y.mean()).sum()
    r2 = 1.0 - ss_res / ss_tot if ss_tot else 0.0

    feature_names = ["intercept", *feature_cols, *dummies.columns.tolist()]
    coef = pd.DataFrame(
        {
            "feature": feature_names,
            "coefficient": beta,
        }
    )

    # Add effect size (per std dev of input) for numeric columns.
    std_map = data[feature_cols].std().replace({0.0: np.nan})
    coef["effect_per_std"] = coef["coefficient"]
    for feature in feature_cols:
        std = std_map.get(feature)
        mask = coef["feature"] == feature
        if mask.any() and std and not math.isnan(std):
            coef.loc[mask, "effect_per_std"] = coef.loc[mask, "coefficient"] * std

    return RegressionResult(coefficients=coef, r2=r2, sample_count=len(data))


def summarize_by_exchange_feed(
    df: pd.DataFrame, min_samples: int, skip_ratio: float
) -> pd.DataFrame:
    rows = []
    group_cols = ["exchange", "feed"]
    if "feed" not in df.columns:
        group_cols = ["exchange"]

    for group_key, grp in df.groupby(group_cols):
        subset = grp.dropna(subset=["latency_engine_ms", TARGET_COLUMN])
        sample_count = len(subset)
        if sample_count < min_samples:
            continue

        rel_lat = subset["latency_engine_ms_rel"]
        fast_q = rel_lat.quantile(0.25)
        slow_q = rel_lat.quantile(0.75)
        fast = subset.loc[rel_lat <= fast_q]
        slow = subset.loc[rel_lat >= slow_q]

        fast_err = fast[TARGET_COLUMN].mean()
        slow_err = slow[TARGET_COLUMN].mean()
        fast_lat = fast["latency_engine_ms_rel"].mean()
        slow_lat = slow["latency_engine_ms_rel"].mean()

        err_ratio = slow_err / fast_err if fast_err else np.nan
        lambda_val = np.nan
        if (
            np.isfinite(err_ratio)
            and err_ratio > 0
            and fast_lat is not None
            and slow_lat is not None
            and slow_lat != fast_lat
        ):
            lambda_val = max(0.0, math.log(err_ratio) / (slow_lat - fast_lat))

        corr = subset["latency_engine_ms_rel"].corr(subset[TARGET_COLUMN])
        skip_threshold = np.nan
        if err_ratio and err_ratio >= skip_ratio:
            skip_threshold = subset["latency_engine_ms_rel"].quantile(0.9)
            if skip_threshold <= 0:
                skip_threshold = np.nan

        rows.append(
            {
                "exchange": group_key[0] if isinstance(group_key, tuple) else group_key,
                "feed": group_key[1] if isinstance(group_key, tuple) and len(group_key) > 1 else "unknown",
                "samples": sample_count,
                "median_latency_ms": subset["latency_engine_ms"].median(),
                "mean_abs_error_bps": subset[TARGET_COLUMN].mean(),
                "corr_rel_latency_error": corr,
                "fast_mean_rel_latency_ms": fast_lat,
                "slow_mean_rel_latency_ms": slow_lat,
                "fast_mean_error_bps": fast_err,
                "slow_mean_error_bps": slow_err,
                "slow_fast_error_ratio": err_ratio,
                "lambda_for_exp_weight": lambda_val,
                "skip_threshold_rel_ms": skip_threshold,
            }
        )

    sort_cols = ["exchange", "feed"] if "feed" in df.columns else ["exchange"]
    summary = pd.DataFrame(rows).sort_values(sort_cols).reset_index(drop=True)
    return summary


def attach_weighting_decisions(
    df: pd.DataFrame, summary: pd.DataFrame
) -> pd.DataFrame:
    df = df.copy()
    key_cols = ["exchange", "feed"] if "feed" in df.columns else ["exchange"]
    summary_keyed = summary.set_index(key_cols)
    lambda_map = summary_keyed["lambda_for_exp_weight"].to_dict()
    skip_map = summary_keyed["skip_threshold_rel_ms"].to_dict()

    weights = []
    skips = []
    for idx, row in df.iterrows():
        exchange = row["exchange"]
        feed = row.get("feed", "unknown")
        rel_latency = row["latency_engine_ms_rel"]
        key = (exchange, feed) if "feed" in df.columns else exchange

        lam = lambda_map.get(key, np.nan)
        if np.isnan(rel_latency) or np.isnan(lam):
            weights.append(np.nan)
        else:
            weights.append(math.exp(-lam * rel_latency))

        threshold = skip_map.get(key, np.nan)
        skips.append(rel_latency > threshold if threshold == threshold else False)

    df["recommended_weight"] = weights
    df["skip_due_to_latency"] = skips
    return df


def main() -> None:
    args = parse_args()
    df_raw = load_data(args.input, args.limit)
    df_targets = add_future_targets(df_raw, args.horizon_ms)
    df_features = add_latency_features(df_targets)

    regression = run_regression(
        df_features,
        feature_cols=DEFAULT_FEATURES,
        cat_cols=["exchange", "feed"],
        target_col=TARGET_COLUMN,
    )

    summary = summarize_by_exchange_feed(df_features, args.min_samples, args.skip_ratio)
    df_with_weights = attach_weighting_decisions(df_features, summary)

    print("\n=== Data overview ===")
    print(f"Total samples used: {len(df_features):,}")
    print(f"Horizon: {args.horizon_ms} ms")
    print(f"Regression samples: {regression.sample_count:,}")
    print(f"Regression R^2: {regression.r2:.3f}")

    print("\n=== Top latency coefficients (per std effect, bps) ===")
    latency_rows = regression.coefficients[
        regression.coefficients["feature"].isin(DEFAULT_FEATURES)
    ].copy()
    latency_rows["effect_per_std"] = latency_rows["effect_per_std"].fillna(0.0)
    latency_rows.sort_values("effect_per_std", key=np.abs, ascending=False, inplace=True)
    print(latency_rows.to_string(index=False, float_format=lambda x: f"{x:,.4f}"))

    print("\n=== Exchange heuristics ===")
    printable_cols = [
        "exchange",
        "feed",
        "samples",
        "median_latency_ms",
        "mean_abs_error_bps",
        "corr_rel_latency_error",
        "slow_fast_error_ratio",
        "lambda_for_exp_weight",
        "skip_threshold_rel_ms",
    ]
    if summary.empty:
        print("No exchange met the minimum sample requirement.")
    else:
        print(summary.loc[:, printable_cols].to_string(index=False, justify="left"))

    if summary.empty:
        skipped = 0
    else:
        skipped = df_with_weights["skip_due_to_latency"].sum()
    total = len(df_with_weights)
    if total:
        print(
            f"\nUpdates flagged for skipping: {int(skipped):,} "
            f"({skipped / total:.2%} of analysed rows)"
        )

    if args.output_summary:
        summary.to_csv(args.output_summary, index=False)
        print(f"\nSaved exchange summary to {args.output_summary}")

    if args.output_regression:
        regression.coefficients.to_csv(args.output_regression, index=False)
        print(f"Saved regression coefficients to {args.output_regression}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:  # pragma: no cover - cli convenience
        sys.exit(130)
