#!/usr/bin/env python3
"""
Multi-Exchange Price Predictor for Lighter Exchange.

Key features:
1. Track exchange-specific biases (different exchanges trade at different levels)
2. Use rolling bias estimation to adjust external exchange prices to lighter's level
3. Recency-weighted fusion of signals (fresher data gets higher weight)

The model predicts what lighter's mid-price should be RIGHT NOW, using:
- The most recent lighter observation (if any)
- Bias-corrected signals from other exchanges
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from typing import Dict, Optional, Tuple, List
from dataclasses import dataclass, field
from collections import deque


# =============================================================================
# Configuration
# =============================================================================

DATA_PATH = Path("/Users/eriksreinfelds/Documents/GitHub/rust_test/logs/lighter_activity.csv")
TRAIN_FRACTION = 0.80
EXCHANGES = ["binance", "bybit", "bitget", "gate", "okx", "lighter"]

# How much history to use for bias estimation (in number of observations)
BIAS_WINDOW = 200

# Decay rate for recency weighting (half-life in nanoseconds)
RECENCY_HALF_LIFE_NS = 100_000_000  # 100ms


# =============================================================================
# Data Loading
# =============================================================================

def load_data(path: Path) -> pd.DataFrame:
    """Load and preprocess the lighter activity CSV."""
    print(f"Loading data from {path}...")
    df = pd.read_csv(path, low_memory=False)
    
    # Filter out our own quotes (keep only market data)
    market_data = df[df["event_type"] == "market"].copy()
    
    # Convert numeric columns
    numeric_cols = ["ts_ns", "price", "size", "source_engine_ts_ns", "source_system_ts_ns"]
    bid_ask_cols = [c for c in market_data.columns if c.startswith(("bid_", "ask_"))]
    for col in numeric_cols + bid_ask_cols:
        if col in market_data.columns:
            market_data[col] = pd.to_numeric(market_data[col], errors="coerce")
    
    # Sort by timestamp
    market_data = market_data.sort_values("ts_ns").reset_index(drop=True)
    
    # Compute mid price from BBO when available
    has_bbo = market_data["bid_px_1"].notna() & market_data["ask_px_1"].notna()
    market_data.loc[has_bbo, "mid"] = (
        market_data.loc[has_bbo, "bid_px_1"] + market_data.loc[has_bbo, "ask_px_1"]
    ) / 2.0
    
    # For trades, use the trade price as the observation
    is_trade = market_data["feed"] == "trade"
    market_data.loc[is_trade & market_data["mid"].isna(), "mid"] = market_data.loc[
        is_trade & market_data["mid"].isna(), "price"
    ]
    
    # Fill any remaining NaNs with the price column
    market_data["mid"] = market_data["mid"].fillna(market_data["price"])
    
    print(f"Loaded {len(market_data):,} market data rows")
    print(f"Exchanges: {market_data['exchange'].unique().tolist()}")
    print(f"Feeds: {market_data['feed'].unique().tolist()}")
    
    # Show price ranges by exchange
    print("\nPrice ranges by exchange:")
    for ex in EXCHANGES:
        ex_data = market_data[market_data["exchange"] == ex]["mid"]
        if len(ex_data) > 0:
            print(f"  {ex:10s}: mean={ex_data.mean():.2f}, std={ex_data.std():.2f}")
    
    return market_data


# =============================================================================
# Exchange State Tracking
# =============================================================================

@dataclass
class ExchangeTracker:
    """Track state and bias for a single exchange."""
    name: str
    last_mid: Optional[float] = None
    last_ts: Optional[int] = None
    
    # Rolling history for bias estimation
    price_history: deque = field(default_factory=lambda: deque(maxlen=BIAS_WINDOW))
    lighter_history: deque = field(default_factory=lambda: deque(maxlen=BIAS_WINDOW))
    
    # Estimated bias (this exchange's price - lighter's price)
    estimated_bias: float = 0.0
    bias_confidence: float = 0.0  # How confident we are in bias estimate
    
    def update(self, mid: float, ts: int):
        """Update with new observation."""
        self.last_mid = mid
        self.last_ts = ts
        self.price_history.append((ts, mid))
    
    def update_bias(self, lighter_mid: float, lighter_ts: int):
        """Update bias estimate when we get a lighter observation."""
        if self.last_mid is None:
            return
        
        # Only use recent data for bias estimation
        age_ns = lighter_ts - self.last_ts if self.last_ts else float('inf')
        if age_ns > 1_000_000_000:  # More than 1 second old, skip
            return
        
        self.lighter_history.append((lighter_ts, lighter_mid))
        
        # Compute rolling bias from recent pairs
        if len(self.price_history) >= 5 and len(self.lighter_history) >= 5:
            # Get recent prices from this exchange
            recent_ex = [p for ts, p in self.price_history][-50:]
            recent_lt = [p for ts, p in self.lighter_history][-50:]
            
            # Match by time proximity (simple approach: use means)
            ex_mean = np.mean(recent_ex)
            lt_mean = np.mean(recent_lt)
            
            # Exponential moving average of bias
            new_bias = ex_mean - lt_mean
            alpha = 0.1  # Smoothing factor
            self.estimated_bias = alpha * new_bias + (1 - alpha) * self.estimated_bias
            self.bias_confidence = min(1.0, len(self.price_history) / 50.0)
    
    def get_bias_corrected_mid(self) -> Optional[float]:
        """Return the bias-corrected mid price (estimate of what lighter should be)."""
        if self.last_mid is None:
            return None
        return self.last_mid - self.estimated_bias
    
    def get_recency_weight(self, current_ts: int) -> float:
        """Get weight based on how recent this data is."""
        if self.last_ts is None:
            return 0.0
        
        age_ns = current_ts - self.last_ts
        if age_ns < 0:
            age_ns = 0
        
        # Exponential decay
        decay_rate = np.log(2) / RECENCY_HALF_LIFE_NS
        return np.exp(-decay_rate * age_ns)


# =============================================================================
# Price Predictor
# =============================================================================

class LighterPricePredictor:
    """
    Predicts lighter mid-price using multi-exchange data with bias correction.
    
    The prediction formula:
        pred = sum(w_ex * bias_corrected_ex) / sum(w_ex)
    
    Where weights are based on:
    - Data recency (exponential decay)
    - Exchange reliability (binance > bybit > okx > ...)
    - Bias calibration confidence
    """
    
    def __init__(self):
        self.trackers: Dict[str, ExchangeTracker] = {
            ex: ExchangeTracker(name=ex) for ex in EXCHANGES
        }
        
        # Base reliability weights for each exchange
        self.exchange_weights = {
            "lighter": 2.0,   # Highest weight - it's what we're predicting
            "binance": 1.0,   # Most liquid, best price discovery
            "bybit": 0.9,
            "okx": 0.8,
            "bitget": 0.6,
            "gate": 0.5,
        }
        
        self.last_prediction = None
        self.last_ts = None
        
        # Warmup tracking
        self.n_updates = 0
        
        # Results storage
        self.predictions = []
        self.timestamps = []
        self.lighter_actuals = []
        self.prediction_sources = []  # Track what contributed to each prediction
    
    def process_update(
        self,
        ts_ns: int,
        exchange: str,
        feed: str,
        mid_price: float,
    ) -> float:
        """
        Process a market data update and return current prediction.
        """
        self.n_updates += 1
        
        if pd.isna(mid_price) or not np.isfinite(mid_price):
            # No valid observation, return last prediction
            pred = self.last_prediction if self.last_prediction else 0.0
            self.predictions.append(pred)
            self.timestamps.append(ts_ns)
            return pred
        
        # Update the exchange tracker
        if exchange in self.trackers:
            self.trackers[exchange].update(mid_price, ts_ns)
        
        # If this is a lighter update, update all bias estimates
        if exchange == "lighter":
            for ex_name, tracker in self.trackers.items():
                if ex_name != "lighter":
                    tracker.update_bias(mid_price, ts_ns)
        
        # During warmup, just use the raw price
        if self.n_updates < 100:
            pred = mid_price
        else:
            # Make prediction
            pred = self._compute_prediction(ts_ns)
        
        # Guard against NaN/inf - fallback to raw mid price
        if not np.isfinite(pred):
            pred = mid_price
        
        self.last_prediction = pred
        self.last_ts = ts_ns
        self.predictions.append(pred)
        self.timestamps.append(ts_ns)
        
        return pred
    
    def _compute_prediction(self, current_ts: int) -> float:
        """
        Compute weighted prediction from all available data.
        """
        weighted_sum = 0.0
        weight_total = 0.0
        raw_prices = []  # Fallback
        
        for ex_name, tracker in self.trackers.items():
            if tracker.last_mid is None:
                continue
            
            raw_prices.append(tracker.last_mid)
            
            # Get recency weight
            recency_weight = tracker.get_recency_weight(current_ts)
            if recency_weight < 0.01:  # Too old, skip
                continue
            
            # Get base exchange weight
            base_weight = self.exchange_weights.get(ex_name, 0.5)
            
            # Get the price estimate
            if ex_name == "lighter":
                # Use lighter directly
                price_est = tracker.last_mid
            else:
                # Use bias-corrected price if we have confidence, else raw price
                if tracker.bias_confidence > 0.1:
                    price_est = tracker.get_bias_corrected_mid()
                    if price_est is None:
                        continue
                    # Scale weight by confidence
                    base_weight *= tracker.bias_confidence
                else:
                    # Use raw price with reduced weight during warmup
                    price_est = tracker.last_mid
                    base_weight *= 0.1  # Low weight for uncalibrated exchanges
            
            # Combine weights
            combined_weight = base_weight * recency_weight
            
            weighted_sum += price_est * combined_weight
            weight_total += combined_weight
        
        if weight_total < 1e-6:
            # No valid weighted data - use simple average of raw prices as fallback
            if raw_prices:
                return float(np.mean(raw_prices))
            if self.last_prediction and np.isfinite(self.last_prediction):
                return self.last_prediction
            return 85900.0  # Reasonable default for BTC price
        
        prediction = weighted_sum / weight_total
        
        # Sanity check - prediction should be close to raw prices
        if raw_prices:
            raw_mean = np.mean(raw_prices)
            if abs(prediction - raw_mean) > 100:  # More than $100 off? Something's wrong
                return raw_mean
        
        return prediction
    
    def get_results(self) -> pd.DataFrame:
        """Return predictions as a DataFrame."""
        return pd.DataFrame({
            "ts_ns": self.timestamps,
            "prediction": self.predictions,
        })


# =============================================================================
# Evaluation
# =============================================================================

def evaluate_predictions(
    df: pd.DataFrame,
    predictions: pd.Series,
    train_end_idx: int,
) -> Dict[str, float]:
    """Evaluate prediction quality against lighter mid-price."""
    
    # Get lighter-only data for evaluation
    lighter_mask = df["exchange"] == "lighter"
    lighter_df = df[lighter_mask].copy()
    lighter_df["prediction"] = predictions.reindex(lighter_df.index)
    
    # Split into train/test
    train_lighter = lighter_df[lighter_df.index <= train_end_idx]
    test_lighter = lighter_df[lighter_df.index > train_end_idx]
    
    metrics = {}
    
    for name, subset in [("train", train_lighter), ("test", test_lighter)]:
        valid = subset.dropna(subset=["mid", "prediction"])
        if len(valid) == 0:
            continue
        
        # Filter out inf/nan
        errors = valid["prediction"] - valid["mid"]
        finite_mask = np.isfinite(errors)
        errors = errors[finite_mask]
        
        if len(errors) == 0:
            print(f"  WARNING: No finite errors for {name} set!")
            continue
        
        metrics[f"{name}_mae"] = errors.abs().mean()
        metrics[f"{name}_rmse"] = np.sqrt((errors ** 2).mean())
        metrics[f"{name}_mean_error"] = errors.mean()
        metrics[f"{name}_n"] = len(errors)
        metrics[f"{name}_max_error"] = errors.abs().max()
        metrics[f"{name}_median_error"] = errors.abs().median()
        
        # Percentiles
        metrics[f"{name}_p95_error"] = errors.abs().quantile(0.95)
        metrics[f"{name}_p99_error"] = errors.abs().quantile(0.99)
        
        # Report how many were dropped
        n_dropped = len(valid) - len(errors)
        if n_dropped > 0:
            print(f"  NOTE: Dropped {n_dropped} non-finite predictions from {name} set")
    
    return metrics


def compute_naive_baseline(df: pd.DataFrame, train_end_idx: int) -> Dict[str, float]:
    """
    Compute baseline: use PREVIOUS lighter mid to predict CURRENT lighter mid.
    This is what you'd get by just using the last known lighter price.
    """
    lighter_mask = df["exchange"] == "lighter"
    lighter_df = df[lighter_mask].copy()
    
    # Shift to get previous mid
    lighter_df["prev_mid"] = lighter_df["mid"].shift(1)
    
    # Split - only evaluate on test set
    test_lighter = lighter_df[lighter_df.index > train_end_idx].copy()
    
    valid = test_lighter.dropna(subset=["mid", "prev_mid"])
    if len(valid) == 0:
        return {}
    
    errors = valid["prev_mid"] - valid["mid"]
    return {
        "naive_test_mae": errors.abs().mean(),
        "naive_test_rmse": np.sqrt((errors ** 2).mean()),
        "naive_test_n": len(valid),
        "naive_test_median": errors.abs().median(),
        "naive_test_p95": errors.abs().quantile(0.95),
    }


def compute_cross_exchange_baseline(df: pd.DataFrame, train_end_idx: int) -> Dict[str, float]:
    """
    Baseline: use most recent observation from ANY exchange (no bias correction).
    """
    # For each lighter observation, find the most recent other-exchange observation
    lighter_mask = df["exchange"] == "lighter"
    lighter_indices = df[lighter_mask].index.tolist()
    
    last_other_mid = None
    predictions = []
    
    for idx in range(len(df)):
        row = df.iloc[idx]
        if row["exchange"] != "lighter":
            last_other_mid = row["mid"]
        
        if row["exchange"] == "lighter":
            predictions.append((idx, row["mid"], last_other_mid))
    
    # Convert to dataframe and evaluate
    pred_df = pd.DataFrame(predictions, columns=["idx", "actual", "pred"])
    pred_df = pred_df.dropna()
    
    test_pred = pred_df[pred_df["idx"] > train_end_idx]
    
    if len(test_pred) == 0:
        return {}
    
    errors = test_pred["pred"] - test_pred["actual"]
    return {
        "crossex_test_mae": errors.abs().mean(),
        "crossex_test_rmse": np.sqrt((errors ** 2).mean()),
        "crossex_test_n": len(test_pred),
    }


# =============================================================================
# Visualization
# =============================================================================

def plot_results(
    df: pd.DataFrame,
    predictions: pd.Series,
    train_end_idx: int,
    save_path: Optional[Path] = None,
):
    """Plot the prediction results."""
    
    fig, axes = plt.subplots(3, 1, figsize=(16, 12), sharex=True)
    
    # Convert timestamps to datetime for plotting
    df = df.copy()
    df["dt"] = pd.to_datetime(df["ts_ns"], unit="ns", utc=True)
    df["prediction"] = predictions
    
    # Get train/test boundary
    train_end_dt = df.loc[train_end_idx, "dt"]
    
    # --- Plot 1: All exchange mids ---
    ax1 = axes[0]
    
    colors = {
        "lighter": "black",
        "binance": "blue",
        "bybit": "green",
        "bitget": "orange",
        "gate": "purple",
        "okx": "red",
    }
    
    for exchange in EXCHANGES:
        ex_data = df[df["exchange"] == exchange]
        if len(ex_data) == 0:
            continue
        ax1.scatter(
            ex_data["dt"],
            ex_data["mid"],
            c=colors.get(exchange, "gray"),
            alpha=0.3,
            s=1,
            label=f"{exchange}",
        )
    
    ax1.axvline(train_end_dt, color="red", linestyle="--", linewidth=2, label="Train/Test Split")
    ax1.set_ylabel("Price")
    ax1.set_title("Exchange Mid Prices (raw)")
    ax1.legend(loc="upper left", markerscale=5)
    ax1.grid(True, alpha=0.3)
    
    # --- Plot 2: Lighter mid vs Prediction ---
    ax2 = axes[1]
    
    lighter_df = df[df["exchange"] == "lighter"].copy()
    
    ax2.scatter(
        lighter_df["dt"],
        lighter_df["mid"],
        c="black",
        alpha=0.6,
        s=3,
        label="Lighter Mid (actual)",
    )
    ax2.scatter(
        lighter_df["dt"],
        lighter_df["prediction"],
        c="red",
        alpha=0.4,
        s=2,
        label="Prediction",
    )
    
    ax2.axvline(train_end_dt, color="red", linestyle="--", linewidth=2)
    ax2.set_ylabel("Price")
    ax2.set_title("Lighter Mid Price vs Prediction")
    ax2.legend(loc="upper left", markerscale=5)
    ax2.grid(True, alpha=0.3)
    
    # --- Plot 3: Prediction Error ---
    ax3 = axes[2]
    
    lighter_df["error"] = lighter_df["prediction"] - lighter_df["mid"]
    
    train_lighter = lighter_df[lighter_df.index <= train_end_idx]
    test_lighter = lighter_df[lighter_df.index > train_end_idx]
    
    ax3.scatter(
        train_lighter["dt"],
        train_lighter["error"],
        c="blue",
        alpha=0.3,
        s=2,
        label="In-sample error",
    )
    ax3.scatter(
        test_lighter["dt"],
        test_lighter["error"],
        c="red",
        alpha=0.3,
        s=2,
        label="Out-of-sample error",
    )
    
    ax3.axhline(0, color="black", linestyle="-", linewidth=1)
    ax3.axvline(train_end_dt, color="red", linestyle="--", linewidth=2)
    ax3.set_ylabel("Prediction Error ($)")
    ax3.set_xlabel("Time (UTC)")
    ax3.set_title("Prediction Error (Prediction - Actual)")
    ax3.legend(loc="upper left", markerscale=5)
    ax3.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches="tight")
        print(f"Plot saved to {save_path}")
    
    plt.show()


def plot_error_distribution(
    df: pd.DataFrame,
    predictions: pd.Series,
    train_end_idx: int,
):
    """Plot error distribution histograms."""
    
    lighter_mask = df["exchange"] == "lighter"
    lighter_df = df[lighter_mask].copy()
    lighter_df["prediction"] = predictions.reindex(lighter_df.index)
    lighter_df["error"] = lighter_df["prediction"] - lighter_df["mid"]
    
    test_errors = lighter_df[lighter_df.index > train_end_idx]["error"].dropna()
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    # Histogram
    ax1 = axes[0]
    ax1.hist(test_errors, bins=100, alpha=0.7, edgecolor='black')
    ax1.axvline(0, color='red', linestyle='--', linewidth=2)
    ax1.axvline(test_errors.mean(), color='green', linestyle='--', linewidth=2, label=f'Mean: {test_errors.mean():.2f}')
    ax1.set_xlabel("Prediction Error ($)")
    ax1.set_ylabel("Frequency")
    ax1.set_title("Out-of-Sample Error Distribution")
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # QQ plot / cumulative
    ax2 = axes[1]
    sorted_errors = np.sort(test_errors.abs())
    percentiles = np.arange(1, len(sorted_errors) + 1) / len(sorted_errors) * 100
    ax2.plot(sorted_errors, percentiles)
    ax2.axhline(50, color='gray', linestyle='--', alpha=0.5)
    ax2.axhline(95, color='orange', linestyle='--', alpha=0.5)
    ax2.axhline(99, color='red', linestyle='--', alpha=0.5)
    ax2.set_xlabel("Absolute Error ($)")
    ax2.set_ylabel("Percentile")
    ax2.set_title("Cumulative Error Distribution")
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.show()


def plot_zoomed_comparison(
    df: pd.DataFrame,
    predictions: pd.Series,
    train_end_idx: int,
    window_seconds: float = 30.0,
):
    """Plot zoomed view comparing prediction to actual."""
    
    df = df.copy()
    df["dt"] = pd.to_datetime(df["ts_ns"], unit="ns", utc=True)
    df["prediction"] = predictions
    
    # Get a window in the test set
    train_end_ts = df.loc[train_end_idx, "ts_ns"]
    window_start = train_end_ts + int(10 * 1e9)  # 10 seconds into test
    window_end = window_start + int(window_seconds * 1e9)
    
    mask = (df["ts_ns"] >= window_start) & (df["ts_ns"] <= window_end)
    window_df = df[mask].copy()
    
    fig, axes = plt.subplots(2, 1, figsize=(16, 10), sharex=True)
    
    # Plot 1: Prices
    ax1 = axes[0]
    
    lighter_window = window_df[window_df["exchange"] == "lighter"]
    
    # Plot all exchanges
    for exchange in ["binance", "bybit", "okx", "gate"]:
        ex_data = window_df[window_df["exchange"] == exchange]
        if len(ex_data) > 0:
            ax1.scatter(
                ex_data["dt"],
                ex_data["mid"],
                alpha=0.6,
                s=10,
                label=f"{exchange}",
            )
    
    ax1.scatter(
        lighter_window["dt"],
        lighter_window["mid"],
        c="black",
        s=30,
        zorder=5,
        label="Lighter (actual)",
    )
    
    # Plot predictions at lighter timestamps
    ax1.scatter(
        lighter_window["dt"],
        lighter_window["prediction"],
        c="red",
        s=20,
        marker="x",
        zorder=6,
        label="Prediction",
    )
    
    ax1.set_ylabel("Price ($)")
    ax1.set_title(f"Zoomed View: {window_seconds}s Window (Out-of-Sample)")
    ax1.legend(loc="upper left")
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: Error
    ax2 = axes[1]
    
    lighter_window["error"] = lighter_window["prediction"] - lighter_window["mid"]
    
    ax2.bar(
        lighter_window["dt"],
        lighter_window["error"],
        width=pd.Timedelta(milliseconds=50),
        alpha=0.7,
    )
    ax2.axhline(0, color="black", linestyle="-", linewidth=1)
    ax2.set_ylabel("Error ($)")
    ax2.set_xlabel("Time (UTC)")
    ax2.set_title("Prediction Error")
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.show()


# =============================================================================
# Main
# =============================================================================

def main():
    # Load data
    df = load_data(DATA_PATH)
    
    # Train/test split (80/20)
    n = len(df)
    train_end_idx = int(n * TRAIN_FRACTION)
    print(f"\nData split: {train_end_idx:,} train, {n - train_end_idx:,} test")
    
    # Run predictor
    print("\n" + "=" * 60)
    print("Running Price Predictor")
    print("=" * 60)
    
    predictor = LighterPricePredictor()
    
    print(f"Processing {len(df):,} updates...")
    n_nan_predictions = 0
    n_inf_predictions = 0
    
    for idx, row in df.iterrows():
        pred = predictor.process_update(
            row["ts_ns"],
            row["exchange"],
            row["feed"],
            row["mid"],
        )
        
        if np.isnan(pred):
            n_nan_predictions += 1
        elif np.isinf(pred):
            n_inf_predictions += 1
        
        if idx % 50000 == 0:
            print(f"  Processed {idx:,} / {len(df):,} rows... (NaN: {n_nan_predictions}, Inf: {n_inf_predictions})")
    
    print(f"\nTotal invalid predictions: NaN={n_nan_predictions}, Inf={n_inf_predictions}")
    
    # Get predictions
    results = predictor.get_results()
    predictions = pd.Series(results["prediction"].values, index=df.index)
    
    # Evaluate
    print("\n" + "=" * 60)
    print("Evaluation Metrics")
    print("=" * 60)
    
    metrics = evaluate_predictions(df, predictions, train_end_idx)
    naive = compute_naive_baseline(df, train_end_idx)
    crossex = compute_cross_exchange_baseline(df, train_end_idx)
    
    print("\nModel Performance:")
    for key, value in sorted(metrics.items()):
        if "_n" in key:
            print(f"  {key}: {value:,}")
        else:
            print(f"  {key}: {value:.4f}")
    
    print("\nNaive Baseline (Previous Lighter Mid):")
    for key, value in sorted(naive.items()):
        if "_n" in key:
            print(f"  {key}: {value:,}")
        else:
            print(f"  {key}: {value:.4f}")
    
    print("\nCross-Exchange Baseline (Most Recent Other Exchange):")
    for key, value in sorted(crossex.items()):
        if "_n" in key:
            print(f"  {key}: {value:,}")
        else:
            print(f"  {key}: {value:.4f}")
    
    # Compare to baselines
    if "test_rmse" in metrics and "naive_test_rmse" in naive:
        vs_naive = (naive["naive_test_rmse"] - metrics["test_rmse"]) / naive["naive_test_rmse"] * 100
        print(f"\nRMSE Improvement vs Naive: {vs_naive:.1f}%")
    
    if "test_rmse" in metrics and "crossex_test_rmse" in crossex:
        vs_crossex = (crossex["crossex_test_rmse"] - metrics["test_rmse"]) / crossex["crossex_test_rmse"] * 100
        print(f"RMSE Improvement vs Cross-Exchange: {vs_crossex:.1f}%")
    
    # Show exchange bias estimates
    print("\n" + "=" * 60)
    print("Learned Exchange Biases (vs Lighter)")
    print("=" * 60)
    for ex_name, tracker in predictor.trackers.items():
        if ex_name != "lighter":
            print(f"  {ex_name:10s}: bias = ${tracker.estimated_bias:+.2f} (confidence: {tracker.bias_confidence:.2f})")
    
    # Plot results
    print("\n" + "=" * 60)
    print("Generating Plots")
    print("=" * 60)
    
    plot_results(df, predictions, train_end_idx)
    plot_error_distribution(df, predictions, train_end_idx)
    plot_zoomed_comparison(df, predictions, train_end_idx, window_seconds=30.0)
    
    return df, predictions, metrics, predictor


if __name__ == "__main__":
    df, predictions, metrics, predictor = main()
