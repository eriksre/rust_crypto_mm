#!/usr/bin/env python3
"""
Lighter Price Predictor - "Fastest Credible Signal" Model

This is NOT a smoothing/averaging model. That would be fatal for HFT market making.

The key insight: We want to INSTANTLY react to REAL price moves, while IGNORING
stale/noisy data. This is fundamentally different from Kalman filtering or averaging.

The approach:
1. Track the "consensus price" across exchanges (bias-adjusted)
2. When a new update comes in, decide: Is this a REAL MOVE or NOISE/STALE?
3. If REAL MOVE: Jump instantly to new price
4. If NOISE/STALE: Ignore it, keep current price

How to detect real moves vs noise:
- Real moves: Multiple exchanges agree (within short time window)
- Stale data: One exchange disagrees with recent consensus
- Use engine timestamps to know actual event time, not arrival time

The model outputs the price we SHOULD quote around right now.
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from typing import Dict, Optional, Tuple, List
from dataclasses import dataclass, field
from collections import deque
import argparse


# =============================================================================
# Configuration
# =============================================================================

DEFAULT_DATA_PATH = Path(__file__).parent.parent / "logs" / "lighter_activity.csv"
TRAIN_FRACTION = 0.80
EXCHANGES = ["binance", "bybit", "bitget", "gate", "okx", "lighter"]

# Time windows
CONSENSUS_WINDOW_NS = 100_000_000  # 100ms - window to check for consensus
STALE_THRESHOLD_NS = 500_000_000   # 500ms - data older than this is stale
BIAS_ALIGN_WINDOW_NS = 200_000_000  # 200ms - max time diff for bias pair alignment

# Outlier detection
OUTLIER_THRESHOLD_MULT = 3.0  # If price differs by more than 3x recent volatility, it's outlier

# Exchange reliability (used for tie-breaking, not averaging)
EXCHANGE_PRIORITY = {
    "lighter": 100,  # Always trust lighter most (it's what we're trading on)
    "binance": 90,   # Most liquid
    "bybit": 80,
    "okx": 70,
    "bitget": 50,
    "gate": 40,
}


# =============================================================================
# Data Loading
# =============================================================================

def load_data(path: Path) -> pd.DataFrame:
    """Load and preprocess the lighter activity CSV."""
    print(f"Loading data from {path}...")
    df = pd.read_csv(path, low_memory=False)
    
    # Filter out our own quotes
    market_data = df[df["event_type"] == "market"].copy()
    
    # Convert numeric columns
    numeric_cols = ["ts_ns", "price", "size", "source_engine_ts_ns", "source_system_ts_ns"]
    bid_ask_cols = [c for c in market_data.columns if c.startswith(("bid_", "ask_"))]
    for col in numeric_cols + bid_ask_cols:
        if col in market_data.columns:
            market_data[col] = pd.to_numeric(market_data[col], errors="coerce")
    
    # Sort by arrival timestamp
    market_data = market_data.sort_values("ts_ns").reset_index(drop=True)
    
    # Compute mid from BBO (NOT for trades)
    has_bbo = market_data["bid_px_1"].notna() & market_data["ask_px_1"].notna()
    market_data["mid"] = np.nan
    market_data.loc[has_bbo, "mid"] = (
        market_data.loc[has_bbo, "bid_px_1"] + market_data.loc[has_bbo, "ask_px_1"]
    ) / 2.0
    
    # Trade price separate
    is_trade = market_data["feed"] == "trade"
    market_data["trade_price"] = np.nan
    market_data.loc[is_trade, "trade_price"] = market_data.loc[is_trade, "price"]
    
    print(f"Loaded {len(market_data):,} market data rows")
    print(f"Exchanges: {market_data['exchange'].unique().tolist()}")
    
    return market_data


# =============================================================================
# Bias Tracker (Time-Aligned)
# =============================================================================

@dataclass
class BiasTracker:
    """
    Track bias for each exchange vs lighter using time-aligned pairs.
    Bias = exchange_price - lighter_price
    """
    exchange: str
    
    # Recent observations for alignment
    exchange_obs: deque = field(default_factory=lambda: deque(maxlen=500))
    lighter_obs: deque = field(default_factory=lambda: deque(maxlen=500))
    
    # Bias estimate
    bias: float = 0.0
    n_pairs: int = 0
    
    def add_exchange_obs(self, engine_ts: int, mid: float):
        """Add exchange observation with its ENGINE timestamp."""
        if np.isfinite(mid) and engine_ts > 0:
            self.exchange_obs.append((engine_ts, mid))
    
    def add_lighter_obs(self, engine_ts: int, mid: float):
        """Add lighter observation and update bias from aligned pairs."""
        if not np.isfinite(mid) or engine_ts <= 0:
            return
        
        self.lighter_obs.append((engine_ts, mid))
        
        # Find nearest exchange observation by ENGINE timestamp
        best_match = None
        best_dt = float('inf')
        
        for ex_ts, ex_mid in self.exchange_obs:
            dt = abs(engine_ts - ex_ts)
            if dt < best_dt and dt < BIAS_ALIGN_WINDOW_NS:
                best_dt = dt
                best_match = ex_mid
        
        if best_match is not None:
            diff = best_match - mid  # exchange - lighter
            # Fast EMA update
            alpha = 0.1
            self.bias = alpha * diff + (1 - alpha) * self.bias
            self.n_pairs += 1
    
    def correct(self, exchange_mid: float) -> float:
        """Return bias-corrected price (estimate in lighter terms)."""
        return exchange_mid - self.bias


# =============================================================================
# Price State Tracker
# =============================================================================

@dataclass 
class ExchangeState:
    """Track the latest state from each exchange."""
    exchange: str
    last_mid: Optional[float] = None
    last_engine_ts: Optional[int] = None  # When event happened at exchange
    last_arrival_ts: Optional[int] = None  # When we received it
    last_corrected_mid: Optional[float] = None  # Bias-corrected


# =============================================================================
# Fastest Credible Signal Model
# =============================================================================

class FastestCredibleSignal:
    """
    Model that tracks the "fastest credible signal" rather than averaging.
    
    Key principles:
    1. NEVER smooth or average - that causes slow drift
    2. Jump instantly to new consensus
    3. Ignore outliers and stale data
    4. Trust lighter directly when available
    """
    
    def __init__(self):
        # Bias trackers
        self.bias_trackers: Dict[str, BiasTracker] = {
            ex: BiasTracker(exchange=ex) for ex in EXCHANGES if ex != "lighter"
        }
        
        # Exchange states
        self.states: Dict[str, ExchangeState] = {
            ex: ExchangeState(exchange=ex) for ex in EXCHANGES
        }
        
        # Current best estimate
        self.current_price: Optional[float] = None
        self.current_price_ts: Optional[int] = None
        self.current_price_source: Optional[str] = None
        
        # Recent prices for volatility estimation
        self.recent_prices: deque = field(default_factory=lambda: deque(maxlen=100))
        self.recent_prices = deque(maxlen=100)
        
        # Results
        self.lighter_priors = []  # (ts, prior, actual)
        self.all_estimates = []   # (ts, estimate, source)
    
    def _get_engine_ts(self, row_ts: int, source_engine_ts: Optional[float]) -> int:
        """Get the engine timestamp, falling back to arrival if not available."""
        if source_engine_ts and np.isfinite(source_engine_ts) and source_engine_ts > 0:
            return int(source_engine_ts)
        return row_ts
    
    def _is_stale(self, engine_ts: int, current_ts: int) -> bool:
        """Check if data is too old to trust."""
        age = current_ts - engine_ts
        return age > STALE_THRESHOLD_NS
    
    def _is_outlier(self, price: float) -> bool:
        """Check if price is an outlier compared to recent history."""
        if len(self.recent_prices) < 10:
            return False
        
        recent = list(self.recent_prices)
        mean = np.mean(recent)
        std = np.std(recent)
        
        if std < 0.01:  # Very low volatility, be conservative
            std = 0.5
        
        return abs(price - mean) > OUTLIER_THRESHOLD_MULT * std
    
    def _get_consensus_price(self, arrival_ts: int, exclude_exchange: Optional[str] = None) -> Optional[float]:
        """
        Get consensus price from recent, non-stale, non-outlier exchange data.
        
        This is NOT an average - we find exchanges that agree and take their price.
        """
        valid_prices = []
        valid_priorities = []
        
        for ex_name, state in self.states.items():
            if ex_name == exclude_exchange:
                continue
            if state.last_corrected_mid is None:
                continue
            if state.last_engine_ts is None:
                continue
            
            # Check staleness
            if self._is_stale(state.last_engine_ts, arrival_ts):
                continue
            
            # Check outlier
            if self._is_outlier(state.last_corrected_mid):
                continue
            
            valid_prices.append(state.last_corrected_mid)
            valid_priorities.append(EXCHANGE_PRIORITY.get(ex_name, 0))
        
        if not valid_prices:
            return None
        
        # Return the price from the highest-priority valid exchange
        # NOT an average - winner takes all
        best_idx = np.argmax(valid_priorities)
        return valid_prices[best_idx]
    
    def _should_update_price(
        self,
        new_price: float,
        source_exchange: str,
        engine_ts: int,
        arrival_ts: int,
    ) -> bool:
        """
        Decide whether to update our price estimate based on new data.
        
        KEY INSIGHT: Lighter usually doesn't move (naive baseline median = 0).
        So we should be VERY conservative about updating from other exchanges.
        
        Update if:
        1. This is from lighter (always trust lighter for lighter's price)
        2. From other exchange: ONLY if there's a SIGNIFICANT move (> threshold)
           AND it's not stale AND not outlier AND agrees with consensus
        """
        # Always update from lighter (unless clearly stale)
        if source_exchange == "lighter":
            if not self._is_stale(engine_ts, arrival_ts):
                return True
            return False
        
        # For non-lighter exchanges: be VERY conservative
        # Only update if there's a significant move from current price
        
        # Check if stale
        if self._is_stale(engine_ts, arrival_ts):
            return False
        
        # Check if outlier
        if self._is_outlier(new_price):
            return False
        
        # Must have a current price to compare against
        if self.current_price is None:
            return True  # Initialize
        
        # CRITICAL: Only update if the move is SIGNIFICANT
        # Small moves are likely noise - stick with lighter's last price
        price_diff = abs(new_price - self.current_price)
        SIGNIFICANT_MOVE_THRESHOLD = 0.5  # Only react to moves > $0.50
        
        if price_diff < SIGNIFICANT_MOVE_THRESHOLD:
            return False  # Small move, probably noise, ignore
        
        # For significant moves, check consensus
        consensus = self._get_consensus_price(arrival_ts, exclude_exchange=source_exchange)
        if consensus is not None:
            # The significant move should agree with consensus
            consensus_diff = abs(new_price - consensus)
            if consensus_diff > 1.0:  # Disagrees with consensus
                return False
        
        # Significant move that agrees with consensus - update!
        return True
    
    def process_update(
        self,
        arrival_ts: int,
        exchange: str,
        feed: str,
        mid: Optional[float],
        trade_price: Optional[float],
        source_engine_ts: Optional[float],
    ) -> Tuple[float, str]:
        """
        Process an update and return (current_estimate, source).
        
        For lighter updates: stores PRIOR for evaluation.
        """
        # Get engine timestamp
        engine_ts = self._get_engine_ts(arrival_ts, source_engine_ts)
        
        # Determine observation value (ignore trades for now - too noisy)
        obs = mid if feed != "trade" else None
        
        # For lighter: store prior BEFORE updating
        if exchange == "lighter" and obs is not None and np.isfinite(obs):
            prior = self.current_price if self.current_price else obs
            self.lighter_priors.append((arrival_ts, prior, obs))
        
        # Skip if no valid observation
        if obs is None or not np.isfinite(obs):
            est = self.current_price if self.current_price else 0.0
            src = self.current_price_source if self.current_price_source else "none"
            self.all_estimates.append((arrival_ts, est, src))
            return est, src
        
        # Update bias trackers
        if exchange == "lighter":
            for ex_name, tracker in self.bias_trackers.items():
                tracker.add_lighter_obs(engine_ts, obs)
            corrected_mid = obs  # Lighter is reference, no correction needed
        else:
            self.bias_trackers[exchange].add_exchange_obs(engine_ts, obs)
            corrected_mid = self.bias_trackers[exchange].correct(obs)
        
        # Update exchange state
        self.states[exchange].last_mid = obs
        self.states[exchange].last_engine_ts = engine_ts
        self.states[exchange].last_arrival_ts = arrival_ts
        self.states[exchange].last_corrected_mid = corrected_mid
        
        # Update recent prices for volatility
        self.recent_prices.append(corrected_mid)
        
        # Decide whether to update our estimate
        if self._should_update_price(corrected_mid, exchange, engine_ts, arrival_ts):
            self.current_price = corrected_mid
            self.current_price_ts = engine_ts
            self.current_price_source = exchange
        
        # Return current estimate
        est = self.current_price if self.current_price else corrected_mid
        src = self.current_price_source if self.current_price_source else exchange
        self.all_estimates.append((arrival_ts, est, src))
        
        return est, src
    
    def get_lighter_priors(self) -> pd.DataFrame:
        """Return PRIOR predictions at lighter timestamps."""
        if not self.lighter_priors:
            return pd.DataFrame(columns=["ts_ns", "prior", "actual"])
        return pd.DataFrame(self.lighter_priors, columns=["ts_ns", "prior", "actual"])
    
    def get_all_estimates(self) -> pd.DataFrame:
        """Return all estimates."""
        return pd.DataFrame(self.all_estimates, columns=["ts_ns", "estimate", "source"])


# =============================================================================
# Evaluation
# =============================================================================

def evaluate_priors(priors_df: pd.DataFrame, train_end_ts: int) -> Dict[str, float]:
    """Evaluate on PRIOR predictions (before seeing lighter)."""
    if priors_df.empty:
        return {}
    
    train = priors_df[priors_df["ts_ns"] <= train_end_ts]
    test = priors_df[priors_df["ts_ns"] > train_end_ts]
    
    metrics = {}
    
    for name, subset in [("train", train), ("test", test)]:
        if subset.empty:
            continue
        
        errors = subset["prior"] - subset["actual"]
        errors = errors[np.isfinite(errors)]
        
        if len(errors) == 0:
            continue
        
        metrics[f"{name}_mae"] = errors.abs().mean()
        metrics[f"{name}_rmse"] = np.sqrt((errors ** 2).mean())
        metrics[f"{name}_mean_error"] = errors.mean()
        metrics[f"{name}_n"] = len(errors)
        metrics[f"{name}_median_error"] = errors.abs().median()
        metrics[f"{name}_p95_error"] = errors.abs().quantile(0.95)
        metrics[f"{name}_p99_error"] = errors.abs().quantile(0.99)
        
        # Percentage of times we're within thresholds
        metrics[f"{name}_within_0.1"] = (errors.abs() <= 0.1).mean() * 100
        metrics[f"{name}_within_0.5"] = (errors.abs() <= 0.5).mean() * 100
        metrics[f"{name}_within_1.0"] = (errors.abs() <= 1.0).mean() * 100
    
    return metrics


def compute_naive_baseline(df: pd.DataFrame, train_end_ts: int) -> Dict[str, float]:
    """Naive: previous lighter mid → current lighter mid."""
    lighter_mask = (df["exchange"] == "lighter") & (df["feed"] != "trade")
    lighter_df = df[lighter_mask & df["mid"].notna()].copy()
    
    if lighter_df.empty:
        return {}
    
    lighter_df["prev_mid"] = lighter_df["mid"].shift(1)
    test = lighter_df[lighter_df["ts_ns"] > train_end_ts].dropna(subset=["mid", "prev_mid"])
    
    if test.empty:
        return {}
    
    errors = test["prev_mid"] - test["mid"]
    return {
        "naive_test_mae": errors.abs().mean(),
        "naive_test_rmse": np.sqrt((errors ** 2).mean()),
        "naive_test_n": len(errors),
        "naive_test_median": errors.abs().median(),
        "naive_test_p95": errors.abs().quantile(0.95),
        "naive_test_within_0.1": (errors.abs() <= 0.1).mean() * 100,
        "naive_test_within_0.5": (errors.abs() <= 0.5).mean() * 100,
        "naive_test_within_1.0": (errors.abs() <= 1.0).mean() * 100,
    }


# =============================================================================
# Visualization
# =============================================================================

def plot_results(df: pd.DataFrame, priors_df: pd.DataFrame, train_end_ts: int):
    """
    Two-panel step graph:
    1. Top: Step graph of lighter actual, model prediction, and other exchange mids
    2. Bottom: Step graph of prediction error
    """
    if priors_df.empty:
        print("No priors to plot!")
        return
    
    fig, axes = plt.subplots(2, 1, figsize=(16, 10), sharex=True)
    
    # Convert timestamps
    df = df.copy()
    df["dt"] = pd.to_datetime(df["ts_ns"], unit="ns", utc=True)
    
    priors_df = priors_df.copy()
    priors_df["dt"] = pd.to_datetime(priors_df["ts_ns"], unit="ns", utc=True)
    priors_df["error"] = priors_df["prior"] - priors_df["actual"]
    priors_df = priors_df.sort_values("dt")
    
    train_end_dt = pd.to_datetime(train_end_ts, unit="ns", utc=True)
    
    # --- Plot 1: Step graph of prices ---
    ax1 = axes[0]
    
    colors = {
        "lighter": "black",
        "binance": "blue",
        "bybit": "green",
        "bitget": "orange",
        "gate": "purple",
        "okx": "red",
    }
    
    # Plot other exchanges first (background, more transparent)
    plot_df = df[(df["feed"] != "trade") & df["mid"].notna()].copy()
    
    for exchange in ["binance", "bybit", "bitget", "gate", "okx"]:
        ex_data = plot_df[plot_df["exchange"] == exchange].sort_values("dt")
        if len(ex_data) == 0:
            continue
        ax1.step(
            ex_data["dt"],
            ex_data["mid"],
            where="post",
            color=colors.get(exchange, "gray"),
            alpha=0.4,
            linewidth=0.5,
            label=exchange,
        )
    
    # Plot lighter actual (solid black)
    lighter_data = plot_df[plot_df["exchange"] == "lighter"].sort_values("dt")
    if len(lighter_data) > 0:
        ax1.step(
            lighter_data["dt"],
            lighter_data["mid"],
            where="post",
            color="black",
            alpha=0.9,
            linewidth=1.5,
            label="lighter (actual)",
        )
    
    # Plot model prediction (red, dashed)
    ax1.step(
        priors_df["dt"],
        priors_df["prior"],
        where="post",
        color="red",
        alpha=0.8,
        linewidth=1.5,
        linestyle="--",
        label="prediction",
    )
    
    ax1.axvline(train_end_dt, color="green", linestyle="--", linewidth=2, label="Train/Test Split")
    ax1.set_ylabel("Price ($)")
    ax1.set_title("Exchange Mid Prices (Step Graph)")
    ax1.legend(loc="upper left")
    ax1.grid(True, alpha=0.3)
    
    # --- Plot 2: Step graph of error ---
    ax2 = axes[1]
    
    ax2.step(
        priors_df["dt"],
        priors_df["error"],
        where="post",
        color="black",
        alpha=0.7,
        linewidth=1,
    )
    
    # Color the background by train/test
    ax2.axvspan(priors_df["dt"].min(), train_end_dt, alpha=0.1, color="blue", label="Train")
    ax2.axvspan(train_end_dt, priors_df["dt"].max(), alpha=0.1, color="red", label="Test")
    
    ax2.axhline(0, color="black", linestyle="-", linewidth=1)
    ax2.axvline(train_end_dt, color="green", linestyle="--", linewidth=2)
    ax2.set_ylabel("Error ($)")
    ax2.set_xlabel("Time (UTC)")
    ax2.set_title("Prediction Error (Prior - Actual)")
    ax2.legend(loc="upper left")
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.show()


# =============================================================================
# Main
# =============================================================================

def main(data_path: Optional[Path] = None):
    path = data_path or DEFAULT_DATA_PATH
    df = load_data(path)
    
    n = len(df)
    train_end_idx = int(n * TRAIN_FRACTION)
    train_end_ts = df.loc[train_end_idx, "ts_ns"]
    
    print(f"\nData split: {train_end_idx:,} train, {n - train_end_idx:,} test")
    
    # Run model
    print("\n" + "=" * 60)
    print("Running Fastest Credible Signal Model")
    print("=" * 60)
    
    model = FastestCredibleSignal()
    
    print(f"Processing {len(df):,} updates...")
    for idx, row in df.iterrows():
        model.process_update(
            arrival_ts=row["ts_ns"],
            exchange=row["exchange"],
            feed=row["feed"],
            mid=row["mid"] if pd.notna(row.get("mid")) else None,
            trade_price=row["trade_price"] if pd.notna(row.get("trade_price")) else None,
            source_engine_ts=row["source_engine_ts_ns"] if pd.notna(row.get("source_engine_ts_ns")) else None,
        )
        
        if idx % 100000 == 0:
            print(f"  Processed {idx:,} / {len(df):,}")
    
    priors_df = model.get_lighter_priors()
    estimates_df = model.get_all_estimates()
    
    print(f"\nCollected {len(priors_df):,} lighter prior predictions")
    
    # Evaluate
    print("\n" + "=" * 60)
    print("Evaluation")
    print("=" * 60)
    
    metrics = evaluate_priors(priors_df, train_end_ts)
    naive = compute_naive_baseline(df, train_end_ts)
    
    print("\nModel Performance (Prior Predictions):")
    for key, value in sorted(metrics.items()):
        if "_n" in key:
            print(f"  {key}: {value:,}")
        elif "within" in key:
            print(f"  {key}: {value:.1f}%")
        else:
            print(f"  {key}: {value:.4f}")
    
    print("\nNaive Baseline (Previous Lighter Mid):")
    for key, value in sorted(naive.items()):
        if "_n" in key:
            print(f"  {key}: {value:,}")
        elif "within" in key:
            print(f"  {key}: {value:.1f}%")
        else:
            print(f"  {key}: {value:.4f}")
    
    if "test_rmse" in metrics and "naive_test_rmse" in naive:
        improvement = (naive["naive_test_rmse"] - metrics["test_rmse"]) / naive["naive_test_rmse"] * 100
        print(f"\nRMSE Improvement vs Naive: {improvement:.1f}%")
    
    # Biases
    print("\n" + "=" * 60)
    print("Learned Biases")
    print("=" * 60)
    for ex, tracker in model.bias_trackers.items():
        print(f"  {ex:10s}: bias=${tracker.bias:+.2f} (n={tracker.n_pairs:,})")
    
    # Source stats
    print("\n" + "=" * 60)
    print("Price Source Statistics (Test Period)")
    print("=" * 60)
    test_estimates = estimates_df[estimates_df["ts_ns"] > train_end_ts]
    source_counts = test_estimates["source"].value_counts()
    total = len(test_estimates)
    for src, count in source_counts.items():
        print(f"  {src:10s}: {count:,} ({count/total*100:.1f}%)")
    
    # Plot
    print("\n" + "=" * 60)
    print("Generating Plots")
    print("=" * 60)
    
    plot_results(df, priors_df, train_end_ts)
    
    return df, priors_df, metrics, model


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, default=None)
    args = parser.parse_args()
    
    df, priors_df, metrics, model = main(args.data)
