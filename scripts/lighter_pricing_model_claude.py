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

# Latency offset for evaluation
# We predict where lighter will be AFTER this delay (cancellation latency)
LATENCY_OFFSET_NS = 700_000_000  # 700ms - adjust based on your actual latency

# Outlier detection
OUTLIER_THRESHOLD_MULT = 3.0  # If price differs by more than 3x recent volatility, it's outlier

# Error correction (AR coefficients from fitting AR(5) to errors)
# These will be learned from training data
ERROR_CORRECTION_ENABLED = True
ERROR_CORRECTION_AR_ORDER = 5
# Damping factor: multiply learned coefficients by this (< 1.0 = more conservative)
ERROR_CORRECTION_DAMPING = 0.6  # Tuned value
# Number of iterations to refine error correction coefficients
ERROR_CORRECTION_ITERATIONS = 1  # Just one learning pass
# Initial AR coefficients (will be updated from training)
DEFAULT_AR_COEFFS = [0.7116, 0.2547, 0.0702, 0.0179, -0.0811]
# Online error correction: EMA of recent error to add real-time adaptation
ONLINE_ERROR_CORRECTION_ENABLED = True
ONLINE_ERROR_EMA_ALPHA = 0.3  # How fast to adapt to recent errors

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
            # SLOW EMA update - we want to capture the "normal" level difference,
            # not adapt to every short-term price move
            # With alpha=0.01, it takes ~100 observations to mostly adapt to a new level
            alpha = 0.01
            self.bias = alpha * diff + (1 - alpha) * self.bias
            self.n_pairs += 1
    
    def correct(self, exchange_mid: float) -> float:
        """Return bias-corrected price (estimate in lighter terms)."""
        return exchange_mid - self.bias


# =============================================================================
# Price State Tracker (Enhanced with Velocity)
# =============================================================================

@dataclass 
class ExchangeState:
    """Track the latest state from each exchange, including velocity."""
    exchange: str
    last_mid: Optional[float] = None
    last_engine_ts: Optional[int] = None  # When event happened at exchange
    last_arrival_ts: Optional[int] = None  # When we received it
    last_corrected_mid: Optional[float] = None  # Bias-corrected
    
    # Velocity/momentum tracking
    prev_corrected_mid: Optional[float] = None  # Previous corrected price
    prev_engine_ts: Optional[int] = None
    last_change: float = 0.0  # Most recent price change (corrected)
    velocity: float = 0.0  # EMA of price velocity
    velocity_ema_alpha: float = 0.3  # How fast to update velocity estimate
    
    def update_with_change(self, new_corrected_mid: float, new_ts: int):
        """Update state and compute the price change."""
        if self.prev_corrected_mid is not None and self.prev_engine_ts is not None:
            dt_ns = new_ts - self.prev_engine_ts
            if dt_ns > 1_000_000:  # At least 1ms between updates
                # Record the price change
                self.last_change = new_corrected_mid - self.prev_corrected_mid
                # Velocity in $/ns
                instant_velocity = self.last_change / dt_ns
                # EMA update
                self.velocity = self.velocity_ema_alpha * instant_velocity + (1 - self.velocity_ema_alpha) * self.velocity
            else:
                self.last_change = 0.0
        else:
            self.last_change = 0.0
        
        # Store for next update
        self.prev_corrected_mid = new_corrected_mid
        self.prev_engine_ts = new_ts
    
    def extrapolate_price(self, target_ts: int) -> Optional[float]:
        """Extrapolate price to target timestamp using velocity."""
        if self.last_corrected_mid is None or self.last_engine_ts is None:
            return None
        
        dt_ns = target_ts - self.last_engine_ts
        # Limit extrapolation to reasonable range
        if abs(dt_ns) > 2_000_000_000:  # Don't extrapolate more than 2 seconds
            return self.last_corrected_mid
        
        return self.last_corrected_mid + self.velocity * dt_ns
    
    def get_recent_change(self) -> float:
        """Get the most recent price change (bias-corrected)."""
        return self.last_change


# =============================================================================
# Momentum-Based Price Predictor
# =============================================================================

class FastestCredibleSignal:
    """
    Model that predicts FUTURE price using momentum from multiple exchanges.
    
    Key principles:
    1. Track price VELOCITY on each exchange, not just levels
    2. Predict where price will be in LATENCY_OFFSET_NS, not where it is now
    3. Use consensus of velocities weighted by exchange reliability
    4. Jump to new price levels but extrapolate with momentum
    
    The high autocorrelation in errors suggests we're always late - 
    this model addresses that by extrapolating forward.
    """
    
    def __init__(self):
        # Bias trackers
        self.bias_trackers: Dict[str, BiasTracker] = {
            ex: BiasTracker(exchange=ex) for ex in EXCHANGES if ex != "lighter"
        }
        
        # Exchange states (now with velocity tracking)
        self.states: Dict[str, ExchangeState] = {
            ex: ExchangeState(exchange=ex) for ex in EXCHANGES
        }
        
        # Current best estimate (of CURRENT price, not future)
        self.current_price: Optional[float] = None
        self.current_price_ts: Optional[int] = None
        self.current_price_source: Optional[str] = None
        
        # Aggregate momentum tracking
        self.aggregate_velocity: float = 0.0  # Combined velocity from all exchanges
        self.velocity_update_count: int = 0
        
        # Recent prices for volatility estimation
        self.recent_prices: deque = deque(maxlen=100)
        
        # Results
        self.lighter_priors = []  # (ts, prior, actual) - evaluated at lighter updates
        self.all_estimates = []   # (ts, estimate, source) - every tick
        self.continuous_estimates = []  # (ts, estimate, exchange_source) - for continuous eval
        
        # Track current estimate continuously
        self.current_lighter_estimate: float = 0.0
        
        # Light velocity (lighter's own momentum)
        self.lighter_velocity: float = 0.0
    
    def _get_engine_ts(self, row_ts: int, source_engine_ts: Optional[float]) -> int:
        """Get the engine timestamp, falling back to arrival if not available."""
        if source_engine_ts and np.isfinite(source_engine_ts) and source_engine_ts > 0:
            return int(source_engine_ts)
        return row_ts
    
    def _is_stale(self, engine_ts: int, current_ts: int) -> bool:
        """Check if data is too old to trust."""
        age = current_ts - engine_ts
        return age > STALE_THRESHOLD_NS
    
    def _get_other_exchange_consensus(self, arrival_ts: int) -> Optional[float]:
        """
        Get consensus price from non-lighter exchanges.
        
        Other exchanges typically LEAD lighter - their current price
        is where lighter will be in the near future.
        """
        valid_prices = []
        valid_weights = []
        
        for ex_name, state in self.states.items():
            if ex_name == "lighter":
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
            
            # Weight by priority AND recency
            priority = EXCHANGE_PRIORITY.get(ex_name, 50)
            age_ns = arrival_ts - state.last_engine_ts
            recency_weight = max(0.1, 1.0 - age_ns / STALE_THRESHOLD_NS)
            
            weight = (priority / 100.0) * recency_weight
            valid_prices.append(state.last_corrected_mid)
            valid_weights.append(weight)
        
        if not valid_prices:
            return None
        
        # Weighted average of other exchange prices
        total_weight = sum(valid_weights)
        if total_weight > 0:
            return sum(p * w for p, w in zip(valid_prices, valid_weights)) / total_weight
        return None
    
    def _get_raw_exchange_consensus(self, arrival_ts: int) -> Optional[float]:
        """
        Get RAW consensus price from non-lighter exchanges (NO bias correction).
        
        Used to see where other exchanges ACTUALLY are in absolute terms,
        ignoring any bias alignment. This reveals true divergence when
        lighter is stale but others have moved.
        """
        valid_prices = []
        valid_weights = []
        
        for ex_name, state in self.states.items():
            if ex_name == "lighter":
                continue
            if state.last_mid is None:  # Use RAW mid, not corrected
                continue
            if state.last_engine_ts is None:
                continue
            
            # Check staleness
            if self._is_stale(state.last_engine_ts, arrival_ts):
                continue
            
            # Weight by priority AND recency
            priority = EXCHANGE_PRIORITY.get(ex_name, 50)
            age_ns = arrival_ts - state.last_engine_ts
            recency_weight = max(0.1, 1.0 - age_ns / STALE_THRESHOLD_NS)
            
            weight = (priority / 100.0) * recency_weight
            valid_prices.append(state.last_mid)
            valid_weights.append(weight)
        
        if not valid_prices:
            return None
        
        # Weighted average of other exchange prices
        total_weight = sum(valid_weights)
        if total_weight > 0:
            return sum(p * w for p, w in zip(valid_prices, valid_weights)) / total_weight
        return None
    
    def _predict_future_price(self, current_price: float, current_ts: int, target_offset_ns: int) -> float:
        """
        Predict where lighter will be in target_offset_ns.
        
        KEY INSIGHT: Other exchanges LEAD lighter. When they move, lighter follows.
        So we predict lighter will CONVERGE TOWARD the current consensus of other exchanges.
        
        The convergence rate depends on how far into the future we're predicting.
        """
        # Get where other exchanges currently are (bias-corrected)
        other_consensus = self._get_other_exchange_consensus(current_ts)
        
        if other_consensus is None:
            return current_price
        
        # How much to converge toward other exchanges' price
        # This is the KEY parameter: how much do we trust other exchanges lead lighter?
        # 
        # If convergence_rate = 1.0: fully trust others, predict lighter = others
        # If convergence_rate = 0.0: don't trust others, predict lighter stays put
        # 
        # For 700ms ahead, we expect significant convergence
        # Let's be more aggressive - if other exchanges have moved, lighter will follow
        convergence_rate = 0.8  # 80% convergence toward other exchanges
        
        # Predict lighter converges toward other exchanges
        gap = other_consensus - current_price
        predicted_price = current_price + convergence_rate * gap
        
        return predicted_price
    
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
        
        KEY INSIGHT: Other exchanges LEAD lighter. When binance/bybit move,
        lighter often follows 100-200ms later. We WANT to react to those moves
        to predict where lighter will be in 700ms.
        
        Update if:
        1. This is from lighter (always trust for lighter's own price)
        2. From other exchange: if not stale, not outlier, and either:
           - More recent (by engine timestamp) than current, OR
           - From a higher-priority exchange
        """
        # Always update from lighter (unless clearly stale)
        if source_exchange == "lighter":
            if not self._is_stale(engine_ts, arrival_ts):
                return True
            return False
        
        # Check if stale
        if self._is_stale(engine_ts, arrival_ts):
            return False
        
        # Check if outlier
        if self._is_outlier(new_price):
            return False
        
        # Must have a current price to compare against
        if self.current_price is None:
            return True  # Initialize
        
        # Check if this is newer by engine timestamp
        if self.current_price_ts is not None and engine_ts > self.current_price_ts:
            # Newer data - update!
            return True
        
        # Same or older timestamp - only update if higher priority source
        if self.current_price_ts is not None:
            current_priority = EXCHANGE_PRIORITY.get(self.current_price_source, 0)
            new_priority = EXCHANGE_PRIORITY.get(source_exchange, 0)
            if new_priority > current_priority:
                return True
        
        return False
    
    def _compute_lighter_estimate(self, arrival_ts: int) -> float:
        """
        Compute our best estimate of where Lighter's price WILL BE after the
        cancellation delay clears (LATENCY_OFFSET_NS into the future).
        
        KEY INSIGHT: Don't blend LEVELS - predict CHANGES.
        When other exchanges move by $X, Lighter will move by ~$X after the delay.
        
        Strategy:
        1. Start with Lighter's last known price as base
        2. Add the recent CHANGE from other exchanges (momentum signal)
        3. This predicts: Lighter_future = Lighter_now + OtherExchanges_recent_change
        """
        lighter_state = self.states["lighter"]
        lighter_last = lighter_state.last_corrected_mid
        
        if lighter_last is None:
            # No lighter data yet - use consensus or any price
            consensus = self._get_other_exchange_consensus(arrival_ts)
            if consensus is not None:
                return consensus
            return self.current_price if self.current_price else 0.0
        
        # Compute aggregate CHANGE from other exchanges
        # This is the momentum signal: how much have other exchanges moved recently?
        total_change = 0.0
        total_weight = 0.0
        
        for ex_name, state in self.states.items():
            if ex_name == "lighter":
                continue
            if state.last_corrected_mid is None:
                continue
            if state.last_engine_ts is None:
                continue
            
            # Use the pre-computed last_change from the exchange
            change = state.last_change
            
            # Skip if no meaningful change recorded
            if abs(change) < 0.001:
                continue
            
            # Weight by:
            # 1. Exchange priority (more liquid = more trusted)
            # 2. Recency of the update
            priority_weight = EXCHANGE_PRIORITY.get(ex_name, 50) / 100.0
            
            # More recent changes get more weight
            age_ns = arrival_ts - state.last_engine_ts
            recency_weight = max(0.1, 1.0 - age_ns / STALE_THRESHOLD_NS)
            
            weight = priority_weight * recency_weight
            total_change += change * weight
            total_weight += weight
        
        if total_weight > 0:
            # Average weighted change from other exchanges
            avg_change = total_change / total_weight
            
            # Predict Lighter will follow this change
            # Use a fraction of the change (don't assume 100% follow-through)
            # Be conservative - other exchange moves are noisy
            follow_through_rate = 0.3  # Lighter follows ~30% of other exchange moves
            
            estimate = lighter_last + avg_change * follow_through_rate
        else:
            # No momentum signal - just use lighter's last price
            estimate = lighter_last
        
        return estimate
    
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
        
        KEY CHANGE: At EVERY update from ANY exchange, we:
        1. Update our continuous estimate of Lighter's current price
        2. Store this estimate with timestamp
        3. When Lighter updates, we can see how accurate we were
        
        This gives us a prediction at EVERY tick, not just at lighter update times.
        """
        # Get engine timestamp
        engine_ts = self._get_engine_ts(arrival_ts, source_engine_ts)
        
        # Determine observation value (ignore trades for now - too noisy)
        obs = mid if feed != "trade" else None
        
        # ALWAYS compute and store our current estimate of lighter's price
        # This happens at EVERY tick from EVERY exchange
        current_estimate = self._compute_lighter_estimate(arrival_ts)
        self.current_lighter_estimate = current_estimate
        self.continuous_estimates.append((arrival_ts, current_estimate, exchange))
        
        # For lighter updates: predict based on TREND from other exchanges
        if exchange == "lighter" and obs is not None and np.isfinite(obs):
            # KEY INSIGHT: We want to capture the TREND - consistent movement
            # in one direction. Only act when there's strong consensus.
            
            # Get velocities from other exchanges
            velocities = []  # (vel_per_sec, weight)
            vel_details = []
            
            for ex_name, state in self.states.items():
                if ex_name == "lighter":
                    continue
                if state.velocity is None or state.last_engine_ts is None:
                    continue
                
                # Skip stale exchanges
                age_ns = arrival_ts - state.last_engine_ts
                if age_ns > STALE_THRESHOLD_NS:
                    continue
                
                # Convert velocity from $/ns to $/sec
                vel_per_sec = state.velocity * 1_000_000_000
                
                # Skip very small velocities (noise)
                if abs(vel_per_sec) < 2.0:
                    continue
                
                # Weight by priority and recency
                priority = EXCHANGE_PRIORITY.get(ex_name, 50)
                recency = max(0.1, 1.0 - age_ns / STALE_THRESHOLD_NS)
                weight = (priority / 100.0) * recency
                
                velocities.append((vel_per_sec, weight))
                vel_details.append(f"{ex_name[:3]}:{vel_per_sec:+.1f}/s")
            
            # Check for CONSENSUS - are all significant velocities in same direction?
            if len(velocities) >= 2:
                all_negative = all(v < 0 for v, w in velocities)
                all_positive = all(v > 0 for v, w in velocities)
                consensus = all_negative or all_positive
            else:
                consensus = False
            
            if consensus and velocities:
                # Calculate weighted average velocity
                total_vel = sum(v * w for v, w in velocities)
                total_weight = sum(w for v, w in velocities)
                avg_velocity = total_vel / total_weight  # $/sec
                
                # Extrapolate 700ms = 0.7 sec, but only use 30%
                extrapolated_change = avg_velocity * 0.7 * 0.3
                
                # Cap to prevent extreme extrapolations
                MAX_EXTRAPOLATION = 5.0  # Max $5 extrapolation
                extrapolated_change = max(-MAX_EXTRAPOLATION, 
                                         min(MAX_EXTRAPOLATION, extrapolated_change))
                
                future_prediction = obs + extrapolated_change
                action = "EXTRAP"
            else:
                # No consensus or not enough data - predict no change
                future_prediction = obs
                extrapolated_change = 0.0
                action = "HOLD"
            
            # Store for evaluation
            self.lighter_priors.append((arrival_ts, future_prediction, obs))
            
            # Debug: show what we're doing
            if len(self.lighter_priors) < 30:
                vels_str = ",".join(vel_details[:3]) if vel_details else "none"
                print(f"  DEBUG: {action} lighter={obs:.2f}, vels=[{vels_str}], "
                      f"extrap={extrapolated_change:+.2f}, pred={future_prediction:.2f}")
            
            # Update lighter's own state
            self.states["lighter"].update_with_change(obs, engine_ts)
        
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
        
        # Update exchange state AND compute price change
        state = self.states[exchange]
        state.update_with_change(corrected_mid, engine_ts)  # Compute change BEFORE setting new values
        state.last_mid = obs
        state.last_engine_ts = engine_ts
        state.last_arrival_ts = arrival_ts
        state.last_corrected_mid = corrected_mid
        
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
    
    def get_continuous_estimates(self) -> pd.DataFrame:
        """
        Return continuous estimates at EVERY tick.
        This shows our estimate of lighter's price at every point in time,
        regardless of which exchange triggered the update.
        """
        if not self.continuous_estimates:
            return pd.DataFrame(columns=["ts_ns", "estimate", "trigger_exchange"])
        return pd.DataFrame(self.continuous_estimates, columns=["ts_ns", "estimate", "trigger_exchange"])


# =============================================================================
# Evaluation
# =============================================================================

def add_future_lighter_prices(
    priors_df: pd.DataFrame, 
    df: pd.DataFrame,
    latency_ns: int = LATENCY_OFFSET_NS,
) -> pd.DataFrame:
    """
    For each prior prediction, find the lighter price at time + latency_ns.
    This is what we should have predicted - the price AFTER our cancellation latency.
    """
    # Get all lighter BBO updates sorted by time
    lighter_updates = df[
        (df["exchange"] == "lighter") & 
        (df["feed"] != "trade") & 
        df["mid"].notna()
    ][["ts_ns", "mid"]].copy()
    lighter_updates = lighter_updates.sort_values("ts_ns").reset_index(drop=True)
    
    if lighter_updates.empty:
        priors_df["future_actual"] = np.nan
        return priors_df
    
    # For each prior, find the lighter price at ts + latency
    future_actuals = []
    lighter_ts = lighter_updates["ts_ns"].values
    lighter_mid = lighter_updates["mid"].values
    
    for _, row in priors_df.iterrows():
        target_ts = row["ts_ns"] + latency_ns
        
        # Find the latest lighter update before target_ts
        idx = np.searchsorted(lighter_ts, target_ts, side="right") - 1
        
        if idx >= 0 and idx < len(lighter_mid):
            future_actuals.append(lighter_mid[idx])
        else:
            future_actuals.append(np.nan)
    
    priors_df = priors_df.copy()
    priors_df["future_actual"] = future_actuals
    return priors_df


def fit_ar_model(errors: np.ndarray, order: int = 5) -> Dict[str, float]:
    """
    Fit an AR(order) model to the errors to check for autocorrelation.
    
    If errors are autocorrelated, it means there's predictable structure
    we're not capturing in our model.
    """
    errors = errors[np.isfinite(errors)]
    if len(errors) < order + 10:
        return {}
    
    # First compute autocorrelations
    autocorrs = []
    for lag in range(1, order + 1):
        if len(errors) > lag:
            corr = np.corrcoef(errors[:-lag], errors[lag:])[0, 1]
            autocorrs.append(corr if np.isfinite(corr) else 0.0)
        else:
            autocorrs.append(np.nan)
    
    # Build design matrix for AR(order)
    n = len(errors)
    X = np.zeros((n - order, order))
    y = errors[order:]
    
    for i in range(order):
        X[:, i] = errors[order - 1 - i:n - 1 - i]
    
    # Check for any inf/nan in X or y
    valid_mask = np.all(np.isfinite(X), axis=1) & np.isfinite(y)
    X = X[valid_mask]
    y = y[valid_mask]
    
    if len(y) < order + 10:
        return {"autocorrelations": autocorrs}
    
    # Fit OLS using ridge regression for numerical stability
    import warnings
    
    try:
        # Add constant term
        X_with_const = np.column_stack([np.ones(len(y)), X])
        
        # Ridge regression: (X'X + lambda*I)^{-1} X'y
        lambda_reg = 1e-6
        XtX = X_with_const.T @ X_with_const
        XtX += lambda_reg * np.eye(XtX.shape[0])
        Xty = X_with_const.T @ y
        
        try:
            coeffs = np.linalg.solve(XtX, Xty)
        except np.linalg.LinAlgError:
            coeffs = np.linalg.lstsq(XtX, Xty, rcond=None)[0]
        
        if not np.all(np.isfinite(coeffs)):
            return {"autocorrelations": autocorrs}
        
        # Check coefficient magnitudes - very large coeffs cause overflow
        max_coeff = np.max(np.abs(coeffs))
        if max_coeff > 1e10:
            return {"autocorrelations": autocorrs}
        
        # Predictions and residuals - suppress warnings and handle gracefully
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            y_pred = X_with_const @ coeffs
        
        if not np.all(np.isfinite(y_pred)):
            return {"autocorrelations": autocorrs}
            
        residuals = y - y_pred
        
        # R-squared
        ss_res = np.sum(residuals ** 2)
        ss_tot = np.sum((y - np.mean(y)) ** 2)
        r_squared = 1 - ss_res / ss_tot if ss_tot > 1e-10 else 0
        r_squared = max(0, min(1, r_squared))  # Clamp to [0, 1]
        
        return {
            "const": float(coeffs[0]),
            "ar_coeffs": [float(c) for c in coeffs[1:]],
            "r_squared": float(r_squared),
            "residual_std": float(np.std(residuals)),
            "original_std": float(np.std(errors)),
            "autocorrelations": autocorrs,
        }
    except Exception as e:
        return {"autocorrelations": autocorrs}


def evaluate_priors(
    priors_df: pd.DataFrame, 
    train_end_ts: int,
    use_future: bool = True,
) -> Dict[str, float]:
    """
    Evaluate on PRIOR predictions.
    
    If use_future=True, evaluate against lighter price at ts + LATENCY_OFFSET_NS
    (i.e., where lighter will be after our cancellation latency).
    """
    if priors_df.empty:
        return {}
    
    # Choose which actual to compare against
    actual_col = "future_actual" if (use_future and "future_actual" in priors_df.columns) else "actual"
    
    train = priors_df[priors_df["ts_ns"] <= train_end_ts]
    test = priors_df[priors_df["ts_ns"] > train_end_ts]
    
    metrics = {}
    
    for name, subset in [("train", train), ("test", test)]:
        if subset.empty:
            continue
        
        errors = subset["prior"] - subset[actual_col]
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


def compute_naive_baseline_nowcast(
    df: pd.DataFrame, 
    train_end_ts: int,
) -> Dict[str, float]:
    """
    Naive baseline for nowcast: previous lighter mid → current lighter mid.
    
    This measures: if I use the previous lighter price as my estimate,
    how wrong am I when lighter reveals its new price?
    """
    lighter_mask = (df["exchange"] == "lighter") & (df["feed"] != "trade")
    lighter_df = df[lighter_mask & df["mid"].notna()][["ts_ns", "mid"]].copy()
    lighter_df = lighter_df.sort_values("ts_ns").reset_index(drop=True)
    
    if len(lighter_df) < 2:
        return {}
    
    # Previous price predicts current price
    lighter_df["prev_mid"] = lighter_df["mid"].shift(1)
    lighter_df = lighter_df.dropna(subset=["mid", "prev_mid"])
    
    # Test set only
    test = lighter_df[lighter_df["ts_ns"] > train_end_ts]
    
    if test.empty:
        return {}
    
    # Naive prediction = previous mid, actual = current mid
    errors = test["prev_mid"] - test["mid"]
    
    return {
        "naive_test_mae": errors.abs().mean(),
        "naive_test_rmse": np.sqrt((errors ** 2).mean()),
        "naive_test_median": errors.abs().median(),
        "naive_test_n": len(errors),
        "naive_test_p95": errors.abs().quantile(0.95),
        "naive_test_within_0.1": (errors.abs() <= 0.1).mean() * 100,
        "naive_test_within_0.5": (errors.abs() <= 0.5).mean() * 100,
        "naive_test_within_1.0": (errors.abs() <= 1.0).mean() * 100,
    }


def evaluate_continuous_estimates(
    continuous_df: pd.DataFrame,
    df: pd.DataFrame,
    train_end_ts: int,
    latency_offset_ns: int = LATENCY_OFFSET_NS,
) -> Dict[str, float]:
    """
    Evaluate continuous estimates against Lighter's price LATENCY_OFFSET_NS later.
    
    KEY INSIGHT: Lighter's displayed price is STALE due to cancellation delays.
    When other exchanges move, Lighter lags behind. We need to predict where
    Lighter WILL BE after the delay clears (500-700ms), not where it is now.
    
    For each estimate at time T (triggered by any exchange), find
    Lighter's price at T + LATENCY_OFFSET_NS and compute the error.
    """
    if continuous_df.empty:
        return {}
    
    # Get lighter observations
    lighter_mask = (df["exchange"] == "lighter") & (df["feed"] != "trade")
    lighter_df = df[lighter_mask & df["mid"].notna()][["ts_ns", "mid"]].copy()
    lighter_df = lighter_df.sort_values("ts_ns").reset_index(drop=True)
    
    if lighter_df.empty:
        return {}
    
    lighter_ts = lighter_df["ts_ns"].values
    lighter_mid = lighter_df["mid"].values
    
    # For each continuous estimate, find lighter's price at T + latency_offset
    test_errors = []
    
    for _, row in continuous_df.iterrows():
        ts = row["ts_ns"]
        estimate = row["estimate"]
        
        # Target time: when we expect Lighter to reflect the "true" price
        target_ts = ts + latency_offset_ns
        
        # Find lighter observation closest to target time (but not after)
        idx = np.searchsorted(lighter_ts, target_ts, side="right") - 1
        if idx >= 0 and idx < len(lighter_mid):
            actual = lighter_mid[idx]
            error = estimate - actual
            if np.isfinite(error):
                if ts > train_end_ts:
                    test_errors.append(error)
    
    if not test_errors:
        return {}
    
    test_errors = np.array(test_errors)
    
    return {
        "continuous_test_mae": np.abs(test_errors).mean(),
        "continuous_test_rmse": np.sqrt((test_errors ** 2).mean()),
        "continuous_test_median": np.median(np.abs(test_errors)),
        "continuous_test_n": len(test_errors),
        "continuous_test_p95": np.percentile(np.abs(test_errors), 95),
        "continuous_test_within_0.1": (np.abs(test_errors) <= 0.1).mean() * 100,
        "continuous_test_within_0.5": (np.abs(test_errors) <= 0.5).mean() * 100,
        "continuous_test_within_1.0": (np.abs(test_errors) <= 1.0).mean() * 100,
    }


def compute_naive_baseline(
    df: pd.DataFrame, 
    train_end_ts: int,
    latency_ns: int = LATENCY_OFFSET_NS,
) -> Dict[str, float]:
    """
    Naive baseline: current lighter mid → lighter mid at ts + latency.
    
    This measures: if I quote at the current lighter price, what's my error
    after the cancellation latency?
    """
    lighter_mask = (df["exchange"] == "lighter") & (df["feed"] != "trade")
    lighter_df = df[lighter_mask & df["mid"].notna()][["ts_ns", "mid"]].copy()
    lighter_df = lighter_df.sort_values("ts_ns").reset_index(drop=True)
    
    if lighter_df.empty:
        return {}
    
    # For each lighter update, find the lighter price at ts + latency
    lighter_ts = lighter_df["ts_ns"].values
    lighter_mid = lighter_df["mid"].values
    
    future_mids = []
    for i, ts in enumerate(lighter_ts):
        target_ts = ts + latency_ns
        idx = np.searchsorted(lighter_ts, target_ts, side="right") - 1
        if idx >= 0 and idx < len(lighter_mid):
            future_mids.append(lighter_mid[idx])
        else:
            future_mids.append(np.nan)
    
    lighter_df["future_mid"] = future_mids
    
    # Test set only
    test = lighter_df[lighter_df["ts_ns"] > train_end_ts].dropna(subset=["mid", "future_mid"])
    
    if test.empty:
        return {}
    
    # Naive prediction = current mid, actual = future mid
    errors = test["mid"] - test["future_mid"]
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

def plot_results(df: pd.DataFrame, priors_df: pd.DataFrame, train_end_ts: int, continuous_df: pd.DataFrame = None):
    """
    Two-panel step graph:
    1. Top: Step graph of lighter actual, model prediction (continuous!), and other exchange mids
    2. Bottom: Step graph of prediction error
    
    KEY: Uses continuous estimates (updated at every tick from any exchange)
    so the prediction moves when OTHER exchanges move, not just lighter.
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
    # Use future_actual if available (accounting for latency), else actual
    actual_col = "future_actual" if "future_actual" in priors_df.columns else "actual"
    priors_df["error"] = priors_df["prior"] - priors_df[actual_col]
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
    
    # Plot model prediction - use CONTINUOUS estimates if available (updates at every tick!)
    # This is key: shows our prediction moving when OTHER exchanges move
    if continuous_df is not None and not continuous_df.empty:
        continuous_df = continuous_df.copy()
        continuous_df["dt"] = pd.to_datetime(continuous_df["ts_ns"], unit="ns", utc=True)
        continuous_df = continuous_df.sort_values("dt")
        
        # Downsample for performance (every Nth point)
        step = max(1, len(continuous_df) // 5000)
        plot_cont = continuous_df.iloc[::step]
        
        ax1.step(
            plot_cont["dt"],
            plot_cont["estimate"],
            where="post",
            color="red",
            alpha=0.8,
            linewidth=1.5,
            linestyle="--",
            label="prediction (continuous)",
        )
    else:
        # Fallback to priors-only plot
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
    ax2.set_title(f"Prediction Error (Prior - Lighter {LATENCY_OFFSET_NS/1e6:.0f}ms Later)")
    ax2.legend(loc="upper left")
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.show()


# =============================================================================
# Main
# =============================================================================

def run_model_pass(df: pd.DataFrame, verbose: bool = True) -> FastestCredibleSignal:
    """Run a single pass through the data."""
    model = FastestCredibleSignal()
    
    if verbose:
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
        
        if verbose and idx % 100000 == 0:
            print(f"  Processed {idx:,} / {len(df):,}")
    
    return model


def main(data_path: Optional[Path] = None):
    path = data_path or DEFAULT_DATA_PATH
    df = load_data(path)
    
    n = len(df)
    train_end_idx = int(n * TRAIN_FRACTION)
    train_end_ts = df.loc[train_end_idx, "ts_ns"]
    
    print(f"\nData split: {train_end_idx:,} train, {n - train_end_idx:,} test")
    
    # =========================================================================
    # Run Momentum-Based Price Predictor
    # =========================================================================
    print("\n" + "=" * 60)
    print("Running Momentum-Based Price Predictor")
    print(f"  Prediction horizon: {LATENCY_OFFSET_NS/1e6:.0f}ms")
    print("=" * 60)
    
    model = run_model_pass(df)
    priors_df = model.get_lighter_priors()
    estimates_df = model.get_all_estimates()
    continuous_df = model.get_continuous_estimates()
    
    # NOTE: We now have predictions at EVERY tick, not just lighter updates
    # - priors_df: evaluated at lighter observation times
    # - continuous_df: our estimate at EVERY tick from ANY exchange
    
    print(f"\nCollected {len(priors_df):,} lighter prior predictions (at lighter update times)")
    print(f"Collected {len(continuous_df):,} continuous estimates (at EVERY tick)")
    
    # Show breakdown of continuous estimates by trigger exchange
    if not continuous_df.empty:
        trigger_counts = continuous_df["trigger_exchange"].value_counts()
        print("\nContinuous estimates by trigger exchange:")
        for ex, count in trigger_counts.items():
            print(f"  {ex:10s}: {count:,} ({count/len(continuous_df)*100:.1f}%)")
    
    # Print exchange state statistics
    print("\n" + "=" * 60)
    print("Exchange State Statistics (end of run)")
    print("=" * 60)
    for ex_name, state in model.states.items():
        if state.last_corrected_mid is not None:
            print(f"  {ex_name:10s}: last_price = ${state.last_corrected_mid:.2f}")
    
    # Evaluate
    print("\n" + "=" * 60)
    print(f"Evaluation (predicting Lighter {LATENCY_OFFSET_NS/1e6:.0f}ms into future)")
    print("=" * 60)
    
    # Add future lighter prices for evaluation
    priors_df = add_future_lighter_prices(priors_df, df, LATENCY_OFFSET_NS)
    
    # Evaluate: estimate vs lighter price LATENCY_OFFSET_NS later
    metrics = evaluate_priors(priors_df, train_end_ts, use_future=True)
    
    # Naive baseline: use current lighter price to predict future
    naive = compute_naive_baseline(df, train_end_ts, LATENCY_OFFSET_NS)
    
    print("\nModel Performance (Prior Predictions):")
    for key, value in sorted(metrics.items()):
        if "_n" in key:
            print(f"  {key}: {value:,}")
        elif "within" in key:
            print(f"  {key}: {value:.1f}%")
        else:
            print(f"  {key}: {value:.4f}")
    
    print(f"\nNaive Baseline (Current Lighter Mid → {LATENCY_OFFSET_NS/1e6:.0f}ms later):")
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
    
    # Evaluate continuous estimates (at EVERY tick, predicting future)
    print("\n" + "=" * 60)
    print(f"Continuous Estimates (at EVERY tick, vs Lighter {LATENCY_OFFSET_NS/1e6:.0f}ms later)")
    print("=" * 60)
    
    continuous_metrics = evaluate_continuous_estimates(continuous_df, df, train_end_ts)
    if continuous_metrics:
        for key, value in sorted(continuous_metrics.items()):
            if "_n" in key:
                print(f"  {key}: {value:,}")
            elif "within" in key:
                print(f"  {key}: {value:.1f}%")
            else:
                print(f"  {key}: {value:.4f}")
    
    # Fit AR(5) model to test errors to check for autocorrelation
    print("\n" + "=" * 60)
    print("AR(5) Model on Test Errors (checking autocorrelation)")
    print("=" * 60)
    
    actual_col = "future_actual" if "future_actual" in priors_df.columns else "actual"
    test_priors = priors_df[priors_df["ts_ns"] > train_end_ts].copy()
    test_errors = (test_priors["prior"] - test_priors[actual_col]).values
    
    ar_results = fit_ar_model(test_errors, order=5)
    if ar_results:
        if "autocorrelations" in ar_results:
            print(f"\n  Autocorrelations of errors (lag 1-5):")
            for i, ac in enumerate(ar_results["autocorrelations"], 1):
                if np.isfinite(ac):
                    print(f"    Lag {i}: {ac:.4f}")
                else:
                    print(f"    Lag {i}: N/A")
        
        if "ar_coeffs" in ar_results:
            print(f"\n  AR(5) coefficients:")
            print(f"    Constant: {ar_results['const']:.4f}")
            for i, coef in enumerate(ar_results["ar_coeffs"], 1):
                print(f"    AR({i}): {coef:.4f}")
            
            print(f"\n  Model fit:")
            print(f"    R-squared: {ar_results['r_squared']:.4f}")
            print(f"    Original error std: ${ar_results['original_std']:.4f}")
            print(f"    Residual std (after AR): ${ar_results['residual_std']:.4f}")
            
            if ar_results['original_std'] > 1e-10:
                variance_explained = (1 - (ar_results['residual_std']**2 / ar_results['original_std']**2)) * 100
                print(f"    Variance explained by AR(5): {variance_explained:.1f}%")
            
            if ar_results['r_squared'] > 0.1:
                print(f"\n  WARNING: High autocorrelation detected!")
                print(f"  The errors have predictable structure we're not capturing.")
    
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
    
    plot_results(df, priors_df, train_end_ts, continuous_df)
    
    return df, priors_df, metrics, model


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, default=None)
    args = parser.parse_args()
    
    df, priors_df, metrics, model = main(args.data)
