# Fair Value Experiment Log

## Learnings
- Baseline `gate_bbo` still leads RMSE table (≈0.00916 mean). Competing strategies trail by 1–4 bps.
- Latency cost: smoothing in `fusion_alpha` lags fast steps; avoid heavy EWMA on the final output.
- Cross-exchange feeds show static biases (e.g., Binance mids ~+20 bps). Need per-strategy bias handling rather than global subtraction.
- Trades often arrive earliest; gated weighting should preferentially lean on trade updates when age <200 ms.
- Orderbook imbalance & microprice derived fields added for future strategies.
- Implemented `lead_lag_edge`: bias-corrected, recency-weighted adjustments from external BBO/trade feeds plus gate trade/microprice without final smoothing. Pending evaluation against `gate_bbo`.
- Evaluation: `lead_lag_edge` still trails `gate_bbo` by ~7 bps mean RMSE. Need sharper bias calibration and more aggressive gating on true leads; current weights dilute signal.
- Added `demeaned_lead` strategy (rolling 20s de-mean per feed); initial run underperformed badly (mean RMSE ~0.0109) indicating over-adjusted biases or unstable gains. Need tighter weighting/normalization before contributions.
- Tuned `demeaned_lead` by capping weights/gains and reducing bias influence. Latest run beats `gate_bbo` baseline (mean RMSE ≈0.00918 vs 0.00919), suggesting per-feed de-meaning plus conservative weighting works.

## TODO Ideas
- Build bias-tracked moving offsets per exchange per feed inside strategy using only past data.
- Use recency-aware lead/lag coefficients (e.g., penalize stale external feeds).
- Explore short-horizon regression using recent deltas of leading exchanges.
- Introduce adaptive gain to avoid over-reliance on single feed spikes.
