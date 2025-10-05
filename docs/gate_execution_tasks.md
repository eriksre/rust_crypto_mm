# Gate.io Execution MVP Task Board

## Legend
- 🔄 In progress
- ✅ Complete (to be checked off manually)
- ⏳ Blocked / waiting

## 1. Foundations
- [ ] Extract Gate mid/BBO publisher from `spawn_state_engine` exposing `GateSnapshot` (mid, bid/ask, ts).
- [ ] Define shared types: `QuoteIntent`, `ExecutionReport`, `ClientOrderId`, `RiskLimit`.
- [ ] Create `config/gate_mvp.yaml` schema and loader (symbol, tick size, step size, risk limits).

## 2. Strategy Stub
- [ ] Implement `strategy::simple_quote::SimpleQuoter`:
  - Inputs: `GateSnapshot`, config (spread, size, inventory target).
  - Outputs: `QuoteIntent` pair (bid/ask) or cancel instructions if market stale.
- [ ] Add dry-run CLI flag to log intents without sending.
- [ ] Unit test quoting math (spread clamps, min/max price, inventory skew placeholder).

## 3. Execution Gateway
- [ ] Implement Gate signing helper (HMAC SHA512) using `api_key`, `api_secret` env vars.
- [ ] Build REST client for:
  - `futures.order_batch_place`
  - `futures.orders_batch_cancel`
  - `futures.usertrades` (optional for reconciliation)
  - `futures.positions`
- [ ] Introduce rate limiter (token bucket) + retry policy with jitter.
- [ ] Support deterministic `ClientOrderId` generator (`uuid`-less, timestamp+seq).
- [ ] Integration test client against Gate sandbox (requires credentials).

## 4. Order Manager
- [ ] Design order state enum and in-memory store keyed by `ClientOrderId`.
- [ ] Handle intent lifecycle: new → submit; reprice → cancel/replace; stale → cancel.
- [ ] Persist state (JSON file or SQLite) on each transition; reload on startup.
- [ ] Implement reconciliation step on startup (query open orders, diff with store, fix).
- [ ] Expose metrics/logs: active orders, pending cancels, rejection counts.

## 5. Runner Binary (`src/bin/gate_runner.rs`)
- [ ] Wire ingestion snapshot stream → strategy → execution.
- [ ] Provide CLI options: `--config`, `--dry-run`, `--log-json`.
- [ ] Spawn background tasks for:
  - Market data consumer (tokio mpsc channel)
  - Strategy loop (single-threaded)
  - Execution gateway (REST + reconciliation)
  - Metrics exporter (optional `hyper` server)

## 6. Safety & Controls
- [ ] Implement global notional limit check before submitting orders.
- [ ] Add kill-switch command (e.g., watch file or CLI signal) that cancels all live orders.
- [ ] Watchdog for stale market data: cancel quotes if snapshot older than threshold.
- [ ] Logging of each action with trace ID (market event ts → order id).

## 7. Testing & Verification
- [ ] Unit tests across modules (signing, order manager transitions, quote math).
- [ ] Integration test harness with mocked Gate REST replies.
- [ ] Replay test using recorded Gate frames to ensure stable quoting output.
- [ ] Dry-run in production environment; compare logged quotes to expected spreads.
- [ ] Live test with tiny size in sandbox/production once confidence achieved.

## 8. Documentation & Ops
- [ ] README section describing Gate MVP setup, environment variables, run instructions.
- [ ] Runbook for reconciliation + credential rotation.
- [ ] Capture sample config + log output for reference.
