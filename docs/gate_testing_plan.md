# Gate.io Execution MVP Testing & Rollout Plan

## 1. Unit Tests
| Module | Test Focus |
|--------|------------|
| `gate_client` | signature correctness, header construction, error parsing |
| `order_manager` | state transitions, dedupe, persistence reload |
| `simple_quote` | spread calculation, rounding to tick/step size, stale snapshot handling |
| `risk` (not yet created) | notional limit enforcement, kill-switch behavior |

## 2. Integration Tests
- **Mocked REST Flow**: run `gate_runner` against a `wiremock`/`httpmock` server emulating Gate responses (success, rejection, rate limit, timeout).
- **Replay Harness**: feed historical Gate websocket frames from `raw_frames.md` through the snapshot publisher; assert emitted `QuoteIntent`s match golden file.
- **Persistence Reload**: simulate crash by serializing order store, reload, ensure reconciliation cancels orphaned venue orders.

## 3. Sandbox Validation
1. Configure sandbox credentials; confirm REST connectivity with ping endpoint.
2. Submit single small order manually via `gate_client` CLI helper; verify acknowledgment and cancel.
3. Run `gate_runner --dry-run` to validate quoting cadence and logs.
4. Enable live mode with minimal size; monitor order state, cancel and reprice flows.

## 4. Production Shadow & Cutover
- **Shadow Mode**: run live market data + strategy, but execution gateway mocks responses; compare quotes vs. desired behavior over full trading session.
- **Pilot Trade**: once shadow stable, allow gateway to place one order cycle (bid+ask) with tiny size; validate fills and ledger.
- **Scale Up**: gradually increase size/spread adjustments while monitoring metrics.

## 5. Monitoring & Alerting Checklist
- Latency: measure time from snapshot to order submission (< 10ms expected for MVP).
- Error counters: REST failures, rejected orders, stale data trips.
- Position tracking: confirm inventory updates after fills.
- Heartbeats: ensure periodic status logs every N seconds.

## 6. Rollback Strategy
- `gate_runner --cancel-all` to unwind positions and stop quoting.
- Ability to disable strategy via config flag or kill-switch file.
- Snapshot current state (orders, fills) before restarting.

## 7. Acceptance Criteria
- Unit/integration suites green in CI.
- Sandbox trial completes without uncaught panics or stuck orders.
- Reconciliation run returns zero discrepancies.
- Operators sign off after shadow trading review.

## 8. Open Items
- Decide on persistence backend (JSON vs. SQLite) before writing tests.
- Determine required duration for shadow mode (e.g., 3 trading days).
- Validate availability of Gate sandbox environment and rate limits.
