# Gate.io Execution MVP Overview

## Objective
Stand up a minimally viable yet scalable pathway that turns existing Gate.io market data into executable orders on the venue. The MVP should:
- Leverage the current ingestion (`GateHandler`, `GateBook`, CSV pipeline) without blocking future expansion.
- Route a single-instrument quoting strategy (stubbed) through a Gate.io execution adapter.
- Persist enough state to recover from process restarts and reconcile with the venue.

## Scope for MVP
- **Ingestion**: reuse current `spawn_state_engine` flow; add a lightweight publisher that exposes Gate mid/BBO to the strategy loop.
- **Strategy stub**: simple spread-based quote generator producing `QuoteIntent` (bid/ask) for one perpetual contract.
- **Execution**: REST client supporting `futures.order_batch_place` (single order per batch), cancel endpoint, and authenticated heartbeats.
- **State tracking**: in-memory order map with periodic persistence (JSON/SQLite) and REST-based reconciliation on startup.
- **Safety**: global notional limit, per-order size cap, manual kill switch.
- **Observability**: structured logs + basic metrics (quote latency, order status) via stdout/prometheus exporter.

## High-Level Architecture
```
Gate Market Data  --->  Snapshot Publisher  --->  Strategy Loop  --->  QuoteIntent
                                                 |                         |
                                                 v                         v
                                             Risk Checks             Execution Adapter
                                                 |                         |
                                                 +------> Order Manager <---+
                                                            |
                                                     Persistence / Reconcile
                                                            |
                                                      Operator Interface (CLI)
```

## Key Modules to Add
- `src/strategy/simple_quote.rs`: consumes Gate snapshots, produces intents.
- `src/execution/gate_client.rs`: REST signing + request helpers.
- `src/execution/order_manager.rs`: tracks orders, retries, reconciliation.
- `src/bin/gate_runner.rs`: wires ingestion → strategy → execution.
- `config/gate_mvp.yaml`: credentials (in env), trading limits, symbol details.

## Interfaces (Sketch)
```rust
pub struct QuoteIntent {
    pub venue: Venue,
    pub symbol: String,
    pub side: Side,
    pub price: f64,
    pub size: f64,
    pub tif: TimeInForce,
}

pub trait ExecutionGateway {
    fn submit(&self, intents: &[QuoteIntent]) -> Result<Vec<OrderAck>>;
    fn cancel(&self, id: &ClientOrderId) -> Result<()>;
    fn poll_reports(&self) -> Vec<ExecutionReport>;
}
```

## Dependencies & Config
- Add `hmac`, `sha2`, `reqwest`, `serde`, `tokio` for Gate authenticated REST calls.
- Use `.env` or config file for API key/secret, risk limits, symbol, quoting parameters.
- Ensure optional feature flag `gate_exec` gating MVP binaries.

## Delivery Milestones
1. **Week 1**: shared types + strategy stub + CLI skeleton; dry-run mode (no REST calls) logging intents.
2. **Week 2**: Gate REST client + order manager with simulated responses; unit tests.
3. **Week 3**: Live credentials in sandbox/production; reconciliation + persistence; basic observability.

## Risks & Mitigations
- **REST throttle**: implement token bucket with config-driven rate limits.
- **Time sync**: ensure local clock sync (NTP) and provide helper to query Gate server time before signing.
- **Credential safety**: load secrets via env vars; avoid committing to repo.
- **Data drift**: validate mid prices vs. Gate REST snapshot periodically.

## Out of Scope (Future)
- Multi-venue routing, complex strategy models, portfolio-level risk, Kafka persistence.
- Websocket private feed (optional later once REST flow stable).
