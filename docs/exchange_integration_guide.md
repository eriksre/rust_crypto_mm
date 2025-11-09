# Exchange Integration Guide

This guide explains how to add a new exchange (perp futures) to the engine using the current code layout. Follow this as a checklist; existing venues (Gate, Bybit, Binance, Bitget, OKX) are good reference implementations.

---

## 1. Gather Requirements

### Protocol Specs
- WebSocket endpoints (public vs private)
- Channels for: orderbook (depth snapshots + increments), best bid/offer, public trades, tickers
- Expected update cadence, sequence semantics, heartbeat rules
- Auth requirements (if private features needed)

### Instrument Metadata
- Symbol formatting (e.g., `BTC-USDT-SWAP`)
- Scaling factors for price/quantity
- Contract size / quanto multiplier
- REST endpoints for bootstrap (e.g., initial orderbook)

Document quirks or throttling requirements alongside the official documentation link.

---

## 2. Code Skeleton

### Module Layout
Create a new directory under `src/exchanges/<exchange>/` with:
- `mod.rs`: public exports for the exchange submodule
- `parser.rs`: the WS handler (implements `ExchangeHandler`)
- `orderbook.rs`: orderbook impl and deltas (implements `OrderBookOps`)
- `rest.rs`: optional REST client (bootstrap or checksums)

Also add any endpoint constants in `src/exchanges/endpoints.rs:1`.

Finally, register the exchange submodule in `src/exchanges/mod.rs:1`.

### Handler
Implement `ExchangeHandler` in `parser.rs` (`src/base_classes/ws.rs:1`):
- `url()` returns the WS endpoint
- `initial_subscriptions()` returns subscribe messages
- `parse_text/parse_binary()` push minimal frames: `{ ts, recv_instant, raw }`
- Sequence gating: override `sequence_key_*()` when the venue provides per-stream sequence
- Heartbeats: implement `app_heartbeat()` or dynamic `app_heartbeat_interval()` + `build_app_heartbeat()` if required by the venue

Keep the handler minimal. It should not decode into rich types on the hot path; downstream collectors will parse and update shared stores.

---

## 3. Collectors

Add `src/collectors/<exchange>.rs:1`:
- `events_for(...)` (or `events_for_book(...)`) to apply OB deltas and emit mid updates
- `update_bbo_store(...)` to keep `BboStore` in sync
- `update_trades(...)` to push `Trade` into `FixedTrades`
- `update_tickers(...)` to update `TickerStore`

Use shared JSON helpers in `src/collectors/helpers.rs:1`. Ensure consistent integer scaling for price/qty (see existing constants per venue).

Register the module in `src/collectors/mod.rs:1`.

---

## 4. Engine Wiring

Touch `src/base_classes/engine.rs:1`:
1. Import the new handler and collector module.
2. In `spawn_state_engine(...)`, add `spawn_ws_worker::<NewExchangeHandler, N>(...)`.
3. Create per-exchange state: book, `BboStore`, `FixedTrades`, `TickerStore`.
4. Add BBO-drain detection for your frames (e.g., `is_newx_bbo_frame(...)`) and call `drain_latest_bbo(...)` before processing to drop stale BBO frames when backlog exists.
5. In the main loop, parse frames and route to collectors:
   - Apply deltas via `events_for`/`events_for_book` and update mid/levels
   - Update `BboStore`, trades, tickers via `update_*`
   - Populate `GlobalState` snapshots for orderbook/BBO/trade/ticker
   - Publish reference updates via the reference publisher

Feed gating and demeaning:
- Extend `ExchangeFeed` in `src/base_classes/feed_gate.rs:1` for gating per-feed timestamps
- Extend `ExchangeKind` in `src/base_classes/demean.rs:1` for demean offsets
- Optionally use `configure_feed_overrides(...)` and `FeedToggles` (`src/base_classes/feed_config.rs:1`) to control which feeds start enabled

---

## 5. Shared Infrastructure

- Enums: extend `ExchangeFeed` (`src/base_classes/feed_gate.rs:1`),
  `ExchangeKind` (`src/base_classes/demean.rs:1`), and `ReferenceSource` (`src/base_classes/reference.rs:1`).
- State: ensure `GlobalState` and `DemeanState` have fields for the new venue (`src/base_classes/state.rs:1`).
- Reference publisher: add adjusted-price selection for the new venue (`src/base_classes/reference_publisher.rs:1`).
- CSV logging: make sure `QuoteCsvLogger` covers the new venue when logging is enabled (`src/logging/quote.rs:1`).
- Endpoints: add/reuse channel constants (`src/exchanges/endpoints.rs:1`).

---

## 6. Testing & Validation

### Local Debug Utility
Create a small bin (see `src/bin/okx_orderbook_debug.rs:1` for reference):
- Connect to public WS, subscribe, apply snapshot+delta flow, print mid/top levels
- Validate sequence/prev-seq rules and checksums if applicable

### Hot Path Verification
1. Enable logging and run `cargo run --bin gate_runner --features gate_exec`.
2. Verify CSV has entries for the new exchange across OB/BBO/trades/ticker.
3. Confirm reference publisher includes the venue and applies demean offsets when required.

### Monitoring
- Add metrics or logs for sequence drops, checksum mismatches, reconnection/backoff.

---

## 7. Launch Checklist

- [ ] Submodule added under `src/exchanges/<exchange>/` with `parser.rs` and `orderbook.rs`
- [ ] `OrderBookOps` implemented (`src/base_classes/orderbook_trait.rs:1`)
- [ ] Collectors added (`src/collectors/<exchange>.rs:1`) and registered
- [ ] Engine wired: WS worker spawn, BBO drain detection, main-loop routing
- [ ] Enums/state/publisher extended (Feed/Demean/Reference/GlobalState)
- [ ] CSV logging emits rows for the new venue (if enabled)
- [ ] Heartbeats, reconnects, and gating validated end-to-end
- [ ] Docs updated (this guide + any venue notes)

Once the above passes, the exchange is live in the hot path and ready for production strategy work.

---

Happy integrating — with this structure, each new venue should be faster to add.
