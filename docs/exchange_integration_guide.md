# Exchange Integration Guide

This document briefly outlines the steps and checkpoints for adding a new venue to the multi-exchange engine. Use it as a high-level checklist; existing exchanges (Gate, Bybit, Binance, Bitget, OKX) provide concrete examples for each step.

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
Create a new directory under `src/exchanges/<exchange>/` containing:
- `mod.rs`: re-exports parser/orderbook modules
- `parser.rs`: implements the WS handler
- `orderbook.rs`: wraps `ArrayOrderBook` with venue-specific delta logic (optional if not tracking full depth)
- `rest.rs`: optional module for REST fetches

Register the exchange in `src/exchanges/mod.rs`.

### Handler
Implement `ExchangeHandler` in `parser.rs`:
- `initial_subscriptions()` list
- `url()` returning the websocket endpoint
- `parse_text/parse_binary` mapping inbound messages to a venue-specific frame struct
- Sequence gating (override `sequence_key_*` if possible)
- Application-level heartbeats (`app_heartbeat*`) if required

Keep the handler minimal: just capture raw frames + metadata (timestamps, recv instants). Parsing to structured events happens in the collectors module.

---

## 3. Collectors

Add a new file under `src/collectors/<exchange>.rs`:
- Implement `events_for()` to feed orderbook updates or mid-price events into the shared book (if tracking), returning optional `(feed, mid)` pairs for metric logging
- Implement `update_bbo_store`, `update_trades`, `update_tickers` to populate `BboStore`, `FixedTrades`, and `TickerStore`
- Convert all numeric fields into `i64` prices/quantities using consistent scales (see existing venues for constants)

Register the module in `src/collectors/mod.rs`.

---

## 4. Engine Wiring

In `src/base_classes/engine.rs`:
1. Import the new handler/frame modules and collector functions.
2. In `spawn_state_engine`, spawn the websocket worker (`spawn_ws_worker::<NewExchangeHandler, N>`).
3. Maintain per-exchange state: book, bbo store, trades ring buffer, ticker store.
4. Add a pending-frame slot and drain logic similar to existing exchanges (respect BBO gating functions).
5. Update the main loop to process frames:
   - Run orderbook deltas (`events_for`) and BBO/trade/ticker updates using the new collectors.
   - Update `GlobalState` snapshots for orderbook, BBO, trades, ticker.
   - Notify the reference publisher (it reads from `GlobalState`) and publish feed metrics.

Ensure `FeedTimestampGate` and `DemeanTracker` receive the new `ExchangeFeed`/`ExchangeKind` enum variant.

---

## 5. Shared Infrastructure

- **Enums:** Extend `ExchangeFeed`, `ExchangeKind`, and `ReferenceSource` with the new venue.
- **State:** Add fields to `GlobalState` and `DemeanState` for the new exchange. Mirror the existing structure (orderbook, bbo, trade, ticker).
- **Reference Publisher:** Add adjusted-price logic for the new venue (see `src/base_classes/reference_publisher.rs`).
- **CSV Logger:** Update `QuoteCsvLogger` to write snapshots for the new exchange.
- **Docs:** Add notes to any relevant documentation (see `docs/okx_perp_ws.md` for style).

---

## 6. Testing & Validation

### Local Debug Utility
Create a standalone binary (see `src/bin/okx_orderbook_debug.rs`):
- Connect to the venue’s public websocket
- Subscribe to the relevant channels
- Run updates through the local orderbook/delta logic
- Print the mid/top levels to confirm sequencing and deltas behave as expected

### Hot Path Verification
1. Enable logging (`config.logging`) and run `cargo run --bin gate_runner --features gate_exec`.
2. Confirm the CSV contains entries for the new exchange across feeds (orderbook, bbo, trade).
3. Check reference price logs to ensure the new venue contributes adjusted prices.

### Monitoring
- Add metrics or log statements where useful (e.g., sequence drops, checksum mismatches).
- Consider simulators for network throttling or forced restarts to test reconnection logic.

---

## 7. Launch Checklist

- [ ] All modules compile (`cargo check --all`)
- [ ] CSV logging shows the new venue
- [ ] Orderbook/BBO/trade/ticker snapshots appear in `GlobalState`
- [ ] Reference publisher includes the new venue’s adjusted price
- [ ] Demean tracker stores offsets for the new venue
- [ ] Heartbeats and reconnect logic verified
- [ ] Documentation updated

Once the above passes, the exchange is live in the hot path and ready for production strategy work.

---

Happy integrating—each subsequent venue should be faster once this skeleton is in place! 💪
