# OKX Perpetual Futures Market Data Notes

Source: [OKX API guide – WebSocket public channels](https://www.okx.com/docs-v5/en/#websocket-api-public-channels)

## Endpoints
- `wss://ws.okx.com:8443/ws/v5/public` – public market data (tickers, trades, order books, BBO, etc.).
- `wss://ws.okx.com:8443/ws/v5/business` – tick-by-tick trade stream (`trades-all`). Useful when you need every individual match instead of price-aggregated trades.

Connections are kept alive by responding to server `ping` frames (send the literal string `pong`) or proactively sending `ping` when idle for 30 seconds.

## Instrument Conventions
- Perpetual futures are identified with the `-SWAP` suffix, for example `BTC-USDT-SWAP`.
- `instType` for perpetuals is `SWAP`. Most channels accept either `instId` or `instFamily`; we use `instId` for single instruments.

## Subscriptions
All subscriptions share the same request envelope:

```jsonc
{
  "id": "client-issued-uuid",
  "op": "subscribe",
  "args": [
    { "channel": "<channel>", "instId": "<instrument>" }
  ]
}
```

A successful response echoes the `id` plus `{ "event": "subscribe", "arg": { ... } }`.

### `tickers`
- Channel: `tickers`
- Useful fields from the push payload (`data[0]`):
  - `instType`, `instId`
  - `last`, `lastSz`
  - `bidPx`, `bidSz`, `askPx`, `askSz`
  - `open24h`, `high24h`, `low24h`
  - `vol24h` (base contracts) and `volCcy24h` / `volCcyQuote` (quote volume)
  - `ts` (ms)
- Update cadence: best-effort 100 ms, only when a trade or top-of-book change occurs.

Example push:
```jsonc
{
  "arg": { "channel": "tickers", "instId": "BTC-USDT-SWAP" },
  "data": [{
    "instType": "SWAP",
    "instId": "BTC-USDT-SWAP",
    "last": "62890.2",
    "lastSz": "1",
    "bidPx": "62890.1",
    "bidSz": "23",
    "askPx": "62890.2",
    "askSz": "18",
    "high24h": "64000",
    "low24h": "61000",
    "vol24h": "157834",
    "volCcyQuote": "9.84e9",
    "ts": "1718190485123"
  }]
}
```

### `trades`
- Channel: `trades`
- Endpoint: `public`
- Events aggregate fills from a taker order at a single price; `count` indicates how many matches were combined.
- Payload fields:
  - `instId`, `tradeId`
  - `px`, `sz` (contracts for swaps)
  - `side` (`buy` if taker aggressed ask)
  - `ts` (ms)
  - `count` (optional, number of fills aggregated)
  - `source` (order source flag) and `seqId`

Example:
```jsonc
{
  "arg": { "channel": "trades", "instId": "BTC-USDT-SWAP" },
  "data": [{
    "instId": "BTC-USDT-SWAP",
    "tradeId": "130639474",
    "px": "62890.2",
    "sz": "4",
    "side": "buy",
    "ts": "1718190485123",
    "count": "2",
    "source": "0",
    "seqId": 123456789
  }]
}
```

### `trades-all` (optional)
- Channel: `trades-all`
- Endpoint: `business`
- Sends one update per individual match (no aggregation). The payload is identical to the `trades` channel but without `count`.
- Use if you need tick-by-tick fills; otherwise `trades` on the public endpoint is usually sufficient.

### `bbo-tbt`
- Channel: `bbo-tbt`
- Tick-by-tick best bid/offer, one level per side.
- Fields:
  - `asks` / `bids`: arrays of `[price, sz, 0, orderCount]`
  - `ts`, `seqId`
- Push frequency up to 10 ms when top-of-book moves.

Example:
```jsonc
{
  "arg": { "channel": "bbo-tbt", "instId": "BTC-USDT-SWAP" },
  "data": [{
    "asks": [["62890.3", "12", "0", "3"]],
    "bids": [["62890.2", "18", "0", "4"]],
    "ts": "1718190485123",
    "seqId": 363996337
  }]
}
```

### `books5`
- Channel: `books5`
- Provides 5-level snapshots refreshed every ~100 ms when top-5 change.
- Fields:
  - `asks`, `bids`: arrays of `[price, sz, 0, orderCount]`
  - `ts`, `checksum` (CRC32), `seqId`
- Initial message is a snapshot (`action: "snapshot"` only appears for `books`/`books-l2-tbt`). For swaps we can treat every push as a fresh 5-level snapshot.

### `books` (400 depth, incremental)
- Channel: `books`
- Initial push is full snapshot. Subsequent pushes may include `action: "update"` with deltas.
- Fields extend `books5` with:
  - `action`: `"snapshot"` or `"update"`
  - `prevSeqId`, `seqId`
  - `checksum`
- Merge incremental updates by price, dropping levels when `sz` equals `"0"`. Validate the checksum by joining the first 25 bid/ask pairs as described in the docs.

## Error Handling
- Subscription errors return `{ "event": "error", "code": "<code>", "msg": "<details>" }`.
- Common codes: `60012` invalid request, `60042` (if VIP level required), `64000` (deprecated params).
- Close frames: status `4004` indicates no data in 30s (connection idle), `4008` too many channels.

## Rate & Sequencing Notes
- Best-effort update cadence: tickers/`books5` ~100 ms; `bbo-tbt` / `books-l2` ~10 ms.
- Order book updates include `seqId`/`prevSeqId` monotonic pairing; expect reset after maintenance.
- When no depth change for ~60s, a heartbeat update is sent with empty `asks`/`bids` but identical `seqId`.

## Integration Notes & Quirks
- **Sequence handling:** `prevSeqId` is `-1` on snapshots and can arrive as a signed string. Deserialize using a signed type, treat negative values as “no previous sequence,” and maintain separate counters for `books` and `bbo-tbt` since each channel has its own monotonic stream.
- **BBO + depth reconciliation:** We subscribe to both `books` (snapshot + deltas) and `bbo-tbt`. `books` drives the main depth state, while `bbo-tbt` lets us tighten top-of-book between depth pushes. Both feeds update the same `OkxBook`, so the mid and top levels stay aligned regardless of which channel fired last.
- **Signed checksums:** The CRC32 checksum is published as a signed 32-bit integer. Negative values are expected—they’re simply the raw CRC interpreted as `i32`. We log the value but do not yet hard-fail on mismatch.
- **Heartbeat keep-alives:** Expect keep-alive updates with empty `asks`/`bids` but unchanged `seqId`. Treat them as connection health signals and leave the book untouched.
- **Pipeline wiring:** OKX events now flow through the engine, feed gate, demean tracker, reference publisher, and CSV logger. Running `gate_runner` with logging enabled will emit OKX rows alongside Gate/Bybit/Binance/Bitget without extra configuration.
