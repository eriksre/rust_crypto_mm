# Exchange Integrations

This directory contains integrations for cryptocurrency perpetual futures exchanges.

## Structure

Each exchange has its own subdirectory with a consistent structure:

```
exchanges/
├── gate/          # Gate.io (full execution support)
│   ├── parser.rs      # WebSocket message parsing
│   ├── orderbook.rs   # Orderbook management
│   ├── rest.rs        # REST API client
│   └── signing.rs     # Request signing utilities
├── bybit/         # Bybit (market data only)
│   ├── parser.rs
│   └── orderbook.rs
├── binance/       # Binance (market data only)
│   ├── parser.rs
│   ├── orderbook.rs
│   ├── rest.rs
│   └── parsed.rs
├── bitget/        # Bitget (market data only)
│   ├── parser.rs
│   └── orderbook.rs
└── endpoints.rs   # Shared endpoint constants
```

## Key Components

### Parser (`parser.rs`)
- Parses WebSocket messages from the exchange
- Extracts orderbook updates, trades, tickers
- Handles exchange-specific message formats
- Updates BBO stores and trade stores

### Orderbook (`orderbook.rs`)
- Manages limit orderbook state
- Implements `OrderBookOps` trait (see `base_classes/orderbook_trait.rs`)
- Handles snapshots and delta updates
- Provides mid price and top-N levels

### REST (`rest.rs`)
- REST API client for fetching snapshots, positions, etc.
- Used for initialization and reconciliation

### Signing (`signing.rs`)
- HMAC signing for authenticated requests
- Exchange-specific authentication schemes

## Adding a New Exchange

To add support for a new exchange:

1. **Create directory structure:**
   ```bash
   mkdir src/exchanges/newexchange
   ```

2. **Create `parser.rs`:**
   - Parse WebSocket messages
   - Implement update functions for BBO, trades, tickers
   - Follow the pattern in existing parsers

3. **Create `orderbook.rs`:**
   - Define exchange-specific orderbook struct
   - Implement `OrderBookOps` trait
   - Handle exchange message format

4. **Create `mod.rs`:**
   ```rust
   pub mod parser;
   pub mod orderbook;
   pub use orderbook::NewExchangeBook;
   pub use parser::*;
   ```

5. **Update `src/exchanges/mod.rs`:**
   - Add `pub mod newexchange;`
   - Add backwards-compat re-exports if needed

6. **Add endpoints to `endpoints.rs`** (if needed)

7. **Update `Cargo.toml`** with feature flags if needed

## Backwards Compatibility

For existing code, the old flat import structure still works:
```rust
use crate::exchanges::gate_book::GateBook;  // Old
use crate::exchanges::gate::GateBook;        // New (preferred)
```

Both import styles are supported via re-exports in `mod.rs`.
