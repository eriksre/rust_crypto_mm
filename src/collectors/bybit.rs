use crate::base_classes::bbo_store::BboStore;
use crate::base_classes::tickers::{TickerSnapshot, TickerStore};
use crate::base_classes::trades::{FixedTrades, Trade};
use crate::base_classes::types::{Price, Qty};
use crate::collectors::helpers::{find_first_bool, find_json_string};
use crate::exchanges::bybit::orderbook::{
    BybitBook, BybitData, BybitDataCont, BybitMsg, PRICE_SCALE, QTY_SCALE,
};
use crate::utils::time::ms_to_ns;
use serde_json::{self, Value};

pub fn events_for<const N: usize>(s: &str, book: &mut BybitBook<N>) -> Vec<(&'static str, f64)> {
    let mut out = Vec::with_capacity(1);
    if let Some(topic) = find_json_string(s, "topic") {
        if topic.starts_with("orderbook.") {
            if let Ok(msg) = serde_json::from_str::<BybitMsg>(s) {
                // Route BBO vs Depth
                if topic.starts_with("orderbook.1.") {
                    // Apply BBO to the book; emit bbo mid from book only
                    let dopt: Option<BybitData> = match &msg.data {
                        BybitDataCont::Obj(d) => Some(d.clone()),
                        BybitDataCont::Arr(v) => v.get(0).cloned(),
                    };
                    if let Some(d) = dopt {
                        if let (Some(b), Some(a)) = (d.b.get(0), d.a.get(0)) {
                            if let (Ok(bid), Ok(ask)) = (b[0].parse::<f64>(), a[0].parse::<f64>()) {
                                let cts_ms = d
                                    .cts
                                    .or(msg.cts)
                                    .expect("Bybit BBO message missing engine timestamp (cts)");
                                let system_ts_ms = msg
                                    .ts
                                    .expect(
                                        "Bybit BBO message missing system timestamp (ts) for skew tracking",
                                    );
                                if book.apply_bbo(
                                    bid,
                                    b[1].parse().unwrap_or(0.0),
                                    ask,
                                    a[1].parse().unwrap_or(0.0),
                                    d.seq,
                                    cts_ms,
                                    system_ts_ms,
                                ) {
                                    if let Some(mid) = book.mid_price_f64() {
                                        out.push(("bbo", mid));
                                    }
                                }
                            }
                        }
                    }
                } else {
                    // Depth updates (50) go through full book apply; emit orderbook mid from book only
                    if book.apply(&msg) {
                        if let Some(mid) = book.mid_price_f64() {
                            out.push(("orderbook", mid));
                        }
                    }
                }
            }
        }
    }
    out
}

// Update Bybit BBO store from orderbook.1 messages (structure-only emitting)
pub fn update_bbo_store(s: &str, store: &mut BboStore) -> bool {
    if let Some(topic) = find_json_string(s, "topic") {
        if topic.starts_with("orderbook.1.") {
            if let Ok(msg) = serde_json::from_str::<BybitMsg>(s) {
                let dopt: Option<BybitData> = match &msg.data {
                    BybitDataCont::Obj(d) => Some(d.clone()),
                    BybitDataCont::Arr(v) => v.get(0).cloned(),
                };
                if let Some(d) = dopt {
                    if let (Some(b), Some(a)) = (d.b.get(0), d.a.get(0)) {
                        if let (Ok(bid), Ok(ask)) = (b[0].parse::<f64>(), a[0].parse::<f64>()) {
                            let bid_qty = b[1].parse::<f64>().unwrap_or(0.0);
                            let ask_qty = a[1].parse::<f64>().unwrap_or(0.0);
                            let cts_ms = d
                                .cts
                                .or(msg.cts)
                                .expect("Bybit BBO message missing engine timestamp (cts)");
                            let system_ts_ms = msg.ts.expect(
                                "Bybit BBO message missing system timestamp (ts) for source tracking",
                            );
                            let system_ts_ns = ms_to_ns(system_ts_ms);
                            let ts_ns = ms_to_ns(cts_ms);
                            if let Some(symbol) = topic.rsplit('.').next() {
                                store.update(
                                    symbol,
                                    bid,
                                    bid_qty,
                                    ask,
                                    ask_qty,
                                    ts_ns,
                                    Some(system_ts_ns),
                                );
                                return true;
                            }
                        }
                    }
                }
            }
        }
    }
    false
}

// Update Bybit trades store from publicTrade messages
pub fn update_trades<const N: usize>(s: &str, trades: &mut FixedTrades<N>) -> usize {
    if let Some(topic) = find_json_string(s, "topic") {
        if !topic.starts_with("publicTrade.") {
            return 0;
        }
    } else {
        return 0;
    }

    let mut inserted = 0usize;
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(s) {
        if let Some(data) = value.get("data").and_then(|d| d.as_array()) {
            let system_ts_ms = value
                .get("ts")
                .and_then(|v| v.as_u64())
                .expect("Bybit trade message missing system timestamp `ts`");
            for entry in data {
                let price = entry
                    .get("p")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok());
                let size = entry
                    .get("v")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok());
                if price.is_none() || size.is_none() {
                    continue;
                }

                let px_i = (price.unwrap() * PRICE_SCALE).round() as Price;
                let qty_i = (size.unwrap() * QTY_SCALE).round() as Qty;
                let ts_ms = entry
                    .get("T")
                    .and_then(|v| match v {
                        Value::Number(n) => n.as_u64(),
                        Value::String(s) => s.parse::<u64>().ok(),
                        _ => None,
                    })
                    .expect("Bybit trade message missing engine timestamp `T`");
                let system_ts_ns = ms_to_ns(system_ts_ms);
                let ts = ms_to_ns(ts_ms);
                // Prefer explicit taker side (`S`/`side`) and fall back to legacy buyer/taker flag.
                let is_buyer_maker = entry
                    .get("S")
                    .and_then(|v| v.as_str())
                    .map(|side| side.eq_ignore_ascii_case("Sell"))
                    .or_else(|| {
                        entry
                            .get("side")
                            .and_then(|v| v.as_str())
                            .map(|side| side.eq_ignore_ascii_case("Sell"))
                    })
                    .or_else(|| {
                        entry
                            .get("BT")
                            .and_then(|v| v.as_bool())
                            .map(|buyer_is_taker| !buyer_is_taker)
                    })
                    .or_else(|| find_first_bool(s, &["BT"]).map(|buyer_is_taker| !buyer_is_taker))
                    .unwrap_or(false);

                let seq = entry
                    .get("t")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0) as u64;

                let trade = Trade::new(px_i, qty_i, ts, seq, is_buyer_maker, Some(system_ts_ns));
                trades.push(trade);
                inserted += 1;
            }
        }
    }
    inserted
}

pub fn update_tickers(s: &str, store: &mut TickerStore) -> Option<(String, TickerSnapshot)> {
    let topic = find_json_string(s, "topic")?;
    if !topic.starts_with("tickers.") {
        return None;
    }

    let root: Value = serde_json::from_str(s).ok()?;
    let data = root.get("data")?;
    let payload = if data.is_array() {
        data.as_array()?.first()?
    } else {
        data
    };

    if !payload.is_object() {
        return None;
    }

    let symbol = payload
        .get("symbol")
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| topic.rsplit('.').next().unwrap_or(topic));
    let mut snapshot = store.get(symbol).copied().unwrap_or_default();

    if let Some(last_px) = value_to_f64(payload, &["lastPrice", "last_price"]) {
        snapshot.ticker.last_px = (last_px * PRICE_SCALE).round() as Price;
    }
    if let Some(last_qty) = value_to_f64(payload, &["lastSize", "lastQty", "lastSz"]) {
        snapshot.ticker.last_qty = (last_qty * QTY_SCALE).round() as Qty;
    }
    if let Some(bid_px) = value_to_f64(payload, &["bid1Price", "bestBidPrice", "bidPrice"]) {
        snapshot.ticker.best_bid = (bid_px * PRICE_SCALE).round() as Price;
    }
    if let Some(ask_px) = value_to_f64(payload, &["ask1Price", "bestAskPrice", "askPrice"]) {
        snapshot.ticker.best_ask = (ask_px * PRICE_SCALE).round() as Price;
    }

    snapshot.mark_px = value_to_f64(payload, &["markPrice", "mark_price"]).or(snapshot.mark_px);
    snapshot.index_px = value_to_f64(payload, &["indexPrice", "index_price"]).or(snapshot.index_px);
    snapshot.funding_rate =
        value_to_f64(payload, &["fundingRate", "funding_rate"]).or(snapshot.funding_rate);
    snapshot.turnover_24h = value_to_f64(payload, &["turnover24h", "turnover_24h", "quoteVolume"])
        .or(snapshot.turnover_24h);
    snapshot.open_interest =
        value_to_f64(payload, &["openInterest", "open_interest"]).or(snapshot.open_interest);

    if let Some(oi_value) = value_to_f64(payload, &["openValue", "open_value"]) {
        snapshot.open_interest_value = Some(oi_value);
    } else if let (Some(oi), Some(mark)) = (snapshot.open_interest, snapshot.mark_px) {
        snapshot.open_interest_value = Some(oi * mark);
    }

    if let Some(seq) = value_to_u64(payload, &["seq", "sequence"]) {
        snapshot.ticker.seq = seq;
    } else {
        snapshot.ticker.seq = 0;
    }

    if let Some(ts_ms) =
        value_to_u64(payload, &["ts"]).or_else(|| root.get("ts").and_then(|v| v.as_u64()))
    {
        snapshot.ticker.ts = ms_to_ns(ts_ms);
    }

    let stored = store.update(symbol.to_string(), snapshot);
    Some((symbol.to_string(), stored))
}

fn value_to_f64(value: &Value, keys: &[&str]) -> Option<f64> {
    for key in keys {
        if let Some(entry) = value.get(*key) {
            match entry {
                Value::Number(n) => {
                    if let Some(v) = n.as_f64() {
                        return Some(v);
                    }
                }
                Value::String(s) => {
                    if let Ok(v) = s.parse::<f64>() {
                        return Some(v);
                    }
                }
                _ => {}
            }
        }
    }
    None
}

fn value_to_u64(value: &Value, keys: &[&str]) -> Option<u64> {
    for key in keys {
        if let Some(entry) = value.get(*key) {
            match entry {
                Value::Number(n) => {
                    if let Some(v) = n.as_u64() {
                        return Some(v);
                    }
                }
                Value::String(s) => {
                    if let Ok(v) = s.parse::<u64>() {
                        return Some(v);
                    }
                }
                _ => {}
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_tickers_bybit_basic() {
        let json = r#"{
            "topic":"tickers.BTCUSDT",
            "ts":1700000000000,
            "data":{
                "symbol":"BTCUSDT",
                "lastPrice":"43000",
                "lastSize":"0.01",
                "bid1Price":"42990",
                "ask1Price":"43010",
                "markPrice":"43005",
                "indexPrice":"42995",
                "fundingRate":"0.0001",
                "turnover24h":"1234567.89",
                "openInterest":"456.0",
                "openValue":"1234.5"
            }
        }"#;

        let mut store = TickerStore::default();
        let (_, snap) = update_tickers(json, &mut store).expect("ticker parsed");

        assert_eq!(
            snap.ticker.last_px,
            (43000.0 * PRICE_SCALE).round() as Price
        );
        assert_eq!(snap.ticker.last_qty, (0.01 * QTY_SCALE).round() as Qty);
        assert_eq!(
            snap.ticker.best_bid,
            (42990.0 * PRICE_SCALE).round() as Price
        );
        assert_eq!(
            snap.ticker.best_ask,
            (43010.0 * PRICE_SCALE).round() as Price
        );
        assert_eq!(snap.mark_px, Some(43005.0));
        assert_eq!(snap.index_px, Some(42995.0));
        assert_eq!(snap.funding_rate, Some(0.0001));
        assert_eq!(snap.turnover_24h, Some(1_234_567.89));
        assert_eq!(snap.open_interest, Some(456.0));
        assert_eq!(snap.open_interest_value, Some(1234.5));
        assert_ne!(snap.ticker.seq, 0);
        assert_eq!(snap.ticker.ts, 1_700_000_000_000_000_000);
    }

    #[test]
    fn test_update_trades_uses_exchange_ts_ns() {
        let json = r#"{
            "topic":"publicTrade.BTCUSDT",
            "ts":1700000000456,
            "data":[{
                "p":"43000",
                "v":"0.5",
                "T":1700000000123,
                "S":"Sell",
                "t":"101"
            }]
        }"#;

        let mut trades = FixedTrades::<4>::default();
        let inserted = update_trades(json, &mut trades);
        assert_eq!(inserted, 1);

        let last = trades.last().expect("trade stored");
        assert_eq!(last.seq, 101);
        assert!(last.is_buyer_maker);
        assert_eq!(last.ts, 1_700_000_000_123_000_000);
        assert_eq!(last.system_ts_ns, Some(ms_to_ns(1_700_000_000_456)));
        assert_eq!(last.px, (43_000f64 * PRICE_SCALE).round() as Price);
        assert_eq!(last.qty, (0.5f64 * QTY_SCALE).round() as Qty);
    }
}
