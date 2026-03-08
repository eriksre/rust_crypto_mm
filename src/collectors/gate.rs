use crate::base_classes::bbo_store::BboStore;
use crate::base_classes::tickers::{TickerSnapshot, TickerStore};
use crate::base_classes::trades::{FixedTrades, Trade};
use crate::base_classes::types::{Price, Qty, Seq};
use crate::exchanges::gate::orderbook::{GateBook, GateMsg};
use crate::utils::parsing::log_parse_drop;
use crate::utils::time::ms_to_ns;
use serde_json::{self, Value};

pub fn events_for<const N: usize>(s: &str, book: &mut GateBook<N>) -> Vec<(&'static str, f64)> {
    let mut out = Vec::with_capacity(1);
    let raw: Value = match serde_json::from_str(s) {
        Ok(val) => val,
        Err(err) => {
            log_parse_drop("gate_collector", "json", &err, s);
            return out;
        }
    };
    if let Some(ch) = raw.get("channel").and_then(|v| v.as_str()) {
        match ch {
            "futures.book_ticker" => {}
            "futures.obu" => match serde_json::from_str::<GateMsg>(s) {
                Ok(msg) => {
                    if book.apply(&msg) {
                        if let Some(mid) = book.mid_price_f64() {
                            out.push(("orderbook", mid));
                        }
                    }
                }
                Err(err) => log_parse_drop("gate_collector", "orderbook", &err, s),
            },
            "futures.trades" => { /* handled by trades updater in caller */ }
            _ => {}
        }
    }
    out
}

pub const PRICE_SCALE: f64 = GateBook::<1>::PRICE_SCALE;
pub const QTY_SCALE: f64 = GateBook::<1>::QTY_SCALE;

fn first_result_object(value: &Value) -> Option<&Value> {
    if let Some(result) = value.get("result") {
        if result.is_array() {
            result.as_array().and_then(|arr| arr.first())
        } else {
            Some(result)
        }
    } else {
        Some(value)
    }
}

fn event_name(value: &Value) -> Option<&str> {
    value.get("event").and_then(|v| v.as_str())
}

fn as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::String(s) => match s.parse::<f64>() {
            Ok(v) if v.is_finite() => Some(v),
            Ok(_) => {
                log_parse_drop("gate_collector", "non_finite", &"non-finite number", s);
                None
            }
            Err(err) => {
                log_parse_drop("gate_collector", "f64", &err, s);
                None
            }
        },
        Value::Number(n) => {
            let v = n.as_f64()?;
            if v.is_finite() {
                Some(v)
            } else {
                log_parse_drop(
                    "gate_collector",
                    "non_finite",
                    &"non-finite number",
                    &n.to_string(),
                );
                None
            }
        }
        _ => None,
    }
}

fn as_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(n) => {
            let v = n.as_u64();
            if v.is_none() {
                log_parse_drop("gate_collector", "u64", &"non-u64 number", &n.to_string());
            }
            v
        }
        Value::String(s) => match s.parse::<u64>() {
            Ok(v) => Some(v),
            Err(err) => {
                log_parse_drop("gate_collector", "u64", &err, s);
                None
            }
        },
        _ => None,
    }
}

fn value_to_f64<'a>(value: &'a Value, keys: &[&str]) -> Option<f64> {
    for key in keys {
        if let Some(v) = value.get(*key) {
            if let Some(num) = as_f64(v) {
                return Some(num);
            }
        }
    }
    None
}

fn value_to_u64<'a>(value: &'a Value, keys: &[&str]) -> Option<u64> {
    for key in keys {
        if let Some(v) = value.get(*key) {
            if let Some(num) = as_u64(v) {
                return Some(num);
            }
        }
    }
    None
}

pub fn update_tickers(
    s: &str,
    store: &mut TickerStore,
    qty_multiplier: f64,
) -> Option<(String, TickerSnapshot)> {
    let raw: Value = match serde_json::from_str(s) {
        Ok(val) => val,
        Err(err) => {
            log_parse_drop("gate_collector", "json", &err, s);
            return None;
        }
    };
    if raw.get("channel").and_then(|v| v.as_str()) != Some("futures.tickers") {
        return None;
    }
    if let Some(event) = event_name(&raw) {
        if event != "update" {
            return None;
        }
    }

    let data_obj = first_result_object(&raw)?;
    if !data_obj.is_object() {
        return None;
    }

    let symbol = data_obj
        .get("contract")
        .or_else(|| data_obj.get("symbol"))
        .and_then(|v| v.as_str())?
        .to_string();

    let prev = store.get(&symbol).copied();
    let mut snapshot = prev.unwrap_or_default();

    if let Some(last_px) = value_to_f64(data_obj, &["last", "last_price"]) {
        snapshot.ticker.last_px = (last_px * PRICE_SCALE).round() as Price;
    }
    if let Some(bid_px) = value_to_f64(data_obj, &["best_bid", "bid"]) {
        snapshot.ticker.best_bid = (bid_px * PRICE_SCALE).round() as Price;
    }
    if let Some(ask_px) = value_to_f64(data_obj, &["best_ask", "ask"]) {
        snapshot.ticker.best_ask = (ask_px * PRICE_SCALE).round() as Price;
    }

    if let Some(mark) = value_to_f64(data_obj, &["mark_price"]) {
        snapshot.mark_px = Some(mark);
    }
    if let Some(index) = value_to_f64(data_obj, &["index_price"]) {
        snapshot.index_px = Some(index);
    }
    if let Some(rate) = value_to_f64(data_obj, &["funding_rate"]) {
        snapshot.funding_rate = Some(rate);
    }
    if let Some(turnover) = value_to_f64(data_obj, &["volume_24h_settle", "volume_24h_quote"]) {
        snapshot.turnover_24h = Some(turnover);
    }
    if let Some(oi) = value_to_f64(data_obj, &["total_size", "open_interest"]) {
        snapshot.open_interest = Some(oi);
    }

    if let Some(last_size) = value_to_f64(data_obj, &["last_size", "last_qty"]) {
        snapshot.ticker.last_qty = (last_size * qty_multiplier * QTY_SCALE).round() as Qty;
    }

    let ts_ms = match value_to_u64(data_obj, &["time_ms", "ts"])
        .or_else(|| raw.get("time_ms").and_then(as_u64))
    {
        Some(ts_ms) if ts_ms > 0 => ts_ms,
        _ => {
            log_parse_drop("gate_collector", "missing_ts", &"missing ts", s);
            return None;
        }
    };
    snapshot.ticker.ts = ms_to_ns(ts_ms);

    let fallback_seq = prev
        .map(|snapshot| snapshot.ticker.seq.wrapping_add(1))
        .unwrap_or(1);
    snapshot.ticker.seq = value_to_u64(data_obj, &["update_id", "seq", "t"])
        .filter(|seq| *seq > 0)
        .unwrap_or(fallback_seq);

    let stored = store.update(symbol.clone(), snapshot);
    Some((symbol, stored))
}

#[cfg(test)]
mod tests {
    use crate::base_classes::orderbook_trait::OrderBookOps;

    use super::*;

    #[test]
    fn test_update_tickers_gate_basic() {
        let json = r#"{
            "time":1700000000,
            "channel":"futures.tickers",
            "result":[{
                "contract":"BTC_USDT",
                "last":"43000",
                "mark_price":"43005",
                "index_price":"42995",
                "funding_rate":"0.0001",
                "volume_24h_settle":"1234567.89",
                "total_size":"456.0",
                "last_size":"2.5",
                "seq":"123456",
                "time_ms":1700000000000
            }]
        }"#;

        let mut store = TickerStore::default();
        let (_, snap) = update_tickers(json, &mut store, 1.0).expect("ticker parsed");

        assert_eq!(
            snap.ticker.last_px,
            (43000.0 * PRICE_SCALE).round() as Price
        );
        assert_eq!(snap.mark_px, Some(43005.0));
        assert_eq!(snap.index_px, Some(42995.0));
        assert_eq!(snap.funding_rate, Some(0.0001));
        assert_eq!(snap.turnover_24h, Some(1_234_567.89));
        assert_eq!(snap.open_interest, Some(456.0));
        assert_eq!(snap.quanto_multiplier, None);
        assert_eq!(snap.open_interest_value, None);
        assert_eq!(snap.ticker.last_qty, (2.5 * QTY_SCALE).round() as Qty);
        assert_eq!(snap.ticker.ts, 1_700_000_000_000_000_000);
        assert_eq!(snap.ticker.seq, 123_456);
    }

    #[test]
    fn test_update_bbo_store_gate() {
        let json = r#"{
            "channel":"futures.book_ticker",
            "event":"update",
            "result":{
                "t":1758808025186,
                "u":89543752356,
                "s":"BTC_USDT",
                "b":"110756.8",
                "B":1,
                "a":"110756.9",
                "A":49712
            }
        }"#;
        let mut store = BboStore::default();
        assert!(update_bbo_store(json, &mut store, 1.0));
        let mid = store.mid_price_f64_for("BTC_USDT").unwrap();
        assert!((mid - 110756.85).abs() < 1e-6);
    }

    #[test]
    fn test_update_tickers_gate_uses_top_level_timestamp_and_synthesizes_sequence() {
        let json = r#"{
            "time_ms":1700000000123,
            "event":"update",
            "channel":"futures.tickers",
            "result":[{
                "contract":"BTC_USDT",
                "last":"43000"
            }]
        }"#;
        let mut store = TickerStore::default();
        let (_, snap) = update_tickers(json, &mut store, 1.0).expect("ticker parsed");
        assert_eq!(snap.ticker.ts, 1_700_000_000_123_000_000);
        assert_eq!(snap.ticker.seq, 1);
    }

    #[test]
    fn test_update_bbo_store_gate_ignores_subscribe_ack() {
        let json = r#"{
            "time":1700000000,
            "time_ms":1700000000100,
            "channel":"futures.book_ticker",
            "event":"subscribe",
            "payload":["BTC_USDT"],
            "result":{"status":"success"}
        }"#;
        let mut store = BboStore::default();
        assert!(!update_bbo_store(json, &mut store, 1.0));
        assert!(store.get("BTC_USDT").is_none());
    }

    #[test]
    fn test_update_trades_gate_rejects_missing_trade_id() {
        let json = r#"{
            "channel":"futures.trades",
            "time_ms":1700000000100,
            "result":[{"price":"43000","size":"-0.5","create_time_ms":1700000000000}]
        }"#;
        let mut trades = FixedTrades::<4>::default();
        assert_eq!(update_trades(json, &mut trades, 1.0), 0);
    }

    #[test]
    fn test_events_for_ignores_obu_subscribe_ack() {
        let json = r#"{
            "time":1772858040,
            "time_ms":1772858040687,
            "channel":"futures.obu",
            "event":"subscribe",
            "payload":["ob.XRP_USDT.50"],
            "result":{"status":"success"}
        }"#;
        let mut book = GateBook::<16>::new(
            "XRP_USDT",
            GateBook::<16>::PRICE_SCALE,
            GateBook::<16>::QTY_SCALE,
            1.0,
        );

        let events = events_for(json, &mut book);

        assert!(events.is_empty());
        assert!(!book.is_initialized());
    }

    #[test]
    fn test_events_for_processes_obu_snapshot_and_delta_without_seq_or_full() {
        let snapshot = r#"{
            "channel":"futures.obu",
            "event":"update",
            "result":{
                "t":1772858041160,
                "s":"ob.XRP_USDT.50",
                "u":24819532359,
                "full":true,
                "b":[["1.3623","477"]],
                "a":[["1.3624","312"]]
            },
            "time_ms":1772858041161
        }"#;
        let delta = r#"{
            "channel":"futures.obu",
            "event":"update",
            "result":{
                "t":1772858041410,
                "s":"ob.XRP_USDT.50",
                "U":24819532360,
                "u":24819532365,
                "a":[["1.3624","351"]]
            },
            "time_ms":1772858041411
        }"#;
        let mut book = GateBook::<16>::new(
            "XRP_USDT",
            GateBook::<16>::PRICE_SCALE,
            GateBook::<16>::QTY_SCALE,
            1.0,
        );

        let snapshot_events = events_for(snapshot, &mut book);
        assert_eq!(snapshot_events, vec![("orderbook", 1.36235)]);
        assert!(book.is_initialized());

        let delta_events = events_for(delta, &mut book);
        assert_eq!(delta_events, vec![("orderbook", 1.36235)]);
        assert_eq!(book.best_ask_f64(), Some((1.3624, 351.0)));
    }

    #[test]
    fn test_events_for_advances_timestamp_on_empty_obu_delta() {
        let snapshot = r#"{
            "channel":"futures.obu",
            "event":"update",
            "result":{
                "t":1772858041160,
                "s":"ob.XRP_USDT.50",
                "u":24819532359,
                "full":true,
                "b":[["1.3623","477"]],
                "a":[["1.3624","312"]]
            },
            "time_ms":1772858041161
        }"#;
        let empty_delta = r#"{
            "channel":"futures.obu",
            "event":"update",
            "result":{
                "t":1772858041200,
                "s":"ob.XRP_USDT.50",
                "U":24819532360,
                "u":24819532360,
                "b":[],
                "a":[]
            },
            "time_ms":1772858041201
        }"#;
        let mut book = GateBook::<16>::new(
            "XRP_USDT",
            GateBook::<16>::PRICE_SCALE,
            GateBook::<16>::QTY_SCALE,
            1.0,
        );

        assert_eq!(
            events_for(snapshot, &mut book),
            vec![("orderbook", 1.36235)]
        );
        let snapshot_ts = book.last_ts();

        let delta_events = events_for(empty_delta, &mut book);
        assert_eq!(delta_events, vec![("orderbook", 1.36235)]);
        assert!(book.last_ts() > snapshot_ts);
    }
}

// Update BBO store for Gate from futures.book_ticker
pub fn update_bbo_store(s: &str, store: &mut BboStore, qty_multiplier: f64) -> bool {
    let raw = match serde_json::from_str::<Value>(s) {
        Ok(raw) => raw,
        Err(err) => {
            log_parse_drop("gate_collector", "json", &err, s);
            return false;
        }
    };
    if raw.get("channel").and_then(|v| v.as_str()) == Some("futures.book_ticker") {
        if let Some(event) = event_name(&raw) {
            if event != "update" {
                return false;
            }
        }
        if let Some(obj) = first_result_object(&raw) {
            let bid = match obj.get("b").or_else(|| obj.get("bid")).and_then(as_f64) {
                Some(bid) => bid,
                None => {
                    log_parse_drop("gate_collector", "missing_bid", &"missing bid", s);
                    return false;
                }
            };
            let ask = match obj.get("a").or_else(|| obj.get("ask")).and_then(as_f64) {
                Some(ask) => ask,
                None => {
                    log_parse_drop("gate_collector", "missing_ask", &"missing ask", s);
                    return false;
                }
            };
            let bid_qty = match obj
                .get("B")
                .or_else(|| obj.get("bid_size"))
                .or_else(|| obj.get("bidSize"))
                .and_then(as_f64)
            {
                Some(qty) => qty,
                None => {
                    log_parse_drop("gate_collector", "missing_bid_qty", &"missing bid qty", s);
                    return false;
                }
            };
            let ask_qty = match obj
                .get("A")
                .or_else(|| obj.get("ask_size"))
                .or_else(|| obj.get("askSize"))
                .and_then(as_f64)
            {
                Some(qty) => qty,
                None => {
                    log_parse_drop("gate_collector", "missing_ask_qty", &"missing ask qty", s);
                    return false;
                }
            };
            let ts_ms = match obj
                .get("t")
                .and_then(as_u64)
                .or_else(|| raw.get("time_ms").and_then(as_u64))
            {
                Some(ts_ms) if ts_ms > 0 => ts_ms,
                _ => {
                    log_parse_drop("gate_collector", "missing_ts", &"missing ts", s);
                    return false;
                }
            };
            let ts_ns = ms_to_ns(ts_ms);
            let system_ts_ns = raw.get("time_ms").and_then(as_u64).map(ms_to_ns);
            let symbol = match obj
                .get("contract")
                .or_else(|| obj.get("symbol"))
                .or_else(|| obj.get("s"))
                .and_then(|v| v.as_str())
                .or_else(|| raw.get("contract").and_then(|v| v.as_str()))
            {
                Some(symbol) => symbol,
                None => {
                    log_parse_drop("gate_collector", "missing_symbol", &"missing symbol", s);
                    return false;
                }
            };
            store.update(
                symbol,
                bid,
                bid_qty * qty_multiplier,
                ask,
                ask_qty * qty_multiplier,
                ts_ns,
                system_ts_ns,
            );
            return true;
        }
    }
    false
}

// Update trades store for Gate from futures.trades
pub fn update_trades<const N: usize>(
    s: &str,
    trades: &mut FixedTrades<N>,
    qty_multiplier: f64,
) -> usize {
    let raw = match serde_json::from_str::<Value>(s) {
        Ok(raw) => raw,
        Err(err) => {
            log_parse_drop("gate_collector", "json", &err, s);
            return 0;
        }
    };
    if raw.get("channel").and_then(|v| v.as_str()) == Some("futures.trades") {
        if let Some(event) = event_name(&raw) {
            if event != "update" {
                return 0;
            }
        }
        let mut inserted = 0usize;
        if let Some(entries) = raw.get("result").and_then(|res| {
            if res.is_array() {
                Some(res.as_array().expect("validated array above"))
            } else {
                None
            }
        }) {
            for entry in entries {
                let price = match entry
                    .get("price")
                    .or_else(|| entry.get("p"))
                    .and_then(as_f64)
                {
                    Some(price) => price,
                    None => {
                        log_parse_drop("gate_collector", "missing_price", &"missing price", s);
                        continue;
                    }
                };
                let size = match entry
                    .get("size")
                    .or_else(|| entry.get("v"))
                    .or_else(|| entry.get("amount"))
                    .and_then(as_f64)
                {
                    Some(size) => size,
                    None => {
                        log_parse_drop("gate_collector", "missing_size", &"missing size", s);
                        continue;
                    }
                };
                let ts_ms = match entry
                    .get("create_time_ms")
                    .or_else(|| entry.get("t"))
                    .or_else(|| entry.get("ts"))
                    .and_then(as_u64)
                    .or_else(|| raw.get("time_ms").and_then(as_u64))
                {
                    Some(ts_ms) if ts_ms > 0 => ts_ms,
                    _ => {
                        log_parse_drop("gate_collector", "missing_ts", &"missing ts", s);
                        continue;
                    }
                };
                let seq = match entry
                    .get("id")
                    .or_else(|| entry.get("trade_id"))
                    .and_then(as_u64)
                {
                    Some(seq) if seq > 0 => seq as Seq,
                    _ => {
                        log_parse_drop("gate_collector", "missing_seq", &"missing seq", s);
                        continue;
                    }
                };
                let px_i = (price * PRICE_SCALE).round() as Price;
                let qty_i = (size * qty_multiplier * QTY_SCALE).round() as Qty;
                let ts_ns = ms_to_ns(ts_ms);
                let system_ts_ns = raw.get("time_ms").and_then(as_u64).map(ms_to_ns);
                let is_buyer_maker = size < 0.0;
                trades.push(Trade::new(
                    px_i,
                    qty_i,
                    ts_ns,
                    seq,
                    is_buyer_maker,
                    system_ts_ns,
                ));
                inserted += 1;
            }
        }
        return inserted;
    }
    0
}
