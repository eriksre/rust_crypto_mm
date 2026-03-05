use crate::base_classes::bbo_store::BboStore;
use crate::base_classes::tickers::{TickerSnapshot, TickerStore};
use crate::base_classes::trades::{FixedTrades, Trade};
use crate::base_classes::types::{Price, Qty, Seq};
use crate::exchanges::okx::{OkxBook, OkxFrame};
use crate::utils::parsing::log_parse_drop;
use crate::utils::time::ms_to_ns;
use serde_json::{self, Value};

pub fn events_for<const N: usize>(
    frame: &mut OkxFrame,
    book: &mut OkxBook<N>,
) -> Vec<(&'static str, f64)> {
    let mut out = Vec::with_capacity(1);
    match frame.channel() {
        "books" => {
            if let Some(msg) = frame.orderbook_msg() {
                if book.apply(msg) {
                    if let Some(mid) = book.mid_price_f64() {
                        out.push(("orderbook", mid));
                    }
                }
            }
        }
        "bbo-tbt" => {
            if let Some(msg) = frame.orderbook_msg() {
                if book.apply_bbo(msg) {
                    if let Some(mid) = book.mid_price_f64() {
                        out.push(("orderbook", mid));
                    }
                }
            }
        }
        _ => {}
    }
    out
}

pub const PRICE_SCALE: f64 = OkxBook::<1>::PRICE_SCALE;
pub const QTY_SCALE: f64 = OkxBook::<1>::QTY_SCALE;

pub fn update_tickers(
    frame: &mut OkxFrame,
    store: &mut TickerStore,
    qty_multiplier: f64,
) -> Option<(String, TickerSnapshot)> {
    let sample = frame.text().unwrap_or("").to_string();
    let raw = frame.json()?;
    let channel = raw
        .get("arg")
        .and_then(|arg| arg.get("channel"))
        .and_then(|v| v.as_str())?;
    if channel != "tickers" {
        return None;
    }

    let data = raw.get("data").and_then(|v| v.as_array())?;
    let payload = data.first()?;
    if !payload.is_object() {
        return None;
    }

    let inst_id = payload.get("instId").and_then(|v| v.as_str()).or_else(|| {
        raw.get("arg")
            .and_then(|arg| arg.get("instId"))
            .and_then(|v| v.as_str())
    })?;

    let prev = store.get(inst_id).copied();
    let mut snapshot = prev.unwrap_or_default();

    if let Some(last_px) = value_to_f64(payload, &["last"]) {
        snapshot.ticker.last_px = (last_px * PRICE_SCALE).round() as Price;
    }
    if let Some(last_sz) = value_to_f64(payload, &["lastSz"]) {
        snapshot.ticker.last_qty = (last_sz * qty_multiplier * QTY_SCALE).round() as Qty;
    }
    if let Some(bid_px) = value_to_f64(payload, &["bidPx"]) {
        snapshot.ticker.best_bid = (bid_px * PRICE_SCALE).round() as Price;
    }
    if let Some(ask_px) = value_to_f64(payload, &["askPx"]) {
        snapshot.ticker.best_ask = (ask_px * PRICE_SCALE).round() as Price;
    }

    snapshot.mark_px = value_to_f64(payload, &["markPx"]).or(snapshot.mark_px);
    snapshot.index_px = value_to_f64(payload, &["idxPx", "indexPx"]).or(snapshot.index_px);
    snapshot.funding_rate = value_to_f64(payload, &["fundingRate"]).or(snapshot.funding_rate);
    snapshot.turnover_24h =
        value_to_f64(payload, &["volCcy24h", "volCcyQuote"]).or(snapshot.turnover_24h);
    snapshot.open_interest = value_to_f64(payload, &["openInterest"]).or(snapshot.open_interest);
    snapshot.open_interest_value =
        value_to_f64(payload, &["openInterestCcy"]).or(snapshot.open_interest_value);

    if let Some(seq) = value_to_u64(payload, &["seqId", "seq"]) {
        snapshot.ticker.seq = seq;
    } else {
        // OKX tickers payloads do not guarantee seq fields; use local monotonic fallback.
        snapshot.ticker.seq = 0;
    }

    let ts_ns = match value_to_u64(payload, &["ts"]) {
        Some(ts_ms) => ms_to_ns(ts_ms),
        None => {
            log_parse_drop(
                "okx_collector",
                "missing_ts",
                &"missing ts",
                sample.as_str(),
            );
            return None;
        }
    };
    if let Some(prev) = prev {
        let prev_ts = prev.ticker.ts;
        if prev_ts != 0 && ts_ns < prev_ts {
            let err = format!("stale ticker ts {} < {}", ts_ns, prev_ts);
            log_parse_drop("okx_collector", "stale_ts", &err, sample.as_str());
            return None;
        }
    }
    snapshot.ticker.ts = ts_ns;

    let stored = store.update(inst_id.to_string(), snapshot);
    Some((inst_id.to_string(), stored))
}

pub fn update_bbo_store(frame: &mut OkxFrame, store: &mut BboStore, qty_multiplier: f64) -> bool {
    let sample = frame.text().unwrap_or("").to_string();
    let raw = match frame.json() {
        Some(v) => v,
        None => {
            log_parse_drop(
                "okx_collector",
                "missing_json",
                &"missing json",
                sample.as_str(),
            );
            return false;
        }
    };
    let channel = raw
        .get("arg")
        .and_then(|arg| arg.get("channel"))
        .and_then(|v| v.as_str());
    if channel != Some("bbo-tbt") {
        return false;
    }
    let inst_id = raw
        .get("arg")
        .and_then(|arg| arg.get("instId"))
        .and_then(|v| v.as_str())
        .or_else(|| {
            log_parse_drop(
                "okx_collector",
                "missing_inst_id",
                &"missing instId",
                sample.as_str(),
            );
            None
        })
        .unwrap_or_default();
    let data = raw
        .get("data")
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.first());
    let payload = match data {
        Some(Value::Object(map)) => map,
        _ => return false,
    };

    let bid = payload
        .get("bids")
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.first())
        .and_then(|lvl| level_to_pair(lvl));
    let ask = payload
        .get("asks")
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.first())
        .and_then(|lvl| level_to_pair(lvl));

    let (bid_px, bid_qty) = match bid {
        Some((px, qty)) => (px, qty),
        None => return false,
    };
    let (ask_px, ask_qty) = match ask {
        Some((px, qty)) => (px, qty),
        None => return false,
    };

    let ts_ms = payload
        .get("ts")
        .and_then(|v| value_to_u64_raw(v))
        .or_else(|| {
            log_parse_drop(
                "okx_collector",
                "missing_ts",
                &"missing ts",
                sample.as_str(),
            );
            None
        });
    let ts_ms = match ts_ms {
        Some(ts) => ts,
        None => return false,
    };
    let ts_ns = ms_to_ns(ts_ms);
    store.update(
        inst_id,
        bid_px,
        bid_qty * qty_multiplier,
        ask_px,
        ask_qty * qty_multiplier,
        ts_ns,
        None,
    );
    true
}

pub fn update_trades<const N: usize>(
    frame: &mut OkxFrame,
    trades: &mut FixedTrades<N>,
    qty_multiplier: f64,
) -> usize {
    let sample = frame.text().unwrap_or("").to_string();
    let raw = match frame.json() {
        Some(v) => v,
        None => {
            log_parse_drop(
                "okx_collector",
                "missing_json",
                &"missing json",
                sample.as_str(),
            );
            return 0;
        }
    };
    let channel = raw
        .get("arg")
        .and_then(|arg| arg.get("channel"))
        .and_then(|v| v.as_str());
    if channel != Some("trades") {
        return 0;
    }

    let data = match raw.get("data").and_then(|v| v.as_array()) {
        Some(arr) => arr,
        None => return 0,
    };

    let mut inserted = 0usize;
    for entry in data {
        let price = entry
            .get("px")
            .and_then(|v| v.as_str())
            .and_then(|s| match s.parse::<f64>() {
                Ok(v) if v.is_finite() => Some(v),
                Ok(_) => {
                    log_parse_drop("okx_collector", "non_finite_px", &"non-finite px", s);
                    None
                }
                Err(err) => {
                    log_parse_drop("okx_collector", "px", &err, s);
                    None
                }
            })
            .or_else(|| entry.get("px").and_then(|v| v.as_f64()));
        let size = entry
            .get("sz")
            .and_then(|v| v.as_str())
            .and_then(|s| match s.parse::<f64>() {
                Ok(v) if v.is_finite() => Some(v),
                Ok(_) => {
                    log_parse_drop("okx_collector", "non_finite_qty", &"non-finite qty", s);
                    None
                }
                Err(err) => {
                    log_parse_drop("okx_collector", "qty", &err, s);
                    None
                }
            })
            .or_else(|| entry.get("sz").and_then(|v| v.as_f64()));
        let ts_ms = entry
            .get("ts")
            .and_then(|v| v.as_str())
            .and_then(|s| match s.parse::<u64>() {
                Ok(v) => Some(v),
                Err(err) => {
                    log_parse_drop("okx_collector", "ts", &err, s);
                    None
                }
            })
            .or_else(|| entry.get("ts").and_then(|v| v.as_u64()));
        if price.is_none() || size.is_none() || ts_ms.is_none() {
            log_parse_drop(
                "okx_collector",
                "missing_trade_fields",
                &"missing trade fields",
                sample.as_str(),
            );
            continue;
        }
        let px_i = (price.unwrap() * PRICE_SCALE).round() as Price;
        let qty_i = (size.unwrap() * qty_multiplier * QTY_SCALE).round() as Qty;
        let ts_ns = ms_to_ns(ts_ms.unwrap());
        let seq = entry
            .get("tradeId")
            .and_then(|v| v.as_str())
            .and_then(|s| match s.parse::<u64>() {
                Ok(v) => Some(v),
                Err(err) => {
                    log_parse_drop("okx_collector", "seq", &err, s);
                    None
                }
            })
            .or_else(|| entry.get("tradeId").and_then(|v| v.as_u64()))
            .or_else(|| {
                log_parse_drop(
                    "okx_collector",
                    "missing_seq",
                    &"missing seq",
                    sample.as_str(),
                );
                None
            })
            .map(|v| v as Seq);
        let side = entry.get("side").and_then(|v| v.as_str()).or_else(|| {
            log_parse_drop(
                "okx_collector",
                "missing_side",
                &"missing side",
                sample.as_str(),
            );
            None
        });
        if let (Some(seq), Some(side)) = (seq, side) {
            let is_buyer_maker = side.eq_ignore_ascii_case("sell");
            let trade = Trade::new(px_i, qty_i, ts_ns, seq, is_buyer_maker, None);
            trades.push(trade);
            inserted += 1;
        }
    }
    inserted
}

fn value_to_f64(value: &Value, keys: &[&str]) -> Option<f64> {
    for key in keys {
        if let Some(entry) = value.get(*key) {
            if let Some(v) = value_to_f64_raw(entry) {
                return Some(v);
            }
        }
    }
    None
}

fn value_to_f64_raw(value: &Value) -> Option<f64> {
    match value {
        Value::Number(n) => {
            let v = n.as_f64()?;
            if v.is_finite() {
                Some(v)
            } else {
                log_parse_drop(
                    "okx_collector",
                    "non_finite",
                    &"non-finite number",
                    &n.to_string(),
                );
                None
            }
        }
        Value::String(s) => match s.parse::<f64>() {
            Ok(v) if v.is_finite() => Some(v),
            Ok(_) => {
                log_parse_drop("okx_collector", "non_finite", &"non-finite number", s);
                None
            }
            Err(err) => {
                log_parse_drop("okx_collector", "f64", &err, s);
                None
            }
        },
        _ => None,
    }
}

fn value_to_u64(value: &Value, keys: &[&str]) -> Option<u64> {
    for key in keys {
        if let Some(entry) = value.get(*key) {
            if let Some(v) = value_to_u64_raw(entry) {
                return Some(v);
            }
        }
    }
    None
}

fn value_to_u64_raw(value: &Value) -> Option<u64> {
    match value {
        Value::Number(n) => {
            let v = n.as_u64();
            if v.is_none() {
                log_parse_drop("okx_collector", "u64", &"non-u64 number", &n.to_string());
            }
            v
        }
        Value::String(s) => match s.parse::<u64>() {
            Ok(v) => Some(v),
            Err(err) => {
                log_parse_drop("okx_collector", "u64", &err, s);
                None
            }
        },
        _ => None,
    }
}

fn level_to_pair(value: &Value) -> Option<(f64, f64)> {
    let arr = value.as_array()?;
    let px_str = arr.get(0)?.as_str()?;
    let qty_str = arr.get(1)?.as_str()?;
    let px = match px_str.parse::<f64>() {
        Ok(v) if v.is_finite() => v,
        Ok(_) => {
            log_parse_drop("okx_collector", "non_finite_px", &"non-finite px", px_str);
            return None;
        }
        Err(err) => {
            log_parse_drop("okx_collector", "px", &err, px_str);
            return None;
        }
    };
    let qty = match qty_str.parse::<f64>() {
        Ok(v) if v.is_finite() => v,
        Ok(_) => {
            log_parse_drop(
                "okx_collector",
                "non_finite_qty",
                &"non-finite qty",
                qty_str,
            );
            return None;
        }
        Err(err) => {
            log_parse_drop("okx_collector", "qty", &err, qty_str);
            return None;
        }
    };
    Some((px, qty))
}

#[cfg(test)]
mod tests {
    use super::update_tickers;
    use crate::base_classes::tickers::TickerStore;
    use crate::exchanges::okx::OkxFrame;
    use crate::utils::time::ms_to_ns;
    use std::time::Instant;

    #[test]
    fn update_tickers_accepts_missing_seq_with_local_monotonic_fallback() {
        let mut store = TickerStore::default();
        let msg1 = r#"{"arg":{"channel":"tickers","instId":"SOL-USDT-SWAP"},"data":[{"instType":"SWAP","instId":"SOL-USDT-SWAP","last":"90.1","lastSz":"0.01","askPx":"90.2","askSz":"10","bidPx":"90.0","bidSz":"11","ts":"1000"}]}"#;
        let msg2 = r#"{"arg":{"channel":"tickers","instId":"SOL-USDT-SWAP"},"data":[{"instType":"SWAP","instId":"SOL-USDT-SWAP","last":"90.2","lastSz":"0.02","askPx":"90.3","askSz":"12","bidPx":"90.1","bidSz":"13","ts":"1001"}]}"#;

        let mut frame1 = OkxFrame::from_text(msg1, 1, Instant::now());
        let (_, snap1) = update_tickers(&mut frame1, &mut store, 1.0).expect("first ticker");
        assert_eq!(snap1.ticker.seq, 1);
        assert_eq!(snap1.ticker.ts, ms_to_ns(1000));

        let mut frame2 = OkxFrame::from_text(msg2, 2, Instant::now());
        let (_, snap2) = update_tickers(&mut frame2, &mut store, 1.0).expect("second ticker");
        assert_eq!(snap2.ticker.seq, 2);
        assert_eq!(snap2.ticker.ts, ms_to_ns(1001));
    }

    #[test]
    fn update_tickers_rejects_stale_timestamp() {
        let mut store = TickerStore::default();
        let newer = r#"{"arg":{"channel":"tickers","instId":"SOL-USDT-SWAP"},"data":[{"instType":"SWAP","instId":"SOL-USDT-SWAP","last":"90.2","lastSz":"0.02","askPx":"90.3","askSz":"12","bidPx":"90.1","bidSz":"13","ts":"2000"}]}"#;
        let older = r#"{"arg":{"channel":"tickers","instId":"SOL-USDT-SWAP"},"data":[{"instType":"SWAP","instId":"SOL-USDT-SWAP","last":"90.1","lastSz":"0.01","askPx":"90.2","askSz":"10","bidPx":"90.0","bidSz":"11","ts":"1999"}]}"#;

        let mut newer_frame = OkxFrame::from_text(newer, 1, Instant::now());
        update_tickers(&mut newer_frame, &mut store, 1.0).expect("newer ticker");

        let mut older_frame = OkxFrame::from_text(older, 2, Instant::now());
        let stale = update_tickers(&mut older_frame, &mut store, 1.0);
        assert!(stale.is_none(), "stale ticker update must be rejected");

        let stored = store
            .get("SOL-USDT-SWAP")
            .expect("store must retain last accepted ticker");
        assert_eq!(stored.ticker.ts, ms_to_ns(2000));
    }
}
