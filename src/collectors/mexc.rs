use crate::base_classes::bbo_store::BboStore;
use crate::base_classes::tickers::{TickerSnapshot, TickerStore};
use crate::base_classes::trades::{FixedTrades, Trade};
use crate::base_classes::types::{Price, Qty, Seq};
use crate::exchanges::mexc::{MexcBook, MexcFrame};
use crate::utils::time::ms_to_ns;
use serde_json::Value;

pub const PRICE_SCALE: f64 = MexcBook::<1>::PRICE_SCALE;
pub const QTY_SCALE: f64 = MexcBook::<1>::QTY_SCALE;

pub fn events_for<const N: usize>(
    frame: &mut MexcFrame,
    book: &mut MexcBook<N>,
) -> Vec<(&'static str, f64)> {
    let mut out = Vec::with_capacity(1);
    if matches!(frame.channel(), Some("push.depth")) {
        if let Some(msg) = frame.depth_msg() {
            if book.apply(msg) {
                if let Some(mid) = book.mid_price_f64() {
                    out.push(("orderbook", mid));
                }
            }
        }
    }
    out
}

fn as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn as_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => s.parse::<u64>().ok(),
        _ => None,
    }
}

pub fn update_bbo_store(frame: &mut MexcFrame, store: &mut BboStore) -> bool {
    // Mexc BBO updates are intentionally disabled; uncomment the block below to restore them.
    /*
    if !matches!(frame.channel(), Some("push.ticker")) {
        return false;
    }
    let raw = match frame.json() {
        Some(v) => v,
        None => return false,
    };
    let data = match raw.get("data") {
        Some(Value::Object(map)) => map,
        _ => return false,
    };
    let symbol = match data.get("symbol").and_then(|v| v.as_str()) {
        Some(s) => s,
        None => return false,
    };
    let bid = data.get("bid1").and_then(as_f64).unwrap_or(0.0);
    let ask = data.get("ask1").and_then(as_f64).unwrap_or(0.0);
    if bid <= 0.0 || ask <= 0.0 {
        return false;
    }
    let ts_ms = data
        .get("timestamp")
        .and_then(as_u64)
        .or_else(|| raw.get("ts").and_then(as_u64))
        .unwrap_or(0);
    let ts_ns = ms_to_ns(ts_ms);
    store.update(symbol, bid, 0.0, ask, 0.0, ts_ns, Some(ts_ns));
    true
    */
    let _ = frame;
    let _ = store;
    false
}

pub fn update_trades<const N: usize>(frame: &mut MexcFrame, trades: &mut FixedTrades<N>) -> usize {
    if !matches!(frame.channel(), Some("push.deal")) {
        return 0;
    }
    let raw = match frame.json() {
        Some(v) => v,
        None => return 0,
    };
    let entries = match raw.get("data").and_then(|v| v.as_array()) {
        Some(arr) => arr,
        None => return 0,
    };
    let system_ts_ns = raw.get("ts").and_then(as_u64).map(ms_to_ns);
    let mut inserted = 0usize;
    for trade in entries {
        let price = trade.get("p").and_then(as_f64);
        let qty = trade.get("v").and_then(as_f64);
        let side = trade.get("T").and_then(as_u64);
        let ts_ms = trade
            .get("t")
            .and_then(as_u64)
            .or_else(|| raw.get("ts").and_then(as_u64))
            .unwrap_or(0);
        if price.is_none() || qty.is_none() || side.is_none() {
            continue;
        }
        let px_i = (price.unwrap() * PRICE_SCALE).round() as Price;
        let qty_i = (qty.unwrap() * QTY_SCALE).round() as Qty;
        let seq = trade.get("t").and_then(as_u64).unwrap_or_else(|| ts_ms);
        let is_buyer_maker = side.unwrap_or(0) != 1;
        let record = Trade::new(
            px_i,
            qty_i,
            ms_to_ns(ts_ms),
            seq as Seq,
            is_buyer_maker,
            system_ts_ns,
        );
        trades.push(record);
        inserted += 1;
    }
    inserted
}

pub fn update_tickers(
    frame: &mut MexcFrame,
    store: &mut TickerStore,
) -> Option<(String, TickerSnapshot)> {
    if !matches!(frame.channel(), Some("push.ticker")) {
        return None;
    }
    let raw = frame.json()?;
    let data = match raw.get("data") {
        Some(Value::Object(map)) => map,
        _ => return None,
    };
    let symbol = data.get("symbol").and_then(|v| v.as_str())?.to_string();
    let mut snapshot = store.get(&symbol).copied().unwrap_or_default();

    if let Some(last_px) = data.get("lastPrice").and_then(as_f64) {
        snapshot.ticker.last_px = (last_px * PRICE_SCALE).round() as Price;
    }
    if let Some(bid_px) = data.get("bid1").and_then(as_f64) {
        snapshot.ticker.best_bid = (bid_px * PRICE_SCALE).round() as Price;
    }
    if let Some(ask_px) = data.get("ask1").and_then(as_f64) {
        snapshot.ticker.best_ask = (ask_px * PRICE_SCALE).round() as Price;
    }
    if let Some(fair_px) = data.get("fairPrice").and_then(as_f64) {
        snapshot.mark_px = Some(fair_px);
    }
    if let Some(index_px) = data.get("indexPrice").and_then(as_f64) {
        snapshot.index_px = Some(index_px);
    }
    if let Some(funding) = data.get("fundingRate").and_then(as_f64) {
        snapshot.funding_rate = Some(funding);
    }
    if let Some(turnover) = data.get("amount24").and_then(as_f64) {
        snapshot.turnover_24h = Some(turnover);
    }
    if let Some(oi) = data.get("holdVol").and_then(as_f64) {
        snapshot.open_interest = Some(oi);
    }

    if let (Some(oi), Some(mark)) = (snapshot.open_interest, snapshot.mark_px) {
        snapshot.open_interest_value = Some(oi * mark);
    }

    if let Some(ts_ms) = data
        .get("timestamp")
        .and_then(as_u64)
        .or_else(|| raw.get("ts").and_then(as_u64))
    {
        snapshot.ticker.ts = ms_to_ns(ts_ms);
        snapshot.ticker.seq = ts_ms;
    } else {
        snapshot.ticker.seq = 0;
    }

    let stored = store.update(symbol.clone(), snapshot);
    Some((symbol, stored))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    const SAMPLE_PUSH_DEAL_BATCH: &str = r#"{
        "symbol": "BTC_USDT",
        "data": [
            {"p": 102900, "v": 130, "T": 2, "O": 3, "M": 2, "t": 1763026190867},
            {"p": 102900, "v": 100, "T": 2, "O": 3, "M": 2, "t": 1763026190837},
            {"p": 102900, "v": 4843, "T": 2, "O": 3, "M": 2, "t": 1763026190837},
            {"p": 102900, "v": 519, "T": 2, "O": 3, "M": 2, "t": 1763026190837},
            {"p": 102900.1, "v": 184, "T": 1, "O": 3, "M": 2, "t": 1763026190836},
            {"p": 102900, "v": 1, "T": 2, "O": 3, "M": 2, "t": 1763026190810},
            {"p": 102900, "v": 551, "T": 2, "O": 3, "M": 2, "t": 1763026190804},
            {"p": 102900, "v": 12, "T": 2, "O": 3, "M": 2, "t": 1763026190803},
            {"p": 1.029e5, "v": 1, "T": 2, "O": 3, "M": 2, "t": 1763026190803},
            {"p": 1.029e5, "v": 1, "T": 2, "O": 3, "M": 2, "t": 1763026190803},
            {"p": 1.029e5, "v": 7, "T": 2, "O": 3, "M": 2, "t": 1763026190803},
            {"p": 1.029e5, "v": 2, "T": 2, "O": 3, "M": 2, "t": 1763026190803},
            {"p": 1.029e5, "v": 1, "T": 2, "O": 3, "M": 2, "t": 1763026190803},
            {"p": 1.029e5, "v": 2, "T": 2, "O": 2, "M": 2, "t": 1763026190803},
            {"p": 1.029e5, "v": 13, "T": 2, "O": 3, "M": 2, "t": 1763026190802},
            {"p": 1.029e5, "v": 1, "T": 2, "O": 2, "M": 2, "t": 1763026190802},
            {"p": 1.029e5, "v": 55, "T": 2, "O": 3, "M": 2, "t": 1763026190802},
            {"p": 1.029e5, "v": 1e1, "T": 2, "O": 3, "M": 2, "t": 1763026190802},
            {"p": 1.029e5, "v": 1, "T": 2, "O": 3, "M": 2, "t": 1763026190802},
            {"p": 1.029e5, "v": 13, "T": 2, "O": 3, "M": 2, "t": 1763026190802},
            {"p": 1.029e5, "v": 1, "T": 2, "O": 3, "M": 2, "t": 1763026190802},
            {"p": 1.029e5, "v": 2, "T": 2, "O": 3, "M": 2, "t": 1763026190802},
            {"p": 1.029e5, "v": 1, "T": 2, "O": 3, "M": 2, "t": 1763026190802},
            {"p": 1.029e5, "v": 13, "T": 2, "O": 3, "M": 2, "t": 1763026190802},
            {"p": 102900, "v": 14, "T": 2, "O": 3, "M": 2, "t": 1763026190802},
            {"p": 1.029e5, "v": 1, "T": 2, "O": 3, "M": 2, "t": 1763026190802},
            {"p": 1.029e5, "v": 1, "T": 2, "O": 3, "M": 2, "t": 1763026190802},
            {"p": 1.029e5, "v": 1, "T": 2, "O": 3, "M": 2, "t": 1763026190802},
            {"p": 1.029e5, "v": 1, "T": 2, "O": 3, "M": 2, "t": 1763026190802},
            {"p": 1.029e5, "v": 1, "T": 2, "O": 3, "M": 2, "t": 1763026190802},
            {"p": 1.029e5, "v": 1, "T": 2, "O": 3, "M": 2, "t": 1763026190802},
            {"p": 1.029e5, "v": 1, "T": 2, "O": 2, "M": 2, "t": 1763026190802},
            {"p": 1.029e5, "v": 2, "T": 2, "O": 3, "M": 2, "t": 1763026190802},
            {"p": 1.029e5, "v": 1e1, "T": 2, "O": 3, "M": 2, "t": 1763026190802}
        ],
        "channel": "push.deal",
        "ts": 1763026190867
    }"#;

    const SAMPLE_PUSH_DEAL_SMALL: &str = r#"{
        "symbol": "BTC_USDT",
        "data": [
            {"p": 102901.8, "v": 5362, "T": 1, "O": 1, "M": 2, "t": 1763026190572},
            {"p": 102901.7, "v": 504, "T": 2, "O": 3, "M": 2, "t": 1763026190553},
            {"p": 102901.7, "v": 333, "T": 2, "O": 3, "M": 2, "t": 1763026190549},
            {"p": 102901.7, "v": 28, "T": 2, "O": 3, "M": 2, "t": 1763026190549},
            {"p": 102904, "v": 2, "T": 2, "O": 3, "M": 2, "t": 1763026190549},
            {"p": 102905, "v": 1, "T": 2, "O": 1, "M": 2, "t": 1763026190515},
            {"p": 102905.6, "v": 1, "T": 2, "O": 1, "M": 2, "t": 1763026190515}
        ],
        "channel": "push.deal",
        "ts": 1763026190572
    }"#;

    fn frame_from_str(json: &str) -> MexcFrame {
        let mut frame = MexcFrame::from_text(json, 0, Instant::now());
        frame.preparse_text(json);
        frame
    }

    #[test]
    fn test_update_trades_parses_push_deal_batch() {
        let mut frame = frame_from_str(SAMPLE_PUSH_DEAL_BATCH);
        let mut trades = FixedTrades::<64>::default();

        let inserted = update_trades(&mut frame, &mut trades);
        let expected = serde_json::from_str::<serde_json::Value>(SAMPLE_PUSH_DEAL_BATCH)
            .unwrap()
            .get("data")
            .and_then(|v| v.as_array())
            .map(|arr| arr.len())
            .unwrap();

        assert_eq!(frame.channel(), Some("push.deal"));
        assert_eq!(inserted, expected);
        assert_eq!(trades.len(), expected);

        let last_trade = trades.last().expect("missing last trade");
        assert_eq!(last_trade.px, (102_900.0 * PRICE_SCALE).round() as Price);
        assert_eq!(last_trade.qty, (10.0 * QTY_SCALE).round() as Qty);
        assert_eq!(last_trade.ts, ms_to_ns(1763026190802));
        assert_eq!(last_trade.seq, 1_763_026_190_802);
        assert!(last_trade.is_buyer_maker);
        assert_eq!(last_trade.system_ts_ns, Some(ms_to_ns(1_763_026_190_867)));

        let taker_buy_qty = (184.0 * QTY_SCALE).round() as Qty;
        let taker_buy = trades
            .iter_last(inserted)
            .find(|t| t.qty == taker_buy_qty)
            .expect("missing taker buy trade");
        assert!(!taker_buy.is_buyer_maker);
    }

    #[test]
    fn test_update_trades_flags_side_and_system_ts() {
        let mut frame = frame_from_str(SAMPLE_PUSH_DEAL_SMALL);
        let mut trades = FixedTrades::<16>::default();

        let inserted = update_trades(&mut frame, &mut trades);
        assert_eq!(inserted, 7);

        let trades_vec: Vec<_> = trades.iter_last(inserted).collect();
        let taker_buy_px = (102_901.8 * PRICE_SCALE).round() as Price;
        let taker_buy_qty = (5_362.0 * QTY_SCALE).round() as Qty;
        let taker_buy = trades_vec
            .iter()
            .find(|t| t.px == taker_buy_px && t.qty == taker_buy_qty)
            .expect("missing taker buy trade");
        assert!(!taker_buy.is_buyer_maker);
        assert_eq!(taker_buy.ts, ms_to_ns(1_763_026_190_572));
        assert_eq!(taker_buy.seq, 1_763_026_190_572);

        let last_trade = trades_vec.last().expect("missing last trade");
        assert_eq!(last_trade.system_ts_ns, Some(ms_to_ns(1_763_026_190_572)));
        assert_eq!(last_trade.ts, ms_to_ns(1_763_026_190_515));
        assert!(last_trade.is_buyer_maker);
    }
}
