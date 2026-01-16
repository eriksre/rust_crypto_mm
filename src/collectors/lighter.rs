use crate::base_classes::tickers::{TickerSnapshot, TickerStore};
use crate::base_classes::trades::{FixedTrades, Trade};
use crate::base_classes::types::{Price, Qty, Seq};
use crate::exchanges::lighter::{LighterBook, LighterFrame};
use crate::utils::parsing::log_parse_drop;
use crate::utils::time::ms_to_ns;

fn ts_from_exchange(raw: u64) -> u64 {
    if raw > 1_000_000_000_000 {
        ms_to_ns(raw)
    } else {
        raw.saturating_mul(1_000_000_000)
    }
}

fn parse_f64_field(field: &str, value: &str) -> Option<f64> {
    match value.parse::<f64>() {
        Ok(v) if v.is_finite() => Some(v),
        Ok(_) => {
            log_parse_drop("lighter_collector", field, &"non-finite number", value);
            None
        }
        Err(err) => {
            log_parse_drop("lighter_collector", field, &err, value);
            None
        }
    }
}

pub fn events_for<const N: usize>(
    frame: &mut LighterFrame,
    book: &mut LighterBook<N>,
    market_id: u32,
) -> Vec<(&'static str, f64)> {
    let sample = frame.text().unwrap_or("").to_string();
    let mut out = Vec::with_capacity(1);
    if let Some(msg) = frame.order_book_msg() {
        if msg
            .channel
            .rsplit_once(':')
            .and_then(|(_, id)| match id.parse::<u32>() {
                Ok(v) => Some(v),
                Err(err) => {
                    log_parse_drop("lighter_collector", "market_id", &err, id);
                    None
                }
            })
            != Some(market_id)
        {
            log_parse_drop(
                "lighter_collector",
                "market_id_mismatch",
                &"market_id mismatch",
                sample.as_str(),
            );
            return out;
        }
        if book.apply(msg) {
            if let Some(mid) = book.mid_price_f64() {
                out.push(("orderbook", mid));
            }
        }
    }
    out
}

pub fn update_trades<const N: usize>(
    frame: &mut LighterFrame,
    market_id: u32,
    trades: &mut FixedTrades<N>,
    price_scale: f64,
    qty_scale: f64,
) -> usize {
    let _frame_ts = frame.ts;
    let sample = frame.text().unwrap_or("").to_string();
    let msg = match frame.trades_msg() {
        Some(m) => m,
        None => return 0,
    };
    if msg
        .channel
        .rsplit_once(':')
        .and_then(|(_, id)| match id.parse::<u32>() {
            Ok(v) => Some(v),
            Err(err) => {
                log_parse_drop("lighter_collector", "market_id", &err, id);
                None
            }
        })
        != Some(market_id)
    {
        log_parse_drop(
            "lighter_collector",
            "market_id_mismatch",
            &"market_id mismatch",
            sample.as_str(),
        );
        return 0;
    }

    let mut inserted = 0usize;
    for trade in msg.trades.iter() {
        let px = trade
            .price
            .as_deref()
            .and_then(|v| parse_f64_field("trade_price", v))
            .or_else(|| {
                log_parse_drop(
                    "lighter_collector",
                    "missing_price",
                    &"missing price",
                    sample.as_str(),
                );
                None
            });
        let qty = trade
            .size
            .as_deref()
            .and_then(|v| parse_f64_field("trade_size", v))
            .or_else(|| {
                log_parse_drop(
                    "lighter_collector",
                    "missing_qty",
                    &"missing qty",
                    sample.as_str(),
                );
                None
            });
        let ts = trade.timestamp.map(ts_from_exchange).or_else(|| {
            log_parse_drop(
                "lighter_collector",
                "missing_ts",
                &"missing timestamp",
                sample.as_str(),
            );
            None
        });
        let seq = trade.trade_id.or_else(|| {
            log_parse_drop(
                "lighter_collector",
                "missing_trade_id",
                &"missing trade_id",
                sample.as_str(),
            );
            None
        });
        if px.is_none() || qty.is_none() || ts.is_none() || seq.is_none() {
            continue;
        }
        let px = px.unwrap();
        let qty = qty.unwrap();
        if px <= 0.0 || qty < 0.0 {
            log_parse_drop(
                "lighter_collector",
                "invalid_price_qty",
                &"invalid price or qty",
                sample.as_str(),
            );
            continue;
        }
        let ts = ts.unwrap();
        let seq = seq.unwrap();
        let px_i = (px * price_scale).round() as Price;
        let qty_i = (qty * qty_scale).round() as Qty;
        let is_maker_ask = trade.is_maker_ask.or_else(|| {
            log_parse_drop(
                "lighter_collector",
                "missing_is_maker_ask",
                &"missing is_maker_ask",
                sample.as_str(),
            );
            None
        });
        if is_maker_ask.is_none() {
            continue;
        }
        let is_maker_ask = is_maker_ask.unwrap();
        let buyer_maker = !is_maker_ask;
        let record = Trade::new(px_i, qty_i, ts, seq as Seq, buyer_maker, None);
        trades.push(record);
        inserted += 1;
    }
    inserted
}

pub fn update_tickers(
    frame: &mut LighterFrame,
    market_id: u32,
    symbol: &str,
    price_scale: f64,
    store: &mut TickerStore,
) -> Option<(String, TickerSnapshot)> {
    let sample = frame.text().unwrap_or("").to_string();
    let msg = frame.market_stats_msg()?;
    if msg
        .channel
        .rsplit_once(':')
        .and_then(|(_, id)| match id.parse::<u32>() {
            Ok(v) => Some(v),
            Err(err) => {
                log_parse_drop("lighter_collector", "market_id", &err, id);
                None
            }
        })
        != Some(market_id)
    {
        log_parse_drop(
            "lighter_collector",
            "market_id_mismatch",
            &"market_id mismatch",
            sample.as_str(),
        );
        return None;
    }

    let mut snapshot = store.get(symbol).copied().unwrap_or_default();
    if let Some(last_px) = msg
        .market_stats
        .last_trade_price
        .as_deref()
        .and_then(|v| parse_f64_field("last_trade_price", v))
    {
        snapshot.ticker.last_px = (last_px * price_scale).round() as Price;
    }
    if let Some(mark) = msg
        .market_stats
        .mark_price
        .as_deref()
        .and_then(|v| parse_f64_field("mark_price", v))
    {
        snapshot.mark_px = Some(mark);
    }
    if let Some(index) = msg
        .market_stats
        .index_price
        .as_deref()
        .and_then(|v| parse_f64_field("index_price", v))
    {
        snapshot.index_px = Some(index);
    }
    if let Some(oi) = msg
        .market_stats
        .open_interest
        .as_deref()
        .and_then(|v| parse_f64_field("open_interest", v))
    {
        snapshot.open_interest = Some(oi);
    }
    if let Some(funding) = msg
        .market_stats
        .current_funding_rate
        .as_deref()
        .and_then(|v| parse_f64_field("current_funding_rate", v))
    {
        snapshot.funding_rate = Some(funding);
    } else if let Some(funding) = msg
        .market_stats
        .funding_rate
        .as_deref()
        .and_then(|v| parse_f64_field("funding_rate", v))
    {
        snapshot.funding_rate = Some(funding);
    }
    if let Some(turnover) = msg.market_stats.daily_quote_token_volume {
        snapshot.turnover_24h = Some(turnover);
    }
    if let (Some(oi), Some(mark)) = (snapshot.open_interest, snapshot.mark_px) {
        snapshot.open_interest_value = Some(oi * mark);
    }

    if let Some(ts_raw) = msg.market_stats.funding_timestamp {
        snapshot.ticker.ts = ts_from_exchange(ts_raw);
        snapshot.ticker.seq = ts_raw;
    }

    let stored = store.update(symbol.to_string(), snapshot);
    Some((symbol.to_string(), stored))
}
