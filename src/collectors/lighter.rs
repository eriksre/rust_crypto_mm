use crate::base_classes::tickers::{TickerSnapshot, TickerStore};
use crate::base_classes::trades::{FixedTrades, Trade};
use crate::base_classes::types::{Price, Qty, Seq};
use crate::exchanges::lighter::{LighterBook, LighterFrame};
use crate::utils::time::ms_to_ns;

fn ts_from_exchange(raw: u64) -> u64 {
    if raw > 1_000_000_000_000 {
        ms_to_ns(raw)
    } else {
        raw.saturating_mul(1_000_000_000)
    }
}

fn parse_f64(value: &str) -> Option<f64> {
    value.parse::<f64>().ok()
}

pub fn events_for<const N: usize>(
    frame: &mut LighterFrame,
    book: &mut LighterBook<N>,
    market_id: u32,
) -> Vec<(&'static str, f64)> {
    let mut out = Vec::with_capacity(1);
    if let Some(msg) = frame.order_book_msg() {
        if msg
            .channel
            .rsplit_once(':')
            .and_then(|(_, id)| id.parse::<u32>().ok())
            != Some(market_id)
        {
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
    let frame_ts = frame.ts;
    let msg = match frame.trades_msg() {
        Some(m) => m,
        None => return 0,
    };
    if msg
        .channel
        .rsplit_once(':')
        .and_then(|(_, id)| id.parse::<u32>().ok())
        != Some(market_id)
    {
        return 0;
    }

    let mut inserted = 0usize;
    for trade in msg.trades.iter() {
        let px = trade
            .price
            .as_deref()
            .and_then(parse_f64)
            .unwrap_or_default();
        let qty = trade
            .size
            .as_deref()
            .and_then(parse_f64)
            .unwrap_or_default();
        if px <= 0.0 || qty < 0.0 {
            continue;
        }
        let ts = trade.timestamp.map(ts_from_exchange).unwrap_or(frame_ts);
        let seq = trade.trade_id.unwrap_or_else(|| ts);
        let px_i = (px * price_scale).round() as Price;
        let qty_i = (qty * qty_scale).round() as Qty;
        let is_maker_ask = trade.is_maker_ask.unwrap_or(false);
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
    let msg = frame.market_stats_msg()?;
    if msg
        .channel
        .rsplit_once(':')
        .and_then(|(_, id)| id.parse::<u32>().ok())
        != Some(market_id)
    {
        return None;
    }

    let mut snapshot = store.get(symbol).copied().unwrap_or_default();
    if let Some(last_px) = msg
        .market_stats
        .last_trade_price
        .as_deref()
        .and_then(parse_f64)
    {
        snapshot.ticker.last_px = (last_px * price_scale).round() as Price;
    }
    if let Some(mark) = msg.market_stats.mark_price.as_deref().and_then(parse_f64) {
        snapshot.mark_px = Some(mark);
    }
    if let Some(index) = msg.market_stats.index_price.as_deref().and_then(parse_f64) {
        snapshot.index_px = Some(index);
    }
    if let Some(oi) = msg
        .market_stats
        .open_interest
        .as_deref()
        .and_then(parse_f64)
    {
        snapshot.open_interest = Some(oi);
    }
    if let Some(funding) = msg
        .market_stats
        .current_funding_rate
        .as_deref()
        .and_then(parse_f64)
    {
        snapshot.funding_rate = Some(funding);
    } else if let Some(funding) = msg.market_stats.funding_rate.as_deref().and_then(parse_f64) {
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
