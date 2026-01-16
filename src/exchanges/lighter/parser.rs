use crate::base_classes::types::Ts;
use crate::base_classes::ws::{AppHeartbeat, ExchangeHandler, HeartbeatPayload};
use crate::utils::parsing::{log_parse_drop, log_parse_drop_bytes};
use crate::exchanges::endpoints::LighterWs;
use crate::exchanges::lighter::orderbook::LighterOrderBookMsg;
use crate::exchanges::lighter::rest::LighterMarketMeta;
use serde::Deserialize;
use serde_json::{self, Value};
use std::time::Instant;

#[derive(Debug, Clone, Deserialize)]
pub struct LighterTrade {
    pub trade_id: Option<u64>,
    pub tx_hash: Option<String>,
    pub r#type: Option<String>,
    pub market_id: Option<u32>,
    pub size: Option<String>,
    pub price: Option<String>,
    pub usd_amount: Option<String>,
    pub ask_id: Option<u64>,
    pub bid_id: Option<u64>,
    pub ask_account_id: Option<u64>,
    pub bid_account_id: Option<u64>,
    pub is_maker_ask: Option<bool>,
    pub block_height: Option<u64>,
    pub timestamp: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LighterTradesMsg {
    pub channel: String,
    #[serde(default)]
    pub trades: Vec<LighterTrade>,
    #[serde(default, rename = "type")]
    pub msg_type: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LighterMarketStats {
    pub market_id: Option<u32>,
    pub index_price: Option<String>,
    pub mark_price: Option<String>,
    pub open_interest: Option<String>,
    pub last_trade_price: Option<String>,
    pub current_funding_rate: Option<String>,
    pub funding_rate: Option<String>,
    pub funding_timestamp: Option<u64>,
    pub daily_base_token_volume: Option<f64>,
    pub daily_quote_token_volume: Option<f64>,
    pub daily_price_low: Option<f64>,
    pub daily_price_high: Option<f64>,
    pub daily_price_change: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LighterMarketStatsMsg {
    pub channel: String,
    pub market_stats: LighterMarketStats,
    #[serde(default, rename = "type")]
    pub msg_type: Option<String>,
}

#[derive(Debug, Clone)]
pub struct LighterFrame {
    pub ts: Ts,
    pub recv_instant: Instant,
    pub raw: Vec<u8>,
    json_cache: Option<Value>,
    channel_cache: Option<String>,
    order_book_cache: Option<LighterOrderBookMsg>,
    trades_cache: Option<LighterTradesMsg>,
    stats_cache: Option<LighterMarketStatsMsg>,
}

pub struct LighterHandler {
    _market_id: u32,
    subs: Vec<String>,
    label: String,
}

impl LighterHandler {
    pub fn new(meta: LighterMarketMeta) -> Self {
        let mut subs = Vec::with_capacity(3);
        subs.push(LighterWs::sub(LighterWs::ORDER_BOOK, meta.market_id));
        subs.push(LighterWs::sub(LighterWs::TRADE, meta.market_id));
        subs.push(LighterWs::sub(LighterWs::MARKET_STATS, meta.market_id));
        let label = format!(
            "lighter:{}#{}",
            meta.symbol.to_ascii_uppercase(),
            meta.market_id
        );
        Self {
            _market_id: meta.market_id,
            subs,
            label,
        }
    }
}

impl ExchangeHandler for LighterHandler {
    type Out = LighterFrame;

    fn url(&self) -> &str {
        LighterWs::BASE
    }

    fn initial_subscriptions(&self) -> &[String] {
        &self.subs
    }

    fn parse_text(&self, text: &str, ts: Ts, recv_instant: Instant) -> Option<Self::Out> {
        let mut frame = LighterFrame::from_text(text, ts, recv_instant);
        frame.preparse_text(text);
        Some(frame)
    }

    fn parse_binary(&self, data: &[u8], ts: Ts, recv_instant: Instant) -> Option<Self::Out> {
        let mut frame = LighterFrame::from_bytes(data.to_vec(), ts, recv_instant);
        frame.preparse_binary();
        Some(frame)
    }

    fn app_heartbeat(&self) -> Option<AppHeartbeat> {
        Some(AppHeartbeat {
            interval_secs: 20,
            payload: HeartbeatPayload::Text(r#"{"type":"ping"}"#.to_string()),
        })
    }

    fn label(&self) -> String {
        self.label.clone()
    }
}

impl LighterFrame {
    pub fn from_text(text: &str, ts: Ts, recv_instant: Instant) -> Self {
        Self {
            ts,
            recv_instant,
            raw: text.as_bytes().to_vec(),
            json_cache: None,
            channel_cache: None,
            order_book_cache: None,
            trades_cache: None,
            stats_cache: None,
        }
    }

    pub fn from_bytes(raw: Vec<u8>, ts: Ts, recv_instant: Instant) -> Self {
        Self {
            ts,
            recv_instant,
            raw,
            json_cache: None,
            channel_cache: None,
            order_book_cache: None,
            trades_cache: None,
            stats_cache: None,
        }
    }

    pub fn preparse_text(&mut self, text: &str) {
        if self.json_cache.is_none() {
            match serde_json::from_str::<Value>(text) {
                Ok(value) => {
                    self.channel_cache = value
                        .get("channel")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    self.json_cache = Some(value);
                }
                Err(err) => {
                    log_parse_drop("lighter_parser", "json", &err, text);
                }
            }
        }
        self.maybe_parse_specialized(text.as_bytes());
    }

    pub fn preparse_binary(&mut self) {
        let text = match core::str::from_utf8(&self.raw) {
            Ok(text) => text,
            Err(err) => {
                log_parse_drop_bytes("lighter_parser", "utf8", &err, &self.raw);
                return;
            }
        };
        let owned = text.to_string();
        self.preparse_text(&owned);
    }

    fn maybe_parse_specialized(&mut self, bytes: &[u8]) {
        if self
            .channel()
            .map_or(false, |ch| ch.starts_with("order_book"))
            && self.order_book_cache.is_none()
        {
            match serde_json::from_slice::<LighterOrderBookMsg>(bytes) {
                Ok(msg) => self.order_book_cache = Some(msg),
                Err(err) => {
                    log_parse_drop_bytes("lighter_parser", "order_book", &err, bytes);
                }
            }
        }
        if self.channel().map_or(false, |ch| ch.starts_with("trade")) && self.trades_cache.is_none()
        {
            match serde_json::from_slice::<LighterTradesMsg>(bytes) {
                Ok(msg) => self.trades_cache = Some(msg),
                Err(err) => {
                    log_parse_drop_bytes("lighter_parser", "trades", &err, bytes);
                }
            }
        }
        if self
            .channel()
            .map_or(false, |ch| ch.starts_with("market_stats"))
            && self.stats_cache.is_none()
        {
            match serde_json::from_slice::<LighterMarketStatsMsg>(bytes) {
                Ok(msg) => self.stats_cache = Some(msg),
                Err(err) => {
                    log_parse_drop_bytes("lighter_parser", "market_stats", &err, bytes);
                }
            }
        }
    }

    pub fn text(&self) -> Option<&str> {
        match core::str::from_utf8(&self.raw) {
            Ok(text) => Some(text),
            Err(err) => {
                log_parse_drop_bytes("lighter_parser", "utf8", &err, &self.raw);
                None
            }
        }
    }

    pub fn json(&mut self) -> Option<&Value> {
        if self.json_cache.is_none() {
            match serde_json::from_slice::<Value>(&self.raw) {
                Ok(value) => {
                    self.channel_cache = value
                        .get("channel")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    self.json_cache = Some(value);
                }
                Err(err) => {
                    log_parse_drop_bytes("lighter_parser", "json", &err, &self.raw);
                }
            }
        }
        self.json_cache.as_ref()
    }

    pub fn channel(&self) -> Option<&str> {
        self.channel_cache.as_deref().or_else(|| {
            let text = match core::str::from_utf8(&self.raw) {
                Ok(text) => text,
                Err(err) => {
                    log_parse_drop_bytes("lighter_parser", "utf8", &err, &self.raw);
                    return None;
                }
            };
            if let Some(pos) = text.find("\"channel\"") {
                let rest = &text[pos + 9..];
                if let Some(colon) = rest.find(':') {
                    let rest = &rest[colon + 1..];
                    if let Some(q) = rest.find('"') {
                        let rest = &rest[q + 1..];
                        if let Some(end) = rest.find('"') {
                            return Some(&rest[..end]);
                        }
                    }
                }
            }
            None
        })
    }

    pub fn order_book_msg(&mut self) -> Option<&LighterOrderBookMsg> {
        if self.order_book_cache.is_none()
            && self
                .channel()
                .map_or(false, |ch| ch.starts_with("order_book"))
        {
            match serde_json::from_slice::<LighterOrderBookMsg>(&self.raw) {
                Ok(msg) => self.order_book_cache = Some(msg),
                Err(err) => {
                    log_parse_drop_bytes("lighter_parser", "order_book", &err, &self.raw);
                }
            }
        }
        self.order_book_cache.as_ref()
    }

    pub fn trades_msg(&mut self) -> Option<&LighterTradesMsg> {
        if self.trades_cache.is_none() && self.channel().map_or(false, |ch| ch.starts_with("trade"))
        {
            match serde_json::from_slice::<LighterTradesMsg>(&self.raw) {
                Ok(msg) => self.trades_cache = Some(msg),
                Err(err) => {
                    log_parse_drop_bytes("lighter_parser", "trades", &err, &self.raw);
                }
            }
        }
        self.trades_cache.as_ref()
    }

    pub fn market_stats_msg(&mut self) -> Option<&LighterMarketStatsMsg> {
        if self.stats_cache.is_none()
            && self
                .channel()
                .map_or(false, |ch| ch.starts_with("market_stats"))
        {
            match serde_json::from_slice::<LighterMarketStatsMsg>(&self.raw) {
                Ok(msg) => self.stats_cache = Some(msg),
                Err(err) => {
                    log_parse_drop_bytes("lighter_parser", "market_stats", &err, &self.raw);
                }
            }
        }
        self.stats_cache.as_ref()
    }
}
