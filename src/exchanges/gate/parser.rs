#![allow(dead_code)]

use crate::base_classes::types::Ts;
use crate::base_classes::ws::{ExchangeHandler, HeartbeatPayload};
use crate::utils::parsing::{log_parse_drop, log_parse_drop_bytes};
use crate::exchanges::endpoints::GateioWs;
use crate::exchanges::gate::orderbook::GateMsg;
use serde_json::{self, Value};
use std::time::Instant;

/// Normalize a Gate.io contract symbol into the expected `BASE_QUOTE` form.
pub fn canonical_contract_symbol<S: AsRef<str>>(symbol: S) -> String {
    let sym = symbol.as_ref().to_ascii_uppercase().replace('-', "_");

    // If the symbol already contains an underscore, assume it is in BASE_QUOTE form.
    if let Some(pos) = sym.rfind('_') {
        let (base, quote) = sym.split_at(pos);
        let base = base.trim_end_matches('_');
        if !base.is_empty() {
            return format!("{}_{}", base, quote.trim_start_matches('_'));
        }
    }

    const QUOTES: [&str; 3] = ["USDT", "USD", "USDC"];
    for quote in QUOTES.iter() {
        if sym.ends_with(quote) && sym.len() > quote.len() {
            let base = sym[..sym.len() - quote.len()].trim_end_matches('_');
            if !base.is_empty() {
                return format!("{}_{}", base, quote);
            }
        }
    }

    if sym.len() > 4 {
        let (base, quote) = sym.split_at(sym.len() - 4);
        let base = base.trim_end_matches('_');
        if !base.is_empty() {
            return format!("{}_{}", base, quote);
        }
    }

    sym
}

#[derive(Debug, Clone)]
pub struct GateFrame {
    pub ts: Ts,
    pub recv_instant: Instant,
    pub raw: Vec<u8>,
    json_cache: Option<Value>,
    orderbook_cache: Option<GateMsg>,
}

const ENABLE_ORDERBOOK_SUB: bool = true;

pub struct GateHandler {
    // E.g. BTC_USDT
    contract: String,
    subs: Vec<String>,
}

impl GateHandler {
    pub fn new<S: Into<String>>(symbol: S) -> Self {
        let contract = canonical_contract_symbol(symbol.into());
        let now = || now_secs();
        let mut subs = vec![
            // Book ticker (BBO)
            format!(
                r#"{{"time":{},"channel":"{}","event":"subscribe","payload":["{}"]}}"#,
                now(),
                GateioWs::BBO,
                contract
            ),
            // Trades
            format!(
                r#"{{"time":{},"channel":"{}","event":"subscribe","payload":["{}"]}}"#,
                now(),
                GateioWs::PUBLIC_TRADES,
                contract
            ),
            // Tickers
            format!(
                r#"{{"time":{},"channel":"{}","event":"subscribe","payload":["{}"]}}"#,
                now(),
                GateioWs::TICKER,
                contract
            ),
        ];
        if ENABLE_ORDERBOOK_SUB {
            subs.push(format!(
                r#"{{"time":{},"channel":"futures.obu","event":"subscribe","payload":["ob.{}.50"]}}"#,
                now(),
                contract
            ));
        }
        Self { contract, subs }
    }
}

impl ExchangeHandler for GateHandler {
    type Out = GateFrame;
    fn url(&self) -> &str {
        GateioWs::BASE
    }
    fn initial_subscriptions(&self) -> &[String] {
        &self.subs
    }
    fn parse_text(&self, text: &str, ts: Ts, recv_instant: Instant) -> Option<Self::Out> {
        let mut frame = GateFrame::from_text(text, ts, recv_instant);
        frame.preparse_text(text);
        Some(frame)
    }
    fn parse_binary(&self, data: &[u8], ts: Ts, recv_instant: Instant) -> Option<Self::Out> {
        let mut frame = GateFrame::from_bytes(data.to_vec(), ts, recv_instant);
        frame.preparse_binary();
        Some(frame)
    }

    // Gate requires app-level heartbeats with current time
    fn app_heartbeat_interval(&self) -> Option<u64> {
        Some(30)
    }
    fn build_app_heartbeat(&self) -> Option<HeartbeatPayload> {
        let msg = format!(
            r#"{{"time":{},"channel":"{}","event":"ping"}}"#,
            now_secs(),
            GateioWs::PING
        );
        Some(HeartbeatPayload::Text(msg))
    }

    // Gate OB (v1/v2) monotonic gating by time_ms (or t/u)
    fn sequence_key_text(&self, text: &str) -> Option<(u64, u64)> {
        if !text.contains("\"channel\":\"futures.obu\"") {
            return None;
        }
        let t = find_json_u64(text, "time_ms")
            .or_else(|| find_json_u64(text, "t"))
            .or_else(|| find_json_u64(text, "u"))?;
        let key = fnv1a64(self.contract.as_bytes()) ^ 0x0B00u64;
        Some((key, t))
    }

    fn label(&self) -> String {
        format!("gate:{}", self.contract)
    }
}

#[inline(always)]
fn now_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

impl GateFrame {
    pub fn from_text(text: &str, ts: Ts, recv_instant: Instant) -> Self {
        Self {
            ts,
            recv_instant,
            raw: text.as_bytes().to_vec(),
            json_cache: None,
            orderbook_cache: None,
        }
    }

    pub fn from_bytes(raw: Vec<u8>, ts: Ts, recv_instant: Instant) -> Self {
        Self {
            ts,
            recv_instant,
            raw,
            json_cache: None,
            orderbook_cache: None,
        }
    }

    pub fn preparse_text(&mut self, text: &str) {
        let (needs_json, is_obu) = gate_preparse_flags(text);
        if needs_json && self.json_cache.is_none() {
            self.json_cache = match serde_json::from_str::<Value>(text) {
                Ok(val) => Some(val),
                Err(err) => {
                    log_parse_drop("gate_parser", "json", &err, text);
                    None
                }
            };
        }
        if is_obu && self.orderbook_cache.is_none() {
            self.orderbook_cache = match serde_json::from_str::<GateMsg>(text) {
                Ok(val) => Some(val),
                Err(err) => {
                    log_parse_drop("gate_parser", "orderbook", &err, text);
                    None
                }
            };
        }
    }

    pub fn preparse_binary(&mut self) {
        let text = match core::str::from_utf8(&self.raw) {
            Ok(text) => text,
            Err(err) => {
                log_parse_drop_bytes("gate_parser", "utf8", &err, &self.raw);
                return;
            }
        };
        let (needs_json, is_obu) = gate_preparse_flags(text);
        if !needs_json && !is_obu {
            return;
        }
        if needs_json && self.json_cache.is_none() {
            self.json_cache = match serde_json::from_slice(&self.raw) {
                Ok(val) => Some(val),
                Err(err) => {
                    log_parse_drop_bytes("gate_parser", "json", &err, &self.raw);
                    None
                }
            };
        }
        if is_obu && self.orderbook_cache.is_none() {
            self.orderbook_cache = match serde_json::from_slice(&self.raw) {
                Ok(val) => Some(val),
                Err(err) => {
                    log_parse_drop_bytes("gate_parser", "orderbook", &err, &self.raw);
                    None
                }
            };
        }
    }

    #[inline(always)]
    pub fn text(&self) -> Option<&str> {
        match core::str::from_utf8(&self.raw) {
            Ok(text) => Some(text),
            Err(err) => {
                log_parse_drop_bytes("gate_parser", "utf8", &err, &self.raw);
                None
            }
        }
    }

    #[inline(always)]
    pub fn json(&mut self) -> Option<&Value> {
        if self.json_cache.is_none() {
            self.json_cache = match serde_json::from_slice(&self.raw) {
                Ok(val) => Some(val),
                Err(err) => {
                    log_parse_drop_bytes("gate_parser", "json", &err, &self.raw);
                    None
                }
            };
        }
        self.json_cache.as_ref()
    }

    #[inline(always)]
    pub fn orderbook_msg(&mut self) -> Option<&GateMsg> {
        if self.orderbook_cache.is_none() {
            self.orderbook_cache = match serde_json::from_slice(&self.raw) {
                Ok(val) => Some(val),
                Err(err) => {
                    log_parse_drop_bytes("gate_parser", "orderbook", &err, &self.raw);
                    None
                }
            };
        }
        self.orderbook_cache.as_ref()
    }

    pub fn channel(&self) -> &str {
        if let Ok(s) = core::str::from_utf8(&self.raw) {
            if let Some(i) = s.find("\"channel\"") {
                if let Some(j) = s[i..].find(':') {
                    let rest = &s[i + j + 1..];
                    if let Some(start) = rest.find('"') {
                        let rest2 = &rest[start + 1..];
                        if let Some(end) = rest2.find('"') {
                            return &rest2[..end];
                        }
                    }
                }
            }
        }
        "(unknown)"
    }

    pub fn event(&self) -> &str {
        if let Ok(s) = core::str::from_utf8(&self.raw) {
            if let Some(i) = s.find("\"event\"") {
                if let Some(j) = s[i..].find(':') {
                    let rest = &s[i + j + 1..];
                    if let Some(start) = rest.find('"') {
                        let rest2 = &rest[start + 1..];
                        if let Some(end) = rest2.find('"') {
                            return &rest2[..end];
                        }
                    }
                }
            }
        }
        "(unknown)"
    }
}

#[inline(always)]
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

fn find_json_u64(s: &str, key: &str) -> Option<u64> {
    let k = format!("\"{}\":", key);
    let pos = s.find(&k)?;
    let rest = &s[pos + k.len()..];
    let mut v: u64 = 0;
    let mut f = false;
    for ch in rest.bytes() {
        if ch.is_ascii_digit() {
            f = true;
            v = v.saturating_mul(10).saturating_add((ch - b'0') as u64);
        } else {
            break;
        }
    }
    if f { Some(v) } else { None }
}

#[inline(always)]
fn gate_preparse_flags(text: &str) -> (bool, bool) {
    let is_obu = text.contains("\"channel\":\"futures.obu\"");
    let needs_json = is_obu
        || text.contains("\"channel\":\"futures.book_ticker\"")
        || text.contains("\"channel\":\"futures.tickers\"")
        || text.contains("\"channel\":\"futures.trades\"");
    (needs_json, is_obu)
}
