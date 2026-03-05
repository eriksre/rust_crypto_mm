#![allow(dead_code)]

use crate::base_classes::types::Ts;
use crate::base_classes::ws::{AppHeartbeat, ExchangeHandler, HeartbeatPayload};
use crate::exchanges::endpoints::OkxWs;
use crate::exchanges::okx::orderbook::OkxMsg;
use crate::utils::parsing::{log_parse_drop, log_parse_drop_bytes};
use serde_json::{self, Value};
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct OkxFrame {
    pub ts: Ts,
    pub recv_instant: Instant,
    pub raw: Vec<u8>,
    json_cache: Option<Value>,
    orderbook_cache: Option<OkxMsg>,
}

pub struct OkxHandler {
    inst_id: String,
    subs: Vec<String>,
}

impl OkxHandler {
    pub fn new<S: Into<String>>(symbol: S) -> Self {
        let inst_id = normalize_inst_id(symbol.into());
        let subs = vec![OkxWs::subscribe_multi(
            &inst_id,
            &[OkxWs::BOOKS, OkxWs::BBO_TBT, OkxWs::TICKERS, OkxWs::TRADES],
        )];
        Self { inst_id, subs }
    }
}

impl ExchangeHandler for OkxHandler {
    type Out = OkxFrame;

    #[inline(always)]
    fn url(&self) -> &str {
        OkxWs::PUBLIC_BASE
    }

    #[inline(always)]
    fn initial_subscriptions(&self) -> &[String] {
        &self.subs
    }

    #[inline(always)]
    fn parse_text(&self, text: &str, ts: Ts, recv_instant: Instant) -> Option<Self::Out> {
        let mut frame = OkxFrame::from_text(text, ts, recv_instant);
        frame.preparse_text(text);
        Some(frame)
    }

    #[inline(always)]
    fn parse_binary(&self, data: &[u8], ts: Ts, recv_instant: Instant) -> Option<Self::Out> {
        let mut frame = OkxFrame::from_bytes(data.to_vec(), ts, recv_instant);
        frame.preparse_binary();
        Some(frame)
    }

    fn app_heartbeat(&self) -> Option<AppHeartbeat> {
        Some(AppHeartbeat {
            interval_secs: 25,
            payload: HeartbeatPayload::Text("ping".to_string()),
        })
    }

    fn sequence_key_text(&self, text: &str) -> Option<(u64, u64)> {
        let channel = find_json_string(text, "channel")?;
        if channel != OkxWs::BOOKS && channel != OkxWs::BBO_TBT {
            return None;
        }
        let seq = find_json_u64(text, "seqId")?;
        let mut key = fnv1a64(self.inst_id.as_bytes());
        key ^= match channel {
            OkxWs::BOOKS => 0x4F4B5F424B,   // "OK_BK"
            OkxWs::BBO_TBT => 0x4F4B5F4242, // "OK_BB"
            _ => 0,
        };
        Some((key, seq))
    }

    fn label(&self) -> String {
        format!("okx:{}", self.inst_id)
    }
}

impl OkxFrame {
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
        let flags = okx_preparse_flags(text);
        if flags.needs_json && self.json_cache.is_none() {
            self.json_cache = match serde_json::from_str::<Value>(text) {
                Ok(val) => Some(val),
                Err(err) => {
                    log_parse_drop("okx_parser", "json", &err, text);
                    None
                }
            };
        }
        if flags.needs_orderbook && self.orderbook_cache.is_none() {
            self.orderbook_cache = match serde_json::from_str::<OkxMsg>(text) {
                Ok(val) => Some(val),
                Err(err) => {
                    log_parse_drop("okx_parser", "orderbook", &err, text);
                    None
                }
            };
        }
    }

    pub fn preparse_binary(&mut self) {
        let text = match core::str::from_utf8(&self.raw) {
            Ok(text) => text,
            Err(err) => {
                log_parse_drop_bytes("okx_parser", "utf8", &err, &self.raw);
                return;
            }
        };
        let flags = okx_preparse_flags(text);
        if !flags.needs_json && !flags.needs_orderbook {
            return;
        }
        if flags.needs_json && self.json_cache.is_none() {
            self.json_cache = match serde_json::from_slice(&self.raw) {
                Ok(val) => Some(val),
                Err(err) => {
                    log_parse_drop_bytes("okx_parser", "json", &err, &self.raw);
                    None
                }
            };
        }
        if flags.needs_orderbook && self.orderbook_cache.is_none() {
            self.orderbook_cache = match serde_json::from_slice(&self.raw) {
                Ok(val) => Some(val),
                Err(err) => {
                    log_parse_drop_bytes("okx_parser", "orderbook", &err, &self.raw);
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
                log_parse_drop_bytes("okx_parser", "utf8", &err, &self.raw);
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
                    log_parse_drop_bytes("okx_parser", "json", &err, &self.raw);
                    None
                }
            };
        }
        self.json_cache.as_ref()
    }

    #[inline(always)]
    pub fn orderbook_msg(&mut self) -> Option<&OkxMsg> {
        if self.orderbook_cache.is_none() {
            self.orderbook_cache = match serde_json::from_slice(&self.raw) {
                Ok(val) => Some(val),
                Err(err) => {
                    log_parse_drop_bytes("okx_parser", "orderbook", &err, &self.raw);
                    None
                }
            };
        }
        self.orderbook_cache.as_ref()
    }

    pub fn channel(&self) -> &str {
        let s = match core::str::from_utf8(&self.raw) {
            Ok(text) => text,
            Err(err) => {
                log_parse_drop_bytes("okx_parser", "utf8", &err, &self.raw);
                return "(unknown)";
            }
        };
        if let Some(ch) = find_json_string(s, "channel") {
            return ch;
        }
        "(unknown)"
    }

    pub fn event(&self) -> &str {
        let s = match core::str::from_utf8(&self.raw) {
            Ok(text) => text,
            Err(err) => {
                log_parse_drop_bytes("okx_parser", "utf8", &err, &self.raw);
                return "(unknown)";
            }
        };
        if let Some(ev) = find_json_string(s, "event") {
            return ev;
        }
        "(unknown)"
    }
}

fn normalize_inst_id(symbol: String) -> String {
    let trimmed = symbol.trim().to_ascii_uppercase();
    let replaced = trimmed.replace('/', "-").replace('_', "-");
    if replaced.contains('-') {
        if replaced.ends_with("-SWAP") {
            replaced
        } else {
            format!("{replaced}-SWAP")
        }
    } else {
        const QUOTES: [&str; 4] = ["USDT", "USD", "USDC", "BTC"];
        for quote in QUOTES {
            if replaced.ends_with(quote) && replaced.len() > quote.len() {
                let base = &replaced[..replaced.len() - quote.len()];
                let base = base.trim_matches('-');
                if !base.is_empty() {
                    return format!("{base}-{quote}-SWAP");
                }
            }
        }
        format!("{replaced}-SWAP")
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

fn find_json_string<'a>(s: &'a str, key: &str) -> Option<&'a str> {
    let needle = format!("\"{key}\"");
    let pos = s.find(&needle)?;
    let rest = &s[pos + needle.len()..];
    let colon = rest.find(':')?;
    let rest = &rest[colon + 1..];
    let start = rest.find('"')?;
    let rest = &rest[start + 1..];
    let end = rest.find('"')?;
    Some(&rest[..end])
}

fn find_json_u64(s: &str, key: &str) -> Option<u64> {
    let needle = format!("\"{key}\":");
    let pos = s.find(&needle)?;
    let rest = &s[pos + needle.len()..];
    let mut value: u64 = 0;
    let mut found = false;
    for ch in rest.bytes() {
        if ch.is_ascii_digit() {
            found = true;
            value = value.saturating_mul(10).saturating_add((ch - b'0') as u64);
        } else if found {
            break;
        } else if ch == b' ' {
            continue;
        } else {
            return None;
        }
    }
    if found { Some(value) } else { None }
}

struct OkxParseFlags {
    needs_json: bool,
    needs_orderbook: bool,
}

#[inline(always)]
fn okx_preparse_flags(text: &str) -> OkxParseFlags {
    let is_books = text.contains("\"channel\":\"books\"");
    let is_bbo = text.contains("\"channel\":\"bbo-tbt\"");
    let needs_orderbook = is_books || is_bbo;
    let needs_json = needs_orderbook
        || text.contains("\"channel\":\"tickers\"")
        || text.contains("\"channel\":\"trades\"");
    OkxParseFlags {
        needs_json,
        needs_orderbook,
    }
}
