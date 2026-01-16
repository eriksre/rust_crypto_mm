#![allow(dead_code)]

use crate::base_classes::types::Ts;
use crate::base_classes::ws::ExchangeHandler;
use crate::utils::parsing::{log_parse_drop, log_parse_drop_bytes};
#[cfg(feature = "binance_book")]
use crate::exchanges::binance::parsed::DepthUpdate;
use serde_json::{self, Value};
use std::time::Instant;

// Minimal frame wrapper with cached JSON/typed payloads prepared on the WS thread.
#[derive(Debug, Clone)]
pub struct BinanceFrame {
    pub ts: Ts,
    pub recv_instant: Instant,
    pub raw: Vec<u8>,
    json_cache: Option<Value>,
    #[cfg(feature = "binance_book")]
    depth_cache: Option<DepthUpdate>,
}

pub struct BinanceHandler {
    symbol_lc: String,
    subs: Vec<String>,
}

impl BinanceHandler {
    pub fn new<S: Into<String>>(symbol: S) -> Self {
        let symbol_lc = symbol.into().replace('_', "").to_lowercase();
        // Single SUBSCRIBE message (multiple streams)
        let sub = format!(
            r#"{{"method":"SUBSCRIBE","params":["{s}@depth@100ms","{s}@bookTicker","{s}@markPrice@1s","{s}@aggTrade"],"id":1}}"#,
            s = symbol_lc
        );
        Self {
            symbol_lc,
            subs: vec![sub],
        }
    }
}

impl ExchangeHandler for BinanceHandler {
    type Out = BinanceFrame;

    #[inline(always)]
    fn url(&self) -> &str {
        "wss://fstream.binance.com/ws"
    }

    #[inline(always)]
    fn initial_subscriptions(&self) -> &[String] {
        &self.subs
    }

    #[inline(always)]
    fn parse_text(&self, text: &str, ts: Ts, recv_instant: Instant) -> Option<Self::Out> {
        let mut frame = BinanceFrame::from_text(text, ts, recv_instant);
        frame.preparse_text(text);
        Some(frame)
    }

    #[inline(always)]
    fn parse_binary(&self, data: &[u8], ts: Ts, recv_instant: Instant) -> Option<Self::Out> {
        let mut frame = BinanceFrame::from_bytes(data.to_vec(), ts, recv_instant);
        frame.preparse_binary();
        Some(frame)
    }

    // Binance futures WS does not accept JSON PING on this endpoint; rely on WS-level pings only.

    // Gate depthUpdate by 'u' per symbol
    fn sequence_key_text(&self, text: &str) -> Option<(u64, u64)> {
        if !text.contains("\"e\":\"depthUpdate\"") {
            return None;
        }
        let sym = find_json_string(text, "s")?;
        let u = find_json_u64(text, "u")?;
        let key = fnv1a64(sym.as_bytes()) ^ 0x4445_5054_48u64; // 'DEPTH'
        Some((key, u))
    }

    fn label(&self) -> String {
        format!("binance:{}", self.symbol_lc)
    }
}

impl BinanceFrame {
    pub fn from_text(text: &str, ts: Ts, recv_instant: Instant) -> Self {
        Self {
            ts,
            recv_instant,
            raw: text.as_bytes().to_vec(),
            json_cache: None,
            #[cfg(feature = "binance_book")]
            depth_cache: None,
        }
    }

    pub fn from_bytes(raw: Vec<u8>, ts: Ts, recv_instant: Instant) -> Self {
        Self {
            ts,
            recv_instant,
            raw,
            json_cache: None,
            #[cfg(feature = "binance_book")]
            depth_cache: None,
        }
    }

    pub fn preparse_text(&mut self, text: &str) {
        let flags = binance_preparse_flags(text);
        if flags.needs_json && self.json_cache.is_none() {
            self.json_cache = match serde_json::from_str::<Value>(text) {
                Ok(val) => Some(val),
                Err(err) => {
                    log_parse_drop("binance_parser", "json", &err, text);
                    None
                }
            };
        }
        #[cfg(feature = "binance_book")]
        {
            if flags.needs_depth && self.depth_cache.is_none() {
                self.depth_cache = match serde_json::from_str::<DepthUpdate>(text) {
                    Ok(val) => Some(val),
                    Err(err) => {
                        log_parse_drop("binance_parser", "depth_update", &err, text);
                        None
                    }
                };
            }
        }
    }

    pub fn preparse_binary(&mut self) {
        let text = match core::str::from_utf8(&self.raw) {
            Ok(text) => text,
            Err(err) => {
                log_parse_drop_bytes("binance_parser", "utf8", &err, &self.raw);
                return;
            }
        };
        let flags = binance_preparse_flags(text);
        if !flags.needs_json && !flags.needs_depth {
            return;
        }
        if flags.needs_json && self.json_cache.is_none() {
            self.json_cache = match serde_json::from_slice(&self.raw) {
                Ok(val) => Some(val),
                Err(err) => {
                    log_parse_drop_bytes("binance_parser", "json", &err, &self.raw);
                    None
                }
            };
        }
        #[cfg(feature = "binance_book")]
        {
            if flags.needs_depth && self.depth_cache.is_none() {
                self.depth_cache = match serde_json::from_slice(&self.raw) {
                    Ok(val) => Some(val),
                    Err(err) => {
                        log_parse_drop_bytes("binance_parser", "depth_update", &err, &self.raw);
                        None
                    }
                };
            }
        }
    }

    #[inline(always)]
    pub fn text(&self) -> Option<&str> {
        match core::str::from_utf8(&self.raw) {
            Ok(text) => Some(text),
            Err(err) => {
                log_parse_drop_bytes("binance_parser", "utf8", &err, &self.raw);
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
                    log_parse_drop_bytes("binance_parser", "json", &err, &self.raw);
                    None
                }
            };
        }
        self.json_cache.as_ref()
    }

    #[inline(always)]
    #[cfg(feature = "binance_book")]
    pub fn depth_update(&mut self) -> Option<&DepthUpdate> {
        if self.depth_cache.is_none() {
            self.depth_cache = match serde_json::from_slice(&self.raw) {
                Ok(val) => Some(val),
                Err(err) => {
                    log_parse_drop_bytes("binance_parser", "depth_update", &err, &self.raw);
                    None
                }
            };
        }
        self.depth_cache.as_ref()
    }

    // Try to extract event type (e), or label some common messages.
    pub fn topic(&self) -> &str {
        if let Ok(s) = core::str::from_utf8(&self.raw) {
            if let Some(i) = s.find("\"e\"") {
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
            if s.contains("\"result\"") {
                return "ack";
            }
            if s.contains("bookTicker") {
                return "bookTicker";
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

fn find_json_string<'a>(s: &'a str, key: &str) -> Option<&'a str> {
    let k = format!("\"{}\"", key);
    if let Some(pos) = s.find(&k) {
        let rest = &s[pos + k.len()..];
        let colon = rest.find(':')?;
        let rest = &rest[colon + 1..];
        let q = rest.find('"')?;
        let rest2 = &rest[q + 1..];
        let end = rest2.find('"')?;
        return Some(&rest2[..end]);
    }
    None
}

fn find_json_u64(s: &str, key: &str) -> Option<u64> {
    let k = format!("\"{}\":", key);
    if let Some(pos) = s.find(&k) {
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
        if f {
            return Some(v);
        }
    }
    None
}

struct BinanceParseFlags {
    needs_json: bool,
    #[cfg(feature = "binance_book")]
    needs_depth: bool,
}

#[inline(always)]
fn binance_preparse_flags(text: &str) -> BinanceParseFlags {
    let needs_json = text.contains("\"e\":\"bookTicker\"")
        || text.contains("\"e\":\"markPriceUpdate\"")
        || text.contains("\"e\":\"aggTrade\"");
    #[cfg(feature = "binance_book")]
    {
        let needs_depth = text.contains("\"e\":\"depthUpdate\"");
        BinanceParseFlags {
            needs_json,
            needs_depth,
        }
    }
    #[cfg(not(feature = "binance_book"))]
    {
        BinanceParseFlags { needs_json }
    }
}
