#![allow(dead_code)]

use crate::base_classes::types::Ts;
use crate::base_classes::ws::ExchangeHandler;
use crate::exchanges::endpoints::BybitWs;
use std::time::Instant;

// Lightweight event wrapper. For production, map to typed events.
#[derive(Debug, Clone)]
pub struct BybitFrame {
    pub ts: Ts,
    pub recv_instant: Instant,
    pub raw: Vec<u8>,
}

pub struct BybitHandler {
    symbol: String,
    subs: Vec<String>,
}

impl BybitHandler {
    pub fn new<S: Into<String>>(symbol: S) -> Self {
        let symbol = symbol.into().replace('_', "").to_uppercase();
        // Orderbook depth 1 (BBO), Tickers, Public Trades, and 1m Kline
        let subs = vec![
            format!(
                r#"{{"op":"subscribe","args":["{}"]}}"#,
                BybitWs::orderbook("1", &symbol)
            ),
            format!(
                r#"{{"op":"subscribe","args":["{}"]}}"#,
                BybitWs::orderbook("50", &symbol)
            ),
            format!(
                r#"{{"op":"subscribe","args":["{}"]}}"#,
                BybitWs::tickers(&symbol)
            ),
            format!(
                r#"{{"op":"subscribe","args":["{}"]}}"#,
                BybitWs::public_trades(&symbol)
            ),
            format!(
                r#"{{"op":"subscribe","args":["{}"]}}"#,
                BybitWs::kline("1", &symbol)
            ),
        ];
        Self { symbol, subs }
    }
}

impl ExchangeHandler for BybitHandler {
    type Out = BybitFrame;

    #[inline(always)]
    fn url(&self) -> &str {
        BybitWs::BASE
    }

    #[inline(always)]
    fn initial_subscriptions(&self) -> &[String] {
        &self.subs
    }

    #[inline(always)]
    fn parse_text(&self, text: &str, ts: Ts, recv_instant: Instant) -> Option<Self::Out> {
        Some(BybitFrame {
            ts,
            recv_instant,
            raw: text.as_bytes().to_vec(),
        })
    }

    #[inline(always)]
    fn parse_binary(&self, data: &[u8], ts: Ts, recv_instant: Instant) -> Option<Self::Out> {
        Some(BybitFrame {
            ts,
            recv_instant,
            raw: data.to_vec(),
        })
    }

    // Gate orderbook.* streams by Bybit 'seq' only. Accept strictly increasing.
    fn sequence_key_text(&self, text: &str) -> Option<(u64, u64)> {
        let topic = find_json_string(text, "topic")?;
        if !topic.starts_with("orderbook.") {
            return None;
        }
        let seq = find_json_u64(text, "seq")?;
        Some((fnv1a64(topic.as_bytes()), seq))
    }

    fn label(&self) -> String {
        format!("bybit:{}", self.symbol)
    }
}

// Small helpers for demo/display
impl BybitFrame {
    pub fn topic(&self) -> Option<&str> {
        // Fast substring find for "topic":"..."
        if let Ok(s) = core::str::from_utf8(&self.raw) {
            if let Some(i) = s.find("\"topic\"") {
                if let Some(j) = s[i..].find(':') {
                    let rest = &s[i + j + 1..];
                    let start = rest.find('"')? + 1;
                    let rest2 = &rest[start..];
                    let end = rest2.find('"')?;
                    return Some(&rest2[..end]);
                }
            }
        }
        None
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
    // looks for "key":"value"
    let k = format!("\"{}\"", key);
    let pos = s.find(&k)?;
    let idx = pos + k.len();
    let rest = &s[idx..];
    let colon = rest.find(':')?;
    let rest = &rest[colon + 1..];
    let quote = rest.find('"')?;
    let rest2 = &rest[quote + 1..];
    let end = rest2.find('"')?;
    Some(&rest2[..end])
}

fn find_json_u64(s: &str, key: &str) -> Option<u64> {
    // looks for "key":12345
    let mut i = 0usize;
    let k = format!("\"{}\":", key);
    while let Some(pos) = s[i..].find(&k) {
        let idx = i + pos + k.len();
        let rest = &s[idx..];
        let mut val: u64 = 0;
        let mut found = false;
        for ch in rest.bytes() {
            if ch.is_ascii_digit() {
                found = true;
                val = val.saturating_mul(10).saturating_add((ch - b'0') as u64);
            } else {
                break;
            }
        }
        if found {
            return Some(val);
        }
        i = idx;
    }
    None
}
