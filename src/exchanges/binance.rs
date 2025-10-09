#![allow(dead_code)]

use crate::base_classes::types::Ts;
use crate::base_classes::ws::ExchangeHandler;
use std::time::Instant;

// Minimal frame wrapper. For prod, map to typed events.
#[derive(Debug, Clone)]
pub struct BinanceFrame {
    pub ts: Ts,
    pub recv_instant: Instant,
    pub raw: Vec<u8>,
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
        Some(BinanceFrame {
            ts,
            recv_instant,
            raw: text.as_bytes().to_vec(),
        })
    }

    #[inline(always)]
    fn parse_binary(&self, data: &[u8], ts: Ts, recv_instant: Instant) -> Option<Self::Out> {
        Some(BinanceFrame {
            ts,
            recv_instant,
            raw: data.to_vec(),
        })
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
