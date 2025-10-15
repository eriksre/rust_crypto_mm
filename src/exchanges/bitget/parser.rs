#![allow(dead_code)]

use crate::base_classes::types::Ts;
use crate::base_classes::ws::{AppHeartbeat, ExchangeHandler, HeartbeatPayload};
use crate::exchanges::endpoints::BitgetWs;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct BitgetFrame {
    pub ts: Ts,
    pub recv_instant: Instant,
    pub raw: Vec<u8>,
}

pub struct BitgetHandler {
    inst_id: String,
    subs: Vec<String>,
}

impl BitgetHandler {
    pub fn new<S: Into<String>>(symbol: S) -> Self {
        let inst_id = symbol.into().replace('_', "").to_uppercase();
        let subs = vec![
            BitgetWs::sub_msg(&inst_id, BitgetWs::BBO), // books1 (BBO)
            BitgetWs::sub_msg(&inst_id, BitgetWs::ORDERBOOK), // books (full book)
            BitgetWs::sub_msg(&inst_id, BitgetWs::TICKERS), // ticker
            BitgetWs::sub_msg(&inst_id, BitgetWs::PUBLIC_TRADES), // public trades
        ];
        Self { inst_id, subs }
    }
}

impl ExchangeHandler for BitgetHandler {
    type Out = BitgetFrame;
    fn url(&self) -> &str {
        BitgetWs::PUBLIC_BASE
    }
    fn initial_subscriptions(&self) -> &[String] {
        &self.subs
    }
    fn parse_text(&self, text: &str, ts: Ts, recv_instant: Instant) -> Option<Self::Out> {
        Some(BitgetFrame {
            ts,
            recv_instant,
            raw: text.as_bytes().to_vec(),
        })
    }
    fn parse_binary(&self, data: &[u8], ts: Ts, recv_instant: Instant) -> Option<Self::Out> {
        Some(BitgetFrame {
            ts,
            recv_instant,
            raw: data.to_vec(),
        })
    }

    // Bitget app-level heartbeat
    fn app_heartbeat(&self) -> Option<AppHeartbeat> {
        // Bitget expects the literal string "ping" and replies with "pong".
        Some(AppHeartbeat {
            interval_secs: 30,
            payload: HeartbeatPayload::Text("ping".to_string()),
        })
    }

    // Gate book updates by seq (preferred) or ts per instId and channel
    fn sequence_key_text(&self, text: &str) -> Option<(u64, u64)> {
        // Look for arg.channel and only gate books/books1
        let ch = find_json_string(text, "channel")?;
        if ch != BitgetWs::BBO && ch != BitgetWs::ORDERBOOK {
            return None;
        }
        let inst = find_json_string(text, "instId").unwrap_or(&self.inst_id);
        // Prefer 'seq' for monotonic ordering; fallback to top-level ts
        if let Some(seq) = find_json_u64(text, "seq") {
            let tag = if ch == BitgetWs::BBO {
                0x4242_4F31u64
            } else {
                0x424F_4F4Bu64
            }; // 'BBO1'/'BOOK'
            return Some((fnv1a64(inst.as_bytes()) ^ tag, seq));
        }
        if let Some(ts) = find_json_u64(text, "ts") {
            let tag = if ch == BitgetWs::BBO {
                0x4242_4F31u64
            } else {
                0x424F_4F4Bu64
            };
            return Some((fnv1a64(inst.as_bytes()) ^ tag, ts));
        }
        None
    }

    fn label(&self) -> String {
        format!("bitget:{}", self.inst_id)
    }
}

impl BitgetFrame {
    pub fn channel(&self) -> &str {
        if let Ok(s) = core::str::from_utf8(&self.raw) {
            if let Some(v) = find_json_string(s, "channel") {
                return v;
            }
        }
        "(unknown)"
    }
    pub fn event(&self) -> &str {
        if let Ok(s) = core::str::from_utf8(&self.raw) {
            if let Some(v) = find_json_string(s, "event") {
                return v;
            }
        }
        "(unknown)"
    }

    pub fn action(&self) -> &str {
        if let Ok(s) = core::str::from_utf8(&self.raw) {
            if let Some(v) = find_json_string(s, "action") {
                return v;
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
    let pos = s.find(&k)?;
    let rest = &s[pos + k.len()..];
    let colon = rest.find(':')?;
    let rest = &rest[colon + 1..];
    let q = rest.find('"')?;
    let rest2 = &rest[q + 1..];
    let end = rest2.find('"')?;
    Some(&rest2[..end])
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
