#![allow(dead_code)]

use crate::base_classes::types::Ts;
use crate::base_classes::ws::{AppHeartbeat, ExchangeHandler, HeartbeatPayload};
use crate::exchanges::endpoints::OkxWs;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct OkxFrame {
    pub ts: Ts,
    pub recv_instant: Instant,
    pub raw: Vec<u8>,
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
        Some(OkxFrame {
            ts,
            recv_instant,
            raw: text.as_bytes().to_vec(),
        })
    }

    #[inline(always)]
    fn parse_binary(&self, data: &[u8], ts: Ts, recv_instant: Instant) -> Option<Self::Out> {
        Some(OkxFrame {
            ts,
            recv_instant,
            raw: data.to_vec(),
        })
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
    pub fn channel(&self) -> &str {
        if let Ok(s) = core::str::from_utf8(&self.raw) {
            if let Some(ch) = find_json_string(s, "channel") {
                return ch;
            }
        }
        "(unknown)"
    }

    pub fn event(&self) -> &str {
        if let Ok(s) = core::str::from_utf8(&self.raw) {
            if let Some(ev) = find_json_string(s, "event") {
                return ev;
            }
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
