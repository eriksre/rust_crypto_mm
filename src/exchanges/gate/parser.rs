#![allow(dead_code)]

use crate::base_classes::types::Ts;
use crate::base_classes::ws::{ExchangeHandler, HeartbeatPayload};
use crate::exchanges::endpoints::GateioWs;
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
        Some(GateFrame {
            ts,
            recv_instant,
            raw: text.as_bytes().to_vec(),
        })
    }
    fn parse_binary(&self, data: &[u8], ts: Ts, recv_instant: Instant) -> Option<Self::Out> {
        Some(GateFrame {
            ts,
            recv_instant,
            raw: data.to_vec(),
        })
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
