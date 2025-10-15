#![allow(dead_code)]
#![allow(non_snake_case)]

use crate::base_classes::types::Ts;
use crate::base_classes::ws::ExchangeHandler;
use std::time::Instant;

#[derive(Debug, Clone)]
pub enum BinanceEvent {
    BookTicker(BookTicker),
    AggTrade(AggTrade),
    DepthUpdate(DepthUpdate),
    MarkPrice(MarkPriceUpdate),
    Ack,
    Other(Vec<u8>),
}

#[derive(Debug, Clone, serde::Deserialize)]
struct GenericTag {
    #[serde(rename = "e")]
    e: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct BookTicker {
    #[serde(rename = "e")]
    pub e: String,
    #[serde(rename = "E")]
    pub E: u64,
    #[serde(rename = "s")]
    pub s: String,
    #[serde(rename = "u")]
    pub u: Option<u64>,
    #[serde(rename = "b")]
    pub b: String, // best bid px
    #[serde(rename = "B")]
    pub B: String, // best bid qty
    #[serde(rename = "a")]
    pub a: String, // best ask px
    #[serde(rename = "A")]
    pub A: String, // best ask qty
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct AggTrade {
    #[serde(rename = "e")]
    pub e: String,
    #[serde(rename = "E")]
    pub E: u64,
    #[serde(rename = "s")]
    pub s: String,
    #[serde(rename = "a")]
    pub a: u64,
    #[serde(rename = "p")]
    pub p: String,
    #[serde(rename = "q")]
    pub q: String,
    #[serde(rename = "f")]
    pub f: u64,
    #[serde(rename = "l")]
    pub l: u64,
    #[serde(rename = "T")]
    pub T: u64,
    #[serde(rename = "m")]
    pub m: bool,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct DepthUpdate {
    #[serde(rename = "e")]
    pub e: String, // depthUpdate
    #[serde(rename = "E")]
    pub E: u64, // event time
    #[serde(rename = "s")]
    pub s: String, // symbol
    #[serde(rename = "U")]
    pub U: u64, // first update ID in event
    #[serde(rename = "u")]
    pub u: u64, // final update ID in event
    #[serde(rename = "pu")]
    pub pu: Option<u64>, // previous final update ID
    #[serde(rename = "b")]
    pub b: Vec<[String; 2]>,
    #[serde(rename = "a")]
    pub a: Vec<[String; 2]>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct MarkPriceUpdate {
    #[serde(rename = "e")]
    pub e: String, // markPriceUpdate
    #[serde(rename = "E")]
    pub E: u64,
    #[serde(rename = "s")]
    pub s: String,
    #[serde(rename = "p")]
    pub p: String, // mark price
    #[serde(rename = "i")]
    pub i: Option<String>, // index price
}

#[derive(Debug, Clone)]
pub struct BinanceParsedFrame {
    pub ts: Ts,
    pub recv_instant: Instant,
    pub event: BinanceEvent,
}

pub struct BinanceParsedHandler {
    symbol_lc: String,
    subs: Vec<String>,
}

impl BinanceParsedHandler {
    pub fn new<S: Into<String>>(symbol: S) -> Self {
        let symbol_lc = symbol.into().to_lowercase();
        let sub = format!(
            r#"{{"method":"SUBSCRIBE","params":["{s}@depth@100ms","{s}@bookTicker","{s}@markPrice@1s","{s}@aggTrade"],"id":1}}"#,
            s = symbol_lc
        );
        Self {
            symbol_lc,
            subs: vec![sub],
        }
    }

    #[inline(always)]
    fn parse_event(bytes: &[u8]) -> Option<BinanceEvent> {
        // ack result
        if let Ok(v) = serde_json::from_slice::<serde_json::Value>(bytes) {
            if v.get("result").is_some() {
                return Some(BinanceEvent::Ack);
            }
        }
        // try typed by e
        if let Ok(tag) = serde_json::from_slice::<GenericTag>(bytes) {
            if let Some(e) = tag.e.as_deref() {
                match e {
                    "bookTicker" => {
                        if let Ok(bt) = serde_json::from_slice::<BookTicker>(bytes) {
                            return Some(BinanceEvent::BookTicker(bt));
                        }
                    }
                    "aggTrade" => {
                        if let Ok(t) = serde_json::from_slice::<AggTrade>(bytes) {
                            return Some(BinanceEvent::AggTrade(t));
                        }
                    }
                    "depthUpdate" => {
                        if let Ok(d) = serde_json::from_slice::<DepthUpdate>(bytes) {
                            return Some(BinanceEvent::DepthUpdate(d));
                        }
                    }
                    "markPriceUpdate" => {
                        if let Ok(m) = serde_json::from_slice::<MarkPriceUpdate>(bytes) {
                            return Some(BinanceEvent::MarkPrice(m));
                        }
                    }
                    _ => return Some(BinanceEvent::Other(bytes.to_vec())),
                }
                return Some(BinanceEvent::Other(bytes.to_vec()));
            }
        }
        Some(BinanceEvent::Other(bytes.to_vec()))
    }
}

impl ExchangeHandler for BinanceParsedHandler {
    type Out = BinanceParsedFrame;
    fn url(&self) -> &str {
        "wss://fstream.binance.com/ws"
    }
    fn initial_subscriptions(&self) -> &[String] {
        &self.subs
    }
    fn parse_text(&self, text: &str, ts: Ts, recv_instant: Instant) -> Option<Self::Out> {
        let event = Self::parse_event(text.as_bytes())?;
        Some(BinanceParsedFrame {
            ts,
            recv_instant,
            event,
        })
    }
    fn parse_binary(&self, data: &[u8], ts: Ts, recv_instant: Instant) -> Option<Self::Out> {
        let event = Self::parse_event(data)?;
        Some(BinanceParsedFrame {
            ts,
            recv_instant,
            event,
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
