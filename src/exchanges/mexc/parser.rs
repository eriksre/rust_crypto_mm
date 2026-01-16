#![allow(dead_code)]

use crate::base_classes::types::Ts;
use crate::base_classes::ws::{AppHeartbeat, ExchangeHandler, HeartbeatPayload};
use crate::utils::parsing::{log_parse_drop, log_parse_drop_bytes};
use crate::exchanges::endpoints::MexcWs;
use crate::exchanges::mexc::orderbook::MexcDepthMsg;
use serde_json::{self, Value};
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct MexcFrame {
    pub ts: Ts,
    pub recv_instant: Instant,
    pub raw: Vec<u8>,
    json_cache: Option<Value>,
    depth_cache: Option<MexcDepthMsg>,
    channel_cache: Option<String>,
}

fn mexc_preparse_flags(text: &str) -> (Option<&str>, bool, bool) {
    let channel = find_channel(text);
    let needs_depth = matches!(channel, Some(ch) if ch == "push.depth");
    let needs_json =
        needs_depth || matches!(channel, Some(ch) if ch == "push.ticker" || ch == "push.deal");
    (channel, needs_json, needs_depth)
}

fn find_channel(text: &str) -> Option<&str> {
    find_json_string(text, "channel").or_else(|| find_json_string(text, "c"))
}

fn find_json_string<'a>(s: &'a str, key: &str) -> Option<&'a str> {
    let needle = format!("\"{key}\"");
    let pos = s.find(&needle)?;
    let rest = &s[pos + needle.len()..];
    let colon = rest.find(':')?;
    let rest = &rest[colon + 1..];
    let quote = rest.find('"')?;
    let rest2 = &rest[quote + 1..];
    let end = rest2.find('"')?;
    Some(&rest2[..end])
}

pub struct MexcHandler {
    symbol: String,
    subs: Vec<String>,
}

impl MexcHandler {
    pub fn new<S: Into<String>>(symbol: S) -> Self {
        let normalized = symbol.into().replace('-', "_").to_uppercase();
        let mut subs = Vec::with_capacity(3);
        subs.push(MexcWs::sub_depth(&normalized, None));
        subs.push(MexcWs::sub_ticker(&normalized));
        subs.push(MexcWs::sub_trades(&normalized));
        Self {
            symbol: normalized,
            subs,
        }
    }
}

impl ExchangeHandler for MexcHandler {
    type Out = MexcFrame;

    fn url(&self) -> &str {
        MexcWs::BASE
    }

    fn initial_subscriptions(&self) -> &[String] {
        &self.subs
    }

    fn parse_text(&self, text: &str, ts: Ts, recv_instant: Instant) -> Option<Self::Out> {
        let mut frame = MexcFrame::from_text(text, ts, recv_instant);
        frame.preparse_text(text);
        Some(frame)
    }

    fn parse_binary(&self, data: &[u8], ts: Ts, recv_instant: Instant) -> Option<Self::Out> {
        let mut frame = MexcFrame::from_bytes(data.to_vec(), ts, recv_instant);
        frame.preparse_binary();
        Some(frame)
    }

    fn app_heartbeat(&self) -> Option<AppHeartbeat> {
        // MEXC terminates the session if it does not receive a ping within ~60s.
        // Docs recommend pinging every 10-20s, so we pick 15s for comfortable margin.
        Some(AppHeartbeat {
            interval_secs: 15,
            payload: HeartbeatPayload::Text(r#"{"method":"ping"}"#.to_string()),
        })
    }

    fn label(&self) -> String {
        format!("mexc:{}", self.symbol)
    }
}

impl MexcFrame {
    pub fn from_text(text: &str, ts: Ts, recv_instant: Instant) -> Self {
        Self {
            ts,
            recv_instant,
            raw: text.as_bytes().to_vec(),
            json_cache: None,
            depth_cache: None,
            channel_cache: None,
        }
    }

    pub fn from_bytes(raw: Vec<u8>, ts: Ts, recv_instant: Instant) -> Self {
        Self {
            ts,
            recv_instant,
            raw,
            json_cache: None,
            depth_cache: None,
            channel_cache: None,
        }
    }

    pub fn preparse_text(&mut self, text: &str) {
        let (channel_opt, needs_json, needs_depth) = mexc_preparse_flags(text);
        if self.channel_cache.is_none() {
            if let Some(ch) = channel_opt {
                self.channel_cache = Some(ch.to_string());
            }
        }

        if needs_json && self.json_cache.is_none() {
            match serde_json::from_str::<Value>(text) {
                Ok(value) => {
                    if self.channel_cache.is_none() {
                        self.channel_cache = value
                            .get("channel")
                            .or_else(|| value.get("c"))
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                    }
                    self.json_cache = Some(value);
                }
                Err(err) => {
                    log_parse_drop("mexc_parser", "json", &err, text);
                }
            }
        }

        if needs_depth && self.depth_cache.is_none() {
            match serde_json::from_str::<MexcDepthMsg>(text) {
                Ok(msg) => self.depth_cache = Some(msg),
                Err(err) => {
                    log_parse_drop("mexc_parser", "depth", &err, text);
                }
            }
        }
    }

    pub fn preparse_binary(&mut self) {
        let text = match core::str::from_utf8(&self.raw) {
            Ok(text) => text,
            Err(err) => {
                log_parse_drop_bytes("mexc_parser", "utf8", &err, &self.raw);
                return;
            }
        };
        let (channel_opt, needs_json, needs_depth) = mexc_preparse_flags(text);
        if !needs_json && !needs_depth && channel_opt.is_none() {
            return;
        }
        if self.channel_cache.is_none() {
            if let Some(ch) = channel_opt {
                self.channel_cache = Some(ch.to_string());
            }
        }
        if needs_json && self.json_cache.is_none() {
            match serde_json::from_slice::<Value>(&self.raw) {
                Ok(value) => {
                    if self.channel_cache.is_none() {
                        self.channel_cache = value
                            .get("channel")
                            .or_else(|| value.get("c"))
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                    }
                    self.json_cache = Some(value);
                }
                Err(err) => {
                    log_parse_drop_bytes("mexc_parser", "json", &err, &self.raw);
                }
            }
        }
        if needs_depth && self.depth_cache.is_none() {
            match serde_json::from_slice::<MexcDepthMsg>(&self.raw) {
                Ok(msg) => self.depth_cache = Some(msg),
                Err(err) => {
                    log_parse_drop_bytes("mexc_parser", "depth", &err, &self.raw);
                }
            }
        }
    }

    pub fn channel(&self) -> Option<&str> {
        self.channel_cache.as_deref()
    }

    pub fn text(&self) -> Option<&str> {
        match core::str::from_utf8(&self.raw) {
            Ok(text) => Some(text),
            Err(err) => {
                log_parse_drop_bytes("mexc_parser", "utf8", &err, &self.raw);
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
                        .or_else(|| value.get("c"))
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    self.json_cache = Some(value);
                }
                Err(err) => {
                    log_parse_drop_bytes("mexc_parser", "json", &err, &self.raw);
                }
            }
        }
        self.json_cache.as_ref()
    }

    pub fn depth_msg(&mut self) -> Option<&MexcDepthMsg> {
        if self.depth_cache.is_none() && self.channel().map_or(false, |ch| ch == "push.depth") {
            match serde_json::from_slice::<MexcDepthMsg>(&self.raw) {
                Ok(msg) => self.depth_cache = Some(msg),
                Err(err) => {
                    log_parse_drop_bytes("mexc_parser", "depth", &err, &self.raw);
                }
            }
        }
        self.depth_cache.as_ref()
    }
}
