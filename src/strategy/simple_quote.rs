#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use serde::Deserialize;

use crate::base_classes::types::Side;
use crate::base_classes::reference::ReferenceEvent;
use crate::execution::{
    ClientOrderId, ExecutionReport, OrderStatus, QuoteIntent, TimeInForce, Venue,
};

const DEFAULT_REPRICE_BPS: f64 = 2.0;
const DEFAULT_MIN_TICK: f64 = 1e-8;
const DEFAULT_DEBOUNCE_MS: u64 = 50;
const DEFAULT_CANCEL_BUFFER_MS: u64 = 50;

fn default_cancel_on_cross() -> bool {
    false
}

fn default_reprice_bps() -> f64 {
    DEFAULT_REPRICE_BPS
}

fn default_min_tick() -> f64 {
    DEFAULT_MIN_TICK
}

fn default_debounce_ms() -> u64 {
    DEFAULT_DEBOUNCE_MS
}

fn default_cancel_buffer_ms() -> u64 {
    DEFAULT_CANCEL_BUFFER_MS
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QuoteMode {
    Mid,
    Bbo,
}

fn default_quote_mode() -> QuoteMode {
    QuoteMode::Mid
}

#[derive(Debug, Clone)]
pub enum SizeSpec {
    Fixed(f64),
    ExchangeMin,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
enum SizeSpecInput {
    Fixed(f64),
    Text(String),
}

fn deserialize_size_spec<'de, D>(deserializer: D) -> std::result::Result<SizeSpec, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = SizeSpecInput::deserialize(deserializer)?;
    match raw {
        SizeSpecInput::Fixed(v) => Ok(SizeSpec::Fixed(v)),
        SizeSpecInput::Text(s) => {
            let lower = s.trim().to_ascii_lowercase();
            if ["min", "minimum", "exchange_min", "exchange"].contains(&lower.as_str()) {
                Ok(SizeSpec::ExchangeMin)
            } else {
                Err(serde::de::Error::custom(format!(
                    "unknown size value '{}'; use a number or 'min'",
                    s
                )))
            }
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct QuoteConfig {
    pub venue: Venue,
    pub symbol: String,
    #[serde(deserialize_with = "deserialize_size_spec")]
    pub size: SizeSpec,
    #[serde(default = "default_quote_mode")]
    pub quote_mode: QuoteMode,
    pub spread_bps: f64,
    #[serde(default)]
    pub offset_bps: Option<f64>,
    #[serde(default)]
    pub bid_offset_bps: Option<f64>,
    #[serde(default)]
    pub ask_offset_bps: Option<f64>,
    #[serde(default = "default_min_tick")]
    pub min_tick: f64,
    #[serde(default = "default_reprice_bps")]
    pub reprice_bps: f64,
    #[serde(default = "default_debounce_ms")]
    pub debounce_ms: u64,
    #[serde(default = "default_cancel_buffer_ms")]
    pub cancel_buffer_ms: u64,
    #[serde(default = "default_cancel_on_cross")]
    pub cancel_on_cross: bool,
}

impl QuoteConfig {
    pub fn resolve_size(&self, exchange_min_base: Option<f64>) -> Result<f64> {
        match self.size {
            SizeSpec::Fixed(v) => Ok(v),
            SizeSpec::ExchangeMin => exchange_min_base
                .ok_or_else(|| anyhow!("exchange minimum size unavailable for this venue")),
        }
    }

    pub fn effective_bid_offset_bps(&self) -> f64 {
        self.bid_offset_bps
            .or(self.offset_bps)
            .unwrap_or(0.0)
            .max(0.0)
    }

    pub fn effective_ask_offset_bps(&self) -> f64 {
        self.ask_offset_bps
            .or(self.offset_bps)
            .unwrap_or(0.0)
            .max(0.0)
    }
}

#[derive(Debug, Clone)]
pub struct QuotePlan {
    pub reference_price: f64,
    pub reference_best_bid: Option<f64>,
    pub reference_best_ask: Option<f64>,
    pub cancels: Vec<ClientOrderId>,
    pub intents: Vec<QuoteIntent>,
    pub planned_at: Instant,
    pub reference_meta: Option<ReferenceMeta>,
}

#[derive(Debug, Clone, Copy)]
pub struct QuoteStateMetrics {
    pub active_orders: usize,
    pub pending_cancels: usize,
    pub needs_requote: bool,
}

#[derive(Debug, Clone)]
pub struct ReferenceMeta {
    pub source: String,
    pub ts_ns: Option<u64>,
    pub received_at: Instant,
}

pub struct SimpleQuoteStrategy {
    config: QuoteConfig,
    base_size: f64,
    next_id: u64,
    last_anchor: Option<f64>,
    last_refresh_at: Option<Instant>,
    active_orders: Vec<ClientOrderId>,
    active_quotes: HashMap<ClientOrderId, (Side, f64)>,
    pending_cancels: HashSet<ClientOrderId>,
    latest_price: Option<f64>,
    latest_best_bid: Option<f64>,
    latest_best_ask: Option<f64>,
    latest_meta: Option<ReferenceMeta>,
    needs_requote: bool,
    last_cancel_submission_at: Option<Instant>,
}

impl SimpleQuoteStrategy {
    pub fn new(config: QuoteConfig, base_size: f64) -> Self {
        Self {
            config,
            base_size,
            next_id: 0,
            last_anchor: None,
            last_refresh_at: None,
            active_orders: Vec::new(),
            active_quotes: HashMap::new(),
            pending_cancels: HashSet::new(),
            latest_price: None,
            latest_best_bid: None,
            latest_best_ask: None,
            latest_meta: None,
            needs_requote: true,
            last_cancel_submission_at: None,
        }
    }

    pub fn resolve_size(&self, exchange_min_base: Option<f64>) -> Result<f64> {
        match self.config.size {
            SizeSpec::Fixed(v) => Ok(v),
            SizeSpec::ExchangeMin => exchange_min_base
                .ok_or_else(|| anyhow!("exchange minimum size unavailable for this venue")),
        }
    }

    pub fn on_market_update(&mut self, reference: &ReferenceEvent) -> Vec<ClientOrderId> {
        let price = reference.price;
        if !price.is_finite() || price <= 0.0 {
            return Vec::new();
        }

        self.latest_price = Some(price);
        if let Some(best_bid) = reference
            .best_bid
            .filter(|b| b.is_finite() && *b > 0.0)
        {
            self.latest_best_bid = Some(best_bid);
        }
        if let Some(best_ask) = reference
            .best_ask
            .filter(|a| a.is_finite() && *a > 0.0)
        {
            self.latest_best_ask = Some(best_ask);
        }
        self.latest_meta = Some(ReferenceMeta {
            source: reference.source.clone(),
            ts_ns: reference.ts_ns,
            received_at: reference.received_at,
        });

        if self.active_orders.is_empty() {
            self.needs_requote = true;
        }

        let mut cancels = Vec::new();

        if self.config.cancel_on_cross {
            if let (Some(best_bid), Some(best_ask)) = (self.latest_best_bid, self.latest_best_ask)
            {
                let crossed = self.prepare_cross_cancels(best_bid, best_ask);
                if !crossed.is_empty() {
                    self.needs_requote = true;
                    cancels.extend(crossed);
                }
            }
        }

        let anchor = self.reference_anchor().unwrap_or(price);
        if let Some(last_anchor) = self.last_anchor.filter(|p| *p > 0.0) {
            let change_bps = ((anchor - last_anchor).abs() / last_anchor) * 10_000.0;
            if change_bps >= self.config.reprice_bps.max(f64::EPSILON) {
                self.needs_requote = true;
                cancels.extend(self.prepare_cancels());
            }
        } else {
            self.needs_requote = true;
        }

        cancels
    }

    pub fn plan_quotes(&mut self, now: Instant) -> Option<QuotePlan> {
        let price = self.latest_price?;
        let best_bid = self.latest_best_bid;
        let best_ask = self.latest_best_ask;

        if !self.needs_requote {
            return None;
        }

        if !self.cancel_buffer_elapsed(now) {
            return None;
        }

        if !self.active_orders.is_empty() && !self.debounce_elapsed(now) {
            return None;
        }

        let has_unknown_live_orders = self.active_orders.iter().any(|id| {
            !self.pending_cancels.contains(id) && !self.active_quotes.contains_key(id)
        });
        if has_unknown_live_orders {
            return None;
        }

        let (has_bid, has_ask) = self.active_orders.iter().fold((false, false), |acc, id| {
            let (mut bid_seen, mut ask_seen) = acc;
            if let Some((side, _)) = self.active_quotes.get(id) {
                match side {
                    Side::Bid => bid_seen = true,
                    Side::Ask => ask_seen = true,
                }
            }
            (bid_seen, ask_seen)
        });

        let want_bid = !has_bid;
        let want_ask = !has_ask;
        if !want_bid && !want_ask {
            return None;
        }

        let mut intents = self.build_intents(price, best_bid, best_ask);
        intents.retain(|intent| match intent.side {
            Side::Bid => want_bid,
            Side::Ask => want_ask,
        });
        if intents.is_empty() {
            return None;
        }
        let anchor = self.reference_anchor().unwrap_or(price);

        Some(QuotePlan {
            reference_price: anchor,
            reference_best_bid: best_bid,
            reference_best_ask: best_ask,
            cancels: Vec::new(),
            intents,
            planned_at: now,
            reference_meta: self.latest_meta.clone(),
        })
    }

    pub fn commit_plan(&mut self, plan: &QuotePlan) {
        self.last_anchor = Some(plan.reference_price);
        self.last_refresh_at = Some(plan.planned_at);
        for intent in &plan.intents {
            if !self
                .active_orders
                .iter()
                .any(|id| id == &intent.client_order_id)
            {
                self.active_orders.push(intent.client_order_id.clone());
            }
            self.active_quotes
                .insert(intent.client_order_id.clone(), (intent.side, intent.price));
        }
        self.needs_requote = false;
    }

    pub fn record_cancel_submission(&mut self, when: Instant) {
        self.last_cancel_submission_at = Some(when);
    }

    pub fn state_metrics(&self) -> QuoteStateMetrics {
        QuoteStateMetrics {
            active_orders: self.active_orders.len(),
            pending_cancels: self.pending_cancels.len(),
            needs_requote: self.needs_requote,
        }
    }

    pub fn handle_report(&mut self, report: &ExecutionReport) {
        match report.status {
            OrderStatus::Filled | OrderStatus::Canceled | OrderStatus::Rejected => {
                self.pending_cancels.remove(&report.client_order_id);
                self.active_orders
                    .retain(|id| id != &report.client_order_id);
                self.active_quotes.remove(&report.client_order_id);
                self.needs_requote = true;
            }
            OrderStatus::PartiallyFilled => {
                self.pending_cancels.remove(&report.client_order_id);
                if !self
                    .active_orders
                    .iter()
                    .any(|id| id == &report.client_order_id)
                {
                    self.active_orders.push(report.client_order_id.clone());
                }
                self.needs_requote = true;
            }
            OrderStatus::New | OrderStatus::Unknown => {
                let was_pending_cancel = self.pending_cancels.contains(&report.client_order_id);
                if was_pending_cancel {
                    self.pending_cancels.remove(&report.client_order_id);
                    self.needs_requote = true;
                }
                if !self
                    .active_orders
                    .iter()
                    .any(|id| id == &report.client_order_id)
                {
                    self.active_orders.push(report.client_order_id.clone());
                }
            }
        }
    }

    fn build_intents(
        &mut self,
        mid: f64,
        best_bid: Option<f64>,
        best_ask: Option<f64>,
    ) -> Vec<QuoteIntent> {
        let (bid_px, ask_px) = if self.config.quote_mode == QuoteMode::Bbo {
            if let (Some(b), Some(a)) = (best_bid, best_ask) {
                self.quote_levels_from_reference_bbo(b, a)
            } else {
                self.quote_levels_from_mid(mid)
            }
        } else {
            self.quote_levels_from_mid(mid)
        };

        let bid = QuoteIntent::new(
            self.config.venue,
            self.config.symbol.clone(),
            Side::Bid,
            bid_px,
            self.base_size,
            TimeInForce::PostOnly,
            self.next_client_id("B"),
        );
        let ask = QuoteIntent::new(
            self.config.venue,
            self.config.symbol.clone(),
            Side::Ask,
            ask_px,
            self.base_size,
            TimeInForce::PostOnly,
            self.next_client_id("S"),
        );
        vec![bid, ask]
    }

    fn quote_levels_from_mid(&self, mid: f64) -> (f64, f64) {
        let mut spread = mid * self.config.spread_bps / 10_000.0;
        if spread < self.config.min_tick {
            spread = self.config.min_tick;
        }
        let half = spread / 2.0;
        self.quote_levels_rounded(mid - half, mid + half)
    }

    fn quote_levels_from_reference_bbo(&self, best_bid: f64, best_ask: f64) -> (f64, f64) {
        let bid_offset = self.config.effective_bid_offset_bps() / 10_000.0;
        let ask_offset = self.config.effective_ask_offset_bps() / 10_000.0;

        let bid_ref = best_bid.min(best_ask);
        let ask_ref = best_ask.max(best_bid);

        let bid_raw = bid_ref * (1.0 - bid_offset);
        let ask_raw = ask_ref * (1.0 + ask_offset);
        self.quote_levels_rounded(bid_raw, ask_raw)
    }

    fn quote_levels_rounded(&self, bid_raw: f64, ask_raw: f64) -> (f64, f64) {
        let tick = self.config.min_tick.max(1e-8);
        let mut bid = (bid_raw / tick).floor() * tick;
        let mut ask = (ask_raw / tick).ceil() * tick;

        if bid <= 0.0 {
            bid = tick;
        }
        if ask <= bid {
            ask = bid + tick;
        }
        (bid, ask)
    }

    fn next_client_id(&mut self, side_tag: &str) -> ClientOrderId {
        self.next_id = self.next_id.wrapping_add(1);
        ClientOrderId::new(format!(
            "t-{}-{}-{}",
            self.config.venue.as_str(),
            side_tag.to_lowercase(),
            self.next_id
        ))
    }

    fn debounce_duration(&self) -> Duration {
        Duration::from_millis(self.config.debounce_ms.max(1))
    }

    fn debounce_elapsed(&self, now: Instant) -> bool {
        self.last_refresh_at
            .map(|ts| now.saturating_duration_since(ts) >= self.debounce_duration())
            .unwrap_or(true)
    }

    fn cancel_buffer_duration(&self) -> Duration {
        Duration::from_millis(self.config.cancel_buffer_ms)
    }

    fn cancel_buffer_elapsed(&self, now: Instant) -> bool {
        let buffer = self.cancel_buffer_duration();
        if buffer.is_zero() {
            return true;
        }
        self.last_cancel_submission_at
            .map(|ts| now.saturating_duration_since(ts) >= buffer)
            .unwrap_or(true)
    }

    fn prepare_cancels(&mut self) -> Vec<ClientOrderId> {
        if self.active_orders.is_empty() {
            return Vec::new();
        }

        let mut newly_requested = Vec::with_capacity(self.active_orders.len());
        for id in &self.active_orders {
            if !self.pending_cancels.contains(id) {
                newly_requested.push(id.clone());
                self.pending_cancels.insert(id.clone());
            }
        }

        newly_requested
    }

    fn reference_anchor(&self) -> Option<f64> {
        match (self.latest_best_bid, self.latest_best_ask, self.latest_price) {
            (Some(b), Some(a), _) if b.is_finite() && a.is_finite() && b > 0.0 && a > 0.0 => {
                Some((a + b) / 2.0)
            }
            (_, _, px) => px,
        }
    }

    fn prepare_cross_cancels(&mut self, best_bid: f64, best_ask: f64) -> Vec<ClientOrderId> {
        if self.active_quotes.is_empty() {
            return Vec::new();
        }

        let mut newly_requested = Vec::new();
        for (id, (side, px)) in self.active_quotes.iter() {
            if self.pending_cancels.contains(id) {
                continue;
            }
            let crossed = match side {
                Side::Bid => px >= &best_ask,
                Side::Ask => px <= &best_bid,
            };
            if crossed {
                newly_requested.push(id.clone());
            }
        }

        for id in &newly_requested {
            self.pending_cancels.insert(id.clone());
        }

        newly_requested
    }
}
