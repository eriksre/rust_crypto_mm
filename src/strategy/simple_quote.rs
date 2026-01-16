#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use serde::Deserialize;

use crate::base_classes::reference::ReferenceEvent;
use crate::base_classes::state::state;
use crate::base_classes::types::Side;
use crate::execution::{
    ClientOrderId, ExecutionReport, OrderStatus, QuoteIntent, TimeInForce, Venue,
};

const DEFAULT_MIN_TICK: f64 = 1e-8;
const DEFAULT_MIN_HALF_SPREAD_BPS: f64 = 15.0;
const DEFAULT_VOL_EWMA_ALPHA: f64 = 0.2;
const DEFAULT_VOL_MULTIPLIER: f64 = 1.5;
const DEFAULT_FEE_BPS: f64 = 1.0;
const DEFAULT_VENUE_BUFFER_BPS: f64 = 1.0;
const DEFAULT_REPRICE_FRACTION: f64 = 0.25;
const DEFAULT_QUOTE_INTERVAL_MS: u64 = 200;
const DEFAULT_MAX_AGE_MS: u64 = 5_000;
const DEFAULT_CROSS_GUARD_TICKS: u32 = 1;
const DEFAULT_CANCELLATION_DELAY_MS: u64 = 200;

fn default_min_tick() -> f64 {
    DEFAULT_MIN_TICK
}

fn default_min_half_spread_bps() -> f64 {
    DEFAULT_MIN_HALF_SPREAD_BPS
}

fn default_vol_ewma_alpha() -> f64 {
    DEFAULT_VOL_EWMA_ALPHA
}

fn default_vol_multiplier() -> f64 {
    DEFAULT_VOL_MULTIPLIER
}

fn default_fee_bps() -> f64 {
    DEFAULT_FEE_BPS
}

fn default_venue_buffer_bps() -> f64 {
    DEFAULT_VENUE_BUFFER_BPS
}

fn default_reprice_fraction() -> f64 {
    DEFAULT_REPRICE_FRACTION
}

fn default_quote_interval_ms() -> u64 {
    DEFAULT_QUOTE_INTERVAL_MS
}

fn default_max_age_ms() -> u64 {
    DEFAULT_MAX_AGE_MS
}

fn default_cross_guard_ticks() -> u32 {
    DEFAULT_CROSS_GUARD_TICKS
}

fn default_use_reference_bbo() -> bool {
    false
}

fn default_quote_at_reference_bbo() -> bool {
    false
}

fn default_cancellation_delay_ms() -> u64 {
    DEFAULT_CANCELLATION_DELAY_MS
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
    #[serde(default = "default_quote_interval_ms")]
    pub quote_interval_ms: u64,
    #[serde(default = "default_min_tick")]
    pub min_tick: f64,
    #[serde(default = "default_min_half_spread_bps")]
    pub min_half_spread_bps: f64,
    #[serde(default = "default_vol_ewma_alpha")]
    pub volatility_ewma_alpha: f64,
    #[serde(default = "default_vol_multiplier")]
    pub volatility_multiplier: f64,
    #[serde(default = "default_fee_bps")]
    pub fee_bps: f64,
    #[serde(default = "default_venue_buffer_bps")]
    pub venue_buffer_bps: f64,
    #[serde(default = "default_reprice_fraction")]
    pub reprice_fraction: f64,
    #[serde(default = "default_max_age_ms")]
    pub max_age_ms: u64,
    #[serde(default = "default_cross_guard_ticks")]
    pub cross_guard_ticks: u32,
    #[serde(default = "default_cancellation_delay_ms", alias = "cross_grace_ms")]
    pub cancellation_delay_ms: u64,
    #[serde(default = "default_use_reference_bbo")]
    pub use_reference_bbo: bool,
    #[serde(default = "default_quote_at_reference_bbo")]
    pub quote_at_reference_bbo: bool,
}

impl QuoteConfig {
    pub fn resolve_size(&self, exchange_min_base: Option<f64>) -> Result<f64> {
        match self.size {
            SizeSpec::Fixed(v) => Ok(v),
            SizeSpec::ExchangeMin => exchange_min_base
                .ok_or_else(|| anyhow!("exchange minimum size unavailable for this venue")),
        }
    }
}

#[derive(Debug, Clone)]
struct ActiveQuote {
    side: Side,
    price: f64,
    placed_at: Instant,
    delayed_cancel_since: Option<Instant>,
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
    active_orders: Vec<ClientOrderId>,
    active_quotes: HashMap<ClientOrderId, ActiveQuote>,
    pending_cancels: HashSet<ClientOrderId>,
    scheduled_cancels: HashSet<ClientOrderId>,
    latest_price: Option<f64>,
    latest_best_bid: Option<f64>,
    latest_best_ask: Option<f64>,
    latest_meta: Option<ReferenceMeta>,
    needs_requote: bool,
    last_mid: Option<f64>,
    ewma_abs_ret_bps: Option<f64>,
}

impl SimpleQuoteStrategy {
    pub fn new(config: QuoteConfig, base_size: f64) -> Self {
        Self {
            config,
            base_size,
            next_id: 0,
            active_orders: Vec::new(),
            active_quotes: HashMap::new(),
            pending_cancels: HashSet::new(),
            scheduled_cancels: HashSet::new(),
            latest_price: None,
            latest_best_bid: None,
            latest_best_ask: None,
            latest_meta: None,
            needs_requote: true,
            last_mid: None,
            ewma_abs_ret_bps: None,
        }
    }

    pub fn resolve_size(&self, exchange_min_base: Option<f64>) -> Result<f64> {
        match self.config.size {
            SizeSpec::Fixed(v) => Ok(v),
            SizeSpec::ExchangeMin => exchange_min_base
                .ok_or_else(|| anyhow!("exchange minimum size unavailable for this venue")),
        }
    }

    pub fn latest_price(&self) -> Option<f64> {
        self.latest_price
    }

    pub fn on_market_update(&mut self, reference: &ReferenceEvent) -> Vec<ClientOrderId> {
        let price = reference.price;
        if !price.is_finite() || price <= 0.0 {
            return Vec::new();
        }

        let now = reference.received_at;
        self.latest_price = Some(price);
        if let Some(best_bid) = reference.best_bid.filter(|b| b.is_finite() && *b > 0.0) {
            self.latest_best_bid = Some(best_bid);
        }
        if let Some(best_ask) = reference.best_ask.filter(|a| a.is_finite() && *a > 0.0) {
            self.latest_best_ask = Some(best_ask);
        }
        self.latest_meta = Some(ReferenceMeta {
            source: reference.source.clone(),
            ts_ns: reference.ts_ns,
            received_at: reference.received_at,
        });

        self.update_volatility(price);

        if self.active_orders.is_empty() {
            self.needs_requote = true;
        }

        let mut cancels = Vec::new();
        let mut cancel_ids = HashSet::new();
        let mut immediate_bid = false;
        let mut immediate_ask = false;
        let (bid_target, ask_target, half_spread_px) =
            self.compute_targets(price, self.latest_best_bid, self.latest_best_ask);
        let reprice_threshold_px = self.reprice_threshold_px(half_spread_px);
        let max_age = if self.config.max_age_ms == 0 {
            None
        } else {
            Some(Duration::from_millis(self.config.max_age_ms))
        };
        let other_side_delay = Duration::from_millis(self.config.cancellation_delay_ms);

        let quotes_snapshot: Vec<(ClientOrderId, ActiveQuote)> = self
            .active_quotes
            .iter()
            .map(|(id, quote)| (id.clone(), quote.clone()))
            .collect();
        let mut delayed_since_updates: HashMap<ClientOrderId, Option<Instant>> = HashMap::new();

        for (id, quote) in &quotes_snapshot {
            if self.pending_cancels.contains(id) || self.scheduled_cancels.contains(id) {
                continue;
            }
            let side = quote.side;
            let price = quote.price;

            let crossed = match (self.latest_best_bid, self.latest_best_ask) {
                (Some(best_bid), Some(best_ask)) => {
                    let guard = self.cross_guard_px();
                    match side {
                        Side::Bid => price >= best_ask - guard,
                        Side::Ask => price <= best_bid + guard,
                    }
                }
                _ => false,
            };

            let stale = max_age
                .map(|age| now.saturating_duration_since(quote.placed_at) >= age)
                .unwrap_or(false);

            let target_price = match side {
                Side::Bid => bid_target,
                Side::Ask => ask_target,
            };
            let needs_reprice = (target_price - price).abs() >= reprice_threshold_px;
            let cancel_signal = stale || crossed || needs_reprice;
            if cancel_signal {
                match side {
                    Side::Bid => immediate_bid = true,
                    Side::Ask => immediate_ask = true,
                }
                cancels.push(id.clone());
                cancel_ids.insert(id.clone());
                continue;
            }

            let delay_elapsed = quote
                .delayed_cancel_since
                .map(|since| now.saturating_duration_since(since) >= other_side_delay)
                .unwrap_or(false);
            if delay_elapsed {
                cancels.push(id.clone());
                cancel_ids.insert(id.clone());
                continue;
            }

            delayed_since_updates.insert(id.clone(), quote.delayed_cancel_since);
        }

        if immediate_bid || immediate_ask {
            // Cancel the triggered side immediately, but leave the opposite side live briefly.
            for (id, quote) in &quotes_snapshot {
                if self.pending_cancels.contains(id) || self.scheduled_cancels.contains(id) {
                    continue;
                }
                let same_side = match quote.side {
                    Side::Bid => immediate_bid,
                    Side::Ask => immediate_ask,
                };
                if cancel_ids.contains(id) || same_side {
                    continue;
                }
                let entry = delayed_since_updates
                    .entry(id.clone())
                    .or_insert(quote.delayed_cancel_since);
                if entry.is_none() {
                    *entry = Some(now);
                }
            }
        }

        for (id, delayed_since) in delayed_since_updates {
            if let Some(quote) = self.active_quotes.get_mut(&id) {
                quote.delayed_cancel_since = delayed_since;
            }
        }

        for id in &cancels {
            if self.config.venue == Venue::Lighter {
                self.scheduled_cancels.insert(id.clone());
            } else {
                self.pending_cancels.insert(id.clone());
            }
        }

        if !cancels.is_empty() {
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

        let has_unknown_live_orders = self
            .active_orders
            .iter()
            .any(|id| !self.pending_cancels.contains(id) && !self.active_quotes.contains_key(id));
        if has_unknown_live_orders {
            return None;
        }

        let delayed_cancel = self.has_delayed_cancel();

        let (has_bid, has_ask) = self.active_orders.iter().fold((false, false), |acc, id| {
            let (mut bid_seen, mut ask_seen) = acc;
            if self.scheduled_cancels.contains(id)
                || (self.config.venue == Venue::Lighter && self.pending_cancels.contains(id))
            {
                return (bid_seen, ask_seen);
            }
            if let Some(quote) = self.active_quotes.get(id) {
                match quote.side {
                    Side::Bid => bid_seen = true,
                    Side::Ask => ask_seen = true,
                }
            }
            (bid_seen, ask_seen)
        });

        let mut want_bid = !has_bid;
        let mut want_ask = !has_ask;
        if delayed_cancel {
            want_bid = false;
            want_ask = false;
        }

        let mut intents = Vec::new();
        if want_bid || want_ask {
            let (bid_px, ask_px, _) = self.compute_targets(price, best_bid, best_ask);
            if self.config.venue == Venue::Lighter {
                if let Some(lighter_mid) = Self::lighter_mid_from_state() {
                    if bid_px >= lighter_mid || ask_px <= lighter_mid {
                        return None;
                    }
                }
            }
            if want_bid {
                intents.push(QuoteIntent::new(
                    self.config.venue,
                    self.config.symbol.clone(),
                    Side::Bid,
                    bid_px,
                    self.base_size,
                    TimeInForce::PostOnly,
                    self.next_client_id("B"),
                ));
            }
            if want_ask {
                intents.push(QuoteIntent::new(
                    self.config.venue,
                    self.config.symbol.clone(),
                    Side::Ask,
                    ask_px,
                    self.base_size,
                    TimeInForce::PostOnly,
                    self.next_client_id("S"),
                ));
            }
        }

        let cancels = self
            .active_orders
            .iter()
            .filter(|id| self.scheduled_cancels.contains(*id))
            .cloned()
            .collect::<Vec<_>>();

        if intents.is_empty() && cancels.is_empty() {
            return None;
        }

        Some(QuotePlan {
            reference_price: price,
            reference_best_bid: best_bid,
            reference_best_ask: best_ask,
            cancels,
            intents,
            planned_at: now,
            reference_meta: self.latest_meta.clone(),
        })
    }

    pub fn commit_plan(&mut self, plan: &QuotePlan) {
        for id in &plan.cancels {
            self.scheduled_cancels.remove(id);
            self.pending_cancels.insert(id.clone());
        }
        for intent in &plan.intents {
            if !self
                .active_orders
                .iter()
                .any(|id| id == &intent.client_order_id)
            {
                self.active_orders.push(intent.client_order_id.clone());
            }
            self.active_quotes.insert(
                intent.client_order_id.clone(),
                ActiveQuote {
                    side: intent.side,
                    price: intent.price,
                    placed_at: plan.planned_at,
                    delayed_cancel_since: None,
                },
            );
        }
        self.needs_requote = false;
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

    fn update_volatility(&mut self, mid: f64) {
        if let Some(prev) = self.last_mid.filter(|v| *v > 0.0) {
            let change_bps = ((mid - prev).abs() / prev) * 10_000.0;
            let alpha = self.config.volatility_ewma_alpha.clamp(0.0, 1.0);
            let next = if let Some(cur) = self.ewma_abs_ret_bps {
                alpha * change_bps + (1.0 - alpha) * cur
            } else {
                change_bps
            };
            self.ewma_abs_ret_bps = Some(next);
        }
        self.last_mid = Some(mid);
    }

    fn half_spread_bps(&self) -> f64 {
        let vol = self.ewma_abs_ret_bps.unwrap_or(0.0).max(0.0);
        let mut half = self.config.volatility_multiplier.max(0.0) * vol;
        half += self.config.fee_bps.max(0.0);
        half += self.config.venue_buffer_bps.max(0.0);
        half.max(self.config.min_half_spread_bps.max(0.0))
    }

    fn half_spread_px(&self, mid: f64) -> f64 {
        let half_bps = self.half_spread_bps();
        let half = mid * half_bps / 10_000.0;
        half.max(self.config.min_tick.max(1e-8))
    }

    fn reprice_threshold_px(&self, half_spread_px: f64) -> f64 {
        let frac = self.config.reprice_fraction.clamp(0.0, 1.0);
        (half_spread_px * frac).max(self.config.min_tick.max(1e-8))
    }

    fn cross_guard_px(&self) -> f64 {
        self.config.min_tick.max(1e-8) * f64::from(self.config.cross_guard_ticks)
    }

    fn compute_targets(
        &self,
        mid: f64,
        best_bid: Option<f64>,
        best_ask: Option<f64>,
    ) -> (f64, f64, f64) {
        if self.config.use_reference_bbo && self.config.quote_at_reference_bbo {
            if let (Some(bid), Some(ask)) = (best_bid, best_ask) {
                if bid.is_finite() && ask.is_finite() && bid > 0.0 && ask > 0.0 && ask > bid {
                    let (bid_px, ask_px) = self.quote_levels_rounded(bid, ask);
                    let half = ((ask_px - bid_px) / 2.0).max(self.config.min_tick.max(1e-8));
                    return (bid_px, ask_px, half);
                }
            }
        }

        let half = self.half_spread_px(mid);
        let mut bid_raw = mid - half;
        let mut ask_raw = mid + half;

        if let (Some(bid), Some(ask)) = (best_bid, best_ask) {
            if bid.is_finite() && ask.is_finite() && bid > 0.0 && ask > 0.0 {
                let guard = self.cross_guard_px();
                bid_raw = bid_raw.min(ask - guard);
                ask_raw = ask_raw.max(bid + guard);
            }
        }

        let (bid_px, ask_px) = self.quote_levels_rounded(bid_raw, ask_raw);
        (bid_px, ask_px, half)
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

    fn lighter_mid_from_state() -> Option<f64> {
        let guard = match state().lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                eprintln!("ERROR: lighter state lock poisoned: {}", poisoned);
                return None;
            }
        };
        let snap = guard.lighter.orderbook;
        if let Some(price) = snap.price.filter(|p| p.is_finite() && *p > 0.0) {
            return Some(price);
        }
        let bid = snap.bid_levels[0].map(|lvl| lvl.0);
        let ask = snap.ask_levels[0].map(|lvl| lvl.0);
        match (bid, ask) {
            (Some(bid), Some(ask))
                if bid.is_finite() && ask.is_finite() && bid > 0.0 && ask > 0.0 && ask >= bid =>
            {
                Some((bid + ask) / 2.0)
            }
            _ => None,
        }
    }

    fn has_delayed_cancel(&self) -> bool {
        self.active_quotes.iter().any(|(id, quote)| {
            !self.pending_cancels.contains(id)
                && !self.scheduled_cancels.contains(id)
                && quote.delayed_cancel_since.is_some()
        })
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
}
