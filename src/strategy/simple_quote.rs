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

const DEFAULT_MIN_TICK: f64 = 1e-8;
const DEFAULT_MIN_HALF_SPREAD_BPS: f64 = 15.0;
const DEFAULT_VOL_EWMA_ALPHA: f64 = 0.2;
const DEFAULT_VOL_MULTIPLIER: f64 = 1.5;
const DEFAULT_FEE_BPS: f64 = 1.0;
const DEFAULT_VENUE_BUFFER_BPS: f64 = 1.0;
const DEFAULT_REPRICE_FRACTION: f64 = 0.25;
const DEFAULT_MIN_REST_MS: u64 = 500;
const DEFAULT_MAX_AGE_MS: u64 = 5_000;
const DEFAULT_CROSS_GUARD_TICKS: u32 = 1;
const DEFAULT_REPRICE_MIN_AGE_MS: u64 = 250;
const DEFAULT_CROSS_GRACE_MS: u64 = 150;

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

fn default_min_rest_ms() -> u64 {
    DEFAULT_MIN_REST_MS
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

fn default_reprice_min_age_ms() -> u64 {
    DEFAULT_REPRICE_MIN_AGE_MS
}

fn default_cross_grace_ms() -> u64 {
    DEFAULT_CROSS_GRACE_MS
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
    #[serde(default = "default_min_rest_ms")]
    pub min_rest_ms: u64,
    #[serde(default = "default_max_age_ms")]
    pub max_age_ms: u64,
    #[serde(default = "default_cross_guard_ticks")]
    pub cross_guard_ticks: u32,
    #[serde(default = "default_reprice_min_age_ms")]
    pub reprice_min_age_ms: u64,
    #[serde(default = "default_cross_grace_ms")]
    pub cross_grace_ms: u64,
    #[serde(default = "default_use_reference_bbo")]
    pub use_reference_bbo: bool,
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
    crossed_since: Option<Instant>,
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
    latest_price: Option<f64>,
    latest_best_bid: Option<f64>,
    latest_best_ask: Option<f64>,
    latest_meta: Option<ReferenceMeta>,
    needs_requote: bool,
    last_mid: Option<f64>,
    ewma_abs_ret_bps: Option<f64>,
    last_bid_action_at: Option<Instant>,
    last_ask_action_at: Option<Instant>,
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
            latest_price: None,
            latest_best_bid: None,
            latest_best_ask: None,
            latest_meta: None,
            needs_requote: true,
            last_mid: None,
            ewma_abs_ret_bps: None,
            last_bid_action_at: None,
            last_ask_action_at: None,
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

        let now = reference.received_at;
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

        self.update_volatility(price);

        if self.active_orders.is_empty() {
            self.needs_requote = true;
        }

        let mut cancels = Vec::new();
        let (bid_target, ask_target, half_spread_px) =
            self.compute_targets(price, self.latest_best_bid, self.latest_best_ask);
        let reprice_threshold_px = self.reprice_threshold_px(half_spread_px);
        let max_age = if self.config.max_age_ms == 0 {
            None
        } else {
            Some(Duration::from_millis(self.config.max_age_ms))
        };
        let reprice_min_age = Duration::from_millis(self.config.reprice_min_age_ms);
        let cross_grace = Duration::from_millis(self.config.cross_grace_ms);

        let quotes_snapshot: Vec<(ClientOrderId, ActiveQuote)> = self
            .active_quotes
            .iter()
            .map(|(id, quote)| (id.clone(), quote.clone()))
            .collect();
        let mut update_crossed: Vec<(ClientOrderId, Option<Instant>)> = Vec::new();

        for (id, quote) in quotes_snapshot {
            if self.pending_cancels.contains(&id) {
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

            let crossed_since = if crossed {
                Some(quote.crossed_since.unwrap_or(now))
            } else {
                None
            };
            update_crossed.push((id.clone(), crossed_since));

            let stale = max_age
                .map(|age| now.saturating_duration_since(quote.placed_at) >= age)
                .unwrap_or(false);

            let target_price = match side {
                Side::Bid => bid_target,
                Side::Ask => ask_target,
            };
            let needs_reprice = (target_price - price).abs() >= reprice_threshold_px;
            let rest_elapsed = self.min_rest_elapsed(now, side);
            let age_ok = now.saturating_duration_since(quote.placed_at) >= reprice_min_age;

            let cross_ready = match crossed_since {
                Some(since) => now.saturating_duration_since(since) >= cross_grace,
                None => false,
            };

            if stale || (crossed && cross_ready) || (needs_reprice && rest_elapsed && age_ok) {
                self.pending_cancels.insert(id.clone());
                cancels.push(id.clone());
                self.mark_action(side, now);
            }
        }

        for (id, crossed_since) in update_crossed {
            if let Some(quote) = self.active_quotes.get_mut(&id) {
                quote.crossed_since = crossed_since;
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

        let has_unknown_live_orders = self.active_orders.iter().any(|id| {
            !self.pending_cancels.contains(id) && !self.active_quotes.contains_key(id)
        });
        if has_unknown_live_orders {
            return None;
        }

        let (has_bid, has_ask) = self.active_orders.iter().fold((false, false), |acc, id| {
            let (mut bid_seen, mut ask_seen) = acc;
            if let Some(quote) = self.active_quotes.get(id) {
                match quote.side {
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

        let (bid_px, ask_px, _) = self.compute_targets(price, best_bid, best_ask);
        let want_bid = want_bid && self.min_rest_elapsed(now, Side::Bid);
        let want_ask = want_ask && self.min_rest_elapsed(now, Side::Ask);

        let mut intents = Vec::new();
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
        if intents.is_empty() {
            return None;
        }

        Some(QuotePlan {
            reference_price: price,
            reference_best_bid: best_bid,
            reference_best_ask: best_ask,
            cancels: Vec::new(),
            intents,
            planned_at: now,
            reference_meta: self.latest_meta.clone(),
        })
    }

    pub fn commit_plan(&mut self, plan: &QuotePlan) {
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
                    crossed_since: None,
                },
            );
            self.mark_action(intent.side, plan.planned_at);
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
        if self.config.use_reference_bbo {
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

    fn next_client_id(&mut self, side_tag: &str) -> ClientOrderId {
        self.next_id = self.next_id.wrapping_add(1);
        ClientOrderId::new(format!(
            "t-{}-{}-{}",
            self.config.venue.as_str(),
            side_tag.to_lowercase(),
            self.next_id
        ))
    }

    fn min_rest_elapsed(&self, now: Instant, side: Side) -> bool {
        let min_rest = Duration::from_millis(self.config.min_rest_ms);
        if min_rest.is_zero() {
            return true;
        }
        let last = match side {
            Side::Bid => self.last_bid_action_at,
            Side::Ask => self.last_ask_action_at,
        };
        last.map(|ts| now.saturating_duration_since(ts) >= min_rest)
            .unwrap_or(true)
    }

    fn mark_action(&mut self, side: Side, when: Instant) {
        match side {
            Side::Bid => self.last_bid_action_at = Some(when),
            Side::Ask => self.last_ask_action_at = Some(when),
        }
    }
}
