#![allow(dead_code)]

use std::time::{Duration, Instant};

use serde::Deserialize;

use crate::base_classes::types::Side;
use crate::execution::{
    ClientOrderId, ExecutionReport, OrderStatus, QuoteIntent, TimeInForce, Venue,
};

const DEFAULT_REPRICE_BPS: f64 = 2.0;
const DEFAULT_MIN_TICK: f64 = 1e-8;
const DEFAULT_DEBOUNCE_MS: u64 = 50;
const DEFAULT_CANCEL_BUFFER_MS: u64 = 50;

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

#[derive(Debug, Deserialize, Clone)]
pub struct QuoteConfig {
    pub venue: Venue,
    pub symbol: String,
    pub size: f64,
    pub spread_bps: f64,
    #[serde(default = "default_min_tick")]
    pub min_tick: f64,
    #[serde(default = "default_reprice_bps")]
    pub reprice_bps: f64,
    #[serde(default = "default_debounce_ms")]
    pub debounce_ms: u64,
    #[serde(default = "default_cancel_buffer_ms")]
    pub cancel_buffer_ms: u64,
}

#[derive(Debug, Clone)]
pub struct QuotePlan {
    pub reference_price: f64,
    pub cancels: Vec<ClientOrderId>,
    pub intents: Vec<QuoteIntent>,
    pub planned_at: Instant,
    pub reference_meta: Option<ReferenceMeta>,
}

#[derive(Debug, Clone)]
pub struct ReferenceMeta {
    pub source: String,
    pub ts_ns: Option<u64>,
    pub received_at: Instant,
}

pub struct SimpleQuoteStrategy {
    config: QuoteConfig,
    next_id: u64,
    last_reference: Option<f64>,
    last_refresh_at: Option<Instant>,
    active_orders: Vec<ClientOrderId>,
    pending_cancels: Vec<ClientOrderId>,
    latest_price: Option<f64>,
    latest_meta: Option<ReferenceMeta>,
    needs_requote: bool,
    last_cancel_submission_at: Option<Instant>,
}

impl SimpleQuoteStrategy {
    pub fn new(config: QuoteConfig) -> Self {
        Self {
            config,
            next_id: 0,
            last_reference: None,
            last_refresh_at: None,
            active_orders: Vec::new(),
            pending_cancels: Vec::new(),
            latest_price: None,
            latest_meta: None,
            needs_requote: true,
            last_cancel_submission_at: None,
        }
    }

    pub fn on_market_update(
        &mut self,
        price: f64,
        meta: Option<ReferenceMeta>,
        _now: Instant,
    ) -> Vec<ClientOrderId> {
        if !price.is_finite() || price <= 0.0 {
            return Vec::new();
        }

        self.latest_price = Some(price);
        self.latest_meta = meta;

        if self.active_orders.is_empty() {
            self.needs_requote = true;
        }

        let mut cancels = Vec::new();
        if let Some(last_price) = self.last_reference.filter(|p| *p > 0.0) {
            let change_bps = ((price - last_price).abs() / last_price) * 10_000.0;
            if change_bps >= self.config.reprice_bps.max(f64::EPSILON) {
                self.needs_requote = true;
                cancels = self.prepare_cancels();
            }
        } else {
            self.needs_requote = true;
        }

        cancels
    }

    pub fn plan_quotes(&mut self, now: Instant) -> Option<QuotePlan> {
        let price = self.latest_price?;

        if !self.needs_requote {
            return None;
        }

        if !self.cancel_buffer_elapsed(now) {
            return None;
        }

        if !self.active_orders.is_empty() && !self.debounce_elapsed(now) {
            return None;
        }

        let has_uncancelled_live_orders = self
            .active_orders
            .iter()
            .any(|id| !self.pending_cancels.iter().any(|pending| pending == id));
        if has_uncancelled_live_orders {
            return None;
        }

        let intents = self.build_intents(price);

        Some(QuotePlan {
            reference_price: price,
            cancels: Vec::new(),
            intents,
            planned_at: now,
            reference_meta: self.latest_meta.clone(),
        })
    }

    pub fn commit_plan(&mut self, plan: &QuotePlan) {
        self.last_reference = Some(plan.reference_price);
        self.last_refresh_at = Some(plan.planned_at);
        for intent in &plan.intents {
            if !self
                .active_orders
                .iter()
                .any(|id| id == &intent.client_order_id)
            {
                self.active_orders.push(intent.client_order_id.clone());
            }
        }
        self.needs_requote = false;
    }

    pub fn record_cancel_submission(&mut self, when: Instant) {
        self.last_cancel_submission_at = Some(when);
    }

    pub fn handle_report(&mut self, report: &ExecutionReport) {
        match report.status {
            OrderStatus::Filled | OrderStatus::Canceled | OrderStatus::Rejected => {
                self.pending_cancels
                    .retain(|id| id != &report.client_order_id);
                self.active_orders
                    .retain(|id| id != &report.client_order_id);
                self.needs_requote = true;
            }
            OrderStatus::PartiallyFilled => {
                self.pending_cancels
                    .retain(|id| id != &report.client_order_id);
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
                let was_pending_cancel = self
                    .pending_cancels
                    .iter()
                    .any(|id| id == &report.client_order_id);
                if was_pending_cancel {
                    self.pending_cancels
                        .retain(|id| id != &report.client_order_id);
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

    fn build_intents(&mut self, mid: f64) -> Vec<QuoteIntent> {
        let mut spread = mid * self.config.spread_bps / 10_000.0;
        if spread < self.config.min_tick {
            spread = self.config.min_tick;
        }
        let half = spread / 2.0;
        let (bid_px, ask_px) = self.quote_levels(mid, half);

        let bid = QuoteIntent::new(
            self.config.venue,
            self.config.symbol.clone(),
            Side::Bid,
            bid_px,
            self.config.size,
            TimeInForce::PostOnly,
            self.next_client_id("B"),
        );
        let ask = QuoteIntent::new(
            self.config.venue,
            self.config.symbol.clone(),
            Side::Ask,
            ask_px,
            self.config.size,
            TimeInForce::PostOnly,
            self.next_client_id("S"),
        );
        vec![bid, ask]
    }

    fn quote_levels(&self, mid: f64, half_spread: f64) -> (f64, f64) {
        let tick = self.config.min_tick.max(1e-8);
        let mut bid = ((mid - half_spread) / tick).floor() * tick;
        let mut ask = ((mid + half_spread) / tick).ceil() * tick;

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
            "t-gate-{}-{}",
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

        let mut newly_requested = Vec::new();
        for id in &self.active_orders {
            if !self.pending_cancels.iter().any(|pending| pending == id) {
                newly_requested.push(id.clone());
            }
        }

        if !newly_requested.is_empty() {
            self.pending_cancels.extend(newly_requested.iter().cloned());
        }

        newly_requested
    }
}
