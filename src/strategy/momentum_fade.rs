#![allow(dead_code)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use serde::Deserialize;

use crate::base_classes::reference::ReferenceEvent;
use crate::base_classes::state::state;
use crate::base_classes::types::Side;
use crate::execution::{
    ClientOrderId, ExecutionReport, OrderStatus, QuoteIntent, TimeInForce, Venue,
};

use super::{FillContext, QuotePlan, QuoteStateMetrics, ReferenceMeta};

const DEFAULT_LOOKBACK_MS: u64 = 140;
const DEFAULT_ENTRY_THRESHOLD_BPS: f64 = 3.0;
const DEFAULT_TICK_OFFSET: u32 = 1;
const DEFAULT_ADVERSE_THRESHOLD_BPS: f64 = 1.0;
const DEFAULT_MAX_AGE_MS: u64 = 2_000;
const DEFAULT_MIN_INTERVAL_MS: u64 = 50;

fn default_entry_price_source() -> EntryPriceSource {
    EntryPriceSource::Model
}

fn default_lookback_ms() -> u64 {
    DEFAULT_LOOKBACK_MS
}

fn default_entry_threshold_bps() -> f64 {
    DEFAULT_ENTRY_THRESHOLD_BPS
}

fn default_tick_offset() -> u32 {
    DEFAULT_TICK_OFFSET
}

fn default_adverse_threshold_bps() -> f64 {
    DEFAULT_ADVERSE_THRESHOLD_BPS
}

fn default_max_age_ms() -> u64 {
    DEFAULT_MAX_AGE_MS
}

fn default_min_interval_ms() -> u64 {
    DEFAULT_MIN_INTERVAL_MS
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EntryPriceSource {
    Model,
    Lighter,
}

impl EntryPriceSource {
    pub fn as_str(self) -> &'static str {
        match self {
            EntryPriceSource::Model => "model",
            EntryPriceSource::Lighter => "lighter",
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct MomentumFadeConfig {
    #[serde(default = "default_entry_price_source")]
    pub entry_price_source: EntryPriceSource,
    #[serde(default = "default_lookback_ms")]
    pub lookback_ms: u64,
    #[serde(default = "default_entry_threshold_bps")]
    pub entry_threshold_bps: f64,
    #[serde(default = "default_tick_offset")]
    pub tick_offset: u32,
    #[serde(default = "default_adverse_threshold_bps")]
    pub adverse_threshold_bps: f64,
    #[serde(default = "default_max_age_ms")]
    pub max_age_ms: u64,
    #[serde(default = "default_min_interval_ms")]
    pub min_interval_ms: u64,
    #[serde(default)]
    pub symbol: Option<String>,
    #[serde(default)]
    pub min_tick: Option<f64>,
    #[serde(default)]
    pub max_order_notional: Option<f64>,
    #[serde(default)]
    pub max_position_notional: Option<f64>,
}

#[derive(Debug, Clone)]
struct PriceSample {
    ts: Instant,
    price: f64,
}

#[derive(Debug, Default, Clone)]
struct PriceHistory {
    samples: VecDeque<PriceSample>,
}

impl PriceHistory {
    fn push(&mut self, ts: Instant, price: f64) {
        if let Some(last) = self.samples.back() {
            if ts < last.ts {
                eprintln!(
                    "WARN: momentum_fade history out-of-order sample; ignoring (ts={:?} < {:?})",
                    ts, last.ts
                );
                return;
            }
        }
        self.samples.push_back(PriceSample { ts, price });
    }

    fn price_at_or_before(&mut self, target: Instant) -> Option<f64> {
        while self.samples.len() > 1 {
            let second = self.samples.get(1)?;
            if second.ts <= target {
                self.samples.pop_front();
            } else {
                break;
            }
        }
        self.samples
            .front()
            .filter(|sample| sample.ts <= target)
            .map(|sample| sample.price)
    }
}

#[derive(Debug, Clone)]
struct ActiveOrder {
    side: Side,
    price: f64,
    placed_at: Instant,
}

pub struct MomentumFadeStrategy {
    config: MomentumFadeConfig,
    venue: Venue,
    symbol: String,
    min_tick: f64,
    base_size: f64,
    next_id: u64,
    active_orders: Vec<ClientOrderId>,
    active_quotes: HashMap<ClientOrderId, ActiveOrder>,
    pending_cancels: HashSet<ClientOrderId>,
    scheduled_cancels: HashSet<ClientOrderId>,
    history: PriceHistory,
    latest_fair_mid: Option<f64>,
    latest_lighter_mid: Option<f64>,
    latest_lighter_bid: Option<f64>,
    latest_lighter_ask: Option<f64>,
    latest_lighter_source: Option<String>,
    latest_lighter_ts_ns: Option<u64>,
    latest_lighter_received_at: Option<Instant>,
    latest_meta: Option<ReferenceMeta>,
    needs_quote: bool,
    last_submit_at: Option<Instant>,
}

impl MomentumFadeStrategy {
    pub fn new(
        config: MomentumFadeConfig,
        venue: Venue,
        symbol: String,
        min_tick: f64,
        base_size: f64,
    ) -> Self {
        Self {
            config,
            venue,
            symbol,
            min_tick,
            base_size,
            next_id: 0,
            active_orders: Vec::new(),
            active_quotes: HashMap::new(),
            pending_cancels: HashSet::new(),
            scheduled_cancels: HashSet::new(),
            history: PriceHistory::default(),
            latest_fair_mid: None,
            latest_lighter_mid: None,
            latest_lighter_bid: None,
            latest_lighter_ask: None,
            latest_lighter_source: None,
            latest_lighter_ts_ns: None,
            latest_lighter_received_at: None,
            latest_meta: None,
            needs_quote: true,
            last_submit_at: None,
        }
    }

    pub fn latest_price(&self) -> Option<f64> {
        self.latest_fair_mid.or(self.latest_lighter_mid)
    }

    pub fn on_market_update(&mut self, reference: &ReferenceEvent) -> Vec<ClientOrderId> {
        let now = reference.received_at;
        let source = reference.source.as_str();
        let mut entry_updated = false;

        if source.starts_with("model:") {
            entry_updated |= self.update_model(reference);
        } else if is_lighter_bbo_source(source) {
            entry_updated |= self.update_lighter(reference);
        }

        if entry_updated {
            self.needs_quote = true;
        }

        self.evaluate_cancels(now)
    }

    pub fn plan_quotes(&mut self, now: Instant) -> Option<QuotePlan> {
        if !self.needs_quote {
            return None;
        }

        let has_unknown_live_orders = self
            .active_orders
            .iter()
            .any(|id| !self.pending_cancels.contains(id) && !self.active_quotes.contains_key(id));
        if has_unknown_live_orders {
            return None;
        }

        let cancels = self
            .scheduled_cancels
            .iter()
            .cloned()
            .collect::<Vec<_>>();

        let bbo = self.latest_lighter_bbo().or_else(Self::lighter_bbo_from_state);
        let (best_bid, best_ask) = bbo
            .map(|(bid, ask)| (Some(bid), Some(ask)))
            .unwrap_or((None, None));

        let mut intents = Vec::new();
        let mut blocked_by_min_interval = false;

        if let Some(side) = self.entry_signal(now) {
            if self.has_live_order(side) {
                // Already have a live order on this side.
            } else if !self.min_interval_elapsed(now) {
                blocked_by_min_interval = true;
            } else if let (Some(bid), Some(ask)) = (best_bid, best_ask) {
                if let Some(price) = self.entry_price_for_side(side, bid, ask) {
                    intents.push(QuoteIntent::new(
                        self.venue,
                        self.symbol.clone(),
                        side,
                        price,
                        self.base_size,
                        TimeInForce::PostOnly,
                        self.next_client_id(match side {
                            Side::Bid => "B",
                            Side::Ask => "S",
                        }),
                    ));
                }
            }
        }

        if intents.is_empty() && cancels.is_empty() {
            if !blocked_by_min_interval {
                self.needs_quote = false;
            }
            return None;
        }

        let reference_price = match self.config.entry_price_source {
            EntryPriceSource::Model => self.latest_fair_mid,
            EntryPriceSource::Lighter => self.latest_lighter_mid,
        }
        .or(self.latest_fair_mid)
        .or(self.latest_lighter_mid)
        .or_else(|| self.active_quotes.values().next().map(|order| order.price))?;
        let reference_meta = match self.config.entry_price_source {
            EntryPriceSource::Model => self.latest_meta.clone(),
            EntryPriceSource::Lighter => self.latest_lighter_received_at.map(|recv_at| ReferenceMeta {
                source: self
                    .latest_lighter_source
                    .clone()
                    .unwrap_or_else(|| "lighter_orderbook".to_string()),
                ts_ns: self.latest_lighter_ts_ns,
                received_at: recv_at,
            }),
        };

        Some(QuotePlan {
            reference_price,
            reference_best_bid: best_bid,
            reference_best_ask: best_ask,
            cancels,
            intents,
            planned_at: now,
            reference_meta,
        })
    }

    pub fn commit_plan(&mut self, plan: &QuotePlan) {
        for id in &plan.cancels {
            self.scheduled_cancels.remove(id);
            self.pending_cancels.insert(id.clone());
        }
        if !plan.intents.is_empty() {
            self.last_submit_at = Some(plan.planned_at);
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
                ActiveOrder {
                    side: intent.side,
                    price: intent.price,
                    placed_at: plan.planned_at,
                },
            );
        }
        self.needs_quote = false;
    }

    pub fn state_metrics(&self) -> QuoteStateMetrics {
        QuoteStateMetrics {
            active_orders: self.active_orders.len(),
            pending_cancels: self.pending_cancels.len(),
            needs_requote: self.needs_quote,
        }
    }

    pub fn handle_report(&mut self, report: &ExecutionReport) {
        match report.status {
            OrderStatus::Filled | OrderStatus::Canceled | OrderStatus::Rejected => {
                self.pending_cancels.remove(&report.client_order_id);
                self.scheduled_cancels.remove(&report.client_order_id);
                self.active_orders
                    .retain(|id| id != &report.client_order_id);
                self.active_quotes.remove(&report.client_order_id);
                self.needs_quote = true;
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
                self.needs_quote = true;
            }
            OrderStatus::New | OrderStatus::Unknown => {
                if self.pending_cancels.contains(&report.client_order_id) {
                    self.pending_cancels.remove(&report.client_order_id);
                    self.needs_quote = true;
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

    pub fn fill_context(&self, order_id: &ClientOrderId, now: Instant) -> FillContext {
        let order_age_ms = self.active_quotes.get(order_id).map(|order| {
            now.saturating_duration_since(order.placed_at)
                .as_millis() as u64
        });
        let lighter_mid = self.latest_lighter_mid.or_else(Self::lighter_mid_from_state);
        FillContext {
            client_order_id: order_id.clone(),
            fair_mid: self.latest_fair_mid,
            lighter_mid,
            order_age_ms,
        }
    }

    fn update_model(&mut self, reference: &ReferenceEvent) -> bool {
        let price = reference.price;
        if !is_valid_price(price) {
            return false;
        }
        self.latest_fair_mid = Some(price);
        self.latest_meta = Some(ReferenceMeta {
            source: reference.source.clone(),
            ts_ns: reference.ts_ns,
            received_at: reference.received_at,
        });
        if self.config.entry_price_source == EntryPriceSource::Model {
            self.history.push(reference.received_at, price);
            return true;
        }
        false
    }

    fn update_lighter(&mut self, reference: &ReferenceEvent) -> bool {
        if let Some(bid) = reference.best_bid.filter(|v| is_valid_price(*v)) {
            self.latest_lighter_bid = Some(bid);
        }
        if let Some(ask) = reference.best_ask.filter(|v| is_valid_price(*v)) {
            self.latest_lighter_ask = Some(ask);
        }

        let mut mid = reference.price;
        if !is_valid_price(mid) {
            if let (Some(bid), Some(ask)) = (self.latest_lighter_bid, self.latest_lighter_ask) {
                mid = (bid + ask) / 2.0;
            }
        }
        if !is_valid_price(mid) {
            return false;
        }

        self.latest_lighter_mid = Some(mid);
        self.latest_lighter_source = Some(reference.source.clone());
        self.latest_lighter_ts_ns = reference.ts_ns;
        self.latest_lighter_received_at = Some(reference.received_at);
        if self.config.entry_price_source == EntryPriceSource::Lighter {
            self.history.push(reference.received_at, mid);
            return true;
        }
        false
    }

    fn evaluate_cancels(&mut self, now: Instant) -> Vec<ClientOrderId> {
        let max_age = if self.config.max_age_ms == 0 {
            None
        } else {
            Some(Duration::from_millis(self.config.max_age_ms))
        };
        let adverse_bps = self.config.adverse_threshold_bps;

        let mut cancels = Vec::new();
        let snapshot: Vec<(ClientOrderId, ActiveOrder)> = self
            .active_quotes
            .iter()
            .map(|(id, order)| (id.clone(), order.clone()))
            .collect();
        for (id, order) in snapshot {
            if self.pending_cancels.contains(&id) || self.scheduled_cancels.contains(&id) {
                continue;
            }
            let stale = max_age
                .map(|age| now.saturating_duration_since(order.placed_at) >= age)
                .unwrap_or(false);
            let adverse_model = self
                .latest_fair_mid
                .filter(|v| is_valid_price(*v))
                .map(|mid| is_adverse(order.side, order.price, mid, adverse_bps))
                .unwrap_or(false);
            let adverse_lighter = self
                .latest_lighter_mid
                .filter(|v| is_valid_price(*v))
                .map(|mid| is_adverse(order.side, order.price, mid, adverse_bps))
                .unwrap_or(false);

            if stale || adverse_model || adverse_lighter {
                cancels.push(id);
            }
        }

        if !cancels.is_empty() {
            for id in &cancels {
                if self.venue == Venue::Lighter {
                    self.scheduled_cancels.insert(id.clone());
                } else {
                    self.pending_cancels.insert(id.clone());
                }
            }
            self.needs_quote = true;
        }

        cancels
    }

    fn entry_signal(&mut self, now: Instant) -> Option<Side> {
        let price_now = match self.config.entry_price_source {
            EntryPriceSource::Model => self.latest_fair_mid,
            EntryPriceSource::Lighter => self.latest_lighter_mid,
        }?;
        if !is_valid_price(price_now) {
            return None;
        }

        let lookback = Duration::from_millis(self.config.lookback_ms.max(1));
        let target = now.checked_sub(lookback)?;
        let price_old = self.history.price_at_or_before(target)?;
        if !is_valid_price(price_old) {
            return None;
        }

        let move_bps = (price_now - price_old) / price_old * 10_000.0;
        if move_bps >= self.config.entry_threshold_bps {
            Some(Side::Bid)
        } else if move_bps <= -self.config.entry_threshold_bps {
            Some(Side::Ask)
        } else {
            None
        }
    }

    fn entry_price_for_side(
        &self,
        side: Side,
        best_bid: f64,
        best_ask: f64,
    ) -> Option<f64> {
        let tick = self.min_tick.max(1e-8);
        let offset = tick * f64::from(self.config.tick_offset);
        match side {
            Side::Bid => {
                let price = best_bid + offset;
                if !is_valid_price(price) || price >= best_ask {
                    None
                } else {
                    Some(price)
                }
            }
            Side::Ask => {
                let price = best_ask - offset;
                if !is_valid_price(price) || price <= best_bid {
                    None
                } else {
                    Some(price)
                }
            }
        }
    }

    fn has_live_order(&self, side: Side) -> bool {
        self.active_quotes.iter().any(|(id, order)| {
            order.side == side
                && !self.pending_cancels.contains(id)
                && !self.scheduled_cancels.contains(id)
        })
    }

    fn min_interval_elapsed(&self, now: Instant) -> bool {
        let min_interval = Duration::from_millis(self.config.min_interval_ms.max(1));
        self.last_submit_at
            .map(|last| now.saturating_duration_since(last) >= min_interval)
            .unwrap_or(true)
    }

    fn latest_lighter_bbo(&self) -> Option<(f64, f64)> {
        match (self.latest_lighter_bid, self.latest_lighter_ask) {
            (Some(bid), Some(ask))
                if is_valid_price(bid) && is_valid_price(ask) && ask >= bid =>
            {
                Some((bid, ask))
            }
            _ => None,
        }
    }

    fn lighter_bbo_from_state() -> Option<(f64, f64)> {
        let guard = match state().lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                eprintln!("ERROR: lighter state lock poisoned: {}", poisoned);
                return None;
            }
        };
        let snap = guard.lighter.orderbook;
        let bid = snap.bid_levels[0].map(|lvl| lvl.0);
        let ask = snap.ask_levels[0].map(|lvl| lvl.0);
        match (bid, ask) {
            (Some(bid), Some(ask))
                if is_valid_price(bid) && is_valid_price(ask) && ask >= bid =>
            {
                Some((bid, ask))
            }
            _ => None,
        }
    }

    fn lighter_mid_from_state() -> Option<f64> {
        Self::lighter_bbo_from_state().map(|(bid, ask)| (bid + ask) / 2.0)
    }

    fn next_client_id(&mut self, side_tag: &str) -> ClientOrderId {
        self.next_id = self.next_id.wrapping_add(1);
        ClientOrderId::new(format!(
            "mf-{}-{}-{}",
            self.venue.as_str(),
            side_tag.to_lowercase(),
            self.next_id
        ))
    }
}

fn is_valid_price(price: f64) -> bool {
    price.is_finite() && price > 0.0
}

fn is_lighter_bbo_source(source: &str) -> bool {
    matches!(source, "lighter_orderbook" | "lighter_ob" | "lighter_bbo")
}

// Treat "adverse" as a move against the order by the configured bps.
fn is_adverse(side: Side, order_price: f64, mid: f64, threshold_bps: f64) -> bool {
    if !is_valid_price(order_price) || !is_valid_price(mid) {
        return false;
    }
    if !threshold_bps.is_finite() || threshold_bps < 0.0 {
        return false;
    }
    let buffer = threshold_bps / 10_000.0;
    match side {
        Side::Bid => mid < order_price * (1.0 - buffer),
        Side::Ask => mid > order_price * (1.0 + buffer),
    }
}
