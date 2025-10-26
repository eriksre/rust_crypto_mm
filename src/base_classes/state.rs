#![allow(dead_code)]

use std::collections::VecDeque;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use crate::base_classes::types::Ts;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TradeDirection {
    Buy,
    Sell,
}

impl TradeDirection {
    #[inline(always)]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "buy",
            Self::Sell => "sell",
        }
    }
}

#[derive(Clone, Copy, Default, Debug)]
pub struct FeedSnap {
    pub price: Option<f64>,
    pub seq: u64,
    pub ts_ns: Option<Ts>,
    pub source_engine_ts_ns: Option<Ts>,
    pub source_system_ts_ns: Option<Ts>,
    pub direction: Option<TradeDirection>,
    pub bid_levels: [Option<(f64, f64)>; 3],
    pub ask_levels: [Option<(f64, f64)>; 3],
    pub received_at: Option<Instant>,
}

#[derive(Clone, Copy, Default, Debug)]
pub struct TradeEvent {
    pub ts_ns: Ts,
    pub price: f64,
    pub direction: Option<TradeDirection>,
    pub quantity: Option<f64>,
}

#[derive(Clone, Copy, Default, Debug)]
pub struct ExchangeAdjustment {
    pub offset: Option<f64>,
    pub samples: u32,
    pub last_update_ns: Option<Ts>,
}

#[derive(Clone, Default, Debug)]
pub struct DemeanState {
    pub bybit: ExchangeAdjustment,
    pub binance: ExchangeAdjustment,
    pub bitget: ExchangeAdjustment,
    pub okx: ExchangeAdjustment,
}

#[derive(Clone, Copy, Default, Debug)]
pub struct TickerSnap {
    pub last_price: Option<f64>,
    pub last_qty: Option<f64>,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub mark_price: Option<f64>,
    pub index_price: Option<f64>,
    pub funding_rate: Option<f64>,
    pub turnover_24h: Option<f64>,
    pub open_interest: Option<f64>,
    pub open_interest_value: Option<f64>,
    pub seq: u64,
    pub ts_ns: Option<Ts>,
    pub quanto_multiplier: Option<f64>,
}

#[derive(Clone, Default, Debug)]
pub struct UserTradeSnap {
    pub price: Option<f64>,
    pub contracts: Option<f64>,
    pub quantity: Option<f64>,
    pub fee: Option<f64>,
    pub role: Option<String>,
    pub text: Option<String>,
    pub order_id: Option<String>,
    pub ts_ns: Option<Ts>,
    pub direction: Option<TradeDirection>,
    pub seq: u64,
}

#[derive(Clone, Default, Debug)]
pub struct ExchangeSnap {
    pub orderbook: FeedSnap,
    pub bbo: FeedSnap,
    pub trade: FeedSnap,
    pub user_trade: UserTradeSnap,
    pub ticker: TickerSnap,
    pub trade_events: VecDeque<TradeEvent>,
}

#[derive(Default, Debug)]
pub struct GlobalState {
    pub bybit: ExchangeSnap,
    pub binance: ExchangeSnap,
    pub gate: ExchangeSnap,
    pub bitget: ExchangeSnap,
    pub okx: ExchangeSnap,
    pub demean: DemeanState,
}

static STATE: OnceLock<Mutex<GlobalState>> = OnceLock::new();

#[inline(always)]
pub fn state() -> &'static Mutex<GlobalState> {
    STATE.get_or_init(|| Mutex::new(GlobalState::default()))
}
