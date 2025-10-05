#![allow(dead_code)]

use std::sync::{Mutex, OnceLock};

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
    pub direction: Option<TradeDirection>,
    pub bid_levels: [Option<(f64, f64)>; 3],
    pub ask_levels: [Option<(f64, f64)>; 3],
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
pub struct ExchangeSnap {
    pub orderbook: FeedSnap,
    pub bbo: FeedSnap,
    pub trade: FeedSnap,
    pub ticker: TickerSnap,
}

#[derive(Default, Debug)]
pub struct GlobalState {
    pub bybit: ExchangeSnap,
    pub binance: ExchangeSnap,
    pub gate: ExchangeSnap,
    pub bitget: ExchangeSnap,
}

static STATE: OnceLock<Mutex<GlobalState>> = OnceLock::new();

#[inline(always)]
pub fn state() -> &'static Mutex<GlobalState> {
    STATE.get_or_init(|| Mutex::new(GlobalState::default()))
}
