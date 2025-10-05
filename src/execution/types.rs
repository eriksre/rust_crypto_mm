use std::fmt;

use serde::{Deserialize, Serialize};

use crate::base_classes::types::Side;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Venue {
    Gate,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeInForce {
    Gtc,
    Ioc,
    Fok,
    PostOnly,
}

impl fmt::Display for TimeInForce {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let as_str = match self {
            Self::Gtc => "gtc",
            Self::Ioc => "ioc",
            Self::Fok => "fok",
            Self::PostOnly => "poc",
        };
        write!(f, "{}", as_str)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClientOrderId(pub String);

impl ClientOrderId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl fmt::Display for ClientOrderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExchangeOrderId(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    New,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
    Unknown,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QuoteIntent {
    pub venue: Venue,
    pub symbol: String,
    pub side: Side,
    pub price: f64,
    pub size: f64,
    pub tif: TimeInForce,
    pub client_order_id: ClientOrderId,
}

impl QuoteIntent {
    pub fn new(
        venue: Venue,
        symbol: impl Into<String>,
        side: Side,
        price: f64,
        size: f64,
        tif: TimeInForce,
        client_order_id: ClientOrderId,
    ) -> Self {
        Self {
            venue,
            symbol: symbol.into(),
            side,
            price,
            size,
            tif,
            client_order_id,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderAck {
    pub client_order_id: ClientOrderId,
    pub exchange_order_id: Option<ExchangeOrderId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionReport {
    pub client_order_id: ClientOrderId,
    pub exchange_order_id: Option<ExchangeOrderId>,
    pub status: OrderStatus,
    pub filled_qty: f64,
    pub avg_fill_price: Option<f64>,
    pub ts: Option<u64>,
}
