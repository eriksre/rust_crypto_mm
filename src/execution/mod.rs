#![allow(dead_code)]

pub mod dry_run;
pub mod gate_client;
pub mod gate_ws;
pub mod gateway;
pub mod inventory;
pub mod order_manager;
pub mod types;

pub use dry_run::DryRunGateway;
pub use gate_client::{GateClient, GateCredentials};
pub use gate_ws::{GateWsConfig, GateWsGateway};
pub use gateway::ExecutionGateway;
pub use inventory::{
    InventoryReportOutcome, InventoryTracker, InventoryUpdate, InventoryUpdateSource,
};
pub use order_manager::OrderManager;
pub use types::{
    ClientOrderId, ExchangeOrderId, ExecutionReport, OrderAck, OrderStatus, QuoteIntent,
    TimeInForce, Venue,
};
