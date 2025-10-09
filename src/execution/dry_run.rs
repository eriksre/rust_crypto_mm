#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Result;

use super::gateway::ExecutionGateway;
use super::types::{ClientOrderId, ExchangeOrderId, ExecutionReport, OrderAck, QuoteIntent};

/// Simple in-memory gateway used for dry-run/testing flows.
pub struct DryRunGateway {
    id_counter: AtomicU64,
}

impl Default for DryRunGateway {
    fn default() -> Self {
        Self {
            id_counter: AtomicU64::new(1),
        }
    }
}

impl DryRunGateway {
    pub fn new() -> Self {
        Self::default()
    }

    fn next_exchange_id(&self) -> ExchangeOrderId {
        let id = self.id_counter.fetch_add(1, Ordering::Relaxed);
        ExchangeOrderId(format!("SIM-{}", id))
    }
}

#[async_trait::async_trait]
impl ExecutionGateway for DryRunGateway {
    async fn submit(&self, intents: &[QuoteIntent]) -> Result<Vec<OrderAck>> {
        let acks = intents
            .iter()
            .map(|intent| OrderAck {
                client_order_id: intent.client_order_id.clone(),
                exchange_order_id: Some(self.next_exchange_id()),
            })
            .collect();
        Ok(acks)
    }

    async fn cancel_batch(&self, _ids: &[ClientOrderId]) -> Result<()> {
        Ok(())
    }

    async fn poll_reports(&self) -> Result<Vec<ExecutionReport>> {
        Ok(Vec::new())
    }
}
