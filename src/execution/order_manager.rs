#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::sync::Mutex;

use super::gateway::ExecutionGateway;
use super::types::{ClientOrderId, ExecutionReport, OrderAck, OrderStatus, QuoteIntent};

/// Coordinates order submission, tracking, and reconciliation for a single venue.
pub struct OrderManager {
    gateway: Arc<dyn ExecutionGateway>,
    inflight: Mutex<HashMap<ClientOrderId, QuoteIntent>>,
    last_persist: Mutex<Instant>,
    persist_interval: Duration,
}

impl OrderManager {
    pub fn new(gateway: Arc<dyn ExecutionGateway>, persist_interval: Duration) -> Self {
        Self {
            gateway,
            inflight: Mutex::new(HashMap::new()),
            last_persist: Mutex::new(Instant::now()),
            persist_interval,
        }
    }

    pub async fn submit(&self, intents: Vec<QuoteIntent>) -> Result<Vec<OrderAck>> {
        let acks = self.gateway.submit(&intents).await?;
        let mut inflight = self.inflight.lock().await;
        for intent in intents.into_iter() {
            let key = intent.client_order_id.clone();
            inflight.insert(key, intent);
        }
        self.maybe_persist().await;
        Ok(acks)
    }

    pub async fn cancel(&self, id: &ClientOrderId) -> Result<()> {
        self.cancel_many(std::slice::from_ref(id)).await
    }

    pub async fn cancel_many(&self, ids: &[ClientOrderId]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        self.gateway.cancel_batch(ids).await?;
        let mut inflight = self.inflight.lock().await;
        for id in ids {
            inflight.remove(id);
        }
        Ok(())
    }

    pub async fn poll_reports(&self) -> Result<Vec<ExecutionReport>> {
        let reports = self.gateway.poll_reports().await?;
        if !reports.is_empty() {
            let mut inflight = self.inflight.lock().await;
            for report in &reports {
                if matches!(
                    report.status,
                    OrderStatus::Filled | OrderStatus::Canceled | OrderStatus::Rejected
                ) {
                    inflight.remove(&report.client_order_id);
                }
            }
        }
        self.maybe_persist().await;
        Ok(reports)
    }

    async fn maybe_persist(&self) {
        let mut last = self.last_persist.lock().await;
        if last.elapsed() >= self.persist_interval {
            // TODO: dump inflight orders to disk for crash recovery.
            *last = Instant::now();
        }
    }
}
