#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use anyhow::Result;
use tokio::sync::Notify;

use super::gateway::ExecutionGateway;
use super::types::{
    ClientOrderId, ExchangeOrderId, ExecutionReport, OrderAck, OrderStatus, QuoteIntent,
};

/// Simple in-memory gateway used for dry-run/testing flows.
pub struct DryRunGateway {
    id_counter: AtomicU64,
    order_ids: Mutex<HashMap<ClientOrderId, ExchangeOrderId>>,
    reports: Mutex<VecDeque<ExecutionReport>>,
    notify: Notify,
    closed: AtomicBool,
}

impl Default for DryRunGateway {
    fn default() -> Self {
        Self {
            id_counter: AtomicU64::new(1),
            order_ids: Mutex::new(HashMap::new()),
            reports: Mutex::new(VecDeque::new()),
            notify: Notify::new(),
            closed: AtomicBool::new(false),
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

    fn enqueue_report(&self, report: ExecutionReport) {
        let mut reports = self.reports.lock().unwrap();
        reports.push_back(report);
        drop(reports);
        self.notify.notify_one();
    }
}

#[async_trait::async_trait]
impl ExecutionGateway for DryRunGateway {
    async fn submit(&self, intents: &[QuoteIntent]) -> Result<Vec<OrderAck>> {
        let mut acks = Vec::with_capacity(intents.len());
        let mut order_ids = self.order_ids.lock().unwrap();
        for intent in intents {
            let exchange_order_id = self.next_exchange_id();
            order_ids.insert(intent.client_order_id.clone(), exchange_order_id.clone());
            acks.push(OrderAck {
                client_order_id: intent.client_order_id.clone(),
                exchange_order_id: Some(exchange_order_id.clone()),
            });
            self.enqueue_report(ExecutionReport {
                client_order_id: intent.client_order_id.clone(),
                exchange_order_id: Some(exchange_order_id),
                status: OrderStatus::New,
                filled_qty: 0.0,
                avg_fill_price: None,
                ts: None,
            });
        }
        Ok(acks)
    }

    async fn cancel_batch(&self, ids: &[ClientOrderId]) -> Result<()> {
        let mut order_ids = self.order_ids.lock().unwrap();
        for id in ids {
            let exchange_order_id = order_ids.remove(id);
            self.enqueue_report(ExecutionReport {
                client_order_id: id.clone(),
                exchange_order_id,
                status: OrderStatus::Canceled,
                filled_qty: 0.0,
                avg_fill_price: None,
                ts: None,
            });
        }
        Ok(())
    }

    async fn poll_reports(&self) -> Result<Vec<ExecutionReport>> {
        loop {
            {
                let mut reports = self.reports.lock().unwrap();
                if !reports.is_empty() {
                    return Ok(reports.drain(..).collect());
                }
                if self.closed.load(Ordering::Relaxed) {
                    return Ok(Vec::new());
                }
            }
            self.notify.notified().await;
        }
    }
}

impl Drop for DryRunGateway {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Relaxed);
        self.notify.notify_waiters();
    }
}
