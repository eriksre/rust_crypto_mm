use anyhow::Result;
use async_trait::async_trait;

use super::types::{ClientOrderId, ExecutionReport, OrderAck, QuoteIntent};

#[async_trait]
pub trait ExecutionGateway: Send + Sync {
    async fn submit(&self, intents: &[QuoteIntent]) -> Result<Vec<OrderAck>>;
    async fn cancel(&self, id: &ClientOrderId) -> Result<()> {
        self.cancel_batch(std::slice::from_ref(id)).await
    }
    async fn cancel_batch(&self, ids: &[ClientOrderId]) -> Result<()>;
    async fn poll_reports(&self) -> Result<Vec<ExecutionReport>>;
}
