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
    /// Cancel existing orders and submit new ones.
    ///
    /// Default behavior is two calls (cancel then submit). Venues that support
    /// batching can override to perform an atomic-ish replace.
    async fn cancel_and_submit(
        &self,
        cancel_ids: &[ClientOrderId],
        intents: &[QuoteIntent],
    ) -> Result<Vec<OrderAck>> {
        if !cancel_ids.is_empty() {
            self.cancel_batch(cancel_ids).await?;
        }
        self.submit(intents).await
    }
    async fn poll_reports(&self) -> Result<Vec<ExecutionReport>>;
}
