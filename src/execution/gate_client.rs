#![allow(dead_code)]

use anyhow::{Result, anyhow};

use super::types::{ExecutionReport, OrderAck, QuoteIntent};

/// Thin wrapper for Gate REST endpoints. Real implementation will handle signing & retries.
pub struct GateClient;

impl GateClient {
    pub fn new() -> Self {
        Self
    }

    pub async fn submit_quotes(&self, _intents: &[QuoteIntent]) -> Result<Vec<OrderAck>> {
        Err(anyhow!("GateClient REST submit not implemented yet"))
    }

    pub async fn cancel(&self, _client_ids: &[String]) -> Result<()> {
        Err(anyhow!("GateClient REST cancel not implemented yet"))
    }

    pub async fn poll_reports(&self) -> Result<Vec<ExecutionReport>> {
        Ok(Vec::new())
    }
}
