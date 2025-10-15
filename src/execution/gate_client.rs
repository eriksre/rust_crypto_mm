#![allow(dead_code)]

use anyhow::{Context, Result, anyhow, bail};
use reqwest::{Client, Method};
use serde_json::Value;

use crate::exchanges::{endpoints::GateioGet, gate_sign};
use crate::utils::parsing::value_to_f64;
use crate::utils::time::current_unix_seconds_string;

use super::types::{ExecutionReport, OrderAck, QuoteIntent};

/// Gate API credentials used for both REST and websocket clients.
#[derive(Debug, Clone)]
pub struct GateCredentials {
    pub api_key: String,
    pub api_secret: String,
}

/// Thin wrapper for Gate REST endpoints. Only the subset required by the runner is implemented.
pub struct GateClient {
    http: Client,
    credentials: GateCredentials,
}

impl GateClient {
    pub fn new(credentials: GateCredentials) -> Self {
        let http = Client::builder()
            .user_agent("gate-client/0.1")
            .build()
            .expect("reqwest client");
        Self { http, credentials }
    }

    pub fn credentials(&self) -> &GateCredentials {
        &self.credentials
    }

    pub async fn fetch_position_contracts(
        &self,
        settle: &str,
        contract: &str,
    ) -> Result<Option<f64>> {
        let path = format!("/api/v4/futures/{}/positions", settle);
        let query = format!("contract={}", contract);
        let value = self
            .signed_request(Method::GET, &path, &query, "")
            .await
            .with_context(|| format!("failed to GET positions for {}", contract))?;

        let matching_entry = match &value {
            Value::Array(entries) => entries.iter().find(|entry| {
                entry
                    .get("contract")
                    .and_then(|v| v.as_str())
                    .map(|s| s.eq_ignore_ascii_case(contract))
                    .unwrap_or(false)
            }),
            Value::Object(_) => {
                if value
                    .get("contract")
                    .and_then(|v| v.as_str())
                    .map(|s| s.eq_ignore_ascii_case(contract))
                    .unwrap_or(false)
                {
                    Some(&value)
                } else {
                    None
                }
            }
            _ => None,
        };

        Ok(matching_entry.and_then(extract_position_contracts))
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

    async fn signed_request(
        &self,
        method: Method,
        path: &str,
        query: &str,
        body: &str,
    ) -> Result<Value> {
        let method_name = method.as_str();
        let ts = current_unix_seconds_string();
        let payload_hash = gate_sign::sha512_hex(body);
        let sign_payload = format!(
            "{}\n{}\n{}\n{}\n{}",
            method_name, path, query, payload_hash, ts
        );
        let signature = gate_sign::hmac_sha512_hex(&self.credentials.api_secret, &sign_payload);

        let url = if query.is_empty() {
            format!("{}{}", GateioGet::BASE, path)
        } else {
            format!("{}{}?{}", GateioGet::BASE, path, query)
        };

        let mut request = self.http.request(method, &url);
        if !body.is_empty() {
            request = request.body(body.to_string());
        }
        let response = request
            .header("KEY", &self.credentials.api_key)
            .header("Timestamp", &ts)
            .header("SIGN", signature)
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .send()
            .await?;

        let status = response.status();
        let text = response.text().await?;
        if !status.is_success() {
            bail!("HTTP {} -> {}", status, text);
        }
        if text.trim().is_empty() {
            return Ok(Value::Null);
        }
        let value: Value = serde_json::from_str(&text)
            .with_context(|| format!("failed to parse JSON response: {}", text))?;
        Ok(value)
    }
}

fn extract_position_contracts(value: &Value) -> Option<f64> {
    let fields = ["size", "current_size", "position", "contracts"];
    for key in fields {
        if let Some(entry) = value.get(key) {
            if let Some(val) = value_to_f64(entry) {
                return Some(val);
            }
        }
    }
    None
}

