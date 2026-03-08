use std::collections::{HashMap, HashSet};
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_longlong};
use std::path::Path;
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use libloading::{Library, Symbol};
use parking_lot::Mutex;
use reqwest::{Client, Url};
use serde::Deserialize;
use serde_json::{self, Value, json};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Mutex as AsyncMutex, Notify, mpsc as tokio_mpsc, oneshot};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config, tungstenite::Message,
};

use crate::base_classes::types::Side;
use crate::execution::types::{
    ClientOrderId, ExchangeOrderId, ExecutionReport, OrderAck, OrderStatus, QuoteIntent,
    TimeInForce,
};
use crate::execution::{ExecutionGateway, Venue};
use crate::utils::parsing::log_parse_drop;
use crate::utils::time::{current_unix_ms, current_unix_ts};
mod protocol;
mod reconcile;
mod rest;
mod signer;
mod state;

pub use protocol::LighterCredentials;
pub use signer::resolve_lighter_signer_path;

use protocol::*;
use reconcile::{map_status, update_from_entry};
use rest::LighterRestClient;
use signer::SignerHandle;
use state::*;

enum LighterWsCommand {
    SendBatch {
        txs: Vec<(SignedTx, ClientOrderId)>,
        resp: oneshot::Sender<Result<SendTxBatchResponse>>,
    },
    Shutdown,
}

#[derive(Clone)]
struct LighterWsConfig {
    ws_url: String,
    account_index: i64,
    api_key_index: i32,
    market_index: u8,
    debug_prints: bool,
}

struct LighterWsWorker {
    cfg: LighterWsConfig,
    signer: SignerHandle,
    command_rx: tokio_mpsc::Receiver<LighterWsCommand>,
    orders: Arc<Mutex<HashMap<ClientOrderId, OrderState>>>,
    pending_reports: Arc<Mutex<Vec<ExecutionReport>>>,
    report_notify: Arc<Notify>,
    size_scale: f64,
    request_id: u64,
}

impl LighterWsWorker {
    fn collect_expected_tx_hashes(txs: &[(SignedTx, ClientOrderId)]) -> Result<Vec<String>> {
        let mut hashes = Vec::new();
        let mut seen = HashSet::new();
        for (tx, client_order_id) in txs {
            let Some(hash) = tx
                .tx_hash
                .as_deref()
                .map(str::trim)
                .filter(|hash| !hash.is_empty())
            else {
                continue;
            };
            if !seen.insert(hash.to_string()) {
                bail!(
                    "duplicate signer tx hash {} in sendTxBatch for client_order_id={}",
                    hash,
                    client_order_id.0
                );
            }
            hashes.push(hash.to_string());
        }
        Ok(hashes)
    }

    fn extract_sendtx_req_id(value: &Value) -> Option<String> {
        value
            .get("data")
            .and_then(|d| d.get("id"))
            .or_else(|| value.get("id"))
            .and_then(|v| {
                if let Some(s) = v.as_str() {
                    Some(s.to_string())
                } else {
                    v.as_i64().map(|n| n.to_string())
                }
            })
    }

    fn parse_sendtx_code(value: &Value) -> Result<i32> {
        if let Some(code) = value.as_i64() {
            return i32::try_from(code)
                .map_err(|_| anyhow!("sendTxBatch code out of range: {code}"));
        }
        if let Some(code) = value.as_str() {
            return code
                .parse::<i32>()
                .with_context(|| format!("invalid sendTxBatch code '{code}'"));
        }
        bail!("missing/invalid sendTxBatch code")
    }

    fn parse_sendtx_hashes(value: Option<&Value>) -> Vec<String> {
        let Some(value) = value else {
            return Vec::new();
        };
        if let Some(arr) = value.as_array() {
            return arr
                .iter()
                .filter_map(|v| v.as_str().map(ToString::to_string))
                .collect();
        }
        if let Some(single) = value.as_str() {
            let trimmed = single.trim();
            if !trimmed.is_empty() {
                return vec![trimmed.to_string()];
            }
        }
        Vec::new()
    }

    fn parse_sendtx_response_obj(
        obj: &serde_json::Map<String, Value>,
    ) -> Result<SendTxBatchResponse> {
        let code_val = obj
            .get("code")
            .ok_or_else(|| anyhow!("missing code in sendTxBatch response"))?;
        let code = Self::parse_sendtx_code(code_val)?;
        let message = obj
            .get("message")
            .and_then(|v| v.as_str())
            .map(ToString::to_string);
        let tx_hash = Self::parse_sendtx_hashes(obj.get("tx_hash").or_else(|| obj.get("txHash")));
        Ok(SendTxBatchResponse {
            code,
            message,
            tx_hash,
        })
    }

    fn parse_sendtx_response(value: &Value) -> Result<SendTxBatchResponse> {
        if let Some(obj) = value
            .get("data")
            .and_then(|d| d.get("attributes"))
            .and_then(|v| v.as_object())
        {
            return Self::parse_sendtx_response_obj(obj);
        }
        if let Some(obj) = value.get("data").and_then(|v| v.as_object()) {
            if obj.contains_key("code") || obj.contains_key("tx_hash") || obj.contains_key("txHash")
            {
                return Self::parse_sendtx_response_obj(obj);
            }
        }
        if let Some(obj) = value.as_object() {
            if obj.contains_key("code") || obj.contains_key("tx_hash") || obj.contains_key("txHash")
            {
                return Self::parse_sendtx_response_obj(obj);
            }
        }
        bail!("invalid sendtxbatch response: expected code/tx_hash fields")
    }

    fn parse_sendtx_error(value: &Value) -> Result<anyhow::Error> {
        let Some(err_obj) = value.get("error").and_then(|v| v.as_object()) else {
            bail!("missing error object in sendtxbatch error frame");
        };
        let code = err_obj
            .get("code")
            .map(Self::parse_sendtx_code)
            .transpose()?
            .unwrap_or_default();
        let message = err_obj
            .get("message")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("unknown sendTxBatch error");
        Ok(anyhow!("sendTxBatch error {}: {}", code, message))
    }

    fn normalize_tx_hashes(tx_hashes: &[String]) -> Result<Vec<String>> {
        if tx_hashes.is_empty() {
            bail!("sendTxBatch response missing tx_hash values");
        }
        let mut normalized = Vec::with_capacity(tx_hashes.len());
        let mut seen = HashSet::new();
        for raw_hash in tx_hashes {
            let hash = raw_hash.trim();
            if hash.is_empty() {
                bail!("sendTxBatch response contained empty tx_hash value");
            }
            if !seen.insert(hash.to_string()) {
                bail!("sendTxBatch response contained duplicate tx_hash {}", hash);
            }
            normalized.push(hash.to_string());
        }
        Ok(normalized)
    }

    fn value_has_sendtx_marker(value: &Value) -> bool {
        let msg_type = value
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        if msg_type.contains("jsonapi/sendtx") {
            return true;
        }
        if value
            .get("data")
            .and_then(|d| d.get("type"))
            .and_then(|v| v.as_str())
            .map(|s| s.contains("jsonapi/sendtx"))
            .unwrap_or(false)
        {
            return true;
        }
        let has_code = value
            .get("code")
            .or_else(|| value.get("data").and_then(|d| d.get("code")))
            .or_else(|| {
                value
                    .get("data")
                    .and_then(|d| d.get("attributes"))
                    .and_then(|a| a.get("code"))
            })
            .is_some();
        let has_hash = value
            .get("tx_hash")
            .or_else(|| value.get("txHash"))
            .or_else(|| value.get("data").and_then(|d| d.get("tx_hash")))
            .or_else(|| value.get("data").and_then(|d| d.get("txHash")))
            .or_else(|| {
                value
                    .get("data")
                    .and_then(|d| d.get("attributes"))
                    .and_then(|a| a.get("tx_hash").or_else(|| a.get("txHash")))
            })
            .is_some();
        has_code || has_hash
    }

    fn remove_pending_batch(
        pending: &mut HashMap<String, PendingSendTxBatch>,
        pending_by_hash: &mut HashMap<String, String>,
        req_id: Option<String>,
        debug_prints: bool,
    ) -> Option<(String, PendingSendTxBatch)> {
        let req_id = req_id
            .map(|id| id.trim().to_string())
            .filter(|id| !id.is_empty());
        if let Some(req_id) = req_id {
            if let Some(batch) = pending.remove(&req_id) {
                Self::cleanup_pending_hash_links(&req_id, &batch.expected_hashes, pending_by_hash);
                return Some((req_id, batch));
            }
            if let Some(stripped) = req_id.strip_prefix("txb-") {
                if let Some(batch) = pending.remove(stripped) {
                    Self::cleanup_pending_hash_links(
                        stripped,
                        &batch.expected_hashes,
                        pending_by_hash,
                    );
                    if debug_prints {
                        eprintln!(
                            "[lighter-ws] matched sendtx response id {} to pending {}",
                            req_id, stripped
                        );
                    }
                    return Some((stripped.to_string(), batch));
                }
            } else {
                let prefixed = format!("txb-{req_id}");
                if let Some(batch) = pending.remove(&prefixed) {
                    Self::cleanup_pending_hash_links(
                        &prefixed,
                        &batch.expected_hashes,
                        pending_by_hash,
                    );
                    if debug_prints {
                        eprintln!(
                            "[lighter-ws] matched sendtx response id {} to pending {}",
                            req_id, prefixed
                        );
                    }
                    return Some((prefixed, batch));
                }
            }
            if pending.len() == 1 {
                let (only_id, batch) = pending.drain().next().expect("pending len checked");
                Self::cleanup_pending_hash_links(&only_id, &batch.expected_hashes, pending_by_hash);
                eprintln!(
                    "WARN: unmatched sendtx response id {}; pairing with only pending request {}",
                    req_id, only_id
                );
                return Some((only_id, batch));
            }
            eprintln!(
                "WARN: unmatched sendtx response id {}; pending requests={:?}",
                req_id,
                pending.keys().collect::<Vec<_>>()
            );
            return None;
        }
        if pending.len() == 1 {
            let (only_id, batch) = pending.drain().next().expect("pending len checked");
            Self::cleanup_pending_hash_links(&only_id, &batch.expected_hashes, pending_by_hash);
            if debug_prints {
                eprintln!(
                    "[lighter-ws] response missing id; pairing with pending {}",
                    only_id
                );
            }
            return Some((only_id, batch));
        }
        if !pending.is_empty() {
            eprintln!(
                "WARN: sendtx response missing id with multiple pending requests={:?}",
                pending.keys().collect::<Vec<_>>()
            );
        }
        None
    }

    fn cleanup_pending_hash_links(
        req_id: &str,
        hashes: &[String],
        pending_by_hash: &mut HashMap<String, String>,
    ) {
        for hash in hashes {
            if pending_by_hash
                .get(hash)
                .map(|pending_req| pending_req == req_id)
                == Some(true)
            {
                pending_by_hash.remove(hash);
            }
        }
    }

    fn validate_pending_batch_insert(
        pending: &HashMap<String, PendingSendTxBatch>,
        pending_by_hash: &HashMap<String, String>,
        req_id: &str,
        expected_hashes: &[String],
    ) -> Result<()> {
        if pending.contains_key(req_id) {
            bail!("duplicate pending sendTxBatch request id {}", req_id);
        }
        for hash in expected_hashes {
            if let Some(existing_req_id) = pending_by_hash.get(hash) {
                bail!(
                    "pending sendTxBatch tx hash collision hash={} existing_req_id={} new_req_id={}",
                    hash,
                    existing_req_id,
                    req_id
                );
            }
        }
        Ok(())
    }

    fn insert_pending_batch(
        pending: &mut HashMap<String, PendingSendTxBatch>,
        pending_by_hash: &mut HashMap<String, String>,
        req_id: String,
        batch: PendingSendTxBatch,
    ) -> Result<()> {
        Self::validate_pending_batch_insert(
            pending,
            pending_by_hash,
            &req_id,
            &batch.expected_hashes,
        )?;
        for hash in &batch.expected_hashes {
            pending_by_hash.insert(hash.clone(), req_id.clone());
        }
        pending.insert(req_id, batch);
        Ok(())
    }

    fn complete_pending_batch(
        pending: &mut HashMap<String, PendingSendTxBatch>,
        pending_by_hash: &mut HashMap<String, String>,
        req_id: &str,
    ) -> Option<PendingSendTxBatch> {
        let batch = pending.remove(req_id)?;
        Self::cleanup_pending_hash_links(req_id, &batch.expected_hashes, pending_by_hash);
        Some(batch)
    }

    fn parse_account_tx_event_info(tx: &LighterTxEntry) -> Result<Option<Value>> {
        let event_info = tx.event_info.as_deref().unwrap_or_default().trim();
        if event_info.is_empty() {
            return Ok(None);
        }
        let parsed = serde_json::from_str::<Value>(event_info).with_context(|| {
            format!(
                "invalid lighter account_tx event_info JSON: {}",
                truncate_for_log(event_info, 256)
            )
        })?;
        Ok(Some(parsed))
    }

    fn collect_account_tx_order_refs(
        tx_type: u8,
        event_info: Option<&Value>,
    ) -> Vec<AccountTxOrderRef> {
        let mut refs = HashSet::new();
        let Some(event_info) = event_info else {
            return Vec::new();
        };

        fn extract_ref(value: &Value) -> Option<AccountTxOrderRef> {
            let obj = value.as_object()?;
            let client_order_index = obj.get("u").and_then(Value::as_i64)?;
            let order_index = obj.get("i").and_then(Value::as_i64)?;
            if client_order_index <= 0 || order_index <= 0 {
                return None;
            }
            Some(AccountTxOrderRef {
                client_order_index,
                order_index,
            })
        }

        if tx_type == LIGHTER_TX_TYPE_CANCEL_ORDER {
            if let Some(order_ref) = extract_ref(event_info) {
                refs.insert(order_ref);
            }
        }

        if tx_type == LIGHTER_TX_TYPE_CREATE_ORDER {
            for key in ["to", "mo"] {
                if let Some(value) = event_info.get(key) {
                    if let Some(order_ref) = extract_ref(value) {
                        refs.insert(order_ref);
                    }
                }
            }
            if let Some(order_ref) = extract_ref(event_info) {
                refs.insert(order_ref);
            }
        }

        refs.into_iter().collect()
    }

    fn parse_account_tx_outcome(
        tx: &LighterTxEntry,
        event_info: Option<&Value>,
    ) -> Result<AccountTxOutcome> {
        let message = tx.message.as_deref().unwrap_or_default().trim();
        let mut application_error = if message.is_empty() {
            None
        } else {
            Some(message.to_string())
        };
        if let Some(parsed) = event_info {
            if let Some(ae) = parsed.get("ae").and_then(|value| value.as_str()) {
                let ae = ae.trim();
                if !ae.is_empty() {
                    let detail = match serde_json::from_str::<Value>(ae) {
                        Ok(nested) => nested
                            .get("message")
                            .and_then(|value| value.as_str())
                            .map(ToString::to_string)
                            .unwrap_or_else(|| truncate_for_log(ae, 256)),
                        Err(_) => truncate_for_log(ae, 256),
                    };
                    application_error = Some(detail);
                }
            }
        }
        Ok(AccountTxOutcome {
            confirmed: tx.status == Some(2) && tx.executed_at.unwrap_or_default() > 0,
            application_error,
        })
    }

    fn update_pending_batch_from_account_tx(
        tx: &LighterTxEntry,
        pending: &mut HashMap<String, PendingSendTxBatch>,
        pending_by_hash: &HashMap<String, String>,
    ) -> Option<(String, Result<SendTxBatchResponse>)> {
        let hash = tx
            .hash
            .as_deref()
            .map(str::trim)
            .filter(|hash| !hash.is_empty())?
            .to_string();
        let req_id = pending_by_hash.get(&hash)?.clone();
        let event_info = match Self::parse_account_tx_event_info(tx) {
            Ok(event_info) => event_info,
            Err(err) => {
                return Some((
                    req_id,
                    Err(anyhow!(
                        "lighter account_tx for hash {} could not be decoded: {:#}",
                        hash,
                        err
                    )),
                ));
            }
        };
        let outcome = match Self::parse_account_tx_outcome(tx, event_info.as_ref()) {
            Ok(outcome) => outcome,
            Err(err) => return Some((req_id, Err(err))),
        };
        if let Some(detail) = outcome.application_error {
            return Some((
                req_id,
                Err(anyhow!(
                    "lighter account_tx reported tx failure tx_type={} hash={} detail={}",
                    tx.tx_type.unwrap_or_default(),
                    hash,
                    detail
                )),
            ));
        }
        if !outcome.confirmed {
            return None;
        }
        let batch = pending.get_mut(&req_id)?;
        batch.observed_hashes.insert(hash);
        if batch.expected_hashes.is_empty() {
            return None;
        }
        if batch
            .expected_hashes
            .iter()
            .all(|expected_hash| batch.observed_hashes.contains(expected_hash))
        {
            return Some((
                req_id,
                Ok(SendTxBatchResponse {
                    code: 200,
                    message: Some("confirmed via account_tx".to_string()),
                    tx_hash: batch.expected_hashes.clone(),
                }),
            ));
        }
        None
    }

    fn new(
        cfg: LighterWsConfig,
        signer: SignerHandle,
        command_rx: tokio_mpsc::Receiver<LighterWsCommand>,
        orders: Arc<Mutex<HashMap<ClientOrderId, OrderState>>>,
        pending_reports: Arc<Mutex<Vec<ExecutionReport>>>,
        report_notify: Arc<Notify>,
        size_scale: f64,
    ) -> Self {
        Self {
            cfg,
            signer,
            command_rx,
            orders,
            pending_reports,
            report_notify,
            size_scale,
            request_id: 1,
        }
    }

    fn confirm_order_refs_from_account_tx(&self, tx_type: u8, tx: &LighterTxEntry) -> Result<()> {
        let event_info = Self::parse_account_tx_event_info(tx)?;
        let order_refs = Self::collect_account_tx_order_refs(tx_type, event_info.as_ref());
        if order_refs.is_empty() {
            return Ok(());
        }

        let mut reports = Vec::new();
        {
            let mut orders = self.orders.lock();
            for order_ref in order_refs {
                let Some((id, state)) = orders
                    .iter_mut()
                    .find(|(_, state)| state.client_order_index == order_ref.client_order_index)
                else {
                    continue;
                };
                if state.order_index == Some(order_ref.order_index) {
                    continue;
                }
                let order_index_was_missing = state.order_index.is_none();
                state.order_index = Some(order_ref.order_index);
                if order_index_was_missing && matches!(state.status, OrderStatus::New) {
                    reports.push(ExecutionReport {
                        client_order_id: id.clone(),
                        exchange_order_id: state.exchange_order_id.clone(),
                        status: OrderStatus::New,
                        filled_qty: state.filled,
                        avg_fill_price: Some(state.price),
                        ts: tx.executed_at.map(|value| value as u64),
                    });
                }
            }
        }

        if !reports.is_empty() {
            let mut pending = self.pending_reports.lock();
            pending.extend(reports);
            self.report_notify.notify_one();
        }
        Ok(())
    }

    fn handle_pending_batch_rejection(
        &self,
        req_id: &str,
        mut batch: PendingSendTxBatch,
        err: &anyhow::Error,
    ) {
        eprintln!(
            "WARN: lighter sendTxBatch async rejection req_id={} err={:#} tx_meta={:?}",
            req_id, err, batch.tx_meta
        );
        if let Some(resp) = batch.resp.take() {
            let _ = resp.send(Err(anyhow!(err.to_string())));
        }
        let mut reports = Vec::new();
        {
            let mut orders = self.orders.lock();
            for tx in &batch.tx_meta {
                if tx.tx_type != LIGHTER_TX_TYPE_CREATE_ORDER {
                    continue;
                }
                let Some(state) = orders.get_mut(&tx.client_order_id) else {
                    continue;
                };
                state.status = OrderStatus::Rejected;
                reports.push(ExecutionReport {
                    client_order_id: tx.client_order_id.clone(),
                    exchange_order_id: state.exchange_order_id.clone(),
                    status: OrderStatus::Rejected,
                    filled_qty: state.filled,
                    avg_fill_price: Some(state.price),
                    ts: Some(current_unix_ms() as u64),
                });
            }
        }
        if !reports.is_empty() {
            let mut pending = self.pending_reports.lock();
            pending.extend(reports);
            self.report_notify.notify_one();
        }
    }

    fn validate_sendtx_response_hashes(
        req_id: &str,
        batch: &mut PendingSendTxBatch,
        response: &SendTxBatchResponse,
    ) -> Result<()> {
        let response_hashes = Self::normalize_tx_hashes(&response.tx_hash)?;
        if batch.expected_hashes.is_empty() {
            batch.expected_hashes = response_hashes;
            return Ok(());
        }
        if batch.expected_hashes != response_hashes {
            bail!(
                "sendTxBatch hash mismatch req_id={} expected={:?} actual={:?}",
                req_id,
                batch.expected_hashes,
                response_hashes
            );
        }
        Ok(())
    }

    async fn run(mut self) -> Result<()> {
        let initial_backoff = Duration::from_millis(250);
        let max_backoff = Duration::from_millis(3_000);
        let mut backoff = initial_backoff;

        loop {
            let mut ws = match self.connect_ws().await {
                Ok(ws) => ws,
                Err(err) => {
                    eprintln!("[lighter-ws] connect failed: {:#}", err);
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff.saturating_mul(2)).min(max_backoff);
                    continue;
                }
            };

            if let Err(err) = self.subscribe_private(&mut ws).await {
                eprintln!("[lighter-ws] subscribe failed: {:#}", err);
                tokio::time::sleep(backoff).await;
                backoff = (backoff.saturating_mul(2)).min(max_backoff);
                continue;
            }

            backoff = initial_backoff;
            let shutdown = self.run_session(ws).await?;
            if shutdown {
                return Ok(());
            }
        }
    }

    async fn connect_ws(&self) -> Result<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>> {
        let (mut ws, _) = connect_async_with_config(&self.cfg.ws_url, None, true).await?;
        Self::await_initialization_message(&mut ws, self.cfg.debug_prints).await?;
        Ok(ws)
    }

    async fn await_initialization_message<S>(
        ws: &mut WebSocketStream<S>,
        debug_prints: bool,
    ) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let init = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match ws.next().await {
                    Some(Ok(Message::Ping(payload))) => {
                        ws.send(Message::Pong(payload))
                            .await
                            .context("failed to respond to lighter ws init ping")?;
                    }
                    Some(Ok(Message::Pong(_))) => continue,
                    Some(Ok(Message::Text(text))) => break Ok(text.to_string()),
                    Some(Ok(Message::Binary(data))) => {
                        let text = String::from_utf8(data)
                            .context("lighter ws init binary was not utf8")?;
                        break Ok(text);
                    }
                    Some(Ok(Message::Close(frame))) => {
                        break Err(anyhow!(
                            "lighter ws closed before initialization message: {:?}",
                            frame
                        ));
                    }
                    Some(Err(err)) => {
                        break Err(anyhow!(err).context("lighter ws init read failed"));
                    }
                    None => break Err(anyhow!("lighter ws closed before initialization message")),
                    Some(Ok(other)) => {
                        break Err(anyhow!("unexpected lighter ws init message: {:?}", other));
                    }
                }
            }
        })
        .await
        .context("lighter ws initialization timed out after 5s")??;

        if debug_prints {
            eprintln!("[lighter-ws] init message={}", truncate_for_log(&init, 512));
        }
        Ok(())
    }

    async fn subscribe_private(
        &self,
        ws: &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    ) -> Result<()> {
        let auth = self.auth_token().await?;
        for channel in [
            format!(
                "account_orders/{}/{}",
                self.cfg.market_index, self.cfg.account_index
            ),
            format!("account_tx/{}", self.cfg.account_index),
        ] {
            let sub = json!({
                "type": "subscribe",
                "channel": channel,
                "auth": auth.clone(),
            })
            .to_string();
            ws.send(Message::Text(sub)).await?;
            if self.cfg.debug_prints {
                eprintln!("[lighter-ws] subscribed {}", channel);
            }
        }
        if self.cfg.debug_prints {
            eprintln!("[lighter-ws] private subscriptions ready");
        }
        Ok(())
    }

    async fn auth_token(&self) -> Result<String> {
        let deadline = current_unix_ts() + 10 * 60;
        self.signer
            .auth_token(deadline, self.cfg.api_key_index, self.cfg.account_index)
            .await
    }

    async fn run_session(
        &mut self,
        ws: WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    ) -> Result<bool> {
        let (mut sink, mut stream) = ws.split();
        let mut pending: HashMap<String, PendingSendTxBatch> = HashMap::new();
        let mut pending_by_hash: HashMap<String, String> = HashMap::new();
        let mut heartbeat = tokio::time::interval(Duration::from_secs(20));

        loop {
            tokio::select! {
                cmd = self.command_rx.recv() => {
                    match cmd {
                        Some(LighterWsCommand::SendBatch { txs, resp }) => {
                            let req_id = format!("txb-{}", self.request_id);
                            self.request_id = self.request_id.saturating_add(1);
                            let payload = match Self::build_send_batch_payload(&txs, &req_id) {
                                Ok(payload) => payload,
                                Err(err) => {
                                    let _ = resp.send(Err(err));
                                    continue;
                                }
                            };
                            let expected_hashes = match Self::collect_expected_tx_hashes(&txs) {
                                Ok(hashes) => hashes,
                                Err(err) => {
                                    let _ = resp.send(Err(err));
                                    continue;
                                }
                            };
                            if let Err(err) = sink.send(Message::Text(payload)).await {
                                let _ = resp.send(Err(anyhow!(err)));
                                for (_, resp) in pending.drain() {
                                    eprintln!(
                                        "WARN: lighter ws disconnected with pending batch tx_meta={:?}",
                                        resp.tx_meta
                                    );
                                }
                                return Ok(false);
                            }
                            let tx_meta = txs
                                .iter()
                                .map(|(tx, id)| SignedBatchTxMeta {
                                    tx_type: tx.tx_type,
                                    client_order_id: id.clone(),
                                    tx_hash: tx.tx_hash.clone(),
                                })
                                .collect::<Vec<_>>();
                            let batch = PendingSendTxBatch {
                                tx_meta,
                                expected_hashes,
                                observed_hashes: HashSet::new(),
                                resp: Some(resp),
                            };
                            if let Err(err) = Self::insert_pending_batch(
                                &mut pending,
                                &mut pending_by_hash,
                                req_id,
                                batch,
                            ) {
                                return Err(
                                    err.context("failed to register pending lighter sendTxBatch"),
                                );
                            }
                        }
                        Some(LighterWsCommand::Shutdown) => {
                            for (_, mut resp) in pending.drain() {
                                eprintln!(
                                    "WARN: lighter ws shutdown dropped pending batch tx_meta={:?}",
                                    resp.tx_meta
                                );
                                if let Some(sender) = resp.resp.take() {
                                    let _ = sender.send(Err(anyhow!(
                                        "lighter ws shutdown dropped pending sendTxBatch"
                                    )));
                                }
                            }
                            return Ok(true);
                        }
                        None => {
                            for (_, mut resp) in pending.drain() {
                                eprintln!(
                                    "WARN: lighter ws command channel closed with pending batch tx_meta={:?}",
                                    resp.tx_meta
                                );
                                if let Some(sender) = resp.resp.take() {
                                    let _ = sender.send(Err(anyhow!(
                                        "lighter ws command channel closed with pending sendTxBatch"
                                    )));
                                }
                            }
                            return Ok(true);
                        }
                    }
                }
                msg = stream.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            match serde_json::from_str::<Value>(&text) {
                                Ok(value) => {
                                    self.handle_inbound(value, &mut pending, &mut pending_by_hash, &mut sink).await;
                                }
                                Err(err) => {
                                    eprintln!(
                                        "WARN: failed to parse lighter ws text frame as json: err={:#} raw={}",
                                        err,
                                        truncate_for_log(&text, 512)
                                    );
                                }
                            }
                        }
                        Some(Ok(Message::Binary(data))) => {
                            match String::from_utf8(data) {
                                Ok(text) => {
                                    match serde_json::from_str::<Value>(&text) {
                                        Ok(value) => {
                                            self.handle_inbound(value, &mut pending, &mut pending_by_hash, &mut sink).await;
                                        }
                                        Err(err) => {
                                            eprintln!(
                                                "WARN: failed to parse lighter ws binary frame as json: err={:#} raw={}",
                                                err,
                                                truncate_for_log(&text, 512)
                                            );
                                        }
                                    }
                                }
                                Err(err) => {
                                    eprintln!(
                                        "WARN: failed to decode lighter ws binary frame as utf8: {:#}",
                                        err
                                    );
                                }
                            }
                        }
                        Some(Ok(Message::Ping(payload))) => {
                            let _ = sink.send(Message::Pong(payload)).await;
                        }
                        Some(Ok(Message::Close(_))) => {
                            break;
                        }
                        Some(Err(err)) => {
                            eprintln!("[lighter-ws] stream error: {:#}", err);
                            break;
                        }
                        None => break,
                        _ => {}
                    }
                }
                _ = heartbeat.tick() => {
                    let _ = sink
                        .send(Message::Text(r#"{"type":"ping"}"#.to_string()))
                        .await;
                }
            }
        }

        for (_, mut resp) in pending.drain() {
            eprintln!(
                "WARN: lighter ws disconnected with pending batch tx_meta={:?}",
                resp.tx_meta
            );
            if let Some(sender) = resp.resp.take() {
                let _ = sender.send(Err(anyhow!(
                    "lighter ws disconnected with pending sendTxBatch"
                )));
            }
        }
        Ok(false)
    }

    fn build_send_batch_payload(txs: &[(SignedTx, ClientOrderId)], req_id: &str) -> Result<String> {
        let tx_types = txs.iter().map(|(tx, _)| tx.tx_type).collect::<Vec<_>>();
        let tx_infos = txs
            .iter()
            .map(|(tx, _)| tx.tx_info.clone())
            .collect::<Vec<_>>();
        let tx_types_json = serde_json::to_string(&tx_types)?;
        let tx_infos_json = serde_json::to_string(&tx_infos)?;
        Ok(json!({
            "type": "jsonapi/sendtxbatch",
            "data": {
                "id": req_id,
                "tx_types": tx_types_json,
                "tx_infos": tx_infos_json,
            }
        })
        .to_string())
    }

    fn handle_account_orders(&self, msg: LighterAccountOrdersMsg) {
        let key = self.cfg.market_index.to_string();
        let Some(entries) = msg.orders.get(&key) else {
            return;
        };
        let mut reports = Vec::new();
        {
            let mut guard = self.orders.lock();
            for entry in entries {
                let Some(coi) = entry.client_order_index else {
                    continue;
                };
                let matched = guard
                    .iter_mut()
                    .find(|(_, state)| state.client_order_index == coi);
                let Some((id, state)) = matched else {
                    continue;
                };
                if let Some(price_str) = entry.price.as_ref() {
                    if let Ok(price) = price_str.parse::<f64>() {
                        state.price = price;
                    }
                }
                if let Some(size_str) = entry.initial_base_amount.as_ref() {
                    if let Ok(size_int) = size_str.parse::<f64>() {
                        state.size = size_int / self.size_scale;
                    }
                }
                update_from_entry(self.size_scale, entry, state, id, &mut reports);
                if let Some(status_str) = entry.status.as_deref() {
                    let status = map_status(status_str);
                    if status != state.status {
                        if matches!(
                            status,
                            OrderStatus::Canceled | OrderStatus::Rejected | OrderStatus::Filled
                        ) {
                            state.status = status.clone();
                            reports.push(ExecutionReport {
                                client_order_id: id.clone(),
                                exchange_order_id: state.exchange_order_id.clone(),
                                status,
                                filled_qty: 0.0,
                                avg_fill_price: None,
                                ts: entry.timestamp.map(|v| v as u64),
                            });
                        } else {
                            state.status = status;
                        }
                    }
                }
            }
        }
        if !reports.is_empty() {
            let mut pending = self.pending_reports.lock();
            pending.extend(reports);
            self.report_notify.notify_one();
        }
    }

    fn handle_account_tx(
        &self,
        msg: LighterAccountTxMsg,
        pending: &mut HashMap<String, PendingSendTxBatch>,
        pending_by_hash: &mut HashMap<String, String>,
    ) {
        for tx in msg.txs {
            let tx_type = tx.tx_type.unwrap_or_default();
            if tx_type != LIGHTER_TX_TYPE_CREATE_ORDER && tx_type != LIGHTER_TX_TYPE_CANCEL_ORDER {
                continue;
            }
            if let Err(err) = self.confirm_order_refs_from_account_tx(tx_type, &tx) {
                eprintln!(
                    "WARN: lighter account_tx tx_type={} hash={:?} status={:?} executed_at={:?} failed_to_apply_order_refs={:#}",
                    tx_type, tx.hash, tx.status, tx.executed_at, err
                );
            }
            if let Some((req_id, result)) =
                Self::update_pending_batch_from_account_tx(&tx, pending, pending_by_hash)
            {
                let Some(mut batch) =
                    Self::complete_pending_batch(pending, pending_by_hash, &req_id)
                else {
                    eprintln!(
                        "WARN: lighter account_tx matched pending request {} but it was already removed",
                        req_id
                    );
                    continue;
                };
                match result {
                    Ok(payload) => {
                        if self.cfg.debug_prints {
                            eprintln!(
                                "[lighter-ws] confirmed pending batch {} via account_tx hash={:?}",
                                req_id, tx.hash
                            );
                        }
                        if let Some(resp) = batch.resp.take() {
                            let _ = resp.send(Ok(payload));
                        }
                    }
                    Err(err) => {
                        eprintln!(
                            "WARN: lighter account_tx matched pending request {} but reported failure: {:#}",
                            req_id, err
                        );
                        self.handle_pending_batch_rejection(&req_id, batch, &err);
                    }
                }
            }
            let message = tx.message.as_deref().unwrap_or_default().trim();
            let event_info_raw = tx.event_info.as_deref().unwrap_or_default().trim();
            let event_info = match Self::parse_account_tx_event_info(&tx) {
                Ok(value) => value,
                Err(err) => {
                    eprintln!(
                        "WARN: lighter account_tx tx_type={} hash={:?} status={:?} executed_at={:?} invalid_event_info={} raw_event_info={}",
                        tx_type,
                        tx.hash,
                        tx.status,
                        tx.executed_at,
                        err,
                        truncate_for_log(event_info_raw, 512)
                    );
                    continue;
                }
            };
            let outcome = match Self::parse_account_tx_outcome(&tx, event_info.as_ref()) {
                Ok(outcome) => outcome,
                Err(err) => {
                    eprintln!(
                        "WARN: lighter account_tx tx_type={} hash={:?} status={:?} executed_at={:?} invalid_event_info={} raw_event_info={}",
                        tx_type,
                        tx.hash,
                        tx.status,
                        tx.executed_at,
                        err,
                        truncate_for_log(event_info_raw, 512)
                    );
                    continue;
                }
            };
            let order_refs = Self::collect_account_tx_order_refs(tx_type, event_info.as_ref());
            if let Some(detail) = outcome.application_error.as_deref() {
                eprintln!(
                    "WARN: lighter account_tx tx_type={} hash={:?} status={:?} executed_at={:?} detail={} order_refs={:?} event_info={}",
                    tx_type,
                    tx.hash,
                    tx.status,
                    tx.executed_at,
                    truncate_for_log(detail, 256),
                    order_refs,
                    truncate_for_log(event_info_raw, 512)
                );
                continue;
            }
            if outcome.confirmed && tx_type == LIGHTER_TX_TYPE_CREATE_ORDER && order_refs.is_empty()
            {
                eprintln!(
                    "WARN: lighter account_tx confirmed create without order refs hash={:?} status={:?} executed_at={:?} event_info={}",
                    tx.hash,
                    tx.status,
                    tx.executed_at,
                    truncate_for_log(event_info_raw, 512)
                );
            }
            if !outcome.confirmed {
                eprintln!(
                    "WARN: lighter account_tx tx_type={} hash={:?} status={:?} executed_at={:?} message={} order_refs={:?} event_info={}",
                    tx_type,
                    tx.hash,
                    tx.status,
                    tx.executed_at,
                    truncate_for_log(message, 256),
                    order_refs,
                    truncate_for_log(event_info_raw, 512)
                );
            }
        }
    }

    async fn handle_inbound(
        &self,
        value: Value,
        pending: &mut HashMap<String, PendingSendTxBatch>,
        pending_by_hash: &mut HashMap<String, String>,
        sink: &mut futures_util::stream::SplitSink<
            WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
            Message,
        >,
    ) {
        let msg_type = value
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        if self.cfg.debug_prints && !msg_type.is_empty() {
            eprintln!("[lighter-ws] recv type={}", msg_type);
        }
        if msg_type == "ping" {
            let _ = sink
                .send(Message::Text(r#"{"type":"pong"}"#.to_string()))
                .await;
            return;
        }
        if msg_type == "pong" {
            return;
        }
        if matches!(
            msg_type,
            "update/account_orders" | "update/account_all_orders"
        ) {
            match serde_json::from_value::<LighterAccountOrdersMsg>(value) {
                Ok(msg) => self.handle_account_orders(msg),
                Err(err) => {
                    eprintln!(
                        "WARN: failed to decode lighter account_orders payload: {:#}",
                        err
                    );
                }
            }
            return;
        }
        if matches!(msg_type, "update/account_tx" | "update/account_txs") {
            match serde_json::from_value::<LighterAccountTxMsg>(value) {
                Ok(msg) => self.handle_account_tx(msg, pending, pending_by_hash),
                Err(err) => {
                    eprintln!(
                        "WARN: failed to decode lighter account_tx payload: {:#}",
                        err
                    );
                }
            }
            return;
        }
        if pending.is_empty() {
            return;
        }
        let req_id = Self::extract_sendtx_req_id(&value);
        let looks_like_jsonapi_response = msg_type == "jsonapi/response";
        let looks_like_error_response = value.get("error").is_some() && req_id.is_some();
        if !Self::value_has_sendtx_marker(&value)
            && !looks_like_jsonapi_response
            && !looks_like_error_response
        {
            return;
        }

        let Some((req_id, batch)) =
            Self::remove_pending_batch(pending, pending_by_hash, req_id, self.cfg.debug_prints)
        else {
            return;
        };
        let parsed = if looks_like_error_response {
            Err(
                Self::parse_sendtx_error(&value).unwrap_or_else(|parse_err| {
                    anyhow!("invalid sendTxBatch error frame: {parse_err:#}")
                }),
            )
        } else {
            Self::parse_sendtx_response(&value).and_then(|resp| {
                if resp.code == 200 || resp.code == 0 {
                    Ok(resp)
                } else {
                    Err(anyhow!(
                        "sendTxBatch error {}: {}",
                        resp.code,
                        resp.message.clone().unwrap_or_default()
                    ))
                }
            })
        };
        if let Err(ref err) = parsed {
            eprintln!(
                "WARN: failed to parse lighter sendtx response req_id={} err={} raw={}",
                req_id,
                err,
                truncate_for_log(&value.to_string(), 512)
            );
            self.handle_pending_batch_rejection(&req_id, batch, err);
            return;
        }
        if self.cfg.debug_prints {
            eprintln!(
                "[lighter-ws] acknowledged sendTxBatch req_id={} raw={}",
                req_id,
                truncate_for_log(&value.to_string(), 256)
            );
        }
        let mut batch = batch;
        let payload = parsed.expect("checked success response");
        if let Err(err) = Self::validate_sendtx_response_hashes(&req_id, &mut batch, &payload) {
            self.handle_pending_batch_rejection(&req_id, batch, &err);
            return;
        }
        if let Err(err) = Self::validate_pending_batch_insert(
            pending,
            pending_by_hash,
            &req_id,
            &batch.expected_hashes,
        ) {
            let err = err.context(format!(
                "failed to restore pending lighter sendTxBatch req_id={}",
                req_id
            ));
            self.handle_pending_batch_rejection(&req_id, batch, &err);
            return;
        }
        for hash in &batch.expected_hashes {
            pending_by_hash.insert(hash.clone(), req_id.clone());
        }
        pending.insert(req_id, batch);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CancelRef, ClientOrderId, LIGHTER_TX_TYPE_CANCEL_ORDER, LIGHTER_TX_TYPE_CREATE_ORDER,
        LighterGateway, LighterWsWorker, OrderState, OrderStatus, PendingSendTxBatch, Side,
        SignedTx, batch_observed_with_orders, collect_cancel_targets,
        order_status_is_terminal_for_cancel, parse_tx_lookup_response, reconcile_wait_for_batch,
    };
    use anyhow::anyhow;
    use futures_util::SinkExt;
    use serde_json::json;
    use std::collections::{HashMap, HashSet};
    use std::time::Duration;
    use tokio::io::duplex;
    use tokio_tungstenite::{WebSocketStream, tungstenite::Message, tungstenite::protocol::Role};

    #[test]
    fn parse_sendtx_response_handles_data_attributes_shape() {
        let value = json!({
            "type": "jsonapi/response",
            "data": {
                "id": "txb-7",
                "attributes": {
                    "code": "200",
                    "message": "ok",
                    "txHash": ["0xabc"]
                }
            }
        });
        let parsed = LighterWsWorker::parse_sendtx_response(&value).expect("parse sendtx response");
        assert_eq!(parsed.code, 200);
        assert_eq!(parsed.message.as_deref(), Some("ok"));
        assert_eq!(parsed.tx_hash, vec!["0xabc".to_string()]);
    }

    #[test]
    fn parse_sendtx_error_handles_raw_error_frame() {
        let value = json!({
            "error": {
                "code": 23000,
                "message": "Too Many Requests!:  Not enough volume quota"
            },
            "id": "txb-9"
        });
        let err = LighterWsWorker::parse_sendtx_error(&value).expect("parse sendtx error");
        assert!(
            err.to_string()
                .contains("sendTxBatch error 23000: Too Many Requests!:  Not enough volume quota")
        );
    }

    #[test]
    fn validate_sendtx_response_hashes_rejects_hash_mismatch() {
        let mut batch = PendingSendTxBatch {
            tx_meta: Vec::new(),
            expected_hashes: vec!["0xabc".to_string()],
            observed_hashes: HashSet::new(),
            resp: None,
        };
        let response = super::SendTxBatchResponse {
            code: 200,
            message: Some("ok".to_string()),
            tx_hash: vec!["0xdef".to_string()],
        };
        let err = LighterWsWorker::validate_sendtx_response_hashes("txb-10", &mut batch, &response)
            .expect_err("mismatched hashes must fail loudly");
        assert!(err.to_string().contains("sendTxBatch hash mismatch"));
    }

    #[test]
    fn value_has_sendtx_marker_on_code_and_hash_fields() {
        let value = json!({
            "id": 3,
            "code": 200,
            "tx_hash": ["0x1"]
        });
        assert!(LighterWsWorker::value_has_sendtx_marker(&value));
    }

    #[test]
    fn extract_sendtx_req_id_reads_numeric_id() {
        let value = json!({"id": 42, "code": 200});
        assert_eq!(
            LighterWsWorker::extract_sendtx_req_id(&value).as_deref(),
            Some("42")
        );
    }

    #[test]
    fn collect_account_tx_order_refs_parses_live_create_and_cancel_shapes() {
        let create = super::LighterTxEntry {
            hash: Some("0xcreate".to_string()),
            tx_type: Some(LIGHTER_TX_TYPE_CREATE_ORDER),
            status: Some(2),
            message: None,
            event_info: Some(r#"{"m":7,"t":{"p":0,"s":0,"tf":0,"mf":0},"mo":{"i":0,"u":0,"a":0,"is":0,"p":0,"rs":0,"ia":0,"ot":0,"f":0,"ro":0,"tp":0,"e":0,"st":0,"ts":0,"t0":0,"t1":0,"c0":0},"to":{"i":2251800161206198,"u":1,"a":498195,"is":20,"p":1363280,"rs":20,"ia":1,"ot":0,"f":2,"ro":0,"tp":0,"e":1775286032674,"st":2,"ts":0,"t0":0,"t1":0,"c0":0},"ae":""}"#.to_string()),
            executed_at: Some(1772866832794),
        };
        let parsed =
            LighterWsWorker::parse_account_tx_event_info(&create).expect("parse create event_info");
        let refs = LighterWsWorker::collect_account_tx_order_refs(
            LIGHTER_TX_TYPE_CREATE_ORDER,
            parsed.as_ref(),
        );
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].client_order_index, 1);
        assert_eq!(refs[0].order_index, 2251800161206198);

        let cancel = super::LighterTxEntry {
            hash: Some("0xcancel".to_string()),
            tx_type: Some(LIGHTER_TX_TYPE_CANCEL_ORDER),
            status: Some(2),
            message: None,
            event_info: Some(r#"{"a":498195,"i":2251800161206198,"u":1,"ae":""}"#.to_string()),
            executed_at: Some(1772866833794),
        };
        let parsed =
            LighterWsWorker::parse_account_tx_event_info(&cancel).expect("parse cancel event_info");
        let refs = LighterWsWorker::collect_account_tx_order_refs(
            LIGHTER_TX_TYPE_CANCEL_ORDER,
            parsed.as_ref(),
        );
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].client_order_index, 1);
        assert_eq!(refs[0].order_index, 2251800161206198);
    }

    #[tokio::test]
    async fn pending_batch_completes_from_account_tx_hash() {
        let mut pending = HashMap::new();
        let mut pending_by_hash = HashMap::new();
        LighterWsWorker::insert_pending_batch(
            &mut pending,
            &mut pending_by_hash,
            "txb-1".to_string(),
            PendingSendTxBatch {
                tx_meta: Vec::new(),
                expected_hashes: vec!["0xabc".to_string()],
                observed_hashes: HashSet::new(),
                resp: None,
            },
        )
        .expect("insert pending batch");

        let tx = super::LighterTxEntry {
            hash: Some("0xabc".to_string()),
            tx_type: Some(LIGHTER_TX_TYPE_CREATE_ORDER),
            status: Some(2),
            message: None,
            event_info: Some(r#"{"ae":""}"#.to_string()),
            executed_at: Some(123),
        };

        let (req_id, result) = LighterWsWorker::update_pending_batch_from_account_tx(
            &tx,
            &mut pending,
            &pending_by_hash,
        )
        .expect("matching account_tx should complete batch");
        assert_eq!(req_id, "txb-1");
        let payload = result.expect("success payload");
        assert_eq!(payload.code, 200);
        assert_eq!(payload.message.as_deref(), Some("confirmed via account_tx"));
        assert_eq!(payload.tx_hash, vec!["0xabc".to_string()]);
        let removed =
            LighterWsWorker::complete_pending_batch(&mut pending, &mut pending_by_hash, &req_id)
                .expect("remove pending batch");
        assert_eq!(removed.expected_hashes, vec!["0xabc".to_string()]);
        assert!(pending.is_empty());
        assert!(pending_by_hash.is_empty());
    }

    #[tokio::test]
    async fn pending_batch_fails_from_account_tx_application_error() {
        let mut pending = HashMap::new();
        let mut pending_by_hash = HashMap::new();
        LighterWsWorker::insert_pending_batch(
            &mut pending,
            &mut pending_by_hash,
            "txb-2".to_string(),
            PendingSendTxBatch {
                tx_meta: Vec::new(),
                expected_hashes: vec!["0xdef".to_string()],
                observed_hashes: HashSet::new(),
                resp: None,
            },
        )
        .expect("insert pending batch");

        let tx = super::LighterTxEntry {
            hash: Some("0xdef".to_string()),
            tx_type: Some(LIGHTER_TX_TYPE_CANCEL_ORDER),
            status: Some(2),
            message: None,
            event_info: Some(
                r#"{"ae":"{\"code\":21700,\"message\":\"invalid order index\"}"}"#.to_string(),
            ),
            executed_at: Some(456),
        };

        let (req_id, result) = LighterWsWorker::update_pending_batch_from_account_tx(
            &tx,
            &mut pending,
            &pending_by_hash,
        )
        .expect("matching account_tx should fail batch");
        assert_eq!(req_id, "txb-2");
        let err = result.expect_err("account_tx error should reject batch");
        assert!(err.to_string().contains("invalid order index"));
        let removed =
            LighterWsWorker::complete_pending_batch(&mut pending, &mut pending_by_hash, &req_id)
                .expect("remove pending batch");
        assert_eq!(removed.expected_hashes, vec!["0xdef".to_string()]);
        assert!(pending.is_empty());
        assert!(pending_by_hash.is_empty());
    }

    #[tokio::test]
    async fn pending_batch_waits_for_all_expected_hashes() {
        let mut pending = HashMap::new();
        let mut pending_by_hash = HashMap::new();
        LighterWsWorker::insert_pending_batch(
            &mut pending,
            &mut pending_by_hash,
            "txb-3".to_string(),
            PendingSendTxBatch {
                tx_meta: Vec::new(),
                expected_hashes: vec!["0x111".to_string(), "0x222".to_string()],
                observed_hashes: HashSet::new(),
                resp: None,
            },
        )
        .expect("insert pending batch");

        let first = super::LighterTxEntry {
            hash: Some("0x111".to_string()),
            tx_type: Some(LIGHTER_TX_TYPE_CANCEL_ORDER),
            status: Some(2),
            message: None,
            event_info: Some(r#"{"ae":""}"#.to_string()),
            executed_at: Some(100),
        };
        assert!(
            LighterWsWorker::update_pending_batch_from_account_tx(
                &first,
                &mut pending,
                &pending_by_hash
            )
            .is_none()
        );
        assert_eq!(
            pending.get("txb-3").expect("pending batch").observed_hashes,
            HashSet::from(["0x111".to_string()])
        );

        let second = super::LighterTxEntry {
            hash: Some("0x222".to_string()),
            tx_type: Some(LIGHTER_TX_TYPE_CREATE_ORDER),
            status: Some(2),
            message: None,
            event_info: Some(r#"{"ae":""}"#.to_string()),
            executed_at: Some(101),
        };
        let (req_id, result) = LighterWsWorker::update_pending_batch_from_account_tx(
            &second,
            &mut pending,
            &pending_by_hash,
        )
        .expect("second hash should complete batch");
        assert_eq!(req_id, "txb-3");
        let payload = result.expect("success payload");
        assert_eq!(
            payload.tx_hash,
            vec!["0x111".to_string(), "0x222".to_string()]
        );
        let removed =
            LighterWsWorker::complete_pending_batch(&mut pending, &mut pending_by_hash, &req_id)
                .expect("remove pending batch");
        assert_eq!(
            removed.expected_hashes,
            vec!["0x111".to_string(), "0x222".to_string()]
        );
    }

    #[tokio::test]
    async fn remove_pending_batch_matches_unprefixed_response_id() {
        let mut pending = HashMap::new();
        let mut pending_by_hash = HashMap::new();
        LighterWsWorker::insert_pending_batch(
            &mut pending,
            &mut pending_by_hash,
            "txb-7".to_string(),
            PendingSendTxBatch {
                tx_meta: Vec::new(),
                expected_hashes: vec!["0x777".to_string()],
                observed_hashes: HashSet::new(),
                resp: None,
            },
        )
        .expect("insert pending batch");

        let (req_id, batch) = LighterWsWorker::remove_pending_batch(
            &mut pending,
            &mut pending_by_hash,
            Some("7".to_string()),
            false,
        )
        .expect("response id should match prefixed pending request");
        assert_eq!(req_id, "txb-7");
        assert_eq!(batch.expected_hashes, vec!["0x777".to_string()]);
        assert!(pending.is_empty());
        assert!(pending_by_hash.is_empty());
    }

    #[test]
    fn batch_observed_with_orders_recognizes_create_and_cancel_completion() {
        let create_id = ClientOrderId::new("mf-lighter-b-1");
        let cancel_id = ClientOrderId::new("mf-lighter-s-2");

        let mut orders = HashMap::new();
        orders.insert(
            create_id.clone(),
            OrderState {
                client_order_index: 1,
                order_index: Some(1001),
                side: Side::Bid,
                price: 100.0,
                size: 1.0,
                filled: 0.0,
                status: OrderStatus::New,
                exchange_order_id: None,
            },
        );
        orders.insert(
            cancel_id.clone(),
            OrderState {
                client_order_index: 2,
                order_index: Some(1002),
                side: Side::Ask,
                price: 101.0,
                size: 1.0,
                filled: 0.0,
                status: OrderStatus::Canceled,
                exchange_order_id: None,
            },
        );

        let tx_meta = vec![
            (LIGHTER_TX_TYPE_CREATE_ORDER, create_id),
            (LIGHTER_TX_TYPE_CANCEL_ORDER, cancel_id),
        ];
        assert!(batch_observed_with_orders(&orders, &tx_meta));
    }

    #[test]
    fn batch_observed_with_orders_rejects_unconfirmed_create() {
        let create_id = ClientOrderId::new("mf-lighter-b-3");
        let mut orders = HashMap::new();
        orders.insert(
            create_id.clone(),
            OrderState {
                client_order_index: 3,
                order_index: None,
                side: Side::Bid,
                price: 100.0,
                size: 1.0,
                filled: 0.0,
                status: OrderStatus::New,
                exchange_order_id: None,
            },
        );
        let tx_meta = vec![(LIGHTER_TX_TYPE_CREATE_ORDER, create_id)];
        assert!(!batch_observed_with_orders(&orders, &tx_meta));
    }

    #[test]
    fn batch_observed_with_orders_rejects_non_terminal_cancel() {
        let cancel_id = ClientOrderId::new("mf-lighter-c-3");
        let mut orders = HashMap::new();
        orders.insert(
            cancel_id.clone(),
            OrderState {
                client_order_index: 4,
                order_index: Some(2004),
                side: Side::Ask,
                price: 101.0,
                size: 1.0,
                filled: 0.0,
                status: OrderStatus::New,
                exchange_order_id: None,
            },
        );

        let tx_meta = vec![(LIGHTER_TX_TYPE_CANCEL_ORDER, cancel_id)];
        assert!(!batch_observed_with_orders(&orders, &tx_meta));
    }

    #[test]
    fn collect_cancel_targets_prefers_confirmed_exchange_order_index() {
        let cancel_id = ClientOrderId::new("mf-lighter-s-9");
        let mut orders = HashMap::new();
        orders.insert(
            cancel_id.clone(),
            OrderState {
                client_order_index: 9,
                order_index: Some(1009),
                side: Side::Ask,
                price: 101.0,
                size: 1.0,
                filled: 0.0,
                status: OrderStatus::New,
                exchange_order_id: None,
            },
        );

        let targets = collect_cancel_targets(&orders, &[cancel_id], true)
            .expect("confirmed order_index should be used");
        assert_eq!(
            targets,
            vec![(
                ClientOrderId::new("mf-lighter-s-9"),
                CancelRef::ExchangeOrderIndex(1009)
            )]
        );
    }

    #[test]
    fn collect_cancel_targets_falls_back_to_client_order_index_for_open_order() {
        let cancel_id = ClientOrderId::new("mf-lighter-s-10");
        let mut orders = HashMap::new();
        orders.insert(
            cancel_id.clone(),
            OrderState {
                client_order_index: 10,
                order_index: None,
                side: Side::Ask,
                price: 101.0,
                size: 1.0,
                filled: 0.0,
                status: OrderStatus::New,
                exchange_order_id: None,
            },
        );

        let targets = collect_cancel_targets(&orders, &[cancel_id], true)
            .expect("open order should fall back to client_order_index");
        assert_eq!(
            targets,
            vec![(
                ClientOrderId::new("mf-lighter-s-10"),
                CancelRef::ClientOrderIndex(10)
            )]
        );
    }

    #[test]
    fn collect_cancel_targets_rejects_terminal_order_without_confirmed_index() {
        let cancel_id = ClientOrderId::new("mf-lighter-s-11");
        let mut orders = HashMap::new();
        orders.insert(
            cancel_id.clone(),
            OrderState {
                client_order_index: 11,
                order_index: None,
                side: Side::Ask,
                price: 101.0,
                size: 1.0,
                filled: 1.0,
                status: OrderStatus::Filled,
                exchange_order_id: None,
            },
        );

        let err = collect_cancel_targets(&orders, &[cancel_id], true)
            .expect_err("terminal order should fail loudly");
        assert!(
            err.to_string()
                .contains("refusing client_order_index fallback")
        );
    }

    #[test]
    fn order_status_is_terminal_for_cancel_only_for_terminal_statuses() {
        assert!(!order_status_is_terminal_for_cancel(&OrderStatus::New));
        assert!(!order_status_is_terminal_for_cancel(
            &OrderStatus::PartiallyFilled
        ));
        assert!(order_status_is_terminal_for_cancel(&OrderStatus::Canceled));
        assert!(order_status_is_terminal_for_cancel(&OrderStatus::Filled));
        assert!(order_status_is_terminal_for_cancel(&OrderStatus::Rejected));
    }

    #[test]
    fn is_nonce_consuming_failure_matches_confirmed_account_tx_failure() {
        let err = anyhow!(
            "lighter account_tx reported tx failure tx_type=15 hash=0xabc detail=invalid order index"
        );
        assert!(LighterGateway::is_nonce_consuming_failure(&err));
        let err = anyhow!("sendTxBatch error 21104: invalid nonce");
        assert!(!LighterGateway::is_nonce_consuming_failure(&err));
    }

    #[test]
    fn update_from_entry_emits_new_when_order_index_is_first_confirmed() {
        let id = ClientOrderId::new("mf-lighter-b-10");
        let mut state = OrderState {
            client_order_index: 10,
            order_index: None,
            side: Side::Bid,
            price: 100.0,
            size: 1.0,
            filled: 0.0,
            status: OrderStatus::New,
            exchange_order_id: None,
        };
        let entry = super::LighterOrderEntry {
            order_index: Some(12345),
            client_order_index: Some(10),
            market_index: Some(1),
            price: Some("100".to_string()),
            initial_base_amount: Some("1000".to_string()),
            remaining_base_amount: Some("1000".to_string()),
            filled_base_amount: Some("0".to_string()),
            is_ask: Some(false),
            status: Some("open".to_string()),
            timestamp: Some(1),
        };
        let mut reports = Vec::new();

        super::update_from_entry(1000.0, &entry, &mut state, &id, &mut reports);

        assert_eq!(state.order_index, Some(12345));
        assert_eq!(reports.len(), 1);
        assert!(matches!(reports[0].status, OrderStatus::New));
    }

    #[test]
    fn reconcile_wait_for_batch_extends_when_cancel_present() {
        let tx_meta_with_cancel = vec![(
            LIGHTER_TX_TYPE_CANCEL_ORDER,
            ClientOrderId::new("mf-lighter-c-1"),
        )];
        let tx_meta_create_only = vec![(
            LIGHTER_TX_TYPE_CREATE_ORDER,
            ClientOrderId::new("mf-lighter-n-1"),
        )];
        assert_eq!(
            reconcile_wait_for_batch(&tx_meta_with_cancel),
            Duration::from_secs(20)
        );
        assert_eq!(
            reconcile_wait_for_batch(&tx_meta_create_only),
            Duration::from_secs(8)
        );
    }

    #[test]
    fn map_status_supports_common_terminal_aliases() {
        assert_eq!(super::map_status("cancelled"), OrderStatus::Canceled);
        assert_eq!(
            super::map_status("canceled-post-only"),
            OrderStatus::Canceled
        );
        assert_eq!(super::map_status("closed"), OrderStatus::Canceled);
        assert_eq!(super::map_status("rejected"), OrderStatus::Rejected);
        assert_eq!(
            super::map_status("partially_filled"),
            OrderStatus::PartiallyFilled
        );
    }

    #[test]
    fn parse_tx_lookup_response_accepts_valid_payload() {
        let body = r#"{
            "code": 200,
            "hash": "0xabc",
            "type": 15,
            "status": 2,
            "nonce": 77,
            "queued_at": 101,
            "executed_at": 202,
            "event_info": "{\"ok\":true}"
        }"#;
        let parsed = parse_tx_lookup_response(body, "0xabc").expect("tx lookup should parse");
        assert_eq!(parsed.hash, "0xabc");
        assert_eq!(parsed.tx_type, 15);
        assert_eq!(parsed.status, 2);
        assert_eq!(parsed.nonce, 77);
    }

    #[test]
    fn parse_tx_lookup_response_rejects_empty_hash() {
        let body = r#"{
            "code": 200,
            "hash": "",
            "type": 15,
            "status": 0,
            "nonce": 0,
            "queued_at": 0,
            "executed_at": 0,
            "event_info": ""
        }"#;
        let err = parse_tx_lookup_response(body, "0xdead").expect_err("empty hash must fail");
        assert!(err.to_string().contains("missing hash"));
    }

    #[test]
    fn collect_expected_tx_hashes_rejects_duplicate_hashes() {
        let txs = vec![
            (
                SignedTx {
                    tx_type: LIGHTER_TX_TYPE_CREATE_ORDER,
                    tx_info: "a".to_string(),
                    tx_hash: Some("0xdup".to_string()),
                },
                ClientOrderId::new("mf-lighter-a"),
            ),
            (
                SignedTx {
                    tx_type: LIGHTER_TX_TYPE_CANCEL_ORDER,
                    tx_info: "b".to_string(),
                    tx_hash: Some("0xdup".to_string()),
                },
                ClientOrderId::new("mf-lighter-b"),
            ),
        ];

        let err = LighterWsWorker::collect_expected_tx_hashes(&txs)
            .expect_err("duplicate tx hashes must fail");
        assert!(err.to_string().contains("duplicate signer tx hash"));
    }

    #[tokio::test]
    async fn await_initialization_message_accepts_first_server_frame() {
        let (client_io, server_io) = duplex(1024);
        let mut client_ws = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;

        let server = tokio::spawn(async move {
            let mut ws = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
            ws.send(Message::Text(r#"{"type":"welcome"}"#.into()))
                .await
                .expect("send init frame");
            ws.close(None).await.expect("close websocket");
        });

        LighterWsWorker::await_initialization_message(&mut client_ws, false)
            .await
            .expect("initialization frame should be accepted");

        server.await.expect("server task");
    }
}

struct LighterResyncWorker {
    rest: LighterRestClient,
    size_scale: f64,
    orders: Arc<Mutex<HashMap<ClientOrderId, OrderState>>>,
    pending_reports: Arc<Mutex<Vec<ExecutionReport>>>,
    report_notify: Arc<Notify>,
    interval: Duration,
}

impl LighterResyncWorker {
    async fn run(self) {
        let mut ticker = tokio::time::interval(self.interval);
        if let Err(err) = self.resync_once().await {
            eprintln!("[lighter-resync] initial sync failed: {:#}", err);
        }
        loop {
            ticker.tick().await;
            if let Err(err) = self.resync_once().await {
                eprintln!("[lighter-resync] sync failed: {:#}", err);
            }
        }
    }

    async fn resync_once(&self) -> Result<()> {
        let active = self.rest.fetch_active_orders().await?;
        let mut seen = HashMap::new();
        for entry in active.iter() {
            if let Some(coi) = entry.client_order_index {
                seen.insert(coi, entry);
            }
        }

        let mut missing_for_inactive = Vec::new();
        let mut reports = Vec::new();
        {
            let mut guard = self.orders.lock();
            for (id, state) in guard.iter_mut() {
                if let Some(entry) = seen.get(&state.client_order_index) {
                    update_from_entry(self.size_scale, entry, state, id, &mut reports);
                } else if matches!(
                    state.status,
                    OrderStatus::New | OrderStatus::PartiallyFilled
                ) {
                    missing_for_inactive.push(id.clone());
                }
            }
        }

        if !missing_for_inactive.is_empty() {
            if let Ok(inactive) = self.rest.fetch_inactive_orders(50).await {
                let mut guard = self.orders.lock();
                for entry in inactive {
                    if let Some(coi) = entry.client_order_index {
                        if let Some((id, state)) = guard.iter_mut().find(|(id, st)| {
                            missing_for_inactive.contains(id) && st.client_order_index == coi
                        }) {
                            if let Some(price_str) = entry.price.as_ref() {
                                if let Ok(price) = price_str.parse::<f64>() {
                                    state.price = price;
                                }
                            }
                            if let Some(size_str) = entry.initial_base_amount.as_ref() {
                                if let Ok(size_int) = size_str.parse::<f64>() {
                                    state.size = size_int / self.size_scale;
                                }
                            }
                            update_from_entry(self.size_scale, &entry, state, id, &mut reports);
                            let mut status = entry
                                .status
                                .as_deref()
                                .map(map_status)
                                .unwrap_or(OrderStatus::Unknown);
                            if status == OrderStatus::Unknown {
                                if let Some(raw) = entry.status.as_deref() {
                                    eprintln!(
                                        "WARN: unknown inactive order status '{}'; treating as canceled",
                                        raw
                                    );
                                }
                                status = OrderStatus::Canceled;
                            }
                            if status != state.status {
                                if matches!(
                                    status,
                                    OrderStatus::Canceled
                                        | OrderStatus::Rejected
                                        | OrderStatus::Filled
                                ) {
                                    state.status = status.clone();
                                    reports.push(ExecutionReport {
                                        client_order_id: id.clone(),
                                        exchange_order_id: state.exchange_order_id.clone(),
                                        status,
                                        filled_qty: 0.0,
                                        avg_fill_price: None,
                                        ts: entry.timestamp.map(|v| v as u64),
                                    });
                                } else {
                                    state.status = status;
                                }
                            }
                        }
                    }
                }
            }
        }

        if !reports.is_empty() {
            let mut pending = self.pending_reports.lock();
            pending.extend(reports);
            self.report_notify.notify_one();
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct LighterAuthClient {
    signer: SignerHandle,
    creds: LighterCredentials,
}

impl LighterAuthClient {
    pub async fn connect(creds: LighterCredentials, debug_prints: bool) -> Result<Self> {
        let signer = SignerHandle::new(creds.signer_lib.clone(), debug_prints)?;
        if debug_prints {
            eprintln!(
                "[lighter-sign] init signer base_url={} api_key_idx={} account_idx={}",
                creds.base_url, creds.api_key_index, creds.account_index
            );
        }
        signer
            .init_client(
                creds.base_url.clone(),
                creds.api_key_hex.clone(),
                creds.chain_id.unwrap_or(304),
                creds.api_key_index,
                creds.account_index,
            )
            .await?;
        Ok(Self { signer, creds })
    }

    pub async fn auth_token(&self) -> Result<String> {
        let deadline = current_unix_ts() + 10 * 60;
        self.signer
            .auth_token(deadline, self.creds.api_key_index, self.creds.account_index)
            .await
    }
}

pub struct LighterGateway {
    signer: SignerHandle,
    creds: LighterCredentials,
    market_index: u8,
    price_scale: f64,
    size_scale: f64,
    debug_prints: bool,
    rest: LighterRestClient,
    ws_tx: tokio_mpsc::Sender<LighterWsCommand>,
    next_client_index: Mutex<i64>,
    next_nonce: Mutex<Option<i64>>,
    nonce_lock: AsyncMutex<()>,
    pending_reports: Arc<Mutex<Vec<ExecutionReport>>>,
    orders: Arc<Mutex<HashMap<ClientOrderId, OrderState>>>,
    report_notify: Arc<Notify>,
    last_api_call: Arc<Mutex<Option<Instant>>>,
}

impl LighterGateway {
    pub async fn connect(
        creds: LighterCredentials,
        market_index: u32,
        price_decimals: u32,
        size_decimals: u32,
        debug_prints: bool,
    ) -> Result<Self> {
        let signer = SignerHandle::new(creds.signer_lib.clone(), debug_prints)?;
        let base_url = creds.base_url.clone();
        // Initialize signer client on its dedicated thread before use
        if debug_prints {
            eprintln!(
                "[lighter-sign] init signer base_url={} api_key_idx={} account_idx={}",
                base_url, creds.api_key_index, creds.account_index
            );
        }
        signer
            .init_client(
                base_url.clone(),
                creds.api_key_hex.clone(),
                creds.chain_id.unwrap_or(304),
                creds.api_key_index,
                creds.account_index,
            )
            .await?;

        let http = Client::builder().timeout(Duration::from_secs(10)).build()?;
        let api_base = Url::parse(&base_url)?;
        let price_scale = 10_f64.powi(price_decimals as i32);
        let size_scale = 10_f64.powi(size_decimals as i32);
        let pending_reports = Arc::new(Mutex::new(Vec::new()));
        let orders = Arc::new(Mutex::new(HashMap::new()));
        let report_notify = Arc::new(Notify::new());
        let last_api_call = Arc::new(Mutex::new(None));
        let rest = LighterRestClient::new(
            signer.clone(),
            creds.clone(),
            market_index as u8,
            http.clone(),
            api_base.clone(),
            debug_prints,
            last_api_call.clone(),
        );
        let ws_url = ws_url_from_base(&base_url)?;
        let (ws_tx, ws_rx) = tokio_mpsc::channel(128);

        let ws_worker = LighterWsWorker::new(
            LighterWsConfig {
                ws_url,
                account_index: creds.account_index,
                api_key_index: creds.api_key_index,
                market_index: market_index as u8,
                debug_prints,
            },
            signer.clone(),
            ws_rx,
            orders.clone(),
            pending_reports.clone(),
            report_notify.clone(),
            size_scale,
        );
        tokio::spawn(async move {
            if let Err(err) = ws_worker.run().await {
                eprintln!("[lighter-ws] worker terminated: {:#}", err);
            }
        });

        let resync_worker = LighterResyncWorker {
            rest: rest.clone(),
            size_scale,
            orders: orders.clone(),
            pending_reports: pending_reports.clone(),
            report_notify: report_notify.clone(),
            interval: Duration::from_secs(20),
        };
        tokio::spawn(async move {
            resync_worker.run().await;
        });

        let gw = Self {
            signer: signer,
            creds,
            market_index: market_index as u8,
            price_scale,
            size_scale,
            debug_prints,
            rest,
            ws_tx,
            next_client_index: Mutex::new(1),
            next_nonce: Mutex::new(None),
            nonce_lock: AsyncMutex::new(()),
            pending_reports,
            orders,
            report_notify,
            last_api_call,
        };
        // Seed nonces once at startup to avoid hot-looping nextNonce under load.
        // If the endpoint is temporarily rate-limited, this will back off and retry.
        let _ = gw.ensure_nonce_seed().await?;
        Ok(gw)
    }

    fn client_order_index(&self) -> i64 {
        let mut guard = self.next_client_index.lock();
        let idx = *guard;
        *guard = guard.saturating_add(1);
        idx
    }

    fn validate_intents(&self, intents: &[QuoteIntent]) -> Result<()> {
        for intent in intents {
            if intent.venue != Venue::Lighter {
                bail!(
                    "lighter gateway received non-lighter intent client_order_id={} venue={:?} symbol={}",
                    intent.client_order_id,
                    intent.venue,
                    intent.symbol
                );
            }
        }
        Ok(())
    }

    async fn ensure_nonce_seed(&self) -> Result<i64> {
        if let Some(n) = *self.next_nonce.lock() {
            return Ok(n);
        }
        let fresh = self.rest.fetch_nonce_with_backoff("seed").await?;
        eprintln!("[lighter-nonce] seeded nonce={}", fresh);
        let mut guard = self.next_nonce.lock();
        *guard = Some(fresh);
        Ok(fresh)
    }

    async fn refresh_nonce_from_server(&self) -> Result<i64> {
        let fresh = self.rest.fetch_nonce_with_backoff("refresh").await?;
        eprintln!("[lighter-nonce] refreshed nonce={}", fresh);
        let mut guard = self.next_nonce.lock();
        *guard = Some(fresh);
        Ok(fresh)
    }

    async fn peek_nonces(&self, count: usize) -> Result<(i64, Vec<i64>)> {
        if count == 0 {
            return Ok((0, Vec::new()));
        }
        let _ = self.ensure_nonce_seed().await?;
        let guard = self.next_nonce.lock();
        let start = guard.expect("nonce seed must be set");
        let mut nonces = Vec::with_capacity(count);
        for i in 0..count {
            nonces.push(start + i as i64);
        }
        Ok((start, nonces))
    }

    fn commit_nonces(&self, start: i64, count: usize) {
        if count == 0 {
            return;
        }
        let mut guard = self.next_nonce.lock();
        *guard = Some(start + count as i64);
    }

    fn to_price_int(&self, px: f64) -> Result<u32> {
        if !px.is_finite() || px <= 0.0 {
            bail!("invalid price {px}");
        }
        let scaled = (px * self.price_scale).round();
        if scaled > u32::MAX as f64 {
            bail!("price too large after scaling");
        }
        Ok(scaled as u32)
    }

    fn to_size_int(&self, size: f64) -> Result<i64> {
        if !size.is_finite() || size <= 0.0 {
            bail!("invalid size {size}");
        }
        Ok((size * self.size_scale).round() as i64)
    }

    fn signed_batch_meta(&self, txs: &[(SignedTx, ClientOrderId)]) -> Vec<SignedBatchTxMeta> {
        txs.iter()
            .map(|(tx, id)| SignedBatchTxMeta {
                tx_type: tx.tx_type,
                client_order_id: id.clone(),
                tx_hash: tx.tx_hash.clone(),
            })
            .collect()
    }

    async fn log_tx_hash_diagnostics(&self, reason: &str, txs: &[SignedBatchTxMeta]) {
        let lookup_targets = txs
            .iter()
            .filter_map(|tx| {
                tx.tx_hash
                    .as_ref()
                    .map(|hash| (tx.tx_type, tx.client_order_id.0.clone(), hash.to_string()))
            })
            .collect::<Vec<_>>();
        if lookup_targets.is_empty() {
            eprintln!(
                "WARN: lighter tx diagnostics unavailable for {}: signer returned no tx hashes",
                reason
            );
            return;
        }

        for (tx_type, client_order_id, hash) in lookup_targets {
            match self.rest.fetch_tx_by_hash(&hash).await {
                Ok(tx) => {
                    eprintln!(
                        "WARN: lighter tx diagnostic reason={} client_order_id={} tx_type={} hash={} lookup_type={} status={} nonce={} queued_at={} executed_at={} event_info={}",
                        reason,
                        client_order_id,
                        tx_type,
                        hash,
                        tx.tx_type,
                        tx.status,
                        tx.nonce,
                        tx.queued_at,
                        tx.executed_at,
                        truncate_for_log(&tx.event_info, 512)
                    );
                }
                Err(err) => {
                    eprintln!(
                        "WARN: lighter tx diagnostic lookup failed reason={} client_order_id={} tx_type={} hash={} err={:#}",
                        reason, client_order_id, tx_type, hash, err
                    );
                }
            }
        }
    }

    fn tracked_order_tx_meta(&self, ids: &[ClientOrderId], tx_type: u8) -> Vec<SignedBatchTxMeta> {
        let orders = self.orders.lock();
        ids.iter()
            .filter_map(|id| {
                let state = orders.get(id)?;
                let hash = state.exchange_order_id.as_ref()?.0.trim();
                if hash.is_empty() {
                    return None;
                }
                Some(SignedBatchTxMeta {
                    tx_type,
                    client_order_id: id.clone(),
                    tx_hash: Some(hash.to_string()),
                })
            })
            .collect()
    }

    fn split_terminal_cancel_targets(
        &self,
        ids: &[ClientOrderId],
    ) -> Result<(Vec<ClientOrderId>, Vec<SkippedCancelTarget>)> {
        let orders = self.orders.lock();
        let mut active = Vec::with_capacity(ids.len());
        let mut skipped = Vec::new();
        for id in ids {
            let state = orders
                .get(id)
                .ok_or_else(|| anyhow!("unknown order {}", id.0))?;
            if order_status_is_terminal_for_cancel(&state.status) {
                skipped.push(SkippedCancelTarget {
                    client_order_id: id.clone(),
                    status: state.status.clone(),
                    order_index: state.order_index,
                    client_order_index: state.client_order_index,
                    filled: state.filled,
                });
                continue;
            }
            active.push(id.clone());
        }
        Ok((active, skipped))
    }

    fn log_skipped_terminal_cancel_targets(&self, reason: &str, skipped: &[SkippedCancelTarget]) {
        if skipped.is_empty() {
            return;
        }
        eprintln!(
            "WARN: skipping terminal lighter cancel targets reason={} skipped={:?}",
            reason,
            skipped
                .iter()
                .map(|target| (
                    target.client_order_id.0.as_str(),
                    &target.status,
                    target.order_index,
                    target.client_order_index,
                    target.filled
                ))
                .collect::<Vec<_>>()
        );
    }

    async fn send_batch(&self, txs: Vec<(SignedTx, ClientOrderId)>) -> Result<Vec<OrderAck>> {
        if txs.is_empty() {
            return Ok(Vec::new());
        }
        let tx_meta = self.signed_batch_meta(&txs);
        let reconcile_meta = tx_meta
            .iter()
            .map(|tx| (tx.tx_type, tx.client_order_id.clone()))
            .collect::<Vec<_>>();
        let fallback_hashes = txs
            .iter()
            .map(|(tx, _)| tx.tx_hash.as_ref().cloned())
            .collect::<Vec<_>>();
        let client_ids = txs.iter().map(|(_, id)| id.clone()).collect::<Vec<_>>();

        let (resp_tx, resp_rx) = oneshot::channel();
        self.ws_tx
            .send(LighterWsCommand::SendBatch { txs, resp: resp_tx })
            .await
            .map_err(|e| anyhow!("lighter ws send queue closed: {e}"))?;
        let payload = match tokio::time::timeout(Duration::from_secs(5), resp_rx).await {
            Ok(resp) => resp.context("lighter ws local sendTxBatch response dropped")??,
            Err(_) => {
                let reconcile_wait = reconcile_wait_for_batch(&reconcile_meta);
                eprintln!(
                    "WARN: lighter sendTxBatch confirmation timeout after 5s; reconciling {} tx(s) via order-state sync for up to {}s",
                    reconcile_meta.len(),
                    reconcile_wait.as_secs()
                );
                self.reconcile_after_send_timeout(&reconcile_meta, reconcile_wait)
                    .await
                    .context("lighter sendTxBatch confirmation timeout")?;
                eprintln!(
                    "WARN: lighter sendTxBatch confirmation missing on ws but reconciliation confirmed batch outcome"
                );
                return Ok(self.build_acks_from_hashes(&client_ids, &fallback_hashes, None));
            }
        };
        Ok(self.build_acks_from_hashes(&client_ids, &fallback_hashes, Some(&payload.tx_hash)))
    }

    fn build_acks_from_hashes(
        &self,
        client_ids: &[ClientOrderId],
        fallback_hashes: &[Option<String>],
        response_hashes: Option<&[String]>,
    ) -> Vec<OrderAck> {
        let mut acks = Vec::with_capacity(client_ids.len());
        for (idx, client_id) in client_ids.iter().cloned().enumerate() {
            let exch = response_hashes
                .and_then(|h| h.get(idx))
                .or(fallback_hashes.get(idx).and_then(|h| h.as_ref()))
                .cloned()
                .map(ExchangeOrderId);
            acks.push(OrderAck {
                client_order_id: client_id,
                exchange_order_id: exch,
            });
        }
        acks
    }

    fn batch_observed(&self, tx_meta: &[(u8, ClientOrderId)]) -> bool {
        let orders = self.orders.lock();
        batch_observed_with_orders(&orders, tx_meta)
    }

    async fn reconcile_after_send_timeout(
        &self,
        tx_meta: &[(u8, ClientOrderId)],
        max_wait: Duration,
    ) -> Result<()> {
        let start = Instant::now();
        let mut backoff_ms = 200u64;
        while start.elapsed() < max_wait {
            self.reconcile_orders_once().await?;
            if self.batch_observed(tx_meta) {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            backoff_ms = (backoff_ms.saturating_mul(2)).min(1_500);
        }

        let snapshot = {
            let orders = self.orders.lock();
            tx_meta
                .iter()
                .map(|(tx_type, id)| {
                    let state = orders.get(id);
                    (
                        *tx_type,
                        id.0.clone(),
                        state.and_then(|s| s.order_index),
                        state.map(|s| s.status.clone()),
                        state.map(|s| s.filled),
                    )
                })
                .collect::<Vec<_>>()
        };
        bail!(
            "sendTxBatch timeout: reconciliation did not confirm batch within {}ms; order may still be live; snapshot={:?}",
            max_wait.as_millis(),
            snapshot
        )
    }

    async fn reconcile_orders_once(&self) -> Result<()> {
        let active = self.rest.fetch_active_orders().await?;
        let mut seen = HashMap::new();
        for entry in active.iter() {
            if let Some(coi) = entry.client_order_index {
                seen.insert(coi, entry);
            }
        }

        let mut missing_for_inactive = Vec::new();
        let mut reports = Vec::new();
        {
            let mut guard = self.orders.lock();
            for (id, state) in guard.iter_mut() {
                if let Some(entry) = seen.get(&state.client_order_index) {
                    update_from_entry(self.size_scale, entry, state, id, &mut reports);
                } else if matches!(
                    state.status,
                    OrderStatus::New | OrderStatus::PartiallyFilled
                ) {
                    missing_for_inactive.push(id.clone());
                }
            }
        }

        if !missing_for_inactive.is_empty() {
            let inactive = self.rest.fetch_inactive_orders(50).await?;
            let mut guard = self.orders.lock();
            for entry in inactive {
                if let Some(coi) = entry.client_order_index {
                    if let Some((id, state)) = guard.iter_mut().find(|(id, st)| {
                        missing_for_inactive.contains(id) && st.client_order_index == coi
                    }) {
                        if let Some(price_str) = entry.price.as_ref() {
                            if let Ok(price) = price_str.parse::<f64>() {
                                state.price = price;
                            }
                        }
                        if let Some(size_str) = entry.initial_base_amount.as_ref() {
                            if let Ok(size_int) = size_str.parse::<f64>() {
                                state.size = size_int / self.size_scale;
                            }
                        }
                        update_from_entry(self.size_scale, &entry, state, id, &mut reports);
                        let mut status = entry
                            .status
                            .as_deref()
                            .map(map_status)
                            .unwrap_or(OrderStatus::Unknown);
                        if status == OrderStatus::Unknown {
                            if let Some(raw) = entry.status.as_deref() {
                                eprintln!(
                                    "WARN: unknown inactive order status '{}'; treating as canceled",
                                    raw
                                );
                            }
                            status = OrderStatus::Canceled;
                        }
                        if status != state.status {
                            if matches!(
                                status,
                                OrderStatus::Canceled | OrderStatus::Rejected | OrderStatus::Filled
                            ) {
                                state.status = status.clone();
                                reports.push(ExecutionReport {
                                    client_order_id: id.clone(),
                                    exchange_order_id: state.exchange_order_id.clone(),
                                    status,
                                    filled_qty: 0.0,
                                    avg_fill_price: None,
                                    ts: entry.timestamp.map(|v| v as u64),
                                });
                            } else {
                                state.status = status;
                            }
                        }
                    }
                }
            }
        }

        if !reports.is_empty() {
            let mut pending = self.pending_reports.lock();
            pending.extend(reports);
            self.report_notify.notify_one();
        }
        Ok(())
    }

    fn is_nonce_error(err: &anyhow::Error) -> bool {
        let msg = err.to_string();
        msg.contains("invalid nonce") || msg.contains("nonce is not increasing")
    }

    fn is_definitive_send_rejection(err: &anyhow::Error) -> bool {
        let msg = err.to_string();
        Self::is_nonce_error(err)
            || msg.contains("sendTxBatch error")
            || msg.contains("lighter ws send queue closed")
    }

    fn is_nonce_consuming_failure(err: &anyhow::Error) -> bool {
        err.to_string()
            .contains("lighter account_tx reported tx failure")
    }

    fn save_order(
        &self,
        intent: &QuoteIntent,
        client_order_index: i64,
        exchange_order_id: Option<ExchangeOrderId>,
    ) {
        let mut orders = self.orders.lock();
        orders.insert(
            intent.client_order_id.clone(),
            OrderState {
                client_order_index,
                order_index: None,
                side: intent.side,
                price: intent.price,
                size: intent.size,
                filled: 0.0,
                status: OrderStatus::New,
                exchange_order_id,
            },
        );
    }

    fn push_report(&self, report: ExecutionReport) {
        let mut guard = self.pending_reports.lock();
        guard.push(report);
        self.report_notify.notify_one();
    }

    fn drop_orders(&self, ids: &[ClientOrderId]) {
        if ids.is_empty() {
            return;
        }
        let mut orders = self.orders.lock();
        for id in ids {
            orders.remove(id);
        }
    }

    fn cancel_target_snapshot(
        &self,
        ids: &[ClientOrderId],
    ) -> Vec<(
        String,
        Option<i64>,
        Option<i64>,
        Option<OrderStatus>,
        Option<f64>,
    )> {
        let orders = self.orders.lock();
        ids.iter()
            .map(|id| {
                let state = orders.get(id);
                (
                    id.0.clone(),
                    state.and_then(|s| s.order_index),
                    state.map(|s| s.client_order_index),
                    state.map(|s| s.status.clone()),
                    state.map(|s| s.filled),
                )
            })
            .collect()
    }

    async fn resolve_cancel_targets(
        &self,
        ids: &[ClientOrderId],
    ) -> Result<Vec<(ClientOrderId, CancelRef)>> {
        let (active_ids, skipped_terminal) = self.split_terminal_cancel_targets(ids)?;
        self.log_skipped_terminal_cancel_targets("already_terminal", &skipped_terminal);
        if active_ids.is_empty() {
            return Ok(Vec::new());
        }

        let deadline = Instant::now() + ORDER_INDEX_WS_WAIT;
        loop {
            let targets = {
                let orders = self.orders.lock();
                collect_cancel_targets(&orders, &active_ids, false)
            };
            if let Ok(targets) = targets {
                return Ok(targets);
            }
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            let wait = (deadline - now).min(Duration::from_millis(50));
            let _ = tokio::time::timeout(wait, self.report_notify.notified()).await;
        }

        eprintln!(
            "WARN: cancel requested without confirmed order_index after waiting {}ms for ws state; reconciling before retry ids={:?}",
            ORDER_INDEX_WS_WAIT.as_millis(),
            active_ids
                .iter()
                .map(|id| id.0.as_str())
                .collect::<Vec<_>>()
        );
        let pre_reconcile_diagnostics =
            self.tracked_order_tx_meta(&active_ids, LIGHTER_TX_TYPE_CREATE_ORDER);
        self.reconcile_orders_once()
            .await
            .context("cancel target reconciliation failed")?;

        let (reconciled_active_ids, reconciled_skipped_terminal) =
            self.split_terminal_cancel_targets(&active_ids)?;
        self.log_skipped_terminal_cancel_targets(
            "became_terminal_after_reconcile",
            &reconciled_skipped_terminal,
        );
        if reconciled_active_ids.is_empty() {
            return Ok(Vec::new());
        }

        let snapshot = self.cancel_target_snapshot(&reconciled_active_ids);
        if !pre_reconcile_diagnostics.is_empty() {
            self.log_tx_hash_diagnostics(
                "cancel_target_missing_confirmed_index",
                &pre_reconcile_diagnostics,
            )
            .await;
        }
        let orders = self.orders.lock();
        let targets =
            collect_cancel_targets(&orders, &reconciled_active_ids, true).with_context(|| {
                format!(
                    "unable to resolve cancel target after reconciliation; snapshot={:?}",
                    snapshot
                )
            })?;
        for (id, cancel_ref) in &targets {
            if !matches!(cancel_ref, CancelRef::ClientOrderIndex(_)) {
                continue;
            }
            if let Some(state) = orders.get(id) {
                eprintln!(
                    "WARN: lighter cancel using client_order_index fallback client_order_id={} client_order_index={} status={:?} filled={} exchange_order_id={:?}",
                    id.0,
                    state.client_order_index,
                    state.status,
                    state.filled,
                    state.exchange_order_id
                );
            }
        }
        Ok(targets)
    }
}

#[async_trait]
impl ExecutionGateway for LighterGateway {
    async fn submit(&self, intents: &[QuoteIntent]) -> Result<Vec<OrderAck>> {
        if intents.is_empty() {
            return Ok(Vec::new());
        }
        self.validate_intents(intents)?;
        let _nonce_lock = self.nonce_lock.lock().await;
        for attempt in 0..2 {
            let (start_nonce, nonces) = self.peek_nonces(intents.len()).await?;
            let mut txs = Vec::with_capacity(intents.len());
            let mut pending_orders = Vec::with_capacity(intents.len());
            for (intent, nonce) in intents.iter().zip(nonces.into_iter()) {
                let px = self.to_price_int(intent.price)?;
                let size = self.to_size_int(intent.size)?;
                let client_order_index = self.client_order_index();
                let signed = self
                    .signer
                    .sign_order(
                        self.market_index,
                        client_order_index,
                        size,
                        px,
                        matches!(intent.side, Side::Ask),
                        0, // limit
                        match intent.tif {
                            TimeInForce::PostOnly => 2,
                            TimeInForce::Ioc => 0,
                            TimeInForce::Fok => 0,
                            TimeInForce::Gtc => 1,
                        },
                        false,
                        0,
                        -1,
                        nonce,
                        self.creds.api_key_index,
                        self.creds.account_index,
                    )
                    .await?;
                let exch_id = signed.tx_hash.clone().map(ExchangeOrderId);
                pending_orders.push((intent.clone(), client_order_index, exch_id.clone()));
                txs.push((signed, intent.client_order_id.clone()));
            }
            for (intent, client_order_index, exch_id) in &pending_orders {
                self.save_order(intent, *client_order_index, exch_id.clone());
            }
            let pending_ids = pending_orders
                .iter()
                .map(|(intent, _, _)| intent.client_order_id.clone())
                .collect::<Vec<_>>();
            match self.send_batch(txs).await {
                Ok(acks) => {
                    self.commit_nonces(start_nonce, intents.len());
                    return Ok(acks);
                }
                Err(err) => {
                    if Self::is_nonce_consuming_failure(&err) {
                        let _ = self.refresh_nonce_from_server().await?;
                    }
                    if Self::is_definitive_send_rejection(&err) {
                        self.drop_orders(&pending_ids);
                    }
                    if attempt == 0 && Self::is_nonce_error(&err) {
                        // Refresh nonce from server and retry once.
                        let _ = self.refresh_nonce_from_server().await?;
                        continue;
                    } else {
                        return Err(err);
                    }
                }
            }
        }
        bail!("unexpected submit retry exhaustion")
    }

    async fn cancel_and_submit(
        &self,
        cancel_ids: &[ClientOrderId],
        intents: &[QuoteIntent],
    ) -> Result<Vec<OrderAck>> {
        if cancel_ids.is_empty() {
            return self.submit(intents).await;
        }
        if intents.is_empty() {
            self.cancel_batch(cancel_ids).await?;
            return Ok(Vec::new());
        }
        self.validate_intents(intents)?;

        let _nonce_lock = self.nonce_lock.lock().await;
        for attempt in 0..2 {
            let cancels_snapshot = self.resolve_cancel_targets(cancel_ids).await?;
            if cancels_snapshot.is_empty() {
                return self.submit(intents).await;
            }

            let total = cancels_snapshot.len() + intents.len();
            let (start_nonce, nonces) = self.peek_nonces(total).await?;
            let mut nonce_iter = nonces.into_iter();

            let mut txs = Vec::with_capacity(total);
            let mut pending_orders = Vec::with_capacity(intents.len());
            let mut cancel_debug = Vec::with_capacity(cancels_snapshot.len());

            // Group cancels first, then new orders (nonce order must be strictly increasing).
            for (id, cancel_ref) in cancels_snapshot.iter().cloned() {
                let nonce = nonce_iter.next().expect("nonce iterator exhausted");
                let signed = self
                    .signer
                    .sign_cancel(
                        self.market_index,
                        cancel_ref.value(),
                        nonce,
                        self.creds.api_key_index,
                        self.creds.account_index,
                    )
                    .await?;
                cancel_debug.push((
                    id.0.clone(),
                    cancel_ref.source(),
                    cancel_ref.value(),
                    nonce,
                    signed.tx_hash.clone(),
                ));
                txs.push((signed, id));
            }

            for intent in intents {
                let nonce = nonce_iter.next().expect("nonce iterator exhausted");
                let px = self.to_price_int(intent.price)?;
                let size = self.to_size_int(intent.size)?;
                let client_order_index = self.client_order_index();
                let signed = self
                    .signer
                    .sign_order(
                        self.market_index,
                        client_order_index,
                        size,
                        px,
                        matches!(intent.side, Side::Ask),
                        0, // limit
                        match intent.tif {
                            TimeInForce::PostOnly => 2,
                            TimeInForce::Ioc => 0,
                            TimeInForce::Fok => 0,
                            TimeInForce::Gtc => 1,
                        },
                        false,
                        0,
                        -1,
                        nonce,
                        self.creds.api_key_index,
                        self.creds.account_index,
                    )
                    .await?;
                let exch_id = signed.tx_hash.clone().map(ExchangeOrderId);
                pending_orders.push((intent.clone(), client_order_index, exch_id));
                txs.push((signed, intent.client_order_id.clone()));
            }
            for (intent, client_order_index, exch_id) in &pending_orders {
                self.save_order(intent, *client_order_index, exch_id.clone());
            }
            let pending_ids = pending_orders
                .iter()
                .map(|(intent, _, _)| intent.client_order_id.clone())
                .collect::<Vec<_>>();

            match self.send_batch(txs).await {
                Ok(acks) => {
                    self.commit_nonces(start_nonce, total);
                    return Ok(acks.into_iter().skip(cancels_snapshot.len()).collect());
                }
                Err(err) => {
                    eprintln!(
                        "WARN: lighter cancel_and_submit batch failed cancel_debug={:?} err={:#}",
                        cancel_debug, err
                    );
                    if Self::is_nonce_consuming_failure(&err) {
                        let _ = self.refresh_nonce_from_server().await?;
                    }
                    if Self::is_definitive_send_rejection(&err) {
                        self.drop_orders(&pending_ids);
                    }
                    if attempt == 0 && Self::is_nonce_error(&err) {
                        let _ = self.refresh_nonce_from_server().await?;
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        bail!("unexpected cancel_and_submit retry exhaustion")
    }

    async fn cancel_batch(&self, ids: &[ClientOrderId]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let _nonce_lock = self.nonce_lock.lock().await;
        for attempt in 0..2 {
            let orders_snapshot = self.resolve_cancel_targets(ids).await?;
            if orders_snapshot.is_empty() {
                return Ok(());
            }
            let (start_nonce, nonces) = self.peek_nonces(orders_snapshot.len()).await?;

            let mut txs = Vec::with_capacity(orders_snapshot.len());
            let mut cancel_debug = Vec::with_capacity(orders_snapshot.len());
            for ((id, cancel_ref), nonce) in orders_snapshot.into_iter().zip(nonces.into_iter()) {
                let signed = self
                    .signer
                    .sign_cancel(
                        self.market_index,
                        cancel_ref.value(),
                        nonce,
                        self.creds.api_key_index,
                        self.creds.account_index,
                    )
                    .await?;
                cancel_debug.push((
                    id.0.clone(),
                    cancel_ref.source(),
                    cancel_ref.value(),
                    nonce,
                    signed.tx_hash.clone(),
                ));
                txs.push((signed, id));
            }
            let tx_count = txs.len();
            match self.send_batch(txs).await {
                Ok(_) => {
                    self.commit_nonces(start_nonce, tx_count);
                    return Ok(());
                }
                Err(err) => {
                    eprintln!(
                        "WARN: lighter cancel_batch failed cancel_debug={:?} err={:#}",
                        cancel_debug, err
                    );
                    if Self::is_nonce_consuming_failure(&err) {
                        let _ = self.refresh_nonce_from_server().await?;
                    }
                    if attempt == 0 && Self::is_nonce_error(&err) {
                        let _ = self.refresh_nonce_from_server().await?;
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        bail!("unexpected cancel retry exhaustion")
    }

    async fn poll_reports(&self) -> Result<Vec<ExecutionReport>> {
        loop {
            {
                let mut guard = self.pending_reports.lock();
                if !guard.is_empty() {
                    return Ok(guard.drain(..).collect());
                }
            }
            self.report_notify.notified().await;
        }
    }
}

pub async fn lighter_auth_token(creds: &LighterCredentials, debug_prints: bool) -> Result<String> {
    let client = LighterAuthClient::connect(creds.clone(), debug_prints).await?;
    client.auth_token().await
}
