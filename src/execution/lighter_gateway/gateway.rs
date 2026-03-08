use super::*;
use async_trait::async_trait;

pub struct LighterGateway {
    signer: SignerHandle,
    creds: LighterCredentials,
    instrument: LighterInstrument,
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
    pub async fn connect(config: LighterGatewayConfig) -> Result<Self> {
        let LighterGatewayConfig {
            creds,
            instrument,
            debug_prints,
            suppress_sendtx_quota_logs,
        } = config;
        if instrument.symbol.trim().is_empty() {
            bail!("lighter gateway config symbol must not be empty");
        }
        let market_index = instrument.market_index_u8()?;
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
        let price_scale = 10_f64.powi(instrument.price_decimals as i32);
        let size_scale = 10_f64.powi(instrument.size_decimals as i32);
        let pending_reports = Arc::new(Mutex::new(Vec::new()));
        let orders = Arc::new(Mutex::new(HashMap::new()));
        let report_notify = Arc::new(Notify::new());
        let last_api_call = Arc::new(Mutex::new(None));
        let rest = LighterRestClient::new(
            signer.clone(),
            creds.clone(),
            market_index,
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
                market_index,
                debug_prints,
                suppress_sendtx_quota_logs,
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
            instrument,
            market_index,
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
            if !self.instrument.matches_symbol(&intent.symbol) {
                bail!(
                    "lighter gateway received wrong symbol client_order_id={} expected_symbol={} actual_symbol={} market_id={}",
                    intent.client_order_id,
                    self.instrument.symbol,
                    intent.symbol,
                    self.instrument.market_id
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
