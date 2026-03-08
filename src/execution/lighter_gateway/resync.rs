use super::*;

pub(super) struct LighterResyncWorker {
    pub(super) rest: LighterRestClient,
    pub(super) size_scale: f64,
    pub(super) orders: Arc<Mutex<HashMap<ClientOrderId, OrderState>>>,
    pub(super) pending_reports: Arc<Mutex<Vec<ExecutionReport>>>,
    pub(super) report_notify: Arc<Notify>,
    pub(super) interval: Duration,
}

impl LighterResyncWorker {
    pub(super) async fn run(self) {
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
