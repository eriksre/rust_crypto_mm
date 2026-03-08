use super::*;

pub(super) const RECONCILE_WAIT_DEFAULT: Duration = Duration::from_secs(8);
pub(super) const RECONCILE_WAIT_WITH_CANCEL: Duration = Duration::from_secs(20);
pub(super) const ORDER_INDEX_WS_WAIT: Duration = Duration::from_millis(350);

#[derive(Clone)]
pub(super) struct SignedTx {
    pub(super) tx_type: u8,
    pub(super) tx_info: String,
    pub(super) tx_hash: Option<String>,
}

#[derive(Debug, Clone)]
pub(super) struct OrderState {
    pub(super) client_order_index: i64,
    pub(super) order_index: Option<i64>,
    pub(super) side: Side,
    pub(super) price: f64,
    pub(super) size: f64,
    pub(super) filled: f64,
    pub(super) status: OrderStatus,
    pub(super) exchange_order_id: Option<ExchangeOrderId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CancelRef {
    ExchangeOrderIndex(i64),
    ClientOrderIndex(i64),
}

impl CancelRef {
    pub(super) fn value(self) -> i64 {
        match self {
            Self::ExchangeOrderIndex(value) | Self::ClientOrderIndex(value) => value,
        }
    }

    pub(super) fn source(self) -> &'static str {
        match self {
            Self::ExchangeOrderIndex(_) => "exchange_order_index",
            Self::ClientOrderIndex(_) => "client_order_index",
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct SignedBatchTxMeta {
    pub(super) tx_type: u8,
    pub(super) client_order_id: ClientOrderId,
    pub(super) tx_hash: Option<String>,
}

#[derive(Debug, Clone)]
pub(super) struct SkippedCancelTarget {
    pub(super) client_order_id: ClientOrderId,
    pub(super) status: OrderStatus,
    pub(super) order_index: Option<i64>,
    pub(super) client_order_index: i64,
    pub(super) filled: f64,
}

pub(super) struct PendingSendTxBatch {
    pub(super) tx_meta: Vec<SignedBatchTxMeta>,
    pub(super) expected_hashes: Vec<String>,
    pub(super) observed_hashes: HashSet<String>,
    pub(super) resp: Option<oneshot::Sender<Result<SendTxBatchResponse>>>,
}

pub(super) struct AccountTxOutcome {
    pub(super) confirmed: bool,
    pub(super) application_error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct AccountTxOrderRef {
    pub(super) client_order_index: i64,
    pub(super) order_index: i64,
}

pub(super) fn batch_observed_with_orders(
    orders: &HashMap<ClientOrderId, OrderState>,
    tx_meta: &[(u8, ClientOrderId)],
) -> bool {
    tx_meta.iter().all(|(tx_type, id)| {
        let state = orders.get(id);
        match *tx_type {
            LIGHTER_TX_TYPE_CREATE_ORDER => state
                .map(|st| {
                    st.order_index.is_some()
                        || st.filled > 0.0
                        || matches!(
                            st.status,
                            OrderStatus::PartiallyFilled
                                | OrderStatus::Filled
                                | OrderStatus::Canceled
                                | OrderStatus::Rejected
                        )
                })
                .unwrap_or(false),
            LIGHTER_TX_TYPE_CANCEL_ORDER => state
                .map(|st| {
                    matches!(
                        st.status,
                        OrderStatus::Canceled | OrderStatus::Filled | OrderStatus::Rejected
                    )
                })
                .unwrap_or(false),
            _ => false,
        }
    })
}

pub(super) fn reconcile_wait_for_batch(tx_meta: &[(u8, ClientOrderId)]) -> Duration {
    if tx_meta
        .iter()
        .any(|(tx_type, _)| *tx_type == LIGHTER_TX_TYPE_CANCEL_ORDER)
    {
        RECONCILE_WAIT_WITH_CANCEL
    } else {
        RECONCILE_WAIT_DEFAULT
    }
}

pub(super) fn order_status_is_terminal_for_cancel(status: &OrderStatus) -> bool {
    matches!(
        status,
        OrderStatus::Canceled | OrderStatus::Filled | OrderStatus::Rejected
    )
}

pub(super) fn collect_cancel_targets(
    orders: &HashMap<ClientOrderId, OrderState>,
    ids: &[ClientOrderId],
    allow_client_order_index_fallback: bool,
) -> Result<Vec<(ClientOrderId, CancelRef)>> {
    ids.iter()
        .map(|id| {
            let state = orders
                .get(id)
                .ok_or_else(|| anyhow!("unknown order {}", id.0))?;
            if let Some(order_index) = state.order_index {
                if order_index <= 0 {
                    bail!(
                        "invalid confirmed order_index for {} (client_order_index={}, status={:?}, filled={})",
                        id.0,
                        state.client_order_index,
                        state.status,
                        state.filled
                    );
                }
                return Ok((id.clone(), CancelRef::ExchangeOrderIndex(order_index)));
            }
            if !allow_client_order_index_fallback {
                bail!(
                    "missing confirmed order_index for {} (client_order_index={}, status={:?}, filled={})",
                    id.0,
                    state.client_order_index,
                    state.status,
                    state.filled
                );
            }
            if state.client_order_index <= 0 {
                bail!(
                    "invalid client_order_index for {} (client_order_index={}, status={:?}, filled={})",
                    id.0,
                    state.client_order_index,
                    state.status,
                    state.filled
                );
            }
            if !matches!(state.status, OrderStatus::New | OrderStatus::PartiallyFilled) {
                bail!(
                    "missing confirmed order_index and refusing client_order_index fallback for {} (client_order_index={}, status={:?}, filled={})",
                    id.0,
                    state.client_order_index,
                    state.status,
                    state.filled
                );
            }
            Ok((
                id.clone(),
                CancelRef::ClientOrderIndex(state.client_order_index),
            ))
        })
        .collect()
}
