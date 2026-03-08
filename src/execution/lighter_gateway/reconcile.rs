use super::*;

pub(super) fn parse_f64(field: &str, s: Option<&String>) -> Option<f64> {
    let value = s?;
    match value.parse::<f64>() {
        Ok(v) if v.is_finite() => Some(v),
        Ok(_) => {
            log_parse_drop("lighter_gateway", field, &"non-finite number", value);
            None
        }
        Err(err) => {
            log_parse_drop("lighter_gateway", field, &err, value);
            None
        }
    }
}

pub(super) fn map_status(status: &str) -> OrderStatus {
    match status {
        "filled" => OrderStatus::Filled,
        "canceled"
        | "cancelled"
        | "canceled-oco"
        | "cancelled-oco"
        | "canceled-expired"
        | "cancelled-expired"
        | "canceled-child"
        | "cancelled-child"
        | "canceled-post-only"
        | "cancelled-post-only"
        | "closed" => OrderStatus::Canceled,
        "rejected" => OrderStatus::Rejected,
        "partially_filled" | "partial_filled" | "partially-filled" => OrderStatus::PartiallyFilled,
        "in-progress" | "pending" | "open" => OrderStatus::New,
        _ => OrderStatus::Unknown,
    }
}

pub(super) fn update_from_entry(
    size_scale: f64,
    entry: &LighterOrderEntry,
    state: &mut OrderState,
    id: &ClientOrderId,
    reports: &mut Vec<ExecutionReport>,
) {
    let order_index_was_missing = state.order_index.is_none();
    if let Some(idx) = entry.order_index {
        state.order_index = Some(idx);
        if order_index_was_missing && matches!(state.status, OrderStatus::New) {
            reports.push(ExecutionReport {
                client_order_id: id.clone(),
                exchange_order_id: state.exchange_order_id.clone(),
                status: OrderStatus::New,
                filled_qty: state.filled,
                avg_fill_price: Some(state.price),
                ts: entry.timestamp.map(|v| v as u64),
            });
        }
    }
    let filled_base = match parse_f64("filled_base_amount", entry.filled_base_amount.as_ref()) {
        Some(v) => v,
        None => {
            log_parse_drop(
                "lighter_gateway",
                "missing_filled_base_amount",
                &"missing filled_base_amount",
                "",
            );
            return;
        }
    };
    let filled_size = filled_base / size_scale;
    if filled_size > state.filled + 1e-9 {
        state.filled = filled_size;
        let done = entry
            .remaining_base_amount
            .as_ref()
            .and_then(|s| match s.parse::<f64>() {
                Ok(v) if v.is_finite() => Some(v),
                Ok(_) => {
                    log_parse_drop(
                        "lighter_gateway",
                        "non_finite_remaining_base_amount",
                        &"non-finite remaining_base_amount",
                        s,
                    );
                    None
                }
                Err(err) => {
                    log_parse_drop("lighter_gateway", "remaining_base_amount", &err, s);
                    None
                }
            })
            .map(|rem| rem <= 0.0)
            .unwrap_or(false);
        let status = if done {
            OrderStatus::Filled
        } else {
            OrderStatus::PartiallyFilled
        };
        state.status = status.clone();
        reports.push(ExecutionReport {
            client_order_id: id.clone(),
            exchange_order_id: state.exchange_order_id.clone(),
            status,
            filled_qty: filled_size,
            avg_fill_price: Some(state.price),
            ts: entry.timestamp.map(|v| v as u64),
        });
    }
}
