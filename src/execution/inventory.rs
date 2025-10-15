use std::collections::HashMap;

use super::types::{ClientOrderId, ExecutionReport, OrderStatus, QuoteIntent};
use crate::base_classes::types::Side;

#[derive(Debug, Clone, Copy)]
pub enum InventoryUpdateSource {
    Registered,
    Inferred,
}

#[derive(Debug, Clone)]
pub struct InventoryUpdate {
    pub client_order_id: ClientOrderId,
    pub side: Side,
    pub prev_contracts: f64,
    pub new_contracts: f64,
    pub delta_contracts: f64,
    pub fill_qty: f64,
    pub fill_price: Option<f64>,
    pub status: OrderStatus,
    pub source: InventoryUpdateSource,
}

#[derive(Debug)]
pub enum InventoryReportOutcome {
    None,
    Applied(InventoryUpdate),
    Missing {
        order_id: ClientOrderId,
        filled_qty: f64,
        avg_price: Option<f64>,
        status: OrderStatus,
    },
}

#[derive(Debug, Clone)]
struct DanglingFill {
    side: Side,
    filled_qty: f64,
}

#[derive(Debug)]
struct OrderFillState {
    side: Side,
    filled_qty: f64,
}

#[derive(Debug, Default)]
pub struct InventoryTracker {
    contract_size: f64,
    net_contracts: f64,
    orders: HashMap<ClientOrderId, OrderFillState>,
    dangling: HashMap<ClientOrderId, DanglingFill>,
}

impl InventoryTracker {
    pub fn new(contract_size: f64, initial_contracts: f64) -> Self {
        Self {
            contract_size,
            net_contracts: initial_contracts,
            orders: HashMap::new(),
            dangling: HashMap::new(),
        }
    }

    pub fn net_contracts(&self) -> f64 {
        self.net_contracts
    }

    pub fn record_orders(&mut self, intents: &[QuoteIntent]) -> Vec<ClientOrderId> {
        let mut added = Vec::with_capacity(intents.len());
        for intent in intents {
            let id = intent.client_order_id.clone();
            let mut state = OrderFillState {
                side: intent.side,
                filled_qty: 0.0,
            };
            if let Some(dangling) = self.dangling.remove(&id) {
                state.filled_qty = dangling.filled_qty;
            }
            self.orders.insert(id.clone(), state);
            added.push(id);
        }
        added
    }

    pub fn apply_report(&mut self, report: &ExecutionReport) -> InventoryReportOutcome {
        let contract_size = self.contract_size;
        if let Some(entry) = self.orders.get_mut(&report.client_order_id) {
            let side = entry.side;
            let mut update: Option<InventoryUpdate> = None;

            if report.filled_qty > entry.filled_qty + f64::EPSILON {
                let delta_qty = report.filled_qty - entry.filled_qty;
                let delta_contracts = if contract_size > 0.0 {
                    delta_qty / contract_size
                } else {
                    0.0
                };

                if delta_contracts.abs() > f64::EPSILON {
                    let prev_contracts = self.net_contracts;
                    match side {
                        Side::Bid => self.net_contracts += delta_contracts,
                        Side::Ask => self.net_contracts -= delta_contracts,
                    }
                    entry.filled_qty = report.filled_qty;
                    update = Some(InventoryUpdate {
                        client_order_id: report.client_order_id.clone(),
                        side,
                        prev_contracts,
                        new_contracts: self.net_contracts,
                        delta_contracts: self.net_contracts - prev_contracts,
                        fill_qty: delta_qty,
                        fill_price: report.avg_fill_price,
                        status: report.status.clone(),
                        source: InventoryUpdateSource::Registered,
                    });
                }
            }

            if matches!(
                report.status,
                OrderStatus::Filled | OrderStatus::Canceled | OrderStatus::Rejected
            ) {
                self.orders.remove(&report.client_order_id);
                self.dangling.remove(&report.client_order_id);
            }

            if let Some(update) = update {
                InventoryReportOutcome::Applied(update)
            } else {
                InventoryReportOutcome::None
            }
        } else if report.filled_qty > f64::EPSILON {
            let (side, prev_filled) =
                if let Some(dangling) = self.dangling.get(&report.client_order_id) {
                    (dangling.side, dangling.filled_qty)
                } else if let Some(inferred) = infer_side_from_id(&report.client_order_id) {
                    (inferred, 0.0)
                } else {
                    return InventoryReportOutcome::Missing {
                        order_id: report.client_order_id.clone(),
                        filled_qty: report.filled_qty,
                        avg_price: report.avg_fill_price,
                        status: report.status.clone(),
                    };
                };

            let delta_qty = report.filled_qty - prev_filled;
            let delta_contracts = if contract_size > 0.0 {
                delta_qty / contract_size
            } else {
                0.0
            };
            if delta_contracts.abs() > f64::EPSILON {
                let prev = self.net_contracts;
                match side {
                    Side::Bid => self.net_contracts += delta_contracts,
                    Side::Ask => self.net_contracts -= delta_contracts,
                }
                let update = InventoryUpdate {
                    client_order_id: report.client_order_id.clone(),
                    side,
                    prev_contracts: prev,
                    new_contracts: self.net_contracts,
                    delta_contracts: self.net_contracts - prev,
                    fill_qty: delta_qty,
                    fill_price: report.avg_fill_price,
                    status: report.status.clone(),
                    source: InventoryUpdateSource::Inferred,
                };

                self.dangling.insert(
                    report.client_order_id.clone(),
                    DanglingFill {
                        side,
                        filled_qty: report.filled_qty,
                    },
                );

                if matches!(
                    report.status,
                    OrderStatus::Filled | OrderStatus::Canceled | OrderStatus::Rejected
                ) {
                    self.dangling.remove(&report.client_order_id);
                }

                InventoryReportOutcome::Applied(update)
            } else {
                self.dangling.insert(
                    report.client_order_id.clone(),
                    DanglingFill {
                        side,
                        filled_qty: report.filled_qty,
                    },
                );
                InventoryReportOutcome::None
            }
        } else {
            InventoryReportOutcome::None
        }
    }

    pub fn replace_from_rest(&mut self, contracts: f64) -> Option<(f64, f64)> {
        let prev = self.net_contracts;
        if (contracts - prev).abs() > 1e-9 {
            self.net_contracts = contracts;
            Some((prev, contracts))
        } else {
            None
        }
    }
}

fn infer_side_from_id(id: &ClientOrderId) -> Option<Side> {
    let lower = id.0.to_ascii_lowercase();
    if lower.contains("-b-") {
        Some(Side::Bid)
    } else if lower.contains("-s-") {
        Some(Side::Ask)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base_classes::types::Side;
    use crate::execution::types::{OrderStatus, QuoteIntent, TimeInForce, Venue};

    fn intent(id: &str, side: Side, contract_size: f64) -> QuoteIntent {
        QuoteIntent::new(
            Venue::Gate,
            "TEST".to_string(),
            side,
            contract_size,
            contract_size,
            TimeInForce::PostOnly,
            ClientOrderId::new(id),
        )
    }

    fn report(id: &str, status: OrderStatus, filled_qty: f64) -> ExecutionReport {
        ExecutionReport {
            client_order_id: ClientOrderId::new(id.to_string()),
            exchange_order_id: None,
            status,
            filled_qty,
            avg_fill_price: Some(10.0),
            ts: None,
        }
    }

    #[test]
    fn records_new_orders_and_applies_registered_fills() {
        let mut tracker = InventoryTracker::new(10.0, 0.0);
        let intents = vec![intent("order-b-1", Side::Bid, 10.0)];
        tracker.record_orders(&intents);

        let (first_id, _) = (&intents[0].client_order_id, intents[0].size);
        assert_eq!(tracker.net_contracts(), 0.0);

        match tracker.apply_report(&report(&first_id.0, OrderStatus::PartiallyFilled, 5.0)) {
            InventoryReportOutcome::Applied(update) => {
                assert_eq!(update.delta_contracts, 0.5);
                assert!(matches!(update.source, InventoryUpdateSource::Registered));
            }
            other => panic!("unexpected outcome: {:?}", other),
        }
    }

    #[test]
    fn infers_fills_for_unknown_reports() {
        let mut tracker = InventoryTracker::new(10.0, 1.0);
        let unknown_report = report("dangling-unknown-1", OrderStatus::PartiallyFilled, 12.0);
        match tracker.apply_report(&unknown_report) {
            InventoryReportOutcome::Missing { .. } => {}
            other => panic!("expected missing, got {:?}", other),
        }

        // Supply prior dangling state and ensure inference applies delta.
        let report = report("dangling-s-1", OrderStatus::PartiallyFilled, 15.0);
        tracker.dangling.insert(
            report.client_order_id.clone(),
            DanglingFill {
                side: Side::Ask,
                filled_qty: 5.0,
            },
        );
        match tracker.apply_report(&report) {
            InventoryReportOutcome::Applied(update) => {
                assert!(matches!(update.source, InventoryUpdateSource::Inferred));
                assert_eq!(update.delta_contracts, -1.0);
            }
            other => panic!("unexpected outcome: {:?}", other),
        }
    }

    #[test]
    fn rest_replacement_handles_no_change() {
        let mut tracker = InventoryTracker::new(5.0, 3.0);
        assert!(tracker.replace_from_rest(3.0).is_none());
        assert_eq!(tracker.net_contracts(), 3.0);
        assert_eq!(tracker.replace_from_rest(1.0), Some((3.0, 1.0)));
    }
}
