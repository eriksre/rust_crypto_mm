use std::time::Duration;

use crate::base_classes::demean::{DemeanTracker, ExchangeKind};
use crate::base_classes::state::ExchangeAdjustment;
use crate::base_classes::types::Ts;

use super::helpers::lock_state;

pub struct DemeanController {
    enabled: bool,
    tracker: DemeanTracker,
}

impl DemeanController {
    pub fn new(enabled: bool, window: Duration) -> Self {
        Self {
            enabled,
            tracker: DemeanTracker::new(window),
        }
    }

    pub fn record_other(&mut self, exchange: ExchangeKind, ts: Option<Ts>, price: Option<f64>) {
        if self.enabled {
            self.tracker.record_other(exchange, ts, price);
        }
    }

    pub fn on_gate_event(
        &mut self,
        ts: Option<Ts>,
        price: Option<f64>,
    ) -> Vec<(ExchangeKind, ExchangeAdjustment)> {
        if self.enabled {
            self.tracker.on_gate_event(ts, price)
        } else {
            Vec::new()
        }
    }

    pub fn apply_updates(&self, updates: &[(ExchangeKind, ExchangeAdjustment)]) {
        if !self.enabled || updates.is_empty() {
            return;
        }

        let mut st = lock_state();
        for (exchange, adj) in updates {
            let target = match exchange {
                ExchangeKind::Bybit => &mut st.demean.bybit,
                ExchangeKind::Binance => &mut st.demean.binance,
                ExchangeKind::Bitget => &mut st.demean.bitget,
                ExchangeKind::Okx => &mut st.demean.okx,
                ExchangeKind::Mexc => &mut st.demean.mexc,
            };
            *target = *adj;
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }
}
