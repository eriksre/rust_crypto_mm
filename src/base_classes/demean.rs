use std::collections::VecDeque;
use std::time::Duration;

use crate::base_classes::state::ExchangeAdjustment;
use crate::base_classes::types::Ts;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ExchangeKind {
    Bybit,
    Binance,
    Bitget,
    Okx,
}

#[derive(Clone, Copy, Debug)]
struct PriceSample {
    ts_ns: Ts,
    price: f64,
}

#[derive(Clone, Copy, Debug)]
struct DemeanSample {
    ts_ns: Ts,
    diff: f64,
}

#[derive(Default, Debug)]
struct ExchangeState {
    prices: VecDeque<PriceSample>,
    diffs: VecDeque<DemeanSample>,
    diff_sum: f64,
}

impl ExchangeState {
    fn record_price(&mut self, ts_ns: Ts, price: f64, window_ns: Ts) {
        self.prices.push_back(PriceSample { ts_ns, price });
        self.prune_prices(ts_ns.saturating_sub(window_ns));
    }

    fn update_with_gate(
        &mut self,
        gate_ts: Ts,
        gate_price: f64,
        window_ns: Ts,
    ) -> ExchangeAdjustment {
        self.prune_prices(gate_ts.saturating_sub(window_ns));
        self.prune_diffs(gate_ts.saturating_sub(window_ns));

        if let Some(other) = self
            .prices
            .iter()
            .rev()
            .find(|sample| sample.ts_ns <= gate_ts)
        {
            let diff = other.price - gate_price;
            self.diffs.push_back(DemeanSample {
                ts_ns: gate_ts,
                diff,
            });
            self.diff_sum += diff;
        }
        self.prune_diffs(gate_ts.saturating_sub(window_ns));

        let samples = self.diffs.len() as u32;
        let offset = if samples > 0 {
            Some(self.diff_sum / samples as f64)
        } else {
            None
        };

        let last_update_ns = self.diffs.back().map(|sample| sample.ts_ns);

        ExchangeAdjustment {
            offset,
            samples,
            last_update_ns,
        }
    }

    fn prune_prices(&mut self, cutoff: Ts) {
        while let Some(front) = self.prices.front() {
            if front.ts_ns >= cutoff {
                break;
            }
            self.prices.pop_front();
        }
    }

    fn prune_diffs(&mut self, cutoff: Ts) {
        while let Some(front) = self.diffs.front() {
            if front.ts_ns >= cutoff {
                break;
            }
            if let Some(front) = self.diffs.pop_front() {
                self.diff_sum -= front.diff;
            }
        }
    }
}

#[derive(Debug)]
pub struct DemeanTracker {
    window_ns: Ts,
    bybit: ExchangeState,
    binance: ExchangeState,
    bitget: ExchangeState,
    okx: ExchangeState,
}

impl DemeanTracker {
    pub fn new(window: Duration) -> Self {
        let window_ns = window.as_nanos().min(Ts::MAX as u128) as Ts;
        Self {
            window_ns,
            bybit: ExchangeState::default(),
            binance: ExchangeState::default(),
            bitget: ExchangeState::default(),
            okx: ExchangeState::default(),
        }
    }

    pub fn record_other(&mut self, exchange: ExchangeKind, ts: Option<Ts>, price: Option<f64>) {
        let ts_ns = match ts {
            Some(ts) if ts > 0 => ts,
            _ => return,
        };
        let price = match price {
            Some(px) if px.is_finite() && px > 0.0 => px,
            _ => return,
        };
        let window_ns = self.window_ns;
        let state = self.state_mut(exchange);
        state.record_price(ts_ns, price, window_ns);
    }

    pub fn on_gate_event(
        &mut self,
        ts: Option<Ts>,
        price: Option<f64>,
    ) -> Vec<(ExchangeKind, ExchangeAdjustment)> {
        let ts_ns = match ts {
            Some(ts) if ts > 0 => ts,
            _ => return Vec::new(),
        };
        let gate_price = match price {
            Some(px) if px.is_finite() && px > 0.0 => px,
            _ => return Vec::new(),
        };

        let mut out = Vec::with_capacity(4);
        let window_ns = self.window_ns;
        for exchange in [
            ExchangeKind::Bybit,
            ExchangeKind::Binance,
            ExchangeKind::Bitget,
            ExchangeKind::Okx,
        ] {
            let adj = self
                .state_mut(exchange)
                .update_with_gate(ts_ns, gate_price, window_ns);
            out.push((exchange, adj));
        }
        out
    }

    fn state_mut(&mut self, exchange: ExchangeKind) -> &mut ExchangeState {
        match exchange {
            ExchangeKind::Bybit => &mut self.bybit,
            ExchangeKind::Binance => &mut self.binance,
            ExchangeKind::Bitget => &mut self.bitget,
            ExchangeKind::Okx => &mut self.okx,
        }
    }
}
