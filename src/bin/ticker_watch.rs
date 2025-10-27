use std::thread;
use std::time::Duration;

use rust_test::base_classes::engine::{configure_feed_overrides, spawn_state_engine};
use rust_test::base_classes::feed_config::FeedToggles;
use rust_test::base_classes::state::{TickerSnap, state};

fn main() {
    let symbol = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "BTCUSDT".to_string());

    configure_feed_overrides(FeedToggles::default());
    let _engine = spawn_state_engine(symbol.clone(), None, None);
    println!("Watching ticker updates for {symbol}. Ctrl-C to exit.\n");

    let mut last_seq = ExchangeSeqs::default();

    loop {
        {
            let st = state().lock().unwrap();

            last_seq.update_and_print("bybit", st.bybit.ticker);
            last_seq.update_and_print("binance", st.binance.ticker);
            last_seq.update_and_print("gate", st.gate.ticker);
            last_seq.update_and_print("bitget", st.bitget.ticker);
        }

        thread::sleep(Duration::from_millis(100));
    }
}

#[derive(Default)]
struct ExchangeSeqs {
    bybit: u64,
    binance: u64,
    gate: u64,
    bitget: u64,
}

impl ExchangeSeqs {
    fn update_and_print(&mut self, name: &str, snap: TickerSnap) {
        let seq_ref = match name {
            "bybit" => &mut self.bybit,
            "binance" => &mut self.binance,
            "gate" => &mut self.gate,
            "bitget" => &mut self.bitget,
            _ => return,
        };

        if snap.seq == 0 || snap.seq == *seq_ref {
            return;
        }

        *seq_ref = snap.seq;

        println!("[{name}] seq={} ts={}", snap.seq, snap.ts_ns.unwrap_or(0));
        println!(
            "  last_price={:?} last_qty={:?} best_bid={:?} best_ask={:?}",
            snap.last_price, snap.last_qty, snap.best_bid, snap.best_ask
        );
        println!(
            "  mark_price={:?} index_price={:?} funding_rate={:?}",
            snap.mark_price, snap.index_price, snap.funding_rate
        );
        println!(
            "  turnover_24h={:?} open_interest={:?} open_interest_value={:?} quanto_multiplier={:?}\n",
            snap.turnover_24h, snap.open_interest, snap.open_interest_value, snap.quanto_multiplier
        );
    }
}
