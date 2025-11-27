use crate::base_classes::feed_gate::{ExchangeFeed, FeedKind};
use crate::base_classes::ring_buffer::Consumer;
use crate::base_classes::state::{GlobalState, SNAPSHOT_DEPTH, state};
use crate::base_classes::types::Ts;

#[inline(always)]
pub fn levels_to_array(levels: &[(f64, f64)]) -> [Option<(f64, f64)>; SNAPSHOT_DEPTH] {
    let mut out = [None; SNAPSHOT_DEPTH];
    for (idx, &(px, qty)) in levels.iter().take(SNAPSHOT_DEPTH).enumerate() {
        out[idx] = Some((px, qty));
    }
    out
}

#[inline(always)]
pub fn level_from_option(level: Option<(f64, f64)>) -> [Option<(f64, f64)>; SNAPSHOT_DEPTH] {
    let mut out = [None; SNAPSHOT_DEPTH];
    if let Some(lvl) = level {
        out[0] = Some(lvl);
    }
    out
}

#[inline(always)]
pub fn drain_latest_bbo<F, const N: usize, P>(
    frame: &mut F,
    consumer: &Consumer<F, N>,
    pending: &mut Option<F>,
    mut is_bbo: P,
) where
    P: FnMut(&F) -> bool,
{
    if !is_bbo(frame) {
        return;
    }
    while let Ok(next) = consumer.try_pop() {
        if is_bbo(&next) {
            *frame = next;
        } else {
            *pending = Some(next);
            break;
        }
    }
}

#[inline(always)]
pub fn format_okx_inst_id(symbol: &str) -> String {
    let upper = symbol.trim().to_ascii_uppercase();
    let replaced = upper.replace('/', "-").replace('_', "-");
    if replaced.contains('-') {
        if replaced.ends_with("-SWAP") {
            replaced
        } else {
            format!("{replaced}-SWAP")
        }
    } else {
        const QUOTES: [&str; 4] = ["USDT", "USD", "USDC", "BTC"];
        for quote in QUOTES {
            if replaced.ends_with(quote) && replaced.len() > quote.len() {
                let base = replaced[..replaced.len() - quote.len()]
                    .trim_matches('-')
                    .to_string();
                if !base.is_empty() {
                    return format!("{base}-{quote}-SWAP");
                }
            }
        }
        format!("{replaced}-SWAP")
    }
}

#[inline(always)]
pub fn log_stale_update(exchange: ExchangeFeed, feed: FeedKind, ts: Ts, last_ts: Ts, count: u64) {
    if count <= 3 || count % 100 == 0 {
        eprintln!(
            "dropping stale {} {} update: ts={} < last={} ({} drops)",
            exchange.as_str(),
            feed.as_str(),
            ts,
            last_ts,
            count
        );
    }
}

/// LOUD state lock helper - panics immediately if lock is poisoned.
/// This is intentional - a poisoned lock means the system is in an undefined state.
#[inline(always)]
pub fn lock_state() -> std::sync::MutexGuard<'static, GlobalState> {
    match state().lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            eprintln!(
                "FATAL: State lock poisoned in engine main loop: {}",
                poisoned
            );
            eprintln!("This indicates a panic occurred while holding the state lock.");
            eprintln!("The system cannot continue safely - terminating immediately.");
            panic!("State lock poisoned - unrecoverable error");
        }
    }
}
