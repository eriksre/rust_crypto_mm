use std::sync::OnceLock;

use crate::base_classes::feed_config::FeedToggles;

static FEED_OVERRIDES: OnceLock<FeedToggles> = OnceLock::new();
static DEMEAN_ENABLED: OnceLock<bool> = OnceLock::new();

/// Configure feed overrides before spawning the state engine.
pub fn configure_feed_overrides(feeds: FeedToggles) {
    if FEED_OVERRIDES.set(feeds).is_err() {
        if let Some(existing) = FEED_OVERRIDES.get().copied() {
            if existing != feeds {
                eprintln!(
                    "feed overrides already configured; keeping existing value {:?}",
                    existing
                );
            }
        }
    }
}

pub fn current_feeds() -> FeedToggles {
    FEED_OVERRIDES.get().copied().unwrap_or_default()
}

/// Configure whether per-feed de-meaning should be applied when updating state.
pub fn configure_demean_enabled(enabled: bool) {
    if DEMEAN_ENABLED.set(enabled).is_err() {
        if let Some(existing) = DEMEAN_ENABLED.get().copied() {
            if existing != enabled {
                eprintln!(
                    "demean toggle already configured; keeping existing value {}",
                    existing
                );
            }
        }
    }
}

pub fn demean_enabled() -> bool {
    *DEMEAN_ENABLED.get_or_init(|| true)
}
