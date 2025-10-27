use serde::Deserialize;

/// Toggle state for a market data feed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedToggle {
    On,
    Off,
    Auto,
}

impl FeedToggle {
    /// Returns true when the feed should start enabled before any auto detection.
    pub fn initial_enabled(self) -> bool {
        !matches!(self, Self::Off)
    }

    /// Returns true when automatic detection should decide whether to keep the feed running.
    pub fn is_auto(self) -> bool {
        matches!(self, Self::Auto)
    }
}

impl Default for FeedToggle {
    fn default() -> Self {
        FeedToggle::Auto
    }
}

impl<'de> Deserialize<'de> for FeedToggle {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ToggleVisitor;

        impl<'de> serde::de::Visitor<'de> for ToggleVisitor {
            type Value = FeedToggle;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a boolean or one of \"on\", \"off\", \"auto\"")
            }

            fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(if v { FeedToggle::On } else { FeedToggle::Off })
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match v.trim().to_ascii_lowercase().as_str() {
                    "on" | "true" => Ok(FeedToggle::On),
                    "off" | "false" => Ok(FeedToggle::Off),
                    "auto" => Ok(FeedToggle::Auto),
                    other => Err(E::unknown_variant(other, &["on", "off", "auto"])),
                }
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_str(&v)
            }
        }

        deserializer.deserialize_any(ToggleVisitor)
    }
}

/// Aggregated feed toggles for all exchanges the engine can track.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct FeedToggles {
    pub gate: FeedToggle,
    pub binance: FeedToggle,
    pub bybit: FeedToggle,
    pub bitget: FeedToggle,
    pub okx: FeedToggle,
}

impl Default for FeedToggles {
    fn default() -> Self {
        Self {
            gate: FeedToggle::On,
            binance: FeedToggle::Auto,
            bybit: FeedToggle::Auto,
            bitget: FeedToggle::Auto,
            okx: FeedToggle::Auto,
        }
    }
}
