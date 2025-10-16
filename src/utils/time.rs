use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Returns the current Unix timestamp in seconds.
#[inline]
pub fn current_unix_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before epoch")
        .as_secs() as i64
}

/// Returns the current Unix timestamp in milliseconds.
#[inline]
pub fn current_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before epoch")
        .as_millis() as u64
}

/// Returns the current Unix timestamp in nanoseconds.
#[inline]
pub fn current_unix_ns() -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before epoch");
    now.as_secs()
        .saturating_mul(1_000_000_000)
        .saturating_add(now.subsec_nanos() as u64)
}

/// Formats the current Unix timestamp as a string with nanosecond precision.
/// Format: "seconds.nanoseconds" (e.g., "1234567890.123456789")
pub fn current_unix_seconds_string() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before epoch");
    let secs = now.as_secs();
    let nanos = now.subsec_nanos();
    format!("{}.{:09}", secs, nanos)
}

/// Converts an exchange-supplied millisecond timestamp to nanoseconds, saturating on overflow.
#[inline]
pub fn ms_to_ns(ts_ms: u64) -> u64 {
    ((ts_ms as u128).saturating_mul(1_000_000)).min(u64::MAX as u128) as u64
}

/// Converts a Duration to microseconds.
#[inline]
pub fn dur_us(d: Duration) -> u128 {
    d.as_micros()
}

/// Converts a Duration to milliseconds.
#[inline]
pub fn dur_ms(d: Duration) -> u128 {
    d.as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_current_unix_ts() {
        let ts = current_unix_ts();
        assert!(ts > 1_600_000_000); // After Sep 2020
        assert!(ts < 2_000_000_000); // Before May 2033
    }

    #[test]
    fn test_current_unix_ms() {
        let ms = current_unix_ms();
        assert!(ms > 1_600_000_000_000);
    }

    #[test]
    fn test_current_unix_ns() {
        let ns = current_unix_ns();
        assert!(ns > 1_600_000_000_000_000_000);
    }

    #[test]
    fn test_duration_conversion() {
        let d = Duration::from_micros(1500);
        assert_eq!(dur_us(d), 1500);
        assert_eq!(dur_ms(d), 1);
    }

    #[test]
    fn test_seconds_string_format() {
        let s = current_unix_seconds_string();
        assert!(s.contains('.'));
        let parts: Vec<&str> = s.split('.').collect();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[1].len(), 9); // 9 nanosecond digits
    }

    #[test]
    fn test_ms_to_ns_basic() {
        assert_eq!(ms_to_ns(0), 0);
        assert_eq!(ms_to_ns(123), 123_000_000);
    }

    #[test]
    fn test_ms_to_ns_saturating() {
        let big = u64::MAX / 1_000_000 + 1;
        assert_eq!(ms_to_ns(big), u64::MAX);
    }
}
