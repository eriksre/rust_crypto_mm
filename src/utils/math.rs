/// Formats a price by removing trailing zeros and decimal point if unnecessary.
/// Example: 100.50000000 -> "100.5", 100.00000000 -> "100"
pub fn format_price(price: f64) -> String {
    let formatted = format!("{:.8}", price);
    formatted
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}

/// Formats an f64 value with specified precision, removing trailing zeros.
pub fn format_f64(value: f64) -> String {
    format_f64_with_precision(value, 8)
}

/// Formats an f64 value with custom precision, removing trailing zeros.
pub fn format_f64_with_precision(value: f64, precision: usize) -> String {
    let formatted = format!("{:.width$}", value, width = precision);
    formatted
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}

/// Rounds a price down to the nearest tick size.
#[inline]
pub fn round_down_to_tick(price: f64, tick_size: f64) -> f64 {
    if tick_size <= 0.0 || !tick_size.is_finite() {
        return price;
    }
    (price / tick_size).floor() * tick_size
}

/// Rounds a price up to the nearest tick size.
#[inline]
pub fn round_up_to_tick(price: f64, tick_size: f64) -> f64 {
    if tick_size <= 0.0 || !tick_size.is_finite() {
        return price;
    }
    (price / tick_size).ceil() * tick_size
}

/// Rounds a price to the nearest tick size.
#[inline]
pub fn round_to_tick(price: f64, tick_size: f64) -> f64 {
    if tick_size <= 0.0 || !tick_size.is_finite() {
        return price;
    }
    (price / tick_size).round() * tick_size
}

/// Calculates the basis points (bps) change between two prices.
/// Returns the absolute percentage change * 10,000.
#[inline]
pub fn price_change_bps(old_price: f64, new_price: f64) -> f64 {
    if old_price <= 0.0 {
        return 0.0;
    }
    ((new_price - old_price).abs() / old_price) * 10_000.0
}

/// Calculates the mid price from bid and ask.
#[inline]
pub fn mid_price(bid: f64, ask: f64) -> f64 {
    (bid + ask) / 2.0
}

/// Applies a basis point offset to a price.
/// Example: apply_bps_offset(100.0, 50.0) = 100.5 (50 bps = 0.5%)
#[inline]
pub fn apply_bps_offset(price: f64, bps: f64) -> f64 {
    price * (1.0 + bps / 10_000.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_price() {
        assert_eq!(format_price(100.50000000), "100.5");
        assert_eq!(format_price(100.00000000), "100");
        assert_eq!(format_price(0.12345678), "0.12345678");
        assert_eq!(format_price(0.10000000), "0.1");
    }

    #[test]
    fn test_format_f64() {
        assert_eq!(format_f64(123.456), "123.456");
        assert_eq!(format_f64(100.0), "100");
    }

    #[test]
    fn test_round_to_tick() {
        assert_eq!(round_down_to_tick(100.7, 0.5), 100.5);
        assert_eq!(round_up_to_tick(100.3, 0.5), 100.5);
        assert_eq!(round_to_tick(100.7, 0.5), 100.5);
        assert_eq!(round_to_tick(100.8, 0.5), 101.0);
    }

    #[test]
    fn test_price_change_bps() {
        assert_eq!(price_change_bps(100.0, 100.5), 50.0); // 0.5% = 50 bps
        assert_eq!(price_change_bps(100.0, 101.0), 100.0); // 1% = 100 bps
        assert_eq!(price_change_bps(0.0, 100.0), 0.0); // Invalid old price
    }

    #[test]
    fn test_mid_price() {
        assert_eq!(mid_price(100.0, 102.0), 101.0);
        assert_eq!(mid_price(99.5, 100.5), 100.0);
    }

    #[test]
    fn test_apply_bps_offset() {
        let result = apply_bps_offset(100.0, 50.0);
        assert!((result - 100.5).abs() < 1e-10); // 50 bps = 0.5%
    }
}
