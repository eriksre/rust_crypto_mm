/// Common trait for exchange-specific order book implementations.
/// This provides a unified interface across all exchanges (Gate, Bybit, Binance, Bitget).
pub trait OrderBookOps {
    /// Returns the mid price as f64, or None if the book is empty.
    fn mid_price_f64(&self) -> Option<f64>;

    /// Returns the top N levels of the order book as (price, quantity) tuples.
    /// Returns (bids, asks) where bids are sorted descending and asks are sorted ascending.
    fn top_levels_f64(&self, depth: usize) -> (Vec<(f64, f64)>, Vec<(f64, f64)>);

    /// Returns true if the order book has been initialized with a snapshot.
    fn is_initialized(&self) -> bool;

    /// Returns true if the order book is empty (no bids or asks).
    fn is_empty(&self) -> bool;

    /// Returns the best bid as (price, quantity), or None if no bids exist.
    fn best_bid_f64(&self) -> Option<(f64, f64)>;

    /// Returns the best ask as (price, quantity), or None if no asks exist.
    fn best_ask_f64(&self) -> Option<(f64, f64)>;

    /// Clears all data from the order book.
    fn clear(&mut self);
}

/// Extension trait for order books that support spread calculation.
pub trait OrderBookSpread: OrderBookOps {
    /// Returns the spread in absolute price units, or None if the book is empty.
    fn spread(&self) -> Option<f64> {
        let bid = self.best_bid_f64()?.0;
        let ask = self.best_ask_f64()?.0;
        Some(ask - bid)
    }

    /// Returns the spread in basis points, or None if the book is empty.
    fn spread_bps(&self) -> Option<f64> {
        let mid = self.mid_price_f64()?;
        let spread = self.spread()?;
        if mid > 0.0 {
            Some((spread / mid) * 10_000.0)
        } else {
            None
        }
    }
}

// Blanket implementation: any type implementing OrderBookOps also gets OrderBookSpread
impl<T: OrderBookOps> OrderBookSpread for T {}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockBook {
        bid: Option<(f64, f64)>,
        ask: Option<(f64, f64)>,
    }

    impl OrderBookOps for MockBook {
        fn mid_price_f64(&self) -> Option<f64> {
            let bid = self.bid?.0;
            let ask = self.ask?.0;
            Some((bid + ask) / 2.0)
        }

        fn top_levels_f64(&self, _depth: usize) -> (Vec<(f64, f64)>, Vec<(f64, f64)>) {
            (
                self.bid.into_iter().collect(),
                self.ask.into_iter().collect(),
            )
        }

        fn is_initialized(&self) -> bool {
            self.bid.is_some() && self.ask.is_some()
        }

        fn is_empty(&self) -> bool {
            self.bid.is_none() || self.ask.is_none()
        }

        fn best_bid_f64(&self) -> Option<(f64, f64)> {
            self.bid
        }

        fn best_ask_f64(&self) -> Option<(f64, f64)> {
            self.ask
        }

        fn clear(&mut self) {
            self.bid = None;
            self.ask = None;
        }
    }

    #[test]
    fn test_spread() {
        let book = MockBook {
            bid: Some((100.0, 10.0)),
            ask: Some((101.0, 10.0)),
        };
        assert_eq!(book.spread(), Some(1.0));
    }

    #[test]
    fn test_spread_bps() {
        let book = MockBook {
            bid: Some((100.0, 10.0)),
            ask: Some((102.0, 10.0)),
        };
        // Spread = 2.0, Mid = 101.0, BPS = (2.0 / 101.0) * 10_000 ≈ 198.02
        let bps = book.spread_bps().unwrap();
        assert!((bps - 198.02).abs() < 0.1);
    }
}
