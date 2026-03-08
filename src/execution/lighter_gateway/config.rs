use super::*;
use crate::exchanges::lighter::rest::normalize_symbol;

#[derive(Debug, Clone)]
pub struct LighterCredentials {
    pub api_key_hex: String,
    pub account_index: i64,
    pub api_key_index: i32,
    pub base_url: String,
    pub signer_lib: String,
    pub chain_id: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct LighterInstrument {
    pub symbol: String,
    pub market_id: u32,
    pub price_decimals: u32,
    pub size_decimals: u32,
}

impl LighterInstrument {
    pub fn normalized_symbol(&self) -> String {
        normalize_symbol(&self.symbol)
    }

    pub fn matches_symbol(&self, candidate: &str) -> bool {
        normalize_symbol(candidate) == self.normalized_symbol()
    }

    pub fn market_index_u8(&self) -> Result<u8> {
        u8::try_from(self.market_id).map_err(|_| {
            anyhow!(
                "lighter market_id {} exceeds signer/ws u8 limit for symbol {}",
                self.market_id,
                self.symbol
            )
        })
    }
}

#[derive(Debug, Clone)]
pub struct LighterGatewayConfig {
    pub creds: LighterCredentials,
    pub instrument: LighterInstrument,
    pub debug_prints: bool,
    pub suppress_sendtx_quota_logs: bool,
}

#[cfg(test)]
mod tests {
    use super::LighterInstrument;

    #[test]
    fn lighter_instrument_matches_normalized_symbols() {
        let instrument = LighterInstrument {
            symbol: "BTC".to_string(),
            market_id: 1,
            price_decimals: 2,
            size_decimals: 3,
        };
        assert!(instrument.matches_symbol("BTC_USDT"));
        assert!(instrument.matches_symbol("btc"));
        assert!(!instrument.matches_symbol("ETH_USDT"));
    }

    #[test]
    fn lighter_instrument_rejects_market_ids_that_exceed_u8() {
        let instrument = LighterInstrument {
            symbol: "BTC".to_string(),
            market_id: 300,
            price_decimals: 2,
            size_decimals: 3,
        };
        let err = instrument
            .market_index_u8()
            .expect_err("market_id should overflow u8");
        assert!(
            err.to_string().contains("exceeds signer/ws u8 limit"),
            "unexpected error: {err:#}"
        );
    }
}
