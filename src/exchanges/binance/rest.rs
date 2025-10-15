#![allow(dead_code)]

#[cfg(feature = "binance_book")]
use serde::Deserialize;

use crate::exchanges::endpoints::BinanceGet as Endp;

#[cfg(feature = "binance_book")]
#[derive(Debug, Clone, Deserialize)]
pub struct BinanceSnapshot {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: u64,
    pub bids: Vec<[String; 2]>,
    pub asks: Vec<[String; 2]>,
}

#[cfg(feature = "binance_book")]
pub async fn get_orderbook_snapshot(
    symbol: &str,
    limit: usize,
) -> Result<BinanceSnapshot, reqwest::Error> {
    let symbol_uc = symbol.to_uppercase();
    // Binance REST endpoints expect symbols without underscore separators (e.g. BTCUSDT).
    let symbol_param = symbol_uc.replace('_', "");
    let path = format!("/depth?symbol={}&limit={}", symbol_param, limit);
    let url = format!("{}{}", Endp::BASE, path);
    let resp = reqwest::Client::new()
        .get(url)
        .send()
        .await?
        .error_for_status()?;
    let snap = resp.json::<BinanceSnapshot>().await?;
    Ok(snap)
}
