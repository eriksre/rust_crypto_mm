use serde::Deserialize;

use crate::exchanges::endpoints::LighterGet;

#[derive(Debug, Clone, Deserialize)]
pub struct LighterMarketMeta {
    pub symbol: String,
    pub market_id: u32,
    pub price_decimals: u32,
    pub size_decimals: u32,
}

#[derive(Debug, Clone, Deserialize)]
struct OrderBookEntry {
    symbol: String,
    market_id: u32,
    status: String,
    supported_price_decimals: u32,
    supported_size_decimals: u32,
}

#[derive(Debug, Clone, Deserialize)]
struct OrderBooksResponse {
    #[serde(default)]
    order_books: Vec<OrderBookEntry>,
}

pub fn normalize_symbol(sym: &str) -> String {
    let upper = sym.trim().to_ascii_uppercase().replace('/', "");
    if let Some((base, quote)) = upper.split_once('_') {
        // Drop common quote suffixes; otherwise, keep both parts together.
        if matches!(quote, "USDT" | "USDC" | "USD" | "US") {
            base.to_string()
        } else {
            format!("{base}{quote}")
        }
    } else {
        upper
    }
}

pub fn fetch_market_meta(symbol: &str) -> Option<LighterMarketMeta> {
    let url = format!("{}{}", LighterGet::BASE, LighterGet::ORDER_BOOKS);
    let target = normalize_symbol(symbol);

    let rt = tokio::runtime::Runtime::new().ok()?;
    rt.block_on(async move {
        let client = reqwest::Client::new();
        let resp = client.get(url).send().await.ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let data: OrderBooksResponse = resp.json().await.ok()?;
        data.order_books
            .into_iter()
            .find(|entry| {
                entry.status.eq_ignore_ascii_case("active")
                    && (entry.symbol.eq_ignore_ascii_case(&target)
                        || normalize_symbol(&entry.symbol) == target)
            })
            .map(|entry| LighterMarketMeta {
                symbol: entry.symbol,
                market_id: entry.market_id,
                price_decimals: entry.supported_price_decimals,
                size_decimals: entry.supported_size_decimals,
            })
    })
}

pub async fn fetch_market_meta_async(symbol: &str) -> Option<LighterMarketMeta> {
    let url = format!("{}{}", LighterGet::BASE, LighterGet::ORDER_BOOKS);
    let target = normalize_symbol(symbol);
    let client = reqwest::Client::new();
    let resp = client.get(url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let data: OrderBooksResponse = resp.json().await.ok()?;
    data.order_books
        .into_iter()
        .find(|entry| {
            entry.status.eq_ignore_ascii_case("active")
                && (entry.symbol.eq_ignore_ascii_case(&target)
                    || normalize_symbol(&entry.symbol) == target)
        })
        .map(|entry| LighterMarketMeta {
            symbol: entry.symbol,
            market_id: entry.market_id,
            price_decimals: entry.supported_price_decimals,
            size_decimals: entry.supported_size_decimals,
        })
}
