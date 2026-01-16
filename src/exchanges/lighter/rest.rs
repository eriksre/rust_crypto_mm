use serde::Deserialize;
use std::time::Duration;

use crate::exchanges::endpoints::LighterGet;
use crate::utils::parsing::log_parse_drop;

#[derive(Debug, Clone, Deserialize)]
pub struct LighterMarketMeta {
    pub symbol: String,
    pub market_id: u32,
    pub price_decimals: u32,
    pub size_decimals: u32,
    pub min_base_amount: f64,
    pub min_quote_amount: f64,
}

#[derive(Debug, Clone, Deserialize)]
struct OrderBookEntry {
    symbol: String,
    market_id: u32,
    status: String,
    supported_price_decimals: u32,
    supported_size_decimals: u32,
    #[serde(default)]
    min_base_amount: String,
    #[serde(default)]
    min_quote_amount: String,
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

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(err) => {
            eprintln!("ERROR: failed to create tokio runtime for Lighter REST: {err}");
            return None;
        }
    };
    rt.block_on(async move {
        let url = match reqwest::Url::parse(&url) {
            Ok(url) => url,
            Err(err) => {
                eprintln!("ERROR: invalid Lighter REST url {url}: {err}");
                return None;
            }
        };
        let client = match reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
        {
            Ok(client) => client,
            Err(err) => {
                eprintln!("ERROR: failed to build Lighter REST client: {err}");
                return None;
            }
        };
        let resp = match client.get(url.clone()).send().await {
            Ok(resp) => resp,
            Err(err) => {
                eprintln!("ERROR: Lighter REST GET {} failed: {err}", url);
                return None;
            }
        };
        let status = resp.status();
        let body = match resp.text().await {
            Ok(text) => text,
            Err(err) => {
                eprintln!("ERROR: Lighter REST read body {} failed: {err}", url);
                return None;
            }
        };
        if !status.is_success() {
            eprintln!(
                "ERROR: Lighter REST GET {} returned {} body=\"{}\"",
                url,
                status,
                body.chars().take(256).collect::<String>()
            );
            return None;
        }
        let data: OrderBooksResponse = match serde_json::from_str(&body) {
            Ok(data) => data,
            Err(err) => {
                log_parse_drop("lighter_rest", "json", &err, &body);
                return None;
            }
        };
        data.order_books
            .into_iter()
            .find(|entry| {
                entry.status.eq_ignore_ascii_case("active")
                    && (entry.symbol.eq_ignore_ascii_case(&target)
                        || normalize_symbol(&entry.symbol) == target)
            })
            .and_then(|entry| {
                let min_base_amount = match entry.min_base_amount.parse::<f64>() {
                    Ok(v) if v.is_finite() => v,
                    Ok(_) => {
                        log_parse_drop(
                            "lighter_rest",
                            "min_base_amount",
                            &"non-finite min_base_amount",
                            &entry.min_base_amount,
                        );
                        return None;
                    }
                    Err(err) => {
                        log_parse_drop("lighter_rest", "min_base_amount", &err, &entry.min_base_amount);
                        return None;
                    }
                };
                let min_quote_amount = match entry.min_quote_amount.parse::<f64>() {
                    Ok(v) if v.is_finite() => v,
                    Ok(_) => {
                        log_parse_drop(
                            "lighter_rest",
                            "min_quote_amount",
                            &"non-finite min_quote_amount",
                            &entry.min_quote_amount,
                        );
                        return None;
                    }
                    Err(err) => {
                        log_parse_drop(
                            "lighter_rest",
                            "min_quote_amount",
                            &err,
                            &entry.min_quote_amount,
                        );
                        return None;
                    }
                };
                Some(LighterMarketMeta {
                    symbol: entry.symbol,
                    market_id: entry.market_id,
                    price_decimals: entry.supported_price_decimals,
                    size_decimals: entry.supported_size_decimals,
                    min_base_amount,
                    min_quote_amount,
                })
            })
    })
}

pub async fn fetch_market_meta_async(symbol: &str) -> Option<LighterMarketMeta> {
    let url = format!("{}{}", LighterGet::BASE, LighterGet::ORDER_BOOKS);
    let target = normalize_symbol(symbol);
    let url = match reqwest::Url::parse(&url) {
        Ok(url) => url,
        Err(err) => {
            eprintln!("ERROR: invalid Lighter REST url {url}: {err}");
            return None;
        }
    };
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
    {
        Ok(client) => client,
        Err(err) => {
            eprintln!("ERROR: failed to build Lighter REST client: {err}");
            return None;
        }
    };
    let resp = match client.get(url.clone()).send().await {
        Ok(resp) => resp,
        Err(err) => {
            eprintln!("ERROR: Lighter REST GET {} failed: {err}", url);
            return None;
        }
    };
    let status = resp.status();
    let body = match resp.text().await {
        Ok(text) => text,
        Err(err) => {
            eprintln!("ERROR: Lighter REST read body {} failed: {err}", url);
            return None;
        }
    };
    if !status.is_success() {
        eprintln!(
            "ERROR: Lighter REST GET {} returned {} body=\"{}\"",
            url,
            status,
            body.chars().take(256).collect::<String>()
        );
        return None;
    }
    let data: OrderBooksResponse = match serde_json::from_str(&body) {
        Ok(data) => data,
        Err(err) => {
            log_parse_drop("lighter_rest", "json", &err, &body);
            return None;
        }
    };
    data.order_books
        .into_iter()
        .find(|entry| {
            entry.status.eq_ignore_ascii_case("active")
                && (entry.symbol.eq_ignore_ascii_case(&target)
                    || normalize_symbol(&entry.symbol) == target)
        })
        .and_then(|entry| {
            let min_base_amount = match entry.min_base_amount.parse::<f64>() {
                Ok(v) if v.is_finite() => v,
                Ok(_) => {
                    log_parse_drop(
                        "lighter_rest",
                        "min_base_amount",
                        &"non-finite min_base_amount",
                        &entry.min_base_amount,
                    );
                    return None;
                }
                Err(err) => {
                    log_parse_drop("lighter_rest", "min_base_amount", &err, &entry.min_base_amount);
                    return None;
                }
            };
            let min_quote_amount = match entry.min_quote_amount.parse::<f64>() {
                Ok(v) if v.is_finite() => v,
                Ok(_) => {
                    log_parse_drop(
                        "lighter_rest",
                        "min_quote_amount",
                        &"non-finite min_quote_amount",
                        &entry.min_quote_amount,
                    );
                    return None;
                }
                Err(err) => {
                    log_parse_drop(
                        "lighter_rest",
                        "min_quote_amount",
                        &err,
                        &entry.min_quote_amount,
                    );
                    return None;
                }
            };
            Some(LighterMarketMeta {
                symbol: entry.symbol,
                market_id: entry.market_id,
                price_decimals: entry.supported_price_decimals,
                size_decimals: entry.supported_size_decimals,
                min_base_amount,
                min_quote_amount,
            })
        })
}
