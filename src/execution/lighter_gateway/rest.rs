use super::*;

#[derive(Clone)]
pub(super) struct LighterRestClient {
    signer: SignerHandle,
    creds: LighterCredentials,
    market_index: u8,
    http: Client,
    api_base: Url,
    debug_prints: bool,
    last_api_call: Arc<Mutex<Option<Instant>>>,
}

impl LighterRestClient {
    pub(super) fn new(
        signer: SignerHandle,
        creds: LighterCredentials,
        market_index: u8,
        http: Client,
        api_base: Url,
        debug_prints: bool,
        last_api_call: Arc<Mutex<Option<Instant>>>,
    ) -> Self {
        Self {
            signer,
            creds,
            market_index,
            http,
            api_base,
            debug_prints,
            last_api_call,
        }
    }

    fn log_api_call(&self, method: &str, url: &str, body: Option<&str>) {
        log_api_call(self.last_api_call.as_ref(), method, url, body);
    }

    pub(super) async fn fetch_auth_token(&self) -> Result<String> {
        let deadline = current_unix_ts() + 10 * 60;
        self.signer
            .auth_token(deadline, self.creds.api_key_index, self.creds.account_index)
            .await
    }

    pub(super) async fn fetch_tx_by_hash(&self, hash: &str) -> Result<LighterTxStatusResponse> {
        let url = self.api_base.join("api/v1/tx")?;
        let query_pairs = vec![("by", "hash".to_string()), ("value", hash.to_string())];
        let query = format_query_string(&query_pairs);
        let full_url = format!("{}?{}", url.as_str(), query);
        self.log_api_call("GET", &full_url, None);
        let resp = self
            .http
            .get(url)
            .query(&query_pairs)
            .send()
            .await
            .with_context(|| format!("tx lookup request failed for hash {hash}"))?;
        let status = resp.status();
        let body = resp
            .text()
            .await
            .with_context(|| format!("tx lookup body read failed for hash {hash}"))?;
        if !status.is_success() {
            bail!(
                "tx lookup HTTP {} hash={} body={}",
                status,
                hash,
                truncate_for_log(&body, 512)
            );
        }
        parse_tx_lookup_response(&body, hash)
    }

    pub(super) async fn fetch_nonce(&self) -> Result<i64> {
        let url = self.api_base.join("api/v1/nextNonce")?;
        let query_pairs = vec![
            ("account_index", self.creds.account_index.to_string()),
            ("api_key_index", self.creds.api_key_index.to_string()),
        ];
        let query = format_query_string(&query_pairs);
        let full_url = format!("{}?{}", url.as_str(), query);
        self.log_api_call("GET", &full_url, None);
        let resp = self
            .http
            .get(url)
            .query(&query_pairs)
            .send()
            .await
            .context("nextNonce request failed")?;
        let status = resp.status();
        let body = resp.text().await.context("nextNonce read body failed")?;
        if !status.is_success() {
            bail!("nextNonce HTTP {} body: {}", status, body);
        }
        let data: NextNonceResponse =
            serde_json::from_str(&body).context("invalid nextNonce json")?;
        if data.code != 200 {
            bail!(
                "nextNonce error {}: {}",
                data.code,
                data.message.unwrap_or_default()
            );
        }
        data.nonce
            .ok_or_else(|| anyhow!("nextNonce response missing nonce"))
    }

    pub(super) async fn fetch_nonce_with_backoff(&self, ctx: &str) -> Result<i64> {
        let mut sleep_ms: u64 = 200;
        for attempt in 0..10 {
            let url = self.api_base.join("api/v1/nextNonce")?;
            let query_pairs = vec![
                ("account_index", self.creds.account_index.to_string()),
                ("api_key_index", self.creds.api_key_index.to_string()),
            ];
            let query = format_query_string(&query_pairs);
            let full_url = format!("{}?{}", url.as_str(), query);
            self.log_api_call("GET", &full_url, None);
            let resp = self
                .http
                .get(url)
                .query(&query_pairs)
                .send()
                .await
                .context("nextNonce request failed")?;

            let status = resp.status();
            let retry_after = resp
                .headers()
                .get("retry-after")
                .and_then(|v| match v.to_str() {
                    Ok(s) => match s.parse::<u64>() {
                        Ok(v) => Some(v),
                        Err(err) => {
                            log_parse_drop("lighter_gateway", "retry_after", &err, s);
                            None
                        }
                    },
                    Err(err) => {
                        log_parse_drop("lighter_gateway", "retry_after", &err, "<non-utf8>");
                        None
                    }
                });

            let body = resp.text().await.context("nextNonce read body failed")?;
            if status.is_success() {
                let data: NextNonceResponse =
                    serde_json::from_str(&body).context("invalid nextNonce json")?;
                if data.code != 200 {
                    bail!(
                        "nextNonce error {}: {}",
                        data.code,
                        data.message.unwrap_or_default()
                    );
                }
                return data
                    .nonce
                    .ok_or_else(|| anyhow!("nextNonce response missing nonce"));
            }

            let retryable = status.as_u16() == 429
                || status.as_u16() == 500
                || status.as_u16() == 502
                || status.as_u16() == 503
                || status.as_u16() == 504;
            if !retryable {
                bail!("nextNonce HTTP {} body: {}", status, body);
            }

            let wait = if let Some(secs) = retry_after {
                (secs.saturating_mul(1000)).min(5_000)
            } else {
                let jitter = (current_unix_ms() as u64 % 73).min(72);
                (sleep_ms + jitter).min(5_000)
            };
            eprintln!(
                "[lighter-nonce] nextNonce {} attempt={} status={} waiting_ms={} (body_len={})",
                ctx,
                attempt + 1,
                status,
                wait,
                body.len()
            );
            tokio::time::sleep(Duration::from_millis(wait)).await;
            sleep_ms = (sleep_ms.saturating_mul(2)).min(5_000);
        }
        bail!("nextNonce {}: retry exhausted (HTTP 429/5xx)", ctx)
    }

    pub(super) async fn fetch_active_orders(&self) -> Result<Vec<LighterOrderEntry>> {
        let token = self.fetch_auth_token().await?;
        let url = self.api_base.join("api/v1/accountActiveOrders")?;
        let query_pairs = vec![
            ("account_index", self.creds.account_index.to_string()),
            ("market_id", self.market_index.to_string()),
        ];
        let query = format_query_string(&query_pairs);
        let full_url = format!("{}?{}", url.as_str(), query);
        self.log_api_call("GET", &full_url, None);
        let resp = self
            .http
            .get(url)
            .query(&query_pairs)
            .header("authorization", token)
            .send()
            .await
            .context("activeOrders request failed")?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.context("activeOrders read body failed")?;
            if status.as_u16() == 401 && self.debug_prints {
                eprintln!("[lighter-rest] activeOrders unauthorized");
            }
            bail!("activeOrders HTTP {} body: {}", status, body);
        }
        let data: OrdersEnvelope = resp.json().await.context("invalid activeOrders json")?;
        if data.code != 200 {
            bail!(
                "activeOrders error {}: {}",
                data.code,
                data.message.unwrap_or_default()
            );
        }
        Ok(data.orders)
    }

    pub(super) async fn fetch_inactive_orders(
        &self,
        limit: usize,
    ) -> Result<Vec<LighterOrderEntry>> {
        let token = self.fetch_auth_token().await?;
        let url = self.api_base.join("api/v1/accountInactiveOrders")?;
        let query_pairs = vec![
            ("account_index", self.creds.account_index.to_string()),
            ("market_id", self.market_index.to_string()),
            ("limit", limit.to_string()),
        ];
        let query = format_query_string(&query_pairs);
        let full_url = format!("{}?{}", url.as_str(), query);
        self.log_api_call("GET", &full_url, None);
        let resp = self
            .http
            .get(url)
            .query(&query_pairs)
            .header("authorization", token)
            .send()
            .await
            .context("inactiveOrders request failed")?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp
                .text()
                .await
                .context("inactiveOrders read body failed")?;
            if status.as_u16() == 401 && self.debug_prints {
                eprintln!("[lighter-rest] inactiveOrders unauthorized");
            }
            bail!("inactiveOrders HTTP {} body: {}", status, body);
        }
        let data: OrdersEnvelope = resp.json().await.context("invalid inactiveOrders json")?;
        if data.code != 200 {
            bail!(
                "inactiveOrders error {}: {}",
                data.code,
                data.message.unwrap_or_default()
            );
        }
        Ok(data.orders)
    }
}
