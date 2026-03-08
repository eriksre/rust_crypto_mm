use super::*;

#[derive(Clone)]
pub struct LighterAuthClient {
    signer: SignerHandle,
    creds: LighterCredentials,
}

impl LighterAuthClient {
    pub async fn connect(creds: LighterCredentials, debug_prints: bool) -> Result<Self> {
        let signer = SignerHandle::new(creds.signer_lib.clone(), debug_prints)?;
        if debug_prints {
            eprintln!(
                "[lighter-sign] init signer base_url={} api_key_idx={} account_idx={}",
                creds.base_url, creds.api_key_index, creds.account_index
            );
        }
        signer
            .init_client(
                creds.base_url.clone(),
                creds.api_key_hex.clone(),
                creds.chain_id.unwrap_or(304),
                creds.api_key_index,
                creds.account_index,
            )
            .await?;
        Ok(Self { signer, creds })
    }

    pub async fn auth_token(&self) -> Result<String> {
        let deadline = current_unix_ts() + 10 * 60;
        self.signer
            .auth_token(deadline, self.creds.api_key_index, self.creds.account_index)
            .await
    }
}

pub async fn lighter_auth_token(creds: &LighterCredentials, debug_prints: bool) -> Result<String> {
    let client = LighterAuthClient::connect(creds.clone(), debug_prints).await?;
    client.auth_token().await
}
