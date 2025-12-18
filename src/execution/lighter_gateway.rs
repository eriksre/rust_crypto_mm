use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_longlong};
use std::sync::mpsc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use libloading::{Library, Symbol};
use parking_lot::Mutex;
use reqwest::{Client, Url};
use serde::Deserialize;
use serde_json;
use tokio::sync::{Mutex as AsyncMutex, oneshot};

use crate::base_classes::types::Side;
use crate::execution::types::{
    ClientOrderId, ExchangeOrderId, ExecutionReport, OrderAck, OrderStatus, QuoteIntent,
    TimeInForce,
};
use crate::utils::time::current_unix_ms;

use super::gateway::ExecutionGateway;

#[derive(Debug, Clone)]
pub struct LighterCredentials {
    pub api_key_hex: String,
    pub account_index: i64,
    pub api_key_index: i32,
    pub base_url: String,
    pub signer_lib: String,
    pub chain_id: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct SendTxBatchResponse {
    #[serde(default)]
    code: i32,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    tx_hash: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
struct OrdersEnvelope {
    #[serde(default)]
    code: i32,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    orders: Vec<LighterOrderEntry>,
}

#[derive(Debug, Deserialize)]
struct NextNonceResponse {
    #[serde(default)]
    code: i32,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    nonce: Option<i64>,
}

#[derive(Debug, Deserialize, Clone)]
struct LighterOrderEntry {
    #[serde(default)]
    order_index: Option<i64>,
    #[serde(default)]
    client_order_index: Option<i64>,
    #[serde(default)]
    market_index: Option<u32>,
    #[serde(default)]
    price: Option<String>,
    #[serde(default)]
    initial_base_amount: Option<String>,
    #[serde(default)]
    remaining_base_amount: Option<String>,
    #[serde(default)]
    filled_base_amount: Option<String>,
    #[serde(default)]
    is_ask: Option<bool>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    timestamp: Option<i64>,
}

#[repr(C)]
// Matches the native signer return type used by the Python SDK: string payload + optional error.
struct SignerResp {
    str_ptr: *mut c_char,
    err: *mut c_char,
}

#[repr(C)]
// Matches newer lighter-go sharedlib SignedTxResponse (tx_type + tx_info + optional tx_hash/message + err).
struct SignedTxRespV1 {
    tx_type: u8,
    tx_info: *mut c_char,
    tx_hash: *mut c_char,
    message_to_sign: *mut c_char,
    err: *mut c_char,
}

type CreateClientFn =
    unsafe extern "C" fn(*const c_char, *const c_char, c_int, c_int, c_longlong) -> *mut c_char;
type SwitchApiKeyFn = unsafe extern "C" fn(c_int) -> *mut c_char;
// ABI v0 (older signer): 11 args for create order and 3 for cancel (matching the Python SDK bundled here).
type SignCreateOrderFnV0 = unsafe extern "C" fn(
    c_int,      // market_index
    c_longlong, // client_order_index
    c_longlong, // base_amount
    c_int,      // price
    c_int,      // is_ask
    c_int,      // order_type
    c_int,      // time_in_force
    c_int,      // reduce_only
    c_int,      // trigger_price
    c_longlong, // order_expiry
    c_longlong, // nonce
) -> SignerResp;

type SignCancelOrderFnV0 = unsafe extern "C" fn(c_int, c_longlong, c_longlong) -> SignerResp;
type CreateAuthTokenFnV0 = unsafe extern "C" fn(c_longlong) -> SignerResp;

// ABI v1 (newer lighter-go sharedlib): extra (api_key_idx, account_idx) args and a richer return type.
type SignCreateOrderFnV1 = unsafe extern "C" fn(
    c_int,      // market_index
    c_longlong, // client_order_index
    c_longlong, // base_amount
    c_int,      // price
    c_int,      // is_ask
    c_int,      // order_type
    c_int,      // time_in_force
    c_int,      // reduce_only
    c_int,      // trigger_price
    c_longlong, // order_expiry
    c_longlong, // nonce
    c_int,      // api_key_idx
    c_longlong, // account_idx
) -> SignedTxRespV1;

type SignCancelOrderFnV1 = unsafe extern "C" fn(
    c_int,      // market_index
    c_longlong, // order_index
    c_longlong, // nonce
    c_int,      // api_key_idx
    c_longlong, // account_idx
) -> SignedTxRespV1;

type CreateAuthTokenFnV1 = unsafe extern "C" fn(c_longlong, c_int, c_longlong) -> SignerResp;
type CheckClientFn = unsafe extern "C" fn(c_int, c_longlong) -> *mut c_char;

enum LighterSignerAbi {
    V0 {
        switch_api_key: SwitchApiKeyFn,
        sign_create_order: SignCreateOrderFnV0,
        sign_cancel_order: SignCancelOrderFnV0,
        create_auth_token: CreateAuthTokenFnV0,
    },
    V1 {
        sign_create_order: SignCreateOrderFnV1,
        sign_cancel_order: SignCancelOrderFnV1,
        create_auth_token: CreateAuthTokenFnV1,
    },
}

struct LighterSigner {
    _lib: Library,
    create_client: CreateClientFn,
    check_client: CheckClientFn,
    abi: LighterSignerAbi,
}

impl LighterSigner {
    fn load(lib_path: &str) -> Result<Self> {
        // Avoid a confusing libloading error when the wrong file type is configured
        // (e.g. a macOS .dylib on Linux => "invalid ELF header").
        if !cfg!(target_os = "macos") {
            let header = std::fs::read(lib_path)
                .ok()
                .and_then(|b| if b.len() >= 4 { Some([b[0], b[1], b[2], b[3]]) } else { None });
            if let Some(h) = header {
                // 0x7F 'E' 'L' 'F'
                if h != [0x7F, 0x45, 0x4C, 0x46] {
                    bail!(
                        "Lighter signer at {lib_path} is not an ELF shared object; \
                         on Linux you need a .so built for your architecture (e.g. signer-arm64.so for aarch64)"
                    );
                }
            }
        }
        let lib = unsafe { Library::new(lib_path) }
            .with_context(|| format!("failed to load Lighter signer library at {lib_path}"))?;

        unsafe {
            let create_client: Symbol<CreateClientFn> =
                lib.get(b"CreateClient\0").context("missing CreateClient")?;
            let check_client: Symbol<CheckClientFn> =
                lib.get(b"CheckClient\0").context("missing CheckClient")?;

            // ABI detection:
            // - Older signer (bundled in this repo as signer-amd64.so) exports SwitchAPIKey and uses v0 signatures.
            // - Newer lighter-go sharedlib (release artifacts like lighter-signer-linux-arm64.so) does NOT export
            //   SwitchAPIKey and uses v1 signatures (extra api_key_idx/account_idx + richer return types).
            let abi = match lib.get::<SwitchApiKeyFn>(b"SwitchAPIKey\0") {
                Ok(switch_api_key) => {
                    let sign_create_order: Symbol<SignCreateOrderFnV0> = lib
                        .get(b"SignCreateOrder\0")
                        .context("missing SignCreateOrder")?;
                    let sign_cancel_order: Symbol<SignCancelOrderFnV0> = lib
                        .get(b"SignCancelOrder\0")
                        .context("missing SignCancelOrder")?;
                    let create_auth_token: Symbol<CreateAuthTokenFnV0> = lib
                        .get(b"CreateAuthToken\0")
                        .context("missing CreateAuthToken")?;
                    LighterSignerAbi::V0 {
                        switch_api_key: *switch_api_key,
                        sign_create_order: *sign_create_order,
                        sign_cancel_order: *sign_cancel_order,
                        create_auth_token: *create_auth_token,
                    }
                }
                Err(_) => {
                    let sign_create_order: Symbol<SignCreateOrderFnV1> = lib
                        .get(b"SignCreateOrder\0")
                        .context("missing SignCreateOrder")?;
                    let sign_cancel_order: Symbol<SignCancelOrderFnV1> = lib
                        .get(b"SignCancelOrder\0")
                        .context("missing SignCancelOrder")?;
                    let create_auth_token: Symbol<CreateAuthTokenFnV1> = lib
                        .get(b"CreateAuthToken\0")
                        .context("missing CreateAuthToken")?;
                    LighterSignerAbi::V1 {
                        sign_create_order: *sign_create_order,
                        sign_cancel_order: *sign_cancel_order,
                        create_auth_token: *create_auth_token,
                    }
                }
            };

            Ok(Self {
                create_client: *create_client,
                check_client: *check_client,
                _lib: lib,
                abi,
            })
        }
    }

    fn cstring(s: &str) -> Result<CString> {
        CString::new(s).map_err(|_| anyhow!("invalid string for FFI (embedded nul)"))
    }

    fn from_c(ptr: *mut c_char) -> Option<String> {
        if ptr.is_null() {
            return None;
        }
        let s = unsafe { CStr::from_ptr(ptr) }.to_string_lossy().to_string();
        // Avoid freeing pointers returned by the signer to sidestep allocator mismatches.
        Some(s)
    }

    fn ensure_client(
        &self,
        base_url: &str,
        private_key_hex: &str,
        chain_id: u32,
        api_key_idx: i32,
        account_idx: i64,
    ) -> Result<()> {
        let url_c = Self::cstring(base_url)?;
        let key_c = Self::cstring(private_key_hex)?;
        let err_ptr = unsafe {
            (self.create_client)(
                url_c.as_ptr(),
                key_c.as_ptr(),
                chain_id as c_int,
                api_key_idx as c_int,
                account_idx as c_longlong,
            )
        };
        if let Some(err) = Self::from_c(err_ptr) {
            bail!("CreateClient failed: {err}");
        }
        Ok(())
    }

    fn switch_api_key(&self, api_key_idx: i32) -> Result<()> {
        match self.abi {
            LighterSignerAbi::V0 { switch_api_key, .. } => {
                let err_ptr = unsafe { (switch_api_key)(api_key_idx as c_int) };
                if let Some(err) = Self::from_c(err_ptr) {
                    bail!("SwitchAPIKey failed: {err}");
                }
                Ok(())
            }
            LighterSignerAbi::V1 { .. } => Ok(()),
        }
    }

    fn check_client(&self, api_key_idx: i32, account_idx: i64) -> Result<()> {
        let err_ptr =
            unsafe { (self.check_client)(api_key_idx as c_int, account_idx as c_longlong) };
        if let Some(err) = Self::from_c(err_ptr) {
            if !err.trim().is_empty() {
                bail!("CheckClient failed: {err}");
            }
        }
        Ok(())
    }

    fn sign_create_order(
        &self,
        market_index: u8,
        client_order_index: i64,
        base_amount: i64,
        price: u32,
        is_ask: bool,
        order_type: u8,
        tif: u8,
        reduce_only: bool,
        trigger_price: u32,
        order_expiry: i64,
        nonce: i64,
        api_key_idx: i32,
        account_idx: i64,
    ) -> Result<SignedTx> {
        match self.abi {
            LighterSignerAbi::V0 {
                sign_create_order, ..
            } => {
                let resp = unsafe {
                    (sign_create_order)(
                        market_index as c_int,
                        client_order_index as c_longlong,
                        base_amount as c_longlong,
                        price as c_int,
                        is_ask as c_int,
                        order_type as c_int,
                        tif as c_int,
                        reduce_only as c_int,
                        trigger_price as c_int,
                        order_expiry as c_longlong,
                        nonce as c_longlong,
                    )
                };
                if let Some(err) = Self::from_c(resp.err) {
                    let msg = if err.trim().is_empty() {
                        "SignCreateOrder failed: signer returned empty error. Verify account_index/api_key_index/private key, and that the key is funded/authorized."
                            .to_string()
                    } else {
                        format!("SignCreateOrder failed: {err}")
                    };
                    bail!(msg);
                }
                let tx_info = Self::from_c(resp.str_ptr).ok_or_else(|| anyhow!("missing tx_info"))?;
                if tx_info.trim().is_empty() {
                    bail!("SignCreateOrder returned empty tx_info");
                }
                Ok(SignedTx {
                    // The v0 signer does not return tx_type; set the expected value explicitly.
                    tx_type: 14,
                    tx_info,
                    tx_hash: None,
                })
            }
            LighterSignerAbi::V1 {
                sign_create_order, ..
            } => {
                let resp = unsafe {
                    (sign_create_order)(
                        market_index as c_int,
                        client_order_index as c_longlong,
                        base_amount as c_longlong,
                        price as c_int,
                        is_ask as c_int,
                        order_type as c_int,
                        tif as c_int,
                        reduce_only as c_int,
                        trigger_price as c_int,
                        order_expiry as c_longlong,
                        nonce as c_longlong,
                        api_key_idx as c_int,
                        account_idx as c_longlong,
                    )
                };
                if let Some(err) = Self::from_c(resp.err) {
                    let msg = if err.trim().is_empty() {
                        "SignCreateOrder failed: signer returned empty error. Verify account_index/api_key_index/private key, and that the key is funded/authorized."
                            .to_string()
                    } else {
                        format!("SignCreateOrder failed: {err}")
                    };
                    bail!(msg);
                }
                let tx_info = Self::from_c(resp.tx_info).ok_or_else(|| anyhow!("missing tx_info"))?;
                if tx_info.trim().is_empty() {
                    bail!("SignCreateOrder returned empty tx_info");
                }
                let tx_hash = Self::from_c(resp.tx_hash).and_then(|s| {
                    let s = s.trim().to_string();
                    if s.is_empty() { None } else { Some(s) }
                });
                Ok(SignedTx {
                    tx_type: resp.tx_type,
                    tx_info,
                    tx_hash,
                })
            }
        }
    }

    fn sign_cancel_order(
        &self,
        market_index: u8,
        order_index: i64,
        nonce: i64,
        api_key_idx: i32,
        account_idx: i64,
    ) -> Result<SignedTx> {
        match self.abi {
            LighterSignerAbi::V0 {
                sign_cancel_order, ..
            } => {
                let resp = unsafe {
                    (sign_cancel_order)(
                        market_index as c_int,
                        order_index as c_longlong,
                        nonce as c_longlong,
                    )
                };
                if let Some(err) = Self::from_c(resp.err) {
                    bail!("SignCancelOrder failed: {err}");
                }
                let tx_info = Self::from_c(resp.str_ptr).ok_or_else(|| anyhow!("missing tx_info"))?;
                if tx_info.trim().is_empty() {
                    bail!("SignCancelOrder returned empty tx_info");
                }
                Ok(SignedTx {
                    tx_type: 15,
                    tx_info,
                    tx_hash: None,
                })
            }
            LighterSignerAbi::V1 {
                sign_cancel_order, ..
            } => {
                let resp = unsafe {
                    (sign_cancel_order)(
                        market_index as c_int,
                        order_index as c_longlong,
                        nonce as c_longlong,
                        api_key_idx as c_int,
                        account_idx as c_longlong,
                    )
                };
                if let Some(err) = Self::from_c(resp.err) {
                    bail!("SignCancelOrder failed: {err}");
                }
                let tx_info = Self::from_c(resp.tx_info).ok_or_else(|| anyhow!("missing tx_info"))?;
                if tx_info.trim().is_empty() {
                    bail!("SignCancelOrder returned empty tx_info");
                }
                let tx_hash = Self::from_c(resp.tx_hash).and_then(|s| {
                    let s = s.trim().to_string();
                    if s.is_empty() { None } else { Some(s) }
                });
                Ok(SignedTx {
                    tx_type: resp.tx_type,
                    tx_info,
                    tx_hash,
                })
            }
        }
    }

    fn auth_token(&self, deadline_ms: i64, api_key_idx: i32, account_idx: i64) -> Result<String> {
        let resp = match self.abi {
            LighterSignerAbi::V0 { create_auth_token, .. } => unsafe {
                (create_auth_token)(deadline_ms as c_longlong)
            },
            LighterSignerAbi::V1 { create_auth_token, .. } => unsafe {
                (create_auth_token)(deadline_ms as c_longlong, api_key_idx as c_int, account_idx as c_longlong)
            },
        };
        if let Some(err) = Self::from_c(resp.err) {
            bail!("CreateAuthToken failed: {err}");
        }
        Self::from_c(resp.str_ptr).ok_or_else(|| anyhow!("missing auth token"))
    }
}

struct SignedTx {
    tx_type: u8,
    tx_info: String,
    tx_hash: Option<String>,
}

#[derive(Debug, Clone)]
struct OrderState {
    client_order_index: i64,
    order_index: Option<i64>,
    side: Side,
    price: f64,
    size: f64,
    filled: f64,
    status: OrderStatus,
    exchange_order_id: Option<ExchangeOrderId>,
}

enum SignerRequest {
    Init {
        base_url: String,
        api_key_hex: String,
        chain_id: u32,
        api_key_idx: i32,
        account_idx: i64,
        resp: oneshot::Sender<Result<()>>,
    },
    SignCreate {
        market_index: u8,
        client_order_index: i64,
        base_amount: i64,
        price: u32,
        is_ask: bool,
        order_type: u8,
        tif: u8,
        reduce_only: bool,
        trigger_price: u32,
        order_expiry: i64,
        nonce: i64,
        api_key_idx: i32,
        account_idx: i64,
        resp: oneshot::Sender<Result<SignedTx>>,
    },
    SignCancel {
        market_index: u8,
        order_index: i64,
        nonce: i64,
        api_key_idx: i32,
        account_idx: i64,
        resp: oneshot::Sender<Result<SignedTx>>,
    },
    Auth {
        deadline_ms: i64,
        api_key_idx: i32,
        account_idx: i64,
        resp: oneshot::Sender<Result<String>>,
    },
}

#[derive(Clone)]
struct SignerHandle {
    tx: mpsc::Sender<SignerRequest>,
}

impl SignerHandle {
    fn new(lib_path: String) -> Result<Self> {
        let (tx, rx) = mpsc::channel::<SignerRequest>();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let signer = match LighterSigner::load(&lib_path) {
                Ok(s) => {
                    let _ = ready_tx.send(Ok(()));
                    s
                }
                Err(e) => {
                    let _ = ready_tx.send(Err(e));
                    return;
                }
            };
            for req in rx {
                match req {
                    SignerRequest::Init {
                        base_url,
                        api_key_hex,
                        chain_id,
                        api_key_idx,
                        account_idx,
                        resp,
                    } => {
                        let res = signer
                            .ensure_client(
                                &base_url,
                                &api_key_hex,
                                chain_id,
                                api_key_idx,
                                account_idx,
                            )
                            .and_then(|_| signer.switch_api_key(api_key_idx))
                            .and_then(|_| signer.check_client(api_key_idx, account_idx));
                        let _ = resp.send(res);
                    }
                    SignerRequest::SignCreate {
                        market_index,
                        client_order_index,
                        base_amount,
                        price,
                        is_ask,
                        order_type,
                        tif,
                        reduce_only,
                        trigger_price,
                        order_expiry,
                        nonce,
                        api_key_idx,
                        account_idx,
                        resp,
                    } => {
                        eprintln!(
                            "[lighter-sign-thread] create mid={} client_idx={} base_amt={} price={} is_ask={} tif={} ak_idx={} acct={}",
                            market_index,
                            client_order_index,
                            base_amount,
                            price,
                            is_ask,
                            tif,
                            api_key_idx,
                            account_idx
                        );
                        let res = signer
                            .switch_api_key(api_key_idx)
                            .and_then(|_| signer.check_client(api_key_idx, account_idx))
                            .and_then(|_| {
                                signer.sign_create_order(
                                    market_index,
                                    client_order_index,
                                    base_amount,
                                    price,
                                    is_ask,
                                    order_type,
                                    tif,
                                    reduce_only,
                                    trigger_price,
                                    order_expiry,
                                    nonce,
                                    api_key_idx,
                                    account_idx,
                                )
                            });
                        if let Err(ref err) = res {
                            eprintln!("[lighter-sign-thread] create error: {err}");
                        }
                        let _ = resp.send(res);
                    }
                    SignerRequest::SignCancel {
                        market_index,
                        order_index,
                        nonce,
                        api_key_idx,
                        account_idx,
                        resp,
                    } => {
                        eprintln!(
                            "[lighter-sign-thread] cancel mid={} order_idx={} ak_idx={} acct={}",
                            market_index, order_index, api_key_idx, account_idx
                        );
                        let res = signer
                            .switch_api_key(api_key_idx)
                            .and_then(|_| signer.check_client(api_key_idx, account_idx))
                            .and_then(|_| signer.sign_cancel_order(market_index, order_index, nonce, api_key_idx, account_idx));
                        if let Err(ref err) = res {
                            eprintln!("[lighter-sign-thread] cancel error: {err}");
                        }
                        let _ = resp.send(res);
                    }
                    SignerRequest::Auth {
                        deadline_ms,
                        api_key_idx,
                        account_idx,
                        resp,
                    } => {
                        let _ = resp.send(signer.auth_token(deadline_ms, api_key_idx, account_idx));
                    }
                }
            }
        });
        ready_rx
            .recv()
            .unwrap_or_else(|_| Err(anyhow!("signer thread failed to start")))?;
        Ok(Self { tx })
    }

    async fn init_client(
        &self,
        base_url: String,
        api_key_hex: String,
        chain_id: u32,
        api_key_idx: i32,
        account_idx: i64,
    ) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(SignerRequest::Init {
                base_url,
                api_key_hex,
                chain_id,
                api_key_idx,
                account_idx,
                resp: resp_tx,
            })
            .map_err(|e| anyhow!("failed to enqueue signer init: {e}"))?;
        resp_rx
            .await
            .map_err(|e| anyhow!("signer thread dropped: {e}"))?
    }

    async fn sign_order(
        &self,
        market_index: u8,
        client_order_index: i64,
        base_amount: i64,
        price: u32,
        is_ask: bool,
        order_type: u8,
        tif: u8,
        reduce_only: bool,
        trigger_price: u32,
        order_expiry: i64,
        nonce: i64,
        api_key_idx: i32,
        account_idx: i64,
    ) -> Result<SignedTx> {
        eprintln!(
            "[lighter-sign] sign_order mid={} client_idx={} base_amt={} price_int={} is_ask={} tif={} api_key_idx={} account_idx={}",
            market_index,
            client_order_index,
            base_amount,
            price,
            is_ask,
            tif,
            api_key_idx,
            account_idx
        );
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(SignerRequest::SignCreate {
                market_index,
                client_order_index,
                base_amount,
                price,
                is_ask,
                order_type,
                tif,
                reduce_only,
                trigger_price,
                order_expiry,
                nonce,
                api_key_idx,
                account_idx,
                resp: resp_tx,
            })
            .map_err(|e| anyhow!("failed to enqueue signer request: {e}"))?;
        let mut res = resp_rx
            .await
            .map_err(|e| anyhow!("signer thread dropped: {e}"))?;
        if let Ok(ref mut tx) = res {
            // Force the expected tx_type for create order (14) only if the signer returned 0.
            if tx.tx_type == 0 {
                tx.tx_type = 14;
            }
            eprintln!(
                "[lighter-sign] sign_order success tx_type={} tx_info_len={}",
                tx.tx_type,
                tx.tx_info.len()
            );
            if tx.tx_info.len() < 32 {
                eprintln!(
                    "[lighter-sign] warning: tx_info unusually short: {}",
                    tx.tx_info
                );
            }
        } else {
            eprintln!("[lighter-sign] sign_order failed: {:?}", res.as_ref().err());
        }
        res
    }

    async fn sign_cancel(
        &self,
        market_index: u8,
        order_index: i64,
        nonce: i64,
        api_key_idx: i32,
        account_idx: i64,
    ) -> Result<SignedTx> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(SignerRequest::SignCancel {
                market_index,
                order_index,
                nonce,
                api_key_idx,
                account_idx,
                resp: resp_tx,
            })
            .map_err(|e| anyhow!("failed to enqueue signer request: {e}"))?;
        let mut res = resp_rx
            .await
            .map_err(|e| anyhow!("signer thread dropped: {e}"))?;
        if let Ok(ref mut tx) = res {
            if tx.tx_type == 0 {
                tx.tx_type = 15; // cancel
            }
            eprintln!(
                "[lighter-sign] sign_cancel success tx_type={} tx_info_len={}",
                tx.tx_type,
                tx.tx_info.len()
            );
        } else {
            eprintln!(
                "[lighter-sign] sign_cancel failed: {:?}",
                res.as_ref().err()
            );
        }
        res
    }

    async fn auth_token(&self, deadline_ms: i64, api_key_idx: i32, account_idx: i64) -> Result<String> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(SignerRequest::Auth {
                deadline_ms,
                api_key_idx,
                account_idx,
                resp: resp_tx,
            })
            .map_err(|e| anyhow!("failed to enqueue signer request: {e}"))?;
        resp_rx
            .await
            .map_err(|e| anyhow!("signer thread dropped: {e}"))?
    }
}

pub struct LighterGateway {
    signer: SignerHandle,
    creds: LighterCredentials,
    market_index: u8,
    price_scale: f64,
    size_scale: f64,
    http: Client,
    api_base: Url,
    next_client_index: Mutex<i64>,
    next_nonce: Mutex<Option<i64>>,
    nonce_lock: AsyncMutex<()>,
    pending_reports: Mutex<Vec<ExecutionReport>>,
    orders: Mutex<HashMap<ClientOrderId, OrderState>>,
}

impl LighterGateway {
    pub async fn connect(
        creds: LighterCredentials,
        market_index: u32,
        price_decimals: u32,
        size_decimals: u32,
    ) -> Result<Self> {
        let signer = SignerHandle::new(creds.signer_lib.clone())?;
        let base_url = creds.base_url.clone();
        // Initialize signer client on its dedicated thread before use
        eprintln!(
            "[lighter-sign] init signer base_url={} api_key_idx={} account_idx={}",
            base_url, creds.api_key_index, creds.account_index
        );
        signer
            .init_client(
                base_url.clone(),
                creds.api_key_hex.clone(),
                creds.chain_id.unwrap_or(304),
                creds.api_key_index,
                creds.account_index,
            )
            .await?;

        let http = Client::builder().timeout(Duration::from_secs(10)).build()?;
        let api_base = Url::parse(&base_url)?;
        let gw = Self {
            signer: signer,
            creds,
            market_index: market_index as u8,
            price_scale: 10_f64.powi(price_decimals as i32),
            size_scale: 10_f64.powi(size_decimals as i32),
            http,
            api_base,
            next_client_index: Mutex::new(1),
            next_nonce: Mutex::new(None),
            nonce_lock: AsyncMutex::new(()),
            pending_reports: Mutex::new(Vec::new()),
            orders: Mutex::new(HashMap::new()),
        };
        // Seed nonces once at startup to avoid hot-looping nextNonce under load.
        // If the endpoint is temporarily rate-limited, this will back off and retry.
        let _ = gw.ensure_nonce_seed().await?;
        Ok(gw)
    }

    fn client_order_index(&self) -> i64 {
        let mut guard = self.next_client_index.lock();
        let idx = *guard;
        *guard = guard.saturating_add(1);
        idx
    }

    async fn ensure_nonce_seed(&self) -> Result<i64> {
        if let Some(n) = *self.next_nonce.lock() {
            return Ok(n);
        }
        let fresh = self.fetch_nonce_with_backoff("seed").await?;
        eprintln!("[lighter-nonce] seeded nonce={}", fresh);
        let mut guard = self.next_nonce.lock();
        *guard = Some(fresh);
        Ok(fresh)
    }

    async fn refresh_nonce_from_server(&self) -> Result<i64> {
        let fresh = self.fetch_nonce_with_backoff("refresh").await?;
        eprintln!("[lighter-nonce] refreshed nonce={}", fresh);
        let mut guard = self.next_nonce.lock();
        *guard = Some(fresh);
        Ok(fresh)
    }

    async fn allocate_nonces(&self, count: usize) -> Result<Vec<i64>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let _ = self.ensure_nonce_seed().await?;
        let mut guard = self.next_nonce.lock();
        let start = guard.expect("nonce seed must be set");
        let mut nonces = Vec::with_capacity(count);
        for i in 0..count {
            nonces.push(start + i as i64);
        }
        *guard = Some(start + count as i64);
        Ok(nonces)
    }

    fn to_price_int(&self, px: f64) -> Result<u32> {
        if !px.is_finite() || px <= 0.0 {
            bail!("invalid price {px}");
        }
        let scaled = (px * self.price_scale).round();
        if scaled > u32::MAX as f64 {
            bail!("price too large after scaling");
        }
        Ok(scaled as u32)
    }

    fn to_size_int(&self, size: f64) -> Result<i64> {
        if !size.is_finite() || size <= 0.0 {
            bail!("invalid size {size}");
        }
        Ok((size * self.size_scale).round() as i64)
    }

    async fn send_batch(&self, txs: Vec<(SignedTx, ClientOrderId)>) -> Result<Vec<OrderAck>> {
        if txs.is_empty() {
            return Ok(Vec::new());
        }
        // The API expects JSON arrays for tx_types/tx_infos, not comma-joined strings.
        let tx_types = txs.iter().map(|(t, _)| t.tx_type).collect::<Vec<_>>();
        let tx_infos = txs
            .iter()
            .map(|(t, _)| t.tx_info.as_str())
            .collect::<Vec<_>>();
        let tx_types_json = serde_json::to_string(&tx_types)?;
        let tx_infos_json = serde_json::to_string(&tx_infos)?;
        eprintln!(
            "[lighter-send] sending batch count={} tx_types={} tx_infos_len={}",
            txs.len(),
            tx_types_json,
            tx_infos_json.len()
        );

        let resp = self
            .http
            .post(self.api_base.join("api/v1/sendTxBatch")?)
            .form(&[("tx_types", tx_types_json), ("tx_infos", tx_infos_json)])
            .send()
            .await
            .context("sendTxBatch request failed")?;

        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("sendTxBatch failed HTTP {} body: {}", status, body);
        }
        let payload: SendTxBatchResponse = serde_json::from_str(&body)
            .context(format!("invalid sendTxBatch JSON body: {}", body))?;
        if payload.code != 200 {
            bail!(
                "sendTxBatch error {}: {} (body={})",
                payload.code,
                payload.message.unwrap_or_default(),
                body
            );
        }

        let mut acks = Vec::with_capacity(txs.len());
        for ((tx, client_id), idx) in txs.into_iter().zip(0..) {
            let exch = payload
                .tx_hash
                .get(idx)
                .or(tx.tx_hash.as_ref())
                .cloned()
                .map(ExchangeOrderId);
            acks.push(OrderAck {
                client_order_id: client_id,
                exchange_order_id: exch.clone(),
            });
        }
        Ok(acks)
    }

    fn is_nonce_error(err: &anyhow::Error) -> bool {
        let msg = err.to_string();
        msg.contains("invalid nonce") || msg.contains("nonce is not increasing")
    }

    fn save_order(
        &self,
        intent: &QuoteIntent,
        client_order_index: i64,
        exchange_order_id: Option<ExchangeOrderId>,
    ) {
        let mut orders = self.orders.lock();
        orders.insert(
            intent.client_order_id.clone(),
            OrderState {
                client_order_index,
                order_index: None,
                side: intent.side,
                price: intent.price,
                size: intent.size,
                filled: 0.0,
                status: OrderStatus::New,
                exchange_order_id,
            },
        );
    }

    fn push_report(&self, report: ExecutionReport) {
        let mut guard = self.pending_reports.lock();
        guard.push(report);
    }

    async fn fetch_auth_token(&self) -> Result<String> {
        // default 10 minute auth token from signer
        let deadline = (current_unix_ms() as i64) + 10 * 60 * 1000;
        self.signer
            .auth_token(deadline, self.creds.api_key_index, self.creds.account_index)
            .await
    }

    async fn fetch_nonce(&self) -> Result<i64> {
        let resp = self
            .http
            .get(self.api_base.join("api/v1/nextNonce")?)
            .query(&[
                ("account_index", self.creds.account_index.to_string()),
                ("api_key_index", self.creds.api_key_index.to_string()),
            ])
            .send()
            .await
            .context("nextNonce request failed")?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("nextNonce HTTP {} body: {}", status, body);
        }
        let data: NextNonceResponse = serde_json::from_str(&body).context("invalid nextNonce json")?;
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

    async fn fetch_nonce_with_backoff(&self, ctx: &str) -> Result<i64> {
        // nextNonce can be rate-limited (HTTP 429) if callers hot-loop when the nonce isn't seeded.
        // We back off aggressively to avoid hammering the endpoint.
        let mut sleep_ms: u64 = 200;
        for attempt in 0..10 {
            let resp = self
                .http
                .get(self.api_base.join("api/v1/nextNonce")?)
                .query(&[
                    ("account_index", self.creds.account_index.to_string()),
                    ("api_key_index", self.creds.api_key_index.to_string()),
                ])
                .send()
                .await
                .context("nextNonce request failed")?;

            let status = resp.status();
            let retry_after = resp
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok());

            let body = resp.text().await.unwrap_or_default();
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

            // Retry on rate limiting and transient gateway errors.
            let retryable = status.as_u16() == 429
                || status.as_u16() == 500
                || status.as_u16() == 502
                || status.as_u16() == 503
                || status.as_u16() == 504;
            if !retryable {
                bail!("nextNonce HTTP {} body: {}", status, body);
            }

            let wait = if let Some(secs) = retry_after {
                // Respect server hint, but cap it.
                (secs.saturating_mul(1000)).min(5_000)
            } else {
                // Exponential backoff + deterministic jitter.
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

    async fn fetch_active_orders(&self) -> Result<Vec<LighterOrderEntry>> {
        let token = self.fetch_auth_token().await?;
        let resp = self
            .http
            .get(self.api_base.join("api/v1/accountActiveOrders")?)
            .query(&[
                ("account_index", self.creds.account_index.to_string()),
                ("market_id", self.market_index.to_string()),
            ])
            .header("authorization", token)
            .send()
            .await
            .context("activeOrders request failed")?;
        if !resp.status().is_success() {
            bail!("activeOrders HTTP {}", resp.status());
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

    async fn fetch_inactive_orders(&self, limit: usize) -> Result<Vec<LighterOrderEntry>> {
        let token = self.fetch_auth_token().await?;
        let resp = self
            .http
            .get(self.api_base.join("api/v1/accountInactiveOrders")?)
            .query(&[
                ("account_index", self.creds.account_index.to_string()),
                ("market_id", self.market_index.to_string()),
                ("limit", limit.to_string()),
            ])
            .header("authorization", token)
            .send()
            .await
            .context("inactiveOrders request failed")?;
        if !resp.status().is_success() {
            bail!("inactiveOrders HTTP {}", resp.status());
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

    fn parse_f64(s: Option<&String>) -> f64 {
        s.and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0)
    }

    fn map_status(status: &str) -> OrderStatus {
        match status {
            "filled" => OrderStatus::Filled,
            "canceled" | "canceled-oco" | "canceled-expired" | "canceled-child" => {
                OrderStatus::Canceled
            }
            "in-progress" | "pending" | "open" => OrderStatus::New,
            _ => OrderStatus::Unknown,
        }
    }

    fn update_from_entry(
        &self,
        entry: &LighterOrderEntry,
        state: &mut OrderState,
        id: &ClientOrderId,
        reports: &mut Vec<ExecutionReport>,
    ) {
        if let Some(idx) = entry.order_index {
            state.order_index = Some(idx);
        }
        let filled_base = Self::parse_f64(entry.filled_base_amount.as_ref());
        let filled_size = filled_base / self.size_scale;
        if filled_size > state.filled + 1e-9 {
            let delta = filled_size - state.filled;
            state.filled = filled_size;
            let done = entry
                .remaining_base_amount
                .as_ref()
                .and_then(|s| s.parse::<f64>().ok())
                .map(|rem| rem <= 0.0)
                .unwrap_or(false);
            let status = if done {
                OrderStatus::Filled
            } else {
                OrderStatus::PartiallyFilled
            };
            state.status = status.clone();
            reports.push(ExecutionReport {
                client_order_id: id.clone(),
                exchange_order_id: state.exchange_order_id.clone(),
                status,
                filled_qty: delta,
                avg_fill_price: Some(state.price),
                ts: entry.timestamp.map(|v| v as u64),
            });
        }
    }
}

#[async_trait]
impl ExecutionGateway for LighterGateway {
    async fn submit(&self, intents: &[QuoteIntent]) -> Result<Vec<OrderAck>> {
        if intents.is_empty() {
            return Ok(Vec::new());
        }
        let _nonce_lock = self.nonce_lock.lock().await;
        for attempt in 0..2 {
            let nonces = self.allocate_nonces(intents.len()).await?;
            let mut txs = Vec::with_capacity(intents.len());
            for (intent, nonce) in intents.iter().zip(nonces.into_iter()) {
                let px = self.to_price_int(intent.price)?;
                let size = self.to_size_int(intent.size)?;
                let client_order_index = self.client_order_index();
                let signed = self
                    .signer
                    .sign_order(
                        self.market_index,
                        client_order_index,
                        size,
                        px,
                        matches!(intent.side, Side::Ask),
                        0, // limit
                        match intent.tif {
                            TimeInForce::PostOnly => 2,
                            TimeInForce::Ioc => 0,
                            TimeInForce::Fok => 0,
                            TimeInForce::Gtc => 1,
                        },
                        false,
                        0,
                        -1,
                        nonce,
                        self.creds.api_key_index,
                        self.creds.account_index,
                    )
                    .await?;
                let exch_id = signed.tx_hash.clone().map(ExchangeOrderId);
                self.save_order(intent, client_order_index, exch_id.clone());

                txs.push((signed, intent.client_order_id.clone()));
            }
            match self.send_batch(txs).await {
                Ok(acks) => {
                    // optimistic new reports only after successful send
                    let ts = Some(current_unix_ms());
                    for (intent, ack) in intents.iter().zip(acks.iter()) {
                        self.push_report(ExecutionReport {
                            client_order_id: intent.client_order_id.clone(),
                            exchange_order_id: ack.exchange_order_id.clone(),
                            status: OrderStatus::New,
                            filled_qty: 0.0,
                            avg_fill_price: Some(intent.price),
                            ts,
                        });
                    }
                    return Ok(acks);
                }
                Err(err) => {
                    if attempt == 0 && Self::is_nonce_error(&err) {
                        // Refresh nonce from server and retry once.
                        let _ = self.refresh_nonce_from_server().await?;
                        continue;
                    } else {
                        return Err(err);
                    }
                }
            }
        }
        bail!("unexpected submit retry exhaustion")
    }

    async fn cancel_batch(&self, ids: &[ClientOrderId]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let _nonce_lock = self.nonce_lock.lock().await;
        // ensure we have order_index for each
        let missing_index = {
            let orders = self.orders.lock();
            orders
                .iter()
                .filter(|(id, st)| ids.contains(id) && st.order_index.is_none())
                .count()
        };
        if missing_index > 0 {
            let actives = self.fetch_active_orders().await.unwrap_or_default();
            let mut map = self.orders.lock();
            for entry in actives {
                if let (Some(coi), Some(idx)) = (entry.client_order_index, entry.order_index) {
                    for state in map.values_mut() {
                        if state.client_order_index == coi {
                            state.order_index = Some(idx);
                        }
                    }
                }
            }
        }

        let orders_snapshot = {
            let guard = self.orders.lock();
            ids.iter()
                .map(|id| {
                    let state = guard
                        .get(id)
                        .ok_or_else(|| anyhow!("unknown order {}", id.0))?;
                    let order_index = state
                        .order_index
                        .or(Some(state.client_order_index))
                        .ok_or_else(|| anyhow!("missing order_index for {}", id.0))?;
                    Ok((id.clone(), order_index))
                })
                .collect::<Result<Vec<_>>>()?
        };
        let nonces = self.allocate_nonces(orders_snapshot.len()).await?;

        let mut txs = Vec::with_capacity(orders_snapshot.len());
        for ((id, order_index), nonce) in orders_snapshot.into_iter().zip(nonces.into_iter()) {
            let signed = self
                .signer
                .sign_cancel(
                    self.market_index,
                    order_index,
                    nonce,
                    self.creds.api_key_index,
                    self.creds.account_index,
                )
                .await?;
            txs.push((signed, id));
        }
        match self.send_batch(txs).await {
            Ok(_) => {
                // optimistic cancel reports
                let mut reports = Vec::new();
                let ts = Some(current_unix_ms());
                for id in ids {
                    reports.push(ExecutionReport {
                        client_order_id: id.clone(),
                        exchange_order_id: None,
                        status: OrderStatus::Canceled,
                        filled_qty: 0.0,
                        avg_fill_price: None,
                        ts,
                    });
                }
                let mut pending = self.pending_reports.lock();
                pending.extend(reports);
                Ok(())
            }
            Err(err) => {
                if Self::is_nonce_error(&err) {
                    let _ = self.refresh_nonce_from_server().await?;
                }
                Err(err)
            }
        }
    }

    async fn poll_reports(&self) -> Result<Vec<ExecutionReport>> {
        let mut out = {
            let mut guard = self.pending_reports.lock();
            let drained = guard.drain(..).collect::<Vec<_>>();
            drained
        };

        let active = self.fetch_active_orders().await.unwrap_or_default();
        let mut seen = HashMap::new();
        for entry in active.iter() {
            if let Some(coi) = entry.client_order_index {
                seen.insert(coi, entry);
            }
        }
        let mut missing_for_inactive = Vec::new();
        {
            let mut state_guard = self.orders.lock();
            for (id, state) in state_guard.iter_mut() {
                if let Some(entry) = seen.get(&state.client_order_index) {
                    self.update_from_entry(entry, state, id, &mut out);
                } else if matches!(
                    state.status,
                    OrderStatus::New | OrderStatus::PartiallyFilled
                ) {
                    missing_for_inactive.push(id.clone());
                }
            }
        }

        if !missing_for_inactive.is_empty() {
            if let Ok(inactive) = self.fetch_inactive_orders(50).await {
                let mut guard = self.orders.lock();
                for entry in inactive {
                    if let Some(coi) = entry.client_order_index {
                        if let Some((id, state)) = guard.iter_mut().find(|(id, st)| {
                            missing_for_inactive.contains(id) && st.client_order_index == coi
                        }) {
                            let status = entry
                                .status
                                .as_deref()
                                .map(Self::map_status)
                                .unwrap_or(OrderStatus::Unknown);
                            state.status = status.clone();
                            let remaining = (state.size - state.filled).max(0.0);
                            out.push(ExecutionReport {
                                client_order_id: id.clone(),
                                exchange_order_id: state.exchange_order_id.clone(),
                                status,
                                filled_qty: remaining,
                                avg_fill_price: Some(state.price),
                                ts: entry.timestamp.map(|v| v as u64),
                            });
                        }
                    }
                }
            }
        }
        Ok(out)
    }
}
