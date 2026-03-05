use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_longlong};
use std::path::Path;
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use libloading::{Library, Symbol};
use parking_lot::Mutex;
use reqwest::{Client, Url};
use serde::Deserialize;
use serde_json::{self, Value, json};
use tokio::sync::{Mutex as AsyncMutex, Notify, mpsc as tokio_mpsc, oneshot};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config, tungstenite::Message,
};

use crate::base_classes::types::Side;
use crate::execution::types::{
    ClientOrderId, ExchangeOrderId, ExecutionReport, OrderAck, OrderStatus, QuoteIntent,
    TimeInForce,
};
use crate::utils::parsing::log_parse_drop;
use crate::utils::time::{current_unix_ms, current_unix_ts};

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
    code: i32,
    message: Option<String>,
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

#[derive(Debug, Deserialize)]
struct LighterAccountOrdersMsg {
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    orders: HashMap<String, Vec<LighterOrderEntry>>,
    #[serde(default, rename = "type")]
    msg_type: Option<String>,
    #[serde(default)]
    account: Option<i64>,
    #[serde(default)]
    nonce: Option<i64>,
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
            let header = match std::fs::read(lib_path) {
                Ok(b) => {
                    if b.len() >= 4 {
                        Some([b[0], b[1], b[2], b[3]])
                    } else {
                        None
                    }
                }
                Err(err) => {
                    eprintln!(
                        "WARN: failed to read Lighter signer header at {}: {}",
                        lib_path, err
                    );
                    None
                }
            };
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
                let tx_info =
                    Self::from_c(resp.str_ptr).ok_or_else(|| anyhow!("missing tx_info"))?;
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
                let tx_info =
                    Self::from_c(resp.tx_info).ok_or_else(|| anyhow!("missing tx_info"))?;
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
                let tx_info =
                    Self::from_c(resp.str_ptr).ok_or_else(|| anyhow!("missing tx_info"))?;
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
                let tx_info =
                    Self::from_c(resp.tx_info).ok_or_else(|| anyhow!("missing tx_info"))?;
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
            LighterSignerAbi::V0 {
                create_auth_token, ..
            } => unsafe { (create_auth_token)(deadline_ms as c_longlong) },
            LighterSignerAbi::V1 {
                create_auth_token, ..
            } => unsafe {
                (create_auth_token)(
                    deadline_ms as c_longlong,
                    api_key_idx as c_int,
                    account_idx as c_longlong,
                )
            },
        };
        if let Some(err) = Self::from_c(resp.err) {
            bail!("CreateAuthToken failed: {err}");
        }
        Self::from_c(resp.str_ptr).ok_or_else(|| anyhow!("missing auth token"))
    }
}

fn is_lighter_rate_limited(msg: &str) -> bool {
    msg.contains("Too Many Requests")
        || msg.contains("\"code\":23000")
        || msg.contains("HTTP 429")
        || msg.contains("status=429")
}

fn encode_form_field(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for b in value.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

fn format_query_string(pairs: &[(&str, String)]) -> String {
    let mut out = String::new();
    for (idx, (k, v)) in pairs.iter().enumerate() {
        if idx > 0 {
            out.push('&');
        }
        out.push_str(&encode_form_field(k));
        out.push('=');
        out.push_str(&encode_form_field(v));
    }
    out
}

fn log_api_call(
    debug_prints: bool,
    last_api_call: &Mutex<Option<Instant>>,
    method: &str,
    url: &str,
    body: Option<&str>,
) {
    if !debug_prints {
        return;
    }
    let now = Instant::now();
    let mut guard = last_api_call.lock();
    let since_ms = guard
        .map(|prev| now.duration_since(prev).as_millis())
        .unwrap_or(0);
    *guard = Some(now);
    if let Some(body) = body {
        eprintln!(
            "[lighter-api] {} {} body={} since_last_ms={}",
            method, url, body, since_ms
        );
    } else {
        eprintln!(
            "[lighter-api] {} {} since_last_ms={}",
            method, url, since_ms
        );
    }
}

fn ws_url_from_base(base_url: &str) -> Result<String> {
    let url = Url::parse(base_url)?;
    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("lighter base_url missing host"))?;
    let scheme = match url.scheme() {
        "http" => "ws",
        "https" => "wss",
        "ws" | "wss" => url.scheme(),
        _ => "wss",
    };
    let mut out = format!("{scheme}://{host}");
    if let Some(port) = url.port() {
        out.push(':');
        out.push_str(&port.to_string());
    }
    out.push_str("/stream");
    Ok(out)
}

pub fn resolve_lighter_signer_path(lib_path: &str) -> Result<String> {
    if Path::new(lib_path).exists() {
        return Ok(lib_path.to_string());
    }
    let mut candidates: Vec<String> = vec![];

    // IMPORTANT: do not try to load a macOS .dylib on Linux (it will error with "invalid ELF header").
    if cfg!(target_os = "macos") {
        if lib_path.ends_with(".so") {
            candidates.push(lib_path.trim_end_matches(".so").to_string() + ".dylib");
        }
    } else {
        if lib_path.ends_with(".dylib") {
            candidates.push(lib_path.trim_end_matches(".dylib").to_string() + ".so");
        }
        // If the user hardcoded the wrong arch name, try swapping it.
        if cfg!(target_arch = "aarch64") {
            candidates.push(
                lib_path
                    .replace("amd64", "arm64")
                    .replace(".dylib", ".so"),
            );
        } else if cfg!(target_arch = "x86_64") {
            candidates.push(
                lib_path
                    .replace("arm64", "amd64")
                    .replace(".dylib", ".so"),
            );
        }
    }

    if let Some(found) = candidates.iter().find(|p| Path::new(p.as_str()).exists()) {
        return Ok(found.clone());
    }

    bail!(
        "Lighter signer library not found at {} (candidates tried: {:?}); \
         please provide the correct native signer for this OS/arch: \
         signer-amd64.so (Linux x86_64), signer-arm64.so (Linux aarch64), signer-arm64.dylib (macOS)",
        lib_path,
        candidates
    );
}

fn check_client_with_backoff(
    signer: &LighterSigner,
    api_key_idx: i32,
    account_idx: i64,
    debug_prints: bool,
) -> Result<()> {
    let mut sleep_ms: u64 = 200;
    for attempt in 0..8 {
        match signer.check_client(api_key_idx, account_idx) {
            Ok(()) => return Ok(()),
            Err(err) => {
                let msg = err.to_string();
                if !is_lighter_rate_limited(&msg) {
                    return Err(err);
                }
                let wait = sleep_ms.min(5_000);
                if debug_prints {
                    eprintln!(
                        "[lighter-sign] CheckClient rate-limited attempt={} waiting_ms={} (msg={})",
                        attempt + 1,
                        wait,
                        msg
                    );
                }
                std::thread::sleep(Duration::from_millis(wait));
                sleep_ms = sleep_ms.saturating_mul(2).min(5_000);
            }
        }
    }
    bail!("CheckClient retry exhausted (rate-limited)")
}

#[derive(Clone)]
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

const LIGHTER_TX_TYPE_CREATE_ORDER: u8 = 14;
const LIGHTER_TX_TYPE_CANCEL_ORDER: u8 = 15;

fn batch_observed_with_orders(
    orders: &HashMap<ClientOrderId, OrderState>,
    tx_meta: &[(u8, ClientOrderId)],
) -> bool {
    tx_meta.iter().all(|(tx_type, id)| {
        let state = orders.get(id);
        match *tx_type {
            LIGHTER_TX_TYPE_CREATE_ORDER => state
                .map(|st| {
                    st.order_index.is_some()
                        || st.filled > 0.0
                        || matches!(
                            st.status,
                            OrderStatus::PartiallyFilled
                                | OrderStatus::Filled
                                | OrderStatus::Canceled
                                | OrderStatus::Rejected
                        )
                })
                .unwrap_or(false),
            LIGHTER_TX_TYPE_CANCEL_ORDER => state
                .map(|st| {
                    matches!(
                        st.status,
                        OrderStatus::Canceled | OrderStatus::Filled | OrderStatus::Rejected
                    )
                })
                .unwrap_or(false),
            _ => false,
        }
    })
}

enum LighterWsCommand {
    SendBatch {
        txs: Vec<(SignedTx, ClientOrderId)>,
        resp: oneshot::Sender<Result<SendTxBatchResponse>>,
    },
    Shutdown,
}

#[derive(Clone)]
struct LighterWsConfig {
    ws_url: String,
    account_index: i64,
    api_key_index: i32,
    market_index: u8,
    debug_prints: bool,
}

struct LighterWsWorker {
    cfg: LighterWsConfig,
    signer: SignerHandle,
    command_rx: tokio_mpsc::Receiver<LighterWsCommand>,
    orders: Arc<Mutex<HashMap<ClientOrderId, OrderState>>>,
    pending_reports: Arc<Mutex<Vec<ExecutionReport>>>,
    report_notify: Arc<Notify>,
    size_scale: f64,
    request_id: u64,
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
    debug_prints: bool,
}

impl SignerHandle {
    fn new(lib_path: String, debug_prints: bool) -> Result<Self> {
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
                            // CheckClient performs an HTTP call and is rate-limited.
                            // Do it at init time only, with backoff.
                            .and_then(|_| {
                                check_client_with_backoff(
                                    &signer,
                                    api_key_idx,
                                    account_idx,
                                    debug_prints,
                                )
                            });
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
                        if debug_prints {
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
                        }
                        let res = signer.switch_api_key(api_key_idx).and_then(|_| {
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
                        if debug_prints {
                            eprintln!(
                                "[lighter-sign-thread] cancel mid={} order_idx={} ak_idx={} acct={}",
                                market_index, order_index, api_key_idx, account_idx
                            );
                        }
                        let res = signer.switch_api_key(api_key_idx).and_then(|_| {
                            signer.sign_cancel_order(
                                market_index,
                                order_index,
                                nonce,
                                api_key_idx,
                                account_idx,
                            )
                        });
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
        Ok(Self {
            tx,
            debug_prints,
        })
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
        if self.debug_prints {
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
        }
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
            if self.debug_prints {
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
            if self.debug_prints {
                eprintln!(
                    "[lighter-sign] sign_cancel success tx_type={} tx_info_len={}",
                    tx.tx_type,
                    tx.tx_info.len()
                );
            }
        } else {
            eprintln!(
                "[lighter-sign] sign_cancel failed: {:?}",
                res.as_ref().err()
            );
        }
        res
    }

    async fn auth_token(
        &self,
        deadline_ms: i64,
        api_key_idx: i32,
        account_idx: i64,
    ) -> Result<String> {
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

impl LighterWsWorker {
    fn extract_sendtx_req_id(value: &Value) -> Option<String> {
        value
            .get("data")
            .and_then(|d| d.get("id"))
            .or_else(|| value.get("id"))
            .and_then(|v| {
                if let Some(s) = v.as_str() {
                    Some(s.to_string())
                } else {
                    v.as_i64().map(|n| n.to_string())
                }
            })
    }

    fn parse_sendtx_code(value: &Value) -> Result<i32> {
        if let Some(code) = value.as_i64() {
            return i32::try_from(code)
                .map_err(|_| anyhow!("sendTxBatch code out of range: {code}"));
        }
        if let Some(code) = value.as_str() {
            return code
                .parse::<i32>()
                .with_context(|| format!("invalid sendTxBatch code '{code}'"));
        }
        bail!("missing/invalid sendTxBatch code")
    }

    fn parse_sendtx_hashes(value: Option<&Value>) -> Vec<String> {
        let Some(value) = value else {
            return Vec::new();
        };
        if let Some(arr) = value.as_array() {
            return arr
                .iter()
                .filter_map(|v| v.as_str().map(ToString::to_string))
                .collect();
        }
        if let Some(single) = value.as_str() {
            let trimmed = single.trim();
            if !trimmed.is_empty() {
                return vec![trimmed.to_string()];
            }
        }
        Vec::new()
    }

    fn parse_sendtx_response_obj(obj: &serde_json::Map<String, Value>) -> Result<SendTxBatchResponse> {
        let code_val = obj
            .get("code")
            .ok_or_else(|| anyhow!("missing code in sendTxBatch response"))?;
        let code = Self::parse_sendtx_code(code_val)?;
        let message = obj
            .get("message")
            .and_then(|v| v.as_str())
            .map(ToString::to_string);
        let tx_hash = Self::parse_sendtx_hashes(obj.get("tx_hash").or_else(|| obj.get("txHash")));
        Ok(SendTxBatchResponse {
            code,
            message,
            tx_hash,
        })
    }

    fn parse_sendtx_response(value: &Value) -> Result<SendTxBatchResponse> {
        if let Some(obj) = value
            .get("data")
            .and_then(|d| d.get("attributes"))
            .and_then(|v| v.as_object())
        {
            return Self::parse_sendtx_response_obj(obj);
        }
        if let Some(obj) = value.get("data").and_then(|v| v.as_object()) {
            if obj.contains_key("code")
                || obj.contains_key("tx_hash")
                || obj.contains_key("txHash")
            {
                return Self::parse_sendtx_response_obj(obj);
            }
        }
        if let Some(obj) = value.as_object() {
            if obj.contains_key("code")
                || obj.contains_key("tx_hash")
                || obj.contains_key("txHash")
            {
                return Self::parse_sendtx_response_obj(obj);
            }
        }
        bail!("invalid sendtxbatch response: expected code/tx_hash fields")
    }

    fn value_has_sendtx_marker(value: &Value) -> bool {
        let msg_type = value
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        if msg_type.contains("jsonapi/sendtx") {
            return true;
        }
        if value
            .get("data")
            .and_then(|d| d.get("type"))
            .and_then(|v| v.as_str())
            .map(|s| s.contains("jsonapi/sendtx"))
            .unwrap_or(false)
        {
            return true;
        }
        let has_code = value
            .get("code")
            .or_else(|| value.get("data").and_then(|d| d.get("code")))
            .or_else(|| {
                value
                    .get("data")
                    .and_then(|d| d.get("attributes"))
                    .and_then(|a| a.get("code"))
            })
            .is_some();
        let has_hash = value
            .get("tx_hash")
            .or_else(|| value.get("txHash"))
            .or_else(|| value.get("data").and_then(|d| d.get("tx_hash")))
            .or_else(|| value.get("data").and_then(|d| d.get("txHash")))
            .or_else(|| {
                value
                    .get("data")
                    .and_then(|d| d.get("attributes"))
                    .and_then(|a| a.get("tx_hash").or_else(|| a.get("txHash")))
            })
            .is_some();
        has_code || has_hash
    }

    fn remove_pending_sender(
        pending: &mut HashMap<String, oneshot::Sender<Result<SendTxBatchResponse>>>,
        req_id: Option<String>,
        debug_prints: bool,
    ) -> Option<oneshot::Sender<Result<SendTxBatchResponse>>> {
        let req_id = req_id.map(|id| id.trim().to_string()).filter(|id| !id.is_empty());
        if let Some(req_id) = req_id {
            if let Some(tx) = pending.remove(&req_id) {
                return Some(tx);
            }
            if let Some(stripped) = req_id.strip_prefix("txb-") {
                if let Some(tx) = pending.remove(stripped) {
                    if debug_prints {
                        eprintln!(
                            "[lighter-ws] matched sendtx response id {} to pending {}",
                            req_id, stripped
                        );
                    }
                    return Some(tx);
                }
            } else {
                let prefixed = format!("txb-{req_id}");
                if let Some(tx) = pending.remove(&prefixed) {
                    if debug_prints {
                        eprintln!(
                            "[lighter-ws] matched sendtx response id {} to pending {}",
                            req_id, prefixed
                        );
                    }
                    return Some(tx);
                }
            }
            if pending.len() == 1 {
                let (only_id, tx) = pending.drain().next().expect("pending len checked");
                eprintln!(
                    "WARN: unmatched sendtx response id {}; pairing with only pending request {}",
                    req_id, only_id
                );
                return Some(tx);
            }
            eprintln!(
                "WARN: unmatched sendtx response id {}; pending requests={:?}",
                req_id,
                pending.keys().collect::<Vec<_>>()
            );
            return None;
        }
        if pending.len() == 1 {
            let (only_id, tx) = pending.drain().next().expect("pending len checked");
            if debug_prints {
                eprintln!(
                    "[lighter-ws] response missing id; pairing with pending {}",
                    only_id
                );
            }
            return Some(tx);
        }
        if !pending.is_empty() {
            eprintln!(
                "WARN: sendtx response missing id with multiple pending requests={:?}",
                pending.keys().collect::<Vec<_>>()
            );
        }
        None
    }

    fn new(
        cfg: LighterWsConfig,
        signer: SignerHandle,
        command_rx: tokio_mpsc::Receiver<LighterWsCommand>,
        orders: Arc<Mutex<HashMap<ClientOrderId, OrderState>>>,
        pending_reports: Arc<Mutex<Vec<ExecutionReport>>>,
        report_notify: Arc<Notify>,
        size_scale: f64,
    ) -> Self {
        Self {
            cfg,
            signer,
            command_rx,
            orders,
            pending_reports,
            report_notify,
            size_scale,
            request_id: 1,
        }
    }

    async fn run(mut self) -> Result<()> {
        let initial_backoff = Duration::from_millis(250);
        let max_backoff = Duration::from_millis(3_000);
        let mut backoff = initial_backoff;

        loop {
            let mut ws = match self.connect_ws().await {
                Ok(ws) => ws,
                Err(err) => {
                    eprintln!("[lighter-ws] connect failed: {:#}", err);
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff.saturating_mul(2)).min(max_backoff);
                    continue;
                }
            };

            if let Err(err) = self.subscribe_private(&mut ws).await {
                eprintln!("[lighter-ws] subscribe failed: {:#}", err);
                tokio::time::sleep(backoff).await;
                backoff = (backoff.saturating_mul(2)).min(max_backoff);
                continue;
            }

            backoff = initial_backoff;
            let shutdown = self.run_session(ws).await?;
            if shutdown {
                return Ok(());
            }
        }
    }

    async fn connect_ws(
        &self,
    ) -> Result<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>> {
        let (ws, _) = connect_async_with_config(&self.cfg.ws_url, None, true).await?;
        Ok(ws)
    }

    async fn subscribe_private(
        &self,
        ws: &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    ) -> Result<()> {
        let auth = self.auth_token().await?;
        let channel = format!(
            "account_orders/{}/{}",
            self.cfg.market_index, self.cfg.account_index
        );
        let sub = json!({
            "type": "subscribe",
            "channel": channel,
            "auth": auth,
        })
        .to_string();
        ws.send(Message::Text(sub)).await?;
        if self.cfg.debug_prints {
            eprintln!(
                "[lighter-ws] subscribed account_orders/{}/{}",
                self.cfg.market_index, self.cfg.account_index
            );
        }
        Ok(())
    }

    async fn auth_token(&self) -> Result<String> {
        let deadline = current_unix_ts() + 10 * 60;
        self.signer
            .auth_token(deadline, self.cfg.api_key_index, self.cfg.account_index)
            .await
    }

    async fn run_session(
        &mut self,
        ws: WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    ) -> Result<bool> {
        let (mut sink, mut stream) = ws.split();
        let mut pending: HashMap<String, oneshot::Sender<Result<SendTxBatchResponse>>> =
            HashMap::new();
        let mut heartbeat = tokio::time::interval(Duration::from_secs(20));

        loop {
            tokio::select! {
                cmd = self.command_rx.recv() => {
                    match cmd {
                        Some(LighterWsCommand::SendBatch { txs, resp }) => {
                            let req_id = format!("txb-{}", self.request_id);
                            self.request_id = self.request_id.saturating_add(1);
                            let payload = match Self::build_send_batch_payload(&txs, &req_id) {
                                Ok(payload) => payload,
                                Err(err) => {
                                    let _ = resp.send(Err(err));
                                    continue;
                                }
                            };
                            if let Err(err) = sink.send(Message::Text(payload)).await {
                                let _ = resp.send(Err(anyhow!(err)));
                                for (_, resp) in pending.drain() {
                                    let _ = resp.send(Err(anyhow!("lighter ws disconnected")));
                                }
                                return Ok(false);
                            }
                            pending.insert(req_id, resp);
                        }
                        Some(LighterWsCommand::Shutdown) => {
                            for (_, resp) in pending.drain() {
                                let _ = resp.send(Err(anyhow!("lighter ws shutdown")));
                            }
                            return Ok(true);
                        }
                        None => {
                            for (_, resp) in pending.drain() {
                                let _ = resp.send(Err(anyhow!("lighter ws command channel closed")));
                            }
                            return Ok(true);
                        }
                    }
                }
                msg = stream.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if let Ok(value) = serde_json::from_str::<Value>(&text) {
                                self.handle_inbound(value, &mut pending, &mut sink).await;
                            }
                        }
                        Some(Ok(Message::Binary(data))) => {
                            if let Ok(text) = String::from_utf8(data) {
                                if let Ok(value) = serde_json::from_str::<Value>(&text) {
                                    self.handle_inbound(value, &mut pending, &mut sink).await;
                                }
                            }
                        }
                        Some(Ok(Message::Ping(payload))) => {
                            let _ = sink.send(Message::Pong(payload)).await;
                        }
                        Some(Ok(Message::Close(_))) => {
                            break;
                        }
                        Some(Err(err)) => {
                            eprintln!("[lighter-ws] stream error: {:#}", err);
                            break;
                        }
                        None => break,
                        _ => {}
                    }
                }
                _ = heartbeat.tick() => {
                    let _ = sink
                        .send(Message::Text(r#"{"type":"ping"}"#.to_string()))
                        .await;
                }
            }
        }

        for (_, resp) in pending.drain() {
            let _ = resp.send(Err(anyhow!("lighter ws disconnected")));
        }
        Ok(false)
    }

    fn build_send_batch_payload(
        txs: &[(SignedTx, ClientOrderId)],
        req_id: &str,
    ) -> Result<String> {
        let tx_types = txs.iter().map(|(tx, _)| tx.tx_type).collect::<Vec<_>>();
        let tx_infos = txs
            .iter()
            .map(|(tx, _)| tx.tx_info.clone())
            .collect::<Vec<_>>();
        let tx_types_json = serde_json::to_string(&tx_types)?;
        let tx_infos_json = serde_json::to_string(&tx_infos)?;
        Ok(json!({
            "type": "jsonapi/sendtxbatch",
            "data": {
                "id": req_id,
                "tx_types": tx_types_json,
                "tx_infos": tx_infos_json,
            }
        })
        .to_string())
    }

    fn handle_account_orders(&self, msg: LighterAccountOrdersMsg) {
        let key = self.cfg.market_index.to_string();
        let Some(entries) = msg.orders.get(&key) else {
            return;
        };
        let mut reports = Vec::new();
        {
            let mut guard = self.orders.lock();
            for entry in entries {
                let Some(coi) = entry.client_order_index else {
                    continue;
                };
                let matched = guard
                    .iter_mut()
                    .find(|(_, state)| state.client_order_index == coi);
                let Some((id, state)) = matched else {
                    continue;
                };
                if let Some(price_str) = entry.price.as_ref() {
                    if let Ok(price) = price_str.parse::<f64>() {
                        state.price = price;
                    }
                }
                if let Some(size_str) = entry.initial_base_amount.as_ref() {
                    if let Ok(size_int) = size_str.parse::<f64>() {
                        state.size = size_int / self.size_scale;
                    }
                }
                LighterGateway::update_from_entry(self.size_scale, entry, state, id, &mut reports);
                if let Some(status_str) = entry.status.as_deref() {
                    let status = LighterGateway::map_status(status_str);
                    if status != state.status {
                        if matches!(
                            status,
                            OrderStatus::Canceled | OrderStatus::Rejected | OrderStatus::Filled
                        ) {
                            state.status = status.clone();
                            reports.push(ExecutionReport {
                                client_order_id: id.clone(),
                                exchange_order_id: state.exchange_order_id.clone(),
                                status,
                                filled_qty: 0.0,
                                avg_fill_price: None,
                                ts: entry.timestamp.map(|v| v as u64),
                            });
                        } else {
                            state.status = status;
                        }
                    }
                }
            }
        }
        if !reports.is_empty() {
            let mut pending = self.pending_reports.lock();
            pending.extend(reports);
            self.report_notify.notify_one();
        }
    }

    async fn handle_inbound(
        &self,
        value: Value,
        pending: &mut HashMap<String, oneshot::Sender<Result<SendTxBatchResponse>>>,
        sink: &mut futures_util::stream::SplitSink<
            WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
            Message,
        >,
    ) {
        let msg_type = value
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        if self.cfg.debug_prints && !msg_type.is_empty() {
            eprintln!("[lighter-ws] recv type={}", msg_type);
        }
        if msg_type == "ping" {
            let _ = sink.send(Message::Text(r#"{"type":"pong"}"#.to_string())).await;
            return;
        }
        if msg_type == "pong" {
            return;
        }
        if matches!(msg_type, "update/account_orders" | "update/account_all_orders") {
            if let Ok(msg) = serde_json::from_value::<LighterAccountOrdersMsg>(value) {
                self.handle_account_orders(msg);
            }
            return;
        }
        if pending.is_empty() {
            return;
        }
        if !Self::value_has_sendtx_marker(&value) {
            return;
        }

        let req_id = Self::extract_sendtx_req_id(&value);
        let Some(resp_tx) = Self::remove_pending_sender(pending, req_id, self.cfg.debug_prints)
        else {
            return;
        };
        let parsed = Self::parse_sendtx_response(&value).and_then(|resp| {
            if resp.code == 200 || resp.code == 0 {
                Ok(resp)
            } else {
                Err(anyhow!(
                    "sendTxBatch error {}: {}",
                    resp.code,
                    resp.message.clone().unwrap_or_default()
                ))
            }
        });
        let _ = resp_tx.send(parsed);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        batch_observed_with_orders, ClientOrderId, LighterWsWorker, OrderState, OrderStatus, Side,
        LIGHTER_TX_TYPE_CANCEL_ORDER, LIGHTER_TX_TYPE_CREATE_ORDER,
    };
    use std::collections::HashMap;
    use serde_json::json;

    #[test]
    fn parse_sendtx_response_handles_data_attributes_shape() {
        let value = json!({
            "type": "jsonapi/response",
            "data": {
                "id": "txb-7",
                "attributes": {
                    "code": "200",
                    "message": "ok",
                    "txHash": ["0xabc"]
                }
            }
        });
        let parsed = LighterWsWorker::parse_sendtx_response(&value).expect("parse sendtx response");
        assert_eq!(parsed.code, 200);
        assert_eq!(parsed.message.as_deref(), Some("ok"));
        assert_eq!(parsed.tx_hash, vec!["0xabc".to_string()]);
    }

    #[test]
    fn value_has_sendtx_marker_on_code_and_hash_fields() {
        let value = json!({
            "id": 3,
            "code": 200,
            "tx_hash": ["0x1"]
        });
        assert!(LighterWsWorker::value_has_sendtx_marker(&value));
    }

    #[test]
    fn extract_sendtx_req_id_reads_numeric_id() {
        let value = json!({"id": 42, "code": 200});
        assert_eq!(
            LighterWsWorker::extract_sendtx_req_id(&value).as_deref(),
            Some("42")
        );
    }

    #[test]
    fn batch_observed_with_orders_recognizes_create_and_cancel_completion() {
        let create_id = ClientOrderId::new("mf-lighter-b-1");
        let cancel_id = ClientOrderId::new("mf-lighter-s-2");

        let mut orders = HashMap::new();
        orders.insert(
            create_id.clone(),
            OrderState {
                client_order_index: 1,
                order_index: Some(1001),
                side: Side::Bid,
                price: 100.0,
                size: 1.0,
                filled: 0.0,
                status: OrderStatus::New,
                exchange_order_id: None,
            },
        );
        orders.insert(
            cancel_id.clone(),
            OrderState {
                client_order_index: 2,
                order_index: Some(1002),
                side: Side::Ask,
                price: 101.0,
                size: 1.0,
                filled: 0.0,
                status: OrderStatus::Canceled,
                exchange_order_id: None,
            },
        );

        let tx_meta = vec![
            (LIGHTER_TX_TYPE_CREATE_ORDER, create_id),
            (LIGHTER_TX_TYPE_CANCEL_ORDER, cancel_id),
        ];
        assert!(batch_observed_with_orders(&orders, &tx_meta));
    }

    #[test]
    fn batch_observed_with_orders_rejects_unconfirmed_create() {
        let create_id = ClientOrderId::new("mf-lighter-b-3");
        let mut orders = HashMap::new();
        orders.insert(
            create_id.clone(),
            OrderState {
                client_order_index: 3,
                order_index: None,
                side: Side::Bid,
                price: 100.0,
                size: 1.0,
                filled: 0.0,
                status: OrderStatus::New,
                exchange_order_id: None,
            },
        );
        let tx_meta = vec![(LIGHTER_TX_TYPE_CREATE_ORDER, create_id)];
        assert!(!batch_observed_with_orders(&orders, &tx_meta));
    }

    #[test]
    fn map_status_supports_common_terminal_aliases() {
        assert_eq!(
            super::LighterGateway::map_status("cancelled"),
            OrderStatus::Canceled
        );
        assert_eq!(
            super::LighterGateway::map_status("closed"),
            OrderStatus::Canceled
        );
        assert_eq!(
            super::LighterGateway::map_status("rejected"),
            OrderStatus::Rejected
        );
        assert_eq!(
            super::LighterGateway::map_status("partially_filled"),
            OrderStatus::PartiallyFilled
        );
    }
}

struct LighterResyncWorker {
    signer: SignerHandle,
    creds: LighterCredentials,
    market_index: u8,
    size_scale: f64,
    http: Client,
    api_base: Url,
    debug_prints: bool,
    last_api_call: Arc<Mutex<Option<Instant>>>,
    orders: Arc<Mutex<HashMap<ClientOrderId, OrderState>>>,
    pending_reports: Arc<Mutex<Vec<ExecutionReport>>>,
    report_notify: Arc<Notify>,
    interval: Duration,
}

impl LighterResyncWorker {
    async fn run(self) {
        let mut ticker = tokio::time::interval(self.interval);
        if let Err(err) = self.resync_once().await {
            eprintln!("[lighter-resync] initial sync failed: {:#}", err);
        }
        loop {
            ticker.tick().await;
            if let Err(err) = self.resync_once().await {
                eprintln!("[lighter-resync] sync failed: {:#}", err);
            }
        }
    }

    async fn resync_once(&self) -> Result<()> {
        let active = self.fetch_active_orders().await?;
        let mut seen = HashMap::new();
        for entry in active.iter() {
            if let Some(coi) = entry.client_order_index {
                seen.insert(coi, entry);
            }
        }

        let mut missing_for_inactive = Vec::new();
        let mut reports = Vec::new();
        {
            let mut guard = self.orders.lock();
            for (id, state) in guard.iter_mut() {
                if let Some(entry) = seen.get(&state.client_order_index) {
                    LighterGateway::update_from_entry(
                        self.size_scale,
                        entry,
                        state,
                        id,
                        &mut reports,
                    );
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
                            if let Some(price_str) = entry.price.as_ref() {
                                if let Ok(price) = price_str.parse::<f64>() {
                                    state.price = price;
                                }
                            }
                            if let Some(size_str) = entry.initial_base_amount.as_ref() {
                                if let Ok(size_int) = size_str.parse::<f64>() {
                                    state.size = size_int / self.size_scale;
                                }
                            }
                            LighterGateway::update_from_entry(
                                self.size_scale,
                                &entry,
                                state,
                                id,
                                &mut reports,
                            );
                            let mut status = entry
                                .status
                                .as_deref()
                                .map(LighterGateway::map_status)
                                .unwrap_or(OrderStatus::Unknown);
                            if status == OrderStatus::Unknown {
                                if let Some(raw) = entry.status.as_deref() {
                                    eprintln!(
                                        "WARN: unknown inactive order status '{}'; treating as canceled",
                                        raw
                                    );
                                }
                                status = OrderStatus::Canceled;
                            }
                            if status != state.status {
                                if matches!(
                                    status,
                                    OrderStatus::Canceled
                                        | OrderStatus::Rejected
                                        | OrderStatus::Filled
                                ) {
                                    state.status = status.clone();
                                    reports.push(ExecutionReport {
                                        client_order_id: id.clone(),
                                        exchange_order_id: state.exchange_order_id.clone(),
                                        status,
                                        filled_qty: 0.0,
                                        avg_fill_price: None,
                                        ts: entry.timestamp.map(|v| v as u64),
                                    });
                                } else {
                                    state.status = status;
                                }
                            }
                        }
                    }
                }
            }
        }

        if !reports.is_empty() {
            let mut pending = self.pending_reports.lock();
            pending.extend(reports);
            self.report_notify.notify_one();
        }
        Ok(())
    }

    async fn fetch_auth_token(&self) -> Result<String> {
        let deadline = current_unix_ts() + 10 * 60;
        self.signer
            .auth_token(deadline, self.creds.api_key_index, self.creds.account_index)
            .await
    }

    async fn fetch_active_orders(&self) -> Result<Vec<LighterOrderEntry>> {
        let token = self.fetch_auth_token().await?;
        let token_len = token.len();
        let url = self.api_base.join("api/v1/accountActiveOrders")?;
        let query_pairs = vec![
            ("account_index", self.creds.account_index.to_string()),
            ("market_id", self.market_index.to_string()),
        ];
        let query = format_query_string(&query_pairs);
        let full_url = format!("{}?{}", url.as_str(), query);
        log_api_call(self.debug_prints, self.last_api_call.as_ref(), "GET", &full_url, None);
        let resp = self
            .http
            .get(url)
            .query(&query_pairs)
            .header("Authorization", token)
            .send()
            .await
            .context("activeOrders request failed")?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.context("activeOrders read body failed")?;
            if status.as_u16() == 401 && self.debug_prints {
                eprintln!(
                    "[lighter-resync] activeOrders unauthorized (token_len={})",
                    token_len
                );
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

    async fn fetch_inactive_orders(&self, limit: usize) -> Result<Vec<LighterOrderEntry>> {
        let token = self.fetch_auth_token().await?;
        let token_len = token.len();
        let url = self.api_base.join("api/v1/accountInactiveOrders")?;
        let query_pairs = vec![
            ("account_index", self.creds.account_index.to_string()),
            ("market_id", self.market_index.to_string()),
            ("limit", limit.to_string()),
        ];
        let query = format_query_string(&query_pairs);
        let full_url = format!("{}?{}", url.as_str(), query);
        log_api_call(self.debug_prints, self.last_api_call.as_ref(), "GET", &full_url, None);
        let resp = self
            .http
            .get(url)
            .query(&query_pairs)
            .header("Authorization", token)
            .send()
            .await
            .context("inactiveOrders request failed")?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.context("inactiveOrders read body failed")?;
            if status.as_u16() == 401 && self.debug_prints {
                eprintln!(
                    "[lighter-resync] inactiveOrders unauthorized (token_len={})",
                    token_len
                );
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

pub struct LighterGateway {
    signer: SignerHandle,
    creds: LighterCredentials,
    market_index: u8,
    price_scale: f64,
    size_scale: f64,
    debug_prints: bool,
    http: Client,
    api_base: Url,
    ws_tx: tokio_mpsc::Sender<LighterWsCommand>,
    next_client_index: Mutex<i64>,
    next_nonce: Mutex<Option<i64>>,
    nonce_lock: AsyncMutex<()>,
    pending_reports: Arc<Mutex<Vec<ExecutionReport>>>,
    orders: Arc<Mutex<HashMap<ClientOrderId, OrderState>>>,
    report_notify: Arc<Notify>,
    last_api_call: Arc<Mutex<Option<Instant>>>,
}

impl LighterGateway {
    pub async fn connect(
        creds: LighterCredentials,
        market_index: u32,
        price_decimals: u32,
        size_decimals: u32,
        debug_prints: bool,
    ) -> Result<Self> {
        let signer = SignerHandle::new(creds.signer_lib.clone(), debug_prints)?;
        let base_url = creds.base_url.clone();
        // Initialize signer client on its dedicated thread before use
        if debug_prints {
            eprintln!(
                "[lighter-sign] init signer base_url={} api_key_idx={} account_idx={}",
                base_url, creds.api_key_index, creds.account_index
            );
        }
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
        let price_scale = 10_f64.powi(price_decimals as i32);
        let size_scale = 10_f64.powi(size_decimals as i32);
        let pending_reports = Arc::new(Mutex::new(Vec::new()));
        let orders = Arc::new(Mutex::new(HashMap::new()));
        let report_notify = Arc::new(Notify::new());
        let last_api_call = Arc::new(Mutex::new(None));
        let ws_url = ws_url_from_base(&base_url)?;
        let (ws_tx, ws_rx) = tokio_mpsc::channel(128);

        let ws_worker = LighterWsWorker::new(
            LighterWsConfig {
                ws_url,
                account_index: creds.account_index,
                api_key_index: creds.api_key_index,
                market_index: market_index as u8,
                debug_prints,
            },
            signer.clone(),
            ws_rx,
            orders.clone(),
            pending_reports.clone(),
            report_notify.clone(),
            size_scale,
        );
        tokio::spawn(async move {
            if let Err(err) = ws_worker.run().await {
                eprintln!("[lighter-ws] worker terminated: {:#}", err);
            }
        });

        let resync_worker = LighterResyncWorker {
            signer: signer.clone(),
            creds: creds.clone(),
            market_index: market_index as u8,
            size_scale,
            http: http.clone(),
            api_base: api_base.clone(),
            debug_prints,
            last_api_call: last_api_call.clone(),
            orders: orders.clone(),
            pending_reports: pending_reports.clone(),
            report_notify: report_notify.clone(),
            interval: Duration::from_secs(20),
        };
        tokio::spawn(async move {
            resync_worker.run().await;
        });

        let gw = Self {
            signer: signer,
            creds,
            market_index: market_index as u8,
            price_scale,
            size_scale,
            debug_prints,
            http,
            api_base,
            ws_tx,
            next_client_index: Mutex::new(1),
            next_nonce: Mutex::new(None),
            nonce_lock: AsyncMutex::new(()),
            pending_reports,
            orders,
            report_notify,
            last_api_call,
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

    async fn peek_nonces(&self, count: usize) -> Result<(i64, Vec<i64>)> {
        if count == 0 {
            return Ok((0, Vec::new()));
        }
        let _ = self.ensure_nonce_seed().await?;
        let guard = self.next_nonce.lock();
        let start = guard.expect("nonce seed must be set");
        let mut nonces = Vec::with_capacity(count);
        for i in 0..count {
            nonces.push(start + i as i64);
        }
        Ok((start, nonces))
    }

    fn commit_nonces(&self, start: i64, count: usize) {
        if count == 0 {
            return;
        }
        let mut guard = self.next_nonce.lock();
        *guard = Some(start + count as i64);
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

    fn log_api_call(&self, method: &str, url: &str, body: Option<&str>) {
        log_api_call(
            self.debug_prints,
            self.last_api_call.as_ref(),
            method,
            url,
            body,
        );
    }

    async fn send_batch(&self, txs: Vec<(SignedTx, ClientOrderId)>) -> Result<Vec<OrderAck>> {
        if txs.is_empty() {
            return Ok(Vec::new());
        }
        let tx_meta = txs
            .iter()
            .map(|(tx, id)| (tx.tx_type, id.clone()))
            .collect::<Vec<_>>();
        let fallback_hashes = txs
            .iter()
            .map(|(tx, _)| tx.tx_hash.as_ref().cloned())
            .collect::<Vec<_>>();
        let client_ids = txs.iter().map(|(_, id)| id.clone()).collect::<Vec<_>>();

        let (resp_tx, resp_rx) = oneshot::channel();
        self.ws_tx
            .send(LighterWsCommand::SendBatch { txs, resp: resp_tx })
            .await
            .map_err(|e| anyhow!("lighter ws send queue closed: {e}"))?;
        let payload = match tokio::time::timeout(Duration::from_secs(5), resp_rx).await {
            Ok(resp) => resp.context("lighter ws sendTxBatch response dropped")??,
            Err(_) => {
                eprintln!(
                    "WARN: lighter ws sendTxBatch ack timeout after 5s; reconciling {} tx(s) via order-state sync",
                    tx_meta.len()
                );
                self.reconcile_after_send_timeout(&tx_meta, Duration::from_secs(8))
                    .await
                    .context("lighter ws sendTxBatch timeout")?;
                eprintln!(
                    "WARN: sendTxBatch ack missing but order-state reconciliation confirmed batch outcome"
                );
                return Ok(self.build_acks_from_hashes(&client_ids, &fallback_hashes, None));
            }
        };

        Ok(self.build_acks_from_hashes(&client_ids, &fallback_hashes, Some(&payload.tx_hash)))
    }

    fn build_acks_from_hashes(
        &self,
        client_ids: &[ClientOrderId],
        fallback_hashes: &[Option<String>],
        response_hashes: Option<&[String]>,
    ) -> Vec<OrderAck> {
        let mut acks = Vec::with_capacity(client_ids.len());
        for (idx, client_id) in client_ids.iter().cloned().enumerate() {
            let exch = response_hashes
                .and_then(|h| h.get(idx))
                .or(fallback_hashes.get(idx).and_then(|h| h.as_ref()))
                .cloned()
                .map(ExchangeOrderId);
            acks.push(OrderAck {
                client_order_id: client_id,
                exchange_order_id: exch,
            });
        }
        acks
    }

    fn batch_observed(&self, tx_meta: &[(u8, ClientOrderId)]) -> bool {
        let orders = self.orders.lock();
        batch_observed_with_orders(&orders, tx_meta)
    }

    async fn reconcile_after_send_timeout(
        &self,
        tx_meta: &[(u8, ClientOrderId)],
        max_wait: Duration,
    ) -> Result<()> {
        let start = Instant::now();
        let mut backoff_ms = 200u64;
        while start.elapsed() < max_wait {
            self.reconcile_orders_once().await?;
            if self.batch_observed(tx_meta) {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            backoff_ms = (backoff_ms.saturating_mul(2)).min(1_500);
        }

        let snapshot = {
            let orders = self.orders.lock();
            tx_meta
                .iter()
                .map(|(tx_type, id)| {
                    let state = orders.get(id);
                    (
                        *tx_type,
                        id.0.clone(),
                        state.and_then(|s| s.order_index),
                        state.map(|s| s.status.clone()),
                        state.map(|s| s.filled),
                    )
                })
                .collect::<Vec<_>>()
        };
        bail!(
            "sendTxBatch timeout: reconciliation did not confirm batch within {}ms; snapshot={:?}",
            max_wait.as_millis(),
            snapshot
        )
    }

    async fn reconcile_orders_once(&self) -> Result<()> {
        let active = self.fetch_active_orders().await?;
        let mut seen = HashMap::new();
        for entry in active.iter() {
            if let Some(coi) = entry.client_order_index {
                seen.insert(coi, entry);
            }
        }

        let mut missing_for_inactive = Vec::new();
        let mut reports = Vec::new();
        {
            let mut guard = self.orders.lock();
            for (id, state) in guard.iter_mut() {
                if let Some(entry) = seen.get(&state.client_order_index) {
                    Self::update_from_entry(self.size_scale, entry, state, id, &mut reports);
                } else if matches!(state.status, OrderStatus::New | OrderStatus::PartiallyFilled) {
                    missing_for_inactive.push(id.clone());
                }
            }
        }

        if !missing_for_inactive.is_empty() {
            let inactive = self.fetch_inactive_orders(50).await?;
            let mut guard = self.orders.lock();
            for entry in inactive {
                if let Some(coi) = entry.client_order_index {
                    if let Some((id, state)) = guard.iter_mut().find(|(id, st)| {
                        missing_for_inactive.contains(id) && st.client_order_index == coi
                    }) {
                        if let Some(price_str) = entry.price.as_ref() {
                            if let Ok(price) = price_str.parse::<f64>() {
                                state.price = price;
                            }
                        }
                        if let Some(size_str) = entry.initial_base_amount.as_ref() {
                            if let Ok(size_int) = size_str.parse::<f64>() {
                                state.size = size_int / self.size_scale;
                            }
                        }
                        Self::update_from_entry(self.size_scale, &entry, state, id, &mut reports);
                        let mut status = entry
                            .status
                            .as_deref()
                            .map(Self::map_status)
                            .unwrap_or(OrderStatus::Unknown);
                        if status == OrderStatus::Unknown {
                            if let Some(raw) = entry.status.as_deref() {
                                eprintln!(
                                    "WARN: unknown inactive order status '{}'; treating as canceled",
                                    raw
                                );
                            }
                            status = OrderStatus::Canceled;
                        }
                        if status != state.status {
                            if matches!(
                                status,
                                OrderStatus::Canceled | OrderStatus::Rejected | OrderStatus::Filled
                            ) {
                                state.status = status.clone();
                                reports.push(ExecutionReport {
                                    client_order_id: id.clone(),
                                    exchange_order_id: state.exchange_order_id.clone(),
                                    status,
                                    filled_qty: 0.0,
                                    avg_fill_price: None,
                                    ts: entry.timestamp.map(|v| v as u64),
                                });
                            } else {
                                state.status = status;
                            }
                        }
                    }
                }
            }
        }

        if !reports.is_empty() {
            let mut pending = self.pending_reports.lock();
            pending.extend(reports);
            self.report_notify.notify_one();
        }
        Ok(())
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
        self.report_notify.notify_one();
    }

    async fn fetch_auth_token(&self) -> Result<String> {
        // default 10 minute auth token from signer
        let deadline = current_unix_ts() + 10 * 60;
        self.signer
            .auth_token(deadline, self.creds.api_key_index, self.creds.account_index)
            .await
    }

    async fn fetch_nonce(&self) -> Result<i64> {
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

    async fn fetch_nonce_with_backoff(&self, ctx: &str) -> Result<i64> {
        // nextNonce can be rate-limited (HTTP 429) if callers hot-loop when the nonce isn't seeded.
        // We back off aggressively to avoid hammering the endpoint.
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

    fn parse_f64(field: &str, s: Option<&String>) -> Option<f64> {
        let value = s?;
        match value.parse::<f64>() {
            Ok(v) if v.is_finite() => Some(v),
            Ok(_) => {
                log_parse_drop(
                    "lighter_gateway",
                    field,
                    &"non-finite number",
                    value,
                );
                None
            }
            Err(err) => {
                log_parse_drop("lighter_gateway", field, &err, value);
                None
            }
        }
    }

    fn map_status(status: &str) -> OrderStatus {
        match status {
            "filled" => OrderStatus::Filled,
            "canceled"
            | "cancelled"
            | "canceled-oco"
            | "cancelled-oco"
            | "canceled-expired"
            | "cancelled-expired"
            | "canceled-child"
            | "cancelled-child"
            | "closed" => {
                OrderStatus::Canceled
            }
            "rejected" => OrderStatus::Rejected,
            "partially_filled" | "partial_filled" | "partially-filled" => {
                OrderStatus::PartiallyFilled
            }
            "in-progress" | "pending" | "open" => OrderStatus::New,
            _ => OrderStatus::Unknown,
        }
    }

    fn update_from_entry(
        size_scale: f64,
        entry: &LighterOrderEntry,
        state: &mut OrderState,
        id: &ClientOrderId,
        reports: &mut Vec<ExecutionReport>,
    ) {
        if let Some(idx) = entry.order_index {
            state.order_index = Some(idx);
        }
        let filled_base = match Self::parse_f64(
            "filled_base_amount",
            entry.filled_base_amount.as_ref(),
        ) {
            Some(v) => v,
            None => {
                log_parse_drop(
                    "lighter_gateway",
                    "missing_filled_base_amount",
                    &"missing filled_base_amount",
                    "",
                );
                return;
            }
        };
        let filled_size = filled_base / size_scale;
        if filled_size > state.filled + 1e-9 {
            state.filled = filled_size;
            let done = entry
                .remaining_base_amount
                .as_ref()
                .and_then(|s| match s.parse::<f64>() {
                    Ok(v) if v.is_finite() => Some(v),
                    Ok(_) => {
                        log_parse_drop(
                            "lighter_gateway",
                            "non_finite_remaining_base_amount",
                            &"non-finite remaining_base_amount",
                            s,
                        );
                        None
                    }
                    Err(err) => {
                        log_parse_drop("lighter_gateway", "remaining_base_amount", &err, s);
                        None
                    }
                })
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
                filled_qty: filled_size,
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
            let (start_nonce, nonces) = self.peek_nonces(intents.len()).await?;
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
                    self.commit_nonces(start_nonce, intents.len());
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

    async fn cancel_and_submit(
        &self,
        cancel_ids: &[ClientOrderId],
        intents: &[QuoteIntent],
    ) -> Result<Vec<OrderAck>> {
        if cancel_ids.is_empty() {
            return self.submit(intents).await;
        }
        if intents.is_empty() {
            self.cancel_batch(cancel_ids).await?;
            return Ok(Vec::new());
        }

        let _nonce_lock = self.nonce_lock.lock().await;
        for attempt in 0..2 {
            let cancels_snapshot = {
                let guard = self.orders.lock();
                cancel_ids
                    .iter()
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

            let total = cancels_snapshot.len() + intents.len();
            let (start_nonce, nonces) = self.peek_nonces(total).await?;
            let mut nonce_iter = nonces.into_iter();

            let mut txs = Vec::with_capacity(total);

            // Group cancels first, then new orders (nonce order must be strictly increasing).
            for (id, order_index) in cancels_snapshot.iter().cloned() {
                let nonce = nonce_iter.next().expect("nonce iterator exhausted");
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

            for intent in intents {
                let nonce = nonce_iter.next().expect("nonce iterator exhausted");
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
                self.save_order(intent, client_order_index, exch_id);
                txs.push((signed, intent.client_order_id.clone()));
            }

            match self.send_batch(txs).await {
                Ok(acks) => {
                    self.commit_nonces(start_nonce, total);

                    // optimistic reports only after successful send
                    let ts = Some(current_unix_ms());
                    for id in cancel_ids {
                        self.push_report(ExecutionReport {
                            client_order_id: id.clone(),
                            exchange_order_id: None,
                            status: OrderStatus::Canceled,
                            filled_qty: 0.0,
                            avg_fill_price: None,
                            ts,
                        });
                    }
                    for (intent, ack) in intents.iter().zip(acks[cancels_snapshot.len()..].iter()) {
                        self.push_report(ExecutionReport {
                            client_order_id: intent.client_order_id.clone(),
                            exchange_order_id: ack.exchange_order_id.clone(),
                            status: OrderStatus::New,
                            filled_qty: 0.0,
                            avg_fill_price: Some(intent.price),
                            ts,
                        });
                    }

                    return Ok(acks.into_iter().skip(cancels_snapshot.len()).collect());
                }
                Err(err) => {
                    if attempt == 0 && Self::is_nonce_error(&err) {
                        let _ = self.refresh_nonce_from_server().await?;
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        bail!("unexpected cancel_and_submit retry exhaustion")
    }

    async fn cancel_batch(&self, ids: &[ClientOrderId]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let _nonce_lock = self.nonce_lock.lock().await;
        for attempt in 0..2 {
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
            let (start_nonce, nonces) = self.peek_nonces(orders_snapshot.len()).await?;

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
            let tx_count = txs.len();
            match self.send_batch(txs).await {
                Ok(_) => {
                    self.commit_nonces(start_nonce, tx_count);
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
                    self.report_notify.notify_one();
                    return Ok(());
                }
                Err(err) => {
                    if attempt == 0 && Self::is_nonce_error(&err) {
                        let _ = self.refresh_nonce_from_server().await?;
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        bail!("unexpected cancel retry exhaustion")
    }

    async fn poll_reports(&self) -> Result<Vec<ExecutionReport>> {
        loop {
            {
                let mut guard = self.pending_reports.lock();
                if !guard.is_empty() {
                    return Ok(guard.drain(..).collect());
                }
            }
            self.report_notify.notified().await;
        }
    }
}

pub async fn lighter_auth_token(
    creds: &LighterCredentials,
    debug_prints: bool,
) -> Result<String> {
    let client = LighterAuthClient::connect(creds.clone(), debug_prints).await?;
    client.auth_token().await
}
