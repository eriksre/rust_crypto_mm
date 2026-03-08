use super::*;

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
type SignCreateOrderFnV0 = unsafe extern "C" fn(
    c_int,
    c_longlong,
    c_longlong,
    c_int,
    c_int,
    c_int,
    c_int,
    c_int,
    c_int,
    c_longlong,
    c_longlong,
) -> SignerResp;

type SignCancelOrderFnV0 = unsafe extern "C" fn(c_int, c_longlong, c_longlong) -> SignerResp;
type CreateAuthTokenFnV0 = unsafe extern "C" fn(c_longlong) -> SignerResp;

type SignCreateOrderFnV1 = unsafe extern "C" fn(
    c_int,
    c_longlong,
    c_longlong,
    c_int,
    c_int,
    c_int,
    c_int,
    c_int,
    c_int,
    c_longlong,
    c_longlong,
    c_int,
    c_longlong,
) -> SignedTxRespV1;

type SignCancelOrderFnV1 =
    unsafe extern "C" fn(c_int, c_longlong, c_longlong, c_int, c_longlong) -> SignedTxRespV1;

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
            if let Some([0xCF, 0xFA, 0xED, 0xFE] | [0xFE, 0xED, 0xFA, 0xCF]) = header {
                bail!(
                    "configured Lighter signer appears to be a macOS dylib, but this host is not macOS: {}. Use the Linux .so for your architecture.",
                    lib_path
                );
            }
        }

        let lib = unsafe { Library::new(lib_path) }
            .with_context(|| format!("load lighter signer lib {lib_path}"))?;
        unsafe {
            let create_client: Symbol<CreateClientFn> = lib
                .get(b"CreateClient")
                .context("missing CreateClient in signer lib")?;
            let check_client: Symbol<CheckClientFn> = lib
                .get(b"CheckClient")
                .context("missing CheckClient in signer lib")?;
            let create_client_fn = *create_client;
            let check_client_fn = *check_client;

            let abi =
                if let Ok(sign_create_order) = lib.get::<SignCreateOrderFnV1>(b"SignCreateOrder") {
                    let sign_cancel_order: Symbol<SignCancelOrderFnV1> = lib
                        .get(b"SignCancelOrder")
                        .context("missing SignCancelOrder in signer lib")?;
                    let create_auth_token: Symbol<CreateAuthTokenFnV1> = lib
                        .get(b"CreateAuthToken")
                        .context("missing CreateAuthToken in signer lib")?;
                    LighterSignerAbi::V1 {
                        sign_create_order: *sign_create_order,
                        sign_cancel_order: *sign_cancel_order,
                        create_auth_token: *create_auth_token,
                    }
                } else {
                    let switch_api_key: Symbol<SwitchApiKeyFn> = lib
                        .get(b"SwitchApiKey")
                        .context("missing SwitchApiKey in signer lib")?;
                    let sign_create_order: Symbol<SignCreateOrderFnV0> = lib
                        .get(b"SignCreateOrder")
                        .context("missing SignCreateOrder in signer lib")?;
                    let sign_cancel_order: Symbol<SignCancelOrderFnV0> = lib
                        .get(b"SignCancelOrder")
                        .context("missing SignCancelOrder in signer lib")?;
                    let create_auth_token: Symbol<CreateAuthTokenFnV0> = lib
                        .get(b"CreateAuthToken")
                        .context("missing CreateAuthToken in signer lib")?;
                    LighterSignerAbi::V0 {
                        switch_api_key: *switch_api_key,
                        sign_create_order: *sign_create_order,
                        sign_cancel_order: *sign_cancel_order,
                        create_auth_token: *create_auth_token,
                    }
                };

            Ok(Self {
                _lib: lib,
                create_client: create_client_fn,
                check_client: check_client_fn,
                abi,
            })
        }
    }

    fn cstring(s: &str) -> Result<CString> {
        CString::new(s).map_err(|_| anyhow!("string contains interior NUL byte"))
    }

    fn from_c(ptr: *mut c_char) -> Option<String> {
        if ptr.is_null() {
            return None;
        }
        let value = unsafe { CStr::from_ptr(ptr) }
            .to_string_lossy()
            .into_owned();
        let _ = unsafe { CString::from_raw(ptr) };
        Some(value)
    }

    fn ensure_client(
        &self,
        base_url: &str,
        api_key_hex: &str,
        chain_id: u32,
        api_key_idx: i32,
        account_idx: i64,
    ) -> Result<()> {
        let base_url = Self::cstring(base_url)?;
        let api_key_hex = Self::cstring(api_key_hex)?;
        let resp = unsafe {
            (self.create_client)(
                base_url.as_ptr(),
                api_key_hex.as_ptr(),
                chain_id as c_int,
                api_key_idx as c_int,
                account_idx as c_longlong,
            )
        };
        if resp.is_null() {
            return Ok(());
        }
        let err = Self::from_c(resp).unwrap_or_else(|| "unknown CreateClient error".to_string());
        bail!("CreateClient failed: {err}")
    }

    fn switch_api_key(&self, api_key_idx: i32) -> Result<()> {
        let LighterSignerAbi::V0 { switch_api_key, .. } = self.abi else {
            return Ok(());
        };
        let resp = unsafe { (switch_api_key)(api_key_idx as c_int) };
        if resp.is_null() {
            return Ok(());
        }
        let err = Self::from_c(resp).unwrap_or_else(|| "unknown SwitchApiKey error".to_string());
        bail!("SwitchApiKey failed: {err}")
    }

    fn check_client(&self, api_key_idx: i32, account_idx: i64) -> Result<()> {
        let resp = unsafe { (self.check_client)(api_key_idx as c_int, account_idx as c_longlong) };
        if resp.is_null() {
            return Ok(());
        }
        let err = Self::from_c(resp).unwrap_or_else(|| "unknown CheckClient error".to_string());
        bail!("CheckClient failed: {err}")
    }

    #[allow(clippy::too_many_arguments)]
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
                        i32::from(is_ask),
                        order_type as c_int,
                        tif as c_int,
                        i32::from(reduce_only),
                        trigger_price as c_int,
                        order_expiry as c_longlong,
                        nonce as c_longlong,
                    )
                };
                if let Some(err) = Self::from_c(resp.err) {
                    bail!("SignCreateOrder failed: {err}");
                }
                Ok(SignedTx {
                    tx_type: 0,
                    tx_info: Self::from_c(resp.str_ptr)
                        .ok_or_else(|| anyhow!("missing create tx_info"))?,
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
                        i32::from(is_ask),
                        order_type as c_int,
                        tif as c_int,
                        i32::from(reduce_only),
                        trigger_price as c_int,
                        order_expiry as c_longlong,
                        nonce as c_longlong,
                        api_key_idx as c_int,
                        account_idx as c_longlong,
                    )
                };
                if let Some(err) = Self::from_c(resp.err) {
                    bail!("SignCreateOrder failed: {err}");
                }
                Ok(SignedTx {
                    tx_type: resp.tx_type,
                    tx_info: Self::from_c(resp.tx_info)
                        .ok_or_else(|| anyhow!("missing create tx_info"))?,
                    tx_hash: Self::from_c(resp.tx_hash),
                })
            }
        }
    }

    fn sign_cancel_order(
        &self,
        market_index: u8,
        cancel_index: i64,
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
                        cancel_index as c_longlong,
                        nonce as c_longlong,
                    )
                };
                if let Some(err) = Self::from_c(resp.err) {
                    bail!("SignCancelOrder failed: {err}");
                }
                Ok(SignedTx {
                    tx_type: 0,
                    tx_info: Self::from_c(resp.str_ptr)
                        .ok_or_else(|| anyhow!("missing cancel tx_info"))?,
                    tx_hash: None,
                })
            }
            LighterSignerAbi::V1 {
                sign_cancel_order, ..
            } => {
                let resp = unsafe {
                    (sign_cancel_order)(
                        market_index as c_int,
                        cancel_index as c_longlong,
                        nonce as c_longlong,
                        api_key_idx as c_int,
                        account_idx as c_longlong,
                    )
                };
                if let Some(err) = Self::from_c(resp.err) {
                    bail!("SignCancelOrder failed: {err}");
                }
                Ok(SignedTx {
                    tx_type: resp.tx_type,
                    tx_info: Self::from_c(resp.tx_info)
                        .ok_or_else(|| anyhow!("missing cancel tx_info"))?,
                    tx_hash: Self::from_c(resp.tx_hash),
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

pub fn resolve_lighter_signer_path(lib_path: &str) -> Result<String> {
    if Path::new(lib_path).exists() {
        return Ok(lib_path.to_string());
    }
    let mut candidates: Vec<String> = vec![];

    if cfg!(target_os = "macos") {
        if lib_path.ends_with(".so") {
            candidates.push(lib_path.trim_end_matches(".so").to_string() + ".dylib");
        }
    } else {
        if lib_path.ends_with(".dylib") {
            candidates.push(lib_path.trim_end_matches(".dylib").to_string() + ".so");
        }
        if cfg!(target_arch = "aarch64") {
            candidates.push(lib_path.replace("amd64", "arm64").replace(".dylib", ".so"));
        } else if cfg!(target_arch = "x86_64") {
            candidates.push(lib_path.replace("arm64", "amd64").replace(".dylib", ".so"));
        }
    }

    if let Some(found) = candidates.iter().find(|p| Path::new(p.as_str()).exists()) {
        return Ok(found.clone());
    }

    bail!(
        "Lighter signer library not found at {} (candidates tried: {:?}); please provide the correct native signer for this OS/arch: signer-amd64.so (Linux x86_64), signer-arm64.so (Linux aarch64), signer-arm64.dylib (macOS)",
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
        cancel_index: i64,
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
pub(super) struct SignerHandle {
    tx: mpsc::Sender<SignerRequest>,
    debug_prints: bool,
}

impl SignerHandle {
    pub(super) fn new(lib_path: String, debug_prints: bool) -> Result<Self> {
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
                        cancel_index,
                        nonce,
                        api_key_idx,
                        account_idx,
                        resp,
                    } => {
                        if debug_prints {
                            eprintln!(
                                "[lighter-sign-thread] cancel mid={} cancel_idx={} ak_idx={} acct={}",
                                market_index, cancel_index, api_key_idx, account_idx
                            );
                        }
                        let res = signer.switch_api_key(api_key_idx).and_then(|_| {
                            signer.sign_cancel_order(
                                market_index,
                                cancel_index,
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
        Ok(Self { tx, debug_prints })
    }

    pub(super) async fn init_client(
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

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn sign_order(
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
            if tx.tx_type == 0 {
                tx.tx_type = LIGHTER_TX_TYPE_CREATE_ORDER;
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

    pub(super) async fn sign_cancel(
        &self,
        market_index: u8,
        cancel_index: i64,
        nonce: i64,
        api_key_idx: i32,
        account_idx: i64,
    ) -> Result<SignedTx> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(SignerRequest::SignCancel {
                market_index,
                cancel_index,
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
                tx.tx_type = LIGHTER_TX_TYPE_CANCEL_ORDER;
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

    pub(super) async fn auth_token(
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
