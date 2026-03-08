use std::collections::{HashMap, HashSet};
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_longlong};
use std::path::Path;
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use futures_util::{SinkExt, StreamExt};
use libloading::{Library, Symbol};
use parking_lot::Mutex;
use reqwest::{Client, Url};
use serde::Deserialize;
use serde_json::{self, Value, json};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Mutex as AsyncMutex, Notify, mpsc as tokio_mpsc, oneshot};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config, tungstenite::Message,
};

use crate::base_classes::types::Side;
use crate::execution::types::{
    ClientOrderId, ExchangeOrderId, ExecutionReport, OrderAck, OrderStatus, QuoteIntent,
    TimeInForce,
};
use crate::execution::{ExecutionGateway, Venue};
use crate::utils::parsing::log_parse_drop;
use crate::utils::time::{current_unix_ms, current_unix_ts};

mod auth;
mod config;
mod gateway;
mod protocol;
mod reconcile;
mod rest;
mod resync;
mod signer;
mod state;
#[cfg(test)]
mod tests;
mod ws;

pub use auth::{LighterAuthClient, lighter_auth_token};
pub use config::{LighterCredentials, LighterGatewayConfig, LighterInstrument};
pub use gateway::LighterGateway;
pub use protocol::is_lighter_sendtx_quota_error;
pub use signer::resolve_lighter_signer_path;

use protocol::*;
use reconcile::{map_status, update_from_entry};
use rest::LighterRestClient;
use resync::LighterResyncWorker;
use signer::SignerHandle;
use state::*;
use ws::{LighterWsCommand, LighterWsConfig, LighterWsWorker};
