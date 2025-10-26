#![allow(dead_code)]

// Pure endpoint definitions and simple helpers.
// Keep these zero-dep and allocation-light; use templates or small builders.

// ---------------- Bitget ----------------
pub struct BitgetWs;
impl BitgetWs {
    pub const PUBLIC_BASE: &str = "wss://ws.bitget.com/v2/ws/public";

    // Channels
    pub const BBO: &str = "books1"; // Best bid/offer (depth 1)
    pub const ORDERBOOK: &str = "books"; // Full orderbook
    pub const ORDERBOOK5: &str = "books5"; // Top 5 (unused for now)
    pub const ORDERBOOK15: &str = "books15"; // Top 15
    pub const TICKERS: &str = "ticker"; // Ticker data
    pub const PUBLIC_TRADES: &str = "trade"; // Public trades
    pub const CANDLES_1M: &str = "candle1m"; // 1-minute candles

    pub const INST_TYPE_FUTURES: &str = "USDT-FUTURES";

    // Subscription JSON format example:
    // {"op":"subscribe","args":[{"instType":"USDT-FUTURES","channel":"books1","instId":"BTCUSDT"}]}
    pub fn sub_msg(inst_id: &str, channel: &str) -> String {
        // minimal string builder without JSON deps
        let mut s = String::with_capacity(128);
        s.push_str("{\"op\":\"subscribe\",\"args\":[{\"instType\":\"");
        s.push_str(Self::INST_TYPE_FUTURES);
        s.push_str("\",\"channel\":\"");
        s.push_str(channel);
        s.push_str("\",\"instId\":\"");
        s.push_str(inst_id);
        s.push_str("\"}]}");
        s
    }
}

// ---------------- Bybit ----------------
pub struct BybitWs;
impl BybitWs {
    pub const BASE: &str = "wss://stream.bybit.com/v5/public/linear";

    // topic templates
    pub const ORDERBOOK_FMT: &str = "orderbook.{depth}.{symbol}";
    pub const KLINE_FMT: &str = "kline.{interval}.{symbol}";
    pub const TICKERS_FMT: &str = "tickers.{symbol}";
    pub const PUBLIC_TRADES_FMT: &str = "publicTrade.{symbol}";

    pub fn orderbook(depth: &str, symbol: &str) -> String {
        format!("orderbook.{depth}.{symbol}")
    }
    pub fn kline(interval: &str, symbol: &str) -> String {
        format!("kline.{interval}.{symbol}")
    }
    pub fn tickers(symbol: &str) -> String {
        format!("tickers.{symbol}")
    }
    pub fn public_trades(symbol: &str) -> String {
        format!("publicTrade.{symbol}")
    }
}

// ---------------- Binance ----------------
pub struct BinanceWs;
impl BinanceWs {
    pub const BASE: &str = "wss://fstream.binance.com";

    // stream templates (lowercase symbol, e.g., btcusdt)
    pub const ORDERBOOK_FMT: &str = "{symbol}@depth@100ms";
    pub const BBO_FMT: &str = "{symbol}@bookTicker";
    pub const TICKERS_FMT: &str = "{symbol}@markPrice@1s";
    pub const TRADES_FMT: &str = "{symbol}@aggTrade";

    pub fn orderbook(symbol: &str) -> String {
        format!("{symbol}@depth@100ms")
    }
    pub fn bbo(symbol: &str) -> String {
        format!("{symbol}@bookTicker")
    }
    pub fn tickers(symbol: &str) -> String {
        format!("{symbol}@markPrice@1s")
    }
    pub fn trades(symbol: &str) -> String {
        format!("{symbol}@aggTrade")
    }

    // Combined streams example:
    // wss://fstream.binance.com/stream?streams=bnbusdt@aggTrade/btcusdt@markPrice
    pub fn combined_stream_path(streams: &[&str]) -> String {
        let mut path = String::from("/stream?streams=");
        for (i, s) in streams.iter().enumerate() {
            if i > 0 {
                path.push('/');
            }
            path.push_str(s);
        }
        path
    }
}

pub struct BinanceGet;
impl BinanceGet {
    pub const BASE: &str = "https://fapi.binance.com/fapi/v1";
    pub const ORDERBOOK_PATH_FMT: &str = "/depth?symbol={symbol}&limit=1000";
    pub fn orderbook(symbol: &str) -> String {
        format!("/depth?symbol={symbol}&limit=1000")
    }
}

// ---------------- Gate.io ----------------
pub struct GateioGet;
impl GateioGet {
    pub const BASE: &str = "https://api.gateio.ws";
    pub const ORDERBOOK: &str = "/api/v4/futures/usdt/order_book";
    pub const ORDERBOOK_BTC: &str = "/api/v4/futures/btc/order_book";
    pub const FUTURES_TICKERS: &str = "/api/v4/futures/usdt/tickers";
    pub const GET_POSITIONS: &str = "/api/v4/futures/usdt/positions";
    pub const SINGLE_CONTRACT_FMT: &str = "/api/v4/futures/usdt/contracts/{contract}";
    pub fn single_contract(contract: &str) -> String {
        format!("/api/v4/futures/usdt/contracts/{contract}")
    }
}

pub struct GateioWs;
impl GateioWs {
    pub const BASE: &str = "wss://fx-ws.gateio.ws/v4/ws/usdt";

    // ping/pong
    pub const PING: &str = "futures.ping";
    pub const PONG: &str = "futures.pong";

    // public channels
    pub const ORDERBOOK_UPDATE: &str = "futures.order_book_update";
    pub const BBO: &str = "futures.book_ticker";
    pub const TICKER: &str = "futures.tickers";
    pub const PUBLIC_TRADES: &str = "futures.trades";
    pub const CANDLESTICKS: &str = "futures.candlesticks";

    // private channels (for future use)
    pub const USER_ORDERS: &str = "futures.autoorders";
    pub const USER_TRADES: &str = "futures.usertrades";
    pub const USER_BALANCES: &str = "futures.balances";
    pub const USER_POSITIONS: &str = "futures.positions";

    // write ops
    pub const CREATE_BATCH_ORDER: &str = "futures.order_batch_place";
    pub const LOGIN: &str = "futures.login";
    pub const CANCEL_BATCH_ORDER_IDS: &str = "futures.order_cancel_ids";
}

// ---------------- OKX ----------------
pub struct OkxWs;
impl OkxWs {
    pub const PUBLIC_BASE: &str = "wss://ws.okx.com:8443/ws/v5/public";
    pub const BUSINESS_BASE: &str = "wss://ws.okx.com:8443/ws/v5/business";

    pub const BOOKS: &str = "books";
    pub const BOOKS5: &str = "books5";
    pub const BBO_TBT: &str = "bbo-tbt";
    pub const TICKERS: &str = "tickers";
    pub const TRADES: &str = "trades";
    pub const TRADES_ALL: &str = "trades-all";

    #[inline]
    pub fn subscribe(inst_id: &str, channel: &str) -> String {
        Self::subscribe_multi(inst_id, &[channel])
    }

    #[inline]
    pub fn subscribe_multi(inst_id: &str, channels: &[&str]) -> String {
        let mut s = String::with_capacity(64 + channels.len() * 32);
        s.push_str("{\"op\":\"subscribe\",\"args\":[");
        for (idx, ch) in channels.iter().enumerate() {
            if idx > 0 {
                s.push(',');
            }
            s.push_str("{\"channel\":\"");
            s.push_str(ch);
            s.push_str("\",\"instId\":\"");
            s.push_str(inst_id);
            s.push_str("\"}");
        }
        s.push_str("]}");
        s
    }

    #[inline]
    pub fn subscribe_business(inst_id: &str, channel: &str) -> String {
        Self::subscribe_business_multi(inst_id, &[channel])
    }

    #[inline]
    pub fn subscribe_business_multi(inst_id: &str, channels: &[&str]) -> String {
        let mut s = String::with_capacity(64 + channels.len() * 32);
        s.push_str("{\"op\":\"subscribe\",\"args\":[");
        for (idx, ch) in channels.iter().enumerate() {
            if idx > 0 {
                s.push(',');
            }
            s.push_str("{\"channel\":\"");
            s.push_str(ch);
            s.push_str("\",\"instId\":\"");
            s.push_str(inst_id);
            s.push_str("\"}");
        }
        s.push_str("]}");
        s
    }
}
