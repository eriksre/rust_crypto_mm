use std::env;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use rust_test::exchanges::endpoints::OkxWs;
use rust_test::exchanges::okx::OkxBook;
use rust_test::exchanges::okx::orderbook::OkxMsg;
use tungstenite::{Message, connect};
use url::Url;

fn main() -> Result<()> {
    let inst_id = env::args()
        .nth(1)
        .unwrap_or_else(|| "BTC-USDT-SWAP".to_string());

    println!("Connecting to OKX public WS for {inst_id} …");
    let (mut socket, _) = connect(Url::parse(OkxWs::PUBLIC_BASE).context("invalid OKX URL")?)
        .context("failed to connect to OKX websocket")?;

    let subscribe =
        OkxWs::subscribe_multi(&inst_id, &[OkxWs::BOOKS, OkxWs::BBO_TBT, OkxWs::TICKERS]);
    println!("> {}", subscribe);
    socket
        .send(Message::Text(subscribe))
        .context("failed to send subscription")?;

    let mut book = OkxBook::<1024>::new(
        &inst_id,
        OkxBook::<1024>::PRICE_SCALE,
        OkxBook::<1024>::QTY_SCALE,
    );

    let mut last_snapshot = Instant::now();
    loop {
        let msg = socket.read().context("error reading websocket message")?;
        match msg {
            Message::Text(text) => {
                handle_message(&mut book, &inst_id, &text, &mut last_snapshot);
            }
            Message::Binary(bin) => {
                if let Ok(text) = std::str::from_utf8(&bin) {
                    handle_message(&mut book, &inst_id, text, &mut last_snapshot);
                }
            }
            Message::Ping(payload) => {
                println!("<- ping ({} bytes)", payload.len());
                socket.send(Message::Pong(payload))?;
            }
            Message::Pong(_) => {}
            Message::Close(frame) => {
                println!("Websocket closed: {:?}", frame);
                break;
            }
            Message::Frame(_) => {}
        }
    }

    Ok(())
}

fn handle_message(
    book: &mut OkxBook<1024>,
    inst_id: &str,
    text: &str,
    last_snapshot: &mut Instant,
) {
    if let Ok(msg) = serde_json::from_str::<OkxMsg>(text) {
        let channel = msg.arg.channel.as_str();
        match channel {
            "books" => {
                if book.apply(&msg) {
                    log_book_state(book, inst_id, "books");
                }
            }
            "bbo-tbt" => {
                if book.apply_bbo(&msg) {
                    log_book_state(book, inst_id, "bbo-tbt");
                }
            }
            _ => {}
        }
    }

    if last_snapshot.elapsed() > Duration::from_secs(30) {
        println!("--- 30s heartbeat ---");
        log_book_state(book, inst_id, "periodic");
        *last_snapshot = Instant::now();
    }
}

fn log_book_state(book: &OkxBook<1024>, inst_id: &str, source: &str) {
    let mid = book.mid_price_f64().unwrap_or_default();
    let (bid_levels, ask_levels) = book.top_levels_f64(3);
    if let (Some(best_bid), Some(best_ask)) = (bid_levels.first(), ask_levels.first()) {
        println!(
            "[{source}] {inst_id} seq={} ts={} mid={:.6} bid={:.6}@{:.3} ask={:.6}@{:.3} checksum={:?}",
            book.last_seq(),
            book.last_ts(),
            mid,
            best_bid.0,
            best_bid.1,
            best_ask.0,
            best_ask.1,
            book.last_checksum()
        );
    } else {
        println!(
            "[{source}] {inst_id} seq={} ts={} mid={:.6} (book incomplete) checksum={:?}",
            book.last_seq(),
            book.last_ts(),
            mid,
            book.last_checksum()
        );
    }
}
