"""
WebSocket latency probe for Lighter (jsonapi/sendtx over wss).

Flow per iteration:
1) Wait 3–5s.
2) Send a limit BTC order over WS, measure send→first response latency.
3) Wait 2–3s.
4) Send a cancel for the same order, measure send→first response latency.

Only the wire latency is measured (signing happens before the timer starts).
Configure credentials via env or CLI flags; defaults target testnet.
"""
import argparse
import asyncio
import datetime as dt
import json
import logging
import os
import random
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, Optional, Tuple

import requests
import websockets

import lighter

DEFAULT_BASE_URL = "https://testnet.zklighter.elliot.ai"


def utc_now() -> str:
    return dt.datetime.utcnow().isoformat(timespec="milliseconds") + "Z"


def to_ws_url(http_url: str) -> str:
    clean = http_url.rstrip("/")
    if clean.startswith("https://"):
        return "wss://" + clean[len("https://") :] + "/stream"
    if clean.startswith("http://"):
        return "ws://" + clean[len("http://") :] + "/stream"
    return "wss://" + clean + "/stream"


def fetch_market_config(base_url: str, symbol: str) -> Dict:
    resp = requests.get(f"{base_url}/api/v1/orderBooks", timeout=10)
    resp.raise_for_status()
    payload = resp.json()
    for ob in payload.get("order_books", []):
        sym = ob.get("symbol", "").upper()
        if sym.split("/")[0] == symbol.upper():
            return ob
    raise RuntimeError(f"Market {symbol} not found in /api/v1/orderBooks")


def fetch_top_of_book(base_url: str, market_id: int) -> Tuple[Optional[Decimal], Optional[Decimal]]:
    resp = requests.get(
        f"{base_url}/api/v1/orderBookOrders",
        params={"market_id": market_id, "limit": 1},
        timeout=10,
    )
    resp.raise_for_status()
    payload = resp.json()
    best_bid = None
    best_ask = None
    try:
        bids = payload.get("bids") or []
        asks = payload.get("asks") or []
        if bids:
            best_bid = Decimal(str(bids[0]["price"]))
        if asks:
            best_ask = Decimal(str(asks[0]["price"]))
    except (KeyError, InvalidOperation):
        pass
    return best_bid, best_ask


def price_to_int(price: Decimal, decimals: int) -> int:
    scale = Decimal(10) ** decimals
    return int((price * scale).to_integral_value())


def size_to_int(size: Decimal, decimals: int) -> int:
    scale = Decimal(10) ** decimals
    return int((size * scale).to_integral_value())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Measure Lighter WS order/cancel latency")
    parser.add_argument("--base-url", default=os.getenv("LIGHTER_BASE_URL", DEFAULT_BASE_URL))
    parser.add_argument("--api-key", default=os.getenv("LIGHTER_API_KEY_PRIVATE_KEY"), required=False, help="API_KEY_PRIVATE_KEY")
    parser.add_argument("--account-index", type=int, default=int(os.getenv("LIGHTER_ACCOUNT_INDEX", "0")))
    parser.add_argument("--api-key-index", type=int, default=int(os.getenv("LIGHTER_API_KEY_INDEX", "1")))
    parser.add_argument("--market-symbol", default=os.getenv("LIGHTER_MARKET", "BTC"))
    parser.add_argument("--order-size", type=float, default=float(os.getenv("LIGHTER_ORDER_SIZE", "0.001")), help="Order size in base units (e.g. BTC)")
    parser.add_argument("--side", choices=["buy", "sell"], default=os.getenv("LIGHTER_SIDE", "buy"))
    parser.add_argument(
        "--fixed-price",
        type=str,
        default=os.getenv("LIGHTER_FIXED_PRICE"),
        help="Explicit order price in quote units. When set, bypasses top-of-book price discovery.",
    )
    parser.add_argument("--price-offset", type=float, default=None, help="Multiplier relative to best bid/ask (None picks 0.5 for buys, 1.5 for sells)")
    parser.add_argument("--iterations", type=int, default=int(os.getenv("LIGHTER_ITERATIONS", "0")), help="0 = run forever")
    parser.add_argument("--ack-timeout", type=float, default=float(os.getenv("LIGHTER_ACK_TIMEOUT", "5.0")), help="Seconds to wait for a WS ack")
    parser.add_argument("--log-file", default=os.getenv("LIGHTER_LATENCY_LOG", "scripts/lighter_latency.log"))
    return parser.parse_args()


def build_price(
    best_bid: Optional[Decimal],
    best_ask: Optional[Decimal],
    side: str,
    offset: float,
    min_quote: Decimal,
    size: Decimal,
) -> Decimal:
    ref = best_bid if side == "buy" else best_ask
    if ref is None:
        ref = best_ask or best_bid or Decimal("50000")
    target = ref * Decimal(str(offset))
    min_price_needed = (min_quote / size) if size > 0 else Decimal("0")
    if target < min_price_needed:
        target = min_price_needed
    return target


def maybe_parse_tx_hash(raw_resp: str) -> Optional[str]:
    try:
        parsed = json.loads(raw_resp)
    except json.JSONDecodeError:
        return None
    if isinstance(parsed, dict):
        data = parsed.get("data", {})
        if isinstance(data, dict) and "tx_hash" in data:
            return str(data["tx_hash"])
        if "tx_hash" in parsed:
            return str(parsed["tx_hash"])
    return None


async def send_and_measure(ws, payload: Dict, timeout: float) -> Tuple[Optional[float], Optional[str]]:
    send_ts = time.perf_counter_ns()
    await ws.send(json.dumps(payload))
    try:
        raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
    except asyncio.TimeoutError:
        return None, None
    recv_ts = time.perf_counter_ns()
    latency_ms = (recv_ts - send_ts) / 1_000_000.0
    return latency_ms, raw


def next_api_key_and_nonce(client: lighter.SignerClient, api_key_index: int) -> Tuple[int, int]:
    resolved_api_key_index, nonce = client.get_api_key_nonce(
        api_key_index=api_key_index,
        nonce=lighter.SignerClient.DEFAULT_NONCE,
    )
    return resolved_api_key_index, nonce


async def sign_and_send_order(
    ws,
    client: lighter.SignerClient,
    api_key_index: int,
    market_id: int,
    order_idx: int,
    base_amount: int,
    price: int,
    is_ask: bool,
    timeout: float,
) -> Tuple[Optional[float], Optional[str], int]:
    resolved_api_key_index, nonce = next_api_key_and_nonce(client, api_key_index)
    tx_type, tx_info, _tx_hash, err = client.sign_create_order(
        market_index=market_id,
        client_order_index=order_idx,
        base_amount=base_amount,
        price=price,
        is_ask=is_ask,
        order_type=client.ORDER_TYPE_LIMIT,
        time_in_force=client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
        reduce_only=False,
        trigger_price=0,
        nonce=nonce,
        api_key_index=resolved_api_key_index,
    )
    if err is not None:
        raise RuntimeError(f"sign_create_order failed: {err}")

    payload = {
        "type": "jsonapi/sendtx",
        "data": {
            "id": f"create-{order_idx}-{nonce}",
            "tx_type": tx_type,
            "tx_info": json.loads(tx_info),
        },
    }
    latency_ms, raw = await send_and_measure(ws, payload, timeout)
    return latency_ms, raw, nonce


async def sign_and_send_cancel(
    ws,
    client: lighter.SignerClient,
    api_key_index: int,
    market_id: int,
    order_idx: int,
    timeout: float,
) -> Tuple[Optional[float], Optional[str], int]:
    resolved_api_key_index, nonce = next_api_key_and_nonce(client, api_key_index)
    tx_type, tx_info, _tx_hash, err = client.sign_cancel_order(
        market_index=market_id,
        order_index=order_idx,
        nonce=nonce,
        api_key_index=resolved_api_key_index,
    )
    if err is not None:
        raise RuntimeError(f"sign_cancel_order failed: {err}")

    payload = {
        "type": "jsonapi/sendtx",
        "data": {
            "id": f"cancel-{order_idx}-{nonce}",
            "tx_type": tx_type,
            "tx_info": json.loads(tx_info),
        },
    }
    latency_ms, raw = await send_and_measure(ws, payload, timeout)
    return latency_ms, raw, nonce


def log_latency(
    log_path: Path,
    event: str,
    latency_ms: Optional[float],
    client_order_index: int,
    nonce: int,
    tx_hash: Optional[str],
    response: Optional[str],
    extra: Dict,
) -> None:
    entry = {
        "ts": utc_now(),
        "event": event,
        "latency_ms": latency_ms,
        "client_order_index": client_order_index,
        "nonce": nonce,
        "tx_hash": tx_hash,
        "response": response,
    }
    entry.update(extra)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")


async def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if not args.api_key:
        raise SystemExit("API key missing. Set LIGHTER_API_KEY_PRIVATE_KEY or --api-key.")

    base_url = args.base_url.rstrip("/")
    price_offset = args.price_offset if args.price_offset is not None else (0.5 if args.side == "buy" else 1.5)

    market_cfg = fetch_market_config(base_url, args.market_symbol)
    market_id = int(market_cfg["market_id"])
    size_decimals = int(market_cfg["supported_size_decimals"])
    price_decimals = int(market_cfg["supported_price_decimals"])
    min_base = Decimal(str(market_cfg["min_base_amount"]))
    min_quote = Decimal(str(market_cfg["min_quote_amount"]))
    fixed_price = None
    if args.fixed_price is not None:
        try:
            fixed_price = Decimal(str(args.fixed_price))
        except InvalidOperation as exc:
            raise SystemExit(f"Invalid --fixed-price value {args.fixed_price!r}: {exc}") from exc
        if fixed_price <= 0:
            raise SystemExit(f"Invalid --fixed-price value {args.fixed_price!r}: must be > 0")

    order_size = Decimal(str(args.order_size))
    if order_size < min_base:
        logging.warning("Requested size %.8f is below min %.8f, bumping up", float(order_size), float(min_base))
        order_size = min_base
    base_amount_int = size_to_int(order_size, size_decimals)

    ws_url = to_ws_url(base_url)
    log_path = Path(args.log_file)
    logging.info(
        "Starting WS latency loop: market=%s (id=%s), side=%s, size=%s (raw int=%s), price_offset=%.3f, ws=%s",
        args.market_symbol.upper(),
        market_id,
        args.side,
        order_size,
        base_amount_int,
        price_offset,
        ws_url,
    )

    client = lighter.SignerClient(
        url=base_url,
        account_index=args.account_index,
        api_private_keys={args.api_key_index: args.api_key},
    )
    err = client.check_client()
    if err is not None:
        raise SystemExit(f"API key check failed: {err}")

    iteration = 0
    while args.iterations <= 0 or iteration < args.iterations:
        try:
            async with websockets.connect(ws_url, ping_interval=None) as ws:
                hello = await ws.recv()
                logging.debug("WS hello: %s", hello)

                while args.iterations <= 0 or iteration < args.iterations:
                    iteration += 1
                    await asyncio.sleep(random.uniform(3, 5))

                    if fixed_price is not None:
                        best_bid, best_ask = None, None
                        target_price = fixed_price
                    else:
                        best_bid, best_ask = fetch_top_of_book(base_url, market_id)
                        target_price = build_price(best_bid, best_ask, args.side, price_offset, min_quote, order_size)
                    price_int = price_to_int(target_price, price_decimals)

                    client_order_index = int(time.time() * 1000)
                    logging.info(
                        "[%s] Iteration %s placing order: price=%s (int=%s) size=%s",
                        args.market_symbol.upper(),
                        iteration,
                        target_price,
                        price_int,
                        order_size,
                    )

                    place_latency, place_resp, place_nonce = await sign_and_send_order(
                        ws=ws,
                        client=client,
                        api_key_index=args.api_key_index,
                        market_id=market_id,
                        order_idx=client_order_index,
                        base_amount=base_amount_int,
                        price=price_int,
                        is_ask=args.side == "sell",
                        timeout=args.ack_timeout,
                    )
                    logging.info("Order ack latency: %s ms (nonce=%s)", place_latency, place_nonce)
                    log_latency(
                        log_path,
                        "place",
                        place_latency,
                        client_order_index,
                        place_nonce,
                        maybe_parse_tx_hash(place_resp) if place_resp else None,
                        place_resp,
                        {"price": float(target_price), "size": float(order_size)},
                    )

                    await asyncio.sleep(random.uniform(2, 3))

                    cancel_latency, cancel_resp, cancel_nonce = await sign_and_send_cancel(
                        ws=ws,
                        client=client,
                        api_key_index=args.api_key_index,
                        market_id=market_id,
                        order_idx=client_order_index,
                        timeout=args.ack_timeout,
                    )
                    logging.info("Cancel ack latency: %s ms (nonce=%s)", cancel_latency, cancel_nonce)
                    log_latency(
                        log_path,
                        "cancel",
                        cancel_latency,
                        client_order_index,
                        cancel_nonce,
                        maybe_parse_tx_hash(cancel_resp) if cancel_resp else None,
                        cancel_resp,
                        {"price": float(target_price), "size": float(order_size)},
                    )
        except Exception as exc:  # reconnect on WS failure
            logging.warning("WS error (%s), reconnecting shortly", exc)
            await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
