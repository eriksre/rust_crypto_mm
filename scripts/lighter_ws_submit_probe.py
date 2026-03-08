#!/usr/bin/env python3
"""
Probe Lighter create/cancel behavior with full raw WS and REST diagnostics.

This script is designed to isolate cases where:
- create batches are sent over WS
- create responses and/or private account updates are delayed or missing
- orders only become visible later through REST reconciliation

It uses the same jsonapi/sendtxbatch payload shape as the Rust gateway.
"""

import argparse
import asyncio
import json
import os
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from pathlib import Path
from typing import Any, Dict, Optional

import requests
import websockets

import lighter


DEFAULT_BASE_URL = "https://mainnet.zklighter.elliot.ai"
HERE = Path(__file__).resolve().parent.parent


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        os.environ.setdefault(key.strip(), val.strip())


def env(key: str, *aliases: str) -> Optional[str]:
    for candidate in (key, *aliases):
        value = os.getenv(candidate)
        if value:
            return value
    return None


def to_ws_url(http_url: str) -> str:
    clean = http_url.rstrip("/")
    if clean.startswith("https://"):
        return "wss://" + clean[len("https://") :] + "/stream"
    if clean.startswith("http://"):
        return "ws://" + clean[len("http://") :] + "/stream"
    raise SystemExit(f"Invalid base URL {http_url!r}")


def scale(value: Decimal, decimals: int) -> int:
    return int((value * (Decimal(10) ** decimals)).to_integral_value(rounding=ROUND_DOWN))


def fetch_market_config(base_url: str, symbol: str) -> Dict[str, Any]:
    resp = requests.get(f"{base_url}/api/v1/orderBooks", timeout=10)
    resp.raise_for_status()
    payload = resp.json()
    for ob in payload.get("order_books", []):
        sym = (ob.get("symbol") or "").upper()
        if sym == symbol.upper() or sym.replace("/", "_") == symbol.upper():
            return ob
    raise SystemExit(f"Market {symbol!r} not found in /api/v1/orderBooks")


def fetch_best_bid_ask(base_url: str, market_id: int) -> tuple[Optional[Decimal], Optional[Decimal]]:
    resp = requests.get(
        f"{base_url}/api/v1/orderBookOrders",
        params={"market_id": market_id, "limit": 1},
        timeout=10,
    )
    resp.raise_for_status()
    payload = resp.json()
    bids = payload.get("bids") or []
    asks = payload.get("asks") or []
    best_bid = Decimal(str(bids[0]["price"])) if bids else None
    best_ask = Decimal(str(asks[0]["price"])) if asks else None
    return best_bid, best_ask


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe Lighter WS submit/confirm behavior")
    parser.add_argument("--base-url", default=os.getenv("LIGHTER_BASE_URL", DEFAULT_BASE_URL))
    parser.add_argument("--api-key", default=env("lighter_private_key", "LIGHTER_PRIVATE_KEY"))
    parser.add_argument("--account-index", type=int, default=498195)
    parser.add_argument("--api-key-index", type=int, default=2)
    parser.add_argument("--market-symbol", default="XRP")
    parser.add_argument("--side", choices=["buy", "sell"], default="buy")
    parser.add_argument("--count", type=int, default=3, help="Number of create orders to send")
    parser.add_argument("--inter-send-ms", type=int, default=50, help="Delay between create submissions")
    parser.add_argument("--watch-seconds", type=float, default=5.0, help="How long to observe WS/REST after creates")
    parser.add_argument("--cancel-mode", choices=["none", "exchange", "client"], default="exchange")
    parser.add_argument("--fixed-price", type=str, default=None)
    parser.add_argument("--order-size", type=str, default="20")
    parser.add_argument("--log-file", default="logs/lighter_submit_probe.jsonl")
    return parser.parse_args()


def choose_price(
    side: str,
    fixed_price: Optional[Decimal],
    best_bid: Optional[Decimal],
    best_ask: Optional[Decimal],
    price_decimals: int,
) -> Decimal:
    if fixed_price is not None:
        return fixed_price
    tick = Decimal(1) / (Decimal(10) ** price_decimals)
    if side == "buy":
        if best_bid is None:
            raise SystemExit("best bid missing and no --fixed-price provided")
        return best_bid
    if best_ask is None:
        raise SystemExit("best ask missing and no --fixed-price provided")
    return best_ask


@dataclass
class ProbeOrder:
    client_order_index: int
    side: str
    price: str
    size: str
    create_req_id: str
    create_nonce: int
    create_payload: Dict[str, Any]
    create_tx_hash: str
    create_response: Optional[Dict[str, Any]] = None
    account_order_open: Optional[Dict[str, Any]] = None
    create_account_tx: Optional[Dict[str, Any]] = None
    order_index: Optional[int] = None
    active_rest_entry: Optional[Dict[str, Any]] = None
    inactive_rest_entry: Optional[Dict[str, Any]] = None
    tx_lookup: Optional[Dict[str, Any]] = None
    cancel_req_id: Optional[str] = None
    cancel_nonce: Optional[int] = None
    cancel_payload: Optional[Dict[str, Any]] = None
    cancel_tx_hash: Optional[str] = None
    cancel_response: Optional[Dict[str, Any]] = None
    cancel_account_tx: Optional[Dict[str, Any]] = None
    account_order_terminal: Optional[Dict[str, Any]] = None


class ProbeLogger:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def log(self, event: str, payload: Any) -> None:
        record = {"ts": utc_now(), "event": event, "payload": payload}
        print(f"[probe] {event} {json.dumps(payload, separators=(',', ':'))}")
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record) + "\n")


def parse_event_info(raw: str) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def build_sendtxbatch_payload(req_id: str, tx_type: int, tx_info: str) -> Dict[str, Any]:
    return {
        "type": "jsonapi/sendtxbatch",
        "data": {
            "id": req_id,
            "tx_types": json.dumps([tx_type]),
            "tx_infos": json.dumps([tx_info]),
        },
    }


def find_order_entry(entries: list[Dict[str, Any]], client_order_index: int) -> Optional[Dict[str, Any]]:
    for entry in entries:
        if entry.get("client_order_index") == client_order_index:
            return entry
    return None


def fetch_tx(base_url: str, tx_hash: str) -> Dict[str, Any]:
    resp = requests.get(
        f"{base_url}/api/v1/tx",
        params={"by": "hash", "value": tx_hash},
        timeout=10,
    )
    return {"http_status": resp.status_code, "body": resp.json() if resp.text else None, "raw": resp.text}


def fetch_orders_snapshot(base_url: str, token: str, account_index: int, market_id: int, inactive: bool) -> Dict[str, Any]:
    endpoint = "accountInactiveOrders" if inactive else "accountActiveOrders"
    params = {"account_index": account_index, "market_id": market_id}
    if inactive:
        params["limit"] = 50
    resp = requests.get(
        f"{base_url}/api/v1/{endpoint}",
        params=params,
        headers={"authorization": token},
        timeout=10,
    )
    body = resp.json() if resp.text else None
    return {"http_status": resp.status_code, "body": body, "raw": resp.text}


async def recv_loop(
    ws: websockets.WebSocketClientProtocol,
    logger: ProbeLogger,
    orders: Dict[int, ProbeOrder],
    stop_event: asyncio.Event,
) -> None:
    while not stop_event.is_set():
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=0.5)
        except asyncio.TimeoutError:
            continue
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        logger.log("ws_inbound_raw", raw)
        try:
            value = json.loads(raw)
        except json.JSONDecodeError:
            continue
        msg_type = value.get("type")
        if msg_type == "ping":
            pong = {"type": "pong"}
            await ws.send(json.dumps(pong))
            logger.log("ws_outbound_pong", pong)
            continue
        if msg_type == "jsonapi/sendtxbatch":
            req_id = value.get("id")
            for order in orders.values():
                if order.create_req_id == req_id:
                    order.create_response = value
                if order.cancel_req_id == req_id:
                    order.cancel_response = value
            continue
        if msg_type == "update/account_orders":
            channel_orders = value.get("orders", {})
            for entries in channel_orders.values():
                for entry in entries:
                    coi = entry.get("client_order_index")
                    if coi not in orders:
                        continue
                    order = orders[coi]
                    if entry.get("status") == "open":
                        order.account_order_open = entry
                    if entry.get("status") in {"canceled", "cancelled", "filled", "rejected"}:
                        order.account_order_terminal = entry
                    if entry.get("order_index"):
                        order.order_index = int(entry["order_index"])
            continue
        if msg_type == "update/account_tx":
            for tx in value.get("txs", []):
                event_info_raw = tx.get("event_info") or ""
                event_info = parse_event_info(event_info_raw)
                if tx.get("type") == 14 and event_info:
                    target = event_info.get("to") or {}
                    coi = target.get("u")
                    if coi in orders:
                        orders[coi].create_account_tx = tx
                        if target.get("i"):
                            orders[coi].order_index = int(target["i"])
                if tx.get("type") == 15 and event_info:
                    coi = event_info.get("u")
                    if coi in orders:
                        orders[coi].cancel_account_tx = tx
            continue


async def main() -> None:
    args = parse_args()
    load_env_file(HERE / ".env")
    if not args.api_key:
        raise SystemExit("Missing Lighter private key")

    try:
        order_size = Decimal(str(args.order_size))
    except InvalidOperation as exc:
        raise SystemExit(f"Invalid --order-size {args.order_size!r}: {exc}") from exc
    if order_size <= 0:
        raise SystemExit("--order-size must be > 0")

    fixed_price = None
    if args.fixed_price is not None:
        try:
            fixed_price = Decimal(str(args.fixed_price))
        except InvalidOperation as exc:
            raise SystemExit(f"Invalid --fixed-price {args.fixed_price!r}: {exc}") from exc
        if fixed_price <= 0:
            raise SystemExit("--fixed-price must be > 0")

    logger = ProbeLogger(Path(args.log_file))
    base_url = args.base_url.rstrip("/")
    market = fetch_market_config(base_url, args.market_symbol)
    market_id = int(market["market_id"])
    price_decimals = int(market["supported_price_decimals"])
    size_decimals = int(market["supported_size_decimals"])
    best_bid, best_ask = fetch_best_bid_ask(base_url, market_id)
    price = choose_price(args.side, fixed_price, best_bid, best_ask, price_decimals)
    price_int = scale(price, price_decimals)
    size_int = scale(order_size, size_decimals)

    logger.log(
        "probe_start",
        {
            "base_url": base_url,
            "ws_url": to_ws_url(base_url),
            "market_symbol": args.market_symbol,
            "market_id": market_id,
            "side": args.side,
            "count": args.count,
            "inter_send_ms": args.inter_send_ms,
            "watch_seconds": args.watch_seconds,
            "cancel_mode": args.cancel_mode,
            "price": str(price),
            "price_int": price_int,
            "size": str(order_size),
            "size_int": size_int,
        },
    )

    client = lighter.SignerClient(
        url=base_url,
        account_index=args.account_index,
        api_private_keys={args.api_key_index: args.api_key},
    )
    err = client.check_client()
    if err is not None:
        raise SystemExit(f"check_client failed: {err}")

    auth, err = client.create_auth_token_with_expiry(api_key_index=args.api_key_index)
    if err is not None or not auth:
        raise SystemExit(f"auth token creation failed: {err}")

    orders: Dict[int, ProbeOrder] = {}
    ws_url = to_ws_url(base_url)
    stop_event = asyncio.Event()

    try:
        async with websockets.connect(ws_url, ping_interval=None) as ws:
            hello = await ws.recv()
            if isinstance(hello, bytes):
                hello = hello.decode("utf-8", errors="replace")
            logger.log("ws_hello", hello)

            for channel in [
                f"account_orders/{market_id}/{args.account_index}",
                f"account_tx/{args.account_index}",
            ]:
                sub = {"type": "subscribe", "channel": channel, "auth": auth}
                logger.log("ws_outbound_subscribe", sub)
                await ws.send(json.dumps(sub))

            recv_task = asyncio.create_task(recv_loop(ws, logger, orders, stop_event))

            start_index = int(time.time() * 1000)
            for i in range(args.count):
                client_order_index = start_index + i
                api_key_index, nonce = client.get_api_key_nonce(
                    api_key_index=args.api_key_index,
                    nonce=lighter.SignerClient.DEFAULT_NONCE,
                )
                tx_type, tx_info, tx_hash, sign_err = client.sign_create_order(
                    market_index=market_id,
                    client_order_index=client_order_index,
                    base_amount=size_int,
                    price=price_int,
                    is_ask=args.side == "sell",
                    order_type=client.ORDER_TYPE_LIMIT,
                    time_in_force=client.ORDER_TIME_IN_FORCE_POST_ONLY,
                    reduce_only=False,
                    trigger_price=0,
                    nonce=nonce,
                    api_key_index=api_key_index,
                )
                if sign_err is not None:
                    raise SystemExit(f"sign_create_order failed for client_order_index={client_order_index}: {sign_err}")
                req_id = f"probe-create-{client_order_index}-{nonce}"
                payload = build_sendtxbatch_payload(req_id, int(tx_type), tx_info)
                order = ProbeOrder(
                    client_order_index=client_order_index,
                    side=args.side,
                    price=str(price),
                    size=str(order_size),
                    create_req_id=req_id,
                    create_nonce=nonce,
                    create_payload=payload,
                    create_tx_hash=str(tx_hash),
                )
                orders[client_order_index] = order
                logger.log("ws_outbound_create", payload)
                await ws.send(json.dumps(payload))
                await asyncio.sleep(args.inter_send_ms / 1000.0)

            await asyncio.sleep(args.watch_seconds)

            snapshot_token, err = client.create_auth_token_with_expiry(api_key_index=args.api_key_index)
            if err is not None or not snapshot_token:
                raise SystemExit(f"snapshot auth token creation failed: {err}")
            active_snapshot = fetch_orders_snapshot(
                base_url, snapshot_token, args.account_index, market_id, inactive=False
            )
            inactive_snapshot = fetch_orders_snapshot(
                base_url, snapshot_token, args.account_index, market_id, inactive=True
            )
            logger.log("rest_active_orders", active_snapshot)
            logger.log("rest_inactive_orders", inactive_snapshot)

            active_orders = ((active_snapshot.get("body") or {}).get("orders") or [])
            inactive_orders = ((inactive_snapshot.get("body") or {}).get("orders") or [])
            for order in orders.values():
                order.active_rest_entry = find_order_entry(active_orders, order.client_order_index)
                order.inactive_rest_entry = find_order_entry(inactive_orders, order.client_order_index)
                order.tx_lookup = fetch_tx(base_url, order.create_tx_hash)
                logger.log(
                    "rest_tx_lookup",
                    {
                        "client_order_index": order.client_order_index,
                        "create_tx_hash": order.create_tx_hash,
                        "lookup": order.tx_lookup,
                    },
                )
                if order.order_index is None and order.active_rest_entry and order.active_rest_entry.get("order_index"):
                    order.order_index = int(order.active_rest_entry["order_index"])
                if order.order_index is None and order.inactive_rest_entry and order.inactive_rest_entry.get("order_index"):
                    order.order_index = int(order.inactive_rest_entry["order_index"])

            if args.cancel_mode != "none":
                for order in orders.values():
                    cancel_index: Optional[int]
                    if args.cancel_mode == "exchange":
                        cancel_index = order.order_index
                    else:
                        cancel_index = order.client_order_index
                    if cancel_index is None:
                        logger.log(
                            "cancel_skipped",
                            {
                                "client_order_index": order.client_order_index,
                                "reason": f"missing {args.cancel_mode} cancel index",
                            },
                        )
                        continue
                    api_key_index, nonce = client.get_api_key_nonce(
                        api_key_index=args.api_key_index,
                        nonce=lighter.SignerClient.DEFAULT_NONCE,
                    )
                    tx_type, tx_info, tx_hash, sign_err = client.sign_cancel_order(
                        market_index=market_id,
                        order_index=cancel_index,
                        nonce=nonce,
                        api_key_index=api_key_index,
                    )
                    if sign_err is not None:
                        logger.log(
                            "cancel_sign_error",
                            {
                                "client_order_index": order.client_order_index,
                                "cancel_index": cancel_index,
                                "error": sign_err,
                            },
                        )
                        continue
                    req_id = f"probe-cancel-{order.client_order_index}-{nonce}"
                    payload = build_sendtxbatch_payload(req_id, int(tx_type), tx_info)
                    order.cancel_req_id = req_id
                    order.cancel_nonce = nonce
                    order.cancel_payload = payload
                    order.cancel_tx_hash = str(tx_hash)
                    logger.log("ws_outbound_cancel", payload)
                    await ws.send(json.dumps(payload))
                    await asyncio.sleep(0.2)

                await asyncio.sleep(max(args.watch_seconds, 2.0))

            stop_event.set()
            await recv_task
    finally:
        await client.close()

    summary = []
    for order in orders.values():
        summary.append(
            {
                "client_order_index": order.client_order_index,
                "create_req_id": order.create_req_id,
                "create_tx_hash": order.create_tx_hash,
                "create_response_seen": order.create_response is not None,
                "account_order_open_seen": order.account_order_open is not None,
                "create_account_tx_seen": order.create_account_tx is not None,
                "order_index": order.order_index,
                "rest_active_seen": order.active_rest_entry is not None,
                "rest_inactive_seen": order.inactive_rest_entry is not None,
                "cancel_req_id": order.cancel_req_id,
                "cancel_tx_hash": order.cancel_tx_hash,
                "cancel_response_seen": order.cancel_response is not None,
                "cancel_account_tx_seen": order.cancel_account_tx is not None,
                "account_order_terminal_seen": order.account_order_terminal is not None,
            }
        )
    logger.log("probe_summary", summary)


if __name__ == "__main__":
    asyncio.run(main())
