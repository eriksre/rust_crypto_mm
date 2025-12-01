#!/usr/bin/env python3
"""
Lighter batch order test script.

Submits a batch of post-only limit orders for XRP/USDT on Lighter.
Hardcoded: Buy 10 XRP at $1.80 (2 orders of 5 each, as a batch test).

Credentials are pulled from .env or environment:
  - lighter_private_key / LIGHTER_PRIVATE_KEY

Configuration from config/lighter_mvp.yaml:
  - account_index: 498195
  - api_key_index: 2
  - base_url: https://mainnet.zklighter.elliot.ai/

Usage:
  python scripts/lighter_batch_test.py
"""

import asyncio
import os
import time
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Dict, Optional, List, Tuple

import requests

try:
    import lighter
except ImportError as exc:
    raise SystemExit(
        "Could not import the Lighter SDK. Install with: pip install lighter-sdk"
    ) from exc

# ============================================================================
# HARDCODED CONFIG (from config/lighter_mvp.yaml)
# ============================================================================
BASE_URL = "https://mainnet.zklighter.elliot.ai"
ACCOUNT_INDEX = 498195
API_KEY_INDEX = 2

# Order parameters - hardcoded for testing
MARKET_SYMBOL = "XRP"
ORDER_PRICE = Decimal("1.80")  # $1.80 per XRP
TOTAL_SIZE = Decimal("20")      # 10 XRP total
ORDERS_IN_BATCH = 2             # Split into 2 orders for batch test
ORDER_SIZE = TOTAL_SIZE / ORDERS_IN_BATCH  # 5 XRP each

HERE = Path(__file__).resolve().parent.parent


def load_env_file(path: Path) -> None:
    """Load .env file into environment."""
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        os.environ.setdefault(key.strip(), val.strip())


def env(key: str, *aliases: str) -> Optional[str]:
    """Get environment variable with fallback aliases."""
    for candidate in (key, *aliases):
        val = os.getenv(candidate)
        if val:
            return val
    return None


def fetch_market_meta(base_url: str, symbol: str) -> Dict:
    """Fetch market metadata from Lighter API."""
    resp = requests.get(f"{base_url.rstrip('/')}/api/v1/orderBooks", timeout=10)
    resp.raise_for_status()
    payload = resp.json()
    for ob in payload.get("order_books", []):
        sym = (ob.get("symbol") or "").upper()
        if sym == symbol.upper() or sym.replace("/", "_") == symbol.upper():
            return ob
    raise SystemExit(f"Market {symbol} not found in /api/v1/orderBooks")


def scale(value: Decimal, decimals: int) -> int:
    """Convert human-readable Decimal into exchange integer amount."""
    factor = Decimal(10) ** decimals
    return int((value * factor).to_integral_value(rounding=ROUND_DOWN))


async def submit_batch_orders() -> None:
    """Submit a batch of post-only limit buy orders for XRP."""
    load_env_file(HERE / ".env")

    private_key = env("lighter_private_key", "LIGHTER_PRIVATE_KEY")
    if not private_key:
        raise SystemExit("Missing API private key (set lighter_private_key or LIGHTER_PRIVATE_KEY).")

    # Fetch market metadata
    print(f"Fetching market metadata for {MARKET_SYMBOL}...")
    market = fetch_market_meta(BASE_URL, MARKET_SYMBOL)
    market_id = int(market["market_id"])
    price_decimals = int(market["supported_price_decimals"])
    size_decimals = int(market["supported_size_decimals"])
    min_base = Decimal(str(market.get("min_base_amount", "0")))
    min_quote = Decimal(str(market.get("min_quote_amount", "0")))

    print(f"Market ID: {market_id}")
    print(f"Price decimals: {price_decimals}, Size decimals: {size_decimals}")
    print(f"Min base: {min_base}, Min quote: {min_quote}")

    if ORDER_SIZE < min_base:
        raise SystemExit(f"Size {ORDER_SIZE} is below min_base_amount {min_base}.")
    if ORDER_PRICE * ORDER_SIZE < min_quote:
        raise SystemExit(f"Notional {ORDER_PRICE * ORDER_SIZE} is below min_quote_amount {min_quote}.")

    # Scale values
    price_int = scale(ORDER_PRICE, price_decimals)
    base_amount_int = scale(ORDER_SIZE, size_decimals)

    print(f"\n=== Order Parameters ===")
    print(f"Account index: {ACCOUNT_INDEX}")
    print(f"API key index: {API_KEY_INDEX}")
    print(f"Price (human): ${ORDER_PRICE} -> Price (int): {price_int}")
    print(f"Size (human): {ORDER_SIZE} -> Size (int): {base_amount_int}")
    print(f"Batch size: {ORDERS_IN_BATCH} orders")

    # Create signer client
    print(f"\nInitializing Lighter signer client...")
    client = lighter.SignerClient(
        url=BASE_URL,
        private_key=private_key,
        account_index=ACCOUNT_INDEX,
        api_key_index=API_KEY_INDEX,
    )

    # Check client is valid
    err = client.check_client()
    if err:
        raise SystemExit(f"API key check failed: {err}")
    print("Client check passed!")

    # Create batch of orders
    print(f"\n=== Submitting Batch of {ORDERS_IN_BATCH} Buy Orders ===")
    print(f"Total: {TOTAL_SIZE} {MARKET_SYMBOL} @ ${ORDER_PRICE}")

    # Try using create_order_batch if available, otherwise submit individually
    # and collect the results
    orders_submitted = []
    
    for i in range(ORDERS_IN_BATCH):
        client_order_index = int(time.time() * 1000) + i
        
        print(f"\nOrder {i+1}/{ORDERS_IN_BATCH}:")
        print(f"  client_order_index: {client_order_index}")
        print(f"  market_id: {market_id}")
        print(f"  base_amount: {base_amount_int}")
        print(f"  price: {price_int}")
        print(f"  is_ask: False (BUY)")
        print(f"  order_type: LIMIT")
        print(f"  time_in_force: POST_ONLY")

        try:
            order, tx_hash, err = await client.create_order(
                market_index=market_id,
                client_order_index=client_order_index,
                base_amount=base_amount_int,
                price=price_int,
                is_ask=False,  # Buy order
                order_type=client.ORDER_TYPE_LIMIT,
                time_in_force=client.ORDER_TIME_IN_FORCE_POST_ONLY,
            )
            
            if err:
                print(f"  ERROR: {err}")
            else:
                print(f"  SUCCESS!")
                print(f"  tx_hash: {tx_hash.tx_hash if hasattr(tx_hash, 'tx_hash') else tx_hash}")
                print(f"  code: {tx_hash.code if hasattr(tx_hash, 'code') else 'N/A'}")
                orders_submitted.append({
                    "client_order_index": client_order_index,
                    "tx_hash": str(tx_hash.tx_hash if hasattr(tx_hash, 'tx_hash') else tx_hash),
                    "order": order,
                })
        except Exception as e:
            print(f"  EXCEPTION: {e}")
            import traceback
            traceback.print_exc()

    await client.close()

    # Summary
    print(f"\n=== Summary ===")
    print(f"Orders submitted: {len(orders_submitted)}/{ORDERS_IN_BATCH}")
    for o in orders_submitted:
        print(f"  - client_order_index={o['client_order_index']}, tx_hash={o['tx_hash']}")

    if len(orders_submitted) == 0:
        print("\nNo orders were successfully submitted. Check the errors above.")
    elif len(orders_submitted) < ORDERS_IN_BATCH:
        print(f"\nPartial success: {len(orders_submitted)} of {ORDERS_IN_BATCH} orders submitted.")
    else:
        print(f"\nAll {ORDERS_IN_BATCH} orders submitted successfully!")


async def submit_batch_via_api() -> None:
    """
    Alternative approach: Sign orders locally and submit as a true batch 
    via sendTxBatch API endpoint.
    """
    load_env_file(HERE / ".env")

    private_key = env("lighter_private_key", "LIGHTER_PRIVATE_KEY")
    if not private_key:
        raise SystemExit("Missing API private key (set lighter_private_key or LIGHTER_PRIVATE_KEY).")

    # Fetch market metadata
    print(f"Fetching market metadata for {MARKET_SYMBOL}...")
    market = fetch_market_meta(BASE_URL, MARKET_SYMBOL)
    market_id = int(market["market_id"])
    price_decimals = int(market["supported_price_decimals"])
    size_decimals = int(market["supported_size_decimals"])

    price_int = scale(ORDER_PRICE, price_decimals)
    base_amount_int = scale(ORDER_SIZE, size_decimals)

    print(f"Market ID: {market_id}, price_int: {price_int}, base_amount_int: {base_amount_int}")

    # Create signer client
    client = lighter.SignerClient(
        url=BASE_URL,
        private_key=private_key,
        account_index=ACCOUNT_INDEX,
        api_key_index=API_KEY_INDEX,
    )

    err = client.check_client()
    if err:
        raise SystemExit(f"API key check failed: {err}")
    print("Client check passed!")

    # Try to access lower-level signing if available
    # Check what methods are available on the client
    print("\nAvailable client methods:")
    methods = [m for m in dir(client) if not m.startswith('_')]
    for m in methods:
        print(f"  - {m}")

    # Check for batch methods
    if hasattr(client, 'create_order_batch'):
        print("\nFound create_order_batch method! Using batch submission...")
        orders = []
        for i in range(ORDERS_IN_BATCH):
            orders.append({
                "market_index": market_id,
                "client_order_index": int(time.time() * 1000) + i,
                "base_amount": base_amount_int,
                "price": price_int,
                "is_ask": False,
                "order_type": client.ORDER_TYPE_LIMIT,
                "time_in_force": client.ORDER_TIME_IN_FORCE_POST_ONLY,
            })
        result = await client.create_order_batch(orders)
        print(f"Batch result: {result}")
    else:
        print("\nNo batch method found. Falling back to sequential submission...")
        await submit_batch_orders()

    await client.close()


if __name__ == "__main__":
    print("=" * 60)
    print("Lighter Batch Order Test")
    print(f"Market: {MARKET_SYMBOL}/USDT")
    print(f"Order: BUY {TOTAL_SIZE} @ ${ORDER_PRICE}")
    print(f"Batch: {ORDERS_IN_BATCH} orders of {ORDER_SIZE} each")
    print("=" * 60)
    
    # Try the batch API first, fall back to sequential
    asyncio.run(submit_batch_orders())

