#!/usr/bin/env python3
"""
Lighter batch order test using the Python SDK (which works!).

Submits 2 buy orders for XRP @ $1.80 as a batch.

Usage:
  python scripts/lighter_batch_sdk.py
"""

import asyncio
import os
import time
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
import requests
import lighter

# ============================================================================
# CONFIG
# ============================================================================
BASE_URL = "https://mainnet.zklighter.elliot.ai"
ACCOUNT_INDEX = 498195
API_KEY_INDEX = 2

MARKET_SYMBOL = "XRP"
ORDER_PRICE = Decimal("1.80")
NUM_ORDERS = 2  # Batch of 2 orders

HERE = Path(__file__).resolve().parent.parent


def load_env():
    env_path = HERE / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ.setdefault(key.strip(), val.strip())


def get_private_key() -> str:
    for key in ("lighter_private_key", "LIGHTER_PRIVATE_KEY"):
        val = os.getenv(key)
        if val:
            return val
    raise SystemExit("Set lighter_private_key")


def fetch_market(symbol: str) -> dict:
    resp = requests.get(f"{BASE_URL}/api/v1/orderBooks", timeout=10)
    resp.raise_for_status()
    for ob in resp.json().get("order_books", []):
        if ob.get("symbol", "").upper() == symbol.upper():
            return ob
    raise SystemExit(f"Market {symbol} not found")


def scale(value: Decimal, decimals: int) -> int:
    return int((value * Decimal(10) ** decimals).to_integral_value(rounding=ROUND_DOWN))


async def main():
    load_env()
    private_key = get_private_key()

    print("=" * 60)
    print("Lighter BATCH Order Test (Python SDK)")
    print("=" * 60)

    # Fetch market info
    print(f"\nFetching market info for {MARKET_SYMBOL}...")
    market = fetch_market(MARKET_SYMBOL)
    market_id = int(market["market_id"])
    price_decimals = int(market["supported_price_decimals"])
    size_decimals = int(market["supported_size_decimals"])
    min_base = Decimal(str(market.get("min_base_amount", "1")))
    min_quote = Decimal(str(market.get("min_quote_amount", "1")))
    
    print(f"Market ID: {market_id}")
    print(f"Price decimals: {price_decimals}")
    print(f"Size decimals: {size_decimals}")
    print(f"Min base amount: {min_base}")
    print(f"Min quote amount: {min_quote}")
    
    # Calculate order size that meets minimums
    # min_quote / price = minimum size in base
    min_size_for_quote = min_quote / ORDER_PRICE
    order_size = max(min_base, min_size_for_quote)
    # Round up to nearest integer since size_decimals=0
    order_size = Decimal(int(order_size) + (1 if order_size % 1 > 0 else 0))
    
    print(f"\nCalculated order size: {order_size} XRP (to meet minimums)")
    print(f"Order notional: ${order_size * ORDER_PRICE}")
    
    price_int = scale(ORDER_PRICE, price_decimals)
    size_int = scale(order_size, size_decimals)
    
    print(f"Scaled price: {price_int}")
    print(f"Scaled size: {size_int}")

    # Create client
    print("\n=== Creating SignerClient ===")
    client = lighter.SignerClient(
        url=BASE_URL,
        private_key=private_key,
        account_index=ACCOUNT_INDEX,
        api_key_index=API_KEY_INDEX,
    )
    
    err = client.check_client()
    if err:
        raise SystemExit(f"check_client failed: {err}")
    print("✓ Client ready")

    # Submit batch of orders
    print(f"\n=== Submitting {NUM_ORDERS} Buy Orders ===")
    print(f"Each order: {order_size} XRP @ ${ORDER_PRICE}")
    print(f"Total: {order_size * NUM_ORDERS} XRP")
    
    results = []
    base_idx = int(time.time() * 1000)
    
    for i in range(NUM_ORDERS):
        client_order_index = base_idx + i
        print(f"\nOrder {i+1}/{NUM_ORDERS}:")
        print(f"  client_order_index: {client_order_index}")
        
        try:
            order, tx_hash, err = await client.create_order(
                market_index=market_id,
                client_order_index=client_order_index,
                base_amount=size_int,
                price=price_int,
                is_ask=False,  # BUY
                order_type=client.ORDER_TYPE_LIMIT,
                time_in_force=client.ORDER_TIME_IN_FORCE_POST_ONLY,
            )
            
            if err:
                print(f"  ✗ Error: {err}")
            else:
                tx_h = tx_hash.tx_hash if hasattr(tx_hash, 'tx_hash') else str(tx_hash)
                print(f"  ✓ Success! tx_hash: {tx_h}")
                results.append({"idx": client_order_index, "tx": tx_h})
        except Exception as e:
            print(f"  ✗ Exception: {e}")

    await client.close()

    # Summary
    print("\n" + "=" * 60)
    print(f"Results: {len(results)}/{NUM_ORDERS} orders submitted")
    for r in results:
        print(f"  - {r['idx']}: {r['tx']}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())

