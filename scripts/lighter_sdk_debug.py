#!/usr/bin/env python3
"""
Debug script using the Lighter Python SDK directly.
This helps identify if the issue is with credentials or the signer library.

Usage:
  python scripts/lighter_sdk_debug.py
"""

import asyncio
import os
from pathlib import Path
from decimal import Decimal, ROUND_DOWN

import requests

# Hardcoded config
BASE_URL = "https://mainnet.zklighter.elliot.ai"
ACCOUNT_INDEX = 498195
API_KEY_INDEX = 2
MARKET_SYMBOL = "XRP"
ORDER_PRICE = Decimal("1.80")
ORDER_SIZE = Decimal("10")

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
            print(f"[DEBUG] Full market info: {ob}")
            return ob
    raise SystemExit(f"Market {symbol} not found")


def scale(value: Decimal, decimals: int) -> int:
    return int((value * Decimal(10) ** decimals).to_integral_value(rounding=ROUND_DOWN))


async def main():
    load_env()
    private_key = get_private_key()
    
    print("=" * 60)
    print("Lighter SDK Debug")
    print("=" * 60)
    
    # Show key info (safely)
    key_clean = private_key.lstrip("0x") if private_key.startswith("0x") else private_key
    print(f"Private key length: {len(key_clean)} chars")
    print(f"Private key prefix: {key_clean[:8]}...")
    print(f"Account index: {ACCOUNT_INDEX}")
    print(f"API key index: {API_KEY_INDEX}")
    print()
    
    # Try importing the SDK
    try:
        import lighter
        print("✓ Lighter SDK imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import lighter SDK: {e}")
        print("Install with: pip install lighter-sdk")
        return
    
    # Show SDK version if available
    if hasattr(lighter, '__version__'):
        print(f"  SDK version: {lighter.__version__}")
    
    # Check what's available in the SDK
    print("\nAvailable in lighter module:")
    for name in sorted(dir(lighter)):
        if not name.startswith('_'):
            print(f"  - {name}")
    
    # Fetch market info
    print(f"\nFetching market info for {MARKET_SYMBOL}...")
    market = fetch_market(MARKET_SYMBOL)
    market_id = int(market["market_id"])
    price_decimals = int(market["supported_price_decimals"])
    size_decimals = int(market["supported_size_decimals"])
    print(f"Market ID: {market_id}")
    print(f"Price decimals: {price_decimals}")
    print(f"Size decimals: {size_decimals}")
    
    price_int = scale(ORDER_PRICE, price_decimals)
    size_int = scale(ORDER_SIZE, size_decimals)
    print(f"Scaled price: {price_int}")
    print(f"Scaled size: {size_int}")
    
    # Create signer client
    print("\n=== Creating SignerClient ===")
    try:
        client = lighter.SignerClient(
            url=BASE_URL,
            private_key=private_key,
            account_index=ACCOUNT_INDEX,
            api_key_index=API_KEY_INDEX,
        )
        print("✓ SignerClient created")
    except Exception as e:
        print(f"✗ Failed to create SignerClient: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Check client
    print("\n=== Checking client ===")
    try:
        err = client.check_client()
        if err:
            print(f"✗ check_client returned error: {err}")
        else:
            print("✓ check_client passed")
    except Exception as e:
        print(f"✗ check_client exception: {e}")
        import traceback
        traceback.print_exc()
    
    # Show client attributes
    print("\nSignerClient attributes:")
    for name in sorted(dir(client)):
        if not name.startswith('_'):
            val = getattr(client, name, None)
            if not callable(val):
                print(f"  {name} = {val}")
            else:
                print(f"  {name}()")
    
    # Try to create an order
    print("\n=== Testing create_order ===")
    import time
    client_order_index = int(time.time() * 1000)
    
    print(f"Parameters:")
    print(f"  market_index: {market_id}")
    print(f"  client_order_index: {client_order_index}")
    print(f"  base_amount: {size_int}")
    print(f"  price: {price_int}")
    print(f"  is_ask: False")
    print(f"  order_type: LIMIT")
    print(f"  time_in_force: POST_ONLY")
    
    try:
        order, tx_hash, err = await client.create_order(
            market_index=market_id,
            client_order_index=client_order_index,
            base_amount=size_int,
            price=price_int,
            is_ask=False,
            order_type=client.ORDER_TYPE_LIMIT,
            time_in_force=client.ORDER_TIME_IN_FORCE_POST_ONLY,
        )
        
        if err:
            print(f"✗ create_order error: {err}")
        else:
            print("✓ Order created successfully!")
            print(f"  tx_hash: {tx_hash}")
            print(f"  order: {order}")
    except Exception as e:
        print(f"✗ create_order exception: {e}")
        import traceback
        traceback.print_exc()
    
    await client.close()
    print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())

