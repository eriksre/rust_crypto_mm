#!/usr/bin/env python3
"""
Direct test of the Lighter native signer library (signer-arm64.dylib).

This script tests the FFI interface directly, bypassing the Python SDK,
to help debug signing issues.

Hardcoded for XRP market: Buy 10 @ $1.80

Usage:
  python scripts/lighter_native_signer_test.py
"""

import ctypes
import os
import sys
import time
from ctypes import (
    c_char_p, c_int, c_longlong, Structure, POINTER, pointer,
    create_string_buffer, cast
)
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Optional

import requests

# ============================================================================
# HARDCODED CONFIG
# ============================================================================
BASE_URL = "https://mainnet.zklighter.elliot.ai"
ACCOUNT_INDEX = 498195
API_KEY_INDEX = 2
CHAIN_ID = 304  # Lighter chain ID

# Order parameters
MARKET_SYMBOL = "XRP"
MARKET_ID = 7  # XRP market ID (from log output)
ORDER_PRICE = Decimal("1.80")
ORDER_SIZE = Decimal("10")
PRICE_DECIMALS = 6  # From log: price_decimals=6
SIZE_DECIMALS = 0   # From log: size_decimals=0

HERE = Path(__file__).resolve().parent.parent
SIGNER_LIB_PATH = HERE / "libs" / "lighter" / "signer-arm64.dylib"


class SignedTxResponse(Structure):
    """Matches the C struct returned by SignCreateOrder."""
    _fields_ = [
        ("tx_type", ctypes.c_uint8),
        ("tx_info", c_char_p),
        ("tx_hash", c_char_p),
        ("message_to_sign", c_char_p),
        ("err", c_char_p),
    ]


class StrOrErr(Structure):
    """Matches the C struct returned by CreateAuthToken."""
    _fields_ = [
        ("str_ptr", c_char_p),
        ("err", c_char_p),
    ]


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


def scale(value: Decimal, decimals: int) -> int:
    """Convert human-readable Decimal into exchange integer amount."""
    factor = Decimal(10) ** decimals
    return int((value * factor).to_integral_value(rounding=ROUND_DOWN))


def fetch_market_meta(base_url: str, symbol: str) -> dict:
    """Fetch market metadata from Lighter API."""
    resp = requests.get(f"{base_url.rstrip('/')}/api/v1/orderBooks", timeout=10)
    resp.raise_for_status()
    payload = resp.json()
    for ob in payload.get("order_books", []):
        sym = (ob.get("symbol") or "").upper()
        if sym == symbol.upper() or sym.replace("/", "_") == symbol.upper():
            return ob
    raise SystemExit(f"Market {symbol} not found in /api/v1/orderBooks")


def main():
    load_env_file(HERE / ".env")
    
    private_key = env("lighter_private_key", "LIGHTER_PRIVATE_KEY")
    if not private_key:
        raise SystemExit("Missing lighter_private_key in environment or .env file")

    # Strip 0x prefix if present
    if private_key.startswith("0x"):
        private_key = private_key[2:]

    print("=" * 60)
    print("Lighter Native Signer Test")
    print("=" * 60)
    
    # Check signer library exists
    if not SIGNER_LIB_PATH.exists():
        # Try amd64 version
        alt_path = HERE / "libs" / "lighter" / "signer-amd64.so"
        if alt_path.exists():
            lib_path = str(alt_path)
        else:
            raise SystemExit(f"Signer library not found at {SIGNER_LIB_PATH}")
    else:
        lib_path = str(SIGNER_LIB_PATH)
    
    print(f"Loading signer library: {lib_path}")
    
    try:
        lib = ctypes.CDLL(lib_path)
    except OSError as e:
        raise SystemExit(f"Failed to load signer library: {e}")
    
    print("Signer library loaded successfully!")
    
    # Define function signatures
    # CreateClient(url *C.char, private_key *C.char, chain_id C.int, 
    #              api_key_idx C.int, account_idx C.longlong) *C.char
    lib.CreateClient.argtypes = [c_char_p, c_char_p, c_int, c_int, c_longlong]
    lib.CreateClient.restype = c_char_p
    
    # SwitchAPIKey(api_key_idx C.int) *C.char
    lib.SwitchAPIKey.argtypes = [c_int]
    lib.SwitchAPIKey.restype = c_char_p
    
    # CheckClient(api_key_idx C.int, account_idx C.longlong) *C.char
    lib.CheckClient.argtypes = [c_int, c_longlong]
    lib.CheckClient.restype = c_char_p
    
    # SignCreateOrder(market_index, client_order_index, base_amount, price, 
    #                 is_ask, order_type, tif, reduce_only, trigger_price,
    #                 order_expiry, nonce, api_key_idx, account_idx) SignedTxResponse
    lib.SignCreateOrder.argtypes = [
        c_int,      # market_index
        c_longlong, # client_order_index
        c_longlong, # base_amount
        c_int,      # price
        c_int,      # is_ask
        c_int,      # order_type
        c_int,      # tif
        c_int,      # reduce_only
        c_int,      # trigger_price
        c_longlong, # order_expiry
        c_longlong, # nonce
        c_int,      # api_key_idx
        c_longlong, # account_idx
    ]
    lib.SignCreateOrder.restype = SignedTxResponse
    
    # CreateAuthToken(deadline_ms C.longlong) StrOrErr
    lib.CreateAuthToken.argtypes = [c_longlong]
    lib.CreateAuthToken.restype = StrOrErr

    # Fetch market metadata to verify
    print(f"\nFetching market metadata for {MARKET_SYMBOL}...")
    market = fetch_market_meta(BASE_URL, MARKET_SYMBOL)
    market_id = int(market["market_id"])
    price_decimals = int(market["supported_price_decimals"])
    size_decimals = int(market["supported_size_decimals"])
    
    print(f"Market ID: {market_id}")
    print(f"Price decimals: {price_decimals}")
    print(f"Size decimals: {size_decimals}")
    
    # Calculate scaled values
    price_int = scale(ORDER_PRICE, price_decimals)
    size_int = scale(ORDER_SIZE, size_decimals)
    
    print(f"\n=== Order Parameters ===")
    print(f"Price: ${ORDER_PRICE} -> {price_int}")
    print(f"Size: {ORDER_SIZE} -> {size_int}")
    print(f"Account index: {ACCOUNT_INDEX}")
    print(f"API key index: {API_KEY_INDEX}")
    print(f"Chain ID: {CHAIN_ID}")

    # Initialize client
    print(f"\n=== Initializing Signer Client ===")
    url_bytes = BASE_URL.encode('utf-8')
    key_bytes = private_key.encode('utf-8')
    
    print(f"Base URL: {BASE_URL}")
    print(f"Private key length: {len(private_key)} chars")
    
    err = lib.CreateClient(url_bytes, key_bytes, CHAIN_ID, API_KEY_INDEX, ACCOUNT_INDEX)
    if err:
        err_str = err.decode('utf-8') if err else ""
        if err_str:
            raise SystemExit(f"CreateClient failed: {err_str}")
    print("CreateClient: OK")
    
    # Switch API key
    err = lib.SwitchAPIKey(API_KEY_INDEX)
    if err:
        err_str = err.decode('utf-8') if err else ""
        if err_str:
            raise SystemExit(f"SwitchAPIKey failed: {err_str}")
    print("SwitchAPIKey: OK")
    
    # Check client
    err = lib.CheckClient(API_KEY_INDEX, ACCOUNT_INDEX)
    if err:
        err_str = err.decode('utf-8') if err else ""
        if err_str.strip():
            raise SystemExit(f"CheckClient failed: {err_str}")
    print("CheckClient: OK")
    
    # Create auth token (test)
    print(f"\n=== Testing Auth Token ===")
    deadline_ms = int(time.time() * 1000) + 600000  # 10 minutes from now
    auth_result = lib.CreateAuthToken(deadline_ms)
    if auth_result.err:
        err_str = auth_result.err.decode('utf-8') if auth_result.err else ""
        if err_str:
            print(f"CreateAuthToken failed: {err_str}")
    elif auth_result.str_ptr:
        token = auth_result.str_ptr.decode('utf-8')
        print(f"Auth token (first 50 chars): {token[:50]}...")
    else:
        print("CreateAuthToken returned empty result")

    # Sign create order
    print(f"\n=== Signing Create Order ===")
    client_order_index = int(time.time() * 1000)
    
    print(f"Parameters:")
    print(f"  market_index: {market_id}")
    print(f"  client_order_index: {client_order_index}")
    print(f"  base_amount: {size_int}")
    print(f"  price: {price_int}")
    print(f"  is_ask: 0 (BUY)")
    print(f"  order_type: 0 (LIMIT)")
    print(f"  tif: 2 (POST_ONLY)")
    print(f"  reduce_only: 0")
    print(f"  trigger_price: 0")
    print(f"  order_expiry: -1")
    print(f"  nonce: -1")
    print(f"  api_key_idx: {API_KEY_INDEX}")
    print(f"  account_idx: {ACCOUNT_INDEX}")
    
    result = lib.SignCreateOrder(
        market_id,           # market_index
        client_order_index,  # client_order_index
        size_int,            # base_amount
        price_int,           # price
        0,                   # is_ask (0 = buy)
        0,                   # order_type (0 = limit)
        2,                   # tif (2 = post_only)
        0,                   # reduce_only
        0,                   # trigger_price
        -1,                  # order_expiry
        -1,                  # nonce (-1 = auto)
        API_KEY_INDEX,       # api_key_idx
        ACCOUNT_INDEX,       # account_idx
    )
    
    print(f"\n=== SignCreateOrder Result ===")
    print(f"tx_type: {result.tx_type}")
    
    if result.err:
        err_str = result.err.decode('utf-8') if result.err else ""
        print(f"ERROR: {err_str if err_str else '(empty error string)'}")
    else:
        tx_info = result.tx_info.decode('utf-8') if result.tx_info else ""
        tx_hash = result.tx_hash.decode('utf-8') if result.tx_hash else ""
        msg_to_sign = result.message_to_sign.decode('utf-8') if result.message_to_sign else ""
        
        print(f"tx_info length: {len(tx_info)}")
        print(f"tx_hash: {tx_hash}")
        if tx_info:
            print(f"tx_info (first 100 chars): {tx_info[:100]}...")
        if msg_to_sign:
            print(f"message_to_sign: {msg_to_sign[:100]}...")
        
        if tx_info:
            # Now submit the batch
            print(f"\n=== Submitting to sendTxBatch ===")
            
            # For a single order, tx_types is just "14" (create order)
            # For batch, it would be "14,14" for two creates
            tx_types = "14"
            tx_infos = tx_info
            
            submit_url = f"{BASE_URL}/api/v1/sendTxBatch"
            print(f"POST {submit_url}")
            print(f"tx_types: {tx_types}")
            print(f"tx_infos length: {len(tx_infos)}")
            
            resp = requests.post(
                submit_url,
                data={"tx_types": tx_types, "tx_infos": tx_infos},
                timeout=30
            )
            
            print(f"\nResponse status: {resp.status_code}")
            print(f"Response body: {resp.text}")
            
            if resp.status_code == 200:
                data = resp.json()
                if data.get("code") == 200:
                    print("\n✓ Order submitted successfully!")
                    print(f"tx_hash: {data.get('tx_hash', [])}")
                else:
                    print(f"\n✗ Order failed: {data.get('message', 'unknown error')}")
            else:
                print(f"\n✗ HTTP error: {resp.status_code}")

    print("\n" + "=" * 60)
    print("Test complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()

