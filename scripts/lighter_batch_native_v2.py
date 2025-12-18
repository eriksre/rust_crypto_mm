#!/usr/bin/env python3
"""
Lighter batch order test using the native signer library directly (NO SDK).

Uses the same signer library that the SDK uses internally.

Submits 2 buy orders for XRP @ $1.80.

Usage:
  python scripts/lighter_batch_native_v2.py
"""

import ctypes
import json
import os
import sys
import time
from ctypes import c_char_p, c_int, c_longlong, Structure
from decimal import Decimal, ROUND_DOWN
from pathlib import Path

import requests

# ============================================================================
# CONFIG
# ============================================================================
BASE_URL = "https://mainnet.zklighter.elliot.ai"
ACCOUNT_INDEX = 498195
API_KEY_INDEX = 2
CHAIN_ID = 304

MARKET_SYMBOL = "XRP"
ORDER_PRICE = Decimal("1.80")
NUM_ORDERS = 2

HERE = Path(__file__).resolve().parent.parent


class StrOrErr(Structure):
    """Return type used by SDK's signer - just str + err, no tx_type/tx_hash."""
    _fields_ = [
        ("str", c_char_p),
        ("err", c_char_p),
    ]

class SignedTxResponse(Structure):
    _fields_ = [
        ("tx_type", ctypes.c_uint8),
        ("tx_info", c_char_p),
        ("tx_hash", c_char_p),
        ("message_to_sign", c_char_p),
        ("err", c_char_p),
    ]


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
            return val  # Keep as-is, don't strip 0x
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


def fetch_nonce() -> int:
    """Fetch the next nonce from the Lighter API."""
    url = f"{BASE_URL}/api/v1/nextNonce?account_index={ACCOUNT_INDEX}&api_key_index={API_KEY_INDEX}"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    nonce = data.get("nonce", 1)
    print(f"Fetched nonce from server: {nonce}")
    return nonce


def find_signer_lib() -> str:
    """Find the signer library - prefer the one bundled with the SDK."""
    override = os.getenv("LIGHTER_SIGNER_LIB")
    if override:
        return override

    # Check SDK location first (this is what works!)
    venv_path = HERE / ".venv" / "lib"
    if venv_path.exists():
        for pydir in venv_path.iterdir():
            if pydir.name.startswith("python"):
                sdk_signer = pydir / "site-packages" / "lighter" / "signers" / "signer-arm64.dylib"
                if sdk_signer.exists():
                    return str(sdk_signer)
                sdk_signer_so_arm = pydir / "site-packages" / "lighter" / "signers" / "signer-arm64.so"
                if sdk_signer_so_arm.exists():
                    return str(sdk_signer_so_arm)
                sdk_signer_so = pydir / "site-packages" / "lighter" / "signers" / "signer-amd64.so"
                if sdk_signer_so.exists():
                    return str(sdk_signer_so)
    
    # Fall back to libs folder
    lib_arm = HERE / "libs" / "lighter" / "signer-arm64.dylib"
    if lib_arm.exists():
        return str(lib_arm)
    lib_arm_so = HERE / "libs" / "lighter" / "signer-arm64.so"
    if lib_arm_so.exists():
        return str(lib_arm_so)
    lib_amd = HERE / "libs" / "lighter" / "signer-amd64.so"
    if lib_amd.exists():
        return str(lib_amd)
    
    raise SystemExit("No signer library found")


def load_signer(lib_path: str):
    """Load and configure the signer library."""
    lib = ctypes.CDLL(lib_path)
    
    lib.CreateClient.argtypes = [c_char_p, c_char_p, c_int, c_int, c_longlong]
    lib.CreateClient.restype = c_char_p

    lib.CheckClient.argtypes = [c_int, c_longlong]
    lib.CheckClient.restype = c_char_p

    # ABI detection:
    # - older signer exports SwitchAPIKey and returns StrOrErr from SignCreateOrder (11 args)
    # - newer lighter-go signer does NOT export SwitchAPIKey and returns SignedTxResponse (13 args incl api_key_idx/account_idx)
    has_switch = hasattr(lib, "SwitchAPIKey")
    if has_switch:
        lib.SwitchAPIKey.argtypes = [c_int]
        lib.SwitchAPIKey.restype = c_char_p

        lib.SignCreateOrder.argtypes = [
            c_int,       # market_index
            c_longlong,  # client_order_index
            c_longlong,  # base_amount
            c_int,       # price
            c_int,       # is_ask
            c_int,       # order_type
            c_int,       # time_in_force
            c_int,       # reduce_only
            c_int,       # trigger_price
            c_longlong,  # order_expiry
            c_longlong,  # nonce
        ]
        lib.SignCreateOrder.restype = StrOrErr
        lib._lighter_abi = "v0"
    else:
        lib.SignCreateOrder.argtypes = [
            c_int,       # market_index
            c_longlong,  # client_order_index
            c_longlong,  # base_amount
            c_int,       # price
            c_int,       # is_ask
            c_int,       # order_type
            c_int,       # time_in_force
            c_int,       # reduce_only
            c_int,       # trigger_price
            c_longlong,  # order_expiry
            c_longlong,  # nonce
            c_int,       # api_key_idx
            c_longlong,  # account_idx
        ]
        lib.SignCreateOrder.restype = SignedTxResponse
        lib._lighter_abi = "v1"
    
    return lib


def main():
    load_env()
    private_key = get_private_key()

    print("=" * 60)
    print("Lighter BATCH Order Test (Native Signer, NO SDK)")
    print("=" * 60)
    
    print(f"\nPrivate key length: {len(private_key)} chars")
    print(f"Private key prefix: {private_key[:12]}...")

    # Find and load signer
    lib_path = find_signer_lib()
    print(f"\nUsing signer library: {lib_path}")
    lib = load_signer(lib_path)
    print("✓ Signer loaded")

    # Initialize client
    print("\n=== Initializing Client ===")
    err = lib.CreateClient(
        BASE_URL.encode(),
        private_key.encode(),
        CHAIN_ID,
        API_KEY_INDEX,
        ACCOUNT_INDEX
    )
    if err:
        s = err.decode() if err else ""
        if s:
            raise SystemExit(f"CreateClient failed: {s}")
    print("✓ CreateClient OK")
    if getattr(lib, "_lighter_abi", "v0") == "v0":
        err = lib.SwitchAPIKey(API_KEY_INDEX)
        if err:
            s = err.decode() if err else ""
            if s:
                raise SystemExit(f"SwitchAPIKey failed: {s}")
        print("✓ SwitchAPIKey OK")

    err = lib.CheckClient(API_KEY_INDEX, ACCOUNT_INDEX)
    if err:
        s = err.decode() if err else ""
        if s.strip():
            raise SystemExit(f"CheckClient failed: {s}")
    print("✓ CheckClient OK")

    # Fetch market info
    print(f"\n=== Fetching Market Info ===")
    market = fetch_market(MARKET_SYMBOL)
    market_id = int(market["market_id"])
    price_decimals = int(market["supported_price_decimals"])
    size_decimals = int(market["supported_size_decimals"])
    min_base = Decimal(str(market.get("min_base_amount", "1")))
    min_quote = Decimal(str(market.get("min_quote_amount", "1")))

    print(f"Market ID: {market_id}")
    print(f"Price decimals: {price_decimals}, Size decimals: {size_decimals}")
    print(f"Min base: {min_base}, Min quote: {min_quote}")

    # Calculate valid order size
    min_size_for_quote = min_quote / ORDER_PRICE
    order_size = max(min_base, min_size_for_quote)
    order_size = Decimal(int(order_size) + (1 if order_size % 1 > 0 else 0))

    price_int = scale(ORDER_PRICE, price_decimals)
    size_int = scale(order_size, size_decimals)

    print(f"\nOrder size: {order_size} XRP @ ${ORDER_PRICE}")
    print(f"Scaled: price={price_int}, size={size_int}")

    # Fetch nonce from server (required!)
    print(f"\n=== Fetching Nonce ===")
    nonce = fetch_nonce()

    # Sign orders
    print(f"\n=== Signing {NUM_ORDERS} Orders ===")
    signed_txs = []
    base_idx = int(time.time() * 1000)
    
    # Calculate order expiry (28 days from now, in milliseconds)
    order_expiry_ms = int(time.time() * 1000) + (28 * 24 * 60 * 60 * 1000)

    for i in range(NUM_ORDERS):
        client_order_idx = base_idx + i
        current_nonce = nonce + i  # Increment nonce for each order
        
        print(f"\nOrder {i+1}/{NUM_ORDERS}:")
        print(f"  client_idx={client_order_idx}")
        print(f"  nonce={current_nonce}")
        print(f"  order_expiry={order_expiry_ms}")

        # SDK uses 11 params - no api_key_idx or account_idx!
        if getattr(lib, "_lighter_abi", "v0") == "v0":
            result = lib.SignCreateOrder(
                market_id,
                client_order_idx,
                size_int,
                price_int,
                0,
                0,
                2,
                0,
                0,
                order_expiry_ms,
                current_nonce,
            )
            if result.err:
                err_str = result.err.decode() if result.err else ""
                print(f"  ✗ SignCreateOrder failed: {err_str if err_str else '(empty error)'}")
                continue
            tx_info = result.str.decode() if result.str else ""
            tx_type = 14
        else:
            result = lib.SignCreateOrder(
                market_id,
                client_order_idx,
                size_int,
                price_int,
                0,
                0,
                2,
                0,
                0,
                order_expiry_ms,
                current_nonce,
                API_KEY_INDEX,
                ACCOUNT_INDEX,
            )
            if result.err:
                err_str = result.err.decode() if result.err else ""
                print(f"  ✗ SignCreateOrder failed: {err_str if err_str else '(empty error)'}")
                continue
            tx_info = result.tx_info.decode() if result.tx_info else ""
            tx_type = int(result.tx_type) if result.tx_type else 14

        if not tx_info:
            print("  ✗ SignCreateOrder returned empty tx_info")
            continue

        print(f"  ✓ Signed! tx_info_len={len(tx_info)}")
        print(f"  tx_info: {tx_info[:100]}...")
        signed_txs.append((tx_type, tx_info))

    if not signed_txs:
        print("\n✗ No orders signed successfully")
        return

    # Submit batch via sendTxBatch
    # API expects JSON arrays, not comma-separated strings!
    print(f"\n=== Submitting Batch ({len(signed_txs)} orders) ===")
    
    tx_types_list = [t[0] for t in signed_txs]  # [14, 14]
    tx_infos_list = [t[1] for t in signed_txs]  # ["{...}", "{...}"]
    
    tx_types_json = json.dumps(tx_types_list)
    tx_infos_json = json.dumps(tx_infos_list)
    
    print(f"POST {BASE_URL}/api/v1/sendTxBatch")
    print(f"tx_types: {tx_types_json}")
    print(f"tx_infos: {tx_infos_json[:200]}...")
    
    resp = requests.post(
        f"{BASE_URL}/api/v1/sendTxBatch",
        data={"tx_types": tx_types_json, "tx_infos": tx_infos_json},
        timeout=30
    )
    
    print(f"\nResponse: {resp.status_code}")
    print(f"Body: {resp.text}")
    
    if resp.status_code == 200:
        data = resp.json()
        if data.get("code") == 200:
            print(f"\n✓ BATCH SUBMITTED SUCCESSFULLY!")
            print(f"tx_hash: {data.get('tx_hash', [])}")
        else:
            print(f"\n✗ Batch failed: {data.get('message', 'unknown')}")
    else:
        print(f"\n✗ HTTP error")

    print("\n" + "=" * 60)
    print("Done!")


if __name__ == "__main__":
    main()
