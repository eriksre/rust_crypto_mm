#!/usr/bin/env python3
"""
Lighter.xyz Exchange Latency Tester

This script measures the network latency for order placement and cancellation
on the Lighter exchange via WebSocket.

Measures:
- Order placement latency: time from WebSocket send to confirmation received
- Order cancellation latency: time from WebSocket send to confirmation received

Note: Latency measurements do NOT include signing time - only network round trip.
"""

import asyncio
import json
import os
import random
import time
from datetime import datetime
from dataclasses import dataclass
from typing import Optional, List, Dict

import aiohttp
import websockets
from eth_account import Account
from eth_account.messages import encode_typed_data

# =============================================================================
# Configuration - Set your credentials here or via environment variables
# =============================================================================

# API credentials - set these before running
API_KEY_PRIVATE_KEY = os.getenv("LIGHTER_PRIVATE_KEY", "YOUR_PRIVATE_KEY_HERE")
ACCOUNT_INDEX = int(os.getenv("LIGHTER_ACCOUNT_INDEX", "0"))
API_KEY_INDEX = int(os.getenv("LIGHTER_API_KEY_INDEX", "2"))

# Lighter endpoints
BASE_URL = "https://mainnet.zklighter.elliot.ai"
WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"

# Chain ID for Lighter
CHAIN_ID = 304

# Market settings - BTC-USDC is typically market_id 0
DEFAULT_MARKET_SYMBOL = "BTC_USDC"

# =============================================================================
# ORDER SETTINGS - MANUALLY SPECIFY THESE
# =============================================================================

# Manually set the Bitcoin price for your order
# Set this to a price that won't get filled (e.g., far below market for buy)
ORDER_PRICE = 60000.0  # <-- SET YOUR DESIRED ORDER PRICE HERE

ORDER_SIDE = "buy"  # "buy" or "sell"
ORDER_SIZE = 0.0002  # Size in base currency (BTC)


@dataclass
class LatencyResult:
    """Stores latency measurement results"""
    timestamp: str
    operation: str  # "order" or "cancel"
    latency_ms: float
    order_id: Optional[str] = None
    success: bool = True
    error: Optional[str] = None


class LighterLatencyTester:
    """
    Lighter exchange latency tester that measures order placement and
    cancellation round-trip times via WebSocket.
    """

    def __init__(
        self,
        private_key: str,
        account_index: int,
        api_key_index: int,
        market_symbol: str = DEFAULT_MARKET_SYMBOL,
    ):
        self.private_key = private_key
        self.account_index = account_index
        self.api_key_index = api_key_index
        self.market_symbol = market_symbol

        # Will be populated after fetching market info
        self.market_id: Optional[int] = None
        self.price_decimals: int = 2
        self.size_decimals: int = 4
        self.price_scale: float = 1.0
        self.size_scale: float = 1.0

        # WebSocket connection
        self.ws: Optional[websockets.WebSocketClientProtocol] = None

        # Nonce management
        self.nonce: int = 0

        # Account derived from private key
        self.account = Account.from_key(private_key)
        self.address = self.account.address

        # Pending orders tracking
        self.pending_orders: Dict[int, float] = {}  # client_order_index -> send_time
        self.pending_cancels: Dict[int, float] = {}  # client_order_index -> send_time
        self.order_ids: Dict[int, str] = {}  # client_order_index -> order_id

        # Results storage
        self.results: List[LatencyResult] = []

        # Client order index counter
        self.client_order_counter = int(time.time() * 1000) % 2**32

    async def fetch_market_info(self) -> bool:
        """Fetch market metadata from REST API"""
        url = f"{BASE_URL}/api/v1/orderBooks"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        print(f"❌ Failed to fetch market info: HTTP {resp.status}")
                        return False
                    data = await resp.json()

            # Find our market
            order_books = data.get("order_books", [])
            for book in order_books:
                symbol = book.get("symbol", "").upper().replace("/", "").replace("-", "_")
                if symbol == self.market_symbol or book.get("symbol", "").upper() == self.market_symbol:
                    self.market_id = book.get("market_id")
                    self.price_decimals = book.get("supported_price_decimals", 2)
                    self.size_decimals = book.get("supported_size_decimals", 4)
                    self.price_scale = 10 ** self.price_decimals
                    self.size_scale = 10 ** self.size_decimals
                    print(f"✓ Found market: {book.get('symbol')} (ID: {self.market_id})")
                    print(f"  Price decimals: {self.price_decimals}, Size decimals: {self.size_decimals}")
                    return True

            print(f"❌ Market {self.market_symbol} not found")
            print(f"Available markets: {[b.get('symbol') for b in order_books]}")
            return False

        except Exception as e:
            print(f"❌ Error fetching market info: {e}")
            return False

    async def fetch_nonce(self) -> int:
        """Fetch current nonce for the API key"""
        url = f"{BASE_URL}/api/v1/nextNonce"
        params = {
            "accountIndex": self.account_index,
            "apiKeyIndex": self.api_key_index,
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        self.nonce = int(data.get("next_nonce", 0))
                        print(f"✓ Current nonce: {self.nonce}")
                        return self.nonce
        except Exception as e:
            print(f"⚠ Error fetching nonce: {e}")
        return self.nonce

    def get_next_client_order_index(self) -> int:
        """Generate unique client order index"""
        self.client_order_counter += 1
        return self.client_order_counter % 2**32

    def sign_create_order(
        self,
        market_index: int,
        is_ask: bool,
        base_amount: int,
        price: int,
        client_order_index: int,
        order_type: int = 0,  # LIMIT
        time_in_force: int = 0,  # GOOD_TILL_TIME
        reduce_only: bool = False,
    ) -> dict:
        """
        Sign a create order message using EIP-712 typed data signing.
        """
        nonce = self.nonce
        self.nonce += 1

        # Build the typed data for signing
        domain = {
            "name": "Lighter",
            "version": "1.0.0",
            "chainId": CHAIN_ID,
        }

        types = {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
            ],
            "CreateOrder": [
                {"name": "accountIndex", "type": "uint32"},
                {"name": "apiKeyIndex", "type": "uint8"},
                {"name": "nonce", "type": "uint64"},
                {"name": "marketIndex", "type": "uint8"},
                {"name": "clientOrderIndex", "type": "uint32"},
                {"name": "orderType", "type": "uint8"},
                {"name": "timeInForce", "type": "uint8"},
                {"name": "isAsk", "type": "bool"},
                {"name": "reduceOnly", "type": "bool"},
                {"name": "price", "type": "uint64"},
                {"name": "baseAmount", "type": "uint64"},
            ],
        }

        message = {
            "accountIndex": self.account_index,
            "apiKeyIndex": self.api_key_index,
            "nonce": nonce,
            "marketIndex": market_index,
            "clientOrderIndex": client_order_index,
            "orderType": order_type,
            "timeInForce": time_in_force,
            "isAsk": is_ask,
            "reduceOnly": reduce_only,
            "price": price,
            "baseAmount": base_amount,
        }

        typed_data = {
            "types": types,
            "domain": domain,
            "primaryType": "CreateOrder",
            "message": message,
        }

        # Sign the typed data
        signable = encode_typed_data(full_message=typed_data)
        signed = self.account.sign_message(signable)

        return {
            "type": "create_order",
            "account_index": self.account_index,
            "api_key_index": self.api_key_index,
            "nonce": nonce,
            "market_index": market_index,
            "client_order_index": client_order_index,
            "order_type": order_type,
            "time_in_force": time_in_force,
            "is_ask": is_ask,
            "reduce_only": reduce_only,
            "price": str(price),
            "base_amount": str(base_amount),
            "signature": signed.signature.hex(),
        }

    def sign_cancel_order(
        self,
        market_index: int,
        order_id: str,
        client_order_index: int,
    ) -> dict:
        """
        Sign a cancel order message using EIP-712 typed data signing.
        """
        nonce = self.nonce
        self.nonce += 1

        # Convert order_id to int if it's a string
        order_id_int = int(order_id) if isinstance(order_id, str) else order_id

        domain = {
            "name": "Lighter",
            "version": "1.0.0",
            "chainId": CHAIN_ID,
        }

        types = {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
            ],
            "CancelOrder": [
                {"name": "accountIndex", "type": "uint32"},
                {"name": "apiKeyIndex", "type": "uint8"},
                {"name": "nonce", "type": "uint64"},
                {"name": "marketIndex", "type": "uint8"},
                {"name": "orderId", "type": "uint64"},
            ],
        }

        message = {
            "accountIndex": self.account_index,
            "apiKeyIndex": self.api_key_index,
            "nonce": nonce,
            "marketIndex": market_index,
            "orderId": order_id_int,
        }

        typed_data = {
            "types": types,
            "domain": domain,
            "primaryType": "CancelOrder",
            "message": message,
        }

        signable = encode_typed_data(full_message=typed_data)
        signed = self.account.sign_message(signable)

        return {
            "type": "cancel_order",
            "account_index": self.account_index,
            "api_key_index": self.api_key_index,
            "nonce": nonce,
            "market_index": market_index,
            "order_id": str(order_id),
            "client_order_index": client_order_index,
            "signature": signed.signature.hex(),
        }

    async def connect_websocket(self) -> bool:
        """Establish WebSocket connection"""
        try:
            self.ws = await websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=10,
            )
            print(f"✓ Connected to WebSocket: {WS_URL}")
            return True
        except Exception as e:
            print(f"❌ WebSocket connection failed: {e}")
            return False

    async def subscribe_to_orders(self):
        """Subscribe to order updates for our account"""
        if self.ws is None:
            return

        # Subscribe to account orders channel
        sub_msg = {
            "type": "subscribe",
            "channel": f"account_orders/{self.account_index}",
        }
        await self.ws.send(json.dumps(sub_msg))
        print(f"✓ Subscribed to account orders channel")

    async def message_handler(self):
        """Handle incoming WebSocket messages"""
        if self.ws is None:
            return

        try:
            async for message in self.ws:
                try:
                    data = json.loads(message)
                    await self.process_message(data)
                except json.JSONDecodeError:
                    pass
        except websockets.exceptions.ConnectionClosed:
            print("⚠ WebSocket connection closed")

    async def process_message(self, data: dict):
        """Process a WebSocket message and check for order/cancel confirmations"""
        msg_type = data.get("type", "")
        channel = data.get("channel", "")

        # Debug: print relevant messages
        if "order" in channel.lower() or "order" in msg_type.lower():
            # Check for order placed confirmation
            if "orders" in data:
                for order in data.get("orders", []):
                    client_idx = order.get("client_order_index")
                    order_id = order.get("order_id")
                    status = order.get("status", "")

                    # Check if this is a response to our pending order
                    if client_idx in self.pending_orders:
                        recv_time = time.perf_counter()
                        send_time = self.pending_orders.pop(client_idx)
                        latency_ms = (recv_time - send_time) * 1000

                        self.order_ids[client_idx] = str(order_id)

                        result = LatencyResult(
                            timestamp=datetime.utcnow().isoformat(),
                            operation="order",
                            latency_ms=latency_ms,
                            order_id=str(order_id),
                            success=True,
                        )
                        self.results.append(result)
                        print(f"📥 Order confirmed: ID={order_id}, Latency={latency_ms:.3f}ms")

                    # Check if this is a cancel confirmation
                    if client_idx in self.pending_cancels:
                        if status in ["cancelled", "canceled", "CANCELLED", "CANCELED"]:
                            recv_time = time.perf_counter()
                            send_time = self.pending_cancels.pop(client_idx)
                            latency_ms = (recv_time - send_time) * 1000

                            result = LatencyResult(
                                timestamp=datetime.utcnow().isoformat(),
                                operation="cancel",
                                latency_ms=latency_ms,
                                order_id=str(order_id),
                                success=True,
                            )
                            self.results.append(result)
                            print(f"📥 Cancel confirmed: ID={order_id}, Latency={latency_ms:.3f}ms")

        # Handle error responses
        if data.get("error"):
            error_msg = data.get("error", "Unknown error")
            print(f"❌ Error from exchange: {error_msg}")

    async def place_order(self) -> Optional[int]:
        """
        Place a limit order and return the client_order_index.
        Latency measurement starts AFTER signing is complete.
        """
        if self.ws is None or self.market_id is None:
            return None

        # Use manually specified price
        order_price = ORDER_PRICE
        is_ask = ORDER_SIDE == "sell"

        # Convert to integer with proper scaling
        price_int = int(order_price * self.price_scale)
        size_int = int(ORDER_SIZE * self.size_scale)

        client_order_index = self.get_next_client_order_index()

        # Sign the order (this is NOT included in latency measurement)
        signed_order = self.sign_create_order(
            market_index=self.market_id,
            is_ask=is_ask,
            base_amount=size_int,
            price=price_int,
            client_order_index=client_order_index,
        )

        # Prepare JSON message before timing
        order_json = json.dumps(signed_order)

        # *** LATENCY MEASUREMENT STARTS HERE ***
        send_time = time.perf_counter()
        await self.ws.send(order_json)
        # *** Message sent - now waiting for confirmation ***

        self.pending_orders[client_order_index] = send_time

        print(f"📤 Order sent: {ORDER_SIDE} {ORDER_SIZE} @ {order_price:.2f} (client_idx={client_order_index})")

        return client_order_index

    async def cancel_order(self, client_order_index: int) -> bool:
        """
        Cancel an order by client_order_index.
        Latency measurement starts AFTER signing is complete.
        """
        if self.ws is None or self.market_id is None:
            return False

        order_id = self.order_ids.get(client_order_index)
        if not order_id:
            print(f"⚠ No order_id found for client_order_index={client_order_index}")
            return False

        # Sign the cancel (this is NOT included in latency measurement)
        signed_cancel = self.sign_cancel_order(
            market_index=self.market_id,
            order_id=order_id,
            client_order_index=client_order_index,
        )

        # Prepare JSON message before timing
        cancel_json = json.dumps(signed_cancel)

        # Track for cancel confirmation
        self.pending_cancels[client_order_index] = 0  # Will be set below

        # *** LATENCY MEASUREMENT STARTS HERE ***
        send_time = time.perf_counter()
        await self.ws.send(cancel_json)
        # *** Message sent - now waiting for confirmation ***

        self.pending_cancels[client_order_index] = send_time

        print(f"📤 Cancel sent: order_id={order_id} (client_idx={client_order_index})")

        return True

    async def run_latency_test(self, num_iterations: int = -1):
        """
        Run the latency test loop.

        Args:
            num_iterations: Number of order/cancel cycles. -1 for infinite loop.
        """
        print("\n" + "=" * 60)
        print("Starting Lighter.xyz Latency Test")
        print("=" * 60)

        # Initialize
        if not await self.fetch_market_info():
            return

        await self.fetch_nonce()

        if not await self.connect_websocket():
            return

        await self.subscribe_to_orders()

        # Start message handler in background
        message_task = asyncio.create_task(self.message_handler())

        iteration = 0
        try:
            while num_iterations == -1 or iteration < num_iterations:
                iteration += 1
                print(f"\n--- Iteration {iteration} ---")

                # Random wait 3-5 seconds before placing order
                wait_time = random.uniform(3.0, 5.0)
                print(f"⏳ Waiting {wait_time:.2f}s before placing order...")
                await asyncio.sleep(wait_time)

                # Place order
                client_idx = await self.place_order()
                if client_idx is None:
                    print("❌ Failed to place order, retrying...")
                    continue

                # Wait for order confirmation (with timeout)
                confirm_timeout = 5.0
                start_wait = time.time()
                while client_idx in self.pending_orders:
                    if time.time() - start_wait > confirm_timeout:
                        print(f"⚠ Order confirmation timeout")
                        self.pending_orders.pop(client_idx, None)
                        break
                    await asyncio.sleep(0.01)

                # Check if order was confirmed
                if client_idx not in self.order_ids:
                    print("❌ Order not confirmed, skipping cancel")
                    continue

                # Random wait 2-3 seconds before canceling
                wait_time = random.uniform(2.0, 3.0)
                print(f"⏳ Waiting {wait_time:.2f}s before canceling...")
                await asyncio.sleep(wait_time)

                # Cancel order
                if not await self.cancel_order(client_idx):
                    print("❌ Failed to send cancel")
                    continue

                # Wait for cancel confirmation (with timeout)
                start_wait = time.time()
                while client_idx in self.pending_cancels:
                    if time.time() - start_wait > confirm_timeout:
                        print(f"⚠ Cancel confirmation timeout")
                        self.pending_cancels.pop(client_idx, None)
                        break
                    await asyncio.sleep(0.01)

                # Clean up
                self.order_ids.pop(client_idx, None)

        except KeyboardInterrupt:
            print("\n\n⚠ Test interrupted by user")
        finally:
            message_task.cancel()
            if self.ws:
                await self.ws.close()

        self.print_results()

    def print_results(self):
        """Print summary of latency results"""
        print("\n" + "=" * 60)
        print("LATENCY TEST RESULTS")
        print("=" * 60)

        order_latencies = [r.latency_ms for r in self.results if r.operation == "order" and r.success]
        cancel_latencies = [r.latency_ms for r in self.results if r.operation == "cancel" and r.success]

        if order_latencies:
            print(f"\n📊 ORDER PLACEMENT LATENCY (n={len(order_latencies)}):")
            print(f"   Min:    {min(order_latencies):.3f} ms")
            print(f"   Max:    {max(order_latencies):.3f} ms")
            print(f"   Avg:    {sum(order_latencies)/len(order_latencies):.3f} ms")
            sorted_latencies = sorted(order_latencies)
            p50_idx = len(sorted_latencies) // 2
            p95_idx = int(len(sorted_latencies) * 0.95)
            p99_idx = int(len(sorted_latencies) * 0.99)
            print(f"   p50:    {sorted_latencies[p50_idx]:.3f} ms")
            if len(sorted_latencies) > 20:
                print(f"   p95:    {sorted_latencies[p95_idx]:.3f} ms")
                print(f"   p99:    {sorted_latencies[p99_idx]:.3f} ms")

        if cancel_latencies:
            print(f"\n📊 CANCEL LATENCY (n={len(cancel_latencies)}):")
            print(f"   Min:    {min(cancel_latencies):.3f} ms")
            print(f"   Max:    {max(cancel_latencies):.3f} ms")
            print(f"   Avg:    {sum(cancel_latencies)/len(cancel_latencies):.3f} ms")
            sorted_latencies = sorted(cancel_latencies)
            p50_idx = len(sorted_latencies) // 2
            p95_idx = int(len(sorted_latencies) * 0.95)
            p99_idx = int(len(sorted_latencies) * 0.99)
            print(f"   p50:    {sorted_latencies[p50_idx]:.3f} ms")
            if len(sorted_latencies) > 20:
                print(f"   p95:    {sorted_latencies[p95_idx]:.3f} ms")
                print(f"   p99:    {sorted_latencies[p99_idx]:.3f} ms")

        # Save detailed results to file
        results_file = f"lighter_latency_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(results_file, "w") as f:
            json.dump(
                {
                    "order_latencies_ms": order_latencies,
                    "cancel_latencies_ms": cancel_latencies,
                    "detailed_results": [
                        {
                            "timestamp": r.timestamp,
                            "operation": r.operation,
                            "latency_ms": r.latency_ms,
                            "order_id": r.order_id,
                            "success": r.success,
                        }
                        for r in self.results
                    ],
                },
                f,
                indent=2,
            )
        print(f"\n💾 Detailed results saved to: {results_file}")


async def main():
    """Main entry point"""
    print("=" * 60)
    print("Lighter.xyz Exchange Latency Tester")
    print("=" * 60)

    # Validate credentials
    if API_KEY_PRIVATE_KEY == "YOUR_PRIVATE_KEY_HERE":
        print("\n❌ ERROR: Please set your API credentials!")
        print("\nYou can either:")
        print("  1. Set environment variables:")
        print("     export LIGHTER_PRIVATE_KEY='your_private_key'")
        print("     export LIGHTER_ACCOUNT_INDEX='your_account_index'")
        print("     export LIGHTER_API_KEY_INDEX='your_api_key_index'")
        print("\n  2. Or edit the script directly and set:")
        print("     API_KEY_PRIVATE_KEY = 'your_private_key'")
        print("     ACCOUNT_INDEX = your_account_index")
        print("     API_KEY_INDEX = your_api_key_index")
        print("\n💡 Generate API keys at: https://app.lighter.xyz/")
        print("   Go to Tools → API Keys → Generate API Key")
        return

    print(f"\nConfiguration:")
    print(f"  Account Index: {ACCOUNT_INDEX}")
    print(f"  API Key Index: {API_KEY_INDEX}")
    print(f"  Market: {DEFAULT_MARKET_SYMBOL}")
    print(f"  Order Side: {ORDER_SIDE}")
    print(f"  Order Size: {ORDER_SIZE}")
    print(f"  Order Price: ${ORDER_PRICE:,.2f}")

    tester = LighterLatencyTester(
        private_key=API_KEY_PRIVATE_KEY,
        account_index=ACCOUNT_INDEX,
        api_key_index=API_KEY_INDEX,
        market_symbol=DEFAULT_MARKET_SYMBOL,
    )

    # Run test (pass number of iterations or -1 for infinite)
    await tester.run_latency_test(num_iterations=-1)


if __name__ == "__main__":
    asyncio.run(main())

