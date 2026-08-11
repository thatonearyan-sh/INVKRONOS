import asyncio
import json
import time
import urllib.request
import ssl
import certifi
from config import TON_WALLET
import db
import telegram_admin

SSL_CTX = ssl.create_default_context(cafile=certifi.where())

# Price caching
_last_price_fetch = 0
_cached_ton_inr = 130.0

def get_ton_inr_price():
    global _last_price_fetch, _cached_ton_inr
    now = time.time()
    # Cache for 60 seconds
    if now - _last_price_fetch < 60:
        return _cached_ton_inr

    try:
        url = "https://api.coingecko.com/api/v3/simple/price?ids=the-open-network&vs_currencies=inr"
        req = urllib.request.Request(url, headers={"User-Agent": "KronosServer/1.0"})
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=6) as resp:
            data = json.loads(resp.read().decode())
            price = float(data["the-open-network"]["inr"])
            if price > 0:
                _cached_ton_inr = price
                _last_price_fetch = now
                return price
    except Exception as e:
        print(f"[TON PRICE WARN] CoinGecko fetch failed, using fallback: {e}")

    # Fallback to Binance USD * 87
    try:
        url = "https://api.binance.com/api/v3/ticker/price?symbol=TONUSDT"
        req = urllib.request.Request(url, headers={"User-Agent": "KronosServer/1.0"})
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=6) as resp:
            data = json.loads(resp.read().decode())
            usd = float(data["price"])
            price = usd * 87.0
            if price > 0:
                _cached_ton_inr = price
                _last_price_fetch = now
                return price
    except Exception:
        pass

    return _cached_ton_inr

def calculate_ton_amount(inr_amount):
    price = get_ton_inr_price()
    ton_raw = inr_amount / price
    # Round to 3 decimal places (e.g. 0.915 TON)
    return max(0.01, round(ton_raw, 3)), price

def _fetch_recent_ton_transactions():
    try:
        url = f"https://tonapi.io/v2/blockchain/accounts/{TON_WALLET}/transactions?limit=15"
        req = urllib.request.Request(url, headers={"User-Agent": "KronosServer/1.0"})
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=8) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        # tonapi.io can occasionally rate-limit, keep quiet on timeouts
        return None

async def ton_blockchain_watcher_loop():
    """Background task monitoring TON blockchain for payments"""
    print(f"[TON WATCHER] Monitoring TON Wallet: {TON_WALLET}")
    loop = asyncio.get_event_loop()
    processed_tx_hashes = set()

    while True:
        try:
            res = await loop.run_in_executor(None, _fetch_recent_ton_transactions)
            if res and "transactions" in res:
                for tx in res["transactions"]:
                    tx_hash = tx.get("hash")
                    if not tx_hash or tx_hash in processed_tx_hashes:
                        continue

                    # Check incoming internal messages
                    in_msg = tx.get("in_msg", {})
                    if not in_msg:
                        continue

                    # Extract memo/comment
                    comment = ""
                    decoded_body = in_msg.get("decoded_body")
                    if isinstance(decoded_body, dict):
                        comment = decoded_body.get("text", "")
                    elif in_msg.get("message"):
                        comment = in_msg.get("message", "")

                    comment = comment.strip()
                    if not comment.startswith("KRN-"):
                        processed_tx_hashes.add(tx_hash)
                        continue

                    # Found a transaction with a Kronos memo!
                    val_nano = in_msg.get("value", 0)
                    ton_received = val_nano / 1e9

                    # Find pending order by memo
                    order = await loop.run_in_executor(None, db.get_order_by_memo, comment)
                    if order and order.get("status") == "pending":
                        required_ton = order.get("amount_ton", 0)
                        # Allow 5% margin for slight price fluctuations
                        if ton_received >= (required_ton * 0.95):
                            print(f"[TON CONFIRMED] Order {order['order_id']} paid {ton_received} TON! Memo: {comment}")
                            updated_order = await loop.run_in_executor(None, db.approve_order, order["order_id"])
                            processed_tx_hashes.add(tx_hash)
