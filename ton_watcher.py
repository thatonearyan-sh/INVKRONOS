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
