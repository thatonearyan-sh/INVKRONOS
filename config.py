import os

# UPI & Payment Configuration
UPI_ID = os.getenv("UPI_ID", "")
PAYEE_NAME = os.getenv("PAYEE_NAME", "KRONOS")

# TON Blockchain Configuration
TON_WALLET = os.getenv("TON_WALLET", "")

# Telegram Admin Notification Bot
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_ADMIN_ID = int(os.getenv("TELEGRAM_ADMIN_ID", "0"))
SECRET_APPROVAL_KEY = os.getenv("SECRET_APPROVAL_KEY", "")

# MongoDB Configuration
MONGODB_URI = os.getenv("MONGODB_URI", "")
MONGODB_DB_NAME = os.getenv("MONGODB_DB_NAME", "kronos_licensing")

# Pricing Plans
PLANS = {
    "1w": {
        "id": "1w",
        "name": "1 Week Access",
        "days": 7,
        "price_inr": 119,
        "badge": "STARTER",
        "desc": "Full stealth access for 7 days"
    },
    "1m": {
        "id": "1m",
        "name": "1 Month Access",
        "days": 30,
        "price_inr": 399,
        "badge": "MOST POPULAR",
        "desc": "Complete suite for 30 days"
    },
    "3m": {
        "id": "3m",
        "name": "3 Months Access",
        "days": 90,
        "price_inr": 499,
        "badge": "HIGH VALUE",
        "desc": "Quarterly pass with priority routing"
    },
    "6m": {
        "id": "6m",
        "name": "6 Months Access",
        "days": 180,
        "price_inr": 599,
        "badge": "PRO",
        "desc": "Half-year elite stealth access"
    },
    "1y": {
        "id": "1y",
        "name": "1 Year Access",
        "days": 365,
        "price_inr": 699,
        "badge": "BEST DEAL",
        "desc": "Annual unlimited license + updates"
    }
}

# Key formatting
KEY_PREFIX = "KRN-"
