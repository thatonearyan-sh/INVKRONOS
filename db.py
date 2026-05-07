import certifi
import secrets
from datetime import datetime, timezone, timedelta
import pymongo
from config import MONGODB_URI, MONGODB_DB_NAME, KEY_PREFIX

# Initialize MongoDB Client
_client = None
_db = None

# Blacklisted / Revoked Keys (Permanently Invalidated e.g. Demo Video Keys)
REVOKED_KEYS = {
    "KRN-BE5F-66E3-AF19-FCB7": "Demo video key permanently revoked.",
    "KRN-64DD-8C68-74F3-115A": "Revoked per user request.",
    "KRN-417F-7043-2F11-926E": "Revoked per user request.",
    "KRN-F8A3-15A9-4F64-A5FD": "Revoked per user request.",
}

def sync_revocations(database):
    """Enforce revoked status in MongoDB collection"""
    try:
        if database is not None and REVOKED_KEYS:
            database.licenses.update_many(
                {"api_key": {"$in": list(REVOKED_KEYS.keys())}},
                {"$set": {"status": "REVOKED", "revocation_reason": "Public Demo Key Revocation"}}
            )
    except Exception as e:
        print(f"[DB WARN] Revocation sync notice: {e}")

def get_db():
    global _client, _db
    if not MONGODB_URI:
        raise RuntimeError("MONGODB_URI environment variable is not configured. Please set it in your environment.")
    if _db is None:
        _client = pymongo.MongoClient(
            MONGODB_URI,
            tlsCAFile=certifi.where(),
            serverSelectionTimeoutMS=8000
        )
        _db = _client[MONGODB_DB_NAME]
        # Ensure indexes
        try:
            _db.orders.create_index("order_id", unique=True)
            _db.orders.create_index("ton_memo")
            _db.orders.create_index("utr")
            _db.licenses.create_index("api_key", unique=True)
            _db.licenses.create_index("utr")
            _db.licenses.create_index("ton_memo")
