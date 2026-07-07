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
        except Exception as e:
            print(f"[DB WARN] Index creation notice: {e}")
        
        # Enforce blacklist synchronization
        sync_revocations(_db)
    return _db

def generate_key():
    # Format: KRN-XXXX-XXXX-XXXX-XXXX
    parts = [secrets.token_hex(2).upper() for _ in range(4)]
    return f"{KEY_PREFIX}{'-'.join(parts)}"

def create_order(plan, payment_method, amount_ton=None, ton_memo=None):
    db = get_db()
    order_id = f"KRN-ORD-{secrets.token_hex(3).upper()}"
    now = datetime.now(timezone.utc)
    
    order = {
        "order_id": order_id,
        "plan_id": plan["id"],
        "plan_name": plan["name"],
        "duration_days": plan["days"],
        "amount_inr": plan["price_inr"],
        "amount_ton": amount_ton,
        "payment_method": payment_method,
        "ton_memo": ton_memo,
        "utr": None,
        "status": "pending",
        "api_key": None,
        "created_at": now,
        "updated_at": now
    }
    db.orders.insert_one(order)
    order.pop("_id", None)
    return order

def get_order(order_id):
    db = get_db()
    order = db.orders.find_one({"order_id": order_id})
    if order:
        order.pop("_id", None)
    return order

def get_order_by_memo(memo):
    if not memo:
        return None
    db = get_db()
    order = db.orders.find_one({"ton_memo": memo, "status": "pending"})
    if order:
        order.pop("_id", None)
    return order

def set_order_utr(order_id, utr):
    db = get_db()
    now = datetime.now(timezone.utc)
    db.orders.update_one(
        {"order_id": order_id},
        {"$set": {"utr": utr, "status": "awaiting_approval", "updated_at": now}}
    )
    return get_order(order_id)

def approve_order(order_id):
    db = get_db()
    order = db.orders.find_one({"order_id": order_id})
    if not order:
        return None
    
    if order.get("status") == "approved" and order.get("api_key"):
        order.pop("_id", None)
        return order

    # Generate License
    api_key = generate_key()
    now = datetime.now(timezone.utc)
    duration_days = order.get("duration_days", 30)
    expires_at = now + timedelta(days=duration_days)

    license_doc = {
        "api_key": api_key,
        "order_id": order_id,
        "plan_id": order.get("plan_id"),
        "plan_name": order.get("plan_name"),
        "duration_days": duration_days,
        "amount_inr": order.get("amount_inr"),
        "amount_ton": order.get("amount_ton"),
        "payment_method": order.get("payment_method"),
        "utr": order.get("utr"),
        "ton_memo": order.get("ton_memo"),
        "status": "ACTIVE",
        "hwid": None,
        "hwids": [],
        "max_devices": 3,
        "created_at": now,
        "expires_at": expires_at,
