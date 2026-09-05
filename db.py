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
        "last_verified_at": None
    }
    db.licenses.insert_one(license_doc)

    # Update Order
    db.orders.update_one(
        {"order_id": order_id},
        {"$set": {"status": "approved", "api_key": api_key, "updated_at": now}}
    )

    updated_order = db.orders.find_one({"order_id": order_id})
    if updated_order:
        updated_order.pop("_id", None)
    return updated_order

def reject_order(order_id, reason="Rejected by admin"):
    db = get_db()
    now = datetime.now(timezone.utc)
    db.orders.update_one(
        {"order_id": order_id},
        {"$set": {"status": "rejected", "reject_reason": reason, "updated_at": now}}
    )
    return get_order(order_id)

def reset_license_hwid(api_key: str):
    """Admin tool to clear all bound devices for a license key."""
    db = get_db()
    res = db.licenses.update_one(
        {"api_key": api_key.strip()},
        {"$set": {"hwids": [], "hwid": None}}
    )
    return res.modified_count > 0

def verify_license(api_key, hwid=None):
    if not api_key:
        return {"valid": False, "reason": "No license key provided."}
    
    clean_key = str(api_key).strip().upper()
    if clean_key in REVOKED_KEYS:
        return {"valid": False, "reason": REVOKED_KEYS[clean_key]}

    db = get_db()
    lic = db.licenses.find_one({"api_key": clean_key})
    if not lic:
        lic = db.licenses.find_one({"api_key": str(api_key).strip()})
    if not lic:
        return {"valid": False, "reason": "License key not found."}
    
    if lic.get("status") in ["REVOKED", "DISABLED", "CANCELLED"] or lic.get("api_key") in REVOKED_KEYS:
        return {"valid": False, "reason": "License key has been permanently revoked."}

    if lic.get("status") != "ACTIVE":
        return {"valid": False, "reason": f"License is {lic.get('status')}."}
    
    now = datetime.now(timezone.utc)
    expires_at = lic.get("expires_at")
    if expires_at:
        # Handle naive vs aware datetime from mongodb
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if now > expires_at:
            db.licenses.update_one({"api_key": api_key}, {"$set": {"status": "EXPIRED"}})
            return {"valid": False, "reason": "License has expired. Please renew."}
    
    # Check Hardware ID binding (Max 3 devices per license key)
    max_devices = lic.get("max_devices", 3)
    
    # Retrieve current list of authorized HWIDs (backward compatible with single 'hwid' string)
    bound_devices = lic.get("hwids")
    if bound_devices is None:
        single_hwid = lic.get("hwid")
        bound_devices = [single_hwid] if single_hwid else []
    else:
        bound_devices = list(bound_devices)

    if hwid:
        clean_hwid = str(hwid).strip()
        if clean_hwid not in bound_devices:
            if len(bound_devices) >= max_devices:
                return {
                    "valid": False,
                    "reason": f"Device limit exceeded ({len(bound_devices)}/{max_devices} devices active). Contact support (@KRONOSSPBOT) to reset your devices.",
                    "devices_used": len(bound_devices),
                    "max_devices": max_devices
                }
            # Authorize new device in available slot
            bound_devices.append(clean_hwid)
            db.licenses.update_one(
                {"api_key": clean_key},
                {
                    "$set": {
                        "hwids": bound_devices,
                        "hwid": bound_devices[0],
                        "max_devices": max_devices,
                        "last_verified_at": now
                    }
                }
            )
        else:
            # Device already authorized
            db.licenses.update_one(
                {"api_key": clean_key},
                {
                    "$set": {
                        "hwids": bound_devices,
                        "last_verified_at": now
                    }
                }
            )

    return {
        "valid": True,
        "status": "ACTIVE",
        "plan_name": lic.get("plan_name"),
        "expires_at": expires_at.isoformat() if expires_at else None,
        "days_left": (expires_at - now).days if expires_at else 0,
        "devices_used": len(bound_devices),
        "max_devices": max_devices
    }

def recover_license(query):
    """Search by UTR or TON memo/address"""
    if not query:
        return None
    clean_q = str(query).strip()
    if clean_q.upper() in REVOKED_KEYS:
        return None

    db = get_db()
    
