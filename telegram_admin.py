import os
import hmac
import hashlib
from config import SECRET_APPROVAL_KEY

_EFFECTIVE_SECRET = SECRET_APPROVAL_KEY or os.getenv("SECRET_APPROVAL_KEY", "KRONOS_APPROVAL_SALT_KEY")

def generate_approval_token(order_id: str) -> str:
    return hmac.new(_EFFECTIVE_SECRET.encode(), order_id.encode(), hashlib.sha256).hexdigest()[:16]

def verify_approval_token(order_id: str, token: str) -> bool:
    expected = generate_approval_token(order_id)
    return hmac.compare_digest(expected, token)

def process_telegram_update(update: dict):
    print(f"[TG PROCESS UPDATE] keys={list(update.keys())}")
    if "callback_query" in update:
        cq = update["callback_query"]
        cq_id = cq["id"]
        data = cq.get("data", "")
        from_user = cq.get("from", {}).get("id")
        msg_id = cq.get("message", {}).get("message_id")
        print(f"[TG CALLBACK_QUERY] from_user={from_user}, expected_admin={TELEGRAM_ADMIN_ID}, data={data}")

        if from_user != TELEGRAM_ADMIN_ID:
            print(f"[TG CALLBACK_QUERY] Unauthorized user {from_user} attempted callback {data}")
            _tg_request("answerCallbackQuery", {
                "callback_query_id": cq_id,
                "text": "Unauthorized.",
                "show_alert": True
            })
            return

        if data.startswith("appr:"):
            order_id = data.split(":", 1)[1]
            print(f"[TG APPROVE EXECUTING] Admin approved order_id={order_id}")
            updated_order = db.approve_order(order_id)
            if updated_order:
                key = updated_order.get("api_key")
                plan = updated_order.get("plan_name")
                utr = updated_order.get("utr")
                _tg_request("answerCallbackQuery", {
                    "callback_query_id": cq_id,
                    "text": "✅ Order Approved! Key Created."
                })
                if msg_id:
                    edit_text = (
                        f"✅ <b>ORDER APPROVED & DELIVERED</b>\n\n"
                        f"💳 <b>Order:</b> <code>{order_id}</code>\n"
                        f"📦 <b>Plan:</b> {plan}\n"
                        f"🔢 <b>UTR:</b> <code>{utr}</code>\n\n"
                        f"🔑 <b>Generated Key:</b>\n<code>{key}</code>\n\n"
                        f"<i>The customer browser screen has automatically unlocked.</i>"
                    )
                    _tg_request("editMessageText", {
                        "chat_id": TELEGRAM_ADMIN_ID,
                        "message_id": msg_id,
                        "text": edit_text,
                        "parse_mode": "HTML"
                    })
        elif data.startswith("rejc:"):
            order_id = data.split(":", 1)[1]
            db.reject_order(order_id, "Rejected by Admin")
            _tg_request("answerCallbackQuery", {
                "callback_query_id": cq_id,
                "text": "❌ Order Rejected."
            })
            if msg_id:
                edit_text = (
                    f"❌ <b>ORDER REJECTED</b>\n\n"
                    f"💳 <b>Order:</b> <code>{order_id}</code>\n"
                    f"<i>Customer checkout screen updated with rejection status.</i>"
                )
                _tg_request("editMessageText", {
                    "chat_id": TELEGRAM_ADMIN_ID,
                    "message_id": msg_id,
                    "text": edit_text,
                    "parse_mode": "HTML"
                })

    if "message" in update:
        msg = update["message"]
        from_user = msg.get("from", {}).get("id")
        text = msg.get("text", "").strip()
        chat_id = msg.get("chat", {}).get("id")

        if from_user != TELEGRAM_ADMIN_ID:
            return

        if text.startswith("/reset"):
            parts = text.split()
            if len(parts) > 1:
                target_key = parts[1].strip()
                ok = db.reset_license_hwid(target_key)
                reply = f"✅ Reset all devices for key:\n<code>{target_key}</code>" if ok else f"❌ Key not found:\n<code>{target_key}</code>"
            else:
                reply = "ℹ️ Usage: <code>/reset KRN-XXXX-XXXX-XXXX-XXXX</code>"
            _tg_request("sendMessage", {"chat_id": chat_id, "text": reply, "parse_mode": "HTML"})

        elif text.startswith("/stats"):
            database = db.get_db()
            total_lic = database.licenses.count_documents({"status": "ACTIVE"})
            pending_orders = database.orders.count_documents({"status": "awaiting_approval"})
            reply = (
                f"📊 <b>KRONOS SYSTEM STATS</b>\n\n"
                f"🔑 Active Licenses: <b>{total_lic}</b>\n"
                f"⏳ Pending Approvals: <b>{pending_orders}</b>\n"
                f"🌐 Cloud Webhook: <code>https://invkronos.vercel.app/api/telegram-webhook</code>"
            )
            _tg_request("sendMessage", {"chat_id": chat_id, "text": reply, "parse_mode": "HTML"})

import asyncio
import json
import urllib.request
import urllib.parse
import ssl
import certifi
from datetime import datetime, timezone, timedelta
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_ADMIN_ID
import db

BOT_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
SSL_CTX = ssl.create_default_context(cafile=certifi.where())

def _tg_request(method, payload):
    try:
        url = f"{BOT_API_URL}/{method}"
        data = json.dumps(payload).encode("utf-8")
