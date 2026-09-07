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
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"[TG ERROR] Failed {method}: {e}")
        return None

def send_admin_utr_alert(order_id, utr, plan_name, amount_inr, base_url="http://localhost:8000"):
    ist = timezone(timedelta(hours=5, minutes=30))
    time_str = datetime.now(ist).strftime("%d-%b %I:%M:%S %p IST")
    
    text = (
        f"🔔 <b>NEW UPI PAYMENT SUBMITTED</b>\n\n"
        f"💳 <b>Order ID:</b> <code>{order_id}</code>\n"
        f"📦 <b>Plan:</b> {plan_name}\n"
        f"💰 <b>Amount:</b> ₹{amount_inr}\n"
        f"🔢 <b>UTR / Ref ID:</b> <code>{utr}</code>\n"
        f"🕒 <b>Time:</b> {time_str}\n\n"
        f"<i>Tap below to approve or reject this payment:</i>"
    )

    keyboard = {
        "inline_keyboard": [
            [
                {"text": "✅ Approve & Issue Key", "callback_data": f"appr:{order_id}"},
                {"text": "❌ Reject", "callback_data": f"rejc:{order_id}"}
            ]
        ]
    }

    return _tg_request("sendMessage", {
        "chat_id": TELEGRAM_ADMIN_ID,
        "text": text,
        "parse_mode": "HTML",
        "reply_markup": keyboard,
        "disable_web_page_preview": True
    })

def send_admin_ton_alert(order_id, memo, plan_name, amount_ton, api_key):
    ist = timezone(timedelta(hours=5, minutes=30))
    time_str = datetime.now(ist).strftime("%d-%b %I:%M:%S %p IST")

    text = (
        f"⚡ <b>TON PAYMENT AUTO-CONFIRMED ON-CHAIN</b>\n\n"
        f"💳 <b>Order ID:</b> <code>{order_id}</code>\n"
        f"📦 <b>Plan:</b> {plan_name}\n"
        f"💎 <b>Received:</b> {amount_ton:.3f} TON\n"
        f"📝 <b>Memo:</b> <code>{memo}</code>\n"
        f"🕒 <b>Time:</b> {time_str}\n\n"
        f"🔑 <b>API Key Generated:</b>\n<code>{api_key}</code>\n\n"
        f"<i>Status: Customer key unlocked automatically in browser.</i>"
    )

    return _tg_request("sendMessage", {
        "chat_id": TELEGRAM_ADMIN_ID,
        "text": text,
        "parse_mode": "HTML"
    })

async def telegram_polling_loop():
    """Background poller for admin inline button clicks"""
    print("[TG BOT] Starting Admin Telegram Approval Listener...")
    offset = 0
    loop = asyncio.get_event_loop()

    while True:
        try:
            payload = {"offset": offset, "timeout": 20}
            res = await loop.run_in_executor(None, _tg_request, "getUpdates", payload)
            
            if res and res.get("ok") and res.get("result"):
                for update in res["result"]:
                    offset = update["update_id"] + 1

                    # Handle inline button callbacks
                    if "callback_query" in update:
                        cq = update["callback_query"]
                        cq_id = cq["id"]
                        data = cq.get("data", "")
                        from_user = cq["from"]["id"]
                        msg_id = cq["message"]["message_id"]

                        # Verify sender is authorized admin
                        if from_user != TELEGRAM_ADMIN_ID:
                            await loop.run_in_executor(
                                None, _tg_request, "answerCallbackQuery",
                                {"callback_query_id": cq_id, "text": "Unauthorized.", "show_alert": True}
                            )
                            continue

                        if data.startswith("appr:"):
                            order_id = data.split(":", 1)[1]
                            updated_order = await loop.run_in_executor(None, db.approve_order, order_id)
                            
                            if updated_order:
                                key = updated_order.get("api_key")
                                plan = updated_order.get("plan_name")
                                utr = updated_order.get("utr")
                                
                                # Acknowledge callback
                                await loop.run_in_executor(
                                    None, _tg_request, "answerCallbackQuery",
                                    {"callback_query_id": cq_id, "text": "✅ Order Approved! Key Created."}
                                )

                                # Edit admin message
                                edit_text = (
                                    f"✅ <b>ORDER APPROVED & DELIVERED</b>\n\n"
                                    f"💳 <b>Order:</b> <code>{order_id}</code>\n"
                                    f"📦 <b>Plan:</b> {plan}\n"
                                    f"🔢 <b>UTR:</b> <code>{utr}</code>\n\n"
                                    f"🔑 <b>Generated Key:</b>\n<code>{key}</code>\n\n"
                                    f"<i>The customer's browser screen has automatically unlocked.</i>"
                                )
                                await loop.run_in_executor(
                                    None, _tg_request, "editMessageText",
                                    {
                                        "chat_id": TELEGRAM_ADMIN_ID,
                                        "message_id": msg_id,
                                        "text": edit_text,
                                        "parse_mode": "HTML"
                                    }
                                )
                        
                        elif data.startswith("rejc:"):
                            order_id = data.split(":", 1)[1]
                            await loop.run_in_executor(None, db.reject_order, order_id, "Rejected by Admin")
                            
                            await loop.run_in_executor(
                                None, _tg_request, "answerCallbackQuery",
                                {"callback_query_id": cq_id, "text": "❌ Order Rejected."}
                            )

                            edit_text = (
                                f"❌ <b>ORDER REJECTED</b>\n\n"
                                f"💳 <b>Order:</b> <code>{order_id}</code>\n"
                                f"<i>Customer checkout screen updated with rejection status.</i>"
                            )
                            await loop.run_in_executor(
                                None, _tg_request, "editMessageText",
                                {
                                    "chat_id": TELEGRAM_ADMIN_ID,
                                    "message_id": msg_id,
                                    "text": edit_text,
                                    "parse_mode": "HTML"
                                }
                            )

        except asyncio.CancelledError:
            print("[TG BOT] Listener stopped.")
            break
        except Exception as e:
            print(f"[TG BOT ERROR] {e}")
            await asyncio.sleep(3)

        await asyncio.sleep(0.5)
