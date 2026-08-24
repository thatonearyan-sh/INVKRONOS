import os
import io
import json
import asyncio
import secrets
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import qrcode

import config
import db
import ton_watcher
import telegram_admin
import crypto_engine

# Background tasks references
bg_tasks = []

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Connect to DB
    print("[SERVER] Connecting to MongoDB Atlas...")
    try:
        database = db.get_db()
        print(f"[SERVER] Connected to MongoDB Atlas: {database.name}")
    except Exception as e:
        print(f"[SERVER ERROR] DB connection failed on start: {e}")

    # Launch background polling loops only if NOT in Vercel Serverless
    is_vercel = os.getenv("VERCEL", "0") == "1"
    if not is_vercel:
        t1 = asyncio.create_task(telegram_admin.telegram_polling_loop())
        t2 = asyncio.create_task(ton_watcher.ton_blockchain_watcher_loop())
        bg_tasks.extend([t1, t2])
        print("[SERVER] Telegram poller and TON watcher started in background.")

    yield

    # Shutdown
    for t in bg_tasks:
        t.cancel()
    print("[SERVER] Background tasks stopped.")

app = FastAPI(title="KRONOS Licensing Engine", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_template_html(filename: str):
    candidates = [
        os.path.join(os.path.dirname(__file__), "templates", filename),
        os.path.join(os.path.dirname(__file__), "..", "templates", filename),
        os.path.join(os.getcwd(), "templates", filename),
    ]
    for p in candidates:
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                return f.read()
    return f"<h1>Template {filename} Not Found</h1>"

def get_checkout_html():
    return get_template_html("checkout.html")

# Request Models
class CreateOrderReq(BaseModel):
    plan_id: str
    payment_method: str = "upi"

class SubmitUTRReq(BaseModel):
    order_id: str
    utr: str

class RecoverReq(BaseModel):
    query: str

class VerifyLicenseReq(BaseModel):
    api_key: str
    hwid: Optional[str] = None

# Routes
@app.get("/", response_class=HTMLResponse)
@app.get("/checkout", response_class=HTMLResponse)
async def checkout_page():
    return HTMLResponse(content=get_checkout_html())

@app.get("/option1", response_class=HTMLResponse)
async def option1_page():
    return HTMLResponse(content=get_template_html("option1.html"))

@app.get("/option2", response_class=HTMLResponse)
async def option2_page():
    return HTMLResponse(content=get_template_html("option2.html"))

@app.get("/option3", response_class=HTMLResponse)
async def option3_page():
    return HTMLResponse(content=get_template_html("option3.html"))

@app.get("/api/plans")
async def get_plans():
    ton_price = ton_watcher.get_ton_inr_price()
    return {
        "plans": config.PLANS,
        "ton_inr_rate": ton_price,
        "upi_id": config.UPI_ID,
        "ton_wallet": config.TON_WALLET
    }

@app.get("/api/qr")
async def generate_qr(data: str):
    """Generates a high-contrast PNG QR Code in memory"""
    if not data:
        raise HTTPException(status_code=400, detail="Missing data parameter")
    
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_M,
        box_size=8,
        border=2,
    )
    qr.add_data(data)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return Response(content=buf.getvalue(), media_type="image/png")

@app.post("/api/order/create")
async def create_order_endpoint(req: CreateOrderReq):
    plan = config.PLANS.get(req.plan_id)
    if not plan:
        raise HTTPException(status_code=400, detail="Invalid plan ID")

    # Generate TON Memo e.g. KRN-829143
    ton_memo = f"KRN-{secrets.token_hex(3).upper()}"
    ton_amount, ton_price = ton_watcher.calculate_ton_amount(plan["price_inr"])

    order = db.create_order(
        plan=plan,
        payment_method=req.payment_method,
        amount_ton=ton_amount,
        ton_memo=ton_memo
    )

    return {
        "order_id": order["order_id"],
        "plan_id": plan["id"],
        "plan_name": plan["name"],
        "amount_inr": plan["price_inr"],
        "amount_ton": ton_amount,
        "ton_memo": ton_memo,
        "ton_wallet": config.TON_WALLET,
        "upi_id": config.UPI_ID,
        "payee_name": config.PAYEE_NAME,
        "status": "pending"
    }

@app.post("/api/order/submit-utr")
async def submit_utr_endpoint(req: SubmitUTRReq, request: Request):
    order = db.get_order(req.order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")

    clean_utr = req.utr.strip()
    if len(clean_utr) < 8:
        raise HTTPException(status_code=400, detail="Invalid UTR number")

    # Update in DB
    updated = db.set_order_utr(req.order_id, clean_utr)

    # Fire Telegram Alert to Admin Phone with quick-approve links
    base_url = str(request.base_url).rstrip("/")
    telegram_admin.send_admin_utr_alert(
        order_id=order["order_id"],
        utr=clean_utr,
        plan_name=order.get("plan_name", "KRONOS Pass"),
        amount_inr=order.get("amount_inr", 399),
        base_url=base_url
    )

    return {"ok": True, "status": "awaiting_approval", "order_id": req.order_id}

@app.get("/api/order/status/{order_id}")
async def get_order_status(order_id: str):
    order = db.get_order(order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    # If order is pending and payment_method is ton, check on-chain right now!
    if order.get("status") == "pending" and order.get("payment_method") == "ton":
        order = ton_watcher.check_order_ton_payment(order)

    return {
        "order_id": order["order_id"],
        "status": order.get("status", "pending"),
        "api_key": order.get("api_key"),
        "plan_name": order.get("plan_name")
    }

@app.get("/api/admin/quick-approve", response_class=HTMLResponse)
async def admin_quick_approve_page(order_id: str, token: str):
    if not telegram_admin.verify_approval_token(order_id, token):
        raise HTTPException(status_code=403, detail="Invalid or expired approval token.")
    
    order = db.get_order(order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found.")
    
    if order.get("status") == "approved" and order.get("api_key"):
        return HTMLResponse(content=f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>KRONOS — Order Approved</title><meta name="viewport" content="width=device-width, initial-scale=1">
<style>body {{ background: #faf7f2; color: #221c18; font-family: -apple-system, sans-serif; display: flex; align-items: center; justify-content: center; min-height: 100vh; margin: 0; padding: 16px; }}
.card {{ background: #fff; border: 2px solid #e5ddd0; border-radius: 20px; padding: 32px; max-width: 440px; width: 100%; box-shadow: 4px 6px 0px #e8dfd1; text-align: center; }}
.badge {{ display: inline-block; background: #edf5f0; color: #2d5a3f; font-weight: 800; padding: 6px 14px; border-radius: 20px; font-size: 13px; margin-bottom: 14px; }}
.key {{ font-family: monospace; font-size: 18px; font-weight: 700; background: #faf7f2; border: 1.5px dashed #2d5a3f; padding: 12px; border-radius: 12px; margin: 16px 0; color: #2d5a3f; word-break: break-all; }}</style></head>
<body><div class="card"><div class="badge">ALREADY APPROVED</div><h2>Order {order_id}</h2><div class="key">{order.get('api_key')}</div><p>This order has already been approved and delivered.</p></div></body></html>""")

    plan = order.get("plan_name", "KRONOS Pass")
    utr = order.get("utr", "N/A")
    amount = order.get("amount_inr", 399)

    html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>KRONOS — Confirm Approval</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body {{ background: #faf7f2; color: #221c18; font-family: -apple-system, sans-serif; display: flex; align-items: center; justify-content: center; min-height: 100vh; margin: 0; padding: 16px; }}
    .card {{ background: #fff; border: 2px solid #e5ddd0; border-radius: 20px; padding: 32px; max-width: 440px; width: 100%; box-shadow: 4px 6px 0px #e8dfd1; text-align: center; }}
    .badge {{ display: inline-block; background: #faede8; color: #c85a32; font-weight: 800; padding: 6px 14px; border-radius: 20px; font-size: 13px; margin-bottom: 14px; }}
    h2 {{ font-size: 24px; margin-bottom: 8px; }}
    .btn {{ display: block; width: 100%; background: #2d5a3f; color: #fff; border: none; font-size: 16px; font-weight: 700; padding: 14px 20px; border-radius: 12px; cursor: pointer; margin-top: 20px; box-shadow: 0 4px 12px rgba(45,90,63,0.25); }}
    p {{ font-size: 14px; color: #6b635b; line-height: 1.6; text-align: left; background: #faf7f2; padding: 16px; border-radius: 12px; margin-top: 16px; }}
  </style>
</head>
<body>
  <div class="card">
    <div class="badge">CONFIRMATION REQUIRED</div>
    <h2>Approve Order?</h2>
    <p><b>Order ID:</b> {order_id}<br><b>Plan:</b> {plan}<br><b>Amount:</b> ₹{amount}<br><b>UTR:</b> <code>{utr}</code></p>
    <form method="POST" action="/api/admin/quick-approve?order_id={order_id}&token={token}">
      <button type="submit" class="btn">✅ Confirm &amp; Issue License Key</button>
    </form>
  </div>
</body>
</html>"""
    return HTMLResponse(content=html)

@app.post("/api/admin/quick-approve", response_class=HTMLResponse)
async def admin_quick_approve_post(order_id: str, token: str):
    if not telegram_admin.verify_approval_token(order_id, token):
        raise HTTPException(status_code=403, detail="Invalid or expired approval token.")
    
    updated_order = db.approve_order(order_id)
    if not updated_order:
        raise HTTPException(status_code=404, detail="Order not found.")
    
    api_key = updated_order.get("api_key")
    plan = updated_order.get("plan_name")
    utr = updated_order.get("utr") or "Direct Approved"
    
    html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>KRONOS — Order Approved</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body {{ background: #faf7f2; color: #221c18; font-family: -apple-system, sans-serif; display: flex; align-items: center; justify-content: center; min-height: 100vh; margin: 0; padding: 16px; }}
    .card {{ background: #fff; border: 2px solid #e5ddd0; border-radius: 20px; padding: 32px; max-width: 440px; width: 100%; box-shadow: 4px 6px 0px #e8dfd1; text-align: center; }}
    .badge {{ display: inline-block; background: #edf5f0; color: #2d5a3f; font-weight: 800; padding: 6px 14px; border-radius: 20px; font-size: 13px; margin-bottom: 14px; }}
    h2 {{ font-size: 24px; margin-bottom: 8px; }}
    .key {{ font-family: monospace; font-size: 18px; font-weight: 700; background: #faf7f2; border: 1.5px dashed #2d5a3f; padding: 12px; border-radius: 12px; margin: 16px 0; color: #2d5a3f; word-break: break-all; }}
    p {{ font-size: 14px; color: #6b635b; line-height: 1.5; }}
  </style>
</head>
<body>
  <div class="card">
    <div class="badge">APPROVED &amp; DELIVERED</div>
    <h2>Order {order_id}</h2>
    <p><b>Plan:</b> {plan}<br><b>UTR:</b> {utr}</p>
    <div class="key">{api_key}</div>
    <p>The customer browser screen has automatically unlocked and their key is active.</p>
  </div>
</body>
</html>"""
    return HTMLResponse(content=html)

@app.get("/api/admin/quick-reject", response_class=HTMLResponse)
@app.post("/api/admin/quick-reject", response_class=HTMLResponse)
async def admin_quick_reject(order_id: str, token: str):
    if not telegram_admin.verify_approval_token(order_id, token):
        raise HTTPException(status_code=403, detail="Invalid or expired token.")
    
    db.reject_order(order_id, "Rejected by admin via 1-click link")
    html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>KRONOS — Order Rejected</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body {{ background: #faf7f2; color: #221c18; font-family: -apple-system, sans-serif; display: flex; align-items: center; justify-content: center; min-height: 100vh; margin: 0; padding: 16px; }}
    .card {{ background: #fff; border: 2px solid #e5ddd0; border-radius: 20px; padding: 32px; max-width: 440px; width: 100%; box-shadow: 4px 6px 0px #e8dfd1; text-align: center; }}
    .badge {{ display: inline-block; background: #faede8; color: #c85a32; font-weight: 800; padding: 6px 14px; border-radius: 20px; font-size: 13px; margin-bottom: 14px; }}
    h2 {{ font-size: 24px; margin-bottom: 8px; }}
    p {{ font-size: 14px; color: #6b635b; line-height: 1.5; }}
  </style>
</head>
<body>
  <div class="card">
    <div class="badge">REJECTED</div>
    <h2>Order {order_id}</h2>
    <p>This order has been marked as rejected. The customer screen has been notified.</p>
  </div>
</body>
</html>"""
    return HTMLResponse(content=html)

@app.post("/api/telegram-webhook")
@app.post("/api/telegram/webhook")
async def telegram_webhook(request: Request):
    try:
        body = await request.json()
        print(f"[WEBHOOK INCOMING PAYLOAD] {json.dumps(body)}")
        telegram_admin.process_telegram_update(body)
        return {"ok": True}
    except Exception as e:
        print(f"[WEBHOOK ERROR] {e}")
        return {"ok": False, "error": str(e)}

@app.get("/api/telegram/set-webhook")
async def set_telegram_webhook(request: Request):
    base_url = str(request.base_url).rstrip("/")
    webhook_url = f"{base_url}/api/telegram-webhook"
    res = telegram_admin._tg_request("setWebhook", {"url": webhook_url})
    return {"webhook_url": webhook_url, "telegram_response": res}

@app.post("/api/license/recover")
async def recover_license_endpoint(req: RecoverReq):
    lic = db.recover_license(req.query)
    if not lic:
        return {"found": False}
    return {
        "found": True,
        "api_key": lic.get("api_key"),
        "plan_name": lic.get("plan_name"),
        "status": lic.get("status")
    }

@app.post("/api/license/verify")
async def verify_license_endpoint(req: VerifyLicenseReq):
    res = db.verify_license(req.api_key, req.hwid)
    return res

class EngineReq(BaseModel):
    api_key: str
    hwid: Optional[str] = None

@app.post("/api/core/engine")
async def get_engine_payload(req: EngineReq):
    """Verifies license and streams encrypted engine payload into client RAM"""
    lic = db.verify_license(req.api_key, req.hwid)
    if not lic.get("valid"):
        return {"ok": False, "reason": lic.get("reason", "Unauthorized")}

    payload_b64 = crypto_engine.build_encrypted_payload(req.api_key)
    return {
        "ok": True,
        "payload": payload_b64,
        "plan_name": lic.get("plan_name"),
        "expires_at": lic.get("expires_at"),
        "days_left": lic.get("days_left"),
        "devices_used": lic.get("devices_used", 1),
        "max_devices": lic.get("max_devices", 3)
    }

class ResetHWIDReq(BaseModel):
    api_key: str
    admin_token: str

@app.post("/api/admin/reset-hwid")
async def reset_hwid_endpoint(req: ResetHWIDReq):
    valid_tokens = [t for t in [config.TELEGRAM_BOT_TOKEN, config.SECRET_APPROVAL_KEY] if t]
    if not valid_tokens or req.admin_token not in valid_tokens:
        raise HTTPException(status_code=403, detail="Unauthorized")
    ok = db.reset_license_hwid(req.api_key)
    return {"ok": ok, "api_key": req.api_key}

class RevokeKeyReq(BaseModel):
