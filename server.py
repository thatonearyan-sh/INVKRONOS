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
