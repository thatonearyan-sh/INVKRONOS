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
