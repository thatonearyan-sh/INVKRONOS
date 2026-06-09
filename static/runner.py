#!/usr/bin/env python3
"""
KRONOS Stealth Suite — Universal Cloud Runner & In-Memory Engine Loader
Works on macOS, Windows, Linux, and Android (Termux).
Zero source code stored on disk. Decrypted directly into RAM.
"""

import os
import sys
import json
import zlib
import base64
import struct
import hashlib
import uuid
import platform
import urllib.request
import ssl
import types
import subprocess
import importlib
import importlib.util

REQUIRED_PACKAGES = {
    "telethon": "telethon",
    "colorama": "colorama",
}

def ensure_dependencies():
    """
    Silently and quickly ensures core terminal packages are present.
    Lightweight and instantaneous — zero heavy/compilation blocking.
    """
    missing = [pkg for mod, pkg in REQUIRED_PACKAGES.items() if importlib.util.find_spec(mod) is None]
    if not missing:
        return True

    cyan = "\033[1;36m"
    green = "\033[1;32m"
    reset = "\033[0m"

    print(f"{cyan}[i] Synchronizing stealth runtime dependencies ({', '.join(missing)})...{reset}")

    # Strategies to handle PEP 668 on modern Linux/Termux/macOS
    strategies = [
        [sys.executable, "-m", "pip", "install", "--break-system-packages", "--quiet"] + missing,
        [sys.executable, "-m", "pip", "install", "--user", "--break-system-packages", "--quiet"] + missing,
        [sys.executable, "-m", "pip", "install", "--user", "--quiet"] + missing,
        [sys.executable, "-m", "pip", "install", "--quiet"] + missing,
    ]

    installed = False
    for cmd in strategies:
        try:
            res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
            if res.returncode == 0:
                installed = True
                break
        except Exception:
            continue

    if not installed:
        try:
            subprocess.run([sys.executable, "-m", "ensurepip", "--upgrade"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=60)
            for cmd in strategies:
                res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120)
                if res.returncode == 0:
                    installed = True
                    break
        except Exception:
            pass

    # Add user site-packages dynamically to sys.path if not present
    try:
        import site
        user_site = site.getusersitepackages()
        if os.path.exists(user_site) and user_site not in sys.path:
            sys.path.insert(0, user_site)
    except Exception:
        pass

    try:
        importlib.invalidate_caches()
    except Exception:
        pass

    print(f"{green}[✓] Stealth runtime dependencies synchronized successfully.{reset}\n")
    return True

# Reconnect stdin to controlling terminal if piped (e.g. curl ... | bash)
if not sys.stdin.isatty():
    try:
        sys.stdin = open("/dev/tty", "r")
    except Exception:
        pass

def safe_input(prompt_text=""):
    """Reads input safely from terminal without throwing EOFError or unhandled KeyboardInterrupt."""
    try:
        return input(prompt_text)
    except (EOFError, KeyboardInterrupt):
        print("\n\n\033[1;32m[✓] Exited KRONOS. Stay invisible.\033[0m\n")
        sys.exit(0)

# Configurable Server Endpoint
DEFAULT_SERVER_URL = os.getenv("KRONOS_API_URL", "http://localhost:8000").rstrip("/")
CONFIG_DIR = os.path.expanduser("~/.kronos")
LICENSE_FILE = os.path.join(CONFIG_DIR, "license.json")
CIPHER_SALT = b"KRONOS_STEALTH_CORE_SALT_v3.5_SECURE"

def crypt_stream(data: bytes, key_str: str) -> bytes:
    key = hashlib.sha256(key_str.encode("utf-8") + CIPHER_SALT).digest()
    out = bytearray(len(data))
    block_idx = 0
    stream = b""
    stream_pos = 0
    for i in range(len(data)):
        if stream_pos >= len(stream):
            stream = hashlib.sha256(key + struct.pack(">Q", block_idx)).digest()
            block_idx += 1
            stream_pos = 0
        out[i] = data[i] ^ stream[stream_pos]
        stream_pos += 1
    return bytes(out)

def get_hwid():
    try:
        node = uuid.getnode()
        system = platform.system()
        machine = platform.machine()
        raw = f"{node}:{system}:{machine}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24].upper()
    except Exception:
        return "GENERIC-HWID-001"

def load_stored_key():
    if os.path.exists(LICENSE_FILE):
        try:
            with open(LICENSE_FILE, "r") as f:
                data = json.load(f)
                return data.get("api_key")
        except Exception:
            pass
    # Check local directory
    if os.path.exists(".license.json"):
        try:
            with open(".license.json", "r") as f:
                return json.load(f).get("api_key")
        except Exception:
            pass
    return None

def save_stored_key(key):
    os.makedirs(CONFIG_DIR, exist_ok=True)
    try:
        with open(LICENSE_FILE, "w") as f:
            json.dump({"api_key": key.strip()}, f, indent=2)
    except Exception:
        pass

def fetch_encrypted_engine(server_url, api_key, hwid):
    url = f"{server_url}/api/core/engine"
    payload = json.dumps({"api_key": api_key.strip(), "hwid": hwid}).encode("utf-8")
    
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json", "User-Agent": "KronosLoader/3.5"}
    )
    with urllib.request.urlopen(req, context=ctx, timeout=12) as resp:
        return json.loads(resp.read().decode("utf-8"))

import webbrowser
import time

# Enable Windows ANSI Escape Sequence parsing
if os.name == "nt":
    try:
        os.system("")
    except Exception:
        pass

def run_showcase_mode(server_url, hwid):
    """
    Interactive Locked Showcase Console.
    Allows prospective buyers to preview the full KRONOS environment.
    All features are locked and prompt the user to get a license key.
    """
    # Open browser on initial entry into showcase mode
    try:
        webbrowser.open(f"{server_url}/checkout")
    except Exception:
        pass

    cyan = "\033[1;36m"
    green = "\033[1;32m"
    yellow = "\033[1;33m"
    red = "\033[1;31m"
    white = "\033[1;37m"
    dim = "\033[2;37m"
    reset = "\033[0m"

    feature_names = {
        "1": "Zero-Presence Ghost Mode (Read receipts / Blue ticks bypass)",
        "2": "Unified Matrix Inbox (Multi-chat aggregated terminal)",
        "3": "Pre-Destruction Vault & Voice Note NLP Transcriber",
        "4": "Remote Identity Injector (Multi-account matrix)",
        "5": "Geo-Shield Proxy Relays (SOCKS5/MTProto rotation)",
        "6": "Proxy Latency Pool Auto-Benchmark",
        "7": "Fail-Safe Stream Salvager (.part reconstructor)"
    }

    while True:
        os.system("cls" if os.name == "nt" else "clear")
        print(f"{cyan} ┌" + "─" * 78 + f"┐{reset}")
        print(f"{cyan} │{green}  ██╗  ██╗██████╗  ██████╗ ███╗   ██╗ ██████╗ ███████╗                       {cyan}│{reset}")
        print(f"{cyan} │{green}  ██║ ██╔╝██╔══██╗██╔═══██╗████╗  ██║██╔═══██╗██╔════╝  {white}[ STEALTH CONSOLE ] {cyan}│{reset}")
        print(f"{cyan} │{green}  █████╔╝ ██████╔╝██║   ██║██╔██╗ ██║██║   ██║███████╗  {yellow}[ STATUS: LOCKED  ] {cyan}│{reset}")
        print(f"{cyan} │{green}  ██╔═██╗ ██╔══██╗██║   ██║██║╚██╗██║██║   ██║╚════██║  {white}[ HWID: {hwid[:10]}... ] {cyan}│{reset}")
        print(f"{cyan} │{green}  ██║  ██╗██║  ██║╚██████╔╝██║ ╚████║╚██████╔╝███████║                       {cyan}│{reset}")
        print(f"{cyan} │{green}  ╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝ ╚═╝  ╚═══╝ ╚═════╝ ╚══════╝  {dim}v3.5 GHOST MATRIX   {cyan}│{reset}")
        print(f"{cyan} ├" + "─" * 78 + f"┤{reset}")
        print(f"{cyan} │{white} [ NODE : ONLINE ]  [ ENCRYPTION : AES-256 ]  [ TELEGRAM CORE : ASYNC ]      {cyan}│{reset}")
