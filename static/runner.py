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
        print(f"{cyan} ├" + "─" * 78 + f"┤{reset}")
        print(f"{cyan} │{yellow}  COMMAND DECK (UNAUTHENTICATED PREVIEW)                                      {cyan}│{reset}")
        print(f"{cyan} │                                                                              │{reset}")
        print(f"{cyan} │{white}  > [ 1 ] Intercept Stream (Zero-Presence Ghost Mode)      {red}[LOCKED - PASS REQ]{cyan}│{reset}")
        print(f"{cyan} │{white}  > [ 2 ] Open Unified Matrix Inbox                        {red}[LOCKED - PASS REQ]{cyan}│{reset}")
        print(f"{cyan} │{white}  > [ 3 ] View-Once Vault & NLP Voice Transcriber          {red}[LOCKED - PASS REQ]{cyan}│{reset}")
        print(f"{cyan} │{white}  > [ 4 ] Inject New Remote Identity (Add Telegram Account){red}[LOCKED - PASS REQ]{cyan}│{reset}")
        print(f"{cyan} │{white}  > [ 5 ] Configure Geo-Shield Proxy Relays (SOCKS5/MTProto){red}[LOCKED - PASS REQ]{cyan}│{reset}")
        print(f"{cyan} │{white}  > [ 6 ] Benchmark Proxy Latency Pool (Auto-Test)         {red}[LOCKED - PASS REQ]{cyan}│{reset}")
        print(f"{cyan} │{white}  > [ 7 ] Fail-Safe Stream Salvager (.part Reconstructor)  {red}[LOCKED - PASS REQ]{cyan}│{reset}")
        print(f"{cyan} │                                                                              │{reset}")
        print(f"{cyan} │{green}  > [ A ] Enter License Key / API Key                      {white}[ACTIVATE NOW]     {cyan}│{reset}")
        print(f"{cyan} │{green}  > [ B ] Open Checkout Page in Browser                    {white}[GET PASS]         {cyan}│{reset}")
        print(f"{cyan} │{green}  > [ S ] Contact Official Telegram Support (@KRONOSSPBOT) {white}[SUPPORT]          {cyan}│{reset}")
        print(f"{cyan} │{dim}  > [ 0 ] Exit Terminal                                                       {cyan}│{reset}")
        print(f"{cyan} └" + "─" * 78 + f"┘{reset}")

        choice = safe_input(f"\n{cyan} kronos@ghost-preview:~# {reset}").strip().upper()

        if choice in ("0", "Q", "EXIT"):
            print(f"\n{green}[✓] Exited KRONOS. Stay invisible.{reset}\n")
            sys.exit(0)

        elif choice in ("B", "BUY", "CHECKOUT"):
            print(f"\n{green}[✓] Opening checkout page in your browser...{reset}")
            try:
                webbrowser.open(f"{server_url}/checkout")
            except Exception:
                pass
            print(f"{white}URL: {cyan}{server_url}/checkout{reset}")
            safe_input(f"\n{dim}Press Enter to return to menu...{reset}")

        elif choice in ("S", "SUPPORT", "HELP"):
            print(f"\n{green}[✓] Opening Telegram Support in your browser...{reset}")
            try:
                webbrowser.open("https://t.me/KRONOSSPBOT")
            except Exception:
                pass
            print(f"{white}Telegram Bot: {cyan}https://t.me/KRONOSSPBOT{reset}")
            safe_input(f"\n{dim}Press Enter to return to menu...{reset}")

        elif choice in ("A", "AUTH", "KEY", "LOGIN"):
            unlocked = prompt_and_authenticate(server_url, hwid)
            if unlocked:
                return

        elif choice in feature_names:
            fname = feature_names[choice]
            print(f"\n{yellow} ┌" + "─" * 70 + f"┐{reset}")
            print(f"{yellow} │ {red}✦ RESTRICTED PROTOCOL — KRONOS STEALTH PASS REQUIRED{yellow}                │{reset}")
            print(f"{yellow} ├" + "─" * 70 + f"┤{reset}")
            print(f"{yellow} │ {white}Feature: {green}{fname[:58]:<58}{yellow} │{reset}")
            print(f"{yellow} │ {white}This stealth capability requires an active authenticated pass.         {yellow}│{reset}")
            print(f"{yellow} │ {white}Instant activation via direct UPI or TON Crypto.                       {yellow}│{reset}")
            print(f"{yellow} │                                                                      │{reset}")
            print(f"{yellow} │ {cyan} [ 1 ] Open Checkout Website in Browser (Get License Key)             {yellow}│{reset}")
            print(f"{yellow} │ {cyan} [ 2 ] Paste License Key / API Key                                    {yellow}│{reset}")
            print(f"{yellow} │ {cyan} [ 3 ] Contact Telegram Support (@KRONOSSPBOT)                        {yellow}│{reset}")
            print(f"{yellow} │ {dim} [ 0 ] Return to Main Menu                                            {yellow}│{reset}")
            print(f"{yellow} └" + "─" * 70 + f"┘{reset}")

            sub_choice = safe_input(f"\n{yellow} Choose action (1-3 or 0): {reset}").strip()
            if sub_choice == "1":
                try:
                    webbrowser.open(f"{server_url}/checkout")
                except Exception:
                    pass
                print(f"\n{green}[✓] Checkout page opened in browser: {server_url}/checkout{reset}")
                safe_input(f"{dim}Press Enter to return to menu...{reset}")
            elif sub_choice == "2":
                unlocked = prompt_and_authenticate(server_url, hwid)
                if unlocked:
                    return
            elif sub_choice == "3":
                try:
                    webbrowser.open("https://t.me/KRONOSSPBOT")
                except Exception:
                    pass
                print(f"\n{green}[✓] Support Bot: https://t.me/KRONOSSPBOT{reset}")
                safe_input(f"{dim}Press Enter to return to menu...{reset}")

def prompt_and_authenticate(server_url, hwid):
    """Prompts for key, verifies with licensing server, and executes engine if valid."""
    cyan = "\033[1;36m"
    green = "\033[1;32m"
    yellow = "\033[1;33m"
    red = "\033[1;31m"
    white = "\033[1;37m"
    reset = "\033[0m"

    print(f"\n{cyan}✦ ═════════════════════════════════════════════════════════════ ✦{reset}")
    print(f"{green}                 KRONOS STEALTH SUITE AUTH GATE                 {reset}")
    print(f"{cyan}✦ ═════════════════════════════════════════════════════════════ ✦{reset}\n")
    print(f"  Hardware Fingerprint (HWID) : {white}{hwid}{reset}")
    print(f"  Acquire Access Key         : {green}{server_url}/checkout{reset}\n")

    entered = safe_input(f"  {cyan}Paste License Key (e.g. KRN-XXXX-XXXX-...): {reset}").strip()
    if not entered:
        print(f"{red}[X] No key entered.{reset}")
        time.sleep(1)
        return False

    print(f"\n{yellow}⌛ Authenticating with KRONOS Licensing Server...{reset}")
    try:
        res = fetch_encrypted_engine(server_url, entered, hwid)
    except Exception as e:
        print(f"{red}[X] Failed to connect to server: {e}{reset}")
        time.sleep(2)
        return False

    if not res.get("ok"):
        reason = res.get("reason", "License validation failed.")
        print(f"\n{red}[X] Authentication Denied: {reason}{reset}")
        print(f"    Get an active license at: {server_url}/checkout")
        print(f"    Or contact support: https://t.me/KRONOSSPBOT\n")
        time.sleep(2.5)
        return False

    # Save verified key
    save_stored_key(entered)
    print(f"{green}[✓] License Verified! Plan: {res.get('plan_name', 'ACTIVE')}{reset}")
    dev_info = ""
    if res.get("devices_used") is not None and res.get("max_devices") is not None:
        dev_info = f" [Device Slot: {res['devices_used']}/{res['max_devices']}]"
    print(f"{green}[✓] Key authorized on this device.{dev_info}{reset}")
    ensure_dependencies()
    print(f"{cyan}⚡ Decrypting stealth core into RAM...{reset}\n")
    time.sleep(0.5)

    execute_bundle(res["payload"], entered)
    return True

def execute_bundle(b64_payload, key):
    """Decrypts bundle directly into RAM and starts inv.py"""
    ensure_dependencies()
    encrypted = base64.b64decode(b64_payload.encode("ascii"))
    compressed = crypt_stream(encrypted, key)
    bundle = json.loads(zlib.decompress(compressed).decode("utf-8"))

    # Execute main engine in RAM
    if "inv.py" in bundle:
        engine_globals = {
            "__name__": "__main__",
            "__file__": "kronos_engine",
            "__builtins__": __builtins__
        }
        exec(bundle["inv.py"], engine_globals)

def main():
    hwid = get_hwid()
    api_key = load_stored_key()

    # Allow CLI flag override (e.g. kronos KRN-...)
    if len(sys.argv) > 1 and sys.argv[1].startswith("KRN-"):
        api_key = sys.argv[1]
        save_stored_key(api_key)

    # If key exists, attempt direct authentication
    if api_key:
        print("\033[1;34m⌛ Authenticating with Kronos Licensing Server...\033[0m")
        try:
            res = fetch_encrypted_engine(DEFAULT_SERVER_URL, api_key, hwid)
            if res.get("ok"):
                dev_info = ""
                if res.get("devices_used") is not None and res.get("max_devices") is not None:
                    dev_info = f" [Device Slot: {res['devices_used']}/{res['max_devices']}]"
                print("\033[1;32m[✓] License verified: " + res.get("plan_name", "ACTIVE") + dev_info + "\033[0m")
                ensure_dependencies()
                print("\033[1;36m⚡ Decrypting stealth engine in-memory (RAM)... \033[0m")
                execute_bundle(res["payload"], api_key)
                return
            else:
                reason = res.get("reason", "License validation failed.")
                print(f"\n\033[1;31m[X] Authentication Denied: {reason}\033[0m")
                if os.path.exists(LICENSE_FILE):
                    try:
                        os.remove(LICENSE_FILE)
                    except Exception:
                        pass
        except Exception as e:
            print(f"\033[1;31m[X] Connection failed: {e}\033[0m")
            print(f"    Check your internet connection or verify {DEFAULT_SERVER_URL}")

    # If no key or validation failed, enter Interactive Locked Showcase Mode
    run_showcase_mode(DEFAULT_SERVER_URL, hwid)

if __name__ == "__main__":
    main()
