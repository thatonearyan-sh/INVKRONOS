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

