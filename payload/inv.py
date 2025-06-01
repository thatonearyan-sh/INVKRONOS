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
