import os
import zlib
import json
import base64
import struct
import hashlib

CIPHER_SALT = b"KRONOS_STEALTH_CORE_SALT_v3.5_SECURE"

def crypt_stream(data: bytes, key_str: str) -> bytes:
    """Zero-dependency stream cipher based on SHA-256 counter mode"""
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

def build_encrypted_payload(api_key: str, client_dir: str = None) -> str:
    """Packages inv.py and subtitle_engine.py into an encrypted payload"""
