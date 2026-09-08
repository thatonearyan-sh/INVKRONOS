# Security Policy & Vulnerability Disclosure

## Supported Versions

Security patches and emergency hotfixes are actively maintained for the following release lines:

| Version | Supported          | Status |
| :--- | :--- | :--- |
| `5.2.x` | :white_check_mark: | Active Production Release |
| `5.0.x` | :white_check_mark: | Security Patches Only |
| `< 5.0` | :x:                | Deprecated / End of Life |

---

## Zero-Presence & Memory Security Model

INVKRONOS is engineered under strict operational security invariants:
1. **Zero Disk Artifacts**: Payload components (`inv.py`, `subtitle_engine.py`) are transmitted over TLS 1.3, decrypted strictly in volatile RAM via counter-mode stream ciphers, and executed directly via memory-mapped buffers without persistent disk writes.
2. **Ephemeral Session Hygiene**: Telethon string sessions are isolated within memory structures. Revocation triggers an immediate atomic zeroization overwrite (`b"\x00" * len(buffer)`) before memory deallocation.
3. **No Credential Persistence**: Production servers consume runtime secrets strictly via POSIX environment variables. Zero cryptographic keys, private tokens, or administrative IDs are checked into source control.

---

## Reporting a Vulnerability

We prioritize responsible vulnerability disclosures. If you discover a security flaw or an MTProto presence leak:

1. **Do NOT open a public GitHub issue.**
2. Dispatch an encrypted vulnerability report to:
   - **Lead Architect**: Aryan
   - **Email**: `thatonearyan@gmail.com`
   - **Subject Prefix**: `[SECURITY VULNERABILITY] INVKRONOS`
3. Include the following telemetry in your report:
   - Affected subsystem (`Client Engine`, `API Gateway`, `TON Blockchain Watcher`, `Telegram Admin`)
   - Complete Reproduction Steps / Proof-of-Concept (PoC)
   - Wireshark / PCAP traces or MTProto RPC update logs illustrating presence leakage or timing side-channels
   - Proposed mitigation or patch (if available)

---

## Response Service Level Agreements (SLA)

- **Initial Triage & Acknowledgment**: Within **24 hours**.
- **Impact Assessment & CVSS Scoring**: Within **48 hours**.
- **Patch Development & Validation**: Within **5 business days**.
- **Coordinated Public Advisory**: Disclosed only following production deployment across all distributed nodes.
