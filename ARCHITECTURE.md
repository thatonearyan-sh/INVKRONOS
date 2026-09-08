# INVKRONOS System Architecture Blueprint

```
+-------------------------------------------------------------------------------+
|                             INVKRONOS SYSTEM TOPOLOGY                         |
+-------------------------------------------------------------------------------+

        +-------------------------------------------------------------+
        |                     OPERATOR TERMINAL                       |
        |  [24-bit TrueColor Matrix TUI / Interactive Dialog Engine]   |
        +------------------------------+------------------------------+
                                       |
                   Encrypted Memory    |  Bootstrap & Telemetry
                   Stream Decryptor    |  (Zero Disk Traces)
                                       v
        +-------------------------------------------------------------+
        |                 FASTAPI SERVERLESS GATEWAY                  |
        |   [/api/v1/license/validate | /api/v1/order/poll | CORS]    |
        +---------------+-----------------------------+---------------+
                        |                             |
        +---------------+-------------+ +-------------+---------------+
        |    TON BLOCKCHAIN ENGINE    | |     UPI PAYMENT ADAPTER     |
        |  Toncenter RPC Polling      | |  Dynamic QR / 12-Digit UTR  |
        |  Unique Order Memo Match    | |  Anti-Double-Spend Buffer   |
        +---------------+-------------+ +-------------+---------------+
                        |                             |
                        v                             v
        +-------------------------------------------------------------+
        |                   TELEGRAM ADMIN PUSH BOT                   |
        |  - Instant Payment Audit Cards (/stats, /keys, /revoke)     |
        |  - 1-Click HMAC-SHA256 Signed Approval Query Handlers       |
        +------------------------------+------------------------------+
                                       |
                                       v
        +-------------------------------------------------------------+
        |                 MONGODB ATLAS PERSISTENCE                   |
        |   [Collections: licenses | hardware_ids | order_audits]    |
        +-------------------------------------------------------------+
```

---

## Core Engineering Invariants

1. **Zero-Presence MTProto Daemon**:
   - Suppresses all outbound `sendMessageTypingAction` and `sendMediaUploadAction` packets.
   - Schedules outgoing frames with `schedule_date = now() + 10s` to decouple send events from online presence updates.
   - Batch RPC queries are pipelined with randomized jitter intervals (150ms - 450ms) to bypass rate limiting heuristics.

2. **In-Memory Volatile RAM Execution**:
   - `static/runner.py` fetches encrypted payload bytes over TLS 1.3.
   - Payload is decrypted in-place using SHA-256 counter-mode stream cipher (`crypto_engine.py`).
   - Module is imported dynamically into Python process namespace via `exec()` with clean globals; zero temporary `.py` files touch the filesystem.

3. **Autonomous Blockchain Verification**:
   - Inbound TON transfers are matched against 6-character cryptographic memos (`KRN-XXXXXX`).
   - Dual-node failover: Toncenter RPC v2 primary with fallback to public gateways.
   - Atomic license issuance prevents duplicate transaction claims.
