# Contributing to INVKRONOS

Thank you for your interest in contributing to **INVKRONOS**, the flagship MTProto stealth client engine and automated licensing daemon.

Please review this document to maintain our high engineering rigor and architectural standards.

---

## Code of Conduct

All contributors are expected to uphold the standards outlined in our [Code of Conduct](CODE_OF_CONDUCT.md). Zero harassment, hate speech, or unprofessional conduct will be tolerated.

---

## Architecture & Subsystems

1. **Client Engine (`payload/inv.py`)**: Asynchronous Telethon wrapper featuring curses TUI, voice transcription, anti-recall cache, and zero-presence RPC query batching.
2. **Licensing Server (`server.py`)**: FastAPI ASGI service with rate-limiting, HMAC-signed webhooks, and UPI payment resolution.
3. **TON Watcher (`ton_watcher.py`)**: Decentralized blockchain polling engine monitoring Toncenter RPC for cryptographically memoized transfers.
4. **Telegram Admin Bot (`telegram_admin.py`)**: Interactive push approval interface with inline callbacks and administrative command dispatchers.

---

## Development Setup

### Prerequisites
- Python 3.10, 3.11, or 3.12
- Git
- Virtual Environment (`venv`)

### Initializing the Workspace

```bash
# 1. Clone the repository
git clone git@github.com:thatonearyan-sh/INVKRONOS.git
cd INVKRONOS

# 2. Provision virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 3. Install dependencies
pip install --upgrade pip
pip install -r requirements.txt
pip install flake8 black mypy pytest pytest-asyncio
```

---

## Commit Message Conventions

We enforce the **Conventional Commits** specification. Every commit must follow this format:

```
<type>(<scope>): <short imperative summary>

[optional body]

[optional footer(s)]
```

### Allowed Types:
- `feat`: New feature or operator capability
- `fix`: Bug fix or error resolution
- `perf`: Performance or memory optimization
- `sec`: Security hardening or vulnerability patch
- `refactor`: Structural changes without behavioral side-effects
- `test`: Adding or refining automated tests
- `docs`: Documentation and architecture updates
- `assets`: Visual assets, terminal previews, or branding

### Allowed Scopes:
- `mtproto`, `ui`, `tui`, `stealth`, `vault`, `media`, `intel`, `proxy`, `ytdlp`, `forward`
- `server`, `api`, `db`, `crypto`, `loader`, `upi`, `ton`, `admin`, `billing`, `deploy`

---

## Pull Request Guidelines

1. **Branch Naming**: Use `feat/<short-name>`, `fix/<short-name>`, or `perf/<short-name>`.
2. **Atomic Commits**: Keep changes scoped and self-contained.
3. **No Secrets**: Never commit `.env` files, API hashes, bot tokens, or private keys.
4. **Code Quality**: Ensure code passes linting (`flake8`) and adheres to PEP 8.
5. **PR Description**: Include a clear explanation of changes, testing steps, and any relevant issue references.
