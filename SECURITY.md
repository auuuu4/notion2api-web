# Security Policy

## Supported versions

Security fixes are applied to the current `main` branch (5.x).

## Reporting a vulnerability

Please do **not** open a public issue for credentials, account-token leaks or
remote-code concerns. Report details privately to the maintainers and allow a
reasonable window for a fix before disclosure.

## Deployment warning

This project is a local proxy/control panel, not a multi-tenant hosted service:

- **The dashboard (`/dashboard`) and management API (`/api/*`) are unauthenticated by design.**
  `/api/settings` returns secrets (API key, temporary-mail credentials) and
  `/api/accounts` can create/delete accounts. **Bind the service to
  `127.0.0.1`** or put it behind an authenticating reverse proxy/firewall.
- The inference API (`/v1/*`) requires `Authorization: Bearer <API_MASTER_KEY>`.
- `token_v2`, `accounts.json`, `.env`, `registration_nodes.json`, and the
  SQLite/JSON state files are sensitive. Keep them out of version control and
  restrict filesystem permissions.
- Do not expose the service directly to the public internet. If remote access is
  needed, terminate TLS and authentication at a trusted reverse proxy.

## Credential hygiene

- Never commit `.env`, `accounts.json`, `notion2api.db`, registration state, or
  captured Notion transcripts containing account identifiers.
- Rotate `API_MASTER_KEY` and any temporary-mail admin credentials if they were
  ever exposed.
