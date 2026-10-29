# Architecture

## Request path

```
OpenAI-compatible client
        │  POST /v1/chat/completions
        ▼
FastAPI (main.py)
  ├─ API-key auth (Bearer)
  ├─ account selection / rotation / quota handling
  ├─ request log (SQLite) + metrics
  ▼
NotionAIProvider (app/providers/notion_provider.py)
  ├─ OpenAI messages → Notion transcript
  ├─ NDJSON stream reducer (app/providers/notion_stream.py)
  ├─ native tool-call translation (app/adapters/notion_native.py)
  ▼
Notion app.notion.com (curl_cffi Chrome impersonation)

Dashboard ── /dashboard + /api/* ──► FastAPI management routes (local, no auth)
```

## Modules

| Module | Responsibility |
|---|---|
| `app/core/config.py` | Environment configuration (pydantic-settings) |
| `app/core/model_registry.py` | Notion model display-name/codename registry |
| `app/core/account_health.py` | Account status, quota prediction, failure taxonomy, archive |
| `app/core/register_egress.py` | Registration egress node pool, cooldown, pause circuit |
| `app/core/metrics.py` | Dependency-free Prometheus metrics |
| `app/adapters/` | Harness detection and Notion native tool protocol translation |
| `app/providers/notion_stream.py` | Structured NDJSON reducer (thinking/text separation, record-map) |
| `app/providers/notion_provider.py` | Upstream client, retries, thread reuse, streaming |
| `app/dashboard/` | Static HTML/CSS/JS dashboard (macOS Vibrancy theme) |

## State and files

- `accounts.json` — account pool (contains secrets; never commit)
- `notion2api.db` — request logs and metrics source (never commit)
- `accounts_archive.json` — archived accounts (contains secrets; never commit)
- `registration_nodes.json` — editable registration node list
- `register_nodes.json` — node health/cooldown state
- `reg_logs.jsonl` — registration logs
- `.env` — runtime credentials (never commit)

## Usage accounting

OpenAI `usage.prompt_tokens` / `completion_tokens` are estimates of the
client-visible request/response, intended for context-window accounting in
clients such as DSH. Notion's internal agent-loop usage is exposed separately as
`notion_input_tokens`, `notion_output_tokens`, `notion_total_tokens` and
`notion_cached_input_tokens_*`; `credits_used` is the real Notion billing value.
