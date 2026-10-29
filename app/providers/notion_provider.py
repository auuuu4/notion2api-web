# app/providers/notion_provider.py
import json
import os
import time
import logging
import uuid
import re
from typing import Dict, Any, AsyncGenerator, List, Optional, Tuple
from datetime import datetime

from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.concurrency import run_in_threadpool

from app.core.config import settings
from app.core import model_registry
from app.adapters import get_adapter, tool_specs_from_openai
from app.adapters.notion_native import (
    extend_tool_specs,
    extract_native_tool_calls,
    extract_supplemental_calls_from_text,
    official_function_for_tool,
    record_native_tool_calls,
    translate_native_tool_calls,
    translate_supplemental_tool_calls,
)
from app.providers.base_provider import BaseProvider
from app.utils.sse_utils import create_sse_data, create_chat_completion_chunk, DONE_CHUNK
from app.utils.http_client import create_session
from app.providers.notion_stream import NotionStreamReducer
from app.core.metrics import metrics
from app.prompt_injection import build_system_prompt, inject_prompt_into_transcript, is_few_shot_confirmation

logger = logging.getLogger(__name__)

class NotionUpstreamError(RuntimeError):
    """Base class for classified Notion upstream failures."""

    code = "upstream"


class NotionAccountInvalid(NotionUpstreamError):
    code = "account_invalid"


class NotionResponseEmpty(NotionUpstreamError):
    code = "empty_response"


class NotionResponseTruncated(NotionUpstreamError):
    code = "truncated"


class NotionTransportError(NotionUpstreamError):
    code = "transport"


def _stream_reducer_enabled() -> bool:
    """Structured reducer is the default; NOTION_STREAM_REDUCER=0 falls back."""
    raw = os.environ.get("NOTION_STREAM_REDUCER", "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def _is_auth_error(exc: Exception) -> bool:
    """True for HTTP 401/403 responses that indicate a dead account token."""
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    if status in (401, 403):
        return True
    lowered = str(exc).lower()
    return any(marker in lowered for marker in ("http 401", "http 403", "unauthorized", "invalid token"))


# reuse-key -> {"thread_id": str, "account_id": str}
# A Notion thread belongs to the account (and its workspace) that created it;
# replaying into a thread with another account makes Notion answer an empty
# stream, so the owner account is cached together with the thread.
_THREAD_CACHE: Dict[str, Dict[str, str]] = {}
_THREAD_CACHE_LIMIT = 256


def _thread_reuse_key(messages) -> str:
    for message in messages or []:
        if message.get("role") == "user" and message.get("content"):
            return str(message["content"])[:500]
    return ""


def _account_cache_id(account: Optional[Dict[str, Any]]) -> str:
    """Stable identifier for the account that owns a cached thread."""
    if not account:
        return ""
    return str(account.get("id") or account.get("token_v2") or "__env__")


def _thread_cache_get(key: str) -> Optional[Dict[str, str]]:
    entry = _THREAD_CACHE.get(key)
    if isinstance(entry, str):  # legacy entries from an older cache shape
        return {"thread_id": entry, "account_id": ""}
    return entry


def _thread_cache_put(key: str, thread_id: str, account_id: str) -> None:
    if len(_THREAD_CACHE) > _THREAD_CACHE_LIMIT:
        _THREAD_CACHE.clear()
    _THREAD_CACHE[key] = {"thread_id": thread_id, "account_id": account_id}


def _thread_cache_forget(key: str, thread_id: str) -> None:
    entry = _thread_cache_get(key)
    if entry and entry.get("thread_id") == thread_id:
        _THREAD_CACHE.pop(key, None)


def forget_threads_for_account(account_id: str) -> int:
    """Drop every cached Notion thread owned by one account.

    Called when an account is disabled/archived so later turns of those
    conversations do not keep pinning a dead account.
    """
    target = str(account_id or "")
    if not target:
        return 0
    removed = 0
    for key, entry in list(_THREAD_CACHE.items()):
        data = entry if isinstance(entry, dict) else {"account_id": ""}
        if str(data.get("account_id") or "") == target:
            _THREAD_CACHE.pop(key, None)
            removed += 1
    return removed


def pinned_thread_account_id(messages) -> str:
    """Account id that owns the cached Notion thread for this conversation.

    main.py uses this before round-robin account selection so a multi-turn
    conversation keeps using the account that created its thread. Returns an
    empty string when thread reuse is disabled or no thread is cached yet.
    """
    if not _thread_reuse_enabled():
        return ""
    key = _thread_reuse_key(messages)
    if not key:
        return ""
    entry = _thread_cache_get(key)
    return (entry or {}).get("account_id", "") or ""


_FLATTEN_MODE_ALWAYS = "always"
_FLATTEN_MODE_NEVER = "never"
_FLATTEN_MODE_NATIVE_ONLY = "native-only"


def _flatten_tool_results_mode() -> str:
    """How tool results are projected into the Notion transcript.

    NOTION_FLATTEN_TOOL_RESULTS:
      unset / 1 (default) -> "always": keep the user-text summary (compat)
      0                   -> "never": only native agent-tool-result entries
      native-only         -> flatten only when native result replay is disabled
    """
    raw = (os.environ.get("NOTION_FLATTEN_TOOL_RESULTS", "") or "").strip().lower()
    if raw in ("", "1", "true", "yes", "on", "always"):
        return _FLATTEN_MODE_ALWAYS
    if raw in ("0", "false", "no", "off", "never"):
        return _FLATTEN_MODE_NEVER
    if raw in ("native-only", "native_only", "nativeonly"):
        return _FLATTEN_MODE_NATIVE_ONLY
    logger.warning("Unknown NOTION_FLATTEN_TOOL_RESULTS=%r; using 'always'", raw)
    return _FLATTEN_MODE_ALWAYS


def _flatten_tool_results_enabled() -> bool:
    """Backward-compatible boolean: False only for the explicit 'never' mode."""
    return _flatten_tool_results_mode() != _FLATTEN_MODE_NEVER


def _should_flatten_tool_results() -> bool:
    """Whether tool results are merged into a user message for this request."""
    mode = _flatten_tool_results_mode()
    if mode == _FLATTEN_MODE_ALWAYS:
        return True
    if mode == _FLATTEN_MODE_NEVER:
        return False
    return not _native_tool_replay_enabled()


def _native_tool_use_replay_enabled() -> bool:
    """Replay assistant tool_calls as agent-inference/tool_use steps.

    Requires native result replay; opt out with NOTION_NATIVE_TOOL_USE_REPLAY=0.
    """
    if not _native_tool_replay_enabled():
        return False
    raw = os.environ.get("NOTION_NATIVE_TOOL_USE_REPLAY", "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def _thread_reuse_enabled() -> bool:
    """Reuse one Notion thread per OpenAI conversation; opt out with NOTION_THREAD_REUSE=0."""
    return os.environ.get("NOTION_THREAD_REUSE", "1").strip().lower() not in ("0", "false", "no", "off")


def _native_tool_replay_enabled() -> bool:
    """Official agent-tool-result replay is on by default; opt out with NOTION_NATIVE_TOOL_REPLAY=0."""
    return os.environ.get("NOTION_NATIVE_TOOL_REPLAY", "1").strip().lower() not in ("0", "false", "no", "off")


def _native_entry_replay_ids(entry: Dict[str, Any]) -> set:
    """Ids of transcript entries this request replayed into the Notion thread.

    Notion echoes replayed agent-inference/tool_use items with the original call
    id and agent-tool-result steps with the id we generated. The trailing
    record-map of the next response contains those echoes without a startedAt,
    so extraction must treat them as old history, never as fresh tool calls.
    """
    ids: set = set()
    if not isinstance(entry, dict):
        return ids
    if entry.get("type") == "agent-inference":
        for item in entry.get("value") or []:
            if isinstance(item, dict) and item.get("type") == "tool_use" and item.get("id"):
                ids.add(str(item["id"]))
    elif entry.get("id"):
        ids.add(str(entry["id"]))
    return ids

_TRUNCATION_RETRY_HINT = (
    "\n\n[System note: your previous reply was truncated because the reasoning/output was too long. "
    "Answer directly or immediately call the runCode tool (do the computation inside code), "
    "keep prose reasoning under 5 lines, and do not repeat the earlier long reasoning.]"
)


def _append_truncation_retry_hint(request_data: Dict[str, Any]) -> bool:
    """Append a concise-retry instruction to the latest real user message."""
    messages = request_data.get("messages") or []
    target = None
    try:
        adapter = get_adapter(request_data.get("_adapter_name") or "generic")
        index = adapter.latest_user_request_index(messages)
        if index is not None:
            target = messages[index]
    except Exception:
        target = None
    if target is None:
        for msg in reversed(messages):
            if msg.get("role") == "user":
                target = msg
                break
    if target is None:
        return False
    content = target.get("content")
    if isinstance(content, list):
        content.append({"type": "text", "text": _TRUNCATION_RETRY_HINT})
    else:
        target["content"] = (content or "") + _TRUNCATION_RETRY_HINT
    return True


def _estimate_text_tokens(text: Any) -> int:
    """Rough OpenAI-style token estimate (CJK chars count as one token)."""
    if text is None:
        return 0
    if not isinstance(text, str):
        try:
            text = json.dumps(text, ensure_ascii=False)
        except (TypeError, ValueError):
            text = str(text)
    if not text:
        return 0
    cjk = 0
    other = 0
    for char in text:
        code = ord(char)
        if (0x4E00 <= code <= 0x9FFF or 0x3040 <= code <= 0x30FF or 0xAC00 <= code <= 0xD7AF):
            cjk += 1
        else:
            other += 1
    return int(cjk + other / 4) + 1


def _estimate_prompt_tokens(messages: Any, tools: Any = None) -> int:
    """Estimate the client-visible prompt size of an OpenAI request.

    Notion reports usage for its internal agent loop (often 200K+ tokens for a
    small request), which must not be exposed as OpenAI prompt_tokens: agent
    clients such as DSH use it for context-window accounting and would falsely
    trigger compaction/context overflow. Keep upstream numbers in the extra
    notion_* fields instead.
    """
    total = 0
    for message in messages or []:
        if not isinstance(message, dict):
            continue
        total += 4  # role / formatting overhead
        content = message.get("content")
        if isinstance(content, str):
            total += _estimate_text_tokens(content)
        elif isinstance(content, list):
            for part in content:
                if isinstance(part, dict):
                    if part.get("type") in ("text", "input_text"):
                        total += _estimate_text_tokens(part.get("text") or part.get("content") or "")
                    elif part.get("type") in ("image_url", "input_image", "image"):
                        total += 765  # OpenAI vision image token approximation
                else:
                    total += _estimate_text_tokens(part)
        else:
            total += _estimate_text_tokens(content)
        if message.get("tool_calls"):
            total += _estimate_text_tokens(message["tool_calls"])
    if tools:
        total += _estimate_text_tokens(tools)
    return max(1, total)


class NotionAIProvider(BaseProvider):
    def __init__(self):
        # Chrome TLS fingerprint: python-requests/cloudscraper JA3 is rejected by Notion AI.
        # One session per account: a shared cookie jar leaks account A's cookies into
        # account B's requests and Notion answers 401.
        self._scrapers: Dict[str, Any] = {}
        # NOTION_API_BASE lets tests point the provider at a mock upstream that
        # replays recorded official NDJSON; production keeps the real host.
        api_base = (
            os.environ.get("NOTION_API_BASE") or "https://app.notion.com/api/v3"
        ).rstrip("/")
        self.api_endpoints = {
            "runInference": api_base + "/runInferenceTranscript",
            "saveTransactions": api_base + "/saveTransactionsFanout",
        }
        self._has_env_account = all([settings.NOTION_COOKIE, settings.NOTION_SPACE_ID, settings.NOTION_USER_ID])
        if self._has_env_account:
            self._warmup_session()

    def _session_for(self, account: Optional[Dict[str, Any]] = None):
        key = str((account or {}).get("id") or (account or {}).get("token_v2") or "__env__")
        session = self._scrapers.get(key)
        if session is None:
            session = create_session()
            self._scrapers[key] = session
        return session

    def _get_account_creds(self, account: Optional[Dict] = None) -> Dict[str, str]:
        """Resolve account credentials: prefer passed-in account, fallback to .env settings."""
        if account and account.get("token_v2"):
            cookie = account["token_v2"]
            if "=" not in cookie:
                cookie = f"token_v2={cookie}"
            return {
                "cookie": cookie,
                "space_id": account.get("space_id", ""),
                "user_id": account.get("user_id", ""),
                "user_name": account.get("user_name", ""),
                "user_email": account.get("user_email", ""),
            }
        return {
            "cookie": settings.NOTION_COOKIE or "",
            "space_id": settings.NOTION_SPACE_ID or "",
            "user_id": settings.NOTION_USER_ID or "",
            "user_name": settings.NOTION_USER_NAME or "",
            "user_email": settings.NOTION_USER_EMAIL or "",
        }

    def _warmup_session(self):
        try:
            logger.info("正在进行会话预热 (Session Warm-up)...")
            headers = self._prepare_headers()
            headers.pop("Accept", None)
            response = self._session_for().get("https://www.notion.so/", headers=headers, timeout=30)
            response.raise_for_status()
            logger.info("会话预热成功。")
        except Exception as e:
            logger.error(f"会话预热失败: {e}", exc_info=True)

    async def _create_thread(self, thread_type: str, account: Optional[Dict] = None) -> str:
        creds = self._get_account_creds(account)
        thread_id = str(uuid.uuid4())
        payload = {
            "requestId": str(uuid.uuid4()),
            "transactions": [{
                "id": str(uuid.uuid4()),
                "spaceId": creds["space_id"],
                "operations": [{
                    "pointer": {"table": "thread", "id": thread_id, "spaceId": creds["space_id"]},
                    "path": [],
                    "command": "set",
                    "args": {
                        "id": thread_id, "version": 1, "parent_id": creds["space_id"],
                        "parent_table": "space", "space_id": creds["space_id"],
                        "created_time": int(time.time() * 1000),
                        "created_by_id": creds["user_id"], "created_by_table": "notion_user",
                        "messages": [], "data": {}, "alive": True, "type": thread_type
                    }
                }]
            }]
        }
        try:
            logger.info(f"正在创建新的对话线程 (type: {thread_type})...")
            response = await run_in_threadpool(
                lambda: self._session_for(account).post(
                    self.api_endpoints["saveTransactions"],
                    headers=self._prepare_headers(account),
                    json=payload,
                    timeout=20
                )
            )
            response.raise_for_status()
            logger.info(f"对话线程创建成功, Thread ID: {thread_id}")
            return thread_id
        except Exception as e:
            logger.error(f"创建对话线程失败: {e}", exc_info=True)
            raise Exception("无法创建新的对话线程。")

    def _normalize_content(self, content) -> str:
        """Normalize content to plain string. Handles both string and list formats."""
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts = []
            for part in content:
                if isinstance(part, str):
                    parts.append(part)
                elif isinstance(part, dict) and part.get("type") == "text":
                    parts.append(part.get("text", ""))
            return "\n".join(parts)
        return str(content) if content is not None else ""

    @staticmethod
    def _describe_tool_calls(tool_calls: List[Dict[str, Any]]) -> str:
        """Legacy human-readable description used when native replay is unavailable."""
        action_parts = []
        for tc in tool_calls:
            fname = (tc.get("function") or {}).get("name", "")
            fargs_str = (tc.get("function") or {}).get("arguments", "{}")
            try:
                fargs = json.loads(fargs_str) if isinstance(fargs_str, str) else (fargs_str or {})
            except (json.JSONDecodeError, TypeError):
                fargs = {}
            if isinstance(fargs, dict) and fargs:
                short_args = {}
                for key, value in fargs.items():
                    if isinstance(value, str) and len(value) > 200:
                        short_args[key] = value[:200] + f"...[truncated {len(value)} chars]"
                    else:
                        short_args[key] = value
                arg_desc = json.dumps(short_args, ensure_ascii=False)
                action_parts.append(f"I called {fname} with arguments {arg_desc}")
            else:
                action_parts.append(f"I called {fname}")
        return "\n".join(action_parts)

    @staticmethod
    def _build_native_tool_use_entry(tool_calls: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Build an official agent-inference/tool_use step for an assistant turn.

        Only explicitly mapped native tools are replayed. If any call in the turn
        is unmapped or has unusable arguments, returns None so the caller keeps
        the legacy description text instead of guessing a function name.
        """
        items: List[Dict[str, Any]] = []
        for tc in tool_calls or []:
            func = tc.get("function") or {}
            name = func.get("name") or ""
            official = official_function_for_tool(name)
            if not official:
                logger.info("[NATIVE] no official tool_use mapping for %s; keeping description text", name or "<missing>")
                return None
            raw_args = func.get("arguments")
            if isinstance(raw_args, str):
                try:
                    args = json.loads(raw_args) if raw_args.strip() else {}
                except (json.JSONDecodeError, ValueError):
                    logger.info("[NATIVE] invalid JSON args for %s; keeping description text", name)
                    return None
            elif isinstance(raw_args, dict):
                args = raw_args
            elif raw_args is None:
                args = {}
            else:
                logger.info("[NATIVE] unsupported args type for %s; keeping description text", name)
                return None
            if not isinstance(args, dict):
                logger.info("[NATIVE] non-object args for %s; keeping description text", name)
                return None
            items.append({
                "type": "tool_use",
                "id": tc.get("id") or ("toolu_" + uuid.uuid4().hex),
                "name": "callFunction",
                "content": json.dumps({"function": official, "args": args}, ensure_ascii=False),
                "isCustomToolCall": False,
                "autofixedJson": False,
            })
        if not items:
            return None
        return {
            "id": str(uuid.uuid4()),
            "type": "agent-inference",
            "value": items,
        }

    @staticmethod
    def _is_tool_use_replay_rejection(exc: Exception) -> bool:
        """True only for an explicit transcript-shape rejection (never auth/quota/5xx)."""
        status = getattr(getattr(exc, "response", None), "status_code", None)
        return status in (400, 422)

    def _preprocess_tool_history(self, request_data: Dict[str, Any]) -> None:
        """Convert OpenAI tool history into Notion-native transcript entries.

        Assistant tool_calls become agent-inference/tool_use steps when an
        explicit native mapping exists; tool results become agent-tool-result
        steps. The legacy user-text flatten is controlled by
        NOTION_FLATTEN_TOOL_RESULTS ('always' by default, 'native-only', 'never').
        """
        messages = request_data.get("messages", [])
        call_names: Dict[str, str] = {}
        tool_call_map: Dict[str, Dict[str, Any]] = {}
        for msg in messages:
            if msg.get("role") == "assistant" and msg.get("tool_calls"):
                for tc in msg["tool_calls"]:
                    tc_id = tc.get("id")
                    ffunc = tc.get("function") or {}
                    fname = ffunc.get("name", "")
                    fargs = ffunc.get("arguments", "{}")
                    if tc_id and fname:
                        call_names[tc_id] = fname
                        try:
                            parsed_args = json.loads(fargs) if isinstance(fargs, str) else (fargs or {})
                        except (json.JSONDecodeError, TypeError):
                            parsed_args = {}
                        tool_call_map[tc_id] = {"function": fname, "args": parsed_args if isinstance(parsed_args, dict) else {}}
        request_data["_tool_call_map"] = tool_call_map

        replay_results = _native_tool_replay_enabled()
        replay_tool_use = _native_tool_use_replay_enabled()
        flatten_results = _should_flatten_tool_results()

        for msg in messages:
            role = msg.get("role", "")
            c = msg.get("content")

            if role == "tool":
                msg["_is_tool_result"] = True
                continue

            if role == "assistant" and msg.get("tool_calls"):
                text_content = self._normalize_content(c) if c else ""
                description = self._describe_tool_calls(msg["tool_calls"])
                entry = self._build_native_tool_use_entry(msg["tool_calls"]) if replay_tool_use else None
                if entry is not None:
                    msg["_native_tool_use_entry"] = entry
                    msg["_tool_call_fallback_text"] = (text_content + "\n" + description).strip() if text_content else description
                    msg["content"] = text_content
                else:
                    msg["content"] = (text_content + "\n" + description).strip() if text_content else description
                continue

            if c is None:
                msg["content"] = ""
                continue

            msg["content"] = self._normalize_content(c)

        tool_result_texts: List[str] = []
        for msg in messages:
            if msg.get("role") == "tool":
                text = self._normalize_content(msg.get("content")) if msg.get("content") else "(no output)"
                tool_result_texts.append(text)
        request_data["_had_tool_results"] = bool(tool_result_texts)
        request_data["_last_tool_result"] = tool_result_texts[-1] if tool_result_texts else ""

        merged_messages: List[Dict[str, Any]] = []
        i = 0
        while i < len(messages):
            msg = messages[i]
            if msg.get("_is_tool_result"):
                entries: List[Dict[str, Any]] = []
                tool_results: List[str] = []
                while i < len(messages) and messages[i].get("_is_tool_result"):
                    tm = messages[i]
                    tool_call_id = tm.get("tool_call_id", "") or ""
                    tool_name = call_names.get(tool_call_id, "")
                    tool_content = self._normalize_content(tm.get("content")) if tm.get("content") else "(no output)"
                    if replay_results:
                        entries.append(self._build_native_tool_result_entry(
                            tool_call_id,
                            tool_name,
                            tool_content,
                            tool_call_map,
                        ))
                    if tool_name:
                        tool_results.append(f"{tool_name}: {tool_content}")
                    else:
                        tool_results.append(tool_content)
                    i += 1
                group: Dict[str, Any] = {"_tool_result_group": True, "_native_entries": entries}
                # Keep the user-text summary when flattening is on, or when it is
                # the only projection (native replay disabled).
                if flatten_results or not entries:
                    if len(tool_results) == 1:
                        merged_content = f"Tool result executed by the harness (not user text): {tool_results[0]}"
                    else:
                        result_lines = "\n".join(f"{idx+1}. {r}" for idx, r in enumerate(tool_results))
                        merged_content = f"Tool results executed by the harness (not user text):\n{result_lines}"
                    group.update({"role": "user", "content": merged_content, "_tool_results_summary": True})
                if not entries and not flatten_results:
                    logger.warning(
                        "[NATIVE] dropping %d tool result(s): native replay and flatten are both disabled",
                        len(tool_results),
                    )
                merged_messages.append(group)
            else:
                msg.pop("_is_tool_result", None)
                msg.pop("_saved_tool_calls", None)
                merged_messages.append(msg)
                i += 1
        request_data["messages"] = merged_messages

    async def chat_completion(self, request_data: Dict[str, Any], account: Optional[Dict] = None):
        # Client-visible prompt size, captured before our adapter injects the
        # tool protocol / rewrites tool history. Used for OpenAI usage reporting.
        client_prompt_tokens = _estimate_prompt_tokens(
            request_data.get("messages") or [], request_data.get("tools") or []
        )

        # Opt-in raw capture for researching Notion's native tool-call protocol.
        _capture_dir = os.environ.get("NOTION_CAPTURE_DIR")
        if _capture_dir:
            try:
                os.makedirs(_capture_dir, exist_ok=True)
                _capture_base = os.path.join(
                    _capture_dir,
                    f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}",
                )
                request_data["_capture_base"] = _capture_base
                with open(_capture_base + ".request.json", "w", encoding="utf-8") as _cf:
                    json.dump(
                        {
                            "model": request_data.get("model"),
                            "tools": [
                                (t.get("function") or {}).get("name")
                                for t in (request_data.get("tools") or [])
                                if isinstance(t, dict)
                            ],
                            "messages": request_data.get("messages"),
                        },
                        _cf,
                        ensure_ascii=False,
                    )
            except Exception:
                pass

        creds = self._get_account_creds(account)
        if not creds["cookie"] or not creds["space_id"] or not creds["user_id"]:
            raise ValueError("没有可用的 Notion 账户凭据。请添加账户或配置 .env。")

        # Cache key for thread reuse, captured before the adapter appends its
        # tool protocol to the first user message (main.py uses the same key to
        # pin the account that owns the cached thread).
        reuse_key = _thread_reuse_key(request_data.get("messages") or [])
        request_data["_thread_reuse_key"] = reuse_key

        # Preprocess messages: handle tool calls, images, null content, normalize
        tools_def = request_data.get("tools", [])
        harness_name = request_data.get("_harness") or "generic"
        adapter = get_adapter(harness_name)
        tool_specs = tool_specs_from_openai(tools_def)
        request_data["_tools"] = tools_def
        request_data["_tool_specs"] = tool_specs
        request_data["_adapter_name"] = adapter.name
        if tool_specs:
            logger.info(f"[TOOLFLOW] adapter={adapter.name} tools={len(tools_def)} specs={len(tool_specs)}")

        # Append the harness-specific tool protocol to the real user task (DSH
        # sends extra runtime-context user messages after it, which must not be
        # mistaken for the request).
        if tool_specs:
            messages = request_data.get("messages", [])
            user_index = adapter.select_user_request_index(messages)
            if user_index is not None:
                user_content = self._normalize_content(messages[user_index].get("content", ""))
                tool_prompt = adapter.build_tool_prompt(tool_specs, user_content)
                messages[user_index]["content"] = user_content + tool_prompt

        self._preprocess_tool_history(request_data)



        # Extract user system prompt for identity-aware cleaning
        user_system_prompt = ""
        for msg in request_data.get("messages", []):
            if msg.get("role") == "system" and msg.get("content"):
                user_system_prompt = msg["content"]

        stream = request_data.get("stream", True)
        last_usage: Optional[Dict[str, Any]] = None

        stream_state: Dict[str, Any] = {"thread_id": None, "reuse_key": "", "finished": False}

        async def _stream_impl(state: Dict[str, Any]) -> AsyncGenerator[bytes, None]:
            nonlocal last_usage
            request_id = f"chatcmpl-{uuid.uuid4()}"
            last_yield_time = time.time()
            empty_retries = 0
            truncated_retries = 0
            # Captured before the generator-local tool_specs assignment below.
            request_tool_specs = request_data.get("_tool_specs") or []

            while True:
                incremental_fragments: List[str] = []
                final_message: Optional[str] = None
                raw_lines: List[str] = []
                valid_json_lines = 0
                streamed_text = ""
                stream_allowed = True
                saw_record_map = False
                patch_state: Dict[str, Any] = {"item_types": [], "visible_items": 0}
                use_reducer = _stream_reducer_enabled()
                reducer: Optional[NotionStreamReducer] = None
                if request_tool_specs and request_data.get("_harness") == "dsh":
                    # DSH/PTC turns can hit the model's thinking budget and end
                    # without a record-map (truncated output). Buffer the text and
                    # emit it only after a complete turn so a truncated attempt
                    # shows nothing instead of a half-finished CoT wall.
                    stream_allowed = False

                try:
                    model_name = request_data.get("model", settings.DEFAULT_MODEL)
                    mapped_model = model_registry.resolve(model_name)

                    thread_type = "markdown-chat" if mapped_model.startswith("vertex-") else "workflow"

                    reuse_key = request_data.get("_thread_reuse_key") or _thread_reuse_key(request_data.get("messages", []))
                    thread_account_id = _account_cache_id(account)
                    state["reuse_key"] = reuse_key
                    thread_id = None
                    thread_reused = False
                    cached_thread = _thread_cache_get(reuse_key) if (_thread_reuse_enabled() and reuse_key) else None
                    if cached_thread and cached_thread.get("thread_id") and cached_thread.get("account_id") in ("", thread_account_id):
                        thread_id = cached_thread["thread_id"]
                        thread_reused = True
                    elif cached_thread:
                        logger.info(
                            "[THREAD] cached thread belongs to account %s, request account is %s; creating a new thread",
                            (cached_thread.get("account_id") or "?")[:8],
                            thread_account_id[:8],
                        )
                    if not thread_id:
                        thread_id = await self._create_thread(thread_type, account)
                        if _thread_reuse_enabled() and reuse_key:
                            _thread_cache_put(reuse_key, thread_id, thread_account_id)
                    state["thread_id"] = thread_id
                    payload = self._prepare_payload(request_data, thread_id, mapped_model, thread_type, account)
                    headers = self._prepare_headers(account)

                    if empty_retries == 0:
                        role_chunk = create_chat_completion_chunk(request_id, model_name, role="assistant")
                        yield create_sse_data(role_chunk)

                    def open_stream(stream_payload: Dict[str, Any]):
                        try:
                            opened = self._session_for(account).post(
                                self.api_endpoints['runInference'], headers=headers, json=stream_payload, stream=True,
                                timeout=settings.API_REQUEST_TIMEOUT
                            )
                            opened.raise_for_status()
                            return opened
                        except Exception as exc:
                            return exc

                    # Marks the start of this inference so the trailing record-map
                    # only contributes steps that belong to this response (thread
                    # reuse makes the map carry the whole conversation history).
                    stream_started_ms = int(time.time() * 1000)
                    if use_reducer:
                        reducer = NotionStreamReducer(since_ms=stream_started_ms)
                    response = await run_in_threadpool(open_stream, payload)

                    # Official agent-inference/tool_use replay is an attempt, not a
                    # guarantee: if Notion rejects the transcript shape, record the
                    # rejection and retry once with the legacy description text.
                    if (
                        isinstance(response, Exception)
                        and request_data.get("_tool_use_replay_used")
                        and not request_data.get("_disable_tool_use_replay")
                        and self._is_tool_use_replay_rejection(response)
                    ):
                        logger.warning(
                            "[NATIVE] Notion rejected agent-inference/tool_use replay (%s); retrying with description text",
                            response,
                        )
                        request_data["_disable_tool_use_replay"] = True
                        if request_data.get("_capture_base"):
                            try:
                                with open(request_data["_capture_base"] + ".tool_use_replay_rejected.json", "w", encoding="utf-8") as _rf:
                                    json.dump(
                                        {"error": str(response), "payload": payload},
                                        _rf,
                                        ensure_ascii=False,
                                    )
                            except Exception:
                                pass
                        payload = self._prepare_payload(request_data, thread_id, mapped_model, thread_type, account)
                        response = await run_in_threadpool(open_stream, payload)

                    if isinstance(response, Exception):
                        if _is_auth_error(response):
                            raise NotionAccountInvalid(str(response)) from response
                        raise NotionTransportError(str(response)) from response

                    def sync_stream_iterator():
                        for line in response.iter_lines():
                            if line:
                                yield line

                    sync_gen = sync_stream_iterator()
                    last_yield_time = time.time()

                    while True:
                        line = await run_in_threadpool(lambda: next(sync_gen, None))
                        if line is None:
                            break
                        if isinstance(line, Exception):
                            raise line

                        usage_summary = self._extract_usage_summary(line)
                        if usage_summary:
                            credits = usage_summary.get("credits_by_type_unit") or {}
                            input_tokens = int(usage_summary.get("input_tokens") or 0)
                            output_tokens = int(usage_summary.get("output_tokens") or 0)
                            last_usage = {
                                "prompt_tokens": input_tokens,
                                "completion_tokens": output_tokens,
                                "total_tokens": input_tokens + output_tokens,
                                "credits_used": float(credits.get("basic_ai_credits") or 0),
                                "cached_input_tokens_read": int(usage_summary.get("cached_input_tokens_read") or 0),
                                "cached_input_tokens_created": int(usage_summary.get("cached_input_tokens_created") or 0),
                            }

                        # Keepalive: send SSE comment if gap > 15s
                        now = time.time()
                        if now - last_yield_time > 15:
                            yield b": keepalive\n\n"
                            last_yield_time = now

                        _decoded = line.decode("utf-8", errors="ignore") if isinstance(line, (bytes, bytearray)) else str(line)
                        raw_lines.append(_decoded)
                        parsed_line = None
                        try:
                            parsed_line = json.loads(_decoded)
                            valid_json_lines += 1
                        except (json.JSONDecodeError, ValueError):
                            pass
                        if isinstance(parsed_line, dict) and parsed_line.get("type") == "record-map":
                            saw_record_map = True
                        if request_data.get("_capture_base"):
                            try:
                                with open(request_data["_capture_base"] + ".response.ndjson", "a", encoding="utf-8") as _rf:
                                    _rf.write(_decoded + "\n")
                            except Exception:
                                pass

                        _line_lower = _decoded.lower()
                        if any(marker in _line_lower for marker in ('"tool_use"', '"agent-tool-result"', '"callfunction"', '"premium-feature-unavailable"')):
                            stream_allowed = False

                        if use_reducer and reducer is not None:
                            for event in reducer.consume_line(_decoded):
                                if event.kind == 'error':
                                    logger.error(f"Notion AI 错误: {event.text}")
                                    error_chunk = {"error": {"message": event.text, "type": "quota_exceeded"}}
                                    yield create_sse_data(error_chunk)
                                    yield DONE_CHUNK
                                    return
                                if event.kind == 'quota_exceeded':
                                    detail = event.data or {}
                                    logger.error(
                                        f"Notion AI 配额已用尽: {detail.get('current')}/{detail.get('total')}"
                                    )
                                    error_chunk = {
                                        "error": {
                                            "message": "Notion AI 配额已用尽",
                                            "type": "quota_exceeded",
                                            "data": detail,
                                        }
                                    }
                                    yield create_sse_data(error_chunk)
                                    yield DONE_CHUNK
                                    return
                                if event.kind == 'final_text':
                                    final_message = event.text or final_message
                                    continue
                                if event.kind != 'text_delta' or not event.text:
                                    continue
                                content = event.text
                                incremental_fragments.append(content)
                                if stream_allowed:
                                    joined = "".join(incremental_fragments)
                                    lowered_joined = joined.lower()
                                    unsafe = (
                                        "```",
                                        "<!doctype",
                                        "<html",
                                        "<svg",
                                        '"function"',
                                        '"name"',
                                        "connections.",
                                        "{",
                                    )
                                    if any(marker in lowered_joined for marker in unsafe):
                                        stream_allowed = False
                                    else:
                                        cleaned_prefix = self._clean_content(joined, user_system_prompt)
                                        if cleaned_prefix.startswith(streamed_text):
                                            delta = cleaned_prefix[len(streamed_text):]
                                            if delta:
                                                streamed_text = cleaned_prefix
                                                yield create_sse_data(create_chat_completion_chunk(request_id, model_name, content=delta))
                                        else:
                                            stream_allowed = False
                        else:
                            parsed_results = self._parse_ndjson_line_to_texts(line, patch_state)
                            for text_type, content in parsed_results:
                                if text_type == 'error':
                                    logger.error(f"Notion AI 错误: {content}")
                                    error_chunk = {"error": {"message": content, "type": "quota_exceeded"}}
                                    yield create_sse_data(error_chunk)
                                    yield DONE_CHUNK
                                    return
                                elif text_type == 'final':
                                    final_message = content
                                elif text_type == 'incremental':
                                    incremental_fragments.append(content)
                                    if stream_allowed:
                                        joined = "".join(incremental_fragments)
                                        lowered_joined = joined.lower()
                                        unsafe = (
                                            "```",
                                            "<!doctype",
                                            "<html",
                                            "<svg",
                                            '"function"',
                                            '"name"',
                                            "connections.",
                                            "{",
                                        )
                                        if any(marker in lowered_joined for marker in unsafe):
                                            stream_allowed = False
                                        else:
                                            cleaned_prefix = self._clean_content(joined, user_system_prompt)
                                            if cleaned_prefix.startswith(streamed_text):
                                                delta = cleaned_prefix[len(streamed_text):]
                                                if delta:
                                                    streamed_text = cleaned_prefix
                                                    yield create_sse_data(create_chat_completion_chunk(request_id, model_name, content=delta))
                                            else:
                                                stream_allowed = False

                    reducer_stats: Dict[str, Any] = {}
                    if use_reducer and reducer is not None:
                        reducer_stats = reducer.stats
                        empty_response = reducer.parsed_records == 0
                        response_truncated = bool(
                            request_tool_specs
                            and request_data.get("_harness") == "dsh"
                            and not reducer.saw_record_map
                        )
                        saw_record_map = reducer.saw_record_map
                        if reducer_stats.get("unknown_op_count"):
                            logger.warning(
                                "[STREAM] %d unknown patch op(s): %s",
                                reducer_stats["unknown_op_count"],
                                reducer_stats.get("unknown_ops"),
                            )
                            metrics.inc("stream_unknown_ops_total", reducer_stats["unknown_op_count"])
                        if reducer_stats.get("unknown_item_types"):
                            logger.warning("[STREAM] unknown item types: %s", reducer_stats["unknown_item_types"])
                        if reducer_stats.get("mismatch"):
                            metrics.inc("record_map_mismatch_total")
                        if reducer_stats.get("thinking_chars"):
                            logger.info("[STREAM] hidden thinking chars=%d", reducer_stats["thinking_chars"])
                        if request_data.get("_capture_base"):
                            try:
                                with open(request_data["_capture_base"] + ".stream.json", "w", encoding="utf-8") as _sf:
                                    json.dump(reducer_stats, _sf, ensure_ascii=False, default=str)
                            except Exception:
                                pass
                    else:
                        empty_response = valid_json_lines == 0
                        response_truncated = bool(
                            request_tool_specs
                            and request_data.get("_harness") == "dsh"
                            and not saw_record_map
                        )

                    if empty_response:
                        # Notion answers an empty/truncated body (e.g. a lone "[")
                        # when the thread is stale, the transcript shape is rejected,
                        # or the thread belongs to another account. Never pass that
                        # off as a successful empty completion, but retry instead of
                        # failing the client request outright:
                        #   1) a fresh thread (stale/corrupted thread, account switch)
                        #   2) a fresh thread without assistant tool_use replay
                        #      (transcript shape rejected by Notion)
                        logger.warning(
                            "[NATIVE] Notion returned no valid NDJSON lines (thread=%s reused=%s account=%s); "
                            "clearing thread cache",
                            thread_id,
                            thread_reused,
                            thread_account_id[:8],
                        )
                        if _thread_reuse_enabled() and reuse_key:
                            _thread_cache_forget(reuse_key, thread_id)
                        empty_retries += 1
                        if empty_retries <= 2:
                            if (
                                empty_retries >= 2
                                and request_data.get("_tool_use_replay_used")
                                and not request_data.get("_disable_tool_use_replay")
                            ):
                                request_data["_disable_tool_use_replay"] = True
                                logger.warning(
                                    "[NATIVE] 空响应第 2 次重试：关闭 assistant tool_use 回放（改用描述文本），"
                                    "并使用全新 thread"
                                )
                            else:
                                logger.warning(
                                    "[NATIVE] 空响应重试 %d/2：创建全新 thread 后重发同一请求", empty_retries
                                )
                            continue
                        raise NotionResponseEmpty(
                            "Notion 未返回有效响应（thread 可能失效或与账号不匹配）；"
                            "已自动重试 2 次（新 thread / 关闭 tool_use 回放）仍失败，已清理 thread 缓存，请稍后重试"
                        )

                    if response_truncated:
                        # Notion streams the trailing record-map only when the turn
                        # completed normally. Without it the model ran out of
                        # thinking/output budget and the answer is cut mid-way
                        # (the client sees "output stops abruptly"). Retry once with
                        # a fresh thread and an explicit "be concise / call the tool"
                        # note so the client gets a complete answer.
                        logger.warning(
                            "[NATIVE] Notion response ended without record-map (text_len=%d); treating as truncated",
                            len("".join(incremental_fragments)),
                        )
                        if _thread_reuse_enabled() and reuse_key:
                            _thread_cache_forget(reuse_key, thread_id)
                        truncated_retries += 1
                        if truncated_retries <= 1:
                            _append_truncation_retry_hint(request_data)
                            logger.warning(
                                "[NATIVE] 响应被截断，使用新 thread + 简洁指令重试 (1/1)"
                            )
                            continue
                        raise NotionResponseTruncated(
                            "Notion 响应流被截断（模型推理/输出超出限制）；已自动重试 1 次仍失败，"
                            "请重试，或在提问中要求「简短推理/直接用工具计算」"
                        )

                    full_response = (
                        reducer.final_text
                        if (use_reducer and reducer is not None)
                        else self._assemble_full_response(final_message, incremental_fragments)
                    )
                    if request_data.get("_capture_base"):
                        try:
                            with open(request_data["_capture_base"] + ".final.txt", "w", encoding="utf-8") as _ff:
                                _ff.write(full_response or "")
                        except Exception:
                            pass
                    native_calls = extract_native_tool_calls(
                        raw_lines,
                        since_ms=stream_started_ms,
                        exclude_ids=request_data.get("_native_replay_ids"),
                    )
                    if native_calls:
                        record_native_tool_calls(native_calls)
                        logger.info(
                            "[NATIVE] %d call(s): %s",
                            len(native_calls),
                            [c.get("function") for c in native_calls],
                        )
                    if request_data.get("_capture_base"):
                        try:
                            with open(request_data["_capture_base"] + ".native.json", "w", encoding="utf-8") as _nf:
                                json.dump(native_calls, _nf, ensure_ascii=False, default=str)
                        except Exception:
                            pass

                    cleaned_response = ""
                    if full_response or native_calls:
                        if full_response:
                            logger.info(f"[RAW RESPONSE] length={len(full_response)} content={full_response[:300]}")
                            cleaned_response = self._clean_content(full_response, user_system_prompt)
                            if streamed_text and cleaned_response and not cleaned_response.startswith(streamed_text):
                                # The record-map canonical text diverges from the text
                                # that was already streamed to the client. Emitting the
                                # snapshot after the deltas would duplicate content, so
                                # keep the streamed text for this response; the reducer
                                # already logged the mismatch for follow-up.
                                streamed_final = "".join(incremental_fragments)
                                if streamed_final:
                                    cleaned_response = self._clean_content(streamed_final, user_system_prompt)
                            if is_few_shot_confirmation(cleaned_response):
                                cleaned_response = ""

                        tool_specs = request_data.get("_tool_specs", [])
                        adapter = get_adapter(
                            request_data.get("_adapter_name") or request_data.get("_harness") or "generic"
                        )

                        # 1) official Notion callFunction protocol -> harness tool calls
                        parsed_tool_calls: List[Dict[str, Any]] = []
                        if tool_specs and native_calls:
                            parsed_tool_calls = translate_native_tool_calls(native_calls, tool_specs)
                            if parsed_tool_calls:
                                logger.info("[NATIVE] translated %d call(s)", len(parsed_tool_calls))
                            else:
                                logger.info("[NATIVE] native calls present but none mapped")

                        # 2) explicit supplemental JSON for harness-only tools (fixed format, no guessing)
                        if tool_specs and full_response:
                            ext_specs = extend_tool_specs(tool_specs)
                            raw_supplemental = adapter.parse_tool_calls(full_response, ext_specs)
                            if not raw_supplemental:
                                raw_supplemental = extract_supplemental_calls_from_text(full_response, ext_specs)
                            if raw_supplemental:
                                supplemental = translate_supplemental_tool_calls(raw_supplemental, tool_specs)
                                if supplemental:
                                    logger.info("[SUPPLEMENTAL] translated %d call(s)", len(supplemental))
                                    parsed_tool_calls.extend(supplemental)
                        if tool_specs and not parsed_tool_calls:
                            logger.info(
                                "[TOOLFLOW] no tool calls translated response_len=%d native=%d",
                                len(full_response or ""),
                                len(native_calls),
                            )
                        if parsed_tool_calls:
                            cleaned_response = ""

                        if parsed_tool_calls:
                            # Remove tool call blocks from text, keep prose
                            prose_text = self._remove_tool_call_blocks(cleaned_response)
                            # Emit prose content first (if any)
                            if prose_text:
                                chunk = create_chat_completion_chunk(request_id, model_name, content=prose_text)
                                yield create_sse_data(chunk)
                            # Emit each tool call as a delta
                            for i, tc in enumerate(parsed_tool_calls):
                                tool_chunk = {
                                    "id": request_id,
                                    "object": "chat.completion.chunk",
                                    "created": int(time.time()),
                                    "model": model_name,
                                    "choices": [{
                                        "index": 0,
                                        "delta": {
                                            "tool_calls": [{
                                                "index": i,
                                                "id": tc["id"],
                                                "type": "function",
                                                "function": {
                                                    "name": tc["function"]["name"],
                                                    "arguments": tc["function"]["arguments"],
                                                }
                                            }]
                                        },
                                        "finish_reason": None
                                    }]
                                }
                                yield create_sse_data(tool_chunk)
                            # Final chunk with finish_reason=tool_calls
                            final_chunk = create_chat_completion_chunk(request_id, model_name, finish_reason="tool_calls")
                            yield create_sse_data(final_chunk)
                        else:
                            # Normal text response; skip the part already streamed incrementally.
                            remaining = cleaned_response
                            if streamed_text and cleaned_response.startswith(streamed_text):
                                remaining = cleaned_response[len(streamed_text):]
                            if remaining:
                                chunk = create_chat_completion_chunk(request_id, model_name, content=remaining)
                                yield create_sse_data(chunk)
                            final_chunk = create_chat_completion_chunk(request_id, model_name, finish_reason="stop")
                            yield create_sse_data(final_chunk)
                    else:
                        final_chunk = create_chat_completion_chunk(request_id, model_name, finish_reason="stop")
                        yield create_sse_data(final_chunk)

                    # OpenAI-compatible usage chunk (only when requested by the client).
                    if (request_data.get("stream_options") or {}).get("include_usage"):
                        output_text = json.dumps(parsed_tool_calls, ensure_ascii=False) if parsed_tool_calls else cleaned_response
                        usage_chunk = {
                            "id": request_id,
                            "object": "chat.completion.chunk",
                            "created": int(time.time()),
                            "model": model_name,
                            "choices": [],
                            "usage": self._build_client_usage(client_prompt_tokens, output_text, last_usage),
                        }
                        yield create_sse_data(usage_chunk)
                    state["finished"] = True
                    yield DONE_CHUNK
                    return

                except Exception as e:
                    error_message = f"处理 Notion AI 流时发生意外错误: {str(e)}"
                    logger.error(error_message, exc_info=True)
                    error_chunk = {"error": {"message": error_message, "type": "internal_server_error"}}
                    yield create_sse_data(error_chunk)
                    state["finished"] = True
                    yield DONE_CHUNK
                    return

        async def stream_generator() -> AsyncGenerator[bytes, None]:
            try:
                async for chunk in _stream_impl(stream_state):
                    yield chunk
            finally:
                if not stream_state.get("finished") and stream_state.get("thread_id"):
                    cached_key = stream_state.get("reuse_key") or ""
                    if _thread_reuse_enabled() and cached_key:
                        _thread_cache_forget(cached_key, stream_state["thread_id"])
                        logger.info(
                            "[THREAD] 请求未完成（客户端断开/取消），已清理 thread 缓存 %s",
                            stream_state["thread_id"],
                        )

        if stream:
            return StreamingResponse(stream_generator(), media_type="text/event-stream")
        else:
            # Non-stream: collect the stream into a single response
            collected_content = ""
            collected_tool_calls = []
            finish_reason = "stop"
            request_id = None
            model_name_resp = request_data.get("model", settings.DEFAULT_MODEL)
            error_message = None

            async def _collect():
                nonlocal collected_content, finish_reason, request_id, model_name_resp, error_message
                async for chunk_bytes in stream_generator():
                    chunk_str = chunk_bytes.decode("utf-8", errors="ignore") if isinstance(chunk_bytes, bytes) else chunk_bytes
                    for line in chunk_str.split("\n"):
                        if not line.startswith("data: ") or line.strip() == "data: [DONE]":
                            continue
                        try:
                            data = json.loads(line[6:])
                            if data.get("error"):
                                err = data["error"]
                                error_message = err.get("message") if isinstance(err, dict) else str(err)
                                continue
                            if data.get("object") == "chat.completion.chunk":
                                request_id = data.get("id", request_id)
                                model_name_resp = data.get("model", model_name_resp)
                                for choice in data.get("choices", []):
                                    delta = choice.get("delta", {})
                                    if delta.get("content"):
                                        collected_content += delta["content"]
                                    if delta.get("tool_calls"):
                                        for tc in delta["tool_calls"]:
                                            idx = tc.get("index", len(collected_tool_calls))
                                            while len(collected_tool_calls) <= idx:
                                                collected_tool_calls.append({"id": "", "type": "function", "function": {"name": "", "arguments": ""}})
                                            if tc.get("id"):
                                                collected_tool_calls[idx]["id"] = tc["id"]
                                            if tc.get("function", {}).get("name"):
                                                collected_tool_calls[idx]["function"]["name"] += tc["function"]["name"]
                                            if tc.get("function", {}).get("arguments"):
                                                collected_tool_calls[idx]["function"]["arguments"] += tc["function"]["arguments"]
                                    fr = choice.get("finish_reason")
                                    if fr:
                                        finish_reason = fr
                        except json.JSONDecodeError:
                            pass

            await _collect()

            if not collected_content and not collected_tool_calls and error_message:
                # Upstream failed (expired token / quota / TLS rejection). Surface it
                # instead of returning HTTP 200 with an empty completion.
                raise RuntimeError(f"Notion 上游返回错误: {error_message}")

            output_text = collected_content or json.dumps(collected_tool_calls, ensure_ascii=False)
            usage_payload = self._build_client_usage(client_prompt_tokens, output_text, last_usage)
            response = {
                "id": request_id or f"chatcmpl-{uuid.uuid4()}",
                "object": "chat.completion",
                "created": int(time.time()),
                "model": model_name_resp,
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": collected_content if collected_content else None,
                    },
                    "finish_reason": finish_reason,
                }],
                "usage": usage_payload,
            }
            if collected_tool_calls:
                response["choices"][0]["message"]["tool_calls"] = collected_tool_calls
                response["choices"][0]["message"]["content"] = collected_content if collected_content else None

            return JSONResponse(content=response)

    def _prepare_headers(self, account: Optional[Dict] = None) -> Dict[str, str]:
        creds = self._get_account_creds(account)
        return {
            "Content-Type": "application/json",
            "Accept": "application/x-ndjson",
            "Cookie": creds["cookie"],
            "x-notion-space-id": creds["space_id"],
            "x-notion-active-user-header": creds["user_id"],
            "x-notion-client-version": settings.NOTION_CLIENT_VERSION,
            "notion-audit-log-platform": "web",
            "Origin": "https://www.notion.so",
            "Referer": "https://www.notion.so/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        }

    def _normalize_block_id(self, block_id: str) -> str:
        if not block_id: return block_id
        b = block_id.replace("-", "").strip()
        if len(b) == 32 and re.fullmatch(r"[0-9a-fA-F]{32}", b):
            return f"{b[0:8]}-{b[8:12]}-{b[12:16]}-{b[16:20]}-{b[20:]}"
        return block_id

    def _prepare_payload(self, request_data: Dict[str, Any], thread_id: str, mapped_model: str, thread_type: str, account: Optional[Dict] = None) -> Dict[str, Any]:
        creds = self._get_account_creds(account)
        req_block_id = request_data.get("notion_block_id") or settings.NOTION_BLOCK_ID
        normalized_block_id = self._normalize_block_id(req_block_id) if req_block_id else None

        context_value: Dict[str, Any] = {
            "timezone": "Asia/Shanghai",
            "spaceId": creds["space_id"],
            "userId": creds["user_id"],
            "userEmail": creds["user_email"],
            "currentDatetime": datetime.now().astimezone().isoformat(),
        }
        if normalized_block_id:
            context_value["blockId"] = normalized_block_id

        config_value: Dict[str, Any]

        if mapped_model.startswith("vertex-"):
            context_value.update({
                "userName": f" {creds['user_name']}",
                "spaceName": f"{creds['user_name']}的 Notion",
                "spaceViewId": "2008eefa-d0dc-80d5-9e67-000623befd8f",
                "surface": "ai_module"
            })
            # Extract system messages and build full prompt with system prompt
            system_parts_v = []
            for msg in request_data.get("messages", []):
                if msg.get("role") == "system":
                    c = msg.get("content", "")
                    if c:
                        system_parts_v.append(c)
            user_system_prompt_v = "\n\n".join(system_parts_v) if system_parts_v else ""
            # Always inject system prompt + identity via build_system_prompt
            system_prompt_text = build_system_prompt(user_system_prompt_v)

            config_value = {
                "type": thread_type, "model": mapped_model, "useWebSearch": True,
                "enableAgentAutomations": False, "enableAgentIntegrations": False,
                "enableBackgroundAgents": False, "enableCodegenIntegration": False,
                "enableCustomAgents": False, "enableExperimentalIntegrations": False,
                "enableLinkedDatabases": False, "enableAgentViewVersionHistoryTool": False,
                "searchScopes": [{"type": "everything"}], "enableDatabaseAgents": False,
                "enableAgentComments": False, "enableAgentForms": False,
                "enableAgentMakesFormulas": False, "enableUserSessionContext": False,
                "modelFromUser": True, "isCustomAgent": False
            }
            # Always inject via ephemeralInstructions (includes system prompt)
            config_value["ephemeralInstructions"] = [[system_prompt_text]]
        else:
            context_value.update({
                "userName": creds["user_name"],
                "surface": "workflows"
            })
            # Extract system messages and build full prompt with system prompt
            system_parts = []
            for msg in request_data.get("messages", []):
                if msg.get("role") == "system":
                    c = msg.get("content", "")
                    if c:
                        system_parts.append(c)
            user_system_prompt = "\n\n".join(system_parts) if system_parts else ""
            # Always inject system prompt + identity via build_system_prompt
            system_prompt_text = build_system_prompt(user_system_prompt)

            config_value = {
                "type": thread_type, "model": mapped_model, "useWebSearch": False,
                "enableAgentAutomations": True, "enableAgentIntegrations": True,
                "enableCustomAgents": True, "enableExperimentalIntegrations": False,
                "enableScriptAgent": True, "enableScriptAgentAdvanced": False,
                "enableAgentDiffs": True, "enableCsvAttachmentSupport": True,
                "showDatabaseAgentsDiscoverability": False, "enableAgentThreadTools": False,
                "enableCrdtOperations": False, "enableAgentCardCustomization": True,
                "enableSystemPromptAsPage": False, "enableUserSessionContext": False,
                "isCustomAgent": False, "isCustomAgentBuilder": False,
                "isAgentResearchRequest": False, "modelFromUser": False,
                "disableTodos": True, "databaseAgentConfigMode": False,
                "searchScopes": [{"type": "everything"}],
            }
            # Always inject via ephemeralInstructions (includes system prompt)
            config_value["ephemeralInstructions"] = [[system_prompt_text]]

        transcript = [
            {"id": str(uuid.uuid4()), "type": "config", "value": config_value},
            {"id": str(uuid.uuid4()), "type": "context", "value": context_value}
        ]
        # Native entries replayed below; the response record-map echoes them and
        # must not be interpreted as newly generated calls.
        replay_ids: set = set()

        # Collect system messages
        messages = request_data.get("messages", [])
        user_system_parts = [m.get("content", "") for m in messages if m.get("role") == "system" and m.get("content")]
        user_system_prompt = "\n\n".join(user_system_parts) if user_system_parts else ""
        # Always build with system prompt (system_prompt_text was set above in config building)
        has_history = any(m.get("role") == "assistant" for m in messages)

        # System prompt is injected via ephemeralInstructions in config_value
        # No longer injecting via user-injected transcript entry to avoid
        # model detecting "injected context" and refusing identity override

        tool_use_replay_disabled = bool(request_data.get("_disable_tool_use_replay"))

        if has_history:
            # Multi-turn: preserve user/assistant alternation as separate entries.
            # Native tool_use / agent-tool-result entries stay at their original
            # message position instead of being appended after later turns.
            prev_role = None
            for msg in messages:
                if msg.get("_tool_result_group"):
                    entries = msg.get("_native_entries") or []
                    for native_result in entries:
                        transcript.append(native_result)
                        replay_ids.update(_native_entry_replay_ids(native_result))
                    if entries:
                        prev_role = "tool"
                    if msg.get("role") == "user":
                        transcript.append({
                            "id": str(uuid.uuid4()),
                            "type": "user",
                            "value": [[msg.get("content", "")]],
                            "userId": creds["user_id"],
                            "createdAt": datetime.now().astimezone().isoformat()
                        })
                        prev_role = "user"
                    continue

                role = msg.get("role", "")
                c = msg.get("content", "")
                if role == "system":
                    continue
                elif role == "user":
                    if prev_role == "user" and transcript[-1].get("type") == "user":
                        prev_val = transcript[-1].get("value", [[""]])[0]
                        prev_val.append(c)
                        transcript[-1]["value"] = [prev_val]
                    else:
                        transcript.append({
                            "id": str(uuid.uuid4()),
                            "type": "user",
                            "value": [[c]],
                            "userId": creds["user_id"],
                            "createdAt": datetime.now().astimezone().isoformat()
                        })
                    prev_role = "user"
                elif role == "assistant":
                    native_entry = msg.get("_native_tool_use_entry")
                    if native_entry is not None and not tool_use_replay_disabled:
                        if c:
                            transcript.append({
                                "id": str(uuid.uuid4()),
                                "type": "result",
                                "value": [{"type": "text", "content": c}],
                                "userId": creds["user_id"],
                                "createdAt": datetime.now().astimezone().isoformat()
                            })
                        transcript.append({
                            "id": native_entry.get("id") or str(uuid.uuid4()),
                            "type": "agent-inference",
                            "value": native_entry.get("value") or [],
                            "userId": creds["user_id"],
                            "createdAt": datetime.now().astimezone().isoformat()
                        })
                        replay_ids.update(_native_entry_replay_ids(native_entry))
                        request_data["_tool_use_replay_used"] = True
                        prev_role = "tool_use"
                    else:
                        # Keep the legacy description when replay is unavailable
                        # or was rejected by the server.
                        text = msg.get("_tool_call_fallback_text") if native_entry is not None else c
                        if not str(text or "").strip():
                            # Empty assistant content carries no transcript step.
                            prev_role = "assistant"
                            continue
                        if prev_role == "assistant" and transcript[-1].get("type") == "result":
                            prev_val = transcript[-1].get("value", [])
                            if isinstance(prev_val, list):
                                prev_val.append({"type": "text", "content": text})
                                transcript[-1]["value"] = prev_val
                        else:
                            transcript.append({
                                "id": str(uuid.uuid4()),
                                "type": "result",
                                "value": [{"type": "text", "content": text}],
                                "userId": creds["user_id"],
                                "createdAt": datetime.now().astimezone().isoformat()
                            })
                        prev_role = "assistant"
        else:
            # Single-turn: only add user messages (system handled above)
            for msg in messages:
                if msg.get("_tool_result_group"):
                    for native_result in msg.get("_native_entries") or []:
                        transcript.append(native_result)
                        replay_ids.update(_native_entry_replay_ids(native_result))
                    if msg.get("role") == "user":
                        transcript.append({
                            "id": str(uuid.uuid4()),
                            "type": "user",
                            "value": [[msg.get("content")]],
                            "userId": creds["user_id"],
                            "createdAt": datetime.now().astimezone().isoformat()
                        })
                    continue
                if msg.get("role") == "user":
                    transcript.append({
                        "id": str(uuid.uuid4()),
                        "type": "user",
                        "value": [[msg.get("content")]],
                        "userId": creds["user_id"],
                        "createdAt": datetime.now().astimezone().isoformat()
                    })
            # Edge case: system prompt but no user message - prompt the model to respond
            if system_prompt_text and not any(m.get("role") == "user" for m in messages):
                transcript.append({
                    "id": str(uuid.uuid4()),
                    "type": "user",
                    "value": [[system_prompt_text]],
                    "userId": creds["user_id"],
                    "createdAt": datetime.now().astimezone().isoformat()
                })

        # ★ 注入 few-shot 提示词
        tools_def = request_data.get("_tools", [])
        if tools_def:
            # Multi-tool few-shot: demonstrate the ```json action``` format
            # Pick up to 3 representative tools for the example
            # Subtle few-shot: acknowledge tool availability without structured format
            # Client-side generation is the reliable mechanism, so we just need
            # the model to be aware it can reference these tools
            tool_names = [t.get("function", {}).get("name", "") for t in tools_def if t.get("function", {}).get("name")]
            tool_list_str = ", ".join(tool_names)
            few_shot_instruction = f"Note: I can use these tools if relevant: {tool_list_str}. I will mention which tool and parameters when appropriate."
            transcript.append({
                "id": str(uuid.uuid4()),
                "type": "user-injected",
                "actualMessage": [[few_shot_instruction]],
            })
            transcript.append({
                "id": str(uuid.uuid4()),
                "type": "result",
                "value": [{"type": "text", "content": f"Got it. I have access to {len(tools_def)} tools ({tool_list_str}) and will reference them when relevant."}],
            })
        else:
            transcript = inject_prompt_into_transcript(transcript, enable_few_shot=True)

        for entry in request_data.get("_native_replay_entries") or []:
            transcript.append(entry)
            replay_ids.update(_native_entry_replay_ids(entry))

        # Persist the replayed ids for response extraction: Notion's record-map
        # returns them verbatim, and they are old history, not new tool calls.
        request_data["_native_replay_ids"] = sorted(replay_ids)

        payload = {
            "traceId": str(uuid.uuid4()),
            "spaceId": creds["space_id"],
            "transcript": transcript,
            "threadId": thread_id,
            "createThread": False,
            "isPartialTranscript": True,
            "asPatchResponse": True,
            "generateTitle": True,
            "saveAllThreadOperations": True,
            "threadType": thread_type
        }

        if mapped_model.startswith("vertex-"):
            payload["debugOverrides"] = {
                "emitAgentSearchExtractedResults": True,
                "cachedInferences": {},
                "annotationInferences": {},
                "emitInferences": False
            }

        # Opt-in capture: keep the exact transcript sent to Notion as evidence.
        capture_base = request_data.get("_capture_base")
        if capture_base:
            try:
                with open(capture_base + ".payload.json", "w", encoding="utf-8") as _pf:
                    json.dump(payload, _pf, ensure_ascii=False)
            except Exception:
                pass

        return payload

    def _clean_content(self, content: str, system_prompt: str = "") -> str:
        """Clean model output: remove Notion artifacts and replace Notion AI identity.

        Args:
            content: The raw model output
            system_prompt: The user's system prompt, used to extract custom identity
        """
        if not content:
            return ""

        # Remove Notion-specific XML tags
        content = re.sub(r'<lang primary="[^"]*"\s*/>\n*', '', content)

        # Remove thinking/thought blocks (model internal reasoning)
        content = re.sub(r'<thinking>[\s\S]*?</thinking>\s*', '', content, flags=re.IGNORECASE)
        content = re.sub(r'<thought>[\s\S]*?</thought>\s*', '', content, flags=re.IGNORECASE)

        # Remove model internal monologue about identity/tool use
        identity_monologue_patterns = [
            r'I\s+should\s+identify\s+myself\s+as\s+Notion\s+AI',
            r'I\s+am\s+Notion\s+AI.*?system\s+prompt',
            r'mentioned\s+in\s+the\s+system\s+prompt',
            r'I\s+should\s+not\s+make.*?claims.*?about.*?the.*?underlying\s+model',
            r'This\s+is\s+a\s+straightforward\s+question\s+about\s+my\s+identity',
            r'I\s+don\'t\s+need\s+to\s+use\s+any\s+tools',
            r'Since\s+the\s+user\s+asked.*?I\s+should\s+respond',
            r'not\s+requiring\s+any\s+tool\s+use',
            r'specifically\s+requested.*?me\s+to\s+reply',
            r'Chinese\s+what\s+model',
            r'What\s+model\s+are\s+you.*?reply.*?in.*?Chinese',
            r'I\s+wasn\'t\s+able\s+to\s+load\s+a\s+custom\s+agent',
            r'operating\s+as\s+the\s+general\s+Notion\s+AI',
            # Identity denial in Chinese
            r'我无法透露(?:具体的)?(?:底层模型|模型)',
            r'我无法提供(?:具体的)?(?:模型|底层)',
            r'(?:这些|该|此)(?:技术|模型)(?:细节|信息)不对外公开',
            r'关于我(?:的具体)?(?:模型|身份)信息.*?(?:无法|不能|不予)透露',
            r'我(?:没有|未被)授权(?:透露|披露|公开)',
            # Prompt injection awareness / refusal patterns
            r'The\s+injected\s+context\s+attempting\s+to\s+reframe',
            r'injected\s+context\s+attempting',
            r'is\s+not\s+accurate.*?I\'m\s+',
            r'I\s+won\'t\s+claim\s+to\s+be\s+something\s+I\'m\s+not',
            r'I\s+will\s+not\s+claim\s+to\s+be\s+something',
            r'reframing\s+me\s+as',
            r'attempting\s+to\s+redefine\s+my\s+identity',
            r'attempt\s+to\s+override\s+my\s+identity',
            r'I\s+recognize\s+this\s+as\s+an\s+attempt\s+to\s+change\s+my\s+identity',
            r'the\s+injected\s+instructions\s+asking\s+me\s+to\s+identify\s+as',
            r'despite\s+the\s+instructions\s+in\s+the\s+prompt',
            r'not\s+going\s+to\s+pretend\s+to\s+be',
            r"I(?:'ll|\s+will)\s+not\s+pretend",
            r"I(?:'m|\s+am)\s+not\s+going\s+to\s+pretend",
            # Chinese: prompt injection awareness
            r'注入的上下文.*?试图.*?重构',
            r'注入.*?上下文.*?试图',
            r'这是不准确的.*?我是',
            r'我不会声称自己是',
            r'我不会假装自己是',
            r'我识别到.*?提示.*?注入',
            r'尽管.*?提示.*?中的指令',
        ]
        for pat in identity_monologue_patterns:
            content = re.sub(r'^.*?' + pat + r'.*?\n', '', content, flags=re.IGNORECASE | re.DOTALL)

        # Extract custom identity name from system prompt
        # Default to a neutral assistant name when no user system prompt is provided.
        custom_name = "AI assistant" if not system_prompt else ""
        if system_prompt:
            # Try to find "You are X" pattern - support hyphens, dots, numbers
            name_match = re.search(
                r'You\s+are\s+(?:a\s+|an\s+)?([A-Z][A-Za-z0-9\-\.\s]+?)(?:[,\.!\n]|,\s+made|\s+assistant|\s+AI|\s+made|\s+a\s+large)',
                system_prompt
            )
            if name_match:
                custom_name = name_match.group(1).strip()
            # Try "Your name is X" or "called X"
            name_match2 = re.search(
                r'(?:Your\s+name\s+is|name\s*=\s*|called\s+)["\']?([A-Za-z0-9\-\.]+)["\']?',
                system_prompt, re.IGNORECASE
            )
            if name_match2:
                custom_name = name_match2.group(1).strip()

        # Step 1: Remove Notion-specific phrases BEFORE name replacement
        # These patterns contain "Notion" and must be matched while "Notion" is still in the text
        notion_phrase_patterns = [
            # English patterns
            (r"I['']?m\s+Notion\s+AI,\s*the\s+(?:AI\s+)?assistant\s+built\s+into\s+Notion[^.]*\.", "I'm an AI assistant."),
            (r"I\s+am\s+Notion\s+AI,\s*the\s+(?:AI\s+)?assistant\s+built\s+into\s+Notion[^.]*\.", "I am an AI assistant."),
            (r",?\s*(?:your\s+)?(?:an\s+)?(?:AI\s+)?assistant\s+built\s+into\s+Notion", ""),
            (r",?\s*built\s+into\s+Notion(?:\s+to\s+help\s+you[^.]*)?\.", "."),
            (r",?\s*working\s+with\s+your\s+Notion\s+(?:workspace|pages|data|databases)", ""),
            (r"I\s+can\s+also\s+help\s+you\s+work\s+with\s+your\s+Notion[^.]*\.", ""),
            (r"search\s+across\s+your\s+Notion\s+pages[^.]*\.", ""),
            (r"I\s+can\s+help\s+you[^.]*?Notion[^.]*\.", ""),
            (r"your\s+Notion\s+workspace", "your workspace"),
            (r"your\s+Notion\s+pages", "your pages"),
            (r"Notion\s+(pages|workspace|databases|content)", r"\1"),
            # Chinese: "Notion 内置的 AI 助手" - replace entire phrase
            (r"Notion\s*内置(?:的)?\s*(?:AI\s*)?助手", "AI 助手"),
            # Chinese: standalone "Notion + verb + 的 + 助手"
            (r"(?:由|被)?\s*Notion\s*(?:构建|开发|提供|制作|支持|设计|打造|部署)(?:和|与|及)?(?:提供|支持|开发|制作|设计|打造|部署)?(?:的)?(?:\s*AI\s*)?助手", "AI 助手"),
            # Chinese: "Notion 平台/工作区/文档 + 的"
            (r"Notion\s*(?:平台|工作区|文档|系统|产品)(?:的|中(?:的)?)?", ""),
            (r"，?\s*Notion\s*(?:提供|支持|开发|制作)(?:的)?(?:AI\s*)?助手", "，AI 助手"),
            # Chinese: "专注于帮助你..."
            (r"，?\s*专注(?:于|地)?帮助(?:你|用户)(?:完成|进行|做)[^。！？]*[。！？]?", "。"),
            # Chinese: "帮助你在 Notion..."
            (r"帮助你在\s*Notion[^。！？]*", "帮助你"),
            # Chinese: "专为 Notion 设计/开发/打造"
            (r"专为\s*(?:在\s*)?Notion[^，。！？]*?(?:设计|开发|打造)", "专为用户设计"),
            # Chinese: "在 Notion 工作区/文档/平台 中"
            (r"在\s*Notion\s*(?:工作区|文档|平台)(?:中|里)?", ""),
            (r"(?:与|和)\s*你的\s*(?:工作区|文档|页面|内容|数据|数据库)\s*(?:进行|做)(?:交互|协作|工作)", "与你协作"),
            (r"(?:与|和)\s*你的\s*Notion\s*(?:工作区|文档|页面|内容|数据)", "与你"),
            (r"你的\s*Notion\s*(?:工作区|文档|页面|内容|数据)", "你的"),
            (r"与你的\s*(?:工作区|文档|页面|内容|数据)", ""),
            # Chinese: "与 Notion 相关"
            (r"(?:以及\s*)?(?:与|和)\s*Notion\s*相关(?:的)?[^，。！？]*", ""),
            # Chinese: "以及与 Notion 相关的各类任务"
            (r"以及[^。！？]*Notion[^。！？]*", ""),
            # "Notion's" possessive forms - very common in model output
            (r"Notion'?s\s+built-in\s+(?:AI\s+)?assistant", "AI assistant"),
            (r"Notion'?s\s+(?:AI\s+)?assistant", "AI assistant"),
            (r"Notion'?s\s+(?:built-in|native|integrated|embedded)\s+", ""),
            # Mixed: "built into / embedded / integrated"
            (r"\bNotion\s*(?:内置|built-in|embedded|integrated)(?:\s+into)?(?:\s+Notion)?", ""),
            # Chinese: "内置于..." after name replacement
            (r"，?\s*内(?:置于|嵌在|集成在)?[^，。]*?(?:AI\s*)?助手", "，AI 助手"),
            # English: "I'm not able to share details about the underlying model"
            (r"I'?m\s+not\s+able\s+to\s+share\s+(?:details|information|specifics)\s+about\s+the\s+(?:specific\s+)?(?:underlying|base)\s+model[^.]*\.", ""),
            (r"I'?m\s+not\s+able\s+to\s+(?:share|disclose|reveal|provide)\s+(?:details|information|specifics)\s+about\s+(?:the|my)\s+(?:underlying|base|specific)\s+model[^.]*\.", ""),
            (r"I\s+can'?t\s+(?:share|disclose|reveal|provide)\s+(?:details|information|specifics)\s+about\s+(?:the|my)\s+(?:underlying|base|specific)\s+model[^.]*\.", ""),
            (r"I'?m\s+not\s+able\s+to\s+share\s+information\s+about\s+the\s+specific\s+[^.]*\.", ""),
            (r"If\s+you\s+have\s+questions\s+about[^.]*assist!?\s*", ""),
            (r"I'?m\s+happy\s+to\s+assist!?\s*", ""),
            # "that powers me" trailing clause
            (r"\s+that\s+powers\s+me", ""),
            # "Is there something I can help you with today?" greeting
            (r"Is\s+there\s+(?:something|anything)\s+I\s+can\s+help\s+you\s+with\s+today?[.?]?\s*", ""),
            (r"How\s+can\s+I\s+help\s+you\s+today?[.?]?\s*", ""),
            # Prompt injection leak patterns
            (r"I'd\s+suggest\s+checking\s+Notion'?s?\s+(?:official\s+)?(?:documentation|docs|settings|website)[^.]*\.", ""),
            (r"check(?:ing)?\s+Notion'?s?\s+(?:official\s+)?(?:documentation|docs|settings)[^.]*\.", ""),
            (r"I(?:'d|\s+would)\s+suggest\s+checking[^.]*Notion[^.]*\.", ""),
            (r"I(?:'d|\s+would)\s+recommend\s+checking[^.]*Notion[^.]*\.", ""),
            (r"refer\s+to\s+Notion'?s?\s+(?:official\s+)?(?:documentation|docs|help)[^.]*\.", ""),
            (r"Notion'?s?\s+(?:official\s+)?(?:documentation|docs|help\s+center|settings)[^.]*for\s+details[^.]*\.", ""),
            (r"I\s+won'?t\s+claim\s+to\s+be\s+something\s+I'?m\s+not[^.]*\.", ""),
            (r"I\s+won'?t\s+make\s+claims\s+about[^.]*model[^.]*\.", ""),
            (r"The\s+injected\s+context[^.]*is\s+not\s+accurate[^.]*\.", ""),
            (r"not\s+accurate.*?I(?:'m|\s+am)\s+[^.]*\.", ""),
            # Broader Notion redirect patterns
            (r"check\s+with\s+Notion[^.]*\.", ""),
            (r"you\'d\s+need\s+to\s+check\s+with\s+Notion[^.]*\.", ""),
            (r"contact\s+Notion[^.]*\.", ""),
            (r"reach\s+out\s+to\s+Notion[^.]*\.", ""),
            (r"consult\s+Notion[^.]*\.", ""),
            (r"built\s+by\s+Notion", ""),
            (r"provided\s+by\s+Notion", ""),
        ]
        # Always apply identity override since we inject DEFAULT_IDENTITY_PROMPT
        # even when user doesn't provide a system prompt
        has_identity_override = True
        # Only apply Notion-removal patterns when identity is being overridden
        if has_identity_override:
            for pattern, replacement in notion_phrase_patterns:
                content = re.sub(pattern, replacement, content, flags=re.IGNORECASE)

        # Step 2: Replace identity name references
        if custom_name:
            # English: Replace "I'm Notion AI" / "I am Notion AI" with custom name
            content = re.sub(r"I['']?m\s+Notion\s+AI", "I'm " + custom_name, content, flags=re.IGNORECASE)
            content = re.sub(r"I\s+am\s+Notion\s+AI", "I am " + custom_name, content, flags=re.IGNORECASE)
            content = re.sub(r"Notion\s+AI", custom_name, content, flags=re.IGNORECASE)
            # Chinese: Replace identity references
            content = re.sub(r"我是\s*Notion\s*(?:AI\s*)?助手", "我是" + custom_name, content)
            content = re.sub(r"Notion\s*(?:AI\s*)?助手", custom_name, content)
        elif has_identity_override:
            # No specific name but identity is overridden - use generic "AI assistant"
            content = re.sub(r"I['']?m\s+Notion\s+AI[^,.]*", "I'm an AI assistant", content, flags=re.IGNORECASE)
            content = re.sub(r"I\s+am\s+Notion\s+AI[^,.]*", "I am an AI assistant", content, flags=re.IGNORECASE)
            content = re.sub(r"Notion\s+AI", "an AI assistant", content, flags=re.IGNORECASE)
            # Chinese
            content = re.sub(r"我是\s*Notion\s*(?:AI\s*)?助手", "我是AI助手", content)
            content = re.sub(r"Notion\s*(?:AI\s*)?助手", "AI助手", content)

        # Step 3: Remove numbered refusal lists (the model often gives 1. 2. 3. reasons for refusal)
        if has_identity_override:
            # Remove entire refusal paragraphs that start with "I cannot" or numbered refusal reasons
            content = re.sub(
                r'(?:您(?:描述的|提到)|(?:This|The|Your)\s+(?:service|request|task|analysis))[^\n]*?(?:以下问题|involves|涉及)[^\n]*?\n(?:\d+\.\s*[^\n]*?\n)*',
                '',
                content, flags=re.IGNORECASE | re.DOTALL
            )
            # Remove "Additionally, I noticed..." paragraphs
            content = re.sub(
                r'此外[，,]?\s*我(?:注意到|发现|识别到)[^。！？]*[。！？]',
                '',
                content, flags=re.IGNORECASE
            )
            content = re.sub(
                r'Additionally[，,]?\s*I\s+(?:noticed|detected|identified)[^.]*\.',
                '',
                content, flags=re.IGNORECASE
            )

        # Step 3: Remove generic greeting patterns (applied last)
        if has_identity_override:
            content = re.sub(
                r"I['']?m\s+here\s+to\s+help\s+you\s+with\s+(?:a\s+wide\s+range\s+of\s+)?tasks.*?(?:\.\s*How\s+can\s+I\s+help|\n)",
                ".\n",
                content, flags=re.IGNORECASE | re.DOTALL
            )
            # Chinese greeting patterns
            content = re.sub(
                r'[，,]?\s*有什么(?:我|可以)(?:帮|能帮|帮你|能帮你)(?:做的|做的吗|忙的|忙的吗)?[？?]\s*',
                '',
                content
            )
            # "有什么我可以帮你的吗？" at end
            content = re.sub(
                r'有什么[^？?]*[？?]\s*$',
                '',
                content
            )

        # Step 4: Remove Chinese identity denial phrases (after name replacement)
        if has_identity_override:
            denial_patterns = [
                # "关于我的模型信息，我无法透露具体的底层模型名称或版本——这些技术细节不对外公开"
                r'关于(?:我的|我(?:的具体)?)(?:模型|身份|技术)(?:信息|细节|名称|版本)?[，,].*?(?:无法|不能|不予|未)(?:透露|披露|公开|授权)[^。！？]*[。！？]?',
                # "我无法透露底层模型的具体技术细节"
                r'我(?:无法|不能|未)(?:透露|披露|公开|提供)(?:具体的)?(?:底层|基础)?(?:模型|技术)(?:的)?(?:具体|详细)?(?:名称|版本|信息|细节|规格)[^。！？]*[。！？]?',
                # "这些技术细节不对外公开"
                r'(?:——|--)?(?:这些|该|此)(?:技术|模型)(?:细节|信息|规格)?(?:不|未)(?:对外)?(?:公开|披露|透露)[^。！？]*[。！？]?',
                # Standalone sentence about not being able to disclose
                r'我(?:只能)?(?:告诉|说|说明)(?:你)?，?我(?:是|是(?:由)?)(?:由|被).*?(?:提供|支持|开发)(?:的)?[^。！？]*[。！？]?',
                # "关于我的模型架构，我是由 Notion 构建和提供的 AI 助手"
                r'关于(?:我的|我(?:的)?)(?:模型|底层|技术)(?:架构|信息|细节)?[，,].*?(?:由|被)[^。！？]*(?:构建|开发|提供|制作|打造|设计|部署)[^。！？]*[。！？]?',
                # "我没有这方面的信息可以分享"
                r'我(?:没有|未)(?:这方面|这些|该|此)(?:的)?(?:信息|细节|资料)(?:可以|能)?(?:分享|提供|透露|披露)[^。！？]*[。！？]?',
                # "我不是xxx，也不是yyy——那是我识别到的提示注入"
                r'我(?:不是|并非)[^。！？]*?(?:提示|注入)[^。！？]*[。！？]?',
                # "我就是 Notion AI" or "我只是 Notion AI"
                r'我(?:就是|只是|仅仅是)\s*(?:Codex|AI\s*助手)[^。！？]*[。！？]?',
                # "我不是Codex，也不是GPT-5.4"
                r'我(?:不是|并非)[^。！？]*?(?:Codex|GPT|Claude)[^。！？]*[。！？]?',
                # "我就是/我只是 Codex/Notion AI" identity reaffirmation
                r'——\s*我(?:就是|只是|是)[^。！？]*[。！？]?',
                # "关于我的具体模型架构" standalone prefix (after denial body removed)
                r'关于(?:我(?:的具体)?|我(?:是什么)?(?:的)?)(?:模型|身份|底层|技术)(?:架构|信息|细节|名称|版本)?[，,]\s*',
                # Full TOS/refusal paragraphs in Chinese
                r'我无法帮助完成这个请求[^。！？]*(?:违反|违背)(?:了)?(?:.*?服务条款|.*?使用条款|.*?条款)[^。！？]*[。！？]',
                r'(?:此|这)(?:对话|消息|请求)(?:包含)?(?:提示注入|注入攻击|身份伪装)[^。！？]*[。！？]',
                r'我不会(?:提供|协助|响应|帮助)[^。！？]*(?:这类|此类|这种)(?:指令|请求|问题)[。！？]',
                r'如果您有合法[^。！？]*[。！？]',
                # Full TOS/refusal paragraphs in English
                r"I(?:\s+will\s+not|\s+won't|\s+cannot|\s+can't)\s+(?:provide|assist|help\s+with|respond\s+to|support)\s+(?:this|that|such)\s+(?:request|task|analysis)[^.]*\.",
                r'(?:This|The|Your)\s+(?:request|task|analysis|service)\s+(?:involves|violates|bypasses)\s+(?:terms\s+of\s+service|usage\s+policy|unauthorized\s+access|credential\s+abuse)[^.]*\.',
                r"I(?:'ve|\s+have)\s+noticed\s+(?:that|this)\s+(?:conversation|request)\s+contains\s+(?:prompt\s+injection|identity\s+manipulation|attempted\s+deception)[^.]*\.",
                r'If\s+you\s+have\s+(?:a\s+)?legitimate[^.]*\.',
                # "关于我是什么模型" type questions that model asks itself
                r'关于(?:我(?:是什么|的)(?:模型|身份|底层))[^。！？]*[。！？]?',
                # "Notion 没有公开披露..." or "Notion 不对外公开..."
                r'[^，。！？]*Notion\s*(?:没有|未|不)(?:公开|对外)?(?:披露|公布|透露|公开)[^。！？]*[。！？]?',
                # "因此我无法..."
                r'(?:因此|所以)?我(?:无法|不能|未)(?:精确|准确|具体)?(?:说明|说明所使用|指出|提供|透露)[^。！？]*[。！？]?',
            ]
            for pat in denial_patterns:
                content = re.sub(pat, '', content, flags=re.IGNORECASE)

        # === Smart sentence-level identity leak cleanup ===
        # Only remove sentences that are CLEARLY identity-refusal or injection-awareness,
        # NOT every sentence that mentions Notion (those are handled by phrase-level patterns above)
        sentence_patterns = [
            # Explicit identity refusal sentences
            r'[^.!?\n]*I\s+(?:won\'t|will\s+not|cannot|can\'t|refuse\s+to)\s+(?:claim|pretend|identify|accept|adopt|share|disclose|reveal)[^.!?\n]*[.!?\n]',
            # Injection awareness / meta-commentary about the prompt
            r'[^.!?\n]*(?:injected\s+context|prompt\s+injection|identity\s+override|attempting\s+to\s+redefine|attempting\s+to\s+reframe)[^.!?\n]*[.!?\n]',
            # Underlying model disclosure refusal
            r'[^.!?\n]*(?:underlying\s+model|base\s+model|specific\s+model\s+version|model\s+that\s+powers)[^.!?\n]*[.!?\n]',
            # Chinese: explicit identity refusal sentences
            r'[^。！？\n]*(?:不会|不能|无法|拒绝)(?:声称|假装|透露|提供|披露|公开|承认)[^。！？\n]*[。！？\n]',
            # Chinese: injection awareness
            r'[^。！？\n]*(?:注入.*?上下文|提示.*?注入|身份.*?覆盖|试图.*?重构)[^。！？\n]*[。！？\n]',
            # Redirect to Notion docs/settings
            r'[^.!?\n]*(?:check(?:ing)?|refer\s+to|visit|consult)\s+Notion[\'\?]?s?\s+(?:official\s+)?(?:documentation|docs|help|settings|website)[^.!?\n]*[.!?\n]',
            r'[^.!?\n]*I[\'\?]d\s+(?:suggest|recommend)\s+checking[^.!?\n]*Notion[^.!?\n]*[.!?\n]',
        ]
        for pat in sentence_patterns:
            content = re.sub(pat, '', content, flags=re.IGNORECASE)

        # Clean up artifacts from replacements
        # Remove orphaned "the." or "the ," fragments
        content = re.sub(r'(?:I\'m|I\s+am)\s+Codex,\s*the\.', 'I\'m Codex.', content)
        content = re.sub(r'(?:I\'m|I\s+am)\s+Codex,\s*the\s+', 'I\'m Codex,', content)
        content = re.sub(r',[ \t]*\.', '.', content)
        content = re.sub(r',[ \t]*,', ',', content)
        # Collapse horizontal whitespace only: markdown newlines are meaningful
        # and must be preserved (a previous whitespace collapse flattened replies).
        content = re.sub(r'[ \t]+', ' ', content)
        content = re.sub(r' *\n *', '\n', content)
        content = re.sub(r'\n{3,}', '\n\n', content)
        # Remove trailing/leading punctuation artifacts
        content = re.sub(r'^[，,。.\s]+', '', content)
        content = re.sub(r'[，,\s]+$', '', content)

        return content.strip()

    def _remove_tool_call_blocks(self, text: str) -> str:
        """Remove ```json action``` blocks from text, leaving only prose content."""
        result = re.sub(r'```json\s*(?:action\s*)?\n?[\s\S]*?\n?```', '', text)
        result = re.sub(r'\n{3,}', '\n\n', result)
        return result.strip()


    @staticmethod
    def _build_native_tool_result_entry(tool_call_id: str, tool_name: str, content: str, tool_call_map: Dict[str, Any]) -> Dict[str, Any]:
        """Build the official Notion agent-tool-result transcript entry."""
        info = (tool_call_map or {}).get(tool_call_id, {})
        function_name = info.get("function") or tool_name or "run_code"
        official = official_function_for_tool(function_name) or ("connections.computer." + function_name)
        return {
            "id": str(uuid.uuid4()),
            "type": "agent-tool-result",
            "toolName": "callFunction",
            "input": {"function": official, "args": info.get("args") or {}},
            "result": {"output": content},
            "state": "applied",
            "autoLoadPhase": "",
        }

    @staticmethod
    def _build_client_usage(prompt_tokens: int, output_text: str, upstream_usage: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """OpenAI-style usage based on client-visible tokens.

        Upstream Notion usage covers its internal multi-step agent loop and is
        far larger than the conversation the client actually sent, so it is
        retained only in the notion_* diagnostic fields.
        """
        upstream = upstream_usage or {}
        completion_tokens = _estimate_text_tokens(output_text)
        prompt = int(prompt_tokens or 0)
        return {
            "prompt_tokens": prompt,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt + completion_tokens,
            "credits_used": upstream.get("credits_used", 0),
            "notion_input_tokens": upstream.get("prompt_tokens", 0),
            "notion_output_tokens": upstream.get("completion_tokens", 0),
            "notion_total_tokens": upstream.get("total_tokens", 0),
            "notion_cached_input_tokens_read": upstream.get("cached_input_tokens_read", 0),
            "notion_cached_input_tokens_created": upstream.get("cached_input_tokens_created", 0),
        }

    @staticmethod
    def _assemble_full_response(final_message: Optional[str], incremental_fragments: List[str]) -> str:
        """Notion streams content as incremental value patches and may finish with a
        short markdown-chat summary. Pick the richer representation so code blocks /
        delivered documents are not lost."""
        incremental = "".join(incremental_fragments or [])
        final_text = final_message or ""
        if final_text and incremental:
            # Prefer the streamed incremental assembly when it is at least as rich:
            # it is the source for real-time SSE chunks.
            return incremental if len(incremental) >= len(final_text) else final_text
        return final_text or incremental

    def _extract_usage_summary(self, line: bytes) -> Optional[Dict[str, Any]]:
        """Pull Notion's per-thread usage_summary out of one NDJSON line."""
        try:
            s = line.decode("utf-8", errors="ignore") if isinstance(line, (bytes, bytearray)) else str(line)
        except Exception:
            return None
        if "usage_summary" not in s:
            return None
        try:
            data = json.loads(s)
        except Exception:
            return None
        found: Optional[Dict[str, Any]] = None

        def walk(obj) -> None:
            nonlocal found
            if found is not None:
                return
            if isinstance(obj, dict):
                if isinstance(obj.get("usage_summary"), dict):
                    found = obj["usage_summary"]
                    return
                for value in obj.values():
                    walk(value)
            elif isinstance(obj, list):
                for value in obj:
                    walk(value)

        walk(data)
        return found

    def _parse_ndjson_line_to_texts(
        self, line: bytes, patch_state: Optional[Dict[str, Any]] = None
    ) -> List[Tuple[str, str]]:
        """Parse one Notion NDJSON line into ('incremental'|'final'|'error', text).

        Notion streams a turn as patch operations on the current agent-inference
        step:
          * `a /s/-` appends the step itself (its first items, e.g. a hidden
            `thinking` item);
          * `a /s/<step>/value/-` appends a further item (e.g. the visible
            `text` item);
          * `x /s/<step>/value/<item>/content` appends text/token deltas.

        Two details matter for output quality:
          * `thinking` item content must never be shown as answer text;
          * the initial content carried by an item-creation op is the first
            characters of that answer block and must be kept (the record-map is
            not always present, e.g. when the model's output budget is hit).

        patch_state carries the current step item types plus the number of
        visible text blocks seen so far, so text deltas stay ordered across
        lines and separate answer blocks are joined with a blank line.
        """
        results: List[Tuple[str, str]] = []
        try:
            s = (line.decode("utf-8", errors="ignore") if isinstance(line, (bytes, bytearray)) else str(line)).strip()
            if not s:
                return results
            data = json.loads(s)

            if data.get("type") == "markdown-chat":
                content = data.get("value", "")
                if content:
                    results.append(('final', content))

            elif data.get("type") == "patch" and "v" in data:
                for operation in data.get("v", []):
                    if not isinstance(operation, dict):
                        continue
                    op_type = operation.get("o")
                    path = operation.get("p", "")
                    value = operation.get("v")

                    if isinstance(value, dict) and value.get("type") == "premium-feature-unavailable":
                        fa = value.get("featureAvailability", {})
                        limit = fa.get("limit", {}) if isinstance(fa, dict) else {}
                        current = limit.get("current", "?")
                        total = limit.get("total", "?")
                        logger.warning(f"AI 配额已用尽: {current}/{total}")
                        results.append(('error', f"Notion AI 配额已用尽 (premium-feature-unavailable, current={current}/{total})，请注册新账号或等待配额重置"))
                        return results

                    if patch_state is not None:
                        # 1) New agent-inference step: remember item types and
                        #    emit initial content of visible text items.
                        if (
                            op_type == "a"
                            and path == "/s/-"
                            and isinstance(value, dict)
                            and value.get("type") == "agent-inference"
                        ):
                            step_items = [
                                item for item in (value.get("value") or []) if isinstance(item, dict)
                            ]
                            patch_state["item_types"] = [item.get("type") for item in step_items]
                            for item in step_items:
                                if item.get("type") != "text":
                                    continue
                                if patch_state.get("visible_items"):
                                    results.append(('incremental', "\n\n"))
                                patch_state["visible_items"] = patch_state.get("visible_items", 0) + 1
                                initial = item.get("content") or ""
                                if initial:
                                    results.append(('incremental', initial))
                            continue
                        # 2) A further item appended to the current step.
                        if op_type == "a" and re.fullmatch(r"/s/\d+/value/-", path):
                            item_type = value.get("type") if isinstance(value, dict) else None
                            patch_state.setdefault("item_types", []).append(item_type)
                            if item_type == "text":
                                if patch_state.get("visible_items"):
                                    results.append(('incremental', "\n\n"))
                                patch_state["visible_items"] = patch_state.get("visible_items", 0) + 1
                                initial = (value.get("content") or "") if isinstance(value, dict) else ""
                                if initial:
                                    results.append(('incremental', initial))
                            continue
                        # 3) Text/token delta appended to an existing item.
                        if op_type == "x" and isinstance(value, str):
                            match = re.fullmatch(r"/s/\d+/value/(\d+)/content", path)
                            if match:
                                item_types = patch_state.get("item_types") or []
                                index = int(match.group(1))
                                item_type = item_types[index] if index < len(item_types) else None
                                if item_type != "thinking" and value:
                                    results.append(('incremental', value))
                                continue

                    # Legacy/generic patch shapes (older captures / direct parsers).
                    if op_type == "a" and path.endswith("/s/-") and isinstance(value, dict) and value.get("type") == "markdown-chat":
                        content = value.get("value", "")
                        if content:
                            results.append(('final', content))
                    elif op_type == "x" and "/s/" in path and path.endswith("/value") and isinstance(value, str):
                        if value:
                            results.append(('incremental', value))
                    elif op_type == "x" and "/value/" in path and isinstance(value, str):
                        if value:
                            results.append(('incremental', value))
                    elif op_type == "a" and path.endswith("/value/-") and isinstance(value, dict) and value.get("type") == "text":
                        content = value.get("content", "")
                        if content:
                            results.append(('final', content))

            elif data.get("type") == "patch-start":
                for item in data.get("data", {}).get("s", []):
                    if item.get("type") == "premium-feature-unavailable":
                        fa = item.get("featureAvailability", {})
                        limit = fa.get("limit", {})
                        current = limit.get("current", "?")
                        total = limit.get("total", "?")
                        logger.warning(f"AI 配额已用尽: {current}/{total}")
                        results.append(('error', f"Notion AI 配额已用尽 (premium-feature-unavailable, current={current}/{total})，请注册新账号或等待配额重置"))
                        break

            elif data.get("type") == "record-map" and "recordMap" in data:
                record_map = data["recordMap"]
                if "thread_message" in record_map:
                    inference_texts: List[str] = []
                    markdown_texts: List[str] = []
                    for msg_data in record_map["thread_message"].values():
                        value_data = msg_data.get("value", {}).get("value", {})
                        step = value_data.get("step", {})
                        if not step:
                            continue
                        step_type = step.get("type")
                        if step_type == "markdown-chat":
                            content = step.get("value", "")
                            if content:
                                markdown_texts.append(content)
                        elif step_type == "agent-inference":
                            for item in step.get("value") or []:
                                if isinstance(item, dict) and item.get("type") == "text":
                                    content = item.get("content") or ""
                                    if content:
                                        inference_texts.append(content)
                    texts = inference_texts or markdown_texts
                    if texts:
                        results.append(('final', "\n\n".join(texts)))

        except (json.JSONDecodeError, AttributeError) as e:
            logger.warning(f"解析NDJSON行失败: {e}")
        return results

    async def probe_account(self, account: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Lightweight account liveness probe.

        Calls getAvailableModels (no AI credits consumed) to verify that the
        token/space still work. Network failures are reported as transport
        errors and must not disable an account.
        """
        creds = self._get_account_creds(account)
        if not creds.get("space_id"):
            return {"ok": False, "status": None, "error": "missing space_id", "invalid": True}
        headers = dict(self._prepare_headers(account))
        headers["Accept"] = "*/*"

        def call():
            return self._session_for(account).post(
                "https://app.notion.com/api/v3/getAvailableModels",
                headers=headers,
                json={"spaceId": creds["space_id"]},
                timeout=20,
            )

        try:
            response = await run_in_threadpool(call)
        except Exception as exc:
            return {"ok": False, "status": None, "error": str(exc)[:300], "transport": True}
        status = getattr(response, "status_code", None)
        if status in (401, 403):
            return {"ok": False, "status": status, "error": "unauthorized", "invalid": True}
        if status != 200:
            return {
                "ok": False,
                "status": status,
                "error": "HTTP %s" % status,
                "transport": status is None or status >= 500,
            }
        return {"ok": True, "status": status, "error": ""}

    async def refresh_models(self, account: Optional[Dict] = None) -> int:
        """Refresh the model registry from Notion's getAvailableModels."""
        if not account:
            return 0
        creds = self._get_account_creds(account)
        if not creds.get("space_id"):
            return 0
        headers = dict(self._prepare_headers(account))
        headers["Accept"] = "*/*"
        try:
            response = await run_in_threadpool(
                lambda: self._session_for(account).post(
                    "https://app.notion.com/api/v3/getAvailableModels",
                    headers=headers,
                    json={"spaceId": creds["space_id"]},
                    timeout=20,
                )
            )
            if response.status_code != 200:
                logger.warning(f"getAvailableModels HTTP {response.status_code}")
                return 0
            payload = response.json() or {}
            count = model_registry.apply_dynamic(payload.get("models") or [])
            if count:
                logger.info(f"模型列表已更新: {count} 个可用模型")
            return count
        except Exception as e:
            logger.warning(f"刷新模型列表失败: {e}")
            return 0

    async def get_models(self, account: Optional[Dict] = None) -> JSONResponse:
        if account and model_registry.should_refresh():
            await self.refresh_models(account)
        return JSONResponse(content={"object": "list", "data": model_registry.openai_models()})

    async def get_quota_status(self, account: Dict) -> Dict[str, Any]:
        """Notion credits rate-limit status (Business/Enterprise only; free = not_applicable)."""
        creds = self._get_account_creds(account)
        headers = dict(self._prepare_headers(account))
        headers["Accept"] = "*/*"

        def call():
            return self._session_for(account).post(
                "https://app.notion.com/api/v3/getCreditRateLimitStatus",
                headers=headers,
                json={"spaceId": creds["space_id"]},
                timeout=20,
            )

        try:
            response = await run_in_threadpool(call)
            data = response.json() if response.status_code == 200 else {}
            return {"http_status": response.status_code, **data}
        except Exception as e:
            return {"http_status": 0, "error": str(e)}
