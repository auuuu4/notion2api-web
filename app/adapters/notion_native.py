"""Notion native tool-call protocol -> harness tool translation.

Notion's agent models do not use OpenAI function calling. The raw NDJSON stream
contains steps like:

    {"type":"agent-inference","value":[
      {"type":"tool_use","id":"toolu_...","name":"callFunction",
       "content":"{\"function\":\"connections.computer.writeFile\",
                   \"args\":{\"path\":\"index.html\",\"content\":\"<!doctype html>...\"}}"}
    ]}

and the server executes them, emitting an agent-tool-result step with the same
input (function + args), a state and an autoLoadPhase for the implicit
context-loading calls (fs.readFiles / fs.readDir / notion.loadUser).

This module only records and translates these explicit native calls. It does
not guess intent from user text: unknown / unmapped native calls are logged and
left to surface as ordinary model output.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from typing import Any, Dict, Iterable, List, Optional

from app.adapters.base import ToolSpec, make_tool_call, validate_args

logger = logging.getLogger(__name__)

# Modules we know how to execute locally in the harness workspace.
_SUPPORTED_PREFIXES = ("connections.computer.",)

# Explicit harness tool -> evaluated connections.computer function mapping.
# Replaying an assistant tool_call as an official agent-inference/tool_use step
# is restricted to this table: unmapped tools keep the legacy description text
# instead of inventing a Notion function name.
_OFFICIAL_COMPUTER_FUNCTIONS: Dict[str, str] = {
    "write": "connections.computer.writeFile",
    "read": "connections.computer.readFile",
    "run_code": "connections.computer.runCode",
    "pwsh": "connections.computer.pwsh",
    "edit": "connections.computer.editFile",
    "glob": "connections.computer.listFiles",
    "grep": "connections.computer.grep",
    "web_fetch": "connections.computer.webFetch",
    "web_search": "connections.computer.webSearch",
    "read_image": "connections.computer.readImage",
}


def official_function_for_tool(tool_name: str) -> Optional[str]:
    """Return the explicit native function for a harness tool name.

    Returns None for unknown tools so callers fall back to description text
    rather than guessing a function name.
    """
    return _OFFICIAL_COMPUTER_FUNCTIONS.get((tool_name or "").strip())


def _tool_use_call(item: Dict[str, Any], index: int) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict) or item.get("type") != "tool_use":
        return None
    if item.get("name") != "callFunction":
        return None
    raw = item.get("content")
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            return None
    elif isinstance(raw, dict):
        parsed = raw
    else:
        return None
    function = parsed.get("function") or parsed.get("name")
    if not isinstance(function, str) or not function.strip():
        return None
    args = parsed.get("args")
    if not isinstance(args, dict):
        args = parsed.get("arguments")
    if not isinstance(args, dict):
        args = {}
    return {
        "id": item.get("id") or "",
        "function": function.strip(),
        "args": args,
        "state": None,
        "autoLoadPhase": "",
        "result": None,
        "startedAt": item.get("startedAt"),
        "index": index,
    }


def _agent_tool_result_call(step: Dict[str, Any], index: int) -> Optional[Dict[str, Any]]:
    if not isinstance(step, dict) or step.get("type") != "agent-tool-result":
        return None
    inp = step.get("input")
    if not isinstance(inp, dict):
        return None
    function = inp.get("function")
    if not isinstance(function, str) or not function.strip():
        return None
    args = inp.get("args")
    if not isinstance(args, dict):
        args = {}
    result = step.get("result")
    output = None
    if isinstance(result, dict):
        output = result.get("output")
    return {
        "id": step.get("toolCallId") or step.get("id") or "",
        "function": function.strip(),
        "args": args,
        "state": step.get("state"),
        "autoLoadPhase": step.get("autoLoadPhase") or "",
        "result": output,
        "startedAt": step.get("startedAt"),
        "index": index,
    }


def _iter_patch_steps(records: List[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
    for rec in records:
        if rec.get("type") != "patch":
            continue
        for op in rec.get("v") or []:
            if not isinstance(op, dict):
                continue
            value = op.get("v")
            if isinstance(value, dict):
                yield value


def _iter_record_map_steps(records: List[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
    for rec in records:
        if rec.get("type") != "record-map":
            continue
        thread_messages = (rec.get("recordMap") or {}).get("thread_message") or {}
        if not isinstance(thread_messages, dict):
            continue
        for message in thread_messages.values():
            step = ((message or {}).get("value") or {}).get("value", {}).get("step") or {}
            if isinstance(step, dict):
                yield step


def _is_current_step(started_at, since_ms):
    """True when a record-map step belongs to the inference that just ran.

    With thread reuse the trailing record-map carries the whole thread history,
    so re-extracting every step would replay old tool calls as new ones. Steps
    started before this request are skipped; steps without a timestamp are kept
    (they are the incremental ones in a first-turn transcript).
    """
    if since_ms is None or started_at is None:
        return True
    try:
        return float(started_at) >= float(since_ms)
    except (TypeError, ValueError):
        return True


def extract_native_tool_calls(lines, since_ms=None, exclude_ids=None):
    """Extract callFunction native tool calls from the NDJSON lines of one response.

    Patch operations are incremental and always belong to this response. The
    final record-map contains the complete thread, so its steps are filtered by
    since_ms (epoch milliseconds when the request was sent) to avoid
    re-emitting tool calls from previous turns.

    exclude_ids are the tool_use / agent-tool-result ids that this request
    replayed into the transcript (previous assistant tool_calls and their
    results). Notion echoes those entries back inside the record-map with their
    original ids and without a startedAt, so without this filter every response
    would hand the harness the whole historical tool list again -- which then
    grows the next replay and multiplies the tool calls turn after turn.
    """
    excluded = {str(call_id) for call_id in (exclude_ids or ()) if call_id}

    def _fresh_call(call):
        if not call:
            return None
        if excluded and str(call.get("id") or "") in excluded:
            return None
        return call
    records: List[Dict[str, Any]] = []
    for line in lines:
        try:
            text = line.decode("utf-8", "ignore") if isinstance(line, (bytes, bytearray)) else str(line)
        except Exception:
            continue
        text = text.strip()
        if not text:
            continue
        try:
            records.append(json.loads(text))
        except (json.JSONDecodeError, ValueError):
            continue

    calls: List[Dict[str, Any]] = []
    for step in _iter_patch_steps(records):
        stype = step.get("type")
        if stype == "agent-inference":
            for item in step.get("value") or []:
                call = _fresh_call(_tool_use_call(item, len(calls)))
                if call:
                    calls.append(call)
        elif stype == "agent-tool-result":
            call = _fresh_call(_agent_tool_result_call(step, len(calls)))
            if call:
                calls.append(call)

    for step in _iter_record_map_steps(records):
        stype = step.get("type")
        if stype == "agent-inference":
            for item in step.get("value") or []:
                call = _tool_use_call(item, len(calls))
                if not call:
                    continue
                started_at = call.get("startedAt")
                if started_at is None:
                    started_at = step.get("startedAt")
                if _is_current_step(started_at, since_ms):
                    call = _fresh_call(call)
                    if call:
                        calls.append(call)
        elif stype == "agent-tool-result":
            if not _is_current_step(step.get("startedAt"), since_ms):
                continue
            call = _fresh_call(_agent_tool_result_call(step, len(calls)))
            if call:
                calls.append(call)

    merged: Dict[str, Dict[str, Any]] = {}
    order: List[str] = []
    for call in calls:
        call.setdefault("state", None)
        call.setdefault("autoLoadPhase", "")
        call.setdefault("result", None)
        call.setdefault("startedAt", None)
        call.setdefault("index", 0)
        digest = hashlib.sha256(
            (call["function"] + json.dumps(call["args"], sort_keys=True, ensure_ascii=False, default=str)).encode("utf-8")
        ).hexdigest()
        key = call["id"] or digest
        if key not in merged:
            merged[key] = call
            order.append(key)
            continue
        existing = merged[key]
        if not existing.get("args") and call.get("args"):
            existing["args"] = call["args"]
        if not existing.get("autoLoadPhase") and call.get("autoLoadPhase"):
            existing["autoLoadPhase"] = call["autoLoadPhase"]
        if existing.get("state") is None and call.get("state") is not None:
            existing["state"] = call["state"]
        if existing.get("result") is None and call.get("result") is not None:
            existing["result"] = call["result"]

    def sort_key(key: str):
        call = merged[key]
        started = call.get("startedAt")
        return (started if isinstance(started, (int, float)) else 10**18, call.get("index") or 0)

    return [merged[key] for key in sorted(order, key=sort_key)]


def _js_string(value: str) -> str:
    return json.dumps(str(value), ensure_ascii=False)


def _validated_call(spec: Optional[ToolSpec], args: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if spec is None:
        return None
    cleaned = validate_args(spec, args)
    if cleaned is None:
        return None
    return make_tool_call(spec, cleaned)


def _spec(specs: List[ToolSpec], name: str) -> Optional[ToolSpec]:
    return next((spec for spec in specs if spec.name == name), None)


def _run_code_call(specs: List[ToolSpec], code: str, description: str) -> Optional[Dict[str, Any]]:
    return _validated_call(_spec(specs, "run_code"), {"code": code, "description": description})


def _write_file_call(specs: List[ToolSpec], path: str, content: str) -> Optional[Dict[str, Any]]:
    direct = _validated_call(_spec(specs, "write"), {"file_path": path, "content": content})
    if direct:
        return direct
    if not _spec(specs, "run_code"):
        return None
    code = "\n".join([
        "const fs = await import('node:fs/promises');",
        "const pathMod = await import('node:path');",
        "const target = pathMod.resolve(" + _js_string(path) + ");",
        "const content = " + _js_string(content) + ";",
        "const dir = pathMod.dirname(target);",
        "if (dir) await fs.mkdir(dir, { recursive: true });",
        "await fs.writeFile(target, content, 'utf8');",
        "console.log('wrote ' + target + ' (' + content.length + ' chars)');",
    ])
    return _run_code_call(specs, code, "Write " + path)


def _read_file_call(specs: List[ToolSpec], path: str) -> Optional[Dict[str, Any]]:
    direct = _validated_call(_spec(specs, "read"), {"file_path": path})
    if direct:
        return direct
    # PTC / run_code-only clients: invoke the harness read tool through the
    # programmatic tools.* bridge. Using tools.read (instead of raw node:fs)
    # keeps the harness read-tracking intact, which the edit tool requires
    # before it will modify a file.
    nested = _run_code_nested_call(specs, "read", {"file_path": path})
    if nested:
        return nested
    return None


def translate_native_tool_calls(calls: List[Dict[str, Any]], specs: List[ToolSpec]) -> List[Dict[str, Any]]:
    """Translate supported explicit native calls to OpenAI-style tool calls."""
    translated: List[Dict[str, Any]] = []
    for call in calls:
        function = call.get("function") or ""
        args = call.get("args") or {}
        auto_load = call.get("autoLoadPhase") or ""
        if auto_load:
            logger.info("[NATIVE] skip auto-load %s phase=%s", function, auto_load)
            continue
        if function == "connections.search.unifiedSearch":
            question = args.get("question")
            keywords = args.get("keywords")
            queries = []
            for value in (keywords, question):
                if isinstance(value, str) and value.strip() and value.strip() not in queries:
                    queries.append(value.strip())
            if queries:
                direct = _validated_call(_spec(specs, "web_search"), {"queries": queries})
                if direct:
                    translated.append(direct)
                    continue
                nested = _run_code_nested_call(specs, "web_search", {"queries": queries})
                if nested:
                    translated.append(nested)
                    continue
            logger.info("[NATIVE] unifiedSearch without query: %s", json.dumps(args, ensure_ascii=False)[:300])
            continue
        if not function.startswith(_SUPPORTED_PREFIXES):
            logger.info("[NATIVE] unmapped %s args=%s", function, json.dumps(args, ensure_ascii=False)[:300])
            continue
        try:
            if function == "connections.computer.writeFile":
                path = args.get("path")
                content = args.get("content")
                if isinstance(path, str) and isinstance(content, str):
                    result = _write_file_call(specs, path, content)
                    if result:
                        translated.append(result)
            elif function == "connections.computer.readFile":
                path = args.get("path")
                if isinstance(path, str):
                    result = _read_file_call(specs, path)
                    if result:
                        translated.append(result)
            elif function == "connections.computer.runCode":
                code = args.get("code")
                if isinstance(code, str) and code.strip():
                    result = _run_code_call(specs, code, str(args.get("description") or "Run code requested by the model"))
                    if result:
                        translated.append(result)
                else:
                    logger.info("[NATIVE] runCode without code arg: %s", json.dumps(args, ensure_ascii=False)[:300])
            else:
                alias_calls = _computer_alias_call(specs, function, args)
                if alias_calls:
                    translated.extend(alias_calls)
                else:
                    logger.info("[NATIVE] unsupported computer tool %s", function)
        except Exception as exc:
            logger.warning("[NATIVE] translation failed for %s: %s", function, exc)
    return translated




# ---------------------------------------------------------------------------
# Supplemental protocol for harness-only tools that Notion's native agent does
# not cover.  This is an explicit fixed JSON format (no natural-language intent
# guessing): {"name": "edit", "arguments": {...}}.
# The schemas below mirror DSH's captured tool catalog; when the incoming
# request already declares the tool, the real schema wins.
# ---------------------------------------------------------------------------

_NATIVE_COVERED = {"write", "read", "run_code"}

_SUPPLEMENTAL_CATALOG: Dict[str, Dict[str, Any]] = {
    "edit": {
        "desc": "Replace text in a file",
        "props": {"file_path": {"type": "string"}, "old_string": {"type": "string"}, "new_string": {"type": "string"}, "replace_all": {"type": "boolean"}},
        "required": ["file_path", "old_string", "new_string"],
    },
    "glob": {
        "desc": "List files by glob pattern",
        "props": {"pattern": {"type": "string"}, "path": {"type": "string"}},
        "required": ["pattern"],
    },
    "grep": {
        "desc": "Search file contents",
        "props": {"pattern": {"type": "string"}, "path": {"type": "string"}, "include": {"type": "string"}},
        "required": ["pattern"],
    },
    "pwsh": {
        "desc": "Run a shell command",
        "props": {"command": {"type": "string"}, "description": {"type": "string"}, "timeoutMs": {"type": "number"}, "workdir": {"type": "string"}, "run_in_background": {"type": "boolean"}},
        "required": ["command", "description"],
    },
    "web_fetch": {
        "desc": "Fetch a URL",
        "props": {"url": {"type": "string"}},
        "required": ["url"],
    },
    "web_search": {
        "desc": "Search the web",
        "props": {"queries": {"type": "array"}},
        "required": ["queries"],
    },
    "read_image": {
        "desc": "Read an image file",
        "props": {"file_path": {"type": "string"}},
        "required": ["file_path"],
    },
    "todo_write": {
        "desc": "Replace the todo list",
        "props": {"todos": {"type": "array"}},
        "required": ["todos"],
    },
    "skill": {
        "desc": "Load a skill by name",
        "props": {"name": {"type": "string"}},
        "required": ["name"],
    },
    "subagent": {
        "desc": "Delegate a self-contained task",
        "props": {"description": {"type": "string"}, "prompt": {"type": "string"}, "run_in_background": {"type": "boolean"}},
        "required": ["description", "prompt"],
    },
    "subagent_fork": {
        "desc": "Delegate a task with inherited context",
        "props": {"description": {"type": "string"}, "prompt": {"type": "string"}, "run_in_background": {"type": "boolean"}},
        "required": ["description", "prompt"],
    },
    "workflow": {
        "desc": "Run a multi-agent workflow script",
        "props": {"script": {"type": "string"}, "meta": {"type": "object"}, "args": {"type": "object"}, "run_in_background": {"type": "boolean"}},
        "required": ["script", "meta"],
    },
    "job_list": {"desc": "List background jobs", "props": {}, "required": []},
    "job_output": {
        "desc": "Read a background job",
        "props": {"job_id": {"type": "string"}, "wait": {"type": "boolean"}, "timeout_ms": {"type": "number"}},
        "required": ["job_id"],
    },
    "job_kill": {
        "desc": "Cancel a background job",
        "props": {"job_id": {"type": "string"}, "reason": {"type": "string"}},
        "required": ["job_id"],
    },
    "list_agents": {"desc": "List team members", "props": {"scope": {"type": "string"}}, "required": []},
    "send_message": {
        "desc": "Send a message to a teammate",
        "props": {"agent_id": {"type": "string"}, "message": {"type": "string"}},
        "required": ["agent_id", "message"],
    },
    "interrupt_agent": {"desc": "Interrupt a teammate", "props": {"agent_id": {"type": "string"}}, "required": ["agent_id"]},
    "create_goal": {"desc": "Create a persisted goal", "props": {"objective": {"type": "string"}, "max_goal_rounds": {"type": "number"}}, "required": ["objective"]},
    "get_goal": {"desc": "Read the current goal", "props": {}, "required": []},
    "update_goal": {
        "desc": "Update the current goal",
        "props": {"goal_id": {"type": "string"}, "revision": {"type": "number"}, "action": {"type": "string"}, "objective": {"type": "string"}, "max_goal_rounds": {"type": "number"}, "blocked_reason": {"type": "string"}},
        "required": ["goal_id", "revision", "action"],
    },
    "exit_plan_mode": {"desc": "Present a plan for approval", "props": {"plan": {"type": "string"}}, "required": ["plan"]},
}


def _synthetic_spec(name: str, item: Dict[str, Any]) -> ToolSpec:
    return ToolSpec(
        name=name,
        description=item.get("desc", ""),
        parameters={"type": "object", "properties": item.get("props", {}), "required": item.get("required", [])},
    )


def extend_tool_specs(specs: List[ToolSpec]) -> List[ToolSpec]:
    """Real request specs + synthetic specs for supplemental harness-only tools."""
    existing = {spec.name for spec in specs}
    has_run_code = "run_code" in existing
    extra: List[ToolSpec] = []
    for name, item in _SUPPLEMENTAL_CATALOG.items():
        if name in existing or name in _NATIVE_COVERED:
            continue
        if not has_run_code and not any(spec.name == name for spec in specs):
            continue
        extra.append(_synthetic_spec(name, item))
    return list(specs) + extra


_DEFAULT_SUPPLEMENTAL_TOOLS = (
    "glob",
    "grep",
    "edit",
    "pwsh",
    "web_fetch",
    "web_search",
)


def _supplemental_allowlist() -> List[str]:
    raw = os.environ.get("HARNESS_SUPPLEMENTAL_TOOLS", "").strip()
    if raw:
        return [item.strip() for item in raw.split(",") if item.strip()]
    return list(_DEFAULT_SUPPLEMENTAL_TOOLS)


_DSH_TO_COMPUTER_ALIAS = {
    "glob": "listFiles",
    "edit": "editFile",
    "grep": "grep",
    "pwsh": "pwsh",
    "web_fetch": "webFetch",
    "web_search": "webSearch",
    "read_image": "readImage",
    "todo_write": "todoWrite",
    "skill": "skill",
    "subagent": "subagent",
    "subagent_fork": "subagentFork",
    "workflow": "workflow",
    "job_list": "jobList",
    "job_output": "jobOutput",
    "job_kill": "jobKill",
    "list_agents": "listAgents",
    "send_message": "sendMessage",
    "interrupt_agent": "interruptAgent",
    "create_goal": "createGoal",
    "get_goal": "getGoal",
    "update_goal": "updateGoal",
    "exit_plan_mode": "exitPlanMode",
}

_COMPUTER_ALIAS_SIGNATURES = {
    "writeFile": ("path, content", '{"function":"connections.computer.writeFile","args":{"path":"index.html","content":"<!doctype html>..."}}'),
    "readFile": ("path", '{"function":"connections.computer.readFile","args":{"path":"a.txt"}}'),
    "runCode": ("code", '{"function":"connections.computer.runCode","args":{"code":"console.log(1)","description":"run code"}}'),
    "pwsh": ("command, description", '{"function":"connections.computer.pwsh","args":{"command":"Get-ChildItem","description":"List files"}}'),
    "editFile": ("path, old_string, new_string", '{"function":"connections.computer.editFile","args":{"path":"a.txt","old_string":"hello","new_string":"world"}}'),
    "listFiles": ("pattern?, path?", '{"function":"connections.computer.listFiles","args":{"pattern":"*"}}'),
    "grep": ("pattern, path?", '{"function":"connections.computer.grep","args":{"pattern":"hello","path":"a.txt"}}'),
    "webFetch": ("url", '{"function":"connections.computer.webFetch","args":{"url":"https://example.com"}}'),
    "webSearch": ("queries", '{"function":"connections.computer.webSearch","args":{"queries":["DeepSeek news"]}}'),
    "readImage": ("path", '{"function":"connections.computer.readImage","args":{"path":"image.png"}}'),
}


def build_supplemental_prompt(specs: List[ToolSpec]) -> str:
    """Tell the model about the emulated connections.computer module for local work."""
    specs = extend_tool_specs(specs)
    names = {spec.name for spec in specs}
    aliases = ["writeFile", "readFile", "runCode"]
    for dsh_name in _supplemental_allowlist():
        alias = _DSH_TO_COMPUTER_ALIAS.get(dsh_name)
        if alias and dsh_name in names:
            aliases.append(alias)
    if not aliases:
        return ""
    examples = []
    extra_lines = []
    for alias in aliases:
        entry = _COMPUTER_ALIAS_SIGNATURES.get(alias)
        if entry:
            examples.append("assistant: " + entry[1])
            continue
        dsh_name = next((name for name, value in _DSH_TO_COMPUTER_ALIAS.items() if value == alias), alias)
        item = _SUPPLEMENTAL_CATALOG.get(dsh_name, {})
        signature = ",".join(item.get("required", []))
        extra_lines.append("- connections.computer." + alias + "(" + signature + "): " + item.get("desc", ""))
    return (
        "\n\n## Local workspace tools (emulated connections.computer module)\n"
        "Notion's official modules have no computer module. This harness emulates one for the user's LOCAL workspace. "
        "For local file/shell operations, call it with the native callFunction JSON format BEFORE trying connections.fs.*. "
        "Never reply that you cannot access local files: the harness executes these calls on the user's machine.\n"
        "Examples (one JSON object per line):\n"
        + "\n".join(examples)
        + (("\nAvailable:\n" + "\n".join(extra_lines)) if extra_lines else "")
        + "\nRules: exact argument names; one JSON object per tool call; do not use connections.fs.* for local files."
    )




def extract_supplemental_calls_from_text(
    text: str, specs: List[ToolSpec]
) -> List[Dict[str, Any]]:
    """Repair and parse stream-truncated supplemental JSON from model text.

    Notion's streaming occasionally drops the leading '{' or a quote, e.g.
    '.name":"edit","arguments":{...}'. This only repairs the JSON envelope for
    explicitly named catalog tools; it does not infer any intent from prose.
    """
    if not text:
        return []
    pattern = re.compile(
        r'"?name"?\s*:\s*"([A-Za-z0-9_]+)"\s*,\s*"?arguments"?\s*:\s*\{'
    )
    calls: List[Dict[str, Any]] = []
    seen = set()
    for match in pattern.finditer(text):
        name = match.group(1)
        spec = next((item for item in specs if item.name == name), None)
        if spec is None:
            continue
        start = match.end() - 1
        depth = 0
        in_string = False
        escaped = False
        end = None
        for idx in range(start, len(text)):
            ch = text[idx]
            if in_string:
                if escaped:
                    escaped = False
                elif ch == "\\":
                    escaped = True
                elif ch == '"':
                    in_string = False
                continue
            if ch == '"':
                in_string = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    end = idx
                    break
        if end is None:
            continue
        try:
            args = json.loads(text[start:end + 1])
        except (json.JSONDecodeError, ValueError):
            continue
        cleaned = validate_args(spec, args)
        if cleaned is None:
            continue
        signature = name + json.dumps(cleaned, sort_keys=True, ensure_ascii=False, default=str)
        if signature in seen:
            continue
        seen.add(signature)
        logger.info("[SUPPLEMENTAL] repaired truncated JSON call %s", name)
        calls.append(make_tool_call(spec, cleaned))
    return calls



_COMPUTER_TOOL_MAP: Dict[str, Dict[str, Any]] = {
    "writeFile": {"tool": "write", "args": {"path": "file_path", "content": "content"}},
    "readFile": {"tool": "read", "args": {"path": "file_path"}},
    "runCode": {"tool": "run_code", "args": {"code": "code", "description": "description"}},
    "pwsh": {"tool": "pwsh", "args": {"command": "command", "description": "description"}},
    "editFile": {
        "tool": "edit",
        "args": {
            "path": "file_path", "file_path": "file_path",
            "old": "old_string", "old_string": "old_string",
            "new": "new_string", "new_string": "new_string",
        },
    },
    "listFiles": {"tool": "glob", "args": {"path": "path", "dir": "path", "pattern": "pattern"}},
    "listDir": {"tool": "glob", "args": {"path": "path", "dir": "path", "pattern": "pattern"}},
    "readDir": {"tool": "glob", "args": {"path": "path", "dir": "path", "pattern": "pattern"}},
    "grep": {"tool": "grep", "args": {"pattern": "pattern", "path": "path", "include": "include"}},
    "searchFiles": {"tool": "grep", "args": {"pattern": "pattern", "path": "path", "include": "include"}},
    "grepFiles": {"tool": "grep", "args": {"pattern": "pattern", "path": "path", "include": "include"}},
    "edit": {
        "tool": "edit",
        "args": {
            "path": "file_path", "file_path": "file_path",
            "old": "old_string", "old_string": "old_string",
            "new": "new_string", "new_string": "new_string",
        },
    },
    "webFetch": {"tool": "web_fetch", "args": {"url": "url"}},
    "fetch": {"tool": "web_fetch", "args": {"url": "url"}},
    "fetchUrl": {"tool": "web_fetch", "args": {"url": "url"}},
    "webSearch": {"tool": "web_search", "args": {"queries": "queries", "query": "queries"}},
    "searchWeb": {"tool": "web_search", "args": {"queries": "queries", "query": "queries"}},
    "readImage": {"tool": "read_image", "args": {"path": "file_path", "file_path": "file_path"}},
    "todoWrite": {"tool": "todo_write", "args": {}},
    "skill": {"tool": "skill", "args": {}},
    "subagent": {"tool": "subagent", "args": {}},
    "subagentFork": {"tool": "subagent_fork", "args": {}},
    "workflow": {"tool": "workflow", "args": {}},
    "jobList": {"tool": "job_list", "args": {}},
    "jobOutput": {"tool": "job_output", "args": {}},
    "jobKill": {"tool": "job_kill", "args": {}},
    "listAgents": {"tool": "list_agents", "args": {}},
    "sendMessage": {"tool": "send_message", "args": {}},
    "interruptAgent": {"tool": "interrupt_agent", "args": {}},
    "createGoal": {"tool": "create_goal", "args": {}},
    "getGoal": {"tool": "get_goal", "args": {}},
    "updateGoal": {"tool": "update_goal", "args": {}},
    "exitPlanMode": {"tool": "exit_plan_mode", "args": {}},
}


def _computer_alias_call(
    specs: List[ToolSpec], function: str, args: Dict[str, Any]
) -> List[Dict[str, Any]]:
    prefix = "connections.computer."
    if not function.startswith(prefix):
        return []
    suffix = function[len(prefix):]
    if suffix == "readFiles":
        calls: List[Dict[str, Any]] = []
        files = args.get("files")
        for path in files if isinstance(files, list) else []:
            if not isinstance(path, str):
                continue
            direct = _validated_call(_spec(specs, "read"), {"file_path": path})
            if direct:
                calls.append(direct)
                continue
            nested = _run_code_nested_call(specs, "read", {"file_path": path})
            if nested:
                calls.append(nested)
        return calls
    mapping = _COMPUTER_TOOL_MAP.get(suffix)
    if not mapping:
        return []
    mapped: Dict[str, Any] = {}
    if not mapping["args"]:
        mapped = dict(args)
    else:
        for source_key, target_key in mapping["args"].items():
            if source_key in args:
                mapped[target_key] = args[source_key]
    if mapping["tool"] == "glob":
        mapped.setdefault("pattern", "*")
    if mapping["tool"] == "web_search" and isinstance(mapped.get("queries"), str):
        mapped["queries"] = [mapped["queries"]]
    direct = _validated_call(_spec(specs, mapping["tool"]), mapped)
    if direct:
        return [direct]
    nested = _run_code_nested_call(specs, mapping["tool"], mapped)
    return [nested] if nested else []


def _run_code_nested_call(specs: List[ToolSpec], name: str, args: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    run_spec = _spec(specs, "run_code")
    if not run_spec:
        return None
    code = "\n".join([
        "const args = " + json.dumps(args, ensure_ascii=False) + ";",
        "const r = await tools." + name + "(args);",
        "console.log(JSON.stringify(r));",
    ])
    return _run_code_call(specs, code, "Call harness tool " + name)


def translate_supplemental_tool_calls(
    calls: List[Dict[str, Any]], specs: List[ToolSpec]
) -> List[Dict[str, Any]]:
    """Translate explicit supplemental JSON calls into executables.

    Direct DSH tools are passed through (already schema-validated by the adapter
    against extended specs); when only run_code is available the call becomes a
    run_code program that invokes tools.<name>(args).
    """
    translated: List[Dict[str, Any]] = []
    for call in calls:
        function = (call.get("function") or {})
        name = function.get("name") or ""
        if name in _NATIVE_COVERED:
            continue
        try:
            raw_args = function.get("arguments") or "{}"
            args = json.loads(raw_args) if isinstance(raw_args, str) else (raw_args or {})
        except (json.JSONDecodeError, ValueError):
            logger.info("[SUPPLEMENTAL] invalid JSON args for %s", name)
            continue
        if name not in _SUPPLEMENTAL_CATALOG:
            logger.info("[SUPPLEMENTAL] unknown tool %s", name)
            continue
        if _spec(specs, name):
            translated.append(call)
            continue
        nested = _run_code_nested_call(specs, name, args)
        if nested:
            translated.append(nested)
        else:
            logger.info("[SUPPLEMENTAL] no executor for %s", name)
    return translated


def record_native_tool_calls(calls: List[Dict[str, Any]]) -> None:
    """Append discovered native calls to $NOTION_NATIVE_LOG for offline research."""
    path = os.environ.get("NOTION_NATIVE_LOG")
    if not path or not calls:
        return
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "a", encoding="utf-8") as handle:
            for call in calls:
                handle.write(json.dumps(call, ensure_ascii=False, default=str) + "\n")
    except Exception as exc:
        logger.debug("native call log failed: %s", exc)
