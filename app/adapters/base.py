# app/adapters/base.py
"""Harness adapter primitives: tool specs, schema validation and harness detection."""
from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional

_INVALID = object()


@dataclass
class ToolSpec:
    name: str
    description: str = ""
    parameters: Dict[str, Any] = field(default_factory=dict)

    @property
    def properties(self) -> Dict[str, Any]:
        return self.parameters.get("properties") or {}

    @property
    def required(self) -> List[str]:
        return [x for x in (self.parameters.get("required") or []) if isinstance(x, str)]


def tool_specs_from_openai(tools_def: Optional[Iterable[dict]]) -> List[ToolSpec]:
    specs: List[ToolSpec] = []
    for item in tools_def or []:
        if not isinstance(item, dict):
            continue
        func = item.get("function")
        if not isinstance(func, dict) or not func.get("name"):
            continue
        specs.append(
            ToolSpec(
                name=str(func["name"]),
                description=str(func.get("description") or ""),
                parameters=func.get("parameters") if isinstance(func.get("parameters"), dict) else {},
            )
        )
    return specs


def detect_harness(headers: Any = None, body: Optional[dict] = None) -> str:
    """Detect the calling harness from explicit hints, then from the User-Agent."""
    body = body or {}
    explicit = body.get("harness") or body.get("_harness")
    if isinstance(explicit, str) and explicit.strip():
        return explicit.strip().lower()

    header_get = None
    if headers is not None:
        try:
            header_get = headers.get
        except AttributeError:
            header_get = None
    if header_get is not None:
        for key in ("x-harness", "x-llm-harness", "x-client-harness"):
            try:
                value = header_get(key)
            except Exception:
                value = None
            if value:
                return str(value).strip().lower()
        try:
            ua = str(header_get("user-agent") or "").lower()
        except Exception:
            ua = ""
        if "deepseek-harness" in ua or "deepseek harness" in ua or "dsh/" in ua:
            return "dsh"

    # Structural fallback: DSH's programmatic tool-calling preset exposes only
    # run_code; older presets expose pwsh + read_image together. This works even
    # when the client sends no helpful User-Agent.
    tool_names = set()
    for tool in body.get("tools") or []:
        func = tool.get("function") if isinstance(tool, dict) else None
        name = func.get("name") if isinstance(func, dict) else None
        if isinstance(name, str) and name:
            tool_names.add(name)
    if "run_code" in tool_names or {"pwsh", "read_image"} <= tool_names:
        return "dsh"
    return "generic"


def _is_null_allowed(schema: dict) -> bool:
    if schema.get("nullable") is True:
        return True
    types = schema.get("type")
    if types == "null":
        return True
    if isinstance(types, list) and "null" in types:
        return True
    for key in ("anyOf", "oneOf"):
        for sub in schema.get(key) or []:
            if isinstance(sub, dict) and _is_null_allowed(sub):
                return True
    return False


def coerce_value(value: Any, schema: dict) -> Any:
    """Best-effort coercion to the schema type. Returns _INVALID when impossible."""
    if not isinstance(schema, dict):
        return value
    if value is None:
        return None if _is_null_allowed(schema) else _INVALID

    enum = schema.get("enum")
    if isinstance(enum, list) and enum:
        if value not in enum:
            # allow a small amount of spelling flexibility (trim/case)
            if isinstance(value, str):
                for entry in enum:
                    if isinstance(entry, str) and entry.lower() == value.strip().lower():
                        return entry
            return _INVALID
        return value

    for key in ("anyOf", "oneOf"):
        branches = schema.get(key)
        if isinstance(branches, list) and branches:
            for branch in branches:
                if isinstance(branch, dict):
                    coerced = coerce_value(value, branch)
                    if coerced is not _INVALID:
                        return coerced
            return _INVALID

    expected = schema.get("type")
    if isinstance(expected, list):
        for candidate in expected:
            if candidate == "null":
                continue
            coerced = coerce_value(value, {**schema, "type": candidate})
            if coerced is not _INVALID:
                return coerced
        return _INVALID

    if expected in ("integer", "number"):
        if isinstance(value, bool):
            return _INVALID
        if isinstance(value, (int, float)):
            number = value
        elif isinstance(value, str) and re.fullmatch(r"-?\d+(?:\.\d+)?", value.strip()):
            number = float(value) if "." in value else int(value)
        else:
            return _INVALID
        if expected == "integer" and isinstance(number, float) and not number.is_integer():
            return _INVALID
        if isinstance(schema.get("minimum"), (int, float)) and number < schema["minimum"]:
            return _INVALID
        if isinstance(schema.get("maximum"), (int, float)) and number > schema["maximum"]:
            return _INVALID
        return int(number) if expected == "integer" else number

    if expected == "boolean":
        if isinstance(value, bool):
            return value
        if isinstance(value, str) and value.strip().lower() in ("true", "false"):
            return value.strip().lower() == "true"
        return _INVALID

    if expected == "string":
        if isinstance(value, str):
            return value
        if isinstance(value, (int, float, bool)):
            return str(value)
        return _INVALID

    if expected == "array":
        if not isinstance(value, list):
            return _INVALID
        items = schema.get("items")
        if isinstance(items, dict):
            out = []
            for item in value:
                coerced = coerce_value(item, items)
                if coerced is _INVALID:
                    return _INVALID
                out.append(coerced)
            return out
        return value

    if expected == "object":
        return value if isinstance(value, dict) else _INVALID

    return value


def validate_args(spec: ToolSpec, args: Any) -> Optional[Dict[str, Any]]:
    """Validate and clean arguments against a tool schema. Returns None if unusable."""
    if args is None:
        args = {}
    if isinstance(args, str):
        try:
            args = json.loads(args)
        except (json.JSONDecodeError, TypeError):
            return None
    if not isinstance(args, dict):
        return None

    props = spec.properties
    cleaned: Dict[str, Any] = {}
    for key, value in args.items():
        if key not in props:
            continue  # strip unknown keys; harnesses reject them
        coerced = coerce_value(value, props.get(key) or {})
        if coerced is _INVALID:
            continue
        cleaned[key] = coerced

    for key in spec.required:
        if key not in cleaned:
            return None
    return cleaned


def make_tool_call(spec: ToolSpec, args: Dict[str, Any], call_id: Optional[str] = None) -> Dict[str, Any]:
    return {
        "id": call_id or f"call_{uuid.uuid4().hex[:24]}",
        "type": "function",
        "function": {
            "name": spec.name,
            "arguments": json.dumps(args, ensure_ascii=False, separators=(",", ":")),
        },
    }


def normalize_tool_call_dict(candidate: dict, specs: List[ToolSpec]) -> Optional[Dict[str, Any]]:
    """Convert a model-provided dict into an OpenAI tool call if it matches a tool."""
    if not isinstance(candidate, dict):
        return None

    raw_name = None
    raw_args: Any = None

    func = candidate.get("function")
    if isinstance(func, dict):
        raw_name = func.get("name")
        raw_args = func.get("arguments", func.get("parameters"))
    if not raw_name:
        for key in ("tool", "tool_name", "name", "action", "function_name"):
            value = candidate.get(key)
            if isinstance(value, str) and value:
                raw_name = value
                break
            if isinstance(value, dict) and value.get("name"):
                raw_name = value.get("name")
                raw_args = value.get("arguments", value.get("parameters"))
                break
    if not raw_name:
        return None
    for key in ("arguments", "args", "parameters", "params", "input", "tool_input"):
        if raw_args is None and key in candidate:
            raw_args = candidate[key]

    name = str(raw_name).strip()
    spec = next((s for s in specs if s.name == name), None)
    if spec is None:
        spec = next((s for s in specs if s.name.lower() == name.lower()), None)
    if spec is None:
        return None
    args = validate_args(spec, raw_args)
    if args is None:
        return None
    return make_tool_call(spec, args)


_FENCE_RE = re.compile(r"```(?:json|JSON)?\s*(?:tool|action|tool_call|function|json)?\s*\n?([\s\S]*?)\n?```")
_JSON_START_RE = re.compile(r"\{")


def _iter_json_objects(text: str):
    decoder = json.JSONDecoder()
    for match in _JSON_START_RE.finditer(text or ""):
        try:
            obj, _end = decoder.raw_decode(text, match.start())
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            yield obj


def parse_tool_call_candidates(text: str) -> List[dict]:
    candidates: List[dict] = []
    for block in _FENCE_RE.findall(text or ""):
        block = block.strip()
        if not block:
            continue
        try:
            parsed = json.loads(block)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, dict):
            candidates.append(parsed)
        elif isinstance(parsed, list):
            candidates.extend([x for x in parsed if isinstance(x, dict)])
        else:
            candidates.extend(_iter_json_objects(block))
    # Bare JSON objects in the response (covers models that omit fences).
    if not candidates:
        candidates.extend(_iter_json_objects(text))
    return candidates


def message_text(msg: dict) -> str:
    content = msg.get("content")
    if isinstance(content, list):
        parts = []
        for part in content:
            if isinstance(part, dict) and part.get("type") == "text":
                parts.append(str(part.get("text", "")))
            elif isinstance(part, str):
                parts.append(part)
        return "\n".join(parts)
    return str(content or "")


class HarnessAdapter:
    """Base adapter. The generic implementation is deliberately conservative."""

    name = "generic"
    max_tool_calls = 2

    def select_user_request_index(self, messages: Optional[List[dict]]) -> Optional[int]:
        messages = messages or []
        for idx in range(len(messages) - 1, -1, -1):
            if messages[idx].get("role") == "user":
                return idx
        return None

    def user_request_text(self, messages: Optional[List[dict]]) -> str:
        idx = self.select_user_request_index(messages)
        if idx is None:
            return ""
        return message_text((messages or [])[idx])

    def build_tool_prompt(self, specs: List[ToolSpec], user_text: str = "") -> str:
        return ""

    def parse_tool_calls(self, text: str, specs: List[ToolSpec]) -> List[Dict[str, Any]]:
        calls: List[Dict[str, Any]] = []
        seen = set()
        for candidate in parse_tool_call_candidates(text):
            call = normalize_tool_call_dict(candidate, specs)
            if call is None:
                continue
            key = (call["function"]["name"], call["function"]["arguments"])
            if key in seen:
                continue
            seen.add(key)
            calls.append(call)
            if len(calls) >= self.max_tool_calls:
                break
        return calls
