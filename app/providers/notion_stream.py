"""Structured reducer for Notion's streaming NDJSON protocol.

Notion streams a turn as a series of JSON records over NDJSON:

* patch-start records announce per-request state (e.g. quota exhaustion);
* patch records carry JSON-patch-like operations against the thread step array
  (/s/<step>): append a step (a /s/-), append an item (a /s/<n>/value/-),
  append text/token deltas (x /s/<n>/value/<i>/content);
* markdown-chat records carry a standalone summary;
* the trailing record-map is a complete snapshot of the thread.

The old provider parser guessed item types from the path string and only kept
the first record-map text, which leaked hidden thinking content into the
visible answer and dropped the first characters of a text item (its initial
content arrives with the creation op, not in a later append). This module keeps
a small state tree, tracks every item type, and exposes a canonical final text:

1. record-map text when present (authoritative snapshot);
2. otherwise markdown-chat text when richer;
3. otherwise the visible text items concatenated (blocks separated by a blank
   line).

Hidden thinking items are tracked separately for diagnostics and never enter
the visible answer.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

_STEP_ONLY = re.compile(r"^/s/(\d+)$")
_STEP_VALUE_ADD = re.compile(r"^/s/(\d+)/value/-$")
_VALUE_PATH = re.compile(r"^/s/(\d+)/value$")
_ITEM_PATH = re.compile(r"^/s/(\d+)/value/(\d+)$")
_ITEM_CONTENT_PATH = re.compile(r"^/s/(\d+)/value/(\d+)/content$")
# Step metadata that does not belong in the visible text tree. Notion emits
# these as patch operations while a turn streams; ignore them silently instead
# of counting them as unknown protocol operations.
_STEP_META_PATH = re.compile(
    r"^/s/\d+/(?:finishedAt|startedAt|inputTokens|outputTokens|cachedTokensRead|cachedTokensCreated|"
    r"serverTimeToSubmitLlmMs|serverTimeToFirstTokenMs|serverPostSubmitTimeToFirstTokenMs|"
    r"maxContextTokens|maxInputTokens|maxOutputTokens)$"
)


@dataclass
class StreamEvent:
    """One observable event produced while reducing a NDJSON line."""

    kind: str  # text_delta | final_text | quota_exceeded | error
    text: str = ""
    data: Dict[str, Any] = field(default_factory=dict)


class _StepState:
    __slots__ = ("raw", "type", "items", "index", "value")

    def __init__(self, raw: Dict[str, Any], index: Optional[int] = None) -> None:
        self.raw = raw
        self.type = str(raw.get("type") or "")
        raw_value = raw.get("value")
        if isinstance(raw_value, list):
            self.items: List[Dict[str, Any]] = [item for item in raw_value if isinstance(item, dict)]
        else:
            self.items = []
        self.index = index
        self.value = raw_value if isinstance(raw_value, str) else None


class NotionStreamReducer:
    """Reduce one Notion NDJSON response stream into text/events/stats."""

    def __init__(self, since_ms: Optional[int] = None) -> None:
        self._since_ms = since_ms
        self._steps: Dict[int, _StepState] = {}
        self._pending_steps: List[_StepState] = []
        self._visible_parts: List[str] = []
        self._thinking_parts: List[str] = []
        self._markdown_text = ""
        self._record_map_text = ""
        self._saw_record_map = False
        self._record_map_seen = 0
        self._visible_items = 0
        self._parsed_records = 0
        self._invalid_lines = 0
        self._unknown_op_count = 0
        self._unknown_ops: List[Dict[str, str]] = []
        self._unknown_paths: List[str] = []
        self._unknown_item_types: List[str] = []
        self._mismatch: Optional[Dict[str, Any]] = None
        self._quota: Optional[Dict[str, Any]] = None

    # -- public properties ----------------------------------------------
    @property
    def parsed_records(self) -> int:
        return self._parsed_records

    @property
    def saw_record_map(self) -> bool:
        return self._saw_record_map

    @property
    def visible_text(self) -> str:
        return "".join(self._visible_parts)

    @property
    def thinking_text(self) -> str:
        return "".join(self._thinking_parts)

    @property
    def record_map_text(self) -> str:
        return self._record_map_text

    @property
    def final_text(self) -> str:
        if self._record_map_text:
            return self._record_map_text
        visible = self.visible_text
        if self._markdown_text and len(self._markdown_text) >= len(visible):
            return self._markdown_text
        return visible

    @property
    def quota(self) -> Optional[Dict[str, Any]]:
        return self._quota

    @property
    def stats(self) -> Dict[str, Any]:
        return {
            "parsed_records": self._parsed_records,
            "invalid_lines": self._invalid_lines,
            "unknown_op_count": self._unknown_op_count,
            "unknown_ops": list(self._unknown_ops),
            "unknown_paths": list(self._unknown_paths),
            "unknown_item_types": list(self._unknown_item_types),
            "mismatch": self._mismatch,
            "thinking_chars": len(self.thinking_text),
            "visible_chars": len(self.visible_text),
            "visible_items": self._visible_items,
            "steps": len(self._steps) + len(self._pending_steps),
            "record_map_seen": self._record_map_seen,
        }

    # -- entry point ----------------------------------------------------
    def consume_line(self, raw_line: Any) -> List[StreamEvent]:
        try:
            text = raw_line.decode("utf-8", "ignore") if isinstance(raw_line, (bytes, bytearray)) else str(raw_line)
        except Exception:
            self._invalid_lines += 1
            return []
        text = text.strip()
        if not text:
            return []
        try:
            record = json.loads(text)
        except (json.JSONDecodeError, ValueError):
            self._invalid_lines += 1
            return []
        if not isinstance(record, dict):
            self._invalid_lines += 1
            return []
        self._parsed_records += 1
        record_type = record.get("type")
        if record_type == "patch":
            return self._handle_patch(record)
        if record_type == "patch-start":
            return self._handle_patch_start(record)
        if record_type == "markdown-chat":
            value = record.get("value")
            if isinstance(value, str) and value:
                self._markdown_text = value
                return [StreamEvent("final_text", value)]
            return []
        if record_type == "record-map":
            return self._handle_record_map(record)
        if record_type == "error":
            message = record.get("message") or record.get("error") or json.dumps(record, ensure_ascii=False)
            return [StreamEvent("error", str(message)[:500])]
        return []

    # -- patch handling -------------------------------------------------
    def _handle_patch(self, record: Dict[str, Any]) -> List[StreamEvent]:
        events: List[StreamEvent] = []
        for operation in record.get("v") or []:
            if not isinstance(operation, dict):
                self._count_unknown_op({"o": "?", "p": "?", "v": str(type(operation))})
                continue
            events.extend(self._apply_operation(operation))
        return events

    def _apply_operation(self, operation: Dict[str, Any]) -> List[StreamEvent]:
        op = operation.get("o")
        path = str(operation.get("p") or "")
        value = operation.get("v")
        if _STEP_META_PATH.match(path):
            return []
        if op == "a":
            return self._apply_add(path, value)
        if op == "x":
            return self._apply_extend(path, value)
        if op == "d":
            return self._apply_delete(path)
        self._count_unknown_op({"o": str(op), "p": path, "v": str(value)[:120]})
        return []

    def _handle_patch_start(self, record: Dict[str, Any]) -> List[StreamEvent]:
        events: List[StreamEvent] = []
        for item in ((record.get("data") or {}).get("s") or []):
            if not isinstance(item, dict):
                continue
            if item.get("type") != "premium-feature-unavailable":
                continue
            limit = ((item.get("featureAvailability") or {}).get("limit") or {})
            detail = {"current": limit.get("current"), "total": limit.get("total")}
            if self._quota is None:
                self._quota = detail
            logger.warning("Notion AI 配额已用尽: %s/%s", detail.get("current"), detail.get("total"))
            events.append(StreamEvent("quota_exceeded", "", detail))
        return events

    def _apply_add(self, path: str, value: Any) -> List[StreamEvent]:
        if path == "/s/-":
            if not isinstance(value, dict):
                self._count_unknown_path(path)
                return []
            step_type = value.get("type")
            if step_type == "premium-feature-unavailable":
                limit = ((value.get("featureAvailability") or {}).get("limit") or {})
                detail = {"current": limit.get("current"), "total": limit.get("total")}
                if self._quota is None:
                    self._quota = detail
                logger.warning("Notion AI 配额已用尽: %s/%s", detail.get("current"), detail.get("total"))
                return [StreamEvent("quota_exceeded", "", detail)]
            state = _StepState(value)
            events: List[StreamEvent] = []
            if state.type == "markdown-chat" and state.value:
                self._markdown_text = state.value
                events.append(StreamEvent("final_text", state.value))
            if state.type == "agent-inference":
                events.extend(self._handle_initial_items(state))
            self._pending_steps.append(state)
            return events

        match = _STEP_VALUE_ADD.match(path)
        if match:
            state = self._ensure_step(int(match.group(1)), "item")
            if not isinstance(value, dict):
                self._count_unknown_path(path)
                return []
            state.items.append(value)
            if value.get("type") == "text":
                return self._emit_new_text_item(value)
            if value.get("type") == "thinking":
                content = value.get("content")
                if isinstance(content, str) and content:
                    self._thinking_parts.append(content)
            return []

        match = _ITEM_CONTENT_PATH.match(path)
        if match:
            if not isinstance(value, str):
                self._count_unknown_path(path)
                return []
            return self._append_item_content(int(match.group(1)), int(match.group(2)), value)

        match = _ITEM_PATH.match(path)
        if match:
            state, index = self._ensure_step_with_item(int(match.group(1)), int(match.group(2)))
            if isinstance(value, dict):
                state.items[index] = value
                if value.get("type") == "text":
                    return self._emit_new_text_item(value)
            else:
                self._count_unknown_path(path)
            return []

        match = _STEP_ONLY.match(path)
        if match:
            index = int(match.group(1))
            state = self._ensure_step(index, "step")
            if isinstance(value, dict):
                state.raw = value
                state.type = str(value.get("type") or "")
                raw_value = value.get("value")
                state.items = [item for item in raw_value if isinstance(item, dict)] if isinstance(raw_value, list) else []
                if state.type == "markdown-chat" and isinstance(raw_value, str) and raw_value:
                    self._markdown_text = raw_value
                    return [StreamEvent("final_text", raw_value)]
            return []

        self._count_unknown_path(path)
        return []

    def _apply_extend(self, path: str, value: Any) -> List[StreamEvent]:
        if not isinstance(value, str):
            self._count_unknown_path(path)
            return []
        match = _ITEM_CONTENT_PATH.match(path)
        if match:
            return self._append_item_content(int(match.group(1)), int(match.group(2)), value)

        match = _ITEM_PATH.match(path)
        if match:
            return self._append_item_content(int(match.group(1)), int(match.group(2)), value)

        match = _VALUE_PATH.match(path)
        if match:
            state = self._ensure_step(int(match.group(1)), "value")
            if state.type == "markdown-chat":
                state.value = (state.value or "") + value
                self._markdown_text = state.value
                return [StreamEvent("final_text", state.value)]
            # Legacy shape: a raw text append on the step value.
            self._visible_parts.append(value)
            return [StreamEvent("text_delta", value)]

        self._count_unknown_path(path)
        return []

    def _apply_delete(self, path: str) -> List[StreamEvent]:
        match = _STEP_ONLY.match(path)
        if match:
            self._steps.pop(int(match.group(1)), None)
            return []
        match = _ITEM_PATH.match(path)
        if match:
            state = self._steps.get(int(match.group(1)))
            if state is not None:
                index = int(match.group(2))
                if 0 <= index < len(state.items):
                    state.items.pop(index)
            return []
        self._count_unknown_path(path)
        return []

    # -- item/text helpers ---------------------------------------------
    def _handle_initial_items(self, state: _StepState) -> List[StreamEvent]:
        events: List[StreamEvent] = []
        for item in state.items:
            item_type = item.get("type")
            content = item.get("content")
            if item_type == "text":
                events.extend(self._emit_new_text_item(item))
            elif item_type == "thinking" and isinstance(content, str) and content:
                self._thinking_parts.append(content)
        return events

    def _emit_new_text_item(self, item: Dict[str, Any]) -> List[StreamEvent]:
        events: List[StreamEvent] = []
        if self._visible_items > 0:
            self._visible_parts.append("\n\n")
            events.append(StreamEvent("text_delta", "\n\n"))
        self._visible_items += 1
        content = item.get("content")
        if isinstance(content, str) and content:
            self._visible_parts.append(content)
            events.append(StreamEvent("text_delta", content))
        return events

    def _append_item_content(self, step_index: int, item_index: int, content: str) -> List[StreamEvent]:
        state, index = self._ensure_step_with_item(step_index, item_index)
        item = state.items[index]
        item_type = item.get("type")
        if item_type == "thinking":
            self._thinking_parts.append(content)
            return []
        if item_type not in ("text", None):
            self._unknown_item_types.append(str(item_type))
        item["content"] = (item.get("content") or "") + content
        self._visible_parts.append(content)
        return [StreamEvent("text_delta", content)]

    def _ensure_step(self, index: int, reason: str = "any") -> _StepState:
        existing = self._steps.get(index)
        if existing is not None:
            return existing
        chosen: Optional[_StepState] = None
        if self._pending_steps:
            if reason in ("item", "value"):
                for candidate in self._pending_steps:
                    if candidate.type == "agent-inference":
                        chosen = candidate
                        break
            if chosen is None:
                chosen = self._pending_steps[0]
            self._pending_steps.remove(chosen)
            chosen.index = index
        else:
            chosen = _StepState({"type": "unknown"}, index)
        self._steps[index] = chosen
        return chosen

    def _ensure_step_with_item(self, step_index: int, item_index: int) -> Tuple[_StepState, int]:
        state = self._ensure_step(step_index, "item")
        while len(state.items) <= item_index:
            state.items.append({"type": "unknown"})
        return state, item_index

    # -- record-map -----------------------------------------------------
    def _handle_record_map(self, record: Dict[str, Any]) -> List[StreamEvent]:
        self._saw_record_map = True
        self._record_map_seen += 1
        thread_messages = ((record.get("recordMap") or {}).get("thread_message") or {})
        texts: List[str] = []
        if isinstance(thread_messages, dict):
            for message in thread_messages.values():
                step = (((message or {}).get("value") or {}).get("value") or {}).get("step") or {}
                if not isinstance(step, dict) or step.get("type") != "agent-inference":
                    continue
                if not self._is_current_step(step):
                    continue
                for item in step.get("value") or []:
                    if not isinstance(item, dict) or item.get("type") != "text":
                        continue
                    content = item.get("content")
                    if isinstance(content, str) and content:
                        texts.append(content)
        if texts:
            self._record_map_text = "\n\n".join(texts)
        self._check_consistency()
        events: List[StreamEvent] = []
        if self._record_map_text:
            events.append(StreamEvent("final_text", self._record_map_text))
        return events

    def _is_current_step(self, step: Dict[str, Any]) -> bool:
        if self._since_ms is None:
            return True
        started = step.get("startedAt")
        if started is None:
            return True
        try:
            return float(started) >= float(self._since_ms)
        except (TypeError, ValueError):
            return True

    def _check_consistency(self) -> None:
        record_text = self._record_map_text
        visible = self.visible_text
        if not record_text or not visible:
            return
        if record_text.strip() == visible.strip():
            return
        limit = min(len(record_text), len(visible))
        offset = next((i for i in range(limit) if record_text[i] != visible[i]), limit)
        self._mismatch = {
            "record_map_chars": len(record_text),
            "visible_chars": len(visible),
            "first_diff_offset": offset,
        }
        logger.warning(
            "record-map 与增量文本不一致（record_map=%d chars, incremental=%d chars, first_diff=%d）；"
            "以 record-map 为准",
            len(record_text),
            len(visible),
            offset,
        )

    # -- diagnostics ----------------------------------------------------
    def _count_unknown_op(self, sample: Dict[str, str]) -> None:
        self._unknown_op_count += 1
        if len(self._unknown_ops) < 5:
            self._unknown_ops.append(sample)

    def _count_unknown_path(self, path: str) -> None:
        self._unknown_op_count += 1
        if len(self._unknown_paths) < 5:
            self._unknown_paths.append(path)
        if len(self._unknown_ops) < 5:
            self._unknown_ops.append({"o": "?", "p": path, "v": ""})
