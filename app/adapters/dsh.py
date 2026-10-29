"""DeepSeek Harness adapter: runtime-context filtering + native tool prompt.

Tool execution itself is handled by app.adapters.notion_native (official
callFunction protocol + emulated connections.computer aliases). This module no
longer contains intent fallbacks or regex-based tool guessing.
"""
from __future__ import annotations

from typing import List, Optional

from app.adapters.base import HarnessAdapter, ToolSpec
from app.adapters.notion_native import build_supplemental_prompt

_RUNTIME_MARKERS = (
    "current runtime context",
    "this snapshot supersedes earlier",
    "current dsh file policy",
    "system-reminder",
    "targeted tool mode",
    "a skill is a reusable set of task-specific instructions",
    "agent teams is available in this session",
)

_NATIVE_PROMPT = (
    "\n\n## Harness tools\n"
    "You have native callFunction tools for local computer/file operations. "
    "Use them for file work instead of describing it in prose. Examples:\n"
    '{"function":"connections.computer.writeFile","args":{"path":"index.html","content":"<!doctype html>..."}}\n'
    '{"function":"connections.computer.readFile","args":{"path":"notes.txt"}}\n'
    '{"function":"connections.computer.runCode","args":{"code":"...","description":"..."}}\n'
    "The harness translates and executes these calls in the user's local workspace. "
    "For a deliverable, call writeFile with the complete content instead of pasting the content in chat.\n"
    "Keep the visible reply short: no analysis, planning, English chain-of-thought, or self-talk. "
    "When a task needs computation, verification or search, call runCode / webSearch immediately and let the code do the work "
    "instead of reasoning in prose; if you catch yourself writing a long chain of thought, stop and call the tool. "
    "The visible answer is played to the user as the final answer, so output only the final answer in the user's language."
)


def _message_text(message: dict) -> str:
    content = message.get("content")
    if isinstance(content, list):
        return " ".join(
            str(part.get("text", ""))
            for part in content
            if isinstance(part, dict) and part.get("type") == "text"
        )
    return str(content or "")


def _is_runtime_message(text: str) -> bool:
    lowered = (text or "").lower()
    return any(marker in lowered for marker in _RUNTIME_MARKERS)


def _strip_tool_prompt(text: str) -> str:
    for marker in ("\n\nYou are an AI agent running in DeepSeek Harness and you can use", "\n\n## Harness tools"):
        if marker in text:
            return text.split(marker, 1)[0]
    return text


def _user_candidates(messages: Optional[List[dict]]) -> List[int]:
    candidates: List[int] = []
    for index, message in enumerate(messages or []):
        if message.get("role") != "user":
            continue
        if _is_runtime_message(_message_text(message)):
            continue
        candidates.append(index)
    return candidates


def select_user_request_index(messages: Optional[List[dict]]) -> Optional[int]:
    """First real user task (used to attach the tool protocol once)."""
    candidates = _user_candidates(messages)
    if candidates:
        return candidates[0]
    for index in range(len(messages or []) - 1, -1, -1):
        if (messages or [])[index].get("role") == "user":
            return index
    return None


def latest_user_request_index(messages: Optional[List[dict]]) -> Optional[int]:
    """Newest real user message (used for follow-up detection/debugging)."""
    candidates = _user_candidates(messages)
    if candidates:
        return candidates[-1]
    for index in range(len(messages or []) - 1, -1, -1):
        if (messages or [])[index].get("role") == "user":
            return index
    return None


def user_request_text(messages: Optional[List[dict]]) -> str:
    index = latest_user_request_index(messages)
    if index is None:
        return ""
    return _strip_tool_prompt(_message_text((messages or [])[index]))


class DeepSeekHarnessAdapter(HarnessAdapter):
    name = "dsh"

    def select_user_request_index(self, messages: Optional[List[dict]]) -> Optional[int]:
        return select_user_request_index(messages)

    def user_request_text(self, messages: Optional[List[dict]]) -> str:
        return user_request_text(messages)

    def build_tool_prompt(self, specs: List[ToolSpec], user_text: str = "") -> str:
        if not specs:
            return ""
        return _NATIVE_PROMPT + build_supplemental_prompt(specs)
