# app/adapters/__init__.py
"""Harness tool-calling adapters.

Each adapter renders the tool protocol for one harness and converts Notion AI
text back into OpenAI-compatible \`tool_calls\`. New harnesses are added by
subclassing HarnessAdapter and registering an instance here.
"""
from __future__ import annotations

from typing import Optional

from app.adapters.base import (
    HarnessAdapter,
    ToolSpec,
    detect_harness,
    make_tool_call,
    normalize_tool_call_dict,
    parse_tool_call_candidates,
    tool_specs_from_openai,
    validate_args,
)
from app.adapters.dsh import DeepSeekHarnessAdapter
from app.adapters.generic import GenericAdapter

_ADAPTERS = {
    "dsh": DeepSeekHarnessAdapter(),
    "deepseek-harness": DeepSeekHarnessAdapter(),
    "deepseek_harness": DeepSeekHarnessAdapter(),
    "generic": GenericAdapter(),
}


def get_adapter(name: Optional[str] = None) -> HarnessAdapter:
    return _ADAPTERS.get((name or "").strip().lower(), _ADAPTERS["generic"])


def adapter_for_request(body: Optional[dict] = None, headers=None) -> HarnessAdapter:
    return get_adapter(detect_harness(headers, body))


__all__ = [
    "HarnessAdapter",
    "ToolSpec",
    "DeepSeekHarnessAdapter",
    "GenericAdapter",
    "adapter_for_request",
    "detect_harness",
    "get_adapter",
    "make_tool_call",
    "normalize_tool_call_dict",
    "parse_tool_call_candidates",
    "tool_specs_from_openai",
    "validate_args",
]
