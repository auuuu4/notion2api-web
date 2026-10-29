# app/adapters/generic.py
"""Conservative adapter used when the caller is not a known harness."""
from __future__ import annotations

from typing import List

from app.adapters.base import HarnessAdapter, ToolSpec


class GenericAdapter(HarnessAdapter):
    name = "generic"
    max_tool_calls = 2

    def build_tool_prompt(self, specs: List[ToolSpec], user_text: str = "") -> str:
        if not specs:
            return ""
        lines = ["- " + s.name + ": " + (s.description or "").splitlines()[0][:160] for s in specs]
        return (
            "\n\nYou can use the following tools. When a tool is required, reply with a JSON object "
            'of the form {"name": "<tool>", "arguments": {...}} and no other text.\n'
            "Available tools:\n" + "\n".join(lines)
        )
