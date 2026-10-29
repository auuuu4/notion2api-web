"""Tests for harness detection and the minimal DSH adapter."""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.adapters import detect_harness, get_adapter, tool_specs_from_openai
from app.adapters.base import coerce_value, validate_args
from app.adapters.dsh import (
    DeepSeekHarnessAdapter,
    latest_user_request_index,
    select_user_request_index,
    user_request_text,
)


def make_tools():
    return [{
        "type": "function",
        "function": {
            "name": "run_code",
            "description": "Execute code",
            "parameters": {
                "type": "object",
                "properties": {"code": {"type": "string"}, "description": {"type": "string"}},
                "required": ["code", "description"],
            },
        },
    }]


def dsh_messages(task, runtime=True):
    messages = [{"role": "system", "content": "system"}, {"role": "user", "content": task}]
    if runtime:
        messages.append({"role": "user", "content": "Current runtime context. This snapshot supersedes earlier snapshots."})
        messages.append({"role": "user", "content": "<system-reminder>A skill is a reusable set of task-specific instructions.</system-reminder>"})
    return messages


class DetectionTests(unittest.TestCase):
    def test_dsh_user_agent(self):
        headers = {"user-agent": "deepseek-harness/0.2.0-rc.2 (+https://github.com/deepseek-ai/deepseek-harness)"}
        self.assertEqual(detect_harness(headers, {}), "dsh")

    def test_dsh_tool_shape_without_ua(self):
        self.assertEqual(detect_harness({"user-agent": "node"}, {"tools": make_tools()}), "dsh")

    def test_unknown(self):
        self.assertEqual(detect_harness({"user-agent": "curl/8.0"}, {}), "generic")
        self.assertEqual(get_adapter("nope").name, "generic")


class RequestSelectionTests(unittest.TestCase):
    def test_skips_runtime_messages(self):
        messages = dsh_messages("读取 a.txt")
        index = select_user_request_index(messages)
        self.assertEqual(messages[index]["content"], "读取 a.txt")

    def test_latest_message_for_followup(self):
        messages = dsh_messages("做一个动画，html交付")
        messages.append({"role": "assistant", "content": "已生成"})
        messages.append({"role": "user", "content": "文件路径？"})
        self.assertEqual(user_request_text(messages), "文件路径？")
        self.assertEqual(latest_user_request_index(messages), len(messages) - 1)

    def test_tool_prompt_contains_native_and_emulated_computer(self):
        adapter = DeepSeekHarnessAdapter()
        prompt = adapter.build_tool_prompt(tool_specs_from_openai(make_tools()))
        self.assertIn("connections.computer.writeFile", prompt)
        self.assertIn("connections.computer.listFiles", prompt)
        self.assertIn("connections.computer.pwsh", prompt)


class ValidationTests(unittest.TestCase):
    def test_coerce(self):
        self.assertEqual(coerce_value("3", {"type": "integer"}), 3)
        self.assertEqual(coerce_value("true", {"type": "boolean"}), True)
        self.assertEqual(coerce_value("WORKSPACE-WRITE", {"type": "string", "enum": ["workspace-write"]}), "workspace-write")

    def test_validate_args(self):
        spec = tool_specs_from_openai([{
            "type": "function",
            "function": {"name": "read", "parameters": {"type": "object", "properties": {"file_path": {"type": "string"}, "limit": {"type": "integer", "minimum": 1}}, "required": ["file_path"]}},
        }])[0]
        self.assertEqual(validate_args(spec, {"file_path": "a", "extra": 1}), {"file_path": "a"})
        self.assertIsNone(validate_args(spec, {"limit": 2}))
        self.assertEqual(validate_args(spec, {"file_path": "a", "limit": "5"}), {"file_path": "a", "limit": 5})


if __name__ == "__main__":
    unittest.main()
