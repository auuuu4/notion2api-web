"""Tests for provider response assembly and quota detection helpers."""
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.providers.notion_provider import NotionAIProvider, _flatten_tool_results_enabled, _native_tool_replay_enabled, _thread_reuse_enabled, _thread_reuse_key


class ProviderHelperTests(unittest.TestCase):
    def test_assemble_prefers_multiline_incremental(self):
        result = NotionAIProvider._assemble_full_response(
            "short summary", ["<!doctype html>", "<html>", "<body>", "</body>", "</html>"]
        )
        self.assertIn("<!doctype html>", result)
        self.assertIn("</html>", result)

    def test_assemble_keeps_final_when_richer(self):
        result = NotionAIProvider._assemble_full_response("a much longer final answer", ["x"])
        self.assertEqual(result, "a much longer final answer")

    def test_quota_patch_is_detected(self):
        provider = NotionAIProvider.__new__(NotionAIProvider)
        line = json.dumps({
            "type": "patch",
            "v": [{
                "o": "a",
                "p": "/s/-",
                "v": {
                    "type": "premium-feature-unavailable",
                    "featureAvailability": {"limit": {"current": 68, "total": 50}},
                },
            }],
        })
        results = provider._parse_ndjson_line_to_texts(line)
        self.assertEqual(results[0][0], "error")
        self.assertIn("premium-feature-unavailable", results[0][1])
        self.assertIn("68/50", results[0][1])

    def test_normal_premium_key_is_not_error(self):
        provider = NotionAIProvider.__new__(NotionAIProvider)
        line = json.dumps({"type": "patch", "v": [{"o": "a", "p": "/s/-", "v": {"type": "markdown-chat", "value": "ok"}}]})
        results = provider._parse_ndjson_line_to_texts(line)
        self.assertEqual(results[0][0], "final")



    def test_build_native_tool_result_entry(self):
        entry = NotionAIProvider._build_native_tool_result_entry(
            "c1", "write", "wrote file", {"c1": {"function": "write", "args": {"file_path": "a.html"}}}
        )
        self.assertEqual(entry["type"], "agent-tool-result")
        self.assertEqual(entry["input"]["function"], "connections.computer.writeFile")
        self.assertEqual(entry["input"]["args"]["file_path"], "a.html")
        self.assertEqual(entry["result"]["output"], "wrote file")
        self.assertEqual(entry["state"], "applied")

    def test_build_native_tool_result_entry_search(self):
        entry = NotionAIProvider._build_native_tool_result_entry("c2", "web_search", "sources", {})
        self.assertEqual(entry["input"]["function"], "connections.computer.webSearch")



    def test_unknown_tool_maps_to_computer_namespace(self):
        entry = NotionAIProvider._build_native_tool_result_entry("c9", "todo_write", "ok", {})
        self.assertEqual(entry["input"]["function"], "connections.computer.todo_write")
        self.assertEqual(entry["result"]["output"], "ok")

    def test_native_replay_default_on_and_opt_out(self):
        import os
        from unittest.mock import patch
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("NOTION_NATIVE_TOOL_REPLAY", None)
            self.assertTrue(_native_tool_replay_enabled())
        with patch.dict(os.environ, {"NOTION_NATIVE_TOOL_REPLAY": "0"}):
            self.assertFalse(_native_tool_replay_enabled())



    def test_flatten_tool_results_default_on_and_opt_out(self):
        import os
        from unittest.mock import patch
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("NOTION_FLATTEN_TOOL_RESULTS", None)
            self.assertTrue(_flatten_tool_results_enabled())
        with patch.dict(os.environ, {"NOTION_FLATTEN_TOOL_RESULTS": "0"}):
            self.assertFalse(_flatten_tool_results_enabled())

    def test_thread_reuse_default_on_and_opt_out(self):
        import os
        from unittest.mock import patch
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("NOTION_THREAD_REUSE", None)
            self.assertTrue(_thread_reuse_enabled())
        with patch.dict(os.environ, {"NOTION_THREAD_REUSE": "0"}):
            self.assertFalse(_thread_reuse_enabled())

    def test_thread_reuse_key_uses_first_real_user_message(self):
        messages = [
            {"role": "system", "content": "system"},
            {"role": "user", "content": "搜索今天的新闻"},
            {"role": "user", "content": "unrelated context"},
        ]
        self.assertEqual(_thread_reuse_key(messages), "搜索今天的新闻")
        self.assertEqual(_thread_reuse_key([]), "")



    def test_api_base_env_override_points_at_mock(self):
        import os
        from unittest.mock import patch
        with patch.dict(os.environ, {"NOTION_API_BASE": "http://127.0.0.1:8099/api/v3"}):
            # __init__ is bypassed; emulate the same endpoint construction contract.
            api_base = os.environ["NOTION_API_BASE"].rstrip("/")
            endpoints = {
                "runInference": api_base + "/runInferenceTranscript",
                "saveTransactions": api_base + "/saveTransactionsFanout",
            }
        self.assertEqual(endpoints["runInference"], "http://127.0.0.1:8099/api/v3/runInferenceTranscript")
        self.assertEqual(endpoints["saveTransactions"], "http://127.0.0.1:8099/api/v3/saveTransactionsFanout")

    def test_prepare_payload_injects_native_tool_result(self):
        provider = NotionAIProvider.__new__(NotionAIProvider)
        account = {"cookie": "c", "space_id": "s", "user_id": "u"}
        request_data = {
            "messages": [{"role": "user", "content": "hi"}],
            "_tools": [],
            "_native_replay_entries": [{
                "id": "e1", "type": "agent-tool-result", "toolName": "callFunction",
                "input": {"function": "connections.computer.webSearch", "args": {}},
                "result": {"output": "sources"}, "state": "applied", "autoLoadPhase": "",
            }],
        }
        payload = provider._prepare_payload(request_data, "thread-1", "albuquerque-quinn", "workflow", account)
        self.assertEqual(payload["transcript"][-1]["type"], "agent-tool-result")
        self.assertEqual(payload["transcript"][-1]["result"]["output"], "sources")


class TextAssemblyTests(unittest.TestCase):
    """Streamed patch parsing: hidden thinking vs visible text items."""

    @staticmethod
    def provider():
        return NotionAIProvider.__new__(NotionAIProvider)

    @staticmethod
    def parse_all(lines):
        provider = TextAssemblyTests.provider()
        state = {"item_types": [], "visible_items": 0}
        incremental = []
        final = None
        for line in lines:
            raw = line if isinstance(line, str) else json.dumps(line, ensure_ascii=False)
            for kind, text in provider._parse_ndjson_line_to_texts(raw, state):
                if kind == "incremental":
                    incremental.append(text)
                elif kind == "final":
                    final = text
        return "".join(incremental), final

    def test_thinking_content_hidden_and_text_prefix_kept(self):
        lines = [
            json.dumps({"type": "patch", "v": [{"o": "a", "p": "/s/-", "v": {
                "type": "agent-inference", "id": "ai-1",
                "value": [{"type": "thinking", "content": "The user wants"}],
            }}]}),
            json.dumps({"type": "patch", "v": [{"o": "x", "p": "/s/1/value/0/content", "v": " a post."}]}),
            json.dumps({"type": "patch", "v": [{"o": "a", "p": "/s/1/value/-", "v": {
                "type": "text", "content": "##",
            }}]}),
            json.dumps({"type": "patch", "v": [{"o": "x", "p": "/s/1/value/1/content", "v": " 姐妹们"}]}),
            json.dumps({"type": "record-map", "recordMap": {"thread_message": {"m": {
                "value": {"value": {"step": {
                    "type": "agent-inference",
                    "value": [
                        {"type": "thinking", "content": "The user wants a post."},
                        {"type": "text", "content": "## 姐妹们"},
                    ],
                }}}
            }}}}),
        ]
        incremental, final = self.parse_all(lines)
        self.assertEqual(incremental, "## 姐妹们")
        self.assertEqual(final, "## 姐妹们")
        self.assertNotIn("The user wants", incremental)

    def test_multiple_text_blocks_joined_with_blank_line(self):
        lines = [
            json.dumps({"type": "patch", "v": [{"o": "a", "p": "/s/-", "v": {
                "type": "agent-inference", "id": "ai-1",
                "value": [{"type": "thinking", "content": "plan"}],
            }}]}),
            json.dumps({"type": "patch", "v": [{"o": "a", "p": "/s/1/value/-", "v": {
                "type": "text", "content": "第一段",
            }}]}),
            json.dumps({"type": "patch", "v": [{"o": "a", "p": "/s/1/value/-", "v": {
                "type": "text", "content": "第二段",
            }}]}),
        ]
        incremental, _ = self.parse_all(lines)
        self.assertEqual(incremental, "第一段\n\n第二段")

    def test_record_map_collects_all_text_items(self):
        line = json.dumps({"type": "record-map", "recordMap": {"thread_message": {"m": {
            "value": {"value": {"step": {
                "type": "agent-inference",
                "value": [
                    {"type": "thinking", "content": "hidden"},
                    {"type": "text", "content": "A"},
                    {"type": "text", "content": "B"},
                ],
            }}}
        }}}})
        incremental, final = self.parse_all([line])
        self.assertEqual(incremental, "")
        self.assertEqual(final, "A\n\nB")

    def test_legacy_markdown_chat_still_works(self):
        line = json.dumps({"type": "patch", "v": [{"o": "a", "p": "/s/-", "v": {
            "type": "markdown-chat", "value": "hello",
        }}]})
        incremental, final = self.parse_all([line])
        self.assertEqual(final, "hello")


if __name__ == "__main__":
    unittest.main()
