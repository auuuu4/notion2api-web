"""Tests for Notion's native callFunction protocol translation."""
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.adapters import tool_specs_from_openai
from app.adapters.notion_native import (
    build_supplemental_prompt,
    extend_tool_specs,
    extract_native_tool_calls,
    extract_supplemental_calls_from_text,
    translate_native_tool_calls,
    translate_supplemental_tool_calls,
)


def run_code_specs():
    return tool_specs_from_openai([{
        "type": "function",
        "function": {
            "name": "run_code",
            "parameters": {
                "type": "object",
                "properties": {"code": {"type": "string"}, "description": {"type": "string"}},
                "required": ["code", "description"],
            },
        },
    }])


def full_specs():
    return tool_specs_from_openai([
        {"type": "function", "function": {"name": "write", "parameters": {"type": "object", "properties": {"file_path": {"type": "string"}, "content": {"type": "string"}}, "required": ["file_path", "content"]}}},
        {"type": "function", "function": {"name": "read", "parameters": {"type": "object", "properties": {"file_path": {"type": "string"}}, "required": ["file_path"]}}},
        {"type": "function", "function": {"name": "run_code", "parameters": {"type": "object", "properties": {"code": {"type": "string"}, "description": {"type": "string"}}, "required": ["code", "description"]}}},
    ])


def record_map_line(steps):
    thread = {}
    for idx, step in enumerate(steps):
        thread[f"msg{idx}"] = {"value": {"value": {"step": step}}}
    return json.dumps({"type": "record-map", "recordMap": {"thread_message": thread}})


def inference_step(tool_use_items):
    return {"type": "agent-inference", "value": tool_use_items}


def tool_use(call_id, function, args):
    return {
        "type": "tool_use",
        "id": call_id,
        "name": "callFunction",
        "content": json.dumps({"function": function, "args": args}, ensure_ascii=False),
        "isCustomToolCall": False,
        "autofixedJson": False,
    }


class ExtractionTests(unittest.TestCase):
    def test_extract_write_file_call(self):
        lines = [record_map_line([
            inference_step([tool_use("toolu_1", "connections.computer.writeFile", {"path": "index.html", "content": "<!doctype html>"})]),
        ])]
        calls = extract_native_tool_calls(lines)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["function"], "connections.computer.writeFile")
        self.assertEqual(calls[0]["args"]["path"], "index.html")
        self.assertEqual(calls[0]["args"]["content"], "<!doctype html>")

    def test_extract_auto_load_result_with_phase(self):
        line = json.dumps({
            "type": "record-map",
            "recordMap": {"thread_message": {"m": {"value": {"value": {"step": {
                "type": "agent-tool-result",
                "input": {"function": "connections.fs.readDir", "args": {"dir": ".", "tree": True}},
                "state": "applied",
                "autoLoadPhase": "file_tree",
            }}}}}},
        })
        calls = extract_native_tool_calls([line])
        self.assertEqual(calls[0]["function"], "connections.fs.readDir")
        self.assertEqual(calls[0]["autoLoadPhase"], "file_tree")

    def test_record_map_steps_from_previous_turns_are_skipped(self):
        old_item = tool_use("toolu_old", "connections.computer.webSearch", {"queries": ["old"]})
        old_item["startedAt"] = 1000
        new_item = tool_use("toolu_new", "connections.computer.webSearch", {"queries": ["new"]})
        new_item["startedAt"] = 5000
        lines = [record_map_line([inference_step([old_item]), inference_step([new_item])])]
        calls = extract_native_tool_calls(lines, since_ms=4000)
        self.assertEqual([c["id"] for c in calls], ["toolu_new"])

    def test_current_agent_tool_result_kept_old_skipped(self):
        def result_step(result_id, function, started_at):
            return {
                "type": "agent-tool-result",
                "id": result_id,
                "startedAt": started_at,
                "input": {"function": function, "args": {}},
                "result": {"output": "ok"},
            }
        lines = [record_map_line([
            result_step("old", "connections.computer.webSearch", 1000),
            result_step("new", "connections.computer.writeFile", 5000),
        ])]
        calls = extract_native_tool_calls(lines, since_ms=4000)
        self.assertEqual([c["id"] for c in calls], ["new"])

    def test_missing_started_at_is_kept(self):
        lines = [record_map_line([
            inference_step([tool_use("toolu_1", "connections.computer.webSearch", {"queries": ["q"]})]),
        ])]
        calls = extract_native_tool_calls(lines, since_ms=4000)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["id"], "toolu_1")

    def test_replayed_transcript_echo_is_not_reemitted(self):
        # The request replayed these two entries into the Notion transcript; the
        # response record-map echoes them back without a startedAt.
        replayed_tool_use = tool_use("call_1", "connections.computer.webSearch", {"queries": ["old"]})
        replayed_result = {
            "type": "agent-tool-result",
            "id": "result-uuid-1",
            "toolName": "callFunction",
            "input": {"function": "connections.computer.webSearch", "args": {"queries": ["old"]}},
            "result": {"output": "old output"},
            "state": "applied",
            "autoLoadPhase": "",
        }
        new_item = tool_use("toolu_new", "connections.computer.writeFile", {"path": "a.txt", "content": "x"})
        new_item["startedAt"] = 9000
        lines = [record_map_line([
            inference_step([replayed_tool_use]),
            replayed_result,
            inference_step([new_item]),
        ])]
        calls = extract_native_tool_calls(
            lines,
            since_ms=5000,
            exclude_ids={"call_1", "result-uuid-1"},
        )
        self.assertEqual([c["id"] for c in calls], ["toolu_new"])

    def test_replayed_echo_in_patch_is_not_reemitted(self):
        replayed_patch = {"type": "patch", "v": [{"o": "a", "p": "/s/-", "v": {
            "type": "agent-inference",
            "value": [tool_use("call_1", "connections.computer.webSearch", {"queries": ["old"]})],
        }}]}
        new_patch = {"type": "patch", "v": [{"o": "a", "p": "/s/-", "v": {
            "type": "agent-inference",
            "value": [tool_use("toolu_new", "connections.computer.writeFile", {"path": "a.txt", "content": "x"})],
        }}]}
        calls = extract_native_tool_calls(
            [json.dumps(replayed_patch), json.dumps(new_patch)],
            exclude_ids={"call_1"},
        )
        self.assertEqual([c["id"] for c in calls], ["toolu_new"])

    def test_dedup_by_id_keeps_full_args(self):
        partial = {"type": "patch", "v": [{"o": "a", "p": "/s/-", "v": {
            "type": "agent-inference",
            "value": [tool_use("same-id", "connections.computer.writeFile", {})],
        }}]}
        complete = record_map_line([
            inference_step([tool_use("same-id", "connections.computer.writeFile", {"path": "a.html", "content": "hi"})]),
        ])
        calls = extract_native_tool_calls([json.dumps(partial), complete])
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["args"]["path"], "a.html")


class TranslationTests(unittest.TestCase):
    def test_write_file_prefers_write_tool(self):
        calls = [{"id": "c", "function": "connections.computer.writeFile", "args": {"path": "index.html", "content": "<html>"}, "autoLoadPhase": ""}]
        translated = translate_native_tool_calls(calls, full_specs())
        self.assertEqual(translated[0]["function"]["name"], "write")
        args = json.loads(translated[0]["function"]["arguments"])
        self.assertEqual(args["file_path"], "index.html")
        self.assertEqual(args["content"], "<html>")

    def test_write_file_uses_run_code_when_only_run_code(self):
        calls = [{"id": "c", "function": "connections.computer.writeFile", "args": {"path": "index.html", "content": "<html>"}, "autoLoadPhase": ""}]
        translated = translate_native_tool_calls(calls, run_code_specs())
        self.assertEqual(translated[0]["function"]["name"], "run_code")
        args = json.loads(translated[0]["function"]["arguments"])
        self.assertIn("writeFile", args["code"])
        self.assertIn("index.html", args["code"])

    def test_auto_load_calls_are_not_translated(self):
        calls = [{"id": "c", "function": "connections.fs.readDir", "args": {"dir": "."}, "autoLoadPhase": "file_tree"}]
        self.assertEqual(translate_native_tool_calls(calls, full_specs()), [])

    def test_notion_tools_are_not_mapped(self):
        calls = [{"id": "c", "function": "connections.notion.loadUser", "args": {"url": "user://x"}, "autoLoadPhase": ""}]
        self.assertEqual(translate_native_tool_calls(calls, full_specs()), [])

    def test_run_code_native_call_maps_directly(self):
        calls = [{"id": "c", "function": "connections.computer.runCode", "args": {"code": "console.log(1)", "description": "d"}, "autoLoadPhase": ""}]
        translated = translate_native_tool_calls(calls, full_specs())
        self.assertEqual(translated[0]["function"]["name"], "run_code")
        self.assertIn("console.log(1)", json.loads(translated[0]["function"]["arguments"])["code"])



class SupplementalTests(unittest.TestCase):
    def test_extend_specs_adds_harness_only_tools(self):
        specs = run_code_specs()
        names = {s.name for s in extend_tool_specs(specs)}
        self.assertIn("web_search", names)
        self.assertIn("edit", names)
        self.assertNotIn("write", names)

    def test_prompt_lists_emulated_computer_tools(self):
        prompt = build_supplemental_prompt(run_code_specs())
        self.assertIn("emulated connections.computer module", prompt)
        self.assertIn("connections.computer.writeFile", prompt)
        self.assertIn("connections.computer.listFiles", prompt)
        self.assertIn("connections.computer.pwsh", prompt)
        self.assertIn("connections.computer.webSearch", prompt)
        self.assertNotIn("connections.computer.readImage", prompt)

    def test_prompt_allowlist_can_narrow(self):
        import os
        from unittest.mock import patch
        with patch.dict(os.environ, {"HARNESS_SUPPLEMENTAL_TOOLS": "glob"}):
            prompt = build_supplemental_prompt(run_code_specs())
        self.assertIn("connections.computer.listFiles", prompt)
        self.assertNotIn("connections.computer.webSearch", prompt)
        self.assertNotIn("connections.computer.pwsh", prompt)

    def test_computer_alias_pwsh_maps_to_nested_run_code(self):
        calls = [{"id": "c", "function": "connections.computer.pwsh", "args": {"command": "Get-ChildItem", "description": "list"}, "autoLoadPhase": ""}]
        translated = translate_native_tool_calls(calls, run_code_specs())
        self.assertEqual(translated[0]["function"]["name"], "run_code")
        code = json.loads(translated[0]["function"]["arguments"])["code"]
        self.assertIn("tools.pwsh", code)
        self.assertIn("Get-ChildItem", code)

    def test_computer_alias_list_files_maps_to_glob(self):
        calls = [{"id": "c", "function": "connections.computer.listFiles", "args": {"path": "."}, "autoLoadPhase": ""}]
        translated = translate_native_tool_calls(calls, run_code_specs())
        code = json.loads(translated[0]["function"]["arguments"])["code"]
        self.assertIn("tools.glob", code)
        self.assertIn('"pattern": "*"', code)

    def test_supplemental_tool_call_maps_to_run_code(self):
        call = {
            "id": "c", "type": "function",
            "function": {"name": "web_search", "arguments": json.dumps({"queries": ["test"]})},
        }
        translated = translate_supplemental_tool_calls([call], run_code_specs())
        self.assertEqual(translated[0]["function"]["name"], "run_code")
        code = json.loads(translated[0]["function"]["arguments"])["code"]
        self.assertIn("tools.web_search", code)
        self.assertIn("test", code)

    def test_supplemental_direct_tool_passthrough(self):
        specs = full_specs() + [s for s in extend_tool_specs(full_specs()) if s.name == "web_search"]
        call = {
            "id": "c", "type": "function",
            "function": {"name": "web_search", "arguments": json.dumps({"queries": ["q"]})},
        }
        translated = translate_supplemental_tool_calls([call], specs)
        self.assertEqual(translated[0]["function"]["name"], "web_search")



    def test_repair_truncated_supplemental_json(self):
        text = (
            'function":"connections.fs.readDir","args":{"dir":"modules"} } '
            'to replace "hello" with "bonjour" in a.txt using the harness edit tool.'
            'name":"edit","arguments":{"file_path":"a.txt","old_string":"hello","new_string":"bonjour"}}'
        )
        specs = extend_tool_specs(run_code_specs())
        calls = extract_supplemental_calls_from_text(text, specs)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["function"]["name"], "edit")
        args = json.loads(calls[0]["function"]["arguments"])
        self.assertEqual(args["file_path"], "a.txt")

    def test_repair_does_not_guess_unknown_tool(self):
        text = 'name":"rm_rf","arguments":{"path":"/"}}'
        specs = extend_tool_specs(run_code_specs())
        self.assertEqual(extract_supplemental_calls_from_text(text, specs), [])



    def test_computer_alias_web_fetch_maps_to_nested_tool(self):
        calls = [{"id": "c", "function": "connections.computer.webFetch", "args": {"url": "https://example.com"}, "autoLoadPhase": ""}]
        translated = translate_native_tool_calls(calls, run_code_specs())
        code = json.loads(translated[0]["function"]["arguments"])["code"]
        self.assertIn("tools.web_fetch", code)
        self.assertIn("example.com", code)

    def test_computer_alias_web_search_maps_to_nested_tool(self):
        calls = [{"id": "c", "function": "connections.computer.webSearch", "args": {"query": "DeepSeek"}, "autoLoadPhase": ""}]
        translated = translate_native_tool_calls(calls, run_code_specs())
        code = json.loads(translated[0]["function"]["arguments"])["code"]
        self.assertIn("tools.web_search", code)
        self.assertIn("DeepSeek", code)

    def test_computer_alias_read_image_maps_to_nested_tool(self):
        calls = [{"id": "c", "function": "connections.computer.readImage", "args": {"path": "a.png"}, "autoLoadPhase": ""}]
        translated = translate_native_tool_calls(calls, run_code_specs())
        code = json.loads(translated[0]["function"]["arguments"])["code"]
        self.assertIn("tools.read_image", code)
        self.assertIn("a.png", code)



    def test_unified_search_maps_to_web_search(self):
        calls = [{"id": "c", "function": "connections.search.unifiedSearch", "args": {"question": "DeepSeek news", "keywords": "DeepSeek"}, "autoLoadPhase": ""}]
        translated = translate_native_tool_calls(calls, run_code_specs())
        self.assertEqual(translated[0]["function"]["name"], "run_code")
        code = json.loads(translated[0]["function"]["arguments"])["code"]
        self.assertIn("tools.web_search", code)
        self.assertIn("DeepSeek", code)



    def test_computer_alias_read_files_maps_to_multiple_reads(self):
        calls = [{"id": "c", "function": "connections.computer.readFiles", "args": {"files": ["a.txt", "b.txt"]}, "autoLoadPhase": ""}]
        translated = translate_native_tool_calls(calls, run_code_specs())
        self.assertEqual(len(translated), 2)
        for call in translated:
            self.assertEqual(call["function"]["name"], "run_code")
            code = json.loads(call["function"]["arguments"])["code"]
            self.assertIn("tools.read", code)

    def test_computer_alias_list_dir_variant_maps_to_glob(self):
        calls = [{"id": "c", "function": "connections.computer.listDir", "args": {"dir": "."}, "autoLoadPhase": ""}]
        translated = translate_native_tool_calls(calls, run_code_specs())
        code = json.loads(translated[0]["function"]["arguments"])["code"]
        self.assertIn("tools.glob", code)



    def test_orchestration_alias_prompt_and_translation(self):
        import os
        from unittest.mock import patch
        with patch.dict(os.environ, {"HARNESS_SUPPLEMENTAL_TOOLS": "todo_write,subagent"}):
            prompt = build_supplemental_prompt(run_code_specs())
        self.assertIn("connections.computer.todoWrite", prompt)
        self.assertIn("connections.computer.subagent", prompt)
        self.assertNotIn("connections.computer.pwsh", prompt)
        calls = [{"id": "c", "function": "connections.computer.todoWrite", "args": {"todos": []}, "autoLoadPhase": ""}]
        translated = translate_native_tool_calls(calls, run_code_specs())
        self.assertEqual(translated[0]["function"]["name"], "run_code")
        code = json.loads(translated[0]["function"]["arguments"])["code"]
        self.assertIn("tools.todo_write", code)


if __name__ == "__main__":
    unittest.main()
