"""Tests for Notion official tool-result / tool_use replay alignment."""
import json
import os
import sys
import unittest
from contextlib import contextmanager

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.adapters.notion_native import _COMPUTER_TOOL_MAP, _OFFICIAL_COMPUTER_FUNCTIONS
from app.providers.notion_provider import (
    NotionAIProvider,
    _THREAD_CACHE,
    _flatten_tool_results_enabled,
    _flatten_tool_results_mode,
    _native_tool_use_replay_enabled,
    _should_flatten_tool_results,
    pinned_thread_account_id,
)

_ENV_KEYS = (
    "NOTION_FLATTEN_TOOL_RESULTS",
    "NOTION_NATIVE_TOOL_REPLAY",
    "NOTION_NATIVE_TOOL_USE_REPLAY",
    "NOTION_THREAD_REUSE",
)


@contextmanager
def notion_env(**values):
    """Set the replay switches for one test and restore the previous values."""
    saved = {key: os.environ.get(key) for key in _ENV_KEYS}
    for key in _ENV_KEYS:
        value = values.get(key)
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
    try:
        yield
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def account():
    return {"cookie": "c", "space_id": "s", "user_id": "u"}


def tool_call(call_id, name, args):
    return {
        "id": call_id,
        "type": "function",
        "function": {"name": name, "arguments": json.dumps(args, ensure_ascii=False)},
    }


def history():
    return [
        {"role": "system", "content": "system"},
        {"role": "user", "content": "search and save"},
        {"role": "assistant", "content": "", "tool_calls": [tool_call("call_1", "web_search", {"queries": ["DeepSeek"]})]},
        {"role": "tool", "tool_call_id": "call_1", "content": "RESULT-ONE"},
        {"role": "assistant", "content": "saved"},
        {"role": "user", "content": "headline?"},
    ]


def prepare(mode="native-only", disable_tool_use=False, native_replay="1", tool_use_replay="1"):
    with notion_env(
        NOTION_FLATTEN_TOOL_RESULTS=mode,
        NOTION_NATIVE_TOOL_REPLAY=native_replay,
        NOTION_NATIVE_TOOL_USE_REPLAY=tool_use_replay,
    ):
        provider = NotionAIProvider.__new__(NotionAIProvider)
        request_data = {"messages": history()}
        provider._preprocess_tool_history(request_data)
        if disable_tool_use:
            request_data["_disable_tool_use_replay"] = True
        payload = provider._prepare_payload(request_data, "thread", "albuquerque-quinn", "workflow", account())
    return request_data, payload


def transcript_kinds(payload):
    kinds = []
    for entry in payload["transcript"]:
        kind = entry.get("type")
        if kind in ("user", "result", "agent-inference", "agent-tool-result"):
            kinds.append(kind)
    return kinds


class FlattenModeTests(unittest.TestCase):
    def test_default_is_always(self):
        with notion_env():
            self.assertEqual(_flatten_tool_results_mode(), "always")
            self.assertTrue(_flatten_tool_results_enabled())
            self.assertTrue(_should_flatten_tool_results())

    def test_never_mode(self):
        with notion_env(NOTION_FLATTEN_TOOL_RESULTS="0"):
            self.assertEqual(_flatten_tool_results_mode(), "never")
            self.assertFalse(_flatten_tool_results_enabled())
            self.assertFalse(_should_flatten_tool_results())

    def test_native_only_skips_flatten_when_replay_on(self):
        with notion_env(NOTION_FLATTEN_TOOL_RESULTS="native-only", NOTION_NATIVE_TOOL_REPLAY="1"):
            self.assertEqual(_flatten_tool_results_mode(), "native-only")
            self.assertTrue(_flatten_tool_results_enabled())
            self.assertFalse(_should_flatten_tool_results())

    def test_native_only_falls_back_to_flatten_when_replay_off(self):
        with notion_env(NOTION_FLATTEN_TOOL_RESULTS="native-only", NOTION_NATIVE_TOOL_REPLAY="0"):
            self.assertTrue(_should_flatten_tool_results())

    def test_unknown_value_keeps_compat(self):
        with notion_env(NOTION_FLATTEN_TOOL_RESULTS="bogus"):
            self.assertEqual(_flatten_tool_results_mode(), "always")

    def test_tool_use_replay_switch(self):
        with notion_env(NOTION_NATIVE_TOOL_REPLAY="1", NOTION_NATIVE_TOOL_USE_REPLAY=None):
            self.assertTrue(_native_tool_use_replay_enabled())
        with notion_env(NOTION_NATIVE_TOOL_REPLAY="1", NOTION_NATIVE_TOOL_USE_REPLAY="0"):
            self.assertFalse(_native_tool_use_replay_enabled())
        with notion_env(NOTION_NATIVE_TOOL_REPLAY="0", NOTION_NATIVE_TOOL_USE_REPLAY="1"):
            self.assertFalse(_native_tool_use_replay_enabled())


class MappingTests(unittest.TestCase):
    def test_official_mapping_matches_computer_alias_table(self):
        for tool_name, function in _OFFICIAL_COMPUTER_FUNCTIONS.items():
            self.assertTrue(function.startswith("connections.computer."))
            suffix = function[len("connections.computer."):]
            self.assertIn(suffix, _COMPUTER_TOOL_MAP)
            self.assertEqual(_COMPUTER_TOOL_MAP[suffix]["tool"], tool_name)


class NativeToolUseEntryTests(unittest.TestCase):
    def test_write_entry_shape(self):
        entry = NotionAIProvider._build_native_tool_use_entry(
            [tool_call("call_1", "write", {"file_path": "a.html", "content": "<html>"})]
        )
        self.assertEqual(entry["type"], "agent-inference")
        self.assertEqual(len(entry["value"]), 1)
        item = entry["value"][0]
        self.assertEqual(item["type"], "tool_use")
        self.assertEqual(item["name"], "callFunction")
        self.assertEqual(item["id"], "call_1")
        self.assertFalse(item["isCustomToolCall"])
        payload = json.loads(item["content"])
        self.assertEqual(payload["function"], "connections.computer.writeFile")
        self.assertEqual(payload["args"], {"file_path": "a.html", "content": "<html>"})

    def test_multiple_calls_share_one_inference(self):
        entry = NotionAIProvider._build_native_tool_use_entry([
            tool_call("call_1", "web_search", {"queries": ["q"]}),
            tool_call("call_2", "write", {"file_path": "a.txt", "content": "x"}),
        ])
        self.assertEqual(len(entry["value"]), 2)
        functions = [json.loads(item["content"])["function"] for item in entry["value"]]
        self.assertEqual(functions, ["connections.computer.webSearch", "connections.computer.writeFile"])

    def test_unknown_tool_is_not_guessed(self):
        self.assertIsNone(
            NotionAIProvider._build_native_tool_use_entry([tool_call("call_1", "todo_write", {"todos": []})])
        )

    def test_invalid_arguments_are_not_replayed(self):
        bad = {"id": "call_1", "type": "function", "function": {"name": "write", "arguments": "{"}}
        self.assertIsNone(NotionAIProvider._build_native_tool_use_entry([bad]))


class PreprocessTests(unittest.TestCase):
    def test_native_only_drops_user_summary(self):
        request_data, payload = prepare("native-only")
        # system, user, assistant(tool_use), result group, assistant, user
        group = [m for m in request_data["messages"] if m.get("_tool_result_group")]
        self.assertEqual(len(group), 1)
        self.assertNotIn("role", group[0])
        self.assertEqual(group[0]["_native_entries"][0]["type"], "agent-tool-result")
        assistant = [m for m in request_data["messages"] if m.get("tool_calls")][0]
        self.assertIn("_native_tool_use_entry", assistant)
        self.assertEqual(assistant["content"], "")
        self.assertEqual(transcript_kinds(payload), ["user", "agent-inference", "agent-tool-result", "result", "user"])
        self.assertNotIn("Tool result executed by the harness", json.dumps(payload["transcript"], ensure_ascii=False))
        self.assertTrue(request_data["_tool_use_replay_used"])

    def test_always_keeps_user_summary_and_native_result(self):
        _, payload = prepare("always")
        joined = json.dumps(payload["transcript"], ensure_ascii=False)
        self.assertIn("Tool result executed by the harness", joined)
        self.assertEqual(transcript_kinds(payload).count("agent-tool-result"), 1)
        self.assertEqual(transcript_kinds(payload).count("agent-inference"), 1)

    def test_never_keeps_native_result_only(self):
        _, payload = prepare("never")
        joined = json.dumps(payload["transcript"], ensure_ascii=False)
        self.assertNotIn("Tool result executed by the harness", joined)
        self.assertEqual(transcript_kinds(payload), ["user", "agent-inference", "agent-tool-result", "result", "user"])

    def test_disabled_tool_use_keeps_description(self):
        _, payload = prepare("native-only", disable_tool_use=True)
        joined = json.dumps(payload["transcript"], ensure_ascii=False)
        self.assertIn("I called web_search", joined)
        self.assertNotIn('"type": "agent-inference"', joined)
        self.assertEqual(transcript_kinds(payload).count("agent-tool-result"), 1)

    def test_tool_use_switch_off_keeps_description(self):
        request_data, payload = prepare("native-only", tool_use_replay="0")
        assistant = [m for m in request_data["messages"] if m.get("tool_calls")][0]
        self.assertNotIn("_native_tool_use_entry", assistant)
        self.assertIn("I called web_search", assistant["content"])
        self.assertNotIn('"type": "agent-inference"', json.dumps(payload["transcript"], ensure_ascii=False))

    def test_unknown_tool_keeps_description_but_native_result(self):
        with notion_env(
            NOTION_FLATTEN_TOOL_RESULTS="native-only",
            NOTION_NATIVE_TOOL_REPLAY="1",
            NOTION_NATIVE_TOOL_USE_REPLAY="1",
        ):
            provider = NotionAIProvider.__new__(NotionAIProvider)
            request_data = {"messages": [
                {"role": "user", "content": "plan"},
                {"role": "assistant", "content": "", "tool_calls": [tool_call("call_9", "todo_write", {"todos": []})]},
                {"role": "tool", "tool_call_id": "call_9", "content": "ok"},
            ]}
            provider._preprocess_tool_history(request_data)
            payload = provider._prepare_payload(request_data, "thread", "albuquerque-quinn", "workflow", account())
        joined = json.dumps(payload["transcript"], ensure_ascii=False)
        self.assertIn("I called todo_write", joined)
        self.assertNotIn('"type": "agent-inference"', joined)
        self.assertIn("connections.computer.todo_write", joined)

    def test_replay_ids_are_recorded_for_response_filtering(self):
        request_data, _ = prepare("never")
        replay_ids = set(request_data["_native_replay_ids"])
        self.assertIn("call_1", replay_ids)
        group = [m for m in request_data["messages"] if m.get("_tool_result_group")][0]
        result_id = group["_native_entries"][0]["id"]
        self.assertIn(result_id, replay_ids)
        # Without the flatten summary ('never') the same holds.
        request_data, _ = prepare("always")
        self.assertIn("call_1", set(request_data["_native_replay_ids"]))

    def test_replayed_echo_does_not_become_new_tool_call(self):
        """Regression: record-map echoes grew tool calls exponentially."""
        from app.adapters.notion_native import extract_native_tool_calls
        request_data, _ = prepare("never")
        replay_ids = request_data["_native_replay_ids"]
        helper = [m for m in request_data["messages"] if m.get("tool_calls")][0]
        tool_use = helper["_native_tool_use_entry"]["value"][0]
        group = [m for m in request_data["messages"] if m.get("_tool_result_group")][0]
        replayed_result = group["_native_entries"][0]
        echo_line = json.dumps({
            "type": "record-map",
            "recordMap": {"thread_message": {
                "a": {"value": {"value": {"step": {"type": "agent-inference", "value": [tool_use]}}}},
                "b": {"value": {"value": {"step": dict(replayed_result, startedAt=1)}}},
            }},
        })
        new_line = json.dumps({
            "type": "patch",
            "v": [{"o": "a", "p": "/s/-", "v": {
                "type": "agent-tool-result",
                "id": "toolu_new",
                "startedAt": 9999,
                "input": {"function": "connections.computer.writeFile", "args": {"path": "a.txt"}},
                "result": {"output": "ok"},
                "state": "applied",
                "autoLoadPhase": "",
            }}],
        })
        calls = extract_native_tool_calls([echo_line, new_line], exclude_ids=replay_ids)
        self.assertEqual([c["id"] for c in calls], ["toolu_new"])

    def test_native_result_is_not_duplicated(self):
        _, payload = prepare("native-only")
        results = [e for e in payload["transcript"] if e.get("type") == "agent-tool-result"]
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["result"]["output"], "RESULT-ONE")
        self.assertEqual(payload["transcript"][-1]["type"], "user")


class ReplayRejectionTests(unittest.TestCase):
    class FakeResponse:
        def __init__(self, status):
            self.status_code = status

    class FakeError(Exception):
        def __init__(self, status=None):
            self.response = ReplayRejectionTests.FakeResponse(status) if status is not None else None

    def test_client_rejections_are_retried(self):
        for status in (400, 422):
            self.assertTrue(NotionAIProvider._is_tool_use_replay_rejection(self.FakeError(status)))

    def test_transport_quota_and_auth_errors_are_not_replay_rejections(self):
        for status in (401, 403, 429, 500, 502):
            self.assertFalse(NotionAIProvider._is_tool_use_replay_rejection(self.FakeError(status)))
        self.assertFalse(NotionAIProvider._is_tool_use_replay_rejection(self.FakeError()))
        self.assertFalse(NotionAIProvider._is_tool_use_replay_rejection(Exception("boom")))


class _FakeResponse:
    """Minimal requests.Response stand-in for iter_lines()."""

    def __init__(self, lines, status=200):
        self._lines = list(lines)
        self.status_code = status

    def raise_for_status(self):
        return None

    def iter_lines(self):
        for line in self._lines:
            yield line.encode("utf-8") if isinstance(line, str) else line


class _FakeSession:
    def __init__(self, lines):
        self.lines = list(lines)
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append({"url": url, "payload": kwargs.get("json")})
        return _FakeResponse(self.lines)


class _SequencedSession:
    """Returns a different response body for each post() call."""

    def __init__(self, responses):
        self.responses = [list(item) for item in responses]
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append({"url": url, "payload": kwargs.get("json")})
        lines = self.responses.pop(0) if self.responses else []
        return _FakeResponse(lines)


def _account(account_id):
    return {
        "id": account_id,
        "token_v2": "token-" + account_id,
        "space_id": "space",
        "user_id": "user",
        "user_name": account_id,
        "user_email": account_id + "@example.com",
    }


_MAX_MARKDOWN_LINE = '{"type":"markdown-chat","value":"hello"}'
_RECORD_MAP_LINE = (
    '{"type":"record-map","recordMap":{"thread_message":{"m":{"value":{"value":{"step":'
    '{"type":"markdown-chat","value":"hello"}}}}}}}'
)
_PARTIAL_PATCH_LINE = '{"type":"patch","v":[{"o":"x","p":"/s/7/value/0/content","v":"partial reasoning text"}]}'
_RUN_CODE_TOOL = {
    "type": "function",
    "function": {
        "name": "run_code",
        "description": "Execute a program",
        "parameters": {
            "type": "object",
            "properties": {"code": {"type": "string"}},
            "required": ["code"],
        },
    },
}


class ThreadAffinityTests(unittest.IsolatedAsyncioTestCase):
    """A cached Notion thread must stay with the account that created it."""

    def setUp(self):
        _THREAD_CACHE.clear()
        notion_env_ctx = notion_env(NOTION_THREAD_REUSE="1")
        notion_env_ctx.__enter__()
        self.addCleanup(notion_env_ctx.__exit__, None, None, None)

    def tearDown(self):
        _THREAD_CACHE.clear()

    def _provider(self, response_lines=None, responses=None):
        provider = NotionAIProvider.__new__(NotionAIProvider)
        provider._scrapers = {}
        provider.api_endpoints = {
            "runInference": "https://example.invalid/runInferenceTranscript",
            "saveTransactions": "https://example.invalid/saveTransactionsFanout",
        }
        created = []
        payloads = []

        async def fake_create_thread(thread_type, account=None):
            thread_id = "thread-%d" % (len(created) + 1)
            created.append({"thread_id": thread_id, "account_id": (account or {}).get("id")})
            return thread_id

        def fake_prepare(request_data, thread_id, mapped_model, thread_type, account=None):
            user_text = ""
            for msg in reversed(request_data.get("messages") or []):
                if msg.get("role") == "user":
                    user_text = str(msg.get("content") or "")
                    break
            payloads.append({"thread_id": thread_id, "account_id": (account or {}).get("id"),
                             "last_user": user_text})
            if not request_data.get("_disable_tool_use_replay"):
                if any(m.get("_native_tool_use_entry") for m in request_data.get("messages") or []):
                    request_data["_tool_use_replay_used"] = True
            return {"transcript": []}

        provider._create_thread = fake_create_thread
        provider._prepare_payload = fake_prepare
        if responses is not None:
            shared_session = _SequencedSession(responses)
            provider._session_for = lambda account=None: shared_session
        else:
            provider._session_for = lambda account=None: _FakeSession(response_lines or [])
        return provider, created, payloads

    async def _chat(self, provider, account, messages):
        request_data = {
            "model": "albuquerque-quinn",
            "messages": messages,
            "stream": False,
            "tools": [],
        }
        await provider.chat_completion(request_data, account=account)
        return request_data

    async def test_same_account_reuses_thread_and_pins_account(self):
        provider, created, payloads = self._provider([_MAX_MARKDOWN_LINE])
        history = [{"role": "user", "content": "task"}, {"role": "assistant", "content": "ok"}]
        account = _account("acc-a")
        await self._chat(provider, account, [dict(m) for m in history])
        self.assertEqual(len(created), 1)
        self.assertEqual(pinned_thread_account_id(history), "acc-a")
        followup = history + [{"role": "user", "content": "followup"}]
        await self._chat(provider, account, [dict(m) for m in followup])
        self.assertEqual(len(created), 1)
        self.assertEqual([p["thread_id"] for p in payloads], ["thread-1", "thread-1"])

    async def test_other_account_gets_new_thread_and_repins(self):
        provider, created, payloads = self._provider([_MAX_MARKDOWN_LINE])
        history = [{"role": "user", "content": "task"}, {"role": "assistant", "content": "ok"}]
        await self._chat(provider, _account("acc-a"), [dict(m) for m in history])
        other = history + [{"role": "user", "content": "followup"}]
        await self._chat(provider, _account("acc-b"), [dict(m) for m in other])
        self.assertEqual(len(created), 2)
        self.assertEqual(created[1]["account_id"], "acc-b")
        self.assertEqual(payloads[-1]["thread_id"], "thread-2")
        self.assertEqual(pinned_thread_account_id(other), "acc-b")

    async def test_thread_reuse_opt_out_skips_pinning(self):
        provider, created, _ = self._provider([_MAX_MARKDOWN_LINE])
        history = [{"role": "user", "content": "task"}]
        with notion_env(NOTION_THREAD_REUSE="0"):
            await self._chat(provider, _account("acc-a"), [dict(m) for m in history])
            await self._chat(provider, _account("acc-a"), [dict(m) for m in history])
            self.assertEqual(pinned_thread_account_id(history), "")
        self.assertEqual(len(created), 2)

    async def test_empty_upstream_response_retries_with_fresh_threads(self):
        # Every attempt returns an empty body: two fresh-thread retries happen
        # before the request finally fails, and no unusable thread stays cached.
        provider, created, payloads = self._provider([])
        history = [{"role": "user", "content": "task"}, {"role": "assistant", "content": "ok"}]
        account = _account("acc-a")
        with self.assertRaises(RuntimeError):
            await self._chat(provider, account, [dict(m) for m in history])
        self.assertEqual(len(created), 3)
        self.assertEqual([p["thread_id"] for p in payloads], ["thread-1", "thread-2", "thread-3"])
        self.assertEqual(pinned_thread_account_id(history), "")
        with self.assertRaises(RuntimeError):
            await self._chat(provider, account, [dict(m) for m in history])
        self.assertEqual(len(created), 6)

    async def test_empty_response_retried_with_fresh_thread_succeeds(self):
        provider, created, payloads = self._provider(responses=[[], [_MAX_MARKDOWN_LINE]])
        history = [{"role": "user", "content": "task"}]
        account = _account("acc-a")
        request_data = {
            "model": "albuquerque-quinn",
            "messages": [dict(m) for m in history],
            "stream": False,
            "tools": [],
        }
        result = await provider.chat_completion(request_data, account=account)
        self.assertIsNotNone(result)
        self.assertEqual(len(created), 2)
        self.assertEqual([p["thread_id"] for p in payloads], ["thread-1", "thread-2"])
        # The retried (second) thread must be the cached one.
        self.assertEqual(pinned_thread_account_id(history), "acc-a")

    async def test_truncated_response_retried_with_concise_hint(self):
        # First stream ends without the trailing record-map (model ran out of
        # output budget mid-answer); a fresh thread + concise hint must recover.
        provider, created, payloads = self._provider(
            responses=[[_PARTIAL_PATCH_LINE], [_MAX_MARKDOWN_LINE, _RECORD_MAP_LINE]]
        )
        request_data = {
            "model": "albuquerque-quinn",
            "messages": [{"role": "user", "content": "solve this"}],
            "stream": False,
            "tools": [_RUN_CODE_TOOL],
            "_harness": "dsh",
        }
        await provider.chat_completion(request_data, account=_account("acc-a"))
        self.assertEqual(len(created), 2)
        self.assertEqual([p["thread_id"] for p in payloads], ["thread-1", "thread-2"])
        self.assertIn("[System note:", payloads[-1]["last_user"])
        self.assertIn("runCode", payloads[-1]["last_user"])

    async def test_truncated_response_retry_exhausted_raises(self):
        provider, created, _ = self._provider(
            responses=[[_PARTIAL_PATCH_LINE], [_PARTIAL_PATCH_LINE]]
        )
        request_data = {
            "model": "albuquerque-quinn",
            "messages": [{"role": "user", "content": "solve this"}],
            "stream": False,
            "tools": [_RUN_CODE_TOOL],
            "_harness": "dsh",
        }
        with self.assertRaises(RuntimeError):
            await provider.chat_completion(request_data, account=_account("acc-a"))
        self.assertEqual(len(created), 2)

    async def test_truncated_detection_skipped_for_non_dsh(self):
        # A markdown-chat response without record-map is still valid for plain
        # OpenAI clients; no retry must happen there.
        provider, created, payloads = self._provider(responses=[[_MAX_MARKDOWN_LINE]])
        request_data = {
            "model": "albuquerque-quinn",
            "messages": [{"role": "user", "content": "hello"}],
            "stream": False,
            "tools": [],
        }
        await provider.chat_completion(request_data, account=_account("acc-a"))
        self.assertEqual(len(created), 1)

    async def test_client_disconnect_clears_polluted_thread(self):
        # A stream aborted by the client leaves an incomplete turn in the Notion
        # thread; the next request must not reuse it (it answers an empty body).
        provider, created, _ = self._provider([_MAX_MARKDOWN_LINE])
        history = [{"role": "user", "content": "task"}]
        account = _account("acc-a")
        request_data = {
            "model": "albuquerque-quinn",
            "messages": [dict(m) for m in history],
            "stream": True,
            "tools": [],
        }
        result = await provider.chat_completion(request_data, account=account)
        body = result.body_iterator
        first_chunk = await body.__anext__()
        self.assertIn(b"assistant", first_chunk)
        self.assertEqual(len(created), 1)
        self.assertEqual(pinned_thread_account_id(history), "acc-a")
        await body.aclose()  # simulate client disconnect mid-stream
        self.assertEqual(pinned_thread_account_id(history), "")

    async def test_empty_response_second_retry_disables_tool_use_replay(self):
        provider, created, payloads = self._provider(
            responses=[[], [], [_MAX_MARKDOWN_LINE]]
        )
        account = _account("acc-a")
        request_data = {
            "model": "albuquerque-quinn",
            "messages": [
                {"role": "user", "content": "run code"},
                {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [{
                        "id": "call_1",
                        "type": "function",
                        "function": {"name": "run_code", "arguments": json.dumps({"code": "console.log(1)"})},
                    }],
                },
                {"role": "tool", "tool_call_id": "call_1", "content": "1"},
            ],
            "stream": False,
            "tools": [{
                "type": "function",
                "function": {
                    "name": "run_code",
                    "description": "Execute a program",
                    "parameters": {
                        "type": "object",
                        "properties": {"code": {"type": "string"}},
                        "required": ["code"],
                    },
                },
            }],
        }
        result = await provider.chat_completion(request_data, account=account)
        self.assertIsNotNone(result)
        self.assertEqual(len(created), 3)
        self.assertTrue(request_data.get("_tool_use_replay_used"))
        self.assertTrue(request_data.get("_disable_tool_use_replay"))


if __name__ == "__main__":
    unittest.main()
