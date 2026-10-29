"""Fixture-driven tests for the structured Notion NDJSON reducer + provider integration."""
import json
import os
import sys
import unittest
from contextlib import contextmanager

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.adapters.notion_native import extract_native_tool_calls
from app.providers.notion_provider import (
    NotionAIProvider,
    _THREAD_CACHE,
    _stream_reducer_enabled,
    pinned_thread_account_id,
)
from app.providers.notion_stream import NotionStreamReducer

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "notion_ndjson")
RUN_CODE_TOOL = {
    "type": "function",
    "function": {
        "name": "run_code",
        "description": "Execute a program",
        "parameters": {"type": "object", "properties": {"code": {"type": "string"}}, "required": ["code"]},
    },
}


@contextmanager
def reducer_env(value):
    old = os.environ.get("NOTION_STREAM_REDUCER")
    if value is None:
        os.environ.pop("NOTION_STREAM_REDUCER", None)
    else:
        os.environ["NOTION_STREAM_REDUCER"] = value
    try:
        yield
    finally:
        if old is None:
            os.environ.pop("NOTION_STREAM_REDUCER", None)
        else:
            os.environ["NOTION_STREAM_REDUCER"] = old


def fixture_names():
    names = []
    for filename in sorted(os.listdir(FIXTURES)):
        if filename.endswith(".jsonl") and not filename.startswith("empty_body"):
            names.append(filename[: -len(".jsonl")])
    return names


def load_fixture(name):
    lines = []
    with open(os.path.join(FIXTURES, name + ".jsonl"), "r", encoding="utf-8") as handle:
        for raw in handle:
            raw = raw.rstrip("\n")
            if raw:
                lines.append(raw)
    with open(os.path.join(FIXTURES, name + ".expected.json"), "r", encoding="utf-8") as handle:
        expected = json.load(handle)
    return lines, expected


def run_reducer(name):
    lines, expected = load_fixture(name)
    reducer = NotionStreamReducer(since_ms=expected.get("reducer_since_ms"))
    for raw in lines:
        reducer.consume_line(raw)
    return lines, expected, reducer


class _FakeResponse:
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
    def __init__(self, responses):
        self.responses = [list(item) for item in responses]
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append({"url": url, "payload": kwargs.get("json")})
        lines = self.responses.pop(0) if self.responses else []
        return _FakeResponse(lines)


def _account(account_id="acc-a"):
    return {
        "id": account_id,
        "token_v2": "token-" + account_id,
        "space_id": "space",
        "user_id": "user",
        "user_name": account_id,
        "user_email": account_id + "@example.com",
    }


def make_provider(responses=None, lines=None):
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
        for message in reversed(request_data.get("messages") or []):
            if message.get("role") == "user":
                user_text = str(message.get("content") or "")
                break
        payloads.append({"thread_id": thread_id, "last_user": user_text})
        return {"transcript": []}

    provider._create_thread = fake_create_thread
    provider._prepare_payload = fake_prepare
    if responses is not None:
        session = _SequencedSession(responses)
    else:
        session = _FakeSession(lines or [])
    provider._session_for = lambda account=None: session
    return provider, created, payloads


async def collect_stream(result):
    chunks = []
    async for chunk in result.body_iterator:
        chunks.append(chunk.decode("utf-8", "ignore") if isinstance(chunk, (bytes, bytearray)) else str(chunk))
    return "".join(chunks)


def stream_content(raw_sse):
    parts = []
    for line in raw_sse.splitlines():
        line = line.strip()
        if not line.startswith("data: ") or line == "data: [DONE]":
            continue
        try:
            payload = json.loads(line[6:])
        except json.JSONDecodeError:
            continue
        for choice in payload.get("choices") or []:
            delta = choice.get("delta") or {}
            if delta.get("content"):
                parts.append(delta["content"])
    return "".join(parts)


class ReducerFixtureTests(unittest.TestCase):
    def test_all_fixtures_roundtrip(self):
        names = fixture_names()
        self.assertGreaterEqual(len(names), 8)
        for name in names:
            with self.subTest(fixture=name):
                lines, expected, reducer = run_reducer(name)
                self.assertEqual(reducer.visible_text, expected["visible_text"])
                self.assertEqual(reducer.final_text, expected["final_text"])
                self.assertEqual(reducer.record_map_text, expected["record_map_text"])
                self.assertEqual(reducer.saw_record_map, expected["saw_record_map"])
                self.assertEqual(reducer.parsed_records, expected["parsed_records"])
                self.assertEqual(reducer.stats["thinking_chars"], expected["thinking_chars"])
                self.assertEqual(reducer.stats["unknown_op_count"], expected["unknown_op_count"])
                self.assertEqual(reducer.stats["mismatch"], expected["mismatch"])
                self.assertEqual(reducer.quota, expected["quota"])
                if expected.get("native_extract") is not None:
                    config = expected["native_extract"]
                    calls = extract_native_tool_calls(
                        lines, since_ms=config.get("since_ms"), exclude_ids=config.get("exclude_ids")
                    )
                    projected = [{"function": call.get("function"), "args": call.get("args")} for call in calls]
                    self.assertEqual(projected, expected["native_calls"])

    def test_thinking_is_tracked_but_never_visible(self):
        _, expected, reducer = run_reducer("thinking_and_text_split")
        self.assertIn("The user wants a post.", reducer.thinking_text)
        self.assertNotIn("The user", reducer.visible_text)
        self.assertNotIn("user wants", reducer.visible_text)
        self.assertEqual(reducer.visible_text, expected["visible_text"])

    def test_text_item_initial_content_is_kept(self):
        _, _, reducer = run_reducer("thinking_and_text_split")
        self.assertTrue(reducer.visible_text.startswith("## "))
        self.assertNotIn("post.", reducer.visible_text)

    def test_multiple_text_blocks_get_blank_line(self):
        _, _, reducer = run_reducer("multi_text_items")
        self.assertEqual(reducer.visible_text, "第一段\n\n第二段")

    def test_record_map_is_authoritative_and_warns(self):
        with self.assertLogs("app.providers.notion_stream", level="WARNING") as captured:
            _, expected, reducer = run_reducer("record_map_authoritative_mismatch")
        self.assertIsNotNone(reducer.stats["mismatch"])
        self.assertEqual(reducer.final_text, expected["record_map_text"])
        self.assertTrue(any("record-map" in line for line in captured.output))

    def test_since_ms_filters_old_record_map_steps(self):
        old = json.dumps({"type": "record-map", "recordMap": {"thread_message": {"m": {"value": {"value": {"step": {
            "type": "agent-inference", "startedAt": 500,
            "value": [{"type": "text", "content": "旧回答"}]}}}}}}})
        new = json.dumps({"type": "record-map", "recordMap": {"thread_message": {"m": {"value": {"value": {"step": {
            "type": "agent-inference", "startedAt": 2000,
            "value": [{"type": "text", "content": "新回答"}]}}}}}}})
        reducer = NotionStreamReducer(since_ms=1000)
        reducer.consume_line(old)
        reducer.consume_line(new)
        self.assertEqual(reducer.record_map_text, "新回答")

    def test_unknown_operations_are_counted_not_raised(self):
        reducer = NotionStreamReducer()
        reducer.consume_line(json.dumps({"type": "patch", "v": [{"o": "z", "p": "/weird", "v": 1}]}))
        reducer.consume_line(json.dumps({"type": "patch", "v": [{"o": "x", "p": "/s/9/unknown", "v": "x"}]}))
        self.assertGreaterEqual(reducer.stats["unknown_op_count"], 2)

    def test_quota_event(self):
        _, expected, reducer = run_reducer("quota_patch_start")
        self.assertEqual(reducer.quota, expected["quota"])
        events = reducer.consume_line(json.dumps({"type": "patch-start", "data": {"s": []}}))
        self.assertEqual(events, [])

    def test_quota_event_from_patch(self):
        line = json.dumps({"type": "patch", "v": [{"o": "a", "p": "/s/-", "v": {
            "type": "premium-feature-unavailable",
            "featureAvailability": {"limit": {"current": 10, "total": 5}}}}]})
        reducer = NotionStreamReducer()
        events = reducer.consume_line(line)
        self.assertEqual(events[0].kind, "quota_exceeded")
        self.assertEqual(events[0].data, {"current": 10, "total": 5})

    def test_legacy_value_append_is_still_text(self):
        reducer = NotionStreamReducer()
        reducer.consume_line(json.dumps({"type": "patch", "v": [
            {"o": "a", "p": "/s/-", "v": {"type": "markdown-chat", "id": "m"}}]}))
        events = reducer.consume_line(json.dumps({"type": "patch", "v": [{"o": "x", "p": "/s/0/value", "v": "hello"}]}))
        self.assertEqual(events[-1].kind, "final_text")
        self.assertEqual(reducer.final_text, "hello")

    def test_invalid_lines_do_not_raise(self):
        reducer = NotionStreamReducer()
        reducer.consume_line("not json")
        reducer.consume_line("[1,2,3]")
        self.assertEqual(reducer.stats["invalid_lines"], 2)
        self.assertEqual(reducer.parsed_records, 0)

    def test_thinking_only_response_has_empty_visible_text(self):
        reducer = NotionStreamReducer()
        reducer.consume_line(json.dumps({"type": "patch", "v": [{"o": "a", "p": "/s/-", "v": {
            "type": "agent-inference", "value": [{"type": "thinking", "content": "hidden"}]}}]}))
        reducer.consume_line(json.dumps({"type": "patch", "v": [{"o": "x", "p": "/s/2/value/0/content", "v": " more"}]}))
        self.assertEqual(reducer.visible_text, "")
        self.assertEqual(reducer.final_text, "")
        self.assertIn("hidden more", reducer.thinking_text)

    def test_stream_reducer_switch(self):
        with reducer_env(None):
            self.assertTrue(_stream_reducer_enabled())
        with reducer_env("0"):
            self.assertFalse(_stream_reducer_enabled())


class StepMetadataTests(unittest.TestCase):
    def test_step_metadata_ops_are_ignored(self):
        reducer = NotionStreamReducer()
        line = json.dumps({
            "type": "patch",
            "v": [
                {"o": "a", "p": "/s/0/finishedAt", "v": 123},
                {"o": "a", "p": "/s/0/inputTokens", "v": 10},
                {"o": "a", "p": "/s/0/outputTokens", "v": 2},
            ],
        })
        reducer.consume_line(line)
        self.assertEqual(reducer.stats["unknown_op_count"], 0)
        self.assertEqual(reducer.visible_text, "")


class ProviderReducerIntegrationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _THREAD_CACHE.clear()

    def tearDown(self):
        _THREAD_CACHE.clear()

    async def test_stream_chat_returns_clean_text(self):
        lines, expected = load_fixture("thinking_and_text_split")
        provider, created, _ = make_provider(lines=lines)
        request_data = {"model": "albuquerque-quinn", "messages": [{"role": "user", "content": "写个笔记"}],
                        "stream": True, "tools": []}
        result = await provider.chat_completion(request_data, account=_account())
        raw = await collect_stream(result)
        content = stream_content(raw)
        self.assertEqual(content, expected["final_text"])
        self.assertNotIn("user wants", content)
        self.assertEqual(len(created), 1)

    async def test_nonstream_mismatch_keeps_streamed_text(self):
        lines, expected = load_fixture("record_map_authoritative_mismatch")
        provider, _, _ = make_provider(lines=lines)
        request_data = {"model": "albuquerque-quinn", "messages": [{"role": "user", "content": "hi"}],
                        "stream": False, "tools": []}
        with self.assertLogs("app.providers.notion_stream", level="WARNING"):
            result = await provider.chat_completion(request_data, account=_account())
        body = json.loads(result.body)
        # Deltas already reached the client; keep the streamed text instead of
        # duplicating the record-map snapshot.
        self.assertEqual(body["choices"][0]["message"]["content"], expected["visible_text"])

    async def test_buffered_dsh_client_gets_record_map_text(self):
        lines, expected = load_fixture("record_map_authoritative_mismatch")
        provider, _, _ = make_provider(lines=lines)
        request_data = {
            "model": "albuquerque-quinn",
            "messages": [{"role": "user", "content": "hi"}],
            "stream": False,
            "tools": [RUN_CODE_TOOL],
            "_harness": "dsh",
        }
        with self.assertLogs("app.providers.notion_stream", level="WARNING"):
            result = await provider.chat_completion(request_data, account=_account())
        body = json.loads(result.body)
        # DSH text is buffered (stream_allowed=False), so the authoritative
        # record-map text is used.
        self.assertEqual(body["choices"][0]["message"]["content"], expected["record_map_text"])

    async def test_truncated_fixture_retries_with_hint(self):
        truncated_lines, _ = load_fixture("truncated_no_record_map")
        good_lines, expected = load_fixture("thinking_and_text_split")
        provider, created, payloads = make_provider(responses=[truncated_lines, good_lines])
        request_data = {
            "model": "albuquerque-quinn",
            "messages": [{"role": "user", "content": "solve this"}],
            "stream": False,
            "tools": [RUN_CODE_TOOL],
            "_harness": "dsh",
        }
        result = await provider.chat_completion(request_data, account=_account())
        body = json.loads(result.body)
        self.assertEqual(body["choices"][0]["message"]["content"], expected["final_text"])
        self.assertEqual(len(created), 2)
        self.assertIn("[System note:", payloads[-1]["last_user"])

    async def test_empty_body_raises_typed_exception_after_retries(self):
        provider, created, _ = make_provider(responses=[[], []])
        request_data = {"model": "albuquerque-quinn", "messages": [{"role": "user", "content": "hi"}],
                        "stream": False, "tools": []}
        with self.assertRaises(RuntimeError) as captured:
            await provider.chat_completion(request_data, account=_account())
        self.assertIn("未返回有效响应", str(captured.exception))
        self.assertEqual(len(created), 3)
        self.assertEqual(pinned_thread_account_id([{"role": "user", "content": "hi"}]), "")

    async def test_legacy_parser_fallback(self):
        lines, _ = load_fixture("markdown_chat_only")
        provider, _, _ = make_provider(lines=lines)
        request_data = {"model": "albuquerque-quinn", "messages": [{"role": "user", "content": "hi"}],
                        "stream": False, "tools": []}
        with reducer_env("0"):
            result = await provider.chat_completion(request_data, account=_account())
        body = json.loads(result.body)
        self.assertEqual(body["choices"][0]["message"]["content"], "hello")

    async def test_usage_summary_is_mapped(self):
        lines, expected = load_fixture("usage_summary")
        provider, _, _ = make_provider(lines=lines)
        request_data = {"model": "albuquerque-quinn", "messages": [{"role": "user", "content": "hi"}],
                        "stream": False, "tools": []}
        result = await provider.chat_completion(request_data, account=_account())
        body = json.loads(result.body)
        usage = body["usage"]
        # OpenAI usage reports client-visible tokens (for context accounting);
        # Notion's internal agent-loop totals are kept in the notion_* fields.
        self.assertEqual(usage["notion_input_tokens"], expected["provider_usage"]["input_tokens"])
        self.assertEqual(usage["notion_output_tokens"], expected["provider_usage"]["output_tokens"])
        self.assertGreater(usage["prompt_tokens"], 0)
        self.assertLess(usage["prompt_tokens"], 1000)
        self.assertEqual(usage["total_tokens"], usage["prompt_tokens"] + usage["completion_tokens"])
        self.assertEqual(usage["credits_used"], 1.5)


class ClientFacingOutputTests(unittest.TestCase):
    def test_clean_content_preserves_markdown_newlines(self):
        from app.providers.notion_provider import NotionAIProvider

        provider = NotionAIProvider()
        text = "以下是今日新闻：\n\n### 国内要闻\n- 朝闻天下\n- 共同关注\n\n### 国际动态\n- 今日环球"
        cleaned = provider._clean_content(text, "")
        self.assertIn("\n\n### 国内要闻", cleaned)
        self.assertIn("\n- 朝闻天下", cleaned)
        self.assertNotIn("： ###", cleaned)
        self.assertGreaterEqual(cleaned.count("\n"), 5)

    def test_clean_content_collapses_horizontal_spaces_only(self):
        from app.providers.notion_provider import NotionAIProvider

        provider = NotionAIProvider()
        cleaned = provider._clean_content("a ,   .\nb\tc", "")
        self.assertNotIn("   ", cleaned)
        self.assertIn("\nb c", cleaned)

    def test_prompt_token_estimate_is_client_visible(self):
        from app.providers.notion_provider import _estimate_prompt_tokens

        messages = [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": "帮我搜索下今天的新闻。"},
        ]
        estimate = _estimate_prompt_tokens(messages, tools=None)
        self.assertGreater(estimate, 3)
        self.assertLess(estimate, 200)


if __name__ == "__main__":
    unittest.main()
