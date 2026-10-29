"""Mock Notion upstream for deterministic agent-tool-result alignment e2e.

Notion's agent protocol lives in the runInferenceTranscript NDJSON stream, but
running every alignment-matrix case against the live service burns a 50-credit
account per turn. This mock replays the *official* shapes recorded from the real
service (agent-inference/tool_use, agent-tool-result, record-map echo) so the
real proxy + real DSH harness can be exercised end to end without consuming
Notion accounts.

Implemented with http.server on purpose: the proxy talks to Notion through
curl_cffi Chrome impersonation, which offers an h2c upgrade; h11-based servers
(FastAPI/uvicorn) drop the body of such requests, while http.server reads the
plain Content-Length body correctly.

Usage:
    python scripts/mock_notion_upstream.py --port 8099 \
        --scenarios e2e-workspace/matrix/matrix_cases.json \
        --record-dir e2e-workspace/matrix/mock-records

Point the proxy at it with NOTION_API_BASE=http://127.0.0.1:8099/api/v3.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List

CASE_RE = re.compile(r"MATRIX-CASE:([a-z0-9_\-]+)", re.IGNORECASE)
TITLE_MARKER = "Generate the session title"

_LOCK = threading.Lock()
_THREADS: Dict[str, Dict[str, Any]] = {}
_CONFIG: Dict[str, Any] = {
    "scenarios": {},
    "record_dir": "",
}


def _json_line(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _flatten(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "\n".join(_flatten(item) for item in value)
    if isinstance(value, dict):
        return "\n".join(_flatten(v) for v in value.values())
    return ""


def _user_text(payload: Dict[str, Any]) -> str:
    parts: List[str] = []
    for entry in payload.get("transcript") or []:
        if not isinstance(entry, dict):
            continue
        if entry.get("type") == "user":
            parts.append(_flatten(entry.get("value")))
        elif entry.get("type") == "user-injected":
            parts.append(_flatten(entry.get("actualMessage")))
    return "\n".join(parts)


def _title_request(payload: Dict[str, Any]) -> bool:
    return TITLE_MARKER in _user_text(payload)


def _thread(payload: Dict[str, Any]) -> Dict[str, Any]:
    thread_id = str(payload.get("threadId") or "")
    if not thread_id:
        thread_id = "mock-" + uuid.uuid4().hex
    state = _THREADS.get(thread_id)
    if state is None:
        state = {"thread_id": thread_id, "round": 0, "case": "", "requests": []}
        _THREADS[thread_id] = state
    return state


def _scenario_for_case(case: str) -> Dict[str, Any]:
    cases = _CONFIG["scenarios"].get("cases") or {}
    return cases.get(case) or _CONFIG["scenarios"].get("default") or {"rounds": [{"text": "MOCK-DEFAULT"}]}


def _tool_use_item(call: Dict[str, Any], now_ms: int) -> Dict[str, Any]:
    call_id = str(call.get("id") or ("toolu_" + uuid.uuid4().hex))
    call["_mock_id"] = call_id
    return {
        "type": "tool_use",
        "id": call_id,
        "name": "callFunction",
        "content": _json_line({"function": call.get("function"), "args": call.get("args") or {}}),
        "isCustomToolCall": False,
        "autofixedJson": False,
    }


def _result_step(call: Dict[str, Any], now_ms: int) -> Dict[str, Any]:
    return {
        "type": "agent-tool-result",
        "id": str(uuid.uuid4()),
        "toolCallId": call.get("_mock_id") or "",
        "input": {"function": call.get("function"), "args": call.get("args") or {}},
        "state": call.get("state") or "applied",
        "autoLoadPhase": call.get("auto_load") or "",
        "result": {"output": call.get("result") or "ok"},
        "startedAt": now_ms,
    }


def _echo_steps(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Reproduce Notion's record-map echo of the transcript we just replayed.

    The real service returns the whole thread in the trailing record-map; echoed
    tool_use items keep the OpenAI call ids but lose startedAt, which used to be
    re-emitted as new tool calls (the runaway-echo regression).
    """
    steps: List[Dict[str, Any]] = []
    for entry in payload.get("transcript") or []:
        if isinstance(entry, dict) and entry.get("type") in ("agent-inference", "agent-tool-result"):
            steps.append(dict(entry))
    return steps


def _round_lines(state: Dict[str, Any], payload: Dict[str, Any]) -> List[str]:
    case = state["case"]
    rounds = _scenario_for_case(case).get("rounds") or [{"text": "MOCK-DEFAULT"}]
    # state["round"] is the 1-based number of the current request.
    spec = rounds[min(state["round"] - 1, len(rounds) - 1)]
    now_ms = int(time.time() * 1000)
    lines: List[str] = []
    new_steps: List[Dict[str, Any]] = []

    native_calls = [dict(call) for call in (spec.get("native") or [])]
    tool_use_items = []
    for call in native_calls:
        if not call.get("auto_load"):
            tool_use_items.append(_tool_use_item(call, now_ms))
    if tool_use_items:
        inference = {
            "type": "agent-inference",
            "id": str(uuid.uuid4()),
            "value": tool_use_items,
            "startedAt": now_ms,
        }
        lines.append(_json_line({"type": "patch", "v": [{"o": "a", "p": "/s/-", "v": inference}]}))
        new_steps.append(inference)
    for call in native_calls:
        step = _result_step(call, now_ms)
        lines.append(_json_line({"type": "patch", "v": [{"o": "a", "p": "/s/-", "v": step}]}))
        new_steps.append(step)
    text = spec.get("text")
    if text:
        lines.append(_json_line({"type": "markdown-chat", "value": text}))

    usage = dict(spec.get("usage") or {"input_tokens": 900, "output_tokens": 60})
    usage["credits_by_type_unit"] = {"basic_ai_credits": 1, "exempt_ai_credits": 0}
    lines.append(_json_line({"type": "patch", "v": [{"v": {"usage_summary": usage}}]}))

    for raw in spec.get("raw_lines") or []:
        lines.append(raw if isinstance(raw, str) else _json_line(raw))

    thread_messages: Dict[str, Any] = {}
    for idx, step in enumerate(_echo_steps(payload)):
        thread_messages["echo%d" % idx] = {"value": {"value": {"step": step}}}
    for idx, step in enumerate(new_steps):
        thread_messages["new%d" % idx] = {"value": {"value": {"step": step}}}
    lines.append(_json_line({"type": "record-map", "recordMap": {"thread_message": thread_messages}}))
    return lines


def _record_request(state: Dict[str, Any], payload: Dict[str, Any], lines: List[str], round_no: int) -> None:
    record_dir = _CONFIG.get("record_dir") or ""
    if not record_dir:
        return
    os.makedirs(record_dir, exist_ok=True)
    stamp = time.strftime("%H%M%S")
    base = os.path.join(record_dir, "%s__r%02d__%s" % (state["thread_id"][:8], round_no, stamp))
    entry = {"thread_id": state["thread_id"], "round": round_no, "case": state["case"],
             "payload": payload, "response_lines": lines}
    with open(base + ".json", "w", encoding="utf-8") as handle:
        json.dump(entry, handle, ensure_ascii=False, indent=1)
    transcript = payload.get("transcript") or []
    with open(os.path.join(record_dir, "index.jsonl"), "a", encoding="utf-8") as handle:
        handle.write(_json_line({
            "thread_id": state["thread_id"],
            "round": round_no,
            "case": state["case"],
            "base": os.path.basename(base),
            "transcript_types": [e.get("type") for e in transcript if isinstance(e, dict)],
            "replayed_tool_use_ids": [
                item.get("id")
                for e in transcript if isinstance(e, dict) and e.get("type") == "agent-inference"
                for item in (e.get("value") or []) if isinstance(item, dict) and item.get("type") == "tool_use"
            ],
            "replayed_result_ids": [
                e.get("id") for e in transcript if isinstance(e, dict) and e.get("type") == "agent-tool-result"
            ],
        }) + "\n")


def handle_run_inference(payload: Dict[str, Any]) -> List[str]:
    if _title_request(payload):
        return [
            _json_line({"type": "markdown-chat", "value": "Matrix Case"}),
            _json_line({"type": "patch", "v": [{"v": {"usage_summary": {
                "input_tokens": 120, "output_tokens": 8,
                "credits_by_type_unit": {"basic_ai_credits": 1},
            }}}]}),
        ]
    with _LOCK:
        state = _thread(payload)
        user_text = _user_text(payload)
        match = CASE_RE.search(user_text)
        if match and not state["case"]:
            state["case"] = match.group(1).lower()
        state["round"] += 1
        lines = _round_lines(state, payload)
        _record_request(state, payload, lines, state["round"])
        return lines


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, _fmt: str, *_args: Any) -> None:  # quieter access log
        pass

    def _send_json(self, status: int, obj: Dict[str, Any]) -> None:
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length") or 0)
        if length:
            return self.rfile.read(length)
        return b""

    def do_POST(self) -> None:  # noqa: N802
        raw = self._read_body()
        path = self.path.split("?", 1)[0]
        if path.endswith("/saveTransactionsFanout"):
            self._send_json(200, {"ok": True})
            return
        if path.endswith("/runInferenceTranscript"):
            try:
                payload = json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                print("MOCK-BAD-JSON len=%d head=%r" % (len(raw), raw[:160]), flush=True)
                payload = {}
            if not payload:
                print("MOCK-EMPTY-BODY content-length=%s h2-upgrade=%r" % (
                    self.headers.get("Content-Length"), self.headers.get("Upgrade")), flush=True)
            try:
                lines = handle_run_inference(payload)
            except Exception as exc:  # keep the mock alive for diagnostics
                print("MOCK-ERROR %r" % (exc,), flush=True)
                lines = [_json_line({"type": "markdown-chat", "value": "MOCK-ERROR"})]
            body = ("\n".join(lines) + "\n").encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/x-ndjson")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self._send_json(404, {"error": "not found"})

    def do_GET(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        if path.endswith("/health"):
            with _LOCK:
                self._send_json(200, {"ok": True, "threads": len(_THREADS),
                                      "cases": sorted({t.get("case") for t in _THREADS.values()})})
            return
        self._send_json(404, {"error": "not found"})


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8099)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--scenarios", required=True)
    parser.add_argument("--record-dir", default="")
    args = parser.parse_args()
    with open(args.scenarios, "r", encoding="utf-8") as handle:
        _CONFIG["scenarios"] = json.load(handle)
    _CONFIG["record_dir"] = args.record_dir
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print("mock notion upstream on http://%s:%d (cases: %s)" % (
        args.host, args.port, ",".join(sorted((_CONFIG["scenarios"].get("cases") or {}).keys()))), flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
