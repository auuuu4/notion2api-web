"""Convert a NOTION_CAPTURE_DIR .response.ndjson capture into a test fixture.

Usage:
    python scripts/capture_to_fixture.py --input e2e-workspace/capture/<base>.response.ndjson --name live_case

The response NDJSON is redacted by default (emails, token/cookie-like values),
written as tests/fixtures/notion_ndjson/<name>.jsonl, and the expected reducer
result is generated next to it as <name>.expected.json. Refuses to overwrite an
existing fixture unless --force is given.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from app.adapters.notion_native import extract_native_tool_calls  # noqa: E402
from app.providers.notion_stream import NotionStreamReducer  # noqa: E402

DEFAULT_OUT_DIR = os.path.join(ROOT, "tests", "fixtures", "notion_ndjson")
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
SECRET_RE = re.compile(r"(token_v2|secret|session|password)([=:]\s*)([^\s\"',;}]{6,})", re.IGNORECASE)


def redact_value(value):
    """Recursively redact e-mail addresses and token-like strings in JSON values."""
    if isinstance(value, str):
        value = EMAIL_RE.sub("<email>", value)
        value = SECRET_RE.sub(lambda match: "%s%s<redacted>" % (match.group(1), match.group(2)), value)
        return value
    if isinstance(value, list):
        return [redact_value(item) for item in value]
    if isinstance(value, dict):
        return {key: redact_value(item) for key, item in value.items()}
    return value


def redact_line(raw: str) -> str:
    try:
        record = json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        return raw
    return json.dumps(redact_value(record), ensure_ascii=False)


def build_expected(lines):
    reducer = NotionStreamReducer()
    for raw in lines:
        reducer.consume_line(raw)
    stats = reducer.stats
    native_calls = extract_native_tool_calls(lines)
    return {
        "visible_text": reducer.visible_text,
        "final_text": reducer.final_text,
        "record_map_text": reducer.record_map_text,
        "saw_record_map": reducer.saw_record_map,
        "parsed_records": reducer.parsed_records,
        "thinking_chars": stats["thinking_chars"],
        "unknown_op_count": stats["unknown_op_count"],
        "mismatch": stats["mismatch"],
        "quota": reducer.quota,
        "reducer_since_ms": None,
        "native_extract": {"since_ms": None, "exclude_ids": []},
        "native_calls": [
            {"function": call.get("function"), "args": call.get("args")} for call in native_calls
        ],
        "provider_usage": None,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Turn a captured Notion NDJSON response into a test fixture.")
    parser.add_argument("--input", required=True, help="path to <base>.response.ndjson from NOTION_CAPTURE_DIR")
    parser.add_argument("--name", required=True, help="fixture base name (without extension)")
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    parser.add_argument("--force", action="store_true", help="overwrite an existing fixture")
    parser.add_argument("--no-redact", action="store_true", help="keep e-mail/token values (not recommended)")
    args = parser.parse_args()

    if not re.fullmatch(r"[a-z0-9_\-]+", args.name):
        parser.error("--name must be lowercase letters, digits, underscore or dash")

    with open(args.input, "r", encoding="utf-8", errors="replace") as handle:
        raw_lines = [line.rstrip("\n") for line in handle if line.strip()]

    lines = [line if args.no_redact else redact_line(line) for line in raw_lines]
    os.makedirs(args.out_dir, exist_ok=True)
    fixture_path = os.path.join(args.out_dir, args.name + ".jsonl")
    expected_path = os.path.join(args.out_dir, args.name + ".expected.json")
    if not args.force:
        for path in (fixture_path, expected_path):
            if os.path.exists(path):
                parser.error("fixture already exists (use --force): " + path)

    expected = build_expected(lines)
    with open(fixture_path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + ("\n" if lines else ""))
    with open(expected_path, "w", encoding="utf-8") as handle:
        json.dump(expected, handle, ensure_ascii=False, indent=2)

    print("fixture: ", fixture_path)
    print("expected:", expected_path)
    print(
        "records=%d saw_record_map=%s visible=%d chars final=%d chars mismatch=%s"
        % (
            expected["parsed_records"],
            expected["saw_record_map"],
            len(expected["visible_text"]),
            len(expected["final_text"]),
            bool(expected["mismatch"]),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
