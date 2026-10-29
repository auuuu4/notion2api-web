# 官方工具对齐全量矩阵 e2e

用 **mock Notion 上游** 回放官方 `agent-inference/tool_use`、`agent-tool-result`、
record-map 等 NDJSON，跑 **真实代理 + 真实 DSH headless harness**，不消耗 Notion 账号额度。

- `matrix_cases.json`：13 个用例（写/读/执行/列目录/编辑/搜索/抓网页/Notion 工具/auto-load/未知工具/多轮/错误），
  每个用例定义 DSH 任务、fixture、期望执行的 harness 工具、mock 轮次脚本；
- `run_matrix.py`：逐用例启动真实 DSH（完整 24 工具 / PTC run_code-only），
  断言 tool_call/tool_result、文件结果、下一轮 transcript 的官方回放、无 flatten 文本、
  无 record-map 回显放大、请求 tools 数量；
- `run-headless.ps1`：DSH headless 启动包装（`DSH_PERMISSION_MODE=danger-full-access`、`--json` 事件流）。

## 依赖

1. 代理指向 mock 上游并关闭 user-text flatten（另开 8081，避免占用 8088）：

   ```bat
   set NOTION_API_BASE=http://127.0.0.1:8099/api/v3
   set NOTION_FLATTEN_TOOL_RESULTS=0
   set NOTION_NATIVE_TOOL_REPLAY=1
   set NOTION_NATIVE_TOOL_USE_REPLAY=1
   uvicorn main:app --host 127.0.0.1 --port 8081
   ```

2. mock 上游（仓库根目录执行）：

   ```bash
   python scripts/mock_notion_upstream.py --port 8099 \
     --scenarios scripts/alignment_matrix/matrix_cases.json \
     --record-dir scripts/alignment_matrix/_work/mock-records
   ```

3. DSH 侧 patch 需要把 `notion-local` provider 的 baseURL 指到 `http://127.0.0.1:8081/v1`
   （`run_matrix.py` 会自动生成 `patch-<model>.yml`，contextWindow=262144）。

## 运行

```bash
python scripts/alignment_matrix/run_matrix.py \
  --models albuquerque-quinn,achira-donut \
  --work-dir /path/to/work   # 可选；默认 scripts/alignment_matrix/_work
```

结果：`<work-dir>/evidence/results.json`（每个用例的断言明细）与 `results.md`（概览表）。
本次结果：54/54 通过（13 用例 × {PTC, 完整} × 2 模型，含 multi_turn 两个 CLI turn），550 项断言全部通过。
