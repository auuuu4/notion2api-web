# 官方工具对齐全量矩阵

目标：验证与 Notion 官方 agent 机制一致，而不只是“OpenAI tool_calls 能跑”：

1. 模型输出官方 `agent-inference/tool_use`；
2. 工具结果以官方 `agent-tool-result` step 回放到**同一 thread**；
3. 模型把结果当工具结果，而不是用户消息；
4. 多轮、连续工具、错误结果都能正确继续。

## 当前状态（2026-10-27）

- 单元测试：`python -m unittest discover -s tests` → **94 个全部通过**；
- 真机多轮 e2e（真实 Notion + headless DSH，`NOTION_FLATTEN_TOOL_RESULTS=0`）：
  turn1 搜索 + 写 `news.md`、turn2 同一 thread 读取并从工具结果回答；6 次请求同 thread，
  assistant tool_use / 结果跨轮回放，无回显放大，无 flatten 摘要；
- 全量矩阵 e2e（mock 上游 + 真实代理 + 真实 DSH，不消耗账号额度）：
  **54/54 runs 通过，550 项断言 0 失败**（13 用例 × PTC/完整 24 工具 × 2 模型，含 multi_turn 两个 CLI turn）。

## 矩阵用例

| 类别 | 用例 | 断言 |
|---|---|---|
| 写文件 | write | 原生 writeFile → 本地文件存在且内容正确 |
| 读文件 | read | readFile → 工具结果包含文件内容 |
| 运行代码 | run_code | runCode → 结果包含程序输出 |
| 列目录 | glob | listFiles → 返回目录项 |
| 编辑 | edit | 先读再 editFile → 文件内容变化 |
| 搜索 | web_search | webSearch/unifiedSearch → 翻译成 web_search 并执行 |
| 抓网页 | web_fetch | webFetch → 翻译成 web_fetch 并执行 |
| Notion 工具 | notion_tool | 官方 notion 调用不产生 DSH 调用、不报错 |
| auto-load | auto_load | fs.readFiles/readDir 被跳过，只执行真实调用 |
| 未知工具 | unknown_tool | 不猜测、不执行，按文本返回 |
| 多轮 | multi_turn | 两次 CLI turn 共 4 轮，跨 turn 回放 assistant tool_use |
| 错误 | error | 工具报错作为结果回放，模型继续 |

每个用例断言：DSH 实际执行的工具名、tool_result 内容、文件结果、下一轮 transcript 是否包含
官方 `agent-inference` + `agent-tool-result` 回放、无 flatten 文本、
无 record-map 回显放大（轮数精确等于脚本轮数）、请求 tools 数（PTC=1 / 完整=24）。

## 运行方式

依赖：一个指向 mock 上游的代理（8081）和一个运行的 DSH 环境（headless profile）。

```bash
# 1) mock Notion 上游（回放官方 NDJSON，不消耗账号）
python scripts/mock_notion_upstream.py --port 8099 \
  --scenarios scripts/alignment_matrix/matrix_cases.json \
  --record-dir <work-dir>/mock-records

# 2) 代理指向 mock，并关闭 user 文本 flatten
set NOTION_API_BASE=http://127.0.0.1:8099/api/v3
set NOTION_FLATTEN_TOOL_RESULTS=0
uvicorn main:app --host 127.0.0.1 --port 8081

# 3) 跑矩阵（自动生成 patch-<model>.yml，contextWindow 按测试模型设置）
python scripts/alignment_matrix/run_matrix.py \
  --models albuquerque-quinn,achira-donut --work-dir <work-dir>
```

结果：`<work-dir>/evidence/results.json`（每个用例的断言明细）与
`results.md`（概览表）；用例定义见 `scripts/alignment_matrix/matrix_cases.json`。

## 说明

- mock 上游只替换 Notion `runInferenceTranscript`，其余链路（真实代理、真实 DSH、
  真实本地工具执行）都是真实的；mock 的 record-map 会故意回显本次请求 transcript，
  持续回归“回显被当新调用”的放大问题；
- 代理返回的 OpenAI usage 是客户端可见 token 估算；DSH 的 `contextWindow` 按模型真实窗口设置即可；
- 空响应 / 输出截断（流结束无 record-map）会自动换 thread 重试，细节见 README 与
  [docs/notion-native-tool-calls.md](notion-native-tool-calls.md)。
