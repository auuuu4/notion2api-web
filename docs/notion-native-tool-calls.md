# Notion 原生工具调用协议与翻译规则

Notion AI 不使用 OpenAI function calling。agent 模型在 `runInferenceTranscript` 的 NDJSON 流中
按官方协议发起/接收工具调用，本服务解析该协议并适配到 harness 工具。

## 1. 协议形状

```jsonc
// agent-inference step：模型发起的工具调用
{"type":"agent-inference","value":[
  {"type":"tool_use","id":"toolu_01...","name":"callFunction",
   "content":"{\"function\":\"connections.computer.writeFile\",\"args\":{\"path\":\"a.html\",\"content\":\"...\"}}",
   "isCustomToolCall":false,"autofixedJson":false}
]}

// agent-tool-result step：服务端执行结果
{"type":"agent-tool-result","id":"...","toolCallId":"toolu_01...",
 "input":{"function":"connections.computer.writeFile","args":{"path":"a.html"}},
 "state":"applied" | "applied:error" | "streaming",
 "autoLoadPhase": "" | "core_docs" | "user_docs" | "file_tree" | "context_urls",
 "result":{"output":"..."}}
```

- 流中还有 `patch`（增量）与 `record-map`（整条 thread 快照，正常完成的流末尾一定有）；
- 同一调用会在 patch 与 record-map 中重复出现，按 `id`（或 `toolCallId`）去重；
- `autoLoadPhase` 非空的调用是 Notion 内部上下文加载，不属于用户任务，直接跳过。

## 2. 已观察到的原生工具

| function | 说明 |
|---|---|
| `connections.fs.readFiles / readDir` | Notion 虚拟文件系统（auto-load） |
| `connections.notion.loadUser / loadPage / createPage / list*` | Notion 页面操作 |
| `connections.search.unifiedSearch / notionSearch` | 搜索 |
| `connections.computer.writeFile / readFile / runCode` | 模型按历史知识尝试的本地计算机操作（官方无 computer 模块，但参数明确，翻译层仍处理） |

harness 额外工具（edit/glob/grep/pwsh/web_fetch/web_search 等）通过提示词模拟成
`connections.computer.*` 别名调用：listFiles → glob、editFile → edit、grep → grep、
webFetch → web_fetch、webSearch → web_search、runCode → run_code。

## 3. 翻译规则（`app/adapters/notion_native.py`）

| 原生调用 | 目标 |
|---|---|
| `connections.computer.writeFile {path, content}` | `write`；只有 run_code 时生成 Node fs.writeFile 代码 |
| `connections.computer.readFile {path}` | `read`；只有 run_code 时在 run_code 内调用 `tools.read`（保留 read-tracking，便于后续 edit） |
| `connections.computer.runCode {code}` | `run_code` |
| `connections.computer.listFiles/editFile/grep/webFetch/webSearch` | 对应 `glob/edit/grep/web_fetch/web_search` |
| `connections.search.unifiedSearch` | `web_search`（question/keywords → queries） |
| auto-load、`connections.fs.*`、`connections.notion.*`、未知工具 | 不映射，记录日志后按普通文本返回（不猜意图） |

原则：只翻译显式、参数形态已确认的原生调用；不做用户意图猜测，没有原生调用就按文本返回。

## 4. 官方 tool-result 回放（`app/providers/notion_provider.py`）

- **thread 复用**：按对话首条 user message 建立 key，缓存 Notion thread 并绑定创建它的账号；
  后续轮次复用同一 thread（`NOTION_THREAD_REUSE=1`）。
- **assistant tool_use 回放**：历史 assistant `tool_calls` 回放为 `agent-inference/tool_use`
  （`NOTION_NATIVE_TOOL_USE_REPLAY=1`），只回放有明确映射的工具，未知工具保留描述文本。
- **工具结果回放**：tool 消息回放为 `agent-tool-result`（`NOTION_NATIVE_TOOL_REPLAY=1`）。
- **flatten 开关**：`NOTION_FLATTEN_TOOL_RESULTS` = `always`（默认，兼容旧行为，附加 user 文本摘要）
  / `native-only`（有原生回放时不加摘要）/ `0`（只用官方 step）。
- **record-map 回显过滤**：Notion 的 record-map 会回显本次请求回放的 tool_use / result（保留 id、无
  startedAt）。提取响应时用本次回放 id 集合（`request_data["_native_replay_ids"]`）排除，
  否则历史调用会被当成新调用重复执行，工具数逐轮放大。
- **稳健性**：空响应（thread 失效 / transcript 被拒）自动换新 thread 重试；输出被截断（流结束仍无
  record-map，模型思考/输出预算耗尽）自动换新 thread + 简短提示重试；客户端断流时清理 thread 缓存。
- **流式正文解析**：patch 流按 item 类型区分 `thinking`（隐藏，不进入正文）与 `text`（可见）；
  保留文本块创建 op 携带的首字符（首 token 常在其中，record-map 缺失时也能拼出完整开头）；
  多个文本块之间补空行，避免思维链与正文粘连。
- **结构化归约器**（`app/providers/notion_stream.py`，默认开启，`NOTION_STREAM_REDUCER=0` 回退旧解析器）：
  把 patch op 应用到内存中的 step/item 状态树，按 item 类型提取 `text`（可见）与 `thinking`（隐藏），
  支持 step/item 创建初始 content、content 增量、markdown-chat、record-map、premium 配额事件和未知 op 计数；
  最终文本以 record-map 为权威来源，与增量文本不一致时告警并以快照为准（增量已实时下发的响应保留已下发文本，避免重复）。
- **fixture 回放测试**（`tests/fixtures/notion_ndjson/`）：真实抓包经 `scripts/capture_to_fixture.py`
  脱敏后连同 expected 一起提交，单测用 reducer + `extract_native_tool_calls` 零额度回归
  （thinking 泄漏、正文首字、多文本块、截断、配额、record-map 回显等形状）。

## 5. 调试与抓包

- `NOTION_CAPTURE_DIR`：保存每次请求的 `*.request.json` / `*.payload.json` /
  `*.response.ndjson` / `*.final.txt` / `*.native.json`；
- `NOTION_NATIVE_LOG`：解析出的原生调用 JSONL；
- `NOTION_API_BASE`：把 provider 指到 mock 上游（矩阵 e2e 用）。

## 6. 已知限制

- Notion 官方没有 `computer` 模块，`connections.computer.*` 是模型“历史知识”式调用；
  参数明确所以翻译层照常执行，但上游通常会返回 `applied:error`；
- `connections.fs.*` 是 Notion 自己的文档工作区，不映射到用户本地文件系统；
- PTC（仅 run_code）模式下，部分工具通过 run_code 里的 `tools.*` 桥接执行；
- Notion 协议可能调整，矩阵 e2e（见 [tool-alignment.md](tool-alignment.md)）用于回归。
