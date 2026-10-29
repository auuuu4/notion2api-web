
# notion-2api

将 **Notion AI** 转换为 OpenAI 兼容 API 的本地代理：多账号轮转、额度管理、Web 管理面板，
并支持把 Notion agent 的原生工具调用协议翻译成 harness（DeepSeek Harness / PTC 等）的工具调用。

Fork 自 [lza6/notion-2api](https://github.com/lza6/notion-2api)，在原项目基础上重构。

## 功能

- OpenAI 兼容接口：`/v1/chat/completions`（流式 / 非流式）、`/v1/models`
- 多账号轮转、额度耗尽无缝切号、自动补号（EmailNator / cfmail，可选注册出口代理）
- Notion 官方 agent 工具调用协议解析与翻译（`callFunction` / `agent-tool-result`），
  适配 DSH 完整工具模式与 PTC（仅 `run_code`）
- Thread 复用：同一会话复用同一个 Notion thread，assistant tool_use 与工具结果按官方 step 回放
- 用量统计与 SQLite 请求日志：OpenAI `usage` 的 prompt/completion 为客户端可见 token 估算
  （供上下文窗口核算），`credits_used` 为 Notion 实际计费值；Notion 内部 agent 用量保留在
  `notion_*` 附加字段
- 输出保真：清洗只压缩水平空白，Markdown 换行/空行完整保留，流式增量和非流式一致
- 结构化 NDJSON 归约器（thinking/正文分离、record-map 权威文本）+ 真实抓包 fixture 回放测试
- 账号健康度：失败分类、自动禁用坏号、额度预测、死号归档/恢复/批量清理
- 注册出口节点池：失败分类（banned/429/hcaptcha/邮箱/网络）、节点健康冷却（连续失败达阈值才冻结、成功清零）、暂停式熔断（自动恢复）
- Web 管理面板：概览 / 账户 / 请求日志 / 数据分析 / 自动注册 / 系统设置
  （账户状态筛选与搜索、一键删除无额度账号、请求日志分页与响应详情、设置页当前配置可视化、
  注册出口节点可编辑/批量操作/翻页、注册日志持久化）
- 面板视觉：macOS Vibrancy 暗色毛玻璃风格（三级深灰 #1c1c1e/#2c2c2e/#3a3a3c、侧栏 backdrop-blur、
  1px 半透明边框、衬线标题 + 系统无衬线正文 + 等宽数据、无渐变/无大阴影/无装饰动画、移动端响应式）
- Prometheus 指标：`GET /metrics`（请求、延迟、token/credit、账号状态、注册节点/暂停状态）

## 快速开始

要求 Python 3.11+。

```bash
pip install -r requirements.txt
cp .env.example .env    # 填入 Notion 凭证
uvicorn main:app --host 0.0.0.0 --port 8000
# 或：docker compose up -d（默认 8088，经 nginx 反代）
```

必填环境变量：`API_MASTER_KEY`、`NOTION_COOKIE`（token_v2）、
`NOTION_SPACE_ID`、`NOTION_USER_ID`。

## 配置

完整说明见 `.env.example`。

| 分类 | 变量 | 说明 |
|---|---|---|
| 核心 | `API_MASTER_KEY` / `NOTION_COOKIE` / `NOTION_SPACE_ID` / `NOTION_USER_ID` | 鉴权与 Notion 凭证 |
| 自动注册 | `TEMP_EMAIL_PROVIDER` / `TEMP_EMAIL_BASE` / `TEMP_EMAIL_DOMAIN` | EmailNator 或 cfmail |
| 注册代理 | `REGISTER_PROXY` / `REGISTER_PROXY_ROTATE` | 注册流量出口（http/https/socks5，可逗号分隔轮转），推理不受影响 |
| 补号 | `AUTO_REPLENISH_ENABLED` / `AUTO_REPLENISH_TARGET` | 自动补号开关与目标可用账号数 |
| 解析 | `NOTION_STREAM_REDUCER` | 1=结构化归约器（默认）；0=回退旧解析器 |
| 账号健康 | `ACCOUNT_HEALTH_ENABLED` / `ACCOUNT_HEALTH_INTERVAL_MINUTES` / `ACCOUNT_FAILURE_THRESHOLD` / `ACCOUNT_EMPTY_THRESHOLD` | 轻量探活 + 坏号自动禁用阈值 |
| 死号清理 | `ACCOUNT_ARCHIVE_ENABLED` / `ACCOUNT_ARCHIVE_MODE` / `ACCOUNT_ARCHIVE_QUOTA_DAYS` / `ACCOUNT_ARCHIVE_DISABLED_DAYS` | 归档（默认，可恢复）或删除，按天数宽限 |
| 额度预测 | `FREE_CREDITS_LIMIT` / `QUOTA_RESERVE_REPLIES` | 估算剩余回复；低于预留视为不可调度并提前补号 |
| 自动删除 | `AUTO_DELETE_QUOTA_EXCEEDED` | 默认关闭；仅在 Notion 明确返回无额度时删除账号，其他异常不删 |
| 注册节点池 | `REGISTER_NODE_FAILURE_THRESHOLD` / `REGISTER_NODE_COOLDOWN_BASE` / `REGISTER_NODE_COOLDOWN_MAX` / `REGISTER_PAUSE_AFTER_FAILURES` / `REGISTER_PAUSE_SECONDS` | 单节点连续失败达阈值才冻结（默认 3 次，成功清零）；冷却指数退避；全部冷却时暂停自动恢复 |
| Clash 轮换（可选） | `REGISTER_EGRESS_MODE=clash` / `CLASH_*` | 默认关闭；切换全局节点，空闲恢复，支持 dry-run |
| 工具对齐 | `NOTION_THREAD_REUSE` / `NOTION_NATIVE_TOOL_REPLAY` / `NOTION_NATIVE_TOOL_USE_REPLAY` / `NOTION_FLATTEN_TOOL_RESULTS` | thread 复用与官方 step 回放（默认开） |
| 调试 | `NOTION_CAPTURE_DIR` / `NOTION_NATIVE_LOG` | 抓取原始 NDJSON / 原生调用 JSONL |

## API 使用

```bash
curl http://localhost:8088/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your_secret_key" \
  -d '{"model":"claude-sonnet-4.6","messages":[{"role":"user","content":"Hello"}],"stream":true}'

curl http://localhost:8088/v1/models -H "Authorization: Bearer your_secret_key"
```

- 模型列表由 Notion `getAvailableModels` 动态同步；请求时显示名称或 Notion 代号均可；
- 响应的 `usage` 返回客户端可见 token 估算与 Notion `credits_used`；Notion 内部 agent 用量在
  `notion_input_tokens` / `notion_output_tokens` / `notion_total_tokens` 等附加字段；流式请求加
  `stream_options.include_usage: true` 可在 `[DONE]` 前收到 usage chunk；
- 免费 / 个人工作区有累计 ~50 条 AI 回复限制，用尽后自动切换账号；
- 运维接口：`GET /api/health`（池健康/预计剩余/补号暂停状态）、`GET /metrics`（Prometheus）、
  `GET /api/accounts`（token 掩码 + health + 额度估算）、`POST /api/accounts/cleanup`（dry_run 支持）、
  `GET /api/accounts/archive`、`POST /api/accounts/restore/{id}`；
  注册节点：`GET/POST /api/reg-nodes`、`PUT/DELETE /api/reg-nodes/{id}`、`POST /api/reg-nodes/batch`、
  `POST /api/reg-nodes/reset`；
  日志：`GET /api/logs?limit/offset/status/model/search`、`GET /api/logs/{id}`（含账号邮箱与响应详情）；
  模型：`GET /api/models`（面板筛选用）。

## 在 DeepSeek Harness 中接入

写入 DSH profile 的 `llm-pi-ai` → `providers`（以 desktop 为例）：

```yaml
      notion-local:
        displayName: Notion-2API (本地 8088)
        api: openai-completions
        baseURL: http://127.0.0.1:8088/v1
        headers:
          Authorization: Bearer your_secret_key
        streamIdleTimeoutMs: 240000
        retryPolicy:
          mode: normal
          maxRetries: 1
        # Notion 不需要开发角色/推理档位；给 pi-ai 声明后选择档位不会报错，
        # 同时这两个开关会让请求里不携带 reasoning_effort、system 走 system 角色。
        compat:
          supportsDeveloperRole: false
          supportsReasoningEffort: false
        models:
          - id: achira-donut          # Notion 代号（显示名也可）
            name: Notion · Sonnet 5.5
            contextWindow: 262144      # 按模型的真实上下文窗口配置
            maxTokens: 32000
            input:
              - text
            # Notion 上游不使用推理档位参数，但 DSH 要求模型声明能力才允许选择
            # 推理档位（否则报 UNSUPPORTED_REASONING_EFFORT）。声明全部档位即可，
            # 该参数会被代理忽略、不会发往 Notion。
            reasoningEfforts:
              "off":
              low: low
              medium: medium
              high: high
              max: max
```

> `usage` 采用客户端可见 token 估算，不会把 Notion 内部 agent 用量泄漏给 DSH；`contextWindow`
> 按所选模型的真实窗口设置即可。

> 若 DSH 报 `provider "notion-local" model "..." does not support reasoning effort "low"`，
> 说明模型条目缺少 `reasoningEfforts` 声明。补上上面的 `reasoningEfforts` 段即可（可选档位按需增减）；
> 该参数对 Notion 上游没有实际作用。

## 工具调用对齐

Notion AI 没有 OpenAI function calling：agent 模型在 NDJSON 流里发 `agent-inference/tool_use`
（`name: callFunction`），服务端执行后产生 `agent-tool-result` step。
本服务只翻译模型**明确发出**的原生调用，不猜意图：

| Notion 原生调用 | 目标工具 |
|---|---|
| `connections.computer.writeFile` / `readFile` / `runCode` | `write` / `read` / `run_code`（PTC 下走 run_code 内 `tools.read` 等） |
| `connections.computer.listFiles / editFile / grep / webFetch / webSearch` | `glob / edit / grep / web_fetch / web_search` |
| `connections.search.unifiedSearch` | `web_search` |
| `connections.fs.* / notion.*`、auto-load 调用 | 不映射，只记录（auto-load 跳过） |

- 同一会话复用 Notion thread；历史 assistant tool_calls 回放为官方 `agent-inference/tool_use`，
  工具结果回放为 `agent-tool-result`，`NOTION_FLATTEN_TOOL_RESULTS=0` 时完全不做 user 文本摘要；
- record-map 会回显本次回放的 step，提取时按回放 id 过滤，避免历史调用被当成新调用重复执行；
- 空响应自动换 thread 重试，输出被截断（无 record-map）自动换 thread + 简短提示重试；
- 流式正文由结构化归约器解析：`thinking` 隐藏、text 块初始首字符保留、多块之间补空行，
  最终文本以 record-map 为权威来源（增量已实时下发与快照不一致时保留已下发文本并告警）。

协议细节见 [docs/notion-native-tool-calls.md](docs/notion-native-tool-calls.md)，
对齐测试与矩阵见 [docs/tool-alignment.md](docs/tool-alignment.md)。

## 自动注册与补号

Notion 对注册来源 IP 风控较严，推荐配置注册出口代理：

```bash
# .env
REGISTER_PROXY=http://127.0.0.1:7890   # 单个代理；多个用逗号分隔轮转
REGISTER_PROXY_ROTATE=1                # 0 = 固定第一个
```

- 支持 http / https / socks5，凭证在日志与 `/api/reg-status` 中脱敏；
- 每次注册尝试从节点池按健康度选择出口；**每个节点独立计算连续失败次数，达到
  `REGISTER_NODE_FAILURE_THRESHOLD`（默认 3）才冻结，期间任意一次成功会清零重新计数**；冻结后按
  指数退避冷却（banned/429 冷得更久，hcaptcha/网络类减半），全部节点冷却时补号立即暂停；
  只有节点相关失败（banned/429/hcaptcha/network）计入连败，邮箱服务类失败（取地址/收码超时）
  只记录统计、不冻结节点也不清零连败；
- 连续失败默认**暂停 300s 后自动恢复**（不再是硬关闭）；手动开启/重置节点会清除暂停与失败统计；
- 可选 `REGISTER_EGRESS_MODE=clash` 自动切换 Clash 节点（默认关闭、支持 `CLASH_DRY_RUN=1`、空闲恢复原节点）；
- 触发方式：管理面板「自动注册」，或 `POST /api/auto-register {"count":1}`；
- 自动补号：`POST /api/auto-replenish {"enabled":true,"target":10}`；节点健康：`GET /api/reg-nodes`；
- 账号健康与清理：轻量探活（getAvailableModels，不消耗 AI credits）+ 请求结果回写，
  连续空响应/失败自动禁用；死号按天归档到 `accounts_archive.json`，可从面板/API 恢复。

## 测试

```bash
python -m unittest discover -s tests -v      # 单元测试（187 个）
```

官方工具对齐全量矩阵 e2e（mock Notion 上游 + 真实代理 + 真实 DSH，不消耗账号额度）：

```bash
python scripts/alignment_matrix/run_matrix.py --models albuquerque-quinn,achira-donut
```

真实抓包 → fixture 回放（零额度回归 thinking/正文分离、截断、record-map、配额、record-map 回显）：

```bash
# 1) 开启抓包后请求一次（NOTION_CAPTURE_DIR=e2e-workspace/capture）
# 2) 脱敏转换为夹具 + expected
python scripts/capture_to_fixture.py --input e2e-workspace/capture/<base>.response.ndjson --name live_case
# 3) 单测会自动回放 tests/fixtures/notion_ndjson/ 下所有夹具
```

用法与结果见 [docs/tool-alignment.md](docs/tool-alignment.md) 和
[scripts/alignment_matrix/README.md](scripts/alignment_matrix/README.md)。CI 见 `.github/workflows/ci.yml`。

## 项目结构

```
notion-2api-web/
├── main.py                     # FastAPI 应用（路由、账号、注册、面板 API）
├── app/
│   ├── adapters/               # harness 适配 + 官方 callFunction 协议解析/翻译
│   ├── providers/              # Notion 上游对接（流式归约器、thread、回放、重试）
│   ├── core/                   # 配置、模型注册表、账号健康、注册节点池、指标
│   ├── utils/                  # HTTP session、临时邮箱、SSE
│   ├── dashboard/              # Web 管理面板
│   └── prompt_injection.py
├── scripts/
│   ├── mock_notion_upstream.py # 回放官方 NDJSON 的 mock 上游（矩阵 e2e 用）
│   ├── capture_to_fixture.py   # 真实抓包脱敏转 fixture + expected
│   └── alignment_matrix/       # 全量对齐矩阵 e2e（用例 / runner / 说明）
├── tests/                      # 单元测试 + fixtures/notion_ndjson 抓包回放夹具
├── docs/                       # 协议与对齐文档
├── Dockerfile / docker-compose.yml / nginx.conf
└── .env.example
```

## 安全

- **管理面板 `/dashboard` 与管理 API `/api/*` 默认无登录鉴权**，且 `/api/settings` 会返回
  完整 API Key、临时邮箱凭据等敏感配置；请只监听 `127.0.0.1`，或放在带认证的反向代理/防火墙后，
  **不要直接暴露到公网**；
- `token_v2`、`.env`、`accounts.json`、`notion2api.db`、注册节点状态等均为敏感文件，请勿提交；
- 详细部署与漏洞上报方式见 [SECURITY.md](SECURITY.md)。

## 贡献

欢迎提交 Issue / PR；开发、测试和提交前检查见 [CONTRIBUTING.md](CONTRIBUTING.md)。
整体模块与数据流见 [docs/architecture.md](docs/architecture.md)。

## 说明

- 本项目仅供学习与研究使用；`token_v2` 属于敏感凭证，请勿泄露；
- Notion 可能随时调整协议，本项目不保证持续可用；
- 变更记录见 [CHANGELOG.md](CHANGELOG.md)，代码审计记录见 [docs/audit-2026-10-29.md](docs/audit-2026-10-29.md)。

## License

Apache License 2.0，详见 [LICENSE](LICENSE)。
