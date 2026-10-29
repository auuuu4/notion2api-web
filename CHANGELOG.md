# Changelog

## 2026-10-29（开源基线审计）

- 清理未使用的导入、异常类、路由辅助函数和旧版工具调用启发式代码（约 650 行）；
- 修复指标标签转义（反斜杠/换行/引号），避免 Prometheus exposition 注入；
- 修复 CI workflow 的 ${{ matrix.python-version }} 表达式和 Dockerfile 基础镜像版本；
- 移除配置/环境模板中的真实临时邮箱地址、域名、Clash 分组等内部默认值，改为占位符；
- prompt 模块移除竞赛化/规避式提示词，改为中性的通用助手 system prompt；
- 新增 SECURITY / CONTRIBUTING / NOTICE / docs/architecture.md，README 更新安全、许可（Apache-2.0）与用量说明；
- 全量单测 187 通过。

## 2026-10-28（macOS Vibrancy 面板重构）

- 全部 6 个仪表盘页面迁移到 macOS Vibrancy 暗色毛玻璃风格：三级深灰背景（#1c1c1e / #2c2c2e /
  #3a3a3c）、侧栏 backdrop-blur + rgba(28,28,30,.8)、全站 1px white/8~12 半透明边框、
  最大 rounded-xl 圆角、无渐变 / 无发光 / 无装饰动画。
- 排版：页面标题、面板标题、KPI 数值、账户名、配置分组使用 Georgia 衬线；正文使用系统无衬线；
  技术数值/日志/配置值使用 SF Mono 等宽字体；文字层级 white/95、white/72、white/58。
- 布局：固定毛玻璃侧栏 + 主内容区；KPI/图表/表格组件统一为深灰卡片；表单/按钮/分页/弹窗/toast
  统一交互态（hover 背景提亮、200ms 颜色过渡、focus-visible 强调环）。
- 交互与无障碍：跳过导航链接、导航 aria-label、prefers-reduced-motion、键盘焦点样式、
  移动端 390px 无横向溢出（用 CDP 设备模拟验证）。
- analytics 版式调整：模型详情全宽表格、账户筛选行内联；日志时间列不再折行；手机端筛选/按钮栈式布局。

## 2026-10-28（输出格式与用量修复）

- 修复回复中所有换行被压成空格的问题：_clean_content 的空白清理从全局 \s+ 改为只压缩水平空白
  （空格/Tab），保留 Markdown 换行与空行；多行标题/列表现在能正常渲染。
- 修复 OpenAI usage 泄漏 Notion 内部 agent 用量的问题：prompt_tokens/completion_tokens 改为
  客户端可见 token 估算（供 DSH 等客户端做上下文窗口核算），Notion 上游用量保留在
  notion_input_tokens / notion_output_tokens / notion_total_tokens / notion_cached_* 字段；
  credits_used 仍为 Notion 真实计费值；DB 日志与指标同步使用客户端口径。此前 DSH 会把 Notion
  内部 200K+ 用量当成本轮上下文，对话到几百 K 后误报 CONTEXT_WINDOW_EXCEEDED。
- 单测 182 → 185。

## 2026-10-28（节点冻结策略）

- 注册出口节点冻结改为「每个节点独立连续失败计数」：默认连续失败 3 次才冻结该节点，期间任意一次
  成功立即清零重新计数；未达阈值时节点仍可被选中（选择器会优先连败更少的节点）。
- 冻结后冷却从 60s 起指数退避（上限 900s），banned/429 类升级更快、轻量类别减半；单个节点冻结后
  没有可用节点时会触发补号暂停（all_nodes_cooling）。
- 只有节点相关失败（banned/429/hcaptcha/network）计入节点连败；邮箱服务类失败（取地址失败/收码超时）
  与未知错误只记录统计，不冻结节点，也不会重置已有连败计数。
- 新增配置 `REGISTER_NODE_FAILURE_THRESHOLD`（默认 3）；面板节点状态显示「可用（连败 x/3）」，
  设置页当前配置与编辑区同步展示；rotate=False 仍保持旧行为（固定第一个节点、忽略健康）。
- 单测 173 → 179。

## 2026-10-28（面板增强）

- 注册出口节点改为可编辑列表：新增/编辑/删除/批量启用停用删除/分页，配置持久化到
  registration_nodes.json；API 支持 CRUD，注册日志持久化到 reg_logs.jsonl（重启不丢）。
- 系统设置页重做：当前配置置顶显示（完整值、大字号），可编辑核心/临时邮箱/补号/账号健康配置；
  运行时项立即生效，其余写 .env 后重启；新增 AUTO_DELETE_QUOTA_EXCEEDED 开关（默认关闭，
  仅 Notion 明确无额度时删除账号）。
- 请求日志：新增账号邮箱与响应内容列、响应详情弹窗、页码窗口分页（首尾/前后/相邻 5 页）、
  按账号邮箱/内容搜索；移除 token 统计列；旧数据无邮箱/响应的显示为 —。
- 账户页：移除手动添加表单；新增状态筛选、邮箱/名称/空间ID 搜索、归档死号与一键删除无额度账号。
- 分析页账户状态：状态/来源中文化，支持状态、来源筛选与名称/邮箱搜索。
- 注册页：按 TEMP_EMAIL_PROVIDER 显示说明（emailnator 无需域名/Base/Admin），新增节点管理表格。
- 后端：SQLite request_logs 增加 account_email/response_content 列；/api/models、/api/logs/{id}；
  单测 171 → 173。

## 2026-10-28

- 流式解析重构：新增结构化 NDJSON 归约器（thinking/正文 item 类型隔离、文本块首字符保留、
  多块空行、record-map 权威文本与不一致告警），默认开启，`NOTION_STREAM_REDUCER=0` 可回退；
  新增 9 组真实形状 fixtures 与 `capture_to_fixture.py` 抓包脱敏工具。
- 账号健康：失败分类（quota/invalid/empty/truncated/transport）、连续失败/空响应自动禁用、
  轻量探活（getAvailableModels，不消耗额度）、额度预测（credits 均值 + reserve 预留）、
  死号按天归档/恢复/批量清理 API 与面板；`/api/accounts` token 改为掩码。
- 可观测性：新增轻量指标模块与 `GET /metrics`（Prometheus）、`/api/metrics` JSON；
  请求/延迟/token/credit/账号结果/注册节点/暂停状态均可抓取。
- 注册出口节点池：失败分类（banned/429/hcaptcha/邮箱/网络）、节点健康冷却与 LRU 选优、
  暂停式熔断（默认 300s 后自动恢复，可回退旧硬关闭）、可选 Clash 节点轮换（默认关闭、
  dry-run、空闲恢复）；新增 `/api/reg-nodes` 与面板节点健康展示。
- 工程：新增 `.github/workflows/ci.yml`；单测 98 → 171。

## 2026-10-27

- 完成 Notion 官方 agent-tool-result 对齐：thread 复用、assistant tool_use / 工具结果回放、
  record-map 回显过滤、全量矩阵 e2e（54/54 runs，550 项断言 0 失败）。
- 自动补号新增 `REGISTER_PROXY` 注册出口代理（多代理轮转、凭证脱敏）。
- 稳定性：空响应自动换 thread 重试、输出截断检测 + 简短指令重试、断流时清理 thread 缓存。
- 修复流式正文解析：隐藏 thinking 思维链、保留每个文本块首字符、多个文本块之间补空行
  （此前会出现正文开头缺失 + 英文思维链混入正文的格式问题）。
- DSH 接入文档补齐模型 `reasoningEfforts` 声明，避免选择推理档位时报
  `UNSUPPORTED_REASONING_EFFORT`（该参数对 Notion 上游无影响，代理会忽略）。

## 2026-10-26 / 27

- 官方 `callFunction` / `agent-tool-result` 协议解析与翻译层；模拟
  `connections.computer` 工具；DSH / PTC（仅 run_code）适配。

## 2026-10-25

- 首次重构：多账号轮转、额度无缝切号、Web 管理面板、自动注册 / 自动补号。
