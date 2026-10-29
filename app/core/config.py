from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Optional

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding='utf-8',
        extra="ignore"
    )

    APP_NAME: str = "notion-2api"
    APP_VERSION: str = "5.0.0"
    DESCRIPTION: str = "一个将 Notion AI 转换为兼容 OpenAI 格式 API 的高性能代理。"

    API_MASTER_KEY: Optional[str] = None

    # --- Notion 凭证 ---
    NOTION_COOKIE: Optional[str] = None
    NOTION_SPACE_ID: Optional[str] = None
    NOTION_USER_ID: Optional[str] = None
    NOTION_USER_NAME: Optional[str] = None
    NOTION_USER_EMAIL: Optional[str] = None
    NOTION_BLOCK_ID: Optional[str] = None
    NOTION_CLIENT_VERSION: str = "23.13.20260628.0724"

    API_REQUEST_TIMEOUT: int = 180
    NGINX_PORT: int = 8088

    # --- 临时邮箱服务配置 (用于自动注册) ---
    TEMP_EMAIL_BASE: str = "https://temp-mail.example.com"
    TEMP_EMAIL_ADMIN_AUTH: str = "change-me"
    # 临时邮箱域名；必须使用你自己的临时邮箱服务中已配置、且未被 Notion 拒绝的域名。
    TEMP_EMAIL_DOMAIN: str = "example.com"
    # 临时邮箱提供商: emailnator（无需自建服务，推荐） / cfmail（自建 cloudflare_temp_email）
    TEMP_EMAIL_PROVIDER: str = "emailnator"

    # --- 自动注册/补号的出口代理 ---
    # 注册（Notion 注册接口 + 临时邮箱）走代理出口，避免本机 IP 被 Notion 风控。
    # 支持 http/https/socks5；多个代理用逗号/分号分隔，默认按注册尝试轮转。
    # 例: REGISTER_PROXY=http://127.0.0.1:7890
    #     REGISTER_PROXY=http://a:7897,socks5://b:1080
    REGISTER_PROXY: Optional[str] = None
    REGISTER_PROXY_ROTATE: bool = True

    # --- 账号健康 / 额度预测 / 死号清理 ---
    ACCOUNT_HEALTH_ENABLED: bool = True
    ACCOUNT_HEALTH_INTERVAL_MINUTES: int = 30
    ACCOUNT_FAILURE_THRESHOLD: int = 3
    ACCOUNT_EMPTY_THRESHOLD: int = 2
    ACCOUNT_ARCHIVE_ENABLED: bool = True
    ACCOUNT_ARCHIVE_MODE: str = "archive"
    # 每次确认账号无额度（Notion premium 信号）后直接删除该账号（默认关闭）
    AUTO_DELETE_QUOTA_EXCEEDED: bool = False
    ACCOUNT_ARCHIVE_QUOTA_DAYS: int = 7
    ACCOUNT_ARCHIVE_DISABLED_DAYS: int = 3
    ACCOUNT_MAINTENANCE_INTERVAL_MINUTES: int = 360
    FREE_CREDITS_LIMIT: float = 50.0
    QUOTA_RESERVE_REPLIES: int = 2

    # --- 注册出口节点池 / 暂停式熔断 ---
    REGISTER_NODE_COOLDOWN_BASE: int = 60
    REGISTER_NODE_COOLDOWN_MAX: int = 900
    # 单个节点连续失败达到该次数才冻结（成功一次即清零）
    REGISTER_NODE_FAILURE_THRESHOLD: int = 3
    REGISTER_NODE_STATE_FILE: str = "register_nodes.json"
    REGISTER_PAUSE_AFTER_FAILURES: int = 10
    REGISTER_PAUSE_SECONDS: int = 300
    AUTO_REPLENISH_HARD_DISABLE: bool = False
    REGISTER_EGRESS_MODE: str = "proxy"
    CLASH_API_URL: Optional[str] = None
    CLASH_PIPE: Optional[str] = None
    CLASH_SECRET: str = "set-your-secret"
    CLASH_GROUP: str = "YOUR_PROXY_GROUP"
    CLASH_NODES: Optional[str] = None
    CLASH_PROXY_URL: Optional[str] = None
    CLASH_RESTORE_NODE: Optional[str] = None
    CLASH_IDLE_RESTORE_SECONDS: int = 60
    CLASH_DRY_RUN: bool = False

    DEFAULT_MODEL: str = "almond-croissant-low"
    
    # Models from HAR getAvailableModels (codename → display name)
    KNOWN_MODELS: List[str] = [
        "almond-croissant-low",      # Sonnet 4.6
        "oatmeal-cookie",            # GPT-5.2
        "avocado-froyo-medium",     # Opus 4.6
        "apricot-sorbet-high",      # Opus 4.7
        "ambrosia-tart-high",        # Opus 4.8
        "opal-quince-medium",       # GPT-5.5
        "oval-kumquat-medium",      # GPT-5.4
        "oregon-grape-medium",      # GPT-5.4 Mini
        "otaheite-apple-medium",    # GPT-5.4 Nano
        "anthropic-haiku-4.5",      # Haiku 4.5
        "vertex-gemini-2.5-flash",  # Gemini 2.5 Flash
        "vertex-gemini-3.5-flash",  # Gemini 3.5 Flash
        "gingerbread",              # Gemini 3 Flash
        "galette-medium-thinking",  # Gemini 3.1 Pro
        "fireworks-kimi-k2.6",      # Kimi K2.6
        "baseten-deepseek-v4-pro",  # DeepSeek V4 Pro
        "baseten-glm-5.2",          # GLM 5.2
        "xigua-mochi-medium",       # Grok 4.3
        "xinomavro-cake",            # Grok Build 0.1
    ]
    
    # Map display names to backend codenames (for user-facing model selection)
    MODEL_MAP: dict = {
        "claude-sonnet-4.6": "almond-croissant-low",
        "gpt-5.2": "oatmeal-cookie",
        "claude-opus-4.6": "avocado-froyo-medium",
        "claude-opus-4.7": "apricot-sorbet-high",
        "claude-opus-4.8": "ambrosia-tart-high",
        "gpt-5.5": "opal-quince-medium",
        "gpt-5.4": "oval-kumquat-medium",
        "gpt-5.4-mini": "oregon-grape-medium",
        "gpt-5.4-nano": "otaheite-apple-medium",
        "claude-haiku-4.5": "anthropic-haiku-4.5",
        "gemini-2.5-flash": "vertex-gemini-2.5-flash",
        "gemini-3.5-flash": "vertex-gemini-3.5-flash",
        "gemini-3-flash": "gingerbread",
        "gemini-3.1-pro": "galette-medium-thinking",
        "kimi-k2.6": "fireworks-kimi-k2.6",
        "deepseek-v4-pro": "baseten-deepseek-v4-pro",
        "glm-5.2": "baseten-glm-5.2",
        "grok-4.3": "xigua-mochi-medium",
        "grok-build-0.1": "xinomavro-cake",
    }

settings = Settings()
