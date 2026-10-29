# main.py
import logging
import json
import time
import os
import uuid
import re
import copy
import asyncio
import sqlite3
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta

from fastapi import FastAPI, Request, HTTPException, Depends, Header
from fastapi.responses import JSONResponse, StreamingResponse, HTMLResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from fastapi.concurrency import run_in_threadpool

from app.core.config import settings
from app.core import model_registry
from app.core import account_health
from app.core import register_egress
from app.core.metrics import metrics
from app.providers.notion_provider import (
    NotionAIProvider,
    forget_threads_for_account,
    pinned_thread_account_id,
)
from app.utils.sse_utils import create_sse_data, DONE_CHUNK
from app.utils.http_client import (
    create_session,
    get_cookie,
    last_registration_egress,
    mask_proxy,
    next_registration_egress,
    registration_proxy_info,
    report_registration_result,
)
from app.adapters import detect_harness

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ── Global state ──
provider = NotionAIProvider()
START_TIME = time.time()

# ── SQLite-backed request log persistence ──
DB_PATH = os.path.join(os.path.dirname(__file__), "notion2api.db")

def _init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""CREATE TABLE IF NOT EXISTS request_logs (
        id TEXT PRIMARY KEY,
        timestamp TEXT NOT NULL,
        model TEXT NOT NULL,
        mapped_model TEXT DEFAULT '',
        content_preview TEXT DEFAULT '',
        prompt_tokens INTEGER DEFAULT 0,
        stream INTEGER DEFAULT 1,
        status TEXT DEFAULT 'pending',
        duration_ms INTEGER,
        error TEXT DEFAULT ''
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_ts ON request_logs(timestamp)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_model ON request_logs(model)")
    # Migration for databases created by older versions.
    columns = {row[1] for row in conn.execute("PRAGMA table_info(request_logs)")}
    if "completion_tokens" not in columns:
        conn.execute("ALTER TABLE request_logs ADD COLUMN completion_tokens INTEGER DEFAULT 0")
    if "account_email" not in columns:
        conn.execute("ALTER TABLE request_logs ADD COLUMN account_email TEXT DEFAULT ''")
    if "response_content" not in columns:
        conn.execute("ALTER TABLE request_logs ADD COLUMN response_content TEXT DEFAULT ''")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_account ON request_logs(account_email)")
    conn.commit()
    conn.close()

_init_db()

def _db_insert_log(entry: Dict[str, Any]):
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            "INSERT INTO request_logs (id,timestamp,model,mapped_model,content_preview,prompt_tokens,stream,status,duration_ms,error,account_email) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (entry["id"], entry["timestamp"], entry.get("model",""), entry.get("mapped_model",""),
             entry.get("content_preview",""), entry.get("prompt_tokens",0),
             1 if entry.get("stream") else 0, entry.get("status","pending"),
             entry.get("duration_ms"), entry.get("error",""), entry.get("account_email","")))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"DB insert failed: {e}")

def _db_update_log(log_id: str, **kwargs):
    try:
        conn = sqlite3.connect(DB_PATH)
        sets = ", ".join(f"{k}=?" for k in kwargs)
        vals = list(kwargs.values()) + [log_id]
        conn.execute(f"UPDATE request_logs SET {sets} WHERE id=?", vals)
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"DB update failed: {e}")

def _db_query_logs(limit: int = 50, offset: int = 0, model: str = "", status: str = "", search: str = "") -> Dict:
    """Paged request-log query with account/e-mail/content search."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    where_parts = []
    params = []
    if model:
        where_parts.append("model=?")
        params.append(model)
    if status:
        where_parts.append("status=?")
        params.append(status)
    if search:
        like = "%" + str(search).strip() + "%"
        where_parts.append("(account_email LIKE ? OR content_preview LIKE ? OR response_content LIKE ? OR model LIKE ? OR error LIKE ?)")
        params.extend([like, like, like, like, like])
    where = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""
    total = conn.execute(f"SELECT COUNT(*) FROM request_logs{where}", params).fetchone()[0]
    rows = conn.execute(
        "SELECT id,timestamp,model,mapped_model,content_preview,account_email,prompt_tokens,completion_tokens,"
        "stream,status,duration_ms,error,response_content "
        f"FROM request_logs{where} ORDER BY timestamp DESC LIMIT ? OFFSET ?",
        params + [limit, offset],
    ).fetchall()
    results = [dict(r) for r in rows]
    for item in results:
        # Keep the list response small; the detail view can show the full text.
        response = item.get("response_content") or ""
        item["response_preview"] = response[:200]
        item.pop("response_content", None)
    conn.close()
    page_size = max(1, int(limit or 1))
    total_pages = max(1, (total + page_size - 1) // page_size) if total else 1
    return {"total": total, "page": int(offset // page_size) + 1, "page_size": page_size,
            "total_pages": total_pages, "results": results}

def _db_count_logs() -> int:
    try:
        conn = sqlite3.connect(DB_PATH)
        count = conn.execute("SELECT COUNT(*) FROM request_logs").fetchone()[0]
        conn.close()
        return count
    except:
        return 0

# ── Registration state ──
REG_LOGS_FILE = os.path.join(os.path.dirname(__file__), "reg_logs.jsonl")
reg_logs: List[Dict[str, Any]] = []
MAX_REG_LOGS = 1000


def _load_reg_logs_from_disk() -> List[Dict[str, Any]]:
    if not os.path.exists(REG_LOGS_FILE):
        return []
    entries: List[Dict[str, Any]] = []
    try:
        with open(REG_LOGS_FILE, "r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(item, dict):
                    entries.append(item)
    except OSError:
        return []
    return entries[-MAX_REG_LOGS:]


reg_logs.extend(_load_reg_logs_from_disk())
reg_active_count = 0
reg_max_concurrent = 3
reg_semaphore = asyncio.Semaphore(reg_max_concurrent)

# ── Account store ──
ACCOUNTS_FILE = os.path.join(os.path.dirname(__file__), "accounts.json")

def load_accounts() -> List[Dict[str, Any]]:
    if os.path.exists(ACCOUNTS_FILE):
        try:
            with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []
    return []

def save_accounts(accounts: List[Dict[str, Any]]):
    with open(ACCOUNTS_FILE, "w", encoding="utf-8") as f:
        json.dump(accounts, f, indent=2, ensure_ascii=False)


accounts_store = [account_health.normalize_account(account) for account in load_accounts()]

# ── Account health / quota prediction / dead-account archiving ──
ARCHIVE_FILE = os.path.join(os.path.dirname(__file__), "accounts_archive.json")
ACCOUNT_HEALTH_ENABLED = bool(getattr(settings, "ACCOUNT_HEALTH_ENABLED", True))
ACCOUNT_HEALTH_INTERVAL_MINUTES = max(1, int(getattr(settings, "ACCOUNT_HEALTH_INTERVAL_MINUTES", 30) or 30))
ACCOUNT_FAILURE_THRESHOLD = max(1, int(getattr(settings, "ACCOUNT_FAILURE_THRESHOLD", 3) or 3))
ACCOUNT_EMPTY_THRESHOLD = max(1, int(getattr(settings, "ACCOUNT_EMPTY_THRESHOLD", 2) or 2))
ACCOUNT_ARCHIVE_ENABLED = bool(getattr(settings, "ACCOUNT_ARCHIVE_ENABLED", True))
ACCOUNT_ARCHIVE_MODE = str(getattr(settings, "ACCOUNT_ARCHIVE_MODE", "archive") or "archive").lower()
if ACCOUNT_ARCHIVE_MODE not in ("archive", "delete"):
    ACCOUNT_ARCHIVE_MODE = "archive"
ACCOUNT_ARCHIVE_QUOTA_DAYS = max(0, int(getattr(settings, "ACCOUNT_ARCHIVE_QUOTA_DAYS", 7) or 0))
ACCOUNT_ARCHIVE_DISABLED_DAYS = max(0, int(getattr(settings, "ACCOUNT_ARCHIVE_DISABLED_DAYS", 3) or 0))
ACCOUNT_MAINTENANCE_INTERVAL_MINUTES = max(1, int(getattr(settings, "ACCOUNT_MAINTENANCE_INTERVAL_MINUTES", 360) or 360))
FREE_CREDITS_LIMIT = float(getattr(settings, "FREE_CREDITS_LIMIT", 50.0) or 50.0)
QUOTA_RESERVE_REPLIES = max(0, int(getattr(settings, "QUOTA_RESERVE_REPLIES", 2) or 0))
_account_health_task = None
_account_maintenance_task = None

AUTO_DELETE_QUOTA_EXCEEDED = bool(getattr(settings, "AUTO_DELETE_QUOTA_EXCEEDED", False))


def _delete_account_now(account: Dict[str, Any], reason: str = "quota_exceeded") -> None:
    """Remove one account from the pool immediately (used by auto-delete)."""
    account_id = str(account.get("id") or "")
    if not account_id:
        return
    accounts_store[:] = [item for item in accounts_store if str(item.get("id") or "") != account_id]
    try:
        forget_threads_for_account(account_id)
    except Exception:
        pass
    label = account.get("user_name") or account.get("user_email") or account_id[:8]
    _add_reg_log("warning", f"账号 {label} 无额度，已自动删除（原因: {reason}）")
    metrics.inc("accounts_auto_deleted_total", reason=reason)
    save_accounts(accounts_store)
    _trigger_auto_replenish()


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def _masked_account(account: Dict[str, Any]) -> Dict[str, Any]:
    """Safe-to-render account (token masked) with health and quota estimate."""
    account_health.normalize_account(account)
    payload = dict(account)
    payload.pop("token_v2", None)
    payload["token_preview"] = account_health.token_preview(account.get("token_v2", ""))
    payload["health"] = dict(account.get("health") or {})
    payload["credit_stats"] = dict(account.get("credit_stats") or {})
    payload["quota"] = dict(account_health.predict_quota(account, free_credits_limit=FREE_CREDITS_LIMIT))
    return payload


def _record_account_outcome(account: Dict[str, Any], outcome: str, detail: Any = "", usage: Optional[Dict[str, Any]] = None):
    """Persist one classified request outcome on an account (Feature B core hook)."""
    before = account.get("status")
    if isinstance(detail, dict) and outcome != account_health.OUTCOME_QUOTA:
        apply_detail: Any = json.dumps(detail, ensure_ascii=False)
    else:
        apply_detail = detail
    account_health.apply_outcome(
        account,
        outcome,
        detail=apply_detail,
        usage=usage,
        failure_threshold=ACCOUNT_FAILURE_THRESHOLD,
        empty_threshold=ACCOUNT_EMPTY_THRESHOLD,
        free_credits_limit=FREE_CREDITS_LIMIT,
    )
    metrics.inc("account_outcomes_total", outcome=outcome)
    after = account.get("status")
    label = account.get("user_name") or account.get("user_email") or str(account.get("id"))[:8]
    if after != before and after == account_health.STATUS_DISABLED:
        try:
            forget_threads_for_account(account.get("id") or "")
        except Exception:
            pass
        _add_reg_log("warning", f"账号 {label} 已禁用: {account.get('health', {}).get('disabled_reason', '')}")
        _trigger_auto_replenish()
    elif after != before and after == account_health.STATUS_QUOTA_EXCEEDED:
        if account_health.should_auto_delete(outcome, AUTO_DELETE_QUOTA_EXCEEDED):
            _delete_account_now(account, reason="quota_exceeded")
            return account
        _add_reg_log("warning", f"账号 {label} Notion 额度已耗尽")
        _trigger_auto_replenish()
    save_accounts(accounts_store)
    return account


def _record_request_metrics(model: str, harness: str, status: str, duration_seconds: float) -> None:
    metrics.inc("notion_requests_total", status=status, model=str(model or "unknown"), harness=str(harness or "generic"))
    metrics.observe("notion_request_duration_seconds", float(duration_seconds), model=str(model or "unknown"))


def _account_age_seconds(account: Dict[str, Any]) -> float:
    source = account.get("status_changed_at") or account.get("created_at") or ""
    try:
        return max(0.0, time.time() - datetime.fromisoformat(source).timestamp())
    except (TypeError, ValueError):
        return 0.0


async def _run_account_health_pass() -> int:
    """Probe active accounts with a lightweight getAvailableModels call."""
    if not ACCOUNT_HEALTH_ENABLED:
        return 0
    now = time.time()
    interval = ACCOUNT_HEALTH_INTERVAL_MINUTES * 60
    candidates = []
    for account in accounts_store:
        if account.get("status") != account_health.STATUS_ACTIVE:
            continue
        last_probe = (account.get("health") or {}).get("last_probe_at") or ""
        try:
            age = now - datetime.fromisoformat(last_probe).timestamp()
        except (TypeError, ValueError):
            age = interval + 1
        if age >= interval:
            candidates.append(account)
    if not candidates:
        return 0
    semaphore = asyncio.Semaphore(2)

    async def check(account: Dict[str, Any]) -> None:
        async with semaphore:
            try:
                result = await provider.probe_account(account)
            except Exception as exc:
                result = {"ok": False, "error": str(exc)[:200], "transport": True}
        health = account.setdefault("health", {})
        health["last_probe_at"] = _now_iso()
        health["last_probe_ok"] = bool(result.get("ok"))
        if result.get("ok"):
            return
        health["last_probe_error"] = str(result.get("error") or "")[:200]
        if result.get("invalid"):
            _record_account_outcome(account, account_health.OUTCOME_INVALID, detail=result.get("error") or "probe invalid")

    await asyncio.gather(*[check(account) for account in candidates[:20]], return_exceptions=True)
    save_accounts(accounts_store)
    return len(candidates)


async def _account_health_monitor() -> None:
    await asyncio.sleep(60)
    while True:
        try:
            checked = await _run_account_health_pass()
            if checked:
                logger.info("账号健康巡检: %d 个账号", checked)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(f"账号健康巡检异常: {exc}")
        await asyncio.sleep(max(60, ACCOUNT_HEALTH_INTERVAL_MINUTES * 60))


def _run_account_maintenance() -> int:
    """Archive/delete dead accounts past their configured grace periods."""
    if not ACCOUNT_ARCHIVE_ENABLED:
        return 0
    targets = account_health.archivable_accounts(
        accounts_store,
        now=time.time(),
        quota_days=ACCOUNT_ARCHIVE_QUOTA_DAYS,
        disabled_days=ACCOUNT_ARCHIVE_DISABLED_DAYS,
    )
    if not targets:
        return 0
    removed, _remaining = account_health.archive_accounts(
        accounts_store, ARCHIVE_FILE, targets, mode=ACCOUNT_ARCHIVE_MODE
    )
    for account in removed:
        try:
            forget_threads_for_account(account.get("id") or "")
        except Exception:
            pass
    save_accounts(accounts_store)
    _trigger_auto_replenish()
    _add_reg_log("info", f"账号维护: 清理 {len(removed)} 个死号（{ACCOUNT_ARCHIVE_MODE}）")
    logger.info("账号维护: 清理 %d 个死号 (%s)", len(removed), ACCOUNT_ARCHIVE_MODE)
    return len(removed)


async def _account_maintenance_monitor() -> None:
    await asyncio.sleep(120)
    while True:
        try:
            _run_account_maintenance()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(f"账号维护任务异常: {exc}")
        await asyncio.sleep(max(60, ACCOUNT_MAINTENANCE_INTERVAL_MINUTES * 60))

# ── Quota & Auto-Replenish ──
QUOTA_LIMIT = 0  # 0 = no manual limit, detect from Notion's premium-feature-unavailable response
auto_replenish_enabled = os.environ.get("AUTO_REPLENISH_ENABLED", "1").strip().lower() not in ("0", "false", "no", "off")
AUTO_REPLENISH_TARGET = max(1, int(os.environ.get("AUTO_REPLENISH_TARGET", "10") or 10))
auto_replenish_domain = settings.TEMP_EMAIL_DOMAIN
_account_rotation_index = 0  # Round-robin counter
_replenish_in_progress = False  # Prevents stacking replenish tasks
_replenish_consecutive_failures = 0
_REPLENISH_FAILURE_LIMIT = 10  # kept for API/dashboard compatibility
REGISTER_PAUSE_AFTER_FAILURES = max(1, int(getattr(settings, "REGISTER_PAUSE_AFTER_FAILURES", 10) or 10))
REGISTER_PAUSE_SECONDS = max(1, int(getattr(settings, "REGISTER_PAUSE_SECONDS", 300) or 300))
AUTO_REPLENISH_HARD_DISABLE = bool(getattr(settings, "AUTO_REPLENISH_HARD_DISABLE", False))
_replenish_paused_until = 0.0
_replenish_pause_reason = ""
_registration_failure_categories: Dict[str, int] = {}
_registration_inflight = 0  # Registration tasks queued/running in the background
_replenish_monitor_task = None  # Background task keeping the account pool at target


def _replenish_paused() -> bool:
    return bool(_replenish_paused_until) and time.time() < float(_replenish_paused_until)


def _clear_replenish_pause() -> None:
    global _replenish_paused_until, _replenish_pause_reason, _replenish_consecutive_failures, _registration_failure_categories
    _replenish_paused_until = 0.0
    _replenish_pause_reason = ""
    _replenish_consecutive_failures = 0
    _registration_failure_categories = {}


def _apply_replenish_pause(reason: str = "") -> None:
    """Timed pause after repeated registration failures (default), or old hard disable."""
    global _replenish_paused_until, _replenish_pause_reason, auto_replenish_enabled
    decision = register_egress.compute_pause(
        _replenish_consecutive_failures,
        _registration_failure_categories,
        now=time.time(),
        pause_after=REGISTER_PAUSE_AFTER_FAILURES,
        pause_seconds=REGISTER_PAUSE_SECONDS,
        hard_disable=AUTO_REPLENISH_HARD_DISABLE,
    )
    if decision.get("hard_disable"):
        auto_replenish_enabled = False
        _add_reg_log(
            "error",
            f"自动补号熔断: 连续 {_replenish_consecutive_failures} 次注册失败，已自动关闭自动补号。",
        )
        return
    if decision.get("should_pause"):
        _replenish_paused_until = float(decision.get("paused_until") or 0)
        _replenish_pause_reason = reason or str(decision.get("reason") or "consecutive_failures")
        metrics.inc("registration_pauses_total", reason=_replenish_pause_reason)
        _add_reg_log(
            "warning",
            f"自动补号暂停 {REGISTER_PAUSE_SECONDS}s（原因: {_replenish_pause_reason}），到期自动恢复",
        )

def _pick_account() -> Optional[Dict[str, Any]]:
    """Pick a schedulable account with round-robin; None when the pool is dry."""
    global _account_rotation_index
    active = [a for a in accounts_store if a.get("status") == "active"]
    if not active:
        return None
    available = [
        a for a in active
        if account_health.is_available(a, QUOTA_LIMIT, QUOTA_RESERVE_REPLIES)
    ]
    if not available:
        # Retain the legacy behaviour: mark accounts over the manual limit.
        changed = False
        for a in active:
            if QUOTA_LIMIT > 0 and a.get("total_requests", 0) >= QUOTA_LIMIT:
                a["status"] = "quota_exceeded"
                a["quota"] = {"used": a.get("total_requests", 0), "total": QUOTA_LIMIT,
                              "remaining": 0, "exceeded": True}
                a["status_changed_at"] = _now_iso()
                changed = True
        if changed:
            save_accounts(accounts_store)
        return None
    _account_rotation_index = _account_rotation_index % len(available)
    chosen = available[_account_rotation_index]
    _account_rotation_index += 1
    return chosen

def _available_account_count() -> int:
    """Accounts that can currently serve inference (health + quota reserve)."""
    return sum(1 for account in accounts_store if account_health.is_available(account, QUOTA_LIMIT, QUOTA_RESERVE_REPLIES))


async def _run_registration_task(task_id: str, domain: str) -> None:
    """One queued registration worker (bounded by reg_semaphore)."""
    global reg_active_count, _registration_inflight, _replenish_consecutive_failures
    async with reg_semaphore:
        reg_active_count += 1
        _add_reg_log("info", f"开始注册任务 (并发: {reg_active_count}/{reg_max_concurrent})", task_id)
        try:
            if register_egress.get_egress_pool().snapshot().get("all_cooling"):
                raise register_egress.RegistrationFailure(
                    category=register_egress.CATEGORY_ALL_COOLING,
                    detail="所有注册出口节点均在冷却中",
                )
            await run_in_threadpool(_do_auto_register_sync, task_id, domain)
            _replenish_consecutive_failures = 0
            metrics.inc("registration_tasks_total", result="success", category="ok")
            _add_reg_log("info", "注册任务完成，账号已加入池", task_id)
        except Exception as e:
            category = getattr(e, "category", None) or register_egress.classify_registration_error(str(e))
            _replenish_consecutive_failures += 1
            _registration_failure_categories[category] = _registration_failure_categories.get(category, 0) + 1
            metrics.inc("registration_tasks_total", result="failure", category=category)
            _add_reg_log(
                "error",
                f"注册失败类别[{category}] ({_replenish_consecutive_failures}/{REGISTER_PAUSE_AFTER_FAILURES}): {str(e)[:200]}",
                task_id,
            )
            _apply_replenish_pause(reason=category)
        finally:
            reg_active_count -= 1
            _registration_inflight = max(0, _registration_inflight - 1)


def _enqueue_registrations(count: int, domain: str = "", reason: str = "") -> List[str]:
    """Append registration tasks to the background queue and return their ids."""
    global _registration_inflight
    if count <= 0:
        return []
    target_domain = (domain or auto_replenish_domain).strip()
    task_ids: List[str] = []
    for _ in range(min(int(count), 20)):
        task_id = uuid.uuid4().hex[:8]
        task_ids.append(task_id)
        _registration_inflight += 1
        asyncio.create_task(_run_registration_task(task_id, target_domain))
    _add_reg_log(
        "info",
        f"注册队列 +{len(task_ids)}" + (f" ({reason})" if reason else "") + f"，队列中: {_registration_inflight}",
    )
    return task_ids


def _trigger_auto_replenish():
    """Top the account pool back up to AUTO_REPLENISH_TARGET in the background."""
    if not auto_replenish_enabled:
        return
    if _replenish_paused():
        return
    available = _available_account_count()
    shortage = AUTO_REPLENISH_TARGET - available - _registration_inflight
    if shortage <= 0:
        return
    _add_reg_log(
        "info",
        f"自动补号: 可用 {available}/{AUTO_REPLENISH_TARGET}，注册中 {_registration_inflight}，追加 {shortage} 个注册任务",
    )
    _enqueue_registrations(shortage, reason="auto-replenish")


async def _auto_replenish_monitor():
    """Periodically keep AUTO_REPLENISH_TARGET accounts available."""
    await asyncio.sleep(3)
    while True:
        try:
            if auto_replenish_enabled:
                if _replenish_paused():
                    if time.time() >= float(_replenish_paused_until):
                        _add_reg_log("info", f"自动补号暂停结束（{_replenish_pause_reason or 'consecutive_failures'}），恢复检查")
                        _clear_replenish_pause()
                else:
                    available = _available_account_count()
                    if available + _registration_inflight < AUTO_REPLENISH_TARGET:
                        _trigger_auto_replenish()
            # Clash rotation: restore the user's original node once registration is idle.
            try:
                register_egress.get_egress_pool().maybe_restore(
                    int(getattr(settings, "CLASH_IDLE_RESTORE_SECONDS", 60) or 60)
                )
            except Exception:
                pass
        except Exception as exc:  # never let the monitor die
            logger.warning(f"自动补号监控异常: {exc}")
        await asyncio.sleep(15)


# ── Config ──
# Temp-email service used by auto-registration. Configure via .env instead of
# editing this file (TEMP_EMAIL_BASE / TEMP_EMAIL_ADMIN_AUTH / TEMP_EMAIL_DOMAIN).
TEMP_EMAIL_BASE = settings.TEMP_EMAIL_BASE.rstrip("/")
TEMP_EMAIL_ADMIN_AUTH = settings.TEMP_EMAIL_ADMIN_AUTH
TEMP_EMAIL_DOMAIN = settings.TEMP_EMAIL_DOMAIN
NOTION_BASE = "https://app.notion.com"

# ── Helpers ──
def _add_reg_log(level: str, message: str, task_id: str = ""):
    entry = {
        "id": str(uuid.uuid4()),
        "timestamp": datetime.now().astimezone().isoformat(),
        "level": level,
        "message": message,
        "task_id": task_id,
    }
    reg_logs.append(entry)
    if len(reg_logs) > MAX_REG_LOGS:
        reg_logs.pop(0)
    try:
        with open(REG_LOGS_FILE, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except OSError:
        pass
    logger.info(f"[REG{(':' + task_id) if task_id else ''}] {message}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _replenish_monitor_task, _account_health_task, _account_maintenance_task
    logger.info(f"应用启动中... {settings.APP_NAME} v{settings.APP_VERSION}")
    logger.info(f"服务将在 http://0.0.0.0:{settings.NGINX_PORT} 上可用")
    logger.info(
        f"自动补号: {'开启' if auto_replenish_enabled else '关闭'}，目标 {AUTO_REPLENISH_TARGET} 个可用账号"
    )
    logger.info(
        "账号健康: %s（每 %d 分钟探活），死号清理: %s（quota %d 天 / disabled %d 天，%s）",
        "开启" if ACCOUNT_HEALTH_ENABLED else "关闭",
        ACCOUNT_HEALTH_INTERVAL_MINUTES,
        "开启" if ACCOUNT_ARCHIVE_ENABLED else "关闭",
        ACCOUNT_ARCHIVE_QUOTA_DAYS,
        ACCOUNT_ARCHIVE_DISABLED_DAYS,
        ACCOUNT_ARCHIVE_MODE,
    )
    _replenish_monitor_task = asyncio.create_task(_auto_replenish_monitor())
    if ACCOUNT_HEALTH_ENABLED:
        _account_health_task = asyncio.create_task(_account_health_monitor())
    if ACCOUNT_ARCHIVE_ENABLED:
        _account_maintenance_task = asyncio.create_task(_account_maintenance_monitor())
    yield
    for task in (_replenish_monitor_task, _account_health_task, _account_maintenance_task):
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
    logger.info("应用关闭。")

app = FastAPI(title=settings.APP_NAME, version=settings.APP_VERSION, description=settings.DESCRIPTION, lifespan=lifespan)

DASHBOARD_DIR = os.path.join(os.path.dirname(__file__), "app", "dashboard")
app.mount("/dashboard/static", StaticFiles(directory=DASHBOARD_DIR), name="dashboard_static")


# Dashboard pages and their JSON APIs must never be cached: the browser would
# otherwise keep an old shared.js after an upgrade and every data page would
# stay empty until a hard refresh.
@app.middleware("http")
async def _dashboard_no_cache(request: Request, call_next):
    response = await call_next(request)
    path = request.url.path
    if path.startswith("/dashboard") or path.startswith("/api/") or path == "/metrics":
        response.headers["Cache-Control"] = "no-store, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response

async def verify_api_key(authorization: Optional[str] = Header(None)):
    if settings.API_MASTER_KEY and settings.API_MASTER_KEY != "1":
        if not authorization or "bearer" not in authorization.lower():
            raise HTTPException(status_code=401, detail="需要 Bearer Token 认证。")
        token = authorization.split(" ")[-1]
        if token != settings.API_MASTER_KEY:
            raise HTTPException(status_code=403, detail="无效的 API Key。")

# ── API Routes ──
@app.post("/v1/chat/completions", dependencies=[Depends(verify_api_key)])
async def chat_completions(request: Request) -> StreamingResponse:
    request_data = await request.json()
    # Which harness is calling us? Drives the tool-call protocol adapter.
    request_data["_harness"] = detect_harness(request.headers, request_data)
    if request_data.get("tools"):
        _ua = str(request.headers.get("user-agent") or "")[:100]
        logger.info(f"[REQ] harness={request_data['_harness']} tools={len(request_data.get('tools') or [])} ua={_ua!r}")
    model = request_data.get("model", settings.DEFAULT_MODEL)
    mapped_model = model_registry.resolve(model)

    # Pick initial account. Multi-turn conversations must keep using the
    # account that owns their cached Notion thread: Notion answers an empty body
    # when a thread is queried by a different account, so round-robin must not
    # rotate the account between turns of the same conversation.
    account = None
    pinned_account_id = pinned_thread_account_id(request_data.get("messages") or [])
    if pinned_account_id:
        for candidate in accounts_store:
            if str(candidate.get("id")) != pinned_account_id:
                continue
            quota = candidate.get("quota") or {}
            if candidate.get("status") == "active" and not (
                quota.get("exceeded") and quota.get("source") == "notion"
            ):
                account = candidate
            break
        if account is None:
            logger.info(f"[THREAD] 绑定的账号 {pinned_account_id[:8]} 不可用，重新选择账号")
    if account is None:
        account = _pick_account()
    if not account:
        _trigger_auto_replenish()
        raise HTTPException(status_code=429, detail="所有账号额度已用尽，请稍后再试或开启自动补号。")

    # Normalize content for preview & token counting
    def _norm_content(c):
        if isinstance(c, str): return c
        if isinstance(c, list):
            parts = []
            for p in c:
                if isinstance(p, str): parts.append(p)
                elif isinstance(p, dict) and p.get("type") == "text": parts.append(p.get("text", ""))
            return "\n".join(parts)
        return str(c) if c is not None else ""

    log_entry = {
        "id": str(uuid.uuid4()),
        "timestamp": datetime.now().astimezone().isoformat(),
        "model": model,
        "mapped_model": mapped_model,
        "account_id": account.get("id", ""),
        "account_email": account.get("user_email", "") or account.get("user_name", ""),
        "content_preview": _norm_content(request_data.get("messages", [{}])[-1].get("content", ""))[:80],
        "prompt_tokens": sum(len(_norm_content(m.get("content", "")).split()) for m in request_data.get("messages", [])),
        "stream": request_data.get("stream", True),
        "status": "pending",
        "duration_ms": None,
        "completion_tokens": None,
    }
    _db_insert_log(log_entry)
    t_start = time.time()

    # Always ask the provider for a usage chunk; strip it from the client stream
    # unless the client requested stream_options.include_usage.
    client_include_usage = bool((request_data.get("stream_options") or {}).get("include_usage"))
    if request_data.get("stream", True) and not client_include_usage:
        request_data.setdefault("stream_options", {})["include_usage"] = True

    # ★ Seamless account switching: if quota exceeded, auto-retry with next account
    MAX_SEAMLESS_RETRIES = 5  # Max accounts to try before giving up
    for _attempt in range(MAX_SEAMLESS_RETRIES):
        try:
            result = await provider.chat_completion(copy.deepcopy(request_data), account=account)
            break  # Success, proceed
        except Exception as e:
            outcome = account_health.classify_failure(e)
            err_str = str(e)
            _record_account_outcome(account, outcome, detail=err_str)
            if outcome == account_health.OUTCOME_QUOTA:
                _add_reg_log("warning", f"账号 {account.get('user_name','')} 额度耗尽，自动切换下一账号")
                account = _pick_account()
                if not account:
                    _trigger_auto_replenish()
                    raise HTTPException(status_code=429, detail="所有账号额度已用尽，请稍后再试或开启自动补号。")
                continue
            if outcome in (
                account_health.OUTCOME_INVALID,
                account_health.OUTCOME_EMPTY,
                account_health.OUTCOME_TRUNCATED,
            ):
                next_account = _pick_account()
                if next_account and _attempt < MAX_SEAMLESS_RETRIES - 1:
                    logger.warning("账号异常(%s)，切换下一账号重试: %s", outcome, err_str[:160])
                    account = next_account
                    continue
                if next_account is None:
                    _trigger_auto_replenish()
                    raise HTTPException(status_code=429, detail="所有账号当前不可用，请稍后再试或开启自动补号。")
            elapsed = int((time.time() - t_start) * 1000)
            log_entry["status"] = "error"
            log_entry["error"] = err_str[:500]
            log_entry["duration_ms"] = elapsed
            _db_update_log(log_entry["id"], status="error", duration_ms=elapsed, error=err_str[:500])
            _record_request_metrics(model, request_data.get("_harness") or "generic", "error", elapsed / 1000.0)
            logger.error(f"Notion 上游请求失败: {e}")
            raise HTTPException(status_code=502, detail=f"Notion 上游请求失败: {err_str}")
    else:
        raise HTTPException(status_code=429, detail="所有账号额度已用尽，请稍后再试或开启自动补号。")

    try:
        # For streaming responses, wrap the generator for seamless switching + keepalive
        if isinstance(result, StreamingResponse):
            original_body = result.body_iterator
            quota_hit = False
            stream_error = False
            content_seen = False
            stream_usage = None
            response_parts: List[str] = []

            async def seamless_generator():
                nonlocal quota_hit, account, stream_error, content_seen, stream_usage
                # ★ Seamless account switching: if quota exceeded mid-stream,
                # mark account, pick next, and retry the request transparently
                chunks_buffer = []  # Buffer chunks until we know if quota was hit
                try:
                    async for chunk in original_body:
                        # Check for quota exceeded in SSE data
                        try:
                            chunk_str = chunk.decode("utf-8", errors="ignore") if isinstance(chunk, bytes) else str(chunk)
                            if "premium-feature-unavailable" in chunk_str or "quota_exceeded" in chunk_str:
                                quota_hit = True
                            if '"error"' in chunk_str:
                                stream_error = True
                            if '"content":"' in chunk_str.replace(" ", ""):
                                content_seen = True
                            if chunk_str.startswith("data: "):
                                try:
                                    payload = json.loads(chunk_str[6:].strip())
                                    for choice in payload.get("choices") or []:
                                        delta = choice.get("delta") or {}
                                        if delta.get("content"):
                                            response_parts.append(str(delta["content"]))
                                except Exception:
                                    pass
                            if '"usage"' in chunk_str:
                                try:
                                    payload = json.loads(chunk_str.split("data: ", 1)[1].strip())
                                    if payload.get("usage"):
                                        stream_usage = payload["usage"]
                                        if not client_include_usage:
                                            continue  # internal usage chunk, keep it away from the client
                                except Exception:
                                    pass
                        except Exception:
                            pass
                        if quota_hit:
                            # Don't yield the error chunk - we'll retry
                            break
                        chunks_buffer.append(chunk)
                    
                    # Check if we got any actual content (not just role chunk)
                    has_content = any(
                        b'"content"' in (c if isinstance(c, bytes) else c.encode())
                        for c in chunks_buffer
                    )
                    if quota_hit and not has_content:
                        # Quota hit before any content - transparent retry with next account
                        account["status"] = "quota_exceeded"
                        account["quota"] = {"used": account.get("total_requests", 0), "total": None, "remaining": 0, "exceeded": True, "source": "notion"}
                        save_accounts(accounts_store)
                        _add_reg_log("info", f"账号 {account.get('user_name','')} 额度耗尽，无缝切换下一账号")
                        _trigger_auto_replenish()
                        
                        # Try up to 5 more accounts
                        for _retry in range(5):
                            next_account = _pick_account()
                            if not next_account:
                                _trigger_auto_replenish()
                                # No more accounts - yield the error
                                yield create_sse_data({"error": {"message": "所有账号额度已用尽", "type": "quota_exceeded"}})
                                yield DONE_CHUNK
                                return
                            try:
                                retry_result = await provider.chat_completion(copy.deepcopy(request_data), account=next_account)
                                if isinstance(retry_result, StreamingResponse):
                                    # Yield the retry stream directly
                                    async for retry_chunk in retry_result.body_iterator:
                                        # Check for quota in retry too
                                        try:
                                            rc_str = retry_chunk.decode("utf-8", errors="ignore") if isinstance(retry_chunk, bytes) else str(retry_chunk)
                                            if "premium-feature-unavailable" in rc_str or "quota_exceeded" in rc_str:
                                                # This account also exhausted, try next
                                                next_account["status"] = "quota_exceeded"
                                                next_account["quota"] = {"used": next_account.get("total_requests", 0), "total": None, "remaining": 0, "exceeded": True, "source": "notion"}
                                                save_accounts(accounts_store)
                                                _trigger_auto_replenish()
                                                break
                                        except Exception:
                                            pass
                                        yield retry_chunk
                                    else:
                                        # Retry succeeded. Clear quota_hit, otherwise the
                                        # finally block would mark the *new* account as exhausted.
                                        account = next_account
                                        quota_hit = False
                                        return
                                    continue  # This account failed, try next
                                else:
                                    # Non-stream retry result (shouldn't happen in stream mode)
                                    account = next_account
                                    return
                            except Exception:
                                continue
                        # All retries exhausted
                        yield create_sse_data({"error": {"message": "所有账号额度已用尽", "type": "quota_exceeded"}})
                        yield DONE_CHUNK
                        return
                    else:
                        # No quota hit, or quota hit after some content - yield buffered chunks
                        for chunk in chunks_buffer:
                            yield chunk
                finally:
                    elapsed = int((time.time() - t_start) * 1000)
                    final_status = "error" if (stream_error and not content_seen) else "success"
                    log_entry["status"] = final_status
                    log_entry["duration_ms"] = elapsed
                    update_fields = {"status": final_status, "duration_ms": elapsed}
                    if stream_usage:
                        update_fields["prompt_tokens"] = stream_usage.get("prompt_tokens", 0)
                        update_fields["completion_tokens"] = stream_usage.get("completion_tokens", 0)
                        prompt_tokens = int(stream_usage.get("prompt_tokens") or 0)
                        completion_tokens = int(stream_usage.get("completion_tokens") or 0)
                        credits_used = float(stream_usage.get("credits_used") or 0)
                        account["total_prompt_tokens"] = account.get("total_prompt_tokens", 0) + prompt_tokens
                        account["total_completion_tokens"] = account.get("total_completion_tokens", 0) + completion_tokens
                        account["total_credits"] = round(account.get("total_credits", 0) + credits_used, 2)
                        metrics.inc("notion_tokens_total", prompt_tokens, kind="prompt")
                        metrics.inc("notion_tokens_total", completion_tokens, kind="completion")
                        metrics.inc("notion_credits_total", credits_used)
                    update_fields["account_email"] = account.get("user_email", "") or account.get("user_name", "")
                    update_fields["response_content"] = "".join(response_parts)[:20000]
                    _db_update_log(log_entry["id"], **update_fields)
                    account["total_requests"] = account.get("total_requests", 0) + 1
                    if quota_hit:
                        _record_account_outcome(account, account_health.OUTCOME_QUOTA, detail="premium-feature-unavailable")
                    elif final_status == "success":
                        _record_account_outcome(account, account_health.OUTCOME_SUCCESS, usage=stream_usage)
                    if QUOTA_LIMIT > 0 and account["total_requests"] >= QUOTA_LIMIT:
                        account["status"] = "quota_exceeded"
                        account["quota"] = {"used": account["total_requests"], "total": QUOTA_LIMIT, "remaining": 0, "exceeded": True}
                        account["status_changed_at"] = _now_iso()
                        _add_reg_log("warning", f"账号 {account.get('user_name','')} 额度已耗尽 ({account['total_requests']}/{QUOTA_LIMIT})")
                        _trigger_auto_replenish()
                    _record_request_metrics(model, request_data.get("_harness") or "generic", final_status, elapsed / 1000.0)
                    save_accounts(accounts_store)

            result = StreamingResponse(seamless_generator(), media_type=result.media_type)
        else:
            elapsed = int((time.time() - t_start) * 1000)
            usage = None
            response_text = ""
            try:
                if isinstance(result, JSONResponse) and result.body:
                    body_payload = json.loads(result.body)
                    usage = body_payload.get("usage")
                    message = ((body_payload.get("choices") or [{}])[0].get("message") or {})
                    response_text = str(message.get("content") or "")
                    if not response_text and message.get("tool_calls"):
                        response_text = json.dumps(message["tool_calls"], ensure_ascii=False)
            except Exception:
                usage = None
            log_entry["status"] = "success"
            log_entry["duration_ms"] = elapsed
            update_fields = {"status": "success", "duration_ms": elapsed,
                             "account_email": account.get("user_email", "") or account.get("user_name", ""),
                             "response_content": response_text[:20000]}
            if usage:
                update_fields["prompt_tokens"] = usage.get("prompt_tokens", 0)
                update_fields["completion_tokens"] = usage.get("completion_tokens", 0)
                prompt_tokens = int(usage.get("prompt_tokens") or 0)
                completion_tokens = int(usage.get("completion_tokens") or 0)
                credits_used = float(usage.get("credits_used") or 0)
                account["total_prompt_tokens"] = account.get("total_prompt_tokens", 0) + prompt_tokens
                account["total_completion_tokens"] = account.get("total_completion_tokens", 0) + completion_tokens
                account["total_credits"] = round(account.get("total_credits", 0) + credits_used, 2)
                metrics.inc("notion_tokens_total", prompt_tokens, kind="prompt")
                metrics.inc("notion_tokens_total", completion_tokens, kind="completion")
                metrics.inc("notion_credits_total", credits_used)
            _db_update_log(log_entry["id"], **update_fields)
            account["total_requests"] = account.get("total_requests", 0) + 1
            _record_account_outcome(account, account_health.OUTCOME_SUCCESS, usage=usage)
            if QUOTA_LIMIT > 0 and account["total_requests"] >= QUOTA_LIMIT:
                account["status"] = "quota_exceeded"
                account["quota"] = {"used": account["total_requests"], "total": QUOTA_LIMIT, "remaining": 0, "exceeded": True}
                account["status_changed_at"] = _now_iso()
                _trigger_auto_replenish()
            _record_request_metrics(model, request_data.get("_harness") or "generic", "success", elapsed / 1000.0)
            save_accounts(accounts_store)
        return result
    except HTTPException:
        raise
    except Exception as e:
        elapsed = int((time.time() - t_start) * 1000)
        log_entry["status"] = "error"
        log_entry["error"] = str(e)
        log_entry["duration_ms"] = elapsed
        _db_update_log(log_entry["id"], status="error", duration_ms=elapsed, error=str(e)[:500])
        _record_request_metrics(model, request_data.get("_harness") or "generic", "error", elapsed / 1000.0)
        logger.error(f"处理聊天请求时发生顶层错误: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"内部服务器错误: {str(e)}")

@app.get("/v1/models", dependencies=[Depends(verify_api_key)], response_class=JSONResponse)
async def list_models():
    return await provider.get_models(_pick_account())

@app.get("/", summary="根路径")
def root():
    return {"message": f"欢迎来到 {settings.APP_NAME} v{settings.APP_VERSION}. 服务运行正常。"}

# ── Dashboard HTML ──
def _serve_html(filename: str) -> HTMLResponse:
    with open(os.path.join(DASHBOARD_DIR, filename), "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_overview(): return _serve_html("overview.html")
@app.get("/dashboard/accounts", response_class=HTMLResponse)
async def dashboard_accounts_page(): return _serve_html("accounts.html")
@app.get("/dashboard/logs", response_class=HTMLResponse)
async def dashboard_logs_page(): return _serve_html("logs.html")
@app.get("/dashboard/settings", response_class=HTMLResponse)
async def dashboard_settings_page(): return _serve_html("settings.html")
@app.get("/dashboard/auto-register", response_class=HTMLResponse)
async def dashboard_auto_register_page(): return _serve_html("auto-register.html")
@app.get("/dashboard/analytics", response_class=HTMLResponse)
async def dashboard_analytics_page(): return _serve_html("analytics.html")

# ── Dashboard Data API ──
@app.get("/api/health")
async def health():
    uptime = int(time.time() - START_TIME)
    total = len(accounts_store)
    status_counts = {"active": 0, "quota_exceeded": 0, "disabled": 0}
    estimated_replies_total = 0
    for acc in accounts_store:
        account_health.normalize_account(acc)
        status = str(acc.get("status") or "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1
        estimate = account_health.predict_quota(acc, free_credits_limit=FREE_CREDITS_LIMIT).get("estimated_replies_left")
        if isinstance(estimate, (int, float)):
            estimated_replies_total += int(estimate)
    available = _available_account_count()
    return {
        "uptime": uptime,
        "accounts": {
            "total": total,
            "available": available,
            "available_with_quota": available,
            "active": status_counts.get("active", 0),
            "status_counts": status_counts,
            "estimated_replies_total": estimated_replies_total,
            "reserve_replies": QUOTA_RESERVE_REPLIES,
        },
        "total_requests": _db_count_logs(),
        "version": settings.APP_VERSION,
        "quota_limit": QUOTA_LIMIT,
        "auto_replenish": auto_replenish_enabled,
        "quota_mode": "manual" if QUOTA_LIMIT > 0 else "auto_detect",
        "health": {
            "probe_enabled": ACCOUNT_HEALTH_ENABLED,
            "probe_interval_minutes": ACCOUNT_HEALTH_INTERVAL_MINUTES,
            "failure_threshold": ACCOUNT_FAILURE_THRESHOLD,
            "empty_threshold": ACCOUNT_EMPTY_THRESHOLD,
            "archive_enabled": ACCOUNT_ARCHIVE_ENABLED,
            "archive_mode": ACCOUNT_ARCHIVE_MODE,
        },
        "replenish": {
            "enabled": auto_replenish_enabled,
            "target": AUTO_REPLENISH_TARGET,
            "paused": _replenish_paused(),
            "pause_reason": _replenish_pause_reason,
            "paused_until": _replenish_paused_until or None,
        },
        "metrics_url": "/metrics",
    }

def _reg_nodes_snapshot() -> Dict[str, Any]:
    """Node-pool snapshot with credentials masked (dashboard/API safe)."""
    try:
        snapshot = register_egress.get_egress_pool().snapshot()
    except Exception:
        return {}
    for node in snapshot.get("nodes") or []:
        node["node_id"] = mask_proxy(str(node.get("node_id") or ""))
    return snapshot


@app.get("/metrics", summary="Prometheus 指标")
def prometheus_metrics():
    """Prometheus text-format scrape endpoint (counters, gauges, histograms)."""
    nodes = (_reg_nodes_snapshot() or {}).get("nodes") or []
    cooling = sum(1 for node in nodes if int(node.get("cooldown_seconds") or 0) > 0)
    extra_gauges = [
        ("account_status", sum(1 for a in accounts_store if a.get("status") == "active"), {"status": "active"}),
        ("account_status", sum(1 for a in accounts_store if a.get("status") == "quota_exceeded"), {"status": "quota_exceeded"}),
        ("account_status", sum(1 for a in accounts_store if a.get("status") == "disabled"), {"status": "disabled"}),
        ("accounts_total", len(accounts_store), {}),
        ("registration_nodes_cooling", cooling, {}),
        ("registration_paused", 1 if _replenish_paused() else 0, {}),
    ]
    return PlainTextResponse(metrics.prometheus_text(extra_gauges=extra_gauges))


@app.get("/api/metrics")
async def api_get_metrics():
    """JSON snapshot of the same registry for the dashboard."""
    return metrics.snapshot()


@app.get("/api/accounts")
async def api_get_accounts():
    """Accounts with masked credentials, health counters and quota estimate."""
    return [_masked_account(account) for account in accounts_store]

@app.post("/api/accounts")
async def api_add_account(request: Request):
    data = await request.json()
    token_v2 = data.get("token_v2", "")
    if not token_v2:
        raise HTTPException(status_code=400, detail="token_v2 is required")
    space_id = data.get("space_id", "")
    user_id = data.get("user_id", "")
    user_name = data.get("user_name", "")
    user_email = data.get("user_email", "")
    if not space_id or not user_id:
        try:
            scraper = create_session()
            cookie = token_v2 if "=" in token_v2 else f"token_v2={token_v2}"
            resp = scraper.post("https://www.notion.so/api/v3/loadUserContent", headers={"Content-Type": "application/json", "Cookie": cookie, "User-Agent": "Mozilla/5.0"}, json={}, timeout=15)
            if resp.status_code == 200:
                content = resp.json()
                for uid, uval in content.get("recordMap", {}).get("notion_user", {}).items():
                    user_info = uval.get("value", {}).get("value", {})
                    if not user_id: user_id = uid
                    if not user_name or user_name == "unknown": user_name = user_info.get("name", "")
                    if not user_email: user_email = user_info.get("email", "")
                for uid, uval in content.get("recordMap", {}).get("user_root", {}).items():
                    sv = uval.get("value", {}).get("value", {}).get("space_view_pointers", [])
                    if sv and not space_id: space_id = sv[0].get("spaceId", "")
        except Exception as e:
            logger.warning(f"Auto-fetch account info failed: {e}")
    account = {"id": str(uuid.uuid4()), "token_v2": token_v2, "space_id": space_id, "user_id": user_id, "user_name": user_name, "user_email": user_email, "status": "active", "created_at": datetime.now().astimezone().isoformat(), "total_requests": 0}
    account_health.normalize_account(account)
    accounts_store.append(account)
    save_accounts(accounts_store)
    return {"success": True, "account": _masked_account(account)}

@app.delete("/api/accounts/{account_id}")
async def api_delete_account(account_id: str):
    global accounts_store
    account = next((a for a in accounts_store if a["id"] == account_id), None)
    accounts_store = [a for a in accounts_store if a["id"] != account_id]
    if account:
        try:
            forget_threads_for_account(account_id)
        except Exception:
            pass
    save_accounts(accounts_store)
    return {"success": True}

@app.post("/api/accounts/cleanup")
async def api_cleanup_accounts(request: Request):
    """Archive or delete dead accounts (dry_run supported)."""
    data = await request.json()
    statuses = set(data.get("statuses") or ["quota_exceeded", "disabled"])
    allowed = {account_health.STATUS_QUOTA_EXCEEDED, account_health.STATUS_DISABLED}
    if statuses - allowed:
        raise HTTPException(status_code=400, detail=f"statuses must be a subset of {sorted(allowed)}")
    mode = str(data.get("mode") or ACCOUNT_ARCHIVE_MODE).lower()
    if mode not in ("archive", "delete"):
        raise HTTPException(status_code=400, detail="mode must be 'archive' or 'delete'")
    older_than_days = data.get("older_than_days")
    if older_than_days is not None:
        try:
            older_than_days = float(older_than_days)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="older_than_days must be a number")
    dry_run = bool(data.get("dry_run", False))
    targets = []
    for account in accounts_store:
        if account.get("status") not in statuses:
            continue
        if older_than_days is not None and _account_age_seconds(account) < older_than_days * 86400:
            continue
        targets.append(account)
    if dry_run:
        return {"success": True, "dry_run": True, "matched": len(targets),
                "account_ids": [a.get("id") for a in targets]}
    removed, _remaining = account_health.archive_accounts(accounts_store, ARCHIVE_FILE, targets, mode=mode)
    for account in removed:
        try:
            forget_threads_for_account(account.get("id") or "")
        except Exception:
            pass
    save_accounts(accounts_store)
    _trigger_auto_replenish()
    _add_reg_log("info", f"手动清理账号: {len(removed)} 个（{mode}）")
    return {"success": True, "dry_run": False, "matched": len(targets),
            "archived": len(removed) if mode == "archive" else 0,
            "deleted": 0 if mode == "archive" else len(removed)}

@app.get("/api/accounts/archive")
async def api_accounts_archive(limit: int = 50, offset: int = 0):
    """Archived accounts (credentials masked)."""
    entries = account_health.load_archive(ARCHIVE_FILE)
    results = []
    for entry in entries[offset:offset + max(0, limit)]:
        account = entry.get("account") or {}
        results.append({"archived_at": entry.get("archived_at", ""), "account": _masked_account(account)})
    return {"total": len(entries), "results": results}

@app.post("/api/accounts/restore/{archived_id}")
async def api_restore_account(archived_id: str):
    """Restore one archived account back into the active pool."""
    entry = account_health.find_archived(ARCHIVE_FILE, archived_id)
    if not entry:
        raise HTTPException(status_code=404, detail="archived account not found")
    account = entry.get("account") or {}
    if any(str(item.get("id")) == str(account.get("id")) for item in accounts_store):
        raise HTTPException(status_code=409, detail="account already active")
    account_health.pop_archived(ARCHIVE_FILE, archived_id)
    account_health.normalize_account(account)
    account["status"] = account_health.STATUS_ACTIVE
    account["quota"] = {"exceeded": False, "source": "restore", "estimated_replies_left": None, "confidence": "unknown"}
    account["health"]["consecutive_failures"] = 0
    account["health"]["consecutive_empty"] = 0
    account["health"]["disabled_reason"] = ""
    account["health"]["disabled_at"] = ""
    account.pop("status_changed_at", None)
    accounts_store.append(account)
    save_accounts(accounts_store)
    _trigger_auto_replenish()
    return {"success": True, "account": _masked_account(account)}

@app.get("/api/models")
async def api_get_models():
    """Model list for dashboard filters (no API key; local panel endpoint)."""
    return {"data": model_registry.openai_models(), "known": model_registry.codenames() or settings.KNOWN_MODELS}


@app.get("/api/logs")
async def api_get_logs(limit: int = 20, offset: int = 0, model: str = "", status: str = "", search: str = ""):
    page_size = max(1, min(int(limit or 20), 200))
    return _db_query_logs(page_size, max(0, int(offset or 0)), model, status, search)


@app.get("/api/logs/{log_id}")
async def api_get_log_detail(log_id: str):
    """Full request log row (used by the response detail modal)."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM request_logs WHERE id=?", (log_id,)).fetchone()
    conn.close()
    if row is None:
        raise HTTPException(status_code=404, detail="log not found")
    return dict(row)

@app.get("/api/settings")
async def api_get_settings():
    """Current runtime configuration (local panel; values intentionally unmasked)."""
    dynamic_models = model_registry.codenames()
    current = {
        "APP_VERSION": settings.APP_VERSION,
        "API_MASTER_KEY": settings.API_MASTER_KEY or "",
        "NGINX_PORT": settings.NGINX_PORT,
        "DEFAULT_MODEL": settings.DEFAULT_MODEL,
        "NOTION_CLIENT_VERSION": settings.NOTION_CLIENT_VERSION,
        "API_REQUEST_TIMEOUT": settings.API_REQUEST_TIMEOUT,
        "TEMP_EMAIL_PROVIDER": settings.TEMP_EMAIL_PROVIDER,
        "TEMP_EMAIL_BASE": TEMP_EMAIL_BASE,
        "TEMP_EMAIL_ADMIN_AUTH": TEMP_EMAIL_ADMIN_AUTH,
        "TEMP_EMAIL_DOMAIN": settings.TEMP_EMAIL_DOMAIN,
        "REGISTER_PROXY": "\n".join(
            node["proxy"] for node in register_egress.ensure_node_config() if node.get("enabled", True)
        ),
        "REGISTER_PROXY_ROTATE": bool(settings.REGISTER_PROXY_ROTATE),
        "REGISTER_EGRESS_MODE": str(getattr(settings, "REGISTER_EGRESS_MODE", "proxy")),
        "AUTO_REPLENISH_ENABLED": auto_replenish_enabled,
        "AUTO_REPLENISH_TARGET": AUTO_REPLENISH_TARGET,
        "AUTO_REPLENISH_DOMAIN": auto_replenish_domain,
        "AUTO_DELETE_QUOTA_EXCEEDED": AUTO_DELETE_QUOTA_EXCEEDED,
        "ACCOUNT_HEALTH_ENABLED": ACCOUNT_HEALTH_ENABLED,
        "ACCOUNT_HEALTH_INTERVAL_MINUTES": ACCOUNT_HEALTH_INTERVAL_MINUTES,
        "ACCOUNT_FAILURE_THRESHOLD": ACCOUNT_FAILURE_THRESHOLD,
        "ACCOUNT_EMPTY_THRESHOLD": ACCOUNT_EMPTY_THRESHOLD,
        "ACCOUNT_ARCHIVE_ENABLED": ACCOUNT_ARCHIVE_ENABLED,
        "ACCOUNT_ARCHIVE_MODE": ACCOUNT_ARCHIVE_MODE,
        "ACCOUNT_ARCHIVE_QUOTA_DAYS": ACCOUNT_ARCHIVE_QUOTA_DAYS,
        "ACCOUNT_ARCHIVE_DISABLED_DAYS": ACCOUNT_ARCHIVE_DISABLED_DAYS,
        "FREE_CREDITS_LIMIT": FREE_CREDITS_LIMIT,
        "QUOTA_RESERVE_REPLIES": QUOTA_RESERVE_REPLIES,
        "REGISTER_PAUSE_AFTER_FAILURES": REGISTER_PAUSE_AFTER_FAILURES,
        "REGISTER_PAUSE_SECONDS": REGISTER_PAUSE_SECONDS,
        "AUTO_REPLENISH_HARD_DISABLE": AUTO_REPLENISH_HARD_DISABLE,
        "REGISTER_NODE_COOLDOWN_BASE": int(getattr(settings, "REGISTER_NODE_COOLDOWN_BASE", 60) or 60),
        "REGISTER_NODE_COOLDOWN_MAX": int(getattr(settings, "REGISTER_NODE_COOLDOWN_MAX", 900) or 900),
        "REGISTER_NODE_FAILURE_THRESHOLD": int(getattr(settings, "REGISTER_NODE_FAILURE_THRESHOLD", 3) or 3),
        "CLASH_GROUP": str(getattr(settings, "CLASH_GROUP", "") or ""),
        "CLASH_NODES": str(getattr(settings, "CLASH_NODES", "") or ""),
    }
    return {
        "current": current,
        "known_models": dynamic_models or settings.KNOWN_MODELS,
        "model_map": settings.MODEL_MAP,
        "model_count": len(dynamic_models) or len(settings.KNOWN_MODELS),
    }


_RUNTIME_SETTING_APPLIERS = {}


def _apply_runtime_setting(key: str, raw_value: Any) -> bool:
    """Apply settings that can change without a restart; return True when applied."""
    global auto_replenish_enabled, AUTO_REPLENISH_TARGET, auto_replenish_domain
    global AUTO_DELETE_QUOTA_EXCEEDED, TEMP_EMAIL_DOMAIN
    value = raw_value
    if isinstance(value, bool):
        enabled = value
    else:
        enabled = str(value).strip().lower() not in ("0", "false", "no", "off", "")
    try:
        if key == "AUTO_REPLENISH_ENABLED":
            auto_replenish_enabled = enabled
            if enabled:
                _clear_replenish_pause()
            return True
        if key == "AUTO_REPLENISH_TARGET":
            AUTO_REPLENISH_TARGET = max(1, min(int(value), 50))
            _trigger_auto_replenish()
            return True
        if key in ("TEMP_EMAIL_DOMAIN", "AUTO_REPLENISH_DOMAIN"):
            domain = str(value).strip()
            if domain:
                auto_replenish_domain = domain
                TEMP_EMAIL_DOMAIN = domain
            return True
        if key == "AUTO_DELETE_QUOTA_EXCEEDED":
            AUTO_DELETE_QUOTA_EXCEEDED = enabled
            return True
    except (TypeError, ValueError):
        return False
    return False

@app.post("/api/settings")
async def api_update_settings(request: Request):
    data = await request.json()
    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="settings payload must be an object")
    applied, restart_required = [], []
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    env_lines = []
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            env_lines = f.readlines()
    for key, value in data.items():
        str_value = ("1" if value else "0") if isinstance(value, bool) else str(value)
        found = False
        for i, line in enumerate(env_lines):
            if line.strip().startswith(key + "="):
                env_lines[i] = f"{key}={str_value}\n"
                found = True
                break
        if not found:
            env_lines.append(f"{key}={str_value}\n")
        if _apply_runtime_setting(key, value):
            applied.append(key)
        else:
            restart_required.append(key)
    with open(env_path, "w", encoding="utf-8") as f:
        f.writelines(env_lines)
    _add_reg_log("info", f"系统设置已更新（运行时生效: {', '.join(applied) or '无'}；需重启: {', '.join(restart_required) or '无'}）")
    return {"success": True, "applied": applied, "restart_required": restart_required,
            "message": "已保存。运行时项立即生效；其余需重启服务。"}


# ── Dashboard Statistics API ──
@app.get("/api/stats")
async def api_get_stats():
    """Aggregate statistics: model distribution, timing, account usage, trends."""
    from collections import Counter
    now = datetime.now().astimezone()

    # Query all logs from DB for stats computation
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM request_logs ORDER BY timestamp DESC LIMIT 5000").fetchall()
    conn.close()
    all_logs = [dict(r) for r in rows]

    model_counts = Counter()
    model_errors = Counter()
    model_durations: Dict[str, list] = {}
    hourly_counts = [0] * 24
    hourly_errors = [0] * 24
    total_prompt_tokens = 0
    total_duration_ms = 0
    success_count = 0
    error_count = 0

    for log in all_logs:
        model = log.get("model", "unknown")
        model_counts[model] += 1
        if log.get("status") == "error":
            model_errors[model] += 1
            error_count += 1
        elif log.get("status") == "success":
            success_count += 1
        dur = log.get("duration_ms")
        if dur is not None:
            model_durations.setdefault(model, []).append(dur)
            total_duration_ms += dur
        pt = log.get("prompt_tokens", 0)
        if pt:
            total_prompt_tokens += pt
        try:
            ts = datetime.fromisoformat(log.get("timestamp", ""))
            hours_ago = (now - ts).total_seconds() / 3600
            if 0 <= hours_ago < 24:
                hourly_counts[ts.hour] += 1
                if log.get("status") == "error":
                    hourly_errors[ts.hour] += 1
        except Exception:
            pass

    total_logs = len(all_logs)

    # Model stats — include ALL known models, not just those with requests
    model_stats = []
    seen_models = set()
    for model, count in model_counts.most_common():
        seen_models.add(model)
        durations = model_durations.get(model, [])
        avg_dur = sum(durations) / len(durations) if durations else 0
        p50 = sorted(durations)[len(durations) // 2] if durations else 0
        p95 = sorted(durations)[int(len(durations) * 0.95)] if len(durations) >= 2 else (durations[0] if durations else 0)
        model_stats.append({
            "model": model,
            "display_name": settings.MODEL_MAP.get(model, model),
            "codename": settings.MODEL_MAP.get(model, ""),
            "requests": count,
            "errors": model_errors.get(model, 0),
            "success_rate": round((count - model_errors.get(model, 0)) / count * 100, 1) if count > 0 else 0,
            "avg_duration_ms": round(avg_dur),
            "p50_duration_ms": p50,
            "p95_duration_ms": p95,
            "ever_used": True,
        })
    # Add models from KNOWN_MODELS that have no requests yet
    for display_name, codename in settings.MODEL_MAP.items():
        if display_name not in seen_models:
            model_stats.append({
                "model": display_name,
                "display_name": display_name,
                "codename": codename,
                "requests": 0,
                "errors": 0,
                "success_rate": 0,
                "avg_duration_ms": 0,
                "p50_duration_ms": 0,
                "p95_duration_ms": 0,
                "ever_used": False,
            })

    # Account usage — include per-account total_requests from accounts_store
    account_stats = []
    for acc in accounts_store:
        used = acc.get("total_requests", 0)
        q_total = QUOTA_LIMIT if QUOTA_LIMIT > 0 else None
        account_stats.append({
            "id": acc.get("id"),
            "user_name": acc.get("user_name", ""),
            "user_email": acc.get("user_email", ""),
            "status": acc.get("status", ""),
            "auto_registered": acc.get("auto_registered", False),
            "created_at": acc.get("created_at", ""),
            "total_requests": used,
            "quota_used": used,
            "quota_total": q_total,
            "quota_remaining": max(0, QUOTA_LIMIT - used) if QUOTA_LIMIT > 0 else None,
            "quota_exceeded": acc.get("status") == "quota_exceeded",
            "credits_used": acc.get("total_credits", 0),
            "estimated_replies_left": account_health.predict_quota(acc, free_credits_limit=FREE_CREDITS_LIMIT).get("estimated_replies_left"),
            "health": {
                "consecutive_failures": (acc.get("health") or {}).get("consecutive_failures", 0),
                "consecutive_empty": (acc.get("health") or {}).get("consecutive_empty", 0),
                "success_count": (acc.get("health") or {}).get("success_count", 0),
                "last_success_at": (acc.get("health") or {}).get("last_success_at", ""),
                "last_failure_reason": (acc.get("health") or {}).get("last_failure_reason", ""),
                "disabled_reason": (acc.get("health") or {}).get("disabled_reason", ""),
            },
        })

    return {
        "overview": {
            "total_requests": total_logs,
            "success_count": success_count,
            "error_count": error_count,
            "success_rate": round(success_count / total_logs * 100, 1) if total_logs else 0,
            "total_prompt_tokens": total_prompt_tokens,
            "avg_duration_ms": round(total_duration_ms / success_count) if success_count > 0 else 0,
            "total_duration_ms": total_duration_ms,
        },
        "models": model_stats,
        "accounts": account_stats,
        "hourly": {
            "counts": hourly_counts,
            "errors": hourly_errors,
        },
        "known_models": model_registry.codenames() or settings.KNOWN_MODELS,
        "model_map": settings.MODEL_MAP,
    }

@app.get("/api/stats/timeline")
async def api_get_stats_timeline(hours: int = 24):
    """Request counts binned by hour for the last N hours."""
    now = datetime.now().astimezone()
    cutoff = (now - timedelta(hours=hours)).isoformat()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT substr(timestamp,1,13)||':00' as hour, status, COUNT(*) as cnt "
        "FROM request_logs WHERE timestamp > ? GROUP BY hour, status ORDER BY hour",
        (cutoff,)).fetchall()
    conn.close()
    bins: Dict[str, Dict[str, int]] = {}
    for r in rows:
        h = r["hour"]
        if h not in bins:
            bins[h] = {"total": 0, "success": 0, "error": 0}
        bins[h]["total"] += r["cnt"]
        if r["status"] in ("success", "error"):
            bins[h][r["status"]] += r["cnt"]
    return [{"hour": k, **v} for k, v in sorted(bins.items())]

# ── Account Quota Check ──
@app.post("/api/accounts/{account_id}/check")
async def api_check_account(account_id: str):
    """Check Notion AI quota for an account by making a test inference."""
    account = next((a for a in accounts_store if a["id"] == account_id), None)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    
    token_v2 = account.get("token_v2", "")
    space_id = account.get("space_id", "")
    user_id = account.get("user_id", "")
    if not token_v2 or not space_id or not user_id:
        return {"success": False, "error": "Missing token_v2, space_id, or user_id"}
    
    def _check():
        scraper = create_session()
        cookie = token_v2 if "=" in token_v2 else f"token_v2={token_v2}"
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/x-ndjson",
            "Cookie": cookie,
            "x-notion-space-id": space_id,
            "x-notion-active-user-header": user_id,
            "Origin": "https://www.notion.so",
            "Referer": "https://www.notion.so/",
            "User-Agent": "Mozilla/5.0",
        }
        # Create a minimal thread
        thread_id = str(uuid.uuid4())
        save_payload = {
            "requestId": str(uuid.uuid4()),
            "transactions": [{
                "id": str(uuid.uuid4()),
                "spaceId": space_id,
                "operations": [{
                    "pointer": {"table": "thread", "id": thread_id, "spaceId": space_id},
                    "path": [],
                    "command": "set",
                    "args": {
                        "id": thread_id, "version": 1, "parent_id": space_id,
                        "parent_table": "space", "space_id": space_id,
                        "created_time": int(time.time() * 1000),
                        "created_by_id": user_id, "created_by_table": "notion_user",
                        "messages": [], "data": {}, "alive": True, "type": "workflow"
                    }
                }]
            }]
        }
        resp = scraper.post("https://app.notion.com/api/v3/saveTransactionsFanout",
                           headers=headers, json=save_payload, timeout=15)
        if resp.status_code != 200:
            return {"success": False, "error": f"saveTransactions failed: HTTP {resp.status_code}", "status": "error"}
        
        # Try a minimal inference to detect quota
        payload = {
            "traceId": str(uuid.uuid4()),
            "spaceId": space_id,
            "transcript": [
                {"id": str(uuid.uuid4()), "type": "config", "value": {"type": "workflow", "model": settings.DEFAULT_MODEL}},
                {"id": str(uuid.uuid4()), "type": "context", "value": {
                    "timezone": "UTC", "spaceId": space_id, "userId": user_id,
                    "surface": "workflows", "userName": account.get("user_name", "test")
                }},
                {"id": str(uuid.uuid4()), "type": "user", "value": [["hi"]], "userId": user_id,
                 "createdAt": datetime.now().astimezone().isoformat()},
            ],
            "threadId": thread_id,
            "createThread": False, "isPartialTranscript": True, "asPatchResponse": True,
            "generateTitle": False, "saveAllThreadOperations": False, "threadType": "workflow",
        }
        resp = scraper.post("https://app.notion.com/api/v3/runInferenceTranscript",
                           headers=headers, json=payload, stream=True, timeout=30)
        if resp.status_code != 200:
            return {"success": False, "error": f"runInference failed: HTTP {resp.status_code}", "status": "error"}
        
        # Parse the response for quota info and completion signal
        quota_info = {"current": None, "total": None, "exceeded": False, "used": None, "remaining": None}
        got_response = False
        for line in resp.iter_lines():
            if not line:
                continue
            try:
                data = json.loads(line)
                # Check for quota exceeded
                if data.get("type") == "patch-start":
                    for item in data.get("data", {}).get("s", []):
                        if item.get("type") == "premium-feature-unavailable":
                            fa = item.get("featureAvailability", {})
                            limit = fa.get("limit", {})
                            quota_info["current"] = limit.get("current")
                            quota_info["total"] = limit.get("total")
                            quota_info["exceeded"] = True
                            if quota_info["total"] and quota_info["current"] is not None:
                                quota_info["used"] = quota_info["current"]
                                quota_info["remaining"] = max(0, quota_info["total"] - quota_info["current"])
                # Any successful content means quota is OK
                if data.get("type") in ("markdown-chat", "record-map", "patch"):
                    got_response = True
            except Exception:
                pass
        
        if not got_response and not quota_info["exceeded"]:
            return {"success": False, "status": "error", "error": "Notion 未返回有效内容，无法确认额度状态"}

        if quota_info["exceeded"]:
            # Runs in a worker thread: mutate/save directly; the async route
            # triggers replenishment after the threadpool call returns.
            account_health.normalize_account(account)
            account["status"] = account_health.STATUS_QUOTA_EXCEEDED
            quota = account["quota"]
            quota["exceeded"] = True
            quota["source"] = "notion"
            quota["remaining"] = 0
            quota["estimated_replies_left"] = 0
            quota["confidence"] = "exact"
            if quota_info.get("used") is not None:
                quota["used"] = quota_info.get("used")
            if quota_info.get("total") is not None:
                quota["total"] = quota_info.get("total")
            account["status_changed_at"] = _now_iso()
            save_accounts(accounts_store)
            return {"success": True, "status": "quota_exceeded", "quota": quota_info}
        else:
            # Account works: reactivate and reset failure counters (manual recovery).
            previous = account.get("status")
            account_health.normalize_account(account)
            account["status"] = account_health.STATUS_ACTIVE
            quota = account["quota"]
            quota["exceeded"] = False
            quota["source"] = "manual_check"
            quota["remaining"] = None
            account["health"]["consecutive_failures"] = 0
            account["health"]["consecutive_empty"] = 0
            account["health"]["disabled_reason"] = ""
            account["health"]["disabled_at"] = ""
            account.pop("status_changed_at", None)
            quota_info["used"] = account.get("total_requests", 0)
            quota_info["total"] = None
            quota_info["remaining"] = None
            account_health.predict_quota(account, free_credits_limit=FREE_CREDITS_LIMIT)
            save_accounts(accounts_store)
            restored = previous in (account_health.STATUS_DISABLED, account_health.STATUS_QUOTA_EXCEEDED)
            return {"success": True, "status": "active", "restored": restored, "quota": account["quota"]}
    
    result = await run_in_threadpool(_check)
    if result.get("status") == "quota_exceeded":
        if account_health.should_auto_delete(account_health.OUTCOME_QUOTA, AUTO_DELETE_QUOTA_EXCEEDED):
            _delete_account_now(account, reason="manual_check_quota_exceeded")
            return {"success": True, "status": "deleted", "deleted": True, "quota": result.get("quota")}
        _trigger_auto_replenish()
    return result


@app.get("/api/accounts/{account_id}/quota")
async def api_account_quota(account_id: str):
    """Notion credit/rate-limit status + locally tracked token usage for one account."""
    account = next((a for a in accounts_store if a["id"] == account_id), None)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    notion_status = await provider.get_quota_status(account)
    applicable = notion_status.get("status") not in (None, "not_applicable")
    note = (
        "Notion 只对 Business/Enterprise 工作区返回 credit 配额；当前工作区返回 not_applicable，"
        "没有可查询的每月消息/Token 上限，实际只受速率限制和风控影响。"
        if not applicable else ""
    )
    return {
        "success": True,
        "account_id": account_id,
        "user_email": account.get("user_email", ""),
        "notion": notion_status,
        "quota_applicable": applicable,
        "note": note,
        "local_tracking": {
            "total_requests": account.get("total_requests", 0),
            "prompt_tokens": account.get("total_prompt_tokens", 0),
            "completion_tokens": account.get("total_completion_tokens", 0),
            "credits_used": account.get("total_credits", 0),
            "estimated_replies_left": account_health.predict_quota(account, free_credits_limit=FREE_CREDITS_LIMIT).get("estimated_replies_left"),
            "health": dict(account.get("health") or {}),
        },
    }


# ── Auto-Replenish Config API ──
@app.get("/api/auto-replenish")
async def api_get_auto_replenish():
    pause_after = REGISTER_PAUSE_AFTER_FAILURES
    return {
        "enabled": auto_replenish_enabled,
        "target": AUTO_REPLENISH_TARGET,
        "queued": _registration_inflight,
        "quota_limit": QUOTA_LIMIT,
        "domain": auto_replenish_domain,
        "consecutive_failures": _replenish_consecutive_failures,
        "failure_limit": pause_after,
        "circuit_open": _replenish_paused(),
        "paused": _replenish_paused(),
        "paused_until": _replenish_paused_until or None,
        "pause_reason": _replenish_pause_reason,
        "pause_after_failures": pause_after,
        "pause_seconds": REGISTER_PAUSE_SECONDS,
        "hard_disable": AUTO_REPLENISH_HARD_DISABLE,
        "failure_categories": dict(_registration_failure_categories),
        "accounts_with_quota": _available_account_count(),
        "accounts_total": len(accounts_store),
        "accounts_quota_exceeded": sum(1 for a in accounts_store if a.get("status") == "quota_exceeded"),
        "nodes": _reg_nodes_snapshot(),
    }

@app.post("/api/auto-replenish")
async def api_update_auto_replenish(request: Request):
    global auto_replenish_enabled, auto_replenish_domain, QUOTA_LIMIT, AUTO_REPLENISH_TARGET
    data = await request.json()
    if "enabled" in data:
        auto_replenish_enabled = bool(data["enabled"])
        if auto_replenish_enabled:
            _clear_replenish_pause()  # Manual re-enable resets the pause/circuit state
        _add_reg_log("info", f"自动补号已{'开启' if auto_replenish_enabled else '关闭'}")
        if auto_replenish_enabled:
            _trigger_auto_replenish()
    if "target" in data:
        AUTO_REPLENISH_TARGET = max(1, min(int(data["target"]), 50))
        _add_reg_log("info", f"自动补号目标账号数: {AUTO_REPLENISH_TARGET}")
        _trigger_auto_replenish()
    if "domain" in data:
        auto_replenish_domain = str(data["domain"])
    if "quota_limit" in data:
        QUOTA_LIMIT = max(1, int(data["quota_limit"]))
    return {"success": True, "enabled": auto_replenish_enabled, "target": AUTO_REPLENISH_TARGET, "quota_limit": QUOTA_LIMIT, "domain": auto_replenish_domain}

# ── Auto-Registration: Registration Log API ──
@app.get("/api/reg-logs")
async def api_get_reg_logs(limit: int = 200, offset: int = 0):
    return {"total": len(reg_logs), "results": reg_logs[offset:offset+limit]}

@app.delete("/api/reg-logs")
async def api_clear_reg_logs():
    reg_logs.clear()
    try:
        with open(REG_LOGS_FILE, "w", encoding="utf-8"):
            pass
    except OSError:
        pass
    return {"success": True}

@app.get("/api/reg-status")
async def api_get_reg_status():
    return {
        "active_count": reg_active_count,
        "max_concurrent": reg_max_concurrent,
        "total_registered": sum(1 for a in accounts_store if a.get("auto_registered")),
        "domain": auto_replenish_domain,
        "provider": settings.TEMP_EMAIL_PROVIDER,
        "temp_email_base": TEMP_EMAIL_BASE,
        "register_proxy": registration_proxy_info(),
        "paused": _replenish_paused(),
        "pause_reason": _replenish_pause_reason,
        "nodes": _reg_nodes_snapshot(),
    }

def _reg_nodes_view(page: int = 1, page_size: int = 10) -> Dict[str, Any]:
    """Editable node list merged with live health, paginated for the dashboard."""
    config_nodes = register_egress.ensure_node_config()
    try:
        pool_snapshot = register_egress.get_egress_pool().snapshot()
    except Exception:
        pool_snapshot = {}
    health_by_proxy = {str(node.get("node_id")): node for node in (pool_snapshot.get("nodes") or [])}
    merged = []
    for node in config_nodes:
        health = health_by_proxy.get(str(node.get("proxy"))) or {}
        merged.append({
            "id": node.get("id"),
            "proxy": node.get("proxy"),
            "label": node.get("label") or node.get("proxy"),
            "enabled": bool(node.get("enabled", True)),
            "created_at": node.get("created_at", ""),
            "updated_at": node.get("updated_at", ""),
            "successes": int(health.get("successes") or 0),
            "failures": int(health.get("failures") or 0),
            "consecutive_failures": int(health.get("consecutive_failures") or 0),
            "cooldown_seconds": int(health.get("cooldown_seconds") or 0),
            "last_error_category": health.get("last_error_category") or "",
            "last_used_at": health.get("last_used_at") or 0,
        })
    page_size = max(1, min(int(page_size or 10), 100))
    total = len(merged)
    total_pages = max(1, (total + page_size - 1) // page_size) if total else 1
    page = max(1, min(int(page or 1), total_pages))
    start = (page - 1) * page_size
    return {
        "mode": str(getattr(settings, "REGISTER_EGRESS_MODE", "proxy")),
        "nodes": merged[start:start + page_size],
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "all_cooling": bool(pool_snapshot.get("all_cooling")),
        "failure_threshold": int(getattr(settings, "REGISTER_NODE_FAILURE_THRESHOLD", 3) or 3),
        "restore_node": pool_snapshot.get("restore_node"),
        "dry_run": bool(pool_snapshot.get("dry_run")),
    }


def _node_mode() -> str:
    return str(getattr(settings, "REGISTER_EGRESS_MODE", "proxy") or "proxy").lower()


def _normalize_node_proxy(raw_value: Any) -> str:
    value = str(raw_value or "").strip()
    if not value:
        raise HTTPException(status_code=400, detail="节点地址不能为空")
    if _node_mode() == "clash":
        return value
    from app.utils.http_client import normalize_proxy

    normalized = normalize_proxy(value)
    if not normalized:
        raise HTTPException(status_code=400, detail="不支持的代理地址（支持 http/https/socks5）")
    return normalized


@app.get("/api/reg-nodes")
async def api_get_reg_nodes(page: int = 1, page_size: int = 10):
    """Editable registration egress nodes + health/business pause state."""
    view = _reg_nodes_view(page, page_size)
    view.update({
        "paused": _replenish_paused(),
        "paused_until": _replenish_paused_until or None,
        "pause_reason": _replenish_pause_reason,
        "failure_categories": dict(_registration_failure_categories),
        "hard_disable": AUTO_REPLENISH_HARD_DISABLE,
        "pause_after_failures": REGISTER_PAUSE_AFTER_FAILURES,
        "pause_seconds": REGISTER_PAUSE_SECONDS,
    })
    return view


@app.post("/api/reg-nodes")
async def api_create_reg_node(request: Request):
    """Add one registration egress node (proxy URL, or Clash node name in clash mode)."""
    data = await request.json()
    proxy = _normalize_node_proxy(data.get("proxy"))
    label = str(data.get("label") or proxy).strip() or proxy
    enabled = bool(data.get("enabled", True))
    nodes = register_egress.ensure_node_config()
    if any(node.get("proxy") == proxy for node in nodes):
        raise HTTPException(status_code=409, detail="该节点已存在")
    now = _now_iso()
    node = {"id": uuid.uuid4().hex[:12], "proxy": proxy, "label": label, "enabled": enabled,
            "created_at": now, "updated_at": now}
    nodes.append(node)
    register_egress.save_node_config(nodes)
    register_egress.reload_egress_pool()
    _add_reg_log("info", f"注册出口节点已添加: {proxy}")
    return {"success": True, "node": node}


@app.put("/api/reg-nodes/{node_id}")
async def api_update_reg_node(node_id: str, request: Request):
    """Update a node's proxy/label/enabled flag."""
    data = await request.json()
    nodes = register_egress.ensure_node_config()
    target = next((node for node in nodes if str(node.get("id")) == str(node_id)), None)
    if target is None:
        raise HTTPException(status_code=404, detail="节点不存在")
    if "proxy" in data:
        proxy = _normalize_node_proxy(data.get("proxy"))
        if any(str(node.get("id")) != str(node_id) and node.get("proxy") == proxy for node in nodes):
            raise HTTPException(status_code=409, detail="该节点地址已存在")
        target["proxy"] = proxy
    if "label" in data:
        target["label"] = str(data.get("label") or target.get("proxy") or "").strip() or target["proxy"]
    if "enabled" in data:
        target["enabled"] = bool(data.get("enabled"))
    target["updated_at"] = _now_iso()
    register_egress.save_node_config(nodes)
    register_egress.reload_egress_pool()
    _add_reg_log("info", f"注册出口节点已更新: {target.get('proxy')}")
    return {"success": True, "node": target}


@app.delete("/api/reg-nodes/{node_id}")
async def api_delete_reg_node(node_id: str):
    """Delete one node from the editable list."""
    nodes = register_egress.ensure_node_config()
    target = next((node for node in nodes if str(node.get("id")) == str(node_id)), None)
    if target is None:
        raise HTTPException(status_code=404, detail="节点不存在")
    nodes = [node for node in nodes if str(node.get("id")) != str(node_id)]
    register_egress.save_node_config(nodes)
    register_egress.reload_egress_pool()
    _add_reg_log("info", f"注册出口节点已删除: {target.get('proxy')}")
    return {"success": True, "deleted": target.get("proxy")}


@app.post("/api/reg-nodes/batch")
async def api_batch_reg_nodes(request: Request):
    """Batch enable/disable/delete selected nodes."""
    data = await request.json()
    action = str(data.get("action") or "delete").lower()
    if action not in ("delete", "enable", "disable"):
        raise HTTPException(status_code=400, detail="action must be delete/enable/disable")
    ids = {str(item) for item in (data.get("ids") or [])}
    if not ids:
        raise HTTPException(status_code=400, detail="ids is required")
    nodes = register_egress.ensure_node_config()
    affected = []
    if action == "delete":
        affected = [node for node in nodes if str(node.get("id")) in ids]
        nodes = [node for node in nodes if str(node.get("id")) not in ids]
    else:
        for node in nodes:
            if str(node.get("id")) in ids:
                node["enabled"] = action == "enable"
                node["updated_at"] = _now_iso()
                affected.append(node)
    register_egress.save_node_config(nodes)
    register_egress.reload_egress_pool()
    _add_reg_log("info", f"注册出口节点批量{action}: {len(affected)} 个")
    return {"success": True, "affected": len(affected),
            "nodes": _reg_nodes_view(1, 100)["nodes"]}

@app.post("/api/reg-nodes/reset")
async def api_reset_reg_nodes():
    """Clear node cooldowns/failure counters (and restore the Clash node)."""
    try:
        result = register_egress.get_egress_pool().reset()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"重置注册节点失败: {exc}")
    _clear_replenish_pause()
    _add_reg_log("info", "注册出口节点健康状态已重置")
    return {"success": True, "nodes": _reg_nodes_snapshot(), "reset": result}

@app.post("/api/reg-config")
async def api_update_reg_config(request: Request):
    global reg_max_concurrent, reg_semaphore
    data = await request.json()
    new_max = data.get("max_concurrent", reg_max_concurrent)
    if isinstance(new_max, int) and 1 <= new_max <= 10:
        reg_max_concurrent = new_max
        reg_semaphore = asyncio.Semaphore(reg_max_concurrent)
        _add_reg_log("info", f"最大并发数已更新为 {reg_max_concurrent}")
        return {"success": True, "max_concurrent": reg_max_concurrent}
    raise HTTPException(status_code=400, detail="max_concurrent must be 1-10")

# ── Helper: poll temp email for Notion verification ──
def _poll_notion_email(scraper, email_address: str, email_jwt: str, task_id: str) -> str:
    """Poll temp email for Notion verification code via admin API."""
    verification_code = None
    admin_headers = {"x-admin-auth": TEMP_EMAIL_ADMIN_AUTH}

    for attempt in range(36):
        time.sleep(5)
        try:
            # Use admin API for reliability
            admin_resp = scraper.get(
                f"{TEMP_EMAIL_BASE}/admin/mails?limit=20&offset=0",
                headers=admin_headers, timeout=15
            )
            mails = []
            if admin_resp.status_code == 200:
                admin_mails = admin_resp.json().get("results", [])
                mails = [m for m in admin_mails if m.get("address") == email_address]

            for m in mails:
                subject = m.get("subject", "") or ""
                raw = m.get("raw", "") or ""

                is_notion = ("notion" in subject.lower() or "updates.notion.so" in raw or "mail.notion.so" in raw)
                if not is_notion:
                    continue

                # Extract 6-digit code from the plain text MIME part
                plain_parts = re.findall(r'Content-Type:\s*text/plain.*?\r?\n\r?\n(\d{6})\r?\n', raw)
                if plain_parts:
                    verification_code = plain_parts[0]
                    _add_reg_log("info", f"找到验证码: {verification_code}", task_id)
                    return verification_code

                # Fallback: look for code in a <pre> tag in HTML
                pre_match = re.search(r'<pre[^>]*>(\d{6})</pre>', raw)
                if pre_match:
                    verification_code = pre_match.group(1)
                    _add_reg_log("info", f"找到验证码 (HTML): {verification_code}", task_id)
                    return verification_code

        except Exception as e:
            _add_reg_log("warning", f"轮询邮件出错: {e}", task_id)

        if attempt % 4 == 3:
            _add_reg_log("info", f"等待验证邮件中... ({(attempt+1)*5}s)", task_id)

    return ""  # timeout

# ── Auto-Registration via EmailNator (Google-domain temp mail) ──
def _do_auto_register_emailnator(task_id: str) -> Dict[str, Any]:
    """Register a Notion account through EmailNator's Google-domain mailboxes.

    Notion blocks disposable domains and verifies SMTP reachability, so the
    public cloudflare_temp_email instance cannot be used any more. EmailNator
    hands out @googlemail.com aliases, which pass both checks.
    """
    from app.utils.temp_email import EmailnatorMailbox

    notion_headers_base = {
        "Content-Type": "application/json",
        "Origin": NOTION_BASE,
        "Referer": f"{NOTION_BASE}/signup",
        "notion-audit-log-platform": "web",
        "notion-client-version": settings.NOTION_CLIENT_VERSION,
    }
    last_error = ""
    failure_categories: List[str] = []
    for attempt in range(1, 5):
        # Registration egress: pick the healthiest node from the pool per attempt
        # so a risk-locked exit IP does not doom the whole task.
        egress = next_registration_egress()
        node_id = egress.get("node_id", "direct")
        proxy = egress.get("proxy")

        def _attempt_failed(detail: str) -> None:
            report_registration_result(node_id, False, detail)
            failure_categories.append(register_egress.classify_registration_error(detail))

        _add_reg_log(
            "info",
            f"[尝试 {attempt}/4] 注册出口: {mask_proxy(proxy) if proxy else '直连'}"
            f" ({egress.get('label') or node_id})",
            task_id,
        )
        scraper = create_session(proxy=proxy)
        mailbox = EmailnatorMailbox(proxy=proxy)
        try:
            email_address = mailbox.create_address()
        except Exception as e:
            last_error = f"EmailNator 取地址失败: {e}"
            _add_reg_log("warning", last_error, task_id)
            _attempt_failed(last_error)
            time.sleep(3)
            continue
        _add_reg_log("info", f"[尝试 {attempt}/4] EmailNator 邮箱: {email_address}", task_id)

        resp = scraper.post(
            f"{NOTION_BASE}/api/v3/getLoginOptions",
            headers=notion_headers_base,
            json={"email": email_address, "requireWorkTypeEmail": False},
            timeout=25,
        )
        if resp.status_code != 200:
            last_error = f"getLoginOptions HTTP {resp.status_code}: {resp.text[:150]}"
            _attempt_failed(last_error)
            continue
        login_data = resp.json()
        login_options_token = login_data.get("loginOptionsToken", "")
        if login_data.get("challengeProvider"):
            _add_reg_log("warning", f"Notion 返回验证码类型: {login_data.get('challengeProvider')}（继续尝试）", task_id)

        device_id = uuid.uuid4().hex
        send_time = int(time.time()) - 5
        resp = scraper.post(
            f"{NOTION_BASE}/api/v3/sendTemporaryPassword",
            headers=notion_headers_base,
            json={
                "email": email_address,
                "disableLoginLink": False,
                "native": False,
                "isSignup": True,
                "loginOptionsToken": login_options_token,
                "deviceId": device_id,
                "loginRouteOrigin": "signup",
            },
            timeout=25,
        )
        if resp.status_code != 200:
            try:
                err_type = resp.json().get("clientData", {}).get("type", "")
            except Exception:
                err_type = ""
            last_error = f"sendTemporaryPassword HTTP {resp.status_code} ({err_type})"
            _add_reg_log("warning", f"{email_address} 被 Notion 拒绝: {err_type or resp.text[:120]}，更换邮箱重试", task_id)
            _attempt_failed(last_error)
            continue
        csrf_state = resp.json().get("csrfState", "")
        if not csrf_state:
            last_error = f"sendTemporaryPassword 未返回 csrfState: {resp.text[:150]}"
            _attempt_failed(last_error)
            continue

        _add_reg_log("info", "验证码邮件已触发，正在等待 EmailNator 收信...", task_id)
        code = mailbox.poll_code(email_address, send_time, timeout=180)
        if not code:
            last_error = "EmailNator 未收到 Notion 验证码邮件"
            _add_reg_log("warning", f"{last_error}，更换邮箱重试", task_id)
            _attempt_failed(last_error)
            continue
        _add_reg_log("info", f"收到验证码: {code}，正在登录...", task_id)

        resp = scraper.post(
            f"{NOTION_BASE}/api/v3/loginWithEmail",
            headers=notion_headers_base,
            json={
                "state": csrf_state,
                "password": code,
                "appSource": "notion",
                "loginRouteOrigin": "signup",
            },
            timeout=30,
        )
        if resp.status_code != 200:
            last_error = f"loginWithEmail HTTP {resp.status_code}: {resp.text[:150]}"
            _add_reg_log("warning", f"登录失败: {last_error}", task_id)
            _attempt_failed(last_error)
            continue

        login_resp = resp.json()
        user_id = login_resp.get("userId", "")
        is_new = login_resp.get("isNewSignup", False)
        token_v2 = get_cookie(scraper, "token_v2")
        if not token_v2:
            last_error = "登录成功但未拿到 token_v2"
            _add_reg_log("warning", last_error, task_id)
            _attempt_failed(last_error)
            continue
        _add_reg_log("info", f"登录成功 (isNewSignup: {is_new}, userId: {user_id})", task_id)

        # Create workspace
        space_id = ""
        try:
            resp = scraper.post(
                f"{NOTION_BASE}/api/v3/createSpace",
                headers={**notion_headers_base, "x-notion-active-user-header": user_id},
                json={
                    "name": "notion-2api workspace",
                    "icon": "\U0001F4BC",
                    "planType": "team",
                    "planSelection": "team",
                    "initialPersona": "unfilled",
                    "domainType": "professional",
                    "deviceId": device_id,
                    "deviceType": "web-desktop",
                    "source": "handle_root_redirect",
                    "joinableSpacesViewType": "onboarding_list",
                },
                timeout=25,
            )
            if resp.status_code == 200:
                space_id = (resp.json() or {}).get("spaceId", "")
                _add_reg_log("info", f"工作区已创建: {space_id}", task_id)
            else:
                _add_reg_log("warning", f"创建工作区失败: HTTP {resp.status_code}", task_id)
        except Exception as e:
            _add_reg_log("warning", f"创建工作区出错: {e}", task_id)

        user_name = ""
        try:
            resp = scraper.post(
                "https://www.notion.so/api/v3/loadUserContent",
                headers={
                    "Content-Type": "application/json",
                    "Cookie": f"token_v2={token_v2}",
                    "User-Agent": "Mozilla/5.0",
                },
                json={},
                timeout=20,
            )
            if resp.status_code == 200:
                record_map = (resp.json() or {}).get("recordMap", {})
                for uid, uval in record_map.get("notion_user", {}).items():
                    user_name = uval.get("value", {}).get("value", {}).get("name", "") or user_name
                    if not user_id:
                        user_id = uid
                for uid, uval in record_map.get("user_root", {}).items():
                    pointers = uval.get("value", {}).get("value", {}).get("space_view_pointers", [])
                    if pointers and not space_id:
                        space_id = pointers[0].get("spaceId", "")
        except Exception as e:
            _add_reg_log("warning", f"获取用户信息失败: {e}", task_id)

        if not space_id:
            _add_reg_log("warning", "未拿到 space_id，账号可能无法直接使用，仍保存该账号", task_id)

        account = {
            "id": str(uuid.uuid4()),
            "token_v2": token_v2,
            "space_id": space_id,
            "user_id": user_id,
            "user_name": user_name or email_address.split("@")[0],
            "user_email": email_address,
            "status": "active",
            "created_at": datetime.now().astimezone().isoformat(),
            "total_requests": 0,
            "auto_registered": True,
            "temp_email_provider": "emailnator",
        }
        accounts_store.append(account)
        save_accounts(accounts_store)
        report_registration_result(node_id, True, "")
        _add_reg_log("info", f"注册成功! 用户: {account['user_name']}, 邮箱: {email_address}", task_id)
        return {"success": True, "account": account, "email": email_address}

    dominant = register_egress.CATEGORY_UNKNOWN
    if failure_categories:
        dominant = max(set(failure_categories), key=failure_categories.count)
    raise register_egress.RegistrationFailure(
        category=dominant, detail=last_error or "EmailNator 自动注册失败"
    )


# ── Auto-Registration: Core Logic (Pure API Flow) ──
def _do_auto_register_sync(task_id: str, domain: str) -> Dict[str, Any]:
    """Dispatch one registration; always feed the outcome back into the node pool."""
    try:
        if settings.TEMP_EMAIL_PROVIDER == "emailnator":
            return _do_auto_register_emailnator(task_id)
        egress = next_registration_egress()
        result = _do_auto_register_cfmail(task_id, domain, egress)
        report_registration_result(egress.get("node_id", "direct"), True, "")
        return result
    except register_egress.RegistrationFailure:
        raise
    except Exception as exc:
        # Exceptions raised outside the per-attempt handlers (e.g. a network
        # timeout in getLoginOptions) still belong to the node picked last in
        # this worker thread; report it so cooldowns stay accurate.
        category = register_egress.classify_registration_error(str(exc))
        last_egress = last_registration_egress() or {}
        report_registration_result(last_egress.get("node_id", "direct"), False, category)
        raise register_egress.RegistrationFailure(category=category, detail=str(exc))


def _do_auto_register_cfmail(task_id: str, domain: str, egress: Dict[str, Any]) -> Dict[str, Any]:
    """Register a new Notion account via pure API calls.

    Flow (verified 2026-06-28):
    1. Create temp email via cloudflare_temp_email API
    2. getLoginOptions(email) -> loginOptionsToken
    3. sendTemporaryPassword(email, loginOptionsToken, isSignup=True) -> csrfState
    4. Poll temp email for 6-digit verification code
    5. loginWithEmail(csrfState, code) -> userId + token_v2 (set-cookie)
    6. createSpace(userId) -> spaceId
    7. loadUserContent -> user info
    """
    proxy = egress.get("proxy")
    if proxy:
        _add_reg_log("info", f"注册出口代理: {mask_proxy(proxy)} ({egress.get('label') or egress.get('node_id')})", task_id)
    scraper = create_session(proxy=proxy)
    notion_headers_base = {
        "Content-Type": "application/json",
        "x-notion-active-user-header": "",
        "Origin": NOTION_BASE,
        "Referer": f"{NOTION_BASE}/signup",
        "notion-audit-log-platform": "web",
        "notion-client-version": "23.13.20260628.0724",
    }

    # Step 1: Create temp email
    _add_reg_log("info", "正在创建临时邮箱...", task_id)
    email_headers = {"Content-Type": "application/json", "x-admin-auth": TEMP_EMAIL_ADMIN_AUTH}
    username = "nrauto" + uuid.uuid4().hex[:6]
    resp = scraper.post(f"{TEMP_EMAIL_BASE}/api/new_address", headers=email_headers,
                        json={"name": username, "domain": domain}, timeout=15)
    if resp.status_code != 200:
        raise Exception(f"创建邮箱失败: HTTP {resp.status_code} - {resp.text[:200]}")
    email_data = resp.json()
    email_address = email_data["address"]
    email_jwt = email_data["jwt"]
    _add_reg_log("info", f"临时邮箱已创建: {email_address}", task_id)

    # Step 2: getLoginOptions
    _add_reg_log("info", "正在获取登录选项...", task_id)
    resp = scraper.post(
        f"{NOTION_BASE}/api/v3/getLoginOptions",
        headers=notion_headers_base,
        json={"email": email_address, "requireWorkTypeEmail": False},
        timeout=15
    )
    if resp.status_code != 200:
        raise Exception(f"getLoginOptions 失败: HTTP {resp.status_code} - {resp.text[:200]}")
    login_data = resp.json()
    login_options_token = login_data.get("loginOptionsToken", "")
    challenge_provider = login_data.get("challengeProvider", "")
    if challenge_provider:
        _add_reg_log("warning", f"需要验证码: {challenge_provider}，尝试继续...", task_id)
    _add_reg_log("info", f"loginOptionsToken 已获取 (hasAccount: {login_data.get('hasAccount')})", task_id)

    # Step 3: sendTemporaryPassword
    _add_reg_log("info", "正在发送临时密码邮件...", task_id)
    device_id = uuid.uuid4().hex
    resp = scraper.post(
        f"{NOTION_BASE}/api/v3/sendTemporaryPassword",
        headers=notion_headers_base,
        json={
            "email": email_address,
            "disableLoginLink": False,
            "native": False,
            "isSignup": True,
            "loginOptionsToken": login_options_token,
            "deviceId": device_id,
            "loginRouteOrigin": "signup",
        },
        timeout=15
    )
    if resp.status_code != 200:
        raise Exception(f"sendTemporaryPassword 失败: HTTP {resp.status_code} - {resp.text[:200]}")
    send_data = resp.json()
    csrf_state = send_data.get("csrfState", "")
    if not csrf_state:
        raise Exception(f"sendTemporaryPassword 未返回 csrfState: {resp.text[:200]}")
    _add_reg_log("info", "临时密码邮件已发送", task_id)

    # Step 4: Poll for verification code
    _add_reg_log("info", "正在轮询临时邮箱获取验证码...", task_id)
    verification_code = _poll_notion_email(scraper, email_address, email_jwt, task_id)
    if not verification_code:
        raise Exception("超时未收到验证邮件")

    # Step 5: loginWithEmail
    _add_reg_log("info", f"正在登录 (验证码: {verification_code})...", task_id)
    resp = scraper.post(
        f"{NOTION_BASE}/api/v3/loginWithEmail",
        headers=notion_headers_base,
        json={
            "state": csrf_state,
            "password": verification_code,
            "appSource": "notion",
            "loginRouteOrigin": "signup",
        },
        timeout=15
    )
    if resp.status_code != 200:
        raise Exception(f"loginWithEmail 失败: HTTP {resp.status_code} - {resp.text[:200]}")
    login_resp = resp.json()
    user_id = login_resp.get("userId", "")
    is_new = login_resp.get("isNewSignup", False)
    _add_reg_log("info", f"登录成功 (isNewSignup: {is_new}, userId: {user_id})", task_id)

    # Extract token_v2 from cookies
    token_v2 = get_cookie(scraper, "token_v2")

    if not token_v2:
        _add_reg_log("warning", "cookie 中未找到 token_v2，尝试 authValidate...", task_id)
        resp = scraper.post(
            f"{NOTION_BASE}/api/v3/authValidate",
            headers={
                "Content-Type": "application/json",
                "x-notion-active-user-header": user_id,
                "Origin": NOTION_BASE,
            },
            json={},
            timeout=15
        )
        token_v2 = get_cookie(scraper, "token_v2")

    if not token_v2:
        raise Exception("无法获取 token_v2，登录流程未成功完成")

    _add_reg_log("info", f"token_v2 已获取: {token_v2[:30]}...", task_id)

    # Step 6: createSpace
    _add_reg_log("info", "正在创建工作区...", task_id)
    space_id = ""
    try:
        resp = scraper.post(
            f"{NOTION_BASE}/api/v3/createSpace",
            headers={
                **{k: v for k, v in notion_headers_base.items() if k != "x-notion-active-user-header"},
                "x-notion-active-user-header": user_id,
            },
            json={
                "name": f"{username}的工作空间",
                "icon": "💼",
                "planType": "team",
                "planSelection": "team",
                "initialPersona": "unfilled",
                "domainType": "professional",
                "deviceId": device_id,
                "deviceType": "web-desktop",
                "source": "handle_root_redirect",
                "joinableSpacesViewType": "onboarding_list",
            },
            timeout=15
        )
        if resp.status_code == 200:
            space_data = resp.json()
            space_id = space_data.get("spaceId", "")
            _add_reg_log("info", f"工作区已创建: {space_id}", task_id)
        else:
            _add_reg_log("warning", f"创建工作区失败: HTTP {resp.status_code}", task_id)
    except Exception as e:
        _add_reg_log("warning", f"创建工作区出错: {e}", task_id)

    # Step 7: Get user info via loadUserContent
    _add_reg_log("info", "正在获取用户信息...", task_id)
    user_name = ""
    try:
        resp = scraper.post(
            "https://www.notion.so/api/v3/loadUserContent",
            headers={
                "Content-Type": "application/json",
                "Cookie": f"token_v2={token_v2}",
                "User-Agent": "Mozilla/5.0",
            },
            json={},
            timeout=15
        )
        if resp.status_code == 200:
            content = resp.json()
            for uid, uval in content.get("recordMap", {}).get("notion_user", {}).items():
                if not user_id:
                    user_id = uid
                user_info = uval.get("value", {}).get("value", {})
                user_name = user_info.get("name", "")
            for uid, uval in content.get("recordMap", {}).get("user_root", {}).items():
                sv = uval.get("value", {}).get("value", {}).get("space_view_pointers", [])
                if sv and not space_id:
                    space_id = sv[0].get("spaceId", "")
    except Exception as e:
        _add_reg_log("warning", f"获取用户信息失败: {e}", task_id)

    account = {
        "id": str(uuid.uuid4()), "token_v2": token_v2, "space_id": space_id,
        "user_id": user_id, "user_name": user_name or username, "user_email": email_address,
        "status": "active", "created_at": datetime.now().astimezone().isoformat(),
        "total_requests": 0, "auto_registered": True,
    }
    accounts_store.append(account)
    save_accounts(accounts_store)
    _add_reg_log("info", f"注册成功! 用户: {user_name or username}, 邮箱: {email_address}", task_id)
    return {"success": True, "account": account, "email": email_address}

@app.post("/api/auto-register")
async def api_auto_register(request: Request):
    """Register one or more Notion accounts. Tasks enter the shared background queue."""
    data = await request.json()
    domain = (data.get("domain") or auto_replenish_domain).strip()
    count = max(1, min(int(data.get("count", 1)), 20))  # 1-20 per request
    task_ids = _enqueue_registrations(count, domain=domain, reason="manual")
    msg = f"已提交 {count} 个注册任务" + (f" (并发上限: {reg_max_concurrent})" if count > reg_max_concurrent else "")
    return {"success": True, "message": msg, "task_count": count, "task_ids": task_ids}
