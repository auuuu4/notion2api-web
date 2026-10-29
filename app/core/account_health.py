"""Account health, quota prediction and dead-account archiving.

Pure helpers used by main.py so the behaviour can be unit-tested without
starting FastAPI or touching the real accounts.json. Account dictionaries are
mutated in place (main.py saves them) and every helper tolerates accounts
written by older versions by applying defaults first.

Statuses:
    active          usable for inference
    quota_exceeded  Notion reported premium-feature-unavailable
    disabled        dead token or repeatedly failing/empty responses
"""
from __future__ import annotations

import json
import os
import tempfile
import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

OUTCOME_SUCCESS = "success"
OUTCOME_QUOTA = "quota"
OUTCOME_INVALID = "account_invalid"
OUTCOME_EMPTY = "empty"
OUTCOME_TRUNCATED = "truncated"
OUTCOME_TRANSPORT = "transport"

STATUS_ACTIVE = "active"
STATUS_QUOTA_EXCEEDED = "quota_exceeded"
STATUS_DISABLED = "disabled"

_HEALTH_DEFAULTS: Dict[str, Any] = {
    "success_count": 0,
    "failure_count": 0,
    "empty_count": 0,
    "consecutive_failures": 0,
    "consecutive_empty": 0,
    "last_success_at": "",
    "last_failure_at": "",
    "last_failure_reason": "",
    "last_probe_at": "",
    "disabled_reason": "",
    "disabled_at": "",
}

_CREDIT_DEFAULTS: Dict[str, Any] = {"avg_credits": 0.0, "last_credits": 0.0, "samples": 0}

_QUOTA_DEFAULTS: Dict[str, Any] = {
    "exceeded": False,
    "estimated_replies_left": None,
    "confidence": "unknown",
}

_EXCEPTION_CODE_TO_OUTCOME = {
    "quota_exceeded": OUTCOME_QUOTA,
    "account_invalid": OUTCOME_INVALID,
    "empty_response": OUTCOME_EMPTY,
    "truncated": OUTCOME_TRUNCATED,
    "transport": OUTCOME_TRANSPORT,
}


def _iso_now(now: Optional[float] = None) -> str:
    value = now if now is not None else time.time()
    try:
        return datetime.fromtimestamp(value).astimezone().isoformat()
    except (OSError, OverflowError, ValueError):
        # Tiny/negative epoch values are not supported by every platform clock.
        return datetime.now().astimezone().isoformat()


def normalize_account(account: Dict[str, Any]) -> Dict[str, Any]:
    """Fill health/credit/quota/status fields for accounts from older versions."""
    if not isinstance(account, dict):
        return account
    health = account.get("health")
    if not isinstance(health, dict):
        health = {}
    for key, value in _HEALTH_DEFAULTS.items():
        health.setdefault(key, value)
    account["health"] = health

    credit = account.get("credit_stats")
    if not isinstance(credit, dict):
        credit = {}
    for key, value in _CREDIT_DEFAULTS.items():
        credit.setdefault(key, value)
    account["credit_stats"] = credit

    quota = account.get("quota")
    if not isinstance(quota, dict):
        quota = {}
    for key, value in _QUOTA_DEFAULTS.items():
        quota.setdefault(key, value)
    account["quota"] = quota

    account.setdefault("status", STATUS_ACTIVE)
    return account


def should_auto_delete(outcome: str, enabled: bool) -> bool:
    """Only a confirmed quota outcome may trigger automatic account deletion.

    Empty/truncated/transport/account_invalid outcomes must never delete an
    account: they are ambiguous or recoverable, while premium-feature-unavailable
    is Notion's explicit "no quota" signal.
    """
    return bool(enabled) and outcome == OUTCOME_QUOTA


def classify_failure(exc: BaseException) -> str:
    """Map a provider exception to an account-level outcome code."""
    code = getattr(exc, "code", None)
    if code in _EXCEPTION_CODE_TO_OUTCOME:
        return _EXCEPTION_CODE_TO_OUTCOME[code]
    text = str(exc or "")
    lowered = text.lower()
    if "premium-feature-unavailable" in text or "配额已用尽" in text:
        return OUTCOME_QUOTA
    if "401" in lowered or "403" in lowered or "unauthorized" in lowered or "invalid token" in lowered:
        return OUTCOME_INVALID
    if "未返回有效响应" in text or "empty response" in lowered:
        return OUTCOME_EMPTY
    if "被截断" in text or "truncated" in lowered:
        return OUTCOME_TRUNCATED
    return OUTCOME_TRANSPORT


def apply_outcome(
    account: Dict[str, Any],
    outcome: str,
    detail: str = "",
    usage: Optional[Dict[str, Any]] = None,
    now: Optional[float] = None,
    failure_threshold: int = 3,
    empty_threshold: int = 2,
    free_credits_limit: float = 50.0,
) -> Dict[str, Any]:
    """Record one request outcome on an account (in place).

    Returns the account for convenience. Disabled accounts are never
    auto-reenabled here: recovery is a manual /check or archive restore.
    """
    normalize_account(account)
    health = account["health"]
    timestamp = _iso_now(now)

    if outcome == OUTCOME_SUCCESS:
        health["success_count"] = int(health.get("success_count") or 0) + 1
        health["consecutive_failures"] = 0
        health["consecutive_empty"] = 0
        health["last_success_at"] = timestamp
        _update_credit_stats(account, usage)
        predict_quota(account, free_credits_limit=free_credits_limit)
        return account

    if outcome == OUTCOME_QUOTA:
        account["status"] = STATUS_QUOTA_EXCEEDED
        quota = account["quota"]
        quota["exceeded"] = True
        quota["source"] = "notion"
        quota["remaining"] = 0
        quota["estimated_replies_left"] = 0
        quota["confidence"] = "exact"
        if isinstance(detail, dict):
            if detail.get("current") is not None:
                quota["used"] = detail.get("current")
            if detail.get("total") is not None:
                quota["total"] = detail.get("total")
        account["status_changed_at"] = timestamp
        return account

    if outcome == OUTCOME_INVALID:
        account["status"] = STATUS_DISABLED
        health["disabled_reason"] = "account_invalid"
        health["disabled_at"] = timestamp
        health["last_failure_reason"] = (detail or "account invalid")[:300]
        health["failure_count"] = int(health.get("failure_count") or 0) + 1
        account["status_changed_at"] = timestamp
        return account

    if outcome in (OUTCOME_EMPTY, OUTCOME_TRUNCATED):
        health["failure_count"] = int(health.get("failure_count") or 0) + 1
        health["consecutive_failures"] = int(health.get("consecutive_failures") or 0) + 1
        health["last_failure_at"] = timestamp
        health["last_failure_reason"] = (detail or outcome)[:300]
        if outcome == OUTCOME_EMPTY:
            health["empty_count"] = int(health.get("empty_count") or 0) + 1
            health["consecutive_empty"] = int(health.get("consecutive_empty") or 0) + 1
        disable_reason = ""
        if int(health["consecutive_empty"]) >= max(1, empty_threshold):
            disable_reason = "consecutive_empty_responses"
        elif int(health["consecutive_failures"]) >= max(1, failure_threshold):
            disable_reason = "consecutive_failures"
        if disable_reason and account.get("status") == STATUS_ACTIVE:
            account["status"] = STATUS_DISABLED
            health["disabled_reason"] = disable_reason
            health["disabled_at"] = timestamp
            account["status_changed_at"] = timestamp
        return account

    # Transport/network issues are not the account's fault: track but never
    # disable or trip the replenish circuit for them.
    health["failure_count"] = int(health.get("failure_count") or 0) + 1
    health["last_failure_at"] = timestamp
    health["last_failure_reason"] = (detail or outcome)[:300]
    return account


def _update_credit_stats(account: Dict[str, Any], usage: Optional[Dict[str, Any]]) -> None:
    if not usage:
        return
    try:
        credits = float(usage.get("credits_used") or 0)
    except (TypeError, ValueError):
        return
    credit = account["credit_stats"]
    samples = int(credit.get("samples") or 0)
    previous = float(credit.get("avg_credits") or 0.0)
    alpha = 0.3
    credit["avg_credits"] = round(credits if samples == 0 else previous * (1 - alpha) + credits * alpha, 4)
    credit["last_credits"] = credits
    credit["samples"] = samples + 1


def predict_quota(account: Dict[str, Any], free_credits_limit: float = 50.0) -> Dict[str, Any]:
    """Estimate how many replies an account has left.

    Uses Notion-reported totals when available; otherwise estimates from the
    local credit average against the configured free-tier limit.
    """
    normalize_account(account)
    quota = account["quota"]
    if quota.get("exceeded"):
        quota["estimated_replies_left"] = 0
        quota["confidence"] = "exact"
        return quota
    total = quota.get("total")
    used = quota.get("used")
    source = quota.get("source")
    # Only trust totals that came from a real quota report; legacy account files
    # contain stale guesses (e.g. total=75) that would wrongly pin the account
    # to zero remaining replies.
    if (
        isinstance(total, (int, float))
        and isinstance(used, (int, float))
        and total > 0
        and (source == "notion" or quota.get("confidence") == "exact")
    ):
        quota["estimated_replies_left"] = max(0, int(total) - int(used))
        quota["confidence"] = "exact"
        return quota
    avg = float((account.get("credit_stats") or {}).get("avg_credits") or 0.0)
    spent = float(account.get("total_credits") or 0.0)
    if avg <= 0 or spent >= float(free_credits_limit):
        # No samples yet, or the local credit sum already exceeds the free-tier
        # estimate (it can overcount across resets). Stay "unknown" and let the
        # authoritative premium-feature-unavailable response decide.
        quota["estimated_replies_left"] = None
        quota["confidence"] = "unknown"
        return quota
    quota["estimated_replies_left"] = max(0, int((free_credits_limit - spent) // max(avg, 0.5)))
    quota["confidence"] = "estimated"
    return quota


def is_available(account: Dict[str, Any], quota_limit: int = 0, reserve_replies: int = 2) -> bool:
    """Whether an account may serve an inference request right now."""
    normalize_account(account)
    if account.get("status") != STATUS_ACTIVE:
        return False
    quota = account["quota"]
    if quota.get("exceeded") and quota.get("source") == "notion":
        return False
    if quota_limit > 0 and int(account.get("total_requests") or 0) >= quota_limit:
        return False
    left = quota.get("estimated_replies_left")
    if isinstance(left, (int, float)) and int(left) <= max(0, reserve_replies):
        return False
    return True


def token_preview(token: str) -> str:
    """Masked token preview safe for dashboard/API responses."""
    value = str(token or "")
    if not value:
        return ""
    if len(value) <= 8:
        return value[:2] + "***"
    return value[:4] + "..." + value[-4:]


def _age_seconds(iso_value: str, now: float) -> Optional[float]:
    if not iso_value:
        return None
    try:
        return max(0.0, now - datetime.fromisoformat(iso_value).timestamp())
    except (ValueError, TypeError):
        return None


def archivable_accounts(
    accounts: Iterable[Dict[str, Any]],
    now: Optional[float] = None,
    quota_days: int = 7,
    disabled_days: int = 3,
) -> List[Dict[str, Any]]:
    """Accounts older than the configured grace periods in a dead status."""
    current = now if now is not None else time.time()
    targets: List[Dict[str, Any]] = []
    for account in accounts:
        normalize_account(account)
        status = account.get("status")
        age_source = account.get("status_changed_at") or account.get("created_at") or ""
        age = _age_seconds(age_source, current)
        if status == STATUS_QUOTA_EXCEEDED and quota_days > 0 and age is not None:
            if age >= quota_days * 86400:
                targets.append(account)
        elif status == STATUS_DISABLED and disabled_days > 0 and age is not None:
            if age >= disabled_days * 86400:
                targets.append(account)
    return targets


def load_archive(archive_path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(archive_path):
        return []
    try:
        with open(archive_path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, list) else []
    except (OSError, json.JSONDecodeError):
        return []


def save_archive(archive_path: str, entries: List[Dict[str, Any]]) -> None:
    """Atomically persist archived accounts (temp file + os.replace)."""
    directory = os.path.dirname(os.path.abspath(archive_path))
    os.makedirs(directory, exist_ok=True)
    descriptor, temp_path = tempfile.mkstemp(prefix=".archive-", dir=directory)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(entries, handle, ensure_ascii=False, indent=2)
        os.replace(temp_path, archive_path)
    except Exception:
        try:
            os.unlink(temp_path)
        except OSError:
            pass
        raise


def archive_accounts(
    accounts: List[Dict[str, Any]],
    archive_path: str,
    targets: Iterable[Dict[str, Any]],
    mode: str = "archive",
    now: Optional[float] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Remove target accounts from the live list (archive or delete).

    Returns (removed_accounts, remaining_accounts); accounts is mutated.
    """
    target_ids = {str(account.get("id") or "") for account in targets if account.get("id")}
    if not target_ids:
        return [], list(accounts)
    removed = [account for account in accounts if str(account.get("id") or "") in target_ids]
    remaining = [account for account in accounts if str(account.get("id") or "") not in target_ids]
    if mode == "archive" and removed:
        entries = load_archive(archive_path)
        stamp = _iso_now(now)
        entries.extend({"archived_at": stamp, "account": account} for account in removed)
        save_archive(archive_path, entries)
    accounts[:] = remaining
    return removed, remaining


def find_archived(archive_path: str, archived_id: str) -> Optional[Dict[str, Any]]:
    for entry in load_archive(archive_path):
        account = entry.get("account") or {}
        if str(account.get("id") or "") == str(archived_id):
            return entry
    return None


def pop_archived(archive_path: str, archived_id: str) -> Optional[Dict[str, Any]]:
    """Remove one archived entry from the archive file and return it."""
    entries = load_archive(archive_path)
    kept: List[Dict[str, Any]] = []
    found: Optional[Dict[str, Any]] = None
    for entry in entries:
        account = entry.get("account") or {}
        if found is None and str(account.get("id") or "") == str(archived_id):
            found = entry
            continue
        kept.append(entry)
    if found is not None:
        save_archive(archive_path, kept)
    return found
