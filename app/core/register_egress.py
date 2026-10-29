"""Registration egress node pool: health scoring, failure taxonomy and pause logic.

Registration success depends heavily on the exit IP. This module keeps a small
health record per configured egress node (REGISTER_PROXY entries, or Clash nodes
when REGISTER_EGRESS_MODE=clash), classifies failures, backs off flagged nodes
and exposes a snapshot for the dashboard. Registration workers run in a
threadpool, so all pool state is protected by a threading lock.

The pool never stores proxy credentials in its state file: nodes are keyed by a
short hash of the proxy URL and only the masked form is persisted/rendered.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import tempfile
import threading
import time
import uuid
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

CATEGORY_RATE_LIMITED = "rate_limited"
CATEGORY_BANNED = "banned"
CATEGORY_HCAPTCHA = "hcaptcha"
CATEGORY_EMAIL_TIMEOUT = "email_timeout"
CATEGORY_EMAIL_PROVIDER = "email_provider"
CATEGORY_NETWORK = "network"
CATEGORY_ALL_COOLING = "all_nodes_cooling"
CATEGORY_UNKNOWN = "unknown"

_HEAVY_CATEGORIES = {CATEGORY_RATE_LIMITED, CATEGORY_BANNED, CATEGORY_ALL_COOLING}
# Failures attributable to the exit IP. Email-provider / email-timeout / unknown
# failures are recorded for observability but must not freeze a healthy node.
_NODE_RELATED_CATEGORIES = frozenset({
    CATEGORY_RATE_LIMITED, CATEGORY_BANNED, CATEGORY_HCAPTCHA, CATEGORY_NETWORK,
})
_CATEGORY_PATTERNS = (
    (CATEGORY_RATE_LIMITED, ("429", "rate_limited", "rate limited")),
    (CATEGORY_BANNED, ("login_banned_user", "account has been locked", "signup_generic_error")),
    (CATEGORY_HCAPTCHA, ("hcaptcha",)),
    (CATEGORY_EMAIL_TIMEOUT, ("未收到 notion 验证码", "未收到验证码", "超时未收到验证")),
    (CATEGORY_EMAIL_PROVIDER, ("emailnator 取地址失败", "emailnator", "创建邮箱失败", "temp_email")),
    (CATEGORY_NETWORK, ("timeout", "timed out", "connection", "tls", "ssl", "proxy", "curl:")),
)


def is_node_related_failure(category: str) -> bool:
    """True when a failure can be attributed to the egress node itself."""
    return category in _NODE_RELATED_CATEGORIES


class RegistrationFailure(Exception):
    """One registration task failed; category drives node cooldown/pause logic."""

    def __init__(self, category: str = CATEGORY_UNKNOWN, detail: str = ""):
        super().__init__(detail or category)
        self.category = category or CATEGORY_UNKNOWN
        self.detail = detail


def classify_registration_error(detail: Any) -> str:
    """Map a registration error message/exception to a failure category."""
    text = str(detail or "")
    lowered = text.lower()
    for category, patterns in _CATEGORY_PATTERNS:
        if any(pattern in lowered for pattern in patterns):
            return category
    return CATEGORY_UNKNOWN


def _mask_proxy(proxy: Optional[str]) -> str:
    value = str(proxy or "")
    if not value:
        return ""
    return re.sub(r"://([^/@:]+):([^/@]+)@", "://***@", value)


def _node_key(node_id: str) -> str:
    return hashlib.sha256(str(node_id).encode("utf-8")).hexdigest()[:16]


class EgressNodePool:
    """Thread-safe health-aware node selector for auto-registration."""

    def __init__(
        self,
        proxies: Optional[List[str]] = None,
        rotate: bool = True,
        state_path: Optional[str] = None,
        cooldown_base: int = 60,
        cooldown_max: int = 900,
        failure_threshold: int = 3,
        mode: str = "proxy",
        clash_nodes: Optional[List[str]] = None,
        clash_controller: Any = None,
        clash_proxy_url: Optional[str] = None,
        now_fn=time.time,
    ) -> None:
        self._lock = threading.RLock()
        self._rotate = bool(rotate)
        self._mode = str(mode or "proxy").lower()
        self._cooldown_base = max(1, int(cooldown_base))
        self._cooldown_max = max(self._cooldown_base, int(cooldown_max))
        self._failure_threshold = max(1, int(failure_threshold))
        self._state_path = state_path or ""
        self._clash = clash_controller
        self._clash_proxy_url = clash_proxy_url
        self._now = now_fn
        self._last_pick_at = 0.0
        self._nodes: Dict[str, Dict[str, Any]] = {}
        self._order: List[str] = []
        self._categories: Dict[str, int] = {}
        self._proxies = list(proxies or [])
        if self._mode == "clash":
            for node in (clash_nodes or []):
                self._add_node(str(node), str(node), str(node))
        else:
            for proxy in self._proxies:
                self._add_node(proxy, proxy, _mask_proxy(proxy))
        self._load_state()

    # -- node helpers ---------------------------------------------------
    def _add_node(self, node_id: str, proxy: Optional[str], label: str) -> None:
        existing = self._nodes.get(node_id)
        self._nodes[node_id] = {
            "node_id": node_id,
            "proxy": proxy,
            "label": label,
            "proxy_masked": _mask_proxy(proxy) if proxy and self._mode != "clash" else "",
            "successes": (existing or {}).get("successes", 0),
            "failures": (existing or {}).get("failures", 0),
            "consecutive_failures": (existing or {}).get("consecutive_failures", 0),
            "cooldown_until": (existing or {}).get("cooldown_until", 0.0),
            "last_error_category": (existing or {}).get("last_error_category", ""),
            "last_used_at": (existing or {}).get("last_used_at", 0.0),
            "last_used_wall": (existing or {}).get("last_used_wall", ""),
        }
        if node_id not in self._order:
            self._order.append(node_id)

    @property
    def mode(self) -> str:
        return self._mode

    def _eligible(self, now: float) -> List[Dict[str, Any]]:
        return [node for node in self._nodes.values() if float(node.get("cooldown_until") or 0) <= now]

    def pick(self) -> Optional[Dict[str, Any]]:
        """Return {"node_id","proxy","label","mode"} or None when all are cooling."""
        with self._lock:
            if not self._order:
                return None
            now = self._now()
            if not self._rotate:
                # rotate=False keeps the old "always first node" behaviour.
                node = self._nodes[self._order[0]]
                if self._mode == "clash" and not self._switch_clash(node["node_id"]):
                    return None
                node["last_used_at"] = now
                self._last_pick_at = now
                self._save_state()
                return self._result(node)
            eligible = self._eligible(now)
            if not eligible:
                return None
            eligible.sort(key=lambda item: (
                int(item.get("consecutive_failures") or 0),
                -int(item.get("successes") or 0),
                float(item.get("last_used_at") or 0.0),
            ))
            node = eligible[0]
            if self._mode == "clash" and not self._switch_clash(node["node_id"]):
                # Switch failed: penalize and try the next eligible node once.
                self._report_locked(node, False, CATEGORY_NETWORK)
                remaining = [item for item in eligible if item["node_id"] != node["node_id"]]
                if not remaining:
                    return None
                node = remaining[0]
                if not self._switch_clash(node["node_id"]):
                    self._report_locked(node, False, CATEGORY_NETWORK)
                    return None
            node["last_used_at"] = now
            self._last_pick_at = now
            self._save_state()
            return self._result(node)

    def _switch_clash(self, node_name: str) -> bool:
        if self._clash is None:
            return True
        try:
            self._clash.switch(node_name)
            return True
        except Exception as exc:
            logger.warning("Clash 节点切换失败(%s): %s", node_name, exc)
            return False

    def _result(self, node: Dict[str, Any]) -> Dict[str, Any]:
        proxy = self._clash_proxy_url if self._mode == "clash" else node.get("proxy")
        return {
            "node_id": node["node_id"],
            "proxy": proxy,
            "label": node.get("label", node["node_id"]),
            "mode": self._mode,
        }

    def report(self, node_id: Optional[str], ok: bool, detail: Any = "") -> None:
        """Record one registration attempt outcome for a node."""
        if node_id is None:
            return
        category = "" if ok else classify_registration_error(detail)
        with self._lock:
            node = self._nodes.get(node_id)
            if node is None:
                return
            self._report_locked(node, ok, category)
            self._save_state()

    def _report_locked(self, node: Dict[str, Any], ok: bool, category: str) -> None:
        """Per-node circuit breaker: freeze only after N consecutive failures.

        A success resets the consecutive counter to zero. Below the threshold the
        node stays selectable (the picker still prefers nodes with fewer
        consecutive failures). Once the threshold is reached the node is frozen
        with exponential backoff based on how far past the threshold it is.
        """
        now = self._now()
        if ok:
            node["successes"] = int(node.get("successes") or 0) + 1
            node["consecutive_failures"] = 0
            node["cooldown_until"] = 0.0
            node["last_error_category"] = ""
            return
        node["failures"] = int(node.get("failures") or 0) + 1
        node["last_error_category"] = category or CATEGORY_UNKNOWN
        self._categories[category or CATEGORY_UNKNOWN] = self._categories.get(category or CATEGORY_UNKNOWN, 0) + 1
        if not is_node_related_failure(node["last_error_category"]):
            # Email service / unknown failures are recorded only; they neither
            # advance nor reset the node's consecutive-failure streak.
            return
        node["consecutive_failures"] = int(node.get("consecutive_failures") or 0) + 1
        streak = int(node["consecutive_failures"])
        if streak < self._failure_threshold:
            return  # not enough consecutive failures: do not freeze yet
        over = streak - self._failure_threshold + 1
        if category in _HEAVY_CATEGORIES:
            level = over
        else:
            level = max(1, (over + 1) // 2)
        cooldown = min(self._cooldown_base * (2 ** max(0, level - 1)), self._cooldown_max)
        node["cooldown_until"] = now + cooldown
        node["last_used_wall"] = time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(now))

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            now = self._now()
            nodes = []
            for node_id in self._order:
                node = dict(self._nodes[node_id])
                node["cooldown_seconds"] = max(0, int(float(node.get("cooldown_until") or 0) - now))
                node.pop("proxy", None)
                nodes.append(node)
            all_cooling = bool(self._order) and not self._eligible(now)
            return {
                "mode": self._mode,
                "rotate": self._rotate,
                "failure_threshold": self._failure_threshold,
                "nodes": nodes,
                "categories": dict(self._categories),
                "all_cooling": all_cooling,
                "restore_node": getattr(self._clash, "restore_target", None) if self._clash else None,
                "dry_run": bool(getattr(self._clash, "dry_run", False)) if self._clash else False,
            }

    def reset(self) -> Dict[str, Any]:
        with self._lock:
            for node in self._nodes.values():
                node["successes"] = 0
                node["failures"] = 0
                node["consecutive_failures"] = 0
                node["cooldown_until"] = 0.0
                node["last_error_category"] = ""
            self._categories = {}
            if self._clash is not None:
                try:
                    self._clash.restore()
                except Exception as exc:
                    logger.warning("恢复 Clash 原节点失败: %s", exc)
            self._save_state()
            return {"success": True}

    def maybe_restore(self, idle_seconds: int = 60) -> bool:
        """Restore the Clash node after the pool has been idle (clash mode only)."""
        if self._clash is None:
            return False
        with self._lock:
            if self._last_pick_at and (self._now() - self._last_pick_at) >= max(1, idle_seconds):
                try:
                    restored = bool(self._clash.restore())
                    self._last_pick_at = 0.0
                    return restored
                except Exception as exc:
                    logger.warning("恢复 Clash 节点失败: %s", exc)
        return False

    # -- persistence ----------------------------------------------------
    def _load_state(self) -> None:
        path = self._state_path
        if not path or not os.path.exists(path):
            return
        try:
            with open(path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except (OSError, json.JSONDecodeError):
            logger.warning("读取注册节点状态失败，使用空状态: %s", path)
            return
        persisted = data.get("nodes") if isinstance(data, dict) else None
        if not isinstance(persisted, dict):
            return
        for node in self._nodes.values():
            saved = persisted.get(_node_key(node["node_id"]))
            if not isinstance(saved, dict):
                continue
            for field in ("successes", "failures", "consecutive_failures", "last_error_category"):
                if field in saved:
                    node[field] = saved[field]
            try:
                node["cooldown_until"] = float(saved.get("cooldown_until") or 0)
                node["last_used_at"] = float(saved.get("last_used_at") or 0)
            except (TypeError, ValueError):
                pass
            node["last_used_wall"] = str(saved.get("last_used_wall") or "")
        categories = data.get("categories") if isinstance(data, dict) else None
        if isinstance(categories, dict):
            for key, value in categories.items():
                try:
                    self._categories[str(key)] = int(value)
                except (TypeError, ValueError):
                    continue

    def _save_state(self) -> None:
        path = self._state_path
        if not path:
            return
        payload = {
            "nodes": {
                _node_key(node["node_id"]): {
                    "label": node.get("label", ""),
                    "successes": node.get("successes", 0),
                    "failures": node.get("failures", 0),
                    "consecutive_failures": node.get("consecutive_failures", 0),
                    "cooldown_until": node.get("cooldown_until", 0.0),
                    "last_error_category": node.get("last_error_category", ""),
                    "last_used_at": node.get("last_used_at", 0.0),
                    "last_used_wall": node.get("last_used_wall", ""),
                }
                for node in self._nodes.values()
            },
            "categories": dict(self._categories),
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        }
        try:
            directory = os.path.dirname(os.path.abspath(path))
            os.makedirs(directory, exist_ok=True)
            descriptor, temp_path = tempfile.mkstemp(prefix=".reg-nodes-", dir=directory)
            try:
                with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                    json.dump(payload, handle, ensure_ascii=False, indent=1)
                os.replace(temp_path, path)
            except Exception:
                try:
                    os.unlink(temp_path)
                except OSError:
                    pass
                raise
        except Exception as exc:
            logger.warning("保存注册节点状态失败: %s", exc)


def compute_pause(
    consecutive_failures: int,
    categories: Dict[str, int],
    now: Optional[float] = None,
    pause_after: int = 10,
    pause_seconds: int = 300,
    hard_disable: bool = False,
) -> Dict[str, Any]:
    """Decide whether auto-replenish should pause after failures.

    all_nodes_cooling pauses immediately; otherwise the consecutive failure
    threshold applies. Unlike the old circuit breaker the default behaviour is
    a timed pause, so replenishment resumes automatically.
    """
    current = now if now is not None else time.time()
    if hard_disable and int(consecutive_failures) >= max(1, int(pause_after)):
        return {"should_pause": False, "paused_until": None,
                "reason": "hard_disable", "hard_disable": True}
    if categories.get(CATEGORY_ALL_COOLING):
        return {"should_pause": True, "paused_until": current + max(1, int(pause_seconds)),
                "reason": CATEGORY_ALL_COOLING, "hard_disable": False}
    if int(consecutive_failures) >= max(1, int(pause_after)):
        return {"should_pause": True, "paused_until": current + max(1, int(pause_seconds)),
                "reason": "consecutive_failures", "hard_disable": False}
    return {"should_pause": False, "paused_until": None, "reason": "", "hard_disable": False}


_pool: Optional[EgressNodePool] = None
_pool_lock = threading.Lock()
_config_lock = threading.RLock()


def _node_config_path() -> str:
    return os.path.join(_repo_root(), "registration_nodes.json")


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S%z")


def load_node_config() -> List[Dict[str, Any]]:
    """Editable node list (registration_nodes.json); empty when not created yet."""
    path = _node_config_path()
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError):
        logger.warning("读取注册节点配置失败: %s", path)
        return []
    items = data.get("nodes") if isinstance(data, dict) else data
    nodes: List[Dict[str, Any]] = []
    if isinstance(items, list):
        for index, item in enumerate(items):
            if not isinstance(item, dict):
                continue
            proxy = str(item.get("proxy") or "").strip()
            if not proxy:
                continue
            nodes.append({
                "id": str(item.get("id") or uuid.uuid4().hex[:12]),
                "proxy": proxy,
                "label": str(item.get("label") or proxy),
                "enabled": bool(item.get("enabled", True)),
                "created_at": str(item.get("created_at") or ""),
                "updated_at": str(item.get("updated_at") or ""),
            })
    return nodes


def save_node_config(nodes: List[Dict[str, Any]]) -> None:
    """Atomically persist the editable node list."""
    path = _node_config_path()
    payload = {"nodes": nodes, "updated_at": _now_iso()}
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    with _config_lock:
        descriptor, temp_path = tempfile.mkstemp(prefix=".reg-nodes-config-", dir=directory)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False, indent=1)
            os.replace(temp_path, path)
        except Exception:
            try:
                os.unlink(temp_path)
            except OSError:
                pass
            raise


def ensure_node_config() -> List[Dict[str, Any]]:
    """Return the editable node list, seeding it from env on first use."""
    with _config_lock:
        nodes = load_node_config()
        if nodes:
            return nodes
        from app.core.config import settings
        from app.utils.http_client import registration_proxy_pool

        mode = str(getattr(settings, "REGISTER_EGRESS_MODE", "proxy") or "proxy").lower()
        seeds: List[str] = []
        if mode == "clash":
            raw = str(getattr(settings, "CLASH_NODES", "") or "")
            seeds = [item.strip() for item in raw.split(",") if item.strip()]
        else:
            seeds = list(registration_proxy_pool())
        now = _now_iso()
        nodes = [
            {
                "id": uuid.uuid4().hex[:12],
                "proxy": proxy,
                "label": proxy,
                "enabled": True,
                "created_at": now,
                "updated_at": now,
            }
            for proxy in seeds
        ]
        save_node_config(nodes)
        return nodes


def reload_egress_pool() -> EgressNodePool:
    """Rebuild the singleton pool from the editable node config."""
    reset_egress_pool()
    return get_egress_pool()



def _repo_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def get_egress_pool() -> EgressNodePool:
    """Process-wide pool configured from settings (created lazily for tests)."""
    global _pool
    if _pool is not None:
        return _pool
    with _pool_lock:
        if _pool is not None:
            return _pool
        from app.core.config import settings  # local import keeps tests injectable
        from app.utils.http_client import registration_proxy_pool

        proxies = registration_proxy_pool()
        mode = str(getattr(settings, "REGISTER_EGRESS_MODE", "proxy") or "proxy").lower()
        clash_nodes: List[str] = []
        clash_controller = None
        clash_proxy_url = getattr(settings, "CLASH_PROXY_URL", None) or (proxies[0] if proxies else None)
        if mode == "clash":
            raw_nodes = str(getattr(settings, "CLASH_NODES", "") or "")
            clash_nodes = [item.strip() for item in raw_nodes.split(",") if item.strip()]
            try:
                from app.core.clash_egress import build_clash_controller

                clash_controller = build_clash_controller()
            except Exception as exc:  # pragma: no cover - environment specific
                logger.warning("Clash 控制器初始化失败，回退静态代理池: %s", exc)
                mode = "proxy"
        config_nodes = ensure_node_config()
        enabled_nodes = [node for node in config_nodes if node.get("enabled", True)]
        if mode == "clash":
            clash_nodes = [node["proxy"] for node in enabled_nodes] or clash_nodes
        else:
            proxies = [node["proxy"] for node in enabled_nodes] or proxies
        state_name = str(getattr(settings, "REGISTER_NODE_STATE_FILE", "register_nodes.json") or "")
        state_path = state_name if not state_name or os.path.isabs(state_name) else os.path.join(_repo_root(), state_name)
        _pool = EgressNodePool(
            proxies=proxies,
            rotate=bool(getattr(settings, "REGISTER_PROXY_ROTATE", True)),
            state_path=state_path,
            cooldown_base=int(getattr(settings, "REGISTER_NODE_COOLDOWN_BASE", 60) or 60),
            cooldown_max=int(getattr(settings, "REGISTER_NODE_COOLDOWN_MAX", 900) or 900),
            failure_threshold=int(getattr(settings, "REGISTER_NODE_FAILURE_THRESHOLD", 3) or 3),
            mode=mode,
            clash_nodes=clash_nodes,
            clash_controller=clash_controller,
            clash_proxy_url=clash_proxy_url,
        )
        return _pool


def reset_egress_pool() -> None:
    """Test helper: drop the cached singleton."""
    global _pool
    with _pool_lock:
        _pool = None
