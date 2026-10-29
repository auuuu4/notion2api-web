# app/utils/http_client.py
"""HTTP session helper.

Notion now rejects python-requests / cloudscraper TLS fingerprints on its AI
endpoints (legacy calls answer "temporarily-unavailable", agent_service calls
answer "UserValidationError"). The same applies to the Cloudflare-fronted
temp-email service used by auto-registration.

curl_cffi with a Chrome impersonation profile is preferred; cloudscraper is
kept as a fallback for environments where curl_cffi is unavailable.

The auto-registration flow can egress through a proxy (REGISTER_PROXY), because
Notion quickly risk-locks signups from datacenter/reused IPs. Proxies are only
used for registration traffic; AI inference sessions stay direct unless the
caller passes a proxy explicitly.
"""
import itertools
import logging
import re
import threading
from typing import Any, Dict, List, Optional

from app.core.config import settings

logger = logging.getLogger(__name__)

try:  # Chrome TLS/HTTP2 fingerprint
    from curl_cffi import requests as _cffi_requests  # type: ignore
except Exception:  # pragma: no cover
    _cffi_requests = None

try:
    import cloudscraper  # type: ignore
except Exception:  # pragma: no cover
    cloudscraper = None

DEFAULT_IMPERSONATE = "chrome"
_SUPPORTED_PROXY_SCHEMES = ("http://", "https://", "socks5://", "socks5h://", "socks4://")

_PROXY_LOCK = threading.Lock()
_PROXY_ROTATION = itertools.count()
# Last egress picked in this worker thread; lets the registration wrapper report
# node failures for exceptions raised outside the per-attempt try blocks.
_LAST_EGRESS = threading.local()


def last_registration_egress() -> Optional[Dict[str, Any]]:
    value = getattr(_LAST_EGRESS, "value", None)
    return dict(value) if isinstance(value, dict) else None


def normalize_proxy(raw: str) -> str:
    """Normalize one proxy URL; a bare host:port defaults to http://."""
    value = str(raw or "").strip()
    if not value:
        return ""
    if "://" not in value:
        value = "http://" + value
    if not value.lower().startswith(_SUPPORTED_PROXY_SCHEMES):
        logger.warning("Unsupported registration proxy scheme: %s", value)
        return ""
    return value


def parse_proxy_list(raw: Any) -> List[str]:
    """Parse REGISTER_PROXY (comma/semicolon/space separated) into proxy URLs."""
    if isinstance(raw, (list, tuple, set)):
        parts = [str(item) for item in raw]
    else:
        parts = re.split(r"[,;\s]+", str(raw or ""))
    proxies: List[str] = []
    for part in parts:
        value = normalize_proxy(part)
        if value and value not in proxies:
            proxies.append(value)
    return proxies


def mask_proxy(proxy: Optional[str]) -> str:
    """Hide proxy credentials before writing them to logs/dashboard."""
    value = str(proxy or "").strip()
    if not value:
        return ""
    return re.sub(r"://([^/@:]+):([^/@]+)@", "://***@", value)


def registration_proxy_pool() -> List[str]:
    """Configured registration proxy pool (empty when egress is direct)."""
    return parse_proxy_list(getattr(settings, "REGISTER_PROXY", None))


def registration_proxy_info() -> Dict[str, Any]:
    """Safe-to-render description of the registration egress configuration."""
    pool = registration_proxy_pool()
    return {
        "configured": bool(pool),
        "count": len(pool),
        "rotate": bool(getattr(settings, "REGISTER_PROXY_ROTATE", True)),
        "proxies": [mask_proxy(proxy) for proxy in pool],
    }


def next_registration_proxy() -> Optional[str]:
    """Pick the egress proxy for the next registration attempt (round-robin).

    Returns None when REGISTER_PROXY is unset (direct connection).
    """
    pool = registration_proxy_pool()
    if not pool:
        return None
    if not bool(getattr(settings, "REGISTER_PROXY_ROTATE", True)):
        return pool[0]
    with _PROXY_LOCK:
        index = next(_PROXY_ROTATION)
    return pool[index % len(pool)]


def next_registration_egress() -> Dict[str, Any]:
    """Pick the next registration egress from the health-aware node pool.

    Returns a dict with node_id/proxy/label/mode; node_id can be "direct" when
    REGISTER_PROXY is unset. Unlike next_registration_proxy (kept for backward
    compatibility) this accounts for node cooldowns and failure history.
    """
    try:
        from app.core.register_egress import get_egress_pool

        picked = get_egress_pool().pick()
    except Exception as exc:  # never let egress selection break registration
        logger.warning("注册出口节点池选择失败，回退轮转: %s", exc)
        proxy = next_registration_proxy()
        return {"node_id": "direct" if not proxy else proxy, "proxy": proxy, "label": mask_proxy(proxy), "mode": "proxy"}
    if picked:
        _LAST_EGRESS.value = dict(picked)
        return picked
    # All nodes cooling: fall back to the configured round-robin so the task can
    # still start; the caller reports all_nodes_cooling to trigger a pause.
    proxy = next_registration_proxy()
    fallback = {"node_id": "direct" if not proxy else proxy, "proxy": proxy, "label": mask_proxy(proxy), "mode": "proxy"}
    _LAST_EGRESS.value = dict(fallback)
    return fallback


def report_registration_result(node_id: Optional[str], ok: bool, detail: Any = "") -> None:
    """Feed one registration attempt outcome back into the node pool."""
    try:
        from app.core.register_egress import get_egress_pool

        get_egress_pool().report(node_id, bool(ok), detail)
    except Exception as exc:
        logger.warning("注册出口节点池状态更新失败: %s", exc)


def _proxy_map(proxy: Optional[str]) -> Optional[Dict[str, str]]:
    if not proxy:
        return None
    return {"http": proxy, "https": proxy}


def create_session(impersonate: str = DEFAULT_IMPERSONATE, proxy: Optional[str] = None):
    """Return an HTTP session with a browser TLS fingerprint when possible.

    Args:
        impersonate: curl_cffi impersonation profile.
        proxy: optional proxy URL (http/https/socks5); applied to all requests
            of the returned session. Used by auto-registration egress.
    """
    proxies = _proxy_map(normalize_proxy(proxy) if proxy else None)
    if _cffi_requests is not None:
        try:
            session = _cffi_requests.Session(impersonate=impersonate)
            if proxies:
                session.proxies = proxies
            return session
        except Exception as e:  # pragma: no cover - curl_cffi build issues
            logger.warning(f"curl_cffi session creation failed, falling back: {e}")
    if cloudscraper is not None:
        session = cloudscraper.create_scraper()
        if proxies:
            session.proxies.update(proxies)
        return session
    import requests  # last-resort fallback
    session = requests.Session()
    if proxies:
        session.proxies.update(proxies)
    return session


def get_cookie(session, name: str) -> Optional[str]:
    """Read a cookie value from either a requests or a curl_cffi session."""
    try:
        value = session.cookies.get(name)
        if value:
            return value
    except Exception:
        pass
    try:
        for cookie in session.cookies:
            if getattr(cookie, "name", None) == name:
                return cookie.value
    except Exception:
        pass
    return None
