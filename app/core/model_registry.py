# app/core/model_registry.py
"""Notion AI model registry.

The model list is fetched from Notion's own `getAvailableModels` endpoint (per
workspace) and cached. A static snapshot is kept as an offline fallback.
"""
import logging
import threading
import time
from typing import Dict, List, Optional, Tuple

from app.core.config import settings

logger = logging.getLogger(__name__)

# Snapshot from getAvailableModels (2026-10-26, free workspace): (codename, display)
STATIC_MODELS: List[Tuple[str, str]] = [
    ("omniberry-sundae", "GPT-6.1 Sol"),
    ("orchid-muffin", "GPT-5.6 Terra"),
    ("omodi", "GPT-6 Luna"),
    ("albuquerque-quinn", "Opus 5.5"),
    ("soursop-shortcake", "Grok 4.6"),
    ("olive-jellyroll", "GPT-5.6 Luna"),
    ("orange-mousse", "GPT-5.6 Sol"),
    ("oatmeal-cookie", "GPT-5.2"),
    ("oval-kumquat-medium", "GPT-5.4"),
    ("opal-quince-medium", "GPT-5.5"),
    ("vertex-gemini-3.5-flash", "Gemini 3.5 Flash"),
    ("vertex-gemini-3.6-flash", "Gemini 3.6 Flash"),
    ("grapefruit-zeppole", "Gemini 3.7 Flash"),
    ("almond-croissant-low", "Sonnet 4.6"),
    ("angel-cake-high", "Sonnet 5"),
    ("achira-donut", "Sonnet 5.5"),
    ("amla-jam", "Haiku 5.5"),
    ("avocado-froyo-medium", "Opus 4.6"),
    ("apricot-sorbet-high", "Opus 4.7"),
    ("ambrosia-tart-high", "Opus 4.8"),
    ("agave-flan", "Opus 5"),
    ("oregon-grape-medium", "GPT-5.4 Mini"),
    ("otaheite-apple-medium", "GPT-5.4 Nano"),
    ("fireworks-kimi-k3", "Kimi K3"),
    ("baseten-deepseek-v4-pro", "DeepSeek V4 Pro"),
    ("baseten-deepseek-v4-flash", "DeepSeek V4 Flash"),
    ("baseten-deepseek-v4.1-flash", "DeepSeek V4.1 Flash"),
    ("baseten-glm-5.2", "GLM 5.2"),
    ("baseten-glm-5.3", "GLM 5.3"),
    ("baseten-glm-5.3-flash", "GLM 5.3 Flash"),
    ("xigua-mochi-medium", "Grok 4.3"),
    ("strawberry-whoopiepie", "Grok 4.5"),
    ("xinomavro-cake", "Grok Build 0.1"),
    ("galette-medium-thinking", "Gemini 3.1 Pro"),
    ("anthropic-haiku-4.5", "Haiku 4.5"),
    ("gingerbread", "Gemini 3 Flash"),
]

_LOCK = threading.Lock()
_CODENAME_TO_DISPLAY: Dict[str, str] = {c: d for c, d in STATIC_MODELS}
_DISPLAY_TO_CODENAME: Dict[str, str] = {d: c for c, d in STATIC_MODELS}
_STATIC_DISPLAY_BY_CODENAME = dict(_CODENAME_TO_DISPLAY)
_LAST_REFRESH = 0.0
REFRESH_INTERVAL = 600  # seconds


def apply_dynamic(model_entries: List[dict]) -> int:
    """Replace the registry with the models returned by Notion."""
    global _CODENAME_TO_DISPLAY, _DISPLAY_TO_CODENAME, _LAST_REFRESH
    usable: Dict[str, str] = {}
    for entry in model_entries or []:
        if not isinstance(entry, dict):
            continue
        codename = entry.get("model")
        display = entry.get("modelMessage") or codename
        if not codename or entry.get("isDisabled"):
            continue
        # Only keep models the legacy runtime can run.
        workflow = entry.get("workflow") or {}
        if workflow and workflow.get("isDisabled"):
            continue
        usable[codename] = display
    if not usable:
        return 0
    with _LOCK:
        _CODENAME_TO_DISPLAY = usable
        _DISPLAY_TO_CODENAME = {display: codename for codename, display in usable.items()}
        _LAST_REFRESH = time.time()
    return len(usable)


def should_refresh() -> bool:
    return (time.time() - _LAST_REFRESH) > REFRESH_INTERVAL


def resolve(model_name: Optional[str]) -> str:
    """Resolve a display name or raw Notion codename to a codename."""
    if not model_name:
        return settings.DEFAULT_MODEL
    name = str(model_name).strip()
    codename = _DISPLAY_TO_CODENAME.get(name)
    if codename:
        return codename
    if name in _CODENAME_TO_DISPLAY:
        return name
    # Legacy static maps from config (kept for compatibility).
    if name in settings.MODEL_MAP:
        return settings.MODEL_MAP[name]
    if name in settings.KNOWN_MODELS:
        return name
    return settings.DEFAULT_MODEL


def codenames() -> List[str]:
    return list(_CODENAME_TO_DISPLAY.keys())


def openai_models() -> List[dict]:
    """OpenAI /v1/models payload. IDs are the user-facing display names."""
    created = int(time.time())
    data = []
    for codename, display in _CODENAME_TO_DISPLAY.items():
        data.append({
            "id": display,
            "object": "model",
            "created": created,
            "owned_by": "notion",
            "notion_codename": codename,
        })
    return data
