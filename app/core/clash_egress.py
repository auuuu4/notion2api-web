"""Optional Clash node rotation for registration egress.

Only active when REGISTER_EGRESS_MODE=clash. The controller talks to mihomo's
external controller either over HTTP (CLASH_API_URL) or over Clash Verge's
Windows named pipe (CLASH_PIPE or auto-discovery). Registration traffic itself
still goes through CLASH_PROXY_URL (the Clash mixed port), so switching the
global selector changes the exit IP for those requests.

Safety:
* CLASH_DRY_RUN=1 only logs the intended switch;
* the controller remembers the original node and restores it when the pool is
  idle (main.py calls maybe_restore) or when the user resets the node pool;
* a failure to reach the controller is reported to the pool, which falls back
  to the static proxy pool until the next attempt.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import threading
import time
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class _HttpTransport:
    def __init__(self, base_url: str, secret: str) -> None:
        self._base = str(base_url).rstrip("/")
        self._secret = secret or ""

    def call(self, method: str, path: str, body: Optional[Dict[str, Any]] = None) -> Any:
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(self._base + path, data=data, method=method)
        request.add_header("Content-Type", "application/json")
        if self._secret:
            request.add_header("Authorization", "Bearer " + self._secret)
        with urllib.request.urlopen(request, timeout=10) as response:
            raw = response.read().decode("utf-8", "replace")
        return json.loads(raw) if raw.strip() else {}


class _PipeTransport:
    """Mihomo controller over Clash Verge's Windows named pipe (ctypes)."""

    def __init__(self, pipe: Optional[str], secret: str) -> None:
        if sys.platform != "win32":
            raise RuntimeError("Clash named pipe transport is Windows-only")
        import ctypes
        import ctypes.wintypes as wt

        self._ctypes = ctypes
        self._wt = wt
        self._secret = secret or ""
        self._pipe = pipe or self._discover_pipe(ctypes)
        self._k32 = ctypes.WinDLL("kernel32", use_last_error=True)
        self._k32.CreateFileW.restype = wt.HANDLE
        self._k32.CreateFileW.argtypes = [wt.LPCWSTR, wt.DWORD, wt.DWORD, ctypes.c_void_p, wt.DWORD, wt.DWORD, wt.HANDLE]
        self._k32.WriteFile.argtypes = [wt.HANDLE, ctypes.c_void_p, wt.DWORD, ctypes.POINTER(wt.DWORD), ctypes.c_void_p]
        self._k32.ReadFile.argtypes = [wt.HANDLE, ctypes.c_void_p, wt.DWORD, ctypes.POINTER(wt.DWORD), ctypes.c_void_p]
        self._k32.CloseHandle.argtypes = [wt.HANDLE]

    @staticmethod
    def _discover_pipe(ctypes: Any) -> str:
        try:
            names = [name for name in os.listdir(r"\\.\pipe\\") if name.startswith("verge-mihomo")]
        except OSError:
            names = []
        if not names:
            raise RuntimeError("mihomo controller pipe not found; set CLASH_PIPE")
        return r"\\.\pipe\\" + sorted(names)[0]

    def call(self, method: str, path: str, body: Optional[Dict[str, Any]] = None) -> Any:
        ctypes = self._ctypes
        wt = self._wt
        handle = self._k32.CreateFileW(self._pipe, 0x80000000 | 0x40000000, 0, None, 3, 0, None)
        if handle in (None, 0xFFFFFFFFFFFFFFFF):
            raise OSError("open Clash pipe failed: %s" % ctypes.get_last_error())
        try:
            payload = json.dumps(body).encode() if body is not None else b""
            request = ("%s %s HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nAuthorization: Bearer %s\r\n"
                       % (method, path, self._secret)).encode()
            if payload:
                request += b"Content-Type: application/json\r\nContent-Length: " + str(len(payload)).encode() + b"\r\n"
            request += b"\r\n" + payload
            written = wt.DWORD(0)
            if not self._k32.WriteFile(handle, request, len(request), ctypes.byref(written), None):
                raise OSError("Clash pipe write failed")
            buffer = ctypes.create_string_buffer(65536)
            read = wt.DWORD(0)
            chunks = []
            deadline = time.time() + 10
            while time.time() < deadline:
                if not self._k32.ReadFile(handle, buffer, 65536, ctypes.byref(read), None) or read.value == 0:
                    break
                chunks.append(buffer.raw[:read.value])
            raw = b"".join(chunks)
            head, _, body_bytes = raw.partition(b"\r\n\r\n")
            if b"transfer-encoding: chunked" in head.lower():
                body_bytes = _dechunk(body_bytes)
            try:
                return json.loads(body_bytes.decode("utf-8", "replace")) if body_bytes.strip() else {}
            except json.JSONDecodeError:
                return {}
        finally:
            self._k32.CloseHandle(handle)


def _dechunk(body: bytes) -> bytes:
    out = bytearray()
    position = 0
    while position < len(body):
        line_end = body.find(b"\r\n", position)
        if line_end < 0:
            break
        try:
            size = int(body[position:line_end].split(b";")[0].strip(), 16)
        except ValueError:
            break
        position = line_end + 2
        if size == 0:
            break
        out.extend(body[position:position + size])
        position += size + 2
    return bytes(out)


class ClashEgress:
    """Switch the Clash selector between configured nodes for registration."""

    def __init__(
        self,
        transport: Any,
        group: str,
        restore_node: Optional[str] = None,
        dry_run: bool = False,
        settle_seconds: float = 1.5,
        now_fn=time.time,
    ) -> None:
        self._transport = transport
        self._group = str(group or "").strip()
        self._requested_restore = restore_node or None
        self.restore_target: Optional[str] = self._requested_restore
        self.dry_run = bool(dry_run)
        self._settle = max(0.0, float(settle_seconds))
        self._lock = threading.RLock()
        self._now = now_fn

    def _group_path(self) -> str:
        return "/proxies/" + urllib.parse.quote(self._group, safe="")

    def current(self) -> Optional[str]:
        try:
            data = self._transport.call("GET", "/proxies")
            group = (data.get("proxies") or {}).get(self._group) or {}
            return group.get("now")
        except Exception:
            return None

    def switch(self, node: str) -> Optional[str]:
        with self._lock:
            if self.restore_target is None:
                self.restore_target = self.current()
            if self.dry_run:
                logger.info("[CLASH DRY-RUN] 切换节点 %s -> %s", self.restore_target, node)
                return None
            self._transport.call("PUT", self._group_path(), {"name": node})
            if self._settle:
                time.sleep(self._settle)
            logger.info("[CLASH] 注册出口切换: %s -> %s", self.restore_target, node)
            return node

    def restore(self) -> bool:
        with self._lock:
            target = self.restore_target
            if not target:
                return False
            if self.dry_run:
                logger.info("[CLASH DRY-RUN] 恢复节点 %s", target)
                return True
            self._transport.call("PUT", self._group_path(), {"name": target})
            logger.info("[CLASH] 已恢复注册前节点: %s", target)
            self.restore_target = target
            return True


def build_clash_controller() -> ClashEgress:
    from app.core.config import settings  # local import keeps tests lightweight

    group = str(getattr(settings, "CLASH_GROUP", "") or "").strip()
    if not group:
        raise RuntimeError("CLASH_GROUP is required when REGISTER_EGRESS_MODE=clash")
    secret = str(getattr(settings, "CLASH_SECRET", "") or "")
    api_url = getattr(settings, "CLASH_API_URL", None)
    pipe = getattr(settings, "CLASH_PIPE", None)
    if api_url:
        transport: Any = _HttpTransport(api_url, secret)
    else:
        transport = _PipeTransport(pipe, secret)
    return ClashEgress(
        transport,
        group,
        restore_node=getattr(settings, "CLASH_RESTORE_NODE", None),
        dry_run=bool(getattr(settings, "CLASH_DRY_RUN", False)),
    )
