# app/utils/temp_email.py
"""EmailNator-backed temporary mailbox (Gmail / Googlemail addresses).

The public temp-email instance shipped with the original project stopped
delivering mail (last received message 2026-10-24), and Notion rejects most
disposable domains with `invalid_email_domain`. EmailNator hands out aliases on
Google-owned domains (`@googlemail.com`), which pass Notion's domain and SMTP
reachability checks, and exposes an HTTP inbox API.
"""
import logging
import re
import time
from typing import List, Optional

from app.utils.http_client import create_session

logger = logging.getLogger(__name__)

BASE_URL = "https://www.emailnator.com"
# EMAIL_TYPES: domain=1, plusGmail=2, dotGmail=3, googleMail=8
GOOGLE_MAIL_TYPE = 8

_CODE_RE = re.compile(r"(?<!\d)(\d{6})(?!\d)")


class EmailnatorMailbox:
    def __init__(self, proxy: Optional[str] = None):
        # Registration egress: the same proxy as the Notion signup calls keeps
        # the mailbox and the account signup on one exit IP.
        self.session = create_session(proxy=proxy)
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Origin": BASE_URL,
            "Referer": BASE_URL + "/",
        }

    def _post(self, path: str, payload: dict):
        return self.session.post(BASE_URL + path, headers=self.headers, json=payload, timeout=30)

    def create_address(self, kind: int = GOOGLE_MAIL_TYPE) -> str:
        resp = self._post("/api/generate-email", {"ids": [kind]})
        if resp.status_code != 200:
            raise RuntimeError(f"EmailNator 取地址失败: HTTP {resp.status_code} {resp.text[:200]}")
        email = (resp.json() or {}).get("email", "")
        if not email:
            raise RuntimeError(f"EmailNator 返回异常: {resp.text[:200]}")
        return email

    def fetch_messages(self, email: str) -> List[dict]:
        resp = self._post("/api/message-list", {"email": email, "limit": 20})
        if resp.status_code != 200:
            return []
        data = resp.json() or {}
        messages = data.get("messages") or data.get("messageData") or data.get("data") or []
        if isinstance(messages, dict):
            messages = list(messages.values())
        return [m for m in messages if isinstance(m, dict)]

    def message_html(self, message_id: str) -> str:
        resp = self.session.get(BASE_URL + f"/api/message/{message_id}", headers=self.headers, timeout=30)
        return resp.text if resp.status_code == 200 else ""

    @staticmethod
    def extract_code(html: str) -> Optional[str]:
        match = re.search(r"<pre[^>]*>\s*(\d{6})\s*</pre>", html or "")
        if match:
            return match.group(1)
        match = re.search(r"password=(\d{6})", html or "")
        if match:
            return match.group(1)
        match = _CODE_RE.search(re.sub(r"<[^>]+>", " ", html or ""))
        return match.group(1) if match else None

    def poll_code(self, email: str, since_ts: int, timeout: int = 240, interval: int = 5) -> Optional[str]:
        deadline = time.time() + timeout
        while time.time() < deadline:
            for message in self.fetch_messages(email):
                sender = ((message.get("from") or "") + (message.get("subject") or "")).lower()
                if "notion" not in sender:
                    continue
                ts = int(message.get("timestamp") or 0)
                if ts and ts < since_ts - 10:
                    continue
                message_id = message.get("id") or message.get("messageID")
                if not message_id:
                    continue
                code = self.extract_code(self.message_html(message_id))
                if code:
                    return code
            time.sleep(interval)
        return None
