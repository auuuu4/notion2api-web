"""Tests for the auto-registration / auto-replenish egress proxy config."""
import itertools
import os
import sys
import types
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.config import settings
from app.utils import http_client


class ProxyParsingTests(unittest.TestCase):
    def test_empty_pool(self):
        self.assertEqual(http_client.parse_proxy_list(None), [])
        self.assertEqual(http_client.parse_proxy_list(""), [])
        self.assertEqual(http_client.parse_proxy_list("  ,; "), [])

    def test_multiple_entries_and_default_scheme(self):
        raw = "http://127.0.0.1:7897, 10.0.0.2:8080; socks5://user:pass@proxy:1080\nhttp://dup:1,http://dup:1"
        self.assertEqual(
            http_client.parse_proxy_list(raw),
            [
                "http://127.0.0.1:7897",
                "http://10.0.0.2:8080",
                "socks5://user:pass@proxy:1080",
                "http://dup:1",
            ],
        )

    def test_list_input(self):
        self.assertEqual(
            http_client.parse_proxy_list(["http://a:1", "b:2"]),
            ["http://a:1", "http://b:2"],
        )

    def test_invalid_scheme_is_dropped(self):
        self.assertEqual(http_client.parse_proxy_list("ftp://x:1"), [])

    def test_mask_proxy_credentials(self):
        self.assertEqual(http_client.mask_proxy("http://user:pass@host:7897"), "http://***@host:7897")
        self.assertEqual(http_client.mask_proxy("socks5://u:p@1.2.3.4:1080"), "socks5://***@1.2.3.4:1080")
        self.assertEqual(http_client.mask_proxy("http://127.0.0.1:7897"), "http://127.0.0.1:7897")
        self.assertEqual(http_client.mask_proxy(None), "")


class RotationTests(unittest.TestCase):
    def setUp(self):
        http_client._PROXY_ROTATION = itertools.count()

    def test_round_robin_rotation(self):
        with patch.object(settings, "REGISTER_PROXY", "http://a:1,http://b:2"), patch.object(
            settings, "REGISTER_PROXY_ROTATE", True
        ):
            seen = [http_client.next_registration_proxy() for _ in range(5)]
        self.assertEqual(seen, ["http://a:1", "http://b:2", "http://a:1", "http://b:2", "http://a:1"])

    def test_rotation_can_be_disabled(self):
        with patch.object(settings, "REGISTER_PROXY", "http://a:1,http://b:2"), patch.object(
            settings, "REGISTER_PROXY_ROTATE", False
        ):
            seen = [http_client.next_registration_proxy() for _ in range(3)]
        self.assertEqual(seen, ["http://a:1", "http://a:1", "http://a:1"])

    def test_direct_when_unset(self):
        with patch.object(settings, "REGISTER_PROXY", None):
            self.assertIsNone(http_client.next_registration_proxy())

    def test_info_masks_credentials(self):
        with patch.object(settings, "REGISTER_PROXY", "http://user:pass@127.0.0.1:7897"), patch.object(
            settings, "REGISTER_PROXY_ROTATE", True
        ):
            info = http_client.registration_proxy_info()
        self.assertTrue(info["configured"])
        self.assertEqual(info["count"], 1)
        self.assertEqual(info["proxies"], ["http://***@127.0.0.1:7897"])


class SessionProxyTests(unittest.TestCase):
    def test_curl_cffi_session_gets_proxies(self):
        captured = {}

        class FakeSession:
            def __init__(self, impersonate=None):
                captured["impersonate"] = impersonate
                self.proxies = {}

        fake_module = types.SimpleNamespace(Session=FakeSession)
        with patch.object(http_client, "_cffi_requests", fake_module):
            session = http_client.create_session(proxy="127.0.0.1:7897")
        self.assertEqual(
            session.proxies,
            {"http": "http://127.0.0.1:7897", "https": "http://127.0.0.1:7897"},
        )
        self.assertEqual(captured["impersonate"], http_client.DEFAULT_IMPERSONATE)

    def test_direct_session_has_no_proxies(self):
        class FakeSession:
            def __init__(self, impersonate=None):
                self.proxies = {}

        fake_module = types.SimpleNamespace(Session=FakeSession)
        with patch.object(http_client, "_cffi_requests", fake_module):
            session = http_client.create_session()
        self.assertEqual(getattr(session, "proxies", {}), {})

    def test_emailnator_mailbox_receives_proxy(self):
        import app.utils.temp_email as temp_email

        captured = {}

        def fake_create_session(impersonate=None, proxy=None):
            captured["proxy"] = proxy
            return types.SimpleNamespace(headers={})

        with patch.object(temp_email, "create_session", fake_create_session):
            temp_email.EmailnatorMailbox(proxy="http://127.0.0.1:7897")
        self.assertEqual(captured["proxy"], "http://127.0.0.1:7897")


if __name__ == "__main__":
    unittest.main()
