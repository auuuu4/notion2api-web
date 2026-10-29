"""Tests for account health, quota prediction and dead-account archiving helpers."""
import json
import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core import account_health as health


def make_account(**overrides):
    account = {
        "id": "acc-1",
        "token_v2": "secret-token-value",
        "space_id": "space",
        "user_id": "user",
        "user_email": "acc-1@example.com",
        "status": "active",
        "created_at": "2026-01-01T00:00:00+08:00",
        "total_requests": 0,
        "total_credits": 0,
    }
    account.update(overrides)
    return account


class ClassifyTests(unittest.TestCase):
    def test_exception_codes(self):
        class FakeError(Exception):
            def __init__(self, code):
                super().__init__("boom")
                self.code = code

        cases = {
            "quota_exceeded": health.OUTCOME_QUOTA,
            "account_invalid": health.OUTCOME_INVALID,
            "empty_response": health.OUTCOME_EMPTY,
            "truncated": health.OUTCOME_TRUNCATED,
            "transport": health.OUTCOME_TRANSPORT,
        }
        for code, expected in cases.items():
            self.assertEqual(health.classify_failure(FakeError(code)), expected)

    def test_message_fallbacks(self):
        self.assertEqual(health.classify_failure(RuntimeError("premium-feature-unavailable 50/50")), health.OUTCOME_QUOTA)
        self.assertEqual(health.classify_failure(RuntimeError("HTTP 401 Unauthorized")), health.OUTCOME_INVALID)
        self.assertEqual(health.classify_failure(RuntimeError("Notion 未返回有效响应")), health.OUTCOME_EMPTY)
        self.assertEqual(health.classify_failure(RuntimeError("Notion 响应流被截断")), health.OUTCOME_TRUNCATED)
        self.assertEqual(health.classify_failure(RuntimeError("connection reset")), health.OUTCOME_TRANSPORT)


class OutcomeTests(unittest.TestCase):
    def test_normalize_defaults_and_idempotent(self):
        account = {}
        health.normalize_account(account)
        self.assertEqual(account["status"], "active")
        self.assertEqual(account["health"]["consecutive_failures"], 0)
        self.assertEqual(account["credit_stats"]["samples"], 0)
        snapshot = json.dumps(account, ensure_ascii=False, sort_keys=True)
        health.normalize_account(account)
        self.assertEqual(json.dumps(account, ensure_ascii=False, sort_keys=True), snapshot)

    def test_success_updates_health_and_credits(self):
        account = make_account()
        health.apply_outcome(account, health.OUTCOME_SUCCESS, usage={"credits_used": 2.0}, now=1000)
        self.assertEqual(account["health"]["success_count"], 1)
        self.assertEqual(account["credit_stats"]["last_credits"], 2.0)
        self.assertEqual(account["credit_stats"]["samples"], 1)
        self.assertAlmostEqual(account["credit_stats"]["avg_credits"], 2.0)
        self.assertEqual(account["quota"]["confidence"], "estimated")
        self.assertEqual(account["quota"]["estimated_replies_left"], 25)

    def test_quota_marks_account_exceeded(self):
        account = make_account(total_requests=10)
        health.apply_outcome(account, health.OUTCOME_QUOTA, detail={"current": 68, "total": 50}, now=1000)
        self.assertEqual(account["status"], "quota_exceeded")
        self.assertTrue(account["quota"]["exceeded"])
        self.assertEqual(account["quota"]["estimated_replies_left"], 0)
        self.assertEqual(account["quota"]["used"], 68)
        self.assertEqual(account["quota"]["total"], 50)

    def test_invalid_disables_account(self):
        account = make_account()
        health.apply_outcome(account, health.OUTCOME_INVALID, detail="401", now=1000)
        self.assertEqual(account["status"], "disabled")
        self.assertEqual(account["health"]["disabled_reason"], "account_invalid")

    def test_empty_responses_disable_at_threshold(self):
        account = make_account()
        health.apply_outcome(account, health.OUTCOME_EMPTY, now=1)
        self.assertEqual(account["status"], "active")
        health.apply_outcome(account, health.OUTCOME_EMPTY, now=2)
        self.assertEqual(account["status"], "disabled")
        self.assertEqual(account["health"]["disabled_reason"], "consecutive_empty_responses")

    def test_generic_failures_disable_at_threshold(self):
        account = make_account()
        for index in range(2):
            health.apply_outcome(account, health.OUTCOME_TRUNCATED, now=index)
        self.assertEqual(account["status"], "active")
        health.apply_outcome(account, health.OUTCOME_TRUNCATED, now=3)
        self.assertEqual(account["status"], "disabled")
        self.assertEqual(account["health"]["disabled_reason"], "consecutive_failures")

    def test_transport_failures_never_disable(self):
        account = make_account()
        for index in range(10):
            health.apply_outcome(account, health.OUTCOME_TRANSPORT, detail="timeout", now=index)
        self.assertEqual(account["status"], "active")
        self.assertEqual(account["health"]["failure_count"], 10)
        self.assertEqual(account["health"]["consecutive_failures"], 0)

    def test_success_resets_consecutive_counters(self):
        account = make_account()
        health.apply_outcome(account, health.OUTCOME_EMPTY, now=1)
        health.apply_outcome(account, health.OUTCOME_SUCCESS, now=2)
        self.assertEqual(account["health"]["consecutive_empty"], 0)
        health.apply_outcome(account, health.OUTCOME_EMPTY, now=3)
        self.assertEqual(account["status"], "active")

    def test_disabled_is_not_auto_reenabled_by_success(self):
        account = make_account(status="disabled")
        account["health"] = {"disabled_reason": "consecutive_empty_responses"}
        health.apply_outcome(account, health.OUTCOME_SUCCESS, now=1)
        self.assertEqual(account["status"], "disabled")


class AutoDeleteTests(unittest.TestCase):
    def test_only_quota_outcome_with_flag_deletes(self):
        self.assertTrue(health.should_auto_delete(health.OUTCOME_QUOTA, True))
        self.assertFalse(health.should_auto_delete(health.OUTCOME_QUOTA, False))

    def test_other_outcomes_never_delete(self):
        for outcome in (health.OUTCOME_EMPTY, health.OUTCOME_TRUNCATED,
                        health.OUTCOME_INVALID, health.OUTCOME_TRANSPORT,
                        health.OUTCOME_SUCCESS):
            self.assertFalse(health.should_auto_delete(outcome, True), outcome)


class AvailabilityTests(unittest.TestCase):
    def test_status_and_quota(self):
        self.assertTrue(health.is_available(make_account()))
        self.assertFalse(health.is_available(make_account(status="disabled")))
        self.assertFalse(health.is_available(make_account(status="quota_exceeded")))
        notion_quota = make_account(quota={"exceeded": True, "source": "notion"})
        self.assertFalse(health.is_available(notion_quota))

    def test_manual_quota_limit(self):
        account = make_account(total_requests=5)
        self.assertFalse(health.is_available(account, quota_limit=5))
        self.assertTrue(health.is_available(account, quota_limit=10))

    def test_reserve_estimate(self):
        account = make_account(quota={"estimated_replies_left": 2, "confidence": "estimated"})
        self.assertFalse(health.is_available(account, reserve_replies=2))
        self.assertTrue(health.is_available(account, reserve_replies=0))
        unknown = make_account(quota={"estimated_replies_left": None})
        self.assertTrue(health.is_available(unknown, reserve_replies=2))


class QuotaPredictionTests(unittest.TestCase):
    def test_exceeded_is_zero(self):
        account = make_account(quota={"exceeded": True, "source": "notion"})
        self.assertEqual(health.predict_quota(account)["estimated_replies_left"], 0)

    def test_real_totals(self):
        account = make_account(quota={"used": 30, "total": 50, "source": "notion"})
        quota = health.predict_quota(account)
        self.assertEqual(quota["estimated_replies_left"], 20)
        self.assertEqual(quota["confidence"], "exact")

    def test_estimated_from_average(self):
        account = make_account(total_credits=30)
        account["credit_stats"] = {"avg_credits": 2.0, "last_credits": 2.0, "samples": 5}
        quota = health.predict_quota(account, free_credits_limit=50)
        self.assertEqual(quota["estimated_replies_left"], 10)
        self.assertEqual(quota["confidence"], "estimated")

    def test_unknown_without_samples(self):
        account = make_account()
        quota = health.predict_quota(account)
        self.assertIsNone(quota["estimated_replies_left"])

    def test_legacy_stale_totals_are_ignored(self):
        account = make_account(quota={"used": 104, "total": 75})
        account["credit_stats"] = {"avg_credits": 1.0, "last_credits": 1.0, "samples": 3}
        quota = health.predict_quota(account)
        # Falls back to the local estimate instead of the stale 75/104 totals.
        self.assertEqual(quota["estimated_replies_left"], 50)
        self.assertEqual(quota["confidence"], "estimated")

    def test_spent_over_limit_stays_unknown(self):
        account = make_account(total_credits=80)
        account["credit_stats"] = {"avg_credits": 1.0, "last_credits": 1.0, "samples": 3}
        quota = health.predict_quota(account, free_credits_limit=50)
        self.assertIsNone(quota["estimated_replies_left"])
        self.assertEqual(quota["confidence"], "unknown")


class MaskingTests(unittest.TestCase):
    def test_token_preview(self):
        self.assertEqual(health.token_preview(""), "")
        self.assertNotIn("secret", health.token_preview("secret-token-value-1234"))
        self.assertTrue(health.token_preview("abcdefghijklmnopqrstuvwxyz").startswith("abcd"))


class ArchiveTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.archive_path = os.path.join(self.temp_dir.name, "accounts_archive.json")

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_archivable_accounts_respect_grace_periods(self):
        now = time.time()
        old = "2026-01-01T00:00:00+00:00"
        recent = health._iso_now(now)
        accounts = [
            make_account(id="q-old", status="quota_exceeded", status_changed_at=old),
            make_account(id="q-new", status="quota_exceeded", status_changed_at=recent),
            make_account(id="d-old", status="disabled", status_changed_at=old),
            make_account(id="d-new", status="disabled", status_changed_at=recent),
            make_account(id="active"),
        ]
        targets = health.archivable_accounts(accounts, now=now, quota_days=7, disabled_days=3)
        self.assertEqual({account["id"] for account in targets}, {"q-old", "d-old"})

    def test_archive_mode_moves_accounts_to_file(self):
        accounts = [make_account(id="a"), make_account(id="b")]
        removed, remaining = health.archive_accounts(
            accounts, self.archive_path, [accounts[0]], mode="archive", now=1000
        )
        self.assertEqual([account["id"] for account in removed], ["a"])
        self.assertEqual([account["id"] for account in remaining], ["b"])
        self.assertEqual([account["id"] for account in accounts], ["b"])
        archived = health.load_archive(self.archive_path)
        self.assertEqual(archived[0]["account"]["id"], "a")
        self.assertTrue(archived[0]["archived_at"])

    def test_delete_mode_does_not_write_archive(self):
        accounts = [make_account(id="a")]
        health.archive_accounts(accounts, self.archive_path, [accounts[0]], mode="delete")
        self.assertEqual(accounts, [])
        self.assertEqual(health.load_archive(self.archive_path), [])

    def test_pop_archived_restores_entry(self):
        accounts = [make_account(id="a"), make_account(id="b")]
        health.archive_accounts(accounts, self.archive_path, list(accounts), mode="archive")
        entry = health.pop_archived(self.archive_path, "a")
        self.assertIsNotNone(entry)
        self.assertEqual(entry["account"]["id"], "a")
        self.assertEqual([item["account"]["id"] for item in health.load_archive(self.archive_path)], ["b"])
        self.assertIsNone(health.pop_archived(self.archive_path, "missing"))

    def test_corrupt_archive_is_tolerated(self):
        with open(self.archive_path, "w", encoding="utf-8") as handle:
            handle.write("{not json")
        self.assertEqual(health.load_archive(self.archive_path), [])
        health.save_archive(self.archive_path, [{"archived_at": "x", "account": make_account()}])
        self.assertEqual(len(health.load_archive(self.archive_path)), 1)


if __name__ == "__main__":
    unittest.main()
