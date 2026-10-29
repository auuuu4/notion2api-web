"""Tests for registration egress node pool, failure taxonomy and pause logic."""
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core import register_egress as egress


class ClassificationTests(unittest.TestCase):
    def test_category_table(self):
        cases = {
            "HTTP 429 rate_limited": egress.CATEGORY_RATE_LIMITED,
            "sendTemporaryPassword HTTP 400 (login_banned_user)": egress.CATEGORY_BANNED,
            "Your account has been locked.": egress.CATEGORY_BANNED,
            "Notion 返回验证码类型: hcaptcha": egress.CATEGORY_HCAPTCHA,
            "EmailNator 未收到 Notion 验证码邮件": egress.CATEGORY_EMAIL_TIMEOUT,
            "EmailNator 取地址失败: HTTP 500": egress.CATEGORY_EMAIL_PROVIDER,
            "Connection timed out": egress.CATEGORY_NETWORK,
            "something odd": egress.CATEGORY_UNKNOWN,
        }
        for detail, expected in cases.items():
            self.assertEqual(egress.classify_registration_error(detail), expected, detail)

    def test_registration_failure_carries_category(self):
        failure = egress.RegistrationFailure(egress.CATEGORY_BANNED, "locked")
        self.assertEqual(failure.category, egress.CATEGORY_BANNED)
        self.assertIn("locked", str(failure))


class NodePoolTests(unittest.TestCase):
    def make_pool(self, proxies, **kwargs):
        # Legacy tests below assert the freeze-after-every-failure behaviour, so
        # they opt into threshold=1 explicitly; production default is 3.
        kwargs.setdefault("failure_threshold", 1)
        return egress.EgressNodePool(proxies=proxies, cooldown_base=60, cooldown_max=900, **kwargs)

    def test_round_robin_and_lru(self):
        pool = self.make_pool(["http://a:1", "http://b:2"], now_fn=lambda: 1000)
        first = pool.pick()
        second = pool.pick()
        self.assertEqual({first["node_id"], second["node_id"]}, {"http://a:1", "http://b:2"})
        # The just-used node is now less preferred (least-recently-used first).
        third = pool.pick()
        self.assertNotEqual(third["node_id"], second["node_id"])

    def test_single_node_respects_freeze_and_rotate_false_ignores_health(self):
        current = {"now": 1000.0}
        pool = self.make_pool(["http://a:1"], now_fn=lambda: current["now"])
        pool.report("http://a:1", False, "HTTP 429 rate_limited")
        self.assertIsNone(pool.pick())  # single frozen node -> no eligible node
        current["now"] += 61
        self.assertEqual(pool.pick()["node_id"], "http://a:1")  # recovers after cooldown
        non_rotating = self.make_pool(["http://a:1", "http://b:2"], rotate=False, now_fn=lambda: 1000)
        for _ in range(3):
            self.assertEqual(non_rotating.pick()["node_id"], "http://a:1")

    def test_failure_sets_cooldown_and_skips_node(self):
        current = {"now": 1000.0}
        pool = self.make_pool(["http://a:1", "http://b:2"], now_fn=lambda: current["now"])
        pool.report("http://a:1", False, "HTTP 429 rate_limited")
        self.assertEqual(pool.pick()["node_id"], "http://b:2")
        current["now"] = 1061
        # a's cooldown expired; b is now failing as well so a is selectable again.
        pool.report("http://b:2", False, "HTTP 429 rate_limited")
        self.assertEqual(pool.pick()["node_id"], "http://a:1")

    def test_exponential_backoff_caps(self):
        current = {"now": 1000.0}
        pool = self.make_pool(["http://a:1"], now_fn=lambda: current["now"])
        expected = [60, 120, 240, 480, 900, 900]
        observed = []
        for _ in range(len(expected)):
            pool.report("http://a:1", False, "login_banned_user")
            snapshot = pool.snapshot()
            node = snapshot["nodes"][0]
            observed.append(int(node["cooldown_until"] - current["now"]))
            current["now"] += 1
        self.assertEqual(observed, expected)

    def test_light_category_backs_off_slower(self):
        current = {"now": 1000.0}
        pool = self.make_pool(["http://a:1"], now_fn=lambda: current["now"])
        pool.report("http://a:1", False, "timeout")
        first = int(pool.snapshot()["nodes"][0]["cooldown_until"] - current["now"])
        current["now"] += 1
        pool.report("http://a:1", False, "timeout")
        second = int(pool.snapshot()["nodes"][0]["cooldown_until"] - current["now"])
        self.assertEqual(first, 60)
        self.assertEqual(second, 60)

    def test_success_resets_cooldown_and_counter(self):
        current = {"now": 1000.0}
        pool = self.make_pool(["http://a:1"], now_fn=lambda: current["now"])
        pool.report("http://a:1", False, "HTTP 429")
        pool.report("http://a:1", True, "")
        node = pool.snapshot()["nodes"][0]
        self.assertEqual(node["consecutive_failures"], 0)
        self.assertEqual(node["successes"], 1)
        self.assertEqual(node["cooldown_until"], 0.0)

    def test_all_cooling_returns_none(self):
        current = {"now": 1000.0}
        pool = self.make_pool(["http://a:1", "http://b:2"], now_fn=lambda: current["now"])
        pool.report("http://a:1", False, "login_banned_user")
        pool.report("http://b:2", False, "login_banned_user")
        self.assertIsNone(pool.pick())
        self.assertTrue(pool.snapshot()["all_cooling"])

    def test_persistence_roundtrip_and_corruption(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "nodes.json")
            current = {"now": 1000.0}
            pool = self.make_pool(["http://a:1"], state_path=path, now_fn=lambda: current["now"])
            pool.report("http://a:1", False, "HTTP 429")
            self.assertTrue(os.path.exists(path))
            reloaded = self.make_pool(["http://a:1"], state_path=path, now_fn=lambda: current["now"])
            self.assertGreater(reloaded.snapshot()["nodes"][0]["failures"], 0)
            with open(path, "w", encoding="utf-8") as handle:
                handle.write("{broken")
            fresh = self.make_pool(["http://a:1"], state_path=path, now_fn=lambda: current["now"])
            self.assertEqual(fresh.snapshot()["nodes"][0]["failures"], 0)

    def test_reset_clears_health(self):
        pool = self.make_pool(["http://a:1"], now_fn=lambda: 1000)
        pool.report("http://a:1", False, "HTTP 429")
        pool.reset()
        node = pool.snapshot()["nodes"][0]
        self.assertEqual(node["failures"], 0)
        self.assertEqual(node["consecutive_failures"], 0)
        self.assertEqual(pool.snapshot()["categories"], {})


class ThresholdFreezeTests(unittest.TestCase):
    def make_pool(self, proxies=None, threshold=3, now=1000.0):
        return egress.EgressNodePool(
            proxies=proxies or ["http://a:1", "http://b:2"],
            cooldown_base=60, cooldown_max=900,
            failure_threshold=threshold,
            now_fn=lambda: now,
        )

    def test_below_threshold_does_not_freeze(self):
        pool = self.make_pool()
        for _ in range(2):
            pool.report("http://a:1", False, "HTTP 429 rate_limited")
        node = [n for n in pool.snapshot()["nodes"] if n["node_id"] == "http://a:1"][0]
        self.assertEqual(node["consecutive_failures"], 2)
        self.assertEqual(node["cooldown_until"], 0.0)
        self.assertFalse(pool.snapshot()["all_cooling"])
        # Both nodes remain eligible while a is below the threshold.
        self.assertIsNotNone(pool.pick())

    def test_threshold_freezes_and_skips(self):
        pool = self.make_pool()
        for _ in range(3):
            pool.report("http://a:1", False, "HTTP 429 rate_limited")
        node = [n for n in pool.snapshot()["nodes"] if n["node_id"] == "http://a:1"][0]
        self.assertEqual(node["consecutive_failures"], 3)
        self.assertEqual(int(node["cooldown_until"] - 1000.0), 60)
        # a is cooling, so b is the only eligible node.
        self.assertEqual(pool.pick()["node_id"], "http://b:2")

    def test_success_resets_streak(self):
        pool = self.make_pool()
        pool.report("http://a:1", False, "HTTP 429")
        pool.report("http://a:1", False, "HTTP 429")
        pool.report("http://a:1", True, "")
        node = [n for n in pool.snapshot()["nodes"] if n["node_id"] == "http://a:1"][0]
        self.assertEqual(node["consecutive_failures"], 0)
        self.assertEqual(node["cooldown_until"], 0.0)
        # Two failures after the success are still below the threshold.
        pool.report("http://a:1", False, "HTTP 429")
        pool.report("http://a:1", False, "HTTP 429")
        node = [n for n in pool.snapshot()["nodes"] if n["node_id"] == "http://a:1"][0]
        self.assertEqual(node["consecutive_failures"], 2)
        self.assertEqual(node["cooldown_until"], 0.0)
        # The third consecutive failure after the success freezes it again.
        pool.report("http://a:1", False, "HTTP 429")
        node = [n for n in pool.snapshot()["nodes"] if n["node_id"] == "http://a:1"][0]
        self.assertEqual(int(node["cooldown_until"] - 1000.0), 60)

    def test_exponential_backoff_starts_at_threshold(self):
        pool = self.make_pool(proxies=["http://a:1"], threshold=3)
        observed = []
        for _ in range(7):
            pool.report("http://a:1", False, "login_banned_user")
            node = pool.snapshot()["nodes"][0]
            observed.append(int(max(0.0, node["cooldown_until"] - 1000.0)))
        self.assertEqual(observed, [0, 0, 60, 120, 240, 480, 900])

    def test_light_category_backoff_after_threshold(self):
        pool = self.make_pool(proxies=["http://a:1"], threshold=3)
        observed = []
        for _ in range(5):
            pool.report("http://a:1", False, "timeout")
            node = pool.snapshot()["nodes"][0]
            observed.append(int(max(0.0, node["cooldown_until"] - 1000.0)))
        self.assertEqual(observed, [0, 0, 60, 60, 120])

    def test_snapshot_exposes_threshold(self):
        self.assertEqual(self.make_pool().snapshot()["failure_threshold"], 3)


class NonNodeFailureTests(unittest.TestCase):
    def make_pool(self, threshold=3):
        return egress.EgressNodePool(
            proxies=["http://a:1", "http://b:2"],
            cooldown_base=60, cooldown_max=900,
            failure_threshold=threshold, now_fn=lambda: 1000,
        )

    def node(self, pool, node_id="http://a:1"):
        return [n for n in pool.snapshot()["nodes"] if n["node_id"] == node_id][0]

    def test_email_failures_are_recorded_but_do_not_freeze(self):
        pool = self.make_pool()
        for _ in range(10):
            pool.report("http://a:1", False, "EmailNator 取地址失败: HTTP 500")
        node = self.node(pool)
        self.assertEqual(node["failures"], 10)
        self.assertEqual(node["consecutive_failures"], 0)
        self.assertEqual(node["cooldown_until"], 0.0)
        self.assertEqual(node["last_error_category"], egress.CATEGORY_EMAIL_PROVIDER)
        self.assertIsNotNone(pool.pick())

    def test_email_failure_does_not_reset_node_streak(self):
        pool = self.make_pool()
        pool.report("http://a:1", False, "HTTP 429 rate_limited")
        pool.report("http://a:1", False, "HTTP 429 rate_limited")
        pool.report("http://a:1", False, "EmailNator 未收到 Notion 验证码")
        self.assertEqual(self.node(pool)["consecutive_failures"], 2)
        pool.report("http://a:1", False, "HTTP 429 rate_limited")
        node = self.node(pool)
        self.assertEqual(node["consecutive_failures"], 3)
        self.assertEqual(int(node["cooldown_until"] - 1000.0), 60)

    def test_success_still_resets_after_email_failures(self):
        pool = self.make_pool()
        pool.report("http://a:1", False, "HTTP 429 rate_limited")
        pool.report("http://a:1", False, "EmailNator 取地址失败")
        pool.report("http://a:1", True, "")
        self.assertEqual(self.node(pool)["consecutive_failures"], 0)
        self.assertEqual(self.node(pool)["cooldown_until"], 0.0)


class FakeClash:
    def __init__(self, fail_nodes=()):
        self.switched = []
        self.fail_nodes = set(fail_nodes)
        self.restore_target = "original"
        self.dry_run = False

    def switch(self, node):
        if node in self.fail_nodes:
            raise RuntimeError("switch failed")
        self.switched.append(node)

    def restore(self):
        self.switched.append(self.restore_target)
        return True


class ClashModePoolTests(unittest.TestCase):
    def make_pool(self, controller, now=1000):
        return egress.EgressNodePool(
            mode="clash",
            clash_nodes=["英国A", "英国B"],
            clash_controller=controller,
            clash_proxy_url="http://127.0.0.1:7897",
            now_fn=lambda: now,
        )

    def test_switches_selected_node_and_returns_proxy(self):
        controller = FakeClash()
        pool = self.make_pool(controller)
        picked = pool.pick()
        self.assertEqual(picked["proxy"], "http://127.0.0.1:7897")
        self.assertIn(picked["node_id"], controller.switched)

    def test_switch_failure_penalizes_and_falls_back(self):
        controller = FakeClash(fail_nodes={"英国A"})
        pool = self.make_pool(controller)
        picked = pool.pick()
        self.assertIsNotNone(picked)
        snapshot = pool.snapshot()
        failed = [node for node in snapshot["nodes"] if node["node_id"] == "英国A"][0]
        self.assertGreater(failed["failures"], 0)

    def test_maybe_restore_after_idle(self):
        controller = FakeClash()
        pool = egress.EgressNodePool(
            mode="clash", clash_nodes=["英国A"], clash_controller=controller,
            clash_proxy_url="http://127.0.0.1:7897", now_fn=lambda: 1000,
        )
        pool.pick()
        self.assertFalse(pool.maybe_restore(idle_seconds=60))
        pool._last_pick_at = 1000 - 61
        self.assertTrue(pool.maybe_restore(idle_seconds=60))
        self.assertIn("original", controller.switched)


class FakeTransport:
    def __init__(self):
        self.calls = []
        self.current_node = "original"

    def call(self, method, path, body=None):
        self.calls.append((method, path, body))
        if method == "PUT":
            self.current_node = (body or {}).get("name", self.current_node)
            return {}
        return {"proxies": {"良心云": {"now": self.current_node}}}


class ClashEgressTests(unittest.TestCase):
    def test_switch_remembers_and_restores(self):
        from app.core.clash_egress import ClashEgress

        transport = FakeTransport()
        controller = ClashEgress(transport, "良心云", settle_seconds=0)
        controller.switch("英国A")
        self.assertEqual(controller.restore_target, "original")
        self.assertEqual(transport.current_node, "英国A")
        self.assertTrue(controller.restore())
        self.assertEqual(transport.current_node, "original")

    def test_dry_run_does_not_call_transport(self):
        from app.core.clash_egress import ClashEgress

        transport = FakeTransport()
        controller = ClashEgress(transport, "良心云", dry_run=True, settle_seconds=0)
        controller.switch("英国A")
        self.assertNotIn("PUT", [call[0] for call in transport.calls])
        self.assertTrue(controller.restore())


class PauseTests(unittest.TestCase):
    def test_threshold_pauses_with_deadline(self):
        result = egress.compute_pause(10, {}, now=1000, pause_after=10, pause_seconds=300)
        self.assertTrue(result["should_pause"])
        self.assertEqual(result["paused_until"], 1300)
        self.assertEqual(result["reason"], "consecutive_failures")

    def test_below_threshold_does_not_pause(self):
        self.assertFalse(egress.compute_pause(9, {}, now=1000, pause_after=10)["should_pause"])

    def test_all_nodes_cooling_pauses_immediately(self):
        result = egress.compute_pause(0, {egress.CATEGORY_ALL_COOLING: 1}, now=1000)
        self.assertTrue(result["should_pause"])
        self.assertEqual(result["reason"], egress.CATEGORY_ALL_COOLING)

    def test_hard_disable_mode(self):
        result = egress.compute_pause(10, {}, now=1000, hard_disable=True)
        self.assertFalse(result["should_pause"])
        self.assertTrue(result["hard_disable"])


if __name__ == "__main__":
    unittest.main()
