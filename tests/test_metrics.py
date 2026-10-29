"""Tests for the dependency-free metrics registry."""
import os
import sys
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.metrics import MetricsRegistry


class CounterTests(unittest.TestCase):
    def test_counter_labels_are_independent(self):
        registry = MetricsRegistry()
        registry.inc("requests_total", model="a")
        registry.inc("requests_total", model="a")
        registry.inc("requests_total", model="b", status="error")
        snapshot = registry.snapshot()
        values = {(item["name"], tuple(sorted(item["labels"].items()))): item["value"] for item in snapshot["counters"]}
        self.assertEqual(values[("requests_total", (("model", "a"),))], 2.0)
        self.assertEqual(values[("requests_total", (("model", "b"), ("status", "error")))], 1.0)

    def test_gauge_overwrites(self):
        registry = MetricsRegistry()
        registry.set_gauge("accounts_active", 5)
        registry.set_gauge("accounts_active", 3)
        snapshot = registry.snapshot()
        self.assertEqual(snapshot["gauges"][0]["value"], 3.0)


class HistogramTests(unittest.TestCase):
    def test_bucket_counts_are_cumulative_on_render(self):
        registry = MetricsRegistry(buckets=(1, 2, 5))
        registry.observe("latency_seconds", 0.5)
        registry.observe("latency_seconds", 1.5)
        registry.observe("latency_seconds", 3.0)
        registry.observe("latency_seconds", 9.0)
        text = registry.prometheus_text()
        self.assertIn('latency_seconds_bucket{le="1"} 1', text)
        self.assertIn('latency_seconds_bucket{le="2"} 2', text)
        self.assertIn('latency_seconds_bucket{le="5"} 3', text)
        self.assertIn('latency_seconds_bucket{le="+Inf"} 4', text)
        self.assertIn("latency_seconds_count 4", text)
        self.assertIn("latency_seconds_sum 14", text)

    def test_prometheus_header_and_labels(self):
        registry = MetricsRegistry()
        registry.describe("requests_total", "Total requests")
        registry.inc("requests_total", status="success", model="x")
        text = registry.prometheus_text()
        self.assertIn("# HELP requests_total Total requests", text)
        self.assertIn("# TYPE requests_total counter", text)
        self.assertIn('requests_total{model="x",status="success"} 1', text)

    def test_extra_gauges_are_rendered(self):
        registry = MetricsRegistry()
        text = registry.prometheus_text(extra_gauges=[("account_status", 2, {"status": "active"})])
        self.assertIn("# TYPE account_status gauge", text)
        self.assertIn('account_status{status="active"} 2', text)

    def test_extra_gauges_emit_type_once(self):
        registry = MetricsRegistry()
        text = registry.prometheus_text(extra_gauges=[
            ("account_status", 2, {"status": "active"}),
            ("account_status", 1, {"status": "disabled"}),
        ])
        self.assertEqual(text.count("# TYPE account_status gauge"), 1)
        self.assertIn('account_status{status="disabled"} 1', text)

    def test_concurrent_increments_are_lossless(self):
        registry = MetricsRegistry()

        def worker():
            for _ in range(500):
                registry.inc("hits_total")

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        snapshot = registry.snapshot()
        self.assertEqual(snapshot["counters"][0]["value"], 2000.0)

    def test_label_values_are_escaped(self):
        registry = MetricsRegistry()
        registry.inc("demo_total", model='a"b\nc\\d')
        text = registry.prometheus_text()
        self.assertIn('model="a\\"b\\nc\\\\d"', text)


if __name__ == "__main__":
    unittest.main()
