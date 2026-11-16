"""Failure-path tests: limits, transform errors, sink rejection, cancel."""

import threading
import unittest

from iterflow import (
    Graph,
    ManualConsumer,
    RunCancelledError,
    RunFailedError,
    Runtime,
)

from util import build_loop_graph, build_normalize_graph, close_quiet, wait_for


class RoundLimitTest(unittest.TestCase):
    def test_round_limit_is_failure_not_convergence(self):
        rt = Runtime(build_loop_graph(), max_round=3)
        rt.declare_epoch(0)
        rt.start()
        he = rt.subscribe_epoch(0)
        rt.source("src").send(0, "x", msg_id="X")
        close_quiet(rt.source("src"), 0)
        result = rt.wait(10)
        self.assertEqual(result.state, "FAILED")
        self.assertEqual(result.reason_kind, "round_limit")
        with self.assertRaises(RunFailedError):
            he.wait(5)
        kinds = [e["kind"] for e in rt.progress_events()]
        self.assertNotIn("epoch_complete", kinds)
        # sink consumed some copies (rounds 0..3); nothing is rolled back
        self.assertGreater(len(rt.collected("sink")), 0)
        self.assertGreater(result.derived, 0)
        rt.shutdown()


class DerivedLimitTest(unittest.TestCase):
    def test_derived_limit_is_failure(self):
        rt = Runtime(build_loop_graph(), max_derived=5)
        rt.declare_epoch(0)
        rt.start()
        rt.source("src").send(0, "x", msg_id="X")
        close_quiet(rt.source("src"), 0)
        result = rt.wait(10)
        self.assertEqual(result.state, "FAILED")
        self.assertEqual(result.reason_kind, "derived_limit")
        self.assertLessEqual(result.derived, 5)
        rt.shutdown()


class OutputCapTest(unittest.TestCase):
    def test_map_output_cap_enforced(self):
        g = Graph("cap")
        g.add_source("src")
        g.add_map("m", fn=lambda v: [v, v, v], max_outputs=2)
        g.add_sink("sink")
        g.add_edge("src", "m")
        g.add_edge("m", "sink")
        rt = Runtime(g)
        rt.declare_epoch(0)
        rt.start()
        rt.source("src").send(0, "x")
        close_quiet(rt.source("src"), 0)
        result = rt.wait(10)
        self.assertEqual(result.state, "FAILED")
        self.assertEqual(result.reason_kind, "output_cap")
        rt.shutdown()


class TransformErrorTest(unittest.TestCase):
    def test_map_raises_stops_graph_and_keeps_consumed_data(self):
        def fn(v):
            if v == "bad":
                raise ValueError("boom")
            return [v]

        g = Graph("raiser")
        g.add_source("src")
        g.add_map("m", fn=fn, max_outputs=1)
        g.add_sink("sink")
        g.add_edge("src", "m")
        g.add_edge("m", "sink")
        rt = Runtime(g)
        rt.declare_epoch(0)
        rt.start()
        he = rt.subscribe_epoch(0)
        rt.source("src").send(0, "good")
        self.assertTrue(wait_for(lambda: len(rt.collected("sink")) == 1, 5))
        rt.source("src").send(0, "bad")
        close_quiet(rt.source("src"), 0)
        result = rt.wait(10)
        self.assertEqual(result.state, "FAILED")
        self.assertEqual(result.reason_kind, "transform_error")
        self.assertIn("boom", result.reason)
        # already-consumed data is not revoked; failure state is visible
        self.assertEqual([r["value"] for r in rt.collected("sink")], ["good"])
        with self.assertRaises(RunFailedError):
            he.wait(5)
        self.assertEqual(rt.stats()["state"], "FAILED")
        rt.shutdown()


class SinkRejectTest(unittest.TestCase):
    def test_sink_rejection_fails_the_run(self):
        def consumer(rec):
            if rec.value == "no":
                rec.reject("refused")
            else:
                rec.ack()

        g = Graph("rejector")
        g.add_source("src")
        g.add_map("m", spec={"kind": "identity"})
        g.add_sink("sink")
        g.add_edge("src", "m")
        g.add_edge("m", "sink")
        rt = Runtime(g)
        rt.declare_epoch(0)
        rt.attach_consumer("sink", consumer)
        rt.start()
        he = rt.subscribe_epoch(0)
        rt.source("src").send(0, "ok")   # delivered and acked first (FIFO)
        rt.source("src").send(0, "no")   # rejected -> whole run fails
        close_quiet(rt.source("src"), 0)
        result = rt.wait(10)
        self.assertEqual(result.state, "FAILED")
        self.assertEqual(result.reason_kind, "sink_rejected")
        with self.assertRaises(RunFailedError):
            he.wait(5)
        rt.shutdown()


class SlowSinkCancelTest(unittest.TestCase):
    def test_cancel_releases_waiters_and_exposes_counts(self):
        g = Graph("slowsink")
        g.add_source("src")
        g.add_map("m", spec={"kind": "identity"})
        g.add_sink("sink")
        g.add_edge("src", "m")
        g.add_edge("m", "sink")
        rt = Runtime(g)
        rt.declare_epoch(0)
        rt.declare_epoch(1)  # declared but never used
        consumer = ManualConsumer()
        rt.attach_consumer("sink", consumer)
        rt.start()
        he = rt.subscribe_epoch(0)
        hr = rt.subscribe_round(0, 0)
        src = rt.source("src")
        for i in range(3):
            src.send(0, f"v{i}")
        src.close_epoch(0)

        rec = consumer.next(5)  # delivered; ack paused
        self.assertEqual(rec.value, "v0")
        # the remaining records pile up in the bounded queues (backpressure)
        self.assertTrue(wait_for(
            lambda: "sink_unacked" in rt.frontier(0).categories()
            and "queued" in rt.frontier(0).categories(), 5))
        self.assertFalse(he.done())
        self.assertFalse(hr.done())

        rt.cancel()
        result = rt.wait(10)
        self.assertEqual(result.state, "CANCELLED")
        with self.assertRaises(RunCancelledError):
            he.wait(5)
        with self.assertRaises(RunCancelledError):
            hr.wait(5)
        # no completion was ever notified; counts remain visible
        kinds = [e["kind"] for e in rt.progress_events()]
        self.assertNotIn("epoch_complete", kinds)
        self.assertIn("sink_unacked", rt.frontier(0).categories())
        rec.ack()  # late ack after cancel: recorded, but fires nothing
        self.assertNotIn("epoch_complete",
                         [e["kind"] for e in rt.progress_events()])
        # sends into a stopped run are rejected
        from iterflow import RunStoppedError
        with self.assertRaises(RunStoppedError):
            rt.source("src").send(1, "x")
        rt.shutdown()


if __name__ == "__main__":
    unittest.main()
