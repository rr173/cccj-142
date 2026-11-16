"""Core runtime tests: example trace, barrier, backpressure, spill, epochs."""

import threading
import unittest

from iterflow import (
    DuplicateMessageError,
    Graph,
    ManualConsumer,
    Runtime,
    SourceStateError,
    UndeclaredEpochError,
)

from util import build_normalize_graph, wait_for


class ExampleTraceTest(unittest.TestCase):
    """Reproduces the hand-written trace in examples/expected_trace.md."""

    EXPECTED_SINK = {
        ("A>m[0]~m->f_loop>m[0]~m->f_fin>dup[0]", 0, 1, "c"),
        ("A>m[0]~m->f_loop>m[0]~m->f_fin>dup[1]", 0, 1, "c"),
        ("Z>m[0]~m->f_fin>dup[0]", 0, 0, "z"),
        ("Z>m[0]~m->f_fin>dup[1]", 0, 0, "z"),
    }
    EXPECTED_EDGE_SENT = {
        "src->u": 2, "u->m": 3, "m->f_fin": 3, "m->f_loop": 3,
        "f_fin->dup": 2, "f_loop->u": 1, "dup->sink": 4,
    }
    EXPECTED_DERIVED = 16

    def test_example_matches_handwritten_trace(self):
        rt = Runtime(build_normalize_graph(), max_round=16)
        rt.declare_epoch(0)
        rt.start()
        h0 = rt.subscribe_round(0, 0)
        h1 = rt.subscribe_round(0, 1)
        he = rt.subscribe_epoch(0)
        src = rt.source("src")
        src.send(0, "a", msg_id="A")
        src.send(0, "z", msg_id="Z")
        src.close_epoch(0)

        # per-round notifications arrive in logical order
        self.assertEqual(h0.wait(5), {"epoch": 0, "round": 0})
        self.assertEqual(h1.wait(5), {"epoch": 0, "round": 1})
        self.assertEqual(he.wait(5), {"epoch": 0})
        result = rt.wait(5)
        self.assertEqual(result.state, "FINISHED")

        # sink results: identities, rounds, values, multiplicity
        got = {(r["id"], r["epoch"], r["round"], r["value"])
               for r in rt.collected("sink")}
        self.assertEqual(got, self.EXPECTED_SINK)

        # completion order as persisted in the SQLite progress log
        kinds = [(e["kind"], e["epoch"], e["round"])
                 for e in rt.progress_events()]
        self.assertEqual(kinds, [
            ("round_complete", 0, 0),
            ("round_complete", 0, 1),
            ("epoch_complete", 0, None),
            ("run_finished", None, None),
        ])

        # runtime facts: per-edge sends/receives, peaks, derived count
        for eid, n in self.EXPECTED_EDGE_SENT.items():
            st = result.stats["edges"][eid]
            self.assertEqual(st["sent"], n, eid)
            self.assertEqual(st["received"], n, eid)
            self.assertLessEqual(st["peak_buffered"], st["capacity"], eid)
        self.assertEqual(result.derived, self.EXPECTED_DERIVED)
        self.assertTrue(rt.frontier(0).complete)
        rt.shutdown()


class BarrierAndAckPauseTest(unittest.TestCase):
    """A feedback message stopped before sending still blocks (epoch, 1);
    a paused sink ack still blocks the epoch completion."""

    def test_barrier_then_ack_pause(self):
        rt = Runtime(build_normalize_graph(capacity=1), max_round=16)
        rt.declare_epoch(0)
        consumer = ManualConsumer()
        rt.attach_consumer("sink", consumer)
        rt.start()
        fb = rt.feedback_edge("f_loop->u")
        fb.gate = threading.Event()  # barrier: hold the feedback send

        h0 = rt.subscribe_round(0, 0)
        h1 = rt.subscribe_round(0, 1)
        he = rt.subscribe_epoch(0)
        rt.source("src").send(0, "a", msg_id="A")
        rt.source("src").close_epoch(0)

        # The feedback message is assigned (epoch 0, round 1) and stopped
        # before sending; every queue is empty at this point.
        self.assertTrue(fb.gate_arrived.wait(5))
        # round 0 completes once every other obligation is released
        h0.wait(5)
        for eid, st in rt.stats()["edges"].items():
            self.assertEqual(st["current_buffered"], 0, eid)
            self.assertEqual(st["current_spilled"], 0, eid)
        # ... but (epoch, 1) and the whole epoch must not complete early
        self.assertFalse(h1.done())
        self.assertFalse(he.done())
        fr = rt.frontier(0)
        self.assertFalse(fr.complete)
        self.assertEqual(fr.frontier_round, 1)
        self.assertEqual(fr.categories(), {"feedback_pending"})
        (fb_reason,) = [r for r in fr.reasons
                        if r["category"] == "feedback_pending"]
        self.assertEqual(fb_reason["edge"], "f_loop->u")
        self.assertEqual(fb_reason["held"], 1)
        self.assertEqual(fb_reason["rounds"], {1: 1})

        fb.gate.set()  # release the barrier: b -> c flows to the sink

        # Pause sink consumption acks: the epoch must not complete either.
        rec1 = consumer.next(5)
        self.assertEqual((rec1.value, rec1.epoch, rec1.round), ("c", 0, 1))
        # wait until the dropped broadcast copy of c (towards f_loop) is
        # retired: exactly the two dup copies remain outstanding
        self.assertTrue(wait_for(
            lambda: sum(r["count"] for r in rt.frontier(0).reasons) == 2, 5))
        self.assertTrue(wait_for(
            lambda: "sink_unacked" in rt.frontier(0).categories(), 5))
        fr = rt.frontier(0)
        self.assertFalse(fr.complete)
        self.assertFalse(h1.done())
        self.assertFalse(he.done())

        rec1.ack()
        rec2 = consumer.next(5)
        self.assertEqual((rec2.value, rec2.epoch, rec2.round), ("c", 0, 1))
        self.assertTrue(wait_for(
            lambda: rt.frontier(0).categories() == {"sink_unacked"}, 5))
        self.assertFalse(h1.done())
        self.assertFalse(he.done())

        rec2.ack()
        self.assertEqual(h1.wait(5), {"epoch": 0, "round": 1})
        self.assertEqual(he.wait(5), {"epoch": 0})
        self.assertEqual(rt.wait(5).state, "FINISHED")
        rt.shutdown()


class CapacityOneTest(unittest.TestCase):
    def test_capacity_one_fork_join_feedback_no_deadlock(self):
        rt = Runtime(build_normalize_graph(capacity=1), max_round=16)
        rt.declare_epoch(0)
        rt.start()
        src = rt.source("src")
        src.send(0, "a", msg_id="A")
        src.send(0, "z", msg_id="Z")
        src.close_epoch(0)
        result = rt.wait(15)
        self.assertEqual(result.state, "FINISHED")
        bag = sorted((r["value"], r["round"]) for r in rt.collected("sink"))
        self.assertEqual(bag, [("c", 1), ("c", 1), ("z", 0), ("z", 0)])
        for eid, st in result.stats["edges"].items():
            self.assertLessEqual(st["peak_buffered"], 1, eid)
            self.assertEqual(st["sent"], st["received"], eid)
        rt.shutdown()


class FeedbackSpillTest(unittest.TestCase):
    def test_feedback_bursts_spill_to_sqlite(self):
        # 30 looped records against capacity-1 edges with a paused sink:
        # the feedback edge's memory buffer (1) cannot hold the burst, the
        # rest must go to the SQLite spill mailbox.  No deadlock, bounded
        # memory, and the run completes once the sink drains.
        rt = Runtime(build_normalize_graph(capacity=1), max_round=16)
        rt.declare_epoch(0)
        consumer = ManualConsumer()
        rt.attach_consumer("sink", consumer)
        rt.start()
        src = rt.source("src")
        for i in range(30):
            src.send(0, "a", msg_id=f"A{i}")
        src.close_epoch(0)

        first = consumer.next(5)  # hold the sink: upstream must stop controllably
        self.assertEqual(first.value, "c")
        self.assertTrue(wait_for(
            lambda: rt.stats()["edges"]["f_loop->u"]["spilled"] >= 1, 10))
        self.assertFalse(rt.subscribe_epoch(0).done())

        # drain the sink: 30 records x 2 copies
        first.ack()
        seen = 1
        while seen < 60:
            consumer.next(10).ack()
            seen += 1
        self.assertEqual(rt.wait_epoch(0, 10), {"epoch": 0})
        result = rt.wait(10)
        self.assertEqual(result.state, "FINISHED")
        fb = result.stats["edges"]["f_loop->u"]
        self.assertGreaterEqual(fb["spilled"], 1)
        self.assertEqual(fb["spilled"], fb["spill_read"])
        self.assertLessEqual(fb["peak_buffered"], 1)
        self.assertEqual(fb["sent"], fb["received"])
        self.assertEqual(result.stats["nodes"]["sink"]["processed"], 60)
        rt.shutdown()


class InterleavedEpochsTest(unittest.TestCase):
    def test_delayed_source_close_does_not_mis_settle(self):
        g = Graph("interleaved")
        g.add_source("s1")
        g.add_source("s2")
        g.add_union("u")
        g.add_map("m", spec={"kind": "identity"})
        g.add_sink("sink")
        g.add_edge("s1", "u")
        g.add_edge("s2", "u")
        g.add_edge("u", "m")
        g.add_edge("m", "sink")
        rt = Runtime(g)
        rt.declare_epoch(7)
        rt.declare_epoch(8)
        rt.start()
        s1, s2 = rt.source("s1"), rt.source("s2")
        h7r, h7e = rt.subscribe_round(7, 0), rt.subscribe_epoch(7)
        h8r, h8e = rt.subscribe_round(8, 0), rt.subscribe_epoch(8)

        s1.send(7, "a")
        s2.send(8, "x")
        s1.send(8, "b")
        s2.send(7, "y")
        s2.close_epoch(7)
        s2.close_epoch(8)
        s1.close_epoch(8)  # s1 keeps epoch 7 open

        # duplicate close and send-after-close are rejected while running
        with self.assertRaises(SourceStateError):
            s2.send(7, "nope")
        with self.assertRaises(SourceStateError):
            s2.close_epoch(7)

        # the new epoch fully converges while the old one stays unsettled
        self.assertEqual(h8r.wait(5), {"epoch": 8, "round": 0})
        self.assertEqual(h8e.wait(5), {"epoch": 8})
        # epoch 7's records drain to the sink, yet it must not settle:
        # only the unclosed source remains as the frontier reason
        self.assertTrue(wait_for(
            lambda: rt.frontier(7).categories() == {"source_open"}, 5))
        self.assertFalse(h7r.done())
        self.assertFalse(h7e.done())
        fr = rt.frontier(7)
        self.assertFalse(fr.complete)
        self.assertEqual(fr.frontier_round, 0)
        self.assertEqual(fr.reasons[0]["source"], "s1")
        self.assertTrue(rt.frontier(8).complete)

        # late but legitimate source data on the still-open epoch
        s1.send(7, "late")
        self.assertFalse(h7r.done())
        s1.close_epoch(7)
        self.assertEqual(h7r.wait(5), {"epoch": 7, "round": 0})
        self.assertEqual(h7e.wait(5), {"epoch": 7})
        self.assertEqual(rt.wait(5).state, "FINISHED")
        values = sorted(r["value"] for r in rt.collected("sink"))
        self.assertEqual(values, ["a", "b", "late", "x", "y"])

        # sends after close and duplicate closes are still rejected afterwards
        with self.assertRaises(SourceStateError):
            s1.send(7, "nope")
        with self.assertRaises(SourceStateError):
            s1.close_epoch(7)
        rt.shutdown()


class EmptyEpochTest(unittest.TestCase):
    def test_empty_epoch_completes(self):
        rt = Runtime(build_normalize_graph(), max_round=16)
        rt.declare_epoch(0)
        rt.start()
        h0 = rt.subscribe_round(0, 0)
        he = rt.subscribe_epoch(0)
        rt.source("src").close_epoch(0)  # no records at all
        self.assertEqual(h0.wait(5), {"epoch": 0, "round": 0})
        self.assertEqual(he.wait(5), {"epoch": 0})
        self.assertEqual(rt.wait(5).state, "FINISHED")
        self.assertEqual(rt.collected("sink"), [])
        rt.shutdown()

    def test_sourceless_graph_epoch_completes_at_start(self):
        g = Graph("sourceless")
        g.add_map("m", spec={"kind": "identity"})  # no sources at all
        rt = Runtime(g)
        rt.declare_epoch(0)
        rt.start()
        self.assertEqual(rt.wait_epoch(0, 5), {"epoch": 0})
        self.assertEqual(rt.wait(5).state, "FINISHED")
        rt.shutdown()


class NoOutputTransformTest(unittest.TestCase):
    def test_zero_output_map_still_converges(self):
        g = Graph("dropper")
        g.add_source("src")
        g.add_map("m", spec={"kind": "dict_lookup",
                             "dict": {"k": "v"}, "default": "drop"})
        g.add_sink("sink")
        g.add_edge("src", "m")
        g.add_edge("m", "sink")
        rt = Runtime(g)
        rt.declare_epoch(0)
        rt.start()
        he = rt.subscribe_epoch(0)
        rt.source("src").send(0, "k")   # -> ["v"]
        rt.source("src").send(0, "x")   # -> [] (dropped)
        rt.source("src").close_epoch(0)
        self.assertEqual(he.wait(5), {"epoch": 0})
        self.assertEqual(rt.wait(5).state, "FINISHED")
        self.assertEqual([r["value"] for r in rt.collected("sink")], ["v"])
        self.assertEqual(rt.stats()["nodes"]["m"]["dropped"], 1)
        rt.shutdown()


class UnionMultiplicityTest(unittest.TestCase):
    def test_bag_multiplicity_preserved_no_dedup(self):
        rt = Runtime(build_normalize_graph(), max_round=16)
        rt.declare_epoch(0)
        rt.start()
        src = rt.source("src")
        src.send(0, "z", msg_id="Z1")
        src.send(0, "z", msg_id="Z2")  # same business value twice
        src.close_epoch(0)
        rt.wait_epoch(0, 5)
        self.assertEqual(rt.wait(5).state, "FINISHED")
        values = sorted(r["value"] for r in rt.collected("sink"))
        self.assertEqual(values, ["z", "z", "z", "z"])  # 2 in -> 4 out
        rt.shutdown()


class FrontierTransformingTest(unittest.TestCase):
    def test_in_flight_transform_reported(self):
        entered = threading.Event()
        release = threading.Event()

        def slow_fn(v):
            entered.set()
            release.wait(5)
            return [v]

        g = Graph("slowmap")
        g.add_source("src")
        g.add_map("m", fn=slow_fn, max_outputs=1)
        g.add_sink("sink")
        g.add_edge("src", "m")
        g.add_edge("m", "sink")
        rt = Runtime(g)
        rt.declare_epoch(0)
        rt.start()
        he = rt.subscribe_epoch(0)
        rt.source("src").send(0, "x")
        rt.source("src").close_epoch(0)
        self.assertTrue(entered.wait(5))
        fr = rt.frontier(0)
        self.assertEqual(fr.categories(), {"transforming"})
        self.assertEqual(fr.reasons[0]["node"], "m")
        self.assertFalse(he.done())
        release.set()
        self.assertEqual(he.wait(5), {"epoch": 0})
        rt.shutdown()


class SourceStateTest(unittest.TestCase):
    def test_source_protocol_violations(self):
        rt = Runtime(build_normalize_graph(), max_round=16)
        rt.declare_epoch(0)
        rt.start()
        src = rt.source("src")
        with self.assertRaises(UndeclaredEpochError):
            src.send(9, "x")
        src.send(0, "z", msg_id="D1")
        with self.assertRaises(DuplicateMessageError):
            src.send(0, "z", msg_id="D1")
        src.close_epoch(0)
        with self.assertRaises(SourceStateError):
            src.send(0, "z")  # send after close
        with self.assertRaises(SourceStateError):
            src.close_epoch(0)  # duplicate close declaration
        with self.assertRaises(SourceStateError):
            rt.declare_epoch(1)  # no new epochs during the run
        self.assertEqual(rt.wait(5).state, "FINISHED")
        with self.assertRaises(SourceStateError):
            src.send(0, "z")  # epoch closed (specific error wins over run-over)
        rt.shutdown()


if __name__ == "__main__":
    unittest.main()
