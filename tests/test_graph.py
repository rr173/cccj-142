"""Graph validation and builtin declared-spec tests."""

import unittest

from iterflow import Graph, GraphError, Runtime
from iterflow.builtins import build_filter_fn, build_map_fn


class GraphValidationTest(unittest.TestCase):
    def test_legal_cycle_with_feedback_is_accepted(self):
        g = Graph("legal")
        g.add_source("src")
        g.add_map("a", spec={"kind": "identity"})
        g.add_map("b", spec={"kind": "identity"})
        g.add_sink("sink")
        g.add_edge("src", "a")
        g.add_edge("a", "b")
        g.add_edge("b", "sink")
        g.add_feedback_edge("b", "a")  # cycle b->a goes through feedback
        rt = Runtime(g)  # must not raise
        rt.shutdown()

    def test_cycle_without_feedback_is_rejected(self):
        g = Graph("illegal")
        g.add_source("src")
        g.add_map("a", spec={"kind": "identity"})
        g.add_map("b", spec={"kind": "identity"})
        g.add_sink("sink")
        g.add_edge("src", "a")
        g.add_edge("a", "b")
        g.add_edge("b", "a")  # plain-edge cycle a<->b
        g.add_edge("b", "sink")
        with self.assertRaises(GraphError):
            Runtime(g)

    def test_plain_self_loop_is_rejected(self):
        g = Graph("selfloop")
        g.add_source("src")
        g.add_map("a", spec={"kind": "identity"})
        g.add_sink("sink")
        g.add_edge("src", "a")
        g.add_edge("a", "a")
        g.add_edge("a", "sink")
        with self.assertRaises(GraphError):
            Runtime(g)

    def test_feedback_self_loop_is_accepted(self):
        g = Graph("selfloop-fb")
        g.add_source("src")
        g.add_map("a", spec={"kind": "identity"})
        g.add_sink("sink")
        g.add_edge("src", "a")
        g.add_feedback_edge("a", "a")
        g.add_edge("a", "sink")
        rt = Runtime(g)
        rt.shutdown()

    def test_structural_validation(self):
        g = Graph("bad1")
        g.add_source("src")
        g.add_sink("sink")
        with self.assertRaises(GraphError):  # source without outgoing edge
            Runtime(g)

        g = Graph("bad2")
        g.add_source("src")
        g.add_sink("sink")
        with self.assertRaises(GraphError):  # sink without incoming edge
            Runtime(g)

        g = Graph("bad3")
        g.add_source("src")
        g.add_sink("sink")
        with self.assertRaises(GraphError):  # capacity must be >= 1
            g.add_edge("src", "sink", capacity=0)


class BuiltinSpecTest(unittest.TestCase):
    def test_dict_lookup(self):
        fn, cap = build_map_fn({"kind": "dict_lookup",
                                "dict": {"a": "b"}, "default": "self"})
        self.assertEqual(cap, 1)
        self.assertEqual(fn("a"), ["b"])
        self.assertEqual(fn("zzz"), ["zzz"])

        fn, cap = build_map_fn({"kind": "dict_lookup",
                                "dict": {"a": "b"}, "default": "drop"})
        self.assertEqual(fn("a"), ["b"])
        self.assertEqual(fn("zzz"), [])  # no-output transform

    def test_duplicate(self):
        fn, cap = build_map_fn({"kind": "duplicate", "count": 3})
        self.assertEqual(cap, 3)
        self.assertEqual(fn("x"), ["x", "x", "x"])

    def test_identity(self):
        fn, cap = build_map_fn({"kind": "identity"})
        self.assertEqual((fn("v"), cap), (["v"], 1))

    def test_filters(self):
        self.assertTrue(build_filter_fn({"kind": "all"})("x"))
        self.assertFalse(build_filter_fn({"kind": "none"})("x"))
        self.assertTrue(build_filter_fn({"kind": "in", "values": ["a"]})("a"))
        self.assertFalse(build_filter_fn({"kind": "in", "values": ["a"]})("b"))
        self.assertTrue(build_filter_fn({"kind": "not_in", "values": ["a"]})("b"))
        self.assertTrue(build_filter_fn({"kind": "equals", "value": 7})(7))
        self.assertFalse(build_filter_fn({"kind": "equals", "value": 7})(8))

    def test_unknown_spec_kind_rejected(self):
        with self.assertRaises(GraphError):
            build_map_fn({"kind": "eval_this_string"})
        with self.assertRaises(GraphError):
            build_filter_fn({"kind": "lambda_escape_hatch"})


if __name__ == "__main__":
    unittest.main()
