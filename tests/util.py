"""Shared helpers for the iterflow test suite."""

import threading
import time

from iterflow import Graph


def wait_for(cond, timeout=5.0, interval=0.01):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(interval)
    return False


def build_normalize_graph(capacity=None):
    """The example graph from examples/normalize.graph.json.

    src -> u -> m -> {f_fin, f_loop}; f_fin -> dup -> sink;
    f_loop -feedback-> u.  Dictionary a->b, b->c; c and z are final.
    """
    g = Graph("normalize")
    g.add_source("src")
    g.add_union("u")
    g.add_map("m", spec={"kind": "dict_lookup",
                         "dict": {"a": "b", "b": "c"}, "default": "self"})
    g.add_filter("f_fin", spec={"kind": "not_in", "values": ["a", "b"]})
    g.add_filter("f_loop", spec={"kind": "in", "values": ["a", "b"]})
    g.add_map("dup", spec={"kind": "duplicate", "count": 2})
    g.add_sink("sink")
    g.add_edge("src", "u", capacity=capacity)
    g.add_edge("u", "m", capacity=capacity)
    g.add_edge("m", "f_fin", capacity=capacity)
    g.add_edge("m", "f_loop", capacity=capacity)
    g.add_edge("f_fin", "dup", capacity=capacity)
    g.add_edge("dup", "sink", capacity=capacity)
    g.add_feedback_edge("f_loop", "u", capacity=capacity)
    return g


def build_loop_graph():
    """Infinite-loop graph: src -> u -> m(identity) -> f(all) -> {sink, fb->u}."""
    g = Graph("loop")
    g.add_source("src")
    g.add_union("u")
    g.add_map("m", spec={"kind": "identity"})
    g.add_filter("f", spec={"kind": "all"})
    g.add_sink("sink")
    g.add_edge("src", "u")
    g.add_edge("u", "m")
    g.add_edge("m", "f")
    g.add_edge("f", "sink")
    g.add_feedback_edge("f", "u")
    return g


def close_quiet(source_handle, epoch):
    """Close an epoch, tolerating the run having already stopped."""
    from iterflow import RunStoppedError

    try:
        source_handle.close_epoch(epoch)
    except RunStoppedError:
        pass
