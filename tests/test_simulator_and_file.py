"""Independent-simulator cross-checks, graph-file entry, CLI, and the
persisted SQLite progress record."""

import os
import sqlite3
import tempfile
import unittest

from iterflow import Runtime, load_graph, simulate
from iterflow.cli import main as cli_main

from util import build_loop_graph, build_normalize_graph

EXAMPLE_GRAPH = os.path.join(os.path.dirname(__file__), "..",
                             "examples", "normalize.graph.json")


class SimulatorCrossCheckTest(unittest.TestCase):
    def test_simulator_matches_runtime_on_example(self):
        sends = [("src", 0, "a", "A1"),
                 ("src", 0, "z", "Z1"),
                 ("src", 0, "a", "A2")]
        sim = simulate(build_normalize_graph(), sends, max_round=16)
        self.assertIsNone(sim.failure)

        rt = Runtime(build_normalize_graph(), max_round=16)
        rt.declare_epoch(0)
        rt.start()
        src = rt.source("src")
        for (_, ep, value, mid) in sends:
            src.send(ep, value, msg_id=mid)
        src.close_epoch(0)
        result = rt.wait(10)
        self.assertEqual(result.state, "FINISHED")

        # identical sink bag (identities, epochs, rounds, values)
        got = sorted((r["id"], r["epoch"], r["round"], r["value"])
                     for r in rt.collected("sink"))
        self.assertEqual(got, sim.sink_bag())
        # identical derived-message count and per-edge send counts
        self.assertEqual(result.derived, sim.derived)
        for eid, n in sim.edge_sent.items():
            self.assertEqual(result.stats["edges"][eid]["sent"], n, eid)
        # identical completion sequence (from the persisted progress log)
        kinds = [(e["kind"], e["epoch"], e["round"])
                 for e in rt.progress_events()
                 if e["kind"] in ("round_complete", "epoch_complete")]
        expected = ([("round_complete", ep, r) for (ep, r)
                     in sim.round_completions]
                    + [("epoch_complete", ep, None)
                       for ep in sim.epoch_completions])
        self.assertEqual(kinds, expected)
        rt.shutdown()

    def test_simulator_predicts_round_limit_failure(self):
        sim = simulate(build_loop_graph(), [("src", 0, "x", "X")], max_round=3)
        self.assertIsNotNone(sim.failure)
        self.assertEqual(sim.failure[0], "round_limit")

    def test_simulator_multiplicity(self):
        sends = [("src", 0, "z", "Z1"), ("src", 0, "z", "Z2")]
        sim = simulate(build_normalize_graph(), sends, max_round=16)
        values = sorted(m.value for m in sim.sink_records)
        self.assertEqual(values, ["z", "z", "z", "z"])


class GraphFileTest(unittest.TestCase):
    def test_graph_file_entry(self):
        rt = Runtime(load_graph(EXAMPLE_GRAPH), max_round=16)
        rt.declare_epoch(0)
        rt.start()
        src = rt.source("src")
        src.send(0, "a", msg_id="A")
        src.send(0, "z", msg_id="Z")
        src.close_epoch(0)
        self.assertEqual(rt.wait(10).state, "FINISHED")
        got = {(r["id"], r["epoch"], r["round"], r["value"])
               for r in rt.collected("sink")}
        self.assertEqual(got, {
            ("A>m[0]~m->f_loop>m[0]~m->f_fin>dup[0]", 0, 1, "c"),
            ("A>m[0]~m->f_loop>m[0]~m->f_fin>dup[1]", 0, 1, "c"),
            ("Z>m[0]~m->f_fin>dup[0]", 0, 0, "z"),
            ("Z>m[0]~m->f_fin>dup[1]", 0, 0, "z"),
        })
        rt.shutdown()


class CliTest(unittest.TestCase):
    def test_run_and_simulate_commands(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "run.db")
            rc = cli_main(["run", EXAMPLE_GRAPH,
                           "--send", "src:0:a", "--send", "src:0:z",
                           "--max-round", "16", "--db", db])
            self.assertEqual(rc, 0)
            # the progress record persists in the run's SQLite file
            con = sqlite3.connect(db)
            rows = con.execute(
                "SELECT kind, epoch, round FROM progress_events ORDER BY seq"
            ).fetchall()
            con.close()
            self.assertEqual(rows, [
                ("round_complete", 0, 0),
                ("round_complete", 0, 1),
                ("epoch_complete", 0, None),
                ("run_finished", None, None),
            ])
            rc = cli_main(["simulate", EXAMPLE_GRAPH,
                           "--send", "src:0:a", "--send", "src:0:z"])
            self.assertEqual(rc, 0)


if __name__ == "__main__":
    unittest.main()
