"""Command-line entry: run a graph file or simulate it.

    python -m iterflow run      graph.json --send src:0:a --send src:0:z
    python -m iterflow simulate graph.json --send src:0:a --send src:0:z

`run` executes the graph with the real runtime (SQLite spill + progress log
in the --db file) and prints sink records, progress events and edge stats.
`simulate` runs the independent single-threaded simulator and prints the
expected results for comparison.
"""

import argparse
import json
import sys

from .errors import RunStoppedError
from .graphfile import load_graph
from .runtime import Runtime
from .simulator import simulate


def _parse_sends(items):
    sends = []
    for item in items:
        parts = item.split(":", 2)
        if len(parts) != 3:
            raise SystemExit(f"--send expects SRC:EPOCH:VALUE, got {item!r}")
        src, epoch, value = parts
        sends.append((src, int(epoch), value))
    return sends


def _add_common(sp):
    sp.add_argument("graph", help="path to the JSON graph file")
    sp.add_argument("--send", action="append", default=[],
                    metavar="SRC:EPOCH:VALUE", help="one input record")
    sp.add_argument("--max-round", type=int, default=None)
    sp.add_argument("--max-derived", type=int, default=None)
    sp.add_argument("--capacity", type=int, default=8,
                    help="default edge buffer capacity (>=1)")
    sp.add_argument("--db", default=None, help="SQLite file for this run")


def main(argv=None):
    parser = argparse.ArgumentParser(prog="iterflow")
    sub = parser.add_subparsers(dest="cmd", required=True)
    _add_common(sub.add_parser("run", help="execute the graph"))
    _add_common(sub.add_parser("simulate", help="run the independent simulator"))
    args = parser.parse_args(argv)

    graph = load_graph(args.graph)
    sends = _parse_sends(args.send)
    epochs = sorted({ep for (_, ep, _) in sends})

    if args.cmd == "simulate":
        sim = simulate(graph, [(s, e, v, f"{s}#{e}#{i}")
                               for i, (s, e, v) in enumerate(sends)],
                       max_round=args.max_round, max_derived=args.max_derived)
        print("SIMULATED SINK RECORDS (id, epoch, round, value):")
        for m in sim.sink_records:
            print(f"  {m.id}  epoch={m.epoch} round={m.round} value={m.value!r}")
        print("SIMULATED ROUND COMPLETIONS:", sim.round_completions)
        print("SIMULATED EPOCH COMPLETIONS:", sim.epoch_completions)
        print("SIMULATED DERIVED:", sim.derived)
        print("SIMULATED EDGE SENDS:", json.dumps(sim.edge_sent, sort_keys=True))
        if sim.failure:
            print("SIMULATED FAILURE:", sim.failure)
            return 2
        return 0

    rt = Runtime(graph, db_path=args.db, max_round=args.max_round,
                 max_derived=args.max_derived, default_capacity=args.capacity)
    for ep in epochs:
        rt.declare_epoch(ep)
    rt.start()
    try:
        try:
            for (src, ep, value) in sends:
                rt.source(src).send(ep, value)
            for node in graph.source_nodes:
                rt.source(node.id).close_all()
        except RunStoppedError:
            pass  # the run already terminated (e.g. a limit); report it below
        result = rt.wait(timeout=60)

        print("SINK RECORDS (id, epoch, round, value):")
        for sink in graph.sink_nodes:
            for rec in rt.collected(sink.id):
                print(f"  {rec['id']}  epoch={rec['epoch']} "
                      f"round={rec['round']} value={rec['value']!r}")
        print("PROGRESS EVENTS:")
        for ev in rt.progress_events():
            loc = f"epoch={ev['epoch']} round={ev['round']}"
            print(f"  [{ev['seq']:>3}] {ev['kind']:<15} {loc} {ev['detail']}")
        print("EDGE STATS:")
        for eid, st in sorted(result.stats["edges"].items()):
            print(f"  {eid:<16} sent={st['sent']} received={st['received']} "
                  f"spilled={st['spilled']} peak_buffered={st['peak_buffered']} "
                  f"peak_total={st['peak_total']}")
        print(f"RUN: state={result.state} reason={result.reason!r} "
              f"derived={result.derived}")
    finally:
        rt.shutdown()
    return 0 if result.state == "FINISHED" else 2


if __name__ == "__main__":
    sys.exit(main())
